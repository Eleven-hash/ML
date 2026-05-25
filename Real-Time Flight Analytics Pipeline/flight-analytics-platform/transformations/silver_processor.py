"""
=============================================================================
 Silver Processor — Flight Analytics Platform
=============================================================================
 Second layer of the Medallion Architecture. Transforms raw Bronze data
 into cleaned, validated, and enriched Silver tables.

 Responsibilities:
   - Data cleaning and standardization
   - Null handling with business rules
   - Deduplication (window-based, keeping latest per aircraft)
   - Timestamp conversion (Unix epoch → UTC timestamps)
   - Unit conversion (m/s → km/h, meters → feet)
   - Data enrichment (region classification, flight phase detection)
   - Data quality scoring and flagging
   - Quarantine of invalid records

 Usage:
   processor = SilverProcessor(spark, config)
   metrics = processor.process_bronze_to_silver()
=============================================================================
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType, IntegerType

from configs.app_config import AppConfig
from configs.schemas import FlightSchemas
from utils.delta_utils import DeltaTableManager
from utils.spark_utils import SparkUtils
from utils.logger import FlightLogger, log_execution

logger = logging.getLogger("flight_analytics.transformations.silver")


# ══════════════════════════════════════════════════════════════════════
#  REGION MAPPING — Used for geographic enrichment
# ══════════════════════════════════════════════════════════════════════
REGION_MAPPING = {
    # North America
    "United States": "North America",
    "Canada": "North America",
    "Mexico": "North America",
    # Europe
    "Germany": "Europe",
    "France": "Europe",
    "United Kingdom": "Europe",
    "Spain": "Europe",
    "Italy": "Europe",
    "Netherlands": "Europe",
    "Switzerland": "Europe",
    "Austria": "Europe",
    "Belgium": "Europe",
    "Sweden": "Europe",
    "Norway": "Europe",
    "Denmark": "Europe",
    "Finland": "Europe",
    "Poland": "Europe",
    "Ireland": "Europe",
    "Portugal": "Europe",
    "Czech Republic": "Europe",
    "Greece": "Europe",
    "Romania": "Europe",
    "Hungary": "Europe",
    "Turkey": "Europe",
    "Luxembourg": "Europe",
    # Asia
    "China": "Asia",
    "Japan": "Asia",
    "South Korea": "Asia",
    "India": "Asia",
    "Singapore": "Asia",
    "Thailand": "Asia",
    "Malaysia": "Asia",
    "Indonesia": "Asia",
    "Philippines": "Asia",
    "Vietnam": "Asia",
    "Taiwan": "Asia",
    "Hong Kong": "Asia",
    "Pakistan": "Asia",
    # Middle East
    "United Arab Emirates": "Middle East",
    "Saudi Arabia": "Middle East",
    "Qatar": "Middle East",
    "Israel": "Middle East",
    "Iran": "Middle East",
    "Iraq": "Middle East",
    "Kuwait": "Middle East",
    "Oman": "Middle East",
    "Bahrain": "Middle East",
    "Jordan": "Middle East",
    # Oceania
    "Australia": "Oceania",
    "New Zealand": "Oceania",
    # South America
    "Brazil": "South America",
    "Argentina": "South America",
    "Chile": "South America",
    "Colombia": "South America",
    "Peru": "South America",
    # Africa
    "South Africa": "Africa",
    "Egypt": "Africa",
    "Nigeria": "Africa",
    "Kenya": "Africa",
    "Ethiopia": "Africa",
    "Morocco": "Africa",
    # Russia & CIS
    "Russia": "Russia & CIS",
    "Ukraine": "Russia & CIS",
    "Kazakhstan": "Russia & CIS",
}


class SilverProcessor:
    """
    Silver layer processor for data cleaning, validation, and enrichment.

    Transforms raw Bronze data into analysis-ready Silver tables with:
      - Proper data types and timestamps
      - Standardized units (metric + imperial)
      - Geographic enrichment
      - Flight phase classification
      - Data quality flags
    """

    # ── Conversion constants ───────────────────────────────────────────
    METERS_TO_FEET = 3.28084
    MS_TO_KMH = 3.6
    MS_TO_KNOTS = 1.94384
    MS_TO_FPM = 196.85  # m/s to feet per minute

    def __init__(self, spark: SparkSession, config: AppConfig):
        self.spark = spark
        self.config = config
        self._delta = DeltaTableManager(spark, config.delta)
        self._processed_batches = 0
        self._total_records = 0
        self._quarantined_records = 0

        logger.info(
            "SilverProcessor initialized | silver_path=%s",
            config.delta.silver_path,
        )

    @log_execution(logger)
    def process_bronze_to_silver(
        self,
        bronze_df: Optional[DataFrame] = None,
        batch_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Full Bronze → Silver transformation pipeline.

        Steps:
          1. Read from Bronze (or accept pre-loaded DataFrame)
          2. Clean and standardize data
          3. Convert timestamps and units
          4. Deduplicate records
          5. Enrich with derived columns
          6. Validate and score data quality
          7. Separate valid / quarantine records
          8. Write to Silver Delta table

        Args:
            bronze_df: Optional pre-loaded Bronze DataFrame
            batch_filter: Optional batch_id filter for incremental processing

        Returns:
            Processing metrics dict
        """
        metrics = {
            "layer": "silver",
            "start_time": datetime.now(timezone.utc).isoformat(),
        }

        try:
            # ── Step 1: Read Bronze ────────────────────────────────────
            if bronze_df is None:
                bronze_df = self.spark.read.format("delta").load(
                    self.config.delta.bronze_path
                )
                if batch_filter:
                    bronze_df = bronze_df.where(
                        F.col("batch_id") == batch_filter
                    )

            initial_count = bronze_df.count()
            metrics["input_records"] = initial_count
            logger.info("Read %d records from Bronze", initial_count)

            # ── Step 2: Clean data ─────────────────────────────────────
            cleaned_df = self._clean_data(bronze_df)

            # ── Step 3: Convert timestamps and units ───────────────────
            converted_df = self._convert_timestamps(cleaned_df)
            converted_df = self._convert_units(converted_df)

            # ── Step 4: Deduplicate ────────────────────────────────────
            deduped_df = self._deduplicate(converted_df)
            deduped_count = deduped_df.count()
            metrics["after_dedup"] = deduped_count
            metrics["duplicates_removed"] = initial_count - deduped_count

            # ── Step 5: Enrich ─────────────────────────────────────────
            enriched_df = self._enrich_data(deduped_df)

            # ── Step 6: Data quality scoring ───────────────────────────
            scored_df = self._score_data_quality(enriched_df)

            # ── Step 7: Separate valid / quarantine ────────────────────
            valid_df = scored_df.where(F.col("is_valid") == True)
            quarantine_df = scored_df.where(F.col("is_valid") == False)

            valid_count = valid_df.count()
            quarantine_count = quarantine_df.count()

            metrics["valid_records"] = valid_count
            metrics["quarantined_records"] = quarantine_count

            # ── Step 8: Add processing metadata ───────────────────────
            valid_df = valid_df.withColumn(
                "processing_timestamp", F.current_timestamp()
            )

            # ── Step 9: Write to Silver Delta ──────────────────────────
            with FlightLogger.timer(logger, "silver_delta_write"):
                self._delta.write_to_delta(
                    df=valid_df,
                    path=self.config.delta.silver_path,
                    mode="append",
                    partition_by=["ingestion_date"],
                    merge_schema=True,
                )

            # ── Step 10: Write quarantine records ──────────────────────
            if quarantine_count > 0:
                self._delta.write_to_delta(
                    df=quarantine_df,
                    path=self.config.delta.quarantine_path,
                    mode="append",
                )
                logger.warning(
                    "Quarantined %d records", quarantine_count
                )

            self._processed_batches += 1
            self._total_records += valid_count
            self._quarantined_records += quarantine_count

            metrics.update({
                "status": "success",
                "end_time": datetime.now(timezone.utc).isoformat(),
            })

            logger.info(
                "Silver processing complete | valid=%d | quarantined=%d",
                valid_count, quarantine_count,
            )

        except Exception as e:
            logger.error(
                "Silver processing failed: %s", str(e), exc_info=True
            )
            metrics["status"] = "failed"
            metrics["error"] = str(e)

        return metrics

    def _clean_data(self, df: DataFrame) -> DataFrame:
        """
        Clean raw flight data.

        Operations:
          - Trim whitespace from string columns
          - Standardize callsign format
          - Remove clearly invalid records
          - Normalize country names
        """
        logger.info("Cleaning data...")

        cleaned = df

        # ── Trim callsign whitespace ───────────────────────────────────
        if "callsign" in cleaned.columns:
            cleaned = cleaned.withColumn(
                "callsign",
                F.upper(F.trim(F.col("callsign")))
            )

        # ── Standardize country names ──────────────────────────────────
        if "origin_country" in cleaned.columns:
            cleaned = cleaned.withColumn(
                "origin_country",
                F.trim(F.col("origin_country"))
            )

        # ── Remove records with null icao24 (essential identifier) ────
        cleaned = cleaned.where(
            F.col("icao24").isNotNull() & (F.length(F.col("icao24")) > 0)
        )

        # ── Clamp coordinates to valid ranges ──────────────────────────
        if "latitude" in cleaned.columns:
            cleaned = cleaned.withColumn(
                "latitude",
                F.when(
                    (F.col("latitude") >= -90) & (F.col("latitude") <= 90),
                    F.col("latitude"),
                ).otherwise(None),
            )

        if "longitude" in cleaned.columns:
            cleaned = cleaned.withColumn(
                "longitude",
                F.when(
                    (F.col("longitude") >= -180) & (F.col("longitude") <= 180),
                    F.col("longitude"),
                ).otherwise(None),
            )

        return cleaned

    def _convert_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Convert Unix epoch timestamps to proper UTC timestamps.

        OpenSky returns time_position and last_contact as Unix seconds.
        """
        logger.info("Converting timestamps...")

        result = df

        # ── time_position → position_timestamp ─────────────────────────
        if "time_position" in result.columns:
            result = result.withColumn(
                "position_timestamp",
                F.from_unixtime(F.col("time_position")).cast("timestamp"),
            )
            result = result.withColumn(
                "position_date",
                F.date_format(F.col("position_timestamp"), "yyyy-MM-dd"),
            )
            result = result.withColumn(
                "position_hour",
                F.hour(F.col("position_timestamp")),
            )

        # ── last_contact → last_contact_timestamp ──────────────────────
        if "last_contact" in result.columns:
            result = result.withColumn(
                "last_contact_timestamp",
                F.from_unixtime(F.col("last_contact")).cast("timestamp"),
            )

        return result

    def _convert_units(self, df: DataFrame) -> DataFrame:
        """
        Convert units to standard metric AND imperial formats.

        Conversions:
          - baro_altitude: meters → feet
          - geo_altitude: meters → feet
          - velocity: m/s → km/h → knots
          - vertical_rate: m/s → ft/min
        """
        logger.info("Converting units...")

        result = df

        # ── Altitude conversions ───────────────────────────────────────
        if "baro_altitude" in result.columns:
            result = (
                result.withColumn("baro_altitude_m", F.col("baro_altitude"))
                .withColumn(
                    "baro_altitude_ft",
                    F.round(F.col("baro_altitude") * self.METERS_TO_FEET, 0),
                )
            )

        if "geo_altitude" in result.columns:
            result = (
                result.withColumn("geo_altitude_m", F.col("geo_altitude"))
                .withColumn(
                    "geo_altitude_ft",
                    F.round(F.col("geo_altitude") * self.METERS_TO_FEET, 0),
                )
            )

        # ── Speed conversions ──────────────────────────────────────────
        if "velocity" in result.columns:
            result = (
                result.withColumn("velocity_ms", F.col("velocity"))
                .withColumn(
                    "velocity_kmh",
                    F.round(F.col("velocity") * self.MS_TO_KMH, 1),
                )
                .withColumn(
                    "velocity_knots",
                    F.round(F.col("velocity") * self.MS_TO_KNOTS, 1),
                )
            )

        # ── Vertical rate conversion ───────────────────────────────────
        if "vertical_rate" in result.columns:
            result = (
                result.withColumn("vertical_rate_ms", F.col("vertical_rate"))
                .withColumn(
                    "vertical_rate_fpm",
                    F.round(F.col("vertical_rate") * self.MS_TO_FPM, 0),
                )
            )

        # ── Heading ────────────────────────────────────────────────────
        if "true_track" in result.columns:
            result = result.withColumn(
                "true_track_deg", F.col("true_track")
            )

        return result

    def _deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate records using window functions.

        Keeps the latest record per aircraft (icao24) within each batch,
        ordered by time_position descending.
        """
        logger.info("Deduplicating records...")

        window_spec = Window.partitionBy("icao24", "ingestion_date").orderBy(
            F.col("time_position").desc_nulls_last()
        )

        deduped = (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .where(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        return deduped

    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich flight data with derived columns.

        Adds:
          - region: Geographic region from country mapping
          - flight_phase: Derived from altitude and vertical rate
          - speed_category: Speed classification
          - altitude_band: Altitude classification
          - position_source label: Human-readable source name
        """
        logger.info("Enriching data...")

        result = df

        # ── Region mapping ─────────────────────────────────────────────
        region_expr = F.lit("Other")
        for country, region in reversed(list(REGION_MAPPING.items())):
            region_expr = F.when(
                F.col("origin_country") == country, F.lit(region)
            ).otherwise(region_expr)

        result = result.withColumn("region", region_expr)

        # ── Flight phase classification ────────────────────────────────
        result = result.withColumn(
            "flight_phase",
            F.when(F.col("on_ground") == True, "ground")
            .when(
                (F.col("vertical_rate").isNotNull())
                & (F.col("vertical_rate") > 2.0),
                "climbing",
            )
            .when(
                (F.col("vertical_rate").isNotNull())
                & (F.col("vertical_rate") < -2.0),
                "descending",
            )
            .when(
                F.col("baro_altitude").isNotNull()
                & (F.col("baro_altitude") > 9000),
                "cruising",
            )
            .otherwise("en_route"),
        )

        # ── Speed category ─────────────────────────────────────────────
        result = result.withColumn(
            "speed_category",
            F.when(F.col("velocity").isNull(), "unknown")
            .when(F.col("velocity") < 50, "slow")
            .when(F.col("velocity") < 150, "medium")
            .when(F.col("velocity") < 250, "fast")
            .otherwise("very_fast"),
        )

        # ── Altitude band ─────────────────────────────────────────────
        result = result.withColumn(
            "altitude_band",
            F.when(F.col("baro_altitude").isNull(), "unknown")
            .when(F.col("on_ground") == True, "ground")
            .when(F.col("baro_altitude") < 3000, "low")
            .when(F.col("baro_altitude") < 7000, "medium")
            .when(F.col("baro_altitude") < 12000, "high")
            .otherwise("very_high"),
        )

        # ── Position source label ──────────────────────────────────────
        result = result.withColumn(
            "position_source",
            F.when(F.col("position_source") == 0, "ADS-B")
            .when(F.col("position_source") == 1, "ASTERIX")
            .when(F.col("position_source") == 2, "MLAT")
            .when(F.col("position_source") == 3, "FLARM")
            .otherwise("Unknown"),
        )

        return result

    def _score_data_quality(self, df: DataFrame) -> DataFrame:
        """
        Score data quality and flag issues.

        Checks:
          - Missing critical fields (icao24, origin_country)
          - Invalid coordinates
          - Unrealistic speed values
          - Unrealistic altitude values
          - Stale timestamps
        """
        logger.info("Scoring data quality...")

        # ── Build DQ flag array ────────────────────────────────────────
        dq_checks = F.array()

        # Check 1: Missing callsign
        dq_checks = F.when(
            F.col("callsign").isNull() | (F.col("callsign") == ""),
            F.array_union(dq_checks, F.array(F.lit("missing_callsign"))),
        ).otherwise(dq_checks)

        # Check 2: Missing coordinates
        dq_checks = F.when(
            F.col("latitude").isNull() | F.col("longitude").isNull(),
            F.array_union(dq_checks, F.array(F.lit("missing_coordinates"))),
        ).otherwise(dq_checks)

        # Check 3: Extreme velocity (> Mach 1.2 ≈ 400 m/s)
        dq_checks = F.when(
            F.col("velocity").isNotNull() & (F.col("velocity") > 400),
            F.array_union(dq_checks, F.array(F.lit("extreme_velocity"))),
        ).otherwise(dq_checks)

        # Check 4: Unrealistic altitude (> 60,000 ft ≈ 18,288 m)
        dq_checks = F.when(
            F.col("baro_altitude").isNotNull()
            & (F.col("baro_altitude") > 18288),
            F.array_union(dq_checks, F.array(F.lit("extreme_altitude"))),
        ).otherwise(dq_checks)

        result = df.withColumn("dq_flags", dq_checks)

        # ── Determine overall validity ─────────────────────────────────
        # A record is invalid if it has critical DQ issues
        critical_flags = ["missing_coordinates", "extreme_velocity"]

        result = result.withColumn(
            "is_valid",
            ~F.arrays_overlap(
                F.col("dq_flags"),
                F.array(*[F.lit(f) for f in critical_flags]),
            ),
        )

        return result

    def read_silver(
        self, filter_date: Optional[str] = None
    ) -> DataFrame:
        """Read from Silver Delta table."""
        df = self.spark.read.format("delta").load(
            self.config.delta.silver_path
        )
        if filter_date:
            df = df.where(F.col("ingestion_date") == filter_date)
        return df

    def get_metrics(self) -> Dict[str, Any]:
        """Get processor metrics."""
        return {
            "processed_batches": self._processed_batches,
            "total_records": self._total_records,
            "quarantined_records": self._quarantined_records,
        }
