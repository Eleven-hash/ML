"""
=============================================================================
 Gold Processor — Flight Analytics Platform
=============================================================================
 Third layer of the Medallion Architecture. Produces business-ready
 aggregated datasets from Silver data for dashboards and analytics.

 Gold tables are:
   - Denormalized for read performance
   - Pre-aggregated for common queries
   - Optimized for dashboard rendering
   - Updated incrementally

 Output Tables:
   1. flights_by_country — Country-level flight metrics
   2. traffic_summary — Hourly traffic KPIs
   3. speed_analysis — Speed distribution and percentiles
   4. altitude_trends — Altitude patterns over time
   5. kpi_metrics — Executive KPI cards

 Usage:
   processor = GoldProcessor(spark, config)
   processor.process_silver_to_gold()
=============================================================================
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from configs.app_config import AppConfig
from utils.delta_utils import DeltaTableManager
from utils.logger import FlightLogger, log_execution

logger = logging.getLogger("flight_analytics.transformations.gold")


class GoldProcessor:
    """
    Gold layer processor producing business analytics tables.

    Reads from Silver Delta tables and produces aggregated Gold tables
    optimized for dashboard and BI consumption.
    """

    def __init__(self, spark: SparkSession, config: AppConfig):
        self.spark = spark
        self.config = config
        self._delta = DeltaTableManager(spark, config.delta)

        logger.info(
            "GoldProcessor initialized | gold_base=%s",
            config.delta.gold_path,
        )

    @log_execution(logger)
    def process_silver_to_gold(
        self,
        silver_df: Optional[DataFrame] = None,
    ) -> Dict[str, Any]:
        """
        Run all Gold layer transformations.

        Args:
            silver_df: Optional pre-loaded Silver DataFrame

        Returns:
            Processing metrics
        """
        metrics = {
            "layer": "gold",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "tables": {},
        }

        try:
            # ── Load Silver data ───────────────────────────────────────
            if silver_df is None:
                silver_df = self.spark.read.format("delta").load(
                    self.config.delta.silver_path
                )

            silver_count = silver_df.count()
            metrics["input_records"] = silver_count
            logger.info("Loaded %d records from Silver", silver_count)

            # ── Generate each Gold table ───────────────────────────────
            metrics["tables"]["flights_by_country"] = (
                self._build_flights_by_country(silver_df)
            )
            metrics["tables"]["traffic_summary"] = (
                self._build_traffic_summary(silver_df)
            )
            metrics["tables"]["speed_analysis"] = (
                self._build_speed_analysis(silver_df)
            )
            metrics["tables"]["altitude_trends"] = (
                self._build_altitude_trends(silver_df)
            )
            metrics["tables"]["kpi_metrics"] = (
                self._build_kpi_metrics(silver_df)
            )

            metrics["status"] = "success"
            metrics["end_time"] = datetime.now(timezone.utc).isoformat()

            logger.info("Gold processing complete | tables=%d", len(metrics["tables"]))

        except Exception as e:
            logger.error("Gold processing failed: %s", str(e), exc_info=True)
            metrics["status"] = "failed"
            metrics["error"] = str(e)

        return metrics

    def _build_flights_by_country(
        self, silver_df: DataFrame
    ) -> Dict[str, Any]:
        """
        Build flights_by_country Gold table.

        Aggregations:
          - Total flights per country
          - Active vs grounded flights
          - Average speed and altitude
          - Min/max altitude
        """
        logger.info("Building flights_by_country...")

        gold_df = (
            silver_df.groupBy("origin_country", "region")
            .agg(
                F.count("*").alias("total_flights"),
                F.sum(
                    F.when(F.col("on_ground") == False, 1).otherwise(0)
                ).alias("active_flights"),
                F.sum(
                    F.when(F.col("on_ground") == True, 1).otherwise(0)
                ).alias("grounded_flights"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_velocity_kmh"),
                F.round(F.avg("baro_altitude_ft"), 0).alias("avg_altitude_ft"),
                F.round(F.max("baro_altitude_ft"), 0).alias("max_altitude_ft"),
                F.round(F.min(
                    F.when(
                        F.col("baro_altitude_ft").isNotNull()
                        & (F.col("on_ground") == False),
                        F.col("baro_altitude_ft"),
                    )
                ), 0).alias("min_altitude_ft"),
            )
            .withColumn("snapshot_timestamp", F.current_timestamp())
            .withColumn(
                "snapshot_date",
                F.date_format(F.current_timestamp(), "yyyy-MM-dd"),
            )
            .orderBy(F.desc("total_flights"))
        )

        record_count = gold_df.count()

        # ── Write to Gold Delta ────────────────────────────────────────
        self._delta.write_to_delta(
            df=gold_df,
            path=self.config.delta.gold_flights_by_country_path,
            mode="overwrite",
        )

        logger.info(
            "flights_by_country | countries=%d", record_count
        )
        return {"records": record_count, "status": "success"}

    def _build_traffic_summary(
        self, silver_df: DataFrame
    ) -> Dict[str, Any]:
        """
        Build traffic_summary Gold table.

        Hourly aggregations for traffic trend analysis.
        """
        logger.info("Building traffic_summary...")

        # ── Ensure position_hour exists ────────────────────────────────
        df = silver_df
        if "position_hour" not in df.columns:
            if "position_timestamp" in df.columns:
                df = df.withColumn(
                    "position_hour",
                    F.hour(F.col("position_timestamp")),
                )
            else:
                df = df.withColumn("position_hour", F.lit(0))

        if "position_date" not in df.columns:
            df = df.withColumn(
                "position_date", F.col("ingestion_date")
            )

        gold_df = (
            df.groupBy("position_date", "position_hour")
            .agg(
                F.count("*").alias("total_flights"),
                F.sum(
                    F.when(F.col("on_ground") == False, 1).otherwise(0)
                ).alias("airborne_flights"),
                F.sum(
                    F.when(F.col("on_ground") == True, 1).otherwise(0)
                ).alias("grounded_flights"),
                F.countDistinct("origin_country").alias("unique_countries"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_velocity_kmh"),
                F.round(F.avg("baro_altitude_ft"), 0).alias("avg_altitude_ft"),
                F.round(
                    F.percentile_approx("velocity_kmh", 0.95), 1
                ).alias("p95_velocity_kmh"),
                F.round(
                    F.percentile_approx("baro_altitude_ft", 0.95), 0
                ).alias("p95_altitude_ft"),
            )
            .withColumn("snapshot_timestamp", F.current_timestamp())
            .withColumnRenamed("position_date", "snapshot_date")
            .withColumnRenamed("position_hour", "snapshot_hour")
            .orderBy("snapshot_date", "snapshot_hour")
        )

        record_count = gold_df.count()

        self._delta.write_to_delta(
            df=gold_df,
            path=self.config.delta.gold_traffic_summary_path,
            mode="overwrite",
        )

        logger.info("traffic_summary | entries=%d", record_count)
        return {"records": record_count, "status": "success"}

    def _build_speed_analysis(
        self, silver_df: DataFrame
    ) -> Dict[str, Any]:
        """
        Build speed_analysis Gold table.

        Speed distribution by category and region.
        """
        logger.info("Building speed_analysis...")

        gold_df = (
            silver_df.where(
                F.col("velocity_kmh").isNotNull()
                & (F.col("on_ground") == False)
            )
            .groupBy("speed_category", "region")
            .agg(
                F.count("*").alias("flight_count"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
                F.round(F.min("velocity_kmh"), 1).alias("min_speed_kmh"),
                F.round(F.max("velocity_kmh"), 1).alias("max_speed_kmh"),
                F.round(F.stddev("velocity_kmh"), 1).alias("stddev_speed_kmh"),
                F.round(
                    F.percentile_approx("velocity_kmh", 0.5), 1
                ).alias("median_speed_kmh"),
            )
            .withColumn("snapshot_timestamp", F.current_timestamp())
            .orderBy(F.desc("flight_count"))
        )

        record_count = gold_df.count()

        self._delta.write_to_delta(
            df=gold_df,
            path=self.config.delta.gold_speed_analysis_path,
            mode="overwrite",
        )

        logger.info("speed_analysis | entries=%d", record_count)
        return {"records": record_count, "status": "success"}

    def _build_altitude_trends(
        self, silver_df: DataFrame
    ) -> Dict[str, Any]:
        """
        Build altitude_trends Gold table.

        Altitude distribution and trends by band and flight phase.
        """
        logger.info("Building altitude_trends...")

        gold_df = (
            silver_df.where(F.col("baro_altitude_ft").isNotNull())
            .groupBy("altitude_band", "flight_phase", "region")
            .agg(
                F.count("*").alias("flight_count"),
                F.round(F.avg("baro_altitude_ft"), 0).alias("avg_altitude_ft"),
                F.round(F.avg("vertical_rate_fpm"), 0).alias("avg_vertical_rate_fpm"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
            )
            .withColumn("snapshot_timestamp", F.current_timestamp())
            .orderBy(F.desc("flight_count"))
        )

        record_count = gold_df.count()

        self._delta.write_to_delta(
            df=gold_df,
            path=self.config.delta.gold_altitude_trends_path,
            mode="overwrite",
        )

        logger.info("altitude_trends | entries=%d", record_count)
        return {"records": record_count, "status": "success"}

    def _build_kpi_metrics(
        self, silver_df: DataFrame
    ) -> Dict[str, Any]:
        """
        Build kpi_metrics Gold table.

        Executive-level KPI cards for dashboard top-level display.
        """
        logger.info("Building kpi_metrics...")

        kpis = []

        # ── Total flights ──────────────────────────────────────────────
        total = silver_df.count()
        kpis.append(("total_flights", float(total), "count", None, None, "all"))

        # ── Airborne flights ───────────────────────────────────────────
        airborne = silver_df.where(F.col("on_ground") == False).count()
        kpis.append(("airborne_flights", float(airborne), "count", None, None, "all"))

        # ── Unique countries ───────────────────────────────────────────
        countries = silver_df.select("origin_country").distinct().count()
        kpis.append(("unique_countries", float(countries), "count", None, None, "all"))

        # ── Average speed (airborne) ───────────────────────────────────
        avg_speed_row = (
            silver_df.where(
                (F.col("on_ground") == False)
                & F.col("velocity_kmh").isNotNull()
            )
            .agg(F.round(F.avg("velocity_kmh"), 1).alias("val"))
            .collect()
        )
        if avg_speed_row and avg_speed_row[0]["val"]:
            kpis.append((
                "avg_airborne_speed", float(avg_speed_row[0]["val"]),
                "km/h", None, None, "all",
            ))

        # ── Average altitude (airborne) ────────────────────────────────
        avg_alt_row = (
            silver_df.where(
                (F.col("on_ground") == False)
                & F.col("baro_altitude_ft").isNotNull()
            )
            .agg(F.round(F.avg("baro_altitude_ft"), 0).alias("val"))
            .collect()
        )
        if avg_alt_row and avg_alt_row[0]["val"]:
            kpis.append((
                "avg_airborne_altitude", float(avg_alt_row[0]["val"]),
                "feet", None, None, "all",
            ))

        # ── Top country ────────────────────────────────────────────────
        top_country_row = (
            silver_df.groupBy("origin_country")
            .count()
            .orderBy(F.desc("count"))
            .limit(1)
            .collect()
        )
        if top_country_row:
            kpis.append((
                "top_country_flights",
                float(top_country_row[0]["count"]),
                "count",
                "country",
                top_country_row[0]["origin_country"],
                "all",
            ))

        # ── Create DataFrame ───────────────────────────────────────────
        columns = [
            "metric_name", "metric_value", "metric_unit",
            "dimension", "dimension_value", "time_window",
        ]
        kpi_df = self.spark.createDataFrame(kpis, columns)
        kpi_df = kpi_df.withColumn(
            "calculation_timestamp", F.current_timestamp()
        )

        record_count = kpi_df.count()

        self._delta.write_to_delta(
            df=kpi_df,
            path=self.config.delta.gold_kpi_path,
            mode="overwrite",
        )

        logger.info("kpi_metrics | metrics=%d", record_count)
        return {"records": record_count, "status": "success"}

    def read_gold_table(self, table_name: str) -> DataFrame:
        """
        Read a specific Gold table.

        Args:
            table_name: One of 'flights_by_country', 'traffic_summary',
                       'speed_analysis', 'altitude_trends', 'kpi_metrics'

        Returns:
            Gold DataFrame
        """
        path_map = {
            "flights_by_country": self.config.delta.gold_flights_by_country_path,
            "traffic_summary": self.config.delta.gold_traffic_summary_path,
            "speed_analysis": self.config.delta.gold_speed_analysis_path,
            "altitude_trends": self.config.delta.gold_altitude_trends_path,
            "kpi_metrics": self.config.delta.gold_kpi_path,
            "anomalies": self.config.delta.gold_anomalies_path,
        }

        if table_name not in path_map:
            raise ValueError(
                f"Unknown Gold table: {table_name}. "
                f"Available: {list(path_map.keys())}"
            )

        return self.spark.read.format("delta").load(path_map[table_name])
