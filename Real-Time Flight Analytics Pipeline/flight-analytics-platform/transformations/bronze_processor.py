"""
=============================================================================
 Bronze Processor — Flight Analytics Platform
=============================================================================
 First layer of the Medallion Architecture. The Bronze layer stores raw
 data exactly as received from the source, with minimal transformation.

 Responsibilities:
   - Raw data landing from API ingestion
   - Schema enforcement on incoming data
   - Schema evolution handling (auto-merge new columns)
   - Append-only writes (immutable audit trail)
   - Metadata enrichment (ingestion timestamps, batch IDs)
   - Data lineage tracking

 Design Principle:
   "Store everything, transform nothing" — Bronze preserves the raw
   data for reproducibility and auditing. All cleaning and business
   logic happens in Silver and Gold layers.

 Usage:
   processor = BronzeProcessor(spark, config)
   processor.process_raw_data(raw_df)
=============================================================================
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from configs.app_config import AppConfig
from configs.schemas import FlightSchemas
from utils.delta_utils import DeltaTableManager
from utils.spark_utils import SparkUtils
from utils.logger import FlightLogger, log_execution

logger = logging.getLogger("flight_analytics.transformations.bronze")


class BronzeProcessor:
    """
    Bronze layer processor for raw flight data landing.

    Implements the "store everything" principle with:
      - Schema enforcement and evolution
      - Append-only Delta writes
      - Comprehensive metadata tracking
      - Data lineage columns
    """

    def __init__(self, spark: SparkSession, config: AppConfig):
        """
        Initialize Bronze processor.

        Args:
            spark: Active SparkSession
            config: Application configuration
        """
        self.spark = spark
        self.config = config
        self._delta = DeltaTableManager(spark, config.delta)
        self._expected_schema = FlightSchemas.bronze_flights()
        self._processed_batches = 0
        self._total_records = 0

        logger.info(
            "BronzeProcessor initialized | path=%s",
            config.delta.bronze_path,
        )

    @log_execution(logger)
    def process_raw_data(
        self,
        raw_df: DataFrame,
        batch_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Process raw flight data and write to Bronze Delta table.

        Steps:
          1. Validate incoming schema
          2. Add metadata columns (if not already present)
          3. Handle schema evolution
          4. Append to Bronze Delta table
          5. Return processing metrics

        Args:
            raw_df: Raw DataFrame from API ingestion
            batch_id: Batch identifier for tracking

        Returns:
            Processing metrics dict
        """
        batch_id = batch_id or f"bronze_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        metrics = {
            "batch_id": batch_id,
            "layer": "bronze",
            "start_time": datetime.now(timezone.utc).isoformat(),
        }

        try:
            # ── Step 1: Schema Validation ──────────────────────────────
            is_valid, missing, extra = SparkUtils.validate_schema(
                raw_df, self._expected_schema, strict=False
            )

            if missing:
                # Add missing columns as nulls (schema evolution)
                for field_name in missing:
                    field = next(
                        f for f in self._expected_schema.fields
                        if f.name == field_name
                    )
                    raw_df = raw_df.withColumn(
                        field_name, F.lit(None).cast(field.dataType)
                    )
                    logger.info(
                        "Added missing column: %s (%s)",
                        field_name, field.dataType,
                    )

            if extra:
                logger.info(
                    "Extra columns detected (schema evolution): %s", extra
                )

            # ── Step 2: Ensure metadata columns ───────────────────────
            df = self._ensure_metadata(raw_df, batch_id)

            # ── Step 3: Add data lineage columns ──────────────────────
            df = self._add_lineage(df)

            record_count = df.count()

            # ── Step 4: Write to Bronze Delta ─────────────────────────
            with FlightLogger.timer(logger, "bronze_delta_write"):
                self._delta.write_to_delta(
                    df=df,
                    path=self.config.delta.bronze_path,
                    mode="append",
                    partition_by=self.config.delta.partition_columns,
                    merge_schema=True,
                )

            # ── Step 5: Update metrics ────────────────────────────────
            self._processed_batches += 1
            self._total_records += record_count

            metrics.update({
                "status": "success",
                "records_processed": record_count,
                "total_records": self._total_records,
                "total_batches": self._processed_batches,
                "schema_evolution": {
                    "missing_columns_added": missing,
                    "extra_columns_detected": extra,
                },
                "end_time": datetime.now(timezone.utc).isoformat(),
            })

            logger.info(
                "Bronze processing complete | batch=%s | records=%d",
                batch_id, record_count,
            )

        except Exception as e:
            logger.error(
                "Bronze processing failed | batch=%s | error=%s",
                batch_id, str(e), exc_info=True,
            )
            metrics["status"] = "failed"
            metrics["error"] = str(e)

        return metrics

    def _ensure_metadata(
        self, df: DataFrame, batch_id: str
    ) -> DataFrame:
        """
        Ensure metadata columns exist on the DataFrame.

        Args:
            df: Input DataFrame
            batch_id: Batch identifier

        Returns:
            DataFrame with metadata columns
        """
        # ── ingestion_timestamp ────────────────────────────────────────
        if "ingestion_timestamp" not in df.columns:
            df = df.withColumn(
                "ingestion_timestamp", F.current_timestamp()
            )

        # ── ingestion_date (partition column) ──────────────────────────
        if "ingestion_date" not in df.columns:
            df = df.withColumn(
                "ingestion_date",
                F.date_format(F.col("ingestion_timestamp"), "yyyy-MM-dd"),
            )

        # ── batch_id ──────────────────────────────────────────────────
        if "batch_id" not in df.columns:
            df = df.withColumn("batch_id", F.lit(batch_id))

        # ── source_system ─────────────────────────────────────────────
        if "source_system" not in df.columns:
            df = df.withColumn(
                "source_system", F.lit("opensky_api")
            )

        return df

    def _add_lineage(self, df: DataFrame) -> DataFrame:
        """
        Add data lineage tracking columns.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with lineage columns
        """
        return (
            df.withColumn(
                "_bronze_processed_at", F.current_timestamp()
            )
            .withColumn(
                "_pipeline_version", F.lit("1.0.0")
            )
        )

    def read_bronze(
        self,
        filter_date: Optional[str] = None,
        filter_batch: Optional[str] = None,
    ) -> DataFrame:
        """
        Read from Bronze Delta table with optional filters.

        Args:
            filter_date: Filter by ingestion_date (YYYY-MM-DD)
            filter_batch: Filter by batch_id

        Returns:
            Filtered Bronze DataFrame
        """
        df = self.spark.read.format("delta").load(
            self.config.delta.bronze_path
        )

        if filter_date:
            df = df.where(F.col("ingestion_date") == filter_date)

        if filter_batch:
            df = df.where(F.col("batch_id") == filter_batch)

        logger.info(
            "Read Bronze table | date=%s | batch=%s | rows=%d",
            filter_date, filter_batch, df.count(),
        )
        return df

    def get_latest_batch(self) -> Optional[DataFrame]:
        """Get the most recent batch from Bronze."""
        try:
            df = self.spark.read.format("delta").load(
                self.config.delta.bronze_path
            )
            latest_batch = df.agg(
                F.max("batch_id").alias("latest_batch")
            ).collect()[0]["latest_batch"]

            if latest_batch:
                return df.where(F.col("batch_id") == latest_batch)
            return None
        except Exception as e:
            logger.error("Failed to get latest batch: %s", str(e))
            return None

    def optimize_bronze(self) -> None:
        """Run OPTIMIZE and ZORDER on Bronze table."""
        self._delta.optimize_table(
            self.config.delta.bronze_path,
            z_order_columns=self.config.delta.z_order_columns,
        )

    def vacuum_bronze(
        self, retention_hours: Optional[int] = None
    ) -> None:
        """Run VACUUM on Bronze table."""
        self._delta.vacuum_table(
            self.config.delta.bronze_path,
            retention_hours=retention_hours,
        )

    def get_metrics(self) -> Dict[str, Any]:
        """Get processor metrics."""
        return {
            "processed_batches": self._processed_batches,
            "total_records": self._total_records,
            "bronze_path": self.config.delta.bronze_path,
        }
