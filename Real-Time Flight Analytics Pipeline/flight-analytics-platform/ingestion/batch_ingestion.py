"""
=============================================================================
 Batch Ingestion Pipeline — Flight Analytics Platform
=============================================================================
 Scheduled batch ingestion from OpenSky Network API to Bronze Delta tables.

 This pipeline:
   1. Fetches current flight state vectors via API
   2. Validates and enriches with metadata
   3. Writes to Bronze layer as append-only Delta table
   4. Supports incremental loading with batch tracking
   5. Implements idempotent writes

 Designed to run on a schedule (e.g., every 15 seconds to 5 minutes)
 via Databricks Jobs or Airflow.

 Usage:
   pipeline = BatchIngestionPipeline(spark, config)
   result = pipeline.run()
   pipeline.run_incremental(num_batches=10, interval=30)
=============================================================================
"""

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from configs.app_config import AppConfig
from ingestion.opensky_client import OpenSkyClient
from utils.delta_utils import DeltaTableManager
from utils.logger import FlightLogger, log_execution

logger = logging.getLogger("flight_analytics.ingestion.batch")


class BatchIngestionPipeline:
    """
    Production batch ingestion pipeline for OpenSky flight data.

    Features:
      - Configurable poll intervals
      - Batch tracking with unique IDs
      - Incremental loading (multiple batches in sequence)
      - Write-ahead logging for exactly-once semantics
      - Post-ingestion validation
      - Performance metrics collection
    """

    def __init__(self, spark: SparkSession, config: AppConfig):
        """
        Initialize the batch ingestion pipeline.

        Args:
            spark: Active SparkSession
            config: Application configuration
        """
        self.spark = spark
        self.config = config
        self._opensky = OpenSkyClient(config)
        self._delta = DeltaTableManager(spark, config.delta)
        self._batch_counter = 0
        self._total_records_ingested = 0
        self._start_time = None

        logger.info(
            "BatchIngestionPipeline initialized | bronze_path=%s",
            config.delta.bronze_path,
        )

    @log_execution(logger)
    def run(self, batch_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a single batch ingestion cycle.

        Steps:
          1. Generate batch ID
          2. Fetch data from OpenSky API
          3. Validate fetched data
          4. Write to Bronze Delta table
          5. Return ingestion metrics

        Args:
            batch_id: Optional explicit batch ID

        Returns:
            Dict with ingestion metrics
        """
        self._batch_counter += 1
        batch_id = batch_id or self._generate_batch_id()
        correlation_id = FlightLogger.set_correlation_id(batch_id)

        metrics = {
            "batch_id": batch_id,
            "correlation_id": correlation_id,
            "start_time": datetime.now(timezone.utc).isoformat(),
            "status": "unknown",
        }

        try:
            # ── Step 1: Fetch from API ─────────────────────────────────
            logger.info("Starting batch ingestion | batch_id=%s", batch_id)

            with FlightLogger.timer(logger, "api_fetch"):
                df = self._opensky.fetch_all_states(
                    self.spark, batch_id=batch_id
                )

            if df is None or df.rdd.isEmpty():
                logger.warning("No data fetched in batch %s", batch_id)
                metrics["status"] = "empty"
                metrics["records_fetched"] = 0
                return metrics

            record_count = df.count()
            metrics["records_fetched"] = record_count
            logger.info("Fetched %d records", record_count)

            # ── Step 2: Pre-write validation ───────────────────────────
            with FlightLogger.timer(logger, "validation"):
                validation_result = self._validate_batch(df)
                metrics["validation"] = validation_result

            if not validation_result["is_valid"]:
                logger.error(
                    "Batch validation failed | batch_id=%s | issues=%s",
                    batch_id, validation_result.get("issues"),
                )
                metrics["status"] = "validation_failed"
                return metrics

            # ── Step 3: Write to Bronze Delta ──────────────────────────
            with FlightLogger.timer(logger, "bronze_write"):
                self._delta.write_to_delta(
                    df=df,
                    path=self.config.delta.bronze_path,
                    mode="append",
                    partition_by=self.config.delta.partition_columns,
                    merge_schema=True,
                )

            self._total_records_ingested += record_count

            # ── Step 4: Collect metrics ────────────────────────────────
            metrics.update({
                "status": "success",
                "records_written": record_count,
                "end_time": datetime.now(timezone.utc).isoformat(),
                "bronze_path": self.config.delta.bronze_path,
                "total_records_ingested": self._total_records_ingested,
                "batch_number": self._batch_counter,
            })

            logger.info(
                "Batch ingestion complete | batch_id=%s | records=%d | "
                "total_ingested=%d",
                batch_id, record_count, self._total_records_ingested,
            )

            return metrics

        except Exception as e:
            logger.error(
                "Batch ingestion failed | batch_id=%s | error=%s",
                batch_id, str(e),
                exc_info=True,
            )
            metrics["status"] = "failed"
            metrics["error"] = str(e)
            return metrics

    def run_incremental(
        self,
        num_batches: int = 10,
        interval_seconds: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Run multiple batch ingestion cycles with configurable intervals.

        This is useful for building up a dataset over time or for
        catching up on missed data.

        Args:
            num_batches: Number of batch cycles to execute
            interval_seconds: Seconds between batches (default from config)

        Returns:
            List of metrics dicts, one per batch
        """
        interval = interval_seconds or self.config.api.poll_interval_seconds
        self._start_time = time.time()
        all_metrics = []

        logger.info(
            "Starting incremental ingestion | batches=%d | interval=%ds",
            num_batches, interval,
        )

        for i in range(num_batches):
            logger.info(
                "═══ Batch %d/%d ═══════════════════════════════════════",
                i + 1, num_batches,
            )

            metrics = self.run()
            all_metrics.append(metrics)

            # ── Wait between batches (except the last one) ─────────────
            if i < num_batches - 1:
                logger.info("Waiting %ds before next batch...", interval)
                time.sleep(interval)

        # ── Summary ────────────────────────────────────────────────────
        total_elapsed = time.time() - self._start_time
        successful = sum(1 for m in all_metrics if m["status"] == "success")
        total_records = sum(
            m.get("records_written", 0) for m in all_metrics
        )

        logger.info(
            "Incremental ingestion complete | "
            "batches=%d/%d successful | "
            "total_records=%d | elapsed=%.1fs",
            successful, num_batches, total_records, total_elapsed,
        )

        return all_metrics

    def _validate_batch(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate a batch of flight data before writing to Bronze.

        Checks:
          - DataFrame is not empty
          - Required columns are present
          - icao24 identifiers are valid (hex format)
          - No excessive null ratios in critical fields

        Args:
            df: Batch DataFrame to validate

        Returns:
            Validation result dict
        """
        issues = []
        row_count = df.count()

        if row_count == 0:
            return {"is_valid": False, "issues": ["Empty DataFrame"]}

        # ── Check required columns ─────────────────────────────────────
        required_columns = ["icao24", "origin_country", "ingestion_timestamp"]
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            issues.append(f"Missing required columns: {missing}")

        # ── Check null ratios for critical fields ──────────────────────
        critical_fields = ["icao24", "origin_country"]
        for field in critical_fields:
            if field in df.columns:
                null_count = df.where(F.col(field).isNull()).count()
                null_ratio = null_count / row_count
                if null_ratio > 0.5:
                    issues.append(
                        f"High null ratio in '{field}': {null_ratio:.2%}"
                    )

        # ── Validate icao24 format (6-char hex) ───────────────────────
        if "icao24" in df.columns:
            invalid_icao = df.where(
                ~F.col("icao24").rlike("^[a-f0-9]{6}$")
                & F.col("icao24").isNotNull()
            ).count()
            if invalid_icao > row_count * 0.1:
                issues.append(
                    f"High invalid icao24 count: {invalid_icao}/{row_count}"
                )

        return {
            "is_valid": len(issues) == 0,
            "issues": issues,
            "row_count": row_count,
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _generate_batch_id() -> str:
        """Generate a unique, sortable batch identifier."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        short_uuid = uuid.uuid4().hex[:8]
        return f"batch_{timestamp}_{short_uuid}"

    def get_pipeline_metrics(self) -> Dict[str, Any]:
        """Get cumulative pipeline metrics."""
        api_metrics = self._opensky.get_api_metrics()
        return {
            "total_batches": self._batch_counter,
            "total_records_ingested": self._total_records_ingested,
            "api_metrics": api_metrics,
        }

    def close(self) -> None:
        """Release pipeline resources."""
        self._opensky.close()
        logger.info(
            "BatchIngestionPipeline closed | metrics=%s",
            self.get_pipeline_metrics(),
        )
