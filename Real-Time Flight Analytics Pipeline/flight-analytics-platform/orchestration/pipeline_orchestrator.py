"""
=============================================================================
 Pipeline Orchestrator — Flight Analytics Platform
=============================================================================
 Orchestrates the end-to-end data pipeline execution:
   1. Batch Ingestion (API → Bronze)
   2. Bronze → Silver transformation
   3. Silver → Gold aggregation
   4. Anomaly detection
   5. Delta optimization

 Usage:
   orchestrator = PipelineOrchestrator(spark, config)
   orchestrator.run_full_pipeline()
   orchestrator.run_scheduled(interval_minutes=5, max_iterations=100)
=============================================================================
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from pyspark.sql import SparkSession

from configs.app_config import AppConfig
from ingestion.batch_ingestion import BatchIngestionPipeline
from transformations.bronze_processor import BronzeProcessor
from transformations.silver_processor import SilverProcessor
from transformations.gold_processor import GoldProcessor
from ml.anomaly_detector import AnomalyDetector
from utils.delta_utils import DeltaTableManager
from utils.logger import FlightLogger, log_execution

logger = logging.getLogger("flight_analytics.orchestration")


class PipelineOrchestrator:
    """
    End-to-end pipeline orchestrator.

    Coordinates all pipeline stages in the correct order with
    error handling, metrics collection, and optional scheduling.
    """

    def __init__(self, spark: SparkSession, config: AppConfig):
        self.spark = spark
        self.config = config

        # ── Initialize all pipeline components ─────────────────────────
        self._ingestion = BatchIngestionPipeline(spark, config)
        self._bronze = BronzeProcessor(spark, config)
        self._silver = SilverProcessor(spark, config)
        self._gold = GoldProcessor(spark, config)
        self._anomaly = AnomalyDetector(spark, config)
        self._delta = DeltaTableManager(spark, config.delta)

        self._run_count = 0
        self._total_start = None

        logger.info("PipelineOrchestrator initialized")

    @log_execution(logger)
    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Execute the complete end-to-end pipeline.

        Steps:
          1. Ingest from OpenSky API → Bronze
          2. Transform Bronze → Silver
          3. Aggregate Silver → Gold
          4. Detect anomalies
          5. Return comprehensive metrics

        Returns:
            Dict with metrics from each pipeline stage
        """
        self._run_count += 1
        pipeline_id = f"pipeline_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        FlightLogger.set_correlation_id(pipeline_id)

        metrics = {
            "pipeline_id": pipeline_id,
            "run_number": self._run_count,
            "start_time": datetime.now(timezone.utc).isoformat(),
            "stages": {},
        }

        try:
            # ══════════════════════════════════════════════════════════
            #  STAGE 1: INGESTION (API → Bronze)
            # ══════════════════════════════════════════════════════════
            logger.info("═══ STAGE 1: INGESTION ═══════════════════════")
            ingestion_metrics = self._ingestion.run()
            metrics["stages"]["ingestion"] = ingestion_metrics

            if ingestion_metrics["status"] != "success":
                logger.error("Ingestion failed — aborting pipeline")
                metrics["status"] = "failed_at_ingestion"
                return metrics

            # ══════════════════════════════════════════════════════════
            #  STAGE 2: BRONZE → SILVER
            # ══════════════════════════════════════════════════════════
            logger.info("═══ STAGE 2: BRONZE → SILVER ═════════════════")
            silver_metrics = self._silver.process_bronze_to_silver()
            metrics["stages"]["silver"] = silver_metrics

            if silver_metrics.get("status") != "success":
                logger.error("Silver processing failed")
                metrics["status"] = "failed_at_silver"
                return metrics

            # ══════════════════════════════════════════════════════════
            #  STAGE 3: SILVER → GOLD
            # ══════════════════════════════════════════════════════════
            logger.info("═══ STAGE 3: SILVER → GOLD ═══════════════════")
            gold_metrics = self._gold.process_silver_to_gold()
            metrics["stages"]["gold"] = gold_metrics

            # ══════════════════════════════════════════════════════════
            #  STAGE 4: ANOMALY DETECTION
            # ══════════════════════════════════════════════════════════
            logger.info("═══ STAGE 4: ANOMALY DETECTION ═══════════════")
            try:
                silver_df = self.spark.read.format("delta").load(
                    self.config.delta.silver_path
                )
                anomalies = self._anomaly.detect_all(silver_df)
                self._anomaly.save_anomalies(anomalies)
                metrics["stages"]["anomaly_detection"] = {
                    "status": "success",
                    "anomalies_detected": anomalies.count(),
                }
            except Exception as e:
                logger.warning(
                    "Anomaly detection failed (non-critical): %s", str(e)
                )
                metrics["stages"]["anomaly_detection"] = {
                    "status": "failed",
                    "error": str(e),
                }

            # ── Pipeline complete ──────────────────────────────────────
            metrics["status"] = "success"
            metrics["end_time"] = datetime.now(timezone.utc).isoformat()

            logger.info(
                "Pipeline complete | id=%s | status=SUCCESS", pipeline_id
            )

        except Exception as e:
            logger.error(
                "Pipeline failed | id=%s | error=%s",
                pipeline_id, str(e), exc_info=True,
            )
            metrics["status"] = "failed"
            metrics["error"] = str(e)

        return metrics

    def run_scheduled(
        self,
        interval_minutes: int = 5,
        max_iterations: Optional[int] = None,
        optimize_every_n: int = 12,
    ) -> List[Dict[str, Any]]:
        """
        Run the pipeline on a schedule.

        Args:
            interval_minutes: Minutes between pipeline runs
            max_iterations: Max runs (None = infinite)
            optimize_every_n: Run Delta optimization every N iterations

        Returns:
            List of metrics from each run
        """
        self._total_start = time.time()
        all_metrics = []
        iteration = 0

        logger.info(
            "Starting scheduled pipeline | interval=%d min | "
            "optimize_every=%d",
            interval_minutes, optimize_every_n,
        )

        try:
            while max_iterations is None or iteration < max_iterations:
                iteration += 1
                logger.info(
                    "╔══ Scheduled Run %d ══════════════════════════════╗",
                    iteration,
                )

                metrics = self.run_full_pipeline()
                all_metrics.append(metrics)

                # ── Periodic Delta optimization ────────────────────────
                if iteration % optimize_every_n == 0:
                    logger.info("Running periodic Delta optimization...")
                    self._optimize_all_tables()

                # ── Wait for next run ──────────────────────────────────
                if max_iterations is None or iteration < max_iterations:
                    logger.info(
                        "Next run in %d minutes...", interval_minutes
                    )
                    time.sleep(interval_minutes * 60)

        except KeyboardInterrupt:
            logger.info("Scheduled pipeline interrupted by user")

        # ── Summary ────────────────────────────────────────────────────
        total_elapsed = time.time() - self._total_start
        successful = sum(
            1 for m in all_metrics if m.get("status") == "success"
        )
        logger.info(
            "Scheduled pipeline ended | runs=%d/%d successful | "
            "elapsed=%.0f seconds",
            successful, iteration, total_elapsed,
        )

        return all_metrics

    def _optimize_all_tables(self) -> None:
        """Run OPTIMIZE on all Delta tables."""
        paths = [
            (self.config.delta.bronze_path, ["origin_country", "time_position"]),
            (self.config.delta.silver_path, ["origin_country", "position_timestamp"]),
        ]

        for path, z_order_cols in paths:
            try:
                self._delta.optimize_table(path, z_order_columns=z_order_cols)
            except Exception as e:
                logger.warning(
                    "Optimization failed for %s: %s", path, str(e)
                )

    def close(self) -> None:
        """Release all resources."""
        self._ingestion.close()
        logger.info("PipelineOrchestrator closed")
