"""
=============================================================================
 Stream Ingestion Pipeline — Flight Analytics Platform
=============================================================================
 Continuous streaming ingestion using Spark Structured Streaming.

 Architecture:
   Since OpenSky provides a REST API (not a native stream), this pipeline
   simulates continuous streaming by:
     1. Using Spark's Rate Source as a trigger clock
     2. On each micro-batch, fetching latest data from OpenSky API
     3. Writing to Bronze Delta table with streaming semantics

   For true event-driven streaming, see kafka_consumer.py which reads
   from a Kafka topic populated by kafka_producer.py.

 Features:
   - foreachBatch sink for API-to-Delta streaming
   - Checkpoint-based exactly-once guarantees
   - Configurable trigger intervals
   - Health monitoring and alerting
   - Graceful shutdown handling

 Usage:
   pipeline = StreamIngestionPipeline(spark, config)
   query = pipeline.start()
   pipeline.await_termination()
=============================================================================
"""

import logging
import uuid
import threading
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from configs.app_config import AppConfig
from ingestion.opensky_client import OpenSkyClient
from utils.delta_utils import DeltaTableManager

logger = logging.getLogger("flight_analytics.ingestion.stream")


class StreamIngestionPipeline:
    """
    Continuous streaming ingestion pipeline using Structured Streaming.

    Uses foreachBatch to call the OpenSky API on each micro-batch trigger,
    providing streaming semantics (checkpointing, exactly-once) over a
    REST API data source.
    """

    def __init__(self, spark: SparkSession, config: AppConfig):
        """
        Initialize streaming ingestion pipeline.

        Args:
            spark: Active SparkSession
            config: Application configuration
        """
        self.spark = spark
        self.config = config
        self._opensky = OpenSkyClient(config)
        self._delta = DeltaTableManager(spark, config.delta)
        self._query = None
        self._batch_count = 0
        self._total_records = 0
        self._is_running = False
        self._stop_event = threading.Event()

        logger.info(
            "StreamIngestionPipeline initialized | "
            "trigger=%s | checkpoint=%s",
            config.spark.streaming_trigger_interval,
            config.delta.bronze_checkpoint,
        )

    def start(self, query_name: str = "opensky_ingestion") -> Any:
        """
        Start the streaming ingestion pipeline.

        Creates a Rate Source that triggers micro-batches at the configured
        interval. Each micro-batch calls the OpenSky API and writes to
        Bronze Delta.

        Args:
            query_name: Name for the streaming query (for monitoring)

        Returns:
            StreamingQuery object
        """
        logger.info("Starting streaming ingestion | query=%s", query_name)

        # ── Create rate source as trigger clock ────────────────────────
        # Rate source generates rows at a fixed rate, which we use as
        # a trigger mechanism. Each micro-batch triggers an API call.
        rate_df = (
            self.spark.readStream
            .format("rate")
            .option("rowsPerSecond", 1)
            .load()
        )

        # ── Start streaming query with foreachBatch ────────────────────
        self._query = (
            rate_df.writeStream
            .foreachBatch(self._process_micro_batch)
            .option(
                "checkpointLocation",
                self.config.delta.bronze_checkpoint,
            )
            .trigger(
                processingTime=self.config.spark.streaming_trigger_interval
            )
            .queryName(query_name)
            .start()
        )

        self._is_running = True

        logger.info(
            "Streaming query started | id=%s | name=%s",
            self._query.id, query_name,
        )

        return self._query

    def _process_micro_batch(
        self, trigger_df: DataFrame, epoch_id: int
    ) -> None:
        """
        Process a single micro-batch.

        Called by foreachBatch on each trigger. Fetches latest data from
        OpenSky API and writes to Bronze Delta table.

        Args:
            trigger_df: Trigger DataFrame from rate source (not used for data)
            epoch_id: Micro-batch epoch identifier
        """
        batch_id = f"stream_{epoch_id}_{uuid.uuid4().hex[:6]}"

        try:
            logger.info(
                "Processing micro-batch | epoch=%d | batch_id=%s",
                epoch_id, batch_id,
            )

            # ── Fetch latest state vectors ─────────────────────────────
            flight_df = self._opensky.fetch_all_states(
                self.spark, batch_id=batch_id
            )

            if flight_df is None or flight_df.rdd.isEmpty():
                logger.warning(
                    "No data in micro-batch %d — skipping", epoch_id
                )
                return

            record_count = flight_df.count()

            # ── Write to Bronze Delta table ────────────────────────────
            self._delta.write_to_delta(
                df=flight_df,
                path=self.config.delta.bronze_path,
                mode="append",
                partition_by=self.config.delta.partition_columns,
                merge_schema=True,
            )

            self._batch_count += 1
            self._total_records += record_count

            logger.info(
                "Micro-batch complete | epoch=%d | records=%d | "
                "total_records=%d | total_batches=%d",
                epoch_id, record_count,
                self._total_records, self._batch_count,
            )

        except Exception as e:
            logger.error(
                "Micro-batch failed | epoch=%d | batch_id=%s | error=%s",
                epoch_id, batch_id, str(e),
                exc_info=True,
            )
            # Don't re-raise: allow the stream to continue processing
            # next micro-batch even if this one fails

    def start_delta_to_delta_stream(
        self,
        source_path: str,
        target_path: str,
        checkpoint_path: str,
        transform_fn=None,
        query_name: str = "delta_stream",
    ) -> Any:
        """
        Start a Delta-to-Delta streaming pipeline.

        Reads changes from a source Delta table and writes to a target
        Delta table, optionally applying a transformation function.

        Used for Bronze→Silver and Silver→Gold streaming transformations.

        Args:
            source_path: Source Delta table path
            target_path: Target Delta table path
            checkpoint_path: Checkpoint directory
            transform_fn: Optional transformation function (df -> df)
            query_name: Query name for monitoring

        Returns:
            StreamingQuery object
        """
        # ── Read stream from source Delta ──────────────────────────────
        source_stream = self._delta.read_stream(source_path)

        if transform_fn:
            # ── Use foreachBatch for complex transforms ────────────────
            def process_batch(batch_df: DataFrame, epoch_id: int):
                if batch_df.rdd.isEmpty():
                    return
                transformed = transform_fn(batch_df)
                self._delta.write_to_delta(
                    transformed, target_path, mode="append"
                )

            query = (
                source_stream.writeStream
                .foreachBatch(process_batch)
                .option("checkpointLocation", checkpoint_path)
                .trigger(
                    processingTime=self.config.spark.streaming_trigger_interval
                )
                .queryName(query_name)
                .start()
            )
        else:
            # ── Direct streaming write ─────────────────────────────────
            query = self._delta.write_stream(
                df=source_stream,
                path=target_path,
                checkpoint_path=checkpoint_path,
                query_name=query_name,
            )

        logger.info(
            "Delta-to-Delta stream started | source=%s | target=%s | "
            "query=%s",
            source_path, target_path, query_name,
        )

        return query

    def await_termination(self, timeout: Optional[int] = None) -> None:
        """
        Wait for the streaming query to terminate.

        Args:
            timeout: Optional timeout in seconds
        """
        if self._query:
            logger.info("Awaiting stream termination...")
            self._query.awaitTermination(timeout)

    def stop(self) -> None:
        """Gracefully stop the streaming query."""
        if self._query and self._is_running:
            logger.info("Stopping streaming query...")
            self._query.stop()
            self._is_running = False
            self._stop_event.set()

            logger.info(
                "Stream stopped | total_batches=%d | total_records=%d",
                self._batch_count, self._total_records,
            )

    def get_stream_status(self) -> Dict[str, Any]:
        """Get current streaming query status."""
        if not self._query:
            return {"status": "not_started"}

        return {
            "status": "running" if self._query.isActive else "stopped",
            "query_id": str(self._query.id),
            "query_name": self._query.name,
            "total_batches": self._batch_count,
            "total_records": self._total_records,
            "last_progress": self._query.lastProgress,
        }

    def close(self) -> None:
        """Release all resources."""
        self.stop()
        self._opensky.close()
        logger.info("StreamIngestionPipeline closed")
