"""
=============================================================================
 Stream Processor — Flight Analytics Platform
=============================================================================
 Real-time stream processing using Spark Structured Streaming with
 windowed aggregations, watermarks, and stateful processing.

 Capabilities:
   - Sliding window aggregations (5-min, 15-min, 1-hour)
   - Watermark-based late data handling
   - Real-time traffic monitoring
   - Continuous flight tracking
   - Streaming anomaly detection triggers

 Usage:
   processor = StreamProcessor(spark, config)
   query = processor.start_windowed_aggregation()
=============================================================================
"""

import logging
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from configs.app_config import AppConfig
from utils.delta_utils import DeltaTableManager

logger = logging.getLogger("flight_analytics.streaming.processor")


class StreamProcessor:
    """
    Real-time streaming processor with windowed aggregations.

    Uses Structured Streaming with watermarks for processing-time
    and event-time based windowed computations.
    """

    def __init__(self, spark: SparkSession, config: AppConfig):
        self.spark = spark
        self.config = config
        self._delta = DeltaTableManager(spark, config.delta)
        self._active_queries = {}

        logger.info("StreamProcessor initialized")

    def start_windowed_aggregation(
        self,
        source_path: Optional[str] = None,
        output_path: Optional[str] = None,
        window_duration: str = "5 minutes",
        slide_duration: str = "1 minute",
        watermark_delay: str = "10 minutes",
        query_name: str = "windowed_traffic",
    ) -> Any:
        """
        Start a sliding window aggregation stream.

        Computes real-time traffic metrics over sliding windows:
          - Active flights per window
          - Average speed and altitude
          - Flights per country
          - Airborne vs. grounded ratio

        Args:
            source_path: Source Delta table (default: Silver)
            output_path: Output Delta table (default: Gold traffic)
            window_duration: Window size (e.g., '5 minutes')
            slide_duration: Slide interval (e.g., '1 minute')
            watermark_delay: Late data tolerance
            query_name: Streaming query name

        Returns:
            StreamingQuery object
        """
        source = source_path or self.config.delta.silver_path
        output = output_path or self.config.delta.gold_traffic_summary_path
        checkpoint = f"{self.config.delta.streaming_checkpoint}/{query_name}"

        logger.info(
            "Starting windowed aggregation | window=%s | slide=%s | "
            "watermark=%s",
            window_duration, slide_duration, watermark_delay,
        )

        # ── Read stream from source Delta ──────────────────────────────
        stream_df = (
            self.spark.readStream.format("delta")
            .option("maxFilesPerTrigger", 100)
            .load(source)
        )

        # ── Apply watermark on ingestion_timestamp ─────────────────────
        watermarked_df = stream_df.withWatermark(
            "ingestion_timestamp", watermark_delay
        )

        # ── Sliding window aggregation ─────────────────────────────────
        windowed_df = (
            watermarked_df.groupBy(
                F.window("ingestion_timestamp", window_duration, slide_duration),
            )
            .agg(
                F.count("*").alias("total_flights"),
                F.sum(
                    F.when(F.col("on_ground") == False, 1).otherwise(0)
                ).alias("airborne_flights"),
                F.sum(
                    F.when(F.col("on_ground") == True, 1).otherwise(0)
                ).alias("grounded_flights"),
                F.countDistinct("origin_country").alias("unique_countries"),
                F.countDistinct("icao24").alias("unique_aircraft"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_velocity_kmh"),
                F.round(F.avg("baro_altitude_ft"), 0).alias("avg_altitude_ft"),
            )
            # ── Flatten window struct ──────────────────────────────────
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window")
            .withColumn("processing_time", F.current_timestamp())
        )

        # ── Write stream to Delta ──────────────────────────────────────
        query = (
            windowed_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .trigger(
                processingTime=self.config.spark.streaming_trigger_interval
            )
            .queryName(query_name)
            .start(output)
        )

        self._active_queries[query_name] = query

        logger.info(
            "Windowed aggregation started | query=%s | id=%s",
            query_name, query.id,
        )

        return query

    def start_country_stream(
        self,
        query_name: str = "country_traffic",
    ) -> Any:
        """
        Start a stream that continuously updates country-level metrics.

        Uses foreachBatch to compute country aggregations on each
        micro-batch and merge into the Gold country table.
        """
        source = self.config.delta.silver_path
        output = self.config.delta.gold_flights_by_country_path
        checkpoint = f"{self.config.delta.streaming_checkpoint}/{query_name}"

        stream_df = (
            self.spark.readStream.format("delta")
            .option("maxFilesPerTrigger", 50)
            .load(source)
        )

        def process_country_batch(batch_df: DataFrame, epoch_id: int):
            """Compute country aggregations for each micro-batch."""
            if batch_df.rdd.isEmpty():
                return

            country_agg = (
                batch_df.groupBy("origin_country", "region")
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
                            F.col("baro_altitude_ft").isNotNull(),
                            F.col("baro_altitude_ft"),
                        )
                    ), 0).alias("min_altitude_ft"),
                )
                .withColumn("snapshot_timestamp", F.current_timestamp())
                .withColumn(
                    "snapshot_date",
                    F.date_format(F.current_timestamp(), "yyyy-MM-dd"),
                )
            )

            self._delta.write_to_delta(
                country_agg, output, mode="overwrite"
            )

            logger.info(
                "Country batch | epoch=%d | countries=%d",
                epoch_id, country_agg.count(),
            )

        query = (
            stream_df.writeStream
            .foreachBatch(process_country_batch)
            .option("checkpointLocation", checkpoint)
            .trigger(
                processingTime=self.config.spark.streaming_trigger_interval
            )
            .queryName(query_name)
            .start()
        )

        self._active_queries[query_name] = query

        logger.info("Country stream started | query=%s", query_name)
        return query

    def start_flight_tracking_stream(
        self,
        query_name: str = "flight_tracker",
    ) -> Any:
        """
        Start a stream that maintains current position of all aircraft.

        Uses foreachBatch with merge (upsert) to keep a single record
        per aircraft with its latest known position.
        """
        source = self.config.delta.silver_path
        output = f"{self.config.delta.gold_path}/live_positions"
        checkpoint = f"{self.config.delta.streaming_checkpoint}/{query_name}"

        stream_df = (
            self.spark.readStream.format("delta")
            .option("maxFilesPerTrigger", 50)
            .load(source)
        )

        def update_positions(batch_df: DataFrame, epoch_id: int):
            """Upsert latest aircraft positions."""
            if batch_df.rdd.isEmpty():
                return

            # ── Keep only latest position per aircraft ─────────────────
            window_spec = Window.partitionBy("icao24").orderBy(
                F.col("ingestion_timestamp").desc()
            )

            latest = (
                batch_df.withColumn(
                    "_rn", F.row_number().over(window_spec)
                )
                .where(F.col("_rn") == 1)
                .drop("_rn")
                .select(
                    "icao24", "callsign", "origin_country",
                    "latitude", "longitude",
                    "baro_altitude_ft", "velocity_kmh",
                    "true_track_deg", "vertical_rate_fpm",
                    "on_ground", "flight_phase",
                    "ingestion_timestamp",
                )
                .withColumn("last_updated", F.current_timestamp())
            )

            # ── Merge into live positions table ────────────────────────
            self._delta.merge_into(
                source_df=latest,
                target_path=output,
                merge_keys=["icao24"],
            )

            logger.debug(
                "Position update | epoch=%d | aircraft=%d",
                epoch_id, latest.count(),
            )

        query = (
            stream_df.writeStream
            .foreachBatch(update_positions)
            .option("checkpointLocation", checkpoint)
            .trigger(
                processingTime=self.config.spark.streaming_trigger_interval
            )
            .queryName(query_name)
            .start()
        )

        self._active_queries[query_name] = query

        logger.info("Flight tracking stream started | query=%s", query_name)
        return query

    def get_active_queries(self) -> Dict[str, Any]:
        """Get status of all active streaming queries."""
        status = {}
        for name, query in self._active_queries.items():
            status[name] = {
                "id": str(query.id),
                "is_active": query.isActive,
                "last_progress": query.lastProgress,
            }
        return status

    def stop_all(self) -> None:
        """Stop all active streaming queries."""
        for name, query in self._active_queries.items():
            if query.isActive:
                query.stop()
                logger.info("Stopped stream: %s", name)
        self._active_queries.clear()

    def await_all_termination(self, timeout: Optional[int] = None) -> None:
        """Wait for all streaming queries to terminate."""
        for query in self._active_queries.values():
            if query.isActive:
                query.awaitTermination(timeout)
