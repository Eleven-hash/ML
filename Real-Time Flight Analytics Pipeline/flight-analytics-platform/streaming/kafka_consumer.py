"""
=============================================================================
 Kafka Stream Consumer — Flight Analytics Platform
=============================================================================
 Spark Structured Streaming consumer that reads from Kafka topics
 and writes to Delta Lake tables.

 Architecture:
   Kafka Topic (flight-analytics.raw.flights)
        │
        ▼
   Spark Structured Streaming (Kafka Source)
        │  JSON parsing + schema enforcement
        ▼
   Bronze Delta Table (append-only)

 Usage:
   consumer = KafkaStreamConsumer(spark, config)
   query = consumer.start()
=============================================================================
"""

import logging
from typing import Optional, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from configs.app_config import AppConfig
from configs.schemas import FlightSchemas
from utils.delta_utils import DeltaTableManager

logger = logging.getLogger("flight_analytics.streaming.kafka_consumer")


class KafkaStreamConsumer:
    """
    Kafka-to-Delta streaming consumer using Spark Structured Streaming.

    Reads JSON-encoded flight data from a Kafka topic, parses it with
    schema enforcement, and writes to Bronze Delta tables.
    """

    def __init__(self, spark: SparkSession, config: AppConfig):
        self.spark = spark
        self.config = config
        self._delta = DeltaTableManager(spark, config.delta)
        self._query = None

        logger.info(
            "KafkaStreamConsumer initialized | brokers=%s | topic=%s",
            config.kafka.bootstrap_servers,
            config.kafka.topic_raw_flights,
        )

    def start(
        self,
        query_name: str = "kafka_flight_consumer",
    ) -> Any:
        """
        Start consuming from Kafka and writing to Bronze Delta.

        Args:
            query_name: Streaming query name for monitoring

        Returns:
            StreamingQuery object
        """
        checkpoint = f"{self.config.delta.streaming_checkpoint}/kafka_bronze"

        # ── Read from Kafka topic ──────────────────────────────────────
        kafka_df = (
            self.spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                self.config.kafka.bootstrap_servers,
            )
            .option("subscribe", self.config.kafka.topic_raw_flights)
            .option("startingOffsets", self.config.kafka.auto_offset_reset)
            .option(
                "maxOffsetsPerTrigger",
                self.config.kafka.max_poll_records,
            )
            .option("failOnDataLoss", "false")
            .load()
        )

        # ── Parse Kafka message values (JSON) ──────────────────────────
        parsed_df = self._parse_kafka_messages(kafka_df)

        # ── Write to Bronze Delta via foreachBatch ─────────────────────
        self._query = (
            parsed_df.writeStream
            .foreachBatch(self._write_bronze_batch)
            .option("checkpointLocation", checkpoint)
            .trigger(
                processingTime=self.config.spark.streaming_trigger_interval
            )
            .queryName(query_name)
            .start()
        )

        logger.info(
            "Kafka consumer started | query=%s | id=%s",
            query_name, self._query.id,
        )

        return self._query

    def _parse_kafka_messages(self, kafka_df: DataFrame) -> DataFrame:
        """
        Parse Kafka message values from JSON into structured columns.

        Kafka provides:
          - key: binary
          - value: binary (JSON-encoded flight data)
          - topic, partition, offset, timestamp

        Args:
            kafka_df: Raw Kafka stream DataFrame

        Returns:
            Parsed DataFrame with flight data columns
        """
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType,
            BooleanType, LongType, IntegerType, ArrayType,
        )

        # ── Define the expected JSON schema ────────────────────────────
        flight_schema = StructType([
            StructField("icao24", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("time_position", LongType(), True),
            StructField("last_contact", LongType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("baro_altitude", DoubleType(), True),
            StructField("on_ground", BooleanType(), True),
            StructField("velocity", DoubleType(), True),
            StructField("true_track", DoubleType(), True),
            StructField("vertical_rate", DoubleType(), True),
            StructField("geo_altitude", DoubleType(), True),
            StructField("squawk", StringType(), True),
            StructField("spi", BooleanType(), True),
            StructField("position_source", IntegerType(), True),
        ])

        parsed = (
            kafka_df
            # Cast binary key/value to strings
            .withColumn("key_str", F.col("key").cast(StringType()))
            .withColumn("value_str", F.col("value").cast(StringType()))
            # Parse JSON value
            .withColumn(
                "parsed",
                F.from_json(F.col("value_str"), flight_schema),
            )
            # Extract fields
            .select(
                "parsed.*",
                F.col("timestamp").alias("kafka_timestamp"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
            )
            # Add metadata
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn(
                "ingestion_date",
                F.date_format(F.current_timestamp(), "yyyy-MM-dd"),
            )
            .withColumn("source_system", F.lit("kafka"))
            .withColumn(
                "batch_id",
                F.concat(
                    F.lit("kafka_"),
                    F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                ),
            )
        )

        return parsed

    def _write_bronze_batch(
        self, batch_df: DataFrame, epoch_id: int
    ) -> None:
        """Write a micro-batch to Bronze Delta table."""
        if batch_df.rdd.isEmpty():
            return

        count = batch_df.count()

        self._delta.write_to_delta(
            df=batch_df,
            path=self.config.delta.bronze_path,
            mode="append",
            partition_by=self.config.delta.partition_columns,
            merge_schema=True,
        )

        logger.info(
            "Kafka batch written to Bronze | epoch=%d | records=%d",
            epoch_id, count,
        )

    def stop(self) -> None:
        """Stop the Kafka consumer."""
        if self._query and self._query.isActive:
            self._query.stop()
            logger.info("Kafka consumer stopped")

    def await_termination(self, timeout: Optional[int] = None) -> None:
        """Wait for consumer to terminate."""
        if self._query:
            self._query.awaitTermination(timeout)
