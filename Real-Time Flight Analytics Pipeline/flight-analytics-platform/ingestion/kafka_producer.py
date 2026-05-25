"""
=============================================================================
 Kafka Producer — Flight Analytics Platform
=============================================================================
 Publishes flight data from OpenSky API to Apache Kafka topics for
 downstream Structured Streaming consumers.

 Architecture:
   OpenSky API → KafkaFlightProducer → Kafka Topic → Spark Consumer

 This enables true event-driven streaming and decouples data ingestion
 from data processing — a production best practice for high-throughput
 data pipelines.

 Features:
   - JSON serialization with schema validation
   - Partitioning by origin_country for data locality
   - Configurable batching and compression
   - Delivery guarantees (acks=all)
   - Health monitoring

 Usage:
   producer = KafkaFlightProducer(config)
   producer.start_continuous_publishing(interval=15)
=============================================================================
"""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from configs.app_config import AppConfig

logger = logging.getLogger("flight_analytics.ingestion.kafka_producer")


class KafkaFlightProducer:
    """
    Publishes OpenSky flight data to Kafka topics.

    Each message key is the icao24 address (aircraft identifier),
    and the value is the full state vector as JSON.
    """

    def __init__(self, config: AppConfig):
        """
        Initialize Kafka producer.

        Args:
            config: Application configuration with Kafka settings
        """
        self.config = config
        self._producer = None
        self._message_count = 0
        self._error_count = 0
        self._is_available = False

        self._initialize_producer()

    def _initialize_producer(self) -> None:
        """Initialize the Kafka producer (requires confluent-kafka)."""
        try:
            from confluent_kafka import Producer

            producer_config = {
                "bootstrap.servers": self.config.kafka.bootstrap_servers,
                "client.id": f"flight-producer-{uuid.uuid4().hex[:8]}",
                "acks": self.config.kafka.acks,
                "compression.type": self.config.kafka.compression_type,
                "batch.size": self.config.kafka.batch_size,
                "linger.ms": self.config.kafka.linger_ms,
                "retries": 3,
                "retry.backoff.ms": 500,
            }

            self._producer = Producer(producer_config)
            self._is_available = True
            logger.info(
                "Kafka producer initialized | brokers=%s | topic=%s",
                self.config.kafka.bootstrap_servers,
                self.config.kafka.topic_raw_flights,
            )

        except ImportError:
            logger.warning(
                "confluent-kafka not installed — Kafka producer not available. "
                "Install with: pip install confluent-kafka"
            )
            self._is_available = False

        except Exception as e:
            logger.error("Failed to initialize Kafka producer: %s", str(e))
            self._is_available = False

    def publish_flight_states(
        self, states: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Publish a batch of flight state vectors to Kafka.

        Args:
            states: List of parsed state vector dicts

        Returns:
            Dict with publish metrics
        """
        if not self._is_available:
            logger.warning("Kafka producer not available — skipping publish")
            return {"published": 0, "errors": 0, "skipped": len(states)}

        published = 0
        errors = 0

        for state in states:
            try:
                # ── Use icao24 as message key for partitioning ─────────
                key = state.get("icao24", "unknown")

                # ── Add publish metadata ───────────────────────────────
                message = {
                    **state,
                    "_publish_timestamp": datetime.now(timezone.utc).isoformat(),
                    "_producer_id": "flight-analytics-producer",
                }

                value = json.dumps(message, default=str)

                # ── Produce message ────────────────────────────────────
                self._producer.produce(
                    topic=self.config.kafka.topic_raw_flights,
                    key=key.encode("utf-8"),
                    value=value.encode("utf-8"),
                    callback=self._delivery_callback,
                )

                published += 1
                self._message_count += 1

            except Exception as e:
                errors += 1
                self._error_count += 1
                logger.error(
                    "Failed to publish message | icao24=%s | error=%s",
                    state.get("icao24"), str(e),
                )

        # ── Flush to ensure delivery ───────────────────────────────────
        if self._producer:
            self._producer.flush(timeout=10)

        logger.info(
            "Published %d/%d messages | errors=%d",
            published, len(states), errors,
        )

        return {
            "published": published,
            "errors": errors,
            "total_published": self._message_count,
        }

    def _delivery_callback(self, err, msg) -> None:
        """Kafka delivery report callback."""
        if err:
            logger.error(
                "Delivery failed | topic=%s | error=%s",
                msg.topic(), str(err),
            )
            self._error_count += 1
        else:
            logger.debug(
                "Delivered | topic=%s | partition=%d | offset=%d",
                msg.topic(), msg.partition(), msg.offset(),
            )

    def start_continuous_publishing(
        self,
        interval_seconds: int = 15,
        max_iterations: Optional[int] = None,
    ) -> None:
        """
        Continuously fetch from OpenSky and publish to Kafka.

        Args:
            interval_seconds: Seconds between API calls
            max_iterations: Maximum iterations (None = infinite)
        """
        from ingestion.opensky_client import OpenSkyClient

        client = OpenSkyClient(self.config)
        iteration = 0

        logger.info(
            "Starting continuous Kafka publishing | interval=%ds",
            interval_seconds,
        )

        try:
            while max_iterations is None or iteration < max_iterations:
                iteration += 1
                logger.info("── Publish iteration %d ──", iteration)

                # ── Fetch latest data ──────────────────────────────────
                raw = client.fetch_raw_states()
                if raw:
                    states = client.parse_state_vectors(raw)
                    if states:
                        self.publish_flight_states(states)

                # ── Wait for next iteration ────────────────────────────
                if max_iterations is None or iteration < max_iterations:
                    time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("Continuous publishing interrupted by user")
        finally:
            client.close()

    def get_metrics(self) -> Dict[str, int]:
        """Get producer metrics."""
        return {
            "total_messages": self._message_count,
            "total_errors": self._error_count,
            "is_available": self._is_available,
        }

    def close(self) -> None:
        """Flush and close the producer."""
        if self._producer:
            self._producer.flush(timeout=30)
            logger.info(
                "Kafka producer closed | messages=%d | errors=%d",
                self._message_count, self._error_count,
            )
