"""
Streaming package for Flight Analytics Platform.

Provides real-time streaming pipeline components:
- StreamProcessor: Windowed aggregations with watermarks
- KafkaStreamConsumer: Kafka-to-Delta streaming
"""

from streaming.stream_processor import StreamProcessor

__all__ = ["StreamProcessor"]
