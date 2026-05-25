"""
Ingestion package for Flight Analytics Platform.

Provides data ingestion from OpenSky Network API:
- OpenSkyClient: API client for fetching live flight data
- BatchIngestion: Scheduled batch data pulls
- StreamIngestion: Continuous streaming ingestion
- KafkaProducer: Optional Kafka message publishing
"""

from ingestion.opensky_client import OpenSkyClient
from ingestion.batch_ingestion import BatchIngestionPipeline
from ingestion.stream_ingestion import StreamIngestionPipeline

__all__ = [
    "OpenSkyClient",
    "BatchIngestionPipeline",
    "StreamIngestionPipeline",
]
