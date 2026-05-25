# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Streaming Pipeline
# MAGIC
# MAGIC **Continuous streaming ingestion using Structured Streaming**
# MAGIC
# MAGIC This notebook starts real-time streaming pipelines:
# MAGIC 1. API-to-Bronze streaming (foreachBatch)
# MAGIC 2. Bronze-to-Silver streaming transformation
# MAGIC 3. Real-time windowed aggregations

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-username>/flight-analytics-platform")

from configs.app_config import AppConfig
from ingestion.stream_ingestion import StreamIngestionPipeline
from streaming.stream_processor import StreamProcessor
from utils.logger import FlightLogger

config = AppConfig.from_environment("development")
FlightLogger.initialize(level="INFO")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start API → Bronze Stream

# COMMAND ----------

# Initialize streaming ingestion
stream_pipeline = StreamIngestionPipeline(spark, config)

# Start continuous ingestion from OpenSky API
query = stream_pipeline.start(query_name="opensky_to_bronze")

print(f"Stream started: {query.name}")
print(f"Query ID: {query.id}")
print(f"Status: {'Active' if query.isActive else 'Stopped'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Stream Progress

# COMMAND ----------

import time

# Monitor for 2 minutes
for i in range(4):
    status = stream_pipeline.get_stream_status()
    print(f"\n--- Status check {i+1} ---")
    print(f"  Status: {status['status']}")
    print(f"  Batches processed: {status['total_batches']}")
    print(f"  Total records: {status['total_records']}")
    if status.get('last_progress'):
        print(f"  Last progress: {status['last_progress']}")
    time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Windowed Aggregation Stream

# COMMAND ----------

processor = StreamProcessor(spark, config)

# 5-minute sliding window with 1-minute slide
windowed_query = processor.start_windowed_aggregation(
    window_duration="5 minutes",
    slide_duration="1 minute",
    watermark_delay="10 minutes",
    query_name="windowed_traffic_metrics",
)

print(f"Windowed aggregation started: {windowed_query.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Live Flight Tracker

# COMMAND ----------

# This maintains current position of all aircraft
tracker_query = processor.start_flight_tracking_stream(
    query_name="live_flight_tracker",
)

print(f"Flight tracker started: {tracker_query.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View All Active Streams

# COMMAND ----------

# List all active streaming queries
for q in spark.streams.active:
    print(f"  {q.name}: id={q.id}, active={q.isActive}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop All Streams (when done)

# COMMAND ----------

# Uncomment to stop all streams
# stream_pipeline.stop()
# processor.stop_all()
# print("All streams stopped")
