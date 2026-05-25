# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Batch Ingestion
# MAGIC
# MAGIC **Fetch live flight data from OpenSky Network API → Bronze Delta**
# MAGIC
# MAGIC This notebook demonstrates batch ingestion:
# MAGIC 1. Single batch fetch
# MAGIC 2. Incremental loading (multiple batches)
# MAGIC 3. Validation and metrics

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-username>/flight-analytics-platform")

from configs.app_config import AppConfig
from ingestion.batch_ingestion import BatchIngestionPipeline
from utils.logger import FlightLogger

config = AppConfig.from_environment("development")
FlightLogger.initialize(level="INFO")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single Batch Ingestion

# COMMAND ----------

pipeline = BatchIngestionPipeline(spark, config)

# Execute a single batch
metrics = pipeline.run()

print(f"Status: {metrics['status']}")
print(f"Records fetched: {metrics.get('records_fetched', 0)}")
print(f"Records written: {metrics.get('records_written', 0)}")
print(f"Batch ID: {metrics['batch_id']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Data

# COMMAND ----------

bronze_df = spark.read.format("delta").load(config.delta.bronze_path)
print(f"Bronze table row count: {bronze_df.count()}")
bronze_df.printSchema()
display(bronze_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Loading (Multiple Batches)

# COMMAND ----------

# Fetch 5 batches with 30-second intervals
# This builds up a dataset over ~2.5 minutes
all_metrics = pipeline.run_incremental(
    num_batches=5,
    interval_seconds=30,
)

# Summary
for m in all_metrics:
    print(f"Batch {m['batch_id']}: {m['status']} — {m.get('records_written', 0)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Metrics

# COMMAND ----------

pipeline_metrics = pipeline.get_pipeline_metrics()
print(f"Total batches: {pipeline_metrics['total_batches']}")
print(f"Total records ingested: {pipeline_metrics['total_records_ingested']}")
print(f"API Metrics: {pipeline_metrics['api_metrics']}")

# Cleanup
pipeline.close()
