# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Silver to Gold Transformation
# MAGIC
# MAGIC **Medallion Architecture: Silver → Gold**
# MAGIC
# MAGIC Produces business-ready aggregated analytics tables.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-username>/flight-analytics-platform")

from configs.app_config import AppConfig
from transformations.gold_processor import GoldProcessor
from utils.logger import FlightLogger

config = AppConfig.from_environment("development")
FlightLogger.initialize(level="INFO")

# COMMAND ----------

processor = GoldProcessor(spark, config)
metrics = processor.process_silver_to_gold()

print(f"Status: {metrics['status']}")
for table_name, table_metrics in metrics.get("tables", {}).items():
    print(f"  {table_name}: {table_metrics.get('records', 0)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Gold Tables

# COMMAND ----------

# Country rankings
display(processor.read_gold_table("flights_by_country"))

# COMMAND ----------

# Traffic summary
display(processor.read_gold_table("traffic_summary"))

# COMMAND ----------

# KPI metrics
display(processor.read_gold_table("kpi_metrics"))
