# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Delta Lake Optimization
# MAGIC
# MAGIC **Performance optimization for Delta tables: OPTIMIZE, ZORDER, VACUUM, time travel**

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-username>/flight-analytics-platform")

from configs.app_config import AppConfig
from utils.delta_utils import DeltaTableManager
from utils.logger import FlightLogger

config = AppConfig.from_environment("development")
FlightLogger.initialize(level="INFO")
delta = DeltaTableManager(spark, config.delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Sizes & Details

# COMMAND ----------

for name, path in [
    ("Bronze", config.delta.bronze_path),
    ("Silver", config.delta.silver_path),
]:
    try:
        detail = delta.get_table_detail(path)
        display(detail)
    except Exception as e:
        print(f"{name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE with ZORDER

# COMMAND ----------

# Optimize Bronze table — compacts small files, co-locates by country and time
delta.optimize_table(
    config.delta.bronze_path,
    z_order_columns=["origin_country", "time_position"],
)
print("✓ Bronze table optimized")

# Optimize Silver table
delta.optimize_table(
    config.delta.silver_path,
    z_order_columns=["origin_country", "position_timestamp"],
)
print("✓ Silver table optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM

# COMMAND ----------

# Remove old files (retain 7 days = 168 hours)
delta.vacuum_table(config.delta.bronze_path, retention_hours=168)
print("✓ Bronze table vacuumed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel

# COMMAND ----------

# View table history
history = delta.get_table_history(config.delta.bronze_path, limit=10)
display(history)

# COMMAND ----------

# Read a specific version (example: version 0)
# version_0_df = delta.time_travel(config.delta.bronze_path, version=0)
# print(f"Version 0 rows: {version_0_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ### Before/After OPTIMIZE — Query Performance
# MAGIC
# MAGIC Run a filtered query and check the Spark UI for:
# MAGIC - Files read (should be fewer after OPTIMIZE)
# MAGIC - Data scanned (should be less with ZORDER)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This query benefits from ZORDER on origin_country
# MAGIC SELECT origin_country, COUNT(*) as flights
# MAGIC FROM delta.`/mnt/flight-analytics/silver/flights`
# MAGIC WHERE origin_country = 'United States'
# MAGIC GROUP BY origin_country
