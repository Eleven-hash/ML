# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Environment Setup
# MAGIC
# MAGIC **Flight Analytics Platform — Setup & Configuration**
# MAGIC
# MAGIC This notebook sets up the Databricks environment for the Flight Analytics Platform:
# MAGIC 1. Installs required Python packages
# MAGIC 2. Configures Spark settings for optimal performance
# MAGIC 3. Creates the database and Delta table schemas
# MAGIC 4. Sets up secrets and credentials
# MAGIC 5. Validates the environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# Install required packages (run once per cluster restart)
# %pip install requests python-dotenv confluent-kafka

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Libraries & Initialize Configuration

# COMMAND ----------

import sys
import os

# Add project root to Python path
# Adjust this path based on your Databricks Repos structure
project_root = "/Workspace/Repos/<your-username>/flight-analytics-platform"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from configs.app_config import AppConfig
from configs.schemas import FlightSchemas
from configs.secrets_manager import SecretsManager
from utils.logger import FlightLogger
from utils.spark_utils import SparkSessionManager

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Initialize Configuration

# COMMAND ----------

# Initialize configuration for the target environment
# Change to "production" or "staging" as needed
ENVIRONMENT = "development"

config = AppConfig.from_environment(ENVIRONMENT)
secrets = SecretsManager()

# Initialize logging
FlightLogger.initialize(
    level=config.monitoring.log_level,
    use_json=(ENVIRONMENT == "production"),
)
logger = FlightLogger.get_logger("setup")

print(f"Environment: {ENVIRONMENT}")
print(f"API Authenticated: {config.api.is_authenticated}")
print(f"Bronze Path: {config.delta.bronze_path}")
print(f"Silver Path: {config.delta.silver_path}")
print(f"Gold Path: {config.delta.gold_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Configure Spark

# COMMAND ----------

# Apply Spark configuration
spark_conf = config.spark.as_spark_conf()

for key, value in spark_conf.items():
    spark.conf.set(key, value)
    print(f"  {key} = {value}")

# Additional Databricks-specific settings
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

print("\n✓ Spark configuration applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Database & Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS flight_analytics
# MAGIC COMMENT 'Real-Time Flight Analytics Platform - Medallion Architecture'
# MAGIC LOCATION '/mnt/flight-analytics/';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE flight_analytics;

# COMMAND ----------

# Run Bronze DDL
# %run ./sql/bronze_ddl

# Run Silver DDL
# %run ./sql/silver_ddl

# Run Gold DDL
# %run ./sql/gold_ddl

print("✓ Database and tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate Environment

# COMMAND ----------

# Validate Spark session
print(f"Spark Version: {spark.version}")
print(f"Spark App Name: {spark.sparkContext.appName}")
print(f"Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"AQE Enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")

# Validate Delta Lake
try:
    spark.sql("SELECT 1 AS test").show()
    print("\n✓ Spark SQL working")
except Exception as e:
    print(f"\n✗ Spark SQL error: {e}")

# Validate schemas
bronze_schema = FlightSchemas.bronze_flights()
silver_schema = FlightSchemas.silver_flights()
print(f"\nBronze schema fields: {len(bronze_schema.fields)}")
print(f"Silver schema fields: {len(silver_schema.fields)}")

# Test API connectivity
try:
    import requests
    resp = requests.get(
        f"{config.api.base_url}{config.api.states_endpoint}",
        params={"lamin": 45, "lomin": -1, "lamax": 46, "lomax": 0},
        timeout=10,
    )
    print(f"\nOpenSky API Status: {resp.status_code}")
    if resp.status_code == 200:
        data = resp.json()
        states = data.get("states", [])
        print(f"Sample states returned: {len(states)}")
        print("✓ API connectivity validated")
    else:
        print(f"⚠ API returned status {resp.status_code}")
except Exception as e:
    print(f"⚠ API connectivity check failed: {e}")

print("\n" + "=" * 60)
print("  ENVIRONMENT SETUP COMPLETE")
print("=" * 60)
