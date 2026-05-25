# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Bronze to Silver Transformation
# MAGIC
# MAGIC **Medallion Architecture: Bronze → Silver**
# MAGIC
# MAGIC Transforms raw flight data into cleaned, validated, enriched Silver tables:
# MAGIC 1. Data cleaning and standardization
# MAGIC 2. Timestamp conversion
# MAGIC 3. Unit conversion (m/s → km/h, m → ft)
# MAGIC 4. Deduplication
# MAGIC 5. Geographic enrichment
# MAGIC 6. Data quality scoring

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-username>/flight-analytics-platform")

from configs.app_config import AppConfig
from transformations.silver_processor import SilverProcessor
from transformations.data_quality import DataQualityEngine
from utils.logger import FlightLogger

config = AppConfig.from_environment("development")
FlightLogger.initialize(level="INFO")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Bronze → Silver

# COMMAND ----------

processor = SilverProcessor(spark, config)
metrics = processor.process_bronze_to_silver()

print(f"Status: {metrics['status']}")
print(f"Input records: {metrics.get('input_records', 0)}")
print(f"Valid records: {metrics.get('valid_records', 0)}")
print(f"Quarantined: {metrics.get('quarantined_records', 0)}")
print(f"Duplicates removed: {metrics.get('duplicates_removed', 0)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Silver Data

# COMMAND ----------

silver_df = processor.read_silver()
print(f"Silver table rows: {silver_df.count()}")
print(f"Columns: {len(silver_df.columns)}")
silver_df.printSchema()

# COMMAND ----------

# Show sample enriched data
display(
    silver_df.select(
        "icao24", "callsign", "origin_country", "region",
        "velocity_kmh", "baro_altitude_ft", "flight_phase",
        "speed_category", "altitude_band", "is_valid",
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Data Quality Checks

# COMMAND ----------

dq_engine = DataQualityEngine(spark)
dq_engine.add_flight_data_expectations()

report = dq_engine.validate(silver_df)

print(f"\nData Quality Score: {report['overall_score']}%")
print(f"Overall Status: {report['overall_status']}")
print(f"Passed: {report['passed_checks']}/{report['total_checks']}")

print("\nCheck Details:")
for check in report["checks"]:
    status = "✓" if check["passed"] else "✗"
    print(f"  {status} {check['description']} — {check['pass_rate']}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Data Distribution

# COMMAND ----------

# Flights by region
display(
    silver_df.groupBy("region")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# Flight phase distribution
display(
    silver_df.groupBy("flight_phase")
    .count()
    .orderBy("count", ascending=False)
)
