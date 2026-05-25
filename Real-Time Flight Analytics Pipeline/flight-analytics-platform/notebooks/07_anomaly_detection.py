# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Anomaly Detection
# MAGIC
# MAGIC **ML-based and rule-based anomaly detection for flight data**

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-username>/flight-analytics-platform")

from configs.app_config import AppConfig
from ml.anomaly_detector import AnomalyDetector
from ml.feature_engineering import FeatureEngineer
from utils.logger import FlightLogger

config = AppConfig.from_environment("development")
FlightLogger.initialize(level="INFO")

silver_df = spark.read.format("delta").load(config.delta.silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Anomaly Detection Strategies

# COMMAND ----------

detector = AnomalyDetector(spark, config)
anomalies = detector.detect_all(silver_df)

print(f"Total anomalies detected: {anomalies.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Summary

# COMMAND ----------

display(detector.get_anomaly_summary(anomalies))

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Anomaly Details

# COMMAND ----------

display(
    anomalies.select(
        "icao24", "callsign", "origin_country",
        "anomaly_type", "severity", "anomaly_score",
        "anomaly_description", "altitude_ft", "velocity_kmh",
    ).orderBy("anomaly_score", ascending=False)
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Anomalies to Gold Layer

# COMMAND ----------

detector.save_anomalies(anomalies)
print("✓ Anomalies saved to Gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ML-Based Detection (Isolation Forest Approximation)

# COMMAND ----------

ml_anomalies = detector.detect_with_isolation_forest(silver_df, contamination=0.05)
print(f"ML anomalies detected: {ml_anomalies.count()}")
display(ml_anomalies.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance Analysis

# COMMAND ----------

engineer = FeatureEngineer(spark, config)
feature_df = engineer.extract_features(silver_df)

# Show feature correlations
display(
    feature_df.select(
        "velocity_kmh", "baro_altitude_ft", "vertical_rate_fpm",
        "speed_change_kmh", "altitude_change_ft", "velocity_zscore",
    ).describe()
)
