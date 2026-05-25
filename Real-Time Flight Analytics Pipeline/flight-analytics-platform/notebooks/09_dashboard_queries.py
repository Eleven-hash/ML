# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Dashboard Queries
# MAGIC
# MAGIC **Dashboard-ready SQL queries for Databricks SQL, Power BI, and Tableau**

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI Cards

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT metric_name, metric_value, metric_unit, dimension_value
# MAGIC FROM gold_kpi_metrics
# MAGIC ORDER BY metric_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Country Leaderboard

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     origin_country, region, total_flights, active_flights,
# MAGIC     avg_velocity_kmh, avg_altitude_ft,
# MAGIC     RANK() OVER (ORDER BY total_flights DESC) as rank
# MAGIC FROM gold_flights_by_country
# MAGIC ORDER BY total_flights DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Traffic Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT snapshot_hour, total_flights, airborne_flights, avg_velocity_kmh
# MAGIC FROM gold_traffic_summary
# MAGIC ORDER BY snapshot_hour

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT anomaly_type, severity, COUNT(*) as count,
# MAGIC        ROUND(AVG(anomaly_score), 2) as avg_score
# MAGIC FROM gold_anomalies
# MAGIC GROUP BY anomaly_type, severity
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flight Heatmap Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ROUND(latitude, 1) AS lat,
# MAGIC     ROUND(longitude, 1) AS lon,
# MAGIC     COUNT(*) AS intensity
# MAGIC FROM silver_flights
# MAGIC WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND is_valid = true
# MAGIC GROUP BY ROUND(latitude, 1), ROUND(longitude, 1)
# MAGIC HAVING COUNT(*) >= 3
# MAGIC ORDER BY intensity DESC
