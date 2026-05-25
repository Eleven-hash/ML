# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Flight Analytics
# MAGIC
# MAGIC **Comprehensive analytics using PySpark window functions, aggregations, and Spark SQL**

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/<your-username>/flight-analytics-platform")

from configs.app_config import AppConfig
from analytics.flight_analytics import FlightAnalytics
from analytics.geo_analytics import GeoAnalytics
from utils.logger import FlightLogger

config = AppConfig.from_environment("development")
FlightLogger.initialize(level="INFO")

silver_df = spark.read.format("delta").load(config.delta.silver_path)
analytics = FlightAnalytics(spark, config)
geo = GeoAnalytics(spark, config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Countries by Flights

# COMMAND ----------

display(analytics.top_countries_by_flights(silver_df, n=20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Peak Traffic Hours

# COMMAND ----------

display(analytics.peak_traffic_hours(silver_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Speed Distribution

# COMMAND ----------

display(analytics.speed_distribution(silver_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Altitude Distribution

# COMMAND ----------

display(analytics.altitude_distribution(silver_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regional Analysis

# COMMAND ----------

display(analytics.flights_by_region(silver_df))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flight Density Heatmap

# COMMAND ----------

display(geo.flight_density_grid(silver_df, resolution=2.0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hotspot Detection

# COMMAND ----------

display(geo.hotspot_detection(silver_df, min_flights=10, grid_resolution=1.0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark SQL Analytics

# COMMAND ----------

analytics.register_temp_views(silver_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 fastest aircraft
# MAGIC SELECT
# MAGIC     icao24, callsign, origin_country,
# MAGIC     velocity_kmh, baro_altitude_ft,
# MAGIC     RANK() OVER (ORDER BY velocity_kmh DESC) as speed_rank
# MAGIC FROM flights
# MAGIC WHERE on_ground = false AND velocity_kmh IS NOT NULL
# MAGIC ORDER BY velocity_kmh DESC
# MAGIC LIMIT 10
