"""
=============================================================================
 Query Results Script — Flight Analytics Platform
=============================================================================
 Simple script to start a quick Spark session and query the newly generated
 local Delta Lake tables in your workspace, printing the first few records.
=============================================================================
"""

import os
import sys
from configs.app_config import AppConfig
from utils.spark_utils import SparkSessionManager
from utils.logger import FlightLogger

# Initialize structured logging
FlightLogger.initialize(level="WARNING")

def main():
    config = AppConfig.from_environment("development")
    
    # Override paths to local data directory
    local_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
    local_data_dir_spark = local_data_dir.replace("\\", "/")
    config.delta.base_path = local_data_dir_spark
    
    # Setup HADOOP_HOME and JAVA_HOME locally
    os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "hadoop"))
    os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
    os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin;{os.environ['HADOOP_HOME']}/bin;" + os.environ["PATH"]
    
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Start Spark Session
    spark = SparkSessionManager.get_or_create(config)

    try:
        print("\n" + "="*80)
        print(" QUERYING LOCAL MEDALLION DELTA TABLES")
        print("="*80)

        # ── 1. Gold Flights By Country ──────────────────────────────────
        country_path = f"{local_data_dir_spark}/gold/flights_by_country"
        print(f"\n---> Loading table: {country_path}")
        country_df = spark.read.format("delta").load(country_path)
        print(f"Total entries: {country_df.count()}")
        print("Sample records (Top 5 busiest countries):")
        country_df.orderBy(country_df.total_flights.desc()).show(5, truncate=False)

        # ── 2. Gold KPIs ────────────────────────────────────────────────
        kpi_path = f"{local_data_dir_spark}/gold/kpis"
        print(f"\n---> Loading table: {kpi_path}")
        kpi_df = spark.read.format("delta").load(kpi_path)
        print("Computed Key Performance Indicators:")
        kpi_df.show(10, truncate=False)

        # ── 3. Gold Anomalies ───────────────────────────────────────────
        anomaly_path = f"{local_data_dir_spark}/gold/anomalies"
        print(f"\n---> Loading table: {anomaly_path}")
        anomaly_df = spark.read.format("delta").load(anomaly_path)
        print(f"Total anomalies flagged: {anomaly_df.count()}")
        print("Sample flagged anomalies (First 5):")
        anomaly_df.select(
            "icao24", "callsign", "origin_country", "anomaly_type", "severity", "anomaly_score"
        ).show(5, truncate=False)

        print("="*80)

    finally:
        # Close Spark gracefully
        SparkSessionManager.stop()

if __name__ == "__main__":
    main()
