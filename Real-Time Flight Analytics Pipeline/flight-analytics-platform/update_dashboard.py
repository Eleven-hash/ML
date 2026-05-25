"""
=============================================================================
 Dashboard Refresher — Flight Analytics Platform
=============================================================================
 Simple utility script that queries your latest local Delta Lake tables
 and regenerates/updates the dataset baked into the HTML interactive dashboard,
 allowing you to see new live data after each pipeline run!
=============================================================================
"""

import os
import sys
import json
import re
from configs.app_config import AppConfig
from utils.spark_utils import SparkSessionManager
from utils.logger import FlightLogger

# Initialize logging at WARNING level to keep console output clean
FlightLogger.initialize(level="WARNING")

def main():
    print("Reading latest local Delta Lake tables...")
    
    config = AppConfig.from_environment("development")
    local_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
    local_data_dir_spark = local_data_dir.replace("\\", "/")
    config.delta.base_path = local_data_dir_spark

    # Configure local Java, Hadoop, and PySpark variables
    os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "hadoop"))
    os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
    os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin;{os.environ['HADOOP_HOME']}/bin;" + os.environ["PATH"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Start local Spark session
    spark = SparkSessionManager.get_or_create(config)

    try:
        # ── 1. Read Gold KPIs ──────────────────────────────────────────
        kpi_path = f"{local_data_dir_spark}/gold/kpis"
        kpi_df = spark.read.format("delta").load(kpi_path)
        kpis = [row.asDict() for row in kpi_df.collect()]
        # Convert calculation_timestamp to string for JSON serialization
        for k in kpis:
            if k.get("calculation_timestamp"):
                k["calculation_timestamp"] = str(k["calculation_timestamp"])

        # ── 2. Read Gold Countries (Top 10) ─────────────────────────────
        country_path = f"{local_data_dir_spark}/gold/flights_by_country"
        country_df = spark.read.format("delta").load(country_path)
        countries = [row.asDict() for row in country_df.orderBy(country_df.total_flights.desc()).limit(10).collect()]
        for c in countries:
            if c.get("snapshot_timestamp"):
                c["snapshot_timestamp"] = str(c["snapshot_timestamp"])

        # ── 3. Read Gold Anomalies (Top 10) ─────────────────────────────
        anomaly_path = f"{local_data_dir_spark}/gold/anomalies"
        anomaly_df = spark.read.format("delta").load(anomaly_path)
        anomalies = [row.asDict() for row in anomaly_df.limit(10).collect()]
        for a in anomalies:
            if a.get("detection_timestamp"):
                a["detection_timestamp"] = str(a["detection_timestamp"])

        # ── 4. Read Silver Quarantine (Top 5) ────────────────────────────
        quarantine_path = f"{local_data_dir_spark}/quarantine/flights"
        quarantine = []
        if os.path.exists(quarantine_path):
            quarantine_df = spark.read.format("delta").load(quarantine_path)
            quarantine = [row.asDict() for row in quarantine_df.limit(5).collect()]
            for q in quarantine:
                if q.get("ingestion_timestamp"):
                    q["ingestion_timestamp"] = str(q["ingestion_timestamp"])
                # dq_flags is already string representation
                if q.get("dq_flags"):
                    q["dq_flags"] = str(q["dq_flags"])

    finally:
        # Stop Spark Session
        SparkSessionManager.stop()

    # ── 5. Update dashboards/interactive_dashboard.html ──────────────────
    dashboard_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "dashboards", "interactive_dashboard.html"))
    
    if not os.path.exists(dashboard_path):
        print(f"Error: Dashboard file not found at {dashboard_path}")
        return

    with open(dashboard_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    # Regex replacements to update the javascript variables
    html_content = re.sub(
        r"const kpiData = \[[\s\S]*?\];",
        f"const kpiData = {json.dumps(kpis, indent=12)};",
        html_content
    )
    
    html_content = re.sub(
        r"const countriesData = \[[\s\S]*?\];",
        f"const countriesData = {json.dumps(countries, indent=12)};",
        html_content
    )
    
    html_content = re.sub(
        r"const anomaliesData = \[[\s\S]*?\];",
        f"const anomaliesData = {json.dumps(anomalies, indent=12)};",
        html_content
    )
    
    html_content = re.sub(
        r"const quarantineData = \[[\s\S]*?\];",
        f"const quarantineData = {json.dumps(quarantine, indent=12)};",
        html_content
    )

    with open(dashboard_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print("\n" + "="*80)
    print(" SUCCESS: Interactive Web Dashboard updated with latest Delta data!")
    print("="*80)
    print(f"File refreshed:  {dashboard_path}")
    print("Double-click the HTML file to view your updated live data!")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
