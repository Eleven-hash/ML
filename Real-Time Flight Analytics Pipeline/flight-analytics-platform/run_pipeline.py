"""
=============================================================================
 Local Execution Script — Flight Analytics Platform
=============================================================================
 Starts a local Spark session with Delta Lake, overrides DBFS cloud-specific
 storage paths to a local directory, and runs the entire end-to-end pipeline:
   1. Fetch live flights from OpenSky API
   2. Unpack raw states and write to local Bronze Delta table
   3. Run Silver transformation (cleaning, region, flight phase) + Quarantine
   4. Run Gold aggregation (KPI metrics, traffic summaries)
   5. Run Machine Learning anomaly detection (Isolation Forest)
=============================================================================
"""

import os
import sys
import logging
from configs.app_config import AppConfig
from utils.spark_utils import SparkSessionManager
from utils.logger import FlightLogger
from orchestration.pipeline_orchestrator import PipelineOrchestrator

# Initialize structured logging
FlightLogger.initialize(level="INFO")
logger = logging.getLogger("flight_analytics.local_runner")

def main():
    logger.info("Initializing Local Flight Analytics Pipeline...")

    # Load default development configuration
    config = AppConfig.from_environment("development")

    # Override paths to save Delta Lake tables locally in the workspace
    local_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
    local_data_dir_spark = local_data_dir.replace("\\", "/")
    
    config.delta.base_path = local_data_dir_spark
    config.delta.checkpoint_base = f"{local_data_dir_spark}/checkpoints"
    
    logger.info(f"Local Delta Lake storage directory set to: {local_data_dir}")

    # Configure local Java, Hadoop, and PySpark variables
    os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "hadoop"))
    os.environ["JAVA_HOME"] = "C:/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
    os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin;{os.environ['HADOOP_HOME']}/bin;" + os.environ["PATH"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Initialize PySpark Session with Delta Lake support
    spark = SparkSessionManager.get_or_create(config)

    try:
        # Initialize the Pipeline Orchestrator
        orchestrator = PipelineOrchestrator(spark, config)

        # Run the complete end-to-end medallion pipeline
        logger.info("Starting End-to-End Medallion Pipeline Execution...")
        metrics = orchestrator.run_full_pipeline()

        # Print detailed execution report
        print("\n" + "="*80)
        print(" PIPELINE RUN EXECUTION STATUS: " + metrics.get("status", "UNKNOWN").upper())
        print("="*80)
        print(f"Pipeline ID:       {metrics.get('pipeline_id')}")
        print(f"Start Time:        {metrics.get('start_time')}")
        print(f"End Time:          {metrics.get('end_time')}")
        
        stages = metrics.get("stages", {})
        print("\n--- STAGES DETAILS ---")
        
        # Ingestion Details
        ingest = stages.get("ingestion", {})
        print(f"[STAGE 1: Ingestion] Status: {ingest.get('status')} | Records Fetched: {ingest.get('records_fetched', 0)} | Written to Bronze: {ingest.get('records_written', 0)}")
        
        # Silver Details
        silver = stages.get("silver", {})
        print(f"[STAGE 2: Silver]    Status: {silver.get('status')} | Input Records: {silver.get('input_records', 0)} | Cleaned & Valid: {silver.get('valid_records', 0)} | Quarantined: {silver.get('quarantined_records', 0)}")
        
        # Gold Details
        gold = stages.get("gold", {})
        print(f"[STAGE 3: Gold]      Status: {gold.get('status')} | Metrics: {list(gold.get('tables_processed', {}).keys())}")
        
        # Anomaly Detection Details
        ml_anomaly = stages.get("anomaly_detection", {})
        print(f"[STAGE 4: ML Anomaly] Status: {ml_anomaly.get('status')} | Anomalous Flights Found: {ml_anomaly.get('anomalous_flights_found', ml_anomaly.get('anomalies_detected', 0))}")
        
        print("="*80)
        print(f"Delta tables written successfully to: {local_data_dir}")
        print("="*80 + "\n")

    finally:
        # Stop Spark Session gracefully
        SparkSessionManager.stop()
        logger.info("Spark session closed successfully.")

if __name__ == "__main__":
    main()
