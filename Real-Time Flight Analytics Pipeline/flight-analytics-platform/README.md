# ✈️ Real-Time Flight Analytics Platform

<div align="center">

**Enterprise-Grade Aviation Analytics using Databricks, PySpark, Delta Lake & Structured Streaming**

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.4%2B-orange.svg)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4%2B-00ADD8.svg)](https://delta.io)
[![Databricks](https://img.shields.io/badge/Databricks-Runtime%2013.3-red.svg)](https://databricks.com)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

</div>

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Pipeline Walkthrough](#pipeline-walkthrough)
- [Data Engineering Concepts](#data-engineering-concepts)
- [Performance Optimization](#performance-optimization)
- [Deployment Guide](#deployment-guide)
- [Dashboard Setup](#dashboard-setup)
- [Monitoring & Alerting](#monitoring--alerting)

---

## 🎯 Overview

This project implements a **production-grade real-time aviation analytics platform** that:

- **Ingests** live flight data from [OpenSky Network API](https://opensky-network.org/api)
- **Processes** data through a **Medallion Architecture** (Bronze → Silver → Gold)
- **Streams** data using Spark **Structured Streaming** with watermarks and windowed aggregations
- **Detects anomalies** using rule-based, statistical, and ML-based approaches
- **Generates** dashboard-ready analytics for real-time flight monitoring
- **Optimizes** with Delta Lake features: OPTIMIZE, ZORDER, VACUUM, Time Travel

### Key Metrics at a Glance

| Metric | Value |
|--------|-------|
| Data Source | OpenSky Network (Live API) |
| Refresh Rate | 15-30 seconds |
| Aircraft Tracked | 10,000+ simultaneously |
| Countries Covered | 190+ |
| Anomaly Types | 6 detection strategies |
| Analytics Queries | 10+ production SQL queries |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    OPENSKY NETWORK REST API                      │
│              (Live Aircraft State Vectors)                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
              ┌────────────▼────────────┐
              │   API INGESTION LAYER   │
              │ • Retry + Backoff       │
              │ • Rate Limiting         │
              │ • Schema Enforcement    │
              │ • Batch + Streaming     │
              └────────┬───────┬────────┘
                       │       │
           ┌───────────▼─┐   ┌─▼──────────────┐
           │    KAFKA     │   │  DIRECT WRITE   │
           │ (Optional)   │   │  to Delta       │
           └───────┬──────┘   └───────┬─────────┘
                   │                  │
              ┌────▼──────────────────▼────────────────────┐
              │           DATABRICKS LAKEHOUSE             │
              │                                            │
              │  ┌──────────────────────────────────────┐  │
              │  │         🥉 BRONZE LAYER              │  │
              │  │  Raw Delta Tables (Append-Only)      │  │
              │  │  Schema Evolution | Time Travel      │  │
              │  └──────────────┬───────────────────────┘  │
              │                 │                           │
              │  ┌──────────────▼───────────────────────┐  │
              │  │         🥈 SILVER LAYER              │  │
              │  │  Cleaned | Validated | Enriched      │  │
              │  │  Deduplicated | Unit-Converted       │  │
              │  │  Data Quality Scored                 │  │
              │  └──────────────┬───────────────────────┘  │
              │                 │                           │
              │  ┌──────────────▼───────────────────────┐  │
              │  │         🥇 GOLD LAYER                │  │
              │  │  Business Aggregations | KPIs        │  │
              │  │  Dashboard-Ready Tables              │  │
              │  └──────────────────────────────────────┘  │
              │                                            │
              └───────┬────────────┬───────────┬───────────┘
                      │            │           │
         ┌────────────▼──┐  ┌─────▼────┐  ┌───▼──────────┐
         │  📊 DASHBOARDS │  │ 🤖 ML    │  │ 🔔 MONITORING│
         │  SQL / BI      │  │ Anomaly  │  │  & Alerts    │
         │  Power BI      │  │ Detection│  │              │
         │  Tableau       │  │          │  │              │
         └────────────────┘  └──────────┘  └──────────────┘
```

---

## 🛠️ Tech Stack

| Technology | Purpose |
|-----------|---------|
| **Databricks** | Cloud compute platform & workspace |
| **Apache Spark / PySpark** | Distributed data processing |
| **Delta Lake** | ACID-compliant storage layer |
| **Structured Streaming** | Real-time stream processing |
| **Spark SQL** | Analytics queries |
| **MLlib** | Machine learning (anomaly detection) |
| **Apache Kafka** | Message streaming (optional) |
| **OpenSky Network API** | Live flight data source |
| **Power BI / Tableau** | Business intelligence dashboards |

---

## 📁 Project Structure

```
flight-analytics-platform/
│
├── configs/                    # Configuration management
│   ├── __init__.py
│   ├── app_config.py          # Central config (API, Spark, Delta, Kafka, ML)
│   ├── schemas.py             # PySpark StructType schemas (all layers)
│   └── secrets_manager.py     # Secure credential management
│
├── utils/                      # Shared utilities
│   ├── __init__.py
│   ├── logger.py              # Structured logging + correlation IDs
│   ├── spark_utils.py         # Spark session mgmt + DataFrame helpers
│   ├── api_utils.py           # HTTP client with retry/rate-limiting
│   └── delta_utils.py         # Delta operations (OPTIMIZE, VACUUM, etc.)
│
├── ingestion/                  # Data ingestion layer
│   ├── __init__.py
│   ├── opensky_client.py      # OpenSky API client
│   ├── batch_ingestion.py     # Scheduled batch ingestion
│   ├── stream_ingestion.py    # Structured Streaming ingestion
│   └── kafka_producer.py      # Kafka message publishing
│
├── transformations/            # Medallion Architecture processors
│   ├── __init__.py
│   ├── bronze_processor.py    # Bronze: raw data landing
│   ├── silver_processor.py    # Silver: cleaning + enrichment
│   ├── gold_processor.py      # Gold: business aggregations
│   └── data_quality.py        # Data quality engine
│
├── streaming/                  # Real-time streaming
│   ├── __init__.py
│   ├── stream_processor.py    # Windowed aggregations + watermarks
│   └── kafka_consumer.py      # Kafka-to-Delta consumer
│
├── analytics/                  # Analytical queries
│   ├── __init__.py
│   ├── flight_analytics.py    # Core analytics (window functions)
│   └── geo_analytics.py       # Geospatial analytics + heatmaps
│
├── ml/                         # Machine learning
│   ├── __init__.py
│   ├── feature_engineering.py # Feature extraction for ML
│   └── anomaly_detector.py    # Multi-strategy anomaly detection
│
├── sql/                        # SQL definitions
│   ├── bronze_ddl.sql         # Bronze table DDL
│   ├── silver_ddl.sql         # Silver table DDL
│   ├── gold_ddl.sql           # Gold table DDL
│   ├── analytics_queries.sql  # Production analytics queries
│   └── dashboard_views.sql    # Materialized views for BI
│
├── notebooks/                  # Databricks notebooks
│   ├── 01_setup_environment.py
│   ├── 02_batch_ingestion.py
│   ├── 03_streaming_pipeline.py
│   ├── 04_bronze_to_silver.py
│   ├── 05_silver_to_gold.py
│   ├── 06_analytics.py
│   ├── 07_anomaly_detection.py
│   ├── 08_delta_optimization.py
│   └── 09_dashboard_queries.py
│
├── orchestration/              # Pipeline orchestration
│   ├── pipeline_orchestrator.py  # End-to-end pipeline coordinator
│   └── job_scheduler.py         # Databricks job definitions
│
├── dashboards/                 # Dashboard configurations
│   └── dashboard_config.json   # BI tool layout definitions
│
├── tests/                      # Test suite
│   ├── __init__.py
│   ├── test_ingestion.py
│   ├── test_transformations.py
│   ├── test_analytics.py
│   └── test_data_quality.py
│
├── docs/                       # Documentation
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── deployment_guide.md
│   ├── troubleshooting.md
│   └── interview_preparation.md
│
├── .gitignore
├── requirements.txt
├── setup.py
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Databricks workspace (Community Edition works for learning)
- Internet access (for OpenSky API)

### Local Development Setup

```bash
# Clone the repository
git clone https://github.com/<your-username>/flight-analytics-platform.git
cd flight-analytics-platform

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest tests/ -v
```

### Databricks Setup

1. **Import to Databricks Repos**: Settings → Repos → Add Repo
2. **Run Setup Notebook**: `notebooks/01_setup_environment.py`
3. **Start Ingestion**: `notebooks/02_batch_ingestion.py`
4. **Run Pipeline**: Execute notebooks 04 → 05 → 06 → 07 in sequence

---

## 🔄 Pipeline Walkthrough

### Stage 1: Data Ingestion

```python
from configs.app_config import AppConfig
from ingestion.batch_ingestion import BatchIngestionPipeline

config = AppConfig.from_environment("development")
pipeline = BatchIngestionPipeline(spark, config)
metrics = pipeline.run()
```

### Stage 2: Bronze → Silver

```python
from transformations.silver_processor import SilverProcessor

processor = SilverProcessor(spark, config)
metrics = processor.process_bronze_to_silver()
```

### Stage 3: Silver → Gold

```python
from transformations.gold_processor import GoldProcessor

gold = GoldProcessor(spark, config)
metrics = gold.process_silver_to_gold()
```

### Stage 4: End-to-End Orchestration

```python
from orchestration.pipeline_orchestrator import PipelineOrchestrator

orchestrator = PipelineOrchestrator(spark, config)
metrics = orchestrator.run_full_pipeline()
```

---

## 📊 Data Engineering Concepts Demonstrated

| Concept | Implementation |
|---------|---------------|
| **Medallion Architecture** | Bronze → Silver → Gold layer processors |
| **Structured Streaming** | `foreachBatch`, windowed aggregations, watermarks |
| **Delta Lake ACID** | MERGE, Time Travel, Schema Evolution |
| **Data Quality** | Expectation-based DQ engine with quarantine |
| **Schema Enforcement** | StructType schemas for every layer |
| **Partitioning** | Date + country partitioning strategy |
| **Window Functions** | Ranking, LAG/LEAD, running totals |
| **Broadcast Joins** | Small-to-large table join optimization |
| **Caching** | Strategic DataFrame caching |
| **Deduplication** | Window-based dedup (latest per key) |
| **UDFs** | Haversine distance calculation |
| **ML Pipeline** | Feature engineering → Anomaly detection |

---

## ⚡ Performance Optimization

### Spark Optimizations Applied

- **Adaptive Query Execution (AQE)**: Auto-coalesce, skew join handling
- **Broadcast Joins**: For small dimension tables (< 10MB)
- **Partition Pruning**: Date-partitioned tables for time-range queries
- **ZORDER**: Co-locate data by `origin_country` and `time_position`
- **Delta Auto-Optimize**: Automatic file compaction on write
- **Caching**: Strategic caching of frequently-used DataFrames

### Cluster Recommendations

| Environment | Workers | Instance Type | Memory |
|-------------|---------|---------------|--------|
| Development | 1 | Standard_DS3_v2 | 14 GB |
| Staging | 2-4 | Standard_DS4_v2 | 28 GB |
| Production | 4-8 | Standard_DS5_v2 | 56 GB |

---

## 📈 Dashboard Setup

### Databricks SQL

1. Create a SQL Warehouse
2. Run `sql/dashboard_views.sql` to create views
3. Build dashboards using the pre-defined views

### Power BI

1. Install Databricks SQL Connector
2. Connect using server hostname + HTTP path
3. Use Gold tables as data sources

### Tableau

1. Install Databricks connector
2. Connect to `flight_analytics` database
3. Build visualizations from Gold layer views

---

## 🔍 Monitoring & Alerting

- **Structured Logging**: JSON logs with correlation IDs
- **Pipeline Metrics**: Batch counts, record counts, error rates
- **Data Quality Scores**: Per-batch quality metrics
- **Stream Monitoring**: Active query status via Spark UI
- **Alert Configuration**: Email notifications on pipeline failures

---

## 📄 License

This project is licensed under the MIT License.

---

<div align="center">

**Built with ❤️ for Data Engineering Excellence**

*Designed as an enterprise-grade portfolio project for Data Engineer / Big Data Engineer / Analytics Engineer roles*

</div>
