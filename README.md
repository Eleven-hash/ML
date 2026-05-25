# 🌌 Data & Intelligence Engineering Suite

Welcome to my portfolio of enterprise-grade **Data Engineering**, **Structured Streaming**, and **Machine Learning** pipelines. This repository serves as a centralized hub for highly scalable, production-ready analytics systems.

Each project in this suite is designed with modern software engineering practices, focus on data quality, and modular architecture suitable for deployment in cloud environments like Databricks or AWS/Azure/GCP clusters.

---

## 📂 Active Projects Index

### 1. ✈️ Real-Time Flight Analytics & Anomaly Detection Lakehouse
An ultra-premium, real-time analytics platform simulating airspace monitoring by consuming global transponder state vectors, processing them through a multi-tier Delta Lakehouse, and detecting high-severity anomalies with machine learning.

* **Directory**: [`Real-Time Flight Analytics Pipeline/flight-analytics-platform`](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform)
* **Key Architecture**:
  * **Ingestion**: OpenSky API REST connector with token-bucket rate limiting and exponential backoff.
  * **Medallion Lakehouse**: 🥉 Bronze (raw landing) $\rightarrow$ 🥈 Silver (cleaned & unit-standardized with coordinates/callsign quarantining) $\rightarrow$ 🥇 Gold (materialized KPI aggregates).
  * **Advanced Analytics**: PySpark watermarked aggregations and window-ranking deduplications.
  * **Machine Learning**: Rules-based transponder squawk evaluation, $3\sigma$ vertical rate Z-scores, and Scikit-Learn **Isolation Forest** outlier isolation.
  * **Frontend UI**: Visual glassmorphic Single Page App with an HTML5 Canvas radar vector sweeper.
* **Status**: **`🟢 Operational / Complete`**

---

### 2. ⚡ [Future Project Placeholder 1: Real-Time Financial Fraud Detection]
*Description: Planned pipeline using Apache Kafka, Apache Flink, or Spark Structured Streaming to identify fraudulent transactions using Graph neural networks or gradient boosting models.*
* **Directory**: `Real-Time Financial Fraud Detection` (Future)
* **Status**: **`🟡 Planned`**

---

### 3. 🛡️ [Future Project Placeholder 2: Multi-Agent LLM Orchestrator & Log Parser]
*Description: Large-scale log parser utilizing distributed PySpark clusters to process terabytes of system logs and automatically diagnose infrastructure failures using LLM embeddings.*
* **Directory**: `AeroIntel LLM Log Parser` (Future)
* **Status**: **`🟡 Planned`**

---

## 🛠️ General Tech Stack

This suite leverages the following tools for massive scale:
* **Storage & Databases**: Delta Lake, Parquet, AWS S3 / Azure ADLS Gen2, PostgreSQL.
* **Computation Engine**: PySpark (Apache Spark 3.5+), Photon Engine, Databricks.
* **Machine Learning**: Scikit-Learn, MLflow, Isolation Forests.
* **Data Ingestion**: Python HTTP REST, Apache Kafka, Event Hubs.
* **Orchestration & DevOps**: Job Scheduler Crons, Databricks Workflows, Pytest.

---

## 📈 Portfolio Architecture Strategy

All projects inside this directory follow these core software design principles:
1. **Idempotency**: Every stage of the pipeline can be safely re-run without duplicate side effects.
2. **Schema-First**: Strict schema definitions protect downstream consumers from data format corruptions.
3. **Data Quality Quarantine**: Corrupted records are automatically separated into audit paths rather than dropped silently.
4. **Observable Logging**: Structured JSON logger with execution timing metrics.

---

## 🎓 Resume & Portfolio Summary

If you are a hiring manager or reviewer exploring this portfolio, here is a quick overview of the engineering capabilities showcased:
* **Distributed Spark Tuning**: Z-Ordering multi-dimensional sorting, Delta compaction (`OPTIMIZE`), table vacuuming, and Window ranking performance optimization.
* **Advanced Pipeline Architectures**: The Medallion schema, watermarked stream processing, rate-limiter design, and robust API retry clients.
* **Unsupervised Machine Learning**: Custom feature engineering, scaling, and high-dimensional Isolation Forest model deployment.
* **End-to-End Visual Integration**: Python-to-HTML serialization, HTML5 Canvas telemetry rendering, and responsive dark-mode styling.
