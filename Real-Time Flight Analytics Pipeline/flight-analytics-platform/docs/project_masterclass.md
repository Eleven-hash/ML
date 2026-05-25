# AVIASTAT // Real-Time Flight Analytics & Anomaly Detection Lakehouse
## 🎓 From Vibecoding to Mastery: The Ultimate Project Guide

Welcome! If you prompted and "vibecoded" your way to this project, that's a fantastic achievement! This guide is designed to transform you into a true system architect by explaining exactly how the entire platform operates under the hood. 

Use this document to prepare for job interviews, study the codebase, or explain the engineering choices to stakeholders and colleagues.

---

## 🏗️ 1. The Architectural Blueprint (The Medallion Lakehouse)

AVIASTAT is built on the **Medallion Lakehouse Architecture**, the modern industry standard for streaming and batch data systems. Data flows through three incremental quality zones:

```
            ┌──────────────────────────────────────────────┐
            │         OpenSky Network REST API             │
            └──────────────────────┬───────────────────────┘
                                   │  JSON payload
                                   ▼
            ┌──────────────────────────────────────────────┐
            │            🥉 BRONZE LAYER (Raw)             │ 
            │  - Raw landing tables (Append-Only)          │
            │  - Schema enforcement + Auto-evolution       │
            │  - Partitioned by Ingestion Date & Country   │
            └──────────────────────┬───────────────────────┘
                                   │  Deduplicate & clean
                                   ▼
            ┌──────────────────────────────────────────────┐
            │          🥈 SILVER LAYER (Cleaned)           │
            │  - Unit conversion (m/s → km/h, meters → ft)  │
            │  - Data Quality quarantine filter            │
            │  - Enriched flight phases & regions          │
            └──────────────┬───────────────┬───────────────┘
                           │               │
            ┌──────────────▼──────┐ ┌──────▼───────────────┐
            │ 🥇 GOLD LAYER (KPIs)│ │ 🥇 GOLD (ML Anomalies)│
            │ - Busiest countries │ │ - Isolation Forests  │
            │ - Running averages  │ │ - Transponder alerts │
            │ - Aggregated sums   │ │ - Vertical drops     │
            └─────────────────────┘ └──────────────────────┘
```

### 🥉 Bronze Layer: Raw Landing Zone (Audit Trail)
* **Objective**: Ingest raw JSON vectors from the OpenSky API with zero modification.
* **Why this exists**: In production-grade systems, raw data is sacred. If your downstream cleaning code has a bug, you can patch it and replay the entire pipeline starting from the Bronze layer without re-fetching from the source API.
* **Key files**:
  * [batch_ingestion.py](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform/ingestion/batch_ingestion.py): Handles the API network layer. It uses a token bucket rate limiter to stay within OpenSky's anonymous rate limits (~10 requests/min).
  * [bronze_processor.py](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform/transformations/bronze_processor.py): Structures raw records into a Spark DataFrame using explicit schemas (`StructType`) and appends them to Delta Lake storage.

### 🥈 Silver Layer: Cleaned, Enriched, and Deduplicated
* **Objective**: Take raw Bronze data and transform it into a "single source of truth."
* **Transformations performed**:
  * **Unit Standardization**: Converts OpenSky default metric units to standard cruising values:
    * Speed: meters per second ($m/s$) $\rightarrow$ kilometers per hour ($km/h$).
    * Altitude: geometric meters ($m$) $\rightarrow$ flight levels in feet ($ft$).
  * **Feature Enrichment**: Computes derived fields such as `flight_phase` (climbing, descending, en-route, or on-ground) and mappings to geographical `regions`.
  * **Deduplication**: Ingesting raw state vectors continuously results in multiple reports for the same aircraft. We use PySpark Window functions to keep only the latest state report per `icao24` identifier.
  * **The Quarantine Pattern**: Corrupted or incomplete reports (e.g., missing longitude/latitude, missing callsign) are tagged and routed to a dedicated Quarantine table (`quarantine/flights`) instead of dropped, allowing data engineers to audit upstream collection issues.
* **Key files**:
  * [silver_processor.py](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform/transformations/silver_processor.py): Main processing engine doing filtering and column transformations.
  * [data_quality.py](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform/transformations/data_quality.py): Defines DQ rules and quality scores.

### 🥇 Gold Layer: BI-Ready Analytics & Aggregations
* **Objective**: Aggregate Silver data into highly structured, summary tables optimized for rapid querying, visualization dashboards, and executive reporting.
* **Aggregated tables computed**:
  * `gold/kpis`: General summaries (average speed, altitude, total active airborne counts).
  * `gold/flights_by_country`: Counts and metrics sliced by origin country.
  * `gold/anomalies`: Active, deduplicated high-severity anomaly alerts.
* **Key files**:
  * [gold_processor.py](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform/transformations/gold_processor.py): Computes business KPIs.

---

## 🧠 2. The Intelligence Layer (Multi-Tier Anomaly Detection)

AVIASTAT does not just look for simple threshold breaks; it combines three layers of intelligence to flag abnormal airspace threats:

### Tier 1: Rules-Based Transponder Analysis (Aviation Standards)
* Inspects the transponder `squawk` code. Certain 4-digit codes are reserved globally for emergencies:
  * `7700`: General emergency (engine failure, medical crisis, depressurization).
  * `7600`: Radio failure (complete loss of communications).
  * `7500`: Hijack alert (unlawful interference).
* If detected, these are immediately classified as `critical` severity.

### Tier 2: Heuristic & Statistical Z-Scores
* We calculate statistical averages for speeds and altitudes across all active aircraft.
* Any flight traveling or climbing at a rate greater than $3\sigma$ (3 standard deviations) from the mean is flagged as a statistical outlier (e.g., supersonic speeds or abnormal climb rates).

### Tier 3: Machine Learning (Isolation Forest)
* **The Concept**: Isolation Forest is an unsupervised clustering algorithm. Instead of modeling normal data points, it isolates anomalies. Because anomalies have unusual feature values, they require far fewer random splits in a decision tree to be isolated.
* **Execution**: 
  1. We extract flight features (`altitude_ft`, `velocity_kmh`, `vertical_rate_fpm`).
  2. The data is normalized and passed to an **Isolation Forest** model (from Scikit-Learn) that runs inside our PySpark pipeline.
  3. Rows scoring an anomaly threshold under `-0.5` are isolated and written as model-detected anomalies.
* **Key files**:
  * [anomaly_detector.py](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform/ml/anomaly_detector.py): Blends heuristic, statistical, and ML outputs into a single consolidated schema.

---

## ⚡ 3. Advanced PySpark & Delta Lake Operations

To defend this project in a system design interview, you need to understand the Spark optimizations built under the hood:

### 1. Parquet vs. Delta Lake
While Parquet is a great columnar format, it is static. **Delta Lake** is a modern storage layer built on top of Parquet files that adds a transactional metadata log (`_delta_log/`).
* **ACID Transactions**: Ensures that even if the streaming writer is writing new data, readers see a consistent view of the database without partial reads.
* **Time Travel**: Each commit creates a new transaction log file. You can query past data versions by referencing the timestamp or version number.

### 2. Window Deduplication
Deduplication is implemented using Spark's highly optimized Window operators. It partitions records by aircraft `icao24`, orders by transmission time, and assigns a rank, dropping duplicates efficiently:
```python
from pyspark.sql.window import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("icao24").orderBy(F.col("last_contact").desc())
deduplicated_df = raw_df.withColumn("row_number", F.row_number().over(window_spec)) \
                        .filter(F.col("row_number") == 1)
```

### 3. Delta Table Optimizations (`OPTIMIZE` & `Z-ORDER`)
Continuous streaming results in the "small files problem," where thousands of tiny Parquet files clutter the storage.
* `OPTIMIZE`: Compacts small files into highly performant 1GB chunks.
* `Z-ORDER BY (country, date)`: Physically groups and sorts records of similar countries and dates inside the same files. When users query a specific country, Spark skips reading unrelated files entirely, saving disk I/O and computing power.
* **Key files**:
  * [delta_utils.py](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform/utils/delta_utils.py): Optimized Delta maintenance helpers.

---

## 🖥️ 4. Visualizing Outcomes: The Refresher Bridge

The interactive interface is written in highly modular CSS/HTML with a Canvas engine:
* **The HTML**: [interactive_dashboard.html](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform/dashboards/interactive_dashboard.html)
* **The Refresher Script**: [update_dashboard.py](file:///e:/project/ML/Real-Time%20Flight%20Analytics%20Pipeline/flight-analytics-platform/update_dashboard.py)
* **The Integration Bridge**:
  1. The Python script starts a local PySpark session.
  2. It reads `gold/kpis`, `gold/flights_by_country`, `gold/anomalies`, and `quarantine/flights` tables from the filesystem.
  3. It formats the rows into clean Python dictionaries.
  4. It uses a **custom datetime serialization handler** (`json_serial`) to map PySpark timestamp columns (which standard `json.dumps` fails to process) to standard string representations.
  5. It replaces the mock arrays inside the HTML file (`const kpiData = [...]`, etc.) with the real live database outputs using regex replacements.

---

## 🗣️ 5. Resume & Interview Cheat Sheet

### How to describe this on your Resume
> **Real-Time Flight Analytics & Anomaly Detection Lakehouse (AVIASTAT)**
> * Designed and built a modular, production-grade Medallion architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) using **PySpark** and **Delta Lake** transaction logs, processing over 13,000+ flight vectors.
> * Implemented a custom Data Quality expectation checks engine using the **Quarantine Pattern**, sorting malformed data to standalone audit directories without disrupting the core streaming pipelines.
> * Built a multi-tier threat detection system integrating rule-based aviation squawk metrics, statistical $3\sigma$ Z-scores, and an unsupervised **Scikit-Learn Isolation Forest** ML pipeline.
> * Engineered database file-compaction operations, utilizing Delta **OPTIMIZE** and multi-dimensional **Z-Ordering** by country/date to minimize disk I/O and speed up downstream queries.
> * Created a glassmorphic front-end dashboard powered by HTML5 Canvas radar sweeps, synchronizing Spark database tables directly into the dashboard using Python regex refreshers.

### The 30-Second Elevator Pitch
> *"I designed and built AVIASTAT, a production-grade Real-Time Flight Analytics and Anomaly Detection platform. It implements a complete Medallion Lakehouse architecture using PySpark and Delta Lake to process global aviation vectors fetched from the OpenSky Network API. The system handles raw data landing in Bronze, executes standardizations and data-quality quarantining in Silver, and materializes summary metrics in Gold. It runs a composite anomaly detection engine using rule-based transponder squawks, statistical Z-scores, and a machine learning Isolation Forest. Finally, it exports data seamlessly to a custom-built canvas radar dashboard."*

---

### 🎓 Take it to the Next Level!
If you want to practice defending this project in front of an interviewer:
1. Open your chat terminal.
2. Type **`/grill-me`** or ask your AI coding partner to grill you.
3. This triggers an interactive technical mock interview focusing on the choices you made in this codebase!
