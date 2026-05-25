# Deployment Guide — Flight Analytics Platform

## Prerequisites

- Databricks workspace (Azure, AWS, or GCP)
- Python 3.9+
- Git access for Databricks Repos

## Step 1: Databricks Workspace Setup

### 1.1 Create a Cluster
- **Runtime**: Databricks Runtime 13.3 LTS (or latest LTS)
- **Node Type**: Standard_DS3_v2 (development) or Standard_DS4_v2 (production)
- **Workers**: 1 (dev), 2-4 (staging), 4-8 (production)
- **Auto-terminate**: 30 minutes (dev), disabled (prod)

### 1.2 Cluster Spark Configuration
```
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.databricks.delta.optimizeWrite.enabled true
spark.databricks.delta.autoCompact.enabled true
spark.databricks.delta.schema.autoMerge.enabled true
spark.sql.shuffle.partitions 200
```

## Step 2: Import Code

### Option A: Databricks Repos
1. Navigate to **Repos** in Databricks
2. Click **Add Repo**
3. Enter repository URL
4. Click **Create Repo**

### Option B: Manual Upload
1. Create folder structure in Databricks workspace
2. Upload each notebook to `/Workspace/flight-analytics-platform/notebooks/`
3. Upload Python modules to `/Workspace/flight-analytics-platform/`

## Step 3: Configure Secrets

### 3.1 Create Secrets Scope
```bash
databricks secrets create-scope --scope flight-analytics
```

### 3.2 Set API Credentials
```bash
databricks secrets put --scope flight-analytics --key opensky-username
databricks secrets put --scope flight-analytics --key opensky-password
```

## Step 4: Create Database & Tables

Run notebook `01_setup_environment.py` which will:
- Create the `flight_analytics` database
- Configure Spark settings
- Validate API connectivity

## Step 5: Run Initial Pipeline

Execute notebooks in order:
1. `02_batch_ingestion.py` — Fetch initial data
2. `04_bronze_to_silver.py` — Process to Silver
3. `05_silver_to_gold.py` — Generate Gold tables
4. `06_analytics.py` — Run analytics
5. `07_anomaly_detection.py` — Detect anomalies

## Step 6: Schedule Jobs

### Using Databricks Workflows UI
1. Go to **Workflows** → **Create Job**
2. Add tasks matching the pipeline stages
3. Set dependencies (ingestion → silver → gold)
4. Configure schedule (every 5 minutes)

### Using Job Scheduler Code
```python
from orchestration.job_scheduler import JobScheduler
from configs.app_config import AppConfig

config = AppConfig.from_environment("production")
scheduler = JobScheduler(config)

job_config = scheduler.create_pipeline_job(
    schedule_cron="0 */5 * * * ?",
    repo_path="/Repos/<username>/flight-analytics-platform",
)

scheduler.export_job_config(job_config, "pipeline_job.json")
```

## Step 7: Set Up Dashboards

1. Create a **SQL Warehouse** in Databricks
2. Run `sql/dashboard_views.sql`
3. Create dashboards using the pre-built views
4. Connect Power BI / Tableau using Databricks SQL Connector

## CI/CD Pipeline Ideas

### GitHub Actions
```yaml
name: Flight Analytics CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - run: pip install -r requirements.txt
      - run: pytest tests/ -v
```

### Databricks Asset Bundles (DABs)
Use `databricks.yml` for infrastructure-as-code deployment of jobs, clusters, and notebooks.

## Production Checklist

- [ ] Cluster auto-scaling configured
- [ ] Secrets configured (not hardcoded)
- [ ] Delta OPTIMIZE job scheduled (daily)
- [ ] Monitoring alerts configured
- [ ] Log retention policy set
- [ ] Data retention / VACUUM policy defined
- [ ] Backup strategy for Delta tables
- [ ] Access controls configured
