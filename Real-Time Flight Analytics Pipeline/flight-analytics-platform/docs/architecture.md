# Architecture Guide — Flight Analytics Platform

## System Architecture Overview

The Flight Analytics Platform follows a **Lakehouse Architecture** pattern using the **Medallion Architecture** (Bronze → Silver → Gold) built on **Delta Lake** within **Databricks**.

## Data Flow

```
OpenSky Network API (REST)
         │
         ▼
┌─────────────────────┐
│  Ingestion Layer    │  Python requests + retry/backoff
│  ├─ Batch Mode      │  Scheduled fetches every 15-30s
│  └─ Stream Mode     │  Structured Streaming + foreachBatch
└────────┬────────────┘
         │  Raw JSON → PySpark DataFrame
         ▼
┌─────────────────────┐
│  🥉 BRONZE LAYER   │  Delta Table (append-only)
│  ├─ Schema enforce  │  Partitioned by (date, country)
│  ├─ Metadata cols   │  ingestion_timestamp, batch_id
│  └─ Schema evolve   │  Auto-merge new columns
└────────┬────────────┘
         │  Streaming or batch read
         ▼
┌─────────────────────┐
│  🥈 SILVER LAYER   │  Delta Table (cleaned)
│  ├─ Clean/trim      │  Remove whitespace, clamp ranges
│  ├─ Timestamps      │  Unix epoch → UTC timestamps
│  ├─ Unit convert    │  m/s→km/h, m→ft
│  ├─ Deduplicate     │  Window: latest per icao24
│  ├─ Enrich          │  Region, flight phase, speed category
│  └─ DQ score        │  Quality flags + quarantine
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  🥇 GOLD LAYER     │  Delta Tables (aggregated)
│  ├─ flights_by_country    │ Country-level KPIs
│  ├─ traffic_summary       │ Hourly trends
│  ├─ speed_analysis         │ Speed distributions
│  ├─ altitude_trends        │ Altitude patterns
│  ├─ kpi_metrics           │ Executive cards
│  └─ anomalies             │ Detected anomalies
└─────────────────────┘
```

## Design Principles

1. **Idempotency**: Every pipeline stage can be re-run without side effects
2. **Schema-first**: Explicit StructType schemas at every boundary
3. **Append-only Bronze**: Never modify raw data for auditability
4. **Quarantine pattern**: Invalid records separated, not discarded
5. **Configuration-driven**: All parameters externalized to AppConfig
6. **Observable**: Structured logging with correlation IDs at every step

## Scalability Considerations

- **Horizontal scaling**: Add Spark workers for more throughput
- **Partition pruning**: Date + country partitioning for efficient queries
- **ZORDER**: Data co-location for common filter patterns
- **Auto-scaling**: Databricks autoscale clusters based on workload
- **Streaming backpressure**: `maxFilesPerTrigger` controls ingestion rate
