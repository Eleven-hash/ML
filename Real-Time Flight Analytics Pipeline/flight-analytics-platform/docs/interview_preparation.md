# Interview Preparation — Flight Analytics Platform

## PySpark Interview Questions

### Q1: What is the difference between `select()` and `withColumn()`?
**Answer**: `select()` returns a new DataFrame with only the specified columns (can rename/transform). `withColumn()` adds or replaces a single column while keeping all existing columns. Use `select()` when you want to project specific columns; use `withColumn()` when adding derived columns.

### Q2: Explain window functions in Spark.
**Answer**: Window functions perform calculations across a set of rows related to the current row without collapsing them into a single output row (unlike `groupBy`). Key types:
- **Ranking**: `row_number()`, `rank()`, `dense_rank()`
- **Analytic**: `lag()`, `lead()`, `first()`, `last()`
- **Aggregate**: `sum()`, `avg()`, `count()` over a window

```python
from pyspark.sql.window import Window
w = Window.partitionBy("country").orderBy(F.desc("speed"))
df.withColumn("rank", F.rank().over(w))
```

### Q3: What is Adaptive Query Execution (AQE)?
**Answer**: AQE is a Spark 3.x feature that optimizes query plans at runtime based on actual data statistics. It can:
- **Auto-coalesce shuffle partitions**: Reduces small partitions after shuffle
- **Handle skewed joins**: Splits skewed partitions for balanced processing
- **Switch join strategies**: Convert sort-merge to broadcast based on actual size

### Q4: Explain the difference between `repartition()` and `coalesce()`.
**Answer**: `repartition(n)` performs a full shuffle to create exactly n partitions (can increase or decrease). `coalesce(n)` reduces partitions without a full shuffle (only decreases). Use `coalesce()` to reduce partitions (e.g., before writing) and `repartition()` when you need specific partitioning or to increase parallelism.

### Q5: How does Spark handle out-of-memory errors?
**Answer**: Spark uses a unified memory model with execution (shuffles, joins) and storage (caching) regions. When memory is exhausted:
1. Spills data to disk
2. Evicts cached data
3. Eventually throws OOM

Solutions: increase executor memory, reduce data size, optimize partitioning, use disk-based joins.

---

## Delta Lake Interview Questions

### Q6: What is Delta Lake and how does it differ from Parquet?
**Answer**: Delta Lake is an open-source storage layer on top of Parquet that adds:
- **ACID transactions**: Atomic writes, consistent reads
- **Time travel**: Query historical versions via version/timestamp
- **Schema enforcement**: Prevents bad data from corrupting tables
- **Schema evolution**: Safely add new columns
- **MERGE/UPSERT**: Update + Insert in a single operation
- **OPTIMIZE/ZORDER**: File compaction and data co-location

### Q7: Explain Time Travel in Delta Lake.
**Answer**: Time travel allows querying previous versions of a Delta table. Each write creates a new version in the transaction log (`_delta_log/`). You can:
```python
# By version
df = spark.read.format("delta").option("versionAsOf", 5).load(path)

# By timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-15").load(path)
```
Use cases: auditing, rollback, data debugging, reproducibility.

### Q8: What is ZORDER and when would you use it?
**Answer**: ZORDER co-locates related data in the same files based on specified columns. It's a multi-dimensional clustering technique. Use it for columns frequently used in WHERE clauses:
```sql
OPTIMIZE delta.`/path` ZORDER BY (origin_country, position_timestamp)
```
This makes queries filtering by `origin_country` much faster because related rows are in the same data files, enabling file skipping.

### Q9: Explain the MERGE operation in Delta Lake.
**Answer**: MERGE (upsert) matches source rows to target rows using a condition and performs different actions on matched vs unmatched rows:
```python
target.alias("t").merge(
    source.alias("s"), "t.id = s.id"
).whenMatchedUpdate(set={"value": "s.value"})
.whenNotMatchedInsertAll()
.execute()
```

---

## Structured Streaming Questions

### Q10: What is a watermark in Structured Streaming?
**Answer**: A watermark defines how long the engine waits for late data. It's a threshold on event time that tells Spark when a window is "final":
```python
df.withWatermark("event_time", "10 minutes")
  .groupBy(window("event_time", "5 minutes"))
  .count()
```
Data arriving more than 10 minutes late is dropped. This prevents unbounded state growth.

### Q11: Explain output modes in Structured Streaming.
**Answer**:
- **Append**: Only new rows added to result (for non-aggregation or with watermarks)
- **Complete**: Entire result table output each trigger (for aggregations)
- **Update**: Only rows that changed since last trigger

### Q12: What is `foreachBatch` and when do you use it?
**Answer**: `foreachBatch` gives you a regular (non-streaming) DataFrame for each micro-batch, allowing you to use any batch API:
```python
def process_batch(batch_df, epoch_id):
    batch_df.write.format("delta").mode("append").save(path)

stream.writeStream.foreachBatch(process_batch).start()
```
Use cases: calling REST APIs per batch, complex multi-table writes, custom logic that doesn't work with standard sinks.

---

## Scenario-Based Questions

### Q13: How would you handle late-arriving data in a streaming pipeline?
**Answer**: 
1. Set appropriate watermarks (`withWatermark`)
2. Use windowed aggregations with watermark-based state cleanup
3. Store late data in a separate "late arrivals" table
4. Re-process late data in batch mode if needed
5. Configure max staleness thresholds per business requirements

### Q14: Your Delta table has thousands of small files. How do you fix it?
**Answer**:
1. Run `OPTIMIZE` to compact small files into larger ones (~1GB target)
2. Enable `autoCompact` on write: `spark.databricks.delta.autoCompact.enabled`
3. Enable `optimizeWrite`: `spark.databricks.delta.optimizeWrite.enabled`
4. Run `VACUUM` to remove old versions and reduce storage

### Q15: How would you implement exactly-once processing?
**Answer**:
1. Use Structured Streaming with checkpointing (built-in exactly-once)
2. For batch: use Delta MERGE with idempotent write keys
3. Use batch IDs to detect and skip re-processed data
4. Design pipeline stages to be idempotent

### Q16: A Spark job is running slowly. How do you diagnose it?
**Answer**:
1. Check Spark UI for stage details (task distribution, skew)
2. Look for data skew (partitions with much more data than others)
3. Check for full table scans (add partition pruning)
4. Verify AQE is enabled
5. Check for excessive shuffles (reduce with broadcast joins)
6. Monitor GC pressure and memory usage
7. Check cluster utilization (not enough workers?)

### Q17: Explain the Medallion Architecture and why it's useful.
**Answer**: Medallion Architecture organizes data into three quality layers:
- **Bronze**: Raw data as-is from source (audit trail)
- **Silver**: Cleaned, validated, enriched (single source of truth)
- **Gold**: Business-level aggregations (dashboard-ready)

Benefits: separation of concerns, data quality improvement at each step, reprocessing capability, clear data lineage, team ownership by layer.

### Q18: How does your anomaly detection work?
**Answer**: Three strategies:
1. **Rule-based**: Threshold checks (altitude drops > 5000ft, speed > Mach 1.2)
2. **Statistical**: Z-score outliers (> 3 standard deviations from mean)
3. **ML-based**: Feature engineering + distributed Isolation Forest approximation using chi-squared distance from feature means

Results are unioned, deduplicated per aircraft per type, scored by severity, and saved to the Gold anomalies table.

---

## Behavioral / Design Questions

### Q19: How would you ensure data quality in a production pipeline?
**Answer**: This project implements:
1. **Schema enforcement** at ingestion (StructType validation)
2. **Expectation-based DQ engine** (null checks, range validation, regex)
3. **Quarantine pattern** (invalid records separated, not dropped)
4. **Quality scoring** (per-batch quality percentage)
5. **Monitoring** (quality trends over time in dashboards)

### Q20: How would you scale this system for 10x more data?
**Answer**:
1. Add Kafka for decoupled ingestion (horizontal scaling)
2. Increase Spark cluster size (auto-scaling)
3. Optimize partitioning strategy (more granular partitions)
4. Use Delta ZORDER for frequent query patterns
5. Implement incremental processing (only process new data)
6. Use Photon engine on Databricks for vectorized execution
7. Consider separate read/write clusters for isolation
