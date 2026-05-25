# Troubleshooting Guide — Flight Analytics Platform

## Common Issues

### 1. OpenSky API Returns Empty Data
**Symptom**: `fetch_all_states()` returns None or empty DataFrame

**Causes & Solutions**:
- **Rate limiting**: Unauthenticated users get ~10 requests/min. Wait or add credentials.
- **API downtime**: Check https://opensky-network.org/api/states/all in browser.
- **Network issues**: Verify internet connectivity from cluster.
- **Timeout**: Increase `request_timeout_seconds` in config.

### 2. Delta Table Schema Mismatch
**Symptom**: `AnalysisException: A schema mismatch detected`

**Solution**:
```python
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

### 3. Streaming Query Fails to Start
**Symptom**: `StreamingQueryException`

**Check**:
- Checkpoint directory exists and is writable
- Source Delta table exists
- No conflicting queries with same checkpoint

### 4. Out of Memory (OOM)
**Symptom**: `java.lang.OutOfMemoryError`

**Solutions**:
- Increase `spark.executor.memory`
- Reduce `spark.sql.shuffle.partitions`
- Use `coalesce()` for small datasets
- Enable AQE: `spark.sql.adaptive.enabled = true`

### 5. Slow Delta Reads
**Symptom**: Queries take too long

**Solutions**:
- Run `OPTIMIZE` on the table
- Add `ZORDER` on frequently-filtered columns
- Check partition pruning is working (avoid full scans)
- Run `VACUUM` to remove old files

### 6. Data Quality Issues
**Symptom**: High quarantine rate

**Debug**:
```python
quarantine = spark.read.format("delta").load(config.delta.quarantine_path)
quarantine.groupBy("dq_flags").count().show()
```

### 7. Kafka Connection Refused
**Symptom**: `NoBrokersAvailable`

**Solutions**:
- Verify Kafka bootstrap servers are correct
- Check network/firewall rules
- Ensure `confluent-kafka` is installed

## Performance Tuning

| Issue | Solution |
|-------|----------|
| Small files | Run OPTIMIZE regularly |
| Skewed joins | Enable AQE skew join handling |
| Slow aggregations | Pre-aggregate in Gold layer |
| Large shuffles | Tune `shuffle.partitions` |
| Memory pressure | Use disk spill: `spark.memory.fraction = 0.6` |

## Logging & Debugging

Enable debug logging:
```python
FlightLogger.initialize(level="DEBUG", use_json=False)
```

Check Spark UI for:
- Job execution plans
- Stage details and task distribution
- Storage/memory usage
- Streaming query progress
