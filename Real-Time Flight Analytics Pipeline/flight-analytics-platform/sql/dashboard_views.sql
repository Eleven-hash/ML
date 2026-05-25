-- =============================================================================
-- Dashboard Views — Flight Analytics Platform
-- =============================================================================
-- Pre-materialized views for BI tools (Databricks SQL, Power BI, Tableau).
-- These views are optimized for direct consumption by dashboards.
-- =============================================================================

USE flight_analytics;

-- ══════════════════════════════════════════════════════════════════════
--  VIEW: Executive KPI Dashboard
-- ══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_executive_kpis AS
SELECT
    metric_name,
    metric_value,
    metric_unit,
    dimension_value,
    calculation_timestamp
FROM gold_kpi_metrics
WHERE calculation_timestamp = (
    SELECT MAX(calculation_timestamp) FROM gold_kpi_metrics
);

-- ══════════════════════════════════════════════════════════════════════
--  VIEW: Real-Time Traffic Monitor
-- ══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_realtime_traffic AS
SELECT
    snapshot_date,
    snapshot_hour,
    total_flights,
    airborne_flights,
    grounded_flights,
    unique_countries,
    avg_velocity_kmh,
    avg_altitude_ft,
    ROUND(airborne_flights * 100.0 / NULLIF(total_flights, 0), 1) AS airborne_pct
FROM gold_traffic_summary
ORDER BY snapshot_date DESC, snapshot_hour DESC;

-- ══════════════════════════════════════════════════════════════════════
--  VIEW: Country Leaderboard
-- ══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_country_leaderboard AS
SELECT
    origin_country,
    region,
    total_flights,
    active_flights,
    grounded_flights,
    avg_velocity_kmh,
    avg_altitude_ft,
    RANK() OVER (ORDER BY total_flights DESC) AS global_rank,
    RANK() OVER (PARTITION BY region ORDER BY total_flights DESC) AS regional_rank
FROM gold_flights_by_country
WHERE snapshot_date = (
    SELECT MAX(snapshot_date) FROM gold_flights_by_country
);

-- ══════════════════════════════════════════════════════════════════════
--  VIEW: Active Anomalies
-- ══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_active_anomalies AS
SELECT
    icao24,
    callsign,
    origin_country,
    anomaly_type,
    severity,
    anomaly_score,
    anomaly_description,
    latitude,
    longitude,
    altitude_ft,
    velocity_kmh,
    detection_timestamp,
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        ELSE 4
    END AS severity_order
FROM gold_anomalies
WHERE detection_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY severity_order, anomaly_score DESC;

-- ══════════════════════════════════════════════════════════════════════
--  VIEW: Flight Density Heatmap
-- ══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_flight_heatmap AS
SELECT
    ROUND(latitude, 1) AS lat,
    ROUND(longitude, 1) AS lon,
    COUNT(*) AS intensity,
    COUNT(DISTINCT icao24) AS aircraft_count
FROM silver_flights
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND is_valid = true
  AND ingestion_date = (SELECT MAX(ingestion_date) FROM silver_flights)
GROUP BY ROUND(latitude, 1), ROUND(longitude, 1)
HAVING COUNT(*) >= 3;

-- ══════════════════════════════════════════════════════════════════════
--  VIEW: Data Quality Scorecard
-- ══════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_data_quality_scorecard AS
SELECT
    ingestion_date,
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) AS valid_records,
    ROUND(
        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
    ) AS quality_score_pct,
    SIZE(COLLECT_SET(batch_id)) AS batch_count,
    MIN(processing_timestamp) AS first_processed,
    MAX(processing_timestamp) AS last_processed
FROM silver_flights
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;
