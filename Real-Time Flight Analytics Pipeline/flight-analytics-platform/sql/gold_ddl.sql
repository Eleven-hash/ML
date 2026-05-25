-- =============================================================================
-- Gold Layer DDL — Flight Analytics Platform
-- =============================================================================
-- Business analytics tables optimized for dashboard and BI consumption.
-- =============================================================================

USE flight_analytics;

-- ══════════════════════════════════════════════════════════════════════
--  GOLD: Flights by Country
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold_flights_by_country (
    origin_country      STRING      NOT NULL,
    region              STRING,
    total_flights       BIGINT      NOT NULL,
    active_flights      BIGINT,
    grounded_flights    BIGINT,
    avg_velocity_kmh    DOUBLE,
    avg_altitude_ft     DOUBLE,
    max_altitude_ft     DOUBLE,
    min_altitude_ft     DOUBLE,
    snapshot_timestamp   TIMESTAMP   NOT NULL,
    snapshot_date       STRING      NOT NULL
)
USING DELTA
LOCATION '/mnt/flight-analytics/gold/flights_by_country'
COMMENT 'Country-level flight aggregations for dashboards'
TBLPROPERTIES ('quality' = 'gold');

-- ══════════════════════════════════════════════════════════════════════
--  GOLD: Traffic Summary (Hourly)
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold_traffic_summary (
    snapshot_date       STRING      NOT NULL,
    snapshot_hour       INT         NOT NULL,
    total_flights       BIGINT      NOT NULL,
    airborne_flights    BIGINT,
    grounded_flights    BIGINT,
    unique_countries    BIGINT,
    avg_velocity_kmh    DOUBLE,
    avg_altitude_ft     DOUBLE,
    p95_velocity_kmh    DOUBLE,
    p95_altitude_ft     DOUBLE,
    snapshot_timestamp   TIMESTAMP   NOT NULL
)
USING DELTA
LOCATION '/mnt/flight-analytics/gold/traffic_summary'
COMMENT 'Hourly traffic summary for trend analysis';

-- ══════════════════════════════════════════════════════════════════════
--  GOLD: Anomalies
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold_anomalies (
    icao24              STRING      NOT NULL,
    callsign            STRING,
    origin_country      STRING,
    anomaly_type        STRING      NOT NULL,
    anomaly_score       DOUBLE,
    anomaly_description STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    altitude_ft         DOUBLE,
    velocity_kmh        DOUBLE,
    vertical_rate_fpm   DOUBLE,
    detection_timestamp TIMESTAMP   NOT NULL,
    severity            STRING      NOT NULL,
    batch_id            STRING      NOT NULL
)
USING DELTA
LOCATION '/mnt/flight-analytics/gold/anomalies'
COMMENT 'Detected flight anomalies for monitoring';

-- ══════════════════════════════════════════════════════════════════════
--  GOLD: KPI Metrics
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold_kpi_metrics (
    metric_name         STRING      NOT NULL,
    metric_value        DOUBLE      NOT NULL,
    metric_unit         STRING,
    dimension           STRING,
    dimension_value     STRING,
    calculation_timestamp TIMESTAMP NOT NULL,
    time_window         STRING
)
USING DELTA
LOCATION '/mnt/flight-analytics/gold/kpis'
COMMENT 'Executive KPI metrics for dashboard cards';
