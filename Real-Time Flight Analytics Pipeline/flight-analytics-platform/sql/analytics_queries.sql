-- =============================================================================
-- Analytics Queries — Flight Analytics Platform
-- =============================================================================
-- Production-ready Spark SQL queries for flight analytics.
-- These can be run in Databricks SQL or via spark.sql().
-- =============================================================================

-- ══════════════════════════════════════════════════════════════════════
--  1. TOP 20 BUSIEST COUNTRIES BY ACTIVE FLIGHTS
-- ══════════════════════════════════════════════════════════════════════

SELECT
    origin_country,
    region,
    total_flights,
    active_flights,
    grounded_flights,
    ROUND(active_flights * 100.0 / total_flights, 1) AS airborne_pct,
    avg_velocity_kmh,
    avg_altitude_ft,
    RANK() OVER (ORDER BY total_flights DESC) AS country_rank
FROM gold_flights_by_country
ORDER BY total_flights DESC
LIMIT 20;

-- ══════════════════════════════════════════════════════════════════════
--  2. PEAK TRAFFIC HOURS ANALYSIS
-- ══════════════════════════════════════════════════════════════════════

SELECT
    snapshot_hour,
    total_flights,
    airborne_flights,
    unique_countries,
    avg_velocity_kmh,
    avg_altitude_ft,
    CASE
        WHEN total_flights >= PERCENTILE_APPROX(total_flights, 0.75)
            OVER () THEN 'Peak'
        WHEN total_flights <= PERCENTILE_APPROX(total_flights, 0.25)
            OVER () THEN 'Off-Peak'
        ELSE 'Normal'
    END AS traffic_category
FROM gold_traffic_summary
WHERE snapshot_date = CURRENT_DATE()
ORDER BY snapshot_hour;

-- ══════════════════════════════════════════════════════════════════════
--  3. FLIGHT DENSITY HEATMAP DATA
-- ══════════════════════════════════════════════════════════════════════

SELECT
    ROUND(latitude, 0) AS lat_bucket,
    ROUND(longitude, 0) AS lon_bucket,
    COUNT(*) AS flight_count,
    COUNT(DISTINCT icao24) AS unique_aircraft,
    ROUND(AVG(velocity_kmh), 1) AS avg_speed_kmh,
    ROUND(AVG(baro_altitude_ft), 0) AS avg_altitude_ft
FROM silver_flights
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND is_valid = true
GROUP BY
    ROUND(latitude, 0),
    ROUND(longitude, 0)
HAVING COUNT(*) > 5
ORDER BY flight_count DESC;

-- ══════════════════════════════════════════════════════════════════════
--  4. SPEED DISTRIBUTION ANALYSIS
-- ══════════════════════════════════════════════════════════════════════

SELECT
    speed_category,
    COUNT(*) AS flight_count,
    ROUND(AVG(velocity_kmh), 1) AS avg_speed,
    ROUND(MIN(velocity_kmh), 1) AS min_speed,
    ROUND(MAX(velocity_kmh), 1) AS max_speed,
    ROUND(STDDEV(velocity_kmh), 1) AS stddev_speed,
    ROUND(PERCENTILE_APPROX(velocity_kmh, 0.50), 1) AS median_speed,
    ROUND(PERCENTILE_APPROX(velocity_kmh, 0.90), 1) AS p90_speed,
    ROUND(PERCENTILE_APPROX(velocity_kmh, 0.95), 1) AS p95_speed
FROM silver_flights
WHERE on_ground = false
  AND velocity_kmh IS NOT NULL
  AND is_valid = true
GROUP BY speed_category
ORDER BY flight_count DESC;

-- ══════════════════════════════════════════════════════════════════════
--  5. ALTITUDE BAND ANALYSIS
-- ══════════════════════════════════════════════════════════════════════

SELECT
    altitude_band,
    flight_phase,
    COUNT(*) AS flight_count,
    ROUND(AVG(baro_altitude_ft), 0) AS avg_altitude_ft,
    ROUND(AVG(velocity_kmh), 1) AS avg_speed_kmh,
    ROUND(AVG(vertical_rate_fpm), 0) AS avg_vertical_rate_fpm
FROM silver_flights
WHERE on_ground = false
  AND baro_altitude_ft IS NOT NULL
  AND is_valid = true
GROUP BY altitude_band, flight_phase
ORDER BY avg_altitude_ft DESC;

-- ══════════════════════════════════════════════════════════════════════
--  6. REGIONAL TRAFFIC SUMMARY
-- ══════════════════════════════════════════════════════════════════════

SELECT
    region,
    COUNT(*) AS total_flights,
    COUNT(DISTINCT icao24) AS unique_aircraft,
    COUNT(DISTINCT origin_country) AS unique_countries,
    SUM(CASE WHEN on_ground = false THEN 1 ELSE 0 END) AS airborne,
    ROUND(AVG(velocity_kmh), 1) AS avg_speed_kmh,
    ROUND(AVG(baro_altitude_ft), 0) AS avg_altitude_ft,
    ROUND(
        SUM(CASE WHEN on_ground = false THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 1
    ) AS airborne_pct
FROM silver_flights
WHERE is_valid = true
GROUP BY region
ORDER BY total_flights DESC;

-- ══════════════════════════════════════════════════════════════════════
--  7. ANOMALY SUMMARY
-- ══════════════════════════════════════════════════════════════════════

SELECT
    anomaly_type,
    severity,
    COUNT(*) AS anomaly_count,
    COUNT(DISTINCT icao24) AS affected_aircraft,
    ROUND(AVG(anomaly_score), 2) AS avg_score,
    MIN(detection_timestamp) AS first_detected,
    MAX(detection_timestamp) AS last_detected
FROM gold_anomalies
GROUP BY anomaly_type, severity
ORDER BY anomaly_count DESC;

-- ══════════════════════════════════════════════════════════════════════
--  8. ACTIVE FLIGHTS WINDOW ANALYSIS (LAST 24 HOURS)
-- ══════════════════════════════════════════════════════════════════════

SELECT
    DATE_FORMAT(ingestion_timestamp, 'yyyy-MM-dd HH:00') AS hour_bucket,
    COUNT(*) AS total_flights,
    COUNT(DISTINCT icao24) AS unique_aircraft,
    ROUND(AVG(velocity_kmh), 1) AS avg_speed_kmh
FROM silver_flights
WHERE ingestion_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
  AND is_valid = true
GROUP BY DATE_FORMAT(ingestion_timestamp, 'yyyy-MM-dd HH:00')
ORDER BY hour_bucket;

-- ══════════════════════════════════════════════════════════════════════
--  9. TOP AIRCRAFT BY SPEED (WINDOW FUNCTION RANKING)
-- ══════════════════════════════════════════════════════════════════════

WITH ranked_aircraft AS (
    SELECT
        icao24,
        callsign,
        origin_country,
        velocity_kmh,
        baro_altitude_ft,
        ROW_NUMBER() OVER (
            PARTITION BY origin_country
            ORDER BY velocity_kmh DESC
        ) AS speed_rank_in_country,
        RANK() OVER (ORDER BY velocity_kmh DESC) AS global_speed_rank
    FROM silver_flights
    WHERE on_ground = false
      AND velocity_kmh IS NOT NULL
      AND is_valid = true
)
SELECT *
FROM ranked_aircraft
WHERE speed_rank_in_country <= 3
ORDER BY velocity_kmh DESC
LIMIT 50;

-- ══════════════════════════════════════════════════════════════════════
--  10. DATA QUALITY DASHBOARD QUERY
-- ══════════════════════════════════════════════════════════════════════

SELECT
    ingestion_date,
    COUNT(*) AS total_records,
    SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) AS valid_records,
    SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) AS invalid_records,
    ROUND(
        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) AS validity_pct,
    SUM(CASE WHEN callsign IS NULL THEN 1 ELSE 0 END) AS null_callsign,
    SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END) AS null_coordinates
FROM silver_flights
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;
