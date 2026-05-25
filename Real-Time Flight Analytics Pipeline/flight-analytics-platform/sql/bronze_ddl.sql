-- =============================================================================
-- Bronze Layer DDL — Flight Analytics Platform
-- =============================================================================
-- Creates the Bronze (raw) Delta table for flight state vectors.
-- This table stores data exactly as received from the OpenSky API
-- with append-only semantics for full auditability.
-- =============================================================================

-- ══════════════════════════════════════════════════════════════════════
--  DATABASE SETUP
-- ══════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS flight_analytics
COMMENT 'Real-Time Flight Analytics Platform - Medallion Architecture'
LOCATION '/mnt/flight-analytics/';

USE flight_analytics;

-- ══════════════════════════════════════════════════════════════════════
--  BRONZE TABLE: Raw flight state vectors
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bronze_flights (
    -- Core Identification
    icao24              STRING      COMMENT 'ICAO 24-bit address (hex)',
    callsign            STRING      COMMENT 'Aircraft callsign (8 chars)',
    origin_country      STRING      COMMENT 'Country of aircraft registration',

    -- Temporal
    time_position       BIGINT      COMMENT 'Unix timestamp of last position update',
    last_contact        BIGINT      COMMENT 'Unix timestamp of last message received',

    -- Geospatial
    longitude           DOUBLE      COMMENT 'WGS-84 longitude in degrees',
    latitude            DOUBLE      COMMENT 'WGS-84 latitude in degrees',
    baro_altitude       DOUBLE      COMMENT 'Barometric altitude in meters',

    -- Flight State
    on_ground           BOOLEAN     COMMENT 'True if aircraft is on ground',
    velocity            DOUBLE      COMMENT 'Ground speed in m/s',
    true_track          DOUBLE      COMMENT 'True track (heading) in degrees clockwise from North',
    vertical_rate       DOUBLE      COMMENT 'Vertical rate in m/s (positive = climbing)',

    -- Sensors
    sensors             ARRAY<INT>  COMMENT 'IDs of sensors that contributed to this state',

    -- Extended
    geo_altitude        DOUBLE      COMMENT 'Geometric altitude in meters (GPS-derived)',
    squawk              STRING      COMMENT 'Transponder squawk code',
    spi                 BOOLEAN     COMMENT 'Special Purpose Indicator',
    position_source     INT         COMMENT 'Source: 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM',

    -- Pipeline Metadata
    ingestion_timestamp TIMESTAMP   COMMENT 'Timestamp when data was ingested',
    ingestion_date      STRING      COMMENT 'Date partition (YYYY-MM-DD)',
    batch_id            STRING      COMMENT 'Unique batch identifier',
    source_system       STRING      COMMENT 'Source system identifier'
)
USING DELTA
PARTITIONED BY (ingestion_date, origin_country)
LOCATION '/mnt/flight-analytics/bronze/flights'
COMMENT 'Raw flight state vectors from OpenSky Network API'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'quality' = 'bronze'
);
