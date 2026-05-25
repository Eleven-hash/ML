-- =============================================================================
-- Silver Layer DDL — Flight Analytics Platform
-- =============================================================================
-- Creates Silver (cleaned, validated, enriched) Delta tables.
-- =============================================================================

USE flight_analytics;

-- ══════════════════════════════════════════════════════════════════════
--  SILVER TABLE: Cleaned and enriched flight data
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS silver_flights (
    -- Core Identification
    icao24                  STRING      NOT NULL COMMENT 'ICAO 24-bit address',
    callsign                STRING      COMMENT 'Cleaned/uppercased callsign',
    origin_country          STRING      NOT NULL COMMENT 'Country of registration',

    -- Temporal (converted from Unix epoch)
    position_timestamp      TIMESTAMP   COMMENT 'Position time as UTC timestamp',
    last_contact_timestamp  TIMESTAMP   COMMENT 'Last contact as UTC timestamp',
    position_date           STRING      COMMENT 'Date of position (YYYY-MM-DD)',
    position_hour           INT         COMMENT 'Hour of position (0-23)',

    -- Geospatial (validated ranges)
    longitude               DOUBLE      COMMENT 'Validated longitude [-180, 180]',
    latitude                DOUBLE      COMMENT 'Validated latitude [-90, 90]',
    baro_altitude_m         DOUBLE      COMMENT 'Barometric altitude in meters',
    baro_altitude_ft        DOUBLE      COMMENT 'Barometric altitude in feet',
    geo_altitude_m          DOUBLE      COMMENT 'Geometric altitude in meters',
    geo_altitude_ft         DOUBLE      COMMENT 'Geometric altitude in feet',

    -- Flight State (multi-unit)
    on_ground               BOOLEAN     COMMENT 'Is aircraft on ground',
    velocity_ms             DOUBLE      COMMENT 'Speed in meters/second',
    velocity_kmh            DOUBLE      COMMENT 'Speed in kilometers/hour',
    velocity_knots          DOUBLE      COMMENT 'Speed in knots',
    true_track_deg          DOUBLE      COMMENT 'Heading in degrees',
    vertical_rate_ms        DOUBLE      COMMENT 'Vertical rate in m/s',
    vertical_rate_fpm       DOUBLE      COMMENT 'Vertical rate in ft/min',

    -- Enrichment
    region                  STRING      COMMENT 'Geographic region classification',
    flight_phase            STRING      COMMENT 'Phase: ground/climbing/cruising/descending/en_route',
    speed_category          STRING      COMMENT 'Speed class: slow/medium/fast/very_fast',
    altitude_band           STRING      COMMENT 'Altitude class: ground/low/medium/high/very_high',

    -- Source Metadata
    squawk                  STRING      COMMENT 'Transponder squawk code',
    spi                     BOOLEAN     COMMENT 'Special Purpose Indicator',
    position_source         STRING      COMMENT 'Source label: ADS-B/ASTERIX/MLAT/FLARM',

    -- Pipeline Metadata
    ingestion_timestamp     TIMESTAMP   NOT NULL COMMENT 'Original ingestion time',
    processing_timestamp    TIMESTAMP   NOT NULL COMMENT 'Silver processing time',
    batch_id                STRING      NOT NULL COMMENT 'Batch identifier',
    ingestion_date          STRING      COMMENT 'Ingestion date partition',

    -- Data Quality
    dq_flags                ARRAY<STRING> COMMENT 'Data quality issue flags',
    is_valid                BOOLEAN     NOT NULL COMMENT 'Overall validity flag'
)
USING DELTA
PARTITIONED BY (ingestion_date)
LOCATION '/mnt/flight-analytics/silver/flights'
COMMENT 'Cleaned, validated, and enriched flight data'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'silver'
);

-- ══════════════════════════════════════════════════════════════════════
--  QUARANTINE TABLE: Records that failed validation
-- ══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS quarantine_flights (
    icao24              STRING,
    callsign            STRING,
    origin_country      STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    baro_altitude_ft    DOUBLE,
    velocity_kmh        DOUBLE,
    dq_flags            ARRAY<STRING>   COMMENT 'Specific quality issues',
    is_valid            BOOLEAN,
    ingestion_timestamp TIMESTAMP,
    batch_id            STRING
)
USING DELTA
LOCATION '/mnt/flight-analytics/quarantine/flights'
COMMENT 'Quarantined records that failed Silver validation'
TBLPROPERTIES ('quality' = 'quarantine');
