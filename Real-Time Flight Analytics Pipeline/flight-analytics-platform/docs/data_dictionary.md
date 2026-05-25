# Data Dictionary — Flight Analytics Platform

## Bronze Layer: `bronze_flights`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `icao24` | STRING | Yes | ICAO 24-bit transponder address (6-char hex, e.g., `abc123`) |
| `callsign` | STRING | Yes | Aircraft callsign (up to 8 characters, may contain whitespace) |
| `origin_country` | STRING | Yes | Country where the aircraft is registered |
| `time_position` | BIGINT | Yes | Unix timestamp of the last position update |
| `last_contact` | BIGINT | Yes | Unix timestamp of the last message received |
| `longitude` | DOUBLE | Yes | WGS-84 longitude in decimal degrees (-180 to 180) |
| `latitude` | DOUBLE | Yes | WGS-84 latitude in decimal degrees (-90 to 90) |
| `baro_altitude` | DOUBLE | Yes | Barometric altitude in meters above mean sea level |
| `on_ground` | BOOLEAN | Yes | True if the aircraft is on the ground |
| `velocity` | DOUBLE | Yes | Ground speed in meters per second |
| `true_track` | DOUBLE | Yes | True track (heading) in degrees clockwise from North (0-360) |
| `vertical_rate` | DOUBLE | Yes | Vertical rate in m/s. Positive = climbing, negative = descending |
| `sensors` | ARRAY\<INT\> | Yes | IDs of ADS-B sensors that contributed to this state vector |
| `geo_altitude` | DOUBLE | Yes | Geometric (GPS-derived) altitude in meters |
| `squawk` | STRING | Yes | Transponder squawk code (4-digit octal, e.g., `1200`, `7700`) |
| `spi` | BOOLEAN | Yes | Special Purpose Indicator flag |
| `position_source` | INT | Yes | Source: 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM |
| `ingestion_timestamp` | TIMESTAMP | No | Pipeline ingestion time (UTC) |
| `ingestion_date` | STRING | No | Partition key: date in YYYY-MM-DD format |
| `batch_id` | STRING | No | Unique batch identifier for lineage tracking |
| `source_system` | STRING | No | Always `opensky_api` |

## Silver Layer: `silver_flights`

All Bronze columns plus:

| Column | Type | Description |
|--------|------|-------------|
| `position_timestamp` | TIMESTAMP | Converted from `time_position` Unix epoch |
| `last_contact_timestamp` | TIMESTAMP | Converted from `last_contact` Unix epoch |
| `position_date` | STRING | Date extracted from position timestamp |
| `position_hour` | INT | Hour (0-23) extracted from position timestamp |
| `baro_altitude_m` | DOUBLE | Barometric altitude in meters (original) |
| `baro_altitude_ft` | DOUBLE | Barometric altitude in feet (converted) |
| `geo_altitude_m` | DOUBLE | Geometric altitude in meters |
| `geo_altitude_ft` | DOUBLE | Geometric altitude in feet |
| `velocity_ms` | DOUBLE | Speed in m/s (original) |
| `velocity_kmh` | DOUBLE | Speed in km/h (×3.6) |
| `velocity_knots` | DOUBLE | Speed in knots (×1.94384) |
| `true_track_deg` | DOUBLE | Heading in degrees |
| `vertical_rate_ms` | DOUBLE | Vertical rate in m/s |
| `vertical_rate_fpm` | DOUBLE | Vertical rate in ft/min (×196.85) |
| `region` | STRING | Geographic region (e.g., Europe, North America) |
| `flight_phase` | STRING | `ground`, `climbing`, `cruising`, `descending`, `en_route` |
| `speed_category` | STRING | `slow` (<180), `medium` (<540), `fast` (<900), `very_fast` |
| `altitude_band` | STRING | `ground`, `low`, `medium`, `high`, `very_high` |
| `position_source` | STRING | Human-readable: `ADS-B`, `ASTERIX`, `MLAT`, `FLARM` |
| `processing_timestamp` | TIMESTAMP | Silver processing time |
| `dq_flags` | ARRAY\<STRING\> | Data quality issue flags |
| `is_valid` | BOOLEAN | Overall validity flag |

## Gold Layer Tables

### `gold_flights_by_country`
Country-level flight metrics refreshed each pipeline run.

### `gold_traffic_summary`
Hourly traffic metrics for trend analysis.

### `gold_kpi_metrics`
Executive KPI cards (total flights, avg speed, unique countries, etc.)

### `gold_anomalies`
Detected flight anomalies with type, severity, and score.

## Squawk Codes Reference

| Code | Meaning |
|------|---------|
| `1200` | VFR (normal) |
| `7500` | ⚠️ Hijacking |
| `7600` | ⚠️ Communication failure |
| `7700` | ⚠️ General emergency |
