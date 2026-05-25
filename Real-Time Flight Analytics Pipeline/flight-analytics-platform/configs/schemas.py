"""
=============================================================================
 Schema Definitions — Flight Analytics Platform
=============================================================================
 Centralized PySpark StructType schemas for every layer of the Medallion
 Architecture. Strict schema enforcement is critical for data quality in
 production streaming pipelines.

 OpenSky Network API returns state vectors with these fields:
   icao24, callsign, origin_country, time_position, last_contact,
   longitude, latitude, baro_altitude, on_ground, velocity,
   true_track, vertical_rate, sensors, geo_altitude, squawk,
   spi, position_source

 Reference: https://openskynetwork.github.io/opensky-api/rest.html
=============================================================================
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class FlightSchemas:
    """
    Centralized schema registry for all data layers.

    Each schema corresponds to a specific stage in the data pipeline,
    ensuring type safety and enabling schema evolution tracking.
    """

    # ══════════════════════════════════════════════════════════════════
    #  RAW API RESPONSE SCHEMA
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def raw_api_response() -> StructType:
        """
        Schema for the raw JSON response from OpenSky Network API.
        The API returns a 'states' array where each element is a
        positional array (not a named object).
        """
        return StructType(
            [
                StructField("time", LongType(), True),
                StructField(
                    "states",
                    ArrayType(
                        ArrayType(StringType(), True),  # Each state is a mixed array
                        True,
                    ),
                    True,
                ),
            ]
        )

    # ══════════════════════════════════════════════════════════════════
    #  BRONZE LAYER — Raw State Vectors
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def bronze_flights() -> StructType:
        """
        Bronze layer schema — raw state vectors with metadata columns.
        Maps directly from the OpenSky positional array to named fields.
        """
        return StructType(
            [
                # ── Core Identification ────────────────────────────────
                StructField("icao24", StringType(), True),
                StructField("callsign", StringType(), True),
                StructField("origin_country", StringType(), True),
                # ── Temporal ───────────────────────────────────────────
                StructField("time_position", LongType(), True),
                StructField("last_contact", LongType(), True),
                # ── Geospatial ─────────────────────────────────────────
                StructField("longitude", DoubleType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("baro_altitude", DoubleType(), True),
                # ── Flight State ───────────────────────────────────────
                StructField("on_ground", BooleanType(), True),
                StructField("velocity", DoubleType(), True),
                StructField("true_track", DoubleType(), True),
                StructField("vertical_rate", DoubleType(), True),
                # ── Sensors ────────────────────────────────────────────
                StructField("sensors", ArrayType(IntegerType()), True),
                # ── Extended ───────────────────────────────────────────
                StructField("geo_altitude", DoubleType(), True),
                StructField("squawk", StringType(), True),
                StructField("spi", BooleanType(), True),
                StructField("position_source", IntegerType(), True),
                # ── Pipeline Metadata ──────────────────────────────────
                StructField("ingestion_timestamp", TimestampType(), False),
                StructField("ingestion_date", StringType(), False),
                StructField("batch_id", StringType(), False),
                StructField("source_system", StringType(), False),
            ]
        )

    # ══════════════════════════════════════════════════════════════════
    #  SILVER LAYER — Cleaned & Validated
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def silver_flights() -> StructType:
        """
        Silver layer schema — cleaned, validated, and enriched flight data.
        All timestamps converted, units standardized, nulls handled.
        """
        return StructType(
            [
                # ── Core Identification ────────────────────────────────
                StructField("icao24", StringType(), False),
                StructField("callsign", StringType(), True),
                StructField("origin_country", StringType(), False),
                # ── Temporal (proper timestamps) ───────────────────────
                StructField("position_timestamp", TimestampType(), True),
                StructField("last_contact_timestamp", TimestampType(), True),
                StructField("position_date", StringType(), True),
                StructField("position_hour", IntegerType(), True),
                # ── Geospatial ─────────────────────────────────────────
                StructField("longitude", DoubleType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("baro_altitude_m", DoubleType(), True),
                StructField("baro_altitude_ft", DoubleType(), True),
                StructField("geo_altitude_m", DoubleType(), True),
                StructField("geo_altitude_ft", DoubleType(), True),
                # ── Flight State ───────────────────────────────────────
                StructField("on_ground", BooleanType(), True),
                StructField("velocity_ms", DoubleType(), True),
                StructField("velocity_kmh", DoubleType(), True),
                StructField("velocity_knots", DoubleType(), True),
                StructField("true_track_deg", DoubleType(), True),
                StructField("vertical_rate_ms", DoubleType(), True),
                StructField("vertical_rate_fpm", DoubleType(), True),
                # ── Enrichment ─────────────────────────────────────────
                StructField("region", StringType(), True),
                StructField("flight_phase", StringType(), True),
                StructField("speed_category", StringType(), True),
                StructField("altitude_band", StringType(), True),
                # ── Metadata ───────────────────────────────────────────
                StructField("squawk", StringType(), True),
                StructField("spi", BooleanType(), True),
                StructField("position_source", StringType(), True),
                # ── Pipeline Metadata ──────────────────────────────────
                StructField("ingestion_timestamp", TimestampType(), False),
                StructField("processing_timestamp", TimestampType(), False),
                StructField("batch_id", StringType(), False),
                # ── Data Quality ───────────────────────────────────────
                StructField("dq_flags", ArrayType(StringType()), True),
                StructField("is_valid", BooleanType(), False),
            ]
        )

    # ══════════════════════════════════════════════════════════════════
    #  GOLD LAYER — Aggregated Business Tables
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def gold_flights_by_country() -> StructType:
        """Aggregated flight counts and metrics per country."""
        return StructType(
            [
                StructField("origin_country", StringType(), False),
                StructField("region", StringType(), True),
                StructField("total_flights", LongType(), False),
                StructField("active_flights", LongType(), False),
                StructField("grounded_flights", LongType(), False),
                StructField("avg_velocity_kmh", DoubleType(), True),
                StructField("avg_altitude_ft", DoubleType(), True),
                StructField("max_altitude_ft", DoubleType(), True),
                StructField("min_altitude_ft", DoubleType(), True),
                StructField("snapshot_timestamp", TimestampType(), False),
                StructField("snapshot_date", StringType(), False),
            ]
        )

    @staticmethod
    def gold_traffic_summary() -> StructType:
        """Hourly/daily traffic summary for trend analysis."""
        return StructType(
            [
                StructField("snapshot_date", StringType(), False),
                StructField("snapshot_hour", IntegerType(), False),
                StructField("total_flights", LongType(), False),
                StructField("airborne_flights", LongType(), False),
                StructField("grounded_flights", LongType(), False),
                StructField("unique_countries", LongType(), False),
                StructField("avg_velocity_kmh", DoubleType(), True),
                StructField("avg_altitude_ft", DoubleType(), True),
                StructField("p95_velocity_kmh", DoubleType(), True),
                StructField("p95_altitude_ft", DoubleType(), True),
                StructField("snapshot_timestamp", TimestampType(), False),
            ]
        )

    @staticmethod
    def gold_anomalies() -> StructType:
        """Detected flight anomalies for monitoring."""
        return StructType(
            [
                StructField("icao24", StringType(), False),
                StructField("callsign", StringType(), True),
                StructField("origin_country", StringType(), True),
                StructField("anomaly_type", StringType(), False),
                StructField("anomaly_score", DoubleType(), True),
                StructField("anomaly_description", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("altitude_ft", DoubleType(), True),
                StructField("velocity_kmh", DoubleType(), True),
                StructField("vertical_rate_fpm", DoubleType(), True),
                StructField("detection_timestamp", TimestampType(), False),
                StructField("severity", StringType(), False),
                StructField("batch_id", StringType(), False),
            ]
        )

    @staticmethod
    def gold_kpi_metrics() -> StructType:
        """Key Performance Indicators for executive dashboards."""
        return StructType(
            [
                StructField("metric_name", StringType(), False),
                StructField("metric_value", DoubleType(), False),
                StructField("metric_unit", StringType(), True),
                StructField("dimension", StringType(), True),
                StructField("dimension_value", StringType(), True),
                StructField("calculation_timestamp", TimestampType(), False),
                StructField("time_window", StringType(), True),
            ]
        )

    # ══════════════════════════════════════════════════════════════════
    #  STREAMING SCHEMAS
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def kafka_message() -> StructType:
        """Schema for Kafka message payload."""
        return StructType(
            [
                StructField("key", StringType(), True),
                StructField("value", StringType(), False),
                StructField("topic", StringType(), True),
                StructField("partition", IntegerType(), True),
                StructField("offset", LongType(), True),
                StructField("timestamp", TimestampType(), True),
            ]
        )

    @staticmethod
    def streaming_state_vector() -> StructType:
        """Schema for real-time streaming state vectors."""
        return StructType(
            [
                StructField("icao24", StringType(), False),
                StructField("callsign", StringType(), True),
                StructField("origin_country", StringType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("baro_altitude", DoubleType(), True),
                StructField("on_ground", BooleanType(), True),
                StructField("velocity", DoubleType(), True),
                StructField("true_track", DoubleType(), True),
                StructField("vertical_rate", DoubleType(), True),
                StructField("geo_altitude", DoubleType(), True),
                StructField("event_time", TimestampType(), False),
            ]
        )
