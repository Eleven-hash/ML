"""
=============================================================================
 OpenSky Network API Client — Flight Analytics Platform
=============================================================================
 Specialized client for the OpenSky Network REST API that fetches
 real-time flight state vectors (position, velocity, altitude, etc.)
 for all aircraft worldwide.

 API Reference: https://openskynetwork.github.io/opensky-api/rest.html

 Key Features:
   - Fetches all current state vectors (GET /states/all)
   - Fetches state vectors by bounding box (geographic filter)
   - Parses positional array responses into named dictionaries
   - Converts to PySpark DataFrame with schema enforcement
   - Handles API quirks (mixed-type arrays, null fields)

 Usage:
   client = OpenSkyClient(config)
   df = client.fetch_all_states(spark)
   df = client.fetch_states_by_bbox(spark, lamin=45, lamax=55, ...)
=============================================================================
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, IntegerType, ArrayType, TimestampType,
)

from utils.api_utils import APIClient
from configs.app_config import AppConfig

logger = logging.getLogger("flight_analytics.ingestion.opensky_client")


class OpenSkyClient:
    """
    OpenSky Network API client that converts raw API responses
    into PySpark DataFrames with full schema enforcement.

    The OpenSky API returns state vectors as positional arrays:
        [icao24, callsign, origin_country, time_position, ...]

    This client maps each position to a named field and handles
    type casting, null values, and data validation.
    """

    # ── OpenSky state vector field mapping (position → name) ───────────
    STATE_VECTOR_FIELDS = [
        ("icao24", str),
        ("callsign", str),
        ("origin_country", str),
        ("time_position", int),
        ("last_contact", int),
        ("longitude", float),
        ("latitude", float),
        ("baro_altitude", float),
        ("on_ground", bool),
        ("velocity", float),
        ("true_track", float),
        ("vertical_rate", float),
        ("sensors", list),
        ("geo_altitude", float),
        ("squawk", str),
        ("spi", bool),
        ("position_source", int),
    ]

    def __init__(self, config: AppConfig):
        """
        Initialize the OpenSky client.

        Args:
            config: Application configuration with API settings
        """
        self.config = config
        self._api_client = APIClient(config.api)
        logger.info(
            "OpenSkyClient initialized | authenticated=%s",
            config.api.is_authenticated,
        )

    def fetch_raw_states(
        self,
        time_secs: Optional[int] = None,
        icao24: Optional[List[str]] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        extended: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch raw state vectors from the OpenSky API.

        Args:
            time_secs: Unix timestamp for historical data (None = latest)
            icao24: Filter by ICAO24 addresses
            bbox: Bounding box (lamin, lomin, lamax, lomax)
            extended: Include category information

        Returns:
            Raw API response dict or None on failure
        """
        params = {}

        if time_secs:
            params["time"] = time_secs
        if icao24:
            params["icao24"] = ",".join(icao24)
        if bbox:
            params["lamin"] = bbox[0]
            params["lomin"] = bbox[1]
            params["lamax"] = bbox[2]
            params["lomax"] = bbox[3]
        if extended:
            params["extended"] = 1

        response = self._api_client.get_with_backoff(
            self.config.api.states_endpoint,
            params=params,
            max_attempts=self.config.api.max_retries,
        )

        if response and "states" in response:
            state_count = len(response["states"]) if response["states"] else 0
            logger.info(
                "Fetched %d state vectors | time=%s",
                state_count,
                response.get("time", "latest"),
            )
        else:
            logger.warning("No state vectors returned from API")

        return response

    def parse_state_vectors(
        self, raw_response: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parse raw API response into a list of named dictionaries.

        The OpenSky API returns each state vector as a positional array.
        This method maps positions to field names with type casting.

        Args:
            raw_response: Raw API response with 'states' array

        Returns:
            List of parsed state vector dictionaries
        """
        if not raw_response or "states" not in raw_response:
            return []

        states = raw_response.get("states", [])
        if not states:
            return []

        api_time = raw_response.get("time", int(datetime.now(timezone.utc).timestamp()))
        parsed_states = []

        for state_array in states:
            if not state_array or len(state_array) < 17:
                logger.debug("Skipping malformed state vector: %s", state_array)
                continue

            try:
                state = {}
                for i, (field_name, field_type) in enumerate(self.STATE_VECTOR_FIELDS):
                    raw_value = state_array[i] if i < len(state_array) else None

                    if raw_value is None:
                        state[field_name] = None
                    elif field_type == bool:
                        state[field_name] = bool(raw_value)
                    elif field_type == int:
                        state[field_name] = int(raw_value) if raw_value is not None else None
                    elif field_type == float:
                        state[field_name] = float(raw_value) if raw_value is not None else None
                    elif field_type == list:
                        state[field_name] = raw_value if isinstance(raw_value, list) else None
                    else:
                        state[field_name] = str(raw_value).strip() if raw_value else None

                parsed_states.append(state)

            except (ValueError, TypeError, IndexError) as e:
                logger.debug(
                    "Error parsing state vector: %s | error=%s",
                    state_array[:3], str(e),
                )
                continue

        logger.info(
            "Parsed %d/%d state vectors successfully",
            len(parsed_states), len(states),
        )
        return parsed_states

    def fetch_all_states(
        self,
        spark: SparkSession,
        batch_id: Optional[str] = None,
    ) -> Optional[DataFrame]:
        """
        Fetch all current state vectors and return as a PySpark DataFrame
        with Bronze layer schema.

        This is the primary entry point for batch ingestion.

        Args:
            spark: Active SparkSession
            batch_id: Unique batch identifier (auto-generated if None)

        Returns:
            PySpark DataFrame with flight state vectors, or None on failure
        """
        batch_id = batch_id or f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        # ── Fetch from API ─────────────────────────────────────────────
        raw_response = self.fetch_raw_states()
        if not raw_response:
            logger.error("Failed to fetch state vectors from OpenSky API")
            return None

        # ── Parse into dicts ───────────────────────────────────────────
        parsed_states = self.parse_state_vectors(raw_response)
        if not parsed_states:
            logger.warning("No valid state vectors after parsing")
            return None

        # ── Convert to DataFrame ───────────────────────────────────────
        df = self._to_dataframe(spark, parsed_states, batch_id)

        logger.info(
            "Created DataFrame | rows=%d | batch_id=%s",
            df.count(), batch_id,
        )
        return df

    def fetch_states_by_bbox(
        self,
        spark: SparkSession,
        lamin: float,
        lomin: float,
        lamax: float,
        lomax: float,
        batch_id: Optional[str] = None,
    ) -> Optional[DataFrame]:
        """
        Fetch state vectors within a geographic bounding box.

        Useful for regional analysis (e.g., European airspace only).

        Args:
            spark: Active SparkSession
            lamin: Minimum latitude
            lomin: Minimum longitude
            lamax: Maximum latitude
            lomax: Maximum longitude
            batch_id: Unique batch identifier

        Returns:
            PySpark DataFrame or None
        """
        batch_id = batch_id or f"bbox_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        raw_response = self.fetch_raw_states(
            bbox=(lamin, lomin, lamax, lomax)
        )
        if not raw_response:
            return None

        parsed_states = self.parse_state_vectors(raw_response)
        if not parsed_states:
            return None

        return self._to_dataframe(spark, parsed_states, batch_id)

    def _to_dataframe(
        self,
        spark: SparkSession,
        parsed_states: List[Dict[str, Any]],
        batch_id: str,
    ) -> DataFrame:
        """
        Convert parsed state vector dicts to a PySpark DataFrame.

        Applies:
          - Schema enforcement via explicit StructType
          - Metadata column injection (ingestion_timestamp, batch_id, etc.)
          - Null-safe type casting

        Args:
            spark: SparkSession
            parsed_states: List of parsed state dictionaries
            batch_id: Batch identifier

        Returns:
            Schema-enforced PySpark DataFrame
        """
        # ── Define explicit schema for createDataFrame ─────────────────
        schema = StructType([
            StructField("icao24", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("time_position", LongType(), True),
            StructField("last_contact", LongType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("baro_altitude", DoubleType(), True),
            StructField("on_ground", BooleanType(), True),
            StructField("velocity", DoubleType(), True),
            StructField("true_track", DoubleType(), True),
            StructField("vertical_rate", DoubleType(), True),
            StructField("sensors", ArrayType(IntegerType()), True),
            StructField("geo_altitude", DoubleType(), True),
            StructField("squawk", StringType(), True),
            StructField("spi", BooleanType(), True),
            StructField("position_source", IntegerType(), True),
        ])

        # ── Prepare rows (handle sensors field) ───────────────────────
        rows = []
        for state in parsed_states:
            # Sensors may come as int array or None
            sensors = state.get("sensors")
            if sensors and not isinstance(sensors, list):
                sensors = None

            rows.append((
                state.get("icao24"),
                state.get("callsign"),
                state.get("origin_country"),
                state.get("time_position"),
                state.get("last_contact"),
                state.get("longitude"),
                state.get("latitude"),
                state.get("baro_altitude"),
                state.get("on_ground"),
                state.get("velocity"),
                state.get("true_track"),
                state.get("vertical_rate"),
                sensors,
                state.get("geo_altitude"),
                state.get("squawk"),
                state.get("spi"),
                state.get("position_source"),
            ))

        # ── Create DataFrame with schema ───────────────────────────────
        df = spark.createDataFrame(rows, schema=schema)

        # ── Add pipeline metadata columns ──────────────────────────────
        df = (
            df.withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn(
                "ingestion_date",
                F.date_format(F.current_timestamp(), "yyyy-MM-dd"),
            )
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("source_system", F.lit("opensky_api"))
        )

        return df

    def get_api_metrics(self) -> Dict[str, Any]:
        """Get API client performance metrics."""
        return self._api_client.get_metrics()

    def close(self) -> None:
        """Release API client resources."""
        self._api_client.close()
        logger.info("OpenSkyClient closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
