"""
=============================================================================
 Geo Analytics — Flight Analytics Platform
=============================================================================
 Geospatial analytics for flight data including:
   - Flight density grid analysis (lat/lon bucketing)
   - Regional distribution analysis
   - Movement tracking and trajectory analysis
   - Distance calculations using Haversine formula
   - Geo-spatial heatmap data generation

 Usage:
   geo = GeoAnalytics(spark, config)
   heatmap = geo.flight_density_grid(silver_df, resolution=1.0)
   movement = geo.aircraft_movement_analysis(silver_df)
=============================================================================
"""

import logging
import math
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

from configs.app_config import AppConfig

logger = logging.getLogger("flight_analytics.analytics.geo")


class GeoAnalytics:
    """
    Geospatial analytics for flight data.

    Provides grid-based density analysis, movement tracking,
    and distance calculations for aviation analytics.
    """

    # Earth's radius in kilometers
    EARTH_RADIUS_KM = 6371.0

    def __init__(self, spark: SparkSession, config: AppConfig):
        self.spark = spark
        self.config = config

        # ── Register Haversine UDF ─────────────────────────────────────
        self._register_udfs()
        logger.info("GeoAnalytics initialized")

    def _register_udfs(self) -> None:
        """Register geospatial UDFs with Spark."""

        @F.udf(returnType=DoubleType())
        def haversine_distance(
            lat1: float, lon1: float, lat2: float, lon2: float
        ) -> Optional[float]:
            """
            Calculate great-circle distance between two points
            using the Haversine formula.

            Returns distance in kilometers.
            """
            if any(v is None for v in [lat1, lon1, lat2, lon2]):
                return None

            lat1_r = math.radians(lat1)
            lat2_r = math.radians(lat2)
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)

            a = (
                math.sin(dlat / 2) ** 2
                + math.cos(lat1_r) * math.cos(lat2_r)
                * math.sin(dlon / 2) ** 2
            )
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

            return GeoAnalytics.EARTH_RADIUS_KM * c

        self._haversine_udf = haversine_distance

    # ══════════════════════════════════════════════════════════════════
    #  DENSITY ANALYSIS
    # ══════════════════════════════════════════════════════════════════
    def flight_density_grid(
        self,
        df: DataFrame,
        resolution: float = 1.0,
    ) -> DataFrame:
        """
        Create a grid-based flight density map.

        Buckets lat/lon into grid cells and counts flights per cell.
        This data is used for heatmap visualizations.

        Args:
            df: Silver-layer DataFrame with lat/lon
            resolution: Grid cell size in degrees (1.0 = ~111km)

        Returns:
            DataFrame with grid cells and flight counts
        """
        logger.info(
            "Computing flight density grid | resolution=%.1f°",
            resolution,
        )

        density = (
            df.where(
                F.col("latitude").isNotNull()
                & F.col("longitude").isNotNull()
            )
            .withColumn(
                "lat_bucket",
                F.round(F.col("latitude") / resolution) * resolution,
            )
            .withColumn(
                "lon_bucket",
                F.round(F.col("longitude") / resolution) * resolution,
            )
            .groupBy("lat_bucket", "lon_bucket")
            .agg(
                F.count("*").alias("flight_count"),
                F.countDistinct("icao24").alias("unique_aircraft"),
                F.round(F.avg("baro_altitude_ft"), 0).alias("avg_altitude_ft"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
                F.countDistinct("origin_country").alias("unique_countries"),
            )
            .withColumn(
                "density_category",
                F.when(F.col("flight_count") > 100, "very_high")
                .when(F.col("flight_count") > 50, "high")
                .when(F.col("flight_count") > 20, "medium")
                .when(F.col("flight_count") > 5, "low")
                .otherwise("very_low"),
            )
            .orderBy(F.desc("flight_count"))
        )

        logger.info(
            "Density grid computed | cells=%d", density.count()
        )
        return density

    def regional_distribution(self, df: DataFrame) -> DataFrame:
        """
        Flight distribution by geographic region with spatial metrics.

        Computes centroid and spread for each region's flight activity.
        """
        return (
            df.where(
                F.col("latitude").isNotNull()
                & F.col("longitude").isNotNull()
            )
            .groupBy("region")
            .agg(
                F.count("*").alias("total_flights"),
                F.countDistinct("icao24").alias("unique_aircraft"),
                F.round(F.avg("latitude"), 4).alias("centroid_lat"),
                F.round(F.avg("longitude"), 4).alias("centroid_lon"),
                F.round(F.min("latitude"), 4).alias("min_lat"),
                F.round(F.max("latitude"), 4).alias("max_lat"),
                F.round(F.min("longitude"), 4).alias("min_lon"),
                F.round(F.max("longitude"), 4).alias("max_lon"),
                F.round(F.stddev("latitude"), 4).alias("lat_spread"),
                F.round(F.stddev("longitude"), 4).alias("lon_spread"),
            )
            .orderBy(F.desc("total_flights"))
        )

    # ══════════════════════════════════════════════════════════════════
    #  MOVEMENT TRACKING
    # ══════════════════════════════════════════════════════════════════
    def aircraft_movement_analysis(
        self, df: DataFrame
    ) -> DataFrame:
        """
        Analyze aircraft movement patterns using lag window functions.

        For each aircraft, calculates:
          - Previous position (lag)
          - Distance traveled between data points
          - Speed change between observations
          - Heading change (potential route deviation)
        """
        logger.info("Computing aircraft movement analysis...")

        # ── Window per aircraft ordered by time ────────────────────────
        aircraft_window = Window.partitionBy("icao24").orderBy(
            "ingestion_timestamp"
        )

        movement = (
            df.where(
                F.col("latitude").isNotNull()
                & F.col("longitude").isNotNull()
            )
            # ── Previous position (lag) ────────────────────────────────
            .withColumn(
                "prev_latitude", F.lag("latitude", 1).over(aircraft_window)
            )
            .withColumn(
                "prev_longitude", F.lag("longitude", 1).over(aircraft_window)
            )
            .withColumn(
                "prev_velocity_kmh",
                F.lag("velocity_kmh", 1).over(aircraft_window),
            )
            .withColumn(
                "prev_altitude_ft",
                F.lag("baro_altitude_ft", 1).over(aircraft_window),
            )
            .withColumn(
                "prev_heading",
                F.lag("true_track_deg", 1).over(aircraft_window),
            )
            # ── Derived movement metrics ───────────────────────────────
            .withColumn(
                "distance_km",
                self._haversine_udf(
                    F.col("prev_latitude"),
                    F.col("prev_longitude"),
                    F.col("latitude"),
                    F.col("longitude"),
                ),
            )
            .withColumn(
                "speed_change_kmh",
                F.col("velocity_kmh") - F.col("prev_velocity_kmh"),
            )
            .withColumn(
                "altitude_change_ft",
                F.col("baro_altitude_ft") - F.col("prev_altitude_ft"),
            )
            .withColumn(
                "heading_change_deg",
                F.abs(F.col("true_track_deg") - F.col("prev_heading")),
            )
            # ── Observation count per aircraft ─────────────────────────
            .withColumn(
                "observation_num",
                F.row_number().over(aircraft_window),
            )
        )

        logger.info("Aircraft movement analysis complete")
        return movement

    def hotspot_detection(
        self,
        df: DataFrame,
        min_flights: int = 50,
        grid_resolution: float = 0.5,
    ) -> DataFrame:
        """
        Detect high-traffic hotspots (potential congestion areas).

        Returns grid cells exceeding the minimum flight threshold,
        ranked by density.

        Args:
            df: Silver DataFrame
            min_flights: Minimum flights to qualify as hotspot
            grid_resolution: Grid cell size in degrees

        Returns:
            DataFrame with hotspot locations
        """
        density = self.flight_density_grid(df, resolution=grid_resolution)

        hotspots = (
            density.where(F.col("flight_count") >= min_flights)
            .withColumn(
                "hotspot_rank",
                F.rank().over(Window.orderBy(F.desc("flight_count"))),
            )
            .withColumn(
                "hotspot_score",
                F.round(
                    F.col("flight_count")
                    * F.col("unique_aircraft")
                    / F.col("unique_countries"),
                    1,
                ),
            )
            .orderBy(F.desc("hotspot_score"))
        )

        logger.info(
            "Hotspot detection | found %d hotspots (min_flights=%d)",
            hotspots.count(), min_flights,
        )
        return hotspots

    def country_pair_analysis(self, df: DataFrame) -> DataFrame:
        """
        Analyze flight activity between origin countries and
        geographic grid regions (proxy for country-pair routes).
        """
        return (
            df.where(
                F.col("latitude").isNotNull()
                & F.col("longitude").isNotNull()
            )
            .withColumn(
                "current_region_lat",
                F.round(F.col("latitude") / 10.0) * 10.0,
            )
            .withColumn(
                "current_region_lon",
                F.round(F.col("longitude") / 10.0) * 10.0,
            )
            .groupBy(
                "origin_country",
                "current_region_lat",
                "current_region_lon",
            )
            .agg(
                F.count("*").alias("flights"),
                F.countDistinct("icao24").alias("aircraft"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
            )
            .orderBy(F.desc("flights"))
        )
