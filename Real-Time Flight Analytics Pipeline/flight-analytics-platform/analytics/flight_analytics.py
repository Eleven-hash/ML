"""
=============================================================================
 Flight Analytics — Flight Analytics Platform
=============================================================================
 Core analytics module providing complex PySpark queries using:
   - Window functions (ranking, running totals, lag/lead)
   - Multi-level aggregations
   - Percentile computations
   - Spark SQL integration
   - Complex joins

 Analytics Categories:
   1. Traffic Analysis: Busiest countries, peak hours, trends
   2. Speed Analysis: Distribution, percentiles, outliers
   3. Altitude Analysis: Distribution, flight phases, bands
   4. Aircraft Analysis: Unique aircraft, movement patterns
   5. Trend Analysis: Time-series analysis of flight metrics

 Usage:
   analytics = FlightAnalytics(spark, config)
   top_countries = analytics.top_countries_by_flights(silver_df, n=20)
   speed_dist = analytics.speed_distribution(silver_df)
=============================================================================
"""

import logging
from typing import Optional, Dict, Any, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from configs.app_config import AppConfig

logger = logging.getLogger("flight_analytics.analytics.flight")


class FlightAnalytics:
    """
    Core flight analytics engine providing complex analytical queries.
    """

    def __init__(self, spark: SparkSession, config: AppConfig):
        self.spark = spark
        self.config = config
        logger.info("FlightAnalytics initialized")

    # ══════════════════════════════════════════════════════════════════
    #  TRAFFIC ANALYSIS
    # ══════════════════════════════════════════════════════════════════
    def top_countries_by_flights(
        self, df: DataFrame, n: int = 20
    ) -> DataFrame:
        """
        Top N countries by total number of active flights.

        Uses window functions to calculate:
          - Total flights per country
          - Rank within global traffic
          - Percentage of total flights
          - Running cumulative percentage

        Args:
            df: Silver-layer DataFrame
            n: Number of top countries to return

        Returns:
            DataFrame with country rankings
        """
        # ── Count flights per country ──────────────────────────────────
        country_counts = (
            df.groupBy("origin_country", "region")
            .agg(
                F.count("*").alias("total_flights"),
                F.sum(
                    F.when(F.col("on_ground") == False, 1).otherwise(0)
                ).alias("airborne"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
            )
        )

        # ── Total for percentage calculation ───────────────────────────
        total_flights = df.count()

        # ── Window for ranking and cumulative ──────────────────────────
        rank_window = Window.orderBy(F.desc("total_flights"))

        ranked = (
            country_counts
            .withColumn("rank", F.rank().over(rank_window))
            .withColumn(
                "pct_of_total",
                F.round(
                    F.col("total_flights") / F.lit(total_flights) * 100, 2
                ),
            )
            .withColumn(
                "cumulative_pct",
                F.round(
                    F.sum("total_flights").over(
                        rank_window.rowsBetween(
                            Window.unboundedPreceding, Window.currentRow
                        )
                    )
                    / F.lit(total_flights) * 100,
                    2,
                ),
            )
            .where(F.col("rank") <= n)
            .orderBy("rank")
        )

        logger.info("Top %d countries by flights computed", n)
        return ranked

    def peak_traffic_hours(self, df: DataFrame) -> DataFrame:
        """
        Identify peak traffic hours using hourly aggregation.

        Returns flights per hour with categorization:
          - Peak (top 25%), Normal, Off-peak (bottom 25%)
        """
        hourly = (
            df.where(F.col("position_hour").isNotNull())
            .groupBy("position_hour")
            .agg(
                F.count("*").alias("total_flights"),
                F.countDistinct("icao24").alias("unique_aircraft"),
                F.countDistinct("origin_country").alias("unique_countries"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
            )
        )

        # ── Classify hours using percentiles ───────────────────────────
        stats = hourly.agg(
            F.percentile_approx("total_flights", 0.75).alias("p75"),
            F.percentile_approx("total_flights", 0.25).alias("p25"),
        ).collect()[0]

        p75 = stats["p75"]
        p25 = stats["p25"]

        classified = hourly.withColumn(
            "traffic_category",
            F.when(F.col("total_flights") >= p75, "peak")
            .when(F.col("total_flights") <= p25, "off_peak")
            .otherwise("normal"),
        ).orderBy("position_hour")

        logger.info("Peak traffic hours computed")
        return classified

    def flights_over_time(
        self, df: DataFrame, time_col: str = "ingestion_date"
    ) -> DataFrame:
        """
        Active flights over time for trend visualization.

        Uses lag/lead window functions to calculate:
          - Current period count
          - Previous period count
          - Change (absolute and percentage)
        """
        daily = (
            df.groupBy(time_col)
            .agg(
                F.count("*").alias("total_flights"),
                F.countDistinct("icao24").alias("unique_aircraft"),
                F.countDistinct("origin_country").alias("unique_countries"),
            )
        )

        # ── Lag for period-over-period comparison ──────────────────────
        time_window = Window.orderBy(time_col)

        trend = (
            daily.withColumn(
                "prev_flights", F.lag("total_flights", 1).over(time_window)
            )
            .withColumn(
                "flight_change",
                F.col("total_flights") - F.coalesce(F.col("prev_flights"), F.lit(0)),
            )
            .withColumn(
                "change_pct",
                F.round(
                    F.col("flight_change")
                    / F.coalesce(F.col("prev_flights"), F.lit(1))
                    * 100,
                    2,
                ),
            )
            .orderBy(time_col)
        )

        logger.info("Flights over time trend computed")
        return trend

    # ══════════════════════════════════════════════════════════════════
    #  SPEED ANALYSIS
    # ══════════════════════════════════════════════════════════════════
    def speed_distribution(self, df: DataFrame) -> DataFrame:
        """
        Speed distribution analysis with percentile buckets.

        Returns speed statistics per speed_category including:
          - Mean, median, stddev
          - 10th, 25th, 50th, 75th, 90th percentiles
        """
        airborne = df.where(
            (F.col("on_ground") == False)
            & F.col("velocity_kmh").isNotNull()
        )

        speed_stats = (
            airborne.groupBy("speed_category")
            .agg(
                F.count("*").alias("count"),
                F.round(F.avg("velocity_kmh"), 1).alias("mean_speed"),
                F.round(F.stddev("velocity_kmh"), 1).alias("stddev_speed"),
                F.round(F.min("velocity_kmh"), 1).alias("min_speed"),
                F.round(F.max("velocity_kmh"), 1).alias("max_speed"),
                F.round(
                    F.percentile_approx("velocity_kmh", 0.10), 1
                ).alias("p10_speed"),
                F.round(
                    F.percentile_approx("velocity_kmh", 0.25), 1
                ).alias("p25_speed"),
                F.round(
                    F.percentile_approx("velocity_kmh", 0.50), 1
                ).alias("median_speed"),
                F.round(
                    F.percentile_approx("velocity_kmh", 0.75), 1
                ).alias("p75_speed"),
                F.round(
                    F.percentile_approx("velocity_kmh", 0.90), 1
                ).alias("p90_speed"),
            )
            .orderBy(F.desc("count"))
        )

        logger.info("Speed distribution computed")
        return speed_stats

    def speed_by_region(self, df: DataFrame) -> DataFrame:
        """Average speed by geographic region."""
        return (
            df.where(
                (F.col("on_ground") == False)
                & F.col("velocity_kmh").isNotNull()
            )
            .groupBy("region")
            .agg(
                F.count("*").alias("flight_count"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
                F.round(F.avg("velocity_knots"), 1).alias("avg_speed_knots"),
                F.round(F.max("velocity_kmh"), 1).alias("max_speed_kmh"),
            )
            .orderBy(F.desc("flight_count"))
        )

    # ══════════════════════════════════════════════════════════════════
    #  ALTITUDE ANALYSIS
    # ══════════════════════════════════════════════════════════════════
    def altitude_distribution(self, df: DataFrame) -> DataFrame:
        """
        Altitude distribution by band and flight phase.

        Includes average vertical rate to identify climbing/descending
        patterns per altitude band.
        """
        return (
            df.where(
                (F.col("on_ground") == False)
                & F.col("baro_altitude_ft").isNotNull()
            )
            .groupBy("altitude_band", "flight_phase")
            .agg(
                F.count("*").alias("flight_count"),
                F.round(F.avg("baro_altitude_ft"), 0).alias("avg_altitude_ft"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
                F.round(F.avg("vertical_rate_fpm"), 0).alias("avg_vrate_fpm"),
                F.round(F.min("baro_altitude_ft"), 0).alias("min_alt_ft"),
                F.round(F.max("baro_altitude_ft"), 0).alias("max_alt_ft"),
            )
            .orderBy(F.desc("flight_count"))
        )

    def altitude_percentiles(self, df: DataFrame) -> DataFrame:
        """Altitude percentile analysis for airborne flights."""
        airborne = df.where(
            (F.col("on_ground") == False)
            & F.col("baro_altitude_ft").isNotNull()
        )

        return airborne.agg(
            F.count("*").alias("total_airborne"),
            F.round(
                F.percentile_approx("baro_altitude_ft", 0.10), 0
            ).alias("p10_altitude_ft"),
            F.round(
                F.percentile_approx("baro_altitude_ft", 0.25), 0
            ).alias("p25_altitude_ft"),
            F.round(
                F.percentile_approx("baro_altitude_ft", 0.50), 0
            ).alias("median_altitude_ft"),
            F.round(
                F.percentile_approx("baro_altitude_ft", 0.75), 0
            ).alias("p75_altitude_ft"),
            F.round(
                F.percentile_approx("baro_altitude_ft", 0.90), 0
            ).alias("p90_altitude_ft"),
            F.round(F.avg("baro_altitude_ft"), 0).alias("mean_altitude_ft"),
            F.round(F.stddev("baro_altitude_ft"), 0).alias("stddev_altitude_ft"),
        )

    # ══════════════════════════════════════════════════════════════════
    #  AIRCRAFT ANALYSIS
    # ══════════════════════════════════════════════════════════════════
    def aircraft_summary(self, df: DataFrame) -> DataFrame:
        """
        Per-aircraft summary statistics.

        Uses window functions to rank aircraft by various metrics.
        """
        aircraft = (
            df.where(F.col("on_ground") == False)
            .groupBy("icao24", "callsign", "origin_country")
            .agg(
                F.count("*").alias("data_points"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
                F.round(F.max("velocity_kmh"), 1).alias("max_speed_kmh"),
                F.round(F.avg("baro_altitude_ft"), 0).alias("avg_altitude_ft"),
                F.round(F.max("baro_altitude_ft"), 0).alias("max_altitude_ft"),
                F.first("latitude").alias("last_lat"),
                F.first("longitude").alias("last_lon"),
            )
        )

        # ── Rank by speed and altitude ─────────────────────────────────
        speed_window = Window.orderBy(F.desc("max_speed_kmh"))
        alt_window = Window.orderBy(F.desc("max_altitude_ft"))

        ranked = (
            aircraft
            .withColumn("speed_rank", F.rank().over(speed_window))
            .withColumn("altitude_rank", F.rank().over(alt_window))
        )

        logger.info("Aircraft summary computed")
        return ranked

    # ══════════════════════════════════════════════════════════════════
    #  REGION ANALYSIS
    # ══════════════════════════════════════════════════════════════════
    def flights_by_region(self, df: DataFrame) -> DataFrame:
        """Comprehensive regional flight analysis."""
        return (
            df.groupBy("region")
            .agg(
                F.count("*").alias("total_flights"),
                F.countDistinct("icao24").alias("unique_aircraft"),
                F.countDistinct("origin_country").alias("unique_countries"),
                F.sum(
                    F.when(F.col("on_ground") == False, 1).otherwise(0)
                ).alias("airborne_flights"),
                F.round(F.avg("velocity_kmh"), 1).alias("avg_speed_kmh"),
                F.round(F.avg("baro_altitude_ft"), 0).alias("avg_altitude_ft"),
            )
            .withColumn(
                "airborne_ratio",
                F.round(
                    F.col("airborne_flights") / F.col("total_flights") * 100,
                    1,
                ),
            )
            .orderBy(F.desc("total_flights"))
        )

    # ══════════════════════════════════════════════════════════════════
    #  SQL-BASED ANALYTICS
    # ══════════════════════════════════════════════════════════════════
    def register_temp_views(self, df: DataFrame) -> None:
        """
        Register DataFrames as temporary SQL views for Spark SQL queries.

        Args:
            df: Silver-layer DataFrame
        """
        df.createOrReplaceTempView("flights")
        logger.info("Registered 'flights' temp view")

    def run_sql(self, query: str) -> DataFrame:
        """
        Execute a Spark SQL query.

        Args:
            query: SQL query string

        Returns:
            Query result DataFrame
        """
        logger.info("Executing SQL query")
        return self.spark.sql(query)

    def get_comprehensive_analytics(
        self, df: DataFrame
    ) -> Dict[str, DataFrame]:
        """
        Run all analytics and return as a dict of DataFrames.

        Returns:
            Dict mapping analytics name to result DataFrame
        """
        return {
            "top_countries": self.top_countries_by_flights(df),
            "peak_hours": self.peak_traffic_hours(df),
            "speed_distribution": self.speed_distribution(df),
            "speed_by_region": self.speed_by_region(df),
            "altitude_distribution": self.altitude_distribution(df),
            "flights_by_region": self.flights_by_region(df),
            "aircraft_summary": self.aircraft_summary(df),
        }
