"""
=============================================================================
 Feature Engineering — Flight Analytics Platform
=============================================================================
 ML feature extraction and preparation for anomaly detection.

 Features extracted:
   1. Speed features: velocity, speed change rate, speed z-score
   2. Altitude features: altitude, vertical rate, altitude change
   3. Position features: heading change, distance traveled
   4. Temporal features: time of day, day of week
   5. Statistical features: rolling averages, standard deviations

 All features are computed using PySpark window functions and
 vector assembly for MLlib compatibility.

 Usage:
   engineer = FeatureEngineer(spark)
   feature_df = engineer.extract_features(silver_df)
   ml_ready_df = engineer.assemble_features(feature_df)
=============================================================================
"""

import logging
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler

from configs.app_config import AppConfig

logger = logging.getLogger("flight_analytics.ml.feature_engineering")


class FeatureEngineer:
    """
    Feature extraction engine for flight anomaly detection.

    Transforms raw flight data into ML-ready feature vectors using
    PySpark window functions and MLlib feature transformers.
    """

    # ── Default feature columns for anomaly detection ──────────────────
    DEFAULT_FEATURE_COLUMNS = [
        "velocity_kmh",
        "baro_altitude_ft",
        "vertical_rate_fpm",
        "true_track_deg",
        "speed_change_kmh",
        "altitude_change_ft",
        "heading_change_deg",
        "velocity_zscore",
        "altitude_zscore",
        "vertical_rate_zscore",
    ]

    def __init__(self, spark: SparkSession, config: Optional[AppConfig] = None):
        self.spark = spark
        self.config = config
        logger.info("FeatureEngineer initialized")

    def extract_features(self, df: DataFrame) -> DataFrame:
        """
        Extract all features from silver-layer flight data.

        Applies:
          1. Lag-based change features (speed change, altitude change)
          2. Rolling statistics (mean, stddev over window)
          3. Z-score normalization
          4. Temporal features
          5. Movement features

        Args:
            df: Silver-layer DataFrame with cleaned flight data

        Returns:
            DataFrame with extracted feature columns
        """
        logger.info("Extracting features for anomaly detection...")

        # ── Filter to airborne flights with valid data ─────────────────
        feature_df = df.where(
            (F.col("on_ground") == False)
            & F.col("velocity_kmh").isNotNull()
            & F.col("baro_altitude_ft").isNotNull()
            & F.col("icao24").isNotNull()
        )

        # ── Step 1: Lag-based change features ──────────────────────────
        feature_df = self._add_change_features(feature_df)

        # ── Step 2: Rolling statistics ─────────────────────────────────
        feature_df = self._add_rolling_stats(feature_df)

        # ── Step 3: Z-scores ───────────────────────────────────────────
        feature_df = self._add_zscores(feature_df)

        # ── Step 4: Temporal features ──────────────────────────────────
        feature_df = self._add_temporal_features(feature_df)

        # ── Step 5: Derived ratios ─────────────────────────────────────
        feature_df = self._add_derived_ratios(feature_df)

        logger.info(
            "Feature extraction complete | columns=%d",
            len(feature_df.columns),
        )
        return feature_df

    def _add_change_features(self, df: DataFrame) -> DataFrame:
        """Add lag-based change features using window functions."""

        aircraft_window = Window.partitionBy("icao24").orderBy(
            "ingestion_timestamp"
        )

        return (
            df
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
            .withColumn(
                "speed_change_kmh",
                F.coalesce(
                    F.col("velocity_kmh") - F.col("prev_velocity_kmh"),
                    F.lit(0.0),
                ),
            )
            .withColumn(
                "altitude_change_ft",
                F.coalesce(
                    F.col("baro_altitude_ft") - F.col("prev_altitude_ft"),
                    F.lit(0.0),
                ),
            )
            .withColumn(
                "heading_change_deg",
                F.coalesce(
                    F.abs(F.col("true_track_deg") - F.col("prev_heading")),
                    F.lit(0.0),
                ),
            )
        )

    def _add_rolling_stats(self, df: DataFrame) -> DataFrame:
        """Add rolling window statistics per aircraft."""

        # ── Rolling window: 5 previous observations ────────────────────
        rolling_window = Window.partitionBy("icao24").orderBy(
            "ingestion_timestamp"
        ).rowsBetween(-5, 0)

        return (
            df
            .withColumn(
                "rolling_avg_speed",
                F.round(F.avg("velocity_kmh").over(rolling_window), 1),
            )
            .withColumn(
                "rolling_stddev_speed",
                F.round(
                    F.coalesce(
                        F.stddev("velocity_kmh").over(rolling_window),
                        F.lit(0.0),
                    ),
                    1,
                ),
            )
            .withColumn(
                "rolling_avg_altitude",
                F.round(F.avg("baro_altitude_ft").over(rolling_window), 0),
            )
            .withColumn(
                "rolling_stddev_altitude",
                F.round(
                    F.coalesce(
                        F.stddev("baro_altitude_ft").over(rolling_window),
                        F.lit(0.0),
                    ),
                    0,
                ),
            )
        )

    def _add_zscores(self, df: DataFrame) -> DataFrame:
        """
        Add z-score columns for key metrics.

        Z-score = (value - mean) / stddev

        Uses global statistics (across all aircraft) to detect
        outliers relative to the fleet.
        """
        # ── Compute global stats ───────────────────────────────────────
        stats = df.agg(
            F.avg("velocity_kmh").alias("global_avg_speed"),
            F.stddev("velocity_kmh").alias("global_stddev_speed"),
            F.avg("baro_altitude_ft").alias("global_avg_alt"),
            F.stddev("baro_altitude_ft").alias("global_stddev_alt"),
            F.avg("vertical_rate_fpm").alias("global_avg_vrate"),
            F.stddev("vertical_rate_fpm").alias("global_stddev_vrate"),
        ).collect()[0]

        avg_speed = stats["global_avg_speed"] or 0
        std_speed = stats["global_stddev_speed"] or 1
        avg_alt = stats["global_avg_alt"] or 0
        std_alt = stats["global_stddev_alt"] or 1
        avg_vrate = stats["global_avg_vrate"] or 0
        std_vrate = stats["global_stddev_vrate"] or 1

        return (
            df.withColumn(
                "velocity_zscore",
                F.round(
                    (F.col("velocity_kmh") - F.lit(avg_speed))
                    / F.lit(std_speed),
                    2,
                ),
            )
            .withColumn(
                "altitude_zscore",
                F.round(
                    (F.col("baro_altitude_ft") - F.lit(avg_alt))
                    / F.lit(std_alt),
                    2,
                ),
            )
            .withColumn(
                "vertical_rate_zscore",
                F.round(
                    (F.coalesce(F.col("vertical_rate_fpm"), F.lit(0))
                     - F.lit(avg_vrate))
                    / F.lit(std_vrate),
                    2,
                ),
            )
        )

    def _add_temporal_features(self, df: DataFrame) -> DataFrame:
        """Add time-based features."""

        result = df

        if "position_timestamp" in result.columns:
            result = (
                result.withColumn(
                    "hour_of_day",
                    F.hour(F.col("position_timestamp")),
                )
                .withColumn(
                    "day_of_week",
                    F.dayofweek(F.col("position_timestamp")),
                )
                .withColumn(
                    "is_night",
                    F.when(
                        (F.hour(F.col("position_timestamp")) < 6)
                        | (F.hour(F.col("position_timestamp")) > 22),
                        1,
                    ).otherwise(0),
                )
            )
        elif "ingestion_timestamp" in result.columns:
            result = (
                result.withColumn(
                    "hour_of_day",
                    F.hour(F.col("ingestion_timestamp")),
                )
                .withColumn(
                    "day_of_week",
                    F.dayofweek(F.col("ingestion_timestamp")),
                )
                .withColumn("is_night", F.lit(0))
            )

        return result

    def _add_derived_ratios(self, df: DataFrame) -> DataFrame:
        """Add derived ratio features."""
        return (
            df.withColumn(
                "speed_to_altitude_ratio",
                F.when(
                    F.col("baro_altitude_ft") > 0,
                    F.round(
                        F.col("velocity_kmh") / F.col("baro_altitude_ft"),
                        4,
                    ),
                ).otherwise(0.0),
            )
            .withColumn(
                "abs_vertical_rate",
                F.abs(F.coalesce(F.col("vertical_rate_fpm"), F.lit(0))),
            )
        )

    def assemble_features(
        self,
        df: DataFrame,
        feature_columns: Optional[List[str]] = None,
        output_col: str = "features",
        scale: bool = True,
    ) -> DataFrame:
        """
        Assemble feature columns into a single vector column
        for MLlib model input.

        Args:
            df: DataFrame with individual feature columns
            feature_columns: List of column names to include
            output_col: Name of the output vector column
            scale: Whether to apply StandardScaler

        Returns:
            DataFrame with assembled (and optionally scaled) feature vector
        """
        columns = feature_columns or self.DEFAULT_FEATURE_COLUMNS

        # ── Filter to available columns ────────────────────────────────
        available = [c for c in columns if c in df.columns]
        if len(available) < len(columns):
            missing = set(columns) - set(available)
            logger.warning("Missing feature columns: %s", missing)

        # ── Fill nulls with 0 for ML ───────────────────────────────────
        filled_df = df
        for col in available:
            filled_df = filled_df.withColumn(
                col,
                F.coalesce(F.col(col).cast("double"), F.lit(0.0)),
            )

        # ── Vector assembly ────────────────────────────────────────────
        assembler = VectorAssembler(
            inputCols=available,
            outputCol="raw_features" if scale else output_col,
            handleInvalid="skip",
        )
        assembled = assembler.transform(filled_df)

        if scale:
            scaler = StandardScaler(
                inputCol="raw_features",
                outputCol=output_col,
                withStd=True,
                withMean=True,
            )
            scaler_model = scaler.fit(assembled)
            assembled = scaler_model.transform(assembled)

        logger.info(
            "Features assembled | columns=%d | scaled=%s",
            len(available), scale,
        )
        return assembled
