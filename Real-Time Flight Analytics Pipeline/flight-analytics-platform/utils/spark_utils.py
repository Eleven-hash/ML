"""
=============================================================================
 Spark Utilities — Flight Analytics Platform
=============================================================================
 Spark session management, DataFrame optimization helpers, and common
 operations used across the pipeline.

 Key Features:
   - Singleton SparkSession management (Databricks-aware)
   - Adaptive Query Execution configuration
   - DataFrame repartitioning and caching utilities
   - Broadcast join helpers
   - Schema validation utilities

 Usage:
   spark = SparkSessionManager.get_or_create(config)
   df = SparkUtils.optimize_for_join(df, "icao24")
=============================================================================
"""

import logging
from typing import List, Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger("flight_analytics.spark_utils")


class SparkSessionManager:
    """
    Singleton manager for SparkSession lifecycle.

    On Databricks, reuses the existing cluster session.
    Locally, creates a new session with Delta Lake support.
    """

    _session: Optional[SparkSession] = None

    @classmethod
    def get_or_create(
        cls,
        config=None,
        app_name: str = "FlightAnalyticsPlatform",
    ) -> SparkSession:
        """
        Get existing or create new SparkSession.

        Args:
            config: Optional AppConfig with Spark settings
            app_name: Application name for Spark UI

        Returns:
            Active SparkSession instance
        """
        if cls._session is not None and cls._session._jsc is not None:
            try:
                # Verify session is still alive
                cls._session.sparkContext.getConf().get("spark.app.name")
                return cls._session
            except Exception:
                cls._session = None

        builder = SparkSession.builder.appName(app_name)

        # ── Apply configuration if provided ────────────────────────────
        if config and hasattr(config, "spark"):
            for key, value in config.spark.as_spark_conf().items():
                builder = builder.config(key, value)

        # ── Delta Lake support (local mode) ────────────────────────────
        builder = builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        import os
        if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
            # Automatically download the Delta Lake Jars from Maven repository in local dev
            builder = builder.config(
                "spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0"
            )

        # ── Enable Hive support for SQL analytics ──────────────────────
        builder = builder.enableHiveSupport()

        cls._session = builder.getOrCreate()

        # ── Apply runtime settings ─────────────────────────────────────
        cls._apply_runtime_settings(cls._session, config)

        logger.info(
            "SparkSession active | app=%s | version=%s",
            cls._session.sparkContext.appName,
            cls._session.version,
        )

        return cls._session

    @classmethod
    def _apply_runtime_settings(
        cls, spark: SparkSession, config=None
    ) -> None:
        """Apply runtime Spark SQL settings."""

        # ── Adaptive Query Execution ───────────────────────────────────
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set(
            "spark.sql.adaptive.coalescePartitions.enabled", "true"
        )
        spark.conf.set(
            "spark.sql.adaptive.skewJoin.enabled", "true"
        )

        # ── Delta Lake defaults ────────────────────────────────────────
        spark.conf.set(
            "spark.databricks.delta.schema.autoMerge.enabled", "true"
        )
        spark.conf.set(
            "spark.databricks.delta.optimizeWrite.enabled", "true"
        )

        logger.debug("Runtime Spark settings applied")

    @classmethod
    def stop(cls) -> None:
        """Stop the active SparkSession."""
        if cls._session:
            cls._session.stop()
            cls._session = None
            logger.info("SparkSession stopped")


class SparkUtils:
    """
    Collection of DataFrame utility functions for common operations.
    """

    # ══════════════════════════════════════════════════════════════════
    #  SCHEMA VALIDATION
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def validate_schema(
        df: DataFrame,
        expected_schema: StructType,
        strict: bool = False,
    ) -> tuple:
        """
        Validate DataFrame schema against expected schema.

        Args:
            df: DataFrame to validate
            expected_schema: Expected PySpark StructType
            strict: If True, schemas must match exactly.
                    If False, df must contain at least the expected fields.

        Returns:
            Tuple of (is_valid: bool, missing_fields: list, extra_fields: list)
        """
        actual_fields = {f.name: f for f in df.schema.fields}
        expected_fields = {f.name: f for f in expected_schema.fields}

        missing = [
            name for name in expected_fields
            if name not in actual_fields
        ]
        extra = [
            name for name in actual_fields
            if name not in expected_fields
        ]

        if strict:
            is_valid = len(missing) == 0 and len(extra) == 0
        else:
            is_valid = len(missing) == 0

        if not is_valid:
            logger.warning(
                "Schema validation failed | missing=%s | extra=%s",
                missing, extra,
            )

        return is_valid, missing, extra

    # ══════════════════════════════════════════════════════════════════
    #  PARTITIONING & OPTIMIZATION
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def repartition_for_write(
        df: DataFrame,
        partition_cols: List[str],
        num_partitions: Optional[int] = None,
    ) -> DataFrame:
        """
        Repartition DataFrame for optimal Delta write performance.

        Args:
            df: Source DataFrame
            partition_cols: Columns to partition by
            num_partitions: Target number of partitions (auto if None)

        Returns:
            Repartitioned DataFrame
        """
        if num_partitions:
            result = df.repartition(num_partitions, *[F.col(c) for c in partition_cols])
        else:
            result = df.repartition(*[F.col(c) for c in partition_cols])

        logger.debug(
            "Repartitioned DataFrame | cols=%s | partitions=%d",
            partition_cols,
            result.rdd.getNumPartitions(),
        )
        return result

    @staticmethod
    def coalesce_small_files(
        df: DataFrame,
        target_partitions: int = 1,
    ) -> DataFrame:
        """
        Coalesce DataFrame to reduce small files (for small datasets).

        Args:
            df: Source DataFrame
            target_partitions: Target number of output partitions

        Returns:
            Coalesced DataFrame
        """
        return df.coalesce(target_partitions)

    @staticmethod
    def add_metadata_columns(
        df: DataFrame,
        batch_id: str,
        source_system: str = "opensky_api",
    ) -> DataFrame:
        """
        Add standard pipeline metadata columns to a DataFrame.

        Args:
            df: Source DataFrame
            batch_id: Unique batch identifier
            source_system: Source system identifier

        Returns:
            DataFrame with metadata columns added
        """
        return (
            df.withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn(
                "ingestion_date",
                F.date_format(F.current_timestamp(), "yyyy-MM-dd"),
            )
            .withColumn("batch_id", F.lit(batch_id))
            .withColumn("source_system", F.lit(source_system))
        )

    # ══════════════════════════════════════════════════════════════════
    #  CACHING
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def cache_with_logging(
        df: DataFrame, name: str = "unnamed"
    ) -> DataFrame:
        """
        Cache DataFrame with logging for debugging.

        Args:
            df: DataFrame to cache
            name: Descriptive name for logging

        Returns:
            Cached DataFrame
        """
        cached = df.cache()
        count = cached.count()  # Materialize cache
        logger.info(
            "Cached DataFrame [%s] | rows=%d | partitions=%d",
            name,
            count,
            cached.rdd.getNumPartitions(),
        )
        return cached

    @staticmethod
    def unpersist_with_logging(
        df: DataFrame, name: str = "unnamed"
    ) -> None:
        """Unpersist a cached DataFrame with logging."""
        df.unpersist()
        logger.info("Unpersisted DataFrame [%s]", name)

    # ══════════════════════════════════════════════════════════════════
    #  BROADCAST JOINS
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def broadcast_join(
        large_df: DataFrame,
        small_df: DataFrame,
        join_col: str,
        join_type: str = "left",
    ) -> DataFrame:
        """
        Perform a broadcast join for optimal performance when one
        DataFrame is small enough to fit in executor memory.

        Args:
            large_df: Large DataFrame (fact table)
            small_df: Small DataFrame (dimension table, < 10MB)
            join_col: Column to join on
            join_type: Join type (inner, left, right, outer)

        Returns:
            Joined DataFrame
        """
        logger.info("Performing broadcast join on column '%s'", join_col)
        return large_df.join(
            F.broadcast(small_df),
            on=join_col,
            how=join_type,
        )

    # ══════════════════════════════════════════════════════════════════
    #  DATA PROFILING
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def profile_dataframe(df: DataFrame) -> Dict[str, Any]:
        """
        Generate a quick data profile for a DataFrame.

        Args:
            df: DataFrame to profile

        Returns:
            Dict with profiling statistics
        """
        row_count = df.count()
        col_count = len(df.columns)

        # ── Null counts per column ─────────────────────────────────────
        null_counts = {}
        for col_name in df.columns:
            null_count = df.where(F.col(col_name).isNull()).count()
            null_counts[col_name] = {
                "null_count": null_count,
                "null_pct": round(null_count / max(row_count, 1) * 100, 2),
            }

        # ── Distinct counts for key columns ────────────────────────────
        distinct_counts = {}
        for col_name in df.columns[:10]:  # Limit to first 10 columns
            distinct_counts[col_name] = df.select(col_name).distinct().count()

        profile = {
            "row_count": row_count,
            "column_count": col_count,
            "partition_count": df.rdd.getNumPartitions(),
            "null_analysis": null_counts,
            "distinct_counts": distinct_counts,
        }

        logger.info("DataFrame profile | rows=%d | cols=%d", row_count, col_count)
        return profile

    # ══════════════════════════════════════════════════════════════════
    #  DEDUPLICATION
    # ══════════════════════════════════════════════════════════════════
    @staticmethod
    def deduplicate(
        df: DataFrame,
        key_columns: List[str],
        order_column: str,
        ascending: bool = False,
    ) -> DataFrame:
        """
        Deduplicate DataFrame keeping the latest (or earliest) record
        per key using window functions.

        Args:
            df: Source DataFrame
            key_columns: Columns forming the natural key
            order_column: Column to order by (e.g., timestamp)
            ascending: If True, keep earliest; if False, keep latest

        Returns:
            Deduplicated DataFrame
        """
        from pyspark.sql.window import Window

        window_spec = Window.partitionBy(
            *[F.col(c) for c in key_columns]
        ).orderBy(
            F.col(order_column).asc() if ascending else F.col(order_column).desc()
        )

        deduped = (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .where(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        logger.info(
            "Deduplicated on keys=%s | order=%s | direction=%s",
            key_columns,
            order_column,
            "asc" if ascending else "desc",
        )
        return deduped
