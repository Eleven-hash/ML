"""
=============================================================================
 Delta Lake Utilities — Flight Analytics Platform
=============================================================================
 Wrapper functions for Delta Lake operations including:
   - OPTIMIZE and ZORDER
   - VACUUM with safety checks
   - Time travel queries
   - MERGE (upsert) operations
   - Table management and metadata inspection
   - Schema evolution handling

 These utilities abstract away the Spark SQL commands and provide
 logging, error handling, and consistent interfaces for Delta operations.

 Usage:
   manager = DeltaTableManager(spark, config.delta)
   manager.optimize_table("bronze_flights")
   manager.vacuum_table("bronze_flights", retention_hours=168)
   df = manager.time_travel("bronze_flights", version=5)
=============================================================================
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("flight_analytics.delta_utils")


class DeltaTableManager:
    """
    Comprehensive Delta Lake table management.

    Provides high-level operations on Delta tables with logging,
    error handling, and best-practice defaults.
    """

    def __init__(self, spark: SparkSession, delta_config):
        """
        Initialize Delta table manager.

        Args:
            spark: Active SparkSession
            delta_config: DeltaLakeConfig with paths and settings
        """
        self.spark = spark
        self.config = delta_config
        logger.info("DeltaTableManager initialized | base_path=%s", delta_config.base_path)

    # ══════════════════════════════════════════════════════════════════
    #  WRITE OPERATIONS
    # ══════════════════════════════════════════════════════════════════
    def write_to_delta(
        self,
        df: DataFrame,
        path: str,
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        merge_schema: bool = True,
    ) -> None:
        """
        Write DataFrame to Delta table with best-practice settings.

        Args:
            df: DataFrame to write
            path: Delta table path
            mode: Write mode ('append', 'overwrite', 'error', 'ignore')
            partition_by: Columns to partition by
            merge_schema: Allow automatic schema evolution
        """
        try:
            writer = df.write.format("delta").mode(mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            if merge_schema:
                writer = writer.option("mergeSchema", "true")

            writer.save(path)

            logger.info(
                "Delta write | path=%s | mode=%s | partitions=%s",
                path, mode, partition_by,
            )

        except Exception as e:
            logger.error("Delta write failed | path=%s | error=%s", path, str(e))
            raise

    def merge_into(
        self,
        source_df: DataFrame,
        target_path: str,
        merge_keys: List[str],
        update_columns: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        Perform a Delta MERGE (upsert) operation.

        Matches source rows to target using merge_keys. Updates matched
        rows and inserts new rows.

        Args:
            source_df: Source DataFrame with new/updated records
            target_path: Path to target Delta table
            merge_keys: Columns to match on (e.g., ['icao24', 'time_position'])
            update_columns: Specific columns to update (None = update all)

        Returns:
            Dict with merge operation metrics
        """
        try:
            from delta.tables import DeltaTable

            # ── Check if target exists ─────────────────────────────────
            if not DeltaTable.isDeltaTable(self.spark, target_path):
                logger.info(
                    "Target table does not exist — performing initial write | path=%s",
                    target_path,
                )
                self.write_to_delta(source_df, target_path, mode="overwrite")
                return {"inserted": source_df.count(), "updated": 0}

            target_table = DeltaTable.forPath(self.spark, target_path)

            # ── Build merge condition ──────────────────────────────────
            condition = " AND ".join(
                [f"target.{k} = source.{k}" for k in merge_keys]
            )

            # ── Build update set ───────────────────────────────────────
            if update_columns:
                update_set = {
                    col: f"source.{col}" for col in update_columns
                }
            else:
                # Update all non-key columns
                update_set = {
                    col: f"source.{col}"
                    for col in source_df.columns
                    if col not in merge_keys
                }

            # ── Execute merge ──────────────────────────────────────────
            merge_builder = (
                target_table.alias("target")
                .merge(source_df.alias("source"), condition)
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsertAll()
            )

            merge_builder.execute()

            logger.info(
                "Delta MERGE completed | path=%s | keys=%s",
                target_path, merge_keys,
            )

            return {"status": "success", "merge_keys": merge_keys}

        except ImportError:
            logger.warning(
                "delta-spark package not available — falling back to append mode"
            )
            self.write_to_delta(source_df, target_path, mode="append")
            return {"status": "fallback_append"}

        except Exception as e:
            logger.error(
                "Delta MERGE failed | path=%s | error=%s",
                target_path, str(e),
            )
            raise

    # ══════════════════════════════════════════════════════════════════
    #  OPTIMIZATION
    # ══════════════════════════════════════════════════════════════════
    def optimize_table(
        self,
        path: str,
        z_order_columns: Optional[List[str]] = None,
    ) -> None:
        """
        Run OPTIMIZE on a Delta table, optionally with ZORDER.

        OPTIMIZE compacts small files into larger ones for better read
        performance. ZORDER co-locates related data for faster queries
        on specified columns.

        Args:
            path: Delta table path
            z_order_columns: Columns to ZORDER by
        """
        try:
            if z_order_columns:
                z_order_expr = ", ".join(z_order_columns)
                sql = f"OPTIMIZE delta.`{path}` ZORDER BY ({z_order_expr})"
                logger.info(
                    "Optimizing with ZORDER | path=%s | columns=%s",
                    path, z_order_columns,
                )
            else:
                sql = f"OPTIMIZE delta.`{path}`"
                logger.info("Optimizing table | path=%s", path)

            self.spark.sql(sql)
            logger.info("OPTIMIZE completed | path=%s", path)

        except Exception as e:
            logger.error(
                "OPTIMIZE failed | path=%s | error=%s", path, str(e)
            )
            raise

    def vacuum_table(
        self,
        path: str,
        retention_hours: Optional[int] = None,
    ) -> None:
        """
        VACUUM a Delta table to remove old data files.

        WARNING: After VACUUM, time travel to versions older than the
        retention period will no longer be possible.

        Args:
            path: Delta table path
            retention_hours: Retention period in hours (default from config)
        """
        retention = retention_hours or self.config.vacuum_retention_hours

        try:
            # ── Disable safety check for retention < 168 hours ─────────
            if retention < 168:
                self.spark.conf.set(
                    "spark.databricks.delta.retentionDurationCheck.enabled",
                    "false",
                )
                logger.warning(
                    "Retention %dh < 168h — disabled safety check", retention
                )

            sql = f"VACUUM delta.`{path}` RETAIN {retention} HOURS"
            self.spark.sql(sql)

            logger.info(
                "VACUUM completed | path=%s | retention=%dh",
                path, retention,
            )

        except Exception as e:
            logger.error(
                "VACUUM failed | path=%s | error=%s", path, str(e)
            )
            raise

    # ══════════════════════════════════════════════════════════════════
    #  TIME TRAVEL
    # ══════════════════════════════════════════════════════════════════
    def time_travel(
        self,
        path: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
    ) -> DataFrame:
        """
        Query a specific version of a Delta table using time travel.

        Args:
            path: Delta table path
            version: Specific version number
            timestamp: Timestamp string (e.g., '2024-01-15T10:30:00')

        Returns:
            DataFrame from the specified version/timestamp

        Raises:
            ValueError: If neither version nor timestamp is provided
        """
        if version is not None:
            df = (
                self.spark.read.format("delta")
                .option("versionAsOf", version)
                .load(path)
            )
            logger.info(
                "Time travel | path=%s | version=%d", path, version
            )

        elif timestamp is not None:
            df = (
                self.spark.read.format("delta")
                .option("timestampAsOf", timestamp)
                .load(path)
            )
            logger.info(
                "Time travel | path=%s | timestamp=%s", path, timestamp
            )

        else:
            raise ValueError(
                "Either 'version' or 'timestamp' must be provided"
            )

        return df

    # ══════════════════════════════════════════════════════════════════
    #  TABLE METADATA
    # ══════════════════════════════════════════════════════════════════
    def get_table_history(
        self, path: str, limit: int = 20
    ) -> DataFrame:
        """
        Get the operation history of a Delta table.

        Args:
            path: Delta table path
            limit: Maximum number of history entries

        Returns:
            DataFrame with table history
        """
        try:
            history = self.spark.sql(
                f"DESCRIBE HISTORY delta.`{path}` LIMIT {limit}"
            )
            logger.info(
                "Retrieved table history | path=%s | entries=%d",
                path, history.count(),
            )
            return history
        except Exception as e:
            logger.error(
                "Failed to get history | path=%s | error=%s", path, str(e)
            )
            raise

    def get_table_detail(self, path: str) -> DataFrame:
        """
        Get detailed metadata about a Delta table.

        Args:
            path: Delta table path

        Returns:
            DataFrame with table details (size, partitions, etc.)
        """
        return self.spark.sql(f"DESCRIBE DETAIL delta.`{path}`")

    def table_exists(self, path: str) -> bool:
        """
        Check if a Delta table exists at the given path.

        Args:
            path: Delta table path

        Returns:
            True if table exists
        """
        try:
            from delta.tables import DeltaTable
            return DeltaTable.isDeltaTable(self.spark, path)
        except ImportError:
            # Fallback: try to read
            try:
                self.spark.read.format("delta").load(path).limit(0)
                return True
            except Exception:
                return False

    # ══════════════════════════════════════════════════════════════════
    #  STREAMING
    # ══════════════════════════════════════════════════════════════════
    def read_stream(
        self,
        path: str,
        max_files_per_trigger: int = 100,
    ) -> DataFrame:
        """
        Create a streaming DataFrame from a Delta table.

        Args:
            path: Delta table path
            max_files_per_trigger: Max files to process per micro-batch

        Returns:
            Streaming DataFrame
        """
        return (
            self.spark.readStream.format("delta")
            .option("maxFilesPerTrigger", max_files_per_trigger)
            .load(path)
        )

    def write_stream(
        self,
        df: DataFrame,
        path: str,
        checkpoint_path: str,
        output_mode: str = "append",
        trigger_interval: str = "30 seconds",
        partition_by: Optional[List[str]] = None,
        query_name: Optional[str] = None,
    ):
        """
        Write a streaming DataFrame to a Delta table.

        Args:
            df: Streaming DataFrame
            path: Output Delta table path
            checkpoint_path: Checkpoint directory path
            output_mode: 'append', 'complete', or 'update'
            trigger_interval: Trigger interval (e.g., '30 seconds')
            partition_by: Partition columns
            query_name: Optional query name for monitoring

        Returns:
            StreamingQuery object
        """
        writer = (
            df.writeStream.format("delta")
            .outputMode(output_mode)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger_interval)
        )

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        if query_name:
            writer = writer.queryName(query_name)

        # ── Enable schema merge for streaming ──────────────────────────
        writer = writer.option("mergeSchema", "true")

        query = writer.start(path)

        logger.info(
            "Stream write started | path=%s | checkpoint=%s | "
            "mode=%s | trigger=%s | query=%s",
            path, checkpoint_path, output_mode, trigger_interval,
            query_name or query.id,
        )

        return query
