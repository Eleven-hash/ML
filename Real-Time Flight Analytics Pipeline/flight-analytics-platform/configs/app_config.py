"""
=============================================================================
 Application Configuration — Flight Analytics Platform
=============================================================================
 Central configuration hub for the entire platform. All configurable
 parameters are defined here to maintain a single source of truth.

 Design Decisions:
   - Frozen dataclass for immutability after initialization
   - Environment-aware (dev/staging/prod) with sensible defaults
   - All paths, intervals, and thresholds configurable via environment
   - Spark-optimized defaults tuned for Databricks Runtime 13.x+

 Usage:
   config = AppConfig.from_environment("production")
=============================================================================
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(frozen=False)
class APIConfig:
    """OpenSky Network API configuration."""

    # ── Endpoints ──────────────────────────────────────────────────────
    base_url: str = "https://opensky-network.org/api"
    states_endpoint: str = "/states/all"
    flights_endpoint: str = "/flights/all"
    arrivals_endpoint: str = "/flights/arrival"
    departures_endpoint: str = "/flights/departure"
    tracks_endpoint: str = "/tracks/all"

    # ── Authentication ─────────────────────────────────────────────────
    username: Optional[str] = None
    password: Optional[str] = None

    # ── Rate Limiting & Retry ──────────────────────────────────────────
    max_retries: int = 5
    retry_backoff_factor: float = 1.5
    retry_status_codes: tuple = (429, 500, 502, 503, 504)
    rate_limit_calls_per_minute: int = 10       # Unauthenticated: ~10/min
    rate_limit_calls_per_minute_auth: int = 40  # Authenticated: ~40/min
    request_timeout_seconds: int = 30
    poll_interval_seconds: int = 15  # OpenSky updates every ~10s

    @property
    def is_authenticated(self) -> bool:
        return self.username is not None and self.password is not None

    @property
    def effective_rate_limit(self) -> int:
        if self.is_authenticated:
            return self.rate_limit_calls_per_minute_auth
        return self.rate_limit_calls_per_minute


@dataclass(frozen=False)
class DeltaLakeConfig:
    """Delta Lake storage paths and optimization settings."""

    # ── Base Paths (DBFS / Cloud Storage) ──────────────────────────────
    base_path: str = "/mnt/flight-analytics"
    checkpoint_base: str = "/mnt/flight-analytics/checkpoints"

    # ── Medallion Architecture Paths ───────────────────────────────────
    @property
    def bronze_path(self) -> str:
        return f"{self.base_path}/bronze/flights"

    @property
    def silver_path(self) -> str:
        return f"{self.base_path}/silver/flights"

    @property
    def gold_path(self) -> str:
        return f"{self.base_path}/gold"

    @property
    def quarantine_path(self) -> str:
        return f"{self.base_path}/quarantine/flights"

    # ── Gold Sub-Paths ─────────────────────────────────────────────────
    @property
    def gold_flights_by_country_path(self) -> str:
        return f"{self.gold_path}/flights_by_country"

    @property
    def gold_traffic_summary_path(self) -> str:
        return f"{self.gold_path}/traffic_summary"

    @property
    def gold_speed_analysis_path(self) -> str:
        return f"{self.gold_path}/speed_analysis"

    @property
    def gold_altitude_trends_path(self) -> str:
        return f"{self.gold_path}/altitude_trends"

    @property
    def gold_anomalies_path(self) -> str:
        return f"{self.gold_path}/anomalies"

    @property
    def gold_kpi_path(self) -> str:
        return f"{self.gold_path}/kpis"

    # ── Checkpoint Paths ───────────────────────────────────────────────
    @property
    def bronze_checkpoint(self) -> str:
        return f"{self.checkpoint_base}/bronze"

    @property
    def silver_checkpoint(self) -> str:
        return f"{self.checkpoint_base}/silver"

    @property
    def gold_checkpoint(self) -> str:
        return f"{self.checkpoint_base}/gold"

    @property
    def streaming_checkpoint(self) -> str:
        return f"{self.checkpoint_base}/streaming"

    # ── Delta Optimization ─────────────────────────────────────────────
    optimize_interval_hours: int = 6
    vacuum_retention_hours: int = 168  # 7 days
    z_order_columns: list = field(
        default_factory=lambda: ["origin_country", "time_position"]
    )
    partition_columns: list = field(
        default_factory=lambda: ["ingestion_date", "origin_country"]
    )


@dataclass(frozen=False)
class SparkConfig:
    """Apache Spark runtime configuration."""

    app_name: str = "FlightAnalyticsPlatform"

    # ── Performance Tuning ─────────────────────────────────────────────
    shuffle_partitions: int = 200
    default_parallelism: int = 200
    broadcast_threshold_mb: int = 10
    adaptive_query_enabled: bool = True
    adaptive_coalesce_enabled: bool = True
    auto_broadcast_join_threshold: str = "10m"

    # ── Memory ─────────────────────────────────────────────────────────
    driver_memory: str = "4g"
    executor_memory: str = "8g"
    memory_fraction: float = 0.6
    memory_storage_fraction: float = 0.5

    # ── Streaming ──────────────────────────────────────────────────────
    streaming_trigger_interval: str = "30 seconds"
    streaming_max_files_per_trigger: int = 100
    watermark_delay: str = "10 minutes"

    # ── Delta-specific ─────────────────────────────────────────────────
    delta_schema_auto_merge: bool = True
    delta_optimize_write: bool = True
    delta_auto_compact: bool = True

    def as_spark_conf(self) -> Dict[str, str]:
        """Returns a dict of Spark configuration key-value pairs."""
        return {
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.default.parallelism": str(self.default_parallelism),
            "spark.sql.adaptive.enabled": str(self.adaptive_query_enabled).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(
                self.adaptive_coalesce_enabled
            ).lower(),
            "spark.sql.autoBroadcastJoinThreshold": self.auto_broadcast_join_threshold,
            "spark.driver.memory": self.driver_memory,
            "spark.executor.memory": self.executor_memory,
            "spark.memory.fraction": str(self.memory_fraction),
            "spark.memory.storageFraction": str(self.memory_storage_fraction),
            "spark.databricks.delta.schema.autoMerge.enabled": str(
                self.delta_schema_auto_merge
            ).lower(),
            "spark.databricks.delta.optimizeWrite.enabled": str(
                self.delta_optimize_write
            ).lower(),
            "spark.databricks.delta.autoCompact.enabled": str(
                self.delta_auto_compact
            ).lower(),
        }


@dataclass(frozen=False)
class KafkaConfig:
    """Apache Kafka configuration (optional integration)."""

    bootstrap_servers: str = "localhost:9092"
    topic_raw_flights: str = "flight-analytics.raw.flights"
    topic_processed_flights: str = "flight-analytics.processed.flights"
    topic_anomalies: str = "flight-analytics.anomalies"
    consumer_group: str = "flight-analytics-consumer-group"

    # ── Producer Settings ──────────────────────────────────────────────
    batch_size: int = 16384
    linger_ms: int = 10
    acks: str = "all"
    compression_type: str = "snappy"

    # ── Consumer Settings ──────────────────────────────────────────────
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500

    @property
    def producer_config(self) -> Dict[str, str]:
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "topic": self.topic_raw_flights,
        }

    @property
    def consumer_config(self) -> Dict[str, str]:
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": self.topic_raw_flights,
            "startingOffsets": self.auto_offset_reset,
        }


@dataclass(frozen=False)
class AnomalyDetectionConfig:
    """ML anomaly detection thresholds and model parameters."""

    # ── Altitude Anomalies ─────────────────────────────────────────────
    altitude_drop_threshold_ft: float = 5000.0    # Sudden drop > 5000 ft
    max_altitude_ft: float = 60000.0              # Above FL600
    min_altitude_ft: float = -500.0               # Below sea level (unusual)

    # ── Speed Anomalies ────────────────────────────────────────────────
    max_velocity_ms: float = 400.0                # ~Mach 1.2 in m/s
    min_velocity_ms: float = 0.0
    speed_change_threshold_ms: float = 100.0      # Sudden speed change

    # ── Position Anomalies ─────────────────────────────────────────────
    max_vertical_rate_fpm: float = 10000.0        # ft/min vertical rate
    position_deviation_threshold_km: float = 50.0  # Route deviation

    # ── Isolation Forest Parameters ────────────────────────────────────
    contamination_fraction: float = 0.05
    n_estimators: int = 100
    random_state: int = 42

    # ── Statistical Thresholds ─────────────────────────────────────────
    z_score_threshold: float = 3.0
    iqr_multiplier: float = 1.5


@dataclass(frozen=False)
class MonitoringConfig:
    """Monitoring, alerting, and logging configuration."""

    log_level: str = "INFO"
    log_format: str = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    log_date_format: str = "%Y-%m-%d %H:%M:%S"
    metrics_enabled: bool = True
    alert_email: Optional[str] = None
    health_check_interval_seconds: int = 60


class AppConfig:
    """
    Master configuration class that composes all sub-configurations.

    Usage:
        # Default development config
        config = AppConfig.from_environment("development")

        # Production config
        config = AppConfig.from_environment("production")

        # Access sub-configs
        config.api.base_url
        config.delta.bronze_path
        config.spark.as_spark_conf()
    """

    def __init__(
        self,
        environment: str = "development",
        api: Optional[APIConfig] = None,
        delta: Optional[DeltaLakeConfig] = None,
        spark: Optional[SparkConfig] = None,
        kafka: Optional[KafkaConfig] = None,
        anomaly: Optional[AnomalyDetectionConfig] = None,
        monitoring: Optional[MonitoringConfig] = None,
    ):
        self.environment = environment
        self.api = api or APIConfig()
        self.delta = delta or DeltaLakeConfig()
        self.spark = spark or SparkConfig()
        self.kafka = kafka or KafkaConfig()
        self.anomaly = anomaly or AnomalyDetectionConfig()
        self.monitoring = monitoring or MonitoringConfig()

    @classmethod
    def from_environment(cls, environment: str = "development") -> "AppConfig":
        """
        Factory method to create environment-specific configuration.

        Args:
            environment: One of 'development', 'staging', 'production'

        Returns:
            Fully initialized AppConfig instance
        """
        config = cls(environment=environment)

        # ── Load API credentials from environment ──────────────────────
        config.api.username = os.getenv("OPENSKY_USERNAME")
        config.api.password = os.getenv("OPENSKY_PASSWORD")

        if environment == "production":
            # Production overrides
            config.spark.shuffle_partitions = 400
            config.spark.default_parallelism = 400
            config.spark.executor_memory = "16g"
            config.spark.driver_memory = "8g"
            config.delta.vacuum_retention_hours = 720  # 30 days
            config.monitoring.log_level = "WARNING"
            config.monitoring.metrics_enabled = True

        elif environment == "staging":
            config.spark.shuffle_partitions = 200
            config.spark.executor_memory = "8g"
            config.monitoring.log_level = "INFO"

        else:  # development
            config.spark.shuffle_partitions = 8
            config.spark.default_parallelism = 8
            config.spark.executor_memory = "4g"
            config.spark.driver_memory = "2g"
            config.delta.vacuum_retention_hours = 24
            config.monitoring.log_level = "DEBUG"

        return config

    def __repr__(self) -> str:
        return (
            f"AppConfig(environment='{self.environment}', "
            f"api_authenticated={self.api.is_authenticated})"
        )
