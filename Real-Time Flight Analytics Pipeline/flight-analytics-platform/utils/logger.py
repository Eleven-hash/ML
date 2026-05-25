"""
=============================================================================
 Structured Logger — Flight Analytics Platform
=============================================================================
 Production-grade logging with:
   - Correlation IDs for distributed trace tracking
   - Structured JSON output for log aggregation (ELK/Splunk)
   - Log level management per environment
   - Performance metrics logging
   - Pipeline stage tracking

 Usage:
   logger = FlightLogger.get_logger("ingestion.opensky_client")
   logger.info("Fetched %d records", count, extra={"batch_id": bid})
=============================================================================
"""

import logging
import json
import uuid
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from functools import wraps


class StructuredFormatter(logging.Formatter):
    """
    JSON-structured log formatter for production log aggregation.

    Output format:
    {
        "timestamp": "2024-01-15T10:30:00Z",
        "level": "INFO",
        "logger": "ingestion.opensky_client",
        "message": "Fetched 1500 records",
        "correlation_id": "abc-123",
        "extra": {...}
    }
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # ── Add correlation ID if present ──────────────────────────────
        if hasattr(record, "correlation_id"):
            log_entry["correlation_id"] = record.correlation_id

        # ── Add batch ID if present ────────────────────────────────────
        if hasattr(record, "batch_id"):
            log_entry["batch_id"] = record.batch_id

        # ── Add any custom extra fields ────────────────────────────────
        standard_attrs = {
            "name", "msg", "args", "created", "relativeCreated",
            "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "filename", "module", "pathname", "thread", "threadName",
            "process", "processName", "levelname", "levelno", "message",
            "msecs", "taskName",
        }
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in standard_attrs and not k.startswith("_")
        }
        if extras:
            log_entry["extra"] = extras

        # ── Add exception info ─────────────────────────────────────────
        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
            }

        return json.dumps(log_entry, default=str)


class ConsoleFormatter(logging.Formatter):
    """
    Human-readable console formatter with color coding.
    Used in development environments for readability.
    """

    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ── Build the formatted line ───────────────────────────────────
        prefix = (
            f"{color}{timestamp} | {record.levelname:8s}{self.RESET} | "
            f"{record.name:40s} | "
        )
        message = record.getMessage()

        # ── Append correlation ID if available ─────────────────────────
        if hasattr(record, "correlation_id"):
            message += f" [corr_id={record.correlation_id}]"

        return f"{prefix}{message}"


class FlightLogger:
    """
    Factory class for creating consistently configured loggers.

    Usage:
        # Get a logger for a specific module
        logger = FlightLogger.get_logger("ingestion.opensky_client")

        # Log with correlation ID
        FlightLogger.set_correlation_id("batch-001")
        logger.info("Processing batch")

        # Performance logging
        with FlightLogger.timer(logger, "api_fetch"):
            # ... expensive operation ...
            pass
    """

    _correlation_id: Optional[str] = None
    _initialized: bool = False
    _log_level: str = "INFO"

    @classmethod
    def initialize(
        cls,
        level: str = "INFO",
        use_json: bool = False,
        log_file: Optional[str] = None,
    ) -> None:
        """
        Initialize the logging system. Call once at application startup.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            use_json: Use JSON structured output (for production)
            log_file: Optional file path for log output
        """
        if cls._initialized:
            return

        cls._log_level = level
        root_logger = logging.getLogger("flight_analytics")
        root_logger.setLevel(getattr(logging, level.upper()))

        # ── Clear existing handlers ────────────────────────────────────
        root_logger.handlers.clear()

        # ── Console handler ────────────────────────────────────────────
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, level.upper()))

        if use_json:
            console_handler.setFormatter(StructuredFormatter())
        else:
            console_handler.setFormatter(ConsoleFormatter())

        root_logger.addHandler(console_handler)

        # ── File handler (optional) ────────────────────────────────────
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(getattr(logging, level.upper()))
            file_handler.setFormatter(StructuredFormatter())  # Always JSON for files
            root_logger.addHandler(file_handler)

        cls._initialized = True
        root_logger.info(
            "Logging initialized | level=%s | json=%s | file=%s",
            level, use_json, log_file,
        )

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get a named logger under the flight_analytics namespace.

        Args:
            name: Logger name (e.g., 'ingestion.opensky_client')

        Returns:
            Configured Logger instance
        """
        if not cls._initialized:
            cls.initialize()
        return logging.getLogger(f"flight_analytics.{name}")

    @classmethod
    def set_correlation_id(cls, correlation_id: Optional[str] = None) -> str:
        """
        Set or generate a correlation ID for distributed tracing.

        Args:
            correlation_id: Explicit ID or None to auto-generate

        Returns:
            The active correlation ID
        """
        cls._correlation_id = correlation_id or str(uuid.uuid4())[:12]
        return cls._correlation_id

    @classmethod
    def get_correlation_id(cls) -> Optional[str]:
        """Get the current correlation ID."""
        return cls._correlation_id

    @classmethod
    def timer(cls, logger: logging.Logger, operation_name: str):
        """
        Context manager for timing operations.

        Usage:
            with FlightLogger.timer(logger, "bronze_write"):
                df.write.format("delta").save(path)

        Args:
            logger: Logger instance
            operation_name: Name of the operation being timed
        """
        return _TimerContext(logger, operation_name)

    @staticmethod
    def log_dataframe_stats(
        logger: logging.Logger,
        df,
        name: str,
        show_schema: bool = False,
    ) -> None:
        """
        Log DataFrame statistics for pipeline observability.

        Args:
            logger: Logger instance
            df: PySpark DataFrame
            name: Descriptive name for the DataFrame
            show_schema: Whether to log the schema
        """
        try:
            count = df.count()
            num_cols = len(df.columns)
            num_partitions = df.rdd.getNumPartitions()

            logger.info(
                "DataFrame [%s] | rows=%d | cols=%d | partitions=%d",
                name, count, num_cols, num_partitions,
            )

            if show_schema:
                schema_str = df._jdf.schema().treeString()
                logger.debug("Schema [%s]:\n%s", name, schema_str)

        except Exception as e:
            logger.warning(
                "Could not log DataFrame stats for [%s]: %s", name, str(e)
            )


class _TimerContext:
    """Internal context manager for operation timing."""

    def __init__(self, logger: logging.Logger, operation_name: str):
        self.logger = logger
        self.operation_name = operation_name
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        self.logger.info("Started: %s", self.operation_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time

        if exc_type:
            self.logger.error(
                "Failed: %s | elapsed=%.2fs | error=%s",
                self.operation_name, elapsed, str(exc_val),
            )
        else:
            self.logger.info(
                "Completed: %s | elapsed=%.2fs",
                self.operation_name, elapsed,
            )

        return False  # Don't suppress exceptions


def log_execution(logger: logging.Logger):
    """
    Decorator to log function entry, exit, and execution time.

    Usage:
        @log_execution(logger)
        def process_batch(batch_id):
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            logger.info("Entering: %s", func_name)
            start = time.time()

            try:
                result = func(*args, **kwargs)
                elapsed = time.time() - start
                logger.info(
                    "Exiting: %s | elapsed=%.2fs | status=SUCCESS",
                    func_name, elapsed,
                )
                return result
            except Exception as e:
                elapsed = time.time() - start
                logger.error(
                    "Exiting: %s | elapsed=%.2fs | status=FAILED | error=%s",
                    func_name, elapsed, str(e),
                )
                raise

        return wrapper
    return decorator
