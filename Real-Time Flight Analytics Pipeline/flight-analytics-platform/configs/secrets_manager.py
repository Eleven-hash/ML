"""
=============================================================================
 Secrets Manager — Flight Analytics Platform
=============================================================================
 Secure credential management abstraction that works across environments:
   - Databricks: Uses Databricks Secrets API (dbutils.secrets)
   - Local Dev: Falls back to environment variables
   - CI/CD: Supports .env files via python-dotenv

 Security Best Practices:
   - Never hardcode credentials in source files
   - Always use the SecretsManager abstraction
   - Rotate credentials regularly
   - Use least-privilege API keys

 Usage:
   secrets = SecretsManager()
   api_user = secrets.get_secret("opensky-username")
=============================================================================
"""

import os
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class SecretsManager:
    """
    Unified secrets management across Databricks and local environments.

    Resolution order:
      1. Databricks Secrets (if running on Databricks)
      2. Environment variables
      3. .env file (for local development)
    """

    # ── Default Databricks Secrets Scope ───────────────────────────────
    DEFAULT_SCOPE = "flight-analytics"

    # ── Secret Key Mapping ─────────────────────────────────────────────
    SECRET_KEYS = {
        "opensky-username": "OPENSKY_USERNAME",
        "opensky-password": "OPENSKY_PASSWORD",
        "kafka-bootstrap-servers": "KAFKA_BOOTSTRAP_SERVERS",
        "kafka-api-key": "KAFKA_API_KEY",
        "kafka-api-secret": "KAFKA_API_SECRET",
        "storage-account-key": "STORAGE_ACCOUNT_KEY",
        "notification-webhook": "NOTIFICATION_WEBHOOK_URL",
    }

    def __init__(self, scope: Optional[str] = None):
        """
        Initialize the secrets manager.

        Args:
            scope: Databricks secrets scope name. Defaults to 'flight-analytics'.
        """
        self.scope = scope or self.DEFAULT_SCOPE
        self._is_databricks = self._detect_databricks()
        self._cache: Dict[str, str] = {}

        if not self._is_databricks:
            self._load_dotenv()

        logger.info(
            "SecretsManager initialized | environment=%s | scope=%s",
            "databricks" if self._is_databricks else "local",
            self.scope,
        )

    @staticmethod
    def _detect_databricks() -> bool:
        """Detect if running within a Databricks environment."""
        try:
            # In Databricks, dbutils is available in the global namespace
            import IPython
            ip = IPython.get_ipython()
            if ip and hasattr(ip, "user_ns") and "dbutils" in ip.user_ns:
                return True
        except (ImportError, AttributeError):
            pass

        # Fallback: check Databricks-specific env vars
        return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

    @staticmethod
    def _load_dotenv() -> None:
        """Load .env file for local development."""
        try:
            from dotenv import load_dotenv
            load_dotenv()
            logger.debug("Loaded .env file for local development")
        except ImportError:
            logger.debug(
                "python-dotenv not installed — using system environment variables only"
            )

    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Retrieve a secret value by key.

        Args:
            key: Secret key name (e.g., 'opensky-username')
            default: Default value if secret is not found

        Returns:
            Secret value or default
        """
        # ── Check cache first ──────────────────────────────────────────
        if key in self._cache:
            return self._cache[key]

        value = None

        # ── Try Databricks Secrets ─────────────────────────────────────
        if self._is_databricks:
            value = self._get_databricks_secret(key)

        # ── Fallback to environment variable ───────────────────────────
        if value is None:
            env_key = self.SECRET_KEYS.get(key, key.upper().replace("-", "_"))
            value = os.getenv(env_key)

        # ── Use default ────────────────────────────────────────────────
        if value is None:
            if default is not None:
                logger.warning(
                    "Secret '%s' not found — using default value", key
                )
                return default
            logger.warning("Secret '%s' not found and no default provided", key)
            return None

        # ── Cache and return ───────────────────────────────────────────
        self._cache[key] = value
        logger.debug("Secret '%s' retrieved successfully", key)
        return value

    def _get_databricks_secret(self, key: str) -> Optional[str]:
        """
        Retrieve secret from Databricks Secrets API.

        Args:
            key: Secret key within the configured scope

        Returns:
            Secret value or None if not found
        """
        try:
            import IPython
            ip = IPython.get_ipython()
            if ip and hasattr(ip, "user_ns") and "dbutils" in ip.user_ns:
                dbutils = ip.user_ns["dbutils"]
                value = dbutils.secrets.get(scope=self.scope, key=key)
                logger.debug(
                    "Retrieved secret '%s' from Databricks scope '%s'",
                    key,
                    self.scope,
                )
                return value
        except Exception as e:
            logger.warning(
                "Failed to retrieve Databricks secret '%s': %s", key, str(e)
            )
        return None

    def get_opensky_credentials(self) -> tuple:
        """
        Convenience method to retrieve OpenSky API credentials.

        Returns:
            Tuple of (username, password) — either may be None
        """
        username = self.get_secret("opensky-username")
        password = self.get_secret("opensky-password")
        return username, password

    def get_kafka_config(self) -> Dict[str, Optional[str]]:
        """
        Convenience method to retrieve Kafka connection config.

        Returns:
            Dict with Kafka connection parameters
        """
        return {
            "bootstrap_servers": self.get_secret(
                "kafka-bootstrap-servers", "localhost:9092"
            ),
            "api_key": self.get_secret("kafka-api-key"),
            "api_secret": self.get_secret("kafka-api-secret"),
        }

    def clear_cache(self) -> None:
        """Clear the secrets cache (e.g., after credential rotation)."""
        self._cache.clear()
        logger.info("Secrets cache cleared")

    def __repr__(self) -> str:
        return (
            f"SecretsManager(scope='{self.scope}', "
            f"environment={'databricks' if self._is_databricks else 'local'}, "
            f"cached_keys={len(self._cache)})"
        )
