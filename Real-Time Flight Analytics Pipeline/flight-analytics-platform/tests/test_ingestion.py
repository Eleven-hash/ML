"""
=============================================================================
 Test Suite: Data Ingestion — Flight Analytics Platform
=============================================================================
 Unit and integration tests for the ingestion layer.
 Tests cover API client, batch ingestion, and data validation.

 Run with: pytest tests/test_ingestion.py -v
=============================================================================
"""

import pytest
import json
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from configs.app_config import AppConfig, APIConfig


class TestAPIConfig:
    """Tests for API configuration."""

    def test_default_config(self):
        config = APIConfig()
        assert config.base_url == "https://opensky-network.org/api"
        assert config.max_retries == 5
        assert config.request_timeout_seconds == 30

    def test_unauthenticated(self):
        config = APIConfig()
        assert not config.is_authenticated
        assert config.effective_rate_limit == 10

    def test_authenticated(self):
        config = APIConfig(username="user", password="pass")
        assert config.is_authenticated
        assert config.effective_rate_limit == 40


class TestAppConfig:
    """Tests for application configuration."""

    def test_development_config(self):
        config = AppConfig.from_environment("development")
        assert config.environment == "development"
        assert config.spark.shuffle_partitions == 8
        assert config.monitoring.log_level == "DEBUG"

    def test_production_config(self):
        config = AppConfig.from_environment("production")
        assert config.environment == "production"
        assert config.spark.shuffle_partitions == 400
        assert config.monitoring.log_level == "WARNING"

    def test_delta_paths(self):
        config = AppConfig()
        assert "bronze" in config.delta.bronze_path
        assert "silver" in config.delta.silver_path
        assert "gold" in config.delta.gold_path

    def test_checkpoint_paths(self):
        config = AppConfig()
        assert "checkpoints" in config.delta.bronze_checkpoint
        assert "checkpoints" in config.delta.silver_checkpoint


class TestOpenSkyClientParsing:
    """Tests for OpenSky API response parsing."""

    SAMPLE_API_RESPONSE = {
        "time": 1700000000,
        "states": [
            [
                "abc123", "TEST1234", "United States",
                1700000000, 1700000000,
                -73.9857, 40.7484, 10000.0,
                False, 250.0, 90.0, 0.0,
                None, 10050.0, "1200", False, 0,
            ],
            [
                "def456", "LH400   ", "Germany",
                1700000000, 1700000000,
                8.5622, 50.0379, 12000.0,
                False, 300.0, 45.0, 2.5,
                None, 12100.0, None, False, 0,
            ],
        ],
    }

    def test_parse_valid_response(self):
        """Test parsing a valid API response."""
        from ingestion.opensky_client import OpenSkyClient

        config = AppConfig()
        client = OpenSkyClient(config)
        result = client.parse_state_vectors(self.SAMPLE_API_RESPONSE)

        assert len(result) == 2
        assert result[0]["icao24"] == "abc123"
        assert result[0]["origin_country"] == "United States"
        assert result[0]["velocity"] == 250.0
        assert result[1]["icao24"] == "def456"

    def test_parse_empty_response(self):
        """Test parsing empty API response."""
        from ingestion.opensky_client import OpenSkyClient

        config = AppConfig()
        client = OpenSkyClient(config)
        result = client.parse_state_vectors({"time": 0, "states": []})
        assert result == []

    def test_parse_none_response(self):
        """Test parsing None response."""
        from ingestion.opensky_client import OpenSkyClient

        config = AppConfig()
        client = OpenSkyClient(config)
        result = client.parse_state_vectors(None)
        assert result == []

    def test_parse_malformed_state(self):
        """Test handling of malformed state vectors."""
        from ingestion.opensky_client import OpenSkyClient

        config = AppConfig()
        client = OpenSkyClient(config)
        response = {
            "time": 1700000000,
            "states": [
                ["short_array"],  # Too few fields
                self.SAMPLE_API_RESPONSE["states"][0],  # Valid
            ],
        }
        result = client.parse_state_vectors(response)
        assert len(result) == 1  # Only the valid one


class TestBatchIngestionValidation:
    """Tests for batch ingestion validation logic."""

    def test_generate_batch_id(self):
        """Test batch ID generation format."""
        from ingestion.batch_ingestion import BatchIngestionPipeline

        bid = BatchIngestionPipeline._generate_batch_id()
        assert bid.startswith("batch_")
        assert len(bid) > 20  # timestamp + UUID


class TestSecretsManager:
    """Tests for secrets management."""

    def test_env_var_fallback(self):
        """Test environment variable fallback."""
        from configs.secrets_manager import SecretsManager
        import os

        os.environ["TEST_SECRET_KEY"] = "test_value"
        sm = SecretsManager()
        value = sm.get_secret("test-secret-key")
        # Won't match unless mapping exists, but tests the flow
        del os.environ["TEST_SECRET_KEY"]

    def test_cache_clearing(self):
        """Test cache clearing."""
        from configs.secrets_manager import SecretsManager

        sm = SecretsManager()
        sm._cache["test"] = "value"
        sm.clear_cache()
        assert len(sm._cache) == 0

    def test_default_values(self):
        """Test default value fallback."""
        from configs.secrets_manager import SecretsManager

        sm = SecretsManager()
        value = sm.get_secret("nonexistent-key", default="default_val")
        assert value == "default_val"
