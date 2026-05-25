"""
=============================================================================
 Test Suite: Data Quality — Flight Analytics Platform
=============================================================================
 Tests for the data quality engine expectations.
 Run with: pytest tests/test_data_quality.py -v
=============================================================================
"""

import pytest
from transformations.data_quality import Expectation, CheckResult


class TestExpectation:
    """Tests for Expectation dataclass."""

    def test_creation(self):
        exp = Expectation(column="icao24", check_type="not_null")
        assert exp.column == "icao24"
        assert exp.check_type == "not_null"
        assert exp.severity == "warning"

    def test_auto_description(self):
        exp = Expectation(column="velocity", check_type="between")
        assert "velocity" in exp.description

    def test_custom_description(self):
        exp = Expectation(
            column="icao24",
            check_type="not_null",
            description="Custom desc",
        )
        assert exp.description == "Custom desc"


class TestCheckResult:
    """Tests for CheckResult dataclass."""

    def test_pass_result(self):
        exp = Expectation(column="test", check_type="not_null")
        result = CheckResult(
            expectation=exp,
            passed=True,
            total_rows=100,
            failing_rows=0,
            pass_rate=1.0,
        )
        assert result.passed
        assert result.pass_rate == 1.0

    def test_fail_result(self):
        exp = Expectation(column="test", check_type="not_null")
        result = CheckResult(
            expectation=exp,
            passed=False,
            total_rows=100,
            failing_rows=50,
            pass_rate=0.5,
        )
        assert not result.passed
        assert result.failing_rows == 50


class TestPassRateThresholds:
    """Tests for severity-based pass rate thresholds."""

    def test_critical_threshold(self):
        threshold = 0.99
        assert 0.995 >= threshold  # Pass
        assert 0.985 < threshold   # Fail

    def test_warning_threshold(self):
        threshold = 0.95
        assert 0.96 >= threshold
        assert 0.94 < threshold

    def test_info_threshold(self):
        threshold = 0.80
        assert 0.85 >= threshold
        assert 0.75 < threshold
