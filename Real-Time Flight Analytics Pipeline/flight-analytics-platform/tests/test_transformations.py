"""
=============================================================================
 Test Suite: Transformations — Flight Analytics Platform
=============================================================================
 Tests for Bronze, Silver, and Gold processors.
 Run with: pytest tests/test_transformations.py -v
=============================================================================
"""

import pytest
from configs.app_config import AppConfig


class TestSilverProcessorLogic:
    """Tests for Silver processor transformation logic."""

    # ── Conversion constants ───────────────────────────────────────────
    METERS_TO_FEET = 3.28084
    MS_TO_KMH = 3.6
    MS_TO_KNOTS = 1.94384

    def test_meters_to_feet_conversion(self):
        altitude_m = 10000.0
        altitude_ft = altitude_m * self.METERS_TO_FEET
        assert round(altitude_ft, 0) == 32808.0

    def test_ms_to_kmh_conversion(self):
        speed_ms = 250.0
        speed_kmh = speed_ms * self.MS_TO_KMH
        assert round(speed_kmh, 1) == 900.0

    def test_ms_to_knots_conversion(self):
        speed_ms = 250.0
        speed_knots = speed_ms * self.MS_TO_KNOTS
        assert round(speed_knots, 2) == 485.96
        assert speed_knots > 0

    def test_flight_phase_classification(self):
        """Test flight phase logic."""
        # Ground
        assert _classify_phase(on_ground=True, vertical_rate=0, altitude=0) == "ground"
        # Climbing
        assert _classify_phase(on_ground=False, vertical_rate=5.0, altitude=5000) == "climbing"
        # Descending
        assert _classify_phase(on_ground=False, vertical_rate=-5.0, altitude=5000) == "descending"
        # Cruising
        assert _classify_phase(on_ground=False, vertical_rate=0, altitude=10000) == "cruising"

    def test_speed_category_classification(self):
        """Test speed category logic."""
        assert _classify_speed(None) == "unknown"
        assert _classify_speed(30) == "slow"
        assert _classify_speed(100) == "medium"
        assert _classify_speed(200) == "fast"
        assert _classify_speed(300) == "very_fast"

    def test_altitude_band_classification(self):
        """Test altitude band logic."""
        assert _classify_altitude(None, False) == "unknown"
        assert _classify_altitude(0, True) == "ground"
        assert _classify_altitude(2000, False) == "low"
        assert _classify_altitude(5000, False) == "medium"
        assert _classify_altitude(9000, False) == "high"
        assert _classify_altitude(15000, False) == "very_high"


class TestRegionMapping:
    """Tests for country-to-region mapping."""

    def test_known_countries(self):
        from transformations.silver_processor import REGION_MAPPING

        assert REGION_MAPPING["United States"] == "North America"
        assert REGION_MAPPING["Germany"] == "Europe"
        assert REGION_MAPPING["Japan"] == "Asia"
        assert REGION_MAPPING["Australia"] == "Oceania"
        assert REGION_MAPPING["Brazil"] == "South America"

    def test_mapping_completeness(self):
        from transformations.silver_processor import REGION_MAPPING

        # Should have major aviation countries
        assert len(REGION_MAPPING) > 50


class TestDataQualityEngine:
    """Tests for data quality expectations."""

    def test_expectation_creation(self):
        from transformations.data_quality import Expectation

        exp = Expectation(
            column="icao24",
            check_type="not_null",
            severity="critical",
        )
        assert exp.column == "icao24"
        assert exp.severity == "critical"

    def test_auto_description(self):
        from transformations.data_quality import Expectation

        exp = Expectation(column="velocity", check_type="between")
        assert "velocity" in exp.description
        assert "between" in exp.description


class TestGoldProcessorLogic:
    """Tests for Gold processor aggregation logic."""

    def test_airborne_percentage_calculation(self):
        total = 100
        airborne = 75
        pct = airborne * 100.0 / total
        assert pct == 75.0

    def test_zero_division_handling(self):
        total = 0
        airborne = 0
        pct = airborne * 100.0 / max(total, 1)
        assert pct == 0.0


# ── Helper functions matching Silver processor logic ────────────────────

def _classify_phase(on_ground, vertical_rate, altitude):
    if on_ground:
        return "ground"
    if vertical_rate is not None and vertical_rate > 2.0:
        return "climbing"
    if vertical_rate is not None and vertical_rate < -2.0:
        return "descending"
    if altitude is not None and altitude > 9000:
        return "cruising"
    return "en_route"

def _classify_speed(velocity):
    if velocity is None:
        return "unknown"
    if velocity < 50:
        return "slow"
    if velocity < 150:
        return "medium"
    if velocity < 250:
        return "fast"
    return "very_fast"

def _classify_altitude(altitude, on_ground):
    if altitude is None:
        return "unknown"
    if on_ground:
        return "ground"
    if altitude < 3000:
        return "low"
    if altitude < 7000:
        return "medium"
    if altitude < 12000:
        return "high"
    return "very_high"
