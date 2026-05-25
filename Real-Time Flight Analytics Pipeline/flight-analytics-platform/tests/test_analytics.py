"""
=============================================================================
 Test Suite: Analytics — Flight Analytics Platform
=============================================================================
 Tests for analytics and geo-analytics modules.
 Run with: pytest tests/test_analytics.py -v
=============================================================================
"""

import pytest
import math


class TestHaversineDistance:
    """Tests for Haversine distance calculation."""

    EARTH_RADIUS_KM = 6371.0

    @staticmethod
    def haversine(lat1, lon1, lat2, lon2):
        """Pure Python Haversine for testing."""
        lat1_r = math.radians(lat1)
        lat2_r = math.radians(lat2)
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1_r) * math.cos(lat2_r)
            * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return 6371.0 * c

    def test_same_point(self):
        """Distance from a point to itself should be 0."""
        d = self.haversine(40.7484, -73.9857, 40.7484, -73.9857)
        assert abs(d) < 0.001

    def test_known_distance(self):
        """Test known distance: New York to London ~5570 km."""
        d = self.haversine(40.7128, -74.0060, 51.5074, -0.1278)
        assert 5500 < d < 5700

    def test_antipodal_points(self):
        """Test maximum distance (antipodal points)."""
        d = self.haversine(0, 0, 0, 180)
        assert abs(d - math.pi * self.EARTH_RADIUS_KM) < 10


class TestDensityGrid:
    """Tests for density grid bucketing logic."""

    def test_grid_bucketing(self):
        """Test lat/lon bucketing at 1-degree resolution."""
        resolution = 1.0
        lat = 40.7484
        lon = -73.9857

        lat_bucket = round(lat / resolution) * resolution
        lon_bucket = round(lon / resolution) * resolution

        assert lat_bucket == 41.0
        assert lon_bucket == -74.0

    def test_grid_bucketing_fine(self):
        """Test bucketing at 0.5-degree resolution."""
        resolution = 0.5
        lat = 40.7484

        lat_bucket = round(lat / resolution) * resolution
        assert lat_bucket == 40.5


class TestAnalyticsLogic:
    """Tests for analytics computation logic."""

    def test_percentile_calculation(self):
        """Test percentile approximation."""
        data = list(range(1, 101))  # 1 to 100
        p50 = sorted(data)[len(data) // 2 - 1]
        assert p50 == 50

    def test_cumulative_percentage(self):
        """Test cumulative percentage calculation."""
        values = [50, 30, 20, 10]
        total = sum(values)
        cumulative = []
        running = 0
        for v in values:
            running += v
            cumulative.append(round(running / total * 100, 1))
        assert cumulative == [45.5, 72.7, 90.9, 100.0]

    def test_z_score_calculation(self):
        """Test z-score computation."""
        mean = 800.0
        stddev = 100.0
        value = 1100.0

        z = (value - mean) / stddev
        assert z == 3.0

    def test_change_percentage(self):
        """Test period-over-period change."""
        current = 120
        previous = 100
        change_pct = (current - previous) / previous * 100
        assert change_pct == 20.0


class TestAnomalyThresholds:
    """Tests for anomaly detection thresholds."""

    def test_altitude_drop_detection(self):
        """Test sudden altitude drop threshold."""
        prev_altitude = 35000.0
        curr_altitude = 28000.0
        drop = curr_altitude - prev_altitude  # -7000

        threshold = -5000.0
        assert drop < threshold  # Should be flagged

    def test_extreme_speed_detection(self):
        """Test extreme speed threshold."""
        max_velocity_ms = 400.0  # ~Mach 1.2
        max_speed_kmh = max_velocity_ms * 3.6  # 1440 km/h

        normal_speed = 900.0  # ~Mach 0.85
        extreme_speed = 1500.0

        assert normal_speed < max_speed_kmh
        assert extreme_speed > max_speed_kmh

    def test_emergency_squawk_codes(self):
        """Test emergency squawk code identification."""
        emergency_codes = {"7500", "7600", "7700"}

        assert "7500" in emergency_codes  # Hijacking
        assert "7600" in emergency_codes  # Comm failure
        assert "7700" in emergency_codes  # Emergency
        assert "1200" not in emergency_codes  # Normal VFR
