"""
Analytics package for Flight Analytics Platform.

Provides analytical query modules:
- FlightAnalytics: Core flight analytics with window functions
- GeoAnalytics: Geospatial analytics and density analysis
"""

from analytics.flight_analytics import FlightAnalytics
from analytics.geo_analytics import GeoAnalytics

__all__ = ["FlightAnalytics", "GeoAnalytics"]
