"""
ML package for Flight Analytics Platform.

Provides machine learning components:
- FeatureEngineer: Feature extraction from flight data
- AnomalyDetector: Multi-strategy anomaly detection
"""

from ml.feature_engineering import FeatureEngineer
from ml.anomaly_detector import AnomalyDetector

__all__ = ["FeatureEngineer", "AnomalyDetector"]
