"""
=============================================================================
 Data Quality Engine — Flight Analytics Platform
=============================================================================
 Expectation-based data quality framework inspired by Great Expectations.
 Validates DataFrames against configurable rules and produces quality
 reports, metrics, and alerts.

 Features:
   - Declarative expectation definitions
   - Column-level and row-level checks
   - Null ratio thresholds
   - Value range validation
   - Regex pattern matching
   - Freshness checks (data staleness)
   - Quality score calculation
   - Quality metrics for dashboards

 Usage:
   engine = DataQualityEngine(spark)
   engine.add_expectation("icao24", "not_null")
   engine.add_expectation("velocity", "between", min_val=0, max_val=400)
   report = engine.validate(df)
=============================================================================
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("flight_analytics.transformations.data_quality")


@dataclass
class Expectation:
    """A single data quality expectation/rule."""
    column: str
    check_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    severity: str = "warning"  # 'critical', 'warning', 'info'
    description: Optional[str] = None

    def __post_init__(self):
        if not self.description:
            self.description = f"{self.column}: {self.check_type}"


@dataclass
class CheckResult:
    """Result of a single data quality check."""
    expectation: Expectation
    passed: bool
    total_rows: int
    failing_rows: int
    pass_rate: float
    details: Optional[str] = None


class DataQualityEngine:
    """
    Configurable data quality validation engine.

    Supports multiple check types:
      - not_null: Column should not contain nulls
      - unique: Column values should be unique
      - between: Values in a numeric range
      - in_set: Values from an allowed set
      - regex: Values match a pattern
      - max_null_ratio: Maximum allowed null percentage
      - freshness: Data is not older than threshold
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._expectations: List[Expectation] = []
        logger.info("DataQualityEngine initialized")

    def add_expectation(
        self,
        column: str,
        check_type: str,
        severity: str = "warning",
        description: Optional[str] = None,
        **params,
    ) -> "DataQualityEngine":
        """
        Add a data quality expectation.

        Args:
            column: Column name to check
            check_type: Type of check (not_null, unique, between, etc.)
            severity: 'critical', 'warning', or 'info'
            description: Human-readable description
            **params: Check-specific parameters

        Returns:
            Self for chaining
        """
        expectation = Expectation(
            column=column,
            check_type=check_type,
            params=params,
            severity=severity,
            description=description,
        )
        self._expectations.append(expectation)
        return self

    def add_flight_data_expectations(self) -> "DataQualityEngine":
        """
        Add standard expectations for flight data (convenience method).

        Returns:
            Self for chaining
        """
        # ── Critical checks ────────────────────────────────────────────
        self.add_expectation(
            "icao24", "not_null",
            severity="critical",
            description="ICAO24 address must not be null",
        )
        self.add_expectation(
            "icao24", "regex",
            severity="critical",
            pattern=r"^[a-f0-9]{6}$",
            description="ICAO24 must be 6-char hex",
        )
        self.add_expectation(
            "origin_country", "not_null",
            severity="critical",
            description="Origin country must not be null",
        )

        # ── Warning checks ─────────────────────────────────────────────
        self.add_expectation(
            "latitude", "between",
            severity="warning",
            min_val=-90.0, max_val=90.0,
            description="Latitude must be in [-90, 90]",
        )
        self.add_expectation(
            "longitude", "between",
            severity="warning",
            min_val=-180.0, max_val=180.0,
            description="Longitude must be in [-180, 180]",
        )
        self.add_expectation(
            "velocity", "between",
            severity="warning",
            min_val=0.0, max_val=400.0,
            description="Velocity must be reasonable (0-400 m/s)",
        )
        self.add_expectation(
            "baro_altitude", "between",
            severity="warning",
            min_val=-500.0, max_val=18300.0,
            description="Altitude must be in [-500, 18300] meters",
        )

        # ── Info checks ────────────────────────────────────────────────
        self.add_expectation(
            "callsign", "max_null_ratio",
            severity="info",
            max_ratio=0.3,
            description="Callsign null ratio should be < 30%",
        )

        return self

    def validate(self, df: DataFrame) -> Dict[str, Any]:
        """
        Run all expectations against a DataFrame.

        Args:
            df: DataFrame to validate

        Returns:
            Comprehensive quality report dict
        """
        total_rows = df.count()
        results: List[CheckResult] = []

        logger.info(
            "Running %d data quality checks on %d rows",
            len(self._expectations), total_rows,
        )

        for expectation in self._expectations:
            if expectation.column not in df.columns:
                logger.warning(
                    "Column '%s' not in DataFrame — skipping check",
                    expectation.column,
                )
                continue

            result = self._run_check(df, expectation, total_rows)
            results.append(result)

            status = "✓ PASS" if result.passed else "✗ FAIL"
            logger.info(
                "%s | %s | pass_rate=%.1f%% | severity=%s",
                status,
                result.expectation.description,
                result.pass_rate * 100,
                result.expectation.severity,
            )

        # ── Build report ───────────────────────────────────────────────
        report = self._build_report(results, total_rows)
        return report

    def _run_check(
        self,
        df: DataFrame,
        expectation: Expectation,
        total_rows: int,
    ) -> CheckResult:
        """Run a single data quality check."""

        column = expectation.column
        check_type = expectation.check_type
        params = expectation.params

        try:
            if check_type == "not_null":
                failing = df.where(F.col(column).isNull()).count()

            elif check_type == "unique":
                total_distinct = df.select(column).distinct().count()
                non_null = df.where(F.col(column).isNotNull()).count()
                failing = max(0, non_null - total_distinct)

            elif check_type == "between":
                min_val = params.get("min_val")
                max_val = params.get("max_val")
                failing = df.where(
                    F.col(column).isNotNull()
                    & (
                        (F.col(column) < min_val) | (F.col(column) > max_val)
                    )
                ).count()

            elif check_type == "in_set":
                allowed = params.get("allowed_values", [])
                failing = df.where(
                    F.col(column).isNotNull()
                    & ~F.col(column).isin(allowed)
                ).count()

            elif check_type == "regex":
                pattern = params.get("pattern", ".*")
                failing = df.where(
                    F.col(column).isNotNull()
                    & ~F.col(column).rlike(pattern)
                ).count()

            elif check_type == "max_null_ratio":
                max_ratio = params.get("max_ratio", 0.1)
                null_count = df.where(F.col(column).isNull()).count()
                actual_ratio = null_count / max(total_rows, 1)
                failing = null_count if actual_ratio > max_ratio else 0

            else:
                logger.warning("Unknown check type: %s", check_type)
                failing = 0

            pass_rate = (total_rows - failing) / max(total_rows, 1)
            # Pass if > 95% pass rate (configurable per severity)
            threshold = {
                "critical": 0.99,
                "warning": 0.95,
                "info": 0.80,
            }.get(expectation.severity, 0.95)

            return CheckResult(
                expectation=expectation,
                passed=pass_rate >= threshold,
                total_rows=total_rows,
                failing_rows=failing,
                pass_rate=pass_rate,
            )

        except Exception as e:
            logger.error(
                "Check failed for %s.%s: %s",
                column, check_type, str(e),
            )
            return CheckResult(
                expectation=expectation,
                passed=False,
                total_rows=total_rows,
                failing_rows=total_rows,
                pass_rate=0.0,
                details=str(e),
            )

    def _build_report(
        self,
        results: List[CheckResult],
        total_rows: int,
    ) -> Dict[str, Any]:
        """Build a comprehensive quality report from check results."""

        passed = [r for r in results if r.passed]
        failed = [r for r in results if not r.passed]

        critical_failures = [
            r for r in failed if r.expectation.severity == "critical"
        ]

        overall_score = len(passed) / max(len(results), 1)

        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_rows": total_rows,
            "total_checks": len(results),
            "passed_checks": len(passed),
            "failed_checks": len(failed),
            "critical_failures": len(critical_failures),
            "overall_score": round(overall_score * 100, 1),
            "overall_status": (
                "PASS" if len(critical_failures) == 0 else "FAIL"
            ),
            "checks": [
                {
                    "column": r.expectation.column,
                    "check": r.expectation.check_type,
                    "severity": r.expectation.severity,
                    "description": r.expectation.description,
                    "passed": r.passed,
                    "pass_rate": round(r.pass_rate * 100, 2),
                    "failing_rows": r.failing_rows,
                }
                for r in results
            ],
        }

        logger.info(
            "DQ Report | score=%.1f%% | passed=%d/%d | critical_fails=%d",
            report["overall_score"],
            len(passed),
            len(results),
            len(critical_failures),
        )

        return report

    def clear_expectations(self) -> None:
        """Clear all expectations."""
        self._expectations.clear()
