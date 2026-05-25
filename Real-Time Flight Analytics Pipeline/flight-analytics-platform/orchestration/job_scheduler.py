"""
=============================================================================
 Job Scheduler — Flight Analytics Platform
=============================================================================
 Databricks job scheduling configuration and helpers.

 Provides:
   - Job definition templates for Databricks Workflows
   - Schedule configuration (cron expressions)
   - Cluster configuration recommendations
   - Multi-task pipeline definitions

 Usage:
   scheduler = JobScheduler(config)
   job_config = scheduler.create_pipeline_job()
=============================================================================
"""

import json
import logging
from typing import Dict, Any, List, Optional

from configs.app_config import AppConfig

logger = logging.getLogger("flight_analytics.orchestration.scheduler")


class JobScheduler:
    """
    Databricks job scheduling configuration generator.

    Creates JSON job definitions compatible with Databricks
    Workflows API (Jobs API 2.1).
    """

    def __init__(self, config: AppConfig):
        self.config = config
        logger.info("JobScheduler initialized")

    def create_pipeline_job(
        self,
        job_name: str = "flight-analytics-pipeline",
        schedule_cron: str = "0 */5 * * * ?",
        repo_path: str = "/Repos/<username>/flight-analytics-platform",
    ) -> Dict[str, Any]:
        """
        Create a multi-task Databricks Workflow job definition.

        The pipeline runs as a DAG:
          ingestion → bronze_to_silver → silver_to_gold → anomaly_detection

        Args:
            job_name: Name of the Databricks job
            schedule_cron: Cron expression (default: every 5 minutes)
            repo_path: Path to Databricks Repo

        Returns:
            Job definition dict (compatible with Jobs API 2.1)
        """
        job_config = {
            "name": job_name,
            "description": (
                "Real-Time Flight Analytics Pipeline - "
                "End-to-end Medallion Architecture"
            ),
            "schedule": {
                "quartz_cron_expression": schedule_cron,
                "timezone_id": "UTC",
                "pause_status": "UNPAUSED",
            },
            "max_concurrent_runs": 1,
            "timeout_seconds": 3600,  # 1 hour
            "email_notifications": {
                "on_failure": [self.config.monitoring.alert_email]
                if self.config.monitoring.alert_email
                else [],
                "no_alert_for_skipped_runs": True,
            },
            "tasks": [
                # ── Task 1: Batch Ingestion ────────────────────────────
                {
                    "task_key": "batch_ingestion",
                    "description": "Fetch flight data from OpenSky API",
                    "notebook_task": {
                        "notebook_path": f"{repo_path}/notebooks/02_batch_ingestion",
                        "base_parameters": {
                            "environment": self.config.environment,
                        },
                    },
                    "new_cluster": self._get_cluster_config("small"),
                    "timeout_seconds": 600,
                    "max_retries": 2,
                    "retry_on_timeout": True,
                },
                # ── Task 2: Bronze → Silver ────────────────────────────
                {
                    "task_key": "bronze_to_silver",
                    "description": "Clean, validate, and enrich flight data",
                    "depends_on": [{"task_key": "batch_ingestion"}],
                    "notebook_task": {
                        "notebook_path": f"{repo_path}/notebooks/04_bronze_to_silver",
                        "base_parameters": {
                            "environment": self.config.environment,
                        },
                    },
                    "new_cluster": self._get_cluster_config("medium"),
                    "timeout_seconds": 900,
                    "max_retries": 1,
                },
                # ── Task 3: Silver → Gold ──────────────────────────────
                {
                    "task_key": "silver_to_gold",
                    "description": "Generate business analytics tables",
                    "depends_on": [{"task_key": "bronze_to_silver"}],
                    "notebook_task": {
                        "notebook_path": f"{repo_path}/notebooks/05_silver_to_gold",
                        "base_parameters": {
                            "environment": self.config.environment,
                        },
                    },
                    "new_cluster": self._get_cluster_config("medium"),
                    "timeout_seconds": 600,
                    "max_retries": 1,
                },
                # ── Task 4: Anomaly Detection ──────────────────────────
                {
                    "task_key": "anomaly_detection",
                    "description": "Detect flight anomalies",
                    "depends_on": [{"task_key": "bronze_to_silver"}],
                    "notebook_task": {
                        "notebook_path": f"{repo_path}/notebooks/07_anomaly_detection",
                        "base_parameters": {
                            "environment": self.config.environment,
                        },
                    },
                    "new_cluster": self._get_cluster_config("medium"),
                    "timeout_seconds": 600,
                    "max_retries": 1,
                },
            ],
        }

        logger.info("Pipeline job config created: %s", job_name)
        return job_config

    def create_optimization_job(
        self,
        repo_path: str = "/Repos/<username>/flight-analytics-platform",
    ) -> Dict[str, Any]:
        """
        Create a Delta optimization job (runs daily).

        Returns:
            Job definition dict
        """
        return {
            "name": "flight-analytics-optimization",
            "description": "Daily Delta Lake optimization (OPTIMIZE + VACUUM)",
            "schedule": {
                "quartz_cron_expression": "0 0 2 * * ?",  # 2 AM daily
                "timezone_id": "UTC",
            },
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "delta_optimization",
                    "notebook_task": {
                        "notebook_path": f"{repo_path}/notebooks/08_delta_optimization",
                    },
                    "new_cluster": self._get_cluster_config("small"),
                    "timeout_seconds": 1800,
                },
            ],
        }

    def _get_cluster_config(self, size: str = "small") -> Dict[str, Any]:
        """
        Get cluster configuration by size.

        Args:
            size: 'small', 'medium', or 'large'

        Returns:
            Cluster config dict
        """
        configs = {
            "small": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 1,
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                },
            },
            "medium": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_DS4_v2",
                "num_workers": 2,
                "autoscale": {"min_workers": 1, "max_workers": 4},
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.shuffle.partitions": "200",
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true",
                },
            },
            "large": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_DS5_v2",
                "num_workers": 4,
                "autoscale": {"min_workers": 2, "max_workers": 8},
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.shuffle.partitions": "400",
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true",
                },
            },
        }

        return configs.get(size, configs["small"])

    def export_job_config(
        self, job_config: Dict[str, Any], filepath: str
    ) -> None:
        """Export job config to JSON file."""
        with open(filepath, "w") as f:
            json.dump(job_config, f, indent=2)
        logger.info("Job config exported to %s", filepath)
