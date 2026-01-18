"""
Test DAG with a schedule for testing native Temporal scheduling.

This DAG is used by test_native_scheduling_e2e to verify that
the orchestrator correctly creates and manages Temporal Schedules.
"""
from datetime import datetime, timedelta

from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task


def scheduled_task():
    """Simple task that completes successfully."""
    return "scheduled task complete"


# Create DAG with a schedule using SDK
with DAG(
    dag_id="test_scheduled_dag",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(hours=1),  # Run every hour
    catchup=False,
    max_active_runs=1,
    tags=["temporal", "test", "scheduled"],
) as dag:

    @task(task_id="scheduled_task")
    def execute_scheduled_task():
        """Execute the scheduled task."""
        return scheduled_task()

    execute_scheduled_task()
