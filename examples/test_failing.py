"""
Test DAG for testing task failure handling.

This DAG is used by test_task_failure_with_application_error to verify
that the workflow correctly handles task failures and raises ApplicationError
with structured details.
"""
from datetime import datetime

from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task


# Create DAG using SDK
with DAG(
    dag_id="test_failing",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test", "failure"],
) as dag:

    @task(task_id="failing_task")
    def failing_task():
        """Task that always fails with ValueError."""
        print("Executing failing_task")
        raise ValueError("Intentional test failure")

    failing_task()
