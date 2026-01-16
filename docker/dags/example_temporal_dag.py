"""
Example DAG for testing Temporal orchestration.

This DAG demonstrates:
1. Simple task dependencies
2. Parallel execution
3. Task with retries

When triggered:
- If Temporal orchestrator is configured, execution is routed to Temporal
- Temporal workflow manages task execution and state
- Airflow UI shows real-time status from database
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.sdk.definitions.decorators import task

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="example_temporal_dag",
    default_args=default_args,
    description="Example DAG for Temporal orchestration",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "temporal"],
) as dag:

    @task
    def start():
        """Initial task."""
        print("Starting DAG execution via Temporal!")
        return "started"

    @task
    def process_a(data: str):
        """Process branch A."""
        import time
        print(f"Processing A with input: {data}")
        time.sleep(2)  # Simulate work
        return f"A processed: {data}"

    @task
    def process_b(data: str):
        """Process branch B."""
        import time
        print(f"Processing B with input: {data}")
        time.sleep(3)  # Simulate work
        return f"B processed: {data}"

    @task
    def combine(result_a: str, result_b: str):
        """Combine results from parallel branches."""
        print(f"Combining results: {result_a}, {result_b}")
        return f"Combined: [{result_a}] + [{result_b}]"

    @task
    def finish(combined: str):
        """Final task."""
        print(f"DAG completed with result: {combined}")
        return "success"

    # Define task flow
    start_result = start()

    # Parallel execution
    a_result = process_a(start_result)
    b_result = process_b(start_result)

    # Join and finish
    combined = combine(a_result, b_result)
    finish(combined)
