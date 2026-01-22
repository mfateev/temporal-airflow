"""
Example DAG demonstrating Airflow HTTP provider with Temporal orchestration.

This DAG shows that third-party Airflow providers work seamlessly with
the Temporal integration. It uses the apache-airflow-providers-http package
to make HTTP requests and wait for API conditions.

Features demonstrated:
- HttpSensor: Waits for an HTTP endpoint to return a successful response
- SimpleHttpOperator: Makes HTTP requests and returns response data
- Provider operators work exactly like they do in vanilla Airflow
"""
from datetime import datetime
import json

from airflow.sdk.definitions.dag import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator


def process_api_response(**context):
    """Process the response from the HTTP operator."""
    ti = context.get("ti") or context.get("task_instance")
    if ti:
        # Get the response from upstream HTTP operator
        response = ti.xcom_pull(task_ids="fetch_api_data")
        if response:
            # Parse and process the JSON response
            data = json.loads(response) if isinstance(response, str) else response
            return {
                "processed": True,
                "origin": data.get("origin", "unknown"),
                "headers_count": len(data.get("headers", {})),
            }
    return {"processed": False, "error": "No upstream data"}


def log_sensor_success(**context):
    """Log that the sensor condition was met."""
    print("HTTP endpoint is available! Proceeding with data fetch...")
    return "sensor_passed"


# =============================================================================
# DAG: HTTP Provider Example
# =============================================================================
#
# This DAG demonstrates a common pattern:
# 1. Wait for an API to be available (HttpSensor)
# 2. Fetch data from the API (HttpOperator)
# 3. Process the data (PythonOperator)
#
# The HTTP connection is configured via environment variable:
#   AIRFLOW_CONN_HTTP_DEFAULT='http://httpbin.org'
# =============================================================================

with DAG(
    dag_id="provider_http_example",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "example", "provider", "http"],
    doc_md=__doc__,
    default_args={
        # Connection ID for HTTP requests (configured via env var or Airflow UI)
        "http_conn_id": "http_default",
    },
) as dag:

    # Step 1: Wait for the API to be available
    # HttpSensor will poke the endpoint until it returns 2xx status
    wait_for_api = HttpSensor(
        task_id="wait_for_api",
        endpoint="/status/200",  # httpbin endpoint that returns 200
        method="GET",
        poke_interval=5,  # Check every 5 seconds
        timeout=60,  # Give up after 60 seconds
        mode="poke",  # Use poke mode (Temporal handles the retry loop)
    )

    # Log sensor success
    log_success = PythonOperator(
        task_id="log_sensor_success",
        python_callable=log_sensor_success,
    )

    # Step 2: Fetch data from the API
    # HttpOperator makes the request and returns the response
    fetch_data = HttpOperator(
        task_id="fetch_api_data",
        endpoint="/get",  # httpbin endpoint that returns request info
        method="GET",
        headers={"Accept": "application/json"},
        log_response=True,
    )

    # Step 3: Process the response
    process_data = PythonOperator(
        task_id="process_response",
        python_callable=process_api_response,
    )

    # Define task dependencies
    wait_for_api >> log_success >> fetch_data >> process_data


# =============================================================================
# Additional example: POST request with data
# =============================================================================

with DAG(
    dag_id="provider_http_post_example",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "example", "provider", "http"],
    default_args={
        "http_conn_id": "http_default",
    },
) as dag_post:

    def prepare_payload(**context):
        """Prepare payload for POST request."""
        return json.dumps({
            "dag_id": context.get("dag_id"),
            "run_id": context.get("run_id"),
            "timestamp": datetime.now().isoformat(),
        })

    # Prepare the payload
    prepare = PythonOperator(
        task_id="prepare_payload",
        python_callable=prepare_payload,
    )

    # POST the data to httpbin (which echoes it back)
    post_data = HttpOperator(
        task_id="post_data",
        endpoint="/post",
        method="POST",
        headers={"Content-Type": "application/json"},
        data='{"test": "data", "from": "temporal-airflow"}',
        log_response=True,
    )

    def verify_post_response(**context):
        """Verify the POST response."""
        ti = context.get("ti") or context.get("task_instance")
        if ti:
            response = ti.xcom_pull(task_ids="post_data")
            if response:
                data = json.loads(response) if isinstance(response, str) else response
                # httpbin echoes back the posted JSON in the 'json' field
                return {
                    "success": True,
                    "echoed_data": data.get("json"),
                    "url": data.get("url"),
                }
        return {"success": False}

    verify = PythonOperator(
        task_id="verify_response",
        python_callable=verify_post_response,
    )

    prepare >> post_data >> verify
