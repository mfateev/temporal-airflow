"""
Tests for HTTP provider support in Temporal-Airflow integration.

These tests verify that Airflow providers work seamlessly with the Temporal
orchestration. The HTTP provider is used as an example since it's widely used
and can be tested against public endpoints.
"""
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from temporal_airflow.activities import run_airflow_task, load_dag_from_file, clear_dag_cache
from temporal_airflow.models import ActivityTaskInput


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear DAG cache before and after each test."""
    clear_dag_cache()
    yield
    clear_dag_cache()


class TestHttpProviderSupport:
    """Test that HTTP provider operators work with Temporal activities."""

    def test_load_http_provider_dag(self):
        """Test that a DAG using HTTP provider can be loaded."""
        # This tests that the provider imports work
        dag = load_dag_from_file("provider_http_example.py", "provider_http_example")

        assert dag.dag_id == "provider_http_example"
        assert "wait_for_api" in dag.task_dict
        assert "fetch_api_data" in dag.task_dict
        assert "process_response" in dag.task_dict

        # Verify task types
        from airflow.providers.http.sensors.http import HttpSensor
        from airflow.providers.http.operators.http import HttpOperator
        from airflow.providers.standard.operators.python import PythonOperator

        assert isinstance(dag.task_dict["wait_for_api"], HttpSensor)
        assert isinstance(dag.task_dict["fetch_api_data"], HttpOperator)
        assert isinstance(dag.task_dict["process_response"], PythonOperator)

    def test_load_http_post_dag(self):
        """Test that the POST example DAG can be loaded."""
        dag = load_dag_from_file("provider_http_example.py", "provider_http_post_example")

        assert dag.dag_id == "provider_http_post_example"
        assert "prepare_payload" in dag.task_dict
        assert "post_data" in dag.task_dict
        assert "verify_response" in dag.task_dict

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_http_sensor_poke_via_activity(self, mock_run):
        """Test HttpSensor poke() called through activity returns correct result."""
        # Mock successful HTTP response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_run.return_value = mock_response

        # Create activity input for the sensor task
        input = ActivityTaskInput(
            dag_id="provider_http_example",
            task_id="wait_for_api",
            run_id="test_run_001",
            dag_rel_path="provider_http_example.py",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            try_number=1,
            map_index=-1,
            upstream_results={},
            connections={
                "http_default": {
                    "conn_type": "http",
                    "host": "httpbin.org",
                }
            },
            variables={},
        )

        # Execute the activity - sensor should succeed since mock returns 200
        result = run_airflow_task(input)

        assert result.state.value == "success"
        assert result.task_id == "wait_for_api"

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_http_operator_via_activity(self, mock_run):
        """Test HttpOperator execute() called through activity."""
        # Mock HTTP response with JSON data
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"origin": "1.2.3.4", "headers": {"Accept": "application/json"}}'
        mock_response.headers = {"Content-Type": "application/json"}
        mock_run.return_value = mock_response

        input = ActivityTaskInput(
            dag_id="provider_http_example",
            task_id="fetch_api_data",
            run_id="test_run_002",
            dag_rel_path="provider_http_example.py",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            try_number=1,
            map_index=-1,
            upstream_results={},
            connections={
                "http_default": {
                    "conn_type": "http",
                    "host": "httpbin.org",
                }
            },
            variables={},
        )

        result = run_airflow_task(input)

        assert result.state.value == "success"
        assert result.task_id == "fetch_api_data"
        # HttpOperator returns response text
        assert result.return_value is not None

    def test_python_operator_processes_http_result(self):
        """Test PythonOperator can process upstream HTTP results."""
        import json

        # Simulate upstream HTTP response
        upstream_response = json.dumps({
            "origin": "1.2.3.4",
            "headers": {"Accept": "application/json", "Host": "httpbin.org"}
        })

        input = ActivityTaskInput(
            dag_id="provider_http_example",
            task_id="process_response",
            run_id="test_run_003",
            dag_rel_path="provider_http_example.py",
            logical_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            try_number=1,
            map_index=-1,
            upstream_results={"fetch_api_data": upstream_response},
            connections={},
            variables={},
        )

        result = run_airflow_task(input)

        assert result.state.value == "success"
        assert result.return_value is not None
        assert result.return_value["processed"] is True
        assert result.return_value["origin"] == "1.2.3.4"
        assert result.return_value["headers_count"] == 2


class TestProviderConnectionHandling:
    """Test that provider connections are properly set up via environment."""

    def test_connection_uri_format(self):
        """Test that connections are properly formatted for providers."""
        from temporal_airflow.activities import _build_connection_uri

        conn_data = {
            "conn_type": "http",
            "host": "api.example.com",
            "port": 443,
            "schema": "v1",
        }

        uri = _build_connection_uri(conn_data)
        assert uri == "http://api.example.com:443/v1"

    def test_connection_with_auth(self):
        """Test connection URI with authentication."""
        from temporal_airflow.activities import _build_connection_uri

        conn_data = {
            "conn_type": "http",
            "host": "api.example.com",
            "login": "user",
            "password": "pass@word!",  # Special chars should be encoded
        }

        uri = _build_connection_uri(conn_data)
        assert "user" in uri
        assert "pass%40word%21" in uri  # URL-encoded special chars
