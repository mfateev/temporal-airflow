"""
Tests for sensor support in Temporal Airflow integration.

This module tests that Airflow sensors are correctly executed using the
poke() + BENIGN retry pattern instead of the blocking execute() method.
"""
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions
from temporalio.common import RetryPolicy

from airflow._shared.timezones import timezone
from temporal_airflow.workflows import ExecuteAirflowDagWorkflow
from temporal_airflow.models import DagExecutionInput


# Set DAGS_FOLDER to the directory containing test DAGs
TEST_DAGS_FOLDER = Path(__file__).parent.parent


@pytest.fixture(autouse=True)
def set_dags_folder():
    """Set AIRFLOW__CORE__DAGS_FOLDER to test DAGs directory for all tests."""
    old_value = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(TEST_DAGS_FOLDER)
    yield
    if old_value is None:
        os.environ.pop("AIRFLOW__CORE__DAGS_FOLDER", None)
    else:
        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = old_value


@pytest.fixture(autouse=True)
def reset_sensor_counts():
    """Reset global poke counts before each test."""
    from temporal_airflow.test_sensors import reset_poke_counts
    reset_poke_counts()
    yield
    reset_poke_counts()


# Configure sandbox to pass through problematic modules
PASSTHROUGH_MODULES = [
    "structlog",
    "rich",
    "airflow.sdk.observability",
    "airflow._shared.observability",
]


def create_worker(client, task_queue, workflows, activities):
    """Create a Worker with custom sandbox configuration."""
    return Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
        activity_executor=ThreadPoolExecutor(max_workers=5),
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules(
                *PASSTHROUGH_MODULES
            )
        ),
    )


def load_dag_from_file(dag_file: Path, dag_id: str):
    """Load a DAG from a Python file."""
    import importlib.util
    from airflow.sdk.definitions.dag import DAG

    spec = importlib.util.spec_from_file_location("temp_module", dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, DAG) and attr.dag_id == dag_id:
            return attr

    raise ValueError(f"DAG '{dag_id}' not found in {dag_file}")


async def assert_no_workflow_task_failures(client, workflow_id: str) -> None:
    """Check workflow history for WorkflowTaskFailed events (indicates deadlock or other issues)."""
    from temporalio.api.enums.v1 import EventType

    handle = client.get_workflow_handle(workflow_id)
    history = await handle.fetch_history()

    failed_events = []
    for event in history.events:
        if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
            failure = event.workflow_task_failed_event_attributes
            failed_events.append({
                "event_id": event.event_id,
                "cause": str(failure.cause) if failure.cause else "unknown",
                "failure": str(failure.failure.message) if failure.failure else "no message",
            })

    if failed_events:
        raise AssertionError(
            f"Workflow {workflow_id} had {len(failed_events)} WorkflowTaskFailed events: {failed_events}"
        )


# ============================================================================
# Unit Tests - Sensor Detection and Poke Logic
# ============================================================================

class TestSensorDetection:
    """Test that sensors are correctly detected and handled."""

    def test_sensor_is_instance_of_base_sensor_operator(self):
        """Test that our test sensors inherit from BaseSensorOperator."""
        from airflow.sdk.bases.sensor import BaseSensorOperator
        from temporal_airflow.test_sensors import (
            ImmediateSensor,
            CountdownSensor,
            XComSensor,
            DelayedXComSensor,
        )

        assert issubclass(ImmediateSensor, BaseSensorOperator)
        assert issubclass(CountdownSensor, BaseSensorOperator)
        assert issubclass(XComSensor, BaseSensorOperator)
        assert issubclass(DelayedXComSensor, BaseSensorOperator)

    def test_immediate_sensor_poke_returns_true(self):
        """Test ImmediateSensor.poke() returns True immediately."""
        from temporal_airflow.test_sensors import ImmediateSensor

        sensor = ImmediateSensor(task_id="test")
        result = sensor.poke({"dag_id": "test_dag"})
        assert result is True

    def test_countdown_sensor_poke_returns_false_initially(self):
        """Test CountdownSensor.poke() returns False until threshold reached."""
        from temporal_airflow.test_sensors import CountdownSensor, reset_poke_counts

        reset_poke_counts()
        sensor = CountdownSensor(task_id="test", pokes_until_success=3, sensor_id="unit_test")
        context = {"dag_id": "unit_test_dag"}

        # First two pokes should return False
        assert sensor.poke(context) is False
        assert sensor.poke(context) is False

        # Third poke should return True
        assert sensor.poke(context) is True

    def test_xcom_sensor_returns_poke_return_value(self):
        """Test XComSensor.poke() returns PokeReturnValue with xcom_value."""
        from airflow.sdk.bases.sensor import PokeReturnValue
        from temporal_airflow.test_sensors import XComSensor

        sensor = XComSensor(task_id="test", return_value="test_value")
        result = sensor.poke({"dag_id": "test_dag"})

        assert isinstance(result, PokeReturnValue)
        assert result.is_done is True
        assert result.xcom_value == "test_value"

    def test_delayed_xcom_sensor_returns_is_done_false_initially(self):
        """Test DelayedXComSensor.poke() returns is_done=False until threshold."""
        from airflow.sdk.bases.sensor import PokeReturnValue
        from temporal_airflow.test_sensors import DelayedXComSensor, reset_poke_counts

        reset_poke_counts()
        sensor = DelayedXComSensor(
            task_id="test",
            pokes_until_success=2,
            return_value="delayed_value",
            sensor_id="unit_delayed",
        )
        context = {"dag_id": "unit_test_dag"}

        # First poke should return is_done=False
        result1 = sensor.poke(context)
        assert isinstance(result1, PokeReturnValue)
        assert result1.is_done is False

        # Second poke should return is_done=True with xcom_value
        result2 = sensor.poke(context)
        assert isinstance(result2, PokeReturnValue)
        assert result2.is_done is True
        assert result2.xcom_value == "delayed_value"


# ============================================================================
# E2E Tests - Sensor Workflow Execution
# ============================================================================

@pytest.mark.asyncio
async def test_immediate_sensor_workflow():
    """Test workflow with sensor that succeeds on first poke."""
    from airflow.serialization.serialized_objects import DagSerialization
    from temporal_airflow.activities import run_airflow_task

    dag_file = TEST_DAGS_FOLDER / "test_sensors.py"
    dag = load_dag_from_file(dag_file, "test_sensor_immediate")
    serialized = DagSerialization.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_sensor_immediate",
            run_id="test_run_immediate",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        workflow_id = "test-sensor-immediate"

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id=workflow_id,
                task_queue="test-queue",
            )

            assert result.state == "success"
            assert result.dag_id == "test_sensor_immediate"
            assert result.tasks_succeeded == 1
            assert result.tasks_failed == 0

            await assert_no_workflow_task_failures(env.client, workflow_id)


@pytest.mark.skip(reason="Countdown sensor uses global state that doesn't persist across Temporal activity retries")
@pytest.mark.asyncio
async def test_countdown_sensor_workflow():
    """Test workflow with sensor that needs multiple pokes to succeed."""
    from airflow.serialization.serialized_objects import DagSerialization
    from temporal_airflow.activities import run_airflow_task
    from temporal_airflow.test_sensors import reset_poke_counts

    reset_poke_counts()

    dag_file = TEST_DAGS_FOLDER / "test_sensors.py"
    dag = load_dag_from_file(dag_file, "test_sensor_countdown")
    serialized = DagSerialization.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_sensor_countdown",
            run_id="test_run_countdown",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        workflow_id = "test-sensor-countdown"

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id=workflow_id,
                task_queue="test-queue",
            )

            # Sensor should succeed after 3 pokes
            assert result.state == "success"
            assert result.dag_id == "test_sensor_countdown"
            assert result.tasks_succeeded == 1

            await assert_no_workflow_task_failures(env.client, workflow_id)


@pytest.mark.asyncio
async def test_xcom_sensor_workflow():
    """Test workflow with sensor that returns XCom value via PokeReturnValue."""
    from airflow.serialization.serialized_objects import DagSerialization
    from temporal_airflow.activities import run_airflow_task

    dag_file = TEST_DAGS_FOLDER / "test_sensors.py"
    dag = load_dag_from_file(dag_file, "test_sensor_xcom")
    serialized = DagSerialization.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_sensor_xcom",
            run_id="test_run_xcom",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        workflow_id = "test-sensor-xcom"

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id=workflow_id,
                task_queue="test-queue",
            )

            assert result.state == "success"
            assert result.tasks_succeeded == 1

            await assert_no_workflow_task_failures(env.client, workflow_id)


@pytest.mark.asyncio
async def test_sensor_with_downstream_task():
    """Test workflow with sensor followed by downstream task."""
    from airflow.serialization.serialized_objects import DagSerialization
    from temporal_airflow.activities import run_airflow_task

    dag_file = TEST_DAGS_FOLDER / "test_sensors.py"
    dag = load_dag_from_file(dag_file, "test_sensor_with_downstream")
    serialized = DagSerialization.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_sensor_with_downstream",
            run_id="test_run_downstream",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        workflow_id = "test-sensor-downstream"

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id=workflow_id,
                task_queue="test-queue",
            )

            # Both sensor and downstream task should succeed
            assert result.state == "success"
            assert result.tasks_succeeded == 2
            assert result.tasks_failed == 0

            await assert_no_workflow_task_failures(env.client, workflow_id)


@pytest.mark.skip(reason="DelayedXCom sensor uses global state that doesn't persist across Temporal activity retries")
@pytest.mark.asyncio
async def test_delayed_xcom_sensor_workflow():
    """Test workflow with sensor using PokeReturnValue with is_done=False."""
    from airflow.serialization.serialized_objects import DagSerialization
    from temporal_airflow.activities import run_airflow_task
    from temporal_airflow.test_sensors import reset_poke_counts

    reset_poke_counts()

    dag_file = TEST_DAGS_FOLDER / "test_sensors.py"
    dag = load_dag_from_file(dag_file, "test_sensor_delayed_xcom")
    serialized = DagSerialization.to_dict(dag)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        input_data = DagExecutionInput(
            dag_id="test_sensor_delayed_xcom",
            run_id="test_run_delayed_xcom",
            logical_date=timezone.datetime(2025, 1, 1),
            serialized_dag=serialized,
        )

        workflow_id = "test-sensor-delayed-xcom"

        async with create_worker(
            env.client,
            task_queue="test-queue",
            workflows=[ExecuteAirflowDagWorkflow],
            activities=[run_airflow_task],
        ):
            result = await env.client.execute_workflow(
                ExecuteAirflowDagWorkflow.run,
                input_data,
                id=workflow_id,
                task_queue="test-queue",
            )

            # Sensor should succeed after 2 pokes with xcom value
            assert result.state == "success"
            assert result.tasks_succeeded == 1

            await assert_no_workflow_task_failures(env.client, workflow_id)


# ============================================================================
# Activity-Level Tests - Direct Activity Invocation
# ============================================================================

def test_sensor_activity_returns_benign_error_on_false():
    """Test that sensor activity raises BENIGN error when poke returns False."""
    from temporalio.testing import ActivityEnvironment
    from temporalio.exceptions import ApplicationError, ApplicationErrorCategory
    from temporal_airflow.activities import run_airflow_task
    from temporal_airflow.models import ActivityTaskInput
    from temporal_airflow.test_sensors import reset_poke_counts

    reset_poke_counts()

    env = ActivityEnvironment()

    input_data = ActivityTaskInput(
        dag_id="test_sensor_countdown",
        task_id="wait_countdown",
        run_id="test_run",
        logical_date=timezone.datetime(2025, 1, 1),
        dag_rel_path="test_sensors.py",
    )

    # First call should raise BENIGN ApplicationError (poke returns False)
    with pytest.raises(ApplicationError) as exc_info:
        env.run(run_airflow_task, input_data)

    # Verify it's a BENIGN error for retry
    assert exc_info.value.category == ApplicationErrorCategory.BENIGN
    assert "condition not met" in str(exc_info.value)
    assert exc_info.value.next_retry_delay is not None


def test_sensor_activity_succeeds_on_true():
    """Test that sensor activity returns success when poke returns True."""
    from temporalio.testing import ActivityEnvironment
    from temporal_airflow.activities import run_airflow_task
    from temporal_airflow.models import ActivityTaskInput, TaskExecutionResult
    from airflow.utils.state import TaskInstanceState

    env = ActivityEnvironment()

    input_data = ActivityTaskInput(
        dag_id="test_sensor_immediate",
        task_id="wait_immediate",
        run_id="test_run",
        logical_date=timezone.datetime(2025, 1, 1),
        dag_rel_path="test_sensors.py",
    )

    result = env.run(run_airflow_task, input_data)

    assert isinstance(result, TaskExecutionResult)
    assert result.state == TaskInstanceState.SUCCESS
    assert result.task_id == "wait_immediate"


def test_sensor_activity_returns_xcom_from_poke_return_value():
    """Test that sensor activity extracts xcom_value from PokeReturnValue."""
    from temporalio.testing import ActivityEnvironment
    from temporal_airflow.activities import run_airflow_task
    from temporal_airflow.models import ActivityTaskInput, TaskExecutionResult

    env = ActivityEnvironment()

    input_data = ActivityTaskInput(
        dag_id="test_sensor_xcom",
        task_id="wait_xcom",
        run_id="test_run",
        logical_date=timezone.datetime(2025, 1, 1),
        dag_rel_path="test_sensors.py",
    )

    result = env.run(run_airflow_task, input_data)

    assert isinstance(result, TaskExecutionResult)
    assert result.return_value == "sensor_xcom_value"
    assert result.xcom_data == {"return_value": "sensor_xcom_value"}
