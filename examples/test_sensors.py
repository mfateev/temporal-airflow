"""
Test DAGs for testing sensor support in Temporal.

This module contains test DAGs that use Airflow sensors to verify that
the Temporal integration correctly handles sensor polling via the
poke() + BENIGN retry pattern.
"""
from datetime import datetime
import os
import tempfile

from airflow.sdk.definitions.dag import DAG
from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator


# Global counter for testing sensors that need multiple pokes
# This simulates a condition that becomes true after N pokes
_poke_counts: dict[str, int] = {}


class ImmediateSensor(BaseSensorOperator):
    """Sensor that succeeds on first poke (for basic testing)."""

    def poke(self, context) -> bool:
        self.log.info("ImmediateSensor.poke() called - returning True immediately")
        return True


class CountdownSensor(BaseSensorOperator):
    """
    Sensor that succeeds after N pokes.

    Used to test that Temporal correctly retries with BENIGN errors
    until the condition is met.
    """

    def __init__(self, pokes_until_success: int = 3, sensor_id: str = "default", **kwargs):
        super().__init__(**kwargs)
        self.pokes_until_success = pokes_until_success
        self.sensor_id = sensor_id

    def poke(self, context) -> bool:
        global _poke_counts

        key = f"{context.get('dag_id', 'unknown')}_{self.sensor_id}"
        _poke_counts[key] = _poke_counts.get(key, 0) + 1
        current_count = _poke_counts[key]

        self.log.info(
            f"CountdownSensor.poke() called - count={current_count}, "
            f"need={self.pokes_until_success}"
        )

        if current_count >= self.pokes_until_success:
            self.log.info("CountdownSensor condition met!")
            return True

        self.log.info(f"CountdownSensor waiting... ({current_count}/{self.pokes_until_success})")
        return False


class XComSensor(BaseSensorOperator):
    """
    Sensor that returns a value via PokeReturnValue.

    Used to test that XCom values are correctly passed through
    when using PokeReturnValue.
    """

    def __init__(self, return_value: str = "sensor_result", **kwargs):
        super().__init__(**kwargs)
        self.return_value = return_value

    def poke(self, context) -> PokeReturnValue:
        self.log.info(f"XComSensor.poke() called - returning PokeReturnValue with '{self.return_value}'")
        return PokeReturnValue(is_done=True, xcom_value=self.return_value)


class DelayedXComSensor(BaseSensorOperator):
    """
    Sensor that returns PokeReturnValue with is_done=False initially,
    then is_done=True with xcom_value after N pokes.
    """

    def __init__(self, pokes_until_success: int = 2, return_value: str = "delayed_result", sensor_id: str = "default", **kwargs):
        super().__init__(**kwargs)
        self.pokes_until_success = pokes_until_success
        self.return_value = return_value
        self.sensor_id = sensor_id

    def poke(self, context) -> PokeReturnValue:
        global _poke_counts

        key = f"{context.get('dag_id', 'unknown')}_delayed_{self.sensor_id}"
        _poke_counts[key] = _poke_counts.get(key, 0) + 1
        current_count = _poke_counts[key]

        self.log.info(
            f"DelayedXComSensor.poke() called - count={current_count}, "
            f"need={self.pokes_until_success}"
        )

        if current_count >= self.pokes_until_success:
            self.log.info(f"DelayedXComSensor condition met! Returning '{self.return_value}'")
            return PokeReturnValue(is_done=True, xcom_value=self.return_value)

        self.log.info(f"DelayedXComSensor waiting... ({current_count}/{self.pokes_until_success})")
        return PokeReturnValue(is_done=False, xcom_value=None)


class FileSensorSimple(BaseSensorOperator):
    """
    Simple file sensor for testing (doesn't require airflow providers).

    Checks if a file exists at the given path.
    """

    def __init__(self, filepath: str, **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath

    def poke(self, context) -> bool:
        exists = os.path.exists(self.filepath)
        self.log.info(f"FileSensorSimple.poke() - checking '{self.filepath}': exists={exists}")
        return exists


def reset_poke_counts():
    """Reset global poke counts (call between tests)."""
    global _poke_counts
    _poke_counts = {}


# ============================================================================
# Test DAG 1: Immediate sensor success
# ============================================================================

with DAG(
    dag_id="test_sensor_immediate",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test", "sensor"],
) as dag_immediate:

    sensor = ImmediateSensor(
        task_id="wait_immediate",
        poke_interval=1,  # Short interval for testing
    )


# ============================================================================
# Test DAG 2: Sensor with countdown (multiple pokes)
# ============================================================================

with DAG(
    dag_id="test_sensor_countdown",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test", "sensor"],
) as dag_countdown:

    sensor = CountdownSensor(
        task_id="wait_countdown",
        pokes_until_success=3,
        sensor_id="countdown",
        poke_interval=1,  # Short interval for testing
    )


# ============================================================================
# Test DAG 3: Sensor with XCom return value
# ============================================================================

with DAG(
    dag_id="test_sensor_xcom",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test", "sensor"],
) as dag_xcom:

    sensor = XComSensor(
        task_id="wait_xcom",
        return_value="sensor_xcom_value",
        poke_interval=1,
    )


# ============================================================================
# Test DAG 4: Sensor followed by downstream task
# ============================================================================

def process_sensor_result(**context):
    """Process result from upstream sensor."""
    ti = context.get("ti") or context.get("task_instance")
    if ti:
        upstream_value = ti.xcom_pull(task_ids="wait_sensor")
        return f"processed: {upstream_value}"
    return "processed: no_upstream"


with DAG(
    dag_id="test_sensor_with_downstream",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test", "sensor"],
) as dag_downstream:

    sensor = XComSensor(
        task_id="wait_sensor",
        return_value="upstream_data",
        poke_interval=1,
    )

    process = PythonOperator(
        task_id="process_result",
        python_callable=process_sensor_result,
    )

    sensor >> process


# ============================================================================
# Test DAG 5: Delayed XCom sensor (PokeReturnValue with is_done=False)
# ============================================================================

with DAG(
    dag_id="test_sensor_delayed_xcom",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["temporal", "test", "sensor"],
) as dag_delayed_xcom:

    sensor = DelayedXComSensor(
        task_id="wait_delayed",
        pokes_until_success=2,
        return_value="delayed_xcom_value",
        sensor_id="delayed",
        poke_interval=1,
    )
