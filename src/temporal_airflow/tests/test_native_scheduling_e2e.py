# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""End-to-end tests for Temporal native scheduling functionality.

These tests validate the Phase 2 native scheduling features:
  - should_schedule_dagrun() - creates/checks Temporal Schedules
  - sync_pause_state() - pauses/unpauses Temporal Schedules
  - on_dag_deleted() - deletes Temporal Schedules
  - on_timetable_changed() - updates Temporal Schedules

Test categories:
  - Unit tests (TestScheduleIdGeneration, TestTimetableConversion, etc.)
    Run without Airflow: pytest -k "not airflow"

  - Integration tests (TestShouldScheduleDagrun, TestSyncPauseState, etc.)
    Require Airflow and Temporal: Run in Docker/Breeze environment

Using Temporal's WorkflowEnvironment, we can test the orchestrator without
requiring a real Temporal server.
"""
from __future__ import annotations

import os
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from temporalio.client import ScheduleOverlapPolicy

from temporal_airflow.orchestrator import (
    TemporalOrchestrator,
    _get_native_scheduling_enabled,
    _get_schedule_id_prefix,
)


# =============================================================================
# Unit Tests - No Airflow required
# =============================================================================


class TestScheduleIdGeneration:
    """Tests for schedule ID generation."""

    def test_schedule_id_format(self):
        """Test that schedule IDs follow the expected format."""
        orchestrator = TemporalOrchestrator()
        orchestrator._schedule_id_prefix = "airflow-schedule-"

        schedule_id = orchestrator._get_schedule_id("my_dag")
        assert schedule_id == "airflow-schedule-my_dag"

    def test_schedule_id_with_special_characters(self):
        """Test schedule ID with special characters in DAG name."""
        orchestrator = TemporalOrchestrator()
        orchestrator._schedule_id_prefix = "airflow-schedule-"

        schedule_id = orchestrator._get_schedule_id("my-dag_123")
        assert schedule_id == "airflow-schedule-my-dag_123"

    def test_custom_prefix_env_var(self):
        """Test that custom prefix is read from environment variable."""
        with patch.dict(os.environ, {"TEMPORAL_SCHEDULE_ID_PREFIX": "custom-prefix-"}):
            prefix = _get_schedule_id_prefix()
            assert prefix == "custom-prefix-"

    def test_default_prefix(self):
        """Test that default prefix is used when env var not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove the env var if it exists
            os.environ.pop("TEMPORAL_SCHEDULE_ID_PREFIX", None)
            prefix = _get_schedule_id_prefix()
            assert prefix == "airflow-schedule-"


class TestNativeSchedulingEnabled:
    """Tests for native scheduling configuration."""

    def test_native_scheduling_enabled_true(self):
        """Test native scheduling is enabled with 'true'."""
        with patch.dict(os.environ, {"TEMPORAL_NATIVE_SCHEDULING": "true"}):
            assert _get_native_scheduling_enabled() is True

    def test_native_scheduling_enabled_1(self):
        """Test native scheduling is enabled with '1'."""
        with patch.dict(os.environ, {"TEMPORAL_NATIVE_SCHEDULING": "1"}):
            assert _get_native_scheduling_enabled() is True

    def test_native_scheduling_enabled_yes(self):
        """Test native scheduling is enabled with 'yes'."""
        with patch.dict(os.environ, {"TEMPORAL_NATIVE_SCHEDULING": "yes"}):
            assert _get_native_scheduling_enabled() is True

    def test_native_scheduling_disabled_false(self):
        """Test native scheduling is disabled with 'false'."""
        with patch.dict(os.environ, {"TEMPORAL_NATIVE_SCHEDULING": "false"}):
            assert _get_native_scheduling_enabled() is False

    def test_native_scheduling_disabled_by_default(self):
        """Test native scheduling is disabled by default."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("TEMPORAL_NATIVE_SCHEDULING", None)
            assert _get_native_scheduling_enabled() is False


class TestTimetableConversion:
    """Tests for timetable to ScheduleSpec conversion."""

    def test_convert_cron_expression_timetable(self):
        """Test converting a CronTriggerTimetable (has _expression attribute)."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock()
        mock_timetable._expression = "0 0 * * *"

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert spec.cron_expressions == ["0 0 * * *"]

    def test_convert_cron_data_interval_timetable(self):
        """Test converting a CronDataIntervalTimetable (has _cron attribute)."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock(spec=[])  # Empty spec to avoid _expression
        mock_timetable._cron = "30 6 * * *"

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert spec.cron_expressions == ["30 6 * * *"]

    def test_convert_delta_timetable_hourly(self):
        """Test converting a DeltaDataIntervalTimetable with hourly interval."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock(spec=[])
        mock_timetable._delta = timedelta(hours=1)

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert len(spec.intervals) == 1
        assert spec.intervals[0].every == timedelta(hours=1)

    def test_convert_delta_timetable_daily(self):
        """Test converting a DeltaDataIntervalTimetable with daily interval."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock(spec=[])
        mock_timetable._delta = timedelta(days=1)

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert len(spec.intervals) == 1
        assert spec.intervals[0].every == timedelta(days=1)

    def test_convert_delta_timetable_minutes(self):
        """Test converting a DeltaDataIntervalTimetable with minute interval."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock(spec=[])
        mock_timetable._delta = timedelta(minutes=30)

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert len(spec.intervals) == 1
        assert spec.intervals[0].every == timedelta(minutes=30)

    def test_convert_preset_hourly(self):
        """Test converting @hourly preset."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock(spec=[])
        mock_timetable.schedule = "@hourly"

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert spec.cron_expressions == ["0 * * * *"]

    def test_convert_preset_daily(self):
        """Test converting @daily preset."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock(spec=[])
        mock_timetable.schedule = "@daily"

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert spec.cron_expressions == ["0 0 * * *"]

    def test_convert_preset_weekly(self):
        """Test converting @weekly preset."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock(spec=[])
        mock_timetable.schedule = "@weekly"

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert spec.cron_expressions == ["0 0 * * 0"]

    def test_convert_preset_monthly(self):
        """Test converting @monthly preset."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock(spec=[])
        mock_timetable.schedule = "@monthly"

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert spec.cron_expressions == ["0 0 1 * *"]

    def test_convert_preset_yearly(self):
        """Test converting @yearly preset."""
        orchestrator = TemporalOrchestrator()

        mock_timetable = MagicMock(spec=[])
        mock_timetable.schedule = "@yearly"

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is not None
        assert spec.cron_expressions == ["0 0 1 1 *"]

    def test_unconvertible_timetable_returns_none(self):
        """Test that unconvertible timetables return None."""
        orchestrator = TemporalOrchestrator()

        # Mock a timetable with no recognized attributes
        mock_timetable = MagicMock(spec=[])

        spec = orchestrator._convert_timetable_to_spec(mock_timetable)
        assert spec is None


class TestOverlapPolicyMapping:
    """Tests for max_active_runs to overlap policy mapping."""

    def test_max_active_runs_1_maps_to_skip(self):
        """max_active_runs=1 should map to SKIP policy."""
        orchestrator = TemporalOrchestrator()

        mock_dag = MagicMock()
        mock_dag.max_active_runs = 1

        policy = orchestrator._get_overlap_policy(mock_dag)
        assert policy == ScheduleOverlapPolicy.SKIP

    def test_max_active_runs_2_maps_to_buffer_one(self):
        """max_active_runs=2 should map to BUFFER_ONE policy."""
        orchestrator = TemporalOrchestrator()

        mock_dag = MagicMock()
        mock_dag.max_active_runs = 2

        policy = orchestrator._get_overlap_policy(mock_dag)
        assert policy == ScheduleOverlapPolicy.BUFFER_ONE

    def test_max_active_runs_5_maps_to_buffer_one(self):
        """max_active_runs=5 should map to BUFFER_ONE policy (boundary)."""
        orchestrator = TemporalOrchestrator()

        mock_dag = MagicMock()
        mock_dag.max_active_runs = 5

        policy = orchestrator._get_overlap_policy(mock_dag)
        assert policy == ScheduleOverlapPolicy.BUFFER_ONE

    def test_max_active_runs_6_maps_to_buffer_all(self):
        """max_active_runs=6 should map to BUFFER_ALL policy."""
        orchestrator = TemporalOrchestrator()

        mock_dag = MagicMock()
        mock_dag.max_active_runs = 6

        policy = orchestrator._get_overlap_policy(mock_dag)
        assert policy == ScheduleOverlapPolicy.BUFFER_ALL

    def test_max_active_runs_default_maps_to_buffer_all(self):
        """Default max_active_runs (16) should map to BUFFER_ALL policy."""
        orchestrator = TemporalOrchestrator()

        mock_dag = MagicMock()
        mock_dag.max_active_runs = 16

        policy = orchestrator._get_overlap_policy(mock_dag)
        assert policy == ScheduleOverlapPolicy.BUFFER_ALL

    def test_missing_max_active_runs_defaults(self):
        """DAG without max_active_runs attribute uses default."""
        orchestrator = TemporalOrchestrator()

        mock_dag = MagicMock(spec=[])  # No max_active_runs attribute

        policy = orchestrator._get_overlap_policy(mock_dag)
        assert policy == ScheduleOverlapPolicy.BUFFER_ALL


class TestCacheInvalidation:
    """Tests for schedule cache invalidation."""

    def test_invalidate_single_dag(self):
        """Test invalidating cache for a single DAG."""
        orchestrator = TemporalOrchestrator()
        orchestrator._schedule_cache = {"dag1": True, "dag2": True, "dag3": True}

        orchestrator.invalidate_schedule_cache("dag1")

        assert "dag1" not in orchestrator._schedule_cache
        assert "dag2" in orchestrator._schedule_cache
        assert "dag3" in orchestrator._schedule_cache

    def test_invalidate_all_dags(self):
        """Test invalidating cache for all DAGs."""
        orchestrator = TemporalOrchestrator()
        orchestrator._schedule_cache = {"dag1": True, "dag2": True, "dag3": True}

        orchestrator.invalidate_schedule_cache()

        assert len(orchestrator._schedule_cache) == 0

    def test_invalidate_nonexistent_dag(self):
        """Test invalidating cache for a DAG that doesn't exist (no-op)."""
        orchestrator = TemporalOrchestrator()
        orchestrator._schedule_cache = {"dag1": True}

        # Should not raise
        orchestrator.invalidate_schedule_cache("nonexistent")

        assert orchestrator._schedule_cache == {"dag1": True}


class TestNativeSchedulingDisabledBehavior:
    """Tests for orchestrator behavior when native scheduling is disabled."""

    def test_sync_pause_state_noop_when_disabled(self):
        """sync_pause_state should be a no-op when native scheduling is disabled."""
        orchestrator = TemporalOrchestrator()
        orchestrator._native_scheduling_enabled = False

        # Should not raise any exceptions
        orchestrator.sync_pause_state("any_dag", is_paused=True)

    def test_on_dag_deleted_noop_when_disabled(self):
        """on_dag_deleted should be a no-op when native scheduling is disabled."""
        orchestrator = TemporalOrchestrator()
        orchestrator._native_scheduling_enabled = False

        # Should not raise any exceptions
        orchestrator.on_dag_deleted("any_dag")

    def test_on_timetable_changed_noop_when_disabled(self):
        """on_timetable_changed should be a no-op when native scheduling is disabled."""
        orchestrator = TemporalOrchestrator()
        orchestrator._native_scheduling_enabled = False

        mock_dag_model = MagicMock()
        mock_dag_model.dag_id = "test_dag"
        mock_session = MagicMock()

        # Should not raise any exceptions
        orchestrator.on_timetable_changed(mock_dag_model, mock_session)


# =============================================================================
# Integration Tests - Require Airflow and Real Temporal Server
# These tests are marked with pytest.mark.airflow and should be run in
# Docker/Breeze environment where Airflow is installed.
#
# NOTE: Integration tests that use Temporal Schedules API require a REAL
# Temporal server, not the time-skipping test environment. The time-skipping
# environment does not implement DescribeSchedule, CreateSchedule, etc.
#
# To run integration tests:
#   1. Start a real Temporal server: temporal server start-dev
#   2. Set TEMPORAL_TEST_SERVER=real in environment
#   3. Run: pytest test_native_scheduling_e2e.py -v
# =============================================================================

# Try to import Airflow modules, skip integration tests if not available
try:
    import importlib.util
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import asynccontextmanager
    from datetime import datetime, timezone
    from pathlib import Path

    from temporalio.client import Client
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker
    from temporalio.service import RPCError

    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.models.taskinstance import TaskInstance
    from airflow.timetables.base import DataInterval
    from airflow.utils.session import create_session

    from temporal_airflow.activities import run_airflow_task
    from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow
    from temporal_airflow.sync_activities import (
        create_dagrun_record,
        sync_task_status,
        sync_task_status_batch,
        sync_dagrun_status,
        load_serialized_dag,
        ensure_task_instances,
    )

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

# Check if real Temporal server is available (required for Schedule API tests)
def _is_real_temporal_available() -> bool:
    """Check if a real Temporal server is available for Schedule API tests."""
    if os.environ.get("SKIP_TEMPORAL_E2E", "").lower() == "true":
        return False

    addresses_to_try = []
    env_addr = os.environ.get("TEMPORAL_ADDRESS")
    if env_addr:
        addresses_to_try.append(env_addr)

    addresses_to_try.extend([
        "host.docker.internal:7233",
        "172.17.0.1:7233",
        "localhost:7233",
    ])

    import socket
    for addr in addresses_to_try:
        try:
            if ":" in addr:
                hostname, port = addr.rsplit(":", 1)
                port = int(port)
            else:
                hostname = addr
                port = 7233

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((hostname, port))
            sock.close()

            if result == 0:
                os.environ["TEMPORAL_ADDRESS"] = addr
                return True
        except Exception:
            continue

    return False

USE_REAL_TEMPORAL_SERVER = _is_real_temporal_available()

# Skip integration tests if Airflow is not available
airflow_required = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="Airflow not installed - run in Docker/Breeze environment"
)

# Skip tests that require Schedule API (requires real Temporal server, not time-skipping environment)
schedule_api_required = pytest.mark.skipif(
    not USE_REAL_TEMPORAL_SERVER,
    reason="Temporal Schedule API requires real server. Start with: temporal server start-dev"
)

if AIRFLOW_AVAILABLE:
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

    def _load_dag_from_file(dag_file: Path, dag_id: str):
        """Load a DAG from a Python file."""
        from airflow.sdk.definitions.dag import DAG

        spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, DAG) and attr.dag_id == dag_id:
                return attr

        raise ValueError(f"DAG '{dag_id}' not found in {dag_file}")

    def _serialize_dag_to_db(dag):
        """Serialize and save a DAG to the database, returning cleanup function."""
        from airflow.models.dag import DagModel
        from airflow.models.dagbundle import DagBundleModel
        from airflow.models.dag_version import DagVersion
        from airflow.models.dagcode import DagCode
        from airflow.serialization.serialized_objects import LazyDeserializedDAG
        from airflow.serialization.definitions.dag import SerializedDAG

        bundle_name = "test-bundle"
        dag_id = dag.dag_id

        with create_session() as session:
            # Clean up any existing data first
            session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
            session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
            session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id).delete()
            session.query(DagCode).filter(DagCode.dag_id == dag_id).delete()
            session.query(DagVersion).filter(DagVersion.dag_id == dag_id).delete()
            session.query(DagModel).filter(DagModel.dag_id == dag_id).delete()
            session.commit()

            # Create bundle
            existing_bundle = session.query(DagBundleModel).filter(
                DagBundleModel.name == bundle_name
            ).first()
            if not existing_bundle:
                bundle = DagBundleModel(name=bundle_name)
                session.add(bundle)
                session.commit()

            # Create DagModel entry
            SerializedDAG.bulk_write_to_db(
                bundle_name=bundle_name,
                bundle_version=None,
                dags=[dag],
                session=session,
            )
            session.commit()

            # Write serialized DAG
            lazy_dag = LazyDeserializedDAG.from_dag(dag)
            SerializedDagModel.write_dag(
                lazy_dag,
                bundle_name=bundle_name,
                session=session,
            )
            session.commit()

        def cleanup():
            with create_session() as session:
                session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).delete()
                session.query(DagRun).filter(DagRun.dag_id == dag_id).delete()
                session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id).delete()
                session.query(DagCode).filter(DagCode.dag_id == dag_id).delete()
                session.query(DagVersion).filter(DagVersion.dag_id == dag_id).delete()
                session.query(DagModel).filter(DagModel.dag_id == dag_id).delete()
                session.commit()

        return cleanup

    # All activities for the deep workflow
    ALL_ACTIVITIES = [
        create_dagrun_record,
        sync_task_status,
        sync_task_status_batch,
        sync_dagrun_status,
        load_serialized_dag,
        ensure_task_instances,
        run_airflow_task,
    ]

    class NativeSchedulingOrchestrator(TemporalOrchestrator):
        """TemporalOrchestrator that uses an injected test client for native scheduling tests."""

        def __init__(self, test_client: Client, task_queue: str, native_scheduling: bool = True):
            super().__init__()
            self._test_client = test_client
            self._test_task_queue = task_queue
            self._native_scheduling_enabled = native_scheduling
            self._schedule_id_prefix = "test-schedule-"

        async def _get_client(self) -> Client:
            """Return the injected test client."""
            return self._test_client

    async def _cleanup_test_schedule(client: Client, schedule_id: str):
        """Delete a test schedule if it exists."""
        try:
            handle = client.get_schedule_handle(schedule_id)
            await handle.delete()
        except BaseException:
            pass  # Schedule doesn't exist or already deleted, that's fine

    @asynccontextmanager
    async def create_native_scheduling_orchestrator(native_scheduling: bool = True):
        """
        Create a TemporalOrchestrator with native scheduling support for testing.

        Uses real Temporal server if available (required for Schedule API tests),
        otherwise falls back to time-skipping environment.

        Yields:
            tuple: (orchestrator, env) - orchestrator is ready to use with test client
        """
        import time
        from temporal_airflow.client_config import create_temporal_client

        # Use unique task queue to avoid conflicts with other workers
        task_queue = f"test-native-scheduling-{int(time.time())}"
        schedule_id = "test-schedule-test_scheduled_dag"

        if USE_REAL_TEMPORAL_SERVER:
            # Use real Temporal server for Schedule API support
            client = await create_temporal_client()

            # Clean up any existing test schedule before starting
            await _cleanup_test_schedule(client, schedule_id)

            # Create orchestrator with real client
            orchestrator = NativeSchedulingOrchestrator(
                client, task_queue, native_scheduling=native_scheduling
            )

            # Create a mock env object with the client for test compatibility
            class MockEnv:
                def __init__(self, client):
                    self.client = client
            mock_env = MockEnv(client)

            # Start worker
            async with Worker(
                client,
                task_queue=task_queue,
                workflows=[ExecuteAirflowDagDeepWorkflow],
                activities=ALL_ACTIVITIES,
                activity_executor=ThreadPoolExecutor(max_workers=5),
            ):
                try:
                    yield orchestrator, mock_env
                finally:
                    # Clean up the test schedule after test
                    await _cleanup_test_schedule(client, schedule_id)
        else:
            # Fall back to time-skipping environment (no Schedule API support)
            async with await WorkflowEnvironment.start_time_skipping() as env:
                # Create orchestrator with test client
                orchestrator = NativeSchedulingOrchestrator(
                    env.client, task_queue, native_scheduling=native_scheduling
                )

                # Start worker
                async with Worker(
                    env.client,
                    task_queue=task_queue,
                    workflows=[ExecuteAirflowDagDeepWorkflow],
                    activities=ALL_ACTIVITIES,
                    activity_executor=ThreadPoolExecutor(max_workers=5),
                ):
                    yield orchestrator, env

    @airflow_required
    @pytest.mark.usefixtures("airflow_db")
    class TestShouldScheduleDagrunIntegration:
        """Integration tests for should_schedule_dagrun() method."""

        @pytest.mark.asyncio
        async def test_returns_true_when_native_scheduling_disabled(self):
            """When native scheduling is disabled, should always return True."""
            async with create_native_scheduling_orchestrator(native_scheduling=False) as (orchestrator, env):
                # Create mock DagModel
                dag_model = MagicMock(spec=DagModel)
                dag_model.dag_id = "test_dag"
                dag_model.is_paused = False

                # Create mock DataInterval
                data_interval = DataInterval(
                    start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                    end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                )

                # Mock session
                session = MagicMock()

                # Should return True (let Airflow scheduler handle it)
                result = orchestrator.should_schedule_dagrun(dag_model, data_interval, session)
                assert result is True

        @schedule_api_required
        @pytest.mark.asyncio
        async def test_returns_true_for_first_schedule_creation(self):
            """First call should return True and create schedule in background."""
            dag_file = TEST_DAGS_FOLDER / "test_scheduled_dag.py"
            dag = _load_dag_from_file(dag_file, "test_scheduled_dag")
            cleanup = _serialize_dag_to_db(dag)

            try:
                async with create_native_scheduling_orchestrator(native_scheduling=True) as (orchestrator, env):
                    with create_session() as session:
                        dag_model = session.query(DagModel).filter(
                            DagModel.dag_id == "test_scheduled_dag"
                        ).first()

                        if dag_model is None:
                            dag_model = DagModel(dag_id="test_scheduled_dag", is_paused=False)
                            session.add(dag_model)
                            session.commit()

                        data_interval = DataInterval(
                            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                        )

                        # First call - should return True and create schedule
                        result = orchestrator.should_schedule_dagrun(dag_model, data_interval, session)
                        assert result is True

                        # Verify schedule was created (in cache)
                        assert "test_scheduled_dag" in orchestrator._schedule_cache
            finally:
                cleanup()

        @schedule_api_required
        @pytest.mark.asyncio
        async def test_returns_false_when_schedule_exists(self):
            """When schedule already exists, should return False."""
            dag_file = TEST_DAGS_FOLDER / "test_scheduled_dag.py"
            dag = _load_dag_from_file(dag_file, "test_scheduled_dag")
            cleanup = _serialize_dag_to_db(dag)

            try:
                async with create_native_scheduling_orchestrator(native_scheduling=True) as (orchestrator, env):
                    with create_session() as session:
                        dag_model = session.query(DagModel).filter(
                            DagModel.dag_id == "test_scheduled_dag"
                        ).first()

                        if dag_model is None:
                            dag_model = DagModel(dag_id="test_scheduled_dag", is_paused=False)
                            session.add(dag_model)
                            session.commit()

                        data_interval = DataInterval(
                            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                        )

                        # First call creates schedule
                        result1 = orchestrator.should_schedule_dagrun(dag_model, data_interval, session)
                        assert result1 is True

                        # Second call - schedule exists, should return False
                        result2 = orchestrator.should_schedule_dagrun(dag_model, data_interval, session)
                        assert result2 is False
            finally:
                cleanup()

    @airflow_required
    @pytest.mark.usefixtures("airflow_db")
    class TestSyncPauseStateIntegration:
        """Integration tests for sync_pause_state() method."""

        @schedule_api_required
        @pytest.mark.asyncio
        async def test_pause_schedule(self):
            """Test pausing a Temporal Schedule."""
            dag_file = TEST_DAGS_FOLDER / "test_scheduled_dag.py"
            dag = _load_dag_from_file(dag_file, "test_scheduled_dag")
            cleanup = _serialize_dag_to_db(dag)

            try:
                async with create_native_scheduling_orchestrator(native_scheduling=True) as (orchestrator, env):
                    with create_session() as session:
                        dag_model = session.query(DagModel).filter(
                            DagModel.dag_id == "test_scheduled_dag"
                        ).first()

                        if dag_model is None:
                            dag_model = DagModel(dag_id="test_scheduled_dag", is_paused=False)
                            session.add(dag_model)
                            session.commit()

                        data_interval = DataInterval(
                            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                        )

                        # Create schedule first
                        orchestrator.should_schedule_dagrun(dag_model, data_interval, session)

                    # Pause the schedule
                    orchestrator.sync_pause_state("test_scheduled_dag", is_paused=True)

                    # Verify schedule is paused
                    schedule_id = orchestrator._get_schedule_id("test_scheduled_dag")
                    handle = env.client.get_schedule_handle(schedule_id)
                    description = await handle.describe()
                    assert description.schedule.state.paused is True
            finally:
                cleanup()

        @schedule_api_required
        @pytest.mark.asyncio
        async def test_unpause_schedule(self):
            """Test unpausing a Temporal Schedule."""
            dag_file = TEST_DAGS_FOLDER / "test_scheduled_dag.py"
            dag = _load_dag_from_file(dag_file, "test_scheduled_dag")
            cleanup = _serialize_dag_to_db(dag)

            try:
                async with create_native_scheduling_orchestrator(native_scheduling=True) as (orchestrator, env):
                    with create_session() as session:
                        dag_model = session.query(DagModel).filter(
                            DagModel.dag_id == "test_scheduled_dag"
                        ).first()

                        if dag_model is None:
                            dag_model = DagModel(dag_id="test_scheduled_dag", is_paused=True)
                            session.add(dag_model)
                            session.commit()

                        data_interval = DataInterval(
                            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                        )

                        # Create schedule (will be paused initially because dag_model.is_paused=True)
                        orchestrator.should_schedule_dagrun(dag_model, data_interval, session)

                    # Unpause the schedule
                    orchestrator.sync_pause_state("test_scheduled_dag", is_paused=False)

                    # Verify schedule is unpaused
                    schedule_id = orchestrator._get_schedule_id("test_scheduled_dag")
                    handle = env.client.get_schedule_handle(schedule_id)
                    description = await handle.describe()
                    assert description.schedule.state.paused is False
            finally:
                cleanup()

    @airflow_required
    @pytest.mark.usefixtures("airflow_db")
    class TestOnDagDeletedIntegration:
        """Integration tests for on_dag_deleted() method."""

        @schedule_api_required
        @pytest.mark.asyncio
        async def test_deletes_schedule(self):
            """Test that on_dag_deleted deletes the Temporal Schedule."""
            dag_file = TEST_DAGS_FOLDER / "test_scheduled_dag.py"
            dag = _load_dag_from_file(dag_file, "test_scheduled_dag")
            cleanup = _serialize_dag_to_db(dag)

            try:
                async with create_native_scheduling_orchestrator(native_scheduling=True) as (orchestrator, env):
                    with create_session() as session:
                        dag_model = session.query(DagModel).filter(
                            DagModel.dag_id == "test_scheduled_dag"
                        ).first()

                        if dag_model is None:
                            dag_model = DagModel(dag_id="test_scheduled_dag", is_paused=False)
                            session.add(dag_model)
                            session.commit()

                        data_interval = DataInterval(
                            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                        )

                        # Create schedule first
                        orchestrator.should_schedule_dagrun(dag_model, data_interval, session)

                    # Verify schedule exists
                    schedule_id = orchestrator._get_schedule_id("test_scheduled_dag")
                    handle = env.client.get_schedule_handle(schedule_id)
                    description = await handle.describe()
                    assert description is not None

                    # Delete via on_dag_deleted
                    orchestrator.on_dag_deleted("test_scheduled_dag")

                    # Verify schedule is deleted
                    with pytest.raises(RPCError) as exc_info:
                        await handle.describe()
                    assert "not found" in str(exc_info.value).lower()

                    # Verify cache is cleared
                    assert "test_scheduled_dag" not in orchestrator._schedule_cache
            finally:
                cleanup()

    @airflow_required
    @pytest.mark.usefixtures("airflow_db")
    class TestOnTimetableChangedIntegration:
        """Integration tests for on_timetable_changed() method."""

        @schedule_api_required
        @pytest.mark.asyncio
        async def test_updates_schedule_spec(self):
            """Test that on_timetable_changed updates the schedule spec."""
            dag_file = TEST_DAGS_FOLDER / "test_scheduled_dag.py"
            dag = _load_dag_from_file(dag_file, "test_scheduled_dag")
            cleanup = _serialize_dag_to_db(dag)

            try:
                async with create_native_scheduling_orchestrator(native_scheduling=True) as (orchestrator, env):
                    with create_session() as session:
                        dag_model = session.query(DagModel).filter(
                            DagModel.dag_id == "test_scheduled_dag"
                        ).first()

                        if dag_model is None:
                            dag_model = DagModel(dag_id="test_scheduled_dag", is_paused=False)
                            session.add(dag_model)
                            session.commit()

                        data_interval = DataInterval(
                            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
                            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
                        )

                        # Create schedule first
                        orchestrator.should_schedule_dagrun(dag_model, data_interval, session)

                        # Get original schedule spec
                        schedule_id = orchestrator._get_schedule_id("test_scheduled_dag")
                        handle = env.client.get_schedule_handle(schedule_id)
                        description = await handle.describe()
                        original_spec = description.schedule.spec

                        # Call on_timetable_changed (simulating timetable update)
                        orchestrator.on_timetable_changed(dag_model, session)

                        # Verify the update was called (schedule still exists)
                        description_after = await handle.describe()
                        assert description_after is not None
            finally:
                cleanup()
