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
"""Temporal orchestrator for routing DagRun execution to Temporal workflows."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import nest_asyncio

# Allow nested asyncio.run() calls for sync-async bridge
# This is needed because orchestrator methods are called synchronously
# from Airflow, but use async Temporal client internally
nest_asyncio.apply()

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleSpec,
    ScheduleIntervalSpec,
    ScheduleState,
    SchedulePolicy,
    ScheduleOverlapPolicy,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from temporalio.common import RetryPolicy
from temporalio.service import RPCError

from airflow.orchestrators.base_orchestrator import BaseDagRunOrchestrator
from airflow.utils.types import DagRunType

from temporal_airflow.client_config import create_temporal_client, get_task_queue
from temporal_airflow.models import DeepDagExecutionInput
from temporal_airflow.deep_workflow import ExecuteAirflowDagDeepWorkflow

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.timetables.base import DataInterval

log = logging.getLogger(__name__)


def _get_native_scheduling_enabled() -> bool:
    """Check if native Temporal scheduling is enabled via environment variable."""
    return os.environ.get("TEMPORAL_NATIVE_SCHEDULING", "false").lower() in ("true", "1", "yes")


def _get_schedule_id_prefix() -> str:
    """Get the prefix for Temporal Schedule IDs."""
    return os.environ.get("TEMPORAL_SCHEDULE_ID_PREFIX", "airflow-schedule-")


class TemporalOrchestrator(BaseDagRunOrchestrator):
    """
    Routes ALL DagRun execution to Temporal workflows.

    When configured as the orchestrator, every DagRun created in Airflow
    will be executed via Temporal instead of the traditional scheduler+executor.

    Configuration:
        Set in airflow.cfg:

        [core]
        orchestrator = temporal_airflow.orchestrator.TemporalOrchestrator

    Environment variables for native scheduling:
        TEMPORAL_NATIVE_SCHEDULING: Enable native Temporal Schedules (default: false)
        TEMPORAL_SCHEDULE_ID_PREFIX: Prefix for schedule IDs (default: airflow-schedule-)

    The DagRun is marked as EXTERNAL so the Airflow scheduler ignores it.
    Temporal then owns the entire execution lifecycle.

    Note: This orchestrator is synchronous but uses asyncio internally
    to interact with the Temporal client.
    """

    def __init__(self):
        """Initialize the orchestrator."""
        self._client: Client | None = None
        self._lock = asyncio.Lock()
        # Cache of schedule existence: dag_id -> True/False
        self._schedule_cache: dict[str, bool] = {}
        self._native_scheduling_enabled = _get_native_scheduling_enabled()
        self._schedule_id_prefix = _get_schedule_id_prefix()

    async def _get_client(self) -> Client:
        """Get or create the Temporal client (async, with lock for thread safety)."""
        async with self._lock:
            if self._client is None:
                self._client = await create_temporal_client()
                log.info("Created Temporal client")
            return self._client

    def _get_schedule_id(self, dag_id: str) -> str:
        """Get the Temporal Schedule ID for a DAG."""
        return f"{self._schedule_id_prefix}{dag_id}"

    def start_dagrun(self, dag_run: DagRun, session: Session) -> None:
        """
        Start Temporal workflow for this DagRun using deep integration.

        This method:
        1. Marks the DagRun as EXTERNAL so the scheduler ignores it
        2. Starts a deep integration Temporal workflow

        The deep workflow:
        - Loads serialized DAG from Airflow DB via activity
        - Syncs status back to Airflow DB for UI visibility
        - Uses real Airflow DB for connections/variables

        :param dag_run: The DagRun to orchestrate
        :param session: Database session for DB operations
        """
        # Mark as EXTERNAL so scheduler ignores this run
        dag_run.run_type = DagRunType.EXTERNAL
        session.merge(dag_run)

        # Build workflow input for deep integration
        # Note: No serialized_dag passed - workflow loads it via activity
        # Manual triggers in Airflow 3.x may have logical_date=None
        logical_date = dag_run.logical_date or datetime.now(timezone.utc)
        workflow_input = DeepDagExecutionInput(
            dag_id=dag_run.dag_id,
            logical_date=logical_date,
            run_id=dag_run.run_id,  # Pass existing run_id
            conf=dag_run.conf or {},
        )

        # Start the Temporal workflow
        workflow_id = f"airflow-{dag_run.dag_id}-{dag_run.run_id}"

        try:
            asyncio.run(self._start_workflow_async(workflow_id, workflow_input))
            log.info(
                "Started Temporal deep workflow %s for DagRun %s/%s",
                workflow_id,
                dag_run.dag_id,
                dag_run.run_id,
            )
        except Exception as e:
            log.exception(
                "Failed to start Temporal workflow for DagRun %s/%s: %s",
                dag_run.dag_id,
                dag_run.run_id,
                e,
            )
            # Re-raise to let caller handle the failure
            raise

    async def _start_workflow_async(
        self,
        workflow_id: str,
        input: DeepDagExecutionInput,
    ) -> None:
        """Start the Temporal deep integration workflow (async implementation)."""
        client = await self._get_client()
        task_queue = get_task_queue()

        await client.start_workflow(
            ExecuteAirflowDagDeepWorkflow.run,
            input,
            id=workflow_id,
            task_queue=task_queue,
        )

    def cancel_dagrun(self, dag_run: DagRun, session: Session) -> None:
        """
        Cancel a running DagRun by cancelling its Temporal workflow.

        :param dag_run: The DagRun to cancel
        :param session: Database session for DB operations
        """
        workflow_id = f"airflow-{dag_run.dag_id}-{dag_run.run_id}"

        try:
            asyncio.run(self._cancel_workflow_async(workflow_id))
            log.info(
                "Cancelled Temporal workflow %s for DagRun %s/%s",
                workflow_id,
                dag_run.dag_id,
                dag_run.run_id,
            )
        except Exception as e:
            log.exception(
                "Failed to cancel Temporal workflow for DagRun %s/%s: %s",
                dag_run.dag_id,
                dag_run.run_id,
                e,
            )
            # Update DB state to failed even if workflow cancel fails
            from airflow.utils.state import DagRunState

            dag_run.state = DagRunState.FAILED
            session.merge(dag_run)

    async def _cancel_workflow_async(self, workflow_id: str) -> None:
        """Cancel a Temporal workflow (async implementation)."""
        client = await self._get_client()
        handle = client.get_workflow_handle(workflow_id)
        await handle.cancel()

    # =========================================================================
    # Native Scheduling Methods (Phase 2)
    # =========================================================================

    def should_schedule_dagrun(
        self,
        dag_model: DagModel,
        data_interval: DataInterval,
        session: Session,
    ) -> bool:
        """
        Check if the scheduler should create a DagRun for this scheduled execution.

        When native scheduling is enabled and a Temporal Schedule exists for this DAG,
        returns False to let Temporal handle scheduling. Otherwise returns True for
        the default Airflow behavior.

        :param dag_model: The DagModel for the DAG being scheduled
        :param data_interval: The data interval for the potential DagRun
        :param session: Database session for any DB operations
        :return: True if scheduler should create DagRun, False to skip
        """
        if not self._native_scheduling_enabled:
            # Native scheduling disabled - use default Airflow behavior
            return True

        # Check if DAG is asset-triggered (cannot use Temporal schedules for these)
        if self._is_asset_triggered(dag_model, session):
            log.debug(
                "DAG %s is asset-triggered, using Airflow scheduler",
                dag_model.dag_id,
            )
            return True

        # Check if Temporal Schedule exists for this DAG
        if self._has_temporal_schedule(dag_model.dag_id):
            log.info(
                "Temporal Schedule exists for DAG %s, skipping Airflow DagRun creation",
                dag_model.dag_id,
            )
            return False

        # No schedule exists yet - try to create one
        # If creation succeeds, future iterations will skip
        # If creation fails, fall back to Airflow scheduling
        try:
            asyncio.run(self._ensure_schedule_exists(dag_model, data_interval, session))
            # After creating schedule, still return True this first time
            # to create the initial DagRun via existing flow
            # Next iteration will find the schedule and return False
            return True
        except Exception as e:
            log.warning(
                "Failed to create Temporal Schedule for DAG %s, falling back to Airflow: %s",
                dag_model.dag_id,
                e,
            )
            return True

    def _is_asset_triggered(self, dag_model: DagModel, session: Session) -> bool:
        """Check if DAG uses asset-triggered timetable."""
        from airflow.models.serialized_dag import SerializedDagModel

        try:
            # Check for asset-triggered timetables
            from airflow.timetables.assets import AssetTriggeredTimetable

            serdag = SerializedDagModel.get(dag_model.dag_id, session)
            if not serdag:
                return False

            return isinstance(serdag.dag.timetable, AssetTriggeredTimetable)
        except ImportError:
            # AssetTriggeredTimetable not available in older Airflow versions
            return False

    def _has_temporal_schedule(self, dag_id: str) -> bool:
        """Check if a Temporal Schedule exists for this DAG."""
        # Check cache first
        if dag_id in self._schedule_cache:
            return self._schedule_cache[dag_id]

        # Query Temporal
        schedule_id = self._get_schedule_id(dag_id)
        try:
            exists = asyncio.run(self._check_schedule_exists_async(schedule_id))
            self._schedule_cache[dag_id] = exists
            return exists
        except Exception as e:
            log.warning(
                "Failed to check Temporal Schedule existence for %s: %s",
                dag_id,
                e,
            )
            return False

    async def _check_schedule_exists_async(self, schedule_id: str) -> bool:
        """Check if a schedule exists in Temporal."""
        client = await self._get_client()
        try:
            handle = client.get_schedule_handle(schedule_id)
            await handle.describe()
            return True
        except RPCError as e:
            if "not found" in str(e).lower():
                return False
            raise

    async def _ensure_schedule_exists(
        self,
        dag_model: DagModel,
        data_interval: DataInterval,
        session: Session,
    ) -> None:
        """Create Temporal Schedule for this DAG if it doesn't exist."""
        from airflow.models.serialized_dag import SerializedDagModel

        serdag = SerializedDagModel.get(dag_model.dag_id, session)
        if not serdag:
            log.warning("No serialized DAG found for %s", dag_model.dag_id)
            return

        dag = serdag.dag
        schedule_id = self._get_schedule_id(dag_model.dag_id)

        # Convert Airflow timetable to Temporal spec
        spec = self._convert_timetable_to_spec(dag.timetable)
        if spec is None:
            log.warning(
                "Cannot convert timetable for DAG %s to Temporal spec",
                dag_model.dag_id,
            )
            return

        client = await self._get_client()
        task_queue = get_task_queue()

        try:
            await client.create_schedule(
                schedule_id,
                Schedule(
                    action=ScheduleActionStartWorkflow(
                        ExecuteAirflowDagDeepWorkflow.run,
                        DeepDagExecutionInput(
                            dag_id=dag_model.dag_id,
                            logical_date=datetime.now(timezone.utc),  # Will be overwritten
                            run_id=None,  # Generated by workflow
                            conf={},
                        ),
                        id=f"airflow-{dag_model.dag_id}-scheduled-{{{{.ScheduledTime.Format \"20060102T150405\"}}}}",
                        task_queue=task_queue,
                    ),
                    spec=spec,
                    state=ScheduleState(
                        paused=dag_model.is_paused,
                        note=f"Auto-created for Airflow DAG: {dag_model.dag_id}",
                    ),
                    policy=SchedulePolicy(
                        overlap=self._get_overlap_policy(dag),
                        catchup_window=timedelta(days=0) if not getattr(dag, 'catchup', True) else timedelta(days=365),
                    ),
                ),
            )
            self._schedule_cache[dag_model.dag_id] = True
            log.info("Created Temporal Schedule %s for DAG %s", schedule_id, dag_model.dag_id)
        except RPCError as e:
            if "already exists" in str(e).lower():
                self._schedule_cache[dag_model.dag_id] = True
                log.debug("Temporal Schedule %s already exists", schedule_id)
            else:
                raise

    def _convert_timetable_to_spec(self, timetable) -> ScheduleSpec | None:
        """Convert Airflow timetable to Temporal ScheduleSpec."""
        # Handle CronTriggerTimetable
        if hasattr(timetable, '_expression'):
            return ScheduleSpec(cron_expressions=[timetable._expression])

        # Handle CronDataIntervalTimetable (schedule_interval as cron)
        if hasattr(timetable, '_cron'):
            return ScheduleSpec(cron_expressions=[str(timetable._cron)])

        # Handle DeltaDataIntervalTimetable (timedelta-based)
        if hasattr(timetable, '_delta'):
            delta = timetable._delta
            if isinstance(delta, timedelta):
                return ScheduleSpec(
                    intervals=[ScheduleIntervalSpec(every=delta)]
                )

        # Handle common schedule_interval patterns
        # Check for schedule string attribute
        if hasattr(timetable, 'schedule'):
            sched = timetable.schedule
            if isinstance(sched, str) and sched.startswith('@'):
                # Handle presets like @daily, @hourly
                cron_map = {
                    '@hourly': '0 * * * *',
                    '@daily': '0 0 * * *',
                    '@weekly': '0 0 * * 0',
                    '@monthly': '0 0 1 * *',
                    '@yearly': '0 0 1 1 *',
                    '@annually': '0 0 1 1 *',
                }
                if sched in cron_map:
                    return ScheduleSpec(cron_expressions=[cron_map[sched]])

        # Cannot convert - caller should fall back to Airflow scheduling
        log.debug(
            "Cannot convert timetable type %s to Temporal spec",
            type(timetable).__name__,
        )
        return None

    def _get_overlap_policy(self, dag) -> ScheduleOverlapPolicy:
        """Map Airflow max_active_runs to Temporal overlap policy."""
        max_runs = getattr(dag, 'max_active_runs', 16)
        if max_runs == 1:
            return ScheduleOverlapPolicy.SKIP
        elif max_runs <= 5:
            return ScheduleOverlapPolicy.BUFFER_ONE
        else:
            return ScheduleOverlapPolicy.BUFFER_ALL

    def sync_pause_state(self, dag_id: str, is_paused: bool) -> None:
        """
        Sync DAG pause state to Temporal Schedule.

        Called when a DAG's is_paused state changes in the Airflow UI/API.
        Pauses or unpauses the corresponding Temporal Schedule.

        :param dag_id: The DAG ID whose pause state changed
        :param is_paused: True if DAG was paused, False if unpaused
        """
        if not self._native_scheduling_enabled:
            return

        # Only sync if we have a schedule for this DAG
        if not self._has_temporal_schedule(dag_id):
            log.debug(
                "No Temporal Schedule for DAG %s, skipping pause sync",
                dag_id,
            )
            return

        schedule_id = self._get_schedule_id(dag_id)
        try:
            asyncio.run(self._sync_pause_async(schedule_id, is_paused))
            log.info(
                "Synced pause state for DAG %s: %s",
                dag_id,
                "paused" if is_paused else "unpaused",
            )
        except Exception as e:
            log.exception(
                "Failed to sync pause state for DAG %s: %s",
                dag_id,
                e,
            )

    async def _sync_pause_async(self, schedule_id: str, is_paused: bool) -> None:
        """Pause or unpause a Temporal Schedule."""
        client = await self._get_client()
        handle = client.get_schedule_handle(schedule_id)

        if is_paused:
            await handle.pause(note="Paused via Airflow UI")
        else:
            await handle.unpause(note="Unpaused via Airflow UI")

    def on_dag_deleted(self, dag_id: str) -> None:
        """
        Handle DAG deletion cleanup.

        Deletes the corresponding Temporal Schedule when a DAG is deleted.

        :param dag_id: The DAG ID that was deleted
        """
        if not self._native_scheduling_enabled:
            return

        # Remove from cache
        self._schedule_cache.pop(dag_id, None)

        schedule_id = self._get_schedule_id(dag_id)
        try:
            asyncio.run(self._delete_schedule_async(schedule_id))
            log.info(
                "Deleted Temporal Schedule %s for DAG %s",
                schedule_id,
                dag_id,
            )
        except Exception as e:
            # Don't fail if schedule doesn't exist
            if "not found" not in str(e).lower():
                log.exception(
                    "Failed to delete Temporal Schedule for DAG %s: %s",
                    dag_id,
                    e,
                )

    async def _delete_schedule_async(self, schedule_id: str) -> None:
        """Delete a Temporal Schedule."""
        client = await self._get_client()
        handle = client.get_schedule_handle(schedule_id)
        await handle.delete()

    def on_timetable_changed(self, dag_model: DagModel, session: Session) -> None:
        """
        Handle DAG timetable/schedule change.

        Updates the Temporal Schedule when a DAG's timetable changes.

        :param dag_model: The DagModel with updated timetable information
        :param session: Database session for any DB operations
        """
        if not self._native_scheduling_enabled:
            return

        # Only update if we have a schedule for this DAG
        if not self._has_temporal_schedule(dag_model.dag_id):
            log.debug(
                "No Temporal Schedule for DAG %s, skipping timetable update",
                dag_model.dag_id,
            )
            return

        schedule_id = self._get_schedule_id(dag_model.dag_id)
        try:
            asyncio.run(self._update_schedule_async(dag_model, session, schedule_id))
            log.info(
                "Updated Temporal Schedule %s for DAG %s",
                schedule_id,
                dag_model.dag_id,
            )
        except Exception as e:
            log.exception(
                "Failed to update Temporal Schedule for DAG %s: %s",
                dag_model.dag_id,
                e,
            )

    async def _update_schedule_async(
        self,
        dag_model: DagModel,
        session: Session,
        schedule_id: str,
    ) -> None:
        """Update a Temporal Schedule with new timetable."""
        from airflow.models.serialized_dag import SerializedDagModel

        serdag = SerializedDagModel.get(dag_model.dag_id, session)
        if not serdag:
            log.warning("No serialized DAG found for %s", dag_model.dag_id)
            return

        dag = serdag.dag
        new_spec = self._convert_timetable_to_spec(dag.timetable)
        if new_spec is None:
            log.warning(
                "Cannot convert new timetable for DAG %s to Temporal spec, "
                "deleting schedule to fall back to Airflow",
                dag_model.dag_id,
            )
            # Delete schedule so DAG falls back to Airflow scheduling
            await self._delete_schedule_async(schedule_id)
            self._schedule_cache.pop(dag_model.dag_id, None)
            return

        client = await self._get_client()
        handle = client.get_schedule_handle(schedule_id)

        async def update_schedule(input: ScheduleUpdateInput) -> ScheduleUpdate:
            """Update callback for schedule."""
            return ScheduleUpdate(
                schedule=Schedule(
                    action=input.description.schedule.action,
                    spec=new_spec,
                    state=input.description.schedule.state,
                    policy=SchedulePolicy(
                        overlap=self._get_overlap_policy(dag),
                        catchup_window=timedelta(days=0) if not getattr(dag, 'catchup', True) else timedelta(days=365),
                    ),
                )
            )

        await handle.update(update_schedule)

    def invalidate_schedule_cache(self, dag_id: str | None = None) -> None:
        """
        Invalidate the schedule cache for a DAG or all DAGs.

        Useful for testing or when external changes are made to schedules.

        :param dag_id: Specific DAG ID to invalidate, or None to clear all
        """
        if dag_id is None:
            self._schedule_cache.clear()
        else:
            self._schedule_cache.pop(dag_id, None)
