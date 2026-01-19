# High-Level Plan: Implementing Airflow Features with Temporal

**Date**: 2026-01-18
**Status**: Planning Document
**Purpose**: Comprehensive mapping of all Airflow features to Temporal implementation

---

## Executive Summary

This document provides a complete plan for implementing Apache Airflow features using Temporal as the execution engine. The integration preserves Airflow's user experience (UI, configuration, DAG authoring) while leveraging Temporal's durability, reliability, and workflow orchestration capabilities.

**Key Principle**: Temporal owns execution, Airflow owns user experience.

---

## Implementation Status Overview

| Category | Status | Notes |
|----------|--------|-------|
| Core Execution | ✅ Complete | DAG execution via Temporal workflows |
| Task Dependencies | ✅ Complete | Uses Airflow's native TriggerRuleDep |
| Manual Triggers | ✅ Complete | UI/API routed to Temporal |
| Scheduled Triggers | ✅ Complete | Scheduler routed to Temporal |
| XCom | ✅ Complete | Workflow state + DB sync |
| Connections/Variables | ✅ Complete | Read from Airflow DB |
| Native Scheduling Hooks | ✅ Complete | Airflow fork has all hooks |
| Native Scheduling Impl | ✅ Complete | TemporalOrchestrator methods implemented |
| Sensors/Deferrable | 📋 Planned | Temporal signals |
| Dynamic Tasks | 📋 Planned | Child workflows |
| **Parallelism Limits** | ⚠️ Limitation | Not implemented |

---

## Table of Contents

1. [Core Execution Features](#1-core-execution-features)
2. [Scheduling Features](#2-scheduling-features)
3. [Task Dependencies & Control Flow](#3-task-dependencies--control-flow)
4. [Data Passing](#4-data-passing)
5. [Operators & Task Types](#5-operators--task-types)
6. [Retry & Timeout Features](#6-retry--timeout-features)
7. [Monitoring & Alerting](#7-monitoring--alerting)
8. [Configuration Management](#8-configuration-management)
9. [Advanced Features](#9-advanced-features)
10. [Known Limitations](#10-known-limitations)

---

## 1. Core Execution Features

### 1.1 DAG Execution ✅ Complete

**Airflow Feature**: Execute a DAG as a collection of tasks with dependencies.

**Temporal Implementation**:
- `ExecuteAirflowDagDeepWorkflow` orchestrates DAG execution
- In-workflow SQLite database holds Airflow models
- Reuses Airflow's native `dag_run.update_state()` for scheduling decisions
- Activities execute individual tasks

**Files**:
- `src/temporal_airflow/deep_workflow.py`

### 1.2 Task Instance Execution ✅ Complete

**Airflow Feature**: Execute individual tasks (operators).

**Temporal Implementation**:
- `run_airflow_task` activity executes operators
- Task context (logical_date, ti, etc.) passed via activity input
- Hooks automatically read connections from Airflow DB
- Results synced back to Airflow DB for UI visibility

**Files**:
- `src/temporal_airflow/activities.py`

### 1.3 DagRun State Management ✅ Complete

**Airflow Feature**: Track DagRun state (QUEUED, RUNNING, SUCCESS, FAILED).

**Temporal Implementation**:
- Workflow state is source of truth during execution
- Sync activities write state to Airflow DB
- `DagRunType.EXTERNAL` marks runs as Temporal-managed
- Scheduler ignores EXTERNAL runs

**Files**:
- `src/temporal_airflow/sync_activities.py`
- Airflow fork: `DagRunType.EXTERNAL` enum

---

## 2. Scheduling Features

### 2.1 Time-Based Scheduling (Cron/Interval) ✅ Complete

**Airflow Feature**: Trigger DAGs based on cron expressions or intervals.

**Two Scheduling Modes Available**:

**Mode 1: Airflow-Driven Scheduling (Default)**
- Airflow scheduler evaluates timetables
- Creates DagRun, calls `orchestrator.start_dagrun()`
- Temporal workflow executes the DAG

**Mode 2: Native Temporal Scheduling** ✅ Complete
- Enable with `TEMPORAL_NATIVE_SCHEDULING=true`
- Temporal Schedules handle timing natively
- More reliable, no dependency on Airflow scheduler for timing

**Implemented TemporalOrchestrator Methods**:
- ✅ `should_schedule_dagrun()` - Creates Temporal Schedule on first call, returns False thereafter
- ✅ `sync_pause_state()` - Pauses/unpauses Temporal Schedule when DAG state changes
- ✅ `on_dag_deleted()` - Deletes Temporal Schedule when DAG is removed
- ✅ `on_timetable_changed()` - Updates Temporal Schedule spec when timetable changes

**Configuration**:
- `TEMPORAL_NATIVE_SCHEDULING` - Enable/disable (default: disabled)
- `TEMPORAL_SCHEDULE_ID_PREFIX` - Custom prefix (default: `airflow-schedule-`)

**Timetable Conversion Support**:
- Cron expressions (`CronTriggerTimetable`, `CronDataIntervalTimetable`)
- Delta intervals (`DeltaDataIntervalTimetable`)
- Presets (`@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`)

**Overlap Policy Mapping** (from `max_active_runs`):
- `max_active_runs=1` → `SKIP`
- `max_active_runs=2-5` → `BUFFER_ONE`
- `max_active_runs>5` → `BUFFER_ALL`

**Files**:
- `src/temporal_airflow/orchestrator.py`
- `src/temporal_airflow/tests/test_native_scheduling_e2e.py`

### 2.2 Asset-Triggered DAGs ✅ Complete (via Orchestrator)

**Airflow Feature**: Trigger DAGs when upstream assets (datasets) are updated.

**Temporal Implementation**:
- Asset triggers go through normal scheduler path
- Scheduler creates DagRun, calls `orchestrator.start_dagrun()`
- Temporal workflow executes the DAG
- Asset detection remains in Airflow scheduler

**Note**: Asset detection cannot use Temporal Schedules (event-driven, not time-based).

### 2.3 Manual Triggers ✅ Complete

**Airflow Feature**: Trigger DAGs via UI, API, or CLI.

**Temporal Implementation**:
- API endpoint calls `orchestrator.start_dagrun()`
- `TemporalOrchestrator` marks run as EXTERNAL
- Starts Temporal workflow with DAG parameters

**Files**:
- `src/temporal_airflow/orchestrator.py`
- Airflow fork: `dag_run.py:trigger_dag_run()`

### 2.4 Backfill 📋 Planned

**Airflow Feature**: Run historical DAG executions for date ranges.

**Temporal Implementation Options**:

**Option A: Airflow CLI (Recommended for compatibility)**
- Use existing `airflow dags backfill` command
- Creates DagRuns with `run_type=backfill`
- Orchestrator routes to Temporal

**Option B: Temporal Schedule Backfill**
- Use `schedule.backfill()` API
- Triggers workflows for historical times
- Workflows create DagRuns in Airflow DB

**Implementation Steps**:
1. Ensure `DagRunType.BACKFILL_JOB` is handled by orchestrator
2. Test with existing CLI
3. Optional: Add Temporal Schedule backfill support

### 2.5 Catchup 📋 Planned

**Airflow Feature**: Automatically run missed scheduled executions.

**Temporal Implementation**:
- With Airflow-driven scheduling: Works automatically (scheduler creates missed runs)
- With Native Temporal Scheduling: Configure catchup in Schedule policy

**Implementation Steps**:
1. Map `dag.catchup` to Temporal Schedule catchup policy
2. Handle `catchup=False` by skipping past intervals

---

## 3. Task Dependencies & Control Flow

### 3.1 Basic Dependencies (>>/<< operators) ✅ Complete

**Airflow Feature**: Define task execution order.

**Temporal Implementation**:
- Dependencies stored in serialized DAG
- `dag_run.update_state()` evaluates dependencies via `TriggerRuleDep`
- Tasks scheduled only when upstream tasks complete

### 3.2 Trigger Rules ✅ Complete

**Airflow Feature**: Control when downstream tasks run based on upstream states.

All 13 trigger rules supported:

| Trigger Rule | Status | Implementation |
|--------------|--------|----------------|
| `all_success` | ✅ | Via `update_state()` |
| `all_failed` | ✅ | Via `update_state()` |
| `all_done` | ✅ | Via `update_state()` |
| `all_skipped` | ✅ | Via `update_state()` |
| `one_success` | ✅ | Via `update_state()` |
| `one_failed` | ✅ | Via `update_state()` |
| `one_done` | ✅ | Via `update_state()` |
| `none_failed` | ✅ | Via `update_state()` |
| `none_failed_min_one_success` | ✅ | Via `update_state()` |
| `none_skipped` | ✅ | Via `update_state()` |
| `always` | ✅ | Via `update_state()` |
| `dummy` (deprecated) | ✅ | Via `update_state()` |
| `no_trigger` | ✅ | Via `update_state()` |

**Key Insight**: By reusing Airflow's `TriggerRuleDep` class, we get identical behavior to native Airflow.

### 3.3 Task Groups ✅ Complete

**Airflow Feature**: Visually group related tasks.

**Temporal Implementation**:
- Task groups are a UI/serialization concept
- Individual tasks within groups execute normally
- No special Temporal handling needed

### 3.4 Branching (BranchPythonOperator) 📋 Planned

**Airflow Feature**: Conditionally execute one of multiple downstream paths.

**Temporal Implementation**:
- Branch operator returns task_id(s) to execute
- Other tasks marked as SKIPPED
- Workflow updates state via `update_state()` which handles skipping

**Implementation Steps**:
1. Detect branch operator in activity
2. Return branch result as XCom
3. Mark non-selected tasks as SKIPPED in workflow
4. `TriggerRuleDep` handles downstream correctly

### 3.5 ShortCircuit Operator 📋 Planned

**Airflow Feature**: Skip all downstream tasks if condition is False.

**Temporal Implementation**:
- Similar to branching
- If condition False, mark all downstream as SKIPPED
- Workflow handles state propagation

**Implementation Steps**:
1. Detect ShortCircuitOperator type
2. Check return value (truthy/falsy)
3. If falsy, skip downstream tasks

### 3.6 Dynamic Task Mapping (expand()) 📋 Planned (Medium Priority)

**Airflow Feature**: Dynamically generate task instances at runtime.

**Temporal Implementation Options**:

**Option A: Expanded in Workflow (Recommended)**
```python
# In workflow scheduling loop:
if task.is_mapped:
    expand_values = get_expand_values(task, xcom_store)
    for idx, value in enumerate(expand_values):
        ti = TaskInstance(task_id=task.task_id, map_index=idx)
        # ... create and schedule each mapped instance
```

**Option B: Child Workflows**
- Each mapped task group becomes a child workflow
- Parent workflow waits for children

**Implementation Steps**:
1. Detect mapped tasks during scheduling
2. Resolve expand values from XCom
3. Create TaskInstance for each map_index
4. Execute as parallel activities
5. Aggregate results

### 3.7 Cross-DAG Dependencies (ExternalTaskSensor) 📋 Planned

**Airflow Feature**: Wait for tasks in other DAGs to complete.

**Temporal Implementation**:
- Sensor activity polls Airflow DB for external task state
- Uses activity heartbeat for long-running polls
- Consider Temporal signals for push-based notification

**Implementation Steps**:
1. Implement `external_task_sensor` activity
2. Poll Airflow DB for target TaskInstance state
3. Use heartbeat timeout to prevent stuck activities
4. Optional: Signal-based notification from completing workflows

### 3.8 SubDAGs (Deprecated) ⚠️ Low Priority

**Airflow Feature**: Nest DAGs within DAGs (deprecated in Airflow 2.x).

**Temporal Implementation**:
- Recommend using Task Groups instead
- If needed: SubDAG becomes child workflow

---

## 4. Data Passing

### 4.1 XCom ✅ Complete

**Airflow Feature**: Share data between tasks.

**Temporal Implementation**:
- XCom stored in workflow state (`self.xcom_store`)
- Passed to downstream activities via `upstream_results`
- Synced to Airflow DB for UI visibility

**Files**:
- `deep_workflow.py:_get_upstream_xcom()`
- `sync_activities.py:sync_task_status()` (with XCom)

### 4.2 XCom with TaskFlow API (@task) 📋 Planned

**Airflow Feature**: Automatic XCom passing via function returns.

**Temporal Implementation**:
- TaskFlow tasks become PythonOperator during serialization
- Return value automatically becomes XCom
- Already works with current implementation

**Implementation Steps**:
1. Verify TaskFlow DAGs serialize correctly
2. Test return value → XCom flow
3. Test @task decorated functions

### 4.3 Custom XCom Backend 📋 Planned (Low Priority)

**Airflow Feature**: Store XCom in external systems (S3, etc.).

**Temporal Implementation**:
- For small data: Use workflow state (current approach)
- For large data: Activity writes to external storage, XCom contains reference

**Implementation Steps**:
1. Check XCom size in activity
2. If over threshold, write to configured storage
3. Store reference in XCom
4. Downstream activities resolve reference

---

## 5. Operators & Task Types

### 5.1 Standard Operators ✅ Complete

All standard operators work via `run_airflow_task` activity:

| Operator | Status | Notes |
|----------|--------|-------|
| BashOperator | ✅ | Executes bash commands |
| PythonOperator | ✅ | Executes Python callables |
| EmptyOperator | ✅ | No-op placeholder |
| EmailOperator | ✅ | Requires SMTP connection |

### 5.2 Provider Operators ✅ Complete (with providers installed)

Provider operators work if:
1. Provider package installed in worker
2. Required connections configured in Airflow DB

Examples: `GCSOperator`, `S3Operator`, `BigQueryOperator`, etc.

### 5.3 Sensors 📋 Planned (High Priority)

**Airflow Feature**: Wait for external conditions.

**Sensor Modes**:

**Mode: poke (Traditional)**
- Activity polls at intervals
- Uses Temporal heartbeat timeout
- Long-running activity

**Mode: reschedule**
- Activity returns "reschedule" state
- Workflow schedules retry after poke_interval
- More efficient for long waits

**Implementation for `mode='poke'`**:
```python
@activity.defn
async def run_sensor_task(input: SensorTaskInput) -> TaskExecutionResult:
    while True:
        activity.heartbeat()
        if sensor.poke(context):
            return TaskExecutionResult(state=SUCCESS)
        await asyncio.sleep(input.poke_interval)
```

**Implementation for `mode='reschedule'`**:
```python
# In workflow:
result = await execute_activity(run_sensor_task, ...)
if result.state == "up_for_reschedule":
    await workflow.sleep(timedelta(seconds=poke_interval))
    # Re-schedule the sensor activity
```

### 5.4 Deferrable Operators (Async) 📋 Planned (High Priority)

**Airflow Feature**: Operators that yield control while waiting.

**Temporal Implementation**:
- Deferrable operators use Temporal signals
- Trigger creates external trigger in Airflow + workflow signal handler
- External event sends signal to workflow
- Workflow resumes operator execution

**Architecture**:
```
Operator.execute() -> raises TaskDeferred(trigger=...)
                              |
                              v
Workflow registers signal handler for trigger
                              |
                              v
External system sends Temporal signal
                              |
                              v
Workflow resumes, calls operator.execute_complete()
```

**Implementation Steps**:
1. Detect TaskDeferred exception in activity
2. Return deferred state with trigger info
3. Workflow registers signal handler
4. Create Trigger record in Airflow DB
5. Trigger service (or external) sends signal
6. Workflow resumes operator

### 5.5 Branch Operators 📋 Planned

**Operators**: BranchPythonOperator, BranchSQLOperator, etc.

**Temporal Implementation**:
- Execute branch operator
- Return selected task_id(s)
- Workflow marks non-selected paths as SKIPPED
- `update_state()` propagates skips correctly

---

## 6. Retry & Timeout Features

### 6.1 Task Retries ✅ Complete

**Airflow Feature**: Automatically retry failed tasks.

**Temporal Implementation**:
- Map `task.retries` to Temporal `RetryPolicy`
- Map `retry_delay` to `initial_interval`
- Map `retry_exponential_backoff` to backoff coefficient

```python
retry_policy = RetryPolicy(
    initial_interval=timedelta(seconds=task.retry_delay.total_seconds()),
    maximum_attempts=task.retries + 1,  # Temporal counts initial attempt
    backoff_coefficient=2.0 if task.retry_exponential_backoff else 1.0,
    maximum_interval=timedelta(seconds=task.max_retry_delay) if task.max_retry_delay else None,
)
```

### 6.2 Task Execution Timeout ✅ Complete

**Airflow Feature**: Kill tasks that run too long.

**Temporal Implementation**:
- Map `execution_timeout` to `start_to_close_timeout`
- Activity cancelled after timeout

```python
await workflow.execute_activity(
    run_airflow_task,
    start_to_close_timeout=task.execution_timeout or timedelta(hours=2),
)
```

### 6.3 Retry Callbacks 📋 Planned

**Airflow Feature**: `on_retry_callback` function.

**Temporal Implementation**:
- Execute callback before Temporal retry
- Or: Execute callback as separate activity

### 6.4 Task Instance State Transitions 📋 Planned

Ensure all Airflow states are mapped:

| Airflow State | Temporal Mapping | Notes |
|---------------|------------------|-------|
| `none` | Not started | Initial state |
| `scheduled` | Activity queued | Task ready to run |
| `queued` | Activity queued | In execution queue |
| `running` | Activity running | Executing |
| `success` | Activity completed | Task succeeded |
| `failed` | Activity failed | After retries exhausted |
| `up_for_retry` | Temporal retrying | Between retry attempts |
| `up_for_reschedule` | Workflow sleep | Sensor reschedule mode |
| `skipped` | Workflow marked | Branch/trigger rule |
| `upstream_failed` | Workflow marked | Upstream dependency failed |
| `deferred` | Waiting signal | Deferrable operator |
| `removed` | Not applicable | Task removed from DAG |

---

## 7. Monitoring & Alerting

### 7.1 Task Callbacks ✅ Partial

**Airflow Feature**: Execute callbacks on task state changes.

| Callback | Status | Implementation |
|----------|--------|----------------|
| `on_success_callback` | 📋 Planned | Execute in activity after task success |
| `on_failure_callback` | 📋 Planned | Execute in activity after task failure |
| `on_retry_callback` | 📋 Planned | Execute before Temporal retry |
| `on_skipped_callback` | 📋 Planned | Execute in workflow when marking skipped |

**Implementation Steps**:
1. Check if task has callbacks
2. Execute callback function with context
3. Catch and log callback errors (don't fail task)

### 7.2 DAG Callbacks 📋 Planned

**Airflow Feature**: Execute callbacks on DAG run completion.

| Callback | Implementation |
|----------|----------------|
| `on_success_callback` | Activity at workflow end (success) |
| `on_failure_callback` | Activity at workflow end (failure) |

### 7.3 SLA Monitoring 📋 Planned

**Airflow Feature**: Alert when tasks miss SLA.

**Temporal Implementation**:
- Workflow tracks task start times
- Compare against `sla` parameter
- If exceeded: Execute SLA miss callback activity
- Write SLA miss record to Airflow DB

**Implementation Steps**:
1. Track task expected completion time
2. Use Temporal timer to detect SLA breach
3. Execute `sla_miss_callback` if defined
4. Write to `sla_miss` table in Airflow DB

### 7.4 Alerting (Email) 📋 Planned

**Airflow Feature**: Send email on task/DAG failure.

**Temporal Implementation**:
- Check `email_on_failure`, `email_on_retry`
- Execute email sending as activity
- Uses Airflow's SMTP configuration

---

## 8. Configuration Management

### 8.1 Connections ✅ Complete

**Airflow Feature**: Securely store credentials.

**Temporal Implementation**:
- Connections stored in Airflow DB
- Hooks read via `BaseHook.get_connection()`
- Worker needs `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`

### 8.2 Variables ✅ Complete

**Airflow Feature**: Store configuration values.

**Temporal Implementation**:
- Variables stored in Airflow DB
- Read via `Variable.get()`
- Worker needs DB access

### 8.3 Secrets Backend 📋 Planned

**Airflow Feature**: Read secrets from external systems (Vault, AWS SM, etc.).

**Temporal Implementation**:
- Configure secrets backend in `airflow.cfg`
- Workers inherit Airflow configuration
- No special Temporal handling needed

---

## 9. Advanced Features

### 9.1 DAG Versioning 📋 Planned

**Airflow Feature**: Track DAG changes over time.

**Temporal Implementation**:
- Workflow receives DAG version in input
- Version stored with DagRun
- UI shows which version executed

### 9.2 Clear/Rerun Tasks 📋 Planned

**Airflow Feature**: Clear task state to re-execute.

**Temporal Implementation Options**:

**Option A: Cancel & Restart Workflow**
- Cancel running workflow
- Start new workflow for same DagRun
- Simple but loses partial progress

**Option B: Temporal Continue-As-New**
- Workflow continues with cleared tasks
- Preserves completed task results
- More complex implementation

**Option C: Signal to Workflow**
- Send signal with tasks to re-run
- Workflow marks tasks as pending
- Re-executes specific tasks

### 9.3 Task Mapping Results 📋 Planned

**Airflow Feature**: Access all results from mapped task.

**Temporal Implementation**:
- Store mapped results in workflow XCom store
- Key by `(dag_id, task_id, run_id, map_index)`
- Downstream gets list of all results

### 9.4 Setup/Teardown Tasks 📋 Planned (Airflow 2.7+)

**Airflow Feature**: Run setup before and teardown after task groups.

**Temporal Implementation**:
- Detect setup/teardown relationships
- Execute setup before group tasks
- Execute teardown after group tasks (even on failure)

### 9.5 Asset/Dataset Lineage 📋 Planned

**Airflow Feature**: Track data lineage through DAGs.

**Temporal Implementation**:
- Capture inlet/outlet assets from task
- Write lineage events to Airflow DB
- Asset events trigger downstream DAGs (via scheduler)

---

## 10. Known Limitations

The following Airflow features are **intentionally not implemented** in the Temporal integration:

### 10.1 Pool-Based Parallelism Limits ⚠️ NOT IMPLEMENTED

**Airflow Feature**: Limit concurrent tasks using pools.

**Why Not Implemented**:
- Temporal workers have their own concurrency model
- Pool semantics don't map cleanly to Temporal
- Would require global state across workflows

**Workaround**: Use Temporal worker `max_concurrent_activities` setting.

### 10.2 Task Slot Limits ⚠️ NOT IMPLEMENTED

**Airflow Feature**: `pool_slots` parameter on tasks.

**Why Not Implemented**:
- Related to pool-based limits
- Temporal activities don't have slot concept

**Workaround**: Run resource-intensive tasks on dedicated task queues.

### 10.3 DAG-Level Concurrency (`max_active_runs`) ⚠️ PARTIAL

**Airflow Feature**: Limit concurrent runs of same DAG.

**Current Behavior**:
- Airflow scheduler still enforces `max_active_runs` when creating DagRuns
- With Native Temporal Scheduling: Use Schedule overlap policies

**Note**: Schedule overlap policies (SKIP, BUFFER_ONE, BUFFER_ALL) provide similar functionality.

### 10.4 Global Parallelism (`parallelism` config) ⚠️ NOT IMPLEMENTED

**Airflow Feature**: Global limit on concurrent tasks.

**Why Not Implemented**:
- Temporal distributes work across workers
- No centralized task counting

**Workaround**: Configure worker capacity and scaling policies.

### 10.5 Priority Weight ⚠️ NOT IMPLEMENTED

**Airflow Feature**: Prioritize tasks in the queue.

**Why Not Implemented**:
- Temporal task queues are FIFO
- No priority mechanism in standard Temporal

**Workaround**: Use multiple task queues with different worker allocations.

### 10.6 Cluster Policies ⚠️ NOT APPLICABLE

**Airflow Feature**: Mutate DAG/task parameters at parse time.

**Why Not Applicable**:
- DAG parsing still happens in Airflow
- Policies apply before Temporal sees the DAG
- Effectively "works" because policies run in Airflow

---

## Implementation Phases

### Phase 1: Foundation ✅ COMPLETE
- Core DAG execution workflow
- Task dependency handling via TriggerRuleDep
- Manual and scheduled triggers
- Basic operators (Bash, Python)
- XCom support
- Connections and Variables

### Phase 2: Scheduling Enhancements ✅ COMPLETE

**Airflow Fork (Complete)**:
- ✅ `should_schedule_dagrun()` hook in scheduler
- ✅ `sync_pause_state()` hook in DAG API
- ✅ `on_dag_deleted()` hook in delete API
- ✅ `on_timetable_changed()` hook in DAG processor

**TemporalOrchestrator Implementation (Complete)**:
- ✅ Temporal Schedule creation/management
- ✅ `should_schedule_dagrun()` - creates Schedule, returns False when exists
- ✅ `sync_pause_state()` - pauses/unpauses Temporal Schedule
- ✅ `on_dag_deleted()` - deletes Temporal Schedule
- ✅ `on_timetable_changed()` - updates Temporal Schedule spec
- ✅ Timetable to ScheduleSpec conversion (cron, delta, presets)
- ✅ Overlap policy mapping from max_active_runs
- ✅ Schedule existence cache for performance
- ✅ Graceful fallback to Airflow when Schedule API fails

**Remaining (Lower Priority)**:
- 📋 Backfill support via Temporal Schedule backfill API
- 📋 Catchup handling via Temporal Schedule policies

### Phase 3: Advanced Operators
- Sensors (poke and reschedule modes)
- Deferrable operators (Temporal signals)
- Branch operators
- ShortCircuit operators

### Phase 4: Dynamic Features
- Dynamic task mapping (expand())
- Cross-DAG dependencies
- Setup/teardown tasks

### Phase 5: Monitoring & Callbacks
- Task callbacks (on_success, on_failure, on_retry)
- DAG callbacks
- SLA monitoring
- Email alerting

### Phase 6: Polish & Edge Cases
- Clear/rerun functionality
- Task state transitions
- Asset lineage
- Custom XCom backends

---

## Appendix: Feature Matrix

| Feature | Airflow | Temporal | Status |
|---------|---------|----------|--------|
| DAG Execution | DagRun + Executor | Workflow | ✅ |
| Task Execution | TaskInstance | Activity | ✅ |
| Dependencies | upstream_task_ids | Workflow logic | ✅ |
| Trigger Rules | TriggerRuleDep | update_state() | ✅ |
| Time Scheduling | Timetable | Schedule/Scheduler | ✅ |
| Asset Triggers | AssetEvent | Scheduler+Orchestrator | ✅ |
| Manual Triggers | API/UI | Orchestrator | ✅ |
| Retries | retries param | RetryPolicy | ✅ |
| Timeouts | execution_timeout | start_to_close_timeout | ✅ |
| XCom | XCom table | Workflow state | ✅ |
| Connections | Connection table | Airflow DB | ✅ |
| Variables | Variable table | Airflow DB | ✅ |
| Sensors | Sensor.poke() | Activity + heartbeat | 📋 |
| Deferrable | TaskDeferred | Signals | 📋 |
| Dynamic Tasks | expand() | Multiple activities | 📋 |
| Branching | BranchOperator | Workflow logic | 📋 |
| Callbacks | on_* callbacks | Activities | 📋 |
| SLA | sla param | Workflow timer | 📋 |
| Pools | Pool table | ⚠️ Limitation | ❌ |
| Priority | priority_weight | ⚠️ Limitation | ❌ |

---

## Conclusion

This plan provides a comprehensive roadmap for implementing Airflow features using Temporal. The current implementation covers core execution, scheduling, and data passing. Remaining work focuses on advanced operators (sensors, deferrable), dynamic features, and monitoring.

**Key architectural decisions**:
1. **Reuse Airflow's scheduling logic** - `update_state()` and `TriggerRuleDep` ensure identical behavior
2. **In-workflow database** - Enables native Airflow code to run in workflow context
3. **Sync activities** - Keep Airflow DB updated for UI visibility
4. **Parallelism is a limitation** - Pool-based limits don't map to Temporal's model

The integration successfully separates concerns: Airflow handles user experience and configuration, Temporal handles durable execution.
