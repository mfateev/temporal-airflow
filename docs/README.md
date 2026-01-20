# Airflow with Temporal Orchestration

> **Setup Instructions**: See [SETUP.md](../SETUP.md) in the repository root for build and run instructions.

This document explains the architecture of running Apache Airflow with Temporal as the execution engine.

## Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        STANDARD AIRFLOW                               │
│   User → Airflow UI/API → Scheduler → Executor → Worker → Tasks      │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                    AIRFLOW + TEMPORAL                                 │
│   User → Airflow API Server ──→ Temporal Workflow                    │
│              (creates DagRun)       ↓                                │
│                    Airflow DB ← Temporal Worker → Tasks              │
└──────────────────────────────────────────────────────────────────────┘
```

**Key difference:** When the API server (or scheduler) creates a DagRun, the configured `TemporalOrchestrator` routes it directly to Temporal for execution.

**Benefits:**
- Temporal handles retries, timeouts, and failure recovery
- Full visibility in Temporal UI for debugging workflows
- Airflow UI shows familiar DAG status (synced from Temporal in real-time)
- Connections and Variables work from Airflow DB
- Concurrent DAG execution tested and verified

## Architecture

### Services

| Service | Port | Description |
|---------|------|-------------|
| `temporal` | 7233 (gRPC), 8233 (UI) | Temporal server (dev mode with SQLite) |
| `postgres` | 5432 | Airflow metadata database |
| `airflow-apiserver` | 8080 | Airflow UI/API - routes DagRuns to Temporal |
| `airflow-dag-processor` | - | Parses DAG files, serializes to DB |
| `airflow-scheduler` | - | Evaluates timetables, creates scheduled DagRuns |
| `temporal-worker` | - | Executes Temporal workflows/activities |

### Components

```
src/temporal_airflow/
├── orchestrator.py      # TemporalOrchestrator - routes DagRuns to Temporal
├── workflows.py         # ExecuteAirflowDagWorkflow - main workflow
├── deep_workflow.py     # ExecuteAirflowDagDeepWorkflow - workflow with DB sync
├── deep_worker.py       # Worker startup with import pre-warming
├── activities.py        # run_airflow_task - executes individual tasks
├── sync_activities.py   # DB sync activities for Airflow UI visibility
├── models.py            # Pydantic models for workflow/activity I/O
├── client_config.py     # Temporal client configuration
└── submit_dag.py        # CLI for direct DAG submission
```

### Data Flow

```
1. DAG Trigger (UI/CLI/API)
   └─→ API Server/Scheduler calls dag.create_dagrun()
       └─→ TemporalOrchestrator.start_dagrun()
           └─→ Starts Temporal workflow (async)
               └─→ Returns immediately with DagRun info

2. Temporal Workflow Execution (async, in background)
   ├─→ load_serialized_dag (reads DAG structure from Airflow DB)
   ├─→ Creates in-workflow SQLite DB for Airflow's scheduling logic
   ├─→ ensure_task_instances (creates TaskInstance records in DB)
   ├─→ Scheduling loop using dag_run.update_state():
   │   ├─→ Airflow's TriggerRuleDep evaluates which tasks are ready
   │   ├─→ run_airflow_task (execute task in ThreadPoolExecutor)
   │   └─→ sync_task_status_batch (batch sync to Airflow DB)
   └─→ sync_dagrun_status (mark DagRun complete in Airflow DB)

3. Airflow UI reads status from DB (continuously updated by workflow)
```

### Key Design Decisions

1. **In-Workflow Database**: Each workflow creates an in-memory SQLite database to run Airflow's native scheduling logic (`dag_run.update_state()`, `TriggerRuleDep`). This ensures trigger rules work correctly.

2. **Sandboxed=False**: The workflow runs with `sandboxed=False` to allow Airflow's complex module system to work without serialization issues.

3. **ThreadPoolExecutor for Activities**: Sync activities (database operations) run in a ThreadPoolExecutor to avoid blocking Temporal's event loop.

4. **Import Pre-warming**: The worker pre-warms Python imports and deserializes a sample DAG at startup to avoid Temporal's deadlock detector triggering on cold imports (~1.4s).

5. **Batched Syncs**: Task status updates are batched into single DB transactions to reduce round-trips.

6. **DAG Caching**: DAG modules are cached at the worker level to avoid re-parsing Python files on every activity invocation, improving sensor performance.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_ADDRESS` | `temporal:7233` | Temporal server address |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `airflow-tasks` | Task queue name |
| `AIRFLOW__CORE__ORCHESTRATOR` | `DefaultOrchestrator` | Set to `temporal_airflow.orchestrator.TemporalOrchestrator` |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | - | PostgreSQL connection string |

### Airflow Configuration

To enable Temporal orchestration for UI-triggered DAGs, set via environment variable:
```bash
export AIRFLOW__CORE__ORCHESTRATOR="temporal_airflow.orchestrator.TemporalOrchestrator"
```

Or add to `airflow.cfg`:
```ini
[core]
orchestrator = temporal_airflow.orchestrator.TemporalOrchestrator
```

## Triggering DAGs

### Via Airflow UI (Recommended)

1. Navigate to http://localhost:8080
2. Find your DAG
3. Toggle the DAG to "Active" (unpause it)
4. Click the "Play" button → "Trigger DAG"

**What happens behind the scenes:**
1. Airflow creates a DagRun in the database
2. The `TemporalOrchestrator` intercepts the DagRun
3. A Temporal workflow is started
4. Task status is synced back to Airflow DB for UI visibility

### Via Airflow CLI

```bash
docker exec airflow-apiserver airflow dags trigger example_dag
docker exec airflow-apiserver airflow dags list-runs -d example_dag
```

### Via Airflow REST API

```bash
curl -X POST "http://localhost:8080/api/v2/dags/example_dag/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{"conf": {}}'
```

## Monitoring Execution

### In Airflow UI

1. Go to http://localhost:8080
2. Click on your DAG name
3. View the Grid or Graph tab to see task status
4. Click on a task instance for details

### In Temporal UI

1. Go to http://localhost:8233
2. Click "Workflows" in the sidebar
3. Find your workflow (format: `airflow-{dag_id}-{run_id}`)
4. View workflow history, activity details, retry attempts, and payloads

## Current Limitations

1. **Development Mode Only**: The docker-compose uses Temporal's dev server with SQLite. For production, use Temporal Cloud or a production Temporal deployment.

2. **Experimental**: This is a prototype integration. Some Airflow features may not work as expected.

## Further Reading

- [Deep Integration Design](DEEP_INTEGRATION_DESIGN.md) - Architecture details
- [Standalone Mode](STANDALONE_MODE.md) - Running without Airflow DB
- [Implementation Plan](DEEP_INTEGRATION_IMPLEMENTATION_PLAN.md) - Development roadmap
- [Temporal Documentation](https://docs.temporal.io/)
- [Airflow Documentation](https://airflow.apache.org/docs/)
