# Temporal Airflow Integration

Execute Apache Airflow DAGs as Temporal workflows with full durability guarantees.

## Overview

This project provides deep integration between Apache Airflow and Temporal, allowing you to:

- Execute Airflow DAGs as durable Temporal workflows
- Get automatic retry and failure recovery for your data pipelines
- Maintain Airflow's familiar DAG authoring experience
- Leverage Temporal's workflow history, visibility, and debugging capabilities

## Requirements

This integration requires a **fork of Apache Airflow** with pluggability extensions (orchestrator hooks, DagRunType.EXTERNAL). The Docker build process automatically clones this fork.

| Dependency | Source |
|------------|--------|
| Apache Airflow | [mfateev/airflow](https://github.com/mfateev/airflow) branch `temporal-pluggable-scheduler` |
| Temporal | Any Temporal server (dev server included in docker-compose) |

## Quick Start

```bash
# Clone this repository
git clone https://github.com/mfateev/temporal-airflow.git
cd temporal-airflow

# Build and start all services
cd docker
./start.sh

# Access UIs:
# - Airflow: http://localhost:8080
# - Temporal: http://localhost:8233
```

See **[SETUP.md](SETUP.md)** for detailed instructions including:
- Manual build steps
- Running tests
- Configuration options
- Troubleshooting

## How It Works

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

When a DAG is triggered (via UI, CLI, or schedule), the `TemporalOrchestrator` intercepts the DagRun and starts a Temporal workflow. Task execution happens in Temporal activities, with status synced back to Airflow's database for UI visibility.

## Features

- **Full Airflow Compatibility**: DAGs work unchanged - same operators, same syntax
- **Provider Support**: Use any Airflow provider (HTTP, AWS, GCP, etc.) - they work seamlessly
- **Sensor Support**: Airflow sensors use efficient poke() + retry pattern
- **XCom Support**: Task results passed between tasks via Temporal
- **Connections & Variables**: Read from Airflow database or environment
- **Trigger Rules**: All Airflow trigger rules supported via native scheduling logic
- **Parallel Execution**: Independent tasks run concurrently

## Example DAGs

The `examples/` directory contains sample DAGs demonstrating various features:

- `provider_http_example.py` - HTTP provider with HttpSensor and HttpOperator
- `test_sensors.py` - Custom sensors with poke() patterns
- `test_parallel.py` - Parallel task execution
- `test_scheduled_dag.py` - Scheduled DAG execution

## Documentation

- **[SETUP.md](SETUP.md)** - Build and run instructions
- **[docs/](docs/)** - Design documents and architecture

## Development

```bash
# Build test image
docker build -f docker/Dockerfile.test -t airflow-temporal:test .

# Run tests (uses Temporal time-skipping test environment)
docker run --rm airflow-temporal:test

# Run with local changes
docker run --rm \
  -v $(pwd)/src/temporal_airflow:/opt/airflow/temporal_airflow \
  -v $(pwd)/examples:/opt/airflow/examples \
  airflow-temporal:test pytest temporal_airflow/tests/ -v

# Run E2E tests with real Temporal server (includes Schedule API tests)
docker run --rm --network host \
  -e SKIP_TEMPORAL_E2E=false \
  -e TEMPORAL_ADDRESS=localhost:7233 \
  airflow-temporal:test
```

## License

Apache License 2.0
