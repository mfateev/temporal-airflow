# Temporal Airflow Integration

Execute Apache Airflow DAGs as Temporal workflows with full durability guarantees.

## Overview

This project provides deep integration between Apache Airflow and Temporal, allowing you to:

- Execute Airflow DAGs as durable Temporal workflows
- Get automatic retry and failure recovery for your data pipelines
- Maintain Airflow's familiar DAG authoring experience
- Leverage Temporal's workflow history, visibility, and debugging capabilities

## Installation

```bash
pip install temporal-airflow
```

## Quick Start

```python
from temporal_airflow import submit_dag

# Submit an Airflow DAG to run as a Temporal workflow
await submit_dag("my_dag_id")
```

## Documentation

See the [docs/](docs/) directory for detailed design documents and implementation plans.

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

## License

Apache License 2.0
