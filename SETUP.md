# Temporal Airflow Integration - Setup Guide

This guide explains how to build and run the Temporal Airflow integration from this repository.

## Prerequisites

- Docker and Docker Compose
- Git
- ~4GB available memory for containers

## Dependencies

This integration requires a **fork of Apache Airflow** with pluggability extensions that are not yet in upstream Airflow. The Dockerfile automatically clones this fork during the build process.

### Airflow Fork Details

| Repository | Branch | Purpose |
|------------|--------|---------|
| [mfateev/airflow](https://github.com/mfateev/airflow) | `temporal-pluggable-scheduler` | Adds pluggable orchestrator support |

The fork adds these features to Airflow:
- **Pluggable Orchestrator**: Extension point that allows external systems (like Temporal) to intercept and handle DAG execution
- **DagRunType.EXTERNAL**: Marks DAGs triggered by external orchestrators so Airflow's scheduler doesn't interfere
- **Pluggable Time Provider**: Allows external time management for testing

These changes are designed to be contributed upstream to Apache Airflow.

## Quick Start

### Step 1: Clone This Repository

```bash
git clone https://github.com/mfateev/temporal-airflow.git
cd temporal-airflow
```

### Step 2: Build the Docker Image

```bash
cd docker
./start.sh
```

This script:
1. Builds the `airflow-temporal:latest` image (clones the Airflow fork automatically)
2. Starts all services via docker-compose

**First build takes 5-10 minutes** as it:
- Clones the Airflow fork
- Copies UI assets from official Airflow 3.1.6 image
- Installs all Python dependencies

### Step 3: Access the UIs

Once services are healthy:
- **Airflow UI**: http://localhost:8080
- **Temporal UI**: http://localhost:8233

No login required (dev mode with all-admin auth).

### Step 4: Trigger a Test DAG

1. Open Airflow UI at http://localhost:8080
2. Enable any DAG (toggle to "Active")
3. Click "Trigger DAG"
4. Watch execution in Temporal UI at http://localhost:8233

## Manual Build (Alternative)

If you prefer manual steps:

```bash
# From repository root
cd temporal-airflow

# Build the image
docker build -t airflow-temporal:latest -f docker/Dockerfile .

# Start services
cd docker
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f
```

## Running Tests

### Build the Test Image

```bash
# From repository root
docker build -f docker/Dockerfile.test -t temporal-airflow-test .
```

### Run All Tests

```bash
docker run --rm temporal-airflow-test
```

### Run Specific Tests

```bash
# Sensor tests only
docker run --rm temporal-airflow-test pytest temporal_airflow/tests/test_sensors.py -v

# With local code changes (development)
docker run --rm \
  -v $(pwd)/src/temporal_airflow:/opt/airflow/temporal_airflow \
  -v $(pwd)/examples:/opt/examples \
  temporal-airflow-test pytest temporal_airflow/tests/test_sensors.py -v
```

## Project Structure

```
temporal-airflow/
├── src/temporal_airflow/     # Main package
│   ├── activities.py         # Task execution activities
│   ├── workflows.py          # Temporal workflow definitions
│   ├── orchestrator.py       # Airflow orchestrator plugin
│   ├── models.py             # Pydantic models
│   ├── worker.py             # Temporal worker
│   └── tests/                # Test suite
├── examples/                 # Example DAGs
├── docker/
│   ├── Dockerfile            # Main image (includes Airflow fork)
│   ├── Dockerfile.test       # Test image
│   ├── docker-compose.yaml   # Full stack
│   ├── start.sh              # Convenience script
│   └── dags/                 # Sample DAGs for docker-compose
└── docs/                     # Design documents
```

## Services Architecture

| Service | Port | Description |
|---------|------|-------------|
| `temporal` | 7233 (gRPC), 8233 (UI) | Temporal server (dev mode) |
| `postgres` | 5432 | Airflow metadata database |
| `airflow-apiserver` | 8080 | Airflow UI and REST API |
| `airflow-dag-processor` | - | Parses DAG files |
| `airflow-scheduler` | - | Creates scheduled DagRuns |
| `temporal-worker` | - | Executes Temporal workflows |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPORAL_ADDRESS` | `temporal:7233` | Temporal server address |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `airflow-tasks` | Task queue name |
| `AIRFLOW__CORE__ORCHESTRATOR` | - | Set to `temporal_airflow.orchestrator.TemporalOrchestrator` |

### Using a Different Airflow Fork

If you have your own fork with the pluggability changes:

```bash
docker build \
  --build-arg AIRFLOW_REPO=https://github.com/YOUR_USERNAME/airflow.git \
  --build-arg AIRFLOW_BRANCH=your-branch \
  -t airflow-temporal:latest \
  -f docker/Dockerfile .
```

## Common Operations

```bash
cd docker

# Start services
./start.sh

# Start without rebuilding
./start.sh --no-build

# Force rebuild
./start.sh --rebuild

# View logs
./start.sh --logs
# or
docker-compose logs -f temporal-worker

# Stop services
./start.sh --down

# Stop and remove volumes
docker-compose down -v
```

## Troubleshooting

### Build Fails on ARM Mac (M1/M2/M3)

The image builds for both amd64 and arm64. If you encounter issues:

```bash
# Force platform
docker build --platform linux/amd64 -t airflow-temporal:latest -f docker/Dockerfile .
```

### Container Name Conflict

```bash
docker-compose down
docker rm -f temporal postgres airflow-apiserver airflow-dag-processor airflow-scheduler temporal-worker airflow-init 2>/dev/null
docker-compose up -d
```

### DAG Not Appearing in UI

1. Check DAG processor logs: `docker-compose logs airflow-dag-processor`
2. Verify DAG file is in `docker/dags/` directory
3. Check for Python syntax errors in the DAG file

### Workflow Not Starting

1. Check Temporal worker: `docker-compose logs temporal-worker`
2. Verify Temporal server: `docker exec temporal temporal operator cluster health`

## Development Workflow

### Making Changes to temporal-airflow

1. Edit files in `src/temporal_airflow/`
2. Run tests with volume mount:
   ```bash
   docker run --rm \
     -v $(pwd)/src/temporal_airflow:/opt/airflow/temporal_airflow \
     -v $(pwd)/examples:/opt/examples \
     temporal-airflow-test pytest temporal_airflow/tests/ -v
   ```
3. For full integration testing, rebuild the base image:
   ```bash
   docker build -t airflow-temporal:latest -f docker/Dockerfile .
   ```

### Adding New DAGs

1. Create DAG file in `docker/dags/`
2. DAG processor will automatically pick it up
3. Enable and trigger from Airflow UI

## Current Limitations

- **Development Mode Only**: Uses Temporal dev server with SQLite. For production, use Temporal Cloud or a production deployment.
- **Experimental**: This is a prototype integration. Report issues at the [GitHub repository](https://github.com/mfateev/temporal-airflow/issues).

## Further Reading

- [Deep Integration Design](docs/DEEP_INTEGRATION_DESIGN.md) - Architecture details
- [Implementation Plan](docs/DEEP_INTEGRATION_IMPLEMENTATION_PLAN.md) - Development roadmap
- [Temporal Documentation](https://docs.temporal.io/)
- [Airflow Documentation](https://airflow.apache.org/docs/)
