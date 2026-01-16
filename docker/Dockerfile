# Dockerfile for Airflow with Temporal Orchestrator
#
# This builds an Airflow image with:
# 1. Core Airflow from mfateev/airflow (temporal-pluggable-scheduler branch)
#    - Includes DagRunType.EXTERNAL
#    - Includes pluggable orchestrator extension
#    - Includes pluggable time provider
# 2. temporal-airflow package for the Temporal integration
#
# Build:
#   docker build -t airflow-temporal:latest .
#
# Or with custom airflow repo:
#   docker build --build-arg AIRFLOW_REPO=https://github.com/your-fork/airflow.git \
#                --build-arg AIRFLOW_BRANCH=your-branch \
#                -t airflow-temporal:latest .

FROM python:3.12-slim

ARG AIRFLOW_REPO=https://github.com/mfateev/airflow.git
ARG AIRFLOW_BRANCH=temporal-pluggable-scheduler

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    libpq-dev \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Create airflow user
RUN useradd -ms /bin/bash -d /opt/airflow -u 50000 airflow

WORKDIR /opt/airflow

# Clone Airflow with pluggability changes
RUN git clone --depth 1 --branch ${AIRFLOW_BRANCH} ${AIRFLOW_REPO} /tmp/airflow

# Copy required packages
RUN cp -r /tmp/airflow/shared /opt/airflow/shared && \
    cp -r /tmp/airflow/task-sdk /opt/airflow/task-sdk && \
    cp -r /tmp/airflow/airflow-core /opt/airflow/airflow-core && \
    chown -R airflow:0 /opt/airflow

# Build the Airflow UI
WORKDIR /opt/airflow/airflow-core/src/airflow/ui
RUN npm install --legacy-peer-deps && npm run build

# Copy pre-built Auth UI from official Airflow image
COPY --from=apache/airflow:3.0.0 \
    /home/airflow/.local/lib/python3.12/site-packages/airflow/api_fastapi/auth/managers/simple/ui/dist \
    /opt/airflow/airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui/dist

WORKDIR /opt/airflow

# Install build tools
RUN pip install --no-cache-dir hatchling hatch-vcs editables

# Install Airflow packages
RUN pip install --no-cache-dir --no-build-isolation \
    -e /opt/airflow/task-sdk \
    -e /opt/airflow/airflow-core

# Install runtime dependencies
RUN pip install --no-cache-dir \
    temporalio \
    psycopg2-binary \
    asyncpg \
    pydantic \
    structlog

# Remove any conflicting apache-airflow package and reinstall local
RUN pip uninstall -y apache-airflow 2>/dev/null || true && \
    pip install --no-cache-dir --no-build-isolation --force-reinstall --no-deps \
    -e /opt/airflow/task-sdk \
    -e /opt/airflow/airflow-core

# Copy temporal-airflow package
COPY --chown=airflow:0 src/temporal_airflow /opt/airflow/temporal_airflow

# Set environment
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow

# Switch to airflow user
USER airflow

# Create necessary directories
RUN mkdir -p /opt/airflow/{dags,logs,plugins}

EXPOSE 8080

# Default command
CMD ["airflow", "version"]
