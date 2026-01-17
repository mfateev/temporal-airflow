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
    && rm -rf /var/lib/apt/lists/*

# Create airflow user
RUN useradd -ms /bin/bash -d /opt/airflow -u 50000 airflow

WORKDIR /opt/airflow

# Clone Airflow with pluggability changes
RUN git clone --depth 1 --branch ${AIRFLOW_BRANCH} ${AIRFLOW_REPO} /tmp/airflow

# Copy required packages (including shared directory which _shared symlinks to)
RUN cp -r /tmp/airflow/shared /opt/airflow/shared && \
    cp -r /tmp/airflow/task-sdk /opt/airflow/task-sdk && \
    cp -r /tmp/airflow/airflow-core /opt/airflow/airflow-core && \
    chown -R airflow:0 /opt/airflow

# Copy pre-built UI assets from official Airflow image (avoids memory-intensive npm build)
# Using 3.1.6 for better API compatibility with 3.2.0-dev fork
COPY --from=apache/airflow:3.1.6 \
    /home/airflow/.local/lib/python3.12/site-packages/airflow/ui/dist \
    /opt/airflow/airflow-core/src/airflow/ui/dist

COPY --from=apache/airflow:3.1.6 \
    /home/airflow/.local/lib/python3.12/site-packages/airflow/api_fastapi/auth/managers/simple/ui/dist \
    /opt/airflow/airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui/dist

WORKDIR /opt/airflow

# Install build tools and basic dependencies
RUN pip install --no-cache-dir hatchling hatch-vcs editables

# Install both packages without dependencies first (they depend on each other)
RUN pip install --no-cache-dir --no-build-isolation --no-deps \
    -e /opt/airflow/task-sdk \
    -e /opt/airflow/airflow-core

# Install all airflow-core dependencies manually (from pyproject.toml)
RUN pip install --no-cache-dir \
    'a2wsgi>=1.10.8' \
    'aiosqlite>=0.20.0,<0.22.0' \
    'alembic>=1.13.1,<2.0' \
    'argcomplete>=1.10' \
    'asgiref>=2.3.0' \
    'attrs>=22.1.0' \
    'cadwyn>=6.0.0' \
    'colorlog>=6.8.2' \
    'cron-descriptor>=1.2.24' \
    'croniter>=2.0.2' \
    'cryptography>=41.0.0,<46.0.0' \
    'deprecated>=1.2.13' \
    'dill>=0.2.2' \
    'fastapi>=0.128.0' \
    'uvicorn>=0.37.0' \
    'starlette>=0.45.0' \
    'httpx>=0.25.0' \
    'importlib_metadata>=7.0' \
    'itsdangerous>=2.0' \
    'jinja2>=3.1.5' \
    'jsonschema>=4.19.1' \
    'lazy-object-proxy>=1.2.0' \
    'libcst>=1.8.2' \
    'linkify-it-py>=2.0.0' \
    'lockfile>=0.12.2' \
    'methodtools>=0.4.7' \
    'natsort>=8.4.0' \
    'opentelemetry-api>=1.27.0' \
    'opentelemetry-exporter-otlp>=1.27.0' \
    'opentelemetry-proto>=1.27.0' \
    'packaging>=25.0' \
    'pathspec>=0.9.0' \
    'pendulum>=3.1.0' \
    'pluggy>=1.5.0' \
    'psutil>=5.8.0' \
    'pydantic>=2.11.0' \
    'pygments>=2.0.1' \
    'pyjwt>=2.10.0' \
    'python-daemon>=3.0.0' \
    'python-dateutil>=2.7.0' \
    'python-slugify>=5.0' \
    'requests>=2.32.0,<3' \
    'rich-argparse>=1.0.0' \
    'rich>=13.6.0' \
    'setproctitle>=1.3.3' \
    'sqlalchemy[asyncio]>=2.0.36' \
    'sqlalchemy-jsonfield>=1.0' \
    'sqlalchemy-utils>=0.41.2' \
    'svcs>=25.1.0' \
    'tabulate>=0.9.0' \
    'tenacity>=8.3.0' \
    'termcolor>=3.0.0' \
    'typing-extensions>=4.14.1' \
    'universal-pathlib>=0.2.6,<0.3.0' \
    'uuid6>=2024.7.10' \
    'msgspec>=0.19.0' \
    'pygtrie>=2.5.0' \
    'structlog>=25.4.0' \
    'pyyaml>=6.0.3' \
    # Database drivers
    psycopg2-binary \
    asyncpg \
    # Web extras
    flask \
    flask-session \
    werkzeug \
    markupsafe \
    gunicorn \
    # Required Airflow providers
    'apache-airflow-providers-common-compat>=1.7.4' \
    'apache-airflow-providers-common-io>=1.6.3' \
    'apache-airflow-providers-common-sql>=1.28.1' \
    'apache-airflow-providers-smtp>=2.3.1' \
    'apache-airflow-providers-standard>=1.9.0' \
    # Temporal
    temporalio

# Re-install our custom packages to ensure they override any PyPI packages pulled by providers
RUN pip install --no-cache-dir --no-build-isolation --force-reinstall --no-deps \
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
