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
"""Pytest configuration for temporal_airflow tests.

Provides database fixtures for tests that need Airflow's database schema.
"""
from __future__ import annotations

import os
import pytest


_TEST_DB_PATH = "/tmp/test_airflow_db.sqlite"


def pytest_configure(config):
    """
    Pytest hook that runs BEFORE test collection.

    CRITICAL: Set database connection BEFORE any Airflow imports!

    Airflow caches database settings on first import. If we set the env var
    after test modules are imported, it's too late - Airflow will have already
    initialized with the default database.

    This hook runs before pytest collects tests, ensuring the environment
    variable is set before any test file imports trigger Airflow initialization.

    This uses a file-based SQLite database to ensure all connections
    (test process and Temporal worker threads) share the same database.
    """
    # Only set if not already configured (e.g., by docker-compose with PostgreSQL)
    if "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" not in os.environ:
        # Remove any existing database from previous test runs
        if os.path.exists(_TEST_DB_PATH):
            os.remove(_TEST_DB_PATH)
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{_TEST_DB_PATH}"


@pytest.fixture(scope="session")
def airflow_db():
    """
    Initialize Airflow database schema for tests that need it.

    This fixture sets up a file-based SQLite database with Airflow's schema.
    It runs once per test session.

    IMPORTANT: The database connection string is set in pytest_configure hook
    (which runs before test collection) to ensure it's configured BEFORE any
    Airflow imports happen. This fixture just initializes the schema.
    """
    # Initialize Airflow's database schema
    from airflow.utils.db import initdb
    from airflow.utils.session import create_session

    with create_session() as session:
        initdb(session=session)

    yield

    # Cleanup: remove the temporary database file (only for SQLite)
    conn_string = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "")
    if conn_string.startswith("sqlite:///") and os.path.exists(_TEST_DB_PATH):
        os.remove(_TEST_DB_PATH)


@pytest.fixture
def airflow_db_session(airflow_db):
    """
    Provide a database session for tests, with automatic cleanup.

    Each test gets a fresh session that rolls back after the test.
    """
    from airflow.utils.session import create_session

    with create_session() as session:
        yield session
        # Session is automatically closed/rolled back by context manager
