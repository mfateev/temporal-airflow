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


@pytest.fixture(scope="session")
def airflow_db():
    """
    Initialize Airflow database schema for tests that need it.

    This fixture sets up an in-memory SQLite database with Airflow's schema.
    It runs once per test session.
    """
    # Set up in-memory SQLite for testing
    os.environ.setdefault(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "sqlite:////:memory:"
    )

    # Initialize Airflow's database schema
    from airflow.utils.db import initdb
    from airflow.utils.session import create_session

    with create_session() as session:
        initdb(session=session)

    yield

    # Cleanup is automatic with in-memory database


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
