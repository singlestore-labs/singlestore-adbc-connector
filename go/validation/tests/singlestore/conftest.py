# Copyright (c) 2025-2026 ADBC Drivers Contributors
# Copyright (c) 2026 SingleStore, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import pytest


def pytest_generate_tests(metafunc) -> None:
    metafunc.parametrize(
        "driver",
        [pytest.param("singlestore:", id="singlestore")],
        scope="module",
        indirect=["driver"],
    )


@pytest.fixture(scope="session")
def singlestore_host() -> str:
    """SingleStore host. Example: SINGLESTORE_HOST=localhost"""
    return os.environ.get("SINGLESTORE_HOST", "localhost")


@pytest.fixture(scope="session")
def singlestore_port() -> str:
    """SingleStore port. Example: SINGLESTORE_PORT=3307"""
    return os.environ.get("SINGLESTORE_PORT", "3306")


@pytest.fixture(scope="session")
def singlestore_database() -> str:
    """SingleStore database name. Example: SINGLESTORE_DATABASE=db"""
    return os.environ.get("SINGLESTORE_DATABASE", "db")


@pytest.fixture(scope="session")
def creds() -> tuple[str, str]:
    """SingleStore credentials. Example: SINGLESTORE_USERNAME=root SINGLESTORE_PASSWORD=password"""
    username = os.environ.get("SINGLESTORE_USERNAME", "root")
    password = os.environ.get("SINGLESTORE_PASSWORD", "password")
    return username, password


@pytest.fixture(scope="session")
def uri(singlestore_host: str, singlestore_port: str, singlestore_database: str) -> str:
    """
    Constructs a clean SingleStore URI without credentials.
    Example: mysql://localhost:3306/db
    """
    return f"mysql://{singlestore_host}:{singlestore_port}/{singlestore_database}"


@pytest.fixture(scope="session")
def dsn(
    creds: tuple[str, str],
    singlestore_host: str,
    singlestore_port: str,
    singlestore_database: str,
) -> str:
    """
    Constructs a SingleStore DSN in Go SingleStore Driver's native format.
    Example: my:password@tcp(localhost:3306)/db
    """
    username, password = creds
    return f"{username}:{password}@tcp({singlestore_host}:{singlestore_port})/{singlestore_database}"


@pytest.fixture(scope="session")
def singlestore_socket_path() -> str:
    """
    Returns the path to SingleStore Unix socket file.
    Requires a local SingleStore server running with Unix socket enabled.
    Example: SINGLESTORE_SOCKET_PATH=/tmp/singlestore.sock
    """
    path = os.environ.get("SINGLESTORE_SOCKET_PATH")
    if not path:
        pytest.skip("Must set SINGLESTORE_SOCKET_PATH (e.g., /tmp/singlestore.sock)")
    return path
