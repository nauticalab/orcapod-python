"""conftest.py for test_databases.

Installs a lightweight mock psycopg module into sys.modules so that:
  - ``patch("psycopg.connect")`` works even when libpq is not installed.
  - The LazyModule("psycopg") in postgresql_connector.py resolves without
    trying to load the real psycopg (which requires libpq).

Also provides a dual-mode MinIO fixture for S3 integration tests:
  - CI: reads MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY env vars
  - Local: uses testcontainers to spin up a MinIO Docker container
"""
from __future__ import annotations

import os
import sys
import types
import uuid
from unittest.mock import MagicMock
from urllib.parse import urlparse

import pytest
from minio import Minio

# Only install the stub if psycopg is not already importable.
try:
    import psycopg  # noqa: F401
except ImportError:
    _psycopg_stub = types.ModuleType("psycopg")
    _psycopg_stub.connect = MagicMock()  # type: ignore[attr-defined]
    sys.modules["psycopg"] = _psycopg_stub


# ---------------------------------------------------------------------------
# MinIO fixtures
# ---------------------------------------------------------------------------


def _minio_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    """Construct a Minio admin client from a full URL endpoint."""
    parsed = urlparse(endpoint)
    return Minio(
        parsed.netloc,          # "host:port" — no scheme
        access_key=access_key,
        secret_key=secret_key,
        secure=parsed.scheme == "https",
    )


@pytest.fixture(scope="session")
def minio_server():
    """
    Provide MinIO connection info as a dict with keys:
        endpoint, access_key, secret_key, bucket

    CI mode: reads MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY env vars.
    Local dev: spins up a MinIO container via testcontainers (requires Docker).
    Skips gracefully if Docker is unavailable.
    """
    endpoint = os.environ.get("MINIO_ENDPOINT")
    if endpoint:
        access_key = os.environ["MINIO_ACCESS_KEY"]
        secret_key = os.environ["MINIO_SECRET_KEY"]
        bucket = "test-bucket"
        client = _minio_client(endpoint, access_key, secret_key)
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        yield {
            "endpoint": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "bucket": bucket,
        }
    else:
        pytest.importorskip("testcontainers.minio", reason="testcontainers[minio] not installed")
        try:
            from testcontainers.minio import MinioContainer  # type: ignore[import]
            with MinioContainer() as minio:
                client = minio.get_client()
                bucket = "test-bucket"
                client.make_bucket(bucket)
                yield {
                    "endpoint": minio.get_url(),
                    "access_key": minio.access_key,
                    "secret_key": minio.secret_key,
                    "bucket": bucket,
                }
        except Exception as exc:
            pytest.skip(f"MinIO container could not be started: {exc}")


@pytest.fixture
def s3_base_uri(minio_server):
    """Unique S3 URI prefix per test — prevents cross-test table pollution."""
    prefix = f"tables-{uuid.uuid4().hex[:8]}"
    return f"s3://{minio_server['bucket']}/{prefix}"


@pytest.fixture
def s3_storage_options(minio_server):
    """Full set of storage_options required for MinIO via deltalake."""
    return {
        "AWS_ENDPOINT_URL": minio_server["endpoint"],
        "AWS_ACCESS_KEY_ID": minio_server["access_key"],
        "AWS_SECRET_ACCESS_KEY": minio_server["secret_key"],
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
