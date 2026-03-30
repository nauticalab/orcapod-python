# MinIO / S3 Integration for DeltaTableDatabase — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add S3/MinIO support to `DeltaTableDatabase` by exposing `storage_options` and accepting `UPath` as `base_path`, then validate the integration with a full TDD test suite and a GitHub Actions CI workflow.

**Architecture:** A new `storage_utils.py` helper translates `UPath` fsspec credentials to `deltalake`'s `AWS_*` format and detects cloud URIs. `DeltaTableDatabase.__init__` gains a `storage_options` parameter; all internal `DeltaTable()` and `write_deltalake()` calls receive these options. Local-path behaviour is preserved behind `is_cloud_uri()` guards.

**Tech Stack:** Python 3.12, `deltalake>=1.0.2`, `universal-pathlib>=0.2.0`, `testcontainers[minio]>=4.0.0`, `uv`, `pytest`, GitHub Actions.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `pyproject.toml` | Modify | Add `universal-pathlib` (runtime) and `testcontainers[minio]` (dev) |
| `src/orcapod/databases/storage_utils.py` | **Create** | `parse_base_path()` and `is_cloud_uri()` — UPath→AWS credential translation |
| `src/orcapod/databases/delta_lake_databases.py` | Modify | Add `storage_options`/`UPath` support, rename `_get_table_path`→`_get_table_uri`, add `list_sources()` |
| `tests/test_databases/__init__.py` | **Create** | Empty — marks directory as test package |
| `tests/test_databases/conftest.py` | **Create** | Dual-mode MinIO fixture (CI env vars + testcontainers fallback) |
| `tests/test_databases/test_storage_utils.py` | **Create** | Unit tests for `storage_utils.py` |
| `tests/test_databases/test_delta_table_database.py` | **Create** | Local-path CRUD tests (always run) |
| `tests/test_databases/test_delta_table_database_s3.py` | **Create** | MinIO/S3 integration tests (`@pytest.mark.minio`) |
| `pytest.ini` | Modify | Add `markers` section |
| `.github/workflows/tests.yml` | **Create** | CI: run full test suite with MinIO service container |

---

## Task 1: Add Dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1.1: Add `universal-pathlib` as a runtime dependency**

Open `pyproject.toml` and add to the `[project] dependencies` list:

```toml
[project]
dependencies = [
    # ... existing deps ...
    "universal-pathlib>=0.2.0",
]
```

- [ ] **Step 1.2: Add `testcontainers[minio]` as a dev dependency**

Add to `[dependency-groups] dev`:

```toml
[dependency-groups]
dev = [
    # ... existing dev deps ...
    "testcontainers[minio]>=4.0.0",
]
```

- [ ] **Step 1.3: Sync and verify no version conflicts**

```bash
uv sync --all-groups
uv run python -c "import upath; from testcontainers.minio import MinioContainer; print('OK')"
```

Expected: `OK` (no import errors). If `minio` version conflict arises between the existing `minio>=7.2.16` pin and testcontainers' transitive requirement, pin the newer version explicitly.

- [ ] **Step 1.4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add universal-pathlib and testcontainers[minio] dependencies"
```

---

## Task 2: Write and Pass `storage_utils` Tests (TDD)

**Files:**
- Create: `tests/test_databases/__init__.py`
- Create: `tests/test_databases/test_storage_utils.py`
- Create: `src/orcapod/databases/storage_utils.py`

### Step 2a: Write the tests first

- [ ] **Step 2.1: Create the test package**

```bash
mkdir -p tests/test_databases
touch tests/test_databases/__init__.py
```

- [ ] **Step 2.2: Write `test_storage_utils.py`**

Create `tests/test_databases/test_storage_utils.py`:

```python
"""Unit tests for orcapod.databases.storage_utils."""
from pathlib import Path

import pytest
from upath import UPath

from orcapod.databases.storage_utils import is_cloud_uri, parse_base_path


# ---------------------------------------------------------------------------
# is_cloud_uri
# ---------------------------------------------------------------------------

def test_is_cloud_uri_local_string():
    assert is_cloud_uri("/tmp/foo") is False


def test_is_cloud_uri_local_relative():
    assert is_cloud_uri("relative/path") is False


def test_is_cloud_uri_s3():
    assert is_cloud_uri("s3://my-bucket/prefix") is True


def test_is_cloud_uri_s3a():
    assert is_cloud_uri("s3a://my-bucket/prefix") is True


def test_is_cloud_uri_other_schemes():
    assert is_cloud_uri("gs://bucket/path") is True
    assert is_cloud_uri("az://container/blob") is True
    assert is_cloud_uri("abfs://container@account/path") is True


# ---------------------------------------------------------------------------
# parse_base_path — local paths
# ---------------------------------------------------------------------------

def test_parse_base_path_local_path_object(tmp_path):
    uri, opts = parse_base_path(tmp_path)
    assert uri == str(tmp_path)
    assert opts == {}


def test_parse_base_path_local_string(tmp_path):
    uri, opts = parse_base_path(str(tmp_path))
    assert uri == str(tmp_path)
    assert opts == {}


# ---------------------------------------------------------------------------
# parse_base_path — S3 string
# ---------------------------------------------------------------------------

def test_parse_base_path_s3_string():
    uri, opts = parse_base_path("s3://bucket/prefix")
    assert uri == "s3://bucket/prefix"
    assert opts == {}


def test_parse_base_path_s3_string_with_explicit_options():
    opts_in = {"AWS_ACCESS_KEY_ID": "key", "AWS_SECRET_ACCESS_KEY": "secret"}
    uri, opts = parse_base_path("s3://bucket/prefix", storage_options=opts_in)
    assert uri == "s3://bucket/prefix"
    assert opts == opts_in


# ---------------------------------------------------------------------------
# parse_base_path — UPath
# ---------------------------------------------------------------------------

def test_parse_base_path_upath_s3_basic():
    up = UPath("s3://my-bucket/tables", key="mykey", secret="mysecret")
    uri, opts = parse_base_path(up)
    assert uri == "s3://my-bucket/tables"
    assert opts["AWS_ACCESS_KEY_ID"] == "mykey"
    assert opts["AWS_SECRET_ACCESS_KEY"] == "mysecret"


def test_parse_base_path_upath_with_endpoint():
    up = UPath("s3://my-bucket/tables", key="k", secret="s", endpoint_url="http://minio:9000")
    uri, opts = parse_base_path(up)
    assert opts["AWS_ENDPOINT_URL"] == "http://minio:9000"


def test_parse_base_path_upath_client_kwargs_endpoint():
    """Nested client_kwargs.endpoint_url is translated as fallback."""
    up = UPath("s3://my-bucket/tables", client_kwargs={"endpoint_url": "http://minio:9000"})
    uri, opts = parse_base_path(up)
    assert opts.get("AWS_ENDPOINT_URL") == "http://minio:9000"


def test_parse_base_path_upath_client_kwargs_endpoint_not_used_when_top_level_present():
    """Top-level endpoint_url wins over client_kwargs.endpoint_url."""
    up = UPath(
        "s3://my-bucket/tables",
        endpoint_url="http://top-level:9000",
        client_kwargs={"endpoint_url": "http://nested:9000"},
    )
    uri, opts = parse_base_path(up)
    assert opts["AWS_ENDPOINT_URL"] == "http://top-level:9000"


def test_parse_base_path_upath_token():
    up = UPath("s3://b/p", key="k", secret="s", token="tok")
    _, opts = parse_base_path(up)
    assert opts["AWS_SESSION_TOKEN"] == "tok"


def test_parse_base_path_explicit_options_override_upath():
    """Explicit storage_options beat UPath-derived ones."""
    up = UPath("s3://b/p", key="upath-key", secret="s")
    _, opts = parse_base_path(up, storage_options={"AWS_ACCESS_KEY_ID": "override-key"})
    assert opts["AWS_ACCESS_KEY_ID"] == "override-key"
```

- [ ] **Step 2.3: Run tests — expect all to FAIL (module not found)**

```bash
uv run pytest tests/test_databases/test_storage_utils.py -v
```

Expected: `ModuleNotFoundError: No module named 'orcapod.databases.storage_utils'`

### Step 2b: Implement `storage_utils.py`

- [ ] **Step 2.4: Create `src/orcapod/databases/storage_utils.py`**

```python
"""
Storage utility helpers for DeltaTableDatabase.

Provides two public functions:
- parse_base_path: Converts any path type (str, Path, UPath) into a URI string
  and a merged dict of deltalake-compatible storage_options.
- is_cloud_uri: Returns True for cloud storage schemes (s3://, gs://, etc.).
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from upath import UPath

# Recognised cloud URI schemes
_CLOUD_SCHEMES = frozenset({"s3", "s3a", "gs", "az", "abfs"})

# Mapping from fsspec/UPath kwarg names to deltalake's AWS_* storage_options keys
_FSSPEC_TO_AWS: dict[str, str] = {
    "key": "AWS_ACCESS_KEY_ID",
    "secret": "AWS_SECRET_ACCESS_KEY",
    "token": "AWS_SESSION_TOKEN",
    "endpoint_url": "AWS_ENDPOINT_URL",
}


def is_cloud_uri(uri: str) -> bool:
    """Return True if the URI uses a cloud storage scheme (s3, gs, az, etc.)."""
    if "://" not in uri:
        return False
    scheme = uri.split("://", 1)[0].lower()
    return scheme in _CLOUD_SCHEMES


def _extract_upath_options(upath: "UPath") -> dict[str, str]:
    """
    Extract storage options from a UPath and translate to deltalake's AWS_* format.

    Top-level keys take precedence; client_kwargs.endpoint_url is a fallback for
    AWS_ENDPOINT_URL only.
    """
    try:
        fsspec_opts: dict = dict(getattr(upath, "storage_options", {}))
    except Exception:
        return {}

    result: dict[str, str] = {}

    # Translate well-known top-level keys
    for fsspec_key, aws_key in _FSSPEC_TO_AWS.items():
        if fsspec_key in fsspec_opts:
            result[aws_key] = str(fsspec_opts[fsspec_key])

    # Fallback: nested client_kwargs.endpoint_url -> AWS_ENDPOINT_URL
    if "AWS_ENDPOINT_URL" not in result:
        client_kwargs = fsspec_opts.get("client_kwargs", {})
        if isinstance(client_kwargs, dict) and "endpoint_url" in client_kwargs:
            result["AWS_ENDPOINT_URL"] = str(client_kwargs["endpoint_url"])

    return result


def parse_base_path(
    base_path: "str | Path | UPath",
    storage_options: dict[str, str] | None = None,
) -> tuple[str, dict[str, str]]:
    """
    Parse base_path into a plain URI string and merged deltalake storage_options.

    Args:
        base_path: Local filesystem path (str or Path) or cloud URI (str or UPath).
        storage_options: Explicit deltalake-format options (AWS_* keys). These
            override any options derived from a UPath.

    Returns:
        (uri, merged_storage_options) where uri is a string suitable for passing
        directly to DeltaTable() or write_deltalake(), and merged_storage_options
        is a dict ready for the storage_options= parameter.
    """
    try:
        from upath import UPath as _UPath
        is_upath = isinstance(base_path, _UPath)
    except ImportError:
        is_upath = False

    if is_upath:
        uri = str(base_path)
        derived = _extract_upath_options(base_path)  # type: ignore[arg-type]
    else:
        uri = str(base_path)
        derived = {}

    merged = {**derived, **(storage_options or {})}
    return uri, merged
```

- [ ] **Step 2.5: Run tests — expect all to PASS**

```bash
uv run pytest tests/test_databases/test_storage_utils.py -v
```

Expected: all green.

- [ ] **Step 2.6: Commit**

```bash
git add tests/test_databases/__init__.py tests/test_databases/test_storage_utils.py \
        src/orcapod/databases/storage_utils.py
git commit -m "feat: add storage_utils helpers for UPath/S3 credential translation"
```

---

## Task 3: Write Local `DeltaTableDatabase` Tests

**Files:**
- Create: `tests/test_databases/test_delta_table_database.py`

These tests are written against the **new interface** (accepting `str | Path | UPath`) and cover existing local-path behaviour. Most will pass against the existing code; any that don't reveal gaps to fix in Task 5.

- [ ] **Step 3.1: Create `tests/test_databases/test_delta_table_database.py`**

```python
"""
Local-filesystem tests for DeltaTableDatabase.

These tests always run (no external service required).
They cover the full CRUD surface and confirm no regressions
from the S3 refactor.
"""
import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_table(data: dict) -> pa.Table:
    return pa.table(data)


def _simple_record() -> pa.Table:
    return _make_table({"value": [42], "label": ["hello"]})


# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------

def test_add_record_and_retrieve(tmp_path):
    db = DeltaTableDatabase(tmp_path)
    record = _simple_record()
    db.add_record(("source", "run1"), "rec-001", record, flush=True)

    result = db.get_record_by_id(("source", "run1"), "rec-001")
    assert result is not None
    assert result["value"][0].as_py() == 42


def test_add_records_batch(tmp_path):
    db = DeltaTableDatabase(tmp_path)
    table = _make_table({
        "id": ["a", "b", "c"],
        "score": [1.0, 2.0, 3.0],
    })
    db.add_records(("exp", "v1"), table, record_id_column="id", flush=True)

    result = db.get_all_records(("exp", "v1"))
    assert result is not None
    assert result.num_rows == 3


def test_skip_duplicates(tmp_path):
    db = DeltaTableDatabase(tmp_path)
    record = _simple_record()
    db.add_record(("src",), "rec-001", record, flush=True)
    # Re-inserting with skip_duplicates=True should be a no-op
    db.add_record(("src",), "rec-001", record, skip_duplicates=True, flush=True)

    result = db.get_all_records(("src",))
    assert result is not None
    assert result.num_rows == 1


def test_flush_pending(tmp_path):
    db = DeltaTableDatabase(tmp_path)
    db.add_record(("src",), "rec-001", _simple_record())
    db.add_record(("src",), "rec-002", _simple_record())
    # Records are still pending — flush explicitly
    db.flush()

    result = db.get_all_records(("src",), retrieve_pending=False)
    assert result is not None
    assert result.num_rows == 2


def test_get_all_records_includes_pending(tmp_path):
    db = DeltaTableDatabase(tmp_path)
    db.add_record(("src",), "rec-001", _simple_record())  # pending
    result = db.get_all_records(("src",), retrieve_pending=True)
    assert result is not None
    assert result.num_rows == 1


def test_overwrite_semantics(tmp_path):
    """
    DeltaTableDatabase uses merge/insert semantics (no separate update method).
    A record written twice with the same record_id is only stored once.
    """
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        ("src",),
        _make_table({"id": ["x"], "val": [1]}),
        record_id_column="id",
        flush=True,
    )
    db.add_records(
        ("src",),
        _make_table({"id": ["x"], "val": [99]}),
        record_id_column="id",
        skip_duplicates=True,
        flush=True,
    )
    result = db.get_all_records(("src",))
    assert result is not None
    # skip_duplicates=True: second write ignored, original value preserved
    assert result.num_rows == 1
    assert result["val"][0].as_py() == 1


def test_schema_evolution(tmp_path):
    db = DeltaTableDatabase(tmp_path, allow_schema_evolution=True)
    db.add_record(("src",), "r1", _make_table({"a": [1]}), flush=True)
    db.add_record(
        ("src",),
        "r2",
        _make_table({"a": [2], "b": ["new-col"]}),
        schema_handling="merge",
        flush=True,
    )
    result = db.get_all_records(("src",))
    assert result is not None
    assert "b" in result.column_names


def test_list_sources(tmp_path):
    db = DeltaTableDatabase(tmp_path)
    db.add_record(("alpha",), "r1", _simple_record(), flush=True)
    db.add_record(("beta", "sub"), "r1", _simple_record(), flush=True)

    sources = db.list_sources()
    assert ("alpha",) in sources
    assert ("beta", "sub") in sources


def test_create_base_path_false_raises_if_missing(tmp_path):
    missing = tmp_path / "does_not_exist"
    with pytest.raises(ValueError, match="does not exist"):
        DeltaTableDatabase(missing, create_base_path=False)


def test_create_base_path_false_works_if_exists(tmp_path):
    db = DeltaTableDatabase(tmp_path, create_base_path=False)
    assert db is not None


def test_upath_local_accepted(tmp_path):
    """UPath with a local path is accepted without errors."""
    from upath import UPath
    db = DeltaTableDatabase(UPath(tmp_path))
    db.add_record(("src",), "r1", _simple_record(), flush=True)
    result = db.get_record_by_id(("src",), "r1")
    assert result is not None
```

- [ ] **Step 3.2: Run local tests — most should pass, note any failures**

```bash
uv run pytest tests/test_databases/test_delta_table_database.py -v
```

Expected: `test_list_sources`, `test_upath_local_accepted`, and possibly `test_create_base_path_false_*` may fail — these gaps will be resolved in Task 5. All other tests should pass against the existing code.

---

## Task 4: Write S3 `DeltaTableDatabase` Tests

**Files:**
- Create: `tests/test_databases/conftest.py`
- Create: `tests/test_databases/test_delta_table_database_s3.py`

- [ ] **Step 4.1: Create `tests/test_databases/conftest.py`**

```python
"""
Shared pytest fixtures for database tests.
Provides a dual-mode MinIO fixture:
  - CI: reads MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY env vars
  - Local: uses testcontainers to spin up a MinIO Docker container
"""
from __future__ import annotations

import os
import uuid
from urllib.parse import urlparse

import pytest
from minio import Minio


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
        pytest.importorskip("testcontainers", reason="testcontainers not installed")
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
            pytest.skip(
                f"MinIO container could not be started (is Docker running?): {exc}"
            )


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
```

- [ ] **Step 4.2: Create `tests/test_databases/test_delta_table_database_s3.py`**

```python
"""
MinIO / S3 integration tests for DeltaTableDatabase.

Requires a running MinIO instance — provided either by the minio_server
fixture (testcontainers locally, env vars in CI).

Run with:  uv run pytest tests/test_databases/test_delta_table_database_s3.py -v
Skip automatically if Docker is unavailable and MINIO_ENDPOINT is not set.
"""
import pyarrow as pa
import pytest
from upath import UPath

from orcapod.databases import DeltaTableDatabase

pytestmark = pytest.mark.minio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _simple_table() -> pa.Table:
    return pa.table({"value": [10, 20, 30], "label": ["a", "b", "c"]})


# ---------------------------------------------------------------------------
# Core S3 operations
# ---------------------------------------------------------------------------

def test_s3_add_record_and_retrieve(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    record = pa.table({"x": [1], "y": ["hello"]})
    db.add_record(("run1",), "rec-001", record, flush=True)

    result = db.get_record_by_id(("run1",), "rec-001")
    assert result is not None
    assert result["x"][0].as_py() == 1


def test_s3_add_records_batch(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    table = pa.table({"id": ["a", "b", "c"], "score": [1.0, 2.0, 3.0]})
    db.add_records(("exp",), table, record_id_column="id", flush=True)

    result = db.get_all_records(("exp",))
    assert result is not None
    assert result.num_rows == 3


def test_s3_flush_and_read(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    for i in range(3):
        db.add_record(("src",), f"r{i}", pa.table({"n": [i]}))
    db.flush()

    result = db.get_all_records(("src",), retrieve_pending=False)
    assert result is not None
    assert result.num_rows == 3


def test_s3_skip_duplicates(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    record = pa.table({"v": [1]})
    db.add_record(("src",), "dup", record, flush=True)
    db.add_record(("src",), "dup", record, skip_duplicates=True, flush=True)

    result = db.get_all_records(("src",))
    assert result is not None
    assert result.num_rows == 1


# ---------------------------------------------------------------------------
# UPath credential passthrough
# ---------------------------------------------------------------------------

def test_s3_upath_credentials(s3_base_uri, minio_server):
    """
    DeltaTableDatabase accepts a UPath with embedded S3 credentials (key/secret/endpoint_url).
    These are auto-translated from fsspec format to AWS_* keys.

    AWS_REGION, AWS_ALLOW_HTTP, and AWS_S3_ALLOW_UNSAFE_RENAME cannot be embedded
    in UPath kwargs (they have no fsspec equivalent), so they are passed via the
    explicit storage_options argument. Explicit options are merged on top of
    UPath-derived ones — neither alone is sufficient for MinIO.
    """
    bucket = minio_server["bucket"]
    prefix = s3_base_uri.split(f"s3://{bucket}/")[1]

    upath = UPath(
        f"s3://{bucket}/{prefix}",
        key=minio_server["access_key"],
        secret=minio_server["secret_key"],
        endpoint_url=minio_server["endpoint"],
    )
    db = DeltaTableDatabase(
        upath,
        storage_options={
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        },
    )
    record = pa.table({"k": ["hello"]})
    db.add_record(("t",), "r1", record, flush=True)
    result = db.get_record_by_id(("t",), "r1")
    assert result is not None
    assert result["k"][0].as_py() == "hello"


# ---------------------------------------------------------------------------
# Cloud-specific behaviour
# ---------------------------------------------------------------------------

def test_s3_list_sources_raises(s3_base_uri, s3_storage_options):
    db = DeltaTableDatabase(s3_base_uri, storage_options=s3_storage_options)
    with pytest.raises(NotImplementedError, match="not supported for cloud"):
        db.list_sources()


def test_s3_create_base_path_ignored(s3_base_uri, s3_storage_options):
    """create_base_path=False must not raise for cloud URIs."""
    db = DeltaTableDatabase(
        s3_base_uri,
        storage_options=s3_storage_options,
        create_base_path=False,
    )
    assert db is not None
```

- [ ] **Step 4.3: Run S3 tests — all should FAIL (DeltaTableDatabase doesn't support S3 yet)**

```bash
uv run pytest tests/test_databases/test_delta_table_database_s3.py -v
```

Expected: errors like `OSError` or `ValueError` from `Path("s3://...")` — confirms the tests correctly identify the gap.

---

## Task 5: Implement S3 Support in `DeltaTableDatabase`

**Files:**
- Modify: `src/orcapod/databases/delta_lake_databases.py`

This is the core implementation task. Make all changes in one go, then run the full suite.

- [ ] **Step 5.1: Add imports to `delta_lake_databases.py`**

At the top of the file, add after the existing imports:

```python
from orcapod.databases.storage_utils import is_cloud_uri, parse_base_path
```

Also update the TYPE_CHECKING block to include UPath:

```python
if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
    from upath import UPath
```

And update the type hint import at the top of the runtime section (not TYPE_CHECKING):

```python
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast
```

- [ ] **Step 5.2: Replace the `__init__` method**

Replace the existing `__init__` (lines 42-70 in `delta_lake_databases.py`) with:

```python
def __init__(
    self,
    base_path: "str | Path | UPath",
    storage_options: "dict[str, str] | None" = None,
    create_base_path: bool = True,
    batch_size: int = 1000,
    max_hierarchy_depth: int = 10,
    allow_schema_evolution: bool = True,
):
    self._base_uri, self._storage_options = parse_base_path(base_path, storage_options)
    self._is_cloud: bool = is_cloud_uri(self._base_uri)
    self.batch_size = batch_size
    self.max_hierarchy_depth = max_hierarchy_depth
    self.allow_schema_evolution = allow_schema_evolution

    if not self._is_cloud:
        # Keep self.base_path for local-path operations (list_sources, etc.)
        # NOTE: do NOT access self.base_path on cloud instances.
        self.base_path = Path(self._base_uri)
        if create_base_path:
            self.base_path.mkdir(parents=True, exist_ok=True)
        elif not self.base_path.exists():
            raise ValueError(
                f"Base path {self.base_path} does not exist and create_base_path=False"
            )
    # For cloud paths: create_base_path is silently ignored (no directory needed).

    self._delta_table_cache: dict[str, DeltaTable] = {}
    self._pending_batches: dict[str, pa.Table] = {}
    self._pending_record_ids: dict[str, set[str]] = defaultdict(set)
    self._existing_ids_cache: dict[str, set[str]] = defaultdict(set)
    self._cache_dirty: dict[str, bool] = defaultdict(lambda: True)
```

- [ ] **Step 5.3: Rename `_get_table_path` → `_get_table_uri` and return `str`**

Replace the existing `_get_table_path` method with:

```python
def _get_table_uri(self, record_path: tuple[str, ...]) -> str:
    """Get the URI for a given record path (works for local and cloud)."""
    if self._is_cloud:
        return self._base_uri.rstrip("/") + "/" + "/".join(record_path)
    else:
        path = Path(self._base_uri)
        for subpath in record_path:
            path = path / subpath
        return str(path)
```

- [ ] **Step 5.4: Update `_get_delta_table` — rename call and add `storage_options`**

In `_get_delta_table`, replace:
```python
table_path = self._get_table_path(record_path)
# ...
delta_table = DeltaTable(str(table_path))
```

With:
```python
table_uri = self._get_table_uri(record_path)
# ...
delta_table = DeltaTable(table_uri, storage_options=self._storage_options)
```

- [ ] **Step 5.5: Update `flush_batch` — four specific changes**

In `flush_batch`, make these four changes:

1. Replace `table_path = self._get_table_path(record_path)` → `table_uri = self._get_table_uri(record_path)`

2. Replace the `mkdir` block:
```python
# BEFORE:
table_path.mkdir(parents=True, exist_ok=True)
# AFTER:
if not self._is_cloud:
    Path(table_uri).mkdir(parents=True, exist_ok=True)
```

3. Replace the `write_deltalake` call in the **new-table branch** (`delta_table is None`). This is the only `write_deltalake` call that needs updating — the existing-table branch uses `delta_table.merge(...).execute()` which operates on an already-loaded `DeltaTable` object and has no `storage_options` parameter:
```python
# BEFORE:
write_deltalake(
    table_path,
    combined_table,
    mode="overwrite",
    schema_mode="merge" if self.allow_schema_evolution else "overwrite",
)
# AFTER:
write_deltalake(
    table_uri,
    combined_table,
    mode="overwrite",
    schema_mode="merge" if self.allow_schema_evolution else "overwrite",
    storage_options=self._storage_options,
)
```

4. Replace the cache-refresh line:
```python
# BEFORE:
self._delta_table_cache[record_key] = DeltaTable(str(table_path))
# AFTER:
self._delta_table_cache[record_key] = DeltaTable(table_uri, storage_options=self._storage_options)
```

- [ ] **Step 5.6: Add the `list_sources` method**

Add this method to `DeltaTableDatabase` (after `flush_batch`):

```python
def list_sources(self) -> list[tuple[str, ...]]:
    """
    List all record paths that contain a valid Delta table under base_path.

    Returns:
        List of record_path tuples (e.g. [("alpha",), ("beta", "sub")]).

    Raises:
        NotImplementedError: For cloud base paths (not yet supported).
    """
    if self._is_cloud:
        raise NotImplementedError(
            "list_sources() is not supported for cloud storage paths"
        )

    sources: list[tuple[str, ...]] = []

    def _scan(current_path: Path, path_components: tuple[str, ...]) -> None:
        if len(path_components) >= self.max_hierarchy_depth:
            return
        for item in current_path.iterdir():
            if not item.is_dir():
                continue
            components = path_components + (item.name,)
            try:
                DeltaTable(str(item))
                sources.append(components)
            except TableNotFoundError:
                _scan(item, components)

    _scan(self.base_path, ())
    return sources
```

- [ ] **Step 5.7: Run the full test suite**

```bash
uv run pytest tests/test_databases/ -v
```

Expected: all tests pass. If S3 tests are skipped (Docker unavailable), that is fine — they will run in CI.

- [ ] **Step 5.8: Run the full existing test suite to check for regressions**

```bash
uv run pytest --tb=short
```

Expected: no regressions in non-database tests.

- [ ] **Step 5.9: Commit**

```bash
git add src/orcapod/databases/delta_lake_databases.py \
        tests/test_databases/conftest.py \
        tests/test_databases/test_delta_table_database.py \
        tests/test_databases/test_delta_table_database_s3.py
git commit -m "feat: add S3/MinIO and UPath support to DeltaTableDatabase"
```

---

## Task 6: Update `pytest.ini` and Add CI Workflow

**Files:**
- Modify: `pytest.ini`
- Create: `.github/workflows/tests.yml`

- [ ] **Step 6.1: Edit `pytest.ini` to register the `minio` marker**

Replace the contents of `pytest.ini` with:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v
markers =
    minio: tests requiring a running MinIO instance (via testcontainers or MINIO_ENDPOINT env var)
```

- [ ] **Step 6.2: Verify no marker warnings**

```bash
uv run pytest --co -q 2>&1 | grep -i "warning\|PytestUnknownMarkWarning" || echo "No marker warnings"
```

Expected: `No marker warnings`

- [ ] **Step 6.3: Create `.github/workflows/tests.yml`**

```bash
mkdir -p .github/workflows
```

Create `.github/workflows/tests.yml`:

```yaml
name: Tests

on:
  push:
    branches: [main, dev]
  pull_request:
    branches: [main, dev]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      minio:
        image: bitnami/minio:latest
        env:
          MINIO_ROOT_USER: minioadmin
          MINIO_ROOT_PASSWORD: minioadmin
        ports:
          - 9000:9000
        options: >-
          --health-cmd "curl -f http://localhost:9000/minio/health/live"
          --health-interval 5s
          --health-timeout 5s
          --health-retries 10

    steps:
      - uses: actions/checkout@v4

      - uses: astral-sh/setup-uv@v5
        with:
          enable-cache: true

      - name: Install dependencies
        run: uv sync --all-groups

      - name: Run tests
        run: uv run pytest --tb=short
        env:
          MINIO_ENDPOINT: http://localhost:9000
          MINIO_ACCESS_KEY: minioadmin
          MINIO_SECRET_KEY: minioadmin
```

- [ ] **Step 6.4: Commit**

```bash
git add pytest.ini .github/workflows/tests.yml
git commit -m "ci: add GitHub Actions test workflow with MinIO service container"
```

---

## Task 7: Open Pull Request

- [ ] **Step 7.1: Re-authenticate with GitHub (tokens expire after 1 hour)**

```bash
gh-app-token-generator nauticalab | gh auth login --with-token
```

- [ ] **Step 7.2: Push branch to remote**

```bash
git push -u origin HEAD
```

- [ ] **Step 7.3: Create PR against `dev`**

```bash
gh pr create \
  --base dev \
  --title "feat(ENG-258): add MinIO/S3 and UPath support for DeltaTableDatabase" \
  --body "$(cat <<'EOF'
## Summary

Closes ENG-258.

- Add `storage_options: dict[str, str] | None` parameter to `DeltaTableDatabase`, mirroring the `deltalake` API
- Accept `UPath` as `base_path`; credentials embedded in the UPath are auto-translated from fsspec (`key`/`secret`/`endpoint_url`) to `deltalake`'s `AWS_*` format via the new `storage_utils.py` helper
- Gate local-only operations (`mkdir`, `Path.exists`) on `not self._is_cloud`
- Rename `_get_table_path` → `_get_table_uri` returning `str` for both local and cloud paths
- Add `list_sources()` method (local-only; raises `NotImplementedError` for cloud paths)
- Full TDD test suite: `test_storage_utils`, local CRUD tests, and MinIO/S3 integration tests
- GitHub Actions CI with `bitnami/minio` service container — no external S3 server needed

## Test plan

- [ ] `uv run pytest tests/test_databases/test_storage_utils.py` — passes locally
- [ ] `uv run pytest tests/test_databases/test_delta_table_database.py` — passes locally
- [ ] `uv run pytest tests/test_databases/test_delta_table_database_s3.py` — passes with Docker (or in CI)
- [ ] Full CI run passes on GitHub Actions (check the Actions tab)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7.4: Update Linear issue status to "In Review"**

```
mcp__claude_ai_Linear__save_issue(id: "ENG-258", state: "In Review")
```

---

## Quick Reference: Required S3 Storage Options for MinIO

```python
storage_options = {
    "AWS_ENDPOINT_URL": "http://minio-host:9000",
    "AWS_ACCESS_KEY_ID": "<access-key>",
    "AWS_SECRET_ACCESS_KEY": "<secret-key>",
    "AWS_REGION": "us-east-1",          # required by Rust object_store even for MinIO
    "AWS_ALLOW_HTTP": "true",            # required for non-TLS endpoints
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true", # required for Delta Lake commit protocol on MinIO
}
```

**Note:** `AWS_S3_ALLOW_UNSAFE_RENAME` must also be set in **production** MinIO deployments — MinIO does not support atomic rename natively.
