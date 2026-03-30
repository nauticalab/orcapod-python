# Design: MinIO / S3 Integration for DeltaTableDatabase

**Issue:** ENG-258
**Date:** 2026-03-30
**Status:** Approved

---

## Background

`DeltaTableDatabase` is Orcapod's primary Delta Lake-backed record store. All expected production deployments will use MinIO (S3-compatible object storage) as the backing store, but the current implementation unconditionally uses Python's `pathlib.Path`, which hard-codes local filesystem assumptions. The underlying `deltalake` library already has native S3 support via a `storage_options` parameter — it just isn't exposed or used.

This design adds S3/MinIO support to `DeltaTableDatabase`, establishes `UPath` as the idiomatic path type across Orcapod infrastructure, and adds a full test suite (TDD) plus a CI/CD workflow that validates S3 integration without requiring an external S3 server.

---

## Goals

- `DeltaTableDatabase` works with S3/MinIO URIs via `storage_options`, mirroring the `deltalake` API
- `UPath` (from `universal_pathlib`) is accepted as `base_path` and carries credentials implicitly
- All new behaviour is covered by tests written *before* the implementation (TDD)
- S3 tests run in CI via a containerised MinIO instance (no external server dependency)
- Existing local-path behaviour is unchanged

## Non-Goals

- Cloud path support for `list_sources()` (deferred — raises `NotImplementedError` for cloud URIs)
- GCS or Azure Blob support (foundation is laid, but not tested)
- Changes to any class other than `DeltaTableDatabase`

---

## Architecture

### New helper module: `databases/storage_utils.py`

A small, independently testable module with two public functions:

**`parse_base_path(base_path, storage_options) → (uri: str, merged_options: dict[str, str])`**

Accepts `str | Path | UPath` plus an optional explicit `storage_options` dict. Returns:
- `uri`: a plain string URI usable by `deltalake` (`/local/path` or `s3://bucket/prefix`)
- `merged_options`: fsspec-style kwargs extracted from a `UPath` (if provided), translated to `deltalake`'s `AWS_*` uppercase naming convention, then merged with any explicitly-provided `storage_options` (explicit values win)

UPath → deltalake key mapping:

| fsspec (UPath kwarg) | deltalake storage_options key |
|---|---|
| `key` | `AWS_ACCESS_KEY_ID` |
| `secret` | `AWS_SECRET_ACCESS_KEY` |
| `token` | `AWS_SESSION_TOKEN` |
| `endpoint_url` | `AWS_ENDPOINT_URL` |
| `client_kwargs` → `endpoint_url` | `AWS_ENDPOINT_URL` (fallback if top-level `endpoint_url` absent) |

The `client_kwargs.endpoint_url` fallback accesses:
```python
storage_options.get("client_kwargs", {}).get("endpoint_url")
```
Top-level `endpoint_url` takes precedence over the nested form.

**`is_cloud_uri(uri: str) → bool`**

Returns `True` for URI schemes `s3://`, `s3a://`, `gs://`, `az://`, `abfs://`. Used to gate local-only operations like `mkdir()` and `Path.exists()`.

### Changes to `DeltaTableDatabase`

#### Constructor signature

```python
def __init__(
    self,
    base_path: str | Path | UPath,
    storage_options: dict[str, str] | None = None,
    create_base_path: bool = True,
    batch_size: int = 1000,
    max_hierarchy_depth: int = 10,
    allow_schema_evolution: bool = True,
):
```

#### Constructor body changes

- Call `parse_base_path(base_path, storage_options)` to obtain `self._base_uri: str` and `self._storage_options: dict[str, str]`
- `self._is_cloud: bool = is_cloud_uri(self._base_uri)`
- For **local paths only**:
  - Keep `self.base_path = Path(self._base_uri)`
  - If `create_base_path=True`: call `self.base_path.mkdir(parents=True, exist_ok=True)`
  - If `create_base_path=False`: call `self.base_path.exists()` and raise `ValueError` if absent
- For **cloud paths**: skip both `mkdir()` and the `exists()` guard entirely. `create_base_path` is silently ignored for cloud URIs (cloud storage requires no directory pre-creation).

#### `_get_table_path` → renamed `_get_table_uri`

Returns `str` in all cases.
- Local: `str(Path(self._base_uri) / record_path[0] / record_path[1] / ...)`
- Cloud: `self._base_uri.rstrip("/") + "/" + "/".join(record_path)`

Return type changes from `Path` to `str`. All callers updated.

#### `_get_delta_table` / cache refresh

All `DeltaTable(uri, ...)` calls gain `storage_options=self._storage_options`.

#### `_get_delta_table` — update caller

This method calls `table_path = self._get_table_path(record_path)` and passes `str(table_path)` to `DeltaTable(...)`. After the rename, this must become:
```python
table_uri = self._get_table_uri(record_path)
delta_table = DeltaTable(table_uri, storage_options=self._storage_options)
```

#### `flush_batch` — four specific lines to update

1. Rename `_get_table_path(record_path)` → `_get_table_uri(record_path)` (returns `str`)
2. Remove `table_path.mkdir(parents=True, exist_ok=True)` for cloud paths (gate on `not self._is_cloud`)
3. **Critical**: `write_deltalake(table_path, combined_table, ...)` must become `write_deltalake(table_uri, combined_table, ..., storage_options=self._storage_options)` — the most common miss; cloud writes will fail silently without this
4. **Critical**: the cache-refresh line `DeltaTable(str(table_path))` at the end of the method must become `DeltaTable(table_uri, storage_options=self._storage_options)` — same class of silent failure for cloud paths

#### `list_sources` — new method (also added as part of this work)

`list_sources()` is referenced in tests but does not currently exist on `DeltaTableDatabase`. It must be added as part of this work.

**Signature:** `def list_sources(self) -> list[tuple[str, ...]]`

Returns a list of `record_path` tuples (consistent with the `record_path` parameter used everywhere else in the class), one per valid Delta table found under `base_path`.

- **Local paths**: recursively walk `self.base_path` up to `self.max_hierarchy_depth` levels deep, checking each directory with `DeltaTable(str(path))` and catching `TableNotFoundError`. Directories that contain a valid Delta table are included; their path relative to `base_path` is returned as a `tuple[str, ...]` of path components.
- **Cloud paths**: raise `NotImplementedError("list_sources() is not supported for cloud storage paths")`

---

## Dependencies

### Runtime (added to `[project.dependencies]`)

- `universal-pathlib>=0.2.0` — `UPath` as core Orcapod path abstraction

### Dev (added to `[dependency-groups] dev`)

- `testcontainers[minio]>=4.0.0` — MinIO container for S3 integration tests

Note: `testcontainers[minio]` installs the `minio` Python package as a dependency. Since `minio>=7.2.16` is already in dev dependencies, verify there are no version conflicts after adding `testcontainers[minio]` and pin if needed.

---

## S3 / MinIO Storage Options — Required Keys

All S3 storage_options dicts (in fixtures, docs, and production) must include:

| Key | Value (MinIO) | Notes |
|---|---|---|
| `AWS_ENDPOINT_URL` | `http://minio-host:9000` | Required for non-AWS S3 |
| `AWS_ACCESS_KEY_ID` | access key | Required |
| `AWS_SECRET_ACCESS_KEY` | secret key | Required |
| `AWS_REGION` | `us-east-1` (any non-empty value) | Required by the Rust object_store backend even for MinIO |
| `AWS_ALLOW_HTTP` | `"true"` | Required for HTTP (non-TLS) MinIO endpoints |
| `AWS_S3_ALLOW_UNSAFE_RENAME` | `"true"` | Required for Delta Lake commit protocol on MinIO; MinIO does not support atomic rename natively. **This flag must also be set in production MinIO deployments.** |

---

## Test Plan (TDD)

Tests are written *before* implementation. The test suite is run after writing tests to confirm failures, then implementation is written to make all tests pass.

### File layout

```
tests/
  test_databases/
    __init__.py
    conftest.py                        # shared fixtures
    test_storage_utils.py             # unit tests for storage_utils.py
    test_delta_table_database.py      # local-path tests (always run)
    test_delta_table_database_s3.py   # MinIO/S3 tests (minio marker)
```

### `conftest.py` — dual-mode MinIO fixture

The fixture supports two modes:
- **CI mode**: reads connection info from environment variables (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`) — used when GitHub Actions provides a MinIO service container
- **Local dev mode**: falls back to `testcontainers` to spin up a MinIO Docker container automatically

```python
import os
import uuid
import pytest
from minio import Minio
from urllib.parse import urlparse

def _minio_client(endpoint: str, access_key: str, secret_key: str) -> Minio:
    """Construct a Minio client from a full URL endpoint."""
    parsed = urlparse(endpoint)
    return Minio(
        parsed.netloc,  # "host:port" without scheme
        access_key=access_key,
        secret_key=secret_key,
        secure=parsed.scheme == "https",
    )

@pytest.fixture(scope="session")
def minio_server():
    """
    Provide MinIO connection info. Uses environment variables in CI,
    falls back to testcontainers for local dev (requires Docker).
    Skip the test session gracefully if neither is available.
    """
    endpoint = os.environ.get("MINIO_ENDPOINT")
    if endpoint:
        # CI mode: GitHub Actions service container
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
        # Local dev mode: testcontainers (requires Docker)
        pytest.importorskip("testcontainers", reason="testcontainers not installed")
        try:
            from testcontainers.minio import MinioContainer
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
        except Exception as e:
            pytest.skip(f"MinIO container could not be started (is Docker running?): {e}")

@pytest.fixture
def s3_base_uri(minio_server):
    """A unique S3 base URI for each test to avoid cross-test pollution."""
    unique_prefix = f"tables-{uuid.uuid4().hex[:8]}"
    return f"s3://{minio_server['bucket']}/{unique_prefix}"

@pytest.fixture
def s3_storage_options(minio_server):
    return {
        "AWS_ENDPOINT_URL": minio_server["endpoint"],
        "AWS_ACCESS_KEY_ID": minio_server["access_key"],
        "AWS_SECRET_ACCESS_KEY": minio_server["secret_key"],
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
```

### `test_storage_utils.py`

- `test_parse_base_path_local_path` — `Path("/tmp/foo")` yields local URI, empty options
- `test_parse_base_path_string_s3` — `"s3://bucket/path"` yields same string, empty options
- `test_parse_base_path_upath_s3` — `UPath("s3://b/p", key="k", secret="s", endpoint_url="http://x")` yields correct URI + translated AWS_ options
- `test_parse_base_path_client_kwargs_endpoint` — `UPath("s3://b/p", client_kwargs={"endpoint_url": "http://x"})` maps nested endpoint correctly
- `test_parse_base_path_explicit_options_override_upath` — explicit `storage_options` values beat UPath-derived ones
- `test_is_cloud_uri_local` — `False` for `/tmp/foo` and `Path` objects
- `test_is_cloud_uri_s3` — `True` for `s3://` and `s3a://`
- `test_is_cloud_uri_other_schemes` — `True` for `gs://`, `az://`, `abfs://`

### `test_delta_table_database.py` (local, always run)

Written against the *new* interface (i.e., `base_path` accepts `str | Path | UPath`, `_get_table_uri` is the internal helper). These tests should pass with minimal changes to the existing code and will confirm no regressions from the refactor.

- `test_add_record_and_retrieve` — add a record, get it back by ID
- `test_add_records_batch` — add multiple records as a table
- `test_skip_duplicates` — `skip_duplicates=True` silently ignores re-inserts
- `test_flush_pending` — records in pending batch are written on `flush()`
- `test_get_all_records` — retrieves flushed + pending records combined
- `test_overwrite_semantics` — verify merge/overwrite behaviour via `add_records` with matching record IDs (there is no separate `update_record` method; overwrite is achieved through the `merge`/`when_not_matched_insert_all` path)
- `test_schema_evolution` — `allow_schema_evolution=True` handles new columns
- `test_list_sources` — returns correct record paths after flush
- `test_create_base_path_false_raises_if_missing` — local path, `create_base_path=False`, non-existent dir → `ValueError`

### `test_delta_table_database_s3.py` (MinIO, `@pytest.mark.minio`)

Mirrors the local tests with S3 fixtures. All tests use `s3_base_uri` and `s3_storage_options` fixtures.

- `test_s3_add_record_and_retrieve`
- `test_s3_add_records_batch`
- `test_s3_flush_and_read`
- `test_s3_skip_duplicates`
- `test_s3_upath_credentials` — construct `DeltaTableDatabase` using only a `UPath` with embedded credentials (no explicit `storage_options` argument):
  ```python
  upath = UPath(f"s3://...", key=..., secret=..., endpoint_url=..., ...)
  db = DeltaTableDatabase(upath)
  # write and read back a record to confirm end-to-end
  ```
- `test_s3_list_sources_raises` — confirms `NotImplementedError` for cloud paths
- `test_s3_create_base_path_ignored` — `create_base_path=False` does not raise for cloud URIs

### `pytest.ini` — edit existing file

The existing `pytest.ini` already has a `[pytest]` block. Append a `markers` line to it:

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

The `minio` marker does **not** need to be in `addopts` as a default deselection filter. Tests marked `minio` skip themselves gracefully when Docker is unavailable and `MINIO_ENDPOINT` is not set (via the `pytest.skip()` call in the `minio_server` fixture). Developers with Docker get the full suite; developers without Docker see those tests skipped with a clear message.

---

## CI/CD Workflow

New file: `.github/workflows/tests.yml`

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
      - run: uv sync --all-groups
      - run: uv run pytest --tb=short
        env:
          MINIO_ENDPOINT: http://localhost:9000
          MINIO_ACCESS_KEY: minioadmin
          MINIO_SECRET_KEY: minioadmin
```

The `minio_server` fixture detects `MINIO_ENDPOINT` in the environment (set above) and skips testcontainers in CI. Locally, if `MINIO_ENDPOINT` is not set, testcontainers spins up Docker automatically.

---

## Implementation Order (TDD)

1. Add `universal-pathlib` to `[project.dependencies]` and `testcontainers[minio]` to `[dependency-groups] dev` in `pyproject.toml`. Verify no `minio` version conflict between the existing pin and `testcontainers[minio]`'s transitive requirement.
2. Write `tests/test_databases/test_storage_utils.py` — run, confirm all fail (module doesn't exist yet)
3. Write `src/orcapod/databases/storage_utils.py` — run, confirm those tests pass
4. Write `tests/test_databases/test_delta_table_database.py` (local tests) using the new interface (`str | Path | UPath` base_path, `_get_table_uri` naming). Run — most should pass against existing code; failures reveal gaps to fix alongside the S3 work.
5. Write `tests/test_databases/test_delta_table_database_s3.py` — run, confirm all S3 tests fail
6. Update `DeltaTableDatabase`:
   - Add `storage_options` param; call `parse_base_path()` in `__init__`
   - Gate `mkdir()` and `exists()` checks on `not self._is_cloud`
   - Rename `_get_table_path` → `_get_table_uri`, return `str`, update all callers
   - Add `storage_options=self._storage_options` to all `DeltaTable(...)` and `write_deltalake(...)` calls (including the cache-refresh `DeltaTable` call at the end of `flush_batch`)
   - Add `list_sources()` method with `NotImplementedError` for cloud
   - Run full suite; confirm all tests pass
7. Edit `pytest.ini` to add `markers` section
8. Add `.github/workflows/tests.yml`
9. Commit all changes and open PR against `dev`
