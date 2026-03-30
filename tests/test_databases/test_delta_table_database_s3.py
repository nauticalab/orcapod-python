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
