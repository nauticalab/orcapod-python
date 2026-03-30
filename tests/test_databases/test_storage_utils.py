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
