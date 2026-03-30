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
