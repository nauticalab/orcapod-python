"""
Storage utility helpers for DeltaTableDatabase.

Provides three public functions:
- parse_base_path: Converts any path type (str, Path, UPath) into a URI string
  and a merged dict of deltalake-compatible storage_options.
- is_cloud_uri: Returns True for cloud storage schemes (s3://, gs://, etc.).
- delta_to_pyarrow_dataset: Opens a Delta table as a pyarrow dataset, reading local
  tables through a native filesystem.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    from pathlib import Path

    import deltalake
    import pyarrow.dataset as pa_ds
    import pyarrow.fs as pa_fs
    from upath import UPath
else:
    pa_fs = LazyModule("pyarrow.fs")

logger = logging.getLogger(__name__)

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


def delta_to_pyarrow_dataset(
    delta_table: deltalake.DeltaTable, **kwargs: Any
) -> pa_ds.Dataset:
    """Open a Delta table as a pyarrow dataset, natively for local tables.

    By default ``DeltaTable.to_pyarrow_dataset`` reads through
    ``PyFileSystem(DeltaStorageHandler)``, a filesystem implemented in Python. Arrow
    finishes tearing down a scan on a thread-pool worker after the scan returns. If that
    worker releases a Python-backed buffer while the interpreter is finalizing, the process
    hangs at exit in Arrow's thread-pool destructor, or aborts on Linux with Python
    3.11/3.12 (ITL-892; apache/arrow#50482, apache/arrow#54536).

    A local table is therefore read through a native pyarrow filesystem rooted at the
    table, which returns the same schema and values without Python file objects. Any other
    URI, a cloud database or a ``filesystem`` passed by the caller, keeps deltalake's reader.

    ``table_uri`` is percent-encoded: deltalake 1.0.2 and 1.2.1 return a bare path such as
    ``/data/a%20b/``, and 1.6.6 returns ``file:///data/a%20b/``. pyarrow's own URI parser
    decodes it to the native path. A root that is neither a ``file://`` URI nor an absolute
    POSIX path (a Windows drive path, untested) keeps deltalake's reader.

    Args:
        delta_table: The Delta table to open.
        **kwargs: Passed to ``DeltaTable.to_pyarrow_dataset`` unchanged.

    Returns:
        The pyarrow dataset over the table's current version.
    """
    uri = delta_table.table_uri
    file_uri = f"file://{uri}" if uri.startswith("/") else uri
    if kwargs.get("filesystem") is None and file_uri.startswith("file://"):
        filesystem, root = pa_fs.FileSystem.from_uri(file_uri)
        kwargs["filesystem"] = pa_fs.SubTreeFileSystem(root, filesystem)
    return delta_table.to_pyarrow_dataset(**kwargs)


def _extract_upath_options(upath: UPath) -> dict[str, str]:
    """
    Extract storage options from a UPath and translate to deltalake's AWS_* format.

    Top-level keys take precedence; client_kwargs.endpoint_url is a fallback for
    AWS_ENDPOINT_URL only.
    """
    try:
        fsspec_opts: dict = dict(getattr(upath, "storage_options", {}))
    except TypeError:
        logger.warning(
            "Could not extract storage_options from UPath %r; using empty options",
            upath,
        )
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

    uri = str(base_path)
    derived = _extract_upath_options(base_path) if is_upath else {}  # type: ignore[arg-type]

    merged = {**derived, **(storage_options or {})}
    return uri, merged
