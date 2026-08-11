"""Integration tests for per-process extension type cache behaviour.

The ``LogicalTypeRegistry`` stores registered types in an in-memory dict keyed
by Arrow extension name.  ``register_discovered_logical_types`` skips the factory
call (``reconstruct_from_arrow``) when the extension name is already present in
the registry — this is the "cache hit" path.

Two tests:

1. ``test_cache_populated_after_first_read`` — verifies the type is absent from
   a fresh converter's registry before reading a Parquet file, and present after.

2. ``test_factory_not_called_on_second_read`` — verifies that ``reconstruct_from_arrow``
   is called exactly once (first read) and zero additional times on the second
   read of the same file.
"""
from __future__ import annotations

import dataclasses
from unittest.mock import patch

import pyarrow.parquet as pq

from orcapod.contexts import create_registry
from orcapod.logical_types.dataclass_logical_type_factory import DataclassLogicalTypeFactory


# Module-level dataclass — local classes cannot be reconstructed from FQCN.

@dataclasses.dataclass
class _CachePoint:
    x: int
    y: int


# ── Helpers ───────────────────────────────────────────────────────────────────


def _fresh_converter():
    """Return a fresh UniversalTypeConverter from a new registry instance.

    Uses ``create_registry()`` instead of ``get_default_context()`` to avoid
    cross-test contamination through the global singleton cache.
    """
    return create_registry().get_context().type_converter


def _write_parquet(tmp_path, converter) -> str:
    """Write a _CachePoint column to Parquet and return the file path as str."""
    converter.register_python_class(_CachePoint)
    arrow_schema = converter.python_schema_to_arrow_schema({"point": _CachePoint})
    rows = [{"point": _CachePoint(x=1, y=2)}]
    table = converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)
    parquet_path = tmp_path / "cache_test.parquet"
    pq.write_table(table, str(parquet_path))
    return str(parquet_path)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_cache_populated_after_first_read(tmp_path):
    """Registry has _CachePoint after load_logical_types on a fresh converter.

    Before reading: the fresh converter's registry does not know about _CachePoint.
    After reading: register_discovered_logical_types triggers reconstruct_from_arrow
    which registers _CachePoint, populating the cache.
    """
    write_converter = _fresh_converter()
    parquet_path = _write_parquet(tmp_path, write_converter)

    read_converter = _fresh_converter()
    fqcn = f"{_CachePoint.__module__}.{_CachePoint.__qualname__}"

    # Before read: not registered
    assert read_converter._logical_type_registry.get_by_arrow_extension_name(fqcn) is None

    read_converter.load_logical_types(pq.read_table(parquet_path))

    # After read: registered (cache populated)
    assert read_converter._logical_type_registry.get_by_arrow_extension_name(fqcn) is not None


def test_factory_not_called_on_second_read(tmp_path):
    """reconstruct_from_arrow called once on first read, zero times on second read.

    On first read, register_discovered_logical_types finds _CachePoint's extension
    name in the schema, dispatches to the factory (call count = 1), and stores
    the result in the registry.

    On second read, register_discovered_logical_types finds the extension name already
    in the registry and short-circuits — the factory is not called again
    (call count remains 1).
    """
    write_converter = _fresh_converter()
    parquet_path = _write_parquet(tmp_path, write_converter)

    read_converter = _fresh_converter()

    with patch.object(
        DataclassLogicalTypeFactory,
        "reconstruct_from_arrow",
        autospec=True,
        wraps=DataclassLogicalTypeFactory.reconstruct_from_arrow,
    ) as spy:
        # First read: factory is called once
        read_converter.load_logical_types(pq.read_table(parquet_path))
        assert spy.call_count == 1, f"Expected 1 factory call, got {spy.call_count}"

        # Second read on the same file: registry hit — factory not called again
        read_converter.load_logical_types(pq.read_table(parquet_path))
        assert spy.call_count == 1, (
            f"Expected still 1 factory call after second read, got {spy.call_count}"
        )
