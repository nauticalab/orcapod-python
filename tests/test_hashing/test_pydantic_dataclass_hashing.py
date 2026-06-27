"""Regression tests for ITL-432: pydantic/dataclass models as pipeline columns.

Bug A — extension type reaching ArrowDigester:
    Before the fix, hashing a table with a pydantic or dataclass column raised
    ``TypeError: unhashable type: '_ArrowExt_...'`` inside starfix because
    ``StarfixArrowHasher._process_table_columns`` left live ``pa.ExtensionType``
    columns intact, and ``ArrowDigester._primitive_data_type_string`` uses the
    type as a dict key.

Bug B — metadata loss on Polars round-trip:
    Before the fix, ``pl.DataFrame(table).to_arrow()`` raised
    ``ValueError: Arrow extension type '...': expected metadata ... but got b''``
    because the synthesized Polars extension types were built without the
    ``metadata`` argument, so ``__arrow_ext_deserialize__`` received empty bytes.
"""

from __future__ import annotations

import dataclasses

import pyarrow as pa
import polars as pl
import pytest
from pydantic import BaseModel

from orcapod.contexts import get_default_context
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ctx():
    return get_default_context()


@pytest.fixture
def hasher(ctx):
    return StarfixArrowHasher(
        type_converter=ctx.type_converter,
        semantic_hasher=ctx.semantic_hasher,
        hasher_id="test_v0",
    )


# ---------------------------------------------------------------------------
# Model definitions
# ---------------------------------------------------------------------------


class _Point(BaseModel):
    x: int
    y: int


@dataclasses.dataclass
class _Vec:
    a: float
    b: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pydantic_table(ctx) -> pa.Table:
    """Return a two-row Arrow table with a ``_Point`` pydantic model column."""
    arrow_type = ctx.type_converter.register_python_class(_Point)
    storage_vals = [
        ctx.type_converter.python_to_storage(_Point(x=1, y=2), _Point),
        ctx.type_converter.python_to_storage(_Point(x=3, y=4), _Point),
    ]
    ext_arr = pa.ExtensionArray.from_storage(
        arrow_type, pa.array(storage_vals, type=arrow_type.storage_type)
    )
    return pa.table({"pt": ext_arr, "id": pa.array([1, 2], type=pa.int64())})


def _make_dataclass_table(ctx) -> pa.Table:
    """Return a two-row Arrow table with a ``_Vec`` dataclass column."""
    arrow_type = ctx.type_converter.register_python_class(_Vec)
    storage_vals = [
        ctx.type_converter.python_to_storage(_Vec(a=1.0, b=2.0), _Vec),
        ctx.type_converter.python_to_storage(_Vec(a=3.0, b=4.0), _Vec),
    ]
    ext_arr = pa.ExtensionArray.from_storage(
        arrow_type, pa.array(storage_vals, type=arrow_type.storage_type)
    )
    return pa.table({"v": ext_arr, "id": pa.array([1, 2], type=pa.int64())})


# ---------------------------------------------------------------------------
# Bug A regressions — extension type reaching ArrowDigester
# ---------------------------------------------------------------------------


class TestBugAExtensionTypeHashable:
    def test_pydantic_column_does_not_raise(self, ctx, hasher):
        """hash_table on a table with a pydantic model column must not raise TypeError."""
        table = _make_pydantic_table(ctx)
        result = hasher.hash_table(table)
        assert isinstance(result, ContentHash)

    def test_dataclass_column_does_not_raise(self, ctx, hasher):
        """hash_table on a table with a dataclass column must not raise TypeError."""
        table = _make_dataclass_table(ctx)
        result = hasher.hash_table(table)
        assert isinstance(result, ContentHash)

    def test_pydantic_hash_is_deterministic(self, ctx, hasher):
        """Hashing the same pydantic table twice produces identical hashes."""
        table = _make_pydantic_table(ctx)
        assert hasher.hash_table(table) == hasher.hash_table(table)

    def test_dataclass_hash_is_deterministic(self, ctx, hasher):
        """Hashing the same dataclass table twice produces identical hashes."""
        table = _make_dataclass_table(ctx)
        assert hasher.hash_table(table) == hasher.hash_table(table)

    def test_pydantic_different_values_different_hash(self, ctx, hasher):
        """Tables with different pydantic model values produce different hashes."""
        arrow_type = ctx.type_converter.register_python_class(_Point)

        def _table(x, y):
            s = ctx.type_converter.python_to_storage(_Point(x=x, y=y), _Point)
            arr = pa.ExtensionArray.from_storage(
                arrow_type, pa.array([s], type=arrow_type.storage_type)
            )
            return pa.table({"pt": arr})

        assert hasher.hash_table(_table(1, 2)) != hasher.hash_table(_table(9, 9))

    def test_dataclass_different_values_different_hash(self, ctx, hasher):
        """Tables with different dataclass values produce different hashes."""
        arrow_type = ctx.type_converter.register_python_class(_Vec)

        def _table(a, b):
            s = ctx.type_converter.python_to_storage(_Vec(a=a, b=b), _Vec)
            arr = pa.ExtensionArray.from_storage(
                arrow_type, pa.array([s], type=arrow_type.storage_type)
            )
            return pa.table({"v": arr})

        assert hasher.hash_table(_table(1.0, 2.0)) != hasher.hash_table(_table(9.0, 9.0))


# ---------------------------------------------------------------------------
# Bug B regressions — Polars round-trip metadata loss
# ---------------------------------------------------------------------------


class TestBugBPolarsRoundtrip:
    def test_pydantic_polars_roundtrip_does_not_raise(self, ctx, hasher):
        """pl.DataFrame(table).to_arrow() must not raise ValueError for pydantic columns."""
        table = _make_pydantic_table(ctx)
        round_tripped = pl.DataFrame(table).to_arrow()
        result = hasher.hash_table(round_tripped)
        assert isinstance(result, ContentHash)

    def test_dataclass_polars_roundtrip_does_not_raise(self, ctx, hasher):
        """pl.DataFrame(table).to_arrow() must not raise ValueError for dataclass columns."""
        table = _make_dataclass_table(ctx)
        round_tripped = pl.DataFrame(table).to_arrow()
        result = hasher.hash_table(round_tripped)
        assert isinstance(result, ContentHash)

    def test_pydantic_roundtrip_hash_equals_original(self, ctx, hasher):
        """Polars round-trip preserves hash — data content is unchanged."""
        table = _make_pydantic_table(ctx)
        round_tripped = pl.DataFrame(table).to_arrow()
        assert hasher.hash_table(table) == hasher.hash_table(round_tripped)

    def test_dataclass_roundtrip_hash_equals_original(self, ctx, hasher):
        """Polars round-trip preserves hash — data content is unchanged."""
        table = _make_dataclass_table(ctx)
        round_tripped = pl.DataFrame(table).to_arrow()
        assert hasher.hash_table(table) == hasher.hash_table(round_tripped)
