"""Regression tests for ITL-432: pydantic/dataclass models as pipeline columns.

These tests cover the exact scenarios described in bug report #184:

    "Pydantic and dataclass models cannot flow through Orcapod pipelines as
    columns, even though Parquet/IPC serialization works correctly."

Bug A — extension type reaching ArrowDigester:
    Before the fix, hashing a table with a pydantic or dataclass column raised
    ``TypeError: unhashable type: '_ArrowExt_...'`` inside starfix because
    ``StarfixArrowHasher._process_table_columns`` left live ``pa.ExtensionType``
    columns intact, and ``ArrowDigester._primitive_data_type_string`` uses the
    type as a dict key.

    Impact from the bug report: "Building any source that carries a Pydantic or
    dataclass column crashes, because starfix requires hashable types for schema
    operations."

Bug B — metadata loss on Polars round-trip:
    Before the fix, ``pl.DataFrame(table).to_arrow()`` raised
    ``ValueError: Arrow extension type '...': expected metadata ... but got b''``
    because the synthesized Polars extension types were built without the
    ``metadata`` argument, so ``__arrow_ext_deserialize__`` received empty bytes.

    Impact from the bug report: "Join operations that round-trip through
    pl.DataFrame(table).to_arrow() fail when processing model columns."

Test coverage:
    1. Low-level: direct ``StarfixArrowHasher.hash_table`` on extension-type tables.
    2. End-to-end pipeline: ``DictSource``, ``ArrowTableStream.content_hash()``,
       ``PolarsFilter``, and ``Join`` — all operators that trigger the two bugs in
       real usage.
"""

from __future__ import annotations

import dataclasses
from typing import Literal

import pyarrow as pa
import polars as pl
import pytest
from pydantic import BaseModel

from orcapod.contexts import get_default_context
from orcapod.core.operators import Join, PolarsFilter
from orcapod.core.sources import DictSource
from orcapod.core.streams import ArrowTableStream
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
# Model definitions — must be at module level so their FQCNs are importable
# ---------------------------------------------------------------------------


class _Point(BaseModel):
    x: int
    y: int


@dataclasses.dataclass
class _Vec:
    a: float
    b: float


# Models for DictSource pipeline tests.  Separate names to keep registrations
# independent of the hashing-level model registrations above.
class _Cfg(BaseModel):
    lr: float
    epochs: int


@dataclasses.dataclass
class _Run:
    seed: int
    batch_size: int


# #187 follow-up: models whose fields use typing.Literal (a very common pydantic
# config pattern). A Literal field is stored as its underlying scalar type.
class _LiteralCfg(BaseModel):
    method: Literal["dredge", "iterative", "medicine"]
    peak_sign: Literal["neg", "pos", "both"]
    threshold: float


class _MixedLiteralCfg(BaseModel):
    bad: Literal["a", 1]            # members span multiple types — unsupported


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


def _make_pydantic_stream(ctx) -> ArrowTableStream:
    """Return a stream with tag ``id`` and pydantic model data column ``pt``."""
    arrow_type = ctx.type_converter.register_python_class(_Point)
    storage_vals = [
        ctx.type_converter.python_to_storage(_Point(x=1, y=2), _Point),
        ctx.type_converter.python_to_storage(_Point(x=3, y=4), _Point),
    ]
    ext_arr = pa.ExtensionArray.from_storage(
        arrow_type, pa.array(storage_vals, type=arrow_type.storage_type)
    )
    table = pa.table({"id": pa.array([1, 2], type=pa.int64()), "pt": ext_arr})
    return ArrowTableStream(table, tag_columns=["id"])


def _make_dataclass_stream(ctx) -> ArrowTableStream:
    """Return a stream with tag ``id`` and dataclass data column ``v``."""
    arrow_type = ctx.type_converter.register_python_class(_Vec)
    storage_vals = [
        ctx.type_converter.python_to_storage(_Vec(a=1.0, b=2.0), _Vec),
        ctx.type_converter.python_to_storage(_Vec(a=3.0, b=4.0), _Vec),
    ]
    ext_arr = pa.ExtensionArray.from_storage(
        arrow_type, pa.array(storage_vals, type=arrow_type.storage_type)
    )
    table = pa.table({"id": pa.array([1, 2], type=pa.int64()), "v": ext_arr})
    return ArrowTableStream(table, tag_columns=["id"])


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


# ---------------------------------------------------------------------------
# End-to-end pipeline tests — replicating the exact usage scenario from #184
# ---------------------------------------------------------------------------


class TestEndToEndPipelineWithModelColumns:
    """End-to-end pipeline tests matching the bug report scenarios.

    The bug report states:
    - "Building any source that carries a Pydantic or dataclass column crashes,
      because starfix requires hashable types for schema operations." (Bug A)
    - "Join operations that round-trip through pl.DataFrame(table).to_arrow()
      fail when processing model columns." (Bug B)

    These tests replicate those exact paths through the real pipeline API.
    """

    # ------------------------------------------------------------------
    # DictSource — the natural way to build a source with model columns
    # ------------------------------------------------------------------

    def test_dict_source_pydantic_column_content_hash(self, ctx):
        """DictSource with pydantic column: content_hash must not crash (Bug A).

        This is the primary bug-report scenario: a user puts pydantic model
        instances into a source and tries to hash it.
        """
        # Register _Cfg with the default context's type converter so that
        # DictSource can resolve it when building the Arrow schema.
        ctx.type_converter.register_python_class(_Cfg)
        src = DictSource(
            data=[
                {"run_id": 1, "cfg": _Cfg(lr=0.01, epochs=10)},
                {"run_id": 2, "cfg": _Cfg(lr=0.001, epochs=20)},
            ],
            tag_columns=["run_id"],
            data_schema={"run_id": int, "cfg": _Cfg},
        )
        # DictSource IS the stream — content_hash() is called directly on it.
        result = src.content_hash()
        assert isinstance(result, ContentHash)

    def test_dict_source_dataclass_column_content_hash(self, ctx):
        """DictSource with dataclass column: content_hash must not crash (Bug A)."""
        # Register _Run with the default context's type converter so that
        # DictSource can resolve it when building the Arrow schema.
        ctx.type_converter.register_python_class(_Run)
        src = DictSource(
            data=[
                {"run_id": 1, "run": _Run(seed=42, batch_size=32)},
                {"run_id": 2, "run": _Run(seed=7, batch_size=64)},
            ],
            tag_columns=["run_id"],
            data_schema={"run_id": int, "run": _Run},
        )
        result = src.content_hash()
        assert isinstance(result, ContentHash)

    def test_dict_source_pydantic_hash_reflects_model_values(self, ctx):
        """Different model values produce different content hashes."""
        # Register _Cfg so DictSource's Arrow schema conversion succeeds.
        ctx.type_converter.register_python_class(_Cfg)

        def _src(lr):
            return DictSource(
                data=[{"run_id": 1, "cfg": _Cfg(lr=lr, epochs=10)}],
                tag_columns=["run_id"],
                data_schema={"run_id": int, "cfg": _Cfg},
            )

        assert _src(0.01).content_hash() != _src(0.1).content_hash()

    # ------------------------------------------------------------------
    # ArrowTableStream.content_hash — stream-level Bug A trigger
    # ------------------------------------------------------------------

    def test_stream_with_pydantic_column_content_hash(self, ctx):
        """ArrowTableStream.content_hash on a pydantic column must not crash (Bug A)."""
        stream = _make_pydantic_stream(ctx)
        result = stream.content_hash()
        assert isinstance(result, ContentHash)

    def test_stream_with_dataclass_column_content_hash(self, ctx):
        """ArrowTableStream.content_hash on a dataclass column must not crash (Bug A)."""
        stream = _make_dataclass_stream(ctx)
        result = stream.content_hash()
        assert isinstance(result, ContentHash)

    def test_stream_content_hash_is_deterministic(self, ctx):
        """content_hash is stable across repeated calls."""
        stream = _make_pydantic_stream(ctx)
        assert stream.content_hash() == stream.content_hash()

    # ------------------------------------------------------------------
    # PolarsFilter — Bug B trigger via pl.DataFrame(table).filter().to_arrow()
    # ------------------------------------------------------------------

    def test_pydantic_column_through_polars_filter(self, ctx):
        """PolarsFilter on a stream with a pydantic column must not crash (Bug B).

        PolarsFilter calls pl.DataFrame(table).filter(...).to_arrow() internally.
        Before the fix this raised ValueError from __arrow_ext_deserialize__.
        """
        stream = _make_pydantic_stream(ctx)
        filtered = PolarsFilter().process(stream)
        result = filtered.as_table()
        assert len(result) == 2

    def test_dataclass_column_through_polars_filter(self, ctx):
        """PolarsFilter on a stream with a dataclass column must not crash (Bug B)."""
        stream = _make_dataclass_stream(ctx)
        filtered = PolarsFilter().process(stream)
        result = filtered.as_table()
        assert len(result) == 2

    def test_polars_filter_with_constraint_on_pydantic_stream(self, ctx):
        """PolarsFilter with an id constraint correctly filters rows and preserves model column."""
        stream = _make_pydantic_stream(ctx)
        # Filter to only keep id == 1
        filtered = PolarsFilter(constraints={"id": 1}).process(stream)
        result = filtered.as_table()
        assert len(result) == 1

    def test_polars_filter_no_op_preserves_all_rows(self, ctx):
        """A no-op PolarsFilter (no constraints) returns all rows with all columns intact.

        Note: content_hash() will differ between the raw stream and the filtered
        stream because they have different producers (different identity_structure),
        but the underlying data must be identical.
        """
        stream = _make_pydantic_stream(ctx)
        filtered = PolarsFilter().process(stream)
        original_table = stream.as_table()
        filtered_table = filtered.as_table()
        assert len(filtered_table) == len(original_table)
        assert "pt" in filtered_table.column_names
        assert "id" in filtered_table.column_names

    # ------------------------------------------------------------------
    # Join — Bug B trigger via pl.DataFrame(table).join(...).to_arrow()
    # ------------------------------------------------------------------

    def test_pydantic_column_through_join(self, ctx):
        """Join on a stream with a pydantic column must not crash (Bug B).

        Join calls pl.DataFrame(table).join(...).to_arrow() internally.
        Before the fix this raised ValueError from __arrow_ext_deserialize__.
        """
        pydantic_stream = _make_pydantic_stream(ctx)
        # Second stream shares the "id" tag but has a plain score column.
        plain_table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "score": pa.array([0.9, 0.8], type=pa.float64()),
        })
        plain_stream = ArrowTableStream(plain_table, tag_columns=["id"])

        out = Join().process(pydantic_stream, plain_stream)
        result = out.as_table()
        assert len(result) == 2
        # Both the pydantic column and the score column should be present
        assert "pt" in result.column_names
        assert "score" in result.column_names

    def test_dataclass_column_through_join(self, ctx):
        """Join on a stream with a dataclass column must not crash (Bug B)."""
        dataclass_stream = _make_dataclass_stream(ctx)
        plain_table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "score": pa.array([0.9, 0.8], type=pa.float64()),
        })
        plain_stream = ArrowTableStream(plain_table, tag_columns=["id"])

        out = Join().process(dataclass_stream, plain_stream)
        result = out.as_table()
        assert len(result) == 2
        assert "v" in result.column_names
        assert "score" in result.column_names

    def test_join_partial_overlap_with_pydantic_column(self, ctx):
        """Join with partial tag overlap correctly returns only matched rows."""
        pydantic_stream = _make_pydantic_stream(ctx)
        # Second stream only has id=1 — join result should be 1 row.
        plain_table = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "score": pa.array([0.9], type=pa.float64()),
        })
        plain_stream = ArrowTableStream(plain_table, tag_columns=["id"])

        out = Join().process(pydantic_stream, plain_stream)
        result = out.as_table()
        assert len(result) == 1


# ---------------------------------------------------------------------------
# #187: typing.Literal fields in a model column
# ---------------------------------------------------------------------------


class TestLiteralFields:
    """A model whose fields use ``typing.Literal`` must flow as a pipeline column.

    Follow-up to ITL-432/#184: the generic pydantic-column support landed, but a
    ``Literal`` field raised ``Unsupported annotation: typing.Literal[...]`` while
    building the Arrow struct. A ``Literal`` is stored as its underlying scalar
    type, and pydantic re-validates the allowed set on reconstruction.
    """

    def test_literal_model_registers(self, ctx):
        """register_python_class succeeds; Literal[str] fields → large_string storage."""
        arrow_type = ctx.type_converter.register_python_class(_LiteralCfg)
        storage = arrow_type.storage_type
        assert pa.types.is_large_string(storage.field("method").type)
        assert pa.types.is_large_string(storage.field("peak_sign").type)

    def test_literal_field_roundtrips(self, ctx):
        """python_to_storage → storage_to_python preserves the Literal value."""
        ctx.type_converter.register_python_class(_LiteralCfg)
        original = _LiteralCfg(method="iterative", peak_sign="both", threshold=0.5)
        storage = ctx.type_converter.python_to_storage(original, _LiteralCfg)
        restored = ctx.type_converter.storage_to_python(storage, _LiteralCfg)
        assert restored == original
        assert restored.method == "iterative"

    def test_dict_source_literal_column_content_hash(self, ctx):
        """DictSource with a Literal-bearing column hashes and reflects values."""
        ctx.type_converter.register_python_class(_LiteralCfg)

        def _src(method):
            return DictSource(
                data=[{"run_id": 1,
                       "cfg": _LiteralCfg(method=method, peak_sign="neg", threshold=0.5)}],
                tag_columns=["run_id"],
                data_schema={"run_id": int, "cfg": _LiteralCfg},
            )

        assert isinstance(_src("dredge").content_hash(), ContentHash)
        assert _src("dredge").content_hash() != _src("medicine").content_hash()

    def test_mixed_type_literal_rejected(self, ctx):
        """A Literal whose members span multiple types is rejected clearly."""
        with pytest.raises(ValueError, match="Mixed-type Literal"):
            ctx.type_converter.register_python_class(_MixedLiteralCfg)
