"""
Tests for StarfixArrowHasher — the starfix-backed Arrow content hasher.

Coverage
--------
- hash_schema: returns ContentHash with correct method and 35-byte digest
- hash_table / hash_record_batch: returns ContentHash, digest is 35 bytes
- Column-order independence: reordering columns does not change the hash
- Batch-split independence: splitting a table across batches does not change
  the hash (verified via single-batch and multi-batch RecordBatch round-trips)
- Row-order sensitivity: reordering rows changes the hash
- Type normalisation: Utf8/LargeUtf8 and Binary/LargeBinary hash identically
- Integer width sensitivity: int32 and int64 with the same values differ
- Null handling: null position matters; a null changes the hash vs no-null
- Field nullability: nullable=True and nullable=False produce different schema hashes
- Empty table: hash_table handles a table with zero rows
- Schema-only vs data hash: hash_schema and hash_table differ for the same schema
- versioned_hashers factory: get_versioned_semantic_arrow_hasher returns a
  StarfixArrowHasher with hasher_id == ARROW_HASH_LABEL
- Context integration: the v1 context wires up StarfixArrowHasher
- Stability (golden values): fully-pinned 35-byte digests catch accidental
  algorithm changes in both schema and table hashing
"""

from __future__ import annotations

import pyarrow as pa

from orcapod.hashing.arrow_hashers import ARROW_HASH_LABEL, StarfixArrowHasher
from orcapod.hashing.versioned_hashers import (
    get_versioned_semantic_arrow_hasher,
)
from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

HASHER_ID = "arrow_v1"

_DIGEST_LEN = 35  # 3 version bytes + 32 SHA-256 bytes


def _make_hasher() -> StarfixArrowHasher:
    from orcapod.contexts import get_default_context

    ctx = get_default_context()
    return StarfixArrowHasher(
        type_converter=ctx.type_converter,
        semantic_hasher=ctx.semantic_hasher,
        hasher_id=HASHER_ID,
    )


# ---------------------------------------------------------------------------
# hash_schema
# ---------------------------------------------------------------------------


class TestHashSchema:
    def test_returns_content_hash(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        h = _make_hasher().hash_schema(schema)
        assert isinstance(h, ContentHash)

    def test_method_is_hasher_id(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        h = _make_hasher().hash_schema(schema)
        assert h.method == HASHER_ID

    def test_digest_length(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        h = _make_hasher().hash_schema(schema)
        assert len(h.digest) == _DIGEST_LEN

    def test_deterministic(self):
        schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.float32())])
        h1 = _make_hasher().hash_schema(schema)
        h2 = _make_hasher().hash_schema(schema)
        assert h1.digest == h2.digest

    def test_different_schemas_differ(self):
        s1 = pa.schema([pa.field("a", pa.int32())])
        s2 = pa.schema([pa.field("a", pa.float32())])
        h1 = _make_hasher().hash_schema(s1)
        h2 = _make_hasher().hash_schema(s2)
        assert h1.digest != h2.digest

    def test_column_order_independent(self):
        """Schema field order should not affect the hash."""
        s1 = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.float64())])
        s2 = pa.schema([pa.field("b", pa.float64()), pa.field("a", pa.int32())])
        h1 = _make_hasher().hash_schema(s1)
        h2 = _make_hasher().hash_schema(s2)
        assert h1.digest == h2.digest

    def test_golden_value(self):
        """Pin the full 35-byte digest so any algorithm change is caught."""
        schema = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("value", pa.float64(), nullable=True),
            ]
        )
        h = _make_hasher().hash_schema(schema)
        assert (
            h.digest.hex()
            == "000002e9eb1c825a8a882b565ef45a2ec832c10e31ad067736d2a6cb8a9403ff91c33f"
        ), (
            f"Schema golden digest changed — was the starfix algorithm updated? "
            f"Got: {h.digest.hex()}"
        )

    def test_nullability_affects_schema_hash(self):
        """nullable=True and nullable=False must produce different schema hashes."""
        s_nullable = pa.schema([pa.field("x", pa.int32(), nullable=True)])
        s_non_nullable = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        h_nullable = _make_hasher().hash_schema(s_nullable)
        h_non_nullable = _make_hasher().hash_schema(s_non_nullable)
        assert h_nullable.digest != h_non_nullable.digest

    def test_nullability_golden_values(self):
        """Pin nullable and non-nullable schema digests independently."""
        s_nullable = pa.schema([pa.field("x", pa.int32(), nullable=True)])
        s_non_nullable = pa.schema([pa.field("x", pa.int32(), nullable=False)])
        assert _make_hasher().hash_schema(s_nullable).digest.hex() == (
            "000002fa30e1a356fea8d3a5602a6fa1d49a43349a840f65ead55463f5ead55ea64e9a"
        )
        assert _make_hasher().hash_schema(s_non_nullable).digest.hex() == (
            "0000022571a7dcd462c26761f5b5353ccbd0c8d80655b462c3bb622560ec0d590a5662"
        )


# ---------------------------------------------------------------------------
# hash_table
# ---------------------------------------------------------------------------


class TestHashTable:
    def test_returns_content_hash(self):
        table = pa.table({"x": [1, 2, 3]})
        h = _make_hasher().hash_table(table)
        assert isinstance(h, ContentHash)

    def test_method_is_hasher_id(self):
        table = pa.table({"x": [1, 2, 3]})
        h = _make_hasher().hash_table(table)
        assert h.method == HASHER_ID

    def test_digest_length(self):
        table = pa.table({"x": [1, 2, 3]})
        h = _make_hasher().hash_table(table)
        assert len(h.digest) == _DIGEST_LEN

    def test_deterministic(self):
        table = pa.table({"a": [1, 2], "b": [3.0, 4.0]})
        h1 = _make_hasher().hash_table(table)
        h2 = _make_hasher().hash_table(table)
        assert h1.digest == h2.digest

    def test_column_order_independent(self):
        """Reordering columns must not change the hash."""
        t1 = pa.table({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        t2 = pa.table({"b": [4.0, 5.0, 6.0], "a": [1, 2, 3]})
        h1 = _make_hasher().hash_table(t1)
        h2 = _make_hasher().hash_table(t2)
        assert h1.digest == h2.digest

    def test_different_data_differs(self):
        t1 = pa.table({"x": [1, 2, 3]})
        t2 = pa.table({"x": [1, 2, 4]})
        assert (
            _make_hasher().hash_table(t1).digest != _make_hasher().hash_table(t2).digest
        )

    def test_empty_table(self):
        """Zero-row tables should be hashable without error."""
        table = pa.table({"x": pa.array([], type=pa.int32())})
        h = _make_hasher().hash_table(table)
        assert isinstance(h, ContentHash)
        assert len(h.digest) == _DIGEST_LEN

    def test_schema_hash_differs_from_data_hash(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        table = pa.table({"x": [1, 2, 3]})
        hs = _make_hasher().hash_schema(schema)
        ht = _make_hasher().hash_table(table)
        assert hs.digest != ht.digest

    def test_utf8_largeutf8_normalised(self):
        """Utf8 and LargeUtf8 columns should hash identically (starfix normalises)."""
        t_utf8 = pa.table({"s": pa.array(["hello", "world"], type=pa.utf8())})
        t_large = pa.table({"s": pa.array(["hello", "world"], type=pa.large_utf8())})
        h1 = _make_hasher().hash_table(t_utf8)
        h2 = _make_hasher().hash_table(t_large)
        assert h1.digest == h2.digest

    def test_binary_largebinary_normalised(self):
        """Binary and LargeBinary columns should hash identically (starfix normalises)."""
        t_bin = pa.table({"b": pa.array([b"hello", b"world"], type=pa.binary())})
        t_lbin = pa.table({"b": pa.array([b"hello", b"world"], type=pa.large_binary())})
        assert (
            _make_hasher().hash_table(t_bin).digest
            == _make_hasher().hash_table(t_lbin).digest
        )

    def test_row_order_matters(self):
        """Reordering rows must produce a different hash (rows carry positional semantics)."""
        t_orig = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        t_reversed = pa.table({"x": pa.array([3, 2, 1], type=pa.int64())})
        assert (
            _make_hasher().hash_table(t_orig).digest
            != _make_hasher().hash_table(t_reversed).digest
        )

    def test_null_position_matters(self):
        """A null in a different row position must produce a different hash."""
        t_null_first = pa.table({"x": pa.array([None, 2, 3], type=pa.int32())})
        t_null_second = pa.table({"x": pa.array([1, None, 3], type=pa.int32())})
        assert (
            _make_hasher().hash_table(t_null_first).digest
            != _make_hasher().hash_table(t_null_second).digest
        )

    def test_null_differs_from_no_null(self):
        """A table with a null value must hash differently from one without."""
        t_with_null = pa.table({"x": pa.array([1, None, 3], type=pa.int32())})
        t_no_null = pa.table({"x": pa.array([1, 2, 3], type=pa.int32())})
        assert (
            _make_hasher().hash_table(t_with_null).digest
            != _make_hasher().hash_table(t_no_null).digest
        )

    def test_int32_differs_from_int64(self):
        """Same values stored as int32 and int64 must hash differently."""
        t_i32 = pa.table({"x": pa.array([1, 2, 3], type=pa.int32())})
        t_i64 = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        assert (
            _make_hasher().hash_table(t_i32).digest
            != _make_hasher().hash_table(t_i64).digest
        )

    def test_golden_value_table(self):
        """Pin the full 35-byte table digest so any algorithm change is caught."""
        table = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "score": pa.array([0.1, 0.2, 0.3], type=pa.float64()),
                "label": pa.array(["a", "b", "c"], type=pa.utf8()),
            }
        )
        h = _make_hasher().hash_table(table)
        assert (
            h.digest.hex()
            == "00000277215cd88b3019240dec61d8c4bed8bf6fd05e48d5684e1e7d3acc957f54102c"
        ), (
            f"Table golden digest changed — was the starfix algorithm updated? "
            f"Got: {h.digest.hex()}"
        )


# ---------------------------------------------------------------------------
# RecordBatch (batch-split independence)
# ---------------------------------------------------------------------------


class TestHashRecordBatch:
    def test_record_batch_matches_table(self):
        """hash_table on a RecordBatch must equal hash_table on the equivalent Table."""
        table = pa.table({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        batch = table.to_batches()[0]
        h_table = _make_hasher().hash_table(table)
        h_batch = _make_hasher().hash_table(batch)
        assert h_table.digest == h_batch.digest

    def test_method_is_hasher_id_for_batch(self):
        batch = pa.record_batch({"x": [1, 2]})
        h = _make_hasher().hash_table(batch)
        assert h.method == HASHER_ID

    def test_multi_batch_table_matches_single_batch(self):
        """A Table built from multiple RecordBatches must hash the same as
        an equivalent single-batch Table (chunked arrays are transparent)."""
        b1 = pa.record_batch({"x": pa.array([1, 2], type=pa.int64())})
        b2 = pa.record_batch({"x": pa.array([3, 4], type=pa.int64())})
        t_multi = pa.Table.from_batches([b1, b2])
        t_single = pa.table({"x": pa.array([1, 2, 3, 4], type=pa.int64())})
        h_multi = _make_hasher().hash_table(t_multi)
        h_single = _make_hasher().hash_table(t_single)
        assert h_multi.digest == h_single.digest


# ---------------------------------------------------------------------------
# versioned_hashers factory
# ---------------------------------------------------------------------------


class TestVersionedHashersFactory:
    def test_returns_starfix_hasher(self):
        hasher = get_versioned_semantic_arrow_hasher()
        assert isinstance(hasher, StarfixArrowHasher)

    def test_hasher_id_matches_current_constant(self):
        hasher = get_versioned_semantic_arrow_hasher()
        assert hasher.hasher_id == ARROW_HASH_LABEL

    def test_current_hasher_id_is_arrow_v0_2(self):
        """Pin the current version constant to detect accidental version drifts."""
        assert ARROW_HASH_LABEL == "arrow_v1"


# ---------------------------------------------------------------------------
# Context integration
# ---------------------------------------------------------------------------


class TestContextIntegration:
    def test_v01_context_uses_starfix_hasher(self):
        from orcapod.contexts import resolve_context

        ctx = resolve_context("v1")
        assert isinstance(ctx.arrow_hasher, StarfixArrowHasher)

    def test_v01_context_hasher_id(self):
        from orcapod.contexts import resolve_context

        ctx = resolve_context("v1")
        assert ctx.arrow_hasher.hasher_id == "arrow_v1"

    def test_context_hash_table_functional(self):
        from orcapod.contexts import resolve_context

        ctx = resolve_context("v1")
        table = pa.table({"key": [1, 2, 3], "val": [0.1, 0.2, 0.3]})
        h = ctx.arrow_hasher.hash_table(table)
        assert isinstance(h, ContentHash)
        assert len(h.digest) == _DIGEST_LEN


# ---------------------------------------------------------------------------
# Extension-aware hash path (PLT-1737)
# ---------------------------------------------------------------------------

_EXT_NAME_KEY = b"ARROW:extension:name"
_COMMENT_KEY = b"comment"


class TestHashSchemaExtensionAware:
    def test_extension_free_golden_value_unchanged(self):
        """Stability invariant: extension-free schema hash is byte-identical to
        the starfix format-2 (arrow_v1) golden value (include_metadata=False is the default)."""
        schema = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("value", pa.float64(), nullable=True),
            ]
        )
        h = _make_hasher().hash_schema(schema)
        assert (
            h.digest.hex()
            == "000002e9eb1c825a8a882b565ef45a2ec832c10e31ad067736d2a6cb8a9403ff91c33f"
        ), (
            f"Stability invariant broken — extension-free golden digest changed. Got: {h.digest.hex()}"
        )

    def test_mixed_metadata_hash_equals_extension_only_hash(self):
        """Key property: a schema with ARROW:extension:name plus unrelated metadata
        hashes identically to the same schema with only the extension key."""
        ext_only_schema = pa.schema(
            [
                pa.field("t", pa.binary(), metadata={_EXT_NAME_KEY: b"my.Type"}),
            ]
        )
        mixed_schema = pa.schema(
            [
                pa.field(
                    "t",
                    pa.binary(),
                    metadata={
                        _EXT_NAME_KEY: b"my.Type",
                        _COMMENT_KEY: b"ignored",
                    },
                ),
            ]
        )
        h_ext = _make_hasher().hash_schema(ext_only_schema)
        h_mixed = _make_hasher().hash_schema(mixed_schema)
        assert h_ext.digest == h_mixed.digest

    def test_metadata_key_ordering_does_not_affect_hash(self):
        """Reordering metadata keys in the input does not change the hash."""
        s1 = pa.schema(
            [
                pa.field(
                    "t",
                    pa.binary(),
                    metadata={
                        _EXT_NAME_KEY: b"my.Type",
                        b"ARROW:extension:metadata": b"{}",
                    },
                )
            ]
        )
        s2 = pa.schema(
            [
                pa.field(
                    "t",
                    pa.binary(),
                    metadata={
                        b"ARROW:extension:metadata": b"{}",
                        _EXT_NAME_KEY: b"my.Type",
                    },
                )
            ]
        )
        assert (
            _make_hasher().hash_schema(s1).digest
            == _make_hasher().hash_schema(s2).digest
        )

    def test_extension_metadata_affects_hash(self):
        """Changing an ARROW:extension:name value changes the hash."""
        s1 = pa.schema([pa.field("t", pa.binary(), metadata={_EXT_NAME_KEY: b"TypeA"})])
        s2 = pa.schema([pa.field("t", pa.binary(), metadata={_EXT_NAME_KEY: b"TypeB"})])
        assert (
            _make_hasher().hash_schema(s1).digest
            != _make_hasher().hash_schema(s2).digest
        )

    def test_unrelated_metadata_does_not_affect_hash(self):
        """Adding only unrelated metadata keys does not change the hash."""
        s_clean = pa.schema([pa.field("x", pa.int32())])
        s_noise = pa.schema([pa.field("x", pa.int32(), metadata={_COMMENT_KEY: b"hi"})])
        assert (
            _make_hasher().hash_schema(s_clean).digest
            == _make_hasher().hash_schema(s_noise).digest
        )


class TestHashTableExtensionAware:
    def test_extension_free_table_golden_value_unchanged(self):
        """Stability invariant: extension-free table hash is byte-identical to
        the starfix format-2 (arrow_v1) golden value."""
        table = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "score": pa.array([0.1, 0.2, 0.3], type=pa.float64()),
                "label": pa.array(["a", "b", "c"], type=pa.utf8()),
            }
        )
        h = _make_hasher().hash_table(table)
        assert (
            h.digest.hex()
            == "00000277215cd88b3019240dec61d8c4bed8bf6fd05e48d5684e1e7d3acc957f54102c"
        ), (
            f"Stability invariant broken — extension-free table golden digest changed. Got: {h.digest.hex()}"
        )

    def test_table_mixed_schema_metadata_equals_extension_only(self):
        """Table schema with mixed metadata hashes identically to same table
        schema with only the extension key (unrelated keys are stripped)."""
        schema_ext_only = pa.schema(
            [
                pa.field("x", pa.int32(), metadata={_EXT_NAME_KEY: b"my.Type"}),
            ]
        )
        schema_mixed = pa.schema(
            [
                pa.field(
                    "x",
                    pa.int32(),
                    metadata={
                        _EXT_NAME_KEY: b"my.Type",
                        _COMMENT_KEY: b"ignored",
                    },
                ),
            ]
        )
        data = [pa.array([1, 2, 3], type=pa.int32())]
        t_ext = pa.table(data, schema=schema_ext_only)
        t_mixed = pa.table(data, schema=schema_mixed)
        assert (
            _make_hasher().hash_table(t_ext).digest
            == _make_hasher().hash_table(t_mixed).digest
        )

    def test_table_unrelated_schema_metadata_does_not_affect_hash(self):
        """Adding only unrelated schema metadata does not change the table hash."""
        t_clean = pa.table({"x": pa.array([1, 2], type=pa.int32())})
        schema_noise = pa.schema(
            [pa.field("x", pa.int32(), metadata={_COMMENT_KEY: b"hi"})]
        )
        t_noise = pa.table([pa.array([1, 2], type=pa.int32())], schema=schema_noise)
        assert (
            _make_hasher().hash_table(t_clean).digest
            == _make_hasher().hash_table(t_noise).digest
        )


class TestSchemaHashFieldRule:
    """``hash_schema`` hashes a field under the same rule as ``hash_table`` (ITL-981)."""

    @staticmethod
    def _identified(storage: pa.DataType, extension: pa.ExtensionType) -> pa.Schema:
        """A plain ``storage`` field that carries the identifier of ``extension``."""
        return pa.schema(
            [
                pa.field(
                    "c",
                    storage,
                    metadata={
                        "ARROW:extension:name": extension.extension_name,
                        "ARROW:extension:metadata": extension.__arrow_ext_serialize__(),
                    },
                )
            ]
        )

    def test_value_hashed_type_does_not_depend_on_its_storage_type(self):
        """A type hashed by a value handler is a ``large_binary`` field with its identifier."""
        import numpy as np

        import orcapod as op
        from orcapod.contexts import get_default_context

        context = get_default_context()
        hasher = context.arrow_hasher
        assert isinstance(hasher, StarfixArrowHasher)
        for python_type in (op.File, np.ndarray):
            extension = context.type_converter.python_type_to_arrow_type(python_type)
            assert isinstance(extension, pa.ExtensionType)
            schema_hash = hasher.hash_schema(pa.schema([pa.field("c", extension)]))
            assert schema_hash == hasher.hash_schema(
                self._identified(pa.large_binary(), extension)
            )
        # ``File`` is stored as a string; that storage type is not in its schema hash.
        file_type = context.type_converter.python_type_to_arrow_type(op.File)
        assert isinstance(file_type, pa.ExtensionType)
        assert file_type.storage_type == pa.large_string()
        assert hasher.hash_schema(
            pa.schema([pa.field("c", file_type)])
        ) != hasher.hash_schema(self._identified(pa.large_string(), file_type))

    def test_type_without_value_handler_keeps_its_storage_type(self):
        """A type whose stored values are hashed keeps its storage type in the schema hash."""
        from pathlib import Path

        from orcapod.contexts import get_default_context

        context = get_default_context()
        hasher = context.arrow_hasher
        assert isinstance(hasher, StarfixArrowHasher)
        extension = context.type_converter.python_type_to_arrow_type(Path)
        assert isinstance(extension, pa.ExtensionType)
        assert hasher.hash_schema(
            pa.schema([pa.field("c", extension)])
        ) == hasher.hash_schema(self._identified(extension.storage_type, extension))
