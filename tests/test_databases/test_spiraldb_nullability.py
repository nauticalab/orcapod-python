"""The Spiral nullability record (ITL-936): writing, reading and its integrity checks.

SpiralDB reports every top-level column and every struct field nullable, whatever was
written, so Orcapod records the non-nullable column paths in the table's key-value metadata.
These tests drive the two public helpers, ``write_nullability_record`` and
``read_nullability_record``, against a table of the disk-backed pyspiral fake.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.databases import read_nullability_record, write_nullability_record
from orcapod.errors import NullabilityRecordError
from tests.test_databases import fake_pyspiral

_KEY = "__orcapod_nullability__"

_SCHEMA = pa.schema(
    [
        pa.field("k", pa.int64(), False),
        pa.field("v", pa.int32(), True),
        pa.field(
            "g",
            pa.struct(
                [pa.field("x", pa.int32(), False), pa.field("y", pa.int32(), True)]
            ),
            False,
        ),
        pa.field("l", pa.list_(pa.field("item", pa.float32(), False)), False),
        pa.field("f", pa.list_(pa.field("item", pa.float32(), True), 2), True),
        pa.field(
            "n",
            pa.large_list(pa.struct([pa.field("a", pa.string(), False)])),
            True,
        ),
        pa.field("m", pa.map_(pa.string(), pa.int64()), True),
    ]
)
_PATHS = frozenset({("k",), ("g",), ("g", "x"), ("l",), ("l", "[]"), ("n", "[]", "a")})


@pytest.fixture()
def table(tmp_path: Path):
    """Provide an empty fake Spiral table to hold metadata."""
    with fake_pyspiral.install(tmp_path / "spiral"):
        yield (
            fake_pyspiral.FakeSpiral()
            .project("p")
            .create_table("default.t", key_schema=[("k", pa.int64())])
        )


def _header(table: fake_pyspiral.FakeTable) -> dict[str, Any]:
    """Return the decoded header of ``table``'s nullability record."""
    return json.loads(table.get_metadata()[_KEY])


def _chunk_keys(table: fake_pyspiral.FakeTable) -> list[str]:
    """Return the chunk keys of ``table``'s nullability record, sorted."""
    return sorted(k for k in table.get_metadata() if k.startswith(_KEY + "/"))


class TestRoundTrip:
    def test_non_nullable_paths_at_every_depth(self, table):
        """Top-level, struct-field, list-item and nested paths round-trip; maps are skipped."""
        write_nullability_record(table, _SCHEMA)
        assert read_nullability_record(table.get_metadata()) == _PATHS

    def test_no_record_reads_as_none(self, table):
        """A table without a header key has no record."""
        assert read_nullability_record(table.get_metadata()) is None

    def test_all_nullable_schema_records_an_empty_set(self, table):
        """A schema with no non-null field records an empty set, not no record."""
        write_nullability_record(table, pa.schema([pa.field("k", pa.int64())]))
        assert read_nullability_record(table.get_metadata()) == frozenset()

    def test_header_is_versioned(self, table):
        """The header carries the format name and version 1."""
        write_nullability_record(table, _SCHEMA)
        header = _header(table)
        assert header["format"] == "orcapod.nullability"
        assert header["version"] == 1

    def test_unchanged_record_is_not_rewritten(self, table):
        """Writing the same record twice writes once and returns False the second time."""
        assert write_nullability_record(table, _SCHEMA) is True
        assert write_nullability_record(table, _SCHEMA) is False

    def test_replacing_drops_the_old_chunks(self, table):
        """A replacement record is read back and the earlier chunks are gone."""
        write_nullability_record(table, _SCHEMA)
        old_chunks = _chunk_keys(table)
        narrower = pa.schema([pa.field("k", pa.int64(), False)])
        assert write_nullability_record(table, narrower) is True
        assert read_nullability_record(table.get_metadata()) == frozenset({("k",)})
        assert not set(old_chunks) & set(_chunk_keys(table))

    def test_other_metadata_is_left_alone(self, table):
        """Writing the record keeps unrelated metadata keys."""
        table.set_metadata({"__arrow_metadata__": b"{}"})
        write_nullability_record(table, _SCHEMA)
        assert table.get_metadata()["__arrow_metadata__"] == b"{}"

    def test_wide_table_is_chunked_under_the_8_kib_limit(self, table):
        """A 400-column record is split into chunks, each under SpiralDB's limits."""
        names = [f"column_with_a_long_name_{i:04d}" for i in range(400)]
        wide = pa.schema(
            [pa.field("k", pa.int64(), False)]
            + [pa.field(n, pa.float64(), False) for n in names]
        )
        write_nullability_record(table, wide)
        metadata = table.get_metadata()
        chunk_keys = _chunk_keys(table)
        assert len(chunk_keys) > 1
        for key in [_KEY, *chunk_keys]:
            assert len(key.encode()) + len(metadata[key]) <= 8192
            assert len(key) <= 255
        assert read_nullability_record(metadata) == frozenset(
            {("k",), *((n,) for n in names)}
        )

    def test_non_null_map_item_is_refused(self, table):
        """A non-null map item cannot be recorded and nothing is written."""
        schema = pa.schema(
            [
                pa.field(
                    "m",
                    # pyarrow accepts key/item fields here; its stubs list only types.
                    pa.map_(
                        pa.field("key", pa.string(), False),  # pyright: ignore[reportArgumentType]
                        pa.field("value", pa.int64(), False),  # pyright: ignore[reportArgumentType]
                    ),
                )
            ]
        )
        with pytest.raises(NullabilityRecordError, match="'m'"):
            write_nullability_record(table, schema)
        assert read_nullability_record(table.get_metadata()) is None


class TestIntegrity:
    """A record that cannot be read in full is an error, never ignored."""

    def test_unknown_version(self, table):
        """A header with an unknown version raises."""
        write_nullability_record(table, _SCHEMA)
        header = _header(table)
        header["version"] = 2
        table.set_metadata({_KEY: json.dumps(header).encode()})
        with pytest.raises(NullabilityRecordError, match="version 2"):
            read_nullability_record(table.get_metadata())

    def test_unknown_format(self, table):
        """A header with an unknown format raises."""
        write_nullability_record(table, _SCHEMA)
        header = _header(table)
        header["format"] = "something.else"
        table.set_metadata({_KEY: json.dumps(header).encode()})
        with pytest.raises(NullabilityRecordError, match="format"):
            read_nullability_record(table.get_metadata())

    def test_header_not_json(self, table):
        """A header that is not JSON raises."""
        table.set_metadata({_KEY: b"not json"})
        with pytest.raises(NullabilityRecordError):
            read_nullability_record(table.get_metadata())

    def test_missing_chunk(self, table):
        """A missing chunk raises."""
        write_nullability_record(table, _SCHEMA)
        table.drop_metadata(_chunk_keys(table)[:1])
        with pytest.raises(NullabilityRecordError, match="missing"):
            read_nullability_record(table.get_metadata())

    def test_digest_mismatch(self, table):
        """A chunk that does not match the header's digest raises."""
        write_nullability_record(table, _SCHEMA)
        table.set_metadata({_chunk_keys(table)[0]: b'[["v"]]'})
        with pytest.raises(NullabilityRecordError, match="digest"):
            read_nullability_record(table.get_metadata())

    def test_payload_not_a_list_of_paths(self, table):
        """A payload that is not a list of string lists raises."""
        write_nullability_record(table, _SCHEMA)
        header = _header(table)
        payload = b'{"k": true}'
        digest = hashlib.sha256(payload).hexdigest()
        header["sha256"] = digest
        header["chunks"] = 1
        # Chunk keys are named by the payload digest (the documented format).
        chunk = f"{_KEY}/{digest[:16]}/0"
        table.set_metadata({chunk: payload, _KEY: json.dumps(header).encode()})
        with pytest.raises(NullabilityRecordError, match="path"):
            read_nullability_record(table.get_metadata())

    def test_chunks_without_header_are_no_record(self, table):
        """Chunks left without their header are no record."""
        write_nullability_record(table, _SCHEMA)
        table.drop_metadata([_KEY])
        assert read_nullability_record(table.get_metadata()) is None
