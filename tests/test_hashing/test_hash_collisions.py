"""Values of different types never share a hash (ITL-1154, ITL-1152).

Through the public hasher (``hash_object``, ``hash_table``):

* ``bytes``, ``str`` and ``uuid.UUID`` never collide, at the top level or nested (rule 2:
  a value handler describes its value as tagged data).
* ``frozenset`` is a type of its own, apart from ``set``.
* A hash fed into another hash carries its label (rule 1), so two algorithms that produce
  the same digest feed different inputs: a ``File`` whose bytes are a value's canonical
  JSON, and a Polars expression whose canonical tree equals a dict.
* A UUID column is hashed as stored (rule 3): no value handler, so no structure label
  reaches the table hash, while a bare ``uuid.UUID`` keeps its tagged value hash.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import polars as pl
import pyarrow as pa
import pytest

import orcapod as op
from orcapod.contexts import get_default_context
from orcapod.hashing.semantic_hashing.polars_expr import polars_expr_canonical
from orcapod.sources import DictSource
from orcapod.types import ContentHash

RAW = b"hello"
VALUE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _canonical(tree: Any) -> bytes:
    """The canonical JSON bytes the structure hasher digests."""
    return json.dumps(
        tree, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def _hash(value: Any) -> ContentHash:
    return get_default_context().semantic_hasher.hash_object(value)


WRAPS: list[Callable[[Any], Any]] = [
    lambda v: v,
    lambda v: [v],
    lambda v: {"k": v},
    lambda v: (1, v),
    lambda v: {"k": [v]},
]
WRAP_IDS = ["top", "list", "dict", "tuple", "nested"]


class TestBytesStrAndUuid:
    """``bytes``, ``str`` and ``uuid.UUID`` never collide."""

    @pytest.mark.parametrize("wrap", WRAPS, ids=WRAP_IDS)
    @pytest.mark.parametrize("make", [bytes, bytearray], ids=["bytes", "bytearray"])
    @pytest.mark.parametrize(
        "raw", [RAW, b"", b"\x00\x01"], ids=["text", "empty", "binary"]
    )
    def test_bytes_never_hash_as_their_hex_string(
        self, wrap: Callable[[Any], Any], make: type, raw: bytes
    ) -> None:
        """A bytes value differs from the ``str`` of its hex digits."""
        assert _hash(wrap(make(raw))) != _hash(wrap(raw.hex()))

    @pytest.mark.parametrize("wrap", WRAPS, ids=WRAP_IDS)
    def test_bytes_never_hash_as_their_text(self, wrap: Callable[[Any], Any]) -> None:
        """A bytes value differs from the ``str`` it decodes to."""
        assert _hash(wrap(RAW)) != _hash(wrap(RAW.decode()))

    def test_bytes_and_bytearray_of_equal_content_hash_equal(self) -> None:
        """``bytes`` and ``bytearray`` with the same content are one value."""
        assert _hash(RAW) == _hash(bytearray(RAW))

    def test_bytes_is_its_tagged_hex(self) -> None:
        """A bytes value is hashed as ``{"__type__": "bytes", "hex": ...}``."""
        tree = {"__type__": "bytes", "hex": RAW.hex()}
        assert _hash(RAW).digest == hashlib.sha256(_canonical(tree)).digest()

    @pytest.mark.parametrize("wrap", WRAPS, ids=WRAP_IDS)
    def test_uuid_differs_from_its_bytes_and_hex(
        self, wrap: Callable[[Any], Any]
    ) -> None:
        """A UUID differs from its 16 bytes, its hex text and its canonical text."""
        as_uuid = _hash(wrap(VALUE))
        assert as_uuid != _hash(wrap(VALUE.bytes))
        assert as_uuid != _hash(wrap(VALUE.hex))
        assert as_uuid != _hash(wrap(str(VALUE)))

    def test_uuid_is_its_tagged_hex(self) -> None:
        """A bare UUID is hashed as ``{"__type__": "uuid", "hex": ...}``."""
        tree = {"__type__": "uuid", "hex": VALUE.hex}
        assert _hash(VALUE).digest == hashlib.sha256(_canonical(tree)).digest()


class TestFrozensetIsItsOwnType:
    """``frozenset`` is tagged ``frozenset``, apart from ``set``."""

    @pytest.mark.parametrize("wrap", WRAPS, ids=WRAP_IDS)
    @pytest.mark.parametrize(
        "items", [(), (1, 2), ("a", "b")], ids=["empty", "ints", "strs"]
    )
    def test_frozenset_differs_from_set(
        self, wrap: Callable[[Any], Any], items: tuple[Any, ...]
    ) -> None:
        """A frozenset and a set of the same items hash differently."""
        assert _hash(wrap(frozenset(items))) != _hash(wrap(set(items)))

    def test_frozenset_is_tagged_and_sorted_like_set(self) -> None:
        """A frozenset is ``{"__type__": "frozenset", "items": [...]}``, items sorted."""
        tree = {"__type__": "frozenset", "items": [1, 2, 3]}
        expected = hashlib.sha256(_canonical(tree)).digest()
        assert _hash(frozenset({3, 1, 2})).digest == expected

    def test_frozenset_annotation_differs_from_set_annotation(self) -> None:
        """``frozenset[int]`` and ``set[int]`` are different types."""
        assert _hash(frozenset[int]) != _hash(set[int])


class TestTwoAlgorithmsOneDigest:
    """Two algorithms that produce one digest still feed different inputs (rule 1)."""

    def test_file_whose_bytes_are_a_values_canonical_json(self, tmp_path: Path) -> None:
        """A ``File`` and a value whose digests coincide differ inside a structure."""
        value = {"a": 1, "b": [1, 2]}
        path = tmp_path / "value.json"
        path.write_bytes(_canonical(value))
        file_hash = _hash(op.File(path))
        value_hash = _hash(value)
        # The two algorithms give the same digest here ...
        assert file_hash.digest == value_hash.digest
        # ... and the labels keep the structures that hold them apart.
        assert _hash(["x", op.File(path)]) != _hash(["x", value_hash])
        assert _hash({"k": op.File(path)}) != _hash({"k": value_hash})

    def test_polars_expression_and_a_hash_of_its_tree(self) -> None:
        """A Polars expression and the structure hash of its canonical tree differ."""
        expr = pl.col("x") > 1
        tree = polars_expr_canonical(expr)
        expr_hash = _hash(expr)
        tree_hash = _hash(tree)
        assert expr_hash.digest == hashlib.sha256(_canonical(tree)).digest()
        assert _hash(["x", expr]) != _hash(["x", tree_hash])


class TestUuidColumnsAreHashedAsStored:
    """Rule 3: a UUID column has no column value handler."""

    def _table(self) -> pa.Table:
        source = DictSource(
            data=cast(
                "Any", [{"id": "a", "u": VALUE}, {"id": "b", "u": uuid.UUID(int=0)}]
            ),
            tag_columns=["id"],
            source_id="uuid_column",
            tag_only_columns=True,
        )
        return source.as_table()

    def test_uuid_column_hash_does_not_depend_on_the_structure_label(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No structure hash enters a UUID column's table hash."""
        context = get_default_context()
        table = self._table()
        before = context.arrow_hasher.hash_table(table)
        # Non-public: rename the structure label; a UUID cell routed through the value
        # handler would carry it into the table hash.
        monkeypatch.setattr(context.semantic_hasher, "_hasher_id", "renamed_v9")
        assert context.arrow_hasher.hash_table(table).digest == before.digest

    def test_uuid_column_hashes_its_stored_bytes(self) -> None:
        """Two UUID columns hash equal exactly when their stored bytes are equal."""
        context = get_default_context()
        table = self._table()
        other = self._table()
        assert context.arrow_hasher.hash_table(
            table
        ) == context.arrow_hasher.hash_table(other)
        changed = DictSource(
            data=cast(
                "Any", [{"id": "a", "u": VALUE}, {"id": "b", "u": uuid.UUID(int=1)}]
            ),
            tag_columns=["id"],
            source_id="uuid_column",
            tag_only_columns=True,
        ).as_table()
        assert context.arrow_hasher.hash_table(
            changed
        ) != context.arrow_hasher.hash_table(table)
