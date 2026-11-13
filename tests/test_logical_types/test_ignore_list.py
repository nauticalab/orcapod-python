"""The shared ignore-list mechanism of library value types (ITL-981).

Uses a toy type (``ignore_list_toy``) registered through a logical type
registration file, the way a data context configures a real type.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod.contexts as contexts_package
from orcapod.contexts.core import ContextValidationError, DataContext
from orcapod.contexts.registry import JSONDataContextRegistry
from orcapod.errors import TypeIdentityError
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.logical_types.identifier import LogicalTypeIdentifierError
from orcapod.logical_types.ignore_list import (
    checked_ignore,
    ignore_metadata,
    parse_ignore_metadata,
)
from orcapod.semantic_types.universal_converter import UniversalTypeConverter
from tests.test_logical_types.ignore_list_toy import (
    EXTENSION_NAME,
    Labelled,
    LogicalLabelled,
)

TOY_MODULE = "tests.test_logical_types.ignore_list_toy"
STRICT = b'{"ignore":[],"version":1}'
LENIENT = b'{"ignore":["label"],"version":1}'


def _context(tmp_path: Path, ignore: Any) -> DataContext:
    """Build a data context that registers the toy type with ``ignore``."""
    shipped = Path(contexts_package.__file__).parent / "logical_types.json"
    registration = json.loads(shipped.read_text())
    registration.setdefault("hash_ignore", {})[EXTENSION_NAME] = ignore
    reference = {"_ref": f"hash_ignore:{EXTENSION_NAME}"}
    registration["logical_type_registry"]["_config"]["logical_types"].append(
        {"_class": f"{TOY_MODULE}.LogicalLabelled", "_config": {"ignore": reference}}
    )
    registration["value_handlers"].append(
        [
            {"_type": f"{TOY_MODULE}.Labelled"},
            {
                "_class": f"{TOY_MODULE}.LabelledHandler",
                "_config": {"ignore": reference},
            },
        ]
    )
    name = "_".join(str(item) for item in ignore) if isinstance(ignore, list) else ""
    custom = tmp_path / f"logical_types_{name or 'other'}.json"
    custom.write_text(json.dumps(registration))
    return JSONDataContextRegistry(logical_types_file=custom).get_context()


def _table(context: DataContext, value: Labelled) -> pa.Table:
    return context.type_converter.python_dicts_to_arrow_table(
        [{"x": value}], python_schema={"x": Labelled}
    )


def _converter(context: DataContext) -> UniversalTypeConverter:
    """Return the context's converter as the concrete class the databases use."""
    converter = context.type_converter
    assert isinstance(converter, UniversalTypeConverter)
    return converter


# --- metadata bytes ------------------------------------------------------------------


def test_metadata_bytes_are_canonical_json_with_sorted_keys() -> None:
    """The metadata is exactly the documented bytes."""
    assert ignore_metadata(1, []) == b'{"ignore":[],"version":1}'
    assert (
        ignore_metadata(1, ["memory_order", "byte_order", "memory_order"])
        == b'{"ignore":["byte_order","memory_order"],"version":1}'
    )
    assert ignore_metadata(2, ("a_1",)) == b'{"ignore":["a_1"],"version":2}'


@pytest.mark.parametrize("name", ["Memory", "1st", "a-b", "", "é", "a b"])
def test_a_property_name_is_lower_case_snake_case(name: str) -> None:
    """A name outside ``[a-z][a-z0-9_]*`` cannot be written into metadata."""
    with pytest.raises(ValueError, match="snake case"):
        ignore_metadata(1, [name])


@pytest.mark.parametrize("version", [0, -1, True, 1.0, "1"])
def test_version_is_a_positive_integer(version: Any) -> None:
    """The version is a positive ``int``."""
    with pytest.raises(ValueError, match="positive integer"):
        ignore_metadata(version, [])


def test_parse_returns_what_was_written() -> None:
    """Parsing is the inverse of writing."""
    assert parse_ignore_metadata(STRICT) == (1, ())
    assert parse_ignore_metadata(ignore_metadata(3, ["b", "a"])) == (3, ("a", "b"))


@pytest.mark.parametrize(
    "metadata",
    [
        b"",
        b'{"version":1}',
        b'{"version":1,"ignore":[]}',
        b'{"ignore": [], "version": 1}',
        b'{"ignore":["b","a"],"version":1}',
        b'{"ignore":["a","a"],"version":1}',
        b'{"ignore":[],"version":1,"extra":0}',
        b'{"ignore":"a","version":1}',
        b'{"ignore":[1],"version":1}',
        b'{"ignore":[],"version":"1"}',
        b'{"ignore":[],"version":0}',
        b"not json",
        b'{"ignore":' + b"[" * 100000 + b"]" * 100000 + b',"version":1}',
        b"\xff\xfe",
        b"[]",
    ],
)
def test_parse_refuses_anything_but_the_canonical_bytes(metadata: bytes) -> None:
    """Only the exact bytes ``ignore_metadata`` writes are accepted."""
    with pytest.raises(ValueError, match="ignore-list metadata|canonical form"):
        parse_ignore_metadata(metadata)


def test_checked_ignore_sorts_and_refuses_unknown_names() -> None:
    """An ignore list is sorted and unique; an unknown name is an error naming it."""
    options = frozenset({"a", "b", "c"})
    assert checked_ignore(["c", "a", "c"], options) == ("a", "c")
    assert checked_ignore((), options) == ()
    with pytest.raises(ValueError, match="'d'.*allowed names: a, b, c"):
        checked_ignore(["a", "d"], options)
    with pytest.raises(ValueError, match="list of property names"):
        checked_ignore("a", options)


# --- the list reaches the type, the bare value and the cell ----------------------------


def test_the_configured_list_is_the_types_extension_metadata(tmp_path: Path) -> None:
    """The data context's list is written into the Arrow extension metadata."""
    strict = _context(tmp_path, [])
    lenient = _context(tmp_path, ["label"])
    strict_type = strict.type_converter.python_type_to_arrow_type(Labelled)
    lenient_type = lenient.type_converter.python_type_to_arrow_type(Labelled)
    assert isinstance(strict_type, pa.ExtensionType)
    assert isinstance(lenient_type, pa.ExtensionType)
    assert strict_type.extension_name == lenient_type.extension_name == EXTENSION_NAME
    assert strict_type.__arrow_ext_serialize__() == STRICT
    assert lenient_type.__arrow_ext_serialize__() == LENIENT


def test_two_lists_are_two_types(tmp_path: Path) -> None:
    """Types of two lists are unequal, alone, nested, in a schema and when hashed."""
    strict = _context(tmp_path, [])
    lenient = _context(tmp_path, ["label"])
    strict_type = strict.type_converter.python_type_to_arrow_type(Labelled)
    lenient_type = lenient.type_converter.python_type_to_arrow_type(Labelled)
    assert strict_type != lenient_type
    assert strict_type == LogicalLabelled().get_arrow_extension_type()
    assert pa.large_list(strict_type) != pa.large_list(lenient_type)
    assert pa.schema([("x", strict_type)]) != pa.schema([("x", lenient_type)])
    with pytest.raises(pa.ArrowInvalid):
        pa.concat_tables(
            [_table(strict, Labelled(1, "a")), _table(lenient, Labelled(1, "a"))]
        )
    # The type hash and the schema hash take the identifier, which holds the list.
    strict_hasher, lenient_hasher = strict.arrow_hasher, lenient.arrow_hasher
    assert isinstance(strict_hasher, StarfixArrowHasher)
    assert isinstance(lenient_hasher, StarfixArrowHasher)
    assert strict_hasher.hash_schema(
        pa.schema([("x", strict_type)])
    ) != lenient_hasher.hash_schema(pa.schema([("x", lenient_type)]))
    assert strict.semantic_hasher.hash_object(
        Labelled
    ) != lenient.semantic_hasher.hash_object(Labelled)
    assert strict.arrow_hasher.hash_table(
        _table(strict, Labelled(1, "a")).slice(0, 0)
    ) != lenient.arrow_hasher.hash_table(_table(lenient, Labelled(1, "a")).slice(0, 0))


def test_a_bare_value_is_hashed_with_the_contexts_list(tmp_path: Path) -> None:
    """Outside a typed column the data context's list decides."""
    strict = _context(tmp_path, []).semantic_hasher
    lenient = _context(tmp_path, ["label"]).semantic_hasher
    one, other = Labelled(1, "a"), Labelled(1, "b")
    assert strict.hash_object(one) != strict.hash_object(other)
    assert lenient.hash_object(one) == lenient.hash_object(other)
    assert lenient.hash_object(one) != lenient.hash_object(Labelled(2, "a"))
    assert lenient.hash_object(one).method == "labelled_v1"
    # The list is not a hash input: only the canonical field value is.
    assert lenient.hash_object(one) == strict.hash_object(Labelled(1, ""))


def test_a_cell_is_hashed_with_the_list_of_its_column(tmp_path: Path) -> None:
    """In a table the column's extension metadata decides, not the hashing context."""
    strict = _context(tmp_path, [])
    lenient = _context(tmp_path, ["label"])
    one, other = Labelled(1, "a"), Labelled(1, "b")
    assert strict.arrow_hasher.hash_table(
        _table(strict, one)
    ) != strict.arrow_hasher.hash_table(_table(strict, other))
    assert lenient.arrow_hasher.hash_table(
        _table(lenient, one)
    ) == lenient.arrow_hasher.hash_table(_table(lenient, other))
    # The same values in a strict column and in a lenient column hash differently:
    # the cell token and the field carry the identifier, which holds the list.
    assert strict.arrow_hasher.hash_table(
        _table(strict, one)
    ) != lenient.arrow_hasher.hash_table(_table(lenient, one))


def test_an_unknown_option_name_fails_the_context_build(tmp_path: Path) -> None:
    """A context that lists a name the type does not have cannot be built."""
    with pytest.raises(ContextValidationError, match="'colour'.*allowed names: label"):
        _context(tmp_path, ["colour"])


@pytest.mark.parametrize("value", [{"label": False}, "label", [1], None])
def test_hash_ignore_must_be_a_list_of_names(tmp_path: Path, value: Any) -> None:
    """Only a JSON list of names is an ignore list; an object or a string is refused."""
    with pytest.raises(ContextValidationError, match="list of property names"):
        _context(tmp_path, value)


# --- Arrow and stored columns ----------------------------------------------------------


def _ipc_round_trip(table: pa.Table) -> pa.Table:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return pa.ipc.open_stream(sink.getvalue()).read_all()


def test_arrow_ipc_keeps_the_list_of_each_column(tmp_path: Path) -> None:
    """One class is registered with Arrow under the name; it restores any valid list."""
    strict = _context(tmp_path, [])
    lenient = _context(tmp_path, ["label"])
    for context in (strict, lenient):
        table = _table(context, Labelled(1, "a"))
        assert _ipc_round_trip(table).schema == table.schema


def test_arrow_refuses_metadata_that_is_not_a_valid_list() -> None:
    """Unknown option names, other versions and other bytes raise on deserialisation."""
    extension_class = type(LogicalLabelled().get_arrow_extension_type())
    for metadata in (
        b'{"ignore":["colour"],"version":1}',
        b'{"ignore":[],"version":2}',
        b'{"version":1}',
        b"",
    ):
        with pytest.raises(LogicalTypeIdentifierError) as raised:
            extension_class.__arrow_ext_deserialize__(pa.large_string(), metadata)
        assert raised.value.stored.metadata == metadata
    with pytest.raises(TypeIdentityError, match="storage_type"):
        extension_class.__arrow_ext_deserialize__(pa.int64(), STRICT)
    # A plain string column (Delta compaction) is the same storage family.
    assert (
        extension_class.__arrow_ext_deserialize__(pa.string(), LENIENT)
        == LogicalLabelled(["label"]).get_arrow_extension_type()
    )


def test_a_column_stored_under_another_list_is_refused_naming_both(
    tmp_path: Path,
) -> None:
    """A data context reads only columns stored under its own list."""
    strict = _context(tmp_path, [])
    lenient = _context(tmp_path, ["label"])
    stored = _table(lenient, Labelled(1, "a"))
    with pytest.raises(LogicalTypeIdentifierError) as raised:
        _converter(strict).apply_logical_types(stored)
    message = str(raised.value)
    assert "'x'" in message
    assert repr(LENIENT) in message and repr(STRICT) in message
    # The same column as plain storage with the identifier in the field metadata,
    # which is how a database returns it.
    chunk = stored.column("x").chunk(0)
    assert isinstance(chunk, pa.ExtensionArray)
    plain = pa.table(
        {"x": chunk.storage},
        schema=pa.schema(
            [
                pa.field(
                    "x",
                    pa.large_string(),
                    metadata={
                        "ARROW:extension:name": EXTENSION_NAME,
                        "ARROW:extension:metadata": LENIENT,
                    },
                )
            ]
        ),
    )
    with pytest.raises(LogicalTypeIdentifierError, match="'x'"):
        _converter(strict).apply_logical_types(plain)
    assert (
        _converter(lenient).apply_logical_types(plain)["x"].type
        == stored.schema.field("x").type
    )


def test_concurrent_first_use_gives_one_type_per_list() -> None:
    """Threads that first ask for a list at the same time all get the same Arrow type."""
    import sys
    import threading

    from orcapod.logical_types.ignore_list import IgnoreListLogicalType

    interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for trial in range(30):

            class Racy(IgnoreListLogicalType):
                EXTENSION_NAME = f"orcapod.test.racy{trial}"
                STORAGE_TYPE = pa.large_string()
                VERSION = 1
                OPTIONS = frozenset({"a"})

            barrier = threading.Barrier(8)
            seen: list[pa.ExtensionType] = []

            def ask(logical_type: type[IgnoreListLogicalType] = Racy) -> None:
                barrier.wait()
                seen.append(logical_type(["a"]).get_arrow_extension_type())

            threads = [threading.Thread(target=ask) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            assert len({id(t) for t in seen}) == 1
    finally:
        sys.setswitchinterval(interval)
