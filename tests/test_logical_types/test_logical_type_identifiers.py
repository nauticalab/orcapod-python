"""Logical type identifiers (ITL-935).

A logical type's identifier is its Arrow extension name plus its extension
metadata. These tests cover: every built-in type writes one; the whole
identifier is part of the schema hash, of function identity and of the hashed
cell of a value-hashed type; a stored identifier this installation cannot read
is an explicit error; and data written before identifiers existed (empty
metadata) is read as version 1 of the built-in types.
"""

from __future__ import annotations

import json
import pathlib
import typing
import uuid
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from upath import UPath

import orcapod as op
from orcapod import function_pod
from orcapod.contexts import create_registry, get_default_context
from orcapod.contexts.core import DataContext
from orcapod.core.sources import DictSource
from orcapod.databases import DeltaTableDatabase
from orcapod.databases.extension_aware_database import ExtensionAwareDatabase
from orcapod.logical_types.identifier import LogicalTypeIdentifierError
from orcapod.hashing.semantic_hashing.builtin_handlers import GenericAliasHandler
from orcapod.hashing.semantic_hashing.encoded import Encoded
from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.identifier import (
    LogicalTypeIdentifier,
    logical_type_identifier,
)
from orcapod.pipeline import PipelineJob
from orcapod.semantic_types.universal_converter import UniversalTypeConverter
from orcapod.logical_types.protocols import LogicalTypeProtocol
from orcapod.logical_types.registry import (
    LogicalTypeRegistry,
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.types import Schema

VERSION_1 = b'{"version":1}'

# The built-in types that wrote empty extension metadata before identifiers existed.
BUILTINS_WITH_EMPTY_LEGACY: list[tuple[Any, str]] = [
    (pathlib.Path, "orcapod.path"),
    (UPath, "orcapod.upath"),
    (uuid.UUID, "orcapod.uuid"),
    (op.File, "orcapod.file"),
    (op.Directory, "orcapod.directory"),
]
ALL_BUILTINS = [*BUILTINS_WITH_EMPTY_LEGACY, (np.ndarray, "orcapod.numpy.ndarray")]

# A library value type writes its ignore list beside the version (ITL-981).
BUILTIN_METADATA = {"orcapod.numpy.ndarray": b'{"ignore":[],"version":1}'}

# Library value types whose identifier carries an ignore list (ITL-981). pandas values
# were stored under ``pandas.dataframe`` / ``pandas.series`` before v0.3; the current
# names never carried empty metadata (ITL-897).
BUILTINS_WITH_IGNORE_LIST: list[tuple[Any, str]] = [
    (pd.DataFrame, "orcapod.pandas.dataframe"),
    (pd.Series, "orcapod.pandas.series"),
]
VERSION_1_NOTHING_IGNORED = b'{"ignore":[],"version":1}'


def _converter(context: DataContext | None = None) -> UniversalTypeConverter:
    """The type converter of ``context`` (default: the default data context)."""
    converter = (context or get_default_context()).type_converter
    assert isinstance(converter, UniversalTypeConverter)
    return converter


def _extension_type(
    python_type: Any, context: DataContext | None = None
) -> pa.ExtensionType:
    """The Arrow extension type ``python_type`` is stored as."""
    arrow_type = _converter(context).python_type_to_arrow_type(python_type)
    assert isinstance(arrow_type, pa.ExtensionType)
    return arrow_type


class Thing:
    """A third-party value type stored as a string."""

    def __init__(self, text: str) -> None:
        """Store ``text`` as the value."""
        self.text = text


def _thing_logical_type(
    metadata: bytes, also_reads: tuple[bytes, ...] = ()
) -> LogicalTypeProtocol:
    """Build a third-party logical type ``thirdparty.thing`` with the given metadata."""
    arrow_cls: Any = make_arrow_extension_type(
        "thirdparty.thing", pa.large_string(), metadata, also_reads=also_reads
    )
    polars_cls: Any = make_polars_extension_type(
        "thirdparty.thing", pa.large_string(), metadata.decode() or None
    )

    class LogicalThing(BaseLogicalType):
        """Logical type for ``Thing`` built from the enclosing function's arguments."""

        logical_type_name = "thirdparty.thing"
        python_type = Thing
        _arrow: pa.ExtensionType = arrow_cls()
        _polars: pl.BaseExtension = polars_cls()

        def get_arrow_extension_type(self) -> pa.ExtensionType:
            """Return the Arrow extension type instance made for this logical type."""
            return self._arrow

        def get_polars_extension_type(self) -> pl.BaseExtension:
            """Return the Polars extension type instance made for this logical type."""
            return self._polars

        def python_to_storage(self, value: Any, converter: Any = None) -> str:
            """Return the text of ``value`` as its storage value."""
            return value.text

        def storage_to_python(self, storage_value: Any, converter: Any = None) -> Thing:
            """Return a ``Thing`` holding ``storage_value``."""
            return Thing(storage_value)

    return LogicalThing()


def _fresh_context(*logical_types: LogicalTypeProtocol) -> DataContext:
    """Return a new default data context with extra logical types registered."""
    context = create_registry().get_context()
    for logical_type in logical_types:
        _converter(context).register_logical_type(logical_type)
    return context


def _stored_column(
    name: bytes, metadata: bytes | None, storage: "pa.Array[Any]"
) -> pa.Table:
    """A table as a reader sees it: storage column ``c`` with extension field metadata."""
    field_metadata = {b"ARROW:extension:name": name}
    if metadata is not None:
        field_metadata[b"ARROW:extension:metadata"] = metadata
    return pa.table(
        [storage],
        schema=pa.schema([pa.field("c", storage.type, metadata=field_metadata)]),
    )


# --- every built-in type writes an identifier --------------------------------


@pytest.mark.parametrize(("python_type", "name"), ALL_BUILTINS)
def test_builtin_type_writes_version_1(python_type: Any, name: str) -> None:
    """Each built-in extension type has its expected name and writes version 1."""
    arrow_type = _extension_type(python_type)
    assert arrow_type.extension_name == name
    assert arrow_type.__arrow_ext_serialize__() == BUILTIN_METADATA.get(name, VERSION_1)


@pytest.mark.parametrize(("python_type", "name"), ALL_BUILTINS)
def test_builtin_identifier_is_declared_by_the_extension_type(
    python_type: Any, name: str
) -> None:
    """``logical_type_identifier`` of a built-in type is its extension name with
    its version-1 metadata.
    """
    logical_type = _converter().get_logical_type(python_type)
    assert logical_type is not None
    assert logical_type_identifier(logical_type) == LogicalTypeIdentifier(
        name, BUILTIN_METADATA.get(name, VERSION_1)
    )


@pytest.mark.parametrize(("python_type", "name"), BUILTINS_WITH_IGNORE_LIST)
def test_builtin_type_with_ignore_list_writes_version_1_and_its_list(
    python_type: Any, name: str
) -> None:
    """A library value type writes its version and its (default: empty) ignore list."""
    arrow_type = _extension_type(python_type)
    assert arrow_type.extension_name == name
    assert arrow_type.__arrow_ext_serialize__() == VERSION_1_NOTHING_IGNORED
    logical_type = _converter().get_logical_type(python_type)
    assert logical_type is not None
    assert logical_type_identifier(logical_type) == LogicalTypeIdentifier(
        name, VERSION_1_NOTHING_IGNORED
    )


def test_identifier_text_is_name_and_hex_of_metadata() -> None:
    """The identifier text is the name, a semicolon, and the metadata in hex (empty for
    none).
    """
    assert LogicalTypeIdentifier("orcapod.path", VERSION_1).text == (
        "orcapod.path;" + VERSION_1.hex()
    )
    assert LogicalTypeIdentifier("x.y", b"").text == "x.y;"


def test_extension_name_with_a_semicolon_is_refused() -> None:
    """Registering a logical type whose name contains ``;`` raises ``ValueError``."""
    arrow_cls: Any = make_arrow_extension_type("bad;name", pa.large_string(), b"")
    polars_cls: Any = make_polars_extension_type("bad;name", pa.large_string())

    class Bad(BaseLogicalType):
        """Logical type whose name contains a semicolon."""

        logical_type_name = "bad;name"
        python_type = Thing

        def get_arrow_extension_type(self) -> pa.ExtensionType:
            """Return a new instance of the Arrow type named with a semicolon."""
            return arrow_cls()

        def get_polars_extension_type(self) -> pl.BaseExtension:
            """Return a new instance of the Polars type named with a semicolon."""
            return polars_cls()

        def python_to_storage(self, value: Any, converter: Any = None) -> str:
            """Return the text of ``value`` as its storage value."""
            return value.text

        def storage_to_python(self, storage_value: Any, converter: Any = None) -> Thing:
            """Return a ``Thing`` holding ``storage_value``."""
            return Thing(storage_value)

    with pytest.raises(ValueError, match="';'"):
        LogicalTypeRegistry().register_logical_type(Bad())


# --- the identifier survives storage -----------------------------------------


def test_identifier_round_trips_through_delta(tmp_path: pathlib.Path) -> None:
    """A path column keeps its extension name and metadata in the Parquet files and when
    read back.
    """
    converter = _converter()
    path_type = _extension_type(pathlib.Path)
    table = pa.table(
        {
            "p": pa.ExtensionArray.from_storage(
                path_type, pa.array(["/a/b"], pa.large_string())
            )
        }
    )
    database = ExtensionAwareDatabase(DeltaTableDatabase(tmp_path), converter)
    database.add_record(("t",), b"r1", table, flush=True)

    stored = [pq.read_schema(f).field("p") for f in sorted(tmp_path.rglob("*.parquet"))]
    assert stored
    assert all(f.type.extension_name == "orcapod.path" for f in stored)
    assert all(f.type.__arrow_ext_serialize__() == VERSION_1 for f in stored)

    read = database.get_all_records(("t",))
    assert read is not None
    assert read.schema.field("p").type == path_type
    assert read.column("p").to_pylist() == ["/a/b"]


# --- the whole identifier is hashed ------------------------------------------


def test_metadata_alone_changes_the_schema_hash() -> None:
    """Two contexts differing only in a type's metadata hash the type and its schema
    differently.
    """
    v1 = _fresh_context(_thing_logical_type(b'{"version":1}'))
    v2 = _fresh_context(_thing_logical_type(b'{"version":2}'))
    schema = Schema({"c": Thing})
    assert v1.semantic_hasher.hash_object(schema) != v2.semantic_hasher.hash_object(
        schema
    )
    assert v1.semantic_hasher.hash_object(Thing) != v2.semantic_hasher.hash_object(
        Thing
    )


def test_metadata_alone_changes_a_generic_alias_hash() -> None:
    """A schema with ``list[Thing]`` hashes differently when only the metadata of
    ``Thing`` differs.
    """
    v1 = _fresh_context(_thing_logical_type(b'{"version":1}'))
    v2 = _fresh_context(_thing_logical_type(b'{"version":2}'))
    schema = Schema({"c": list[Thing]})
    assert v1.semantic_hasher.hash_object(schema) != v2.semantic_hasher.hash_object(
        schema
    )


def test_metadata_alone_changes_a_function_signature_hash() -> None:
    """A function annotated with ``Thing`` hashes differently when only the type's
    metadata differs.
    """
    v1 = _fresh_context(_thing_logical_type(b'{"version":1}'))
    v2 = _fresh_context(_thing_logical_type(b'{"version":2}'))

    def takes_thing(thing: Thing) -> int:
        """Return the length of the text of ``thing``."""
        return len(thing.text)

    assert v1.semantic_hasher.hash_object(
        takes_thing
    ) != v2.semantic_hasher.hash_object(takes_thing)


def test_hashes_without_logical_types_do_not_depend_on_registered_types() -> None:
    """A schema and a function using only plain types hash the same with an extra type
    registered.
    """
    plain = get_default_context()
    extended = _fresh_context(_thing_logical_type(b'{"version":2}'))
    schema = Schema({"a": int, "b": str, "c": list[float]})

    def plain_function(a: int, b: str = "x") -> float:
        """Return ``a`` as a float."""
        return float(a)

    assert plain.semantic_hasher.hash_object(
        schema
    ) == extended.semantic_hasher.hash_object(schema)
    assert plain.semantic_hasher.hash_object(
        plain_function
    ) == extended.semantic_hasher.hash_object(plain_function)


class _ThingHandler:
    """Value hasher for ``Thing``: its text."""

    def handle(self, obj: Any, hasher: Any) -> Any:
        """Return the text of ``obj`` as its hashable value."""
        return obj.text


@pytest.mark.parametrize("with_value_hasher", [True, False])
def test_metadata_alone_changes_the_row_hash(with_value_hasher: bool) -> None:
    """The table hash of an extension column differs when only the type's metadata
    differs.
    """
    hashes = []
    for metadata in (b'{"version":1}', b'{"version":2}'):
        logical_type = _thing_logical_type(metadata)
        context = _fresh_context(logical_type)
        if with_value_hasher:
            context.semantic_hasher.type_handler_registry.register(
                Thing, _ThingHandler()
            )
        table = pa.table(
            {
                "c": pa.ExtensionArray.from_storage(
                    logical_type.get_arrow_extension_type(),
                    pa.array(["a", "b"], pa.large_string()),
                )
            }
        )
        hashes.append(context.arrow_hasher.hash_table(table))
    assert hashes[0] != hashes[1]


def test_stored_tables_of_a_logical_type_pod_carry_the_identifier(
    tmp_path: pathlib.Path,
) -> None:
    """A pod's stored result column carries the identifier, through a pipeline run.

    The directory a pod's tables are stored under is built from its function
    hash and its schema hash; the tests above show both follow the identifier.
    """

    @function_pod(output_keys="q")
    def shift(p: pathlib.Path) -> pathlib.Path:
        """Return ``p`` with the component ``z`` appended."""
        return p / "z"

    paths = DictSource(
        [{"id": 0, "p": pathlib.Path("/a")}],
        tag_columns=["id"],
        source_id="paths",
        tag_only_columns=True,
    )
    job = PipelineJob(name="job", database=DeltaTableDatabase(tmp_path))
    with job:
        shift.pod(paths, label="shift")
    job.run()

    stored = [
        schema.field("q")
        for schema in (pq.read_schema(f) for f in tmp_path.rglob("*.parquet"))
        if "q" in schema.names
    ]
    assert stored
    for field in stored:
        assert field.type.extension_name == "orcapod.path"
        assert field.type.__arrow_ext_serialize__() == VERSION_1


# --- reading: unknown identifiers and data without metadata ------------------


def test_unknown_metadata_in_field_metadata_names_column_and_identifiers() -> None:
    """Loading a column with unreadable metadata raises an error naming the column, the
    stored and the readable metadata.
    """
    converter = _converter()
    table = _stored_column(
        b"orcapod.path", b'{"version":9}', pa.array(["/a"], pa.large_string())
    )
    with pytest.raises(LogicalTypeIdentifierError) as excinfo:
        converter.load_logical_types(table)
    message = str(excinfo.value)
    assert "'c'" in message
    assert "orcapod.path" in message
    assert '{"version":9}' in message
    assert '{"version":1}' in message
    assert excinfo.value.stored == LogicalTypeIdentifier(
        "orcapod.path", b'{"version":9}'
    )


def test_unknown_metadata_in_a_parquet_file_is_an_explicit_error(
    tmp_path: pathlib.Path,
) -> None:
    """Reading a Parquet file with unreadable metadata raises
    ``LogicalTypeIdentifierError`` carrying both identifiers.
    """
    get_default_context()  # registers the built-in extension types with pyarrow
    file = tmp_path / "t.parquet"
    pq.write_table(
        _stored_column(
            b"orcapod.path", b'{"version":9}', pa.array(["/a"], pa.large_string())
        ),
        file,
    )
    with pytest.raises(LogicalTypeIdentifierError) as excinfo:
        pq.read_table(file)
    assert excinfo.value.stored == LogicalTypeIdentifier(
        "orcapod.path", b'{"version":9}'
    )
    assert VERSION_1 in excinfo.value.readable


_LEGACY_STORAGE = {
    "orcapod.path": pa.array(["/a"], pa.large_string()),
    "orcapod.upath": pa.array(["s3://b/k"], pa.large_string()),
    "orcapod.uuid": pa.array([uuid.UUID(int=7).bytes], pa.large_binary()),
    "orcapod.file": pa.array(["/a/f.txt"], pa.large_string()),
    "orcapod.directory": pa.array(["/a/d"], pa.large_string()),
}


@pytest.mark.parametrize(("python_type", "name"), BUILTINS_WITH_EMPTY_LEGACY)
@pytest.mark.parametrize("metadata", [b"", None])
def test_builtin_data_without_metadata_is_read_as_version_1(
    python_type: Any, name: str, metadata: bytes | None, tmp_path: pathlib.Path
) -> None:
    """A built-in column stored with empty or no metadata is read as the current type on
    both read channels.
    """
    converter = _converter()
    current = _extension_type(python_type)
    stored = _stored_column(name.encode(), metadata, _LEGACY_STORAGE[name])

    # Field-metadata channel (the type was not resolved by pyarrow at read time).
    assert converter.load_logical_types(stored).schema.field("c").type == current

    # pyarrow channel (the type is registered when the file is read).
    file = tmp_path / "legacy.parquet"
    pq.write_table(stored, file)
    read = pq.read_table(file)
    assert read.schema.field("c").type == current
    chunk = read.column("c").chunk(0)
    assert isinstance(chunk, pa.ExtensionArray)
    assert chunk.storage.to_pylist() == _LEGACY_STORAGE[name].to_pylist()


def test_array_without_metadata_is_refused() -> None:
    """The array type never wrote empty metadata, so it does not read it."""
    converter = _converter()
    array_type = _extension_type(np.ndarray)
    table = _stored_column(
        b"orcapod.numpy.ndarray", b"", pa.array([], array_type.storage_type)
    )
    with pytest.raises(LogicalTypeIdentifierError):
        converter.load_logical_types(table)


@pytest.mark.parametrize(
    "name",
    [
        "pandas.dataframe",
        "pandas.series",
        "orcapod.pandas.dataframe",
        "orcapod.pandas.series",
    ],
)
@pytest.mark.parametrize("metadata", [b"", None])
def test_pandas_without_metadata_is_refused(name: str, metadata: bytes | None) -> None:
    """A pandas column without metadata is refused under the old and the current names.

    Before v0.3 pandas values were stored as ``pandas.dataframe`` / ``pandas.series`` with
    empty metadata. Those names are no longer registered, and the current names never
    wrote empty metadata, so neither is read as the current type or as raw bytes.
    """
    table = _stored_column(name.encode(), metadata, pa.array([b"x"], pa.large_binary()))
    with pytest.raises(ValueError):
        _converter().load_logical_types(table)


@pytest.mark.parametrize("name", ["pandas.dataframe", "pandas.series"])
def test_pandas_stored_under_the_old_name_with_version_1_is_refused(name: str) -> None:
    """What an unreleased v0.3 build wrote (old name, ``{"version":1}``) is refused."""
    table = _stored_column(
        name.encode(), b'{"version":1}', pa.array([b"x"], pa.large_binary())
    )
    with pytest.raises(ValueError):
        _converter().load_logical_types(table)


def test_third_party_type_reads_only_what_it_declares() -> None:
    """Old metadata is refused unless the type declares it readable, and is then read as
    the current type.
    """
    strict = _converter(_fresh_context(_thing_logical_type(b'{"v":2}')))
    lenient = _converter(
        _fresh_context(_thing_logical_type(b'{"v":2}', also_reads=(b'{"v":1}',)))
    )
    old = _stored_column(
        b"thirdparty.thing", b'{"v":1}', pa.array(["a"], pa.large_string())
    )

    with pytest.raises(LogicalTypeIdentifierError):
        strict.load_logical_types(old)

    read = lenient.load_logical_types(old)
    assert read.schema.field("c").type.__arrow_ext_serialize__() == b'{"v":2}'


def _typed_thing_column(metadata: bytes) -> pa.Table:
    """A table whose column ``c`` already has the extension type ``thirdparty.thing``."""
    foreign_cls: Any = make_arrow_extension_type(
        "thirdparty.thing", pa.large_string(), metadata
    )
    storage = pa.array(["a"], pa.large_string())
    return pa.table({"c": pa.ExtensionArray.from_storage(foreign_cls(), storage)})


def test_extension_typed_column_with_an_unreadable_identifier_is_refused() -> None:
    """A column that arrives already extension-typed is checked like a stored one."""
    strict = _converter(_fresh_context(_thing_logical_type(b'{"v":2}')))
    table = _typed_thing_column(b'{"v":1}')

    with pytest.raises(LogicalTypeIdentifierError) as excinfo:
        strict.load_logical_types(table)
    assert "'c'" in str(excinfo.value)
    with pytest.raises(LogicalTypeIdentifierError):
        strict.register_storage_type(table.schema.field("c").type)


def test_extension_typed_column_with_a_readable_older_identifier_is_read() -> None:
    """An older identifier the type declares readable comes back as the current type."""
    lenient = _converter(
        _fresh_context(_thing_logical_type(b'{"v":2}', also_reads=(b'{"v":1}',)))
    )
    table = _typed_thing_column(b'{"v":1}')

    read = lenient.load_logical_types(table)
    assert read.schema.field("c").type.__arrow_ext_serialize__() == b'{"v":2}'
    assert read.column("c").to_pylist() == table.column("c").to_pylist()
    registered = lenient.register_storage_type(table.schema.field("c").type)
    assert isinstance(registered, pa.ExtensionType)
    assert registered.__arrow_ext_serialize__() == b'{"v":2}'


def test_extension_typed_struct_child_with_an_unreadable_identifier_is_refused() -> (
    None
):
    """The identifier check also reaches an extension-typed member of a struct column."""
    strict = _converter(_fresh_context(_thing_logical_type(b'{"v":2}')))
    member_type = _typed_thing_column(b'{"v":1}').schema.field("c").type
    assert isinstance(member_type, pa.ExtensionType)
    child = pa.ExtensionArray.from_storage(
        member_type, pa.array(["a"], pa.large_string())
    )
    struct = pa.StructArray.from_arrays([child], fields=[pa.field("t", child.type)])

    with pytest.raises(LogicalTypeIdentifierError):
        strict.load_logical_types(pa.table({"s": struct}))

    # A readable older identifier on the member comes back as the current type.
    lenient = _converter(
        _fresh_context(_thing_logical_type(b'{"v":2}', also_reads=(b'{"v":1}',)))
    )
    read = lenient.load_logical_types(pa.table({"s": struct}))
    member = read.schema.field("s").type.field("t").type
    assert member.__arrow_ext_serialize__() == b'{"v":2}'


def test_values_of_an_unreadable_identifier_are_not_decoded() -> None:
    """Converting values to Python checks the identifier, like loading the table does."""
    strict = _converter(_fresh_context(_thing_logical_type(b'{"v":2}')))
    table = _typed_thing_column(b'{"v":1}')

    with pytest.raises(LogicalTypeIdentifierError):
        strict.arrow_table_to_python_dicts(table)


def test_python_type_of_an_unreadable_identifier_is_refused() -> None:
    """Mapping an Arrow type to a Python type checks the identifier (ITL-975).

    The registered class is never named by extension name alone: a schema must not
    promise ``Thing`` for a column whose values will be refused.
    """
    current = _thing_logical_type(b'{"v":2}')
    strict = _converter(_fresh_context(current))
    table = _typed_thing_column(b'{"v":1}')
    column_type = table.schema.field("c").type
    # The current identifier maps to the class; having mapped it changes nothing below.
    assert strict.arrow_type_to_python_type(current.get_arrow_extension_type()) is Thing

    with pytest.raises(LogicalTypeIdentifierError):
        _ = strict.arrow_type_to_python_type(column_type)
    with pytest.raises(LogicalTypeIdentifierError) as excinfo:
        _ = strict.arrow_schema_to_python_schema(table.schema)
    assert "'c'" in str(excinfo.value)
    # A member of a struct column is checked like a top-level column.
    with pytest.raises(LogicalTypeIdentifierError):
        _ = strict.arrow_type_to_python_type(pa.struct([pa.field("t", column_type)]))
    with pytest.raises(LogicalTypeIdentifierError) as excinfo:
        _ = strict.arrow_schema_to_python_schema(
            pa.schema([pa.field("s", pa.struct([pa.field("t", column_type)]))])
        )
    assert "'s'" in str(excinfo.value)


class _ParameterizedThingType(pa.ExtensionType):
    """One extension class for ``thirdparty.thing`` whose metadata is a parameter.

    Arrow compares two instances of one extension class without their metadata, so
    they are equal whatever identifiers they carry.
    """

    def __init__(self, metadata: bytes) -> None:
        """Store ``metadata`` as the serialized extension metadata."""
        self._metadata = metadata
        super().__init__(pa.large_string(), "thirdparty.thing")

    def __arrow_ext_serialize__(self) -> bytes:
        """Return the metadata given at construction."""
        return self._metadata

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> _ParameterizedThingType:
        """Rebuild the type from its serialized metadata."""
        return cls(serialized)


@pytest.mark.parametrize(
    "container",
    [
        lambda member: pa.struct([pa.field("t", member)]),
        lambda member: pa.large_list(pa.struct([pa.field("t", member)])),
        lambda member: pa.map_(pa.large_string(), pa.struct([pa.field("t", member)])),
        lambda member: pa.dense_union(
            [pa.field("i", pa.int64()), pa.field("t", member)]
        ),
        lambda member: pa.sparse_union(
            [pa.field("i", pa.int64()), pa.field("t", member)]
        ),
    ],
    ids=["struct", "list_of_struct", "map_of_struct", "dense_union", "sparse_union"],
)
def test_python_type_of_an_unreadable_member_is_refused_after_a_readable_one(
    container: Callable[[pa.DataType], pa.DataType],
) -> None:
    """A mapping remembered for a readable identifier never answers for an unreadable
    one (ITL-975): two containers that differ only in a member's metadata are equal
    to Arrow, and the identifier is still checked for each.
    """
    strict = _converter(_fresh_context(_thing_logical_type(b'{"v":2}')))
    readable = container(_ParameterizedThingType(b'{"v":2}'))
    unreadable = container(_ParameterizedThingType(b'{"v":1}'))
    assert readable == unreadable  # what makes a lookup by Arrow type alone unsafe

    # The readable container is mapped first, through both entry points.
    _ = strict.arrow_type_to_python_type(readable)
    _ = strict.arrow_schema_to_python_schema(pa.schema([pa.field("c", readable)]))

    with pytest.raises(LogicalTypeIdentifierError):
        _ = strict.arrow_type_to_python_type(unreadable)
    with pytest.raises(LogicalTypeIdentifierError) as excinfo:
        _ = strict.arrow_schema_to_python_schema(pa.schema([pa.field("c", unreadable)]))
    assert "'c'" in str(excinfo.value)


def _parameterized_thing_column(metadata: bytes) -> "pa.Array[Any]":
    """One ``thirdparty.thing`` value typed by ``_ParameterizedThingType(metadata)``."""
    return pa.ExtensionArray.from_storage(
        _ParameterizedThingType(metadata), pa.array(["a"], pa.large_string())
    )


def _struct_column(member: "pa.Array[Any]") -> "pa.Array[Any]":
    """A struct column with ``member`` as its field ``t``."""
    return pa.StructArray.from_arrays([member], fields=[pa.field("t", member.type)])


def _list_of_struct_column(member: "pa.Array[Any]") -> "pa.Array[Any]":
    """A one-row list column whose element is a struct with ``member`` as field ``t``."""
    return pa.LargeListArray.from_arrays(
        pa.array([0, 1], pa.int64()), _struct_column(member)
    )


@pytest.mark.parametrize(
    "column",
    [_struct_column, _list_of_struct_column],
    ids=["struct", "list_of_struct"],
)
def test_values_of_an_unreadable_member_are_not_decoded_after_a_readable_one(
    column: Callable[["pa.Array[Any]"], "pa.Array[Any]"],
) -> None:
    """A value converter remembered for a readable identifier never decodes values
    stored under an unreadable one (ITL-975): the two columns' types are equal to Arrow.
    """
    strict = _converter(_fresh_context(_thing_logical_type(b'{"v":2}')))
    readable = pa.table({"c": column(_parameterized_thing_column(b'{"v":2}'))})
    unreadable = pa.table({"c": column(_parameterized_thing_column(b'{"v":1}'))})
    assert readable.schema.field("c").type == unreadable.schema.field("c").type

    # The readable table is converted first, so its converter is remembered.
    assert len(strict.arrow_table_to_python_dicts(readable)) == 1

    with pytest.raises(LogicalTypeIdentifierError):
        _ = strict.arrow_table_to_python_dicts(unreadable)
    with pytest.raises(LogicalTypeIdentifierError):
        _ = strict.get_arrow_to_python_converter(unreadable.schema.field("c").type)


@pytest.mark.parametrize("mode", ["dense", "sparse"])
def test_value_converter_of_an_unreadable_union_member_is_refused_after_a_readable_one(
    mode: str,
) -> None:
    """The same holds for a member of a union type (ITL-975)."""
    strict = _converter(_fresh_context(_thing_logical_type(b'{"v":2}')))

    def union(metadata: bytes) -> pa.DataType:
        """A union of ``int64`` and a ``thirdparty.thing`` carrying ``metadata``."""
        fields = [
            pa.field("i", pa.int64()),
            pa.field("t", _ParameterizedThingType(metadata)),
        ]
        return pa.dense_union(fields) if mode == "dense" else pa.sparse_union(fields)

    _ = strict.get_arrow_to_python_converter(union(b'{"v":2}'))

    with pytest.raises(LogicalTypeIdentifierError):
        _ = strict.get_arrow_to_python_converter(union(b'{"v":1}'))


def test_python_type_of_a_readable_older_identifier_is_the_current_class() -> None:
    """An older identifier the type declares readable maps to the registered class."""
    lenient = _converter(
        _fresh_context(_thing_logical_type(b'{"v":2}', also_reads=(b'{"v":1}',)))
    )
    table = _typed_thing_column(b'{"v":1}')

    assert lenient.arrow_type_to_python_type(table.schema.field("c").type) is Thing
    schema = lenient.arrow_schema_to_python_schema(table.schema)
    assert schema["c"] == Thing | None


def test_list_wrapper_written_before_identifiers_is_read() -> None:
    """A list column whose metadata records no element metadata is read as the current
    list type.
    """
    converter = _converter()
    current = _extension_type(list[pathlib.Path])
    legacy_metadata = json.dumps(
        {
            "category": "list",
            "element_ext_name": "orcapod.path",
            "element_ext_metadata": None,
        }
    ).encode()
    stored = _stored_column(
        b"list[orcapod.path]",
        legacy_metadata,
        pa.array([["/a", "/b"]], pa.large_list(pa.large_string())),
    )
    assert converter.load_logical_types(stored).schema.field("c").type == current


def test_list_wrapper_of_an_unreadable_element_is_refused() -> None:
    """A list column whose element metadata is unreadable raises
    ``LogicalTypeIdentifierError``.
    """
    converter = _converter()
    _extension_type(list[pathlib.Path])
    metadata = json.dumps(
        {
            "category": "list",
            "element_ext_name": "orcapod.path",
            "element_ext_metadata": '{"version":9}',
        }
    ).encode()
    stored = _stored_column(
        b"list[orcapod.path]",
        metadata,
        pa.array([["/a"]], pa.large_list(pa.large_string())),
    )
    with pytest.raises(LogicalTypeIdentifierError):
        converter.load_logical_types(stored)


# --- native containers: list[T] / set[T] of a logical type -------------------


def _file_value(tmp_path: pathlib.Path) -> Any:
    """Write a text file under ``tmp_path`` and return it as an ``op.File``."""
    target = tmp_path / "f.txt"
    target.write_text("content")
    return op.File(target)


def _directory_value(tmp_path: pathlib.Path) -> Any:
    """Create a directory holding one file under ``tmp_path`` and return it as an
    ``op.Directory``.
    """
    target = tmp_path / "d"
    target.mkdir(exist_ok=True)
    (target / "f.txt").write_text("content")
    return op.Directory(target)


_ELEMENT_VALUE: dict[Any, Any] = {
    pathlib.Path: lambda tmp_path: pathlib.Path("/a/b"),
    uuid.UUID: lambda tmp_path: uuid.UUID(int=7),
    op.File: _file_value,
    op.Directory: _directory_value,
}


@pytest.mark.parametrize("container", [list, set])
@pytest.mark.parametrize("element", [pathlib.Path, uuid.UUID, op.File, op.Directory])
def test_container_cells_with_different_null_elements_hash_differently(
    container: Any, element: Any, tmp_path: pathlib.Path
) -> None:
    """``[]``, ``[None]``, ``[None, None]`` and ``[x]`` are four different values."""
    context = get_default_context()
    container_type = _extension_type(container[element])
    element_type = _converter().get_logical_type(element)
    assert element_type is not None
    value = element_type.python_to_storage(
        _ELEMENT_VALUE[element](tmp_path), _converter()
    )
    cells: list[list[Any]] = [[], [None], [None, None], [value], [value, None]]

    hashes = []
    for cell in cells:
        storage = pa.array([cell], container_type.storage_type)
        table = pa.table({"c": pa.ExtensionArray.from_storage(container_type, storage)})
        hashes.append(context.arrow_hasher.hash_table(table))
    assert len(set(hashes)) == len(cells)


@pytest.mark.parametrize("annotation", [list[pathlib.Path], set[uuid.UUID]])
def test_native_container_identifier_has_no_version_of_its_own(annotation: Any) -> None:
    """A list/set wrapper is identified by its kind and its element's identifier only."""
    metadata = json.loads(_extension_type(annotation).__arrow_ext_serialize__())
    assert sorted(metadata) == ["category", "element_ext_metadata", "element_ext_name"]
    assert metadata["element_ext_metadata"] == VERSION_1.decode()


@pytest.mark.parametrize("annotation", [list[pathlib.Path], set[op.File]])
def test_native_container_hashes_like_any_generic_alias_in_the_schema_hash(
    annotation: Any,
) -> None:
    """In the type hash a native container is its origin and arguments, like ``list[int]``."""
    context = get_default_context()
    hasher = context.semantic_hasher
    handled = GenericAliasHandler().handle(annotation, hasher)
    # The handler returns the hasher's tagged form (ITL-942) holding the nested
    # hashes themselves, which enter the parent as digests without a label (ITL-945).
    assert handled == Encoded(
        "generic_alias",
        {
            "origin": hasher.hash_object(typing.get_origin(annotation)),
            "args": [hasher.hash_object(arg) for arg in typing.get_args(annotation)],
        },
    )
    # Exactly the form of a container of a plain type: no member for a version.
    plain = GenericAliasHandler().handle(list[int], hasher)
    assert isinstance(plain, Encoded)
    assert (handled.tag, sorted(handled.fields)) == (plain.tag, sorted(plain.fields))
