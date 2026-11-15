"""A ``set[T]`` of a primitive element type keeps its set identity (ITL-611).

``set[T]`` is stored as the native set container, an Arrow extension type named
``set[<element Arrow type>]``, whatever ``T`` is; ``list[T]`` of a primitive stays a plain
``large_list``. So a set reads back as a set everywhere: from the node, in a downstream
pod, from ``as_source()`` and from a job loaded in a fresh process; and a set row never
hashes like the list row with the same elements.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import DictSource

UTC = dt.timezone.utc

# element type -> (one set value, the large-normalized Arrow type text of the element)
PRIMITIVE_SETS: dict[str, tuple[Any, Any, str]] = {
    "int": (int, {3, 1, 2, 10}, "int64"),
    "str": (str, {"b", "a", "10", "2"}, "large_string"),
    "float": (float, {1.5, -2.5, 10.0}, "double"),
    "bool": (bool, {True, False}, "bool"),
    "bytes": (bytes, {b"y", b"x", b"\xff"}, "large_binary"),
    "datetime": (
        dt.datetime,
        {dt.datetime(2026, 1, 2, tzinfo=UTC), dt.datetime(2025, 1, 2, tzinfo=UTC)},
        "timestamp[us, tz=UTC]",
    ),
    "date": (dt.date, {dt.date(2026, 1, 2), dt.date(2025, 1, 2)}, "date32[day]"),
}


def _producer(declared: Any, value: Any) -> Any:
    """Return a function pod with one input ``i`` that returns ``value`` as ``out``."""

    def produce(i: int) -> Any:
        return value

    produce.__annotations__["return"] = declared
    return op.function_pod(output_keys="out")(produce)


def _consumer(declared: Any) -> Any:
    """Return a function pod that reports the Python type of its ``out`` input."""

    def consume(out: Any) -> str:
        if isinstance(out, list):
            return "list[" + ",".join(type(x).__name__ for x in out) + "]"
        return type(out).__name__

    consume.__annotations__["out"] = declared
    return op.function_pod(output_keys="got")(consume)


def _source() -> DictSource:
    return DictSource([{"k": 0, "i": 0}], tag_columns=["k"], source_id="one_row")


def _run_job(database: Any, declared: Any, value: Any, *, use_async: bool) -> Any:
    """Run a job ``produce -> consume`` and return it."""
    job = op.PipelineJob(name="sets", database=database)
    with job:
        produced = _producer(declared, value).pod(_source(), label="produce")
        _consumer(declared).pod(produced, label="consume")
    if use_async:
        job.run(orchestrator=AsyncPipelineOrchestrator())
    else:
        job.run()
    return job


def _out_values(stream: Any) -> list[Any]:
    return [data.as_dict()["out"] for _, data in stream.iter_data()]


def _database(kind: str, tmp_path: Path) -> Any:
    if kind == "delta":
        return DeltaTableDatabase(tmp_path / "store")
    return InMemoryArrowDatabase()


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("database_kind", ["memory", "delta"])
@pytest.mark.parametrize("element", list(PRIMITIVE_SETS))
class TestJobRoundTrip:
    """A set of a primitive reads back as a set from every reader in the process."""

    def test_node_downstream_pod_and_as_source_read_a_set(
        self, element: str, database_kind: str, use_async: bool, tmp_path: Path
    ) -> None:
        element_type, value, _ = PRIMITIVE_SETS[element]
        declared = set[element_type]
        job = _run_job(
            _database(database_kind, tmp_path), declared, value, use_async=use_async
        )
        node = job.nodes["produce"]
        assert _out_values(node) == [value]
        assert all(isinstance(v, set) for v in _out_values(node))

        got = job.nodes["consume"].as_table().column("got").to_pylist()
        assert got == ["set"]

        stored = node.as_source()
        assert stored.output_schema()[1]["out"] == declared
        assert _out_values(stored) == [value]
        assert all(isinstance(v, set) for v in _out_values(stored))


@pytest.mark.parametrize("element", list(PRIMITIVE_SETS))
def test_stored_arrow_type_is_the_set_container(element: str) -> None:
    """The stored column is ``extension<set[<element Arrow type>]>`` over ``large_list``."""
    element_type, value, element_text = PRIMITIVE_SETS[element]
    job = _run_job(InMemoryArrowDatabase(), set[element_type], value, use_async=False)
    arrow_type = job.nodes["produce"].as_table().schema.field("out").type
    assert isinstance(arrow_type, pa.ExtensionType)
    assert arrow_type.extension_name == f"set[{element_text}]"
    assert json.loads(arrow_type.__arrow_ext_serialize__()) == {
        "category": "set",
        "element_kind": "arrow",
    }
    assert pa.types.is_large_list(arrow_type.storage_type)


SET_METADATA = b'{"category": "set", "element_kind": "arrow"}'
"""The extension metadata of every set of a primitive: the element is the storage's."""


@pytest.mark.parametrize(
    ("declared", "value", "name"),
    [
        (set[int], {2, 1}, "set[int64]"),
        (set[str], {"x"}, "set[large_string]"),
        (set[bytes], {b"x"}, "set[large_binary]"),
        (
            set[dt.datetime],
            {dt.datetime(2026, 1, 2, tzinfo=UTC)},
            "set[timestamp[us, tz=UTC]]",
        ),
    ],
    ids=["int", "str", "bytes", "datetime"],
)
def test_identifier_bytes(declared: Any, value: Any, name: str) -> None:
    """The identifier of a set of a primitive: the name holds the element Arrow type,
    the metadata only says the element is a plain Arrow type; no version key.
    """
    source = DictSource(
        [{"k": 0, "s": value}],
        tag_columns=["k"],
        data_schema={"k": int, "s": declared},
    )
    arrow_type = source.as_table().schema.field("s").type
    assert arrow_type.extension_name == name
    assert arrow_type.__arrow_ext_serialize__() == SET_METADATA
    assert pa.types.is_large_list(arrow_type.storage_type)


def test_a_list_of_sets_embeds_the_inner_set_identifier() -> None:
    """``list[set[int]]`` names the inner set by its name and its new metadata."""
    source = DictSource(
        [{"k": 0, "s": [{2, 1}]}],
        tag_columns=["k"],
        data_schema={"k": int, "s": list[set[int]]},
    )
    arrow_type = source.as_table().schema.field("s").type
    assert arrow_type.extension_name == "list[set[int64]]"
    assert json.loads(arrow_type.__arrow_ext_serialize__()) == {
        "category": "list",
        "element_ext_name": "set[int64]",
        "element_ext_metadata": SET_METADATA.decode(),
    }


@pytest.mark.parametrize(
    ("declared", "value", "arrow_type"),
    [
        (list[int], [3, 1, 2], pa.large_list(pa.int64())),
        (list[str], ["b", "a"], pa.large_list(pa.large_string())),
        (list[bytes], [b"y", b"x"], pa.large_list(pa.large_binary())),
    ],
    ids=["int", "str", "bytes"],
)
def test_list_of_a_primitive_is_unchanged(
    declared: Any, value: Any, arrow_type: pa.DataType
) -> None:
    """``list[T]`` of a primitive stays a plain ``large_list`` and reads back as a list."""
    job = _run_job(InMemoryArrowDatabase(), declared, value, use_async=False)
    node = job.nodes["produce"]
    assert node.as_table().schema.field("out").type == arrow_type
    assert _out_values(node) == [value]
    assert _out_values(node.as_source()) == [value]
    assert job.nodes["consume"].as_table().column("got").to_pylist() == [
        "list[" + ",".join(type(x).__name__ for x in value) + "]"
    ]


def _row_hash(declared: Any, value: Any) -> Any:
    source = DictSource(
        [{"k": 0, "s": value}],
        tag_columns=["k"],
        data_schema={"k": int, "s": declared},
        source_id="row_hash",
    )
    _, data = next(iter(source.iter_data()))
    return data.content_hash()


def test_a_set_row_does_not_hash_like_the_list_row() -> None:
    """``{1, 2, 3}`` as ``set[int]`` and ``[1, 2, 3]`` as ``list[int]`` are two rows."""
    assert _row_hash(set[int], {3, 1, 2}) != _row_hash(list[int], [1, 2, 3])


def test_equal_sets_hash_equal() -> None:
    """The stored order is canonical, so equal sets give one row hash."""
    assert _row_hash(set[str], {"a", "b", "c"}) == _row_hash(set[str], {"c", "a", "b"})


@pytest.mark.parametrize(
    ("declared", "value", "expected"),
    [
        (list[set[int]], [{3, 1}, {2}], "list[set,set]"),
        (set[int] | None, {3, 1, 2}, "set"),
    ],
    ids=["list_of_sets", "optional_set"],
)
def test_nested_and_optional_sets_reach_a_downstream_pod_as_sets(
    declared: Any, value: Any, expected: str
) -> None:
    """A set inside a list, and an optional set, reach a downstream pod as sets."""
    job = _run_job(InMemoryArrowDatabase(), declared, value, use_async=False)
    assert job.nodes["consume"].as_table().column("got").to_pylist() == [expected]
    assert _out_values(job.nodes["produce"].as_source()) == [value]


def test_a_set_inside_a_dict_value_is_refused() -> None:
    """``dict[str, set[int]]`` is refused: a set there would read back as a list (ITL-1150)."""
    with pytest.raises(ValueError, match="ITL-1150"):
        _run_job(
            InMemoryArrowDatabase(), dict[str, set[int]], {"a": {2, 1}}, use_async=False
        )


def test_frozenset_stays_refused() -> None:
    """``frozenset[T]`` is not a supported annotation."""
    with pytest.raises(ValueError, match="frozenset"):
        _run_job(
            InMemoryArrowDatabase(), frozenset[int], frozenset({1}), use_async=False
        )


_FRESH_READ = textwrap.dedent(
    """
    import json, sys
    from orcapod import PipelineJob
    from orcapod.databases import DeltaTableDatabase

    job = PipelineJob.load(sys.argv[1], database=DeltaTableDatabase(sys.argv[2]))
    stored = job.nodes["produce"].as_source()
    _, data = next(iter(stored.iter_data()))
    value = data.as_dict()["out"]
    print(json.dumps({
        "schema": str(stored.output_schema()[1]["out"]),
        "python_type": type(value).__name__,
        "elements": sorted(repr(v) for v in value),
        "row_hash": data.content_hash().to_string(),
    }))
    """
)

FRESH_CASES: dict[str, tuple[Any, Any]] = {
    "int": (set[int], {3, 1, 2}),
    "str": (set[str], {"b", "a"}),
    "bytes": (set[bytes], {b"y", b"x"}),
    "path": (set[pathlib.Path], {pathlib.Path("/a/b"), pathlib.Path("/a/c")}),
    "list_of_sets": (list[set[int]], [{3, 1}, {2}]),
}


@pytest.mark.parametrize("case", list(FRESH_CASES))
def test_a_loaded_job_in_a_fresh_process_reads_a_set(case: str, tmp_path: Path) -> None:
    """A job loaded in another process restores the set type, values and row hash."""
    declared, value = FRESH_CASES[case]
    store = tmp_path / "store"
    job = _run_job(DeltaTableDatabase(store), declared, value, use_async=False)
    saved = tmp_path / "job.json"
    job.save(saved)
    _, data = next(iter(job.nodes["produce"].as_source().iter_data()))
    in_process_hash = data.content_hash().to_string()

    result = subprocess.run(
        [sys.executable, "-c", _FRESH_READ, str(saved), str(store)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    read = json.loads(result.stdout.strip().splitlines()[-1])
    assert read["schema"] == str(declared)
    assert read["python_type"] == type(value).__name__
    assert read["elements"] == sorted(repr(v) for v in value)
    assert read["row_hash"] == in_process_hash


_REGISTRATION_ORDER = textwrap.dedent(
    """
    import json, sys
    from typing import Literal
    from orcapod.sources import DictSource

    declared = {"int": set[int], "literal": set[Literal[1, 2]]}
    report = {}
    for name in sys.argv[1:]:
        source = DictSource(
            [{"k": 0, "v": {1}}],
            tag_columns=["k"],
            data_schema={"k": int, "v": declared[name]},
            source_id="one_row",
        )
        report[name] = {
            "schema": str(source.output_schema()[1]["v"]),
            "pipeline_hash": source.pipeline_hash().to_string(),
        }
    print(json.dumps(report))
    """
)


def _declare_in_fresh_process(*names: str) -> dict[str, dict[str, str]]:
    """Build one source per name, in order, in a fresh process; report each."""
    result = subprocess.run(
        [sys.executable, "-c", _REGISTRATION_ORDER, *names],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_a_set_type_does_not_depend_on_what_was_declared_first() -> None:
    """``set[int]`` reads as ``set[int]`` even after ``set[Literal[1, 2]]``.

    Both annotations map to the one container ``set[int64]``; its Python type is the
    element Arrow type's Python type, so neither the schema nor the pipeline hash of a
    source depends on what the process declared before it.
    """
    alone = _declare_in_fresh_process("int")["int"]
    literal_first = _declare_in_fresh_process("literal", "int")
    int_first = _declare_in_fresh_process("int", "literal")

    assert alone["schema"] == "set[int]"
    for report in (literal_first, int_first):
        assert report["int"] == alone
        assert report["literal"] == alone


_TWO_WIDTHS = textwrap.dedent(
    """
    import json, sys
    import numpy as np
    from orcapod.sources import DictSource

    declared = {"int8": (set[np.int8], {1, 2}), "int64": (set[int], {1, 300})}
    report = {}
    for name in sys.argv[1:]:
        annotation, value = declared[name]
        source = DictSource(
            [{"k": 0, "v": value}],
            tag_columns=["k"],
            data_schema={"k": int, "v": annotation},
            source_id="one_row",
        )
        _, data = next(iter(source.iter_data()))
        read = data.as_dict()["v"]
        report[name] = {
            "arrow_type": str(source.as_table().schema.field("v").type),
            "schema": str(source.output_schema()[1]["v"]),
            "read": sorted(int(v) for v in read),
            "read_type": type(read).__name__,
        }
    print(json.dumps(report))
    """
)


@pytest.mark.parametrize("order", [("int8", "int64"), ("int64", "int8")])
def test_two_integer_widths_of_a_set_live_in_one_process(
    order: tuple[str, str],
) -> None:
    """``set[np.int8]`` and ``set[int]`` are two containers in one process, in any order.

    Both read as ``set[int]``, as ``list[np.int8]`` and ``list[int]`` both read as
    ``list[int]``; each column keeps its own element width.
    """
    result = subprocess.run(
        [sys.executable, "-c", _TWO_WIDTHS, *order],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    report = json.loads(result.stdout.strip().splitlines()[-1])
    assert report["int8"] == {
        "arrow_type": "extension<set[int8]<_ArrowExt_set_int8_>>",
        "schema": "set[int]",
        "read": [1, 2],
        "read_type": "set",
    }
    assert report["int64"] == {
        "arrow_type": "extension<set[int64]<_ArrowExt_set_int64_>>",
        "schema": "set[int]",
        "read": [1, 300],
        "read_type": "set",
    }


_LOAD_THEN_DECLARE = textwrap.dedent(
    """
    import json, sys
    from orcapod import PipelineJob
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource

    job = PipelineJob.load(sys.argv[1], database=DeltaTableDatabase(sys.argv[2]))
    stored = job.nodes["produce"].as_source()
    _, data = next(iter(stored.iter_data()))
    loaded = data.as_dict()["out"]
    source = DictSource(
        [{"k": 0, "v": {1, 300}}],
        tag_columns=["k"],
        data_schema={"k": int, "v": set[int]},
        source_id="one_row",
    )
    _, data = next(iter(source.iter_data()))
    print(json.dumps({
        "loaded": sorted(int(v) for v in loaded),
        "declared_arrow_type": str(source.as_table().schema.field("v").type),
        "declared": sorted(data.as_dict()["v"]),
    }))
    """
)


def test_a_loaded_set_of_one_width_leaves_other_widths_declarable(
    tmp_path: Path,
) -> None:
    """A job loaded with a ``set[np.int8]`` node does not take over ``set[int]``."""
    import numpy as np

    store = tmp_path / "store"
    job = _run_job(
        DeltaTableDatabase(store),
        set[np.int8],
        {np.int8(1), np.int8(2)},
        use_async=False,
    )
    saved = tmp_path / "job.json"
    job.save(saved)

    result = subprocess.run(
        [sys.executable, "-c", _LOAD_THEN_DECLARE, str(saved), str(store)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    report = json.loads(result.stdout.strip().splitlines()[-1])
    assert report == {
        "loaded": [1, 2],
        "declared_arrow_type": "extension<set[int64]<_ArrowExt_set_int64_>>",
        "declared": [1, 300],
    }


_READ_STORED_COLUMN = textwrap.dedent(
    """
    import json, sys
    import pyarrow as pa
    from orcapod.contexts import get_default_context
    from orcapod.logical_types.identifier import LogicalTypeIdentifierError
    from orcapod.sources import ArrowTableSource

    name, metadata, storage = sys.argv[1], sys.argv[2].encode(), sys.argv[3]
    columns = {
        "large_list<int64>": pa.array([[1, 2, 3]], pa.large_list(pa.int64())),
        "large_list<int8>": pa.array([[1, 2]], pa.large_list(pa.int8())),
        "large_list<large_string>": pa.array(
            [["a", "b"]], pa.large_list(pa.large_string())
        ),
        "large_string": pa.array(["a"], pa.large_string()),
        "int64": pa.array([1], pa.int64()),
    }
    column = columns[storage]
    field_metadata = {
        b"ARROW:extension:name": name.encode(),
        b"ARROW:extension:metadata": metadata,
    }
    table = pa.table(
        [pa.array([0], pa.int64()), column],
        schema=pa.schema(
            [
                pa.field("k", pa.int64(), nullable=False),
                pa.field("c", column.type, nullable=False, metadata=field_metadata),
            ]
        ),
    )
    try:
        table = get_default_context().type_converter.load_logical_types(table)
    except LogicalTypeIdentifierError as exc:
        print(json.dumps({
            "refused": type(exc).__name__,
            "message": str(exc),
            "notes": getattr(exc, "__notes__", []),
        }))
        sys.exit(0)
    source = ArrowTableSource(table, tag_columns=["k"], source_id="stored")
    _, data = next(iter(source.iter_data()))
    value = data.as_dict()["c"]
    print(json.dumps({
        "arrow_type": str(table.schema.field("c").type),
        "metadata": table.schema.field("c").type.__arrow_ext_serialize__().decode(),
        "schema": str(source.output_schema()[1]["c"]),
        "python_type": type(value).__name__,
        "elements": sorted(value),
    }))
    """
)


def _read_stored_column_in_fresh_process(
    name: str, metadata: bytes, storage: str
) -> dict[str, Any]:
    """Read a stored column (field-metadata channel) in a fresh process; report it."""
    result = subprocess.run(
        [sys.executable, "-c", _READ_STORED_COLUMN, name, metadata.decode(), storage],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize(
    ("name", "storage", "schema", "elements"),
    [
        ("set[int64]", "large_list<int64>", "set[int]", [1, 2, 3]),
        ("set[large_string]", "large_list<large_string>", "set[str]", ["a", "b"]),
    ],
    ids=["int", "str"],
)
def test_a_fresh_process_rebuilds_a_set_from_its_storage(
    name: str, storage: str, schema: str, elements: list[Any]
) -> None:
    """A stored set column is rebuilt from its name, its metadata and its storage alone."""
    report = _read_stored_column_in_fresh_process(name, SET_METADATA, storage)
    assert report["metadata"] == SET_METADATA.decode()
    assert report["arrow_type"].startswith(f"extension<{name}<")
    assert report["schema"] == schema
    assert report["python_type"] == "set"
    assert report["elements"] == elements


@pytest.mark.parametrize(
    ("name", "storage", "reason"),
    [
        ("set[int64]", "large_string", "is not a list"),
        ("set[int64]", "int64", "is not a list"),
        ("set[int64]", "large_list<int8>", "holds the elements of 'set[int8]'"),
    ],
    ids=["string_storage", "int_storage", "other_element_width"],
)
def test_a_set_whose_storage_does_not_hold_its_element_is_refused(
    name: str, storage: str, reason: str
) -> None:
    """``element_kind: arrow`` over a storage that is not a list of the named element is
    refused: the element comes from the storage, so the two must agree.
    """
    report = _read_stored_column_in_fresh_process(name, SET_METADATA, storage)
    assert report.get("refused") == "LogicalTypeIdentifierError", report
    assert "'c'" in report["message"]
    assert any(reason in note for note in report["notes"]), report


def test_a_set_with_the_unreleased_element_arrow_type_metadata_is_refused() -> None:
    """Only ``{"category": "set", "element_kind": "arrow"}`` is read for a set of a
    primitive; the form this pull request first wrote was never released.
    """
    report = _read_stored_column_in_fresh_process(
        "set[int64]",
        b'{"category": "set", "element_arrow_type": "int64"}',
        "large_list<int64>",
    )
    assert report.get("refused") == "LogicalTypeIdentifierError", report


@pytest.mark.parametrize(
    "storage",
    [pa.array(["a"], pa.large_string()), pa.array([[1]], pa.large_list(pa.int8()))],
    ids=["string_storage", "other_element_width"],
)
def test_a_registered_set_whose_storage_does_not_hold_its_element_is_refused(
    storage: "pa.Array[Any]",
) -> None:
    """With ``set[int64]`` already registered, a stored ``set[int64]`` column over another
    storage is refused as it is in a fresh process, with ``LogicalTypeIdentifierError``.
    """
    from orcapod.contexts import get_default_context
    from orcapod.logical_types.identifier import LogicalTypeIdentifierError
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    _row_hash(set[int], {1})  # registers set[int64] in this process
    field_metadata = {
        b"ARROW:extension:name": b"set[int64]",
        b"ARROW:extension:metadata": SET_METADATA,
    }
    table = pa.table(
        [storage],
        schema=pa.schema([pa.field("c", storage.type, metadata=field_metadata)]),
    )
    converter = get_default_context().type_converter
    assert isinstance(converter, UniversalTypeConverter)
    with pytest.raises(LogicalTypeIdentifierError) as excinfo:
        converter.load_logical_types(table)
    assert excinfo.value.column == "c"
    notes = getattr(excinfo.value, "__notes__", [])
    assert any(str(storage.type) in note for note in notes), notes


_TWO_WIDTHS_IN_A_LIST = textwrap.dedent(
    """
    import json, sys
    import numpy as np
    from orcapod.sources import DictSource

    declared = {
        "int8": (list[set[np.int8]], [{1, 2}]),
        "int64": (list[set[int]], [{1, 300}]),
    }
    report = {}
    for name in sys.argv[1:]:
        annotation, value = declared[name]
        source = DictSource(
            [{"k": 0, "v": value}],
            tag_columns=["k"],
            data_schema={"k": int, "v": annotation},
            source_id="one_row",
        )
        _, data = next(iter(source.iter_data()))
        read = data.as_dict()["v"]
        report[name] = {
            "arrow_type": str(source.as_table().schema.field("v").type),
            "schema": str(source.output_schema()[1]["v"]),
            "read": [sorted(int(v) for v in s) for s in read],
            "read_type": [type(s).__name__ for s in read],
        }
    print(json.dumps(report))
    """
)


@pytest.mark.parametrize("order", [("int8", "int64"), ("int64", "int8")])
def test_two_integer_widths_of_a_list_of_sets_live_in_one_process(
    order: tuple[str, str],
) -> None:
    """``list[set[np.int8]]`` and ``list[set[int]]`` are two containers in one process.

    A list of a set of a plain element is found by its identifier only, as the set is.
    """
    result = subprocess.run(
        [sys.executable, "-c", _TWO_WIDTHS_IN_A_LIST, *order],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    report = json.loads(result.stdout.strip().splitlines()[-1])
    assert report["int8"] == {
        "arrow_type": "extension<list[set[int8]]<_ArrowExt_list_set_int8__>>",
        "schema": "list[set[int]]",
        "read": [[1, 2]],
        "read_type": ["set"],
    }
    assert report["int64"] == {
        "arrow_type": "extension<list[set[int64]]<_ArrowExt_list_set_int64__>>",
        "schema": "list[set[int]]",
        "read": [[1, 300]],
        "read_type": ["set"],
    }


def test_a_list_of_sets_inside_a_dict_value_is_refused() -> None:
    """``dict[str, list[set[int]]]`` is refused as a source schema (ITL-1150)."""
    with pytest.raises(ValueError, match="ITL-1150"):
        DictSource(
            [{"k": 0, "v": {"a": [{2, 1}]}}],
            tag_columns=["k"],
            data_schema={"k": int, "v": dict[str, list[set[int]]]},
            source_id="one_row",
        )


def test_the_factory_reads_a_set_element_as_its_arrow_type_does() -> None:
    """``ListLogicalTypeFactory.create_for_python_type`` gives a set of a plain element
    the Python type of its element Arrow type, as reconstruction from Arrow does.
    """
    from typing import Literal

    from orcapod.contexts import get_default_context
    from orcapod.logical_types.list_logical_type_factory import ListLogicalTypeFactory

    converter = get_default_context().type_converter
    # The factory's write path is what this test is about, so it is called directly.
    created = ListLogicalTypeFactory().create_for_python_type(
        set[Literal[1, 2]],
        converter,  # pyright: ignore[reportArgumentType]
    )
    assert created.logical_type_name == "set[int64]"
    assert created.python_type == set[int]
