"""A logical type or native container where storage cannot keep it is refused (ITL-1150).

Arrow cannot hold an extension type inside a struct field, and a Python ``dict`` is stored
as a list of key/value structs (a tuple as a struct). A logical type or a native container
(``set[int]``, ``list[set[int]]``) as a dict key or value would not read back as declared: a
set would read as a list. Such a declared type is refused when it is declared (a pod's
parameter or output, a source's declared or inferred schema, a user-class field), with an
error that names the column or ``Class.field``, the type and ITL-1150.

The positions that do keep the declared type stay accepted: a set, a list of sets and any
logical type as a column, in a list, and as a field of a dataclass, an ``orcapod.dataclass``
or a pydantic model, read back as declared from the node, from ``as_source()`` and from a
job loaded in a fresh process.
"""

from __future__ import annotations

import dataclasses
import json
import re
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path
from typing import Any

import pydantic
import pytest

import orcapod as op
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import MapData
from orcapod.sources import DictSource

# A declared type whose value would not read back as declared -> one value of it.
REFUSED: dict[str, tuple[Any, Any]] = {
    "dict_value_set_int": (dict[str, set[int]], {"a": {2, 1}}),
    "dict_value_set_str": (dict[str, set[str]], {"a": {"y", "x"}}),
    "dict_value_list_of_sets": (dict[str, list[set[int]]], {"a": [{2, 1}]}),
    "dict_value_optional_set": (dict[str, set[int] | None], {"a": {1}}),
    "dict_key_set": (dict[set[int], int], {}),
    "list_of_dict_of_sets": (list[dict[str, set[int]]], [{"a": {1}}]),
    "dict_value_path": (dict[str, Path], {"a": Path("/a")}),
    "dict_value_set_of_paths": (dict[str, set[Path]], {"a": {Path("/a")}}),
}


@dataclasses.dataclass
class HoldsDictOfSets:
    x: dict[str, set[int]]


class ModelHoldsDictOfSets(pydantic.BaseModel):
    x: dict[str, set[int]]


@dataclasses.dataclass
class Fields:
    """An exact class whose fields hold logical types and native containers."""

    s: set[int]
    t: set[str]
    p: Path
    u: uuid.UUID
    ls: list[set[int]]
    lp: list[Path]


@op.dataclass
class StructuralFields:
    s: set[int]
    p: Path
    ls: list[set[int]]


class ModelFields(pydantic.BaseModel):
    s: set[int]
    p: Path
    ls: list[set[int]]


def _producer(declared: Any, value: Any) -> Any:
    """A function pod with one input ``i`` that returns ``value`` as ``out``."""

    def produce(i: int) -> Any:
        return value

    produce.__annotations__["return"] = declared
    return op.function_pod(output_keys="out")(produce)


def _consumer(declared: Any) -> Any:
    """A function pod whose parameter ``got`` is declared ``declared``."""

    def consume(got: Any) -> int:
        return 1

    consume.__annotations__["got"] = declared
    return op.function_pod(output_keys="n")(consume)


def _refusal(column: str, declared: Any) -> str:
    """The pattern a refusal of ``declared`` at ``column`` matches."""
    return rf"(?s){column}.*{_escaped(declared)}.*ITL-1150"


def _escaped(declared: Any) -> str:
    return re.escape(str(declared))


@pytest.mark.parametrize("case", list(REFUSED))
class TestRefused:
    """Every declaration of a type that storage cannot keep is refused when declared."""

    def test_a_pod_output(self, case: str) -> None:
        declared, value = REFUSED[case]
        with pytest.raises(ValueError, match=_refusal("'out'", declared)):
            _producer(declared, value)

    def test_a_pod_parameter(self, case: str) -> None:
        declared, _ = REFUSED[case]
        with pytest.raises(ValueError, match=_refusal("'got'", declared)):
            _consumer(declared)

    def test_a_declared_source_schema(self, case: str) -> None:
        declared, value = REFUSED[case]
        with pytest.raises(ValueError, match=_refusal("'v'", declared)):
            DictSource(
                [{"k": 0, "v": value}],
                tag_columns=["k"],
                data_schema={"k": int, "v": declared},
                source_id="one_row",
            )


def test_a_source_schema_inferred_from_rows_is_refused() -> None:
    """Rows holding a dict of sets infer ``dict[str, set[int]]``, which is refused."""
    with pytest.raises(ValueError, match=_refusal("'v'", dict[str, set[int]])):
        DictSource([{"k": 0, "v": {"a": {1, 2}}}], tag_columns=["k"], source_id="x")


@pytest.mark.parametrize("cls", [HoldsDictOfSets, ModelHoldsDictOfSets])
def test_a_user_class_field_is_refused_naming_the_field(cls: type) -> None:
    """A class field typed ``dict[str, set[int]]`` is refused, naming ``Class.field``."""
    with pytest.raises(
        ValueError, match=rf"(?s){cls.__qualname__}\.x.*{_escaped(dict[str, set[int]])}"
    ) as info:
        _producer(cls, cls(x={"a": {1}}))
    assert "ITL-1150" in str(info.value)


def test_the_refusal_says_how_to_keep_the_value() -> None:
    """The message points to a dataclass field, or the plain type, as the way out."""
    with pytest.raises(ValueError) as info:
        _producer(dict[str, set[int]], {"a": {1}})
    message = str(info.value)
    assert "dataclass" in message
    assert "dict[str, list[int]]" in message


# ---------------------------------------------------------------------------
# Accepted: the positions that keep the declared type
# ---------------------------------------------------------------------------

ACCEPTED: dict[str, tuple[Any, Any]] = {
    "set_int": (set[int], {3, 1, 2}),
    "list_of_sets": (list[set[int]], [{2, 1}, {3}]),
    "list_of_paths": (list[Path], [Path("/a"), Path("/b")]),
    "dict_of_lists": (dict[str, list[int]], {"a": [2, 1]}),
    "dict_of_ints": (dict[str, int], {"a": 1}),
    "list_of_dicts": (list[dict[str, int]], [{"a": 1}]),
    "dataclass_fields": (
        Fields,
        Fields(
            s={2, 1},
            t={"b", "a"},
            p=Path("/a"),
            u=uuid.UUID(int=5),
            ls=[{2, 1}],
            lp=[Path("/b")],
        ),
    ),
    "structural_fields": (
        StructuralFields,
        StructuralFields(s={2, 1}, p=Path("/a"), ls=[{3}]),
    ),
    "pydantic_fields": (ModelFields, ModelFields(s={2, 1}, p=Path("/a"), ls=[{3}])),
}


def _skeleton(value: Any) -> Any:
    """The value with every container and field typed, so a set never equals a list."""
    if isinstance(value, set):
        return ("set", sorted((_skeleton(v) for v in value), key=repr))
    if isinstance(value, list):
        return ("list", [_skeleton(v) for v in value])
    if isinstance(value, dict):
        return ("dict", sorted((k, _skeleton(v)) for k, v in value.items()))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return (
            type(value).__qualname__,
            [
                (f.name, _skeleton(getattr(value, f.name)))
                for f in dataclasses.fields(value)
            ],
        )
    if isinstance(value, pydantic.BaseModel):
        return (
            type(value).__qualname__,
            [
                (name, _skeleton(getattr(value, name)))
                for name in type(value).model_fields
            ],
        )
    return (type(value).__name__, repr(value))


@pytest.mark.parametrize("case", list(ACCEPTED))
def test_an_accepted_type_reads_back_as_declared(case: str) -> None:
    """The node, a downstream pod and ``as_source()`` read the value as it was returned."""
    declared, value = ACCEPTED[case]
    received: list[Any] = []

    def consume(got: Any) -> int:
        received.append(got)
        return 1

    consume.__annotations__["got"] = declared
    source = DictSource([{"k": 0, "i": 0}], tag_columns=["k"], source_id="one_row")
    job = op.PipelineJob(name="accepted", database=InMemoryArrowDatabase())
    with job:
        produced = _producer(declared, value).pod(source, label="produce")
        renamed = MapData({"out": "got"})(produced, label="rename")
        op.function_pod(output_keys="n")(consume).pod(renamed, label="consume")
    job.run()

    expected = _skeleton(value)
    node = job.nodes["produce"]
    stored = node.as_source()
    for stream in (node, stored):
        (_, data), *rest = list(stream.iter_data())
        assert not rest
        assert _skeleton(data.as_dict()["out"]) == expected
    assert [_skeleton(v) for v in received] == [expected]
    assert stored.output_schema()[1]["out"] == declared


_CLASSES = textwrap.dedent(
    """\
    import dataclasses
    import uuid
    from pathlib import Path

    import pydantic

    import orcapod


    @dataclasses.dataclass
    class Fields:
        s: set[int]
        p: Path
        u: uuid.UUID
        ls: list[set[int]]


    @orcapod.dataclass
    class StructuralFields:
        s: set[int]
        p: Path
        ls: list[set[int]]


    class ModelFields(pydantic.BaseModel):
        s: set[int]
        p: Path
        ls: list[set[int]]


    VALUES = {
        "Fields": lambda: Fields(s={2, 1}, p=Path("/a"), u=uuid.UUID(int=5), ls=[{2, 1}]),
        "StructuralFields": lambda: StructuralFields(s={2, 1}, p=Path("/a"), ls=[{3}]),
        "ModelFields": lambda: ModelFields(s={2, 1}, p=Path("/a"), ls=[{3}]),
    }


    def skeleton(value):
        if isinstance(value, set):
            return ["set", sorted((skeleton(v) for v in value), key=repr)]
        if isinstance(value, list):
            return ["list", [skeleton(v) for v in value]]
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return [
                type(value).__qualname__,
                [[f.name, skeleton(getattr(value, f.name))] for f in dataclasses.fields(value)],
            ]
        if isinstance(value, pydantic.BaseModel):
            return [
                type(value).__qualname__,
                [[n, skeleton(getattr(value, n))] for n in type(value).model_fields],
            ]
        return [type(value).__name__, repr(value)]
    """
)

_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys
    import warnings

    warnings.simplefilter("ignore")
    sys.path.insert(0, sys.argv[1])
    mode, work, class_name = sys.argv[2], sys.argv[3], sys.argv[4]

    import classes
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource

    cls = getattr(classes, class_name)


    @function_pod(output_keys="out")
    def produce(i: int) -> cls:
        return classes.VALUES[class_name]()


    if mode == "save":
        source = DictSource([{"k": 0, "i": 0}], tag_columns=["k"], source_id="one_row")
        job = PipelineJob(name="fields", database=DeltaTableDatabase(work + "/store"))
        with job:
            produce.pod(source, label="produce")
        job.run()
        job.save(work + "/job.json")
    else:
        job = PipelineJob.load(work + "/job.json", database=DeltaTableDatabase(work + "/store"))
    _, data = next(iter(job.nodes["produce"].as_source().iter_data()))
    print(json.dumps({
        "value": classes.skeleton(data.as_dict()["out"]),
        "row_hash": data.content_hash().to_string(),
    }))
    """
)


def _run(classes_dir: Path, mode: str, work: Path, class_name: str) -> dict[str, Any]:
    result = subprocess.run(
        [sys.executable, "-c", _SCRIPT, str(classes_dir), mode, str(work), class_name],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("class_name", ["Fields", "StructuralFields", "ModelFields"])
def test_user_class_fields_read_back_as_declared_in_a_fresh_process(
    class_name: str, tmp_path: Path
) -> None:
    """A loaded job reads class fields holding sets and paths as declared, same row hash."""
    classes_dir = tmp_path / "classes"
    classes_dir.mkdir()
    (classes_dir / "classes.py").write_text(_CLASSES)
    saved = _run(classes_dir, "save", tmp_path, class_name)
    loaded = _run(classes_dir, "load", tmp_path, class_name)
    assert loaded == saved
    assert saved["value"][0] == class_name
    assert any(field[1][0] == "set" for field in saved["value"][1])
