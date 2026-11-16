"""``orcapod.dataclass``: a dataclass identified by its name and fields (ITL-972).

Covers the decorator (a real dataclass, its options, what it refuses), the identity
(name plus ordered fields; the class object and its module do not matter), equality
across classes of one type, explicit conversion with ``from_instance``, and reading
stored values in a process that does not define the class.
"""

from __future__ import annotations

import dataclasses
import inspect
from pathlib import Path
from typing import Any

import pydantic
import pytest

import orcapod as op
from orcapod import function_pod
from orcapod.sources import DictSource
from tests.test_user_types.child_harness import WSJOB, run_child, write_files


def _source(value: Any, key: str = "p") -> DictSource:
    """Return a one-row source holding ``value`` in data column ``key``."""
    return DictSource(
        data=[{"k": "a", key: value}],
        tag_columns=["k"],
        source_id="s",
        tag_only_columns=True,
    )


def _extension_name(source: DictSource, key: str = "p") -> str:
    """Return the Arrow extension name of data column ``key``."""
    return source.as_table().schema.field(key).type.extension_name


def _convert(cls: type, other: Any) -> Any:
    """Return ``cls.from_instance(other)``.

    ``from_instance`` is added by the decorator, which static type checkers cannot see.
    """
    return cls.from_instance(other)  # pyright: ignore[reportAttributeAccessIssue]  # added by op.dataclass at run time


def _read(source: Any, key: str = "p") -> Any:
    """Return the value of data column ``key`` in the stream's only row."""
    return [data.as_dict()[key] for _, data in source.iter_data()][0]


# ---------------------------------------------------------------------------
# The decorator
# ---------------------------------------------------------------------------


@op.dataclass
class Plain:
    """Bare decorator: named ``Plain``."""

    x: int
    y: float = 0.5


@op.dataclass(name="itl972.Frozen", frozen=True, slots=True, order=True)
class Frozen:
    """Decorator with a name and dataclass options."""

    x: int
    y: int


def test_result_is_a_real_dataclass() -> None:
    """The class works with the ``dataclasses`` module like any dataclass."""
    assert dataclasses.is_dataclass(Plain)
    assert [f.name for f in dataclasses.fields(Plain)] == ["x", "y"]
    p = Plain(1)
    assert p == Plain(1, 0.5)
    assert dataclasses.asdict(p) == {"x": 1, "y": 0.5}
    assert dataclasses.replace(p, x=2) == Plain(2, 0.5)
    assert repr(p) == "Plain(x=1, y=0.5)"
    assert Plain(x=3, y=1.0).y == 1.0


def test_constructor_signature_is_the_dataclass_signature() -> None:
    """``inspect.signature`` shows the fields, as for a plain dataclass."""
    assert list(inspect.signature(Plain).parameters) == ["x", "y"]


def test_dataclass_options_are_passed_through() -> None:
    """``frozen``, ``slots`` and ``order`` behave as in ``dataclasses.dataclass``."""
    f = Frozen(1, 2)
    with pytest.raises(dataclasses.FrozenInstanceError):
        f.x = 5  # pyright: ignore[reportAttributeAccessIssue]  # the assignment must fail
    assert not hasattr(f, "__dict__")
    assert Frozen(1, 2) < Frozen(1, 3)
    assert hash(f) == hash(Frozen(1, 2))


def test_init_false_is_refused() -> None:
    """``init=False`` is not accepted."""
    with pytest.raises(TypeError, match="init=False"):

        @op.dataclass(init=False)
        class NoInit:
            x: int


def test_field_level_compare_and_hash_choices_are_refused() -> None:
    """Equality and hashing cover exactly the stored fields, for every class of a type."""
    with pytest.raises(TypeError, match="'payload'.*compare=False"):

        @op.dataclass
        class Uncompared:
            x: int
            payload: list[int] = dataclasses.field(default_factory=list, compare=False)

    with pytest.raises(TypeError, match="'payload'.*hash=False"):

        @op.dataclass(frozen=True)
        class Unhashed:
            x: int
            payload: tuple[int, ...] = dataclasses.field(default=(), hash=False)

    with pytest.raises(TypeError, match="'cached'.*init=False"):

        @op.dataclass
        class Derived:
            x: int
            cached: int = dataclasses.field(init=False, default=0)

    @op.dataclass(name="itl972.DerivedOk")
    class DerivedOk:
        x: int
        cached: int = dataclasses.field(init=False, default=0, compare=False)

    assert DerivedOk(1) == DerivedOk(1)


def test_an_existing_dataclass_is_refused() -> None:
    """The decorator defines the dataclass; it is not applied on top of one."""
    with pytest.raises(TypeError, match="already a dataclass"):

        @op.dataclass
        @dataclasses.dataclass
        class Twice:
            x: int


def test_a_pydantic_model_is_refused() -> None:
    """A pydantic model cannot be made structural."""
    with pytest.raises(TypeError, match="pydantic"):

        @op.dataclass
        class Model(pydantic.BaseModel):
            x: int


@pytest.mark.parametrize("name", ["", "a b", "a@b", "a;b", "orcapod.thing"])
def test_invalid_names_are_refused(name: str) -> None:
    """A name must not be empty, contain ``@``, ``;`` or whitespace, or be reserved."""
    with pytest.raises(ValueError, match="name"):

        @op.dataclass(name=name)
        class Named:
            x: int


# ---------------------------------------------------------------------------
# Identity: name plus ordered fields
# ---------------------------------------------------------------------------


@op.dataclass(name="itl972.Pair")
class PairA:
    """``itl972.Pair`` with fields ``x``, ``y``."""

    x: int
    y: int


@op.dataclass(name="itl972.Pair")
class PairB:
    """A second class of the same name and fields."""

    x: int
    y: int


@op.dataclass(name="itl972.Pair")
class PairReordered:
    """The same name with the fields in another order: another type."""

    y: int
    x: int


@op.dataclass(name="itl972.OtherPair")
class PairRenamed:
    """The same fields under another name: another type."""

    x: int
    y: int


@dataclasses.dataclass
class OrdinaryPair:
    """An ordinary dataclass with the same fields: another type."""

    x: int
    y: int


def test_default_name_is_the_class_name() -> None:
    """Without ``name=`` the type is named by the class name, not the module path."""
    assert _extension_name(_source(Plain(1))).split("@")[0] == "Plain"


def test_same_name_and_fields_is_one_type() -> None:
    """Two classes with one name and the same fields share identifier and hashes."""
    a, b = _source(PairA(1, 2)), _source(PairB(1, 2))
    assert _extension_name(a) == _extension_name(b)
    assert a.pipeline_hash() == b.pipeline_hash()
    assert a.as_table().schema.field("p").type == b.as_table().schema.field("p").type


@pytest.mark.parametrize(
    "other", [PairReordered(2, 1), PairRenamed(1, 2), OrdinaryPair(1, 2)]
)
def test_other_order_name_or_kind_is_another_type(other: Any) -> None:
    """Field order, the name and the kind of class are each part of the identity."""
    a, b = _source(PairA(1, 2)), _source(other)
    assert _extension_name(a) != _extension_name(b)
    assert a.pipeline_hash() != b.pipeline_hash()


def test_a_local_class_is_allowed() -> None:
    """A structural class may be defined inside a function."""

    @op.dataclass(name="itl972.Local")
    class Local:
        x: int

    assert _read(_source(Local(3))) == Local(3)


def test_a_field_typed_by_an_exact_class_is_refused() -> None:
    """Every class inside a structural class must be structural."""

    @op.dataclass(name="itl972.HoldsOrdinary")
    class HoldsOrdinary:
        inner: OrdinaryPair

    with pytest.raises(TypeError, match="inner"):
        _ = _source(HoldsOrdinary(OrdinaryPair(1, 2)))


def test_pod_accepts_another_class_of_the_same_type() -> None:
    """A pod annotated with one class accepts a column of the other, fields intact."""

    @function_pod(output_keys="seen")
    def look(p: PairB) -> str:
        """Return the fields of the argument and whether it equals a ``PairB``."""
        return f"{p.x},{p.y},{p == PairB(p.x, p.y)}"

    assert _read(look.pod(_source(PairA(1, 2))), "seen") == "1,2,True"


# ---------------------------------------------------------------------------
# Equality across classes of one type
# ---------------------------------------------------------------------------


def test_instances_of_two_classes_of_one_type_are_equal() -> None:
    """Equal field values make instances of two classes of one type equal."""
    assert PairA(1, 2) == PairB(1, 2)
    assert PairB(1, 2) == PairA(1, 2)
    assert PairA(1, 2) != PairB(1, 3)
    assert not (PairA(1, 2) != PairB(1, 2))


@pytest.mark.parametrize(
    "other", [PairReordered(2, 1), PairRenamed(1, 2), OrdinaryPair(1, 2), (1, 2)]
)
def test_instances_of_another_type_are_not_equal(other: Any) -> None:
    """Another order, name or kind of class is another type: never equal."""
    assert PairA(1, 2) != other
    assert other != PairA(1, 2)


def test_nested_structural_fields_compare_the_same_way() -> None:
    """Equality reaches through fields, lists and optionals of structural classes."""
    a = Segment(PairA(1, 2), [PairA(3, 4)], PairA(5, 6))
    b = SegmentB(PairB(1, 2), [PairB(3, 4)], PairB(5, 6))
    assert a == b
    assert a != SegmentB(PairB(1, 2), [PairB(3, 9)], PairB(5, 6))


def test_hash_agrees_with_equality() -> None:
    """Hashable structural instances that are equal have equal hashes."""

    @op.dataclass(name="itl972.Key", frozen=True)
    class KeyA:
        x: int

    @op.dataclass(name="itl972.Key", frozen=True)
    class KeyB:
        x: int

    assert KeyA(1) == KeyB(1)
    assert hash(KeyA(1)) == hash(KeyB(1))
    lookup: dict[Any, str] = {KeyA(1): "a"}
    assert lookup[KeyB(1)] == "a"


def test_eq_false_and_a_class_eq_are_left_alone() -> None:
    """``eq=False`` and an ``__eq__`` written in the class are not replaced."""

    @op.dataclass(name="itl972.NoEq", eq=False)
    class NoEq:
        x: int

    @op.dataclass(name="itl972.OwnEq")
    class OwnEq:
        x: int

        def __eq__(self, other: object) -> bool:
            """Everything is equal."""
            return True

    assert NoEq(1) != NoEq(1)
    assert OwnEq(1) == "anything"


@pytest.mark.parametrize(
    "other", [PairReordered(2, 1), PairRenamed(1, 2), OrdinaryPair(1, 2)]
)
def test_pod_refuses_another_type(other: Any) -> None:
    """A pod annotated with a structural class refuses a column of another type."""

    @function_pod(output_keys="seen")
    def look(p: PairA) -> str:
        """Return the class name of the argument."""
        return type(p).__name__

    with pytest.raises(ValueError, match="column 'p'"):
        _ = look.pod(_source(other))


# ---------------------------------------------------------------------------
# Conversion
# ---------------------------------------------------------------------------


@op.dataclass(name="itl972.Segment")
class Segment:
    """A class with structural fields, alone, in a list and optional."""

    start: PairA
    stops: list[PairA]
    end: PairA | None = None


@op.dataclass(name="itl972.Segment")
class SegmentB:
    """Another class of the same type, built over ``PairB``."""

    start: PairB
    stops: list[PairB]
    end: PairB | None = None


def test_from_instance_of_an_ordinary_dataclass() -> None:
    """``from_instance`` builds the class from an ordinary dataclass instance."""
    built = _convert(PairA, OrdinaryPair(1, 2))
    assert type(built) is PairA
    assert built == PairA(1, 2)


def test_from_instance_of_another_structural_class() -> None:
    """``from_instance`` builds the class from another structural class's instance."""
    built = _convert(PairA, PairReordered(y=2, x=1))
    assert type(built) is PairA
    assert built == PairA(1, 2)


def test_from_instance_converts_nested_fields() -> None:
    """Fields typed by a structural class are converted, also in lists and optionals."""
    source = SegmentB(PairB(1, 2), [PairB(3, 4)], PairB(5, 6))
    built = _convert(Segment, source)
    assert type(built.start) is PairA
    assert [type(stop) for stop in built.stops] == [PairA]
    assert type(built.end) is PairA
    assert built == Segment(PairA(1, 2), [PairA(3, 4)], PairA(5, 6))
    assert _convert(Segment, SegmentB(PairB(1, 2), [])).end is None


@op.dataclass(name="itl972.Named")
class Named:
    """A structural class with one text field."""

    label: str


@op.dataclass(name="itl972.Either")
class Either:
    """A structural class with a union of two structural classes."""

    value: PairA | Named


@dataclasses.dataclass
class OtherEither:
    """An ordinary dataclass with the same field as ``Either``."""

    value: PairA | Named


@dataclasses.dataclass
class OrdinaryNamed:
    """An ordinary dataclass with the field of ``Named``."""

    label: str


def test_from_instance_keeps_the_union_member_a_value_already_is() -> None:
    """A union field keeps a value of one member and converts to the member that fits."""
    kept = _convert(Either, OtherEither(Named("n")))
    assert type(kept.value) is Named
    converted = _convert(Either, OtherEither(OrdinaryNamed("n")))  # pyright: ignore[reportArgumentType]  # dataclasses do not check field types
    assert type(converted.value) is Named
    assert converted.value == Named("n")


@op.dataclass(name="itl972.IntRecord")
class IntRecord:
    """A structural class whose ``x`` is an ``int``."""

    x: int


@op.dataclass(name="itl972.TextRecord")
class TextRecord:
    """A structural class whose ``x`` is a ``str``: same field name, other type."""

    x: str


@op.dataclass(name="itl972.Records")
class Records:
    """A structural class with unions of the two record classes."""

    one: IntRecord | TextRecord
    many: list[IntRecord] | list[TextRecord]


@dataclasses.dataclass
class OrdinaryText:
    """An ordinary dataclass with the field of ``TextRecord``."""

    x: str


@dataclasses.dataclass
class OrdinaryRecords:
    """An ordinary dataclass with the field names of ``Records``."""

    one: IntRecord | TextRecord
    many: list[IntRecord] | list[TextRecord]


def test_from_instance_chooses_the_union_member_by_field_types() -> None:
    """A union member is chosen by declared field types, not by field names alone."""
    source: Any = OrdinaryRecords(OrdinaryText("a"), [OrdinaryText("b")])  # pyright: ignore[reportArgumentType]  # dataclasses do not check field types
    built = _convert(Records, source)
    assert built.one == TextRecord("a")
    assert type(built.one) is TextRecord
    assert built.many == [TextRecord("b")]
    assert [type(item) for item in built.many] == [TextRecord]


@dataclasses.dataclass
class OrdinaryFlag:
    """An ordinary dataclass that fits neither record class."""

    flag: bool


def test_from_instance_refuses_a_union_value_no_member_can_take() -> None:
    """A value that fits no member of a union is an error, never left unconverted."""
    alone: Any = OrdinaryRecords(OrdinaryFlag(True), [])  # pyright: ignore[reportArgumentType]  # dataclasses do not check field types
    with pytest.raises(TypeError, match="OrdinaryFlag"):
        _ = _convert(Records, alone)
    in_list: Any = OrdinaryRecords(TextRecord("a"), [OrdinaryFlag(True)])  # pyright: ignore[reportArgumentType]  # dataclasses do not check field types
    with pytest.raises(TypeError, match="list"):
        _ = _convert(Records, in_list)


def test_from_instance_leaves_a_union_value_no_member_claims() -> None:
    """A value of a non-class member of a union is passed as it is."""

    @dataclasses.dataclass
    class Source:
        value: int | IntRecord

    converted = _convert(IntOrRecord, Source(3))
    assert converted.value == 3


@op.dataclass(name="itl972.IntOrRecord")
class IntOrRecord:
    """A structural class whose field is an ``int`` or a structural class."""

    value: int | IntRecord


def test_from_instance_refuses_other_fields() -> None:
    """A missing or an extra field is an error naming the fields."""

    @dataclasses.dataclass
    class Wider:
        x: int
        y: int
        z: int

    @dataclasses.dataclass
    class Narrower:
        x: int

    with pytest.raises(TypeError, match="z"):
        _ = _convert(PairA, Wider(1, 2, 3))
    with pytest.raises(TypeError, match="y"):
        _ = _convert(PairA, Narrower(1))


def test_from_instance_refuses_another_field_type() -> None:
    """A field declared with another type is an error naming the field."""

    @dataclasses.dataclass
    class Texts:
        x: str
        y: int

    with pytest.raises(TypeError, match="'x'"):
        _ = _convert(PairA, Texts("1", 2))


def test_from_instance_refuses_what_is_not_a_dataclass_instance() -> None:
    """A pydantic model, a dict or a class is refused."""

    class Model(pydantic.BaseModel):
        x: int
        y: int

    for other in (Model(x=1, y=2), {"x": 1, "y": 2}, OrdinaryPair):
        with pytest.raises(TypeError, match="dataclass"):
            _ = _convert(PairA, other)


def test_constructor_is_the_plain_field_constructor() -> None:
    """``P(other)`` does not convert: the constructor takes field values only."""
    built = PairA(OrdinaryPair(1, 2), 3)  # pyright: ignore[reportArgumentType]  # dataclasses do not check field types
    assert built.x == OrdinaryPair(1, 2)
    with pytest.raises(TypeError):
        _ = PairA(OrdinaryPair(1, 2))  # pyright: ignore[reportCallIssue, reportArgumentType]  # y is missing


def test_conversion_runs_post_init() -> None:
    """The instance is built by the class's own ``__init__``."""

    @op.dataclass(name="itl972.Checked")
    class Checked:
        x: int

        def __post_init__(self) -> None:
            """Refuse negative values."""
            if self.x < 0:
                raise ValueError("x must not be negative")

    @dataclasses.dataclass
    class Raw:
        x: int

    assert _convert(Checked, Raw(1)) == Checked(1)
    with pytest.raises(ValueError, match="negative"):
        _ = _convert(Checked, Raw(-1))


def test_pod_parameter_in_a_container_is_compatible_and_passed_as_is() -> None:
    """A list and an optional of another class of the same type are accepted as they are."""

    @function_pod(output_keys="seen")
    def look(ps: list[PairB], maybe: PairB | None) -> str:
        """Return the sum of all ``x`` fields."""
        return str(sum(p.x for p in [*ps, maybe] if p is not None))

    row: dict[str, Any] = {
        "k": "a",
        "ps": [PairA(1, 2), PairA(3, 4)],
        "maybe": PairA(5, 6),
    }
    source = DictSource(
        data=[row],
        tag_columns=["k"],
        source_id="s",
        tag_only_columns=True,
    )
    assert _read(look.pod(source), "seen") == "9"


# ---------------------------------------------------------------------------
# Reading without the class
# ---------------------------------------------------------------------------

WRITER = """
import os
from pathlib import Path

import orcapod as op
from child_harness import report
from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource


@op.dataclass(name="itl972.child.Point")
class Point:
    x: int
    y: float


@op.dataclass(name="itl972.child.Key", frozen=True)
class Key:
    n: int


@op.dataclass(name="itl972.child.Sample")
class Sample:
    label: str
    where: Point
    tags: list[str]
    note: str | None
    file: Path
    history: list[Point]
    keys: set[Key]


@function_pod(output_keys="sample")
def make(x: int) -> Sample:
    return Sample(f"s{x}", Point(x, x / 2), ["a", "b"], None, Path("/tmp/f"), [Point(0, 0.0)], {Key(1), Key(2)})


job = PipelineJob(name="w", database=DeltaTableDatabase(Path(os.environ["WS_DB"])))
with job:
    make.pod(DictSource(data=[{"k": "a", "x": 1}], tag_columns=["k"], tag_only_columns=True, source_id="s"), label="make")
job.run()
report({"rows": job.nodes["make"].as_table().num_rows})
"""

READER = """
import dataclasses
import os
from pathlib import Path

import wsjob
from child_harness import report
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, ExtensionAwareDatabase

root = Path(os.environ["WS_DB"])
path = tuple(next(p for p in wsjob.table_paths(root) if "/_result/" in p).split("/"))
converter = get_default_context().type_converter
table = ExtensionAwareDatabase(DeltaTableDatabase(root), converter=converter).get_all_records(path)
out = []
for row in converter.arrow_table_to_python_dicts(table.select(["sample"])):
    value = row["sample"]
    out.append({
        "class": type(value).__name__,
        "has_from_instance": hasattr(type(value), "from_instance"),
        "module": type(value).__module__,
        "is_dataclass": dataclasses.is_dataclass(value),
        "fields": [f.name for f in dataclasses.fields(value)],
        "where_class": type(value.where).__name__,
        "where": dataclasses.asdict(value.where),
        "tags": value.tags,
        "note": value.note,
        "file_is_path": isinstance(value.file, Path),
        "history": [dataclasses.asdict(p) for p in value.history],
        "keys_is_set": isinstance(value.keys, set),
        "keys": sorted(key.n for key in value.keys),
    })
report(out)
"""


@pytest.mark.integration
def test_stored_values_are_read_without_the_class(tmp_path: Path) -> None:
    """A process that does not define the class reads a class built from the metadata."""
    write_files(tmp_path, {"writer.py": WRITER, "reader.py": READER, "wsjob.py": WSJOB})
    (tmp_path / "db").mkdir()
    env = {"WS_DB": str(tmp_path / "db")}
    assert run_child(["writer.py"], cwd=tmp_path, env=env) == {"rows": 1}

    read = run_child(["reader.py"], cwd=tmp_path, env=env)
    assert read == [
        {
            "class": "Sample",
            "has_from_instance": True,
            "module": "orcapod.synthesized",
            "is_dataclass": True,
            "fields": ["label", "where", "tags", "note", "file", "history", "keys"],
            "where_class": "Point",
            "where": {"x": 1, "y": 0.5},
            "tags": ["a", "b"],
            "note": None,
            "file_is_path": True,
            "history": [{"x": 0, "y": 0.0}],
            "keys_is_set": True,
            "keys": [1, 2],
        }
    ]


def test_synthesized_class_equals_and_hashes_like_a_defined_class() -> None:
    """A class built from metadata and a defined class of the same type agree.

    Their instances with equal fields are equal in both directions and, both being
    hashable, have equal hashes.
    """
    from orcapod.contexts import get_default_context
    from orcapod.logical_types.structural_class import synthesize_structural_class

    @op.dataclass(name="itl972.Synth", frozen=True)
    class Defined:
        x: int
        label: str

    record = {
        "kind": "structural",
        "name": "itl972.Synth",
        "fields": [["x", "int"], ["label", "str"]],
    }
    # Non-public entry point: with ``Defined`` in the process a read would use it, so
    # the class is built from the record directly, as a read without the class does.
    converter: Any = get_default_context().type_converter
    synthesized = synthesize_structural_class(record, converter)
    assert synthesized is not Defined
    assert synthesized.__module__ == "orcapod.synthesized"

    built, defined = synthesized(1, "a"), Defined(1, "a")
    assert built == defined
    assert defined == built
    assert hash(built) == hash(defined)
    assert built != Defined(2, "a")
    assert len({built, defined}) == 1
