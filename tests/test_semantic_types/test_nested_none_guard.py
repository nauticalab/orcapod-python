"""A None at a nested position that a read cannot restore is refused (ITL-1144).

A None inside a container (a list or set element, a dict value, a user-class field) is
stored faithfully, but reading the stored column back to Python restored it only for a
primitive or a user class: a container read as empty and any other logical type crashed.
Until the read restores them (ITL-1144 follow-up), such declarations are refused when the
type is declared, and a stored column holding such a None raises when it is read.
"""

import dataclasses
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

import orcapod as op
from orcapod.operators import Batch, GroupBy
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import OutputTypeError
from orcapod.sources import ArrowTableSource, DataFrameSource, DictSource, ListSource

U = uuid.UUID(int=1)
P = Path("/tmp/itl-1144")


@dataclasses.dataclass
class Point:
    x: int


@op.dataclass
class StructuralPoint:
    x: int


@dataclasses.dataclass
class OptionalList:
    x: list[int] | None


@dataclasses.dataclass
class OptionalUUID:
    x: uuid.UUID | None


@dataclasses.dataclass
class OptionalPath:
    x: Path | None


@dataclasses.dataclass
class OptionalScalars:
    x: int | None
    xs: list[int | None]
    p: Point | None


REFUSED_TYPES = [
    list[set[int] | None],
    list[list[int] | None],
    list[dict[str, int] | None],
    dict[str, list[int] | None],
    list[uuid.UUID | None],
    set[uuid.UUID | None],
    list[Path | None],
    list[list[uuid.UUID] | None],
    list[np.ndarray | None],  # pyright: ignore[reportMissingTypeArgument]  # the bare class is how a pod declares it
]

REFUSED_CLASSES = [OptionalList, OptionalUUID, OptionalPath]


def _source(value: Any, annotation: Any) -> DictSource:
    """A one-row source holding ``value`` in column ``v`` declared as ``annotation``."""
    return DictSource(
        [{"id": 1, "v": value}],
        tag_columns=["id"],
        data_schema={"id": int, "v": annotation},
        source_id="v",
        tag_only_columns=True,
    )


def _read_v(source: Any) -> tuple[Any, str]:
    """The value of column ``v`` read back, and its row's data hash."""
    [(_, data)] = list(source.iter_data())
    return data.as_dict()["v"], data.content_hash().to_string()


def _rows(stream: Any) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Every row of ``stream`` as Python values."""
    return [(tag.as_dict(), data.as_dict()) for tag, data in stream.iter_data()]


class TestRefusedAtDefinition:
    """A declared type with a None the read cannot restore is refused, naming the column."""

    @pytest.mark.parametrize("annotation", REFUSED_TYPES, ids=str)
    def test_a_source_schema_is_refused(self, annotation: Any) -> None:
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            _source(None, annotation)

    @pytest.mark.parametrize("annotation", REFUSED_TYPES, ids=str)
    def test_a_function_output_is_refused(self, annotation: Any) -> None:
        def produce(n: int) -> Any:
            return None

        produce.__annotations__["return"] = annotation
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            op.function_pod(output_keys="v")(produce)

    @pytest.mark.parametrize("annotation", REFUSED_TYPES, ids=str)
    def test_a_function_input_is_refused(self, annotation: Any) -> None:
        def consume(v: Any) -> int:
            return 0

        consume.__annotations__["v"] = annotation
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            op.function_pod(output_keys="n")(consume)

    @pytest.mark.parametrize("cls", REFUSED_CLASSES, ids=lambda c: c.__name__)
    def test_a_class_field_is_refused_naming_the_field(self, cls: type) -> None:
        with pytest.raises(
            ValueError, match=rf"(?s)Column 'v'.*{cls.__qualname__}\.x.*ITL-1144"
        ):
            _source(cls(None), cls)

    def test_the_message_says_what_to_do(self) -> None:
        with pytest.raises(ValueError, match=r"list\[int\] \| None") as info:
            _source([None], list[list[int] | None])
        assert "without '| None'" in str(info.value)


ACCEPTED = [
    (list[int | None], [None, 1]),
    (list[str | None], [None, "a"]),
    (list[list[int | None]], [[None, 1]]),
    (dict[str, int | None], {"j": 1, "k": None}),
    (set[int] | None, None),
    (list[uuid.UUID] | None, None),
    (uuid.UUID | None, None),
    (list[Point | None], [None, Point(1)]),
    (list[StructuralPoint | None], [None, StructuralPoint(1)]),
    (OptionalScalars, OptionalScalars(None, [None, 1], None)),
]


class TestAccepted:
    """A None that a read restores stays accepted: same value, same row hash."""

    @pytest.mark.parametrize(("annotation", "value"), ACCEPTED, ids=str)
    def test_round_trips(self, annotation: Any, value: Any) -> None:
        read, written_hash = _read_v(_source(value, annotation))
        assert read == value
        _, read_hash = _read_v(_source(read, annotation))
        assert read_hash == written_hash


class TestRaisedAtRead:
    """A stored None the read cannot restore raises instead of a wrong value."""

    def test_an_arrow_table_with_a_null_inner_list(self) -> None:
        table = pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "v": pa.array([[None, [1]]], pa.large_list(pa.large_list(pa.int64()))),
            }
        )
        source = ArrowTableSource(
            table, tag_columns=["id"], source_id="v", tag_only_columns=True
        )
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            _read_v(source)


def _members(annotation: Any, first: Any, second: Any) -> DictSource:
    """Two rows of group ``g=1``: column ``m`` holds ``first`` then ``second``."""
    return DictSource(
        [{"g": 1, "k": 1, "m": first}, {"g": 1, "k": 2, "m": second}],
        tag_columns=["g", "k"],
        data_schema={"g": int, "k": int, "m": annotation},
        source_id="m",
        tag_only_columns=True,
    )


OPERATORS = {
    "group_by": lambda s: GroupBy(by=["g"])(s),
    "batch": lambda s: Batch(2)(s),
}


class TestReducingOperators:
    """GroupBy and Batch list a nullable column's values: a None stays where restorable."""

    @pytest.mark.parametrize("operator", OPERATORS)
    def test_a_nullable_int_column_round_trips(self, operator: str) -> None:
        out = OPERATORS[operator](_members(int | None, None, 2))
        [(_, data)] = _rows(out)
        assert data["m"] == [None, 2]

    @pytest.mark.parametrize("operator", OPERATORS)
    @pytest.mark.parametrize(
        ("annotation", "second"),
        [
            (list[int] | None, [1]),
            (set[int] | None, {1}),
            (Path | None, P),
            (uuid.UUID | None, U),
        ],
        ids=["list", "set", "Path", "UUID"],
    )
    def test_a_nullable_container_or_logical_column_raises_at_read(
        self, operator: str, annotation: Any, second: Any
    ) -> None:
        out = OPERATORS[operator](_members(annotation, None, second))
        with pytest.raises(ValueError, match=r"(?s)Column 'm'.*ITL-1144"):
            _rows(out)

    def test_a_function_declared_for_such_a_list_is_refused(self) -> None:
        def consume(m: list[list[int] | None]) -> int:
            return 0

        with pytest.raises(ValueError, match=r"(?s)Column 'm'.*ITL-1144"):
            op.function_pod(output_keys="n")(consume)


@dataclasses.dataclass
class ListField:
    xs: list[int]


class TestNoneWhereTheTypeDoesNotAllowOne:
    """A None given at a non-optional container position is refused at write.

    It was stored as an empty container (``[]``, ``set()``, ``{}``): a silent value
    change (ITL-1144, CodeRabbit on PR #394).
    """

    @pytest.mark.parametrize(
        ("annotation", "value"),
        [
            (list[list[int]], [None, [1]]),
            (list[set[int]], [None, {1}]),
            (list[dict[str, int]], [None]),
            (dict[str, list[int]], {"k": None}),
        ],
        ids=str,
    )
    def test_a_dict_source_refuses(self, annotation: Any, value: Any) -> None:
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*does not admit None"):
            _source(value, annotation)

    def test_a_class_field_refuses(self) -> None:
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*does not admit None"):
            _source(ListField(None), ListField)  # pyright: ignore[reportArgumentType]  # the wrong value is the case under test

    def test_a_list_source_refuses(self) -> None:
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*does not admit None"):
            ListSource(
                name="v",
                data=[[None, [1]]],
                data_schema={"element_index": int, "v": list[list[int]]},
            )

    def test_a_data_frame_source_raises_at_read(self) -> None:
        # Polars keeps the null inner list; reading it to Python raises.
        source = DataFrameSource(
            {"id": [1], "v": [[None, [1]]]}, tag_columns=["id"], tag_only_columns=True
        )
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            _read_v(source)

    def test_a_function_output_refuses(self) -> None:
        def produce(n: int) -> list[list[int]]:
            return [None, [1]]  # pyright: ignore[reportReturnType]  # the wrong value is the case under test

        pod = op.function_pod(output_keys="v")(produce).pod
        stream = pod(
            DictSource(
                [{"id": 1, "n": 1}],
                tag_columns=["id"],
                source_id="n",
                tag_only_columns=True,
            )
        )
        with pytest.raises(OutputTypeError):
            list(stream.iter_data())

    def test_a_none_at_an_optional_position_is_unchanged(self) -> None:
        read, _ = _read_v(_source([None, 1], list[int | None]))
        assert read == [None, 1]


def test_a_null_map_element_still_reads_as_none() -> None:
    """A null Arrow map element is read back as None, as before (CodeRabbit, PR #394).

    A source refuses a map column (the Arrow hasher cannot hash it); a raw
    ``ArrowTableStream`` holds one as given, and its map decoder passes a null through.
    """
    map_type = pa.map_(pa.large_string(), pa.int64())
    table = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "v": pa.array([[None, [("a", 1)]]], pa.large_list(map_type)),
        }
    )
    [(_, data)] = list(ArrowTableStream(table, tag_columns=["id"]).iter_data())
    value: Any = data.as_dict()["v"]
    assert value[0] is None


class TestNoneForANonOptionalLogicalType:
    """A None given where a logical type is declared without ``| None`` is refused.

    The logical type's own encoder took it: a ``Path`` stored the string ``'None'``
    and read back ``Path('None')``; a ``list[UUID]`` stored ``[]`` (CodeRabbit round 2,
    PR #394).
    """

    def test_a_path_given_none_is_not_stored_as_the_string_none(self) -> None:
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*does not admit None"):
            _source(None, Path)

    @pytest.mark.parametrize(
        ("annotation", "value"),
        [
            (list[Path], [None]),
            (list[uuid.UUID], None),
            (set[uuid.UUID], None),
            (list[list[uuid.UUID]], [None]),
            (uuid.UUID, None),
        ],
        ids=str,
    )
    def test_a_dict_source_refuses(self, annotation: Any, value: Any) -> None:
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*does not admit None"):
            _source(value, annotation)

    @pytest.mark.parametrize(
        ("annotation", "value"),
        [
            (Path | None, None),
            (list[uuid.UUID] | None, None),
            (list[Path] | None, None),
        ],
        ids=str,
    )
    def test_an_optional_column_still_takes_none(
        self, annotation: Any, value: Any
    ) -> None:
        read, _ = _read_v(_source(value, annotation))
        assert read is None


def test_a_null_dict_entry_raises_at_read() -> None:
    """A null key/value entry of a stored dict is not dropped (CodeRabbit round 2)."""
    entries = pa.large_list(
        pa.struct([("key", pa.large_string()), ("value", pa.int64())])
    )
    table = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "v": pa.array([[None, {"key": "a", "value": 1}]], entries),
        }
    )
    source = ArrowTableSource(
        table, tag_columns=["id"], source_id="v", tag_only_columns=True
    )
    with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
        _read_v(source)


class TestConverterWithoutRegistry:
    """A converter with no logical type registry refuses the same declarations."""

    # Only element types a registry-free converter can derive; a logical type there
    # is already unsupported without a registry.
    @pytest.mark.parametrize(
        "annotation",
        [
            list[list[int] | None],
            set[list[int] | None],
            list[dict[str, int] | None],
            dict[str, list[int] | None],
        ],
        ids=str,
    )
    def test_the_type_is_refused(self, annotation: Any) -> None:
        from orcapod.semantic_types.universal_converter import UniversalTypeConverter

        converter = UniversalTypeConverter()
        with pytest.raises(ValueError, match="ITL-1144"):
            converter.python_type_to_arrow_type(annotation)


class TestSetOfAPrimitive:
    """The guard against a set of a primitive, now a native container (ITL-611).

    A set as a dict value is refused before this guard looks at it: storage cannot keep
    a native container inside a struct field (ITL-1150).
    """

    @pytest.mark.parametrize(
        "annotation",
        [
            dict[str, list[set[int] | None]],
            list[set[int] | None],
            set[uuid.UUID | None],
        ],
        ids=str,
    )
    def test_a_none_in_place_of_a_set_is_refused(self, annotation: Any) -> None:
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            _source(None, annotation)

    @pytest.mark.parametrize(
        ("annotation", "value"),
        [
            (dict[str, set[int]], {"a": {2, 1}, "b": set()}),
            (dict[str, set[int | None]], {"a": {None, 1}}),
            (dict[str, set[int] | None], {"a": None}),
            (dict[str, set[Path] | None], {"a": None}),
            (dict[str, set[Path]], {"a": {P}}),
        ],
        ids=str,
    )
    def test_a_set_as_a_dict_value_is_refused_before_the_none_guard(
        self, annotation: Any, value: Any
    ) -> None:
        """The set's extension type cannot sit in a struct field (ITL-1150)."""
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1150") as info:
            _source(value, annotation)
        assert "ITL-1144" not in str(info.value)

    @pytest.mark.parametrize(
        ("annotation", "value"),
        [
            (set[int | None], {None, 1}),
            (list[set[int]], [{1, 2}]),
            (list[set[int | None]], [{None, 1}]),
            (list[set[uuid.UUID]], [{U}]),
            (set[uuid.UUID] | None, None),
        ],
        ids=str,
    )
    def test_a_restorable_none_round_trips(self, annotation: Any, value: Any) -> None:
        read, written_hash = _read_v(_source(value, annotation))
        assert read == value
        _, read_hash = _read_v(_source(read, annotation))
        assert read_hash == written_hash
