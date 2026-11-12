"""Declared field order is part of a user class's identity (ITL-881, ITL-972).

A dataclass compares its fields as a tuple in declared order, and a pydantic model
dumps, iterates and serializes in declared order, so pod code can observe the order.
Two classes with the same fields in another order are therefore different types.

Identity is observed through the public interface: the Arrow type of a ``DictSource``
column, the row's ``content_hash`` and the source's ``content_hash`` and
``pipeline_hash``. Structural classes are compared under names ``itl972.order.``; exact
classes (ordinary dataclass, pydantic) by defining one module path and name twice.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod import function_pod
from orcapod.sources import DictSource


@op.dataclass(name="itl972.order.Data")
class DataXY:
    """Structural class ``itl972.order.Data`` with fields ``x``, ``y``."""

    x: int
    y: int


@op.dataclass(name="itl972.order.Data")
class DataYX:
    """Structural class ``itl972.order.Data`` with fields ``y``, ``x``."""

    y: int
    x: int


def _exact(kind: str, order: str) -> type:
    """Return an exact class ``Ordered`` of this module with fields in ``order``.

    Each call makes a new class object with the same module path and name, so two
    classes that differ only in field order can be compared.
    """
    fields = "".join(f"    {name}: int\n" for name in order)
    head = {
        "dataclass": "import dataclasses\n@dataclasses.dataclass\nclass Ordered:\n",
        "pydantic": "import pydantic\nclass Ordered(pydantic.BaseModel):\n",
    }[kind]
    namespace: dict[str, Any] = {"__name__": __name__}
    exec(head + fields, namespace)
    return namespace["Ordered"]


ModelXY, ModelYX = _exact("pydantic", "xy"), _exact("pydantic", "yx")
ExactXY, ExactYX = _exact("dataclass", "xy"), _exact("dataclass", "yx")


@op.dataclass(name="itl972.order.Outer")
class OuterOfXY:
    """Structural class ``itl972.order.Outer``; nests ``DataXY``."""

    inner: DataXY


@op.dataclass(name="itl972.order.Outer")
class OuterOfYX:
    """Structural class ``itl972.order.Outer``; nests ``DataYX``."""

    inner: DataYX


PAIRS: dict[str, tuple[Any, Any]] = {
    "dataclass": (DataXY(x=1, y=2), DataYX(x=1, y=2)),
    "pydantic": (ModelXY(x=1, y=2), ModelYX(x=1, y=2)),
    "exact_dataclass": (ExactXY(x=1, y=2), ExactYX(x=1, y=2)),
    "list": ([DataXY(x=1, y=2)], [DataYX(x=1, y=2)]),
    "nested": (OuterOfXY(DataXY(x=1, y=2)), OuterOfYX(DataYX(x=1, y=2))),
}


def _source(value: Any) -> DictSource:
    """Return a one-row ``DictSource`` holding ``value`` in data column ``p``."""
    return DictSource(data=[{"k": "a", "p": value}], tag_columns=["k"], source_id="s")


def _column_type(value: Any) -> pa.ExtensionType:
    """Return the Arrow extension type of the column holding ``value``."""
    column_type = _source(value).as_table().schema.field("p").type
    assert isinstance(column_type, pa.ExtensionType)
    return column_type


def _type_name(value: Any) -> str:
    """Return the Arrow extension name of the column holding ``value``."""
    return _column_type(value).extension_name


def _row_hash(value: Any) -> str:
    """Return the content hash of the row holding ``value``."""
    _, data = next(iter(_source(value).iter_data()))
    return data.content_hash().to_string()


@pytest.fixture(params=list(PAIRS))
def pair(request: pytest.FixtureRequest) -> tuple[Any, Any]:
    """Two values that differ only in the declared field order of their class."""
    return PAIRS[request.param]


def test_other_field_order_is_another_type(pair: tuple[Any, Any]) -> None:
    """The same fields in another declared order give another extension name."""
    first, second = pair
    assert _type_name(first) != _type_name(second)


def test_other_field_order_has_other_hashes(pair: tuple[Any, Any]) -> None:
    """Another field order changes the row, content and pipeline hashes."""
    first, second = pair
    assert _row_hash(first) != _row_hash(second)
    assert _source(first).content_hash() != _source(second).content_hash()
    assert _source(first).pipeline_hash() != _source(second).pipeline_hash()


@pytest.mark.parametrize(
    ("value", "names"),
    [
        (DataXY(x=1, y=2), ["x", "y"]),
        (DataYX(x=1, y=2), ["y", "x"]),
        (ModelXY(x=1, y=2), ["x", "y"]),
        (ModelYX(x=1, y=2), ["y", "x"]),
    ],
)
def test_stored_struct_lists_fields_in_declared_order(
    value: Any, names: list[str]
) -> None:
    """The stored struct lists its fields in the class's declared order."""
    storage = _column_type(value).storage_type
    assert isinstance(storage, pa.StructType)
    assert storage.names == names


def test_values_of_both_orders_read_back_as_their_own_class(
    pair: tuple[Any, Any],
) -> None:
    """A value of either order reads back equal and as its own class."""
    for value in pair:
        _, data = next(iter(_source(value).iter_data()))
        read = data.as_dict()["p"]
        assert read == value
        assert type(read) is type(value)


def test_pods_over_the_two_orders_have_different_pipeline_hashes() -> None:
    """Pods over the two orders differ in pipeline hash and output type."""

    @function_pod(output_keys="q")
    def keep_xy(p: DataXY) -> DataXY:
        """Return the ``DataXY`` value unchanged."""
        return p

    @function_pod(output_keys="q")
    def keep_yx(p: DataYX) -> DataYX:
        """Return the ``DataYX`` value unchanged."""
        return p

    xy = keep_xy.pod(_source(DataXY(x=1, y=2)))
    yx = keep_yx.pod(_source(DataYX(x=1, y=2)))
    assert xy.pipeline_hash() != yx.pipeline_hash()
    assert (
        xy.as_table().schema.field("q").type.extension_name
        != yx.as_table().schema.field("q").type.extension_name
    )


@op.dataclass(name="itl972.order.Same")
class SameFirst:
    """First structural class ``itl972.order.Same``."""

    a: int
    b: str


@op.dataclass(name="itl972.order.Same")
class SameSecond:
    """Second structural class ``itl972.order.Same``, same fields in the same order."""

    a: int
    b: str


def test_same_fields_in_the_same_order_are_one_type() -> None:
    """Two classes with one name and the same ordered fields are one type."""
    assert _type_name(SameFirst(1, "x")) == _type_name(SameSecond(1, "x"))
    assert _row_hash(SameFirst(1, "x")) == _row_hash(SameSecond(1, "x"))
