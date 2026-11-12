"""An ordinary dataclass or pydantic model is identified by its exact class (ITL-972).

Two class objects are one type only when they are the same object. A class defined again
under the same module path and name with the same fields (a re-run notebook cell) is a new
class, and a pod built for the earlier one refuses it. There is no way to declare a name
that would make two classes interchangeable.
"""

from __future__ import annotations

import dataclasses
import importlib
from pathlib import Path
from typing import Any

import pytest

import orcapod as op
from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource

DATACLASS_SOURCE = """
import dataclasses


@dataclasses.dataclass
class Reading:
    '''A value defined twice by ``_define`` (same module path, name and fields).'''
    x: int
    y: int
"""

PYDANTIC_SOURCE = """
import pydantic


class Reading(pydantic.BaseModel):
    '''A model defined twice by ``_define`` (same module path, name and fields).'''
    x: int
    y: int
"""

KINDS = {"dataclass": DATACLASS_SOURCE, "pydantic": PYDANTIC_SOURCE}


def _define(source: str) -> type:
    """Execute ``source`` as a cell of this module and return its ``Reading`` class.

    Each call makes a new class object with this module's path and the name ``Reading``,
    as re-running a notebook cell does.
    """
    namespace: dict[str, Any] = {"__name__": __name__}
    exec(source, namespace)
    return namespace["Reading"]


def _numbers() -> DictSource:
    """Return a one-row source holding ``1`` in data column ``x``."""
    return DictSource(data=[{"k": "a", "x": 1}], tag_columns=["k"], source_id="n")


def _source(value: Any) -> DictSource:
    """Return a one-row source holding ``value`` in data column ``p``."""
    return DictSource(data=[{"k": "a", "p": value}], tag_columns=["k"], source_id="s")


def _sum_pod(cls: type) -> Any:
    """Return a pod ``total(p: cls) -> int`` that adds the two fields."""

    def total(p):
        """Return ``p.x + p.y``."""
        return p.x + p.y

    total.__annotations__ = {"p": cls, "return": int}
    return function_pod(output_keys="t")(total)


def _make_pod(cls: type) -> Any:
    """Return a pod ``make(x: int) -> cls`` that builds an instance."""

    def make(x):
        """Return ``cls(x, x + 1)``."""
        return cls(x=x, y=x + 1)

    make.__annotations__ = {"x": int, "return": cls}
    return function_pod(output_keys="q")(make)


def _receive_pod(cls: type) -> Any:
    """Return a pod ``receive(q: cls) -> int`` that adds the two fields."""

    def receive(q):
        """Return ``q.x + q.y``."""
        return q.x + q.y

    receive.__annotations__ = {"q": cls, "return": int}
    return function_pod(output_keys="t")(receive)


# ---------------------------------------------------------------------------
# No declared names
# ---------------------------------------------------------------------------


def test_register_type_is_not_part_of_the_public_api() -> None:
    """``op.register_type`` and ``orcapod.user_types`` no longer exist."""
    assert not hasattr(op, "register_type")
    assert "register_type" not in op.__all__
    with pytest.raises(ModuleNotFoundError):
        _ = importlib.import_module("orcapod.user_types")


# ---------------------------------------------------------------------------
# The schema check uses the class object
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_pod_accepts_its_own_class(kind: str) -> None:
    """A pod annotated with a class accepts a column of that same class object."""
    cls = _define(KINDS[kind])
    result = _sum_pod(cls).pod(_source(cls(x=1, y=2)))
    assert [data.as_dict()["t"] for _, data in result.iter_data()] == [3]


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_pod_refuses_a_class_defined_again(kind: str) -> None:
    """A pod built for a class refuses the class defined again with the same fields."""
    first = _define(KINDS[kind])
    pod = _sum_pod(first)
    second = _define(KINDS[kind])
    assert second is not first

    with pytest.raises(ValueError, match="column 'p'"):
        _ = pod.pod(_source(second(x=1, y=2)))


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_class_defined_again_can_be_used_on_its_own(kind: str) -> None:
    """Defining a class again does not stop either class from being used."""
    first = _define(KINDS[kind])
    before = _sum_pod(first).pod(_source(first(x=1, y=2)))
    second = _define(KINDS[kind])
    after = _sum_pod(second).pod(_source(second(x=3, y=4)))

    assert [data.as_dict()["t"] for _, data in after.iter_data()] == [7]
    assert [data.as_dict()["t"] for _, data in before.iter_data()] == [3]


# ---------------------------------------------------------------------------
# The case measured on November 12, 2026: the same answer in memory and from a store
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_chain_of_two_classes_is_refused_in_memory(kind: str) -> None:
    """Pod A returns the first class; pod B, annotated with the second, refuses it."""
    first = _define(KINDS[kind])
    second = _define(KINDS[kind])
    produced = _make_pod(first).pod(_numbers())

    with pytest.raises(ValueError, match="column 'q'"):
        _ = _receive_pod(second).pod(produced)


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_chain_of_two_classes_is_refused_through_a_store(
    kind: str, tmp_path: Path
) -> None:
    """The same chain in a Delta-backed job is refused as well, before anything runs."""
    first = _define(KINDS[kind])
    second = _define(KINDS[kind])
    job = PipelineJob(name="exact", store=DeltaTableDatabase(tmp_path / "db"))

    with pytest.raises(ValueError, match="column 'q'"):
        with job:
            produced = _make_pod(first).pod(_numbers(), label="make")
            _ = _receive_pod(second).pod(produced, label="receive")


# ---------------------------------------------------------------------------
# Subclasses: Python's subtype rule at a pod input, own identity in storage
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Base:
    """An ordinary dataclass."""

    x: int


@dataclasses.dataclass
class Child(Base):
    """A subclass with a field of its own."""

    z: int = 0


@function_pod(output_keys="seen")
def describe_base(p: Base) -> str:
    """Return the class name of the argument and its ``x``."""
    return f"{type(p).__name__}:{p.x}"


@function_pod(output_keys="q")
def child_as_base(x: int) -> Base:
    """Return a ``Child`` although the pod is annotated ``-> Base``."""
    return Child(x, 9)


def test_subclass_column_is_accepted_at_a_base_class_parameter() -> None:
    """A pod annotated with the base class accepts a column of the subclass."""
    source = _source(Child(1, 7))
    assert source.output_schema()[1]["p"] is Child
    assert source.as_table().schema.field("p").type.storage_type.names == ["x", "z"]

    result = describe_base.pod(source)
    assert [data.as_dict()["seen"] for _, data in result.iter_data()] == ["Child:1"]


def test_base_class_column_is_refused_at_a_subclass_parameter() -> None:
    """The subtype rule has one direction: a base column is not a subclass column."""
    with pytest.raises(ValueError, match="column 'p'"):
        _ = _sum_pod(Child).pod(_source(Base(1)))


def test_subclass_and_base_are_different_types() -> None:
    """A subclass has its own identifier and pipeline hash."""
    base, child = _source(Base(1)), _source(Child(1))
    assert (
        base.as_table().schema.field("p").type.extension_name
        != child.as_table().schema.field("p").type.extension_name
    )
    assert base.pipeline_hash() != child.pipeline_hash()


def test_subclass_output_uses_declared_base_storage() -> None:
    """The declared logical type stores only base fields for a subclass output."""
    table = child_as_base.pod(_numbers()).as_table()
    assert table.schema.field("q").type.storage_type.names == ["x"]


def test_in_memory_iteration_hands_over_the_returned_object() -> None:
    """A compatible subclass remains its own class in memory."""
    values = [
        data.as_dict()["q"] for _, data in child_as_base.pod(_numbers()).iter_data()
    ]
    assert [type(value) for value in values] == [Child]


def test_a_subclass_is_returned_by_declaring_it() -> None:
    """Declaring ``-> Child`` keeps all fields, and a ``Base`` parameter accepts the column."""

    @function_pod(output_keys="p")
    def make_child(x: int) -> Child:
        """Return a ``Child``."""
        return Child(x, 9)

    produced = make_child.pod(_numbers())
    assert produced.as_table().column("p").to_pylist() == [{"x": 1, "z": 9}]
    seen = describe_base.pod(produced)
    assert [data.as_dict()["seen"] for _, data in seen.iter_data()] == ["Child:1"]
