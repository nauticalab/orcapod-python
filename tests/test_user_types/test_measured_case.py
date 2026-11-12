"""Two structural classes of one type behave the same in memory and from a store (ITL-972).

The case measured on November 12, 2026: two classes of one identity, the second with a
method the first lacks. Pod A returns the first; pod B is annotated with the second.
Orcapod checks that the two classes have the same structural identity and passes the
value as it is, without converting it: in memory B receives pod A's object, from a store
the class bound to the type. A structural class promises its fields, not its methods, so
on both paths B can read the fields and the value equals the instance B would build.
"""

from __future__ import annotations

from pathlib import Path

import orcapod as op
from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource


@op.dataclass(name="itl972.measured.Q")
class QOld:
    """The first definition: no ``describe``."""

    x: int
    y: int


@op.dataclass(name="itl972.measured.Q")
class QNew:
    """The second definition: same name and fields, plus ``describe``."""

    x: int
    y: int

    def describe(self) -> str:
        """Return a text only this class can produce; a ``QOld`` has no such method."""
        return f"QNew({self.x}, {self.y})"


@function_pod(output_keys="q")
def make(x: int) -> QOld:
    """Return a ``QOld``."""
    return QOld(x, x + 1)


@function_pod(output_keys="seen")
def look(q: QNew) -> str:
    """Read the fields of the argument and compare it with a ``QNew`` built here."""
    return f"{q.x},{q.y},{q == QNew(q.x, q.y)}"


def _source() -> DictSource:
    """Return a two-row source of ``x`` values."""
    return DictSource(
        data=[{"k": "a", "x": 1}, {"k": "b", "x": 2}],
        tag_columns=["k"],
        source_id="s",
    )


EXPECTED = ["1,2,True", "2,3,True"]


def _job(tmp_path: Path) -> PipelineJob:
    """Return the two-pod job over a Delta store in ``tmp_path``, already run."""
    job = PipelineJob(name="measured", store=DeltaTableDatabase(tmp_path / "db"))
    with job:
        _ = look.pod(make.pod(_source(), label="make"), label="look")
    job.run()
    return job


def _seen(stream: object) -> list[str]:
    """Return the sorted ``seen`` values of a stream or node."""
    return sorted(data.as_dict()["seen"] for _, data in stream.iter_data())  # pyright: ignore[reportAttributeAccessIssue]  # a stream or a job node


def test_in_memory_chain_is_compatible() -> None:
    """In memory, pod B accepts pod A's output, reads its fields and finds it equal."""
    assert _seen(look.pod(make.pod(_source()))) == EXPECTED


def test_stored_chain_is_compatible(tmp_path: Path) -> None:
    """Through a Delta-backed job the result is the same."""
    assert _seen(_job(tmp_path).nodes["look"]) == EXPECTED


def test_second_run_reuses_stored_rows_with_the_same_result(tmp_path: Path) -> None:
    """A second run reads the stored rows and gives the same result."""
    assert _seen(_job(tmp_path).nodes["look"]) == EXPECTED
    assert _seen(_job(tmp_path).nodes["look"]) == EXPECTED


def test_values_are_passed_as_they_are() -> None:
    """Orcapod does not convert: in memory pod B receives pod A's own object."""

    @function_pod(output_keys="cls")
    def class_name(q: QNew) -> str:
        """Return the class name of the argument."""
        return type(q).__name__

    stream = class_name.pod(make.pod(_source()))
    assert {data.as_dict()["cls"] for _, data in stream.iter_data()} == {"QOld"}
