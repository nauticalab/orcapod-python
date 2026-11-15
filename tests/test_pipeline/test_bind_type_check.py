"""Binding a source whose column types differ from the slot's is refused (ITL-1112).

A source slot's declared tag and data schemas are what every downstream pipeline hash is
built from, and the pipeline hash scopes the stored table paths. A source whose columns have
other types would send rows of those types to the paths of the declared ones, so both ways
of binding refuse it: ``PipelineJob.bind(sources=...)`` and the source node's
``bound_source`` setter, on a fresh job and on a loaded one. An accepted bind never moves a
pipeline hash.
"""

from __future__ import annotations

import pathlib
from collections.abc import Callable

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.operators import MapData
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError, SourceSpecMismatchError
from orcapod.sources import DictSource
from orcapod.types import Schema


@function_pod(output_keys="z")
def const(y: int) -> int:
    """Return *y*, so stored results show which source they came from."""
    return y


def _ints(source_id: str = "ints", start: int = 1) -> DictSource:
    """Return a source with tag ``id: int`` and data ``x: int``."""
    return DictSource(
        [{"id": 0, "x": start}, {"id": 1, "x": start + 1}],
        tag_columns=["id"],
        source_id=source_id,
    )


def _strs() -> DictSource:
    """Return a source with tag ``id: int`` and data ``x: str``."""
    return DictSource(
        [{"id": 0, "x": "a"}, {"id": 1, "x": "b"}], tag_columns=["id"], source_id="strs"
    )


def _str_tags() -> DictSource:
    """Return a source with tag ``id: str`` and data ``x: int``."""
    return DictSource(
        [{"id": "a", "x": 1}, {"id": "b", "x": 2}],
        tag_columns=["id"],
        source_id="stags",
    )


def _fresh_job(tmp_path: pathlib.Path) -> PipelineJob:
    """Return a job with slot ``src`` (id: int, x: int) -> MapData -> ``const``, bound to ints."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"x": int}),
    )
    job = PipelineJob(name="j", database=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        _ = const.pod(MapData({"x": "y"})(slot, label="renamed"), label="const")
    job.bind(sources={"src": _ints()})
    return job


def _loaded_job(tmp_path: pathlib.Path) -> PipelineJob:
    """Return the fresh job, run and saved, loaded again from its file."""
    job = _fresh_job(tmp_path)
    _ = job.run()
    path = tmp_path / "job.json"
    job.save(str(path))
    return PipelineJob.load(str(path))


JOBS: dict[str, Callable[[pathlib.Path], PipelineJob]] = {
    "fresh": _fresh_job,
    "loaded": _loaded_job,
}


def _bind(job: PipelineJob, source: DictSource) -> None:
    """Bind *source* to slot ``src`` with ``PipelineJob.bind``."""
    job.bind(sources={"src": source})


def _assign(job: PipelineJob, source: DictSource) -> None:
    """Bind *source* to slot ``src`` by assigning the node's ``bound_source``."""
    job.nodes["src"].bound_source = source  # pyright: ignore[reportAttributeAccessIssue]  # nodes is typed by the base node; "src" is a SourceJobNode


BINDS: dict[str, Callable[[PipelineJob, DictSource], None]] = {
    "bind": _bind,
    "bound_source": _assign,
}


def _pipeline_hashes(job: PipelineJob) -> dict[str, str]:
    """Return every node's pipeline hash, by node label."""
    return {name: node.pipeline_hash().to_string() for name, node in job.nodes.items()}


@pytest.mark.parametrize("make_job", JOBS.values(), ids=JOBS.keys())
@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_data_column_of_another_type_is_refused(
    tmp_path: pathlib.Path,
    make_job: Callable[[pathlib.Path], PipelineJob],
    bind: Callable[[PipelineJob, DictSource], None],
) -> None:
    """A source with ``x: str`` for a slot declaring ``x: int`` is refused, naming both types."""
    job = make_job(tmp_path)
    with pytest.raises(SourceSpecMismatchError) as excinfo:
        bind(job, _strs())
    assert isinstance(excinfo.value, InputValidationError)
    message = str(excinfo.value)
    assert "'src'" in message
    assert "'x'" in message
    assert "int" in message and "str" in message


@pytest.mark.parametrize("make_job", JOBS.values(), ids=JOBS.keys())
@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_tag_column_of_another_type_is_refused(
    tmp_path: pathlib.Path,
    make_job: Callable[[pathlib.Path], PipelineJob],
    bind: Callable[[PipelineJob, DictSource], None],
) -> None:
    """A source with tag ``id: str`` for a slot declaring ``id: int`` is refused."""
    job = make_job(tmp_path)
    with pytest.raises(SourceSpecMismatchError, match="'id'"):
        bind(job, _str_tags())


@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_refused_source_leaves_the_binding_unchanged(
    tmp_path: pathlib.Path, bind: Callable[[PipelineJob, DictSource], None]
) -> None:
    """After a refusal the slot keeps its source, its schema and every pipeline hash."""
    job = _fresh_job(tmp_path)
    before_source = job.nodes["src"].bound_source  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode
    before_hashes = _pipeline_hashes(job)
    with pytest.raises(SourceSpecMismatchError):
        bind(job, _strs())
    assert job.nodes["src"].bound_source is before_source  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode
    assert dict(job.nodes["renamed"].output_schema()[1]) == {"y": int}
    assert _pipeline_hashes(job) == before_hashes


def test_a_refused_bind_leaves_the_database_unchanged(tmp_path: pathlib.Path) -> None:
    """``bind(database=..., sources=...)`` with a refused source changes nothing."""
    job = _fresh_job(tmp_path)
    database = job.database
    with pytest.raises(SourceSpecMismatchError):
        job.bind(sources={"src": _strs()}, database=InMemoryArrowDatabase())
    assert job.database is database


@pytest.mark.parametrize("make_job", JOBS.values(), ids=JOBS.keys())
@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_an_accepted_bind_moves_no_pipeline_hash(
    tmp_path: pathlib.Path,
    make_job: Callable[[pathlib.Path], PipelineJob],
    bind: Callable[[PipelineJob, DictSource], None],
) -> None:
    """Binding another source of the declared types leaves every pipeline hash as it was."""
    job = make_job(tmp_path)
    before = _pipeline_hashes(job)
    bind(job, _ints(source_id="other", start=10))
    assert _pipeline_hashes(job) == before


def test_an_accepted_rebind_runs_and_stores_the_new_rows(
    tmp_path: pathlib.Path,
) -> None:
    """A rebind to a source of the declared types runs and stores its rows."""
    job = _fresh_job(tmp_path)
    _ = job.run()
    job.bind(sources={"src": _ints(source_id="other", start=10)})
    _ = job.run()
    # Results of the first source (x 1, 2) would be [1, 2].
    assert sorted(job.nodes["const"].as_table().column("z").to_pylist()) == [10, 11]


def _two_by_two_slot_job() -> PipelineJob:
    """Return a job whose slot declares tags ``a``, ``b`` and data ``x``, ``y``, in order."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"a": int, "b": int}),
        data_schema=Schema({"x": int, "y": int}),
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "x2"})(slot, label="renamed")
    return job


def test_columns_match_by_name_within_each_group_whatever_their_order() -> None:
    """Tags and data in another order than declared are accepted (developer, November 14,
    2026): within the tag group and the data group, columns match by name."""
    job = _two_by_two_slot_job()
    reordered = DictSource(
        [{"y": 2, "b": 1, "x": 3, "a": 0}], tag_columns=["b", "a"], source_id="r"
    )
    job.bind(sources={"src": reordered})
    assert job.run().ok


def test_a_column_in_the_other_role_is_refused() -> None:
    """A column that is a tag on one side and data on the other is refused: the role
    (tag or data) is part of the match, its position is not."""
    job = _two_by_two_slot_job()
    swapped = DictSource(
        [{"a": 0, "b": 1, "x": 3, "y": 2}], tag_columns=["a", "x"], source_id="s"
    )
    with pytest.raises(SourceSpecMismatchError, match="tag columns"):
        job.bind(sources={"src": swapped})
