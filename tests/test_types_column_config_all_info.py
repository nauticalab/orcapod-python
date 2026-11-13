"""An ``all_info`` given inside a column config means the same as the keyword (ITL-1067)."""

from __future__ import annotations

from typing import Any

import pytest

from orcapod import function_pod
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources import DictSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.job import PipelineJob
from orcapod.types import ColumnConfig


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Double ``x``."""
    return 2 * x


def _source() -> DictSource:
    return DictSource(
        data=[{"k": 1, "x": 2}, {"k": 2, "x": 3}], tag_columns=["k"], source_id="s"
    )


def _make_source() -> Any:
    return _source()


def _make_pod_stream() -> Any:
    return double.pod(_source())


def _make_operator_stream() -> Any:
    return SelectDataColumns(["x"])(_source())


def _make_job_node(label: str) -> Any:
    job = PipelineJob(name="itl1067", store=InMemoryArrowDatabase())
    with job:
        doubled = double.pod(_source(), label="double")
        _ = SelectDataColumns(["y"])(doubled, label="sel")
    _ = job.run()
    return job.nodes[label]


SUBJECTS = {
    "source": _make_source,
    "pod_stream": _make_pod_stream,
    "operator_stream": _make_operator_stream,
    "function_node": lambda: _make_job_node("double"),
    "operator_node": lambda: _make_job_node("sel"),
}

EMBEDDED = {
    "dict": {"all_info": True},
    "config": ColumnConfig(all_info=True),
    "dict_with_meta_false": {"all_info": True, "meta": False},
}


@pytest.fixture(params=list(SUBJECTS))
def subject(request: pytest.FixtureRequest) -> Any:
    return SUBJECTS[request.param]()


@pytest.mark.parametrize("spelling", list(EMBEDDED))
def test_embedded_all_info_gives_the_same_table(subject: Any, spelling: str) -> None:
    expected = subject.as_table(all_info=True)
    assert expected.num_columns > subject.as_table().num_columns
    assert subject.as_table(columns=EMBEDDED[spelling]).equals(expected)


@pytest.mark.parametrize("spelling", list(EMBEDDED))
def test_embedded_all_info_gives_the_same_schema(subject: Any, spelling: str) -> None:
    expected = subject.output_schema(all_info=True)
    assert subject.output_schema(columns=EMBEDDED[spelling]) == expected


@pytest.mark.parametrize("spelling", list(EMBEDDED))
def test_embedded_all_info_gives_the_same_head(subject: Any, spelling: str) -> None:
    expected = subject.head(all_info=True).table
    assert subject.head(columns=EMBEDDED[spelling]).table.equals(expected)


def test_embedded_all_info_wins_over_a_false_keyword(subject: Any) -> None:
    expected = subject.as_table(all_info=True)
    got = subject.as_table(columns={"all_info": True}, all_info=False)
    assert got.equals(expected)


def test_handle_config_normalizes_every_group_on() -> None:
    assert ColumnConfig.handle_config({"all_info": True}) == ColumnConfig.all()
    assert ColumnConfig.handle_config(ColumnConfig(all_info=True)) == ColumnConfig.all()
    assert ColumnConfig.handle_config(None) == ColumnConfig()
