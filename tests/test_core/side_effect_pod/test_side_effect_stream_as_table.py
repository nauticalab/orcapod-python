"""``SideEffectPodStream.as_table`` honours every ColumnConfig (ITL-1066).

The stream is a pass-through of its input, so for every column configuration its table
must have the input stream's columns, order and types; a function pod over the same
input is the second reference. Rows reach the stream in input order; ``sort_by_tags``
sorts them as in every other stream.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.core.function_pod import FunctionPodStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.side_effects import SideEffectPod
from orcapod.sources import DictSource

CONFIGS: list[tuple[str, dict[str, Any]]] = [
    ("default", {}),
    ("meta", {"columns": {"meta": True}}),
    ("context", {"columns": {"context": True}}),
    ("source", {"columns": {"source": True}}),
    ("system_tags", {"columns": {"system_tags": True}}),
    ("content_hash", {"columns": {"content_hash": True}}),
    ("sort_by_tags", {"columns": {"sort_by_tags": True}}),
    ("all_info", {"all_info": True}),
]
IDS = [name for name, _ in CONFIGS]
KWARGS = [kwargs for _, kwargs in CONFIGS]
ORDER = [2, 0, 1]


def deliver(x: int, ctx: Any) -> None:
    return None


def double(x: int) -> int:
    return x * 2


def _source() -> DictSource:
    return DictSource(
        [{"k": f"r{i}", "x": i} for i in ORDER], tag_columns=["k"], source_id="src"
    )


def _stream() -> Any:
    return SideEffectPod(deliver)(_source())


def _signature(table: pa.Table) -> list[tuple[str, pa.DataType]]:
    return [(f.name, f.type) for f in table.schema]


@pytest.mark.parametrize("kwargs", KWARGS, ids=IDS)
def test_the_table_has_the_columns_and_types_of_its_input(
    kwargs: dict[str, Any],
) -> None:
    table = _stream().as_table(**kwargs)
    expected = _source().as_table(**kwargs)
    # A source puts its system tags last, a pod stream after the tags: the column
    # ORDER is checked against a function pod below, the names and types here.
    assert dict(_signature(table)) == dict(_signature(expected))


@pytest.mark.parametrize("kwargs", KWARGS, ids=IDS)
def test_the_rows_follow_the_input_rows(kwargs: dict[str, Any]) -> None:
    table = _stream().as_table(**kwargs)
    expected = _source().as_table(**kwargs)
    assert table.column("k").to_pylist() == expected.column("k").to_pylist()
    assert table.column("x").to_pylist() == expected.column("x").to_pylist()


@pytest.mark.parametrize("kwargs", KWARGS, ids=IDS)
def test_the_groups_agree_with_a_function_pod_over_the_same_input(
    kwargs: dict[str, Any],
) -> None:
    from orcapod import function_pod

    @function_pod(output_keys="x")
    def same(x: int) -> int:
        return x

    pod_stream = same.pod(_source())
    assert isinstance(pod_stream, FunctionPodStream)
    table = _stream().as_table(**kwargs)
    reference = pod_stream.as_table(**kwargs)
    # Tag, data and system-tag groups. Source-info and hash values differ
    # between a pass-through and a function's output, their presence must not.
    assert table.column_names == reference.column_names
    assert [f.type for f in table.schema] == [f.type for f in reference.schema]
    assert table.column("k").to_pylist() == reference.column("k").to_pylist()


def test_sort_by_tags_orders_the_rows() -> None:
    table = _stream().as_table(columns={"sort_by_tags": True})
    assert table.column("k").to_pylist() == ["r0", "r1", "r2"]
    assert table.column("x").to_pylist() == [0, 1, 2]


@pytest.mark.parametrize("sort", [False, True])
def test_each_content_hash_belongs_to_its_own_row(sort: bool) -> None:
    """After a sort every hash is still the hash of the row it sits in (ITL-1029)."""
    stream = _stream()
    config = {"content_hash": True, "sort_by_tags": sort}
    table = stream.as_table(columns=config)
    own = {
        data.as_dict()["x"]: str(data.content_hash()) for _, data in stream.iter_data()
    }
    assert table.column_names[-1] == "_content_hash"
    assert table.column("_content_hash").type == pa.large_string()
    for x, digest in zip(
        table.column("x").to_pylist(), table.column("_content_hash").to_pylist()
    ):
        assert digest == own[x]


def test_a_named_content_hash_column_is_last_under_its_name() -> None:
    table = _stream().as_table(columns={"content_hash": "digest"})
    assert table.column_names[-1] == "digest"


@pytest.mark.parametrize("name", ["_context_key", "_source_x", "_tag_digest"])
def test_a_hash_name_equal_to_another_column_group_is_kept_as_in_other_streams(
    name: str,
) -> None:
    for sort in (False, True):
        config = {"content_hash": name, "sort_by_tags": sort}
        table = _stream().as_table(columns=config)
        assert table.column_names.count(name) == 1
        assert table.num_rows == 3


def test_a_hash_name_equal_to_a_data_column_matches_the_input_stream() -> None:
    config = {"content_hash": "x"}
    try:
        expected = _source().as_table(columns=config)
    except Exception as exc:  # the input stream's behaviour is the reference
        with pytest.raises(type(exc)):
            _stream().as_table(columns=config)
    else:
        assert _stream().as_table(columns=config).column_names == (
            expected.column_names
        )


@pytest.mark.parametrize("kwargs", KWARGS, ids=IDS)
def test_the_table_of_no_rows_agrees_with_a_function_pod_over_no_rows(
    kwargs: dict[str, Any],
) -> None:
    """A pod that drops every row: same columns and types as a function pod's empty table."""
    from orcapod import function_pod
    from orcapod.core.operators import PolarsFilter
    from orcapod.side_effects import SideEffectPodConfig

    def failing(x: int, ctx: Any) -> None:
        raise RuntimeError("no")

    stream: Any = SideEffectPod(failing, config=SideEffectPodConfig(on_error="log"))(
        _source()
    )

    @function_pod(output_keys="x")
    def same(x: int) -> int:
        return x

    reference = same.pod(PolarsFilter(constraints={"x": 99})(_source()))
    table = stream.as_table(**kwargs)
    expected = reference.as_table(**kwargs)
    assert table.num_rows == 0
    assert table.column_names == expected.column_names
    assert [f.type for f in table.schema] == [f.type for f in expected.schema]


@pytest.mark.parametrize("kwargs", KWARGS, ids=IDS)
def test_the_job_node_table_agrees_for_every_config(kwargs: dict[str, Any]) -> None:
    job = PipelineJob(name="se", store=InMemoryArrowDatabase())
    with job:
        SideEffectPod(deliver)(_source(), label="node")
    job.run()
    table = job.nodes["node"].as_table(**kwargs)
    expected = _source().as_table(**kwargs)
    assert sorted(table.column_names) == sorted(expected.column_names)


def test_the_declared_schema_is_asked_for_only_when_there_are_no_rows() -> None:
    """Asking a source for its schema can poll it: rows present, it is never asked."""
    asked: list[bool] = []
    stream: Any = SideEffectPod(deliver)(_source())
    original = stream.output_schema

    def counting(**kwargs: Any) -> Any:
        asked.append(True)
        return original(**kwargs)

    stream.output_schema = counting  # the spy
    stream.as_table(all_info=True)
    assert asked == []


@pytest.mark.parametrize("kwargs", KWARGS, ids=IDS)
def test_a_content_hash_does_not_depend_on_the_groups_requested(
    kwargs: dict[str, Any],
) -> None:
    """The hash of a row is that of its data, whichever other groups are shown."""
    base = _stream().as_table(columns={"content_hash": True, "sort_by_tags": True})
    columns = {**kwargs.get("columns", {}), "content_hash": True, "sort_by_tags": True}
    table = _stream().as_table(columns=columns, all_info=kwargs.get("all_info", False))
    assert table.column("_content_hash").to_pylist() == (
        base.column("_content_hash").to_pylist()
    )


def test_the_keys_are_asked_for_only_after_the_rows_were_read() -> None:
    """Asking a source for its keys can poll it: never before the rows are delivered."""
    events: list[str] = []

    def recording(x: int, ctx: Any) -> None:
        events.append("row")

    stream: Any = SideEffectPod(recording)(_source())
    original = stream.keys

    def counting(**kwargs: Any) -> Any:
        events.append("keys")
        return original(**kwargs)

    stream.keys = counting  # the spy
    stream.as_table()
    assert events.index("keys") > max(i for i, e in enumerate(events) if e == "row")
