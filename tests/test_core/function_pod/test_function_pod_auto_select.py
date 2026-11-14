"""Calling a function pod drops the data columns its function does not take (ITL-807, ITL-193).

``pod(a, b)`` is the convenient call: the inputs are joined as before and, when the joined
stream carries data columns that are not parameters of the function, one automatic
``SelectDataColumns`` is placed between the join and the pod. ``pod(a, b, strict=True)`` and
``pod.process(a, b)`` are exact: an extra column is an error.

Every test drives the public interface.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest

from orcapod import FunctionPod, PipelineJob, function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.data_function import DataFunctionWrapper
from orcapod.core.function_pod import WrappedFunctionPod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.invocation import InvocationContext
from orcapod.operators import Join, SelectDataColumns
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import (
    PythonFunctionExecutorProtocol,
    StreamProtocol,
)
from orcapod.side_effects import SideEffectPod
from orcapod.sources import DictSource

_POD_LOGGER = "orcapod.core.function_pod"


@function_pod(output_keys="total")
def add(x: int, y: int) -> int:
    """Add two integers.

    Args:
        x: First addend.
        y: Second addend.

    Returns:
        The sum.
    """
    return x + y


@function_pod(output_keys="total")
def add_default(x: int, y: int = 10) -> int:
    """Add two integers; ``y`` has a default.

    Args:
        x: First addend.
        y: Second addend.

    Returns:
        The sum.
    """
    return x + y


def _plain_add(x: int, y: int) -> int:
    """Add two integers.

    Args:
        x: First addend.
        y: Second addend.

    Returns:
        The sum.
    """
    return x + y


def _wide(extra: int = 9, source_id: str = "wide") -> DictSource:
    """Return a source with the columns of ``add`` plus one the function does not take.

    Args:
        extra: Value of the unused column.
        source_id: Name of the source.

    Returns:
        A two-row source tagged by ``k``.
    """
    return DictSource(
        [
            {"k": 1, "x": 2, "y": 3, "extra": extra},
            {"k": 2, "x": 4, "y": 5, "extra": extra},
        ],
        tag_columns=["k"],
        source_id=source_id,
    )


def _xs(source_id: str = "xs", **more: int) -> DictSource:
    """Return a source carrying ``x`` (and any further constant columns).

    Args:
        source_id: Name of the source.
        **more: Further data columns, one constant value each.

    Returns:
        A two-row source tagged by ``k``.
    """
    return DictSource(
        [{"k": 1, "x": 2, **more}, {"k": 2, "x": 4, **more}],
        tag_columns=["k"],
        source_id=source_id,
    )


def _ys(source_id: str = "ys", **more: int) -> DictSource:
    """Return a source carrying ``y`` (and any further constant columns).

    Args:
        source_id: Name of the source.
        **more: Further data columns, one constant value each.

    Returns:
        A two-row source tagged by ``k``.
    """
    return DictSource(
        [{"k": 1, "y": 3, **more}, {"k": 2, "y": 5, **more}],
        tag_columns=["k"],
        source_id=source_id,
    )


def _rows(stream: StreamProtocol) -> list[dict[str, Any]]:
    """Return a stream's rows with every column group, ordered by tag.

    Args:
        stream: The stream to read.

    Returns:
        One dict per row, including system tags, source info and content hashes.
    """
    return stream.as_table(all_info=True).sort_by("k").to_pylist()


def _without_source_info(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return rows without their source-info columns.

    Args:
        rows: Rows as returned by ``_rows``.

    Returns:
        The same rows, minus every column whose name starts with ``_source_``.
    """
    return [
        {name: value for name, value in row.items() if not name.startswith("_source_")}
        for row in rows
    ]


def _assert_same_stream(actual: StreamProtocol, expected: StreamProtocol) -> None:
    """Assert two streams have the same identity, schema and rows.

    Args:
        actual: The stream under test.
        expected: The stream it must equal.
    """
    assert actual.pipeline_hash() == expected.pipeline_hash()
    assert actual.content_hash() == expected.content_hash()
    assert actual.output_schema(all_info=True) == expected.output_schema(all_info=True)
    assert actual.as_table(all_info=True).schema == (
        expected.as_table(all_info=True).schema
    )
    # An output's source info names the computation that produced the row, which is
    # new on every in-memory run; every other column must be equal.
    assert _without_source_info(_rows(actual)) == _without_source_info(_rows(expected))


def _drop_messages(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Return the records the function pod module logged at INFO or above.

    Args:
        caplog: The pytest log-capture fixture of the running test.

    Returns:
        The matching records, in emission order.
    """
    return [
        record
        for record in caplog.records
        if record.name == _POD_LOGGER and record.levelno >= logging.INFO
    ]


# ---------------------------------------------------------------------------
# The convenient call
# ---------------------------------------------------------------------------


def test_call_drops_unused_columns_like_a_hand_written_select() -> None:
    """``pod(wide)`` is the pipeline ``pod(SelectDataColumns([...])(wide))``."""
    automatic = add.pod(_wide())
    by_hand = add.pod.process(SelectDataColumns(columns=["x", "y"])(_wide()))

    _assert_same_stream(automatic, by_hand)
    assert [row["total"] for row in _rows(automatic)] == [5, 9]


def test_call_selects_once_after_the_join() -> None:
    """With several inputs the select comes after the join, as written by hand."""
    automatic = add.pod(_xs(a=1), _ys(b=2))
    by_hand = add.pod.process(
        SelectDataColumns(columns=["x", "y"])(Join()(_xs(a=1), _ys(b=2)))
    )

    _assert_same_stream(automatic, by_hand)
    assert [row["total"] for row in _rows(automatic)] == [5, 9]


def test_tags_are_untouched() -> None:
    """Only data columns are dropped; every tag column stays."""
    source = DictSource(
        [{"k": 1, "session": "a", "x": 2, "y": 3, "extra": 9}],
        tag_columns=["k", "session"],
        source_id="two_tags",
    )

    tag_keys, data_keys = add.pod(source).keys()

    assert tuple(tag_keys) == ("k", "session")
    assert tuple(data_keys) == ("total",)


def test_stream_passed_for_its_tags_restricts_the_rows() -> None:
    """A stream whose data columns are all unused is joined, then its columns are dropped."""
    full = DictSource(
        [{"k": 1, "x": 2, "y": 3}, {"k": 2, "x": 4, "y": 5}],
        tag_columns=["k"],
        source_id="full",
    )
    chosen = DictSource([{"k": 2, "note": 0}], tag_columns=["k"], source_id="chosen")

    result = add.pod(full, chosen)

    assert [(row["k"], row["total"]) for row in _rows(result)] == [(2, 9)]


# ---------------------------------------------------------------------------
# Nothing to drop: no step is added
# ---------------------------------------------------------------------------


# The hashes ``add.pod(_xs(), _ys())`` had on main before ITL-807 (commit 25650c42).
_JOINED_PIPELINE_HASH = (
    "3202203f55b6da5b8a255f526d2a2e04792e88bde45b48588df7c31bd7c98b17"
)
_JOINED_CONTENT_HASH = (
    "07a4093d77ff0f2d0267123f8403b860a4ce332857f84e02dbf8f35ed6492f46"
)


def test_matching_inputs_build_the_pipeline_they_built_before() -> None:
    """With nothing to drop, all three calls give the stream ``pod(a, b)`` gave before."""
    convenient = add.pod(_xs(), _ys())
    strict = add.pod(_xs(), _ys(), strict=True)
    exact = add.pod.process(_xs(), _ys())

    _assert_same_stream(convenient, exact)
    _assert_same_stream(strict, exact)
    assert convenient.pipeline_hash().to_hex() == _JOINED_PIPELINE_HASH
    assert convenient.content_hash().to_hex() == _JOINED_CONTENT_HASH


def test_absent_defaulted_parameter_adds_no_step(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A stream lacking only a parameter that has a default already matches."""
    with caplog.at_level(logging.DEBUG):
        convenient = add_default.pod(_xs())
    exact = add_default.pod.process(_xs())

    _assert_same_stream(convenient, exact)
    assert [row["total"] for row in _rows(convenient)] == [12, 14]
    assert _drop_messages(caplog) == []


def test_matching_input_adds_no_select_node_to_a_job() -> None:
    """Inside a job, a matching input records the source and the function only."""
    job = PipelineJob(name="narrow", database=InMemoryArrowDatabase())
    with job:
        add.pod(SelectDataColumns(columns=["x", "y"])(_wide()), label="add")
    by_hand = sorted(type(node).__name__ for node in job.nodes.values())

    job = PipelineJob(name="narrow", database=InMemoryArrowDatabase())
    with job:
        narrow = DictSource(
            [{"k": 1, "x": 2, "y": 3}], tag_columns=["k"], source_id="narrow"
        )
        add.pod(narrow, label="add")

    assert sorted(type(node).__name__ for node in job.nodes.values()) == [
        "FunctionJobNode",
        "SourceJobNode",
    ]
    assert by_hand == ["FunctionJobNode", "OperatorJobNode", "SourceJobNode"]


# ---------------------------------------------------------------------------
# The exact calls
# ---------------------------------------------------------------------------


def test_strict_call_rejects_an_extra_column() -> None:
    """``strict=True`` raises the error ``process`` raises."""
    with pytest.raises(ValueError, match="column 'extra' is not a parameter"):
        add.pod(_wide(), strict=True)


def test_process_rejects_an_extra_column() -> None:
    """``process`` is the exact primitive: it never drops a column."""
    with pytest.raises(ValueError, match="column 'extra' is not a parameter"):
        add.pod.process(_wide())


def test_process_takes_no_strict_argument() -> None:
    """``process`` has no ``strict`` argument: it is always strict."""
    with pytest.raises(TypeError):
        add.pod.process(_wide(), strict=False)  # pyright: ignore[reportCallIssue]


@pytest.mark.parametrize("mode", ["convenient", "strict", "process"])
def test_missing_required_column_is_an_error_in_every_mode(mode: str) -> None:
    """A parameter without a default must have a column, however the pod is applied.

    Args:
        mode: Which of the three calls is made.
    """
    with pytest.raises(ValueError, match="'y'"):
        if mode == "convenient":
            add.pod(_xs(extra=1))
        elif mode == "strict":
            add.pod(_xs(), strict=True)
        else:
            add.pod.process(_xs())


def test_missing_required_column_adds_no_step_to_a_job() -> None:
    """The missing-column error is raised before a select is recorded."""
    job = PipelineJob(name="missing", database=InMemoryArrowDatabase())
    with job:
        with pytest.raises(ValueError, match="'y'"):
            add.pod(_xs(extra=1))

    assert dict(job.nodes) == {}


def test_input_with_no_parameter_column_is_rejected() -> None:
    """A stream cannot be left without data columns: the exact error is raised."""
    only_extra = DictSource(
        [{"k": 1, "extra": 9}], tag_columns=["k"], source_id="only_extra"
    )

    with pytest.raises(ValueError, match="column 'extra' is not a parameter"):
        add_default.pod(only_extra)


@pytest.mark.parametrize("shared", ["x", "extra"])
def test_data_column_shared_by_two_inputs_fails_at_the_join(shared: str) -> None:
    """Two inputs carrying the same data column fail at the join, used or not.

    Args:
        shared: The data column both inputs carry.
    """
    left = DictSource(
        [{"k": 1, "x": 2, "extra": 1}], tag_columns=["k"], source_id="left"
    )
    right = DictSource(
        [{"k": 1, "y": 3, shared: 7}], tag_columns=["k"], source_id="right"
    )

    with pytest.raises(InputValidationError, match="overlapping"):
        add.pod(left, right)


# ---------------------------------------------------------------------------
# The info message
# ---------------------------------------------------------------------------


def test_dropping_logs_one_info_message_naming_the_columns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One INFO record per call names the dropped columns; nothing is a warning."""
    with caplog.at_level(logging.DEBUG):
        add.pod(_xs(a=1), _ys(b=2))

    records = _drop_messages(caplog)
    assert [record.levelno for record in records] == [logging.INFO]
    assert "'a'" in records[0].getMessage()
    assert "'b'" in records[0].getMessage()
    assert "'x'" not in records[0].getMessage()


def test_nothing_dropped_logs_nothing(caplog: pytest.LogCaptureFixture) -> None:
    """A call that drops nothing logs nothing at INFO or above."""
    with caplog.at_level(logging.DEBUG):
        add.pod(_xs(), _ys())

    assert _drop_messages(caplog) == []


def test_misspelt_column_for_a_defaulted_parameter_is_dropped_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A misspelt column is dropped and the default used; the INFO record is the trace."""
    misspelt = DictSource(
        [{"k": 1, "x": 2, "Y": 3}], tag_columns=["k"], source_id="misspelt"
    )

    with caplog.at_level(logging.DEBUG):
        result = add_default.pod(misspelt)

    assert [row["total"] for row in _rows(result)] == [12]
    records = _drop_messages(caplog)
    assert len(records) == 1
    assert "'Y'" in records[0].getMessage()
    with pytest.raises(ValueError, match="column 'Y' is not a parameter"):
        add_default.pod(misspelt, strict=True)


# ---------------------------------------------------------------------------
# Every kind of function pod
# ---------------------------------------------------------------------------


def test_function_with_invocation_context_drops_unused_columns() -> None:
    """The context argument is not a data column and is never selected."""

    @function_pod(output_keys="total", ctx_arg="ctx")
    def add_with_ctx(x: int, y: int, ctx: InvocationContext) -> int:
        """Add two integers, ignoring the invocation context.

        Args:
            x: First addend.
            y: Second addend.
            ctx: The invocation context.

        Returns:
            The sum.
        """
        return x + y

    automatic = add_with_ctx.pod(_wide())
    by_hand = add_with_ctx.pod.process(SelectDataColumns(columns=["x", "y"])(_wide()))

    _assert_same_stream(automatic, by_hand)
    with pytest.raises(ValueError, match="column 'extra' is not a parameter"):
        add_with_ctx.pod(_wide(), strict=True)


def test_cached_function_pod_drops_unused_columns() -> None:
    """A pod-cached function pod has the same convenient and strict calls."""
    cached = CachedFunctionPod(
        function_pod=FunctionPod.from_fn(_plain_add, output_keys="total"),
        result_database=InMemoryArrowDatabase(),
    )

    automatic = cached(_wide())
    by_hand = cached.process(SelectDataColumns(columns=["x", "y"])(_wide()))

    # Rows served from the pod cache carry other meta columns than computed rows, so
    # the two tables are compared on identity and on their user columns.
    assert automatic.pipeline_hash() == by_hand.pipeline_hash()
    assert automatic.content_hash() == by_hand.content_hash()
    assert automatic.output_schema() == by_hand.output_schema()
    assert [(row["k"], row["total"]) for row in _rows(by_hand)] == [(1, 5), (2, 9)]
    assert [row["total"] for row in _rows(automatic)] == [5, 9]
    with pytest.raises(ValueError, match="column 'extra' is not a parameter"):
        cached(_wide(), strict=True)
    with pytest.raises(ValueError, match="column 'extra' is not a parameter"):
        cached.process(_wide())


def test_decorator_cached_pod_drops_unused_columns() -> None:
    """The ``.pod`` of a function decorated with a pod cache database does the same."""

    @function_pod(output_keys="total", pod_cache_database=InMemoryArrowDatabase())
    def cached_add(x: int, y: int) -> int:
        """Add two integers.

        Args:
            x: First addend.
            y: Second addend.

        Returns:
            The sum.
        """
        return x + y

    assert [row["total"] for row in _rows(cached_add.pod(_wide()))] == [5, 9]


def test_wrapped_function_pod_drops_unused_columns() -> None:
    """A wrapper around a function pod has the same convenient and strict calls."""
    wrapped = WrappedFunctionPod(FunctionPod.from_fn(_plain_add, output_keys="total"))

    automatic = wrapped(_wide())
    by_hand = wrapped.process(SelectDataColumns(columns=["x", "y"])(_wide()))

    _assert_same_stream(automatic, by_hand)
    with pytest.raises(ValueError, match="column 'extra' is not a parameter"):
        wrapped(_wide(), strict=True)


def test_function_taking_arbitrary_keywords_gets_no_select() -> None:
    """No select is added for a function that accepts arbitrary keyword arguments.

    No shipped data function accepts them (``PythonDataFunction`` rejects ``**kwargs``),
    so this test is about the ``accepts_arbitrary_keywords`` property the pod reads: a
    data function reporting ``True`` is applied exactly as ``process`` applies it.
    """

    class TakesAnything(DataFunctionWrapper[PythonFunctionExecutorProtocol]):
        """A data function that reports it accepts arbitrary keywords."""

        @property
        def accepts_arbitrary_keywords(self) -> bool:
            """Report that any keyword is accepted."""
            return True

    exact_pod = FunctionPod.from_fn(_plain_add, output_keys="total")
    pod = FunctionPod(data_function=TakesAnything(exact_pod.data_function))

    assert add.pod.data_function.accepts_arbitrary_keywords is False
    with pytest.raises(ValueError, match="column 'extra' is not a parameter"):
        pod(_wide())


def test_other_pods_take_no_strict_argument() -> None:
    """Operators and side-effect pods keep ``__call__`` as an alias of ``process``."""

    def record(x: int, y: int, extra: int, ctx: InvocationContext) -> None:
        """Do nothing with a row.

        Args:
            x: A value.
            y: A value.
            extra: A value.
            ctx: The invocation context.
        """

    pod = SideEffectPod(record)

    with pytest.raises(TypeError):
        pod(_wide(), strict=False)  # pyright: ignore[reportCallIssue]
    with pytest.raises(TypeError):
        Join()(_xs(), _ys(), strict=False)
    assert tuple(pod(_wide()).keys()[1]) == ("x", "y", "extra")


# ---------------------------------------------------------------------------
# Inside a PipelineJob: tracker, stored tables, saved file
# ---------------------------------------------------------------------------


def _counting_pod(calls: list[int]) -> Any:
    """Return a decorated ``add`` function that records each call.

    Args:
        calls: Receives the ``x`` of every invocation.

    Returns:
        The decorated function; its ``pod`` attribute is the function pod.
    """

    @function_pod(output_keys="total", function_name="counted_add")
    def counted_add(x: int, y: int) -> int:
        """Add two integers and record the call.

        Args:
            x: First addend.
            y: Second addend.

        Returns:
            The sum.
        """
        calls.append(x)
        return x + y

    return counted_add


def _saved_pipeline(job: PipelineJob, path: Path) -> dict[str, Any]:
    """Save a job and return the saved file without its run id.

    Args:
        job: The job to save.
        path: Where to write the file.

    Returns:
        The saved document, with ``run.run_id`` removed.
    """
    job.save(path)
    document = json.loads(path.read_text())
    document["run"].pop("run_id")
    return document


def test_job_equals_the_job_with_a_hand_written_select(tmp_path: Path) -> None:
    """Graph, hashes, stored tables and saved file equal the hand-written pipeline."""
    calls: list[int] = []
    counted = _counting_pod(calls)
    db = InMemoryArrowDatabase()

    automatic = PipelineJob(name="job", database=db)
    with automatic:
        counted.pod(_wide(), label="add")
    automatic.run()
    automatic_paths = sorted(db.list_table_paths())
    assert len(calls) == 2

    by_hand = PipelineJob(name="job", database=db)
    with by_hand:
        counted.pod.process(SelectDataColumns(columns=["x", "y"])(_wide()), label="add")
    by_hand.run()

    assert len(calls) == 2  # every row came from the tables the automatic job wrote
    assert sorted(db.list_table_paths()) == automatic_paths
    assert {label: type(node).__name__ for label, node in automatic.nodes.items()} == {
        "wide": "SourceJobNode",
        "SelectDataColumns": "OperatorJobNode",
        "add": "FunctionJobNode",
    }
    for label, node in by_hand.nodes.items():
        assert automatic.nodes[label].pipeline_hash() == node.pipeline_hash()
        assert automatic.nodes[label].content_hash() == node.content_hash()
    assert _rows(automatic.nodes["add"]) == _rows(by_hand.nodes["add"])
    assert _saved_pipeline(automatic, tmp_path / "automatic.json") == (
        _saved_pipeline(by_hand, tmp_path / "by_hand.json")
    )


def test_job_with_two_inputs_equals_hand_written_join_and_select() -> None:
    """With two inputs the job has the join, then one select, then the function."""
    calls: list[int] = []
    counted = _counting_pod(calls)
    db = InMemoryArrowDatabase()

    automatic = PipelineJob(name="job", database=db)
    with automatic:
        counted.pod(_xs(a=1), _ys(b=2), label="add")
    automatic.run()

    by_hand = PipelineJob(name="job", database=db)
    with by_hand:
        joined = Join()(_xs(a=1), _ys(b=2))
        counted.pod.process(SelectDataColumns(columns=["x", "y"])(joined), label="add")
    by_hand.run()

    assert len(calls) == 2
    assert sorted(automatic.nodes) == sorted(by_hand.nodes)
    assert sorted(type(node).__name__ for node in automatic.nodes.values()) == [
        "FunctionJobNode",
        "OperatorJobNode",
        "OperatorJobNode",
        "SourceJobNode",
        "SourceJobNode",
    ]
    for label, node in by_hand.nodes.items():
        assert automatic.nodes[label].pipeline_hash() == node.pipeline_hash()
        assert automatic.nodes[label].content_hash() == node.content_hash()
    assert _rows(automatic.nodes["add"]) == _rows(by_hand.nodes["add"])


def test_change_in_an_unused_column_is_a_cache_hit() -> None:
    """A different value in a dropped column computes nothing again."""
    calls: list[int] = []
    counted = _counting_pod(calls)
    db = InMemoryArrowDatabase()

    first = PipelineJob(name="job", database=db)
    with first:
        counted.pod(_wide(extra=9), label="add")
    first.run()
    assert len(calls) == 2

    second = PipelineJob(name="job", database=db)
    with second:
        counted.pod(_wide(extra=100), label="add")
    second.run()

    assert len(calls) == 2
    assert first.nodes["add"].pipeline_hash() == second.nodes["add"].pipeline_hash()
    assert [row["total"] for row in _rows(second.nodes["add"])] == [5, 9]


def test_sync_and_async_runs_give_the_same_rows() -> None:
    """The automatic select is graph construction: both orchestrators run the same graph."""
    tables = []
    for orchestrator in (SyncPipelineOrchestrator(), AsyncPipelineOrchestrator()):
        job = PipelineJob(name="job", database=InMemoryArrowDatabase())
        with job:
            add.pod(_xs(a=1), _ys(b=2), label="add")
        job.run(orchestrator=orchestrator)
        tables.append(job.nodes["add"].as_table(all_info=True).sort_by("k"))

    assert tables[0].schema == tables[1].schema
    # A source-info token names the result record, which each run makes anew.
    source_info = [c for c in tables[0].column_names if c.startswith("_source_")]
    assert source_info == ["_source_total"]
    assert (
        tables[0].drop(source_info).to_pylist()
        == tables[1].drop(source_info).to_pylist()
    )
    assert [row["total"] for row in tables[0].to_pylist()] == [5, 9]


# ---------------------------------------------------------------------------
# The stream's convenience methods build the same operators (review of PR #327)
# ---------------------------------------------------------------------------


def test_call_equals_the_stream_convenience_methods() -> None:
    """``pod(wide)`` is ``pod(wide.select_data_columns([...]))``; ``a.join(b)`` is ``Join()``."""
    automatic = add.pod(_wide())
    by_method = add.pod(_wide().select_data_columns(["x", "y"]))
    _assert_same_stream(automatic, by_method)
    assert automatic.pipeline_hash() == by_method.pipeline_hash()
    assert automatic.content_hash() == by_method.content_hash()

    automatic_two = add.pod(_xs(a=1), _ys(b=2))
    by_method_two = add.pod(_xs(a=1).join(_ys(b=2)).select_data_columns(["x", "y"]))
    _assert_same_stream(automatic_two, by_method_two)
    assert automatic_two.pipeline_hash() == by_method_two.pipeline_hash()
    assert automatic_two.content_hash() == by_method_two.content_hash()


def test_job_equals_the_job_built_with_convenience_methods() -> None:
    """The job nodes and their hashes equal those of the stream-method pipeline."""
    calls: list[int] = []
    counted = _counting_pod(calls)
    db = InMemoryArrowDatabase()

    automatic = PipelineJob(name="job", database=db)
    with automatic:
        counted.pod(_xs(a=1), _ys(b=2), label="add")
    automatic.run()

    by_method = PipelineJob(name="job", database=db)
    with by_method:
        joined = _xs(a=1).join(_ys(b=2))
        counted.pod(joined.select_data_columns(["x", "y"]), label="add")
    by_method.run()

    assert len(calls) == 2
    assert sorted(automatic.nodes) == sorted(by_method.nodes)
    for label, node in by_method.nodes.items():
        assert type(automatic.nodes[label]) is type(node)
        assert automatic.nodes[label].pipeline_hash() == node.pipeline_hash()
        assert automatic.nodes[label].content_hash() == node.content_hash()
