"""Graph walks over stacked split-and-join stages are linear in the nodes (ITL-1111).

Each stage splits a stream into two branches and joins them again, so every
stage is a diamond over the previous one. A walk that asks a shared upstream
once per path visits the source 2**stages times or more; a walk that keeps
what it has computed asks each node a bounded number of times.

The tests count calls of the schema queries (``output_schema`` and ``keys``)
and of the other graph walks (``__repr__``, ``_schema_requires_io``) on every
stream, source and node class, per object, never wall-clock time. A
counter stops the walk with ``WalkBudgetExceeded`` as soon as the total passes
a budget linear in the node count, so an exponential walk fails fast.

Walks over the system tags (``all_info``, a run) are tested at fewer stages:
every join keeps both inputs' system-tag columns, so a stage doubles their
number, and at ``STAGES`` a row would be millions of columns wide. That width is
the output, not a walk; only the number of schema queries per node is asserted.
"""

from __future__ import annotations

import collections
import functools
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import DictSource
from orcapod.core.streams.base import StreamBase
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

STAGES = 20

# Stages for the walks whose output carries the system tags (2 ** (stages + 1)
# system-tag columns).
SYSTEM_TAG_STAGES = 8

# Upper bound on the schema queries one node may receive in one walk.
MAX_CALLS_PER_NODE = 30

# The schema queries, plus the two other walks found at construction (ITL-1111):
# ``__repr__`` nests every upstream (it was formatted eagerly into a debug log on
# every invocation), and the private ``_schema_requires_io`` hook, counted because
# ``head()`` and the preview walk the graph through it.
_COUNTED_METHODS = ("output_schema", "keys", "__repr__", "_schema_requires_io")


class WalkBudgetExceeded(AssertionError):
    """More schema queries than the linear budget allows: the walk revisits nodes."""


def _all_stream_classes() -> set[type]:
    """Return ``StreamBase`` and every subclass of it defined so far.

    Returns:
        The stream, source and node classes whose methods are counted.
    """
    seen: set[type] = set()
    pending: list[type] = [StreamBase]
    while pending:
        cls = pending.pop()
        if cls in seen:
            continue
        seen.add(cls)
        pending.extend(cls.__subclasses__())
    return seen


class _CallCounter:
    """Count calls per object and stop a walk that passes a budget.

    Records one call per wrapped method call, keyed by the object called. Every
    counted object is kept until the counter is released, so the id of a
    short-lived stream (a run makes many) is never reused by another object and
    two objects' counts never merge.

    Attributes:
        budget: The most calls allowed in total before ``WalkBudgetExceeded``.
        per_object: Calls per object, keyed by ``id``.
        total: Calls in total.
    """

    def __init__(self, budget: int) -> None:
        """Start with no calls counted.

        Args:
            budget: The most calls allowed in total.
        """
        self.budget = budget
        self.per_object: collections.Counter[int] = collections.Counter()
        self.total = 0
        # Every counted object is kept alive while counting, so that the id of a
        # short-lived stream (a run makes many) is never reused by another one.
        self._counted: dict[int, object] = {}

    def record(self, obj: object) -> None:
        """Count one call on ``obj``.

        Args:
            obj: The object whose method was called.

        Raises:
            WalkBudgetExceeded: If the total passes the budget.
        """
        self._counted[id(obj)] = obj
        self.per_object[id(obj)] += 1
        self.total += 1
        if self.total > self.budget:
            raise WalkBudgetExceeded(
                f"more than {self.budget} schema queries: the walk revisits nodes"
            )

    @property
    def max_per_object(self) -> int:
        """The most calls any one object received, 0 when none was counted."""
        return max(self.per_object.values(), default=0)


@contextmanager
def _count_schema_queries(
    monkeypatch: pytest.MonkeyPatch,
    budget: int,
    methods: tuple[str, ...] = _COUNTED_METHODS,
) -> Iterator[_CallCounter]:
    """Count calls of ``methods`` on every stream class, per object."""
    counter = _CallCounter(budget)

    def wrap(method: Callable[..., Any]) -> Callable[..., Any]:
        """Return ``method`` wrapped to count each call on its object."""

        @functools.wraps(method)
        def counted(self: object, *args: Any, **kwargs: Any) -> Any:
            """Count the call, then call the original method."""
            counter.record(self)
            return method(self, *args, **kwargs)

        return counted

    with monkeypatch.context() as patch:
        for cls in _all_stream_classes():
            for name in methods:
                if name in cls.__dict__:
                    patch.setattr(cls, name, wrap(cls.__dict__[name]))
        yield counter


@function_pod(output_keys="a")
def _branch_a(x: int) -> int:
    """One branch of a function-pod stage."""
    return x + 1


@function_pod(output_keys="b")
def _branch_b(x: int) -> int:
    """The other branch of a function-pod stage."""
    return x + 2


@function_pod(output_keys="x")
def _merge(a: int, b: int) -> int:
    """Join the two branches of a function-pod stage."""
    return a + b


def _operator_stage(stream: Any) -> Any:
    """Split ``stream`` into two renamed branches and join them again with operators.

    Args:
        stream: The previous stage's output, with tag ``id`` and data ``x``.

    Returns:
        A stream with the same columns: five operator streams per stage.
    """
    a = stream.map_data({"x": "a"})
    b = stream.map_data({"x": "b"})
    joined = a.join(b)
    return joined.map_data({"a": "x"}).drop_data_columns("b")


def _function_pod_stage(stream: Any) -> Any:
    """Split ``stream`` into two function-pod branches and merge them.

    Args:
        stream: The previous stage's output, with tag ``id`` and data ``x``.

    Returns:
        A stream with the same columns: three stream nodes per stage.
    """
    return _merge.pod(_branch_a.pod(stream), _branch_b.pod(stream))


# shape -> (stage, nodes per stage)
SHAPES: dict[str, tuple[Callable[[Any], Any], int]] = {
    "operators": (_operator_stage, 5),
    "function_pods": (_function_pod_stage, 3),
}


def _node_count(shape: str, stages: int) -> int:
    """Return the number of nodes ``stages`` stages of ``shape`` make, with the source."""
    return SHAPES[shape][1] * stages + 1


def _budget(shape: str, stages: int = STAGES) -> int:
    """Return the most calls a walk over ``stages`` stages of ``shape`` may make."""
    return MAX_CALLS_PER_NODE * _node_count(shape, stages)


def _source() -> DictSource:
    """Return the three-row source every graph starts from."""
    return DictSource(
        [{"id": i, "x": i} for i in range(3)], tag_columns=["id"], source_id="src"
    )


def _build(shape: str, stages: int = STAGES) -> Any:
    """Build ``stages`` stacked stages of ``shape`` as plain streams.

    Args:
        shape: A key of ``SHAPES``.
        stages: The number of stages.

    Returns:
        The last stage's output stream.
    """
    stage, _ = SHAPES[shape]
    stream = _source()
    for _ in range(stages):
        stream = stage(stream)
    return stream


def _build_job(shape: str, stages: int = STAGES) -> PipelineJob:
    """Record ``stages`` stacked stages of ``shape`` in a job and compile it.

    Args:
        shape: A key of ``SHAPES``.
        stages: The number of stages.

    Returns:
        The compiled job, over an in-memory database.
    """
    job = PipelineJob(name="stacked", database=InMemoryArrowDatabase())
    with job:
        _build(shape, stages)
    return job


def _guarded_build(
    shape: str, monkeypatch: pytest.MonkeyPatch, stages: int = STAGES
) -> Any:
    """Build the stream graph, failing fast if building is not linear."""
    with _count_schema_queries(monkeypatch, _budget(shape, stages)):
        return _build(shape, stages)


def _guarded_build_job(
    shape: str, monkeypatch: pytest.MonkeyPatch, stages: int = STAGES
) -> PipelineJob:
    """Record and compile the job, failing fast if that is not linear."""
    with _count_schema_queries(monkeypatch, _budget(shape, stages)):
        return _build_job(shape, stages)


def _assert_linear(counter: _CallCounter, shape: str, stages: int = STAGES) -> None:
    """Assert that no object received more than its share and the total is in budget."""
    assert counter.max_per_object <= MAX_CALLS_PER_NODE
    assert counter.total <= _budget(shape, stages)


@pytest.fixture(params=sorted(SHAPES))
def shape(request: pytest.FixtureRequest) -> str:
    """Run each test once per stage shape."""
    return request.param


class TestStreams:
    """Walks over plain streams, outside a job."""

    def test_building_the_graph_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Building the stages asks each stream a bounded number of times."""
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            _build(shape)
        _assert_linear(counter, shape)

    @pytest.mark.parametrize(
        ("walk", "stages"),
        [
            pytest.param(lambda s: s.output_schema(), STAGES, id="output_schema"),
            pytest.param(lambda s: s.keys(), STAGES, id="keys"),
            # head() and the preview of a stream that has not run build their table
            # from its zero-row table, which carries the system tags (2 ** (stages + 1)
            # columns): the system-tag stage count (ITL-1114, ITL-1132).
            pytest.param(lambda s: s.head(), SYSTEM_TAG_STAGES, id="head"),
            # The notebook preview is the Jupyter display hook.
            pytest.param(lambda s: s._repr_html_(), SYSTEM_TAG_STAGES, id="preview"),
        ],
    )
    def test_a_schema_walk_is_linear(
        self,
        shape: str,
        walk: Callable[[Any], Any],
        stages: int,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A schema query, ``head()`` or the preview asks each stream a bounded number of times."""
        stream = _guarded_build(shape, monkeypatch, stages)
        with _count_schema_queries(monkeypatch, _budget(shape, stages)) as counter:
            walk(stream)
        _assert_linear(counter, shape, stages)

    def test_the_system_tag_schema_walk_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Predicting the schema with system tags asks each stream a bounded number of times."""
        stream = _guarded_build(shape, monkeypatch, SYSTEM_TAG_STAGES)
        budget = _budget(shape, SYSTEM_TAG_STAGES)
        with _count_schema_queries(monkeypatch, budget) as counter:
            stream.output_schema(all_info=True)
        _assert_linear(counter, shape, SYSTEM_TAG_STAGES)

    def test_the_zero_row_derivation_asks_each_stream_once(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stream's zero-row table (ITL-912) is derived once per node, with its schema walks bounded."""
        # typed_zero_row_table is the zero-row derivation head() makes on a stream that
        # has not run; _typed_zero_row_table is its per-stream hook.
        from orcapod.core.streams.base import typed_zero_row_table

        # The zero-row table carries the system tags (2 ** (stages + 1) columns), so
        # the stage count is the system-tag one (ITL-1114, ITL-1132).
        stages = SYSTEM_TAG_STAGES
        stream = _guarded_build(shape, monkeypatch, stages)
        methods = (*_COUNTED_METHODS, "_typed_zero_row_table", "_zero_row_table")
        with _count_schema_queries(monkeypatch, _budget(shape, stages), methods) as c:
            table = typed_zero_row_table(stream)
        assert table is not None and table.num_rows == 0
        _assert_linear(c, shape, stages)
        with _count_schema_queries(
            monkeypatch, _budget(shape, stages), ("_typed_zero_row_table",)
        ) as once:
            typed_zero_row_table(stream)
        assert once.max_per_object == 1

    @pytest.mark.parametrize("all_info", [False, True])
    def test_head_of_a_stream_that_has_not_run_is_linear_with_the_zero_row_table(
        self, shape: str, all_info: bool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``head()`` builds its typed zero-row table by walking each stream a bounded number of times."""
        # The head's table is built from the zero-row table, which carries the system
        # tags whatever the config: the system-tag stage count (ITL-1114, ITL-1132).
        stages = SYSTEM_TAG_STAGES
        stream = _guarded_build(shape, monkeypatch, stages)
        methods = (*_COUNTED_METHODS, "_typed_zero_row_table", "_zero_row_table")
        with _count_schema_queries(monkeypatch, _budget(shape, stages), methods) as c:
            view = stream.head(all_info=all_info)
        assert view.as_table().num_rows == 0
        _assert_linear(c, shape, stages)

    def test_the_poll_check_asks_each_stream_once(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``head()`` asks each stream once whether reading its schema would poll a source."""
        # The private hook is the walk head() makes to learn whether reading the
        # schema would poll a source.
        # head() builds a zero-row table with the system tags: 8 stages (ITL-1132).
        stream = _guarded_build(shape, monkeypatch, SYSTEM_TAG_STAGES)
        with _count_schema_queries(
            monkeypatch,
            _budget(shape, SYSTEM_TAG_STAGES),
            methods=("_schema_requires_io",),
        ) as counter:
            stream.head()
        assert counter.max_per_object == 1

    def test_repr_is_bounded(self, shape: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """A stream's ``repr`` names its upstreams by class and label, not by their ``repr``."""
        stream = _guarded_build(shape, monkeypatch)
        with _count_schema_queries(monkeypatch, _budget(shape)):
            text = repr(stream)
        assert len(text) < 500
        assert type(stream).__name__ in text

    def test_a_join_error_is_bounded_and_names_the_inputs_and_the_column(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A join of two inputs sharing a data column fails with a short message naming both."""
        stream = _guarded_build(shape, monkeypatch)
        left = stream.map_data({"x": "y"}, label="left_branch")
        right = stream.map_data({"x": "y"}, label="right_branch")
        with _count_schema_queries(monkeypatch, _budget(shape)):
            with pytest.raises(InputValidationError) as excinfo:
                left.join(right)
        message = str(excinfo.value)
        assert len(message) < 1000
        assert "left_branch" in message
        assert "right_branch" in message
        assert "'y'" in message

    def test_hashes_are_unchanged_by_asking_twice(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two builds of the same graph have equal hashes and schemas, memo or not."""
        first = _guarded_build(shape, monkeypatch)
        with _count_schema_queries(monkeypatch, _budget(shape)):
            first.output_schema()
        second = _guarded_build(shape, monkeypatch)
        assert first.content_hash() == second.content_hash()
        assert first.pipeline_hash() == second.pipeline_hash()
        assert first.output_schema() == second.output_schema()


class TestPipelineJob:
    """Walks over the nodes of a ``PipelineJob``."""

    def test_recording_and_compiling_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recording the stages in a job and compiling it asks each node a bounded number of times."""
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            _build_job(shape)
        _assert_linear(counter, shape)

    def test_save_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Saving the job asks each node a bounded number of times."""
        job = _guarded_build_job(shape, monkeypatch)
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            job.save(tmp_path / "job.json")
        _assert_linear(counter, shape)

    def test_load_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Loading the saved job asks each node a bounded number of times."""
        job = _guarded_build_job(shape, monkeypatch)
        with _count_schema_queries(monkeypatch, _budget(shape)):
            job.save(tmp_path / "job.json")
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            PipelineJob.load(tmp_path / "job.json")
        _assert_linear(counter, shape)

    @pytest.mark.parametrize(
        "orchestrator",
        [
            pytest.param(SyncPipelineOrchestrator, id="sync"),
            pytest.param(AsyncPipelineOrchestrator, id="async"),
        ],
    )
    def test_run_is_linear(
        self,
        shape: str,
        orchestrator: type,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A run on either orchestrator asks each node a bounded number of times."""
        job = _guarded_build_job(shape, monkeypatch, SYSTEM_TAG_STAGES)
        budget = _budget(shape, SYSTEM_TAG_STAGES)
        with _count_schema_queries(monkeypatch, budget) as counter:
            job.run(orchestrator=orchestrator())
        _assert_linear(counter, shape, SYSTEM_TAG_STAGES)

    def test_node_reprs_are_bounded(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every job node's ``repr`` names its upstreams by class and label only."""
        job = _guarded_build_job(shape, monkeypatch)
        for node in job.nodes.values():
            with _count_schema_queries(monkeypatch, _budget(shape)):
                text = repr(node)
            assert len(text) < 500

    def test_a_node_schema_walk_is_linear(
        self, shape: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Asking the last node for its schema asks each node a bounded number of times."""
        job = _guarded_build_job(shape, monkeypatch)
        last = list(job.nodes.values())[-1]
        with _count_schema_queries(monkeypatch, _budget(shape)) as counter:
            last.output_schema()
            last.keys()
        _assert_linear(counter, shape)


# ---------------------------------------------------------------------------
# A memoized schema is predicted again when an input of the prediction changes
# ---------------------------------------------------------------------------


def _one_row_source(value: int | float | str) -> DictSource:
    """Return a one-row source named ``src`` whose data column ``x`` holds ``value``."""
    return DictSource([{"id": 1, "x": value}], tag_columns=["id"], source_id="src")


def _rename_job() -> PipelineJob:
    """Return a job ``src`` -> ``renamed`` (``x`` to ``y``) -> ``renamed_again`` (``y`` to ``z``)."""
    job = PipelineJob(name="rebind", database=InMemoryArrowDatabase())
    with job:
        renamed = _one_row_source(1).map_data({"x": "y"}, label="renamed")
        renamed.map_data({"y": "z"}, label="renamed_again")
    return job


def _function_job() -> PipelineJob:
    """Return a job with one function node, ``plus_one``, over ``src``."""
    job = PipelineJob(name="rebind_function", database=InMemoryArrowDatabase())
    with job:
        _branch_a.pod(_one_row_source(1), label="plus_one")
    return job


class TestMemoInvalidation:
    """A memoized schema is predicted again when an input of the prediction changes."""

    def test_bind_to_a_source_of_another_column_type_changes_downstream_schemas(
        self,
    ) -> None:
        """``job.bind`` to a source with another column type changes every downstream schema."""
        job = _rename_job()
        assert job.nodes["renamed"].output_schema()[1]["y"] is int
        assert job.nodes["renamed_again"].output_schema()[1]["z"] is int
        assert job.nodes["renamed_again"].keys() == (("id",), ("z",))

        job.bind(sources={"src": _one_row_source("a")})

        assert job.nodes["renamed"].output_schema()[1]["y"] is str
        assert job.nodes["renamed_again"].output_schema()[1]["z"] is str

    def test_setting_an_operator_nodes_upstreams_changes_its_schema(self) -> None:
        """Setting an operator node's ``upstreams`` changes its own schema."""
        job = _rename_job()
        node = job.nodes["renamed"]
        assert node.output_schema()[1]["y"] is int

        node.upstreams = (_one_row_source(1.5),)

        assert node.output_schema()[1]["y"] is float

    def test_setting_a_function_nodes_upstreams_changes_its_schema(self) -> None:
        """Setting a function node's ``upstreams`` changes its own schema."""
        job = _function_job()
        node = job.nodes["plus_one"]
        assert node.output_schema()[0]["id"] is int
        other = DictSource(
            [{"id": "one", "x": 1}], tag_columns=["id"], source_id="other"
        )

        node.upstreams = (other,)

        assert node.output_schema()[0]["id"] is str

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "A direct bound_source assignment reaches no downstream node, so their "
            "memoized schemas are not predicted again; use job.bind (ITL-1111). "
            "Their pipeline hashes are equally stale today."
        ),
    )
    def test_assigning_bound_source_directly_changes_downstream_schemas(
        self,
    ) -> None:
        """A direct ``bound_source`` assignment should change the downstream schemas."""
        job = _rename_job()
        assert job.nodes["renamed_again"].output_schema()[1]["z"] is int

        job.nodes["src"].bound_source = _one_row_source("a")

        assert job.nodes["renamed_again"].output_schema()[1]["z"] is str

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "Setting a wired node's upstreams clears its own memo only; the nodes "
            "downstream of it keep theirs (ITL-1111). Nothing in orcapod sets "
            "upstreams after construction."
        ),
    )
    def test_setting_upstreams_changes_downstream_schemas(self) -> None:
        """Setting a node's ``upstreams`` should change the schemas downstream of it."""
        job = _rename_job()
        assert job.nodes["renamed_again"].output_schema()[1]["z"] is int

        job.nodes["renamed"].upstreams = (_one_row_source(1.5),)

        assert job.nodes["renamed_again"].output_schema()[1]["z"] is float
