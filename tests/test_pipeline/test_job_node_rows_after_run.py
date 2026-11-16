"""After a run, a function job node returns exactly the rows of that run's inputs (ITL-1093).

One store, a source of one ``source_id`` holding 3 rows, then 6, then the same 3 again, then
3 different rows. After each run, on either orchestrator, every function job node returns
the rows of that run's inputs through ``as_table``, ``iter_data``, ``head`` and ``run()``,
and so does a consumer built over the node afterwards; the two orchestrators agree row for
row. Stored results of other inputs stay stored and reusable. A run that raised defines no
rows: the node then reads as a node with no run in this process, which is empty (ITL-1095);
what was stored stays readable through ``as_source()``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.data_function import PythonDataFunction
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import OrcapodDatabaseWriteError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.core.streams import ViewStatus
from orcapod.sources import DictSource


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``."""
    return x + 1


@function_pod(output_keys="z")
def dbl(y: int) -> int:
    """Return ``2 * y``."""
    return 2 * y


@function_pod(output_keys="total")
def total(x: list[int]) -> int:
    """Return the sum of a group's values."""
    return sum(x)


@function_pod(output_keys="y")
def null_on_10(x: int) -> int | None:
    """Return ``x + 1``, or a null value for ``x == 10``."""
    return None if x == 10 else x + 1


@function_pod(output_keys="y")
def fails_on_20(x: int) -> int:
    """Return ``x + 1``; raise for ``x == 20``."""
    if x == 20:
        raise ValueError("x is 20")
    return x + 1


@function_pod(output_keys="y")
def store_fails_on_20(x: int) -> int:
    """Return ``x + 1``; for ``x == 20`` raise the error a failed database write raises.

    A ``OrcapodDatabaseError`` fails the run whatever the error policy, on both orchestrators, as a
    failed write of the node's results does.
    """
    if x == 20:
        raise OrcapodDatabaseWriteError(("simulated", "results"))
    return x + 1


ROWS = [{"g": i % 2, "k": i, "x": 10 * i} for i in range(6)]
# 3 rows, 6 rows (3 more), the same 3 again (fewer), 3 different rows. The source's record
# ids are positional, so the last run's rows are new inputs whose results are stored.
SEQUENCE = [ROWS[:3], ROWS, ROWS[:3], ROWS[3:]]
ORCHESTRATORS = ["sync", "async"]


def _source(data: list[dict[str, int]]) -> DictSource:
    """Return a ``DictSource`` of ``data`` with tags ``g`` and ``k`` and source id ``s``."""
    return DictSource(
        data, tag_columns=["g", "k"], tag_only_columns=True, source_id="s"
    )


def _build(data: list[dict[str, int]], db: Any) -> PipelineJob:
    """Return a job over ``db``: ``rowwise``, ``downstream`` of it, and ``grouped``."""
    job = PipelineJob(name="itl1093", database=db)
    with job:
        rowwise = inc.pod(_source(data), label="rowwise")
        dbl.pod(rowwise, label="downstream")
        total.pod(_source(data).group_by("g"), label="grouped")
    return job


def _orchestrator(kind: str, error_policy: str = "continue") -> Any:
    """Return the ``sync`` or ``async`` orchestrator with ``error_policy``."""
    if kind == "sync":
        return SyncPipelineOrchestrator(error_policy=error_policy)  # pyright: ignore[reportArgumentType]
    return AsyncPipelineOrchestrator(error_policy=error_policy)  # pyright: ignore[reportArgumentType]


def _canonical(rows: list[dict[str, Any]]) -> list[tuple[tuple[str, str], ...]]:
    """Rows as a sorted list, each a sorted tuple of (column, repr(value))."""
    return sorted(tuple(sorted((k, repr(v)) for k, v in row.items())) for row in rows)


ALL = {"system_tags": True}


def _reads(node: Any) -> dict[str, list[tuple[tuple[str, str], ...]]]:
    """Every public read of ``node``, rows with system tags, canonical."""
    iterated = [
        {**tag.as_dict(all_info=True), **data.as_dict()}
        for tag, data in node.iter_data()
    ]
    return {
        "as_table": _canonical(node.as_table(columns=ALL).to_pylist()),
        "head": _canonical(node.head(100, columns=ALL).as_table().to_pylist()),
        "iter_data": _canonical(
            [
                {k: v for k, v in row.items() if not k.startswith("__")}
                for row in iterated
            ]
        ),
    }


def _values(node: Any, key: Callable[[dict[str, Any]], Any]) -> list[Any]:
    """Return ``key`` of every row of ``node.as_table()``, sorted."""
    return sorted(key(row) for row in node.as_table().to_pylist())


def _expected_rowwise(data: list[dict[str, int]]) -> list[tuple[int, int]]:
    """Return the ``(k, y)`` rows ``rowwise`` gives for ``data``, sorted."""
    return sorted((row["k"], row["x"] + 1) for row in data)


def _expected_downstream(data: list[dict[str, int]]) -> list[tuple[int, int]]:
    """Return the ``(k, z)`` rows ``downstream`` gives for ``data``, sorted."""
    return sorted((row["k"], 2 * (row["x"] + 1)) for row in data)


def _expected_grouped(data: list[dict[str, int]]) -> list[tuple[int, int]]:
    """Return the ``(g, total)`` rows ``grouped`` gives for ``data``, sorted."""
    sums: dict[int, int] = {}
    for row in data:
        sums[row["g"]] = sums.get(row["g"], 0) + row["x"]
    return sorted(sums.items())


def _check_run(job: PipelineJob, data: list[dict[str, int]]) -> None:
    """Assert every node of ``job`` returns the rows of ``data``, through every read."""
    nodes = job.nodes
    assert _values(nodes["rowwise"], lambda r: (r["k"], r["y"])) == _expected_rowwise(
        data
    )
    assert _values(nodes["downstream"], lambda r: (r["k"], r["z"])) == (
        _expected_downstream(data)
    )
    assert _values(nodes["grouped"], lambda r: (r["g"], r["total"])) == (
        _expected_grouped(data)
    )
    for label in ("rowwise", "downstream", "grouped"):
        reads = _reads(nodes[label])
        assert reads["head"] == reads["as_table"], label
        assert len(reads["iter_data"]) == len(reads["as_table"]), label
    # A consumer built over the node after the run receives the run's rows.
    consumer = dbl.pod(nodes["rowwise"])
    assert sorted((r["k"], r["z"]) for r in consumer.as_table().to_pylist()) == (
        _expected_downstream(data)
    )


def _snapshot(job: PipelineJob) -> dict[str, Any]:
    """Return every public read of each function node of ``job``."""
    return {
        label: _reads(job.nodes[label])
        for label in ("rowwise", "downstream", "grouped")
    }


class TestFreshJobPerRun:
    """A new job over the same database for every run, as the issue's reproduction."""

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_each_run_returns_its_inputs_rows(self, kind: str) -> None:
        """Every read after each run gives that run's rows; the full run reuses 3."""
        db = InMemoryArrowDatabase()
        for index, data in enumerate(SEQUENCE):
            job = _build(data, db)
            summary = job.run(orchestrator=_orchestrator(kind))
            _check_run(job, data)
            if index == 1:
                # The 6-row run reuses the 3 stored results of the first run.
                assert summary.nodes["rowwise"].reused == 3
                assert summary.nodes["rowwise"].computed == 3

    def test_sync_and_async_agree_row_for_row(self) -> None:
        """Both orchestrators give equal rows, system tags included, after every run."""
        snapshots: dict[str, list[dict[str, Any]]] = {}
        for kind in ORCHESTRATORS:
            db = InMemoryArrowDatabase()
            snapshots[kind] = []
            for data in SEQUENCE:
                job = _build(data, db)
                job.run(orchestrator=_orchestrator(kind))
                snapshots[kind].append(_snapshot(job))
        assert snapshots["sync"] == snapshots["async"]

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_node_run_returns_its_inputs_rows(self, kind: str) -> None:
        """A node's ``run()`` after the job run shows that run's rows."""
        db = InMemoryArrowDatabase()
        for data in SEQUENCE:
            job = _build(data, db)
            job.run(orchestrator=_orchestrator(kind))
            view = job.nodes["rowwise"].run()
            assert sorted((r["k"], r["y"]) for r in view.as_table().to_pylist()) == (
                _expected_rowwise(data)
            )


class TestSameJobRebound:
    """One job object, its source rebound before every run: the same node objects run again."""

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_each_run_returns_its_inputs_rows(self, kind: str) -> None:
        """Every read after each run of the rebound job gives that run's rows."""
        db = InMemoryArrowDatabase()
        job = _build(SEQUENCE[0], db)
        for data in SEQUENCE:
            job.bind(sources={"s": _source(data)})
            job.run(orchestrator=_orchestrator(kind))
            _check_run(job, data)

    def test_sync_and_async_agree_row_for_row(self) -> None:
        """Both orchestrators give equal rows after every run of the rebound job."""
        snapshots: dict[str, list[dict[str, Any]]] = {}
        for kind in ORCHESTRATORS:
            db = InMemoryArrowDatabase()
            job = _build(SEQUENCE[0], db)
            snapshots[kind] = []
            for data in SEQUENCE:
                job.bind(sources={"s": _source(data)})
                job.run(orchestrator=_orchestrator(kind))
                snapshots[kind].append(_snapshot(job))
        assert snapshots["sync"] == snapshots["async"]


def _single(pod: Any, data: list[dict[str, int]], db: Any) -> PipelineJob:
    """Return a job over ``db`` applying ``pod`` to a source of ``data`` as ``node``."""
    job = PipelineJob(name="itl1093_single", database=db)
    with job:
        pod(_source(data), label="node")
    return job


class TestNullAndInactive:
    """A null value is a row; an inactive function's inputs (no output) are rows of no read."""

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_null_value_is_a_row(self, kind: str) -> None:
        """A row whose output value is null is one of the run's rows."""
        db = InMemoryArrowDatabase()
        _single(null_on_10.pod, ROWS, db).run(orchestrator=_orchestrator(kind))
        job = _single(null_on_10.pod, ROWS[:3], db)
        job.run(orchestrator=_orchestrator(kind))
        node = job.nodes["node"]
        assert _values(node, lambda r: (r["k"], r["y"])) == [(0, 1), (1, None), (2, 21)]
        reads = _reads(node)
        assert reads["head"] == reads["as_table"]
        assert len(reads["iter_data"]) == 3

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_inactive_function_gives_no_rows(self, kind: str) -> None:
        """A run of an inactive function has no rows, though other results are stored."""

        @function_pod(output_keys="y")
        def add_one(x: int) -> int:
            """Return ``x + 1``."""
            return x + 1

        db = InMemoryArrowDatabase()
        _single(add_one.pod, ROWS[3:], db).run(orchestrator=_orchestrator(kind))
        # A function switched off produces no output for the inputs it is given.
        data_function: object = add_one.pod.data_function
        assert isinstance(data_function, PythonDataFunction)
        data_function.set_active(False)
        job = _single(add_one.pod, ROWS[:3], db)
        job.run(orchestrator=_orchestrator(kind))
        node = job.nodes["node"]
        assert node.as_table().num_rows == 0
        assert list(node.iter_data()) == []
        assert node.head(100).as_table().num_rows == 0


class TestRaisedRun:
    """A run that raised defines no rows: the node reads as a node with no run (empty)."""

    @pytest.mark.parametrize(
        ("kind", "fn", "policy"),
        [
            ("sync", fails_on_20, "fail_fast"),
            ("async", fails_on_20, "fail_fast"),
            ("sync", store_fails_on_20, "continue"),
            ("async", store_fails_on_20, "continue"),
        ],
    )
    def test_raised_run_reads_as_no_run(self, kind: str, fn: Any, policy: str) -> None:
        """After a run that raised, the node reads every stored row, as with no run."""
        db = InMemoryArrowDatabase()
        # Store the results of rows 3..5 first, then run rows 0..3, which fails at x=20.
        _single(fn.pod, ROWS[3:], db).run(orchestrator=_orchestrator(kind))
        job = _single(fn.pod, ROWS[:4], db)
        with pytest.raises((ValueError, OrcapodDatabaseWriteError)):
            job.run(orchestrator=_orchestrator(kind, policy))
        node = job.nodes["node"]
        assert _values(node, lambda r: (r["k"], r["y"])) == []
        reads = _reads(node)
        assert reads["head"] == reads["as_table"] == []
        assert node.is_stale
        # Nothing stored was dropped: the stored view holds the earlier run's rows.
        stored = {(r["k"], r["y"]) for r in node.as_source().as_table().to_pylist()}
        assert {(3, 31), (4, 41), (5, 51)} <= stored


class TestDuplicateInputs:
    """Two inputs with one base entry id give two held rows, as downstream receives them.

    Only a stream without system tags can carry two such inputs (a source gives every
    row its own record id), so the node is built directly over an ``ArrowTableStream``.
    """

    @staticmethod
    def _node(pod: Any) -> Any:
        """Return a node applying ``pod`` to rows (k=0, x=5) twice and (k=2, x=7)."""
        import pyarrow as pa

        from orcapod.core.nodes import FunctionJobNode
        from orcapod.core.streams import ArrowTableStream

        schema = pa.schema(
            [
                pa.field("k", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        )
        table = pa.Table.from_arrays(
            [pa.array([0, 0, 2], pa.int64()), pa.array([5, 5, 7], pa.int64())],
            schema=schema,
        )
        return FunctionJobNode(
            pod,
            ArrowTableStream(table, tag_columns=["k"]),
            pipeline_database=InMemoryArrowDatabase(),
            result_database=InMemoryArrowDatabase(),
        )

    @staticmethod
    def _flaky_pod() -> Any:
        """Return a pod computing ``x + 1`` whose first call for ``x == 5`` raises."""
        calls: list[int] = []

        @function_pod(output_keys="y")
        def flaky_inc(x: int) -> int:
            """Return ``x + 1``; raise on the first call for ``x == 5``."""
            calls.append(x)
            if x == 5 and calls.count(5) == 1:
                raise ValueError("transient failure")
            return x + 1

        return flaky_inc.pod

    @staticmethod
    def _run_async(node: Any) -> list[tuple[int, int]]:
        """Run ``node`` with ``async_execute`` over its upstream; return the rows sent."""
        import asyncio

        from orcapod.channels import Channel

        async def run() -> list[tuple[int, int]]:
            """Feed the upstream's rows in and collect what the node sends out."""
            inputs = Channel(buffer_size=16)
            outputs = Channel(buffer_size=16)
            # The input stream is the node's own upstream (a test of async_execute).
            for item in node.upstreams[0].iter_data():
                await inputs.writer.send(item)
            await inputs.writer.close()
            await node.async_execute(inputs.reader, outputs.writer)
            got = [
                (tag.as_dict()["k"], data.as_dict()["y"])
                async for tag, data in outputs.reader
            ]
            return sorted(got)

        return asyncio.run(run())

    @staticmethod
    def _held(node: Any) -> list[tuple[int, int]]:
        """Return the node's ``(k, y)`` rows, sorted."""
        return sorted((r["k"], r["y"]) for r in node.as_table().to_pylist())

    def test_sync(self) -> None:
        """Sync: both equal inputs are held, as the run returned them."""
        node = self._node(inc.pod)
        view = node.run()
        assert self._held(node) == [(0, 6), (0, 6), (2, 8)]
        assert sorted((r["k"], r["y"]) for r in view.as_table().to_pylist()) == (
            self._held(node)
        )

    def test_async(self) -> None:
        """Async: both equal inputs are held, as they were sent downstream."""
        node = self._node(inc.pod)
        received = self._run_async(node)
        assert received == [(0, 6), (0, 6), (2, 8)]
        assert self._held(node) == received

    def test_sync_one_of_two_equal_inputs_fails(self) -> None:
        """Sync: an equal input that failed is not held; the one that succeeded is."""
        node = self._node(self._flaky_pod())
        received = sorted(
            (tag.as_dict()["k"], data.as_dict()["y"])
            for tag, data in node.execute(node.upstreams[0])
        )
        assert received == [(0, 6), (2, 8)]
        assert self._held(node) == received

    def test_async_one_of_two_equal_inputs_fails(self) -> None:
        """Async: an equal input that failed is not held; the one that succeeded is."""
        node = self._node(self._flaky_pod())
        received = self._run_async(node)
        assert received == [(0, 6), (2, 8)]
        assert self._held(node) == received


class TestRefusedOrFailedRunAfterAGoodRun:
    """A run that raises after a good run drops that run's rows too (ITL-1093).

    The node then reads as a node with no run in this process: every row stored under
    its content hash, read from the database, whatever raised. An input refused for its data
    context is tested in ``test_stored_data_context.py``, which registers a second one.
    """

    @staticmethod
    def _job(db: Any) -> PipelineJob:
        """Return a job applying ``inc`` to a source of 6 rows as ``node``."""
        return _single(inc.pod, ROWS, db)

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_observer_failure_at_node_end_drops_the_rows(self, kind: str) -> None:
        """An observer raising at the node's end leaves the node with no run's rows."""
        from orcapod.pipeline.observer import NoOpObserver

        class FailsAtNodeEnd(NoOpObserver):
            """An observer whose ``on_node_end`` raises."""

            def on_node_end(self, node_label: str, node_hash: str) -> None:
                """Raise at the function node's end, as a broken observer would."""
                if node_label == "node":
                    raise RuntimeError("observer failed")

        db = InMemoryArrowDatabase()
        job = self._job(db)
        job.run(orchestrator=_orchestrator(kind))
        job.bind(sources={"s": _source(ROWS[:3])})
        job.run(orchestrator=_orchestrator(kind))
        node = job.nodes["node"]
        assert _values(node, lambda r: r["k"]) == [0, 1, 2]
        # A node holding a current run is not run again (ITL-1095): clear it first.
        node.clear_cache()
        with pytest.raises(RuntimeError, match="^observer failed$"):
            job.run(orchestrator=_orchestrator(kind), observer=FailsAtNodeEnd())
        # The node holds no run (it reads empty, ITL-1095); the database keeps every row.
        assert _values(node, lambda r: r["k"]) == []
        stored = node.as_source().as_table().to_pylist()
        assert sorted(r["k"] for r in stored) == [0, 1, 2, 3, 4, 5]


class TestAsyncOutputCloseFails:
    """An async run whose output cannot be closed defines no rows (ITL-1093)."""

    def test_failed_close_leaves_no_run_rows(self) -> None:
        """A close that raises after every row was sent leaves the node with no run."""
        import asyncio

        import pyarrow as pa

        from orcapod.channels import Channel
        from orcapod.core.nodes import FunctionJobNode
        from orcapod.core.streams import ArrowTableStream

        class CloseFails:
            """An output writer that keeps what it is sent and raises on close."""

            def __init__(self) -> None:
                """Start with no rows."""
                self.sent: list[Any] = []

            async def send(self, item: Any) -> None:
                """Keep ``item``."""
                self.sent.append(item)

            async def close(self) -> None:
                """Raise, as a closed or broken channel would."""
                raise OSError("output cannot be closed")

        schema = pa.schema(
            [
                pa.field("k", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        )
        table = pa.Table.from_arrays(
            [pa.array([0, 1], pa.int64()), pa.array([5, 6], pa.int64())],
            schema=schema,
        )
        node = FunctionJobNode(
            inc.pod,  # pyright: ignore[reportArgumentType]  # the decorator's pod type is not declared as FunctionPodProtocol
            ArrowTableStream(table, tag_columns=["k"]),
            pipeline_database=InMemoryArrowDatabase(),
            result_database=InMemoryArrowDatabase(),
        )
        output = CloseFails()

        async def run() -> None:
            """Run the node over its upstream's rows into ``output``."""
            inputs = Channel(buffer_size=16)
            # The input stream is the node's own upstream (a test of async_execute).
            for item in node.upstreams[0].iter_data():
                await inputs.writer.send(item)
            await inputs.writer.close()
            await node.async_execute(inputs.reader, output)  # pyright: ignore[reportArgumentType]  # a test double of the writer

        with pytest.raises(OSError, match="cannot be closed"):
            asyncio.run(run())
        assert len(output.sent) == 2
        # No run is held: the node reads empty (ITL-1095); the database keeps the rows.
        assert node.head(10).status is ViewStatus.NOT_COMPUTED
        assert _values(node, lambda r: (r["k"], r["y"])) == []
        stored = node.as_source().as_table().to_pylist()
        assert sorted((r["k"], r["y"]) for r in stored) == [(0, 6), (1, 7)]
