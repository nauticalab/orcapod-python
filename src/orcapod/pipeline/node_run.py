"""``node.run()`` of a job node: run it and the job nodes it depends on (ITL-1095).

Developer, November 13, 2026: "given that the initially loaded job is empty, I think it's
fair that node.run will cascade node.run on all nodes it depends on recursively.
Critically, node.run(load_only=True) would trigger the upstream node.run also with
load_only=True." And: "if it's not stale, it shouldn't even trigger a run and simply
return what's in the stream cached already."

The cascade is ``job.run()`` restricted to the node's dependencies, on the synchronous
orchestrator: a dependency that holds a current run is not run again (its rows feed the
nodes after it), every other one runs once, in dependency order, and the run ends with
the commit ``job.run()`` makes.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from orcapod.errors import OrcapodDatabaseError, OrcapodDatabaseWriteError
from orcapod.pipeline.run_summary import (
    RunCountingObserver,
    RunSummary,
    run_node_summaries,
)
from orcapod.utils.database_errors import flush_best_effort

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol


def _is_job_node(stream: Any) -> bool:
    """Whether ``stream`` is a node of a job (as opposed to a stream outside any job)."""
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.nodes.staleness import RunHoldingNode
    from orcapod.side_effects import SideEffectJobNode

    return isinstance(stream, (RunHoldingNode, SideEffectJobNode, SourceJobNode))


def depends_on_job_nodes(node: Any) -> bool:
    """Whether any direct input of ``node`` is a job node.

    A node built directly over streams outside a job (a source, a pod's stream) has
    nothing to run first: its ``run()`` reads its input as it always has.
    """
    return any(_is_job_node(upstream) for upstream in node.upstreams)


def _streams_read_by(node: Any) -> Iterator[Any]:
    """Yield the streams outside a job that a run of ``node`` reads.

    The node's inputs, the stream bound to a source slot among them, and, through every
    stream outside a job, that stream's own inputs. A job node other than a source slot
    is not entered: the run reads the rows the node holds (none if it has not run in
    this process). Nor is an ``ArrowTableStream``: reading it reads only the table it
    holds, whatever its ``upstreams`` name (ITL-1121).

    Args:
        node: A job node; for a source slot, its bound stream is what it reads.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.streams.arrow_table_stream import ArrowTableStream

    pending: list[Any] = (
        [node.bound_source] if isinstance(node, SourceJobNode) else list(node.upstreams)
    )
    seen: list[Any] = []
    while pending:
        stream = pending.pop()
        if stream is None or any(stream is other for other in seen):
            continue
        seen.append(stream)
        if isinstance(stream, SourceJobNode):
            pending.append(stream.bound_source)
            continue
        if _is_job_node(stream):
            continue
        yield stream
        if not isinstance(stream, ArrowTableStream):
            pending.extend(getattr(stream, "upstreams", ()))


def refuse_unloadable_inputs(node: Any) -> None:
    """Refuse a load-only run of ``node`` that would read an input which computes or
    delivers when it is read.

    A load-only run computes, delivers and writes nothing (ITL-1095), so it reads only
    job nodes, sources and streams that neither compute nor deliver (developer,
    November 14, 2026: "Refuse is consistent"; ITL-1121). Outside a job, a function
    pod's stream (``FunctionPodStream``, a cached pod's included) computes its rows when
    read, and a side-effect pod's stream (``SideEffectPodStream``) or node
    (``SideEffectNode``, the blueprint a ``Pipeline`` records) delivers its side
    effects. Such a stream is refused before anything is read, whether it is an input of
    ``node``, bound to a source slot, or read through another stream outside a job (an
    operator's stream over it).

    Args:
        node: A job node a load-only run would run or read.

    Raises:
        ValueError: If a run of ``node`` would read such a stream.
    """
    from orcapod.core.function_pod import FunctionPodStream
    from orcapod.side_effects import SideEffectNode, SideEffectPodStream

    for upstream in _streams_read_by(node):
        # A SideEffectJobNode is a job node: _streams_read_by never yields one.
        if isinstance(upstream, (SideEffectPodStream, SideEffectNode)):
            what = (
                "a side-effect pod's stream outside a job, which delivers its side "
                "effects when it is read, and a load-only run delivers nothing"
            )
        elif isinstance(upstream, FunctionPodStream):
            what = (
                "a function pod's stream outside a job, which computes when it is "
                "read, and a load-only run computes nothing"
            )
        else:
            continue
        raise ValueError(
            f"A load-only run cannot run node {node.label!r}: its input "
            f"{upstream.label!r} is {what}. Build the pod inside the job, or run "
            "without load_only."
        )


def _databases(nodes: list[Any]) -> list[ArrowDatabaseProtocol]:
    """The distinct databases the given nodes write to: pipeline and result databases."""
    found: list[ArrowDatabaseProtocol] = []
    for node in nodes:
        pipeline_database = getattr(node, "_pipeline_database", None)
        cached_pod = getattr(node, "_cached_function_pod", None)
        result_database = getattr(cached_pod, "result_database", None)
        for database in (pipeline_database, result_database):
            if database is not None and not any(database is d for d in found):
                found.append(database)
    return found


def _job_name(node: Any) -> str:
    """The name of the job ``node`` belongs to: its pipeline database's location."""
    database = getattr(node, "_pipeline_database", None)
    base_path: tuple[str, ...] = getattr(database, "base_path", ())
    return "/".join(base_path)


def clear_run_rows(nodes: Iterable[Any]) -> None:
    """Drop the rows of a run whose end-of-run commit failed from every node it filled.

    A run that raised defines no rows (ITL-1093): each function and operator job node of
    the run is cleared (not run, stale) before the commit's error propagates. What was
    committed stays stored. A load-only run writes nothing, so its commit has nothing
    to fail on.

    Args:
        nodes: The nodes of the run.
    """
    from orcapod.core.nodes.staleness import RunHoldingNode

    for node in nodes:
        if isinstance(node, RunHoldingNode):
            node.clear_cache()  # pyright: ignore[reportAttributeAccessIssue]  # every RunHoldingNode is a job node with clear_cache


def run_sources(streams: Iterable[Any]) -> list[Any]:
    """The distinct sources that feed ``streams``, in the order first met.

    A ``SourceJobNode`` gives its bound source; a root source is itself; any other
    stream is followed through its ``upstreams``. A bound stream that is not a root
    source (a pod's stream bound to a slot) is followed the same way.

    Args:
        streams: The nodes of a run, or the node whose ``run()`` it is.

    Returns:
        Every ``RootSource`` met, once each.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.base import RootSource

    found: list[Any] = []
    visited: list[Any] = []
    pending = list(streams)
    while pending:
        stream = pending.pop(0)
        if any(stream is seen for seen in visited):
            continue
        visited.append(stream)
        if isinstance(stream, SourceJobNode):
            if stream.bound_source is not None:
                pending.append(stream.bound_source)
        elif isinstance(stream, RootSource):
            if not any(stream is source for source in found):
                found.append(stream)
        else:
            pending.extend(getattr(stream, "upstreams", ()))
    return found


def check_sources(streams: Iterable[Any]) -> tuple[str, ...]:
    """Ask each source feeding ``streams`` once whether its data changed (ITL-1102).

    Called at the start of a run, before the state rule decides which nodes run: a
    source that found new data has moved its modified time, so the nodes below it are
    stale. A check that raises does not stop the run (developer, November 14, 2026:
    "For check that fails, issues a warning"): one ``SourceCheckFailedWarning`` names
    the source and the error, at the user's line that started the run and at every run,
    and the run proceeds with the data the source holds (a ``PollingSource`` that a run
    reads is polled again by that read, and a poll that fails then raises: ITL-1139).

    Args:
        streams: The nodes of the run, or the node whose ``run()`` it is.

    Returns:
        The source ids of the sources that could not be checked, in the order met.
    """
    import sys
    import warnings

    from orcapod.errors import SourceCheckFailedWarning
    from orcapod.utils.warning_utils import outside_orcapod_stacklevel

    unchecked: list[str] = []
    for source in run_sources(streams):
        try:
            source.check_for_new_data()
        except Exception as exc:
            name = _source_name(source)
            unchecked.append(name)
            # Shown at the user's line that started the run, at every run: a fresh
            # registry each time, as for RowFailuresWarning, so the default filter's
            # once-per-line rule cannot hide the next run's failure.
            caller = sys._getframe(outside_orcapod_stacklevel() - 1)  # pyright: ignore[reportPrivateUsage]  # as the warnings module does
            warnings.warn_explicit(
                f"Source {name!r} could not be checked for new data: "
                f"{type(exc).__name__}: {str(exc).rstrip('.')}. The run proceeds "
                "with the data the source already holds.",
                SourceCheckFailedWarning,
                caller.f_code.co_filename,
                caller.f_lineno,
                module=caller.f_globals.get("__name__"),
                registry=None,
                module_globals=caller.f_globals,
            )
    return tuple(unchecked)


def _source_name(source: Any) -> str:
    """A source's name for a report: its source id, or its label without one."""
    try:
        return str(source.source_id)
    except ValueError:
        return str(source.label)


@dataclass(frozen=True)
class RunPlan:
    """What a run does with each node of its graph (ITL-1095).

    Attributes:
        dag: The nodes the run touches: those it runs, and the predecessors that feed
            them (a source slot is read; any other one is held).
        held_rows: The rows of each held node of ``dag``: the rows of its current run,
            or of a stream outside the job; they feed the nodes after it as if it had
            just run.
        to_run: The nodes the run runs, in dependency order.
        held_counts: Every node of the whole graph that holds a current run, with its
            row count, for the summary.
    """

    dag: Any
    held_rows: dict[Any, list[Any]]
    to_run: list[Any]
    held_counts: dict[Any, int]


def plan_run(graph: Any, *, load_only: bool, outside: Sequence[Any] = ()) -> RunPlan:
    """Choose the nodes a run runs: the state rule of every job node's run (ITL-1095).

    A node that holds a current run (``holds_current_run``) is not run; a source slot
    is read only when a node that runs reads it; a stream outside the job
    (``outside``) is read for the nodes it feeds; every other node runs. The same plan
    serves ``job.run()`` (its whole graph) and ``node.run()`` (the node and what it
    depends on). A load-only plan refuses, before reading anything, a node to run whose
    input computes or delivers when read (``refuse_unloadable_inputs``).

    Args:
        graph: The run's node graph (``topological_sort``, ``predecessors``).
        load_only: Whether the run is load-only (a node filled by a load-only run is
            current only for one).
        outside: Streams of ``graph`` that are not job nodes.

    Returns:
        The plan.

    Raises:
        ValueError: As ``refuse_unloadable_inputs``, for a load-only run.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.nodes.staleness import RunHoldingNode
    from orcapod.pipeline.dag import OrcaDAG

    order = list(graph.topological_sort())

    def is_outside(node: Any) -> bool:
        """Whether ``node`` is one of the ``outside`` streams: by identity, not by
        equality (two equal streams are still two inputs)."""
        return any(node is other for other in outside)

    def is_current(node: Any) -> bool:
        """Whether ``node`` holds a run this run need not redo (it is held)."""
        return isinstance(node, RunHoldingNode) and node.holds_current_run(
            load_only=load_only
        )

    def is_source(node: Any) -> bool:
        """Whether ``node`` is a source slot: read when a node that runs reads it."""
        return isinstance(node, SourceJobNode)

    current = [node for node in order if is_current(node)]
    to_run = [
        node
        for node in order
        if not is_source(node)
        and not is_outside(node)
        and not any(node is c for c in current)
    ]
    if load_only:
        # Refused before any stream is read below: a load-only run cannot read an
        # input that computes or delivers (ITL-1121).
        for node in to_run:
            refuse_unloadable_inputs(node)
    included: list[Any] = []
    for node in order:
        feeds_a_run = any(
            any(successor is r for r in to_run) for successor in graph.successors(node)
        )
        if any(node is r for r in to_run) or feeds_a_run:
            included.append(node)
    dag: OrcaDAG[Any] = OrcaDAG()
    for node in included:
        dag.add_node(node)
    for node in to_run:
        for upstream in graph.predecessors(node):
            dag.add_edge(upstream, node)
    held_rows = {
        node: list(node.iter_data())
        for node in included
        if not any(node is r for r in to_run) and not is_source(node)
    }
    held_counts = {node: sum(1 for _ in node.iter_data()) for node in current}
    return RunPlan(dag, held_rows, to_run, held_counts)


def commit_run(databases: Sequence[ArrowDatabaseProtocol], ran: Sequence[Any]) -> None:
    """Commit a run's writes at its end; a failed commit clears the nodes that ran.

    A run whose commit failed did not complete, so no node it ran holds its rows
    (ITL-1095); nodes it held keep the rows of their earlier run.

    Raises:
        OrcapodDatabaseError: The commit's own database error.
        OrcapodDatabaseWriteError: If committing failed for another reason.
    """
    for database in databases:
        try:
            database.flush()
        except OrcapodDatabaseError:
            clear_run_rows(ran)
            raise
        except Exception as exc:
            clear_run_rows(ran)
            raise OrcapodDatabaseWriteError(database.base_path, flush=True) from exc
        except BaseException:
            # An interruption during the commit leaves its outcome unknown too.
            clear_run_rows(ran)
            raise


def run_with_dependencies(
    target: Any, *, load_only: bool, unchecked_sources: Sequence[str] = ()
) -> RunSummary:
    """Run ``target`` and every job node it depends on that holds no current run.

    The dependencies are found through ``upstreams``; the plan (``plan_run``) is the
    one ``job.run()`` makes over the job: a dependency that holds a current run is not
    run and its rows feed the nodes after it, a stream outside the job is read, every
    other node runs once, in dependency order, on the ``SyncPipelineOrchestrator``
    (error policy ``"continue"``). The run ends with a commit of every database it wrote
    to, also, best-effort, when it fails; a load-only run commits nothing (it writes
    nothing, and pending writes of others stay pending). A run that raises leaves no
    node it ran holding rows.

    Args:
        target: The job node whose ``run()`` this is.
        load_only: Whether to load stored results only.
        unchecked_sources: The sources the caller's start-of-run check could not
            check (``check_sources``), for the summary.

    Returns:
        The run's summary, one entry per function node of the graph.

    Raises:
        OrcapodDatabaseWriteError: If committing the run's writes failed.
        ValueError: For a load-only run, if a node it would run reads an input that
            computes or delivers when read (``refuse_unloadable_inputs``); nothing has run then.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.pipeline.dag import OrcaDAG
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

    graph: OrcaDAG[Any] = OrcaDAG()
    seen: list[Any] = []
    outside: list[Any] = []

    def visit(node: Any) -> None:
        """Add ``node`` and what it depends on, up to the sources of the job."""
        if any(node is other for other in seen):
            return
        seen.append(node)
        graph.add_node(node)
        if isinstance(node, SourceJobNode):
            return
        if not _is_job_node(node):
            outside.append(node)
            return
        for upstream in node.upstreams:
            visit(upstream)
            graph.add_edge(upstream, node)

    visit(target)
    plan = plan_run(graph, load_only=load_only, outside=outside)
    databases = _databases(plan.to_run)
    counter = RunCountingObserver()
    try:
        _ = SyncPipelineOrchestrator().run(
            plan.dag, observer=counter, load_only=load_only, held_rows=plan.held_rows
        )
    except BaseException as run_error:
        if isinstance(run_error, Exception) and not load_only:
            # Commit the writes that succeeded before the failure, as job.run() does;
            # a load-only run writes nothing, so it commits nothing (ITL-1095).
            flush_best_effort(run_error, *databases)
        # A run that raised leaves no node it ran holding rows: whether their writes
        # were committed is unknown.
        clear_run_rows(plan.to_run)
        raise
    if not load_only:
        commit_run(databases, plan.to_run)
    return RunSummary(
        _job_name(target),
        run_node_summaries(graph, counter, held=plan.held_counts),
        load_only=load_only,
        target=target.label,
        unchecked_sources=unchecked_sources,
        ran_nothing=not plan.to_run,
    )
