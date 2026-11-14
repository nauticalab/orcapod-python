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

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from orcapod.errors import StoreError, StoreWriteError
from orcapod.pipeline.run_summary import (
    RunCountingObserver,
    RunSummary,
    run_node_summaries,
)
from orcapod.utils.store_errors import flush_best_effort

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


def _stores(nodes: list[Any]) -> list[ArrowDatabaseProtocol]:
    """The distinct stores the given nodes write to: pipeline and result databases."""
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
    depends on).

    Args:
        graph: The run's node graph (``topological_sort``, ``predecessors``).
        load_only: Whether the run is load-only (a node filled by a load-only run is
            current only for one).
        outside: Streams of ``graph`` that are not job nodes.

    Returns:
        The plan.
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


def commit_run(stores: Sequence[ArrowDatabaseProtocol], ran: Sequence[Any]) -> None:
    """Commit a run's writes at its end; a failed commit clears the nodes that ran.

    A run whose commit failed did not complete, so no node it ran holds its rows
    (ITL-1095); nodes it held keep the rows of their earlier run.

    Raises:
        StoreError: The commit's own store error.
        StoreWriteError: If committing failed for another reason.
    """
    for store in stores:
        try:
            store.flush()
        except StoreError:
            clear_run_rows(ran)
            raise
        except Exception as exc:
            clear_run_rows(ran)
            raise StoreWriteError(store.base_path, flush=True) from exc
        except BaseException:
            # An interruption during the commit leaves its outcome unknown too.
            clear_run_rows(ran)
            raise


def run_with_dependencies(target: Any, *, load_only: bool) -> RunSummary:
    """Run ``target`` and every job node it depends on that holds no current run.

    The dependencies are found through ``upstreams``; the plan (``plan_run``) is the
    one ``job.run()`` makes over the job: a dependency that holds a current run is not
    run and its rows feed the nodes after it, a stream outside the job is read, every
    other node runs once, in dependency order, on the ``SyncPipelineOrchestrator``
    (error policy ``"continue"``). The run ends with a commit of every store it wrote
    to, also, best-effort, when it fails; a load-only run commits nothing (it writes
    nothing, and pending writes of others stay pending). A run that raises leaves no
    node it ran holding rows.

    Args:
        target: The job node whose ``run()`` this is.
        load_only: Whether to load stored results only.

    Returns:
        The run's summary, one entry per function node of the graph.

    Raises:
        StoreWriteError: If committing the run's writes failed.
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
    stores = _stores(plan.to_run)
    counter = RunCountingObserver()
    try:
        _ = SyncPipelineOrchestrator().run(
            plan.dag, observer=counter, load_only=load_only, held_rows=plan.held_rows
        )
    except BaseException as run_error:
        if isinstance(run_error, Exception) and not load_only:
            # Commit the writes that succeeded before the failure, as job.run() does;
            # a load-only run writes nothing, so it commits nothing (ITL-1095).
            flush_best_effort(run_error, *stores)
        # A run that raised leaves no node it ran holding rows: whether their writes
        # were committed is unknown.
        clear_run_rows(plan.to_run)
        raise
    if not load_only:
        commit_run(stores, plan.to_run)
    return RunSummary(
        _job_name(target),
        run_node_summaries(graph, counter, held=plan.held_counts),
        load_only=load_only,
        target=target.label,
    )
