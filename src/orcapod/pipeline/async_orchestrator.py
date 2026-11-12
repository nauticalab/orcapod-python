"""Async pipeline orchestrator for push-based channel execution.

Walks a compiled pipeline's node graph and launches all nodes concurrently
via ``asyncio.TaskGroup``, wiring them together with bounded channels.
Uses TypeGuard dispatch with tightened per-type async_execute signatures.
"""

from __future__ import annotations

import asyncio
import uuid
import logging
from collections import defaultdict
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any, Literal

from orcapod.channels import BroadcastChannel, Channel
from orcapod.errors import StoreError, StoreWriteError, TypeIdentityError
from orcapod.pipeline.result import OrchestratorResult
from orcapod.utils.store_errors import keep_store_error
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_side_effect_function_node,
    is_side_effect_node,
    is_source_node,
)

if TYPE_CHECKING:
    from orcapod.pipeline.dag import GraphProtocol
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol

logger = logging.getLogger(__name__)


class AsyncPipelineOrchestrator:
    """Execute a compiled pipeline asynchronously using channels.

    After compilation, the orchestrator:

    1. Walks the node graph in topological order.
    2. Creates bounded channels (or broadcast channels for fan-out)
       between connected nodes.
    3. Launches every node's ``async_execute`` concurrently via
       ``asyncio.TaskGroup``, using TypeGuard dispatch for per-type
       signatures.

    Args:
        buffer_size: Channel buffer size. Defaults to 64.
    """

    def __init__(
        self,
        buffer_size: int = 64,
        error_policy: Literal["continue", "fail_fast"] = "continue",
    ) -> None:
        self._buffer_size = buffer_size
        self._error_policy = error_policy

    def run(
        self,
        graph: "GraphProtocol[Any]",
        *,
        observer: ExecutionObserverProtocol | None = None,
        materialize_results: bool = True,
        run_id: str | None = None,
        pipeline_uri: str = "",
    ) -> OrchestratorResult:
        """Synchronous entry point — runs the async pipeline to completion.

        Args:
            graph: A ``GraphProtocol`` DAG with GraphNode objects as vertices.
            observer: Optional execution observer forwarded to nodes.
            materialize_results: If True, collect all node outputs into
                the result. If False, return empty node_outputs.
            run_id: Optional run identifier.  If not provided, a UUID is
                generated automatically.
            pipeline_uri: Opaque URI string identifying this pipeline run
                (e.g. ``"my_pipeline@a1b2c3d4e5f6a1b2"``).  Forwarded
                verbatim to ``observer.on_run_start``.

        Returns:
            OrchestratorResult with node outputs.
        """
        return asyncio.run(
            self._run_async(
                graph,
                materialize_results,
                observer=observer,
                run_id=run_id,
                pipeline_uri=pipeline_uri,
            )
        )

    async def run_async(
        self,
        graph: "GraphProtocol[Any]",
        *,
        observer: ExecutionObserverProtocol | None = None,
        materialize_results: bool = True,
        run_id: str | None = None,
        pipeline_uri: str = "",
    ) -> OrchestratorResult:
        """Async entry point for callers already inside an event loop.

        Args:
            graph: A ``GraphProtocol`` DAG with GraphNode objects as vertices.
            observer: Optional execution observer forwarded to nodes.
            materialize_results: If True, collect all node outputs.
            run_id: Optional run identifier.  If not provided, a UUID is
                generated automatically.
            pipeline_uri: Opaque URI string identifying this pipeline run.
                Forwarded verbatim to ``observer.on_run_start``.

        Returns:
            OrchestratorResult with node outputs.
        """
        return await self._run_async(
            graph,
            materialize_results,
            observer=observer,
            run_id=run_id,
            pipeline_uri=pipeline_uri,
        )

    async def _run_async(
        self,
        graph: "GraphProtocol[Any]",
        materialize_results: bool,
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
        pipeline_uri: str = "",
    ) -> OrchestratorResult:
        """Core async logic: wire channels, launch tasks, collect results."""
        from orcapod.pipeline.observer import NoOpObserver

        run_id = run_id or str(uuid.uuid4())
        effective_observer = observer if observer is not None else NoOpObserver()
        effective_observer.on_run_start(run_id, pipeline_uri=pipeline_uri)

        topo_order: list[Any] = []
        store_error: StoreError | TypeIdentityError | None = None
        try:
            topo_order = list(graph.topological_sort())
            buf = self._buffer_size

            # Build edge maps
            out_edges: dict[Any, list[Any]] = defaultdict(list)
            in_edges: dict[Any, list[Any]] = defaultdict(list)
            for upstream_node, downstream_node in graph.edges():
                out_edges[upstream_node].append(downstream_node)
                in_edges[downstream_node].append(upstream_node)

            # Create channels for each edge
            node_output_channels: dict[Any, Channel | BroadcastChannel] = {}
            edge_readers: dict[tuple[Any, Any], Any] = {}

            for node, downstreams in out_edges.items():
                if len(downstreams) == 1:
                    ch = Channel(buffer_size=buf)
                    node_output_channels[node] = ch
                    edge_readers[(node, downstreams[0])] = ch.reader
                else:
                    bch = BroadcastChannel(buffer_size=buf)
                    node_output_channels[node] = bch
                    for ds in downstreams:
                        edge_readers[(node, ds)] = bch.add_reader()

            # Terminal nodes need sink channels
            terminal_channels: list[Channel] = []
            for node in topo_order:
                if node not in node_output_channels:
                    ch = Channel(buffer_size=buf)
                    node_output_channels[node] = ch
                    terminal_channels.append(ch)

            # Result collection
            collectors: dict[Any, list[tuple[TagProtocol, DataProtocol]]] = {}
            if materialize_results:
                for node in topo_order:
                    collectors[node] = []

            # Launch all nodes concurrently
            async with asyncio.TaskGroup() as tg:
                for node in topo_order:
                    writer = node_output_channels[node].writer

                    if materialize_results:
                        collector = collectors[node]
                        writer = _CollectingWriter(writer, collector)

                    if is_source_node(node):
                        _start_node(
                            tg,
                            node,
                            node.async_execute(writer, observer=effective_observer),
                        )
                    elif is_function_node(node) or is_side_effect_function_node(node):
                        predecessors = in_edges.get(node, [])
                        if len(predecessors) != 1:
                            raise ValueError(
                                f"FunctionNode expects exactly 1 upstream, got {len(predecessors)}"
                            )
                        input_reader = edge_readers[(predecessors[0], node)]
                        _start_node(
                            tg,
                            node,
                            node.async_execute(
                                input_reader,
                                writer,
                                observer=effective_observer,
                                run_id=run_id,
                            ),
                        )
                    elif is_operator_node(node):
                        predecessors = in_edges.get(node, [])
                        # Sort by node.upstreams order for non-commutative operators
                        upstream_order = {
                            id(s): i for i, s in enumerate(node.upstreams)
                        }
                        sorted_preds = sorted(
                            predecessors,
                            key=lambda p: upstream_order.get(id(p), 0),
                        )
                        input_readers = [
                            edge_readers[(upstream, node)] for upstream in sorted_preds
                        ]
                        _start_node(
                            tg,
                            node,
                            node.async_execute(
                                input_readers, writer, observer=effective_observer
                            ),
                        )
                    elif is_side_effect_node(node):
                        predecessors = in_edges.get(node, [])
                        if not predecessors:
                            raise ValueError(
                                "SideEffectNode expects exactly 1 upstream, got 0"
                            )
                        input_reader = edge_readers[(predecessors[0], node)]
                        _start_node(
                            tg,
                            node,
                            node.async_execute(
                                [input_reader],
                                writer,
                                observer=effective_observer,
                                run_id=run_id,
                            ),
                        )
                    else:
                        raise TypeError(
                            f"Unknown node type: {getattr(node, 'node_type', None)!r}"
                        )

                # Drain terminal channels concurrently
                for ch in terminal_channels:
                    tg.create_task(ch.reader.collect())

            return OrchestratorResult(
                node_outputs=collectors if materialize_results else {}
            )
        except ExceptionGroup as group:
            # A failed store read or write stops the run. Raise it on its own, as the
            # sync orchestrator does, rather than inside the TaskGroup's exception
            # group.
            store_error = _single_store_error(group, topo_order)
            if store_error is not None:
                raise store_error
            # A stored row that cannot be decoded as its class stops the run too,
            # raised on its own as the sync orchestrator raises it (ITL-881).
            identity_errors = [
                exc for exc in _leaves(group) if isinstance(exc, TypeIdentityError)
            ]
            if identity_errors:
                store_error = identity_errors[0]
                raise store_error
            raise
        finally:
            # A failing on_run_end must not replace a failed store read or write.
            with keep_store_error(store_error, "Observer on_run_end"):
                effective_observer.on_run_end(run_id)


def _start_node(
    tg: asyncio.TaskGroup, node: Any, work: Coroutine[Any, Any, None]
) -> None:
    """Start a node's coroutine in ``tg``, naming the node on the store errors it raises.

    A store error raised on its own or inside an exception group (a node that runs
    its own task group) gets the node's label when it has none.

    Args:
        tg: The run's task group.
        node: The node the coroutine runs.
        work: The node's ``async_execute`` coroutine.
    """

    async def run() -> None:
        try:
            await work
        except StoreError as exc:
            # Name the node the failure belongs to, if the raising site did not,
            # so _single_store_error can order it like the sync orchestrator.
            exc.add_context(node_label=node.label)
            raise
        except BaseExceptionGroup as group:
            # A node that runs its own task group raises its store errors inside
            # a group; name the node on each and re-raise the group unchanged.
            for store_error in StoreError.in_group(group):
                store_error.add_context(node_label=node.label)
            raise

    tg.create_task(run())


def _single_store_error(
    group: ExceptionGroup, topo_order: list[Any]
) -> StoreError | None:
    """Pick the one ``StoreError`` an async run raises from its exception group.

    Concurrent node tasks can fail together, so the ``TaskGroup``'s group may hold
    several store errors, read and write ones alike, and other exceptions. The
    caller gets a single ``StoreError``, as from the sync orchestrator: the one
    whose node comes first in topological order (the one the sync orchestrator
    would meet first, when both failures happen), with ties and node-less errors
    broken by their order in the group. Every other exception stays attached for
    diagnosis: each is noted on the chosen error, other store errors are also
    logged at WARNING, and the whole group is the chosen error's ``__context__``.
    Its ``__cause__`` stays the backend error.

    Args:
        group: The exception group the run's ``TaskGroup`` raised.
        topo_order: The run's nodes in topological order.

    Returns:
        The store error to raise, or ``None`` when the group holds none.
    """
    store_errors = StoreError.in_group(group)
    if not store_errors:
        return None
    rank = {getattr(node, "label", None): i for i, node in enumerate(topo_order)}
    chosen = min(store_errors, key=lambda err: rank.get(err.node_label, len(rank)))
    for other in _leaves(group):
        if other is chosen:
            continue
        if isinstance(other, StoreError):
            kind = "write" if isinstance(other, StoreWriteError) else "read"
            logger.warning(
                "Another store %s failed in the same run: %s",
                kind,
                other,
                exc_info=other,
            )
            chosen.add_note(f"Another store {kind} failed in the same run: {other}")
        else:
            chosen.add_note(
                f"Another node failed in the same run: {type(other).__name__}: {other}"
            )
    return chosen


def _leaves(group: BaseExceptionGroup) -> list[BaseException]:
    """Return every non-group exception in ``group``, nested groups flattened."""
    found: list[BaseException] = []
    for exc in group.exceptions:
        if isinstance(exc, BaseExceptionGroup):
            found.extend(_leaves(exc))
        else:
            found.append(exc)
    return found


class _CollectingWriter:
    """Wrapper that collects items while forwarding to real writer."""

    def __init__(self, writer: Any, collector: list) -> None:
        self._writer = writer
        self._collector = collector

    async def send(self, item: Any) -> None:
        self._collector.append(item)
        await self._writer.send(item)

    async def close(self) -> None:
        await self._writer.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._writer, name)
