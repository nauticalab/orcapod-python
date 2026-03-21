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
from typing import TYPE_CHECKING, Any, Literal

from orcapod.channels import BroadcastChannel, Channel
from orcapod.pipeline.result import OrchestratorResult
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_source_node,
)

if TYPE_CHECKING:
    import networkx as nx

    from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol
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
        observer: Optional execution observer for hooks.
        buffer_size: Channel buffer size. Defaults to 64.
    """

    def __init__(
        self,
        observer: ExecutionObserverProtocol | None = None,
        buffer_size: int = 64,
        error_policy: Literal["continue", "fail_fast"] = "continue",
    ) -> None:
        self._observer = observer
        self._buffer_size = buffer_size
        self._error_policy = error_policy

    def run(
        self,
        graph: nx.DiGraph,
        materialize_results: bool = True,
        run_id: str | None = None,
        pipeline_path: tuple[str, ...] = (),
        pipeline_snapshot_hash: str | None = None,
    ) -> OrchestratorResult:
        """Synchronous entry point — runs the async pipeline to completion.

        Args:
            graph: A NetworkX DiGraph with GraphNode objects as vertices.
            materialize_results: If True, collect all node outputs into
                the result. If False, return empty node_outputs.
            run_id: Optional run identifier.  If not provided, a UUID is
                generated automatically.
            pipeline_path: Canonical pipeline identity path (e.g.
                ``("my_pipeline",)``).  Stable across pipeline evolution.
            pipeline_snapshot_hash: Content hash of the compiled pipeline
                structure at the time of this run.

        Returns:
            OrchestratorResult with node outputs.
        """
        return asyncio.run(
            self._run_async(
                graph,
                materialize_results,
                run_id=run_id,
                pipeline_path=pipeline_path,
                pipeline_snapshot_hash=pipeline_snapshot_hash,
            )
        )

    async def run_async(
        self,
        graph: nx.DiGraph,
        materialize_results: bool = True,
        run_id: str | None = None,
        pipeline_path: tuple[str, ...] = (),
        pipeline_snapshot_hash: str | None = None,
    ) -> OrchestratorResult:
        """Async entry point for callers already inside an event loop.

        Args:
            graph: A NetworkX DiGraph with GraphNode objects as vertices.
            materialize_results: If True, collect all node outputs.
            run_id: Optional run identifier.  If not provided, a UUID is
                generated automatically.
            pipeline_path: Canonical pipeline identity path (e.g.
                ``("my_pipeline",)``).  Stable across pipeline evolution.
            pipeline_snapshot_hash: Content hash of the compiled pipeline
                structure at the time of this run.

        Returns:
            OrchestratorResult with node outputs.
        """
        return await self._run_async(
            graph,
            materialize_results,
            run_id=run_id,
            pipeline_path=pipeline_path,
            pipeline_snapshot_hash=pipeline_snapshot_hash,
        )

    async def _run_async(
        self,
        graph: nx.DiGraph,
        materialize_results: bool,
        run_id: str | None = None,
        pipeline_path: tuple[str, ...] = (),
        pipeline_snapshot_hash: str | None = None,
    ) -> OrchestratorResult:
        """Core async logic: wire channels, launch tasks, collect results."""
        import networkx as nx

        run_id = run_id or str(uuid.uuid4())
        if self._observer is not None:
            self._observer.on_run_start(
                run_id,
                pipeline_path=pipeline_path,
                pipeline_snapshot_hash=pipeline_snapshot_hash,
            )

        try:
            topo_order = list(nx.topological_sort(graph))
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
            collectors: dict[Any, list[tuple[TagProtocol, PacketProtocol]]] = {}
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
                        tg.create_task(
                            node.async_execute(writer, observer=self._observer)
                        )
                    elif is_function_node(node):
                        predecessors = in_edges.get(node, [])
                        if len(predecessors) != 1:
                            raise ValueError(
                                f"FunctionNode expects exactly 1 upstream, "
                                f"got {len(predecessors)}"
                            )
                        input_reader = edge_readers[(predecessors[0], node)]
                        tg.create_task(
                            node.async_execute(
                                input_reader, writer, observer=self._observer
                            )
                        )
                    elif is_operator_node(node):
                        predecessors = in_edges.get(node, [])
                        # Sort by node.upstreams order for non-commutative operators
                        upstream_order = {id(s): i for i, s in enumerate(node.upstreams)}
                        sorted_preds = sorted(
                            predecessors,
                            key=lambda p: upstream_order.get(id(p), 0),
                        )
                        input_readers = [
                            edge_readers[(upstream, node)]
                            for upstream in sorted_preds
                        ]
                        tg.create_task(
                            node.async_execute(
                                input_readers, writer, observer=self._observer
                            )
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
        finally:
            if self._observer is not None:
                self._observer.on_run_end(run_id)


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
