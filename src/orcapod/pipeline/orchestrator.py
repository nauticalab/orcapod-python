"""Async pipeline orchestrator for push-based channel execution.

Walks a compiled ``Pipeline``'s persistent node graph and launches all
nodes concurrently via ``asyncio.TaskGroup``, wiring them together with
bounded channels.  After execution, results are available in the
pipeline databases via the usual ``get_all_records()`` / ``as_source()``
accessors on each persistent node.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from orcapod.channels import BroadcastChannel, Channel
from orcapod.types import PipelineConfig

if TYPE_CHECKING:
    import networkx as nx

    from orcapod.pipeline.graph import Pipeline

logger = logging.getLogger(__name__)


class AsyncPipelineOrchestrator:
    """Execute a compiled ``Pipeline`` asynchronously using channels.

    After ``Pipeline.compile()``, the orchestrator:

    1. Walks ``Pipeline._node_graph`` (persistent nodes) in topological
       order.
    2. Creates bounded channels (or broadcast channels for fan-out)
       between connected nodes.
    3. Launches every node's ``async_execute`` concurrently via
       ``asyncio.TaskGroup``.

    Results are written to the pipeline databases by the persistent
    nodes themselves (``PersistentFunctionNode``, ``PersistentOperatorNode``
    in LOG mode, etc.).  After ``run()`` returns, callers retrieve data
    via ``pipeline.<label>.get_all_records()``.
    """

    def run(
        self,
        pipeline: Pipeline,
        config: PipelineConfig | None = None,
    ) -> None:
        """Synchronous entry point — runs the async pipeline to completion.

        Args:
            pipeline: A compiled ``Pipeline`` whose ``_node_graph``
                describes the persistent DAG.
            config: Pipeline configuration (buffer sizes, concurrency).
        """
        config = config or PipelineConfig()
        asyncio.run(self._run_async(pipeline, config))

    async def run_async(
        self,
        pipeline: Pipeline,
        config: PipelineConfig | None = None,
    ) -> None:
        """Async entry point for callers already inside an event loop.

        Args:
            pipeline: A compiled ``Pipeline``.
            config: Pipeline configuration.
        """
        config = config or PipelineConfig()
        await self._run_async(pipeline, config)

    async def _run_async(
        self,
        pipeline: Pipeline,
        config: PipelineConfig,
    ) -> None:
        """Core async logic: wire channels between persistent nodes, launch tasks."""
        import networkx as nx

        if not pipeline._compiled:
            pipeline.compile()

        G: nx.DiGraph | None = pipeline._node_graph
        assert G is not None, "Pipeline must be compiled before async execution"

        topo_order = list(nx.topological_sort(G))

        buf = config.channel_buffer_size

        # Build edge maps keyed by node object identity
        out_edges: dict[Any, list[Any]] = defaultdict(list)
        in_edges: dict[Any, list[Any]] = defaultdict(list)
        for upstream_node, downstream_node in G.edges():
            out_edges[upstream_node].append(downstream_node)
            in_edges[downstream_node].append(upstream_node)

        # Create channels for each edge.
        # If a node fans out to multiple downstreams, use BroadcastChannel.
        # node → output Channel or BroadcastChannel
        node_output_channels: dict[Any, Channel | BroadcastChannel] = {}
        # (upstream, downstream) → reader
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

        # Terminal nodes (no outgoing edges) need a sink channel so their
        # async_execute has somewhere to write.  We drain it after execution.
        terminal_nodes = [n for n in topo_order if G.out_degree(n) == 0]
        terminal_channels: list[Channel] = []
        for node in terminal_nodes:
            if node not in node_output_channels:
                ch = Channel(buffer_size=buf)
                node_output_channels[node] = ch
                terminal_channels.append(ch)

        # Launch all nodes concurrently
        async with asyncio.TaskGroup() as tg:
            for node in topo_order:
                # Gather input readers from upstream edges
                input_readers = [
                    edge_readers[(upstream, node)]
                    for upstream in in_edges.get(node, [])
                ]

                writer = node_output_channels[node].writer

                tg.create_task(node.async_execute(input_readers, writer))

        # Drain terminal channels so nothing is left buffered
        for ch in terminal_channels:
            await ch.reader.collect()
