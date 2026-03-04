"""Async pipeline orchestrator for push-based channel execution.

Compiles a ``GraphTracker``'s DAG into channels and launches all nodes
concurrently via ``asyncio.TaskGroup``.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from orcapod.channels import BroadcastChannel, Channel
from orcapod.core.static_output_pod import StaticOutputPod
from orcapod.core.tracker import GraphTracker, SourceNode
from orcapod.types import PipelineConfig

if TYPE_CHECKING:
    import networkx as nx

    from orcapod.core.streams.arrow_table_stream import ArrowTableStream
    from orcapod.protocols.core_protocols import PacketProtocol, StreamProtocol, TagProtocol

logger = logging.getLogger(__name__)


class AsyncPipelineOrchestrator:
    """Executes a compiled DAG asynchronously using channels and TaskGroup.

    After ``GraphTracker.compile()``, the orchestrator:

    1. Identifies source, intermediate, and terminal nodes.
    2. Creates bounded channels (or broadcast channels for fan-out) between
       connected nodes.
    3. Launches every node's ``async_execute`` concurrently.
    4. Collects the terminal node's output and materializes it as a stream.
    """

    def run(
        self,
        tracker: GraphTracker,
        config: PipelineConfig | None = None,
    ) -> StreamProtocol:
        """Synchronous entry point — runs the async pipeline and returns the result.

        Args:
            tracker: A compiled ``GraphTracker`` whose ``_node_lut`` and
                ``_graph_edges`` describe the DAG.
            config: Pipeline configuration (buffer sizes, concurrency).

        Returns:
            An ``ArrowTableStream`` containing all (tag, packet) pairs
            produced by the terminal node.
        """
        config = config or PipelineConfig()
        return asyncio.run(self._run_async(tracker, config))

    async def run_async(
        self,
        tracker: GraphTracker,
        config: PipelineConfig | None = None,
    ) -> StreamProtocol:
        """Async entry point for callers already inside an event loop.

        Args:
            tracker: A compiled ``GraphTracker``.
            config: Pipeline configuration.

        Returns:
            An ``ArrowTableStream`` of the terminal node's output.
        """
        config = config or PipelineConfig()
        return await self._run_async(tracker, config)

    async def _run_async(
        self,
        tracker: GraphTracker,
        config: PipelineConfig,
    ) -> StreamProtocol:
        """Core async logic: wire channels, launch tasks, collect results."""
        import networkx as nx

        # Build directed graph from edges
        G = nx.DiGraph()
        for upstream_hash, downstream_hash in tracker._graph_edges:
            G.add_edge(upstream_hash, downstream_hash)

        # Add isolated nodes (sources with no downstream edges)
        for node_hash in tracker._node_lut:
            if node_hash not in G:
                G.add_node(node_hash)

        topo_order = list(nx.topological_sort(G))

        # Identify terminal nodes (no outgoing edges)
        terminal_hashes = [h for h in topo_order if G.out_degree(h) == 0]
        if not terminal_hashes:
            raise ValueError("DAG has no terminal nodes")

        # For multiple terminals, we use the last one in topological order
        # (the one furthest downstream)
        terminal_hash = terminal_hashes[-1]

        buf = config.channel_buffer_size

        # Build channel mapping:
        # For each edge (upstream_hash → downstream_hash), create a channel.
        # If an upstream feeds multiple downstreams (fan-out), use BroadcastChannel.

        # Count outgoing edges per node
        out_edges: dict[str, list[str]] = defaultdict(list)
        for upstream_hash, downstream_hash in tracker._graph_edges:
            out_edges[upstream_hash].append(downstream_hash)

        # Count incoming edges per node (to know how many input channels)
        in_edges: dict[str, list[str]] = defaultdict(list)
        for upstream_hash, downstream_hash in tracker._graph_edges:
            in_edges[downstream_hash].append(upstream_hash)

        # For each upstream node, create either a Channel or BroadcastChannel
        # upstream_hash → Channel or BroadcastChannel
        node_output_channels: dict[str, Channel | BroadcastChannel] = {}

        # edge (upstream, downstream) → reader
        edge_readers: dict[tuple[str, str], Any] = {}

        for upstream_hash, downstreams in out_edges.items():
            if len(downstreams) == 1:
                # Simple channel
                ch = Channel(buffer_size=buf)
                node_output_channels[upstream_hash] = ch
                edge_readers[(upstream_hash, downstreams[0])] = ch.reader
            else:
                # Fan-out: use BroadcastChannel
                bch = BroadcastChannel(buffer_size=buf)
                node_output_channels[upstream_hash] = bch
                for ds_hash in downstreams:
                    edge_readers[(upstream_hash, ds_hash)] = bch.add_reader()

        # Terminal node output channel
        terminal_ch = Channel(buffer_size=buf)
        node_output_channels[terminal_hash] = terminal_ch

        # Now launch all nodes
        async with asyncio.TaskGroup() as tg:
            for node_hash in topo_order:
                node = tracker._node_lut[node_hash]

                # Gather input readers for this node (from its upstream edges)
                input_readers = []
                for upstream_hash in in_edges.get(node_hash, []):
                    reader = edge_readers[(upstream_hash, node_hash)]
                    input_readers.append(reader)

                # Get the output writer
                output_channel = node_output_channels.get(node_hash)
                if output_channel is None:
                    # Node with no downstream and not the terminal — still needs
                    # an output channel (it will just be discarded)
                    output_channel = Channel(buffer_size=buf)
                    node_output_channels[node_hash] = output_channel

                writer = output_channel.writer

                tg.create_task(
                    node.async_execute(input_readers, writer)
                )

        # Collect terminal output
        terminal_rows = await terminal_ch.reader.collect()

        # Materialize into a stream
        return StaticOutputPod._materialize_to_stream(terminal_rows)
