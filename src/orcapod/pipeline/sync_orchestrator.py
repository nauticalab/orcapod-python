"""Synchronous pipeline orchestrator.

Walks a compiled pipeline's node graph topologically, executing each node
with materialized buffers and per-packet observer hooks for function nodes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.pipeline.observer import NoOpObserver
from orcapod.pipeline.result import OrchestratorResult
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_source_node,
)

if TYPE_CHECKING:
    import networkx as nx

    from orcapod.pipeline.observer import ExecutionObserver
    from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol

logger = logging.getLogger(__name__)


class SyncPipelineOrchestrator:
    """Execute a compiled pipeline synchronously with observer hooks.

    Walks the node graph in topological order. For each node:

    - **SourceNode**: materializes ``iter_packets()`` into a buffer.
      The source node self-caches on first call.
    - **FunctionNode**: per-packet execution with cache lookup and
      observer hooks. ``execute_packet`` handles computation +
      function-level memoization.
    - **OperatorNode**: bulk execution via ``node.execute()``.

    The orchestrator returns an ``OrchestratorResult`` with all node outputs.

    Args:
        observer: Optional execution observer for hooks. Defaults to
            ``NoOpObserver``.
    """

    def __init__(self, observer: "ExecutionObserver | None" = None) -> None:
        self._observer = observer or NoOpObserver()

    def run(
        self,
        graph: "nx.DiGraph",
        materialize_results: bool = True,
    ) -> OrchestratorResult:
        """Execute the node graph synchronously.

        Args:
            graph: A NetworkX DiGraph with GraphNode objects as vertices.
            materialize_results: If True, keep all node outputs in memory
                and return them. If False, discard buffers after downstream
                consumption (only DB-persisted results survive).

        Returns:
            OrchestratorResult with node outputs.
        """
        import networkx as nx

        topo_order = list(nx.topological_sort(graph))
        buffers: dict[Any, list[tuple[TagProtocol, PacketProtocol]]] = {}
        processed: set[Any] = set()

        for node in topo_order:
            if is_source_node(node):
                buffers[node] = self._execute_source(node)
            elif is_function_node(node):
                upstream_buffer = self._gather_upstream(node, graph, buffers)
                buffers[node] = self._execute_function(node, upstream_buffer)
            elif is_operator_node(node):
                upstream_buffers = self._gather_upstream_multi(node, graph, buffers)
                buffers[node] = self._execute_operator(node, upstream_buffers)
            else:
                raise TypeError(
                    f"Unknown node type: {getattr(node, 'node_type', None)!r}"
                )

            processed.add(node)

            if not materialize_results:
                self._gc_buffers(node, graph, buffers, processed)

        return OrchestratorResult(node_outputs=buffers)

    def _execute_source(self, node: Any) -> list[tuple[Any, Any]]:
        """Execute a source node: materialize its packets."""
        self._observer.on_node_start(node)
        output = list(node.iter_packets())
        self._observer.on_node_end(node)
        return output

    def _execute_function(
        self, node: Any, upstream_buffer: list[tuple[Any, Any]]
    ) -> list[tuple[Any, Any]]:
        """Execute a function node with per-packet hooks."""
        self._observer.on_node_start(node)

        upstream_entries = [
            (tag, packet, node.compute_pipeline_entry_id(tag, packet))
            for tag, packet in upstream_buffer
        ]
        entry_ids = [eid for _, _, eid in upstream_entries]

        cached = node.get_cached_results(entry_ids=entry_ids)

        output: list[tuple[Any, Any]] = []
        for tag, packet, entry_id in upstream_entries:
            self._observer.on_packet_start(node, tag, packet)
            if entry_id in cached:
                tag_out, result = cached[entry_id]
                self._observer.on_packet_end(node, tag, packet, result, cached=True)
                output.append((tag_out, result))
            else:
                tag_out, result = node.execute_packet(tag, packet)
                self._observer.on_packet_end(node, tag, packet, result, cached=False)
                if result is not None:
                    output.append((tag_out, result))

        self._observer.on_node_end(node)
        return output

    def _execute_operator(
        self, node: Any, upstream_buffers: list[tuple[list[tuple[Any, Any]], Any]]
    ) -> list[tuple[Any, Any]]:
        """Execute an operator node: bulk stream processing."""
        self._observer.on_node_start(node)

        cached = node.get_cached_output()
        if cached is not None:
            output = list(cached.iter_packets())
        else:
            input_streams = [
                self._materialize_as_stream(buf, upstream_node)
                for buf, upstream_node in upstream_buffers
            ]
            output = node.execute(*input_streams)

        self._observer.on_node_end(node)
        return output

    @staticmethod
    def _gather_upstream(
        node: Any, graph: "nx.DiGraph", buffers: dict[Any, list[tuple[Any, Any]]]
    ) -> list[tuple[Any, Any]]:
        """Gather a single upstream buffer (for function nodes)."""
        predecessors = list(graph.predecessors(node))
        if len(predecessors) != 1:
            raise ValueError(
                f"FunctionNode expects exactly 1 upstream, got {len(predecessors)}"
            )
        return buffers[predecessors[0]]

    @staticmethod
    def _gather_upstream_multi(
        node: Any, graph: "nx.DiGraph", buffers: dict[Any, list[tuple[Any, Any]]]
    ) -> list[tuple[list[tuple[Any, Any]], Any]]:
        """Gather multiple upstream buffers with their nodes (for operators).

        Returns list of (buffer, upstream_node) tuples preserving the
        order that matches the operator's input_streams order.
        """
        predecessors = list(graph.predecessors(node))
        upstream_order = {id(upstream): i for i, upstream in enumerate(node.upstreams)}
        sorted_preds = sorted(
            predecessors,
            key=lambda p: upstream_order.get(id(p), 0),
        )
        return [(buffers[p], p) for p in sorted_preds]

    @staticmethod
    def _materialize_as_stream(buf: list[tuple[Any, Any]], upstream_node: Any) -> Any:
        """Wrap a (tag, packet) buffer as an ArrowTableStream.

        Uses the same column selection pattern as
        ``StaticOutputOperatorPod._materialize_to_stream``: system_tags
        for tags, source info for packets.

        Args:
            buf: List of (tag, packet) tuples.
            upstream_node: The node that produced this buffer (used to
                determine tag column names).

        Returns:
            An ArrowTableStream.
        """
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream
        from orcapod.utils import arrow_utils
        from orcapod.utils.lazy_module import LazyModule

        pa = LazyModule("pyarrow")

        if not buf:
            # Build an empty stream with the correct schema from the upstream node
            tag_schema, packet_schema = upstream_node.output_schema(
                columns={"system_tags": True, "source": True}
            )
            type_converter = upstream_node.data_context.type_converter
            empty_fields = {}
            for name, py_type in {**tag_schema, **packet_schema}.items():
                arrow_type = type_converter.python_type_to_arrow_type(py_type)
                empty_fields[name] = pa.array([], type=arrow_type)
            empty_table = pa.table(empty_fields)
            tag_keys = upstream_node.keys()[0]
            return ArrowTableStream(
                empty_table,
                tag_columns=tag_keys,
                producer=upstream_node.producer,
                upstreams=upstream_node.upstreams,
            )

        tag_tables = [tag.as_table(columns={"system_tags": True}) for tag, _ in buf]
        packet_tables = [pkt.as_table(columns={"source": True}) for _, pkt in buf]

        combined_tags = pa.concat_tables(tag_tables)
        combined_packets = pa.concat_tables(packet_tables)

        user_tag_keys = tuple(buf[0][0].keys())
        source_info = buf[0][1].source_info()

        full_table = arrow_utils.hstack_tables(combined_tags, combined_packets)

        # Pass the upstream node's producer and upstreams so the
        # materialized stream inherits the correct identity_structure
        # and pipeline_identity_structure (via StreamBase delegation).
        # This ensures downstream operators produce correct system tag
        # column names (which embed pipeline hashes of their inputs).
        producer = upstream_node.producer
        upstreams = upstream_node.upstreams

        return ArrowTableStream(
            full_table,
            tag_columns=user_tag_keys,
            source_info=source_info,
            producer=producer,
            upstreams=upstreams,
        )

    @staticmethod
    def _gc_buffers(
        current_node: Any,
        graph: "nx.DiGraph",
        buffers: dict[Any, list[tuple[Any, Any]]],
        processed: set[Any],
    ) -> None:
        """Discard buffers no longer needed by any unprocessed downstream."""
        for pred in graph.predecessors(current_node):
            if pred not in buffers:
                continue
            all_successors_done = all(
                succ in processed for succ in graph.successors(pred)
            )
            if all_successors_done:
                del buffers[pred]
