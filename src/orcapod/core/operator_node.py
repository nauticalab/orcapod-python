from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.channels import Channel, ReadableChannel, WritableChannel
from orcapod.config import Config
from orcapod.core.static_output_pod import StaticOutputPod
from orcapod.core.streams.base import StreamBase
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
from orcapod.protocols.core_protocols import (
    PacketProtocol,
    StreamProtocol,
    TagProtocol,
    TrackerManagerProtocol,
)
from orcapod.protocols.core_protocols.operator_pod import OperatorPodProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import constants
from orcapod.types import CacheMode, ColumnConfig, Schema
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class OperatorNode(StreamBase):
    """
    Non-persistent stream node representing an operator invocation.

    Provides the core stream interface (identity, schema, iteration) without
    any database persistence. Subclass ``PersistentOperatorNode`` adds DB-backed
    storage and record deduplication.
    """

    node_type = "operator"

    def __init__(
        self,
        operator: OperatorPodProtocol,
        input_streams: tuple[StreamProtocol, ...] | list[StreamProtocol],
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ):
        if tracker_manager is None:
            tracker_manager = DEFAULT_TRACKER_MANAGER
        self.tracker_manager = tracker_manager

        self._operator = operator
        self._input_streams = tuple(input_streams)

        super().__init__(
            label=label,
            data_context=data_context,
            config=config,
        )

        # Validate inputs eagerly
        self._operator.validate_inputs(*self._input_streams)

        # Stream-level caching state
        self._cached_output_stream: StreamProtocol | None = None
        self._cached_output_table: pa.Table | None = None
        self._set_modified_time(None)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        return (self._operator, self._operator.argument_symmetry(self._input_streams))

    def pipeline_identity_structure(self) -> Any:
        return (self._operator, self._operator.argument_symmetry(self._input_streams))

    # ------------------------------------------------------------------
    # Stream interface
    # ------------------------------------------------------------------

    @property
    def producer(self) -> OperatorPodProtocol:
        return self._operator

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        return self._input_streams

    @upstreams.setter
    def upstreams(self, value: tuple[StreamProtocol, ...]) -> None:
        self._input_streams = value

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        tag_schema, packet_schema = self.output_schema(
            columns=columns, all_info=all_info
        )
        return tuple(tag_schema.keys()), tuple(packet_schema.keys())

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._operator.output_schema(
            *self._input_streams,
            columns=columns,
            all_info=all_info,
        )

    # ------------------------------------------------------------------
    # Computation and caching
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Discard all in-memory cached state."""
        self._cached_output_stream = None
        self._cached_output_table = None
        self._update_modified_time()

    def run(self) -> None:
        """Execute the operator if stale or not yet computed."""
        if self.is_stale:
            self.clear_cache()

        if self._cached_output_stream is not None:
            return

        self._cached_output_stream = self._operator.process(
            *self._input_streams,
        )
        self._update_modified_time()

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        self.run()
        assert self._cached_output_stream is not None
        return self._cached_output_stream.iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        self.run()
        assert self._cached_output_stream is not None
        return self._cached_output_stream.as_table(columns=columns, all_info=all_info)

    # ------------------------------------------------------------------
    # Async channel execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """Delegate to the wrapped operator's async_execute.

        Passes pipeline hashes from the input streams so that
        multi-input operators can compute canonical system-tag
        column names without storing state during validation.
        """
        hashes = [s.pipeline_hash() for s in self._input_streams]
        await self._operator.async_execute(
            inputs, output, input_pipeline_hashes=hashes
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(operator={self._operator!r}, "
            f"upstreams={self._input_streams!r})"
        )


class PersistentOperatorNode(OperatorNode):
    """
    DB-backed stream node that applies an operator to input streams.

    Extends ``OperatorNode`` with:

    - Pipeline record storage with per-row deduplication
    - ``get_all_records()`` for retrieving stored results
    - ``as_source()`` for creating a ``DerivedSource`` from DB records
    - Three-tier cache mode: OFF / LOG / REPLAY

    Pipeline path structure::

        pipeline_path_prefix / operator.uri / node:{content_hash}

    Where ``content_hash`` is the data-inclusive hash that encodes both
    pipeline structure and upstream source identities, ensuring each
    unique source combination gets its own cache table.

    Cache modes
    -----------
    - **OFF** (default): compute, don't write to DB.
    - **LOG**: compute AND write to DB (append-only historical record).
    - **REPLAY**: skip computation, flow cached results downstream.
    """

    HASH_COLUMN_NAME = "_record_hash"

    def __init__(
        self,
        operator: OperatorPodProtocol,
        input_streams: tuple[StreamProtocol, ...] | list[StreamProtocol],
        pipeline_database: ArrowDatabaseProtocol,
        cache_mode: CacheMode = CacheMode.OFF,
        pipeline_path_prefix: tuple[str, ...] = (),
        tracker_manager: TrackerManagerProtocol | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ):
        super().__init__(
            operator=operator,
            input_streams=input_streams,
            tracker_manager=tracker_manager,
            label=label,
            data_context=data_context,
            config=config,
        )

        self._pipeline_database = pipeline_database
        self._pipeline_path_prefix = pipeline_path_prefix
        self._cache_mode = cache_mode

        # Use content_hash (data-inclusive) so each source combination
        # gets its own cache table.
        self._pipeline_node_hash = self.content_hash().to_string()

    @property
    def cache_mode(self) -> CacheMode:
        return self._cache_mode

    @property
    def pipeline_path(self) -> tuple[str, ...]:
        return (
            self._pipeline_path_prefix
            + self._operator.uri
            + (f"node:{self._pipeline_node_hash}",)
        )

    def _store_output_stream(self, stream: StreamProtocol) -> None:
        """Materialize stream and store in the pipeline database with per-row dedup."""
        output_table = stream.as_table(
            columns={"source": True, "system_tags": True},
        )

        # Per-row record hashes for dedup: hash(tag + packet + system_tag)
        arrow_hasher = self.data_context.arrow_hasher
        record_hashes = []
        for batch in output_table.to_batches():
            for i in range(len(batch)):
                record_hashes.append(
                    arrow_hasher.hash_table(batch.slice(i, 1)).to_hex()
                )

        output_table = output_table.add_column(
            0,
            self.HASH_COLUMN_NAME,
            pa.array(record_hashes, type=pa.large_string()),
        )

        # Store (identical rows across runs naturally deduplicate)
        self._pipeline_database.add_records(
            self.pipeline_path,
            output_table,
            record_id_column=self.HASH_COLUMN_NAME,
            skip_duplicates=True,
        )

        self._cached_output_table = output_table.drop(self.HASH_COLUMN_NAME)

    def _compute_and_store(self) -> None:
        """Compute operator output, optionally store in DB."""
        self._cached_output_stream = self._operator.process(
            *self._input_streams,
        )

        if self._cache_mode == CacheMode.OFF:
            self._update_modified_time()
            return

        self._store_output_stream(self._cached_output_stream)
        self._update_modified_time()

    def _replay_from_cache(self) -> None:
        """Load cached results from DB, skip computation.

        If no cached records exist yet, produces an empty stream with
        the correct schema (zero rows, correct columns).
        """
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        records = self._pipeline_database.get_all_records(self.pipeline_path)
        if records is None:
            # Build an empty table with the correct schema
            tag_schema, packet_schema = self.output_schema()
            type_converter = self.data_context.type_converter
            empty_fields = {}
            for name, py_type in {**tag_schema, **packet_schema}.items():
                arrow_type = type_converter.python_type_to_arrow_type(py_type)
                empty_fields[name] = pa.array([], type=arrow_type)
            records = pa.table(empty_fields)

        tag_keys = self.keys()[0]
        self._cached_output_stream = ArrowTableStream(records, tag_columns=tag_keys)
        self._update_modified_time()

    def run(self) -> None:
        """
        Execute the operator according to the current cache mode.

        - **OFF**: always compute, no DB writes.
        - **LOG**: always compute, write results to DB.
        - **REPLAY**: skip computation, load from DB.
        """
        if self.is_stale:
            self.clear_cache()

        if self._cached_output_stream is not None:
            return

        if self._cache_mode == CacheMode.REPLAY:
            self._replay_from_cache()
        else:
            self._compute_and_store()

    # ------------------------------------------------------------------
    # DB retrieval
    # ------------------------------------------------------------------

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table | None":
        """
        Retrieve all stored records from the pipeline database.

        Returns the stored output table with column filtering applied
        per ``ColumnConfig`` conventions.
        """
        results = self._pipeline_database.get_all_records(self.pipeline_path)
        if results is None:
            return None

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        drop_columns = []
        if not column_config.meta and not column_config.all_info:
            drop_columns.extend(
                c for c in results.column_names if c.startswith(constants.META_PREFIX)
            )
        if not column_config.source and not column_config.all_info:
            drop_columns.extend(
                c for c in results.column_names if c.startswith(constants.SOURCE_PREFIX)
            )
        if not column_config.system_tags and not column_config.all_info:
            drop_columns.extend(
                c
                for c in results.column_names
                if c.startswith(constants.SYSTEM_TAG_PREFIX)
            )
        if drop_columns:
            results = results.drop(
                [c for c in drop_columns if c in results.column_names]
            )

        return results if results.num_rows > 0 else None

    # ------------------------------------------------------------------
    # Async channel execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """Async execution with cache mode handling.

        REPLAY: emit from DB, close output.
        OFF: delegate to operator, forward results.
        LOG: delegate to operator, forward + collect results, then store in DB.
        """
        try:
            if self._cache_mode == CacheMode.REPLAY:
                self._replay_from_cache()
                assert self._cached_output_stream is not None
                for tag, packet in self._cached_output_stream.iter_packets():
                    await output.send((tag, packet))
                return  # finally block closes output

            # OFF or LOG: delegate to operator, forward results downstream
            intermediate: Channel[tuple[TagProtocol, PacketProtocol]] = Channel()
            should_collect = self._cache_mode == CacheMode.LOG
            collected: list[tuple[TagProtocol, PacketProtocol]] = []

            async def forward() -> None:
                async for item in intermediate.reader:
                    if should_collect:
                        collected.append(item)
                    await output.send(item)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self._operator.async_execute(inputs, intermediate.writer)
                )
                tg.create_task(forward())

            # TaskGroup has completed — store if LOG mode (sync DB write, post-hoc)
            if should_collect and collected:
                stream = StaticOutputPod._materialize_to_stream(collected)
                self._cached_output_stream = stream
                self._store_output_stream(stream)

            self._update_modified_time()
        finally:
            await output.close()

    # ------------------------------------------------------------------
    # DerivedSource
    # ------------------------------------------------------------------

    def as_source(self):
        """Return a DerivedSource backed by the DB records of this node."""
        from orcapod.core.sources.derived_source import DerivedSource

        path_str = "/".join(self.pipeline_path)
        content_frag = self.content_hash().to_string()[:16]
        source_id = f"{path_str}:{content_frag}"
        return DerivedSource(
            origin=self,
            source_id=source_id,
            data_context=self.data_context_key,
            config=self.orcapod_config,
        )
