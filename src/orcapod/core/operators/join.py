import asyncio
import itertools
from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import NonZeroInputOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    PacketProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_data_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


class Join(NonZeroInputOperator):
    @property
    def kernel_id(self) -> tuple[str, ...]:
        """
        Returns a unique identifier for the kernel.
        This is used to identify the kernel in the computational graph.
        """
        return (f"{self.__class__.__name__}",)

    def validate_nonzero_inputs(self, *streams: StreamProtocol) -> None:
        """Validate that input streams are compatible for joining."""
        # TODO: add more helpful validation
        try:
            self.output_schema(*streams)
        except Exception as e:
            raise InputValidationError(f"Input streams are not compatible: {e}") from e

    def order_input_streams(self, *streams: StreamProtocol) -> list[StreamProtocol]:
        # Canonically order by pipeline_hash for deterministic operation.
        # pipeline_hash is structure-only, so streams with the same schema+topology
        # get the same ordering regardless of data content.
        return sorted(streams, key=lambda s: s.pipeline_hash().to_string())

    def argument_symmetry(self, streams: Collection) -> ArgumentGroup:
        return frozenset(streams)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)

        if len(streams) == 1:
            # if single stream, simply return the output schema of the single input stream
            return streams[0].output_schema(columns=columns, all_info=all_info)

        # Always get input schemas WITHOUT system tags for the base computation.
        # System tags are computed separately because the join renames them.
        stream = streams[0]
        tag_schema, packet_schema = stream.output_schema()
        for other_stream in streams[1:]:
            other_tag_schema, other_packet_schema = other_stream.output_schema()
            tag_schema = schema_utils.union_schemas(tag_schema, other_tag_schema)
            intersection_packet_schema = schema_utils.intersection_schemas(
                packet_schema, other_packet_schema
            )
            packet_schema = schema_utils.union_schemas(
                packet_schema, other_packet_schema
            )
            if intersection_packet_schema:
                raise InputValidationError(
                    f"Packets should not have overlapping keys, but {packet_schema.keys()} found in {stream} and {other_stream}."
                )

        # Add system tag columns if requested
        if columns_config.system_tags:
            system_tag_schema = self._predict_system_tag_schema(*streams)
            tag_schema = schema_utils.union_schemas(tag_schema, system_tag_schema)

        return tag_schema, packet_schema

    def _predict_system_tag_schema(self, *streams: StreamProtocol) -> Schema:
        """Predict the system tag columns that the join would produce.

        Each input stream's existing system tag columns get renamed by
        appending ::{pipeline_hash}:{canonical_position}. This method
        computes those output column names without performing the join.
        """
        n_char = self.orcapod_config.system_tag_hash_n_char
        ordered_streams = self.order_input_streams(*streams)

        system_tag_fields: dict[str, type] = {}
        for idx, stream in enumerate(ordered_streams):
            stream_tag_schema, _ = stream.output_schema(columns={"system_tags": True})
            for col_name in stream_tag_schema:
                if col_name.startswith(constants.SYSTEM_TAG_PREFIX):
                    new_name = (
                        f"{col_name}{constants.BLOCK_SEPARATOR}"
                        f"{stream.pipeline_hash().to_hex(n_char)}:{idx}"
                    )
                    system_tag_fields[new_name] = str
        return Schema(system_tag_fields)

    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """
        if len(streams) == 1:
            return streams[0]

        # Canonically order streams by pipeline_hash for deterministic
        # system tag column names regardless of input order (Join is commutative)
        ordered_streams = self.order_input_streams(*streams)

        COMMON_JOIN_KEY = "_common"

        n_char = self.orcapod_config.system_tag_hash_n_char

        stream = ordered_streams[0]

        tag_keys, _ = [set(k) for k in stream.keys()]
        table = stream.as_table(
            columns={"source": True, "system_tags": True, "meta": True}
        )
        # trick to get cartesian product
        table = table.add_column(0, COMMON_JOIN_KEY, pa.array([0] * len(table)))
        table = arrow_data_utils.append_to_system_tags(
            table,
            f"{stream.pipeline_hash().to_hex(n_char)}:0",
        )

        for idx, next_stream in enumerate(ordered_streams[1:], start=1):
            next_tag_keys, _ = next_stream.keys()
            next_table = next_stream.as_table(
                columns={"source": True, "system_tags": True, "meta": True}
            )
            next_table = arrow_data_utils.append_to_system_tags(
                next_table,
                f"{next_stream.pipeline_hash().to_hex(n_char)}:{idx}",
            )
            # trick to ensure that there will always be at least one shared key
            # this ensure that no overlap in keys lead to full caretesian product
            next_table = next_table.add_column(
                0, COMMON_JOIN_KEY, pa.array([0] * len(next_table))
            )

            # Rename any non-key columns in next_table that would collide with
            # the accumulated table, using stream-index-based suffixes instead of
            # Polars' default ``_right`` suffix which causes cascading collisions
            # on 3+ stream joins.  The only legitimately shared column names are
            # the tag join keys; everything else (meta columns, their derived
            # source-info columns, etc.) must be unique.
            join_key_set = tag_keys.intersection(next_tag_keys) | {COMMON_JOIN_KEY}
            existing_names = set(table.column_names)
            rename_map = {}
            for col in next_table.column_names:
                if col not in join_key_set and col in existing_names:
                    new_name = f"{col}_{idx}"
                    counter = idx
                    while new_name in existing_names or new_name in rename_map.values():
                        counter += 1
                        new_name = f"{col}_{counter}"
                    rename_map[col] = new_name
            if rename_map:
                next_table = pl.DataFrame(next_table).rename(rename_map).to_arrow()

            common_tag_keys = tag_keys.intersection(next_tag_keys)
            common_tag_keys.add(COMMON_JOIN_KEY)

            table = (
                pl.DataFrame(table)
                .join(pl.DataFrame(next_table), on=list(common_tag_keys), how="inner")
                .to_arrow()
            )

            tag_keys.update(next_tag_keys)

        # reorder columns to bring tag columns to the front
        # TODO: come up with a better algorithm
        table = table.drop(COMMON_JOIN_KEY)

        # Sort system tag values for same-pipeline-hash streams to ensure commutativity
        table = arrow_data_utils.sort_system_tag_values(table)

        reordered_columns = [col for col in table.column_names if col in tag_keys]
        reordered_columns += [col for col in table.column_names if col not in tag_keys]

        return ArrowTableStream(
            table.select(reordered_columns),
            tag_columns=tuple(tag_keys),
        )

    # ------------------------------------------------------------------
    # Async execution
    # ------------------------------------------------------------------

    def _compute_system_tag_suffixes(
        self,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> list[str]:
        """Compute per-input system-tag suffixes from pipeline hashes.

        Each suffix is ``{truncated_hash}:{canonical_position}`` where
        canonical position is determined by sorting the hashes (matching
        the deterministic ordering used by ``static_process``).

        Args:
            input_pipeline_hashes: Pipeline hash per input, positionally
                matching the input channels.

        Returns:
            List of suffix strings, one per input position.
        """
        n_char = self.orcapod_config.system_tag_hash_n_char
        hex_strings = [h.to_hex() for h in input_pipeline_hashes]

        # Canonical order: sorted by full hex (same as order_input_streams).
        # Use the original index as a tiebreaker so that inputs with
        # identical pipeline hashes still receive distinct positions
        # (matching Python's stable sort used by order_input_streams).
        ranked = sorted(range(len(hex_strings)), key=lambda i: hex_strings[i])
        canon_position = [0] * len(hex_strings)
        for canon_idx, orig_idx in enumerate(ranked):
            canon_position[orig_idx] = canon_idx

        suffixes: list[str] = []
        for orig_idx in range(len(hex_strings)):
            truncated = input_pipeline_hashes[orig_idx].to_hex(n_char)
            suffixes.append(f"{truncated}:{canon_position[orig_idx]}")
        return suffixes

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash] | None = None,
    ) -> None:
        """Async streaming N-way join (MJoin algorithm).

        Single input: streams through directly without any buffering.

        Two or more inputs: N-way symmetric hash join (MJoin) — each
        arriving row is added to its side's hash index and immediately
        probed against all other sides.  When a complete match across
        all N inputs exists for a tag key, the cross-product of
        matching rows is emitted immediately so downstream consumers
        can begin work before any input is fully consumed.

        Args:
            inputs: Readable channels, one per upstream.
            output: Writable channel for downstream.
            input_pipeline_hashes: Pipeline hash for each input,
                positionally matching ``inputs``.  Required for
                correct system-tag renaming with 2+ inputs.
        """
        try:
            if len(inputs) == 1:
                async for tag, packet in inputs[0]:
                    await output.send((tag, packet))
                return

            n = len(inputs)
            suffixes = (
                self._compute_system_tag_suffixes(input_pipeline_hashes)
                if input_pipeline_hashes is not None
                else [str(i) for i in range(n)]
            )
            await self._mjoin(inputs, output, suffixes)
        finally:
            await output.close()

    async def _mjoin(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        suffixes: list[str],
    ) -> None:
        """N-way streaming symmetric hash join (MJoin).

        All N inputs are read concurrently via a single bounded queue.
        Each arriving row is added to its side's hash index and
        immediately probed against all other N-1 sides.  When every
        side has at least one matching row for a tag key, the
        cross-product of matches is emitted immediately.

        Args:
            inputs: Readable channels, one per upstream.
            output: Output channel for matched rows.
            suffixes: Per-input system-tag suffixes (positional),
                computed from pipeline hashes and canonical ordering.
        """
        n = len(inputs)

        # Bounded queue preserves backpressure — producers block when full.
        _SENTINEL = object()
        queue: asyncio.Queue = asyncio.Queue(maxsize=64)

        async def _drain(
            ch: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
            side: int,
        ) -> None:
            async for item in ch:
                await queue.put((side, item))
            await queue.put((side, _SENTINEL))

        block_sep = constants.BLOCK_SEPARATOR

        async with asyncio.TaskGroup() as tg:
            for i, ch in enumerate(inputs):
                tg.create_task(_drain(ch, i))

            # buffers[i] holds all rows seen so far from input i
            buffers: list[list[tuple[TagProtocol, PacketProtocol]]] = [
                [] for _ in range(n)
            ]
            # indexes[i] maps shared-key tuple → list of indices into buffers[i]
            indexes: list[dict[tuple, list[int]]] = [{} for _ in range(n)]
            # Track which sides have contributed at least one row
            seen_sides: set[int] = set()

            shared_keys: tuple[str, ...] | None = None
            needs_reindex = False
            closed_count = 0

            while closed_count < n:
                side, item = await queue.get()

                if item is _SENTINEL:
                    closed_count += 1
                    continue

                tag, pkt = item

                # Determine shared tag keys once we have rows from all sides
                if shared_keys is None:
                    seen_sides.add(side)
                    if len(seen_sides) < n:
                        # Haven't seen all sides yet — just buffer
                        buffers[side].append((tag, pkt))
                        continue

                    # All sides represented; compute shared keys as the
                    # intersection of all sides' tag key sets
                    all_key_sets = []
                    for s in range(n):
                        if s == side:
                            all_key_sets.append(set(tag.keys()))
                        else:
                            all_key_sets.append(set(buffers[s][0][0].keys()))
                    shared_keys = tuple(sorted(set.intersection(*all_key_sets)))
                    needs_reindex = True

                # One-time re-index of all rows buffered before shared_keys
                if needs_reindex:
                    needs_reindex = False
                    for buf_side in range(n):
                        for j, (bt, _bp) in enumerate(buffers[buf_side]):
                            btd = bt.as_dict()
                            k = (
                                tuple(btd[sk] for sk in shared_keys)
                                if shared_keys
                                else (0,)
                            )
                            indexes[buf_side].setdefault(k, []).append(j)

                    # Emit matches for all already-buffered cross-side combos
                    await self._emit_buffered_matches(
                        buffers, indexes, shared_keys, suffixes, block_sep, output
                    )

                # Index the new row
                td = tag.as_dict()
                key = tuple(td[sk] for sk in shared_keys) if shared_keys else (0,)
                row_idx = len(buffers[side])
                buffers[side].append((tag, pkt))
                indexes[side].setdefault(key, []).append(row_idx)

                # Probe all other sides for complete matches
                other_sides_indices = []
                for other in range(n):
                    if other == side:
                        continue
                    matches = indexes[other].get(key)
                    if not matches:
                        break  # Missing side — no complete match yet
                    other_sides_indices.append((other, matches))
                else:
                    # All other sides have matches — emit cross-product
                    # Build list of (side_index, row_indices) in canonical order
                    all_sides = [(side, [row_idx])] + other_sides_indices
                    all_sides.sort(key=lambda x: x[0])

                    ordered_idx_lists = [idxs for _, idxs in all_sides]
                    for combo in itertools.product(*ordered_idx_lists):
                        rows = [buffers[s][combo[i]] for i, (s, _) in enumerate(all_sides)]
                        merged = self._merge_rows(rows, suffixes, block_sep)
                        await output.send(merged)

    async def _emit_buffered_matches(
        self,
        buffers: list[list[tuple[TagProtocol, PacketProtocol]]],
        indexes: list[dict[tuple, list[int]]],
        shared_keys: tuple[str, ...],
        suffixes: list[str],
        block_sep: str,
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """Emit all complete matches from rows buffered before shared_keys were known.

        Iterates over all tag keys seen across any side's index. For each
        key that has matches in every side, emits the full cross-product.
        """
        n = len(buffers)
        # Collect all keys seen across all sides
        all_keys: set[tuple] = set()
        for side_idx in range(n):
            all_keys.update(indexes[side_idx].keys())

        for key in all_keys:
            per_side_indices = []
            for side_idx in range(n):
                matches = indexes[side_idx].get(key)
                if not matches:
                    break
                per_side_indices.append(matches)
            else:
                # All sides have this key — emit cross-product
                for combo in itertools.product(*per_side_indices):
                    rows = [buffers[s][combo[s]] for s in range(n)]
                    merged = self._merge_rows(rows, suffixes, block_sep)
                    await output.send(merged)

    @staticmethod
    def _merge_rows(
        rows: list[tuple[TagProtocol, PacketProtocol]],
        suffixes: list[str],
        block_sep: str,
    ) -> tuple[TagProtocol, PacketProtocol]:
        """Merge N matched rows into one joined (Tag, Packet).

        Tag dicts are merged left-to-right (shared keys come from the
        first side that provides them).  System-tag keys are renamed by
        appending ``{block_sep}{suffix}`` to match the canonical
        name-extending scheme used by ``static_process``.
        """
        from orcapod.core.datagrams import Packet, Tag

        sys_prefix = constants.SYSTEM_TAG_PREFIX

        # Merge tag dicts — shared keys come from first side
        merged_tag_d: dict = {}
        for tag, _ in rows:
            for k, v in tag.as_dict().items():
                if k not in merged_tag_d:
                    merged_tag_d[k] = v

        # Rename and merge system tags with canonical per-side suffixes
        merged_sys: dict = {}
        for i, (tag, _) in enumerate(rows):
            for k, v in tag.system_tags().items():
                new_key = (
                    f"{k}{block_sep}{suffixes[i]}" if k.startswith(sys_prefix) else k
                )
                merged_sys[new_key] = v

        # Sort system tag values within same-provenance-path groups so that
        # Join(A, B) == Join(B, A) when A and B share a pipeline_hash.
        # Mirrors the sort_system_tag_values() call in static_process.
        merged_sys = Join._sort_merged_system_tags(merged_sys)

        merged_tag = Tag(merged_tag_d, system_tags=merged_sys)

        # Merge packet dicts (non-overlapping by Join's validation)
        merged_pkt_d: dict = {}
        merged_si: dict = {}
        for _, pkt in rows:
            merged_pkt_d.update(pkt.as_dict())
            merged_si.update(pkt.source_info())

        merged_pkt = Packet(merged_pkt_d, source_info=merged_si)

        return merged_tag, merged_pkt

    @staticmethod
    def _sort_merged_system_tags(merged_sys: dict) -> dict:
        """Sort system tag values within same-provenance-path groups.

        When two joined inputs share a pipeline_hash, their system tag
        columns share a provenance path but occupy different canonical
        positions.  Sorting the paired (source_id, record_id) values
        across positions ensures commutativity — mirroring what
        ``sort_system_tag_values`` does on Arrow tables in
        ``static_process``.
        """
        sys_prefix = constants.SYSTEM_TAG_PREFIX
        block_sep = constants.BLOCK_SEPARATOR
        field_sep = constants.FIELD_SEPARATOR

        # Parse keys → groups[provenance_path][position][field_type] = key
        groups: dict[str, dict[str, dict[str, str]]] = {}
        for key in merged_sys:
            if not key.startswith(sys_prefix):
                continue
            base, sep, position = key.rpartition(field_sep)
            if not sep or not position.isdigit():
                continue
            after_prefix = base[len(sys_prefix) :]
            field_type, bsep, prov_path = after_prefix.partition(block_sep)
            if not bsep:
                continue
            groups.setdefault(prov_path, {}).setdefault(position, {})[
                field_type
            ] = key

        sid_field = constants.SYSTEM_TAG_SOURCE_ID_PREFIX[len(sys_prefix) :]
        rid_field = constants.SYSTEM_TAG_RECORD_ID_PREFIX[len(sys_prefix) :]

        for _prov_path, positions in groups.items():
            if len(positions) <= 1:
                continue

            sorted_pos_keys = sorted(positions.keys(), key=int)

            # Collect (sort_key, {field_type: value}) per position
            entries: list[tuple[tuple, dict[str, object]]] = []
            for pos in sorted_pos_keys:
                fmap = positions[pos]
                sid_val = merged_sys.get(fmap.get(sid_field, ""))
                rid_val = merged_sys.get(fmap.get(rid_field, ""))
                vals = {ft: merged_sys[k] for ft, k in fmap.items()}
                entries.append(((sid_val or "", rid_val or ""), vals))

            entries.sort(key=lambda e: e[0])

            # Write sorted values back to the original position keys
            for pos, (_, sorted_vals) in zip(sorted_pos_keys, entries):
                fmap = positions[pos]
                for field_type, key in fmap.items():
                    if field_type in sorted_vals:
                        merged_sys[key] = sorted_vals[field_type]

        return merged_sys

    def identity_structure(self) -> Any:
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "Join()"
