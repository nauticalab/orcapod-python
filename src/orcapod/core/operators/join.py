import asyncio
from collections.abc import Callable, Collection, Sequence
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
from orcapod.utils import arrow_data_utils, arrow_utils, schema_utils
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

        result_table = table.select(reordered_columns)
        # Derive nullable per column from actual null counts so that:
        # - Columns with no nulls (e.g. tag/packet fields after inner join) get
        #   nullable=False, avoiding spurious T | None from Polars' all-True default.
        # - Columns that genuinely contain nulls (e.g. Optional fields, or source
        #   info columns after cross-stream joins) keep nullable=True, preventing
        #   cast failures.
        result_table = result_table.cast(arrow_utils.infer_schema_nullable(result_table))
        return ArrowTableStream(
            result_table,
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
        """Async streaming join with pairwise iterative semantics.

        Single input: streams through directly without any buffering.

        Two inputs: binary symmetric hash join — each arriving row is
        probed against the opposite side's buffer, emitting matches as
        soon as found.

        Three or more inputs: staggered pairwise binary joins in
        canonical order — ``join(join(x, y), z)`` — matching
        ``static_process``'s iterative accumulation.  Each binary join
        uses the per-pair intersection of tag keys, so partially
        overlapping tag schemas are handled correctly.

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
            if input_pipeline_hashes is not None and len(input_pipeline_hashes) != n:
                raise ValueError(
                    f"input_pipeline_hashes length ({len(input_pipeline_hashes)}) "
                    f"must match inputs length ({n})"
                )
            suffixes = (
                self._compute_system_tag_suffixes(input_pipeline_hashes)
                if input_pipeline_hashes is not None
                else [str(i) for i in range(n)]
            )
            await self._streaming_join(inputs, output, suffixes)
        finally:
            await output.close()

    async def _streaming_join(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        suffixes: list[str],
    ) -> None:
        """Dispatch between binary join (N=2) and staggered chain (N>=3).

        Args:
            inputs: Readable channels, one per upstream.
            output: Output channel for matched rows.
            suffixes: Per-input system-tag suffixes (positional).
        """
        n = len(inputs)
        block_sep = constants.BLOCK_SEPARATOR

        if n == 2:

            def merge_fn(
                lt: TagProtocol,
                lp: PacketProtocol,
                rt: TagProtocol,
                rp: PacketProtocol,
            ) -> tuple[TagProtocol, PacketProtocol]:
                return self._merge_pair_rename(lt, lp, rt, rp, suffixes, block_sep)

            await self._binary_streaming_join(inputs[0], inputs[1], output, merge_fn)
            return

        # N >= 3: staggered pairwise joins matching static_process
        await self._staggered_join(inputs, output, suffixes)

    async def _staggered_join(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        suffixes: list[str],
    ) -> None:
        """Staggered pairwise binary joins: ``join(join(x, y), z)``.

        Matches ``static_process``'s iterative pairwise join semantics.
        Each input's system tags are pre-renamed, then binary joins are
        chained in canonical order.  Per-pair join keys are computed
        naturally by each binary join (intersection of its two inputs'
        tag keys), so partially overlapping tag schemas produce the
        same results as the sync path.

        Intermediate results flow through channels, so downstream joins
        can start work as soon as earlier joins emit matches — the
        pipeline is fully streaming end-to-end.

        Args:
            inputs: Readable channels, one per upstream.
            output: Output channel for matched rows.
            suffixes: Per-input system-tag suffixes (positional).
        """
        from orcapod.channels import Channel

        n = len(inputs)
        block_sep = constants.BLOCK_SEPARATOR
        sys_prefix = constants.SYSTEM_TAG_PREFIX

        # Canonical order: sorted by canonical position encoded in suffixes.
        # Suffixes are "hash:position" when pipeline hashes are provided,
        # or plain "0", "1", ... otherwise.
        def _canon_pos(i: int) -> int:
            parts = suffixes[i].rsplit(":", 1)
            return int(parts[1]) if len(parts) == 2 else int(parts[0])

        canon_order = sorted(range(n), key=_canon_pos)

        async with asyncio.TaskGroup() as tg:
            # Pre-rename system tags for each input so binary joins
            # can pass them through without modification
            renamed_readers: list[
                ReadableChannel[tuple[TagProtocol, PacketProtocol]]
            ] = []
            for orig_idx in canon_order:
                ch: Channel[tuple[TagProtocol, PacketProtocol]] = Channel(
                    buffer_size=64
                )
                tg.create_task(
                    self._rename_sys_tags(
                        inputs[orig_idx],
                        ch.writer,
                        suffixes[orig_idx],
                        block_sep,
                        sys_prefix,
                    )
                )
                renamed_readers.append(ch.reader)

            # Chain: renamed[0] ⋈ renamed[1] → intermediate ⋈ renamed[2] → … → output
            current_reader = renamed_readers[0]
            for i in range(1, len(renamed_readers)):
                is_last = i == len(renamed_readers) - 1
                if is_last:
                    target_writer = output
                else:
                    intermediate: Channel[
                        tuple[TagProtocol, PacketProtocol]
                    ] = Channel(buffer_size=64)
                    target_writer = intermediate.writer

                tg.create_task(
                    self._binary_streaming_join(
                        current_reader,
                        renamed_readers[i],
                        target_writer,
                        self._merge_pair_passthrough,
                    )
                )

                if not is_last:
                    current_reader = intermediate.reader

    async def _binary_streaming_join(
        self,
        left: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
        right: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        merge_fn: Callable[
            [TagProtocol, PacketProtocol, TagProtocol, PacketProtocol],
            tuple[TagProtocol, PacketProtocol],
        ],
    ) -> None:
        """Binary symmetric hash join.

        Both sides are read concurrently via a merged bounded queue.
        Each arriving row is indexed and immediately probed against the
        opposite side.  Matched rows are emitted via ``merge_fn`` as
        soon as found, so downstream can begin work before either input
        is fully consumed.

        Args:
            left: Left input channel.
            right: Right input channel.
            output: Output channel for matched rows.
            merge_fn: Callable(left_tag, left_pkt, right_tag, right_pkt)
                that produces the merged (Tag, Packet) pair.
        """
        _SENTINEL = object()
        queue: asyncio.Queue = asyncio.Queue(maxsize=64)

        async def _drain(
            ch: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
            side: int,
        ) -> None:
            async for item in ch:
                await queue.put((side, item))
            await queue.put((side, _SENTINEL))

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(_drain(left, 0))
                tg.create_task(_drain(right, 1))

                buffers: list[list[tuple[TagProtocol, PacketProtocol]]] = [
                    [],
                    [],
                ]
                indexes: list[dict[tuple, list[int]]] = [{}, {}]

                shared_keys: tuple[str, ...] | None = None
                needs_reindex = False
                closed_count = 0

                while closed_count < 2:
                    side, item = await queue.get()

                    if item is _SENTINEL:
                        closed_count += 1
                        continue

                    tag, pkt = item
                    other = 1 - side

                    # Determine shared tag keys once we have rows from both sides
                    if shared_keys is None:
                        if not buffers[other]:
                            buffers[side].append((tag, pkt))
                            continue
                        this_keys = set(tag.keys())
                        other_keys = set(buffers[other][0][0].keys())
                        shared_keys = tuple(sorted(this_keys & other_keys))
                        needs_reindex = True

                    # One-time re-index of rows buffered before shared_keys
                    if needs_reindex:
                        needs_reindex = False
                        for buf_side in (0, 1):
                            for j, (bt, _bp) in enumerate(buffers[buf_side]):
                                btd = bt.as_dict()
                                k = (
                                    tuple(btd[sk] for sk in shared_keys)
                                    if shared_keys
                                    else (0,)
                                )
                                indexes[buf_side].setdefault(k, []).append(j)

                    # Index the new row
                    td = tag.as_dict()
                    key = (
                        tuple(td[sk] for sk in shared_keys)
                        if shared_keys
                        else (0,)
                    )
                    row_idx = len(buffers[side])
                    buffers[side].append((tag, pkt))
                    indexes[side].setdefault(key, []).append(row_idx)

                    # Probe the opposite side for matches
                    for mi in indexes[other].get(key, []):
                        ot, op = buffers[other][mi]
                        if side == 0:
                            await output.send(merge_fn(tag, pkt, ot, op))
                        else:
                            await output.send(merge_fn(ot, op, tag, pkt))
        finally:
            await output.close()

    @staticmethod
    async def _rename_sys_tags(
        ch_in: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
        ch_out: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        suffix: str,
        block_sep: str,
        sys_prefix: str,
    ) -> None:
        """Read rows and rename system-tag keys by appending the per-input suffix.

        Used as a pre-processing step in ``_staggered_join`` so that
        downstream binary joins can pass system tags through without
        modification.
        """
        from orcapod.core.datagrams import Tag

        try:
            async for tag, pkt in ch_in:
                sys_tags = tag.system_tags()
                if sys_tags:
                    renamed: dict = {}
                    for k, v in sys_tags.items():
                        new_key = (
                            f"{k}{block_sep}{suffix}"
                            if k.startswith(sys_prefix)
                            else k
                        )
                        renamed[new_key] = v
                    tag = Tag(tag.as_dict(), system_tags=renamed)
                await ch_out.send((tag, pkt))
        finally:
            await ch_out.close()

    @staticmethod
    def _merge_pair_rename(
        left_tag: TagProtocol,
        left_pkt: PacketProtocol,
        right_tag: TagProtocol,
        right_pkt: PacketProtocol,
        suffixes: list[str],
        block_sep: str,
    ) -> tuple[TagProtocol, PacketProtocol]:
        """Merge a matched pair, renaming system tags with per-side suffixes.

        Used for direct 2-input joins where system tags are renamed
        during the merge (not pre-renamed).
        """
        from orcapod.core.datagrams import Packet, Tag

        sys_prefix = constants.SYSTEM_TAG_PREFIX

        # Merge tag dicts — shared keys come from left
        merged_tag_d: dict = {}
        for k, v in left_tag.as_dict().items():
            merged_tag_d[k] = v
        for k, v in right_tag.as_dict().items():
            if k not in merged_tag_d:
                merged_tag_d[k] = v

        # Rename and merge system tags
        merged_sys: dict = {}
        for i, tag in enumerate((left_tag, right_tag)):
            for k, v in tag.system_tags().items():
                new_key = (
                    f"{k}{block_sep}{suffixes[i]}"
                    if k.startswith(sys_prefix)
                    else k
                )
                merged_sys[new_key] = v

        merged_sys = Join._sort_merged_system_tags(merged_sys)
        merged_tag = Tag(merged_tag_d, system_tags=merged_sys)

        # Merge packet dicts (non-overlapping by Join's validation)
        merged_pkt_d: dict = {}
        merged_si: dict = {}
        merged_pkt_d.update(left_pkt.as_dict())
        merged_pkt_d.update(right_pkt.as_dict())
        merged_si.update(left_pkt.source_info())
        merged_si.update(right_pkt.source_info())

        merged_pkt = Packet(merged_pkt_d, source_info=merged_si)
        return merged_tag, merged_pkt

    @staticmethod
    def _merge_pair_passthrough(
        left_tag: TagProtocol,
        left_pkt: PacketProtocol,
        right_tag: TagProtocol,
        right_pkt: PacketProtocol,
    ) -> tuple[TagProtocol, PacketProtocol]:
        """Merge a matched pair, passing system tags through without renaming.

        Used in the staggered chain where system tags have already been
        pre-renamed by ``_rename_sys_tags``.
        """
        from orcapod.core.datagrams import Packet, Tag

        # Merge tag dicts — shared keys come from left
        merged_tag_d: dict = {}
        for k, v in left_tag.as_dict().items():
            merged_tag_d[k] = v
        for k, v in right_tag.as_dict().items():
            if k not in merged_tag_d:
                merged_tag_d[k] = v

        # Combine system tags (already renamed)
        merged_sys: dict = {}
        merged_sys.update(left_tag.system_tags())
        merged_sys.update(right_tag.system_tags())

        # Sort within same-provenance-path groups for commutativity
        merged_sys = Join._sort_merged_system_tags(merged_sys)
        merged_tag = Tag(merged_tag_d, system_tags=merged_sys)

        # Merge packet dicts (non-overlapping by Join's validation)
        merged_pkt_d: dict = {}
        merged_si: dict = {}
        merged_pkt_d.update(left_pkt.as_dict())
        merged_pkt_d.update(right_pkt.as_dict())
        merged_si.update(left_pkt.source_info())
        merged_si.update(right_pkt.source_info())

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
