import asyncio
import contextlib
import warnings
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterator, Collection, Sequence
from typing import TYPE_CHECKING, Any, final

from orcapod.channels import ChannelClosed, ReadableChannel, WritableChannel
from orcapod.core.operators.base import BinaryOperator, require_shared_arrow_type
from orcapod.core.operators.key_nullability import reconcile_key_nullability
from orcapod.core.operators.match_keys import frozen_scalar
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError, SemiJoinIgnoredColumnWarning
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.warning_utils import outside_orcapod_stacklevel

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")


class SemiJoin(BinaryOperator):
    """
    Binary operator that performs a semi-join between two streams.

    A semi-join returns only the entries from the left stream that have
    matching entries in the right stream. The match keys are exactly the
    columns that are tag columns of both streams (ITL-1088): a row matches
    when its values in all of them equal a right row's. A name shared as a
    data column, or a tag on one side and a data column on the other, is not a
    key and is ignored, with a ``SemiJoinIgnoredColumnWarning`` (provisional in
    v0.3); the right stream's data columns are never read.

    If the streams share no tag column, the entire left stream is returned
    unchanged.

    The output stream preserves the schema of the left stream exactly. A shared
    tag key that is ``T`` in one input and ``T | None`` in the other is accepted
    (a null key matches nothing) and keeps the left input's type (ITL-829).
    """

    @staticmethod
    def _match_keys(
        left_tag_keys: Collection[str], right_tag_keys: Collection[str]
    ) -> tuple[str, ...]:
        """Return the semi join's match keys: the names that are tags on both sides.

        Args:
            left_tag_keys: The left input's tag column names.
            right_tag_keys: The right input's tag column names.

        Returns:
            The names in both, in the left input's order.
        """
        return tuple(name for name in left_tag_keys if name in right_tag_keys)

    @staticmethod
    def _ignored_shared_names(
        left_tag_keys: Collection[str],
        left_data_keys: Collection[str],
        right_tag_keys: Collection[str],
        right_data_keys: Collection[str],
    ) -> tuple[tuple[str, str, str], ...]:
        """Return the shared names that are not match keys, with each side's role.

        The decision is by name only: no value or type is read. Meta (``__``)
        columns are not user columns and never count.

        Args:
            left_tag_keys: The left input's tag column names.
            left_data_keys: The left input's data column names.
            right_tag_keys: The right input's tag column names.
            right_data_keys: The right input's data column names.

        Returns:
            ``(name, left role, right role)`` for each name shared as data on both
            sides or as a tag on one side and data on the other, in the left
            input's order; roles are ``"tag"`` or ``"data"``.
        """
        right_roles = {name: "tag" for name in right_tag_keys} | {
            name: "data" for name in right_data_keys
        }
        left_roles = [(name, "tag") for name in left_tag_keys] + [
            (name, "data") for name in left_data_keys
        ]
        return tuple(
            (name, left_role, right_roles[name])
            for name, left_role in left_roles
            if name in right_roles
            and not name.startswith(constants.META_PREFIX)
            and (left_role, right_roles[name]) != ("tag", "tag")
        )

    def _warn_once_per_application(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> None:
        """Warn about shared non-key names once per operator and set of names.

        One application is validated more than once: when the operator is
        called, when a job builds its node and when the sync orchestrator runs
        it over the rows it rebuilt. The ignored names are remembered on the
        operator, so each set is warned about once.

        Args:
            left_stream: The left input.
            right_stream: The right input.
        """
        left_tags, left_data = left_stream.keys()
        right_tags, right_data = right_stream.keys()
        ignored = self._ignored_shared_names(
            left_tags, left_data, right_tags, right_data
        )
        if not ignored:
            return
        warned: set[tuple[tuple[str, str, str], ...]] | None = getattr(
            self, "_warned_ignored", None
        )
        if warned is None:
            warned = set()
            self._warned_ignored: set[tuple[tuple[str, str, str], ...]] = warned
        if ignored in warned:
            return
        # Recorded only once the warning is issued: under an error filter it
        # raises, and a retry must raise again.
        self._warn_ignored(ignored)
        warned.add(ignored)

    @staticmethod
    def _warn_ignored(ignored: Sequence[tuple[str, str, str]]) -> None:
        """Issue ``SemiJoinIgnoredColumnWarning`` for the shared non-key names.

        Args:
            ignored: ``(name, left role, right role)`` per ignored name.
        """
        described = ", ".join(
            f"'{name}' ({left_role} on the left, {right_role} on the right)"
            for name, left_role, right_role in ignored
        )
        warnings.warn(
            f"SemiJoin ignores the shared column(s) {described}: they are not used"
            + " for matching. SemiJoin matches only on tag columns shared by both"
            + " inputs. To silence this, rename or drop the column(s) on the right"
            + " input before the semi join (map_data, drop_data_columns,"
            + " select_data_columns, or map_tags for a tag), or filter"
            + " orcapod.errors.SemiJoinIgnoredColumnWarning.",
            SemiJoinIgnoredColumnWarning,
            stacklevel=outside_orcapod_stacklevel(),
        )

    def _checked_match_keys(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> tuple[str, ...]:
        """Return the match keys after checking that their types agree.

        Raises:
            TypeError: If a key's types differ beyond nullability (ITL-829).
        """
        left_tag_schema, _ = left_stream.output_schema()
        right_tag_schema, _ = right_stream.output_schema()
        # A shared tag key may differ in nullability only (ITL-829); any other
        # difference raises here.
        _ = schema_utils.intersection_schemas(
            *reconcile_key_nullability(left_tag_schema, right_tag_schema)
        )
        return self._match_keys(tuple(left_tag_schema), tuple(right_tag_schema))

    def binary_static_process(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> StreamProtocol:
        """
        Performs a semi-join between left and right streams.
        Returns entries from left stream that have matching entries in right stream.
        """
        left_tag_schema, _ = left_stream.output_schema()
        common_keys = self._checked_match_keys(left_stream, right_stream)

        # If no shared tag column, return the left stream unmodified
        if not common_keys:
            return left_stream

        # include source info and system tags for left stream
        left_table = left_stream.as_table(columns={"source": True, "system_tags": True})
        # Only the right side's keys are read, never its data columns.
        right_table = right_stream.as_table().select(list(common_keys))

        # A key must have one Arrow type on both sides (up to large layouts);
        # otherwise Polars would silently unify, e.g., int8 with int16.
        for key in common_keys:
            _ = require_shared_arrow_type(
                "SemiJoin",
                key,
                arrow_utils.column_arrow_type(left_table.schema, key),
                arrow_utils.column_arrow_type(right_table.schema, key),
            )

        # Only the right side's keys take part. A null key never matches:
        # Polars already applies that to scalar keys but treats a null struct or
        # list as equal to another, so drop right rows with a null key.
        right_keys = right_table.select(list(common_keys))
        valid = right_keys.column(0).is_valid()
        for index in range(1, right_keys.num_columns):
            valid = pc.and_(valid, right_keys.column(index).is_valid())
        right_keys = right_keys.filter(valid)

        # Polars, unlike Arrow's hash join, accepts nested (list, struct,
        # struct-backed logical) columns as keys and as passed-through columns,
        # and matches a string key against a large_string one.
        semi_joined_table = (
            pl.DataFrame(left_table)
            .join(
                pl.DataFrame(right_keys),
                on=list(common_keys),
                how="semi",
                maintain_order="left",
            )
            .to_arrow()
        )
        # The round-trip yields large layouts, nullable fields at every depth and
        # Polars' own types; give every column back the left input's exact field
        # (type, top-level and nested nullability, logical type).
        semi_joined_table = semi_joined_table.cast(left_table.schema)

        return ArrowTableStream(
            semi_joined_table,
            tag_columns=tuple(left_tag_schema.keys()),
        )

    def binary_output_schema(
        self,
        left_stream: StreamProtocol,
        right_stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Returns the output types for the semi-join operation.
        The output preserves the exact schema of the left stream.
        """
        # Semi-join preserves the left stream's schema exactly
        return left_stream.output_schema(columns=columns, all_info=all_info)

    def validate_binary_inputs(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> None:
        """
        Validates that the input streams are compatible for semi-join.
        Checks that the match keys (the shared tag columns) have compatible
        types; data columns are not keys and are not checked.

        Stores the common keys so that ``async_execute`` can use them
        to determine the correct empty-right behavior without data.

        Warns:
            SemiJoinIgnoredColumnWarning: Once per operator and set of names, when
                the inputs share a name that is not a tag on both sides (ITL-1088).
        """
        try:
            self._validated_common_keys: tuple[str, ...] = self._checked_match_keys(
                left_stream, right_stream
            )

        except Exception as e:
            raise InputValidationError(
                f"Input streams are not compatible for semi-join: {e}"
            ) from e
        self._warn_once_per_application(left_stream, right_stream)

    def is_commutative(self) -> bool:
        return False

    def _common_keys_from_schema(self) -> tuple[str, ...]:
        """Return the common keys computed during input validation.

        Falls back to an empty tuple if validation hasn't been called
        (shouldn't happen in normal pipeline execution).
        """
        return getattr(self, "_validated_common_keys", ())

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Build-probe: collect right input, then stream left through a hash lookup.

        Phase 1 — Build: collect all rows from the right (filter) channel and
        index them by the common-key values. The left channel is read at the
        same time (ITL-1075): the left rows that arrive before the right input
        ends are kept, at most the whole left input, as the sync path and
        ``MergeJoin``'s barrier path hold their inputs. Reading the left input
        only after the right one ended deadlocked a run in which one node fed
        both inputs.
        Phase 2 — Probe: the kept left rows, then the rest of the left input
        one at a time with backpressure; for each row whose common-key values
        appear in the right-side index, emit immediately. Left rows keep their
        arrival order.

        Falls back to barrier mode when the right input is empty (schema
        cannot be inferred from data) or when there are no common keys.

        Key types are checked (``require_shared_arrow_type``) on the first row
        of each input, since channels carry no schema. When either input is
        empty there is nothing to check, so a key type mismatch the sync path
        reports gives an empty result here (as in ``Join``'s async path).

        Args:
            inputs: The left and right readable input channels.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: the left rows pass through with
                their system-tag names, so no column is named after an
                input's pipeline hash.
        """
        left_rows: _LeftRows | None = None
        try:
            # Phase 1: Build right-side lookup, reading the left input meanwhile
            right_rows, left_rows = await _collect_right_reading_left(
                inputs[0], inputs[1]
            )

            if not right_rows:
                # Empty right: determine common keys from the validated
                # input schemas (set during __init__) to match sync semantics.
                # Common keys exist → empty result; no common keys → pass left through.
                common = self._common_keys_from_schema()
                if common:
                    # Drain left channel (discard) — result is empty
                    async for _ in left_rows:
                        pass
                    return
                # No common keys — pass all left rows through unchanged
                async for tag, data in left_rows:
                    await output.send((tag, data))
                return

            # The right side's tag columns, from its first row
            right_tag_keys = tuple(right_rows[0][0].keys())

            # Phase 2: Probe — stream left rows
            common_keys: tuple[str, ...] | None = None
            right_lookup: set[tuple[object, ...]] = set()

            async for tag, data in left_rows:
                if common_keys is None:
                    # First left row — determine common keys (the shared tag
                    # columns, ITL-1088) and build index
                    common_keys = self._match_keys(tuple(tag.keys()), right_tag_keys)
                    # Validation warns about shared non-key names; when it did not
                    # run (a direct call), warn here, once, from the first rows.
                    if not hasattr(self, "_validated_common_keys"):
                        ignored = self._ignored_shared_names(
                            tag.keys(),
                            data.keys(),
                            right_tag_keys,
                            right_rows[0][1].keys(),
                        )
                        if ignored:
                            self._warn_ignored(ignored)

                    if not common_keys:
                        # No common keys — pass all left rows through
                        await output.send((tag, data))
                        async for t, p in left_rows:
                            await output.send((t, p))
                        return

                    right_tag, right_data = right_rows[0]
                    for key in common_keys:
                        _ = require_shared_arrow_type(
                            "SemiJoin",
                            key,
                            self._key_arrow_type(tag, data, key),
                            self._key_arrow_type(right_tag, right_data, key),
                        )

                    # Build right-side lookup; a row with a null key never matches.
                    for rt, rp in right_rows:
                        right_key = self._match_key(rt, rp, common_keys)
                        if right_key is not None:
                            right_lookup.add(right_key)

                # Probe
                left_key = self._match_key(tag, data, common_keys)
                if left_key is not None and left_key in right_lookup:
                    await output.send((tag, data))
        finally:
            await output.close()
            if left_rows is not None:
                await left_rows.aclose()

    @staticmethod
    def _key_table(tag: TagProtocol, data: DataProtocol, key: str) -> "pa.Table":
        """Return the one-row Arrow table, the tag's or the data's, holding ``key``."""
        table = tag.as_table()
        return table if key in table.column_names else data.as_table()

    @staticmethod
    def _key_arrow_type(
        tag: TagProtocol, data: DataProtocol, key: str
    ) -> "pa.DataType":
        """Return the Arrow type of column ``key`` in a row's tag or data."""
        table = SemiJoin._key_table(tag, data, key)
        return arrow_utils.column_arrow_type(table.schema, key)

    @staticmethod
    def _match_key(
        tag: TagProtocol, data: DataProtocol, keys: Sequence[str]
    ) -> tuple[object, ...] | None:
        """Return a row's hashable key for the async lookup, or ``None`` if null.

        Values come from the row's Arrow columns (a logical column's storage),
        not from ``as_dict()``, and are frozen so that struct and list keys are
        hashable. The comparison matches the sync path's Polars join: a row
        whose key has a top-level null never matches, nulls inside a struct or
        list compare equal, and NaN equals NaN.

        Args:
            tag: The row's tag.
            data: The row's data.
            keys: The common key columns.

        Returns:
            The frozen key values in ``keys`` order, or ``None`` when any of
            them is null.
        """
        values: list[object] = []
        for key in keys:
            scalar = SemiJoin._key_table(tag, data, key).column(key)[0]
            value = frozen_scalar(scalar)
            if value is None:
                return None
            values.append(value)
        return tuple(values)

    def identity_structure(self) -> Any:
        return self.__class__.__name__


_Row = tuple[TagProtocol, DataProtocol]


async def _collect_right_reading_left(
    left: ReadableChannel[_Row],
    right: ReadableChannel[_Row],
) -> tuple[list[_Row], "_LeftRows"]:
    """Collect ``right`` while reading ``left``, so neither input stops being read.

    A task collects ``right``; meanwhile the rows of ``left`` are read and kept, until
    ``right`` ends or ``left`` does. A semi-join that waited for its right input before
    reading its left one blocked a node feeding both inputs: the channel into the left
    input filled, that node could not finish its right-side rows, and the run waited
    forever (ITL-1075).

    Each left receive runs as a task, awaited together with the right one, so the end
    (or failure) of ``right`` is acted on at once, not when ``left`` next sends or
    closes. A receive still pending then is handed to the returned rows, which await
    it in its place: it is never cancelled while its row may still be read.

    Args:
        left: The left input channel.
        right: The right input channel.

    Returns:
        The rows of ``right``, and every row of ``left`` in arrival order: the rows
        kept while ``right`` was collected, the pending receive's row, then the rest
        of ``left`` as it arrives.

    Raises:
        Exception: Whatever collecting ``right`` raises. A pending left receive is
            then cancelled, since the left rows are not going to be read.
    """
    right_task = asyncio.create_task(right.collect())
    kept: deque[_Row] = deque()
    receive: asyncio.Task[_Row] | None = None
    left_ended = False
    try:
        while not right_task.done():
            if receive is None:
                receive = asyncio.create_task(left.receive())
            _ = await asyncio.wait(
                (receive, right_task), return_when=asyncio.FIRST_COMPLETED
            )
            if receive.done():
                finished, receive = receive, None
                try:
                    kept.append(finished.result())
                except ChannelClosed:
                    left_ended = True
                    break
        right_rows = await right_task
    except BaseException:
        if receive is not None:
            await _cancel_receive(receive)
        raise
    finally:
        if not right_task.done():
            _ = right_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await right_task
    return right_rows, _LeftRows(left, kept, receive, left_ended)


async def _cancel_receive(receive: "asyncio.Task[_Row]") -> None:
    """Cancel a left receive whose row is not going to be read, and wait for it to end.

    Args:
        receive: The receive task; it may already be done.
    """
    _ = receive.cancel()
    _ = await asyncio.wait((receive,))
    if not receive.cancelled():
        # Retrieve its outcome (a row, or ``ChannelClosed``) so it is not reported.
        _ = receive.exception()


@final
class _LeftRows:
    """Every row of the left input in arrival order, for one pass.

    The rows kept while the right input was collected come first, then the row of
    the receive that was pending when it ended, then the rest of the left channel.
    """

    def __init__(
        self,
        left: ReadableChannel[_Row],
        kept: deque[_Row],
        pending: "asyncio.Task[_Row] | None",
        left_ended: bool,
    ) -> None:
        self._left = left
        self._kept = kept
        self._pending = pending
        self._left_ended = left_ended
        self._rows = self._iterate()

    def __aiter__(self) -> AsyncIterator[_Row]:
        return self._rows

    async def _iterate(self) -> AsyncGenerator[_Row, None]:
        while self._kept:
            yield self._kept.popleft()
        if self._pending is not None:
            try:
                row = await self._pending
            except ChannelClosed:
                return
            finally:
                self._pending = None
            yield row
        if not self._left_ended:
            async for row in self._left:
                yield row

    async def aclose(self) -> None:
        """Release the left input when its rows are not all read.

        A pending receive is cancelled. After a complete pass there is none, so this
        does nothing.
        """
        await self._rows.aclose()
        if self._pending is not None:
            pending, self._pending = self._pending, None
            await _cancel_receive(pending)
