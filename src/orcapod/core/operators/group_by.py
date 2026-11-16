"""GroupBy operator — many->one reduction keyed on tag values."""

from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any, get_origin

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def _null_last_key(value: Any) -> tuple[Any, ...]:
    """Build an order key that sorts nulls last, at any list depth.

    The leading integer keeps a null comparable against a real value of any
    type.  A list is keyed element by element, so a list-valued non-key tag
    that holds a null (a list-typed tag column of a source) still compares.

    Args:
        value: A tag or system-tag value: a scalar, ``None``, or a list of
            these nested to any depth.

    Returns:
        A tuple that orders values ascending, with nulls after every value.
    """
    if value is None:
        return (1,)
    if isinstance(value, list):
        return (0, tuple(_null_last_key(v) for v in value))
    return (0, value)


GROUP_BY_FOLD_LABEL = "group_v1"
"""Label of the folds of ``GroupBy``: the folded source id and record id are
``"group_v1:<hex>"`` (ITL-1154). It names the operation; the procedure is
``arrow_utils.fold_system_tag_values``. Change it only when this operation's fold
would give an existing member list a different value.
"""


class GroupBy(UnaryOperator):
    """Reduce rows sharing a key into one row with list-valued data members.

    This is the only many->one operator.  Every other operator preserves one
    row per tag; ``GroupBy`` collapses N rows into one, which is what lets a
    downstream pod receive a whole group at once (for example, all of a
    recording session's per-probe result parquets).

    Given tags ``(subject, date, probe)`` and data ``(path)``, grouping by
    ``["subject", "date"]`` emits one row per distinct ``(subject, date)``:

    * ``subject`` and ``date`` stay scalar tag columns, in the input's tag
      order; they are the only tag columns of the output
    * ``probe`` is **dropped** (ITL-1070): a list of the members' values is not
      a key for the group, as SQL drops a non-key, non-aggregated column after
      ``GROUP BY``
    * ``path`` becomes list-valued
    * the ``_source_*`` column of each data column becomes list-valued, one
      element per member
    * system-tag columns fold to a scalar digest and gain a
      ``::{pipeline_hash}`` name suffix

    No data element ever comes from a tag value: ``GroupBy`` uses tags to form
    groups, but the data columns of the output are exactly the data columns of
    the input.  The members' values of a dropped tag are not carried anywhere.
    If a downstream step needs to know which member each element came from,
    make that value a data column before grouping: declare it as data at the
    source, or use ``DerivedSource(origin, tags_as_data={...})``.

    Members are sorted by the input's non-group-key tag values, with ``record_id``
    appended as a final tiebreaker, so the emitted lists are stable across
    runs.  This matters because orcapod hashes those lists to build the cache
    key -- an unsorted list would make an identical member set hash differently
    and trigger a spurious recompute.  Groups themselves are emitted in
    group-key order for the same reason: a reordered input must produce an
    identical output.

    Tag tuples are expected to be unique within a stream, but nothing enforces
    that.  If the input does contain duplicate tag tuples, the members sharing
    a tuple are separated only by ``record_id``, which is fixed when the source
    is materialized -- so their order is stable for a given source table, but
    undefined if the source table itself is permuted.

    A grouped stream can be grouped again on a coarser key: its data lists nest
    one level deeper (``list[list[T]]``), as source info does.  The members of
    the second grouping are ordered by the first grouping's keys that are not
    in the coarser key (scalar tags of the grouped stream), then by
    ``record_id``; a tag the first grouping dropped no longer takes part.  A
    list-valued tag of the input can order members but cannot be a grouping
    key.

    Contrast with ``Batch``, which partitions by row count for throughput.  It
    has no key, so it keeps every tag column, list-valued: those lists are
    what tells a batch's members apart.

    Args:
        by: Tag column names to group on.  Must be non-empty, free of
            duplicates, and must all be scalar tag columns of the input stream.
    """

    def __init__(self, by: Collection[str], **kwargs: Any) -> None:
        by_tuple = tuple(by)
        if not by_tuple:
            raise ValueError("GroupBy requires at least one column in `by`.")
        duplicates = sorted({c for c in by_tuple if by_tuple.count(c) > 1})
        if duplicates:
            raise ValueError(
                f"GroupBy `by` contains duplicate column names: {duplicates}. "
                f"Got {list(by_tuple)}."
            )
        self.by = by_tuple
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        return (self.__class__.__name__, self.by)

    def to_config(self) -> dict[str, Any]:
        """Serialize this GroupBy operator to a config dict.

        ``by`` is emitted as a list rather than a tuple so the config stays
        JSON-serializable; ``__init__`` normalizes it back to a tuple.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``by``.
        """
        config = super().to_config()
        config["config"] = {"by": list(self.by)}
        return config

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Verify every grouping column is a scalar tag column of the input.

        Args:
            stream: The upstream stream to validate.

        Raises:
            InputValidationError: If any name in ``by`` is not a tag column, or
                names a list-valued tag column.
        """
        tag_columns, data_columns = stream.keys()
        missing = [c for c in self.by if c not in tag_columns]
        if missing:
            raise InputValidationError(
                f"GroupBy: {missing} are not tag columns of the input stream. "
                f"Available tag columns: {list(tag_columns)}. "
                f"(Data columns cannot be grouping keys: {list(data_columns)})"
            )

        # A list-valued tag -- what `Batch` produces -- is unhashable, so it
        # cannot key a group.  Check the schema rather than hashing a value so
        # the error names the operator and column instead of surfacing a bare
        # `TypeError: unhashable type: 'list'` from the grouping loop.
        tag_types, _ = stream.output_schema()
        non_scalar = {
            c: tag_types.get(c) for c in self.by if get_origin(tag_types.get(c)) is list
        }
        if non_scalar:
            raise InputValidationError(
                f"GroupBy: {list(non_scalar)} are list-valued tag columns and "
                f"cannot be grouping keys. Column types: {non_scalar}. "
                "A list-valued tag usually means the stream came from `Batch`; "
                "group before batching rather than after."
            )

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Partition rows by group key and emit one row per group.

        Args:
            stream: The upstream stream.

        Returns:
            A stream with one row per distinct group-key tuple, whose tag
            columns are the group keys, scalar, in the input's tag order.
        """
        table = stream.as_table(columns={"source": True, "system_tags": True})
        tag_columns, _ = stream.keys()

        system_tag_columns = tuple(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        # A tag that is not a key is dropped from the output (ITL-1070), but it
        # still orders the members below, so the data lists keep their order.
        dropped_tags = tuple(c for c in tag_columns if c not in self.by)
        member_columns = tuple(
            c
            for c in table.column_names
            if c not in self.by
            and c not in system_tag_columns
            and c not in dropped_tags
        )
        # Non-key user tags order the members of a group.  The `record_id`
        # columns are appended as a final tiebreaker: tag tuples are supposed
        # to be unique within a stream, but nothing enforces that, and without
        # the tiebreaker duplicate tuples would fall back to emission order --
        # which Ray scheduling and DB fetch order make nondeterministic.
        # `record_id` is fixed when the source is materialized, so it is immune
        # to that shuffling.
        #
        # A joined stream carries one `record_id` per canonical input position,
        # and the broadcast side of a fan-out repeats its id on every row it
        # was joined into.  So *every* record_id column has to take part --
        # consulting only the first would tie on exactly the fan-out rows the
        # tiebreaker exists for.  They are sorted by name so the key order is
        # itself independent of column order in the input table. A record id
        # names a row within its source only (ITL-1154), so the source-id columns
        # come last: two members with equal record ids from different sources still
        # have one order.
        sort_columns = dropped_tags
        for prefix in (
            constants.SYSTEM_TAG_RECORD_ID_PREFIX,
            constants.SYSTEM_TAG_SOURCE_ID_PREFIX,
        ):
            sort_columns += tuple(
                sorted(c for c in system_tag_columns if c.startswith(prefix))
            )

        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in table.to_pylist():
            groups.setdefault(tuple(row[c] for c in self.by), []).append(row)

        grouped_rows: list[dict[str, Any]] = []
        # Emit groups in key order rather than first-appearance order, so a
        # reordered input produces a byte-identical output table.
        for key, members in sorted(
            groups.items(), key=lambda kv: tuple(_null_last_key(v) for v in kv[0])
        ):
            if sort_columns:
                members.sort(
                    key=lambda r: tuple(_null_last_key(r[c]) for c in sort_columns)
                )

            grouped_rows.append(
                {
                    **dict(zip(self.by, key)),
                    **{c: [m[c] for m in members] for c in member_columns},
                    **{
                        c: arrow_utils.fold_system_tag_values(
                            [m[c] for m in members], label=GROUP_BY_FOLD_LABEL
                        )
                        for c in system_tag_columns
                    },
                }
            )

        # The schema handed over must not name the dropped tags: the builder
        # emits every column of it, and would bring them back as nulls.
        grouped_table = arrow_utils.build_aggregated_table(
            grouped_rows,
            table.drop_columns(list(dropped_tags)).schema,
            member_columns,
            stream.data_context.type_converter,
        )

        n_char = self.orcapod_config.hashing.system_tag_n_char
        grouped_table = arrow_utils.append_to_system_tags(
            grouped_table, stream.pipeline_hash().to_hex(n_char)
        )

        # The keys, in the input's tag order rather than the order of `by`.
        return ArrowTableStream(
            grouped_table,
            tag_columns=[c for c in tag_columns if c in self.by],
            data_context=stream.data_context,
        )

    # ------------------------------------------------------------------
    # Schema prediction
    # ------------------------------------------------------------------

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Predict the grouped output schemas without grouping.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config.
            all_info: Include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.  The tag schema holds the
            group keys, scalar, in the input's tag order, and the system tags,
            scalar under their suffixed names; every other user tag is
            dropped.  The data schema holds the input's data columns as
            ``list[T]``.
        """
        tag_types, data_types = stream.output_schema(columns=columns, all_info=all_info)
        n_char = self.orcapod_config.hashing.system_tag_n_char
        suffix = stream.pipeline_hash().to_hex(n_char)

        out_tag_types: dict[str, Any] = {}
        for name, col_type in tag_types.items():
            if name.startswith(constants.SYSTEM_TAG_PREFIX):
                out_tag_types[f"{name}{constants.BLOCK_SEPARATOR}{suffix}"] = col_type
            elif name in self.by:
                out_tag_types[name] = col_type
            # Any other user tag is dropped (ITL-1070).

        out_data_types = {name: list[t] for name, t in data_types.items()}

        return Schema(out_tag_types), Schema(out_data_types)
