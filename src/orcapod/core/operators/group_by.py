"""GroupBy operator — many->one reduction keyed on tag values."""

from __future__ import annotations

import logging
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

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

logger = logging.getLogger(__name__)


class GroupBy(UnaryOperator):
    """Reduce rows sharing a tag tuple into one packet with list-valued members.

    This is the only many->one operator.  Every other operator preserves one
    row per tag; ``GroupBy`` collapses N rows into one, which is what lets a
    downstream pod receive a whole group at once (for example, all of a
    recording session's per-probe result parquets).

    Given tags ``(subject, date, probe)`` and data ``(path)``, grouping by
    ``["subject", "date"]`` emits one row per distinct ``(subject, date)``:

    * ``subject`` and ``date`` stay scalar and remain the output's tag columns
    * ``probe`` becomes a list-valued **data** column, so a consumer can tell
      which member each list element came from
    * ``path`` becomes list-valued
    * ``_source_*`` columns become list-valued, one element per member
    * system-tag columns fold to a scalar digest and gain a
      ``::{pipeline_hash}`` name suffix

    Members are sorted by their non-group-key tag values, so the emitted lists
    are stable across runs.  This matters because orcapod hashes those lists to
    build the cache key -- an unsorted list would make an identical member set
    hash differently and trigger a spurious recompute.  Groups themselves are
    emitted in group-key order for the same reason: a reordered input must
    produce an identical output.

    Contrast with ``Batch``, which partitions by row count for throughput and
    keeps its tag columns as list-valued tags.

    Args:
        by: Tag column names to group on.  Must be non-empty and must all be
            tag columns of the input stream.
    """

    def __init__(self, by: Collection[str], **kwargs: Any) -> None:
        by_tuple = tuple(by)
        if not by_tuple:
            raise ValueError("GroupBy requires at least one column in `by`.")
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
        """Verify every grouping column is a tag column of the input.

        Args:
            stream: The upstream stream to validate.

        Raises:
            InputValidationError: If any name in ``by`` is not a tag column.
        """
        tag_columns, data_columns = stream.keys()
        missing = [c for c in self.by if c not in tag_columns]
        if missing:
            raise InputValidationError(
                f"GroupBy: {missing} are not tag columns of the input stream. "
                f"Available tag columns: {list(tag_columns)}. "
                f"(Data columns cannot be grouping keys: {list(data_columns)})"
            )

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Partition rows by group key and emit one row per group.

        Args:
            stream: The upstream stream.

        Returns:
            A stream with one row per distinct group-key tuple.
        """
        table = stream.as_table(columns={"source": True, "system_tags": True})
        tag_columns, _ = stream.keys()

        system_tag_columns = tuple(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        member_columns = tuple(
            c
            for c in table.column_names
            if c not in self.by and c not in system_tag_columns
        )
        # Non-key user tags give a total order within a group: tags are unique
        # within a stream.  When `by` covers every tag, fall back to record_id.
        sort_columns = tuple(c for c in tag_columns if c not in self.by)
        record_id_column = next(
            (
                c
                for c in system_tag_columns
                if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
            ),
            None,
        )

        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in table.to_pylist():
            groups.setdefault(tuple(row[c] for c in self.by), []).append(row)

        grouped_rows: list[dict[str, Any]] = []
        # Emit groups in key order rather than first-appearance order, so a
        # reordered input produces a byte-identical output table.
        for key, members in sorted(
            groups.items(), key=lambda kv: tuple((v is None, v) for v in kv[0])
        ):
            if sort_columns:
                # The leading bool keeps a null comparable against a real
                # value of any type, and sorts nulls last.
                members.sort(
                    key=lambda r: tuple((r[c] is None, r[c]) for c in sort_columns)
                )
            elif record_id_column is not None:
                members.sort(key=lambda r: r[record_id_column] or b"")

            grouped_rows.append({
                **dict(zip(self.by, key)),
                **{c: [m[c] for m in members] for c in member_columns},
                **{
                    c: arrow_utils.fold_system_tag_values(c, [m[c] for m in members])
                    for c in system_tag_columns
                },
            })

        input_fields = {f.name: f for f in table.schema}
        grouped_schema = pa.schema([
            pa.field(c, pa.list_(input_fields[c].type), nullable=False)
            if c in member_columns
            else input_fields[c]
            for c in table.column_names
        ])
        grouped_table = pa.Table.from_pylist(grouped_rows, schema=grouped_schema)

        n_char = self.orcapod_config.hashing.system_tag_n_char
        grouped_table = arrow_utils.append_to_system_tags(
            grouped_table, stream.pipeline_hash().to_hex(n_char)
        )

        return ArrowTableStream(
            grouped_table,
            tag_columns=self.by,
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
            A ``(tag_schema, data_schema)`` tuple.  Group keys stay scalar in
            the tag schema; promoted non-key tags and list-wrapped data columns
            land in the data schema.
        """
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        tag_types, data_types = stream.output_schema(columns=columns, all_info=all_info)
        n_char = self.orcapod_config.hashing.system_tag_n_char
        suffix = stream.pipeline_hash().to_hex(n_char)

        out_tag_types: dict[str, Any] = {}
        out_data_types: dict[str, Any] = {}

        for name, col_type in tag_types.items():
            if name.startswith(constants.SYSTEM_TAG_PREFIX):
                out_tag_types[f"{name}{constants.BLOCK_SEPARATOR}{suffix}"] = col_type
            elif name in self.by:
                out_tag_types[name] = col_type
            else:
                # Promoted to a list-valued data column.
                out_data_types[name] = list[col_type]
                if column_config.source:
                    # Promoted columns carry no provenance token; the stream
                    # fills in a scalar null.
                    out_data_types[f"{constants.SOURCE_PREFIX}{name}"] = str

        for name, col_type in data_types.items():
            out_data_types[name] = list[col_type]

        return Schema(out_tag_types), Schema(out_data_types)
