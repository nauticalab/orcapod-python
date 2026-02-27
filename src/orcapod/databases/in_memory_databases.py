import logging
from collections import defaultdict
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, cast

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")

logger = logging.getLogger(__name__)


class InMemoryArrowDatabase:
    """
    A pure in-memory implementation of the ArrowDatabase protocol.

    Records are stored in PyArrow tables held in process memory.
    Data is lost when the process exits — intended for tests and ephemeral use.

    Supports the same pending-batch semantics as DeltaTableDatabase:
    records are buffered in a pending batch and become part of the committed
    store only after flush() is called (or flush=True is passed to a write method).
    """

    RECORD_ID_COLUMN = "__record_id"

    def __init__(self, max_hierarchy_depth: int = 10):
        self.max_hierarchy_depth = max_hierarchy_depth
        self._tables: dict[str, pa.Table] = {}
        self._pending_batches: dict[str, pa.Table] = {}
        self._pending_record_ids: dict[str, set[str]] = defaultdict(set)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _get_record_key(self, record_path: tuple[str, ...]) -> str:
        return "/".join(record_path)

    def _validate_record_path(self, record_path: tuple[str, ...]) -> None:
        if not record_path:
            raise ValueError("record_path cannot be empty")

        if len(record_path) > self.max_hierarchy_depth:
            raise ValueError(
                f"record_path depth {len(record_path)} exceeds maximum {self.max_hierarchy_depth}"
            )

        # Only restrict characters that break the "/".join(record_path) key scheme.
        # Unlike DeltaTableDatabase (filesystem-backed), there are no OS-level restrictions here.
        unsafe_chars = ["/", "\0"]
        for i, component in enumerate(record_path):
            if not component or not isinstance(component, str):
                raise ValueError(
                    f"record_path component {i} is invalid: {repr(component)}"
                )
            if any(char in component for char in unsafe_chars):
                raise ValueError(
                    f"record_path component {repr(component)} contains invalid characters"
                )

    # ------------------------------------------------------------------
    # Record-ID column helpers
    # ------------------------------------------------------------------

    def _ensure_record_id_column(
        self, arrow_data: "pa.Table", record_id: str
    ) -> "pa.Table":
        if self.RECORD_ID_COLUMN not in arrow_data.column_names:
            key_array = pa.array([record_id] * len(arrow_data), type=pa.large_string())
            arrow_data = arrow_data.add_column(0, self.RECORD_ID_COLUMN, key_array)
        return arrow_data

    def _remove_record_id_column(self, arrow_data: "pa.Table") -> "pa.Table":
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            arrow_data = arrow_data.drop([self.RECORD_ID_COLUMN])
        return arrow_data

    def _handle_record_id_column(
        self, arrow_data: "pa.Table", record_id_column: str | None = None
    ) -> "pa.Table":
        if not record_id_column:
            return self._remove_record_id_column(arrow_data)
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            new_names = [
                record_id_column if name == self.RECORD_ID_COLUMN else name
                for name in arrow_data.schema.names
            ]
            return arrow_data.rename_columns(new_names)
        raise ValueError(
            f"Record ID column '{self.RECORD_ID_COLUMN}' not found in the table."
        )

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _deduplicate_within_table(self, table: "pa.Table") -> "pa.Table":
        """Keep the last occurrence of each record ID within a single table."""
        if table.num_rows <= 1:
            return table

        ROW_INDEX = "__row_index"
        indices = pa.array(range(table.num_rows))
        table_with_idx = table.add_column(0, ROW_INDEX, indices)
        grouped = table_with_idx.group_by([self.RECORD_ID_COLUMN]).aggregate(
            [(ROW_INDEX, "max")]
        )
        max_indices = grouped[f"{ROW_INDEX}_max"].to_pylist()
        mask = pc.is_in(indices, pa.array(max_indices))
        return table.filter(mask)

    # ------------------------------------------------------------------
    # Internal helpers for duplicate detection
    # ------------------------------------------------------------------

    def _committed_ids(self, record_key: str) -> set[str]:
        committed = self._tables.get(record_key)
        if committed is None or committed.num_rows == 0:
            return set()
        existing_ids = committed[self.RECORD_ID_COLUMN].to_pylist()
        existing_ids = [str(id) for id in existing_ids if id is not None]
        # TODO: evaluate the efficiency of this implementation
        return set(existing_ids)

    def _filter_existing_records(
        self, record_key: str, table: "pa.Table"
    ) -> "pa.Table":
        """Filter out records whose IDs are already in pending or committed store."""
        input_ids = set(table[self.RECORD_ID_COLUMN].to_pylist())
        all_existing = input_ids & (
            self._pending_record_ids[record_key] | self._committed_ids(record_key)
        )
        if not all_existing:
            return table
        mask = pc.invert(
            pc.is_in(table[self.RECORD_ID_COLUMN], pa.array(list(all_existing)))
        )
        return table.filter(mask)

    # ------------------------------------------------------------------
    # Write methods
    # ------------------------------------------------------------------

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record: "pa.Table",
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        data_with_id = self._ensure_record_id_column(record, record_id)
        self.add_records(
            record_path=record_path,
            records=data_with_id,
            record_id_column=self.RECORD_ID_COLUMN,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: "pa.Table",
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        self._validate_record_path(record_path)

        if records.num_rows == 0:
            return

        if record_id_column is None:
            record_id_column = records.column_names[0]

        if record_id_column not in records.column_names:
            raise ValueError(
                f"record_id_column '{record_id_column}' not found in table columns: "
                f"{records.column_names}"
            )

        # Normalise to internal column name
        if record_id_column != self.RECORD_ID_COLUMN:
            rename_map = {record_id_column: self.RECORD_ID_COLUMN}
            records = records.rename_columns(
                [rename_map.get(c, c) for c in records.column_names]
            )

        # Deduplicate within the incoming batch (keep last)
        records = self._deduplicate_within_table(records)

        record_key = self._get_record_key(record_path)

        if skip_duplicates:
            records = self._filter_existing_records(record_key, records)
            if records.num_rows == 0:
                return
        else:
            # Check for conflicts in the pending batch only
            input_ids = set(records[self.RECORD_ID_COLUMN].to_pylist())
            pending_conflicts = input_ids & self._pending_record_ids[record_key]
            if pending_conflicts:
                raise ValueError(
                    f"Records with IDs {pending_conflicts} already exist in the "
                    f"pending batch. Use skip_duplicates=True to skip them."
                )

        # Add to pending batch
        existing_pending = self._pending_batches.get(record_key)
        if existing_pending is None:
            self._pending_batches[record_key] = records
        else:
            self._pending_batches[record_key] = pa.concat_tables(
                [existing_pending, records]
            )
        pending_ids = cast(list[str], records[self.RECORD_ID_COLUMN].to_pylist())
        self._pending_record_ids[record_key].update(pending_ids)

        if flush:
            self.flush()

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    def flush(self) -> None:
        for record_key in list(self._pending_batches.keys()):
            pending = self._pending_batches.pop(record_key)
            self._pending_record_ids.pop(record_key, None)

            committed = self._tables.get(record_key)
            if committed is None:
                self._tables[record_key] = pending
            else:
                # Insert-if-not-exists: keep committed rows not overwritten by new batch,
                # then append the new batch on top.
                new_ids = set(pending[self.RECORD_ID_COLUMN].to_pylist())
                mask = pc.invert(
                    pc.is_in(committed[self.RECORD_ID_COLUMN], pa.array(list(new_ids)))
                )
                kept = committed.filter(mask)
                self._tables[record_key] = pa.concat_tables([kept, pending])

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def _combined_table(self, record_key: str) -> "pa.Table | None":
        """Return pending + committed data for a key, or None if nothing exists."""
        parts = []
        committed = self._tables.get(record_key)
        if committed is not None and committed.num_rows > 0:
            parts.append(committed)
        pending = self._pending_batches.get(record_key)
        if pending is not None and pending.num_rows > 0:
            parts.append(pending)
        if not parts:
            return None
        return parts[0] if len(parts) == 1 else pa.concat_tables(parts)

    # ------------------------------------------------------------------
    # Read methods
    # ------------------------------------------------------------------

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        if flush:
            self.flush()

        record_key = self._get_record_key(record_path)

        # Check pending first
        if record_id in self._pending_record_ids[record_key]:
            pending = self._pending_batches[record_key]
            filtered = pending.filter(pc.field(self.RECORD_ID_COLUMN) == record_id)
            if filtered.num_rows > 0:
                return self._handle_record_id_column(filtered, record_id_column)

        # Check committed store
        committed = self._tables.get(record_key)
        if committed is None:
            return None
        filtered = committed.filter(pc.field(self.RECORD_ID_COLUMN) == record_id)
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> "pa.Table | None":
        record_key = self._get_record_key(record_path)
        table = self._combined_table(record_key)
        if table is None:
            return None
        return self._handle_record_id_column(table, record_id_column)

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: "Collection[str]",
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        if flush:
            self.flush()

        record_ids_list = list(record_ids)
        if not record_ids_list:
            return None

        record_key = self._get_record_key(record_path)
        table = self._combined_table(record_key)
        if table is None:
            return None

        filtered = table.filter(
            pc.is_in(table[self.RECORD_ID_COLUMN], pa.array(record_ids_list))
        )
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: "Collection[tuple[str, Any]] | Mapping[str, Any]",
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        if flush:
            self.flush()

        record_key = self._get_record_key(record_path)
        table = self._combined_table(record_key)
        if table is None:
            return None

        if isinstance(column_values, Mapping):
            pair_list = list(column_values.items())
        else:
            pair_list = cast(list[tuple[str, Any]], list(column_values))

        expressions = [pc.field(c) == v for c, v in pair_list]
        combined_expr = expressions[0]
        for expr in expressions[1:]:
            combined_expr = combined_expr & expr

        filtered = table.filter(combined_expr)
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)
