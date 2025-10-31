import logging
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, Literal


from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

# Module-level logger
logger = logging.getLogger(__name__)


class NoOpDatabase:
    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record: "pa.Table",
        skip_duplicates: bool = False,
        flush: bool = False,
        schema_handling: Literal["merge", "error", "coerce"] = "error",
    ) -> None:
        return

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
        schema_handling: Literal["merge", "error", "coerce"] = "error",
    ) -> None:
        return

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
        retrieve_pending: bool = True,
    ) -> pa.Table | None:
        return None

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        return None

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        return None

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: "Collection[str] | pl.Series | pa.Array",
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        return None

    def flush(self) -> None:
        """Flush all pending batches."""
        return None

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        """
        Flush pending batch for a specific source path.

        Args:
            record_path: Tuple of path components
        """
        return None
