from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class NoOpArrowDatabase:
    """
    An ArrowDatabaseProtocol implementation that performs no real storage.

    All write operations are silently discarded. All read operations return
    None (empty / not found). Useful as a placeholder where a database
    dependency is required by an interface but persistence is unwanted —
    e.g. dry-run pipelines, testing that code paths execute without I/O,
    or benchmarking pure compute overhead.
    """

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record: "pa.Table",
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        pass

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: "pa.Table",
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        pass

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        return None

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> "pa.Table | None":
        return None

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        return None

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        return None

    def flush(self) -> None:
        pass
