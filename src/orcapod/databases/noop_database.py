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

    def __init__(
        self,
        _path_prefix: tuple[str, ...] = (),
        _root: NoOpArrowDatabase | None = None,
        _scoped_path: tuple[str, ...] = (),
    ) -> None:
        self._path_prefix = _path_prefix
        self._root = _root
        self._scoped_path = _scoped_path

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

    @property
    def base_path(self) -> tuple[str, ...]:
        """The current relative root of this database view (always () for root instances)."""
        return self._path_prefix

    def at(self, *path_components: str) -> "NoOpArrowDatabase":
        """Return a new NoOpArrowDatabase scoped to the given sub-path.

        All reads and writes are still discarded; the prefix only affects
        the reported base_path of the returned instance.
        """
        new_root = self._root if self._root is not None else self
        new_scoped_path = self._scoped_path + path_components
        return NoOpArrowDatabase(
            _path_prefix=self._path_prefix + path_components,
            _root=new_root,
            _scoped_path=new_scoped_path,
        )

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Serialize database configuration to a JSON-compatible dict."""
        return {
            "type": "noop",
            "base_path": list(self._path_prefix),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "NoOpArrowDatabase":
        """Reconstruct a NoOpArrowDatabase from a config dict."""
        return cls(_path_prefix=tuple(config.get("base_path", [])))
