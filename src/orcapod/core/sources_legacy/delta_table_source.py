from collections.abc import Collection
from typing import TYPE_CHECKING, Any


from orcapod.core.streams import TableStream
from orcapod.protocols import core_protocols as cp
from orcapod.types import PathLike, Schema
from orcapod.utils.lazy_module import LazyModule
from pathlib import Path


from orcapod.core.sources.base import SourceBase
from orcapod.core.sources.source_registry import GLOBAL_SOURCE_REGISTRY, SourceRegistry
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class DeltaTableSource(SourceBase):
    """Source that generates streams from a Delta table."""

    def __init__(
        self,
        delta_table_path: PathLike,
        tag_columns: Collection[str] = (),
        source_name: str | None = None,
        source_registry: SourceRegistry | None = None,
        auto_register: bool = True,
        **kwargs,
    ):
        """
        Initialize DeltaTableSource with a Delta table.

        Args:
            delta_table_path: Path to the Delta table
            source_name: Name for this source (auto-generated if None)
            tag_columns: Column names to use as tags vs packet data
            source_registry: Registry to register with (uses global if None)
            auto_register: Whether to auto-register this source
        """
        super().__init__(**kwargs)

        # Normalize path
        self._delta_table_path = Path(delta_table_path).resolve()

        # Try to open the Delta table
        try:
            self._delta_table = DeltaTable(str(self._delta_table_path))
        except TableNotFoundError:
            raise ValueError(f"Delta table not found at {self._delta_table_path}")

        # Generate source name if not provided
        if source_name is None:
            source_name = self._delta_table_path.name

        self._source_name = source_name
        self._tag_columns = tuple(tag_columns)
        self._cached_table_stream: TableStream | None = None

        # Auto-register with global registry
        if auto_register:
            registry = source_registry or GLOBAL_SOURCE_REGISTRY
            registry.register(self.source_id, self)

    @property
    def reference(self) -> tuple[str, ...]:
        """Reference tuple for this source."""
        return ("delta_table", self._source_name)

    def source_identity_structure(self) -> Any:
        """
        Identity structure for this source - includes path and modification info.
        This changes when the underlying Delta table changes.
        """
        # Get Delta table version for change detection
        table_version = self._delta_table.version()

        return {
            "class": self.__class__.__name__,
            "path": str(self._delta_table_path),
            "version": table_version,
            "tag_columns": self._tag_columns,
        }

    def validate_inputs(self, *streams: cp.StreamProtocol) -> None:
        """Delta table sources don't take input streams."""
        if len(streams) > 0:
            raise ValueError(
                f"DeltaTableSource doesn't accept input streams, got {len(streams)}"
            )

    def source_output_types(
        self, include_system_tags: bool = False
    ) -> tuple[Schema, Schema]:
        """Return tag and packet types based on Delta table schema."""
        # Create a sample stream to get types
        return self.forward().types(include_system_tags=include_system_tags)

    def forward(self, *streams: cp.StreamProtocol) -> cp.StreamProtocol:
        """
        Generate stream from Delta table data.

        Returns:
            TableStream containing all data from the Delta table
        """
        if self._cached_table_stream is None:
            # Refresh table to get latest data
            self._refresh_table()

            # Load table data
            table_data = self._delta_table.to_pyarrow_dataset(
                as_large_types=True
            ).to_table()

            self._cached_table_stream = TableStream(
                table=table_data,
                tag_columns=self._tag_columns,
                source=self,
            )
        return self._cached_table_stream

    def _refresh_table(self) -> None:
        """Refresh the Delta table to get latest version."""
        try:
            # Create fresh Delta table instance to get latest data
            self._delta_table = DeltaTable(str(self._delta_table_path))
        except Exception as e:
            # If refresh fails, log but continue with existing table
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                f"Failed to refresh Delta table {self._delta_table_path}: {e}"
            )

    def get_table_info(self) -> dict[str, Any]:
        """Get metadata about the Delta table."""
        self._refresh_table()

        schema = self._delta_table.schema()
        history = self._delta_table.history()

        return {
            "path": str(self._delta_table_path),
            "version": self._delta_table.version(),
            "schema": schema,
            "num_files": len(self._delta_table.files()),
            "tag_columns": self._tag_columns,
            "latest_commit": history[0] if history else None,
        }

    def resolve_field(self, collection_id: str, record_id: str, field_name: str) -> Any:
        """
        Resolve a specific field value from source field reference.

        For Delta table sources:
        - collection_id: Not used (single table)
        - record_id: Row identifier (implementation dependent)
        - field_name: Column name
        """
        # This is a basic implementation - you might want to add more sophisticated
        # record identification based on your needs

        # For now, assume record_id is a row index
        try:
            row_index = int(record_id)
            table_data = self._delta_table.to_pyarrow_dataset(
                as_large_types=True
            ).to_table()

            if row_index >= table_data.num_rows:
                raise ValueError(
                    f"Record ID {record_id} out of range (table has {table_data.num_rows} rows)"
                )

            if field_name not in table_data.column_names:
                raise ValueError(
                    f"Field '{field_name}' not found in table columns: {table_data.column_names}"
                )

            return table_data[field_name][row_index].as_py()

        except ValueError as e:
            if "invalid literal for int()" in str(e):
                raise ValueError(
                    f"Record ID must be numeric for DeltaTableSource, got: {record_id}"
                )
            raise

    def __repr__(self) -> str:
        return (
            f"DeltaTableSource(path={self._delta_table_path}, name={self._source_name})"
        )

    def __str__(self) -> str:
        return f"DeltaTableSource:{self._source_name}"
