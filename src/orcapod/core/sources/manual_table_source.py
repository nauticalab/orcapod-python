from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from orcapod.core.sources.source_registry import SourceRegistry
from orcapod.core.streams import TableStream
from orcapod.errors import DuplicateTagError
from orcapod.protocols import core_protocols as cp
from orcapod.types import Schema, PythonSchemaLike
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa
else:
    pl = LazyModule("polars")
    pd = LazyModule("pandas")
    pa = LazyModule("pyarrow")

from orcapod.core.sources.base import SourceBase


class ManualDeltaTableSource(SourceBase):
    """
    A source that allows manual delta updates to a table.
    This is useful for testing and debugging purposes.

    Supports duplicate tag handling:
    - skip_duplicates=True: Use merge operation to only insert new tag combinations
    - skip_duplicates=False: Raise error if duplicate tags would be created
    """

    def __init__(
        self,
        table_path: str | Path,
        python_schema: PythonSchemaLike | None = None,
        tag_columns: Collection[str] | None = None,
        source_name: str | None = None,
        source_registry: SourceRegistry | None = None,
        **kwargs,
    ) -> None:
        """
        Initialize the ManualDeltaTableSource with a label and optional data context.
        """
        super().__init__(**kwargs)

        if source_name is None:
            source_name = Path(table_path).name

        self._source_name = source_name

        self.table_path = Path(table_path)
        self._delta_table: DeltaTable | None = None
        self.load_delta_table()

        if self._delta_table is None:
            if python_schema is None:
                raise ValueError(
                    "Delta table not found and no schema provided. "
                    "Please provide a valid Delta table path or a schema to create a new table."
                )
            if tag_columns is None:
                raise ValueError(
                    "At least one tag column must be provided when creating a new Delta table."
                )
            arrow_schema = (
                self.data_context.type_converter.python_schema_to_arrow_schema(
                    python_schema
                )
            )

            fields = []
            for field in arrow_schema:
                if field.name in tag_columns:
                    field = field.with_metadata({b"tag": b"True"})
                fields.append(field)
            arrow_schema = pa.schema(fields)

        else:
            arrow_schema = pa.schema(self._delta_table.schema().to_arrow())
            python_schema = (
                self.data_context.type_converter.arrow_schema_to_python_schema(
                    arrow_schema
                )
            )

            inferred_tag_columns = []
            for field in arrow_schema:
                if (
                    field.metadata is not None
                    and field.metadata.get(b"tag", b"False").decode().lower() == "true"
                ):
                    inferred_tag_columns.append(field.name)
            tag_columns = tag_columns or inferred_tag_columns

        self.python_schema = python_schema
        self.arrow_schema = arrow_schema
        self.tag_columns = list(tag_columns) if tag_columns else []

    @property
    def reference(self) -> tuple[str, ...]:
        return ("manual_delta", self._source_name)

    @property
    def delta_table_version(self) -> int | None:
        """
        Return the version of the delta table.
        If the table does not exist, return None.
        """
        if self._delta_table is not None:
            return self._delta_table.version()
        return None

    def forward(self, *streams: cp.Stream) -> cp.Stream:
        """Load current delta table data as a stream."""
        if len(streams) > 0:
            raise ValueError("ManualDeltaTableSource takes no input streams")

        if self._delta_table is None:
            arrow_data = pa.Table.from_pylist([], schema=self.arrow_schema)
        else:
            arrow_data = self._delta_table.to_pyarrow_dataset(
                as_large_types=True
            ).to_table()

        return TableStream(
            arrow_data, tag_columns=self.tag_columns, source=self, upstreams=()
        )

    def source_identity_structure(self) -> Any:
        """
        Return the identity structure of the kernel.
        This is a unique identifier for the kernel based on its class name and table path.
        """
        return (self.__class__.__name__, str(self.table_path))

    def source_output_types(
        self, include_system_tags: bool = False
    ) -> tuple[Schema, Schema]:
        """Return tag and packet types based on schema and tag columns."""
        # TODO: auto add system entry tag
        tag_types: Schema = {}
        packet_types: Schema = {}
        for field, field_type in self.python_schema.items():
            if field in self.tag_columns:
                tag_types[field] = field_type
            else:
                packet_types[field] = field_type
        return tag_types, packet_types

    def get_all_records(self, include_system_columns: bool = False) -> pa.Table | None:
        """Get all records from the delta table."""
        if self._delta_table is None:
            return None

        arrow_data = self._delta_table.to_pyarrow_dataset(
            as_large_types=True
        ).to_table()

        if not include_system_columns:
            arrow_data = arrow_data.drop(
                [col for col in arrow_data.column_names if col.startswith("_")]
            )
        return arrow_data

    def _normalize_data_to_table(
        self, data: "dict | pa.Table | pl.DataFrame | pd.DataFrame"
    ) -> pa.Table:
        """Convert input data to PyArrow Table with correct schema."""
        if isinstance(data, dict):
            return pa.Table.from_pylist([data], schema=self.arrow_schema)
        elif isinstance(data, pa.Table):
            return data
        else:
            # Handle polars/pandas DataFrames
            if hasattr(data, "to_arrow"):  # Polars DataFrame
                return data.to_arrow()  # type: ignore
            elif hasattr(data, "to_pandas"):  # Polars to pandas fallback
                return pa.Table.from_pandas(data.to_pandas(), schema=self.arrow_schema)  # type: ignore
            else:  # Assume pandas DataFrame
                return pa.Table.from_pandas(
                    cast(pd.DataFrame, data), schema=self.arrow_schema
                )

    def _check_for_duplicates(self, new_data: pa.Table) -> None:
        """
        Check if new data contains tag combinations that already exist.
        Raises DuplicateTagError if duplicates found.
        """
        if self._delta_table is None or not self.tag_columns:
            return  # No existing data or no tag columns to check

        # Get existing tag combinations
        existing_data = self._delta_table.to_pyarrow_dataset(
            as_large_types=True
        ).to_table()
        if len(existing_data) == 0:
            return  # No existing data

        # Extract tag combinations from existing data
        existing_tags = existing_data.select(self.tag_columns)
        new_tags = new_data.select(self.tag_columns)

        # Convert to sets of tuples for comparison
        existing_tag_tuples = set()
        for i in range(len(existing_tags)):
            tag_tuple = tuple(
                existing_tags.column(col)[i].as_py() for col in self.tag_columns
            )
            existing_tag_tuples.add(tag_tuple)

        # Check for duplicates in new data
        duplicate_tags = []
        for i in range(len(new_tags)):
            tag_tuple = tuple(
                new_tags.column(col)[i].as_py() for col in self.tag_columns
            )
            if tag_tuple in existing_tag_tuples:
                duplicate_tags.append(tag_tuple)

        if duplicate_tags:
            tag_names = ", ".join(self.tag_columns)
            duplicate_strs = [str(tags) for tags in duplicate_tags]
            raise DuplicateTagError(
                f"Duplicate tag combinations found for columns [{tag_names}]: "
                f"{duplicate_strs}. Use skip_duplicates=True to merge instead."
            )

    def _merge_data(self, new_data: pa.Table) -> None:
        """
        Merge new data using Delta Lake merge operation.
        Only inserts rows where tag combinations don't already exist.
        """
        if self._delta_table is None:
            # No existing table, just write the data
            write_deltalake(
                self.table_path,
                new_data,
                mode="overwrite",
            )
        else:
            # Use merge operation - only insert if tag combination doesn't exist
            # Build merge condition based on tag columns
            # Format: "target.col1 = source.col1 AND target.col2 = source.col2"
            merge_conditions = " AND ".join(
                f"target.{col} = source.{col}" for col in self.tag_columns
            )

            try:
                # Use Delta Lake's merge functionality
                (
                    self._delta_table.merge(
                        source=new_data,
                        predicate=merge_conditions,
                        source_alias="source",
                        target_alias="target",
                    )
                    .when_not_matched_insert_all()  # Insert when no match found
                    .execute()
                )
            except Exception:
                # Fallback: manual duplicate filtering if merge fails
                self._manual_merge_fallback(new_data)

    def _manual_merge_fallback(self, new_data: pa.Table) -> None:
        """
        Fallback merge implementation that manually filters duplicates.
        """
        if self._delta_table is None or not self.tag_columns:
            write_deltalake(self.table_path, new_data, mode="append")
            return

        # Get existing tag combinations
        existing_data = self._delta_table.to_pyarrow_dataset(
            as_large_types=True
        ).to_table()
        existing_tags = existing_data.select(self.tag_columns)

        # Create set of existing tag tuples
        existing_tag_tuples = set()
        for i in range(len(existing_tags)):
            tag_tuple = tuple(
                existing_tags.column(col)[i].as_py() for col in self.tag_columns
            )
            existing_tag_tuples.add(tag_tuple)

        # Filter new data to only include non-duplicate rows
        filtered_rows = []
        new_tags = new_data.select(self.tag_columns)

        for i in range(len(new_data)):
            tag_tuple = tuple(
                new_tags.column(col)[i].as_py() for col in self.tag_columns
            )
            if tag_tuple not in existing_tag_tuples:
                # Extract this row
                row_dict = {}
                for col_name in new_data.column_names:
                    row_dict[col_name] = new_data.column(col_name)[i].as_py()
                filtered_rows.append(row_dict)

        # Only append if there are new rows to add
        if filtered_rows:
            filtered_table = pa.Table.from_pylist(
                filtered_rows, schema=self.arrow_schema
            )
            write_deltalake(self.table_path, filtered_table, mode="append")

    def insert(
        self,
        data: "dict | pa.Table | pl.DataFrame | pd.DataFrame",
        skip_duplicates: bool = False,
    ) -> None:
        """
        Insert data into the delta table.

        Args:
            data: Data to insert (dict, PyArrow Table, Polars DataFrame, or Pandas DataFrame)
            skip_duplicates: If True, use merge operation to skip duplicate tag combinations.
                           If False, raise error if duplicate tag combinations are found.

        Raises:
            DuplicateTagError: If skip_duplicates=False and duplicate tag combinations are found.
        """
        # Normalize data to PyArrow Table
        new_data_table = self._normalize_data_to_table(data)

        if skip_duplicates:
            # Use merge operation to only insert new tag combinations
            self._merge_data(new_data_table)
        else:
            # Check for duplicates first, raise error if found
            self._check_for_duplicates(new_data_table)

            # No duplicates found, safe to append
            write_deltalake(self.table_path, new_data_table, mode="append")

        # Update our delta table reference and mark as modified
        self._set_modified_time()
        self._delta_table = DeltaTable(self.table_path)

        # Invalidate any cached streams
        self.invalidate()

    def load_delta_table(self) -> None:
        """
        Try loading the delta table from the file system.
        """
        current_version = self.delta_table_version
        try:
            delta_table = DeltaTable(self.table_path)
        except TableNotFoundError:
            delta_table = None

        if delta_table is not None:
            new_version = delta_table.version()
            if (current_version is None) or (
                current_version is not None and new_version > current_version
            ):
                # Delta table has been updated
                self._set_modified_time()

        self._delta_table = delta_table
