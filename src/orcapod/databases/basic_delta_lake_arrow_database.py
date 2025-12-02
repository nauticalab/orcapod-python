import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from orcapod.system_constants import constants
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    pc = LazyModule("pyarrow.compute")

# Module-level logger
logger = logging.getLogger(__name__)


class BasicDeltaTableArrowStore:
    """
    A basic Delta Table-based Arrow data store with flexible hierarchical path support.
    This store does NOT implement lazy loading or streaming capabilities, therefore
    being "basic" in that sense. It is designed for simple use cases where data is written
    in batches and read back as complete tables. It is worth noting that the Delta table
    structure created by this store IS compatible with more advanced Delta Table-based
    data stores (to be implemented) that will support lazy loading and streaming.

    Uses tuple-based source paths for robust parameter handling:
    - ("source_name", "source_id") -> source_name/source_id/
    - ("org", "project", "dataset") -> org/project/dataset/
    - ("year", "month", "day", "experiment") -> year/month/day/experiment/
    """

    RECORD_ID_COLUMN = f"{constants.META_PREFIX}record_id"

    def __init__(
        self,
        base_path: str | Path,
        duplicate_entry_behavior: str = "error",
        create_base_path: bool = True,
        max_hierarchy_depth: int = 10,
        batch_size: int = 100,
    ):
        """
        Initialize the BasicDeltaTableArrowStore.

        Args:
            base_path: Base directory path where Delta tables will be stored
            duplicate_entry_behavior: How to handle duplicate record_ids:
                - 'error': Raise ValueError when record_id already exists
                - 'overwrite': Replace existing entry with new data
            create_base_path: Whether to create the base path if it doesn't exist
            max_hierarchy_depth: Maximum allowed depth for source paths (safety limit)
            batch_size: Number of records to batch before writing to Delta table
        """
        # Validate duplicate behavior
        if duplicate_entry_behavior not in ["error", "overwrite"]:
            raise ValueError("duplicate_entry_behavior must be 'error' or 'overwrite'")

        self.duplicate_entry_behavior = duplicate_entry_behavior
        self.base_path = Path(base_path)
        self.max_hierarchy_depth = max_hierarchy_depth
        self.batch_size = batch_size

        if create_base_path:
            self.base_path.mkdir(parents=True, exist_ok=True)
        elif not self.base_path.exists():
            raise ValueError(
                f"Base path {self.base_path} does not exist and create_base_path=False"
            )

        # Cache for Delta tables to avoid repeated initialization
        self._delta_table_cache: dict[str, DeltaTable] = {}

        # Batch management
        self._pending_batches: dict[str, dict[str, pa.Table]] = defaultdict(dict)

        logger.info(
            f"Initialized DeltaTableArrowDataStore at {self.base_path} "
            f"with duplicate_entry_behavior='{duplicate_entry_behavior}', "
            f"batch_size={batch_size}, as"
        )

    def flush(self) -> None:
        """
        Flush all pending batches immediately.

        This method is called to ensure all pending data is written to the Delta tables.
        """
        try:
            self.flush_all_batches()
        except Exception as e:
            logger.error(f"Error during flush: {e}")

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        """
        Flush pending batch for a specific source path.

        Args:
            record_path: Tuple of path components
        """
        logger.debug("Flushing triggered!!")
        source_key = self._get_source_key(record_path)

        if (
            source_key not in self._pending_batches
            or not self._pending_batches[source_key]
        ):
            return

        # Get all pending records
        pending_tables = self._pending_batches[source_key]
        self._pending_batches[source_key] = {}

        try:
            # Combine all tables in the batch
            combined_table = pa.concat_tables(pending_tables.values()).combine_chunks()

            table_path = self._get_table_path(record_path)
            table_path.mkdir(parents=True, exist_ok=True)

            # Check if table exists
            delta_table = self._get_existing_delta_table(record_path)

            if delta_table is None:
                # TODO: reconsider mode="overwrite" here
                write_deltalake(
                    table_path,
                    combined_table,
                    mode="overwrite",
                )
                logger.debug(
                    f"Created new Delta table for {source_key} with {len(combined_table)} records"
                )
            else:
                if self.duplicate_entry_behavior == "overwrite":
                    # Get entry IDs from the batch
                    record_ids = combined_table.column(
                        self.RECORD_ID_COLUMN
                    ).to_pylist()
                    unique_record_ids = cast(list[str], list(set(record_ids)))

                    # Delete existing records with these IDs
                    if unique_record_ids:
                        record_ids_str = "', '".join(unique_record_ids)
                        delete_predicate = (
                            f"{self.RECORD_ID_COLUMN} IN ('{record_ids_str}')"
                        )
                        try:
                            delta_table.delete(delete_predicate)
                            logger.debug(
                                f"Deleted {len(unique_record_ids)} existing records from {source_key}"
                            )
                        except Exception as e:
                            logger.debug(
                                f"No existing records to delete from {source_key}: {e}"
                            )

                # otherwise, only insert if same record_id does not exist yet
                delta_table.merge(
                    source=combined_table,
                    predicate=f"target.{self.RECORD_ID_COLUMN} = source.{self.RECORD_ID_COLUMN}",
                    source_alias="source",
                    target_alias="target",
                ).when_not_matched_insert_all().execute()

                logger.debug(
                    f"Appended batch of {len(combined_table)} records to {source_key}"
                )

            # Update cache
            self._delta_table_cache[source_key] = DeltaTable(str(table_path))

        except Exception as e:
            logger.error(f"Error flushing batch for {source_key}: {e}")
            # Put the tables back in the pending queue
            self._pending_batches[source_key] = pending_tables
            raise

    def flush_all_batches(self) -> None:
        """Flush all pending batches."""
        source_keys = list(self._pending_batches.keys())

        # TODO: capture and re-raise exceptions at the end
        for source_key in source_keys:
            record_path = tuple(source_key.split("/"))
            try:
                self.flush_batch(record_path)
            except Exception as e:
                logger.error(f"Error flushing batch for {source_key}: {e}")

    def __del__(self):
        """Cleanup when object is destroyed."""
        self.flush()

    def _validate_record_path(self, record_path: tuple[str, ...]) -> None:
        # TODO: consider removing this as path creation can be tried directly
        """
        Validate source path components.

        Args:
            record_path: Tuple of path components

        Raises:
            ValueError: If path is invalid
        """
        if not record_path:
            raise ValueError("Source path cannot be empty")

        if len(record_path) > self.max_hierarchy_depth:
            raise ValueError(
                f"Source path depth {len(record_path)} exceeds maximum {self.max_hierarchy_depth}"
            )

        # Validate path components
        for i, component in enumerate(record_path):
            if not component or not isinstance(component, str):
                raise ValueError(
                    f"Source path component {i} is invalid: {repr(component)}"
                )

            # Check for filesystem-unsafe characters
            unsafe_chars = ["/", "\\", ":", "*", "?", '"', "<", ">", "|", "\0"]
            if any(char in component for char in unsafe_chars):
                raise ValueError(
                    f"Source path {record_path} component {component} contains invalid characters: {repr(component)}"
                )

    def _get_source_key(self, record_path: tuple[str, ...]) -> str:
        """Generate cache key for source storage."""
        return "/".join(record_path)

    def _get_table_path(self, record_path: tuple[str, ...]) -> Path:
        """Get the filesystem path for a given source path."""
        path = self.base_path
        for subpath in record_path:
            path = path / subpath
        return path

    def _get_existing_delta_table(
        self, record_path: tuple[str, ...]
    ) -> DeltaTable | None:
        """
        Get or create a Delta table, handling schema initialization properly.

        Args:
            record_path: Tuple of path components

        Returns:
            DeltaTable instance or None if table doesn't exist
        """
        source_key = self._get_source_key(record_path)
        table_path = self._get_table_path(record_path)

        # Check cache first
        if dt := self._delta_table_cache.get(source_key):
            return dt

        try:
            # Try to load existing table
            delta_table = DeltaTable(str(table_path))
            self._delta_table_cache[source_key] = delta_table
            logger.debug(f"Loaded existing Delta table for {source_key}")
            return delta_table
        except TableNotFoundError:
            # Table doesn't exist
            return None
        except Exception as e:
            logger.error(f"Error loading Delta table for {source_key}: {e}")
            # Try to clear any corrupted cache and retry once
            if source_key in self._delta_table_cache:
                del self._delta_table_cache[source_key]
            return None

    def _ensure_record_id_column(
        self, arrow_data: "pa.Table", record_id: str
    ) -> "pa.Table":
        """Ensure the table has an record id column."""
        if self.RECORD_ID_COLUMN not in arrow_data.column_names:
            # Add record_id column at the beginning
            key_array = pa.array([record_id] * len(arrow_data), type=pa.large_string())
            arrow_data = arrow_data.add_column(0, self.RECORD_ID_COLUMN, key_array)
        return arrow_data

    def _remove_record_id_column(self, arrow_data: "pa.Table") -> "pa.Table":
        """Remove the record id column if it exists."""
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            column_names = arrow_data.column_names
            indices_to_keep = [
                i
                for i, name in enumerate(column_names)
                if name != self.RECORD_ID_COLUMN
            ]
            arrow_data = arrow_data.select(indices_to_keep)
        return arrow_data

    def _handle_record_id_column(
        self, arrow_data: "pa.Table", record_id_column: str | None = None
    ) -> "pa.Table":
        """
        Handle record_id column based on add_record_id_column parameter.

        Args:
            arrow_data: Arrow table with record id column
            record_id_column: Control entry ID column inclusion:

        """
        if not record_id_column:
            # Remove the record id column
            return self._remove_record_id_column(arrow_data)

        # Rename record id column
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            schema = arrow_data.schema
            new_names = [
                record_id_column if name == self.RECORD_ID_COLUMN else name
                for name in schema.names
            ]
            return arrow_data.rename_columns(new_names)
        else:
            raise ValueError(
                f"Record ID column '{self.RECORD_ID_COLUMN}' not found in the table and cannot be renamed."
            )

    def _create_record_id_filter(self, record_id: str) -> list:
        """
        Create a proper filter expression for Delta Lake.

        Args:
            record_id: The entry ID to filter by

        Returns:
            List containing the filter expression for Delta Lake
        """
        return [(self.RECORD_ID_COLUMN, "=", record_id)]

    def _create_record_ids_filter(self, record_ids: list[str]) -> list:
        """
        Create a proper filter expression for multiple entry IDs.

        Args:
            record_ids: List of entry IDs to filter by

        Returns:
            List containing the filter expression for Delta Lake
        """
        return [(self.RECORD_ID_COLUMN, "in", record_ids)]

    def _read_table_with_filter(
        self,
        delta_table: DeltaTable,
        filters: list | None = None,
    ) -> "pa.Table":
        """
        Read table using to_pyarrow_dataset with original schema preservation.

        Args:
            delta_table: The Delta table to read from
            filters: Optional filters to apply

        Returns:
            Arrow table with preserved schema
        """
        # Use to_pyarrow_dataset with as_large_types for Polars compatible arrow table loading
        dataset = delta_table.to_pyarrow_dataset(as_large_types=True)
        if filters:
            # Apply filters at dataset level for better performance
            import pyarrow.compute as pc

            filter_expr = None
            for filt in filters:
                if len(filt) == 3:
                    col, op, val = filt
                    if op == "=":
                        expr = pc.equal(pc.field(col), pa.scalar(val))  # type: ignore
                    elif op == "in":
                        expr = pc.is_in(pc.field(col), pa.array(val))  # type: ignore
                    else:
                        logger.warning(
                            f"Unsupported filter operation: {op}. Falling back to table-level filter application which may be less efficient."
                        )
                        # Fallback to table-level filtering
                        return dataset.to_table()(filters=filters)

                    if filter_expr is None:
                        filter_expr = expr
                    else:
                        filter_expr = pc.and_(filter_expr, expr)  # type: ignore

            if filter_expr is not None:
                return dataset.to_table(filter=filter_expr)

        return dataset.to_table()

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        data: "pa.Table",
        ignore_duplicates: bool | None = None,
        overwrite_existing: bool = False,
        force_flush: bool = False,
    ) -> "pa.Table":
        self._validate_record_path(record_path)
        source_key = self._get_source_key(record_path)

        # Check for existing entry
        if ignore_duplicates is None:
            ignore_duplicates = self.duplicate_entry_behavior != "error"
        if not ignore_duplicates:
            pending_table = self._pending_batches[source_key].get(record_id, None)
            if pending_table is not None:
                raise ValueError(
                    f"Entry '{record_id}' already exists in pending batch for {source_key}. "
                    f"Use duplicate_entry_behavior='overwrite' to allow updates."
                )
            existing_record = self.get_record_by_id(record_path, record_id, flush=False)
            if existing_record is not None:
                raise ValueError(
                    f"Entry '{record_id}' already exists in {'/'.join(record_path)}. "
                    f"Use duplicate_entry_behavior='overwrite' to allow updates."
                )

        # Add record_id column to the data
        data_with_record_id = self._ensure_record_id_column(data, record_id)

        if force_flush:
            # Write immediately
            table_path = self._get_table_path(record_path)
            table_path.mkdir(parents=True, exist_ok=True)

            delta_table = self._get_existing_delta_table(record_path)

            if delta_table is None:
                # Create new table - save original schema first
                write_deltalake(str(table_path), data_with_record_id, mode="overwrite")
                logger.debug(f"Created new Delta table for {source_key}")
            else:
                if self.duplicate_entry_behavior == "overwrite":
                    try:
                        delta_table.delete(
                            f"{self.RECORD_ID_COLUMN} = '{record_id.replace(chr(39), chr(39) + chr(39))}'"
                        )
                        logger.debug(
                            f"Deleted existing record {record_id} from {source_key}"
                        )
                    except Exception as e:
                        logger.debug(
                            f"No existing record to delete for {record_id}: {e}"
                        )

                write_deltalake(
                    table_path,
                    data_with_record_id,
                    mode="append",
                    schema_mode="merge",
                )

            # Update cache
            self._delta_table_cache[source_key] = DeltaTable(str(table_path))
        else:
            # Add to the batch for later flushing
            self._pending_batches[source_key][record_id] = data_with_record_id
            batch_size = len(self._pending_batches[source_key])

            # Check if we need to flush
            if batch_size >= self.batch_size:
                self.flush_batch(record_path)

        logger.debug(f"Added record {record_id} to {source_key}")
        return data

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: "pa.Table",
        record_id_column: str | None = None,
        ignore_duplicates: bool | None = None,
        overwrite_existing: bool = False,
        force_flush: bool = False,
    ) -> list[str]:
        """
        Add multiple records to the Delta table, using one column as record_id.

        Args:
            record_path: Path tuple identifying the table location
            records: PyArrow table containing the records to add
            record_id_column: Column name to use as record_id (defaults to first column)
            ignore_duplicates: Whether to ignore duplicate entries
            overwrite_existing: Whether to overwrite existing records with same ID
            force_flush: Whether to write immediately instead of batching

        Returns:
            List of record IDs that were added
        """
        self._validate_record_path(record_path)
        source_key = self._get_source_key(record_path)

        # Determine record_id column
        if record_id_column is None:
            record_id_column = records.column_names[0]

        # Validate that the record_id column exists
        if record_id_column not in records.column_names:
            raise ValueError(
                f"Record ID column '{record_id_column}' not found in table. "
                f"Available columns: {records.column_names}"
            )

        # Rename the record_id column to the standard name
        column_mapping = {record_id_column: self.RECORD_ID_COLUMN}
        records_renamed = records.rename_columns(
            [column_mapping.get(col, col) for col in records.column_names]
        )

        # Get unique record IDs from the data
        record_ids_array = records_renamed[self.RECORD_ID_COLUMN]
        unique_record_ids = pc.unique(record_ids_array).to_pylist()

        # Set default behavior for duplicates
        if ignore_duplicates is None:
            ignore_duplicates = self.duplicate_entry_behavior != "error"

        added_record_ids = []

        # Check for duplicates if needed
        if not ignore_duplicates:
            # Check pending batches
            pending_duplicates = []
            for record_id in unique_record_ids:
                if record_id in self._pending_batches[source_key]:
                    pending_duplicates.append(record_id)

            if pending_duplicates:
                raise ValueError(
                    f"Records {pending_duplicates} already exist in pending batch for {source_key}. "
                    f"Use ignore_duplicates=True or duplicate_entry_behavior='overwrite' to allow updates."
                )

            # Check existing table
            existing_duplicates = []
            try:
                for record_id in unique_record_ids:
                    existing_record = self.get_record_by_id(
                        record_path, str(record_id), flush=False
                    )
                    if existing_record is not None:
                        existing_duplicates.append(record_id)
            except Exception as e:
                logger.debug(f"Error checking existing records: {e}")

            if existing_duplicates:
                raise ValueError(
                    f"Records {existing_duplicates} already exist in {'/'.join(record_path)}. "
                    f"Use ignore_duplicates=True or duplicate_entry_behavior='overwrite' to allow updates."
                )

        if force_flush:
            # Write immediately
            table_path = self._get_table_path(record_path)
            table_path.mkdir(parents=True, exist_ok=True)

            delta_table = self._get_existing_delta_table(record_path)

            if delta_table is None:
                # Create new table
                write_deltalake(str(table_path), records_renamed, mode="overwrite")
                logger.debug(f"Created new Delta table for {source_key}")
                added_record_ids = unique_record_ids
            else:
                # Handle existing table
                if self.duplicate_entry_behavior == "overwrite" or overwrite_existing:
                    # Delete existing records with matching IDs
                    try:
                        # Create SQL condition for multiple record IDs
                        escaped_ids = [
                            str(rid).replace("'", "''") for rid in unique_record_ids
                        ]
                        id_list = "', '".join(escaped_ids)
                        delete_condition = f"{self.RECORD_ID_COLUMN} IN ('{id_list}')"

                        delta_table.delete(delete_condition)
                        logger.debug(
                            f"Deleted existing records {unique_record_ids} from {source_key}"
                        )
                    except Exception as e:
                        logger.debug(f"No existing records to delete: {e}")

                # Filter out duplicates if not overwriting
                if not (
                    self.duplicate_entry_behavior == "overwrite" or overwrite_existing
                ):
                    # Get existing record IDs
                    try:
                        existing_table = delta_table.to_pyarrow_table()
                        if len(existing_table) > 0:
                            existing_ids = pc.unique(
                                existing_table[self.RECORD_ID_COLUMN]
                            )

                            # Filter out records that already exist
                            mask = pc.invert(
                                pc.is_in(
                                    records_renamed[self.RECORD_ID_COLUMN], existing_ids
                                )
                            )
                            records_renamed = pc.filter(records_renamed, mask)  # type: ignore

                            # Update the list of record IDs that will actually be added
                            if len(records_renamed) > 0:
                                added_record_ids = pc.unique(
                                    records_renamed[self.RECORD_ID_COLUMN]
                                ).to_pylist()
                            else:
                                added_record_ids = []
                        else:
                            added_record_ids = unique_record_ids
                    except Exception as e:
                        logger.debug(f"Error filtering duplicates: {e}")
                        added_record_ids = unique_record_ids
                else:
                    added_record_ids = unique_record_ids

                # Append the (possibly filtered) records
                if len(records_renamed) > 0:
                    write_deltalake(
                        table_path,
                        records_renamed,
                        mode="append",
                        schema_mode="merge",
                    )

            # Update cache
            self._delta_table_cache[source_key] = DeltaTable(str(table_path))

        else:
            # Add to batches for later flushing
            # Group records by record_id for individual batch entries
            for record_id in unique_record_ids:
                # Filter records for this specific record_id
                mask = pc.equal(records_renamed[self.RECORD_ID_COLUMN], record_id)  # type: ignore
                single_record = pc.filter(records_renamed, mask)  # type: ignore

                # Add to pending batch (will overwrite if duplicate_entry_behavior allows)
                if (
                    self.duplicate_entry_behavior == "overwrite"
                    or overwrite_existing
                    or record_id not in self._pending_batches[source_key]
                ):
                    self._pending_batches[source_key][str(record_id)] = single_record
                    added_record_ids.append(record_id)
                elif ignore_duplicates:
                    logger.debug(f"Ignoring duplicate record {record_id}")
                else:
                    # This should have been caught earlier, but just in case
                    logger.warning(f"Skipping duplicate record {record_id}")

            # Check if we need to flush
            batch_size = len(self._pending_batches[source_key])
            if batch_size >= self.batch_size:
                self.flush_batch(record_path)

        logger.debug(f"Added {len(added_record_ids)} records to {source_key}")
        return [str(rid) for rid in added_record_ids]

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        """
        Get a specific record by record_id with schema preservation.

        Args:
            record_path: Tuple of path components
            record_id: Unique identifier for the record

        Returns:
            Arrow table for the record or None if not found
        """

        if flush:
            self.flush_batch(record_path)
        self._validate_record_path(record_path)

        # check if record_id is found in pending batches
        source_key = self._get_source_key(record_path)
        if record_id in self._pending_batches[source_key]:
            # Return the pending record after removing the entry id column
            return self._remove_record_id_column(
                self._pending_batches[source_key][record_id]
            )

        delta_table = self._get_existing_delta_table(record_path)
        if delta_table is None:
            return None

        try:
            # Use schema-preserving read
            filter_expr = self._create_record_id_filter(record_id)
            result = self._read_table_with_filter(delta_table, filters=filter_expr)

            if len(result) == 0:
                return None

            # Handle (remove/rename) the record id column before returning
            return self._handle_record_id_column(result, record_id_column)

        except Exception as e:
            logger.error(
                f"Error getting record {record_id} from {'/'.join(record_path)}: {e}"
            )
            raise e

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
        retrieve_pending: bool = True,
        flush: bool = False,
    ) -> "pa.Table | None":
        """
        Retrieve all records for a given source path as a single table with schema preservation.

        Args:
            record_path: Tuple of path components
            record_id_column: If not None or empty, record id is returned in the result with the specified column name

        Returns:
            Arrow table containing all records with original schema, or None if no records found
        """
        # TODO: this currently reads everything into memory and then return. Consider implementation that performs everything lazily

        if flush:
            self.flush_batch(record_path)
        self._validate_record_path(record_path)

        collected_tables = []
        if retrieve_pending:
            # Check if there are pending records in the batch
            for record_id, arrow_table in self._pending_batches[
                self._get_source_key(record_path)
            ].items():
                collected_tables.append(
                    self._ensure_record_id_column(arrow_table, record_id)
                )

        delta_table = self._get_existing_delta_table(record_path)
        if delta_table is not None:
            try:
                # Use filter-based read
                result = self._read_table_with_filter(delta_table)

                if len(result) != 0:
                    collected_tables.append(result)

            except Exception as e:
                logger.error(
                    f"Error getting all records from {'/'.join(record_path)}: {e}"
                )
        if collected_tables:
            total_table = pa.concat_tables(collected_tables)

            # Handle record_id column based on parameter
            return self._handle_record_id_column(total_table, record_id_column)

        return None

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: "list[str] | pl.Series | pa.Array",
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        """
        Retrieve records by entry IDs as a single table with schema preservation.

        Args:
            record_path: Tuple of path components
            record_ids: Entry IDs to retrieve
            add_record_id_column: Control entry ID column inclusion
            preserve_input_order: If True, return results in input order with nulls for missing

        Returns:
            Arrow table containing all found records with original schema, or None if no records found
        """

        if flush:
            self.flush_batch(record_path)

        self._validate_record_path(record_path)

        # Convert input to list of strings for consistency
        if isinstance(record_ids, list):
            if not record_ids:
                return None
            record_ids_list = record_ids
        elif isinstance(record_ids, pl.Series):
            if len(record_ids) == 0:
                return None
            record_ids_list = record_ids.to_list()
        elif isinstance(record_ids, (pa.Array, pa.ChunkedArray)):
            if len(record_ids) == 0:
                return None
            record_ids_list = record_ids.to_pylist()
        else:
            raise TypeError(
                f"record_ids must be list[str], pl.Series, or pa.Array, got {type(record_ids)}"
            )

        delta_table = self._get_existing_delta_table(record_path)
        if delta_table is None:
            return None

        try:
            # Use schema-preserving read with filters
            filter_expr = self._create_record_ids_filter(
                cast(list[str], record_ids_list)
            )
            result = self._read_table_with_filter(delta_table, filters=filter_expr)

            if len(result) == 0:
                return None

            # Handle record_id column based on parameter
            return self._handle_record_id_column(result, record_id_column)

        except Exception as e:
            logger.error(
                f"Error getting records by IDs from {'/'.join(record_path)}: {e}"
            )
            return None

    def get_pending_batch_info(self) -> dict[str, int]:
        """
        Get information about pending batches.

        Returns:
            Dictionary mapping source keys to number of pending records
        """
        return {
            source_key: len(tables)
            for source_key, tables in self._pending_batches.items()
            if tables
        }

    def list_sources(self) -> list[tuple[str, ...]]:
        """
        List all available source paths.

        Returns:
            List of source path tuples
        """
        sources = []

        def _scan_directory(current_path: Path, path_components: tuple[str, ...]):
            """Recursively scan for Delta tables."""
            for item in current_path.iterdir():
                if not item.is_dir():
                    continue

                new_path_components = path_components + (item.name,)

                # Check if this directory contains a Delta table
                try:
                    DeltaTable(str(item))
                    sources.append(new_path_components)
                except TableNotFoundError:
                    # Not a Delta table, continue scanning subdirectories
                    if len(new_path_components) < self.max_hierarchy_depth:
                        _scan_directory(item, new_path_components)

        _scan_directory(self.base_path, ())
        return sources

    def delete_source(self, record_path: tuple[str, ...]) -> bool:
        """
        Delete an entire source (all records for a source path).

        Args:
            record_path: Tuple of path components

        Returns:
            True if source was deleted, False if it didn't exist
        """
        self._validate_record_path(record_path)

        # Flush any pending batches first
        self.flush_batch(record_path)

        table_path = self._get_table_path(record_path)
        source_key = self._get_source_key(record_path)

        if not table_path.exists():
            return False

        try:
            # Remove from caches
            if source_key in self._delta_table_cache:
                del self._delta_table_cache[source_key]

            # Remove directory
            import shutil

            shutil.rmtree(table_path)

            logger.info(f"Deleted source {source_key}")
            return True

        except Exception as e:
            logger.error(f"Error deleting source {source_key}: {e}")
            return False

    def delete_record(self, record_path: tuple[str, ...], record_id: str) -> bool:
        """
        Delete a specific record.

        Args:
            record_path: Tuple of path components
            record_id: ID of the record to delete

        Returns:
            True if record was deleted, False if it didn't exist
        """
        self._validate_record_path(record_path)

        # Flush any pending batches first
        self.flush_batch(record_path)

        delta_table = self._get_existing_delta_table(record_path)
        if delta_table is None:
            return False

        try:
            # Check if record exists using proper filter
            filter_expr = self._create_record_id_filter(record_id)
            existing = self._read_table_with_filter(delta_table, filters=filter_expr)
            if len(existing) == 0:
                return False

            # Delete the record using SQL-style predicate (this is correct for delete operations)
            delta_table.delete(
                f"{self.RECORD_ID_COLUMN} = '{record_id.replace(chr(39), chr(39) + chr(39))}'"
            )

            # Update cache
            source_key = self._get_source_key(record_path)
            self._delta_table_cache[source_key] = delta_table

            logger.debug(f"Deleted record {record_id} from {'/'.join(record_path)}")
            return True

        except Exception as e:
            logger.error(
                f"Error deleting record {record_id} from {'/'.join(record_path)}: {e}"
            )
            return False

    def get_table_info(self, record_path: tuple[str, ...]) -> dict[str, Any] | None:
        """
        Get metadata information about a Delta table.

        Args:
            record_path: Tuple of path components

        Returns:
            Dictionary with table metadata, or None if table doesn't exist
        """
        self._validate_record_path(record_path)

        delta_table = self._get_existing_delta_table(record_path)
        if delta_table is None:
            return None

        try:
            # Get basic info
            schema = delta_table.schema()
            history = delta_table.history()
            source_key = self._get_source_key(record_path)

            # Add pending batch info
            pending_info = self.get_pending_batch_info()
            pending_count = pending_info.get(source_key, 0)

            return {
                "path": str(self._get_table_path(record_path)),
                "record_path": record_path,
                "schema": schema,
                "version": delta_table.version(),
                "num_files": len(delta_table.files()),
                "history_length": len(history),
                "latest_commit": history[0] if history else None,
                "pending_records": pending_count,
            }

        # FIXME: handle more specific exception only
        except Exception as e:
            logger.error(f"Error getting table info for {'/'.join(record_path)}: {e}")
            return None
