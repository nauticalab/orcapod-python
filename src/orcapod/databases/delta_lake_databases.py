from __future__ import annotations

import logging
import os
import sys
from collections import defaultdict
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from orcapod.databases._row_index import (
    KeyIndex,
    compact_tail,
    is_indexed_lookup,
    matching_rows,
    rows_at,
)
from orcapod.databases.utils import coerce_record_id
from orcapod.errors import (
    InputValidationError,
    StoreReadError,
    TableListingNotSupportedError,
)
from orcapod.databases.storage_utils import (
    delta_to_pyarrow_dataset,
    is_cloud_uri,
    parse_base_path,
)
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import deltalake
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as pa_ds
    from upath import UPath
else:
    deltalake = LazyModule("deltalake")
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    pc = LazyModule("pyarrow.compute")

# Module-level logger
logger = logging.getLogger(__name__)


# Directory that marks its parent directory as a Delta table.
_DELTA_LOG_DIRECTORY = "_delta_log"

# deltalake storage option -> where s3fs takes the same setting.
_S3FS_OPTION_OF: dict[str, tuple[str, ...]] = {
    "aws_access_key_id": ("key",),
    "aws_secret_access_key": ("secret",),
    "aws_session_token": ("token",),
    "aws_endpoint_url": ("client_kwargs", "endpoint_url"),
    "aws_endpoint": ("client_kwargs", "endpoint_url"),
    "aws_region": ("client_kwargs", "region_name"),
    "aws_default_region": ("client_kwargs", "region_name"),
}


def _s3fs_options(storage_options: Mapping[str, str]) -> dict[str, Any]:
    """Translate deltalake storage options into ``s3fs`` constructor arguments.

    Covers the credentials, the endpoint and the region. Other options are not
    translated; ``s3fs`` then falls back on its default credential chain.

    Args:
        storage_options: Options in deltalake's form (``AWS_*`` keys, any case).

    Returns:
        Keyword arguments for ``s3fs.S3FileSystem``.
    """
    options: dict[str, Any] = {}
    for key, value in storage_options.items():
        target = _S3FS_OPTION_OF.get(key.lower())
        if target is None:
            continue
        if len(target) == 1:
            options[target[0]] = value
        else:
            options.setdefault(target[0], {})[target[1]] = value
    return options


def _list_local_directory(directory: str) -> list[str]:
    """Return the names of the subdirectories of a local directory.

    A symbolic link to a directory is not a subdirectory here: following one that
    points at an ancestor would send the table walk in circles.
    """
    with os.scandir(directory) as entries:
        return [entry.name for entry in entries if entry.is_dir(follow_symlinks=False)]


def _join_local_path(directory: str, name: str) -> str:
    """Return the path of the entry ``name`` of a local directory."""
    return os.path.join(directory, name)


def _join_cloud_path(directory: str, name: str) -> str:
    """Return the path of the entry ``name`` of a cloud directory."""
    return f"{directory}/{name}"


def _stored_component(directory_name: str) -> str:
    """Return the path component a local directory name stands for.

    The inverse of ``DeltaTableDatabase._sanitize_path_component``: on Windows a
    ``:`` is stored as ``!``. Cloud paths are stored unchanged and need no inverse.
    """
    if sys.platform == "win32":
        return directory_name.replace("!", ":")
    return directory_name


def _allows_http(storage_options: Mapping[str, str]) -> bool:
    """Return whether deltalake's ``AWS_ALLOW_HTTP`` option is switched on."""
    for key, value in storage_options.items():
        if key.lower() in ("aws_allow_http", "allow_http"):
            return str(value).strip().lower() in ("true", "1", "yes", "on")
    return False


def _comparable_storage_type(arrow_type: pa.DataType) -> pa.DataType:
    """Return ``arrow_type`` reduced to what the append check compares.

    Extension types are replaced by their storage type and every nested field is
    made nullable, at every depth. The result is used only to compare a batch
    with a stored table (ITL-884); it is never stored, so dropping nested
    non-null flags here changes no schema or hash.

    Args:
        arrow_type: The Arrow type to reduce.

    Returns:
        The storage type with all nested fields nullable.
    """
    if isinstance(arrow_type, pa.ExtensionType):
        return _comparable_storage_type(arrow_type.storage_type)
    if pa.types.is_struct(arrow_type):
        return pa.struct(
            [pa.field(f.name, _comparable_storage_type(f.type)) for f in arrow_type]
        )
    if pa.types.is_large_list(arrow_type):
        field = arrow_type.value_field
        return pa.large_list(pa.field(field.name, _comparable_storage_type(field.type)))
    if pa.types.is_list(arrow_type):
        field = arrow_type.value_field
        return pa.list_(pa.field(field.name, _comparable_storage_type(field.type)))
    if pa.types.is_fixed_size_list(arrow_type):
        field = arrow_type.value_field
        return pa.list_(
            pa.field(field.name, _comparable_storage_type(field.type)),
            arrow_type.list_size,
        )
    if pa.types.is_map(arrow_type):
        key, item = arrow_type.key_field, arrow_type.item_field
        # pyarrow accepts key and item Fields here; its stubs only list types.
        return pa.map_(  # pyright: ignore[reportCallIssue]
            pa.field(key.name, _comparable_storage_type(key.type), nullable=False),
            pa.field(item.name, _comparable_storage_type(item.type)),
            keys_sorted=arrow_type.keys_sorted,
        )
    return arrow_type


def _without_null_types(arrow_type: pa.DataType) -> pa.DataType | None:
    """Return ``arrow_type`` without the parts typed ``null``, or ``None`` if none is left.

    A ``null``-typed value fits a column of any type, and Delta has no ``null``
    type to compare it with, so the append check leaves it out at every depth: a
    ``null`` struct field is dropped (its siblings are still compared, inside lists
    and maps too), and a type that is ``null``, or whose list items or map keys or
    values are entirely ``null``, is not compared.

    Args:
        arrow_type: A type already reduced by ``_comparable_storage_type``.

    Returns:
        The type to compare, or ``None`` when it should not be compared.
    """
    if pa.types.is_null(arrow_type):
        return None
    if pa.types.is_struct(arrow_type):
        kept = [
            pa.field(f.name, t)
            for f in arrow_type
            if (t := _without_null_types(f.type)) is not None
        ]
        return pa.struct(kept) if kept else None
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        item = _without_null_types(arrow_type.value_type)
        if item is None:
            return None
        field = pa.field(arrow_type.value_field.name, item)
        return (
            pa.large_list(field)
            if pa.types.is_large_list(arrow_type)
            else pa.list_(field)
        )
    if pa.types.is_fixed_size_list(arrow_type):
        item = _without_null_types(arrow_type.value_type)
        if item is None:
            return None
        return pa.list_(
            pa.field(arrow_type.value_field.name, item), arrow_type.list_size
        )
    if pa.types.is_map(arrow_type):
        key = _without_null_types(arrow_type.key_type)
        value = _without_null_types(arrow_type.item_type)
        if key is None or value is None:
            return None
        # pyarrow accepts key and item Fields here; its stubs only list types.
        return pa.map_(  # pyright: ignore[reportCallIssue]
            pa.field(arrow_type.key_field.name, key, nullable=False),
            pa.field(arrow_type.item_field.name, value),
            keys_sorted=arrow_type.keys_sorted,
        )
    return arrow_type


def _as_delta_writer_stores(schema: pa.Schema) -> pa.Schema:
    """Return ``schema`` with the types the Delta writer would store it as.

    Applies the conversion ``deltalake.write_deltalake`` and ``DeltaTable.merge``
    apply to incoming data (unsigned to signed integers, every timestamp to
    microseconds, fixed-size binary to binary), then Delta's own type mapping.
    Calling the writer's conversion instead of copying it keeps the append check
    in step with what the writer accepts; ``test_identical_schema_appends``
    guards it across ``deltalake`` versions.

    Args:
        schema: The schema of the rows about to be written.

    Returns:
        The schema as the Delta table would store it.
    """
    from arro3.core import Schema as Arro3Schema
    from deltalake.writer._conversion import (  # pyright: ignore[reportPrivateUsage]  # the writer's own conversion, see the docstring
        _convert_arro3_schema_to_delta,  # pyright: ignore[reportPrivateUsage]
    )

    converted = _convert_arro3_schema_to_delta(Arro3Schema.from_arrow(schema))
    return pa.schema(
        deltalake.Schema.from_arrow(converted).to_arrow()  # pyright: ignore[reportArgumentType]  # arro3 schema; pyarrow reads it through the C data interface
    )


def _without_field_metadata(delta_type: Any) -> Any:
    """Return a Delta type with the metadata of every nested field dropped.

    Args:
        delta_type: A ``deltalake.schema`` type.

    Returns:
        The same type with no field metadata at any depth.
    """
    from deltalake.schema import ArrayType, Field, MapType, StructType

    if isinstance(delta_type, StructType):
        return StructType(
            [
                Field(f.name, _without_field_metadata(f.type), nullable=f.nullable)
                for f in delta_type.fields
            ]
        )
    if isinstance(delta_type, ArrayType):
        return ArrayType(
            _without_field_metadata(delta_type.element_type),
            contains_null=delta_type.contains_null,
        )
    if isinstance(delta_type, MapType):
        return MapType(
            _without_field_metadata(delta_type.key_type),
            _without_field_metadata(delta_type.value_type),
            value_contains_null=delta_type.value_contains_null,
        )
    return delta_type


def _stored_arrow_schema(delta_table: deltalake.DeltaTable) -> pa.Schema:
    """Return the Arrow schema of the table's stored Delta schema, without metadata.

    Delta keeps the Arrow field metadata it was written with, including the
    extension type name of a logical type. Reading that back into pyarrow would
    rebuild the registered extension type over Delta's own storage type (``list``
    where Orcapod wrote ``large_list``), which the extension type rejects. The
    append check compares storage types only, so the metadata is dropped first.

    Args:
        delta_table: The loaded stored table.

    Returns:
        The stored schema as plain Arrow types.
    """
    from deltalake.schema import Field, Schema

    stored = delta_table.schema()
    plain = Schema(
        [
            Field(f.name, _without_field_metadata(f.type), nullable=f.nullable)
            for f in stored.fields
        ]
    )
    return pa.schema(plain.to_arrow())  # pyright: ignore[reportArgumentType]  # arro3 schema; pyarrow reads it through the C data interface


def _types_conflict(stored: pa.DataType, incoming: pa.DataType) -> bool:
    """Return ``True`` if ``incoming`` changes a type that ``stored`` already has.

    Both types must already be reduced by ``_comparable_storage_type``. Struct
    fields are matched by name at every depth, through list and map types. A
    field present on only one side is not a conflict, just as a top-level column
    present on only one side is not: adding or omitting it is schema evolution,
    which Delta's merge accepts or rejects itself. Any other difference is a
    conflict.

    Args:
        stored: The type the stored table has.
        incoming: The type the new rows have.

    Returns:
        Whether a type the stored table already has would change.
    """
    if pa.types.is_struct(stored) and pa.types.is_struct(incoming):
        stored_fields = {f.name: f.type for f in stored}
        return any(
            _types_conflict(stored_fields[f.name], f.type)
            for f in incoming
            if f.name in stored_fields
        )
    if (pa.types.is_large_list(stored) and pa.types.is_large_list(incoming)) or (
        pa.types.is_list(stored) and pa.types.is_list(incoming)
    ):
        return _types_conflict(stored.value_type, incoming.value_type)
    if pa.types.is_fixed_size_list(stored) and pa.types.is_fixed_size_list(incoming):
        return stored.list_size != incoming.list_size or _types_conflict(
            stored.value_type, incoming.value_type
        )
    if pa.types.is_map(stored) and pa.types.is_map(incoming):
        return _types_conflict(stored.key_type, incoming.key_type) or _types_conflict(
            stored.item_type, incoming.item_type
        )
    return stored != incoming


def _store_error_to_raise(errors: list[Exception]) -> Exception | None:
    """Pick the store error a failed flush raises as itself.

    A ``StoreReadError`` is preferred, because it is the error that fails a run
    whatever the orchestrator's ``error_policy``; failing that, an
    ``InputValidationError`` from a refused append.

    Args:
        errors: The error of every batch that failed, in flush order.

    Returns:
        The first ``StoreReadError``, else the first ``InputValidationError``, or
        ``None`` when no batch failed with a store error.
    """
    for kind in (StoreReadError, InputValidationError):
        chosen = next((e for e in errors if isinstance(e, kind)), None)
        if chosen is not None:
            return chosen
    return None


@dataclass
class _CommittedSnapshot:
    """What one version of a stored Delta table holds, kept in memory (ITL-919).

    Per-row lookups are answered from this snapshot instead of re-reading the table.
    Each part is read on first use and extended in place after this store's own
    commits, so its cost follows the rows written, not the rows stored.

    Attributes:
        version: The table version described.
        ids: The stored record ids (never null), or ``None`` until first needed.
        rows: Indexes over every stored row, read exactly as ``get_all_records``
            reads them, or ``None`` until first needed.
        files: The data files ``rows`` was read from (set together with ``rows``).
        dataset_schema: The schema of the dataset ``rows`` was read from.
    """

    version: int
    ids: set[bytes] | None = None
    rows: KeyIndex | None = None
    files: frozenset[str] = frozenset()
    dataset_schema: "pa.Schema | None" = None


class DeltaTableDatabase:
    """
    A Delta table database with clear insert vs update semantics.

    - insert(): Never overwrites existing records by default. Can skip duplicates if requested.
                Can be batched for performance. Supports composite keys.

    - update(): Always overwrites existing records. Executes immediately.
                Requires pending batches to be flushed first (or use force_flush=True).

    Supports both single column and composite (multi-column) record IDs.
    """

    # Class constants for internal column names
    ROW_INDEX_COLUMN = "__row_index"
    RECORD_ID_COLUMN = "__record_id"

    def __init__(
        self,
        base_path: str | Path | UPath,
        storage_options: dict[str, str] | None = None,
        create_base_path: bool = True,
        batch_size: int = 1000,
        max_hierarchy_depth: int = 10,
        allow_schema_evolution: bool = True,
        _path_prefix: tuple[str, ...] = (),
        _root: "DeltaTableDatabase | None" = None,
        _scoped_path: tuple[str, ...] = (),
        _shared_pending_batches: "dict[str, pa.Table] | None" = None,
        _shared_pending_record_ids: "defaultdict[str, set[bytes]] | None" = None,
        _shared_delta_table_cache: "dict[str, deltalake.DeltaTable] | None" = None,
        _shared_snapshots: "dict[str, _CommittedSnapshot] | None" = None,
        _shared_pending_indexes: "dict[str, KeyIndex] | None" = None,
    ):
        self._root_uri, self._storage_options = parse_base_path(
            base_path, storage_options
        )
        self._is_cloud: bool = is_cloud_uri(self._root_uri)
        self._path_prefix = _path_prefix
        self._root = _root
        self._scoped_path = _scoped_path
        self.batch_size = batch_size
        self.max_hierarchy_depth = max_hierarchy_depth
        self.allow_schema_evolution = allow_schema_evolution

        if not self._is_cloud:
            # _local_root is the absolute filesystem root (for list_sources, mkdir, etc.)
            # NOTE: do NOT access self._local_root on cloud instances.
            self._local_root = Path(self._root_uri)
            if create_base_path:
                self._local_root.mkdir(parents=True, exist_ok=True)
            elif not self._local_root.exists():
                raise ValueError(
                    f"Base path {self._local_root} does not exist and create_base_path=False"
                )
        # For cloud paths: create_base_path is silently ignored (no directory needed).

        # Shared state — when constructed via at(), these dicts are shared with the
        # root instance. Any handle's flush() writes every pending key of the family
        # and refreshes the loaded table and existing-ids entries for it, so these
        # caches must be family-wide too: a per-handle cache would keep serving the
        # snapshot a sibling loaded before the flush (ITL-864). All of them are
        # touched only from the thread that drives the pipeline (the async
        # orchestrator runs only user functions in worker threads).
        self._delta_table_cache: dict[str, deltalake.DeltaTable] = (
            _shared_delta_table_cache if _shared_delta_table_cache is not None else {}
        )
        if _shared_pending_batches is not None:
            self._pending_batches = _shared_pending_batches
        else:
            self._pending_batches: dict[str, pa.Table] = {}
        if _shared_pending_record_ids is not None:
            self._pending_record_ids = _shared_pending_record_ids
        else:
            self._pending_record_ids: dict[str, set[bytes]] = defaultdict(set)
        # The committed rows of each loaded table version (ITL-919), shared like the
        # loaded tables: a snapshot describes the cached table's version only.
        self._snapshots: dict[str, _CommittedSnapshot] = (
            _shared_snapshots if _shared_snapshots is not None else {}
        )
        # Indexes over each pending batch, valid while it is the key's batch.
        self._pending_indexes: dict[str, KeyIndex] = (
            _shared_pending_indexes if _shared_pending_indexes is not None else {}
        )

    def _clear_pending(self):
        """Clear all pending state."""
        self._pending_batches = {}
        self._pending_record_ids = defaultdict(set)
        # Note: next_row_index continues incrementing

    def _get_record_key(self, record_path: tuple[str, ...]) -> str:
        """Generate cache key for source storage.

        The key includes the full path (prefix + record_path) so that scoped
        instances that share pending state with the root can be correctly flushed
        from the root.
        """
        return "/".join(self._path_prefix + record_path)

    @staticmethod
    def _sanitize_path_component(component: str) -> str:
        """Sanitize a path component for the current OS.

        On Windows, colons are not allowed in filenames (reserved for drive
        letters). Replace them with '!' so that URIs containing ':' can still
        be stored safely on all platforms.
        """
        import sys

        if sys.platform == "win32":
            return component.replace(":", "!")
        return component

    def _get_table_uri(
        self, record_path: tuple[str, ...], create_dir: bool = False
    ) -> str:
        """Get the URI for a given record path, incorporating base_path prefix.

        Args:
            record_path: Tuple of path components (relative to base_path).
            create_dir: If True, create the local directory (no-op for cloud paths).
        """
        full_path = self._path_prefix + record_path  # prefix applied once, here only
        if self._is_cloud:
            return self._root_uri.rstrip("/") + "/" + "/".join(full_path)
        else:
            path = self._local_root
            for subpath in full_path:
                path = path / self._sanitize_path_component(subpath)
            if create_dir:
                path.mkdir(parents=True, exist_ok=True)
            return str(path)

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

        if len(self._path_prefix) + len(record_path) > self.max_hierarchy_depth:
            raise ValueError(
                f"Source path depth {len(record_path)} exceeds maximum "
                f"{self.max_hierarchy_depth - len(self._path_prefix)} "
                f"(base_path uses {len(self._path_prefix)} components)"
            )

        # Validate path components
        for i, component in enumerate(record_path):
            if not component or not isinstance(component, str):
                raise ValueError(
                    f"Source path component {i} is invalid: {repr(component)}"
                )

            # Check for filesystem-unsafe characters.
            # Note: ':' is handled by _sanitize_path_component (replaced with '!' on Windows)
            # and is allowed here so that prefixed path components (e.g. "schema:<hex digest>")
            # can be stored cross-platform.
            unsafe_chars = ["/", "\\", "*", "?", '"', "<", ">", "|", "\0"]
            if any(char in component for char in unsafe_chars):
                raise ValueError(
                    f"Source path {record_path} component {component} contains invalid characters: {repr(component)}"
                )

    def _get_delta_table(
        self, record_path: tuple[str, ...]
    ) -> deltalake.DeltaTable | None:
        """
        Get an existing Delta table, either from cache or by loading it.

        Args:
            record_path: Tuple of path components

        Returns:
            deltalake.DeltaTable instance or None if table doesn't exist
        """
        record_key = self._get_record_key(record_path)
        table_uri = self._get_table_uri(record_path)

        # Check cache first
        if dt := self._delta_table_cache.get(record_key):
            return dt

        try:
            # Try to load existing table
            delta_table = deltalake.DeltaTable(
                table_uri, storage_options=self._storage_options or None
            )
            self._delta_table_cache[record_key] = delta_table
            logger.debug(f"Loaded existing Delta table for {record_key}")
            return delta_table
        except deltalake.exceptions.TableNotFoundError:
            # Table doesn't exist
            return None
        except Exception as e:
            # The table exists but could not be loaded: never report it as absent.
            self._delta_table_cache.pop(record_key, None)
            raise self._read_failed(record_path) from e

    def _read_failed(self, record_path: tuple[str, ...]) -> StoreReadError:
        """Build the error for a table at ``record_path`` that exists but cannot be read.

        Args:
            record_path: Path components relative to this database view.

        Returns:
            A ``StoreReadError`` naming the table; the caller raises it from the
            backend's exception.
        """
        return StoreReadError(
            self._get_table_uri(record_path), self._path_prefix + record_path
        )

    def list_table_paths(self, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
        """List the paths of the Delta tables stored under ``prefix``.

        Walks the directories below ``prefix``: a directory that holds a
        ``_delta_log`` directory is a table. The walk goes on below a table, because
        tables can be nested (Orcapod v0.3 stores a table one level below where
        v0.2 stored it), and never enters ``_delta_log``. No table is opened. Only
        flushed tables are listed, as with ``table_exists``.

        A local store is listed exactly. A cloud store is listed through ``fsspec``
        with one list request per directory: ``s3://`` and ``s3a://`` through
        ``s3fs`` with the credentials, endpoint and region of this database's
        storage options; ``gs://``, ``az://`` and ``abfs://`` only when the matching
        ``fsspec`` package is installed and reaches the store with its default
        credentials.

        Args:
            prefix: Leading path components, relative to ``self.base_path``.

        Returns:
            The table paths, relative to ``self.base_path``, sorted.

        Raises:
            TableListingNotSupportedError: If the store is on cloud storage and
                could not be listed; the reason is chained.
            StoreReadError: If a local directory could not be listed.
        """
        full_prefix = self._path_prefix + prefix
        if self._is_cloud:
            list_directory = self._cloud_directory_lister()
            start = self._root_uri.split("://", 1)[1].rstrip("/")
            start = "/".join((start,) + full_prefix)
            join = _join_cloud_path
            component = str
        else:
            list_directory = _list_local_directory
            start = str(
                self._local_root.joinpath(
                    *(self._sanitize_path_component(part) for part in full_prefix)
                )
            )
            join = _join_local_path
            component = _stored_component

        found: list[tuple[str, ...]] = []
        # Directories still to visit, each with its path below ``prefix``.
        to_visit: list[tuple[str, tuple[str, ...]]] = [(start, ())]
        while to_visit:
            directory, below = to_visit.pop()
            try:
                children = list_directory(directory)
            except FileNotFoundError:
                continue
            except Exception as exc:
                if self._is_cloud:
                    raise TableListingNotSupportedError(
                        f"Could not list the tables of the Delta store at "
                        f"{self._root_uri!r}: {type(exc).__name__}: {exc}"
                    ) from exc
                raise StoreReadError(directory, full_prefix + below) from exc
            for name in children:
                if name == _DELTA_LOG_DIRECTORY:
                    found.append(prefix + below)
                else:
                    to_visit.append((join(directory, name), below + (component(name),)))
        return sorted(found)

    def _cloud_directory_lister(self) -> "Callable[[str], list[str]]":
        """Return a function listing the subdirectories of a cloud directory.

        Returns:
            A function from a directory (bucket and key, no scheme) to the names of
            its subdirectories.

        Raises:
            TableListingNotSupportedError: If no ``fsspec`` file system is available
                for this store's scheme.
        """
        import fsspec

        scheme = self._root_uri.split("://", 1)[0].lower()
        protocol = "s3" if scheme == "s3a" else scheme
        options = _s3fs_options(self._storage_options) if protocol == "s3" else {}
        endpoint = str(options.get("client_kwargs", {}).get("endpoint_url", ""))
        if protocol == "s3" and not endpoint:
            # Without an endpoint in the storage options the file system takes it
            # from the environment: fsspec's setting first, then the AWS one.
            endpoint = os.environ.get("FSSPEC_S3_ENDPOINT_URL") or os.environ.get(
                "AWS_ENDPOINT_URL", ""
            )
        if endpoint.lower().startswith("http://") and not _allows_http(
            self._storage_options
        ):
            # deltalake refuses a plain-HTTP endpoint unless AWS_ALLOW_HTTP is set;
            # the listing follows the same rule and sends no credentials over it.
            raise TableListingNotSupportedError(
                f"The Delta store at {self._root_uri!r} cannot list its tables: its "
                "endpoint uses plain HTTP and AWS_ALLOW_HTTP is not set."
            )
        try:
            # No listing cache: tables written since the last call must be seen.
            file_system = fsspec.filesystem(
                protocol, use_listings_cache=False, skip_instance_cache=True, **options
            )
        except Exception as exc:
            raise TableListingNotSupportedError(
                f"The Delta store at {self._root_uri!r} cannot list its tables: no "
                f"usable fsspec file system for {scheme!r} "
                f"({type(exc).__name__}: {exc})."
            ) from exc

        def list_directory(directory: str) -> list[str]:
            """Return the names of the subdirectories of a cloud directory."""
            entries = file_system.ls(directory, detail=True)
            return [
                str(entry["name"]).rstrip("/").rsplit("/", 1)[-1]
                for entry in entries
                if entry["type"] == "directory"
            ]

        return list_directory

    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``True`` if a Delta table exists at ``record_path``.

        This is a cheap existence check that does NOT load any records.

        Args:
            record_path: Path components identifying the table, relative to
                ``self.base_path``.

        Returns:
            ``True`` if a table exists at the given path, ``False`` otherwise.
        """
        return self._get_delta_table(record_path) is not None

    def _ensure_record_id_column(
        self, arrow_data: pa.Table, record_id: str | bytes
    ) -> pa.Table:
        """Ensure the table has an record id column."""
        record_id = coerce_record_id(record_id)
        if self.RECORD_ID_COLUMN not in arrow_data.column_names:
            # Add record_id column at the beginning
            key_array = pa.array([record_id] * len(arrow_data), type=pa.large_binary())
            arrow_data = arrow_data.add_column(0, self.RECORD_ID_COLUMN, key_array)
        return arrow_data

    def _remove_record_id_column(self, arrow_data: pa.Table) -> pa.Table:
        """Remove the record id column if it exists."""
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            arrow_data = arrow_data.drop([self.RECORD_ID_COLUMN])
        return arrow_data

    def _handle_record_id_column(
        self, arrow_data: pa.Table, record_id_column: str | None = None
    ) -> pa.Table:
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

    def _snapshot(
        self, record_key: str, delta_table: deltalake.DeltaTable
    ) -> _CommittedSnapshot:
        """Return the snapshot of ``delta_table``'s version, starting an empty one if needed.

        Args:
            record_key: The table's absolute record key.
            delta_table: The loaded table the snapshot must describe.

        Returns:
            The snapshot for the table's version; its parts are read on first use.
        """
        version = delta_table.version()
        snapshot = self._snapshots.get(record_key)
        if snapshot is None or snapshot.version != version:
            snapshot = _CommittedSnapshot(version)
            self._snapshots[record_key] = snapshot
        return snapshot

    @classmethod
    def _scan_ids(cls, delta_table: deltalake.DeltaTable) -> set[bytes]:
        """Read the record ids stored in ``delta_table`` (nulls left out).

        Args:
            delta_table: The loaded table.

        Returns:
            The stored record ids.

        Raises:
            Exception: The backend's error when the table's data cannot be read.
        """
        stored = _stored_arrow_schema(delta_table)
        if cls.RECORD_ID_COLUMN not in stored.names:
            logger.warning(f"Delta table missing {cls.RECORD_ID_COLUMN} column")
            return set()
        # Read through a one-column schema: the other columns' types are never
        # converted, so a logical type stored in a list cannot break the id read.
        id_field = stored.field(cls.RECORD_ID_COLUMN)
        id_schema = pa.schema(
            [id_field.with_type(arrow_utils.normalize_to_large_types(id_field.type))]
        )
        dataset = delta_to_pyarrow_dataset(delta_table, schema=id_schema)
        ids = dataset.to_table()[cls.RECORD_ID_COLUMN]
        return {record_id for record_id in ids.to_pylist() if record_id is not None}

    @staticmethod
    def _scan_rows(
        delta_table: deltalake.DeltaTable,
    ) -> tuple[pa.Table, frozenset[str], pa.Schema]:
        """Read every row of ``delta_table`` as ``get_all_records`` reads it.

        Args:
            delta_table: The loaded table.

        Returns:
            The rows (view types normalized: PyArrow has no comparison kernels for
            ``string_view``, ENG-601), the data
            files they were read from and the dataset's schema.

        Raises:
            Exception: The backend's error when the table's data cannot be read.
        """
        # deltalake builds a FileSystemDataset; its data files identify a version.
        dataset = cast(
            "pa_ds.FileSystemDataset",
            delta_to_pyarrow_dataset(delta_table, as_large_types=True),
        )
        table = arrow_utils.normalize_table_view_types(dataset.to_table())
        return table.combine_chunks(), frozenset(dataset.files), dataset.schema

    def _stored_ids(self, record_path: tuple[str, ...]) -> set[bytes]:
        """Return the record ids stored at ``record_path`` (in the loaded version).

        Raises:
            StoreReadError: If the table exists but its ids could not be read.
        """
        delta_table = self._get_delta_table(record_path)
        if delta_table is None:
            return set()
        snapshot = self._snapshot(self._get_record_key(record_path), delta_table)
        if snapshot.ids is None:
            try:
                snapshot.ids = self._scan_ids(delta_table)
            except Exception as e:
                raise self._read_failed(record_path) from e
        return snapshot.ids

    def _stored_rows(self, record_path: tuple[str, ...]) -> KeyIndex | None:
        """Return the indexed rows stored at ``record_path``, or ``None`` if no table.

        Raises:
            StoreReadError: If the table exists but could not be read.
        """
        delta_table = self._get_delta_table(record_path)
        if delta_table is None:
            return None
        snapshot = self._snapshot(self._get_record_key(record_path), delta_table)
        if snapshot.rows is None:
            try:
                table, files, schema = self._scan_rows(delta_table)
            except Exception as e:
                raise self._read_failed(record_path) from e
            snapshot.rows = KeyIndex(table)
            snapshot.files = files
            snapshot.dataset_schema = schema
        return snapshot.rows

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str | bytes,
        record: pa.Table,
        skip_duplicates: bool = False,
        flush: bool = False,
        schema_handling: Literal["merge", "error", "coerce"] = "error",
    ) -> None:
        data_with_record_id = self._ensure_record_id_column(record, record_id)
        self.add_records(
            record_path=record_path,
            records=data_with_record_id,
            record_id_column=self.RECORD_ID_COLUMN,
            schema_handling=schema_handling,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
        schema_handling: Literal["merge", "error", "coerce"] = "error",
    ) -> None:
        """
        Insert new records. By default, never overwrites existing records.

        Args:
            schema_handling: How to handle schema differences
            skip_duplicates: If True, skip records with IDs that already exist.
                           If False, raise error on duplicates.
            flush: Whether to flush immediately after the insert

        Raises:
            ValueError: If any record IDs already exist and skip_duplicates=False
        """
        self._validate_record_path(record_path)

        if records.num_rows == 0:
            return

        # Convert view types up front so the whole write path (group_by dedup,
        # filters, storage) avoids string_view's missing kernels (ENG-601).
        records = arrow_utils.normalize_table_view_types(records)

        if record_id_column is None:
            record_id_column = records.column_names[0]

        # Step 1: Validate that record ID column exist
        if record_id_column not in records.column_names:
            raise ValueError(
                f"Specified record ID column {record_id_column} not found in input table {records.column_names}"
            )

        # rename record ID column to a standard name
        if record_id_column != self.RECORD_ID_COLUMN:
            rename_map = {record_id_column: self.RECORD_ID_COLUMN}
            total_name_map = {k: rename_map.get(k, k) for k in records.column_names}
            records = records.rename_columns(total_name_map)

        # Step 2: Deduplicate within input table (keep last occurrence)
        deduplicated_table = self._deduplicate_within_table(records)

        # Step 3: Handle conflicts based on skip_duplicates setting
        if skip_duplicates:
            filtered_table = self._filter_existing_records(
                record_path, deduplicated_table
            )
            if filtered_table.num_rows == 0:
                logger.debug("All records were duplicates, nothing to insert")
                return None
        else:
            # Check for conflicts - insert never allows duplicates when skip_duplicates=False
            # self._check_all_conflicts(record_path, deduplicated_table)
            filtered_table = deduplicated_table

        # Step 4: Handle schema compatibility
        schema_compatible_table = self._handle_schema_compatibility(
            record_path, filtered_table, schema_handling
        )

        # Step 5: Add to pending batch (no overwrite logic needed)
        self._add_to_pending_batch(record_path, schema_compatible_table)

        # Step 6: Flush if asked to; otherwise commit this table alone once its
        # pending batch reaches batch_size. Only the full table: committing every
        # pending table here would commit another table's rows, such as pipeline
        # records whose results are still pending, out of their writer's order
        # (ITL-919).
        if flush:
            self.flush()
        elif self._should_auto_flush(record_path):
            self.flush_batch(record_path)

    def _deduplicate_within_table(self, table: pa.Table) -> pa.Table:
        # TODO: consider erroring out if duplicates are found
        """Remove duplicates within the input table, keeping the last occurrence."""
        if table.num_rows <= 1:
            return table

        # Create row indices
        indices = pa.array(range(table.num_rows))

        # Add row index column temporarily
        table_with_indices = table.add_column(0, self.ROW_INDEX_COLUMN, indices)

        # Group by RECORD_ID_COLUMN and get the maximum row index for each group
        # This gives us the last occurrence of each ID
        grouped = table_with_indices.group_by([self.RECORD_ID_COLUMN]).aggregate(
            [(self.ROW_INDEX_COLUMN, "max")]
        )

        # Get the row indices to keep - the aggregated column name has "_max" suffix
        max_indices_column = f"{self.ROW_INDEX_COLUMN}_max"
        indices_to_keep = grouped[max_indices_column].to_pylist()

        # Filter original table to keep only these rows
        mask = pc.is_in(indices, pa.array(indices_to_keep))
        return table.filter(mask)

    def _filter_existing_records(
        self, record_path: tuple[str, ...], table: pa.Table
    ) -> pa.Table:
        """Filter out records that already exist (for skip_duplicates=True)."""
        input_ids = set(table[self.RECORD_ID_COLUMN].to_pylist())
        record_key = self._get_record_key(record_path)

        # Get IDs that already exist in pending batch or Delta table
        existing_in_pending = input_ids.intersection(
            self._pending_record_ids[record_key]
        )
        existing_in_delta = input_ids.intersection(self._stored_ids(record_path))
        all_existing = existing_in_pending.union(existing_in_delta)

        if not all_existing:
            return table  # No duplicates found

        # Filter out existing records
        mask = pc.invert(
            pc.is_in(table[self.RECORD_ID_COLUMN], pa.array(list(all_existing)))
        )
        filtered = table.filter(mask)

        logger.debug(f"Skipped {len(all_existing)} duplicate records")
        return filtered

    def _check_all_conflicts(
        self, record_path: tuple[str, ...], table: pa.Table
    ) -> None:
        """Check for conflicts with both pending batch and Delta table."""
        input_ids = set(table[self.RECORD_ID_COLUMN].to_pylist())
        record_key = self._get_record_key(record_path)
        # Check conflicts with pending batch
        pending_conflicts = input_ids.intersection(self._pending_record_ids[record_key])
        if pending_conflicts:
            raise ValueError(
                f"Cannot insert records with IDs that already exist in pending batch: {pending_conflicts}. "
                f"Use skip_duplicates=True to skip existing records or update() method to overwrite."
            )

        # Check conflicts with Delta table
        existing_ids = self._stored_ids(record_path)
        delta_conflicts = input_ids.intersection(existing_ids)
        if delta_conflicts:
            raise ValueError(
                f"Cannot insert records with IDs that already exist in Delta table: {delta_conflicts}. "
                f"Use skip_duplicates=True to skip existing records or update() method to overwrite."
            )

    def _handle_schema_compatibility(
        self, record_path: tuple[str, ...], table: pa.Table, schema_handling: str
    ) -> pa.Table:
        """Handle schema differences between input and pending batch."""
        record_key = self._get_record_key(record_path)
        pending_batch = self._pending_batches.get(record_key)
        if pending_batch is None:
            return table

        if pending_batch.schema.equals(table.schema):
            # TODO: perform more careful check
            return table

        if schema_handling == "error":
            raise ValueError(
                f"Schema mismatch between input {table.schema} and pending batch {pending_batch.schema}"
            )
        elif schema_handling == "merge":
            try:
                # Unify schemas and cast both input table and pending batch
                unified_schema = pa.unify_schemas([pending_batch.schema, table.schema])

                # Cast the pending batch to unified schema (excluding tracking columns)
                self._pending_batches[record_key] = pending_batch.cast(unified_schema)

                # Cast and return the input table
                return table.cast(unified_schema)
            except Exception as e:
                # TODO: perform more careful error check
                raise ValueError(f"Cannot merge schemas: {e}")
        elif schema_handling == "coerce":
            try:
                # Coerce input table to match existing pending batch schema
                return table.cast(pending_batch.schema)
            except Exception as e:
                raise ValueError(f"Cannot coerce schema: {e}")
        else:
            raise ValueError(f"Unknown schema handling: {schema_handling}")

    def _handle_delta_schema_compatibility(
        self, record_path: tuple[str, ...], table: pa.Table, schema_handling: str
    ) -> pa.Table:
        """Handle schema differences between input and Delta table for updates."""
        record_key = self._get_record_key(record_path)
        if self._delta_table_cache.get(record_key) is None:
            return table

        delta_table = self._delta_table_cache[record_key]

        try:
            # Get Delta table schema and convert from arro3 to pyarrow
            arro3_schema = delta_table.schema().to_arrow()
            delta_schema = pa.schema(arro3_schema)  # type: ignore
        except Exception as e:
            logger.warning(f"Could not get Delta table schema: {e}")
            return table

        if delta_schema.equals(table.schema):
            return table

        if schema_handling == "error":
            raise ValueError("Schema mismatch between input and Delta table")
        elif schema_handling == "merge":
            try:
                # Unify schemas - this might require adding null columns
                unified_schema = pa.unify_schemas([delta_schema, table.schema])
                return table.cast(unified_schema)
            except Exception as e:
                raise ValueError(f"Cannot merge schemas: {e}")
        elif schema_handling == "coerce":
            try:
                # Coerce input table to match Delta table schema
                return table.cast(delta_schema)
            except Exception as e:
                raise ValueError(f"Cannot coerce schema: {e}")
        else:
            raise ValueError(f"Unknown schema handling: {schema_handling}")

    def _add_to_pending_batch(self, record_path: tuple[str, ...], table: pa.Table):
        """Add table to pending batch."""
        # Add row index column for internal tracking
        record_key = self._get_record_key(record_path)
        pending_batch = self._pending_batches.get(record_key)
        if pending_batch is None:
            self._pending_batches[record_key] = table
        else:
            # Kept at O(log n) record batches, and its indexes extended in place, so a
            # lookup's cost does not grow with the rows pending (ITL-919).
            grown = compact_tail(pa.concat_tables([pending_batch, table]))
            self._pending_batches[record_key] = grown
            index = self._pending_indexes.get(record_key)
            if index is not None and index.table is pending_batch:
                index.append(table, grown)

        pending_ids = cast(list[bytes], table[self.RECORD_ID_COLUMN].to_pylist())
        self._pending_record_ids[record_key].update(pending_ids)

    def _should_auto_flush(self, record_path: tuple[str, ...]) -> bool:
        """Check if auto-flush should be triggered."""
        record_key = self._get_record_key(record_path)
        return (
            self._pending_batches.get(record_key) is not None
            and self._pending_batches[record_key].num_rows >= self.batch_size
        )

    def _pending_rows(self, record_path: tuple[str, ...]) -> KeyIndex | None:
        """Return the indexed pending rows at ``record_path``, or ``None`` if none.

        The indexes are built on first use and describe the current pending batch
        only (``_add_to_pending_batch`` extends them; a flush or schema change
        replaces the batch, and with it the indexes).
        """
        record_key = self._get_record_key(record_path)
        pending = self._pending_batches.get(record_key)
        if pending is None or pending.num_rows == 0:
            return None
        index = self._pending_indexes.get(record_key)
        if index is None or index.table is not pending:
            index = KeyIndex(pending)
            self._pending_indexes[record_key] = index
        return index

    def _without_stored_ids(
        self, record_path: tuple[str, ...], pending: pa.Table
    ) -> pa.Table | None:
        """Return the pending rows a flush would insert: those whose id is not stored.

        A flush inserts only rows whose record id is not stored yet (the merge is
        ``when_not_matched_insert_all``), so a pending row with a stored id is
        never returned by a read: the stored row is. The stored ids are read even
        when ``pending`` is empty, so a read with rows pending raises
        ``StoreReadError`` whenever the stored ids cannot be read.

        Args:
            record_path: Path components relative to this database view.
            pending: Pending rows of the table at ``record_path``.

        Returns:
            The rows whose record id is not stored, or ``None`` if there are none.
        """
        stored_ids = self._stored_ids(record_path)
        if stored_ids and pending.num_rows > 0:
            # The mask is built from the pending ids against the cached set, so its
            # cost follows the pending rows, not the stored table. A null id is
            # never matched by the merge (``null = null`` is not true), so its row
            # is kept.
            keep = [
                record_id is None or record_id not in stored_ids
                for record_id in pending[self.RECORD_ID_COLUMN].to_pylist()
            ]
            if not all(keep):
                pending = pending.filter(pa.array(keep, type=pa.bool_()))
        return pending if pending.num_rows > 0 else None

    def _pending_not_stored(self, record_path: tuple[str, ...]) -> pa.Table | None:
        """Return every pending row a flush would insert at ``record_path``, or ``None``.

        Args:
            record_path: Path components relative to this database view.

        Returns:
            The pending rows whose record id is not stored, or ``None``.
        """
        pending = self._pending_rows(record_path)
        if pending is None:
            return None
        return self._without_stored_ids(record_path, pending.table)

    def _combine_stored_and_pending(
        self,
        stored: pa.Table | None,
        pending: pa.Table | None,
        record_id_column: str | None,
    ) -> pa.Table | None:
        """Combine stored rows with pending rows into one read result.

        Combined rows have the schema a flush would give them: both parts take
        their large layouts (a stored table reads back large), a column missing
        on one side is null-filled, a ``null``-typed column takes the other
        side's type, nullability is merged (nullable if either side is), and
        struct children are merged, as Delta's merge evolves the stored schema.
        A column whose types still differ raises ``pyarrow.ArrowTypeError``;
        nothing is widened. A single part is returned as it is.

        Args:
            stored: Rows read from the Delta table, or ``None``.
            pending: Pending rows that are not stored (see ``_pending_not_stored``),
                already filtered by the read's condition, or ``None``.
            record_id_column: Name to give the record id column, or ``None`` to
                drop it.

        Returns:
            The combined rows, or ``None`` if there are none.
        """
        parts = [t for t in (stored, pending) if t is not None and t.num_rows > 0]
        if not parts:
            return None
        if len(parts) == 1:
            table = parts[0]
        else:
            table = pa.concat_tables(
                [self._large_layout(t) for t in parts], promote_options="default"
            )
        return self._handle_record_id_column(table, record_id_column)

    @staticmethod
    def _large_layout(table: pa.Table) -> pa.Table:
        """Return ``table`` with every column in its large layout.

        Only layouts change (``string`` to ``large_string``, ``list`` to
        ``large_list``, recursively); ``null`` stays ``null``, and names,
        nullability and metadata are kept.

        Args:
            table: The table to normalize.

        Returns:
            The table with large-normalized column types.
        """
        schema = table.schema
        for index, field in enumerate(table.schema):
            schema = schema.set(
                index,
                field.with_type(
                    arrow_utils.normalize_to_large_types(field.type, keep_null=True)
                ),
            )
        return table if schema.equals(table.schema) else table.cast(schema)

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
        retrieve_pending: bool = True,
    ) -> pa.Table | None:
        """
        Get all records from both pending batch and Delta table.

        Every read method returns the rows a flush would leave: the stored rows
        plus the pending rows whose record id is not stored yet, as seen through
        any handle of this ``at()`` family.

        Returns:
            Combined Arrow table with all records, or None if no records exist

        Raises:
            StoreReadError: If the Delta table exists but could not be read.
        """
        tables_to_combine = []

        # Add Delta table data
        if (delta_table := self._get_delta_table(record_path)) is not None:
            try:
                delta_table_data = delta_to_pyarrow_dataset(
                    delta_table, as_large_types=True
                ).to_table()
            except Exception as e:
                raise self._read_failed(record_path) from e
            if delta_table_data.num_rows > 0:
                tables_to_combine.append(delta_table_data)

        # Add the pending rows a flush would insert
        pending = self._pending_not_stored(record_path) if retrieve_pending else None
        stored = pa.concat_tables(tables_to_combine) if tables_to_combine else None
        return self._combine_stored_and_pending(stored, pending, record_id_column)

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        if flush:
            self.flush_batch(record_path)

        pair_list: list[tuple[str, Any]]
        if isinstance(column_values, Mapping):
            # Convert Mapping to list of tuples
            pair_list = list(cast(Mapping[str, Any], column_values).items())
        else:
            # Ensure it's a list of tuples
            pair_list = cast(list[tuple[str, Any]], list(column_values))

        expressions = [pc.field(c) == v for c, v in pair_list]
        combined_expression = expressions[0]
        for next_expression in expressions[1:]:
            combined_expression = combined_expression & next_expression

        pending = self._pending_rows(record_path)
        if pending is not None:
            # Rows are pending: the stored ids decide which of them a read returns.
            self._stored_ids(record_path)
        stored = self._stored_rows(record_path)

        # A side without a queried column holds null there once flushed, and
        # ``column == value`` never matches null, so it contributes no rows. A
        # column neither side has is unknown: the filter raises as before.
        queried = {c for c, _ in pair_list}
        stored_columns = set() if stored is None else set(stored.table.column_names)
        pending_columns = set() if pending is None else set(pending.table.column_names)
        if queried <= stored_columns | pending_columns:
            if not queried <= pending_columns:
                pending = None
            if not queried <= stored_columns:
                stored = None

        # Both sides are answered from indexes (ITL-919): no re-read, no scan.
        pending_matches = (
            None
            if pending is None
            else self._without_stored_ids(
                record_path, matching_rows(pending, pair_list, combined_expression)
            )
        )
        result = (
            None
            if stored is None
            else matching_rows(stored, pair_list, combined_expression)
        )
        return self._combine_stored_and_pending(
            result, pending_matches, record_id_column
        )

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str | bytes,
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

        record_id = coerce_record_id(record_id)
        # Serve a pending record only if its id is not stored: a flush keeps the
        # stored row and drops the pending one.
        record_key = self._get_record_key(record_path)
        if record_id in self._pending_record_ids[
            record_key
        ] and record_id not in self._stored_ids(record_path):
            # Return the pending record after removing the entry id column
            pending = self._pending_rows(record_path)
            assert pending is not None, "Pending batch should not be None"
            filtered_table = self._rows_with_ids(
                pending,
                [record_id],
                pending.table.schema.field(self.RECORD_ID_COLUMN).type,
            )
            if filtered_table.num_rows != 1:
                raise ValueError(
                    f"Expected exactly one record in pending batch with record ID {record_id}, but found {filtered_table.num_rows}"
                )
            return self._handle_record_id_column(filtered_table, record_id_column)

        # Now check the stored rows (the indexed snapshot, ITL-919)
        stored = self._stored_rows(record_path)
        if stored is None:
            return None
        result = self._rows_with_ids(stored, [record_id])
        if result.num_rows == 0:
            return None

        # Handle (remove/rename) the record id column before returning
        return self._handle_record_id_column(result, record_id_column)

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str | bytes] | pl.Series | pa.Array,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        """
        Retrieve records by entry IDs as a single table with schema preservation.

        Args:
            record_path: Tuple of path components
            record_ids: Entry IDs to retrieve

        Returns:
            Arrow table containing all found records with original schema, or None if no records found
        """
        _ = self._get_record_key(record_path)
        if flush:
            self.flush_batch(record_path)

        # Convert input to a list of bytes for consistency

        if isinstance(record_ids, pl.Series):
            record_ids_list = [coerce_record_id(r) for r in record_ids.to_list()]
        elif isinstance(record_ids, (pa.Array, pa.ChunkedArray)):
            record_ids_list = [coerce_record_id(r) for r in record_ids.to_pylist()]
        elif isinstance(record_ids, Collection):
            record_ids_list = [coerce_record_id(r) for r in record_ids]
        else:
            raise TypeError(
                f"record_ids must be Collection[str | bytes], pl.Series, or pa.Array, got {type(record_ids)}"
            )
        if len(record_ids) == 0:
            return None

        stored = self._stored_rows(record_path)
        pending = self._pending_rows(record_path)
        result = None
        if stored is not None:
            result = self._rows_with_ids(stored, record_ids_list)

        pending_matches = None
        if pending is not None:
            id_type = pending.table.schema.field(self.RECORD_ID_COLUMN).type
            # Reading the stored ids drops the pending rows a flush would not
            # insert; it raises StoreReadError when the stored table cannot be read.
            pending_matches = self._without_stored_ids(
                record_path, self._rows_with_ids(pending, record_ids_list, id_type)
            )
        return self._combine_stored_and_pending(
            result, pending_matches, record_id_column
        )

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Serialize database configuration to a JSON-compatible dict."""
        config: dict[str, Any] = {
            "type": "delta_table",
            "root_uri": self._root_uri,  # renamed from "base_path"
            "base_path": list(self._path_prefix),  # new: relative prefix tuple
            "batch_size": self.batch_size,
            "max_hierarchy_depth": self.max_hierarchy_depth,
            "allow_schema_evolution": self.allow_schema_evolution,
        }
        if self._storage_options:
            config["storage_options"] = self._storage_options
        return config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DeltaTableDatabase":
        """Reconstruct a DeltaTableDatabase from a config dict.

        Supports both the current format (``"root_uri"`` for the storage root,
        ``"base_path"`` as a list for the scoping prefix) and the legacy format
        produced before ENG-341 (``"base_path"`` as a URI string, no prefix).
        """
        if "root_uri" in config:
            # Current format (post-ENG-341)
            root_uri = config["root_uri"]
            base_path_value = config.get("base_path", [])
            _path_prefix = (
                tuple(base_path_value) if isinstance(base_path_value, list) else ()
            )
        else:
            # Legacy format (pre-ENG-341): "base_path" was the root URI string
            root_uri = config["base_path"]
            _path_prefix = ()
        return cls(
            base_path=root_uri,
            storage_options=config.get("storage_options"),
            create_base_path=True,
            batch_size=config.get("batch_size", 1000),
            max_hierarchy_depth=config.get("max_hierarchy_depth", 10),
            allow_schema_evolution=config.get("allow_schema_evolution", True),
            _path_prefix=_path_prefix,
        )

    def flush(self) -> None:
        """Flush all pending batches.

        Every pending batch is attempted, even after one fails. A batch that fails
        stays queued, so a later flush tries it again, and each failure is logged.

        Raises:
            StoreReadError: If a batch's existing table could not be loaded. It is
                raised as itself; every other batch that failed in this flush is
                added to it as a note.
            InputValidationError: If no batch failed with ``StoreReadError`` but a
                batch's column types differ from its stored table's. Other failed
                batches are added to it as notes.
            RuntimeError: If batches failed with other errors only, naming the
                failed batches, with the first failure as its ``__cause__``.
        """
        errors: list[tuple[str, Exception]] = []
        for record_key in list(self._pending_batches.keys()):
            try:
                self._flush_by_key(record_key)
            except Exception as e:
                logger.error(f"Error flushing batch for {record_key}: {e}")
                errors.append((record_key, e))
        if errors:
            store_error = _store_error_to_raise([e for _, e in errors])
            if store_error is not None:
                # Keep every other failure visible on the error that is raised.
                for record_key, other in errors:
                    if other is not store_error:
                        store_error.add_note(
                            f"Another batch also failed to flush ({record_key}): "
                            f"{type(other).__name__}: {other}"
                        )
                raise store_error
            failed_keys = [k for k, _ in errors]
            raise RuntimeError(
                f"Failed to flush {len(errors)} batch(es): {failed_keys}"
            ) from errors[0][1]

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        """
        Flush pending batch for a specific source path.

        Args:
            record_path: Tuple of path components (relative to this instance's prefix)
        """
        record_key = self._get_record_key(record_path)
        self._flush_by_key(record_key)

    def _flush_by_key(self, record_key: str) -> None:
        """Flush a pending batch identified by its absolute record_key (prefix + path).

        This method is safe to call from any instance (root or scoped) since the
        key already encodes the full path. The URI is derived directly from the key
        without applying any instance-level prefix.
        """
        logger.debug("Flushing triggered!!")

        if (
            record_key not in self._pending_batches
            or not self._pending_batches[record_key]
        ):
            return

        # Get all pending records
        pending_batch = self._pending_batches.pop(record_key)
        pending_ids = self._pending_record_ids.pop(record_key, set())

        # Derive the absolute table URI directly from the key (already includes prefix).
        abs_path_components = tuple(record_key.split("/"))
        if not self._is_cloud:
            path = self._local_root
            for component in abs_path_components:
                path = path / self._sanitize_path_component(component)
            table_uri = str(path)
        else:
            table_uri = self._root_uri.rstrip("/") + "/" + record_key

        try:
            # Combine all tables in the batch. Convert view types so they never
            # reach storage (ENG-601).
            combined_table = arrow_utils.normalize_table_view_types(
                pending_batch.combine_chunks()
            )

            # Ensure parent directory exists (local only)
            if not self._is_cloud:
                import os

                os.makedirs(table_uri, exist_ok=True)

            # Check if Delta table exists at this URI. Only TableNotFoundError
            # means "absent": a table that exists but cannot be loaded must never
            # be replaced by the mode="overwrite" write below (ITL-884).
            from deltalake.exceptions import TableNotFoundError

            try:
                delta_table = deltalake.DeltaTable(
                    table_uri, storage_options=self._storage_options or None
                )
            except TableNotFoundError:
                delta_table = None
            except Exception as load_error:
                raise StoreReadError(table_uri, abs_path_components) from load_error

            if delta_table is None:
                # TODO: reconsider mode="overwrite" here
                deltalake.write_deltalake(
                    table_uri,
                    combined_table,
                    mode="overwrite",
                    schema_mode="merge" if self.allow_schema_evolution else "overwrite",
                    storage_options=self._storage_options or None,
                )
                base = _CommittedSnapshot(version=-1, ids=set())
                inserted_ids = self._non_null_ids(combined_table)
                logger.debug(
                    f"Created new Delta table for {record_key} with {len(combined_table)} records"
                )
            else:
                mismatch = self._incompatible_append_column(delta_table, combined_table)
                if mismatch is not None:
                    column, stored_type, incoming_type = mismatch
                    raise InputValidationError(
                        f"Cannot append to the stored table {table_uri!r}: column "
                        f"{column!r} is stored as {stored_type} but the new rows have "
                        f"{incoming_type} (input type "
                        f"{combined_table.schema.field(column).type}). "
                        "The stored rows are unchanged and nothing was written."
                    )
                base = self._snapshot_for_commit(
                    record_key, delta_table, table_uri, abs_path_components
                )
                assert base.ids is not None
                incoming_ids = self._non_null_ids(combined_table)
                inserted_ids = incoming_ids - base.ids
                if len(inserted_ids) == len(incoming_ids) and self._stores_as_is(
                    delta_table, combined_table
                ):
                    # Every pending id is new in the version just loaded and the rows
                    # have the stored schema: append (ITL-919). A MERGE reads the whole
                    # stored table on each commit; it remains for every other batch.
                    deltalake.write_deltalake(
                        table_uri,
                        combined_table,
                        mode="append",
                        storage_options=self._storage_options or None,
                    )
                else:
                    # Some pending ids are stored already: insert only the others.
                    delta_table.merge(
                        source=combined_table,
                        predicate=f"target.{self.RECORD_ID_COLUMN} = source.{self.RECORD_ID_COLUMN}",
                        source_alias="source",
                        target_alias="target",
                        merge_schema=self.allow_schema_evolution,
                    ).when_not_matched_insert_all().execute()

                logger.debug(
                    f"Appended batch of {len(combined_table)} records to {record_key}"
                )

        except Exception as e:
            logger.error(f"Error flushing batch for {record_key}: {e}")
            # Put the tables back in the pending queue
            self._pending_batches[record_key] = pending_batch
            self._pending_record_ids[record_key] = pending_ids
            raise

        # The rows are committed: nothing below may put them back in the queue.
        try:
            committed = deltalake.DeltaTable(
                table_uri, storage_options=self._storage_options or None
            )
            self._delta_table_cache[record_key] = committed
            self._snapshots[record_key] = self._extend_snapshot(
                base, committed, inserted_ids
            )
        except Exception as e:
            # The next read loads the table again and reports a failure itself.
            logger.warning(f"Could not reload {record_key} after committing: {e}")
            self._delta_table_cache.pop(record_key, None)
            self._snapshots.pop(record_key, None)

    def _rows_with_ids(
        self,
        rows: KeyIndex,
        record_ids: list[bytes],
        value_type: pa.DataType | None = None,
    ) -> pa.Table:
        """Return the rows whose record id is one of ``record_ids``, in table order.

        Served from the record-id index (ITL-919); a table whose id column is not
        binary is filtered instead, with Arrow's comparison.

        Args:
            rows: The indexed rows (stored or pending).
            record_ids: The ids looked up.
            value_type: The Arrow type to build the looked-up ids with for the
                filter, or ``None`` to infer it from the ids.

        Returns:
            The matching rows (possibly none).
        """
        id_type = rows.table.schema.field(self.RECORD_ID_COLUMN).type
        if not all(is_indexed_lookup(id_type, r) for r in record_ids):
            return rows.table.filter(
                pc.is_in(
                    pc.field(self.RECORD_ID_COLUMN),
                    pa.array(record_ids, type=value_type),
                )
            )
        id_index = rows.column(self.RECORD_ID_COLUMN)
        positions = {p for r in set(record_ids) for p in id_index.get(r, ())}
        return rows_at(rows.table, sorted(positions))

    @staticmethod
    def _stores_as_is(delta_table: deltalake.DeltaTable, batch: pa.Table) -> bool:
        """Whether ``batch`` has exactly the stored schema once the Delta writer converts it.

        Only such a batch is appended: an append stores it unchanged, as the MERGE
        would. A batch that needs a cast (a ``null``-typed column, a nullability
        difference) or evolves the schema goes through the MERGE, which handles it.

        Args:
            delta_table: The loaded stored table.
            batch: The rows about to be committed.

        Returns:
            Whether an append stores ``batch`` exactly as the MERGE would.
        """
        try:
            incoming = _as_delta_writer_stores(batch.schema)
        except Exception:
            # A type Delta cannot store directly (null): leave it to the MERGE.
            return False
        return incoming.equals(_stored_arrow_schema(delta_table), check_metadata=False)

    @classmethod
    def _non_null_ids(cls, table: pa.Table) -> set[bytes]:
        """Return the record ids of ``table``'s rows, nulls left out."""
        return {
            record_id
            for record_id in table[cls.RECORD_ID_COLUMN].to_pylist()
            if record_id is not None
        }

    def _snapshot_for_commit(
        self,
        record_key: str,
        delta_table: deltalake.DeltaTable,
        table_uri: str,
        abs_path_components: tuple[str, ...],
    ) -> _CommittedSnapshot:
        """Return the stored ids of the version a commit is about to build on.

        ``delta_table`` was loaded for this commit, so whether the pending ids are
        new is decided against the table as it is now, never against an older
        cached version. The cached snapshot is reused when it describes this
        version; otherwise the ids are read from it.

        Args:
            record_key: The table's absolute record key.
            delta_table: The table freshly loaded for the commit.
            table_uri: The table's location, for the error.
            abs_path_components: The table's absolute path, for the error.

        Returns:
            A snapshot of the loaded version with its ``ids`` read.

        Raises:
            StoreReadError: If the stored ids could not be read.
        """
        cached = self._snapshots.get(record_key)
        if (
            cached is not None
            and cached.ids is not None
            and cached.version == delta_table.version()
        ):
            return cached
        try:
            ids = self._scan_ids(delta_table)
        except Exception as e:
            raise StoreReadError(table_uri, abs_path_components) from e
        return _CommittedSnapshot(delta_table.version(), ids=ids)

    def _extend_snapshot(
        self,
        base: _CommittedSnapshot,
        committed: deltalake.DeltaTable,
        inserted_ids: set[bytes],
    ) -> _CommittedSnapshot:
        """Return the snapshot of ``committed``, built from ``base`` plus this commit.

        When ``committed`` is the version right after ``base`` (no other writer
        committed in between), its ids are ``base``'s plus the inserted ones, and
        its rows, if ``base`` has them, are ``base``'s plus the rows of the data
        files the commit added, read as ``get_all_records`` reads them. Anything
        else (a skipped version, a removed file, a changed schema) leaves that
        part to be read again on first use.

        Args:
            base: The snapshot of the version the commit built on.
            committed: The table reloaded after the commit.
            inserted_ids: The non-null record ids the commit inserted.

        Returns:
            The new snapshot.
        """
        version = committed.version()
        if version != base.version + 1:
            return _CommittedSnapshot(version)
        assert base.ids is not None
        snapshot = _CommittedSnapshot(version, ids=base.ids | inserted_ids)
        if base.rows is None:
            return snapshot
        dataset = cast(
            "pa_ds.FileSystemDataset",
            delta_to_pyarrow_dataset(committed, as_large_types=True),
        )
        files = frozenset(dataset.files)
        if (
            base.dataset_schema is None
            or not base.files <= files
            or not dataset.schema.equals(base.dataset_schema)
        ):
            return snapshot
        added = files - base.files
        new_rows = arrow_utils.normalize_table_view_types(
            pa.concat_tables(
                [
                    fragment.to_table(schema=dataset.schema)
                    for fragment in dataset.get_fragments()
                    if fragment.path in added
                ]
            )
            if added
            else dataset.schema.empty_table()
        )
        if not new_rows.schema.equals(base.rows.table.schema):
            return snapshot
        rows = base.rows
        rows.append(new_rows, compact_tail(pa.concat_tables([rows.table, new_rows])))
        snapshot.rows = rows
        snapshot.files = files
        snapshot.dataset_schema = dataset.schema
        return snapshot

    @staticmethod
    def _incompatible_append_column(
        delta_table: deltalake.DeltaTable, batch: pa.Table
    ) -> tuple[str, pa.DataType, pa.DataType] | None:
        """Find a column whose type in ``batch`` differs from the stored table's.

        Delta's merge either casts such a batch to the stored type (``int32`` or
        ``float64`` rows into an ``int64`` column) or, when an extension type is
        involved, writes data files the table can no longer read. ``null``-typed
        parts of the batch fit any stored type and are not compared (see
        ``_without_null_types``). The batch's types
        are first converted exactly as the Delta writer converts them (see
        ``_as_delta_writer_stores``), then both sides are compared with extension
        types reduced to storage, large-normalized and nullability ignored, so a
        type Delta stores differently (``uint64`` as ``int64``, ``timestamp[ns]``
        as ``timestamp[us]``) is not reported. Columns and nested
        struct fields present on only one side are allowed (schema evolution);
        see ``_types_conflict``. The stored schema comes from the loaded log; no
        data is read.

        Args:
            delta_table: The loaded stored table.
            batch: The rows about to be merged into it.

        Returns:
            ``(column, stored_type, incoming_type)`` for the first column whose
            compared types differ, or ``None`` when the batch can be appended.
        """
        stored = _stored_arrow_schema(delta_table)
        compared = [
            (f.name, _without_null_types(_comparable_storage_type(f.type)))
            for f in batch.schema
        ]
        incoming = _as_delta_writer_stores(
            pa.schema([pa.field(name, t) for name, t in compared if t is not None])
        )
        for field in incoming:
            index = stored.get_field_index(field.name)
            if index < 0:
                continue
            stored_type = _comparable_storage_type(
                arrow_utils.normalize_to_large_types(
                    stored.field(index).type, keep_null=True
                )
            )
            incoming_type = _comparable_storage_type(
                arrow_utils.normalize_to_large_types(field.type, keep_null=True)
            )
            if _types_conflict(stored_type, incoming_type):
                return field.name, stored_type, incoming_type
        return None

    @property
    def base_path(self) -> tuple[str, ...]:
        """The current relative root of this database view (always () for root instances)."""
        return self._path_prefix

    def at(self, *path_components: str) -> "DeltaTableDatabase":
        """Return a new DeltaTableDatabase scoped to the given sub-path.

        The returned instance uses the same underlying filesystem root but
        all reads and writes are relative to the extended prefix.  The scoped
        instance shares the root's ``_pending_batches`` and
        ``_pending_record_ids`` dicts so that a ``flush()`` on the root is
        visible to — and can flush — all scoped instances.

        Raises:
            TypeError: If any component is not a str.
            ValueError: If any component is empty, is ``'.'`` or ``'..'``, or
                contains filesystem-unsafe characters (``/``, ``\\``, ``*``,
                ``?``, ``"``, ``<``, ``>``, ``|``, ``\\0``).
        """
        _unsafe_chars = ["/", "\\", "*", "?", '"', "<", ">", "|", "\0"]
        for i, component in enumerate(path_components):
            if not isinstance(component, str):
                raise TypeError(
                    f"at() path component {i} must be str, got {type(component)!r}"
                )
            if not component:
                raise ValueError(f"at() path component {i} must not be empty")
            if component in (".", ".."):
                raise ValueError(
                    f"at() path component {repr(component)}: '.' and '..' are not allowed"
                )
            if any(char in component for char in _unsafe_chars):
                raise ValueError(
                    f"at() path component {repr(component)} contains invalid characters"
                )
        new_root = self._root if self._root is not None else self
        new_scoped_path = self._scoped_path + path_components
        # Propagate shared pending state to the scoped instance so that
        # root.flush() is visible to — and can flush — all scoped instances.
        root_instance = new_root
        return DeltaTableDatabase(
            base_path=self._root_uri,
            storage_options=self._storage_options,
            batch_size=self.batch_size,
            max_hierarchy_depth=self.max_hierarchy_depth,
            allow_schema_evolution=self.allow_schema_evolution,
            _path_prefix=self._path_prefix + path_components,
            _root=new_root,
            _scoped_path=new_scoped_path,
            _shared_pending_batches=root_instance._pending_batches,
            _shared_pending_record_ids=root_instance._pending_record_ids,
            _shared_delta_table_cache=root_instance._delta_table_cache,
            _shared_snapshots=root_instance._snapshots,
            _shared_pending_indexes=root_instance._pending_indexes,
        )

    def list_sources(self) -> list[tuple[str, ...]]:
        """
        List all record paths that contain a valid Delta table under base_path.

        Returns:
            List of record_path tuples (e.g. [("alpha",), ("beta", "sub")]).

        Raises:
            NotImplementedError: For cloud base paths (filesystem walk not supported).
        """
        if self._is_cloud:
            raise NotImplementedError(
                "list_sources() is not supported for cloud storage paths"
            )

        sources: list[tuple[str, ...]] = []

        def _scan(current_path: Path, path_components: tuple[str, ...]) -> None:
            if len(path_components) >= self.max_hierarchy_depth:
                return
            try:
                items = list(current_path.iterdir())
            except OSError as e:
                logger.warning(
                    "Could not list directory %s during list_sources scan: %s",
                    current_path,
                    e,
                )
                return
            for item in items:
                if not item.is_dir():
                    continue
                components = path_components + (item.name,)
                try:
                    deltalake.DeltaTable(
                        str(item), storage_options=self._storage_options or None
                    )
                    sources.append(components)
                except deltalake.exceptions.TableNotFoundError:
                    _scan(item, components)

        # Build the effective scoped root directory
        scoped_root = self._local_root
        for component in self._path_prefix:
            scoped_root = scoped_root / self._sanitize_path_component(component)

        _scan(scoped_root, ())
        return sources
