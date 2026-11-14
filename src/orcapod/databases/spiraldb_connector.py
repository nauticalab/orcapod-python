"""SpiralDBConnector — DBConnectorProtocol implementation backed by SpiralDB (pyspiral).

Requires the ``spiraldb`` optional extra: ``pip install orcapod[spiraldb]``.
Authentication is handled externally via the ``spiral login`` CLI command,
which stores credentials in pyspiral's per-user application directory (``~/Library/Application Support/pyspiral/`` on
macOS, ``~/.config/pyspiral/`` on Linux).

The connector is dataset-scoped: all tables are read from and written to
a single ``(project_id, dataset)`` pair.

Example::

    connector = SpiralDBConnector(project_id="my-project-123456", dataset="default")
    db = ConnectorArrowDatabase(connector)
    db.add_record(("results", "my_fn"), record_id="abc", record=table)
    db.flush()
"""

from __future__ import annotations

import base64
import json
import logging
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from orcapod.databases.spiraldb_nullability import (
    apply_nullability_record,
    check_no_nulls,
    enforce_nullability_record,
    read_nullability_record,
    remove_nullability_record,
    render_path,
    schema_paths,
    write_nullability_record,
)
from orcapod.errors import (
    NullabilityRecordError,
    NullabilityRecordMismatchError,
    NullInNonNullableColumnError,
    UnsupportedValueError,
)
from orcapod.types import ColumnInfo
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    import pyarrow as pa
    import spiral as sp
else:
    from orcapod.utils.lazy_module import LazyModule

    pa = LazyModule("pyarrow")
    sp = LazyModule("spiral")

logger = logging.getLogger(__name__)

_ARROW_METADATA_KEY = "__arrow_metadata__"


def _serialize_field_meta_tree(field: pa.Field) -> dict | None:
    """Recursively build the metadata tree for a single ``pa.Field``.

    Walks the field's type tree, collecting ``field.metadata`` at each level.
    Covers struct inner fields, list/large_list/fixed_size_list value fields.
    Map key/item fields are not recursed (``pa.map_`` constructor does not
    accept field-level metadata on key/item).

    Args:
        field: The ``pa.Field`` to serialize.

    Returns:
        A dict with optional ``"meta"`` (base64-encoded k/v pairs) and
        ``"children"`` (recursive trees for composite type children) keys,
        or ``None`` if this field and all its descendants have no metadata.
    """
    import pyarrow as _pa  # noqa: PLC0415

    node: dict = {}

    if field.metadata:
        node["meta"] = {
            base64.b64encode(k).decode(): base64.b64encode(v).decode()
            for k, v in field.metadata.items()
        }

    ftype = field.type
    child_fields: list = []
    if _pa.types.is_struct(ftype):
        child_fields = [ftype.field(i) for i in range(ftype.num_fields)]
    elif (
        _pa.types.is_fixed_size_list(ftype)
        or _pa.types.is_list(ftype)
        or _pa.types.is_large_list(ftype)
    ):
        child_fields = [ftype.value_field]

    if child_fields:
        children: dict = {}
        for child in child_fields:
            child_tree = _serialize_field_meta_tree(child)
            if child_tree is not None:
                children[child.name] = child_tree
        if children:
            node["children"] = children

    return node if node else None


def _serialize_arrow_metadata(table: pa.Table) -> dict[str, bytes] | None:
    """Encode all Arrow metadata from ``table`` into a single SpiralDB KV entry.

    Serializes both schema-level (``table.schema.metadata``) and per-field
    metadata (including nested struct/list field metadata) into a single JSON
    blob stored under ``_ARROW_METADATA_KEY``.

    Args:
        table: The Arrow table whose metadata to encode.

    Returns:
        ``{"__arrow_metadata__": blob_bytes}`` if any metadata exists anywhere
        in the schema, or ``None`` if the schema and all fields (at every depth)
        have no metadata.
    """
    blob: dict = {}

    if table.schema.metadata:
        blob["schema"] = {
            base64.b64encode(k).decode(): base64.b64encode(v).decode()
            for k, v in table.schema.metadata.items()
        }

    field_trees: dict = {}
    for field in table.schema:
        tree = _serialize_field_meta_tree(field)
        if tree is not None:
            field_trees[field.name] = tree
    if field_trees:
        blob["fields"] = field_trees

    if not blob:
        return None

    return {_ARROW_METADATA_KEY: json.dumps(blob).encode("utf-8")}


def _load_arrow_metadata(
    kv: dict[str, bytes],
) -> tuple[dict[bytes, bytes] | None, dict[str, dict]]:
    """Decode Arrow metadata from a SpiralDB table KV store.

    Parses the JSON blob stored under ``_ARROW_METADATA_KEY`` and returns
    the schema-level metadata and per-field metadata trees for the caller
    to apply to a reconstructed Arrow schema.

    Args:
        kv: The dict returned by ``tbl.get_metadata()`` on a SpiralDB table
            handle. May be empty or lack the ``_ARROW_METADATA_KEY`` key.

    Returns:
        A 2-tuple ``(schema_meta, field_trees)`` where:
        - ``schema_meta`` is a ``dict[bytes, bytes]`` of schema-level metadata
          (base64-decoded), or ``None`` if no schema metadata was stored.
        - ``field_trees`` is a ``dict[str, dict]`` mapping top-level column
          name to its raw metadata tree dict (as stored in the blob), or
          ``{}`` if no field metadata was stored.
    """
    if _ARROW_METADATA_KEY not in kv:
        return (None, {})

    try:
        blob = json.loads(kv[_ARROW_METADATA_KEY].decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning(
            "Ignoring malformed Arrow metadata in SpiralDB KV store "
            "(%s: %s); table will be read without metadata restoration.",
            type(exc).__name__,
            exc,
        )
        return (None, {})

    schema_meta: dict[bytes, bytes] | None = None
    if "schema" in blob:
        schema_meta = {
            base64.b64decode(k): base64.b64decode(v) for k, v in blob["schema"].items()
        }

    field_trees: dict[str, dict] = blob.get("fields", {})

    return (schema_meta, field_trees)


def _restore_field(field: pa.Field, stored: dict | None) -> tuple[pa.Field, bool]:
    """Recursively restore a field's metadata and rebuild its nested type.

    Walks the stored metadata tree in parallel with the field's type tree,
    reattaching ``field.metadata`` at every level and reconstructing composite
    types bottom-up when any descendant has metadata to restore.

    Handles: struct inner fields, list/large_list/fixed_size_list value fields.
    Map key/item fields are not recursed (consistent with serialization).

    Args:
        field: The ``pa.Field`` from the Arrow schema returned by SpiralDB.
        stored: The metadata tree dict for this field (as stored in the blob),
            or ``None`` if no metadata was stored for this field.

    Returns:
        A 2-tuple ``(restored_field, changed)`` where:
        - ``restored_field`` is the field with metadata restored (new object
          if any change was made, original object if not).
        - ``changed`` is True if any metadata or nested type was modified.
    """
    import pyarrow as _pa  # noqa: PLC0415

    if stored is None or stored == {}:
        return (field, False)

    # Decode own metadata from stored tree if present
    new_meta: dict[bytes, bytes] | None = None
    has_new_meta = False
    if "meta" in stored:
        new_meta = {
            base64.b64decode(k): base64.b64decode(v) for k, v in stored["meta"].items()
        }
        has_new_meta = True
    else:
        new_meta = field.metadata

    ftype = field.type
    children = stored.get("children", {})
    type_changed = False
    new_type = ftype

    if _pa.types.is_struct(ftype):
        restored_children = []
        any_child_changed = False
        for i in range(ftype.num_fields):
            child = ftype.field(i)
            child_stored = children.get(child.name)
            restored_child, child_changed = _restore_field(child, child_stored)
            restored_children.append(restored_child)
            if child_changed:
                any_child_changed = True
        if any_child_changed:
            new_type = _pa.struct(restored_children)
            type_changed = True
    elif _pa.types.is_fixed_size_list(ftype):
        value_field = ftype.value_field
        child_stored = children.get(value_field.name)
        restored_vf, child_changed = _restore_field(value_field, child_stored)
        if child_changed:
            new_type = _pa.list_(restored_vf, ftype.list_size)
            type_changed = True
    elif _pa.types.is_list(ftype):
        value_field = ftype.value_field
        child_stored = children.get(value_field.name)
        restored_vf, child_changed = _restore_field(value_field, child_stored)
        if child_changed:
            new_type = _pa.list_(restored_vf)
            type_changed = True
    elif _pa.types.is_large_list(ftype):
        value_field = ftype.value_field
        child_stored = children.get(value_field.name)
        restored_vf, child_changed = _restore_field(value_field, child_stored)
        if child_changed:
            new_type = _pa.large_list(restored_vf)
            type_changed = True

    meta_changed = has_new_meta and new_meta != field.metadata
    changed = meta_changed or type_changed

    if not changed:
        return (field, False)

    if type_changed:
        return (_pa.field(field.name, new_type, field.nullable, new_meta), True)
    else:
        return (field.with_metadata(new_meta), True)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _parse_table_name(query: str) -> str:
    """Extract the plain table name from a SELECT * FROM ... query string.

    Supports double-quoted identifiers (``"table_name"``) and unquoted bare
    identifiers (``table_name``). Always returns the plain table name — not
    a ``dataset.table`` qualified form.

    Args:
        query: SQL-style query string, e.g. ``'SELECT * FROM "my_table"'``.

    Returns:
        Plain table name string.

    Raises:
        ValueError: If no table name can be parsed from the query.
    """
    m = re.search(r'FROM\s+"([^"]+)"', query, re.IGNORECASE)
    if not m:
        m = re.search(r"FROM\s+(\w+)", query, re.IGNORECASE)
    if not m:
        raise ValueError(f"Cannot parse table name from query: {query!r}")
    return m.group(1)


def _null_struct_path(data: pa.Table) -> str | None:
    """Return the path of the first struct (at any depth) holding a struct-level null.

    SpiralDB cannot store a null struct: pyspiral refuses one unless written with
    ``push_down_nulls=True``, which turns it into a struct of null fields (live, ITL-936).

    Args:
        data: The rows to be written.

    Returns:
        The path (``g``, ``outer.inner``, ``items[]``), or ``None`` if there is none.
    """
    import pyarrow as _pa  # noqa: PLC0415

    def walk(arrays: list[Any], t: pa.DataType, path: str) -> str | None:
        arrays = [a.storage if isinstance(a, _pa.ExtensionArray) else a for a in arrays]
        if isinstance(t, _pa.ExtensionType):
            t = t.storage_type
        if _pa.types.is_struct(t):
            if any(a.null_count for a in arrays):
                return path
            for i in range(t.num_fields):
                child = t.field(i)
                found = walk(
                    [a.field(i) for a in arrays], child.type, f"{path}.{child.name}"
                )
                if found is not None:
                    return found
        elif (
            _pa.types.is_list(t)
            or _pa.types.is_large_list(t)
            or _pa.types.is_fixed_size_list(t)
        ):
            return walk([a.flatten() for a in arrays], t.value_type, f"{path}[]")
        return None

    for name in data.column_names:
        column = data.column(name)
        found = walk(list(column.chunks), column.type, name)
        if found is not None:
            return found
    return None


@contextmanager
def _about_table(table: str) -> Iterator[None]:
    """Re-raise a nullability error that does not name its table with ``table`` added.

    Args:
        table: The table, as ``"<project>/<dataset>/<table>"``.
    """
    try:
        yield
    except NullabilityRecordError as exc:
        if exc.table is not None:
            raise
        raise NullabilityRecordError(exc.reason, table=table) from exc
    except NullInNonNullableColumnError as exc:
        if exc.table is not None:
            raise
        raise NullInNonNullableColumnError(exc.column, table=table) from exc


# ---------------------------------------------------------------------------
# SpiralDBConnector
# ---------------------------------------------------------------------------


class SpiralDBConnector:
    """DBConnectorProtocol implementation backed by SpiralDB (pyspiral).

    Scoped to a single dataset within a SpiralDB project. Auth is handled
    externally — run ``spiral login`` once; pyspiral keeps the credentials in its
    per-user application directory (``~/Library/Application Support/pyspiral/`` on
    macOS, ``~/.config/pyspiral/`` on Linux).

    Args:
        project_id: SpiralDB project identifier (e.g. ``"my-project-123456"``).
        dataset: Dataset within the project. Defaults to ``"default"``.
        overrides: Optional pyspiral client config overrides, e.g.
            ``{"server.url": "https://api.spiraldb.dev"}`` for the dev
            environment. See the pyspiral config docs for full options.
    """

    def __init__(
        self,
        project_id: str,
        dataset: str = "default",
        overrides: dict[str, str] | None = None,
    ) -> None:
        self._project_id = project_id
        self._dataset = dataset
        self._overrides = overrides
        self._closed = False
        self._spiral = sp.Spiral(overrides=overrides)
        self._project = self._spiral.project(project_id)

    def _require_open(self) -> None:
        """Raise RuntimeError if this connector has been closed."""
        if self._closed:
            raise RuntimeError("SpiralDBConnector is closed")

    def _table_id(self, table_name: str) -> str:
        """Return the dataset-qualified table identifier for a plain table name."""
        return f"{self._dataset}.{table_name}"

    def _qualified(self, table_name: str) -> str:
        """Return ``"<project>/<dataset>/<table>"``, the table as errors name it."""
        return f"{self._project_id}/{self._dataset}/{table_name}"

    def get_table_names(self) -> list[str]:
        """Return all table names in this connector's dataset, sorted alphabetically.

        Filters ``project.list_tables()`` to the connector's dataset. Only the
        plain ``.table`` field is returned; the opaque ``.id`` handle is never
        exposed.

        Returns:
            Sorted list of plain table name strings (no ``dataset.`` prefix).
        """
        self._require_open()
        resources = self._project.list_tables()
        return sorted(r.table for r in resources if r.dataset == self._dataset)

    def get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names in declaration order.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            List of PK column names; empty list if the table has no key schema.
        """
        self._require_open()
        return list(self._project.table(self._table_id(table_name)).key_schema.names)

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata with Arrow types.

        SpiralDB is Arrow-native; types are taken directly from the table's
        Arrow schema and normalized to their large variants at every nesting
        level (``arrow_utils.normalize_to_large_types``: ``string`` to
        ``large_string``, ``binary`` to ``large_binary``, ``list`` to
        ``large_list``). SpiralDB hands back ``string`` / ``binary`` whatever
        layout was written, so this gives the same types as ``iter_batches``
        and as the other connectors. Only layouts change: widths, nullability
        and nested field names are kept, and hashes are unaffected.

        Note: SpiralDB also silently drops timezone information from timestamp
        columns at storage time — ``timestamp[us, tz=UTC]`` is returned as
        ``timestamp[us]``.

        SpiralDB reports every top-level column and every struct field nullable. When
        the table carries a nullability record (``write_nullability_record``, written by
        ``upsert_records`` on a table's first write), the recorded columns and nested
        fields are reported non-nullable; without one, nullability is SpiralDB's.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            List of ColumnInfo objects. Propagates pyspiral exception if the
            table does not exist (no empty-list fallback).

        Raises:
            NullabilityRecordError: If the table's nullability record cannot be read or
                names a column the table does not have.
        """
        self._require_open()
        tbl = self._project.table(self._table_id(table_name))
        arrow_schema = tbl.schema().to_arrow()
        with _about_table(self._qualified(table_name)):
            paths = read_nullability_record(tbl.get_metadata())
            if paths is not None:
                arrow_schema = apply_nullability_record(arrow_schema, paths)
        return [
            ColumnInfo(
                name=field.name,
                arrow_type=arrow_utils.normalize_to_large_types(
                    field.type, keep_null=True
                ),
                nullable=field.nullable,
            )
            for field in arrow_schema
        ]

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a full-table scan and yield results as Arrow RecordBatches.

        SpiralDB has no SQL engine. The table name is parsed from the query's
        FROM clause (double-quoted or unquoted). Only ``SELECT * FROM "table"``
        patterns are supported; WHERE clauses and projections are silently ignored.

        The query must use a plain (non-qualified) table name — not
        ``"dataset.table"``. Passing a qualified name would cause a pyspiral
        lookup failure.

        Args:
            query: SQL-style query. Must contain ``FROM "table_name"``.
            params: Accepted for protocol compliance. SpiralDB has no parameterised
                interface — if not ``None``, a warning is logged and params are
                not used.
            batch_size: Accepted for protocol compliance. SpiralDB does not
                support caller-controlled batch sizing — the Spiral execution
                engine determines batch sizes. A warning is logged if a
                non-default value is passed.

        Yields:
            Arrow RecordBatch objects. Each batch has:
            - Every column's type normalized to its large variants at every
              nesting level (``arrow_utils.normalize_to_large_types``), the same
              types ``get_column_info`` reports.
            - Field metadata (including nested struct/list field metadata)
              restored from the SpiralDB native KV store, if any was written
              by ``upsert_records``.
            - Schema-level metadata restored from the KV store.
            - The nullability ``get_column_info`` reports: when the table carries a
              nullability record, every batch is checked and its recorded columns are
              non-nullable. Tables without a record pay no per-batch check.
            Yields nothing for an empty table.

        Raises:
            NullabilityRecordError: If the table's nullability record cannot be read or
                names a column the table does not have.
            NullInNonNullableColumnError: If a batch holds a null in a recorded
                non-nullable column (naming the table and the column).
        """
        self._require_open()
        if params is not None:
            logger.warning(
                "SpiralDBConnector does not support query parameters; "
                "ignoring params=%r",
                params,
            )
        if batch_size != 1000:
            logger.warning(
                "SpiralDBConnector does not support batch_size; "
                "batch sizing is controlled by the Spiral execution engine. "
                "Ignoring batch_size=%r",
                batch_size,
            )
        yield from self.scan_batches(_parse_table_name(query))

    def _batch_preparer(
        self, tbl: Any, table_name: str
    ) -> Callable[[pa.RecordBatch], pa.RecordBatch]:
        """Return the function that gives a scanned batch the types Orcapod reports.

        The function restores field and schema metadata from the table's key-value
        store, normalizes every type to its large variants and applies the table's
        nullability record, exactly as ``iter_batches`` documents.

        Args:
            tbl: The pyspiral table.
            table_name: Plain table name (no dataset prefix).

        Returns:
            A function from a scanned batch to the prepared batch.

        Raises:
            NullabilityRecordError: If the table's nullability record cannot be read.
        """
        import pyarrow as _pa  # noqa: PLC0415

        kv = tbl.get_metadata()
        schema_meta, field_trees = _load_arrow_metadata(kv)
        qualified = self._qualified(table_name)
        with _about_table(qualified):
            paths = read_nullability_record(kv)

        def prepare(batch: pa.RecordBatch) -> pa.RecordBatch:
            schema = batch.schema
            new_fields = []
            needs_rebuild = False
            for field in schema:
                # Recursively restore field metadata (including nested struct/list).
                restored_field, field_changed = _restore_field(
                    field, field_trees.get(field.name)
                )
                # Normalize to the large variants at every depth (layout only).
                normalized_type = arrow_utils.normalize_to_large_types(
                    restored_field.type, keep_null=True
                )
                if normalized_type != restored_field.type:
                    restored_field = restored_field.with_type(normalized_type)
                    field_changed = True
                new_fields.append(restored_field)
                needs_rebuild = needs_rebuild or field_changed

            restored_schema_meta = (
                schema_meta if schema_meta is not None else schema.metadata
            )
            if restored_schema_meta != schema.metadata:
                needs_rebuild = True

            if needs_rebuild:
                target_schema = _pa.schema(new_fields, metadata=restored_schema_meta)
                batch = batch.cast(target_schema)
            if paths is not None:
                with _about_table(qualified):
                    batch = enforce_nullability_record(batch, paths)
            return batch

        return prepare

    def scan_batches(
        self,
        table_name: str,
        *,
        where: Mapping[str, object] | None = None,
        limit: int | None = None,
        asof: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Scan a table, optionally restricted, and yield prepared Arrow batches.

        The restrictions are applied by SpiralDB, before any row reaches this
        process. Batches arrive in key order; their sizes follow SpiralDB's stored
        fragments and cannot be chosen. Each batch has the types, metadata and
        nullability ``iter_batches`` documents.

        Args:
            table_name: Plain table name (no dataset prefix).
            where: Column name to value. A row is read only if every named column
                equals its value.
            limit: Read at most this many rows, the first in key order.
            asof: Read the table as of this version (a ``get_table_version`` value).

        Yields:
            Arrow RecordBatch objects. Nothing for a table with no matching row.

        Raises:
            NullabilityRecordError: If the table's nullability record cannot be read or
                names a column the table does not have.
            NullInNonNullableColumnError: If a batch holds a null in a recorded
                non-nullable column (naming the table and the column).
        """
        self._require_open()
        tbl = self._project.table(self._table_id(table_name))
        prepare = self._batch_preparer(tbl, table_name)
        predicate = None
        for column, value in (where or {}).items():
            equals = tbl[column] == value
            predicate = equals if predicate is None else predicate & equals
        scan = self._spiral.scan(tbl.select(), where=predicate, limit=limit, asof=asof)
        for batch in scan.to_record_batches():
            yield prepare(batch)

    def get_arrow_schema(self, table_name: str) -> pa.Schema:
        """Return the schema of the batches ``scan_batches`` yields, without scanning.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            The prepared Arrow schema: large-normalized types, restored metadata and
            the nullability the table's nullability record gives.

        Raises:
            NullabilityRecordError: If the table's nullability record cannot be read or
                names a column the table does not have.
        """
        import pyarrow as _pa  # noqa: PLC0415

        self._require_open()
        tbl = self._project.table(self._table_id(table_name))
        declared = tbl.schema().to_arrow()
        empty = _pa.RecordBatch.from_pylist([], schema=declared)
        return self._batch_preparer(tbl, table_name)(empty).schema

    def get_spiral_table_id(self, table_name: str) -> str:
        """Return SpiralDB's own id of the table (for example ``table_W7C01nkLTmEJ``).

        The id stays the same when the table is renamed. A table dropped and created
        again under the same name gets a new one.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            The table id.
        """
        self._require_open()
        return str(self._project.table(self._table_id(table_name)).table_id)

    def get_table_version(self, table_name: str) -> int:
        """Return the table's current version, usable as ``asof`` in ``scan_batches``.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            The time of the table's last change, in microseconds; ``0`` for a table
            that was never written.
        """
        self._require_open()
        return int(self._project.table(self._table_id(table_name)).last_modified_at())

    def get_row_count_estimate(self, table_name: str, asof: int | None = None) -> int:
        """Return the table's row count as SpiralDB's manifests record it, without scanning.

        The count is what the manifests hold: it can be above the number of rows a
        scan returns when rows were deleted or overwritten and not yet compacted.

        Args:
            table_name: Plain table name (no dataset prefix).
            asof: Count the table as of this version (a ``get_table_version`` value,
                as ``scan_batches`` takes). If ``None`` (default), the current version.

        Returns:
            The recorded row count.
        """
        self._require_open()
        tbl = self._project.table(self._table_id(table_name))
        stats = self._spiral.core.manifest_stats(tbl.core.get_snapshot(asof=asof))
        return int(stats.key_space_stats.total_rows)

    def delete_table(self, table_name: str) -> None:
        """Drop a table from SpiralDB, if it exists.

        Args:
            table_name: Plain table name (no dataset prefix). Silently ignored
                if the table does not exist.
        """
        self._require_open()
        if table_name in self.get_table_names():
            self._project.drop_table(self._table_id(table_name))

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        """Create a SpiralDB table with a single-column key schema, idempotently.

        Only the primary-key column's type is registered with Spiral at creation
        time (via ``key_schema``). Non-PK columns in ``columns`` are ignored —
        Spiral infers value column schemas from the first write. A second call
        with the same table name is a no-op (``exist_ok=True``).

        Only single-column primary keys are supported via this method. Tables
        with composite primary keys must be created outside this connector but
        can be read and written through it.

        Args:
            table_name: Plain table name (no dataset prefix).
            columns: Full column list; must include ``pk_column``. Non-PK entries
                are accepted but not used at creation time.
            pk_column: Name of the single primary-key column.

        Raises:
            ValueError: If ``pk_column`` is not found in ``columns`` (including
                when ``columns`` is empty).
        """
        self._require_open()
        col_names = [c.name for c in columns]
        if pk_column not in col_names:
            raise ValueError(
                f"pk_column {pk_column!r} not found in columns: {col_names}"
            )
        pk_arrow_type = next(c.arrow_type for c in columns if c.name == pk_column)
        self._project.create_table(
            self._table_id(table_name),
            key_schema=[(pk_column, pk_arrow_type)],
            exist_ok=True,
        )

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        """Write records to a SpiralDB table.

        SpiralDB uses the table's key schema for conflict detection. ``id_column``
        is accepted for protocol compliance; it must appear in the table's key
        schema (``tbl.key_schema.names``), but SpiralDB always uses the full
        composite key schema — not just ``id_column`` — for conflict resolution.

        When ``skip_existing=False`` (default), all rows are written and existing
        rows with matching keys are overwritten (upsert-by-key). When
        ``skip_existing=True``, the entire table is scanned first and only rows
        whose composite key is absent from the existing table are written. This
        is O(n) in table size.

        Args:
            table_name: Plain table name (no dataset prefix).
            records: Arrow table to write.
            id_column: Must be one of the table's key-schema columns. Validated
                for correctness; the full composite key schema is used for
                conflict detection.
            skip_existing: If False (default), overwrite on key conflict. If
                True, skip rows with already-existing composite keys (requires
                full table scan). Note: unreliable when PK columns contain
                timezone-aware timestamps — SpiralDB strips timezone at storage
                time, so timezone-aware values in ``records`` will never match
                the timezone-naive values returned by the existing-row scan,
                causing all rows to appear novel and be re-written.

        Nullability: SpiralDB reports every top-level column and struct field
        nullable, so the connector keeps the non-nullable paths of ``records.schema`` in
        the table's nullability record (``write_nullability_record``). It writes the
        record, before the rows, on the first write to a table that has no value columns
        yet, and removes it again if those rows fail to write. A table that already holds
        rows without a record gets none: recording it is a deliberate act. A later write
        must declare exactly the recorded paths.

        Writers are not coordinated: SpiralDB offers no lock, and its metadata writes
        are not versioned. If concurrent first writes to the same new table race with a
        failed one, the table can end up without a record although rows were written:
        it then reads as unrecorded (``T | None``), never with a hidden null. Repair it with
        ``write_nullability_record`` after checking the data holds no null in the
        non-nullable columns.

        Limitation: "no value columns yet" stands for "no rows yet", which saves a scan
        on every first write. A table whose columns are all key columns, holding rows
        and no record, is recorded by the next write; if that write adds value columns
        declared non-nullable, the earlier rows hold nulls there and every read raises
        ``NullInNonNullableColumnError`` (a null is never hidden).

        Raises:
            ValueError: If ``id_column`` is not in the table key schema (including
                empty key schema), or if any key-schema column is absent from
                ``records``.
            NullInNonNullableColumnError: If ``records`` holds a null in a column (or
                nested field) its schema declares non-nullable. Nothing is written.
            NullabilityRecordMismatchError: If the table's record lists different
                non-nullable paths than ``records.schema``. Nothing is written.
            NullabilityRecordError: If the table's record cannot be read, or
                ``records`` has a non-nullable field inside a map.
            UnsupportedValueError: If ``records`` holds a null struct at any depth, which
                SpiralDB cannot store. Nothing is written; ``push_down_nulls`` is never
                passed.
        """
        self._require_open()
        tbl = self._project.table(self._table_id(table_name))
        pk_cols = list(tbl.key_schema.names)

        # Guard 1: id_column must appear in the table's key schema.
        # Also handles empty-key-schema tables: nothing is ever `in []`.
        if id_column not in pk_cols:
            raise ValueError(
                f"id_column {id_column!r} is not in the table key schema {pk_cols}. "
                "SpiralDB uses the table key schema for conflict detection."
            )

        # Guard 2: all PK columns must be present in the records table.
        missing = [k for k in pk_cols if k not in records.schema.names]
        if missing:
            raise ValueError(
                f"records is missing key column(s) {missing} required by the "
                f"table key schema {pk_cols}."
            )

        # Nullability (ITL-936). SpiralDB keeps no non-null flag except on list items, and
        # stores a null written to a non-null struct field as the type's zero, so every
        # non-nullable path is checked here before anything is written.
        qualified = self._qualified(table_name)
        null_struct = _null_struct_path(records)
        if null_struct is not None:
            raise UnsupportedValueError(
                f"table {qualified}: column {null_struct!r} holds a null struct, which "
                "SpiralDB cannot store (it would read back as a struct of null fields); "
                "nothing was written"
            )
        with _about_table(qualified):
            paths = schema_paths(records.schema)
            check_no_nulls(records, paths)
            recorded = read_nullability_record(tbl.get_metadata())
        if recorded is not None:
            if recorded != paths:
                raise NullabilityRecordMismatchError(
                    [render_path(p) for p in sorted(recorded - paths)],
                    [render_path(p) for p in sorted(paths - recorded)],
                    table=qualified,
                )
        # A table with no value columns yet holds no rows Orcapod did not record (but see
        # the limitation in the docstring: a table whose columns are all keys may).
        record_first = recorded is None and set(tbl.schema().to_arrow().names) <= set(
            pk_cols
        )

        meta_kv = _serialize_arrow_metadata(records)

        def write_rows(rows: pa.Table) -> None:
            # Record first, so rows are never stored without their record; if the rows
            # then fail, remove the record again, since it names columns the table lacks.
            wrote_record = record_first and write_nullability_record(
                tbl, records.schema
            )
            try:
                tbl.write(rows)
            except BaseException:
                # Remove the record only while the table still has no value columns:
                # once another writer's rows have landed, they rely on it.
                if wrote_record:
                    try:
                        if set(tbl.schema().to_arrow().names) <= set(pk_cols):
                            remove_nullability_record(tbl)
                    except Exception as cleanup_exc:
                        logger.warning(
                            "Could not remove the nullability record of %s after a "
                            "failed first write (%s: %s); reads of the table will "
                            "raise NullabilityRecordError until a write succeeds.",
                            qualified,
                            type(cleanup_exc).__name__,
                            cleanup_exc,
                        )
                raise
            if meta_kv is not None:
                tbl.set_metadata(meta_kv)

        if not skip_existing:
            # Always upsert-by-key: existing rows overwritten, novel rows inserted.
            write_rows(records)
            return

        # skip_existing=True: full scan → client-side key filter → write novel rows.
        existing = self._spiral.scan(tbl.select()).to_table()
        existing_keys = {tuple(row[k] for k in pk_cols) for row in existing.to_pylist()}
        mask = pa.array(
            [
                tuple(row[k] for k in pk_cols) not in existing_keys
                for row in records.to_pylist()
            ],
            type=pa.bool_(),
        )
        novel = records.filter(mask)
        if len(novel) > 0:
            write_rows(novel)

    def validate_records(self, records: pa.Table) -> None:
        """No-op: ``SpiralDBConnector`` preserves Arrow field metadata via the native KV store."""

    def close(self) -> None:
        """Mark this connector as closed. Idempotent. No network teardown needed."""
        self._closed = True

    def __enter__(self) -> SpiralDBConnector:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def to_config(self) -> dict[str, Any]:
        """Serialize connection configuration to a JSON-compatible dict.

        Returns:
            Dict with ``connector_type``, ``project_id``, ``dataset``, and
            ``overrides`` keys.

        Raises:
            RuntimeError: If this connector has been closed.
        """
        self._require_open()
        return {
            "connector_type": "spiraldb",
            "project_id": self._project_id,
            "dataset": self._dataset,
            "overrides": self._overrides,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SpiralDBConnector:
        """Reconstruct a SpiralDBConnector from a config dict.

        Args:
            config: Dict with ``connector_type``, ``project_id``, and optionally
                ``dataset`` and ``overrides`` keys.

        Returns:
            A new, open SpiralDBConnector instance.

        Raises:
            ValueError: If ``connector_type`` is not ``"spiraldb"``.
        """
        if config.get("connector_type") != "spiraldb":
            raise ValueError(
                f"Expected connector_type 'spiraldb', "
                f"got {config.get('connector_type')!r}"
            )
        return cls(
            project_id=config["project_id"],
            dataset=config.get("dataset", "default"),
            overrides=config.get("overrides"),
        )
