"""SpiralDBConnector — DBConnectorProtocol implementation backed by SpiralDB (pyspiral).

Requires the ``spiraldb`` optional extra: ``pip install orcapod[spiraldb]``.
Authentication is handled externally via the ``spiral login`` CLI command,
which stores credentials in ``~/.config/pyspiral/auth.json``.

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
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod.types import ColumnInfo

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

    blob = json.loads(kv[_ARROW_METADATA_KEY].decode("utf-8"))

    schema_meta: dict[bytes, bytes] | None = None
    if "schema" in blob:
        schema_meta = {
            base64.b64decode(k): base64.b64decode(v)
            for k, v in blob["schema"].items()
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
            base64.b64decode(k): base64.b64decode(v)
            for k, v in stored["meta"].items()
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


# ---------------------------------------------------------------------------
# SpiralDBConnector
# ---------------------------------------------------------------------------


class SpiralDBConnector:
    """DBConnectorProtocol implementation backed by SpiralDB (pyspiral).

    Scoped to a single dataset within a SpiralDB project. Auth is handled
    externally — run ``spiral login`` once to store credentials in
    ``~/.config/pyspiral/auth.json``.

    Args:
        project_id: SpiralDB project identifier (e.g. ``"my-project-123456"``).
        dataset: Dataset within the project. Defaults to ``"default"``.
        overrides: Optional pyspiral client config overrides, e.g.
            ``{"server.url": "http://api.spiraldb.dev"}`` for the dev
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
        Arrow schema with two normalizations applied for consistency with
        other connectors (SQLite, PostgreSQL):

        - ``pa.string()`` → ``pa.large_string()``: SpiralDB stores all string
          columns as ``string`` regardless of whether they were written as
          ``large_string``. Normalizing to ``large_string`` matches the
          convention used by the other connectors and by ``ConnectorArrowDatabase``.
        - ``pa.binary()`` → ``pa.large_binary()``: same reasoning.

        Note: SpiralDB also silently drops timezone information from timestamp
        columns at storage time — ``timestamp[us, tz=UTC]`` is returned as
        ``timestamp[us]``.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            List of ColumnInfo objects. Propagates pyspiral exception if the
            table does not exist (no empty-list fallback).
        """
        import pyarrow as _pa  # noqa: PLC0415

        self._require_open()
        arrow_schema = (
            self._project.table(self._table_id(table_name)).schema().to_arrow()
        )

        def _normalize(arrow_type: _pa.DataType) -> _pa.DataType:
            if arrow_type == _pa.string():
                return _pa.large_string()
            if arrow_type == _pa.binary():
                return _pa.large_binary()
            return arrow_type

        return [
            ColumnInfo(
                name=field.name,
                arrow_type=_normalize(field.type),
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
            Arrow RecordBatch objects, with ``pa.string()`` normalized to
            ``pa.large_string()`` and ``pa.binary()`` to ``pa.large_binary()``.
            This matches the types reported by ``get_column_info`` and those
            used by ``ConnectorArrowDatabase`` for pending records, preventing
            schema mismatches in ``pa.concat_tables`` calls. Yields nothing for
            an empty table.
        """
        import pyarrow as _pa  # noqa: PLC0415

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
        table_name = _parse_table_name(query)
        tbl = self._project.table(self._table_id(table_name))
        reader = self._spiral.scan(tbl.select()).to_record_batches()
        for batch in reader:
            schema = batch.schema
            new_fields = []
            needs_cast = False
            for field in schema:
                ftype = field.type
                if ftype == _pa.string():
                    new_fields.append(
                        _pa.field(field.name, _pa.large_string(), field.nullable, field.metadata)
                    )
                    needs_cast = True
                elif ftype == _pa.binary():
                    new_fields.append(
                        _pa.field(field.name, _pa.large_binary(), field.nullable, field.metadata)
                    )
                    needs_cast = True
                else:
                    new_fields.append(field)
            if needs_cast:
                target_schema = _pa.schema(new_fields, metadata=schema.metadata)
                batch = batch.cast(target_schema)
            yield batch

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

        Raises:
            ValueError: If ``id_column`` is not in the table key schema (including
                empty key schema), or if any key-schema column is absent from
                ``records``.
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

        meta_kv = _serialize_arrow_metadata(records)

        if not skip_existing:
            # Always upsert-by-key: existing rows overwritten, novel rows inserted.
            tbl.write(records)
            if meta_kv is not None:
                tbl.set_metadata(meta_kv)
            return

        # skip_existing=True: full scan → client-side key filter → write novel rows.
        existing = self._spiral.scan(tbl.select()).to_table()
        existing_keys = {
            tuple(row[k] for k in pk_cols)
            for row in existing.to_pylist()
        }
        mask = pa.array(
            [
                tuple(row[k] for k in pk_cols) not in existing_keys
                for row in records.to_pylist()
            ],
            type=pa.bool_(),
        )
        novel = records.filter(mask)
        if len(novel) > 0:
            tbl.write(novel)
            if meta_kv is not None:
                tbl.set_metadata(meta_kv)

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
