# ITL-471: SpiralDBConnector — Preserve Arrow Table- and Column-Level Metadata via Native KV Store

**Date:** 2026-07-08
**Issue:** [ITL-471](https://linear.app/enigma-metamorphic/issue/ITL-471)
**Status:** Approved

---

## Overview

`SpiralDBConnector` writes Arrow tables to SpiralDB via `tbl.write(records)` and reads them
back via `self._spiral.scan(tbl.select()).to_record_batches()`. SpiralDB's Vortex columnar
format silently drops all Arrow metadata — both schema-level (`schema.metadata`) and
per-column (`field.metadata`) — on ingestion. This makes it impossible to round-trip Arrow
extension types (Path, UUID, dataclass, etc.) through the SpiralDB backend, and prevents
`SpiralDBConnector` from participating in the peek-register-read pattern used for extension
type auto-registration.

SpiralDB exposes a native table-level KV metadata store (`Table.set_metadata` /
`Table.get_metadata` / `Table.drop_metadata`) that does round-trip exactly. This design
uses that store to persist and restore Arrow metadata across write/read cycles.

As an interim safety measure, `ConnectorArrowDatabase.add_records` currently raises
`ValueError` for any column carrying an `ARROW:extension:*` field, because connectors that
drop field metadata silently demote logical types on read. This issue lifts that blanket
guard and replaces it with a per-connector hook.

---

## Goals & Success Criteria

- On write, `SpiralDBConnector` serializes `schema.metadata` and per-field `field.metadata`
  from the incoming Arrow table into the SpiralDB native KV store.
- On read (`iter_batches`), the connector loads stored metadata and reattaches it to the
  reconstructed Arrow schema and per-field `pa.Field` objects; the existing
  `large_string`/`large_binary` normalization is preserved.
- Arrow extension types survive a full write→read cycle through `ConnectorArrowDatabase` +
  `SpiralDBConnector`, enabling `register_discovered_extensions` / peek-register-read.
- The `ValueError` extension-type guard in `ConnectorArrowDatabase.add_records` no longer
  triggers for the SpiralDB backend. Other connectors (SQLite, PostgreSQL) continue to
  reject extension-typed records by default.
- `get_column_info` is intentionally unchanged: `ColumnInfo` has no metadata slot and its
  callers (schema validation, table creation) do not need field metadata. Extension-type
  discovery flows through `iter_batches` → `register_discovered_extensions(table.schema)`.
- End-to-end round-trip integration tests and unit tests added.

---

## Scope & Boundaries

In scope:

- Serializing/deserializing both table-level (`schema.metadata`) and column-level
  (`field.metadata`) Arrow metadata for the SpiralDB connector.
- A single stable KV key encoding scheme under `"__arrow_metadata__"`.
- Restoring metadata on read so extension-type discovery works.
- Per-connector `validate_records` hook on `DBConnectorProtocol` (default rejects
  extension types; `SpiralDBConnector` overrides to no-op).
- Replacing `ConnectorArrowDatabase`'s inline extension-type guard with a delegation
  call to `self._connector.validate_records(records)`.
- Unit tests and SpiralDB-backend integration tests.

Out of scope:

- SQLite (ITL-169) and PostgreSQL connectors — separate follow-ups.
- `get_column_info` metadata changes — `ColumnInfo` has no metadata field; not needed
  for extension-type discovery.
- SpiralDB timezone-stripping on timestamp columns (ITL-40, separate known limitation).
- DB-native ↔ Arrow type mapping changes.

---

## Design

### 1. Metadata Encoding

All Arrow metadata is stored under a **single reserved KV key** `"__arrow_metadata__"`
in the SpiralDB table's native KV store (`Table.set_metadata` / `Table.get_metadata`).
The value is a UTF-8–encoded JSON blob with this structure:

```json
{
  "schema": {
    "<base64(meta_key)>": "<base64(meta_value)>"
  },
  "fields": {
    "<col_name>": {
      "meta": {
        "<base64(meta_key)>": "<base64(meta_value)>"
      },
      "children": {
        "<child_field_name>": {
          "meta": { "<base64(meta_key)>": "<base64(meta_value)>" },
          "children": { ... }
        }
      }
    }
  }
}
```

- `"schema"` holds the table-level `schema.metadata` entries. Arrow metadata keys and
  values are `bytes`; they are base64-encoded (standard `base64.b64encode`) for safe
  embedding in JSON.
- `"fields"` holds a **recursive metadata tree** for each top-level column. Each node in
  the tree has two optional keys:
  - `"meta"`: the field's own `field.metadata`, base64-encoded k/v pairs. Omitted if the
    field has no metadata.
  - `"children"`: a dict mapping child field names to their recursive metadata trees.
    Populated for composite types — `struct` (inner fields), `list_`/`large_list`/
    `fixed_size_list` (value field), `map_` (key and item fields). Omitted for primitives
    or when no child has any metadata.
- An entry in `"fields"` is only present if that top-level column or any of its
  descendants has metadata. If there is **no Arrow metadata at all** (schema.metadata is
  None, all fields and their descendants have no metadata), `set_metadata` is **not
  called**. On read, absence of the key means no metadata to restore — backward
  compatible with tables written before this change.

Four private module-level helpers live in `spiraldb_connector.py`:

```python
def _serialize_arrow_metadata(table: pa.Table) -> dict[str, bytes] | None:
    """Encode all Arrow metadata from ``table`` into a single KV entry.

    Recursively walks each column's type tree. Returns
    ``{"__arrow_metadata__": blob}`` if any metadata exists anywhere in the
    schema (including nested struct/list/map fields), or ``None`` if the schema
    and all fields (at every depth) have no metadata.
    """

def _serialize_field_meta_tree(field: pa.Field) -> dict | None:
    """Recursively build the metadata tree for a single field.

    Returns a dict with optional ``"meta"`` and ``"children"`` keys, or
    ``None`` if this field and all its descendants have no metadata.
    """

def _load_arrow_metadata(
    kv: dict[str, bytes],
) -> tuple[dict[bytes, bytes] | None, dict[str, dict]]:
    """Decode Arrow metadata from a SpiralDB table KV store.

    Returns ``(schema_meta, field_trees)`` where ``schema_meta`` is None if
    absent and ``field_trees`` maps top-level column name → raw metadata tree
    dict (as stored in the blob).
    """

def _restore_field(field: pa.Field, stored: dict | None) -> tuple[pa.Field, bool]:
    """Recursively restore a field's metadata and rebuild its nested type.

    Walks the stored metadata tree in parallel with the field's type tree,
    reattaching ``field.metadata`` at every level and reconstructing composite
    types (``struct``, ``list_``, ``large_list``, ``fixed_size_list``, ``map_``)
    bottom-up when any descendant has metadata to restore.

    Returns ``(restored_field, changed)`` where ``changed`` is True if any
    metadata or type was modified.
    """
```

### 2. Write Path (`upsert_records`)

After the existing `tbl.write(records)` call (and after the `skip_existing` filtered
write), serialize and persist metadata:

```python
meta_kv = _serialize_arrow_metadata(records)
if meta_kv is not None:
    tbl.set_metadata(meta_kv)
```

- `set_metadata` is called on the **SpiralDB table handle** (`tbl`), not the Arrow table.
- It is called **after** the data write. If `tbl.write` raises, metadata is never written.
- For `skip_existing=True`, metadata is derived from the **input** `records` (not the
  filtered `novel` subset), because field metadata is schema-level, not row-level.
- On each flush the metadata is overwritten. This is correct: all writes to the same table
  share the same schema, so the latest write wins and produces the same result.
- If records have no Arrow metadata, `set_metadata` is skipped entirely.

### 3. Read Path (`iter_batches`)

Load KV metadata **once** before the scan, then use `_restore_field` recursively during
the existing field-rebuild loop — unifying string/binary normalization with full-depth
metadata restoration:

```python
tbl = self._project.table(self._table_id(table_name))
schema_meta, field_trees = _load_arrow_metadata(tbl.get_metadata())
reader = self._spiral.scan(tbl.select()).to_record_batches()

for batch in reader:
    schema = batch.schema
    new_fields = []
    needs_rebuild = False

    for field in schema:
        # _restore_field recurses into struct/list/map children, rebuilding
        # the type tree bottom-up and reattaching metadata at every level.
        restored_field, field_changed = _restore_field(field, field_trees.get(field.name))

        # Apply large_string / large_binary normalization on top of the
        # recursively-restored field.
        if restored_field.type == _pa.string():
            restored_field = restored_field.with_type(_pa.large_string())
            field_changed = True
        elif restored_field.type == _pa.binary():
            restored_field = restored_field.with_type(_pa.large_binary())
            field_changed = True

        new_fields.append(restored_field)
        needs_rebuild = needs_rebuild or field_changed

    restored_schema_meta = schema_meta if schema_meta is not None else schema.metadata
    if restored_schema_meta != schema.metadata:
        needs_rebuild = True

    if needs_rebuild:
        target_schema = _pa.schema(new_fields, metadata=restored_schema_meta)
        batch = batch.cast(target_schema)

    yield batch
```

- `tbl.get_metadata()` is called **once per scan**, not once per batch.
- `_restore_field` handles the recursive type rebuilding. The string/binary normalization
  is applied **after** restoration on the top-level field type only (SpiralDB only
  normalises top-level string/binary; nested strings inside a struct remain as-is from
  the wire, which is consistent with current behaviour).
- `needs_rebuild` is True if any field at any depth changed type or metadata, or when
  schema-level metadata differs.
- `batch.cast(target_schema)` handles both type changes and metadata updates — PyArrow
  applies `target_schema` in full including nested field metadata.
- Tables with no stored metadata behave exactly as before (backward compatible).

### 4. Protocol Extension (`validate_records`)

Add `validate_records` to `DBConnectorProtocol` as a concrete default method containing
the existing extension-type guard logic (moved from `ConnectorArrowDatabase`):

```python
def validate_records(self, records: "pa.Table") -> None:
    """Validate that records are safe to write through this connector.

    The default implementation rejects Arrow extension-typed columns because
    connectors that do not preserve field metadata silently demote logical
    types to their storage type on read. Override to relax this check for
    connectors that do preserve Arrow metadata.

    Args:
        records: Arrow table to validate before writing.

    Raises:
        ValueError: If any column carries an Arrow extension type that this
            connector cannot round-trip.
    """
    import pyarrow as _pa
    _EXT_NAME_KEY = b"ARROW:extension:name"
    ext_fields: list[tuple[str, str]] = []
    for field in records.schema:
        if isinstance(field.type, _pa.ExtensionType):
            ext_fields.append((field.name, field.type.extension_name))
        elif field.metadata and _EXT_NAME_KEY in field.metadata:
            ext_fields.append(
                (field.name, field.metadata[_EXT_NAME_KEY].decode("utf-8", errors="replace"))
            )
    if ext_fields:
        ext_info = ", ".join(f"{name!r}: {ext_name!r}" for name, ext_name in ext_fields)
        raise ValueError(
            f"{type(self).__name__} does not support Arrow extension-typed columns "
            f"({ext_info}). This connector does not preserve ARROW:extension:* field "
            "metadata across read/write cycles."
        )
```

`SpiralDBConnector` overrides it as a no-op:

```python
def validate_records(self, records: "pa.Table") -> None:
    """No-op: SpiralDBConnector preserves Arrow field metadata via the native KV store."""
```

`SQLiteConnector` and `PostgreSQLConnector` inherit the default-reject behavior without
any changes. When those connectors implement metadata preservation (ITL-169 etc.), they
override `validate_records` to a no-op — no changes to `ConnectorArrowDatabase` needed.

### 5. `ConnectorArrowDatabase` Change

Replace the inline guard block (~lines 247–273) with a single call:

```python
self._connector.validate_records(records)
```

No other changes to `ConnectorArrowDatabase`.

---

## Testing

### Unit Tests (`tests/test_databases/test_spiraldb_connector.py`)

- `TestSerializeFieldMetaTree` / `TestRestoreField`: pure-function tests for the recursive
  helpers — primitive field with metadata, struct with nested field metadata, list with
  value field metadata, deeply nested struct-in-struct, fields with no metadata (returns
  None / unchanged), round-trip fidelity.
- `TestSerializeArrowMetadata` / `TestLoadArrowMetadata`: top-level roundtrip tests —
  schema-only, fields-only, mixed, no metadata (returns None), column names with special
  characters, multiple fields, nested struct columns.
- `TestIterBatchesMetadata`: mock `tbl.get_metadata()` — field metadata reattached to
  yielded batches; string/binary normalization preserved when field metadata present;
  no stored metadata → batches unchanged (backward compat).
- `TestUpsertRecordsMetadata`: verify `tbl.set_metadata` called after `tbl.write` with
  correct KV dict; skipped when records have no Arrow metadata; `skip_existing=True`
  path also calls `set_metadata`.
- `TestValidateRecords`: default implementation raises for in-memory extension types and
  metadata-only extension fields, passes for plain types; `SpiralDBConnector.validate_records`
  is a no-op for all inputs.

### Unit Tests (`tests/test_databases/test_connector_arrow_database.py`)

- Verify `add_records` delegates the extension-type check to the connector via
  `validate_records`; a mock connector with a permissive override allows extension-typed
  records; a mock connector with the default raises.

### Integration Tests (`tests/test_databases/test_spiraldb_connector_integration.py`)

New class `TestArrowMetadataRoundTrip` (gated on `SPIRAL_INTEGRATION_TESTS=1`):

- `test_schema_metadata_round_trip`: write with `schema.metadata`, read back, assert intact.
- `test_field_metadata_round_trip`: write with per-column `field.metadata`, read back,
  assert restored on correct columns.
- `test_extension_type_metadata_round_trip`: write with `ARROW:extension:name` /
  `ARROW:extension:metadata` in field metadata (storage type `large_string`), read back,
  assert field metadata survives and schema is usable with `register_discovered_extensions`.
- `test_nested_struct_field_metadata_round_trip`: write a table with a `struct` column
  whose inner fields carry `field.metadata`, read back, assert inner field metadata is
  restored at the correct depth.
- `test_no_metadata_backward_compatible`: plain table with no Arrow metadata — read back
  yields None schema.metadata and None field.metadata (no spurious KV entries).
- `test_connector_arrow_database_extension_type_round_trip`: full `ConnectorArrowDatabase`
  path — `add_record` with extension-typed column, `flush`, `get_record_by_id`, assert
  extension field metadata intact on the returned table.

---

## Files Changed

| File | Change |
|---|---|
| `src/orcapod/databases/spiraldb_connector.py` | Add `_serialize_arrow_metadata`, `_load_arrow_metadata` helpers; modify `upsert_records` (write metadata after data write); modify `iter_batches` (load + restore metadata); add `validate_records` no-op override |
| `src/orcapod/protocols/db_connector_protocol.py` | Add `validate_records` concrete default method |
| `src/orcapod/databases/connector_arrow_database.py` | Replace inline guard (~lines 247–273) with `self._connector.validate_records(records)` |
| `tests/test_databases/test_spiraldb_connector.py` | New test classes for serialization helpers, metadata read/write, `validate_records` |
| `tests/test_databases/test_connector_arrow_database.py` | Tests for delegated `validate_records` |
| `tests/test_databases/test_spiraldb_connector_integration.py` | New `TestArrowMetadataRoundTrip` class |

---

## Dependencies & Risks

- **Key-collision safety:** Using a single reserved key `"__arrow_metadata__"` with a
  structured JSON blob avoids all collision risk within the KV namespace.
- **Backward compatibility:** Tables written before this change have no `"__arrow_metadata__"`
  key — `_load_arrow_metadata` returns `(None, {})` and behavior is identical to before.
- **Partial failure:** `set_metadata` is called after `tbl.write`. A failure between the
  two leaves data written but no metadata. This is acceptable — the read path handles
  missing metadata gracefully (returns None), and the next successful write will set it.
- **`batch.cast(target_schema)`:** When types are unchanged and only field metadata
  differs, `cast` is effectively a schema-swap. Verified to propagate field metadata
  from `target_schema` correctly in PyArrow.
- **ITL-169 coordination:** When SQLite adds metadata preservation, it overrides
  `validate_records` to a no-op. No changes to `ConnectorArrowDatabase` needed.
