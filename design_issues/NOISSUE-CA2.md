## `src/orcapod/databases/connector_arrow_database.py`

### CA2 — Extension-type detection logic duplicated across multiple connectors
**Status:** open
**Severity:** low

The `b"ARROW:extension:name"` detection logic (checking both `isinstance(field.type, pa.ExtensionType)`
and `field.metadata.get(b"ARROW:extension:name")`) is copied verbatim into `SQLiteConnector.validate_records`,
`PostgreSQLConnector.validate_records`, and `MockDBConnector` in the test files. The constant
`b"ARROW:extension:name"` also appears inline in `extension_types/schema_walker.py` and other places
without a single canonical definition.

**Fix:** Extract into a shared helper `_check_extension_fields(schema) -> list[tuple[str, str]]`
(and/or a module-level constant `_ARROW_EXT_NAME_KEY`) reachable from all connector implementations.
When SQLite/PostgreSQL implement metadata preservation (PLT-1795), they'll override `validate_records`
to a no-op — at that point the duplication goes away naturally. Until then, the helper would keep
the detection logic in one place.
