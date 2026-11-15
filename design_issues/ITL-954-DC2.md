## `src/orcapod/contexts/` — data context of stored rows

### DC2 — tables written before v0.3 were looked for at a path v0.3 never computes
**Status:** resolved, with open follow-ups listed below
**Severity:** high
**Issue:** ITL-954

DC1 (ITL-944) found a table of an earlier version at the node's own path with an older
layout component. The other v0.3 changes (hash label rule ITL-945, logical type identifiers
ITL-935, corrected hashes ITL-942) move every pipeline hash and function uri, so a real v0.2
table is at a path v0.3 never computes: nothing would be reported, and a side-effecting node
would deliver everything again on a v0.2 store.

**Fix:**
- `ArrowDatabaseProtocol.list_table_paths(prefix)` lists the tables under a path prefix,
  nested tables included, without opening one. In-memory and local Delta: exact. Delta on
  `s3://`: through `s3fs`, one list request per directory. Delta on `gs://`, `az://`,
  `abfs://`: only with the matching `fsspec` package and its default credentials. Connector
  stores (SQLite, PostgreSQL, SpiralDB): by table name, components come back sanitized.
  No-op: nothing. A store that cannot list raises `TableListingNotSupportedError`.
- Old versus current is decided by the layout (last path component), not by a hash:
  `contexts/stored.CURRENT_LAYOUTS`. Under the hash-free beginning of a node's path
  (`<function>`, `side_effect_function/<function>`, `side_effects/<pod>`; the same below
  `_result`) every table that is not at a current layout was written before v0.3
  (`find_stale_tables(..., name_path=...)`). The sibling-path check of DC1 stays.
- Side-effecting nodes of both kinds raise `SideEffectLogDataContextError` naming every
  table, before anything is delivered. Other function nodes report the tables in one
  warning per node. `PipelineJob.run()` reports one total of the marked old tables under the
  job that no node claims by name (operator caches, cached sources, nodes that left the job).
- A store that cannot list its tables: a side-effecting node logs one warning and proceeds
  (developer, November 11, 2026).
- Proven on a database written by Orcapod v0.2.0 itself
  (`tests/test_arrays/data/v02_full_store.tar.gz`), as written and with every hash in its
  paths replaced (`tests/test_pipeline/test_pre_v03_tables.py`).

Known limits, follow-ups:
- Accepted false positives, which err towards failing: an old table of another function or
  pod with the same name in the same job, and of another node of the same function.
- Tables found by listing are not read, so they are reported without a number of rows.
- On a connector store the listed path is in the backend's spelling, so a table found both
  at the sibling path and by listing is named twice in a report or error.
- A failed listing on cloud storage (credentials the `fsspec` file system does not get from
  the deltalake storage options, a network error) is treated as "cannot list": the node
  warns and proceeds. Only credentials, endpoint and region are translated for `s3fs`.
- The S3 listing is tested against MinIO only where Docker or `MINIO_ENDPOINT` is available.
- The job-level total needs a positive mark (`pdb_v1`, `rdb_v1`, `tdb_v1`, or a last
  component starting with `schema:`, `instance:`, `node:`), because observers and users store
  other tables under the job. A cached source with an explicit `cache_path` written by v0.2
  has no mark and is found by the sibling-path check only.
- PostgreSQL truncates identifiers to 63 bytes; Orcapod's table names for function tables
  are longer. Whether such tables work at all, and what a truncated name means for the
  layout suffix, is not verified here.
- To verify again once the hash-moving branches are combined:
  `uv run pytest tests/test_pipeline/test_pre_v03_tables.py tests/test_databases/test_list_table_paths.py -v`
  (the `as_written` cases then exercise real moved hashes).
