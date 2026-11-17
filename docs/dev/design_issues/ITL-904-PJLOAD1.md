## `src/orcapod/pipeline/job.py` — reading a loaded `PipelineJob`

### PJLOAD1 — a loaded job could not read its nodes, and could not load a pod feeding an operator
**Status:** resolved
**Severity:** high
**Issue:** ITL-904

Saving a job and loading it with `PipelineJob.load` failed in four ways:

- On a loaded function node, `as_table()` and `iter_data()` raised "no function pod and no
  database attached", although the database was attached and `get_all_records()` read the rows.
  The cause: `FunctionJobNode.from_descriptor` set `LoadStatus.UNAVAILABLE`, and attaching the
  store never changed that status.
- A loaded node's `output_schema()` returned the serialized type strings (`'int64'`), and its
  `data_context` dereferenced the missing pod. Loading a job in which an operator (a `Join`)
  consumes a function node therefore crashed with `InputValidationError`.
- A loaded operator without a readable cache returned an empty table, as if the operator
  had produced nothing. Operators can store outputs when caching is enabled (`odb_v1`).
- Loading logged every source it could not rebuild, in-memory ones included, as "Could not
  reconstruct source … — skipping", giving neither the consequence nor a remedy.

**Fix:**

- Attaching a database makes a loaded function node `READ_ONLY`. `iter_data()` yields the stored
  rows: those whose `_node_content_hash` is the content hash saved in
  `run.node_job_content_hashes`, the same set `get_all_records()` returns. `as_table()` is an
  `ArrowTableStream` over the stored Arrow records, so stored types are kept and `all_info`
  includes system tags, source info and the data context.
- Without a database, reading names `PipelineJob.load(path, database=...)` and `job.bind(database=...)`.
- `save()` records `run.node_has_stored_records`: whether the database held at least one pipeline
  record carrying each function node's content hash. This is an additive, optional field, and
  the format version is unchanged. A node saved with records raises, naming the database and the
  table, when the attached database holds none of them, instead of reading zero rows. Records
  that other runs wrote to a shared table do not count. Files saved without the field read
  zero rows, as before.
- A loaded node's `output_schema()` returns Python types (`deserialize_output_schema`), and its
  data context comes from the descriptor.
- `parse_arrow_type_string` reads the parameterized types it used to reject: `timestamp[us, tz=UTC]`,
  `duration`, `time32`, `time64`, `date64` and `decimal128`/`decimal256`. Commas inside `[...]` and
  `(...)` no longer split struct fields.
- Logical schema types are saved with the full identifier defined by `arrow_type_identifier`:
  `extension<name;hex-metadata>`, recursively inside containers. The reader resolves the
  registered type and applies `require_readable`. Path, UUID, File, Directory, ndarray,
  registered nominal classes and structural classes reconstruct normally. A missing
  registration is reported by node and column, leaving affected operators as stubs.
  Incompatible identifiers raise `LogicalTypeIdentifierError`; legacy name-only text raises
  an earlier-version error. This narrow saved-file change was approved November 12, 2026.
- `get_all_records()` on a loaded node raises the same database errors as `as_table()`.
- Re-saving a loaded job keeps each node's saved expectation of stored records.
- One row per base entry, for `as_table()` and `iter_data()` alike. When an entry has several
  records (it was recomputed), the record with the greatest recomputation index wins. This
  also changes a live node's hot-load from the database: it used to keep whichever record the
  join returned last.
- A loaded node whose results lived in an ephemeral database yields `EmptyData` placeholders from
  `iter_data()`. Its `as_table()` raises an error saying the ephemeral database is not saved.
  A newer missing ephemeral recomputation takes precedence over an older persistent row.
- A loaded operator node with nothing stored raises, names its input nodes, and says to bind an
  unsaved input source first.
- Stored-record expectations constrain both the node content hash and the saved data context.
  Rows of another data context cannot satisfy the expectation while reads omit those rows.
- Loading logs in-memory sources at INFO, and any other source or database failure at WARNING
  with the exception. Each message states the consequence and the remedy.
- `PipelineJob.run()` rejects every loaded job, including an operator-only one whose
  operators were rebuilt: a loaded job is read-only by design (developer, November 11,
  2026).
- Direct operator `run`, `execute`, and `async_execute` also reject loaded nodes; async
  rejection closes the output channel.

Regressions: `tests/test_pipeline/loaded_job/`, which saves in a separate process and loads
in the test process, for each source kind.

Open, for a follow-up:
- The saved type strings drop nullability (`float | None` is saved as `double`), so a loaded
  node reports the non-null type.
- Map type text also omits custom item field names and item nullability; a non-null map
  item reloads as nullable. Preserving these fields needs additional schema syntax and is
  deferred to the pipeline save/load overhaul, ITL-977 (approved November 12, 2026).
- Unknown classes still require registration in the reader process.

The hash-step review also found that an operator built directly from a saved blueprint
descriptor cannot locate its stored rows: the descriptor lacks `pipeline_path` and carries
the definition content hash rather than the run's hash. The combined hash-step regression
still injects those values from the live node. The developer deferred this change to ITL-977;
the recommended approach is additive runtime descriptors in `run.node_descriptors`, preserving
blueprint identities and edges. The separate full logical-identifier gap is resolved here.

Review follow-ups: readable persistent and ephemeral records now both reach the
latest-index selection (persistent wins equal indices). Loaded REPLAY operators raise
when their cache table is absent; loaded function `run()` rejects computation, and
loaded jobs report `is_runnable()` as false even after binding sources. Source-schema
restoration uses the saved context converter in both normal and fallback branches.

**Superseded in part (ITL-1043, developer decision, November 12, 2026):** the database is the
source of truth, with one rule for function and operator nodes. `run.node_has_stored_records`
is no longer written, and is ignored when an older file has it: a loaded function node no
longer raises on a database without its run's records; it returns what the database holds. The job
file's optional, advisory `run.expected_rows` makes a difference one `OrcapodDatabaseContentWarning`
per node. Likewise, a loaded REPLAY operator whose job file records its run's content hash
reads an absent cache table as no rows instead of raising. See
`docs/dev/design_issues/ITL-1043-LJ1.md`.
