# ITL-1122: large normalization applied unevenly — evaluation and plan

Status: DONE (PR #389). The rule below was REPLACED the same day by the developer: "let's go
ahead and always convert all non-large variants into large variants everywhere" (no exception for
a user's Arrow table or explicit schema). Implemented in one place, `ArrowTableStream.__init__`;
the per-source / per-operator fixes F1-F3 planned below were folded into it and removed. The
measurements below remain the record of what was uneven on `main` 3a7f3850.

## The rule (developer, November 14, 2026)

"any arrow table WE create as part of orcapod should always get things normalized to large
variant. However for things where 1) user directly creates arrow table with specific data
types and 2) some source actually make distinction (in recoverable fashion) of large and
non-large variants: for both of these we dont want to normalize the variants between large and
non-large."

Clauses used below: [O] Orcapod creates the table -> large; [U] the user created the Arrow
table -> kept; [S] the store keeps the distinction recoverably -> kept.

## Measurement

Scripts (scratchpad, re-runnable): `measure.py` (source kind x operator x plain / job sync /
job async, populated and zero-row) and `measure2.py` (function pod, DerivedSource,
PollingSource, Spiral fake, store round trips). Base `origin/main` 3a7f3850.

Columns measured: tag `t: string`, tag `k: int64`, data `s: string`, `b: binary`,
`l: list<string>`, `n: struct<x: list<string>, y: string>` (CSV: `t, k, s` only). Source-info
and system-tag columns are large everywhere (Orcapod builds them).

### Store round trips (write `string` and `large_string`, `binary` and `large_binary`, `list` and `large_list`; read back)

| Store | Result | Recoverable? |
|---|---|---|
| Delta Lake (deltalake directly) | Delta schema has one `string` / `binary` / `array` type for both; read back both as `string` (or both `large_string` with `as_large_types=True`, which Orcapod uses) | no |
| `DeltaTableDatabase` | all large | no |
| `InMemoryArrowDatabase` | each comes back as written | yes (in memory, not persistent) |
| SQLite | `TEXT` / `BLOB` for both; connector maps to `large_string` / `large_binary` | no |
| PostgreSQL | `text` / `bytea` for both; connector maps to large (by code; no server here) | no |
| SpiralDB (fake models observed ITL-40) | `large_string` written comes back `string`; connector normalizes to large | no |
| CSV file | no type in the file; `read_csv` gives `string` for both | no |

### Sources (the source's own table = the job's source node, sync and async)

| Source | Populated | Zero-row | Verdict |
|---|---|---|---|
| `ArrowTableSource` (user's table) | kept (`string`, ...) | kept | CONSISTENT [U] |
| `DictSource` Python rows / Python schema | large | large | CONSISTENT [O] |
| `DictSource(data_schema=pa.Schema)` with `string` | kept | kept | Q2 (CONSISTENT [U] if an explicit Arrow schema counts) |
| `ListSource` | large | n/a | CONSISTENT [O] |
| `CSVSource` (no schema) | `string` | ERROR (header-only CSV: null-typed columns; separate bug) | INCONSISTENT (CSV keeps no distinction) |
| `CSVSource(schema=pa.Schema)` with `string` | kept | kept | Q2 |
| `DataFrameSource` polars / pandas | large | large | CONSISTENT [O] |
| `DataFrameSource(pa.Table)` with `string` | large (through Polars) | large | Q3 |
| `DeltaTableSource` | large | large | CONSISTENT [O] (not recoverable) |
| `SQLiteTableSource` / `PostgreSQLTableSource` / `DBTableSource` | large | SQLite: "Table is empty" by design | CONSISTENT [O] |
| `SpiralDBTableSource` (fake) | large | - | CONSISTENT [O] |
| `CachedSource` over a `string` source, `InMemoryArrowDatabase` | kept `string` | large | INCONSISTENT (zero-row != populated) |
| `CachedSource` over a `string` source, `DeltaTableDatabase` | large | large | INCONSISTENT with the in-memory cache: type depends on the cache store |
| `DerivedSource` (function node, operator node, `tags_as_data`) | large | large | CONSISTENT [O] |
| `PollingSource` (fetch dict or polars) | large | - | CONSISTENT [O]; fetch returning a `pa.Table`: Q3 |
| Function pod output (incl. passed-through tags), plain and job | large | large | CONSISTENT [O] |

### Operators over a non-large input (`ArrowTableSource`, `DictSource(pa.Schema)`, `CSVSource`, in-memory `CachedSource`)

Inside a job (sync and async): every operator gives large. CONSISTENT [O].

On a plain stream (populated and zero-row identical):

* Already large: `PolarsFilter` with a predicate, `DropNulls` (any form), `MergeJoin`, `Batch`,
  `GroupBy` (member columns), `Index`.
* Pass non-large through: `SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`,
  `DropDataColumns`, `SelectColumns`, `DropColumns`, `MapTags`, `MapData`, `PolarsFilter()`
  (no-op), `Join` (every column), `SemiJoin`, `GroupBy`'s key tag (`GroupBy(by=["t"])`: `t`
  stays `string`).
* `Join` of a large input and a `string` input on shared tag `t` picks a side, and which side
  depends on the canonical input order (pipeline hash): `DictSource ⋈ ArrowTableSource` gives
  `large_string`, `DataFrameSource ⋈ ArrowTableSource` gives `string`.

Verdict: INCONSISTENT (given Q1 = large): an operator's output is a table Orcapod creates; the
same operator gives different types inside and outside a job, and operators disagree among
themselves on the plain path.

## Questions to the coordinator

* Q1 (open): a pass-through operator (select, map, no-op filter, join, semijoin) over a user's
  Arrow table: large, or the user's types? Recommendation: large.
* Q2: an explicit Arrow schema (`DictSource(data_schema=pa.Schema)`, `CSVSource(schema=)`,
  `DataFrameSource(schema=)`) with `string`: the user chose the Arrow types, so kept [U]?
  Recommendation: kept.
* Q3: `DataFrameSource` given a `pa.Table`, `PollingSource` whose fetch returns a `pa.Table`:
  both convert through Polars, so large. Recommendation: leave (Polars-frame sources;
  `ArrowTableSource` is the source that keeps a user's Arrow types).

## Proposed fixes (one place each)

* F1 operators: one helper applied to every operator's sync result in the three base
  `static_process` overrides (`UnaryOperator`, `BinaryOperator`, `NonZeroInputOperator`),
  beside `without_input_meta`: if any column of the result's table (with source, system-tag
  and context columns) differs from `normalize_to_large_types(type, keep_null=True)`, rebuild
  the `ArrowTableStream` with those columns re-laid out to the large type
  (`arrow_utils.restore_column_types`, as `filters.py` does). Width, nullability, field
  metadata and extension types are untouched (`normalize_to_large_types` keeps them). The
  async/job path is already large; tests pin it.
* F2 `CSVSource`: normalize the table `read_csv` gives when no `schema=` is passed.
* F3 `CachedSource`: normalize the table it presents in `_build_merged_stream` (read side only;
  stored rows unchanged), so in-memory == Delta and zero-row == populated.

What moves: no hash (large and non-large hash identically; golden suite and a direct test). No
stored schema: operator caches are written only inside jobs (already large); `CachedSource`
writes as before; Delta keeps no distinction. To be confirmed by a test that appends after the
change to a table written before it.

## Out of scope findings (report in worker_done)

* A header-only CSV (zero rows) cannot be read: columns are typed `null` and
  `infer_schema_nullable` marks them non-null -> `ValueError: A null type field may not be
  non-nullable`.

## Implementation steps (after the answers)

1. Failing tests: `tests/test_core/operators/test_large_normalization.py` (every operator,
   plain stream and job sync/async, populated == zero-row), CSVSource, CachedSource, hash
   equality.
2. F1, F2, F3.
3. Golden suite, full suite, stored-schema append check.
4. CLAUDE.md + `.zed/rules` "Arrow types", docs, `changelog.d/ITL-1122.md`,
   `design_issues/ITL-1122-LN1.md`.
