# ITL-991: `head()` and a notebook preview that never computes

Status: approved (developer, November 12, 2026: "agreed with all 3 recs"; D3 and D5 approved by the coordinator).
Implementation notes at the end record where the code differs from the text below.
Issue: ITL-991. Related: ITL-186 / PR #333 (Spiral source), ITL-990 (workshop notebook).

## Rulings this design implements (developer, November 12, 2026)

1. "Stream's preview should only show a short preview of what it can readily access but
   without causing materialization unnecessary and definitely not trigger new computation."
2. "For nodes, they are associated with their storage and preview on nodes should be just a
   head preview of the storage content WITHOUT triggering computation."
3. "Yes bounded 10 row read from Spiral should be considered readily accessible."
4. "Yes they should get explicit head style method and preview should just be triggering
   this. For stream that's downstream of computation, head should NOT trigger the
   computation but rather show the content of cached stream contents if present."

## Today (verified on main 25650c42)

`StreamBase._repr_html_` and `StreamBase.view()` (`core/streams/base.py:443-468`) call
`as_polars_df()` on the whole stream. So displaying:

- a `FunctionPodStream` calls the user's function on every row;
- a `DynamicPodStream` (any operator result) runs `static_process` on the whole input;
- a `SideEffectPodStream` or `SideEffectNode` **delivers the side effects** (sends the
  emails, writes the files) for every row;
- a `SpiralDBTableSource` after #333, a `PollingSource` (polls), a `CachedSource` (reads its
  source and **writes** the cache table), a `DerivedSource` (reads the whole origin store);
- a blueprint `FunctionNode` / `OperatorNode` / unbound `SourceJobNode` raises inside the
  formatter.

`__repr__` never computes anywhere (checked: `TraceableBase`, `ArrowTableStream`,
`DynamicPodStream`, the nodes). `SourceNodeBase` is not a `StreamBase`, so source nodes have
no preview at all. No test or doc uses `view()`, `StreamView` or `_repr_html_`.

## a. The method

```python
def head(
    self,
    n: int = 10,
    *,
    columns: ColumnConfig | dict[str, Any] | None = None,
    all_info: bool = False,
) -> StreamHead: ...
```

- On `StreamBase` (every stream, source and stream-like node) and on `SourceNodeBase`.
- `columns` / `all_info` mean exactly what they mean for `as_table()`. `sort_by_tags=True`
  raises `ValueError` ("head returns the first rows in stream order; sort as_table()
  instead"): sorting needs every row, so it cannot be a bounded read.
- `n` is a non-negative `int`; anything else raises `ValueError`/`TypeError`.
- **Never computes**: never calls a user function, never runs an operator's
  `process`/`static_process`, never delivers a side effect, never polls, never writes to a
  database, never keeps what it read on the object (no hot-loading into in-memory caches).
- The base implementation is the safe default: no rows, the declared schema, status
  `NOT_COMPUTED`. A stream class serves rows only by overriding `head`, so a new stream class
  can never compute by accident.
- `_repr_html_` is `self.head()._repr_html_()` and nothing else: one code path. It catches an
  exception from `head` and renders the schema with "could not read: <error>" instead of
  letting the notebook formatter print a traceback.

**Return type: `StreamHead`** (recommended), a small frozen result object in
`orcapod.core.streams` (exported from `orcapod`):

| Member | Meaning |
|---|---|
| `table: pa.Table` | at most `n` rows, exact Arrow types (logical types, widths, nullability as the stream has them) |
| `status: HeadStatus` | where the rows came from, or why there are none (below) |
| `total_rows: int \| None` | the stream's row count when known without computing |
| `total_is_estimate: bool` | `True` for a recorded estimate (Spiral manifests) |
| `tag_columns: tuple[str, ...]` | which columns of `table` are tags |
| `label`, `kind` | the stream's label and class name, for the header |
| `as_table()`, `as_polars_df()`, `__len__` | convenience, same names as the stream's |
| `_repr_html_()` | the preview (section e) |

`HeadStatus` (a `str` enum): `ROWS` (the first rows of the stream's complete content),
`PARTIAL` (a computation stopped part way; these are the rows computed so far),
`NOT_COMPUTED`, `NOTHING_STORED` (a job node with no stored rows), `UNAVAILABLE` (no data can
exist here: a blueprint node, an unbound source slot, a `SourceProxy`).

Why not the alternatives:

- A plain `pa.Table` (matches `as_table`) cannot tell "empty result" from "not computed"
  (point d) without a second method, and `stream.head(50)` in a notebook would print
  pyarrow's text repr instead of the preview.
- A `pl.DataFrame` has the same two problems, and Polars is the convenience layer here
  (`as_polars_df` is built from `as_table`), not the source of truth for types.
- A small `ArrowTableStream` would be a new root stream with its own identity; feeding it to
  a pod silently builds a pipeline on 10 rows, and it cannot carry the status either.

With `StreamHead`, `stream.head(50)` renders the same preview with 50 rows, the data is one
attribute away (`.table`, or `.as_polars_df()`), and `.status` answers point d.

Header line (rendered by `StreamHead`, one wording table so #333 and every class agree):

| Status / case | Header |
|---|---|
| ROWS, total known, total > n | `ArrowTableSource[readings] — first 10 of 1,234 rows` |
| ROWS, total ≤ n | `ArrowTableSource[readings] — all 7 rows` |
| ROWS, estimate | `SpiralDBTableSource[spike_event] — first 10 rows of about 63,349` |
| ROWS, total unknown | `… — first 10 rows` |
| ROWS from a node's store | `FunctionJobNode[score] — first 10 of 1,234 stored rows` |
| ROWS from a stream's cache | `FunctionPodStream[score] — first 10 of 500 computed rows` |
| ROWS, zero rows | `DynamicPodStream[filter] — computed: no rows` |
| PARTIAL | `FunctionPodStream[score] — first 10 of 37 rows computed so far (computation incomplete)` |
| NOT_COMPUTED | `DynamicPodStream[join] — not computed; as_table(), iter_data() or a job run computes it` |
| NOT_COMPUTED (stale cache) | `… — not computed: inputs changed since the last computation` |
| NOTHING_STORED | `FunctionJobNode[score] — nothing stored; run the job` |
| UNAVAILABLE | `FunctionNode[score] — pipeline blueprint; run it in a PipelineJob` / `SourceJobNode[raw] — no source bound` / `SourceProxy[raw] — source not available` |
| side effects | adds `; displaying never delivers side effects` |

Below the header: a one-line schema summary (`tags: id: int, session: str · data: x: float,
frame: ndarray`), then the table (tag columns first). When there are no rows the table is
the column header row only, so the schema is always visible.

## b. Per kind of object

"Reads" is the I/O head performs; "knows" is how the code decides without computing.
Every row-serving case returns the first `n` rows of what `as_table()` would return when
`as_table()` would not compute (this is the invariant the tests check:
`head(n).table == as_table().slice(0, n)` in every `ROWS` state).

| Object | Readily available | head returns | Reads / cost | How it knows |
|---|---|---|---|---|
| `ArrowTableStream` | its table | ROWS, first n, exact total | slices of the held tables; source info / context / content hash built for n rows only | always holds its table |
| In-memory and file sources: `ArrowTableSource`, `DictSource`, `ListSource`, `DataFrameSource`, `CSVSource`, `DeltaTableSource`, `DBTableSource` / `SQLiteTableSource` / `PostgreSQLTableSource` (all read their table at construction today) | the held `_stream` | delegate to `_stream.head` (RootSource default) | none beyond the slice | `_stream` is built in `__init__` |
| `SpiralDBTableSource` on main (reads at construction) | held `_stream` | as above | none | as above |
| `SpiralDBTableSource` after #333 (streams) | a bounded read | ROWS, ≤ n rows read with `limit=min(n, limit)` and the source's `where`/`asof`; total = `row_count_estimate` (estimate) when no `where`, else unknown | one scan of at most n rows + a manifest count | ruling 3 |
| `PollingSource` | batches already committed | ROWS of committed rows (exact count) or NOT_COMPUTED "nothing polled yet"; **never polls** | none | `self._batches` non-empty. Without a declared schema and before a poll its `keys()` itself polls; head then shows "nothing polled yet; schema unknown" instead of calling `keys()` |
| `CachedSource` | the built merged stream if current; else its wrapped source's head | ROWS; **never writes** the cache table | slice, or the wrapped source's head | `_cached_stream is not None and not is_stale` |
| `DerivedSource` | its cached table/stream; else the origin node's stored rows | ROWS / NOTHING_STORED (origin not run) | v0.3: the origin's stored read (see "bounded node reads" below), not cached on the object | `_cached_table` / `_cached_stream`; the origin node's head |
| `SourceProxy` | nothing | UNAVAILABLE + declared schema | none | it has no delegate |
| `DynamicPodStream` (every operator result) | its cached result, if current | ROWS of the cached result, or NOT_COMPUTED (+ "inputs changed" when stale) | slice of `_cached_stream` | `_cached_stream is not None` and a side-effect-free freshness check (see finding F1) |
| `FunctionPodStream` (`FunctionPod`) | computed rows in `_cached_output_datas` / `_cached_output_table` | ROWS (iteration finished: `_cached_input_iterator is None` and cache non-empty), PARTIAL (iteration stopped: the contiguous prefix of computed indices, skipping filtered `None` outputs), or NOT_COMPUTED | converts ≤ n cached `(tag, data)` pairs to Arrow with the same typed conversion `as_table` uses (factored into one helper) | the cache dict and the iterator state; `is_stale` → NOT_COMPUTED "inputs changed" (head does not clear the cache) |
| `FunctionPodStream` of a `CachedFunctionPod` | same as above: the in-memory cache | same | same | same. Its result store is NOT consulted: looking a row up needs the input row's hash, i.e. reading and hashing the upstream. The note says "results may be in the result store; as_table() reads them without calling the function". |
| `SideEffectPodStream` | nothing (it keeps no rows) | NOT_COMPUTED, header adds "displaying never delivers side effects" | none | always |
| `FunctionNode`, `OperatorNode` (blueprints) | nothing | UNAVAILABLE "pipeline blueprint" + schema | none | the class |
| `SourceNode` (input slot) | nothing | UNAVAILABLE "input slot; bind a source" + declared schema | none | the class |
| `SourceJobNode` | the bound source | delegates to `bound_source.head(...)`; unbound → UNAVAILABLE "no source bound" | that source's head | `bound_source` |
| `OperatorJobNode` | its in-memory output; else its stored table | delegate to `_cached_output_stream.head` (which may itself be an un-run `DynamicPodStream` → NOT_COMPUTED); else stored rows (REPLAY) | stored read in REPLAY only, see below | `_cached_output_stream`; `_cache_mode`, `_pipeline_database` |
| `OperatorJobNode`, cache mode OFF | in-memory output only | ROWS after a run; else NOT_COMPUTED "uncached operator; nothing is stored" | none | `cache_mode == OFF` |
| `OperatorJobNode`, cache mode LOG | in memory; the stored table holds rows of earlier runs | in memory → ROWS; else NOTHING_STORED-style note "not run in this session; N rows stored by earlier runs (LOG mode, not replayed)" with no rows — **decision D3** | — | — |
| `FunctionJobNode` | `_cached_output_datas` after a run; else the stored rows (pipeline table ⋈ result table) | ROWS (in memory, or "stored rows"), NOTHING_STORED | in memory: ≤ n conversions. Stored: see "bounded node reads" | dict non-empty; `_fetch_joined_records` non-empty |
| `SideEffectNode` / `SideEffectJobNode` | nothing it can show without delivering | NOT_COMPUTED, "displaying never delivers side effects" | none | the class |
| Loaded job nodes (`PipelineJob.load`) | the store only | function: stored rows / NOTHING_STORED; operator: REPLAY stored rows, else UNAVAILABLE "not stored (cache mode OFF)"; source: bound source or `SourceProxy` UNAVAILABLE. Never raises the loaded-node errors | stored read | `_function_pod is None`, `_loaded` |

### Bounded node reads

`ArrowDatabaseProtocol` has no limited read: `get_all_records` reads the whole table, and a
node's rows are filtered from a shared table (`__data_context`, node content hash), and for
a function node joined to the result table and de-duplicated by recomputation index (the
newest record of a base entry may lie anywhere in the table).

- **v0.3 (recommended):** a node with nothing in memory reads its stored rows with its
  existing read helpers (`_read_stored_records`, `_fetch_joined_records` /
  `_stored_rows_stream`), returns the first n, and keeps nothing. No computation, exact
  total ("first 10 of 1,234 stored rows"), but the I/O is the node's whole stored slice.
  After `job.run()` in the same process (the workshop's path) the rows are in memory and
  nothing is read at all.
- **Follow-up (new issue):** `ArrowDatabaseProtocol.get_first_records(record_path, n, *,
  column_values=None, record_id_column=None)` implemented by every database class (Delta:
  `dataset.head(n, filter=...)` plus pending rows; in-memory: filter + slice; connector:
  `WHERE … LIMIT n`; no-op; extension-aware wrapper), used by operator and source caches
  directly and by function nodes for the pipeline rows plus `get_records_by_ids` for their
  results. The recomputation-index de-duplication makes the function-node case need care
  (read rows for the chosen base entries only). About one more day; public protocol change.

## c. Operators

Confirmed: head never runs an operator, even when its inputs are readily available, because
the operator of the heads is not the head of the operator:

- `GroupBy(by=["subject"])` over the first 10 rows of a source gives **wrong values**, not
  just fewer rows: a group's list holds only the members among those 10 rows.
- `Join` / `SemiJoin` of two heads can be empty while the full join is not (the matching
  keys are beyond row 10 of one side).
- `PolarsFilter` / `DropNulls` of a head can be empty while the full result is not.
- `MergeJoin` lists and `Batch(drop_partial_batch=True)` change content too.

So an operator stream shows its schema and NOT_COMPUTED until it is materialized, then its
rows. Which stream classes keep their materialized rows today:

| Class | Keeps rows | Where |
|---|---|---|
| `ArrowTableStream` | always | `_table` |
| `DynamicPodStream` | yes, after `as_table`/`iter_data`/`run` | `_cached_stream` (the `ArrowTableStream` from `static_process`) |
| `FunctionPodStream` | yes, incrementally during iteration; the table after `as_table` | `_cached_output_datas`, `_cached_output_table` |
| `SideEffectPodStream`, `SideEffectNode` | no | — |
| `CachedSource` | yes, after first use | `_cached_stream` |
| `DerivedSource` | yes (tags_as_data: only once the origin has rows) | `_cached_table` / `_cached_stream` |
| `PollingSource` | committed batches | `_batches` |
| `OperatorJobNode` | after `run`/`execute` (lazy `DynamicPodStream` after `run` with OFF and no store) | `_cached_output_stream` |
| `FunctionJobNode` | after `run`/`execute`/first `iter_data` | `_cached_output_datas` |

## d. Nothing available

`head` returns a zero-row table with the full schema and a status that is not `ROWS`:

- Classes that hold an Arrow schema (sources, `ArrowTableStream`, stored tables) slice it to
  zero rows: exact types.
- Uncomputed operator / function streams know only their Python schema (`output_schema()`
  exposes Python types). Their empty table is
  `arrow_utils.make_empty_table({**tag_schema, **data_schema}, type_converter)`: correct
  names, nullability and logical types, but widths are the converter's defaults (`int` →
  `int64`) and may differ from the eventual computed column (an `int8` input passed through
  an operator). This is documented on `head`; the status tells the caller the table is a
  schema, not a result.
- Empty result vs not computed: `status == ROWS and len(table) == 0` is an empty result;
  `NOT_COMPUTED` / `NOTHING_STORED` / `UNAVAILABLE` are not.

## e. Large cells (preview only)

`StreamHead._repr_html_` renders a display copy; `StreamHead.table` is never changed.

| Arrow type of the column | Shown as |
|---|---|
| `orcapod.ndarray` extension (and any extension whose storage is a struct with `dtype` and `shape`) | `ndarray float64 (2, 3)`, read from the `dtype`/`shape` fields without decoding `data` |
| other logical (extension) types | Polars' own rendering if the storage is a string or number (`Path` shows the path); otherwise `<extension name>` |
| `large_binary` / `binary` | `bytes (300 B)`, sizes humanised (`1.2 MB`) |
| `large_string` longer than 80 characters | first 77 characters + `…` (length in a tooltip) |
| `large_list` | `[1, 2, 3]` when ≤ 5 scalar items, else `list (len 12)`; nested lists `list (len 3) of list` |
| `struct` | Polars' rendering, with these rules applied to its fields |
| null | Polars' `null` |

Tag columns come first and are marked in the header row (a `tag` group label above them).
The renderer builds a Polars frame of display strings for the summarised columns and uses
Polars' HTML for the table, so the look stays what users see today.

## f. `view()` and `__repr__`

- **Remove `view()` and `StreamView`.** `view()` is today's "whole stream as a frame" and
  would either compute (forbidden by the ruling) or duplicate `head`. Nothing in the repo
  uses it; greenfield rule, no shim. `as_polars_df()` remains the way to get everything.
- **`__repr__`**: unchanged; it already never computes. `str(stream.head())` gives a text
  preview (header + Polars' text table) for terminal use.

## g. Tests (public interface; failing first)

New file `tests/test_core/streams/test_head_and_preview.py`, plus node cases in
`tests/test_core/nodes/`:

1. Function pod: a counting function. `stream.head()` and `stream._repr_html_()` before
   compute → count 0, `NOT_COMPUTED`, the schema's columns in the empty table. After three
   `next()` on `iter_data()` → `PARTIAL`, 3 rows, count still 3. After `as_table()` → `ROWS`,
   `head(n).table == as_table().slice(0, n)`, count unchanged by head. After the upstream is
   modified → `NOT_COMPUTED` (stale).
2. Same with a concurrent executor; and a `CachedFunctionPod` stream whose result store is
   full → still `NOT_COMPUTED` and the function is not called.
3. Operators (`Join`, `GroupBy`, `PolarsFilter`): `unittest.mock.patch.object(<Op>,
   "static_process", wraps=...)` as a spy (non-public, commented: the test is about it not
   running) → zero calls from `head`/`_repr_html_`; after `as_table()` → rows equal to the
   table's prefix; empty filter result → `ROWS` with 0 rows, not `NOT_COMPUTED`.
4. Side effects: a `SideEffectPodStream` and a job's side-effect node → displaying delivers
   nothing (counter 0).
5. Sources: every in-memory source → `ROWS`, exact total, prefix invariant; `PollingSource`
   → no poll (counter on the impl); `CachedSource` → no table written (store unchanged);
   `SourceProxy` → `UNAVAILABLE`. Spiral's bounded read is tested in #333 (fake pyspiral
   records the `limit` it was given) once the two meet.
6. Nodes, in-memory store and Delta store, after `job.run()` sync and async: function,
   operator (OFF, LOG, REPLAY) and source nodes → expected status and prefix rows; the
   function counter unchanged. Before a run → `NOTHING_STORED` / `NOT_COMPUTED`. Loaded
   job (`PipelineJob.load`) in a fresh object → stored rows, no raise; blueprint nodes →
   `UNAVAILABLE`. With the follow-up bounded read, a spy database asserts `get_first_records`
   is called with `n` and `get_all_records` is not.
7. Preview: header wording table above; ndarray / bytes / long string / list summaries;
   `head().table` still holds the full values; `head(n=-1)` and `sort_by_tags=True` raise.
8. Golden hash tests untouched and passing.

## h. What changes for users

- Displaying a stream, source or node in a notebook no longer computes: an operator result
  or function pod output that has not been computed shows its schema and "not computed"
  until `as_table()`, `iter_data()` or a job run computes it. Displaying a side-effect
  stream no longer delivers its side effects. Displaying a `CachedSource` no longer writes
  its cache.
- New `head(n=10)` on every stream, source and node, returning a `StreamHead`.
- `view()` and `StreamView` are removed.
- No hash, golden value, stored table, path or stored schema moves: head only reads, and no
  `identity_structure` changes.

Changelog fragment `changelog.d/ITL-991.md`:

```markdown
### Breaking changes and migration

- Displaying a stream, source or node in a notebook no longer computes anything. The preview
  shows the first 10 rows that are already available (held in memory, cached from an
  earlier computation, stored by a job run, or a bounded read of a source's table) and
  otherwise the schema with a note such as "not computed". Call `as_table()` or run the job
  to compute. Displaying a side-effect stream no longer delivers its side effects.
- `StreamBase.view()` and `StreamView` are removed: use `head(n)` or `as_polars_df()`.

### Added

- `head(n=10, columns=..., all_info=...)` on every stream, source and node returns a
  `StreamHead`: at most `n` rows as an Arrow table with their exact types, the row count
  when it is known without computing, and a `status` that tells an empty result from "not
  computed". It never calls a function, runs an operator or polls a source.
```

## i. Size, commits, schedule

About 600–800 lines of code and 400–600 of tests.

1. `test:` failing tests for streams and sources (g1–g5, g7).
2. `feat(streams):` `StreamHead`, `HeadStatus`, `StreamBase.head` default, renderer with cell
   summaries, `_repr_html_` → `head()`; remove `view`/`StreamView`.
3. `feat(sources):` `ArrowTableStream.head`, RootSource delegation, Polling / Cached /
   Derived / Proxy.
4. `feat(streams):` `DynamicPodStream.head`, `FunctionPodStream.head` (shared typed
   conversion helper), side-effect streams.
5. `test:` + `feat(nodes):` node heads (g6), `SourceNodeBase.head`.
6. `docs:` "Looking at data" section (preview, `head`, `as_table`, `as_polars_df`,
   `as_pandas_df`); changelog fragment; design-issue file.

Commits 1–6 with the v0.3 node read (full stored slice): Nov 13–14, PR and CodeRabbit by
Nov 15. The bounded database read (`get_first_records`) as a separate follow-up after the
workshop unless the developer wants it in v0.3 (then +1 day, still before Nov 16 but with
no slack).

## #333 interplay (whichever merges second makes the mechanical change)

#333 adds `StreamBase._preview(columns, all_info) -> (pl.DataFrame, note)` (default:
today's behaviour) and a Spiral override that reads ≤ 10 rows with Spiral's `limit` and
renders `SpiralDBTableSource[spike_event] — first 10 rows of about 63,349`.

- `StreamBase._preview` is removed; `head` is the override point.
- The Spiral override becomes:

  ```python
  def head(self, n=10, *, columns=None, all_info=False) -> StreamHead:
      config = head_column_config(columns, all_info)  # shared helper; rejects sort_by_tags
      limit = n if self._limit is None else min(n, self._limit)
      rows = <the override's existing bounded read, with limit=limit> -> pa.Table
      return StreamHead.of_rows(
          self, rows, config,
          total_rows=None if self._where else self.row_count_estimate,
          total_is_estimate=True,
      )
  ```

  i.e. the same read, returning the Arrow table instead of `pl.DataFrame(...)`, and the note
  string replaced by `total_rows` + `total_is_estimate`. The header wording above produces
  #333's exact text, so #333's rendering assertions keep passing.
- If #333 merges first: this PR deletes `_preview` from `StreamBase` and rewrites the Spiral
  override as above. If this PR merges first: #333 drops its `StreamBase._preview` and
  implements `head` as above. Suggest the coordinator forwards this section to #333's worker.

## Findings (out of scope unless decided otherwise)

- **F1 `DynamicPodStream` never invalidates its cache.** `_update_cache_status`
  (`core/operators/static_output_pod.py:383,392`) sets `self._cached_results = None` (a
  misspelling; the attribute is `_cached_stream`) and then moves `_cached_time` up to the
  upstream time, so after an upstream changes, `as_table()` returns the old result, and any
  read of `last_modified` makes it look fresh. head needs a correct freshness check.
  Proposal: fix it here (one line plus a regression test), since head depends on it.
- **F2** Displaying a `SideEffectPodStream` / `SideEffectNode` today delivers every side
  effect (fixed by this design).
- **F3** `PollingSource.keys()` polls when no schema was declared and nothing was polled.

## Decisions requested

- **D1** Return type `StreamHead` (recommended) vs `pa.Table` vs `pl.DataFrame`.
- **D2** Node stored reads for v0.3: whole stored slice, no computation (recommended), with
  bounded `get_first_records` as a follow-up; or bounded in v0.3 (+1 day).
- **D3** `OperatorJobNode` in LOG mode with nothing in memory: no rows, note that rows of
  earlier runs are stored but not replayed (recommended, matches `as_table`), or show them.
- **D4** Remove `view()` / `StreamView` (recommended) or keep `view()` as an alias of `head`.
- **D5** Fix F1 in this PR (recommended) or file it separately.

## Implementation notes (November 12, 2026)

- Tag columns come first; the line above the table names them (`tags: id, grp`). Polars'
  HTML shows each column's dtype, so there is no separate schema line.
- Struct cells are left to Polars' rendering (the per-field rules are not applied).
- A loaded `FunctionJobNode` whose attached store lacks the records its saved run stored (the
  wrong store) still raises from `head()`; the preview shows that error as "could not read".
  Every other loaded-node state returns a head without raising.
- `check_head_size` and `head_column_config` live in `core/streams/base.py`, and `StreamHead`
  / `HeadStatus` in `core/streams/head.py`, which imports nothing from `orcapod.types`: that
  keeps `StreamProtocol.head` free of a new import cycle.
- `head()` is part of `StreamProtocol`.
- `_CachedFunctionPodStream` (added on main by ITL-988) drops stored meta columns in head as
  in `as_table`.
- `StreamProtocol.head` returns `StreamHeadProtocol` (in `protocols/core_protocols/streams.py`),
  which `StreamHead` implements; that keeps `head.py` out of the protocol import chain.
  Delegating heads go through `head_of(head, stream)`.
- An array column is recognised by the array logical type's extension name
  (`numpy_type.NDARRAY_EXTENSION_NAME`, read at call time through `importlib` because that
  module sits above `head.py` in the import graph), never by sniffing bytes. Both stored
  forms are shown as `ndarray <dtype> <shape>`: the `{dtype, shape, data}` struct on main
  today, and one `.npy` file per cell after PR #328 (ITL-981), whose header alone is read
  (zero-copy buffer slice). Other binary cells show `bytes (N B)`.
- Long strings keep their first 56 and last 20 characters around an ellipsis, so ~140
  character hash keys stay distinguishable; Polars' own string shortening is raised to 80
  characters while rendering (coordinator input from the ITL-990 workshop rundown).
- A rowless head does not read its schema when that would poll a source upstream
  (`schema_requires_io`); it then has no columns and says so (CodeRabbit round 1).
- #333 merged first (main eeeb993e). Its `StreamBase._preview` / `_preview_header`, `view()` and
  `StreamView` are gone; `SpiralDBTableSource.head()` wraps its bounded `_head` read and uses
  `_head_note` (still the one place for its wording) as the header text, through
  `StreamHead.description`. An unlabelled Spiral source is shown by its table name. #333's
  display tests now go through `head()` / `header()`; their scan assertions are unchanged.
- CodeRabbit rounds 3-4: a `CachedSource` not yet used shows its columns and `NOT_COMPUTED`
  (its rows merge earlier cached rows with the wrapped source's, unknown until read); an
  operator node whose input changed shows `NOT_COMPUTED` ("inputs changed") rather than its
  kept rows; an async operator run with no rows keeps a typed empty result; `head_of` adapts
  any `StreamHeadProtocol` result; the preview shows all `n` rows (Polars `tbl_rows=-1`).
