# ITL-991 `head()` and a preview that never computes: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** every stream, source and node gets `head(n=10, *, columns, all_info) -> StreamHead`,
which never computes, and `_repr_html_` renders `self.head()`.

**Architecture:** one new module `core/streams/head.py` holds `HeadStatus`, `StreamHead`
(result + renderer) and the shared helpers. `StreamBase.head` is the safe default (schema
only, `NOT_COMPUTED`); each class that can serve rows without computing overrides `head`.
Spec: `superpowers/specs/2026-11-12-itl-991-stream-head-and-preview-design.md`.

**Tech stack:** PyArrow (the head table), Polars (HTML rendering only).

## Status of decisions (coordinator, November 12, 2026)

- D3 approved: OperatorJobNode in LOG mode with nothing in memory shows no rows + a note.
- D5 approved: fix F1 (DynamicPodStream `_cached_results` misspelling) in this PR, its own
  commit, with a regression test and its own `### Fixed` changelog bullet.
- D1 (StreamHead), D2 (whole stored slice for nodes; bounded read is a follow-up), D4
  (remove `view()`/`StreamView`) are with the developer. Work proceeds on the
  recommendations, each behind ONE seam:
  - D1 seam: `StreamBase.head` returns `StreamHead`; a `pa.Table` answer would mean
    `head()` returns `self._head_result(...).table` and `_repr_html_` calls `_head_result`.
  - D2 seam: every node's stored read goes through one method per node class
    (`_head_from_store`), so a bounded read replaces only that method.
  - D4 seam: the removal of `view`/`StreamView` is one commit.
- **Do not open the PR until the coordinator confirms D1, D2, D4.**
- Coordinator requirements: (1) a node header that reads its whole stored slice must not
  claim a bounded read, and a test shows the read performs no computation; (2) the workshop
  path (a source preview; a job node right after `job.run()` in the same process) is cheap:
  a slice of what is in memory.

## Files

- Create `src/orcapod/core/streams/head.py`: `HeadStatus`, `StreamHead`, `head_column_config`,
  cell summaries and HTML/text rendering.
- Modify `src/orcapod/core/streams/base.py`: `head` default, `_repr_html_`, remove
  `view`/`StreamView`, `_schema_only_table`.
- Modify `src/orcapod/core/streams/__init__.py`, `src/orcapod/__init__.py`: export
  `StreamHead`, `HeadStatus`.
- Modify `src/orcapod/core/streams/arrow_table_stream.py`: `head`.
- Modify `src/orcapod/core/sources/base.py` (RootSource delegation), `cached_source.py`,
  `derived_source.py`, `polling_source.py`, `source_proxy.py`.
- Modify `src/orcapod/core/operators/static_output_pod.py`: F1 fix; `DynamicPodStream.head`.
- Modify `src/orcapod/core/function_pod.py`: `FunctionPodStream.head` (+ factor the table
  build of `as_table` into helpers shared with head).
- Modify `src/orcapod/side_effects.py`: side-effect note.
- Modify `src/orcapod/core/nodes/{function_node,operator_node,source_node}.py`.
- Tests: `tests/test_core/streams/test_head_and_preview.py` (streams, sources, operators,
  function pods, rendering), `tests/test_core/streams/test_dynamic_pod_stream_staleness.py`
  (F1), `tests/test_pipeline/test_node_head.py` (job nodes, sync/async, in-memory/Delta,
  loaded job).
- Docs: `docs/concepts/` "Looking at data" section; `changelog.d/ITL-991.md`;
  `design_issues/ITL-991-PV1.md`.

## StreamHead contract (Task 1)

```python
class HeadStatus(StrEnum):
    ROWS = "rows"                  # the first rows of the stream's complete content
    PARTIAL = "partial"            # computation stopped part way; rows computed so far
    NOT_COMPUTED = "not_computed"
    NOTHING_STORED = "nothing_stored"
    UNAVAILABLE = "unavailable"

@dataclass(frozen=True)
class StreamHead:
    table: pa.Table
    status: HeadStatus
    kind: str                     # class name of the stream shown
    label: str
    tag_columns: tuple[str, ...]
    total_rows: int | None = None
    total_is_estimate: bool = False
    origin: str = "held"          # "held" | "computed" | "stored" | "read"
    detail: str | None = None     # extra note ("inputs changed ...", side effects)
    def as_table(self) -> pa.Table
    def as_polars_df(self) -> pl.DataFrame
    def __len__(self) -> int
    def header(self) -> str       # the wording table of the spec
    def for_stream(self, stream) -> StreamHead   # same rows, shown as `stream`
    def _repr_html_(self) -> str
    def __str__(self) -> str
```

Header wording (exact; #333 depends on the estimate line):

| case | text after `Kind[label] — ` |
|---|---|
| ROWS, rows == total | `all {k} rows` (`all 1 row`) |
| ROWS, total known | `first {k} of {total:,} rows` / `... stored rows` / `... computed rows` |
| ROWS, estimate | `first {k} rows of about {total:,}` |
| ROWS, total unknown | `first {k} rows` |
| ROWS, 0 rows | `no rows` (`computed: no rows` for origin computed) |
| PARTIAL | `first {k} of {total:,} rows computed so far (computation incomplete)` |
| NOT_COMPUTED | `not computed; as_table(), iter_data() or a job run computes it` |
| NOTHING_STORED | `nothing stored; run the job` |
| UNAVAILABLE | detail only |
| detail present | appended after `; ` (or the whole note for UNAVAILABLE / a NOT_COMPUTED with a reason) |

## Tasks

### Task 1: F1 regression test and fix (own commit)
- [ ] Test `tests/test_core/streams/test_dynamic_pod_stream_staleness.py`: an operator over a
  `FunctionPodStream`; `as_table()`; `fn_stream.clear_cache()`; `as_table()` again calls
  `static_process` twice (spy with `mock.patch.object(SelectDataColumns, "static_process",
  autospec=True, side_effect=original)`; comment: the test is about re-running it).
- [ ] Run: fails (1 call).
- [ ] Fix `static_output_pod.py:383,392`: `self._cached_results = None` → `self._cached_stream = None`.
- [ ] Run test + `tests/test_core/operators` → pass.
- [ ] Commit `fix(operators): an operator stream recomputes after an upstream changes (ITL-991)`.

### Task 2: failing tests for streams, sources, rendering
- [ ] Write `tests/test_core/streams/test_head_and_preview.py` (cases in spec g1–g5, g7).
- [ ] Run: fails (no `head`). Commit `test(streams): head() and the preview never compute (ITL-991)`.

### Task 3: `head.py`, `StreamBase.head`, `_repr_html_`, remove `view`/`StreamView`
- [ ] Implement `head.py` per the contract; `StreamBase.head` default
  (`NOT_COMPUTED`, `_schema_only_table(config)` = `make_empty_table` of
  `output_schema(columns=config)`; any exception → zero-column table).
- [ ] `_repr_html_`: `try: return self.head()._repr_html_() except Exception as e:` header +
  `could not read: {e}`.
- [ ] Remove `view`, `StreamView` (D4 seam: separate commit).

### Task 4: ArrowTableStream and sources
- [ ] `ArrowTableStream.head`: `as_table(columns=config without content_hash).slice(0, n)`;
  content hashes for `islice(iter_data(), n)` only; total = `num_rows`.
- [ ] `RootSource.head`: `self._stream.head(...).for_stream(self)`.
- [ ] CachedSource: current `_cached_stream` → its head; else `_source.head` with detail
  `rows of the wrapped source; the cache is not read until first use`; never `_ensure_stream`.
- [ ] DerivedSource: factor `_peek_stream()` (no assignment) out of `_get_stream`; head uses
  the cached stream/table if any, else `_peek_stream()`; origin `stored`; no origin rows →
  NOTHING_STORED.
- [ ] PollingSource: `_batches` → combined stream head; else NOT_COMPUTED `nothing polled yet`
  with the declared schema, or a zero-column table when none (never `keys()`).
- [ ] SourceProxy: UNAVAILABLE `source not available; bind a live source`.

### Task 5: operator and function streams
- [ ] `DynamicPodStream.head`: `had = self._cached_stream is not None`;
  `self._update_cache_status()`; cached → `self._cached_stream.head(...)` with origin
  `computed`; `had` and now gone → NOT_COMPUTED `inputs changed since the last computation`.
- [ ] `FunctionPodStream`: factor `_table_from_pairs(pairs)` and `_shape_table(table, config,
  hashes)` out of `as_table`; head: stale with cache → NOT_COMPUTED "inputs changed";
  `_cached_output_table` → shape + slice; else contiguous prefix of `_cached_output_datas`
  (skip `None` outputs) → ROWS (iteration finished) or PARTIAL; empty → NOT_COMPUTED.
- [ ] Side-effect streams/nodes: base default with detail `displaying never delivers side effects`.

### Task 6: nodes (`tests/test_pipeline/test_node_head.py` first)
- [ ] Blueprints `FunctionNode`, `OperatorNode`: UNAVAILABLE `pipeline blueprint; run it in a PipelineJob`.
- [ ] `SourceNodeBase.head` + `_repr_html_`; `SourceNode` UNAVAILABLE `input slot; bind a
  source`; `SourceJobNode` → bound source head, else UNAVAILABLE `no source bound`.
- [ ] `OperatorJobNode.head`: in-memory → inner head; REPLAY + store → `_head_from_store`
  (`_read_stored_records`, internal columns dropped, ArrowTableStream head, origin stored);
  none → NOTHING_STORED; LOG → NOT_COMPUTED `not run in this session; rows of earlier runs
  are stored but not replayed (cache mode LOG)`; OFF → NOT_COMPUTED `not run; cache mode OFF
  stores nothing`. Never raises the loaded-node errors.
- [ ] `FunctionJobNode.head`: UNAVAILABLE load status → UNAVAILABLE; `_cached_output_table`
  → slice; `_cached_output_datas` → first n via the node's conversion; else
  `_head_from_store` (loaded: `_stored_rows_stream(_fetch_joined_records())[1].head`;
  live: `_load_cached_entries()` values, not kept) → ROWS origin stored; empty → NOTHING_STORED.
- [ ] Tests: sync and async orchestrators × in-memory and Delta; function counter unchanged
  by head; before run NOTHING_STORED; loaded job; OFF/LOG/REPLAY operator nodes.

### Task 7: docs, changelog, design issue, full suite
- [ ] "Looking at data" section in `docs/concepts/streams.md` (or the closest page), executed
  samples with `# ...` output comments.
- [ ] `changelog.d/ITL-991.md` (spec h text + F1 `### Fixed` bullet);
  `uv run python scripts/assemble_changelog.py --check`.
- [ ] `design_issues/ITL-991-PV1.md` (resolved); `uv run python scripts/design_issues.py --check`.
- [ ] ruff check/format, basedpyright (exit code), `uv run pytest tests/`.
- [ ] Merge origin/main, rerun, push; wait for D1/D2/D4 confirmation before the PR.

## Progress notes (for a successor)

- 2026-11-12: spec 70639010 pushed; design ask answered (D3, D5 approved; D1/D2/D4 pending
  with developer). Plan saved.
- 2026-11-12 (later): D1, D2, D4 approved by the developer. Tasks 1-7 implemented and
  pushed: F1 fix, StreamHead, sources, operator/function streams, cached pod stream, nodes,
  docs ("Looking at data" in docs/concepts/streams.md), changelog fragment, design issue
  ITL-991-PV1. Next: full suite, merge main, PR, CodeRabbit.

## Merge of origin/main 859106f7 (#335 ITL-928, #328 ITL-981, #336) — November 12, 2026

Resolution agreed in advance by the #334 and #335 workers (coordinator brief):

- [x] `git merge origin/main` (merge commit, no rebase). Conflict expected in
      `core/nodes/function_node.py`; check `core/function_pod.py` for a duplicated
      `_table_from_pairs` left by the auto-merge.
- [x] Keep #334's structure: `head`, `_shape_table`, `_computed_prefix`, `as_table` =
      `_table_from_pairs(list(self.iter_data()))` then `_shape_table`.
- [x] Take main's (#335) `_table_from_pairs` bodies as the non-empty build in both classes
      (per-row `tag.as_table` / `data.as_table` concatenation, `_concat_row_tables`,
      mixed Arrow types raise `InputValidationError`).
- [x] Empty pairs: `FunctionPodStream._table_from_pairs` keeps #334's declared-columns branch
      (`make_empty_table`); `FunctionNodeBase._table_from_pairs` keeps main's empty branch.
- [x] (not needed: the resolution was mechanical) If the resolution is not mechanical, ask the coordinator before pushing.
- [x] Tests: #335's (tag types kept on both stores, mixed rows error, join after function
      node), #334's (no computation on display, prefix invariant in every ROWS state),
      #328's array tests + ndarray `.npy` preview summary; full suite on 3.12, 3.11, 3.13;
      ruff, format, basedpyright; golden unchanged.
- [ ] One push; CodeRabbit reviews the new head; answer every thread; worker_done with sha.

Result: function_pod.py auto-merged into two `_table_from_pairs`; the #334 one was removed
and its empty branch moved into main's (the now-dead "no tag tables" fallback dropped).
function_node.py: #334's `_table_from_pairs` removed, main's kept in place. New test
`test_head_keeps_the_tag_arrow_type_of_the_input` (int8 tag, both orchestrators and stores).
Suite 10712 passed on 3.11, 3.12, 3.13; ruff, format, basedpyright exit 0; golden unchanged.

## CodeRabbit round on 19ad76ce (coordinator approved a 2nd push: F3, F4 only)

- [x] F3: rowless head downstream of an unpolled declared-schema `PollingSource` fetched
      when `source`/`meta`/`context`/`content_hash`/`all_info` was asked for. The private
      schema-IO check takes the head's `ColumnConfig`; regression test first (zero polls and
      fetches, `FunctionPodStream` and `SideEffectPodStream`).
- [x] F4: docstring on the nested `build` helper in `test_node_head.py`.
- [ ] Decline F1 (ITL-1002), F2 (follow-up), linked-issues check (D2) in the threads.

Also merged origin/main #337, #338 and #286 (no conflicts; golden and contexts identical to
main). Suite 11029 passed on 3.11, 3.12, 3.13; basedpyright exit 0.
