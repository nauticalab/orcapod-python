# ITL-845 Per-row source info when materializing rows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every row that an operator, operator node or the sync orchestrator materializes into a
stream keeps its own `_source_*` provenance tokens instead of row 0's.

**Architecture:** Both materializers (`StaticOutputOperatorPod._materialize_to_stream` and
`SyncPipelineOrchestrator._materialize_as_stream`) already concatenate each row's
`data.as_table(columns={"source": True})`, so the per-row `_source_*` columns are in the table.
The bug is the extra `source_info=rows[0][1].source_info()` argument: `ArrowTableStream` →
`arrow_utils.prepare_prefixed_columns` lets a concrete mapping value override the table's column
and broadcasts it. The fix drops that argument so the columns pass through, and moves the shared
row-to-table step into one helper used by both materializers.

**Tech Stack:** Python 3.11–3.13, PyArrow, pytest, `uv run`.

---

## Reproduction (before the fix, `ab544982`)

Script: three rows with tokens `<src>::row_0::x`, `row_1`, `row_2` (hash prefix elided).

| Path | Output `_source_x` per row |
|---|---|
| async `PolarsFilter` | `row_0`, `row_0`, `row_0` |
| async `Batch(2)` (4 rows) | `[row_0, row_0]`, `[row_2, row_2]` |
| async `Batch(0)` | `[row_0 ×4]` |
| async `GroupBy(g)` | `[row_0, row_0]`, `[row_0, row_0]` |
| async `MergeJoin` | `[row_0, row_0]` ×3 |
| sync `Index(x, 0)` | `row_0::x[0]` ×3 |
| sync `Pick(scores, speed)` | `row_0::scores['speed']` ×3 |
| `OperatorJobNode.execute` (LOG) stored, `Select` | `row_0` ×3 |
| `OperatorJobNode.execute` (LOG) stored, `Batch(2)` | `[row_0,row_1]`, `[row_0,row_1]` (batch `['c']` gets 2 tokens) |
| `OperatorJobNode.async_execute` (LOG) stored | same broadcast |
| `SyncPipelineOrchestrator._materialize_as_stream` | `row_0` ×3 |

## Nullability and hash invariants

Before the fix, a broadcast concrete value made the `_source_*` field non-nullable; a `None`
row-0 value passed the column through (nullable). `Data.as_table` declares its source columns
nullable. To keep stored schemas and Arrow types identical, the helper sets each `_source_*`
field's nullability to "has nulls" — identical to the old flag whenever the old values were
correct, and the arrow hasher includes nullability in the digest.

Prototype (monkeypatched, scratchpad only) vs. `ab544982`, operator node in LOG mode:

| Case | stored schema | data columns | `_record_hash` unchanged |
|---|---|---|---|
| `execute` Select | same | same | 1 of 3 (row 0) |
| `execute` Batch(2) | same | same | 1 of 2 |
| `async_execute` Select | same | same | 1 of 3 |
| `async_execute` Batch(2) | same | same | 0 of 2 (row 0 was wrong too) |

Unchanged by design: `Data.content_hash` (data columns only), function-node `record_id`
(system tags + input data hash), every `pipeline_hash`, every data value and Arrow type.
Changed because the values were wrong: `OperatorJobNode` `_record_hash` (hashes the stored row
including `_source_*`) for every row whose tokens were wrong, and the `content_hash` of the
producer-less intermediate stream `_materialize_to_stream` returns (`as_table(all_info=True)`).
**Decision gate:** the `_record_hash` change is raised to the coordinator before implementing.

## File structure

- Modify: `src/orcapod/core/operators/static_output_pod.py` — new static helper
  `_rows_to_table(rows) -> tuple[pa.Table, tuple[str, ...]]`; `_materialize_to_stream` uses it
  and no longer passes `source_info`.
- Modify: `src/orcapod/pipeline/sync_orchestrator.py` — `_materialize_as_stream` non-empty path
  uses `StaticOutputOperatorPod._rows_to_table`, keeps `producer`/`upstreams`.
- Callers unchanged (`base.py`, `batch.py`, `index.py`, `pick.py`, `operator_node.py`): they all
  go through `_materialize_to_stream`.
- Create: `tests/test_core/operators/test_materialize_source_info.py`.
- Docs: `DESIGN_ISSUES.md` O3 resolved; `CHANGELOG.md` one `### Fixed` entry.

### Task 1: Failing regression tests

**Files:** Create `tests/test_core/operators/test_materialize_source_info.py`

- [ ] **Step 1:** Write tests. Helpers: `_source(n)` builds an `ArrowTableSource` with tags
  `id` (large_string), `g` (int64, `i % 2`) and data `x` (int64), all non-nullable;
  `_tokens(rows, col)` returns `[d.source_info()[col] for _, d in rows]`; `_run_async(op, *streams)`
  feeds each stream into a `Channel` and collects `op.async_execute` output. Expected tokens come
  from the input stream itself (`list(src.iter_data())`), so the test never hard-codes hashes.
  Tests:
  - `_materialize_to_stream` round-trip: scalar tokens, list tokens (rows from sync `Batch(2)`),
    single row, rows whose source info is `None` (`Data` built with `source_info={"x": None}` and
    mixed `None`/token rows), and that the `_source_*` Arrow fields keep the old nullability
    (non-null when no row is `None`).
  - async `PolarsFilter`, `SelectDataColumns` (UnaryOperator barrier): row `i` keeps token `i`.
  - async `Batch(2)`, `Batch(0)`: each batch's list equals its members' tokens in order.
  - async `GroupBy(by=["g"])`: each group's list equals the tokens of its members.
  - async `MergeJoin` of two sources with distinct tokens: equals the sync `MergeJoin` output.
  - general parity: for each operator above, async tokens == sync `op(stream)` tokens.
  - sync `Index` on a non-null `large_list<int64>` column: `"{token_i}[0]"` per row.
  - sync `Pick` on a `dict[str, int]` `DictSource`: `"{token_i}['speed']"` per row.
  - `OperatorJobNode` LOG mode, `execute` and `async_execute`, `Select` and `Batch(2)`: stored
    `_source_x` per row equals the input tokens (lists for Batch, one token per member).
  - `SyncPipelineOrchestrator._materialize_as_stream(buf, source_node)` keeps per-row tokens and
    the stream's `pipeline_hash`/`content_hash` still equal the node's.
  - Zero-row: `_materialize_to_stream([])` still raises `ValueError`; async `Batch` and
    `PolarsFilter` on an empty channel emit nothing; `_materialize_as_stream([], node)` builds
    an empty stream.
  - Pinned hash: for a fixed source, the `OperatorJobNode` `execute` stored row 0
    `_record_hash`, the data columns, every `pipeline_hash` and the output stream schema equal
    the values recorded on `ab544982`.
- [ ] **Step 2:** Run `uv run pytest tests/test_core/operators/test_materialize_source_info.py -v`;
  expect the per-row token tests to FAIL with row-0 tokens, zero-row/pinned tests to pass.
- [ ] **Step 3:** Commit `test(operators): pin per-row source info through materialization (ITL-845)`.

### Task 2: Fix `_materialize_to_stream`

**Files:** Modify `src/orcapod/core/operators/static_output_pod.py:194-232`

- [ ] **Step 1:** Replace the body:

```python
    @staticmethod
    def _rows_to_table(
        rows: Sequence[tuple[TagProtocol, DataProtocol]],
    ) -> "tuple[pa.Table, tuple[str, ...]]":
        """Concatenate (Tag, Data) pairs into one table, keeping per-row source info.

        Each row contributes its own system-tag and ``_source_*`` columns, so every
        row keeps its own provenance tokens. A ``_source_*`` field is declared
        non-nullable unless some row's token is ``None``.

        Args:
            rows: Non-empty sequence of (Tag, Data) pairs.

        Returns:
            The concatenated table and the user tag column names.

        Raises:
            ValueError: If ``rows`` is empty.
        """
        if not rows:
            raise ValueError("Cannot materialize an empty list of rows into a stream")
        tag_tables = [tag.as_table(columns={"system_tags": True}) for tag, _ in rows]
        data_tables = [data.as_table(columns={"source": True}) for _, data in rows]
        # Rows may list the same columns in a different order (cached vs
        # freshly computed), so match columns by name.
        combined_tags = arrow_utils.concat_tables_by_name(tag_tables)
        combined_data = arrow_utils.concat_tables_by_name(data_tables)
        fields = [
            field.with_nullable(combined_data.column(i).null_count > 0)
            if field.name.startswith(constants.SOURCE_PREFIX)
            else field
            for i, field in enumerate(combined_data.schema)
        ]
        combined_data = combined_data.cast(
            pa.schema(fields, metadata=combined_data.schema.metadata)
        )
        full_table = arrow_utils.hstack_tables(combined_tags, combined_data)
        return full_table, tuple(rows[0][0].keys())

    @staticmethod
    def _materialize_to_stream(rows):
        table, tag_keys = StaticOutputOperatorPod._rows_to_table(rows)
        return ArrowTableStream(table, tag_columns=tag_keys)
```

- [ ] **Step 2:** Run the new test file; scalar/list/node tests pass, orchestrator test still fails.

### Task 3: Fix the sync orchestrator

**Files:** Modify `src/orcapod/pipeline/sync_orchestrator.py:170-235`

- [ ] **Step 1:** Non-empty path: `full_table, user_tag_keys = StaticOutputOperatorPod._rows_to_table(buf)`;
  construct `ArrowTableStream(full_table, tag_columns=user_tag_keys, producer=..., upstreams=...)`
  without `source_info`. Update the docstring.
- [ ] **Step 2:** Run the new file — all pass. Commit `fix(operators): keep each row's source info when materializing rows into a stream (ITL-845)`.

### Task 4: Hash and stability evidence

- [ ] Run `uv run pytest tests/test_hashing tests/test_pipeline/test_system_tag_run_stability.py tests/test_channels tests/test_pipeline -q`.
- [ ] Rerun the scratchpad before/after comparison against the real fix (schema, data, record IDs).

### Task 5: Docs

- [ ] `DESIGN_ISSUES.md` O3: status `resolved`, `**Fix:**` note.
- [ ] `CHANGELOG.md`: one `### Fixed` entry under `[Unreleased]`, stating stored tables written by
  earlier versions keep row 0's tokens for later rows and are not repaired, and operator-node
  `_record_hash` values differ for those rows.
- [ ] Commit `docs: record ITL-845 fix`.

### Task 6: Verification and PR

- [ ] `uv run --with ruff ruff check .`, `ruff format --check .`, `uv run --with basedpyright basedpyright` (exit codes).
- [ ] `uv run --python 3.11|3.12|3.13 pytest tests/`.
- [ ] After ITL-837 merges: merge `origin/main`, remove the strict xfails this fix makes pass, rerun.
- [ ] Push, open PR (`Fixes ITL-845`), CodeRabbit rounds, CI green.
