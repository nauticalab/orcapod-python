# ITL-912: typed zero-row tables for operator nodes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An operator node's zero-row table (before a run, after `clear_cache()`, with no stored
table, after a zero-row run, and as an empty input on the sync orchestrator) has exactly the Arrow
types, nullability and system-tag columns the node's rows would have.

**Architecture:** (Coordinator-approved design (i), November 13, 2026.) One rule, the sync rule: a
node's zero-row table is its operator's result over zero-row inputs. One function,
`typed_zero_row_table(stream) -> pa.Table | None` in `core/streams/base.py` (next to
`schema_requires_io`, same getattr convention), asks a stream for `_typed_zero_row_table()`:
zero rows, system tags and source info, the Arrow types rows would have, or `None` when that cannot
be known without computing or reading rows. No state is stored on the node. Every empty path calls
it and keeps today's Python-schema table only when it returns `None`.

**Tech Stack:** PyArrow, pytest, uv.

## Who answers what (`_typed_zero_row_table`)

| Stream | Answer | Reads rows? |
|---|---|---|
| `StreamBase` (default: function pod streams/nodes, `DynamicPodStream`, ...) | `None` | no |
| `ArrowTableStream` | `_assemble_table(ColumnConfig(system_tags=True, source=True), limit=0)` | no (slice of the in-memory table) |
| `RootSource` with an `ArrowTableStream` `_stream` (ArrowTable, Dict, List, DataFrame, CSV, Delta, SQLite, Postgres: table read at construction; SpiralDB: zero-row schema template) | `_stream`'s answer | no |
| `RootSource` without `_stream` (Polling, Cached, Derived, SourceProxy) | `None` | no |
| `SourceJobNode` | bound source's answer, `None` when unbound | no |
| `OperatorJobNode` | held result's table sliced to 0 rows; else `operator.process(<inputs' zero-row tables>)` read with system tags + source; `None` if any input answers `None` or no live operator | no input row; operator runs on zero rows |

## Callers

1. `OperatorJobNode._make_empty_table` (as_table with nothing held, `_load_cached_stream_from_db`
   and `_replay_from_cache` with no stored table, async zero-row run on `main`).
2. `SyncPipelineOrchestrator._materialize_as_stream`, empty buffer.
3. After PR #369 merges: `StaticOutputOperatorPod._materialize_upstream`, empty-rows branch
   (#369 moves (2) there). Coordinator tells me when #369 merges.

## Fallback (unchanged table, documented as known limitation, ITL-985)

Function node upstream not run / zero rows; unbound input slot; polling/cached/derived source;
loaded node without stored table or live upstreams.

## Tasks

### Task 1: failing tests

**Files:** `tests/test_pipeline/test_operator_node_empty_table_types.py` (new),
`tests/test_pipeline/test_operator_node_async_readback.py` (remove xfail marks).

- [ ] Matrix test: source with tags `id int8 NN`, `grp large_string NN`; data `x int8 NN`, `p Path`,
  `l large_list<int8 not null> NN`. Pipelines: filter over source, select after a filter,
  batch after a filter. States: before run, after clear_cache, zero-row run (same pipeline, source
  with no matching row). Orchestrators sync/async. Assert `as_table(all_info=True).schema` equals
  the run-with-rows schema.
- [ ] No-computation proof: source subclass whose `iter_data`/`as_table` raise; operator node over
  it reads as typed zero rows before a run. Function pod upstream counts calls: zero calls, and the
  node keeps today's table (fallback) — assert the fallback table equals the Python-schema table.
- [ ] Remove xfail from `test_clear_cache_keeps_operator_node_arrow_types` and
  `test_operator_node_with_no_rows_keeps_columns_and_types_after_async_run` (the async zero-row
  case is fixed on main too, through rule (1)).
- [ ] Run, confirm they fail. Commit (WIP, tests only).

### Task 2: implement

**Files:** `src/orcapod/core/streams/base.py`, `src/orcapod/core/streams/arrow_table_stream.py`,
`src/orcapod/core/sources/base.py`, `src/orcapod/core/nodes/source_node.py`,
`src/orcapod/core/nodes/operator_node.py`, `src/orcapod/pipeline/sync_orchestrator.py`.

```python
# core/streams/base.py
def typed_zero_row_table(stream: object) -> "pa.Table | None":
    method = getattr(stream, "_typed_zero_row_table", None)
    return method() if callable(method) else None

class StreamBase:
    def _typed_zero_row_table(self) -> "pa.Table | None":
        return None
```

```python
# OperatorJobNode
def _typed_zero_row_table(self):
    if self._cached_output_stream is not None:
        return self._cached_output_stream.as_table(columns=_INPUT_COLUMNS).slice(0, 0)
    if self._operator is None:
        return None
    inputs = []
    for upstream in self._input_streams:
        table = typed_zero_row_table(upstream)
        if table is None:
            return None
        inputs.append(ArrowTableStream(table, tag_columns=upstream.keys()[0],
                                       producer=upstream.producer, upstreams=upstream.upstreams))
    return self._operator.process(*inputs).as_table(columns=_INPUT_COLUMNS)

def _make_empty_table(self):
    typed = self._typed_zero_row_table()
    if typed is not None:
        return typed
    ... today's code ...
```

- [ ] Implement; run the new tests and the readback tests.
- [ ] Commit.

### Task 3: docs, changelog, design issue, verification

- [ ] `as_table` docstring of `OperatorJobNode`: the zero-row table rule and the fallback cases.
- [ ] `changelog.d/ITL-912.md`: `### Fixed` + `### Known limitations` (ITL-985).
- [ ] `design_issues/ITL-912-<ID>.md` (resolved, with Fix note); `--check`.
- [ ] Targeted tests (pipeline, operators, nodes, golden) in one pytest process on 3.12; ruff;
  basedpyright exit code.
- [ ] Push, PR (`Fixes ITL-912`, #369 merges first), link, CodeRabbit loop.

## Notes for a successor

- #369 (ITL-1080) touches the same block of `OperatorJobNode.async_execute` and
  `_materialize_as_stream`; on merge, keep #369's taps and route its `_materialize_upstream`
  empty-rows branch through `typed_zero_row_table` before the Python-schema fallback.

## Follow-up (coordinator probe, head 45698b4c): non-large inputs

Finding: a source holding `string` tags/data gives `string` in a function or operator node's
zero-row table, `large_string` once rows exist. Fix: a consumer of an input's zero-row table
(`function_zero_row_table`, `_zero_row_input`, the pod stream's `_typed_zero_row_table`)
normalizes it with `arrow_utils.normalize_to_large_types(keep_null=True)` per field, as the
populated path does (a type-only change; no cast of values). A source's own table is not
touched. Steps: failing test (tests/test_core/streams/test_zero_row_large_types.py) -> fix ->
targeted tests -> one commit/push -> source report (main vs branch).
