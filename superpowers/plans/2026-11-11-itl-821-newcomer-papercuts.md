# ITL-821 Newcomer Papercuts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the papercuts the docs audit found (ITL-821) that a v0.3 workshop attendee trips over, each with a
failing regression test first, and make the `examples/` directory run in the required test suite.

**Architecture:** Small, contained fixes in the files that cause each papercut. Items 1, 2, 3 and 5 are
pre-approved. Items 4 and 6 wait for the developer's decision (routed through the coordinator). The examples
run as a pytest that executes each `examples/*.py` in a subprocess with a temporary working directory. That
test is part of `uv run pytest tests/`, so no CI workflow changes.

**Tech Stack:** Python 3.11+, pytest, uv.

**Branch:** `eywalker/itl-821-newcomer-papercuts-found-by-the-docs-audit` (based on origin/main `b197544e`).

---

## Step 1 re-check against current main (`b197544e`, v0.2.0 + #291)

| # | Item | Still reproduces? | Minimal reproduction | Proposed fix | Hash / stored identity |
|---|------|-------------------|----------------------|--------------|------------------------|
| 1 | `op.enable_file_hash_caching` not exported | **Yes** | `import orcapod as op; hasattr(op, "enable_file_hash_caching")` → `False` | Re-export from `orcapod/__init__.py` (+ `__all__`) | None |
| 2 | `DictSource(data=[])` misleading error | **Yes** | `DictSource(data=[], tag_columns=["a"])` → `ValueError: tag_columns not found in table: {'a'}. Available columns: []` | In `SourceStreamBuilder.build` raise `ValueError("Table is empty: ...")` when the table has no columns, before the tag check. Zero-row tables with columns (e.g. `DictSource(data=[], data_schema=...)`) keep working | None (error path only) |
| 3 | Stray line + "data data" | **Yes** | pod `(weight, height)` on stream with extra `age` → stderr `Key 'age' not found in parameter types.` (logging lastResort handler, `schema_utils.check_schema_compatibility` uses `logger.warning`) then `ValueError: Incoming data data type ...` | Add `schema_utils.schema_incompatibility(incoming, receiving) -> str \| None` returning the reason; `check_schema_compatibility` delegates to it and no longer logs. `FunctionPod._validate_input_schema` and `FunctionNode.__init__` raise `Incoming data schema ... is not compatible with expected input schema ...: <reason>` | None (error path only) |
| 4a | `@function_pod` version `"v0.0"` vs `FunctionPod.from_fn` / `side_effect_function_pod` `"v1.0"` | **Yes** | `function_pod(output_keys="r")(f).pod.content_hash()` ≠ `FunctionPod.from_fn(f, output_keys="r").content_hash()` | **Decision needed** | Major version is in `DataFunctionBase.uri` → identity, pipeline hash and result DB path (`.../v0/...`). See decision section |
| 4b | `MapData`/`MapTags` keep unmapped vs `stream.map_data`/`map_tags` drop | **Yes** (documented as a warning in `docs/concepts/operators.md:420`) | `MapData(name_map=...)` default `drop_unmapped=False`; `StreamBase.map_data(..., drop_unmapped=True)` | **Decision needed** | `drop_unmapped` is in `MapData`/`MapTags.identity_structure` → operator content/pipeline hashes, plus output columns |
| 5 | `examples/save_and_load_pipelines.py` fails | **Yes** | `AttributeError: 'DeltaTableDatabase' object has no attribute 'register_tracker'` (the example passes a database where `Pipeline.__init__` now takes `tracker_manager`) | Rewrite the example around `PipelineJob(name, store=DeltaTableDatabase(...))`, `job.run()`, `job.save(path)`, `PipelineJob.load(path)` | None |
| 6a | protobuf gencode `UserWarning` on import | **No** (not reproducible) | `uv run python -W always -c "import orcapod"` and importing every `orcapod.*` module, with and without `--all-extras` (ray, pyspiral): no warnings, `google.protobuf` not loaded. protobuf 6.31.1 in `uv.lock` | Close with reason (likely an older lock/env) | — |
| 6b | deltalake `WARN deltalake_core::writer::stats Skipping column ... because it's a binary field` | **Yes** (11 lines for one tiny job run) | `PipelineJob(..., store=DeltaTableDatabase(dir)).run()` | **Decision needed** | Depends on option |

`examples/` in CI: **not run anywhere** (no workflow or test references it). `async_vs_sync_pipeline.py`
runs in ~4.5 s and passes; `save_and_load_pipelines.py` fails.

## Decisions pending (asked to coordinator)

### Item 4a — `version` default
* **A (recommended): `from_fn` and `side_effect_function_pod` default to `"v0.0"`** (match the decorator,
  the docs and the golden suite). Golden tests unchanged (golden pods use `@function_pod` with the default).
  Hash move: only callers of `FunctionPod.from_fn` / `side_effect_function_pod` that omit `version`; their
  function pod content/pipeline hash, every downstream node hash and the result-store path (`v1` → `v0`)
  change, so their stored results would look missing (recompute). Both APIs are secondary: no docs/README use
  them; `side_effect_function_pod` is documented as superseded.
* **B: decorator defaults to `"v1.0"`.** Moves the golden `pipeline/pod` and `pipeline/join_pod` entries and
  every hash and stored result path of every decorator user who omits `version` (the documented path).
* **C: leave as is**, document the difference.

### Item 4b — `drop_unmapped` default
* **A (recommended): `stream.map_data`/`map_tags` default to `False`** (match the classes; renaming keeps
  the other columns, the non-destructive choice). Golden unchanged (golden pins `MapData(name_map=...)`, no
  fluent call). Hash and output-column change for fluent callers that omit the arg.
* **B: classes default to `True`.** Moves golden `pipeline/map_data`; hash + output change for class users.
* **C: make `drop_unmapped` a required keyword on both.** No hash moves for anyone who passes it; callers that
  omit it get a `TypeError` (API break; the golden `MapData` case and docs samples need the explicit `False`,
  same hash).
* **D: leave as is** (already documented as a warning).

### Item 6b — deltalake noise
* **A (recommended): in `orcapod.databases.delta_lake_databases`, before the first `deltalake` import,
  `os.environ.setdefault("RUST_LOG", "warn,deltalake_core::writer::stats=error")`.** Verified: deltalake's
  Rust `env_logger` reads `RUST_LOG` once when `deltalake` is first imported; setting it before that silences
  the lines, setting it after has no effect. Respects a user's own `RUST_LOG`. Caveat: mutates process env,
  and does nothing if the user imported `deltalake` before orcapod.
* **B: document** in troubleshooting.
* **C: table property** (`delta.dataSkippingStatsColumns`) — changes stored table metadata; not recommended.
* No dependency change in any option.

### Examples in CI
`tests/test_examples/test_examples.py`: parametrized over `examples/*.py`, runs each with
`subprocess.run([sys.executable, path], cwd=tmp_path, timeout=120)` and asserts exit 0. No workflow edits.

---

## File structure

- Modify `src/orcapod/__init__.py` — re-export `enable_file_hash_caching` (item 1). Keep the hunk next to the
  `.config` import and `"load_config"` in `__all__` to stay clear of PR #292's hunks.
- Modify `src/orcapod/core/sources/stream_builder.py` — zero-column check (item 2).
- Modify `src/orcapod/utils/schema_utils.py` — `schema_incompatibility` (item 3). PR #292 edits
  `check_schema_compatibility`; resolve on merge.
- Modify `src/orcapod/core/function_pod.py`, `src/orcapod/core/nodes/function_node.py` — message (item 3).
- Modify `docs/concepts/function-pods.md` — prose quoting the message (item 3).
- Modify `docs/concepts/sources.md` — mention "Table is empty" rule (item 2).
- Rewrite `examples/save_and_load_pipelines.py` (item 5).
- Create `tests/test_examples/__init__.py`, `tests/test_examples/test_examples.py`.
- Create `tests/test_core/test_newcomer_papercuts.py` — regression tests for items 1–3.
- Modify `CHANGELOG.md` (`[0.3.0] — unreleased`), `DESIGN_ISSUES.md` (one entry appended).

## Tasks

### Task 1: export `enable_file_hash_caching` (item 1)
- [ ] Test (`tests/test_core/test_newcomer_papercuts.py`):
  ```python
  def test_enable_file_hash_caching_is_exported():
      import orcapod as op
      from orcapod.contexts import enable_file_hash_caching
      assert op.enable_file_hash_caching is enable_file_hash_caching
      assert "enable_file_hash_caching" in op.__all__
  ```
- [ ] Run, expect FAIL (`AttributeError`).
- [ ] Add `from .contexts import enable_file_hash_caching` after the `.config` import; add to `__all__`.
- [ ] Run, expect PASS. Commit `fix(api): export enable_file_hash_caching at top level (ITL-821)`.

### Task 2: "Table is empty" for column-less input (item 2)
- [ ] Tests:
  ```python
  def test_dict_source_with_no_rows_and_no_schema_says_table_is_empty():
      with pytest.raises(ValueError, match="Table is empty"):
          DictSource(data=[], tag_columns=["a"])

  def test_dict_source_with_no_rows_and_a_schema_still_works():
      src = DictSource(data=[], tag_columns=["a"], data_schema={"a": int, "x": int})
      assert src.as_table().num_rows == 0
  ```
- [ ] Run, expect first FAIL.
- [ ] In `SourceStreamBuilder.build`, after dropping system columns:
  ```python
  if not table.column_names:
      raise ValueError(
          "Table is empty: the input has no columns, so no tag or data columns can be found. "
          "To build a source with zero rows, give it an explicit schema "
          "(for example data_schema= on DictSource)."
      )
  ```
- [ ] Run, PASS. Update `docs/concepts/sources.md` construction rules. Commit.

### Task 3: no stray log line, precise message (item 3)
- [ ] Tests (use `capsys` + `caplog` with a fresh logger state; the stray line comes from logging's
  lastResort handler, so assert nothing on stderr):
  ```python
  def test_extra_column_error_names_the_column_and_prints_nothing(capfd):
      @function_pod(output_keys="bmi")
      def bmi(weight: float, height: float) -> float: ...
      wide = DictSource(data=[{"id": 1, "weight": 70.0, "height": 1.8, "age": 30}], tag_columns=["id"])
      with pytest.raises(ValueError) as err:
          bmi.pod(wide)
      msg = str(err.value)
      assert "data data" not in msg
      assert "'age'" in msg
      out, errout = capfd.readouterr()
      assert out == "" and errout == ""
  ```
  plus the same through a `PipelineJob` (FunctionNode path) and a type-mismatch and a missing-key case.
- [ ] Run, FAIL.
- [ ] `schema_utils.schema_incompatibility` returns:
  `"column 'age' is not a parameter of the function"`,
  `"column 'x' has type int, the function expects str"`,
  `"required parameter 'height' has no matching column"`; `check_schema_compatibility` returns
  `schema_incompatibility(...) is None`. Callers raise
  `f"Incoming data schema {incoming} is not compatible with expected input schema {expected}: {reason}"`.
- [ ] Run, PASS; update `docs/concepts/function-pods.md` prose. Commit.

### Task 4: fix and test examples (item 5 + CI)
- [ ] `tests/test_examples/test_examples.py` (subprocess per example, cwd=tmp_path). Run, expect
  `save_and_load_pipelines.py` FAIL.
- [ ] Rewrite the example with `PipelineJob(name="my_pipeline", store=DeltaTableDatabase("./local_database"))`,
  `job.run()`, `job.save("./pipeline.json")`, `PipelineJob.load("./pipeline.json")`, printing results.
- [ ] Run, PASS. Commit.

### Task 5 / 6: items 4 and 6b per the developer's decision (pending).

### Task 7: CHANGELOG, DESIGN_ISSUES, full suite, ruff, basedpyright, merge main, PR.

## Progress notes
- 2026-11-11: Step 1 re-check done (table above). Asked coordinator about items 4 and 6.
- 2026-11-11: Coordinator: 1, 2, 3, 5 + examples test approved; protobuf half of 6 closed with reason;
  4a, 4b and the deltalake RUST_LOG change HELD for the developer — do not implement until the
  coordinator replies. Tasks 1–4 and 7 done; PR #296 opened ("Refs ITL-821"). Example note: a loaded
  `PipelineJob` is read-only; its nodes read stored rows only via `get_all_records()`.
- Out-of-scope findings (report in worker_done, not filed): `PipelineJob.load` logs
  "Could not reconstruct source ... skipping" for in-memory sources; a loaded node's `as_table()` raises
  "no function pod and no database attached" although the store is attached.
