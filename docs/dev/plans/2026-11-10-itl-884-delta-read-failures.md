# ITL-884: Delta read failures are never reported as "no rows" — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Superseded in part (November 11, 2026):** the developer's review of PR #290 removed every
> parameter used only for an error message. `_read_delta_table` no longer takes `record_path`,
> `_require_compatible_append(table_uri, ...)` became `_incompatible_append_column(...)`,
> `_store_error_to_raise` takes only the errors, and `_single_store_error` no longer takes
> `topo_order`. See `2026-11-11-itl-884-no-message-only-parameters.md`.

**Goal:** A Delta table that exists but cannot be read raises `StoreReadError`, never an
empty result. A flush that would make an existing table unreadable, or silently cast the
stored column type, is refused before anything is written.

**Architecture:** Every read path in `DeltaTableDatabase` separates "table absent"
(`TableNotFoundError` → `None`, unchanged) from "table present but unreadable" (raise
`StoreReadError` naming the table URI, with the backend exception chained). `_flush_by_key`
creates a table only on `TableNotFoundError`. Before it merges into an existing table, it compares
the incoming batch's column types with the stored Delta schema. Both sides go through Delta's
own type mapping, with extension types stripped to storage, large-normalized and
nullability-blind. A difference raises `InputValidationError` naming the table, column and both
types. A store read failure fails the run whatever `error_policy` is (same shape as PR #287's
`StoreWriteError`).

**Tech Stack:** deltalake 1.0.2, pyarrow, pytest.

Approved by the coordinator (2026-11-10): contract as proposed. D1 adds a sibling
`StoreReadError(RuntimeError)`, and the append refusal raises `InputValidationError`. D2 is
option A: re-raise in the two per-row handlers and unwrap in the async orchestrator, with the
same shape as #287. D3 makes the comparison nullability-blind, plus a test that pins
nullability-only behaviour. D4 fixes the `DeltaProtocolError` → overwrite path.

No hash, path or stored-schema changes.

---

## File map

- `src/orcapod/errors.py` gets `StoreReadError` (`table`, `record_path`, `__str__` from
  `__cause__`, `in_group`).
- `src/orcapod/databases/delta_lake_databases.py` changes the read paths and
  `_flush_by_key`, and adds `_require_compatible_append` and the module helper
  `_comparable_storage_type`.
- `src/orcapod/core/nodes/function_node.py` (`execute` per-row handler) and
  `src/orcapod/core/function_pod.py` (async `process_one`) both get
  `except StoreReadError: ...; raise`.
- `src/orcapod/pipeline/async_orchestrator.py` catches the `TaskGroup`'s `ExceptionGroup`
  with a plain `except`, selects one `StoreReadError` with `_single_store_error`, and raises
  it on its own, noting the other failures on it.
- Tests: `tests/test_databases/test_delta_read_failures.py` and
  `tests/test_pipeline/test_store_read_failure.py`.
- Docs: `docs/concepts/function-pods.md` gets an admonition, `CHANGELOG.md` gets Fixed and
  Breaking entries, and `DESIGN_ISSUES.md` gets a new entry at the end.

## Task 1: Failing regression tests (database level)

`tests/test_databases/test_delta_read_failures.py`, public `DeltaTableDatabase` API only:

- `unreadable_table` fixture: write 2 rows, flush, delete the table's `*.parquet` files (the
  log stays intact). Use a fresh `DeltaTableDatabase` instance for the reads.
- `test_get_all_records_raises` checks `StoreReadError`: `exc.table` ends with the table
  directory, `str(exc)` contains the URI, and `__cause__` is set.
- `test_get_records_by_ids_raises`, `test_get_record_by_id_raises`,
  `test_get_records_with_column_value_raises`, and
  `test_add_records_skip_duplicates_raises` (existing-ids refresh). Each raises
  `StoreReadError`.
- `test_absent_table_reads_return_none` covers all four reads on a path never written:
  `None`, `table_exists` is False.
- `test_unloadable_log_raises_on_read_and_flush_never_overwrites`: the table's log names an
  unsupported reader feature (the load fails). Reads raise `StoreReadError`. `add_records(...,
  flush=True)` raises, the log files and parquet files are byte-identical afterwards, and no
  new log version exists.
- `test_protocol_error_on_load_is_not_treated_as_absent`: monkeypatch
  `deltalake.DeltaTable` to raise `DeltaProtocolError` for this table (the error deltalake
  1.0.2 does not raise naturally from a local table). Flush raises and the stored files are
  untouched. The comment explains the monkeypatch.
- Append refusal:
  - `test_castable_type_change_is_refused` (implemented as the `int64->float64` case of
    `test_plain_type_change`): int64 stored, float64 appended. `flush` raises
    `InputValidationError` itself (Task 2 of `2026-11-10-itl-884-coderabbit-flush-and-async-group.md`); assert it
    directly.
    The message names the column `x`, `int64` and `double`. A re-read gives the original row
    only, with type int64.
  - `test_narrowing_change_is_refused`: int64 stored, int32 appended.
  - `test_extension_storage_change_is_refused`: `ext<large_binary>` stored, `ext<large_list<float>>`
    appended. Refused, and the table is still readable afterwards (1 row).
  - Legitimate cases still pass: identical schema; `string` vs `large_string` (and
    `list<int64>` vs `large_list<int64>`); a new column; a missing column; an extension type
    with unchanged storage (2 rows read back).
  - `test_nullability_only_difference_is_accepted`: a nested non-null struct field vs a
    nullable one is accepted. This pins today's nullability-blind behaviour (ITL-842 audit).
    It also records what Delta stores for nullability after the merge, as an assertion on
    the read-back schema.

Run `uv run pytest tests/test_databases/test_delta_read_failures.py -v`. Expected: the read
tests FAIL (`None` / no raise / raw errors), and the refusal tests FAIL (the write is
accepted).

## Task 2: Failing regression tests (pipeline level, both orchestrators)

`tests/test_pipeline/test_store_read_failure.py`, parametrized over
`SyncPipelineOrchestrator()` and `AsyncPipelineOrchestrator()`:

- Run `source -> FunctionPod(double) label="dbl"` on a `DeltaTableDatabase`, then delete
  the parquet files of the `_result` table.
- `test_unreadable_result_table_fails_the_run`: a fresh store and job, `job.run(orchestrator=...)`
  raises `StoreReadError`, and the function is not called.
- `test_unreadable_result_table_fails_the_run_under_continue`: also delete the pipeline
  (tag) table, so that Phase 1 sees no tag rows and the per-row lookup is the read that fails.
  With `error_policy="continue"` it still raises `StoreReadError` and makes 0 calls.
- `test_absent_tables_still_compute`: control case. Delete both tables entirely, then the
  run computes all 3 rows.

Run the tests and expect FAIL.

## Task 3: `StoreReadError`

Add the class to `src/orcapod/errors.py`:

```python
class StoreReadError(RuntimeError):
    """Raised when a stored table exists but could not be read. ..."""

    def __init__(self, table: str, record_path: tuple[str, ...]) -> None:
        self.table = table
        self.record_path = tuple(record_path)
        super().__init__(table, self.record_path)

    def __str__(self) -> str:
        cause = self.__cause__
        reason = f": {type(cause).__name__}: {cause}" if cause is not None else ""
        return (
            f"Could not read the stored table at {self.table!r}{reason}. The table exists, "
            "so its rows are not treated as missing and the table was not overwritten."
        )

    @classmethod
    def in_group(cls, group: BaseExceptionGroup) -> "list[StoreReadError]": ...
```

## Task 4: Delta read paths

- `_get_delta_table`: on `TableNotFoundError` return `None`. On any other exception, drop
  the cache entry and `raise StoreReadError(table_uri, full_path) from exc`.
- Add the helper `_read_failed(record_path, exc) -> StoreReadError`, built from
  `_get_table_uri(record_path)` and `self._path_prefix + record_path`.
- `get_all_records`: `except Exception as exc: raise self._read_failed(...) from exc`
  (replaces the warning-and-skip).
- `get_records_by_ids`, `get_record_by_id`, `get_records_with_column_value`: narrow each
  `try` to the read itself, then `raise self._read_failed(...) from exc` (replaces
  `return None` / the raw re-raise).
- `_refresh_existing_ids_cache`: wrap the `to_pyarrow_table()` failure the same way.

## Task 5: Delta flush path

- In `_flush_by_key`, load only on `TableNotFoundError`, where `delta_table = None`. Any
  other exception does `raise StoreReadError(table_uri, abs_path_components) from exc`. The
  existing outer handler restores the pending batch.
- Before `merge`: `self._require_compatible_append(table_uri, delta_table, combined_table)`.
- `_comparable_storage_type(t)` is a module-level function. It strips extension types to
  storage at every depth, rebuilds struct/list/fixed-size-list/map with nullable children
  (comparison only, never stored), and recurses.
- `_require_compatible_append` builds the stored types from
  `pa.schema(delta_table.schema().to_arrow())`. The incoming types go through
  `deltalake.Schema.from_arrow(pa.schema([pa.field(name, _comparable_storage_type(type))]))`
  and `.to_arrow()`. Both sides are passed through `normalize_to_large_types(...,
  keep_null=True)` and `_comparable_storage_type`. Only columns present on both sides are
  compared. On a mismatch it raises `InputValidationError(f"Cannot append to the stored table
  {table_uri!r}: column {name!r} is stored as {stored} but the new rows have {incoming}. ...")`.

Run the Task 1 and Task 2 tests and expect PASS.

## Task 6: Run-level handling (same shape as #287)

- `FunctionNode.execute` per-row handler: put `except StoreReadError as exc:` before
  `except Exception`. It calls `ctx_obs.on_data_crash(...)` and `ctx_obs.on_node_end(...)`,
  then `raise`.
- `FunctionPod` async `process_one`: put `except StoreReadError as exc:` before
  `except Exception`. It calls `obs.on_data_crash(...)` and then `raise`.
- `AsyncPipelineOrchestrator._run_async`: add `except ExceptionGroup as group:` and pick
  the error with `_single_store_error(group, topo_order)` (the `StoreReadError` of the
  earliest node in topological order, the other failures noted on it); raise it on its own,
  or re-raise the group unchanged when it holds no `StoreReadError`. A leaf raised inside
  `except*` would be wrapped in a new group whenever another task failed concurrently.

Run the Task 2 tests and expect PASS.

## Task 7: Docs, changelog, design issues, full suite

- `docs/concepts/function-pods.md` gets a `!!! note "When the store cannot be read"`
  admonition after "Re-runs compute only what is new". It has no python block.
- `CHANGELOG.md`: a Fixed entry (ITL-884: read failures raise `StoreReadError`; an unloadable
  table is never overwritten). Breaking Changes: a castable column type change appended to an
  existing Delta table is now refused with `InputValidationError` instead of being silently
  cast to the stored type.
- `DESIGN_ISSUES.md`: one self-contained entry at the end with status resolved and a Fix note.
- Then run `uv run ruff format .`, `uv run ruff check .`,
  `uv run --with basedpyright basedpyright` (exit code 0) and `uv run pytest tests/`.
- Commit in Conventional Commits style: `test(databases): ...` for the failing tests first,
  then `fix(databases): ...`, then `docs: ...`.

## Resume notes (2026-11-11, after machine restart)

- Arrived to a clean worktree at d32c84d1, in sync with origin; PR #290 had 0 open threads
  but conflicted with main after #291 (ITL-847).
- Merged origin/main (b197544e) as b4387c00: only DESIGN_ISSUES.md conflicted; kept SJ1 (main)
  then DR1 (this PR) at the end, additions-only against both sides. CHANGELOG auto-merged
  (#291 did not touch it). Pushed.
- Next: full suite on 3.11/3.12/3.13, CI green, one CodeRabbit request, worker_done.
