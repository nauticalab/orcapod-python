# PR #293 CodeRabbit follow-up: stored-ID cache refresh failures (ITL-864)

Revision plan approved by the coordinator (task_355ce670f6bb), provided each finding
reproduces. Branch `eywalker/itl-864-deltatabledatabase-reading-through-the`.

## Findings

1. **Comment 4534785488 (Major), `delta_lake_databases.py:658`.** When
   `_refresh_existing_ids_cache` fails after the Delta table loaded, it stores an empty ID
   set and marks the cache **clean**. The loaded table is cached, so `_get_existing_ids`
   never refreshes again: a pending row whose id is already stored is served next to the
   stored row (a duplicate) by every read method.
   *Fix:* the failure path marks the cache dirty, so the next read retries the refresh.

2. **Comment 4534785497 (Minor), `delta_lake_databases.py:891`.** In
   `get_records_by_ids`, `_pending_not_stored` refreshes the ID cache before the
   method's stored-read `try`, so a refresh failure escapes while a stored-read failure is
   logged and returns `None`.
   *Fix:* run the refresh inside the same `try` as the stored read (one handler, no second
   `return None`). After PR #290 (ITL-884) merges, that handler is gone and both failures
   raise `StoreReadError` naming the table.

## Steps (TDD)

1. Regression tests in `tests/test_databases/test_delta_read_your_writes.py`, public
   read methods only, with the backend's `DeltaTable.to_pyarrow_table` (used only by the
   ID refresh) patched to fail once:
   - stored `a=1`, flushed; pending `a=10`, `d=4`; one read fails in the refresh; every
     read method then returns `a=1`, `d=4` and never `a=10`.
   - stored `a=1`, flushed; pending `c=3`; `get_records_by_ids` with the refresh failing
     follows the method's stored-read error behaviour (now `None`), and the retry returns
     `[1, 3]`.
2. Run them, confirm both fail for the reasons above.
3. Apply the two fixes; rerun the file, then the full suite, ruff, basedpyright.
4. Commit, push, signed replies on both threads, wait for CI and CodeRabbit.

## After PR #290 merges

Merge `main`; in the read methods take #290's error handling (`StoreReadError`, no
`try`/`return None`) around this branch's merge helpers. #290's
`_refresh_existing_ids_cache` leaves `_cache_dirty` untouched on failure, which is still
clean when the refresh ran only because the table was not cached yet, so keep an explicit
`self._cache_dirty[record_key] = True` there. Change the test of finding 2 to expect
`StoreReadError` and extend it to every read method. Rerun the full suite (3.11, 3.12,
3.13), golden tests unchanged.

## Round 2 (CodeRabbit review of 8f9adf8b; coordinator approved both behaviour fixes)

- **Outside-diff (Major), `_get_existing_ids`:** a handle that loaded the table before any
  stored ids were read (a fresh root over an existing table) never refreshed them, so pending
  duplicates were served. Refresh also when the record key has no id entry. Test:
  `test_fresh_handle_drops_pending_rows_with_stored_ids` (four read methods).
- **4535100450 (Major), `_combine_stored_and_pending`:** stored and pending rows with any
  schema difference raised `ArrowInvalid`. Large-normalize both parts (`keep_null=True`),
  then `pa.concat_tables(promote_options="default")`. Contract tests assert the read before
  flush equals the read after flush (missing column either way, nullability merge, struct
  child added, `string`/`list` layouts); type differences keep raising `ArrowTypeError`; a
  `null`-typed new column is pinned (reads back, flush fails).
- **4535100459 (nit):** Google docstrings on the five tests and the wrapper.
