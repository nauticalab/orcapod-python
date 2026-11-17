## `src/orcapod/core/sources/polling_source.py`

### PS2 — Concurrent iteration over a single `PollingSource` has unguarded cursor/stream mutation
**Status:** open
**Severity:** high
**Issue:** ITL-625

`PollingSource` maintains shared mutable state (`_cursor` and `_accumulated_stream`) that is
updated without synchronization. Two simultaneous callers of `async_iter_data()` (or one async
and one sync caller) can therefore race:

- **Cursor interleaving** — both callers call `impl.poll(cursor=self._cursor)` concurrently.
  One receives a new cursor and advances `self._cursor`. The other then calls
  `impl.fetch(new_cursor)` instead of the cursor it intended.
- **Accumulated-stream clobbering** — `self._accumulated_stream = self._combine(...)` may be
  assigned by both callers, with one overwriting the other's in-flight result.

The iteration order guarantee — drain accumulated stream first, then poll from saved cursor —
is correct for a single caller, but breaks under concurrent callers because neither the drain
nor the advance is atomic.

**Fix:** Guard updates to `_cursor` and `_accumulated_stream` with an `asyncio.Lock` (async
path) and a `threading.Lock` (sync path, or use a single lock exposed via `_run_sync`). The
pre-seed loop in `async_iter_data` should take a snapshot of `_accumulated_stream` under the
lock before iterating, so later writes to the stream don't affect the snapshot mid-yield.
