## `src/orcapod/core/streams/` — notebook preview and `head()`

### PV1 — Showing a stream computed it, delivered side effects and wrote caches
**Status:** resolved
**Severity:** high
**Issue:** ITL-991

`StreamBase._repr_html_` and `StreamBase.view()` called `as_polars_df()` on the whole
stream. Displaying a function pod's output in a notebook called the function on every row,
an operator's output ran the operator, a `SideEffectPodStream` or `SideEffectNode` delivered
every side effect, a `PollingSource` polled, a `CachedSource` wrote its cache table, and a
blueprint node raised inside the formatter. The developer ruled (November 12, 2026) that a
preview shows only what is readily available and never triggers computation, and that a
node's preview is a head of its storage.

**Fix:** every stream, source and node has `head(n=10, columns=..., all_info=...)`
returning a `StreamHead` (rows, `total_rows`, `HeadStatus`), and `_repr_html_` renders
`head()`. The default shows the columns and `NOT_COMPUTED`; only classes that can serve rows
without computing override it. `view()` and `StreamView` are removed. A job node with nothing
in memory reads its whole stored slice; a bounded database read is a follow-up. The same
work fixed `DynamicPodStream` serving a stale result after an input changed (misspelled
`_cached_results` in `_update_cache_status`).
