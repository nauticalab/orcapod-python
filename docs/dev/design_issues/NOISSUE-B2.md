## `src/orcapod/core/base.py`

### B2 — Mutable `data_context` setter may invalidate cached state
**Status:** open
**Severity:** medium

`DataContextMixin.data_context` (line ~92) has a property setter that allows runtime context
changes. If a stream or pod has already cached schemas or hashes derived from the previous
context, those caches silently become stale.

Options: (1) remove the setter and make context immutable after construction, or (2) add cache
invalidation on context change and document when changing context is safe.
