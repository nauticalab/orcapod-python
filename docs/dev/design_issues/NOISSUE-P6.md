## `src/orcapod/core/data_function.py`

### P6 — Cache-matching policy (`match_tier`) accepted but never used
**Status:** open
**Severity:** high

`CachedDataFunction.get_cached_output_for_data()` (line ~547) accepts a `match_tier`
parameter that is documented in the interface but completely ignored. Cache lookups always use
exact matching. Two inline TODOs mark this:
- `# TODO: add match based on match_tier if specified`
- `# TODO: implement matching policy/strategy`

This means any caller passing a non-default `match_tier` silently gets exact-match behavior,
which could lead to unnecessary cache misses or incorrect assumptions about cache hit semantics.

Requires: design a matching strategy interface; implement at least exact and fuzzy tiers.
