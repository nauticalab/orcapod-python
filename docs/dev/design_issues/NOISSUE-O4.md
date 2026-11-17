## `src/orcapod/core/operators/` — Async execution

### O4 — `GroupBy` member order is undefined for duplicate tag tuples
**Status:** open
**Severity:** medium

`GroupBy` sorts a group's members by their non-group-key tag values so the emitted lists are
stable across runs — orcapod hashes those lists to build the cache key, so an unstable order
causes spurious recomputes.

When `by` covers *every* tag column there is no non-key tag left to sort on, and the operator
falls back to the system `record_id`. That is a UUID v5 over the source id and a per-row
provenance token, so it encodes source *row position*. Permuting the source table therefore
changes member order, changes the list hash, and triggers a recompute. Measured: 79 distinct
member orders across all 120 permutations of a 5-row duplicate-tag input.

The branch is reachable with more than one member only when the input contains **duplicate tag
tuples** — `sort_columns` is empty exactly when the group key is the full tag tuple, so a
multi-member group requires duplicates.

This is not fixable inside `GroupBy`. The only remaining ordering signal is data content, and
the operator / function pod boundary forbids an operator from inspecting data.

The real root cause is upstream: `DuplicateTagError` (`errors.py:23`) is defined but **never
raised anywhere** in `src/` or `tests/`, so duplicate tag tuples are not prevented in the first
place. Tag uniqueness is assumed throughout the operator layer and enforced nowhere.

Fix: enforce tag uniqueness at stream construction (raising `DuplicateTagError`), which makes
this branch unreachable with more than one member.
