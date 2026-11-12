## `src/orcapod/core/data_function.py`

### P5 — Large dead commented-out block in `get_all_cached_outputs`
**Status:** resolved
**Severity:** low
The block commenting out `pod_id_columns` removal is leftover from an old design. It makes it
ambiguous whether system columns are actually filtered.

**Fix:** Deleted the commented-out block.
