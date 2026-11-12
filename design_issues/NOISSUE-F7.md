## `src/orcapod/core/function_pod.py`

### F7 — TOCTOU race in `FunctionPodNode.add_pipeline_record`
**Status:** resolved
**Severity:** medium
The method checks for an existing record with `get_record_by_id` and skips insertion if found.
But it then calls `add_record(..., skip_duplicates=False)`, which will raise on a duplicate. A
race between the lookup and the insert (e.g. two concurrent processes handling the same tag+data)
would cause a crash instead of a graceful skip. Should use `skip_duplicates=True` for consistency
with the intent.

**Fix:** ITL-508 redesigned `add_pipeline_record` to use indexed entry-ID versioning
(`max_index + 1`) with `skip_duplicates=True` on the versioned key. The
`skip_cache_lookup` parameter was removed entirely, eliminating both the TOCTOU
window and the stale-entry silent-skip failure described in ITL-513.
