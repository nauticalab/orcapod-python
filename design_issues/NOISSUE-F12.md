## `src/orcapod/core/function_pod.py`

### F12 — System tag columns excluded from cache entry ID
**Status:** open
**Severity:** high

`FunctionPodNode.record_data_for_cache()` (line ~1077) builds a tag table for entry-ID
computation but excludes system tag columns:
```python
# TODO: add system tag columns
```

Two data with identical user tags but different provenance (arriving from different
pipeline branches, thus having different system tags) produce the same cache key. This can
cause cache collisions where a result computed for one pipeline branch is returned for
another.

Fix: include system tag columns in the `tag_with_hash` table before computing the entry ID hash.
