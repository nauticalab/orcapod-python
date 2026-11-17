## `src/orcapod/databases/delta_lake_databases.py`

### D6 — `defaultdict` used for `_cache_dirty` is not serializable
**Status:** open
**Severity:** medium

`__init__()` (line ~69) initializes `_cache_dirty` as `defaultdict(bool)`:
```python
# TODO: reconsider this approach as this is NOT serializable
```

`defaultdict` is not pickle-serializable, which blocks multiprocessing or serialization
use cases. Fix: use a regular dict with `.get(key, False)`.
