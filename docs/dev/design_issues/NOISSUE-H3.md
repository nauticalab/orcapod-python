## `src/orcapod/hashing/`

### H3 — Visitor pattern does not traverse map types
**Status:** open
**Severity:** medium

`visit_map()` in `visitors.py:225` is a pass-through that does not recurse into map
keys/values:
```python
TODO: Implement proper map traversal if needed for semantic types in keys/values.
```

Semantic types nested inside map columns will not be processed during hashing, leading to
incorrect or incomplete hash values.
