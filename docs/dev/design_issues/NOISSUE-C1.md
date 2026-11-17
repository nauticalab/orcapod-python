## `src/orcapod/contexts/registry.py`

### C1 — Redundant manual validation duplicates JSON Schema checks
**Status:** open
**Severity:** medium

`_load_spec_file()` (line ~141) performs manual required-field checking followed by JSON Schema
validation:
```python
# TODO: clean this up -- sounds redundant to the validation performed by schema check
```

The manual check is fully subsumed by the JSON Schema validation. Either remove the manual
checks, or make the JSON Schema validation optional and keep manual checks as the fallback.
