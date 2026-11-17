## `src/orcapod/databases/delta_lake_databases.py`

### D5 — `_handle_schema_compatibility()` uses naive equality and broad exception catching
**Status:** open
**Severity:** medium

The method (lines ~467, ~485) compares schemas with simple equality and catches all exceptions:
```python
# TODO: perform more careful check
# TODO: perform more careful error check
```

Should implement nuanced schema comparison (e.g., field-order invariance, nullable vs.
non-nullable promotion) and catch specific exceptions rather than bare `except`.
