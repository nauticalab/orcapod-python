## `src/orcapod/utils/`

### U3 — `check_arrow_schema_compatibility()` lacks strict mode and type coercion
**Status:** open
**Severity:** high

The function (`arrow_utils.py:433`, `462`) documents strict vs. non-strict behavior but only
partially implements the non-strict path:
```python
# TODO: add strict comparison
# TODO: if not strict, allow type coercion
```

Currently, the function always raises on type mismatch instead of coercing compatible types
when in non-strict mode. Users cannot choose between strict field-order checking and permissive
type promotion.
