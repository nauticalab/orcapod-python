## `src/orcapod/utils/`

### U4 — `is_subhint` does not handle type invariance
**Status:** open
**Severity:** high

`check_schema_compatibility()` (`schema_utils.py:37`) uses beartype's `is_subhint` which
treats all generics as covariant:
```python
# TODO: is_subhint does not handle invariance properly
```

For mutable containers (`list[int]` vs `list[float]`), this produces false positives —
schemas are reported as compatible when they are not (e.g., a `list[int]` field is accepted
where `list[float]` is expected, but appending a float to `list[int]` would fail at runtime).

Options: add an invariance-aware wrapper, switch to a stricter type comparison, or document
the limitation prominently.
