## `src/orcapod/hashing/`

### H4 — Legacy backwards-compatible exports in `hashing/__init__.py`
**Status:** open
**Severity:** low

The module (line ~141) re-exports old API names for backwards compatibility:
```python
# TODO: remove legacy section
```

Should be removed in the next breaking release. Consider adding deprecation warnings first.
