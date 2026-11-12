## `src/orcapod/core/streams/`

### ST3 — Column selection operators duplicate `validate_unary_input()` five times
**Status:** open
**Severity:** medium

`SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns` (in
`column_selection.py:58`, `137`, `214`, `292`) and `PolarsFilterByDataColumns`
(`filters.py:135`) each have near-identical `validate_unary_input()` implementations. All are
marked:
```python
# TODO: remove redundant logic
```

The only difference between them is which key set (tag vs. data) is checked and the error
message text. A shared parameterized validation helper would eliminate the duplication.
