## `src/orcapod/core/tracker.py`

### T1 — `SourceNode.identity_structure()` assumes root source
**Status:** open
**Severity:** medium

The method (line ~163) delegates directly to `stream.identity_structure()`:
```python
# TODO: revisit this logic for case where stream is not a root source
```

For derived sources (e.g., `DerivedSource`), the stream may not have a meaningful
`identity_structure()`. Needs an isinstance check or protocol-based dispatch.
