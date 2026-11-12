## `src/orcapod/semantic_types/universal_converter.py`

### UC1 — `python_type_to_arrow_type` raised on `typing.Any` from empty-container inference
**Status:** resolved
**Severity:** medium
**Issue:** ENG-389

`_infer_list_type` and `_infer_dict_type` in `pydata_utils.py` return `list[Any]` /
`dict[Any, Any]` when all sampled containers are empty (no elements to inspect). Passing
these inferred types to `python_type_to_arrow_type` raised `ValueError: Unsupported
Python type: typing.Any`.

**Fix:** Added `Any: pa.null()` to `_PYTHON_TO_ARROW_MAP` (forward path) and an explicit
`pa.types.is_null → Any` check to `_convert_arrow_to_python` (reverse path). `pa.null()`
is Arrow's canonical "unknown/no-type" marker; empty containers have no elements to
validate, so the encoding is semantically correct. The now-unreachable `Any`-specific hint
in the error branch was removed.
