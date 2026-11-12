## `src/orcapod/core/function_pod.py`

### F11 — Schema validation raises `ValueError` instead of custom exception
**Status:** open
**Severity:** high

`_validate_input_schema()` in `_FunctionPodBase` raises a generic `ValueError` when the
incoming data schema is incompatible with the function's expected input schema:
```python
# TODO: use custom exception type for better error handling
raise ValueError(
    f"Incoming data data type {input_schema} is not compatible with ..."
)
```

Using `ValueError` means callers cannot distinguish schema incompatibility from other value
errors without string-matching the message.

Fix: define a new `SchemaCompatibilityError` (subclass of `InputValidationError`) in
`errors.py` and raise it from `_validate_input_schema()`. The existing
`SchemaInconsistencyError` covers batch/polling-source schema drift; this new type covers
function-pod input schema mismatch. Name the exception to reflect the cause:
incompatible upstream stream schema vs. function's expected input schema.
