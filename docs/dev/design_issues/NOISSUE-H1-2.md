## `src/orcapod/hashing/`

### H1 — `FunctionSignatureExtractor` ignores `input_types` and `output_types` parameters
**Status:** open
**Severity:** critical

`extract_function_info()` (`function_info_extractors.py:36`) accepts `input_typespec` and
`output_typespec` but never incorporates them into the extracted signature string:
```python
# FIXME: Fix this implementation!!
# BUG: Currently this is not using the input_types and output_types parameters
```

The extracted signature is therefore type-agnostic — two functions with identical names but
different type annotations produce the same hash. This is a correctness bug that can cause
cache collisions between type-overloaded functions.

Fix: wire the type specs into the signature string; update tests.
