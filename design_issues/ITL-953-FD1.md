## `src/orcapod/hashing/semantic_hashing/function_info_extractors.py` — function signature hash (ITL-953)

### FD1 — Function defaults entered the function hash as `repr()`; parameter kind was not hashed
**Status:** resolved
**Severity:** medium
**Issue:** ITL-953

Finding L2 of the ITL-938 hash-site audit (Appendix A.9).
`FunctionSignatureExtractor` wrote a function's parameters as one line of text in which a
default value appeared as `repr(value)`. A `frozenset` default therefore hashed differently
under every `PYTHONHASHSEED`, a `dict` default by insertion order, an `object()` default
embedded a memory address, and two values with the same `repr()` shared a hash. The text
also dropped the parameter kind, so `f(a, *, b)`, `f(a, /, b)` and `f(a, b)` shared a hash.
A second, parallel description (the text of `hash_utils.get_function_signature`) fed the
stored `__pf_var_function_signature_hash` column with the same defects.

**Fix:** One structured description (developer decision, November 11, 2026): `parameters` is
a list of entries `{name, kind, annotation, default}` in declared order; `annotation` and
`default` are absent when the parameter has none. The extractor reports a default as a
`ParameterDefault`; `FunctionHandler` replaces it with the semantic hasher's hash of the value
(`hash_default_value`, the one place that decides how a default is hashed) and wraps the
hasher's `TypeError` with the parameter and function names. A default the hasher cannot hash
raises: no fallback to `repr()` or the type name. The stored signature hash is now
`hash_object(function)`. Moves: every function-value hash and every newly written stored
signature hash; no pod identity, path or stored schema.

Decided (developer, November 11, 2026): `Path`, `Enum`, `datetime` / `date`, `Decimal` and
numpy scalar defaults keep raising in v0.3. A data value of those types is hashed through the
type converter and the Arrow hasher, never through the structure hasher, which is why it has
no handler for them. The follow-up, ITL-971, is to hash a pod parameter's default as the
column value it would be; `hash_default_value` is where that goes.
