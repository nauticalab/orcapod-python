## `src/orcapod/core/function_pod.py`

### F5 — `FunctionPodStream` and `FunctionPodNodeStream` are near-identical copy-pastes
**Status:** open
**Severity:** medium
`iter_data`, `as_table` (including content_hash and sort_by_tags logic), `keys`,
`output_schema`, `source`, and `upstreams` are duplicated almost line-for-line. The only
behavioural differences are:
- `FunctionPodNodeStream` has `refresh_cache()`
- `FunctionPodNodeStream.output_schema` reads from `_fp_node._cached_data_function` directly

A shared base stream class would eliminate the duplication.
