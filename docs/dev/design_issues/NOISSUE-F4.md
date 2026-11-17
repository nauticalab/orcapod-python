## `src/orcapod/core/function_pod.py`

### F4 — `FunctionPodNode` is not a subclass of `TrackedDataFunctionPod`
**Status:** open
**Severity:** medium
`FunctionPodNode` reimplements `process_data`, `process`, `__call__`, `output_schema`,
`validate_inputs`, and `argument_symmetry` from scratch rather than inheriting from
`SimpleFunctionPod`/`TrackedDataFunctionPod` and overriding the parts that differ
(fixed input stream, pipeline record writing). The result is a large amount of structural
duplication that diverges silently over time.
