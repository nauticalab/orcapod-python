## `src/orcapod/core/function_pod.py`

### F8 — `CallableWithPod` protocol placement breaks logical grouping
**Status:** open
**Severity:** low
`CallableWithPod` is defined between `FunctionPodStream` and `function_pod`, breaking the natural
grouping. It should be co-located with `function_pod` or moved to the protocols module.
