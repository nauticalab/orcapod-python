## `src/orcapod/core/function_pod.py`

### F1 — `_FunctionPodBase.process` is `@abstractmethod` with unreachable body code
**Status:** resolved
**Severity:** high
The method is decorated `@abstractmethod` but has real logic after the `...` (handle_input_streams,
schema validation, tracker recording, FunctionPodStream construction). Since Python never executes
the body of an abstract method via normal dispatch, this code is unreachable. `FunctionPod`
then duplicates this logic verbatim.

**Fix:** Removed the unreachable body code from `_FunctionPodBase.process()`, keeping it as
a pure abstract method with only `...`. `FunctionPod.process()` retains its own concrete
implementation.
