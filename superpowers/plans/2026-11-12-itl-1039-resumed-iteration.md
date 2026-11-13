# ITL-1039 Resumed function pod stream iteration drops rows: plan

**Goal:** A `FunctionPodStream` read again after a partial iteration yields every input row
once, in input order, each with its own cached output, computing each row once.

**Cause:** `_iter_data_sequential` / `_iter_data_concurrent` (`core/function_pod.py`) wrap the
long-lived `_cached_input_iterator` in `enumerate()` on every call. The count restarts at 0
while the input generator resumes, so the resumed read takes input row `k` as index 0, yields
row 0's cached output instead, and drops row `k`. A resumed read also never yields the rows a
partial iteration already cached (with a correct index it would start at `k`).

**Fix:**
1. `_ensure_iterator` stores `enumerate(input_stream.iter_data())`: the input index is
   attached once, when the input is first read, and travels with each row.
2. `_iter_data_sequential` walks indices from 0: a cached index is yielded from the cache, an
   uncached one pulls the next `(index, row)` from the shared input, computes, caches it under
   its own index, yields. Interleaved iterations share the cache and the input.
3. `_iter_data_concurrent` collects the rest of the input with its own indices, computes the
   uncached ones concurrently, then yields every cached index in order from 0.

**Tests (failing first, public interface):** `tests/test_core/function_pod/test_resumed_iteration.py`:
partial `iter_data` then `as_table` (1, 2, 5 rows consumed); partial then full iteration;
two interleaved partial iterations; partial then `head`; switch to a concurrent executor
after a partial iteration; for a plain pod, a concurrent executor and `CachedFunctionPod`.
Each also asserts every row is computed once.

**Evidence to state in the PR:** whether `FunctionNode` / the job path, `head()` and the async
path share the pattern.

**No** hash, golden, stored-table or public API change. design_issues entry + changelog fragment.
