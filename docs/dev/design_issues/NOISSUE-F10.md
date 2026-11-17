## `src/orcapod/core/function_pod.py`

### F10 — `FunctionPodNodeStream.iter_data` recomputes every data on every call
**Status:** resolved
**Severity:** high
`iter_data` always iterates the full input stream and calls `process_data` for every data,
even when results are already stored in the result/pipeline databases.  This defeats the purpose
of the two-database design (result DB + pipeline DB) used to cache computed outputs.

**Fix:** Refactored `iter_data` to first call `FunctionPodNode.get_all_records(columns={"meta": True})`
to load already-computed (tag, output-data) pairs from the databases (mirroring the legacy
`PodNodeStream` design), yield those via `TableStream`, then collect the set of already-processed
`INPUT_PACKET_HASH` values and only call `process_data` for input data not yet in the DB.
Also added `FunctionPodNode.get_all_records(columns, all_info)` using `ColumnConfig` to control
which column groups (meta, source, system_tags) are returned.
