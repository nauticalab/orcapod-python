# ITL-1060 plan: read-back rows carry their stored `__data_id`

Worker notes. Bug: `FunctionNode._load_cached_entries` builds Data via `ArrowTableStream.iter_data`
over a table with `__data_id` dropped, so each row got a fresh uuid7.
Fix: `_stored_rows_stream` also returns the stored ids; `_load_cached_entries` rebuilds each Data
with `record_uuid` (`_with_datagram_id`). All node read paths (sync, async cache-only, loaded)
go through `_load_cached_entries`. Pod-level result cache already passes `record_uuid`.
Evidence on use: `datagram_uuid` is read only on `process_data` outputs. Tests:
tests/test_core/function_pod/test_readback_datagram_id.py.
