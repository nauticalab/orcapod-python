# Repro scripts from the ITL-1034 code sweep

Evidence for issues found by the row-alignment suite and its code sweep. Run each script from
the repository root with `uv run python superpowers/specs/itl-1034-sweep/<script>`. The
results were measured on `main` at 003d2e82 (November 12, 2026).

| Script | Issue | What it shows |
|---|---|---|
| `itl1045_two_pass_as_table.py` | ITL-1045 | `FunctionPodStream.as_table` builds the table and the content hashes in two `iter_data` passes; an upstream reset in between gives the cached table the recomputed rows' hashes (0 of 3 rows carry their own hash) |
| `itl1046_index_token.py` | ITL-1046 | `Index` after `Batch` or `GroupBy` gives the element the token `"['src::x=1::x', 'src::x=2::x'][0]"` instead of the element's own `'src::x=1::x'` |
| `itl1047_same_stream_twice.py` | ITL-1047 | a binary operator given the same stream twice fails inside a job, with different internal errors on the sync and async orchestrators |
| `datagram_uuid_after_readback.py` | ITL-1060, ITL-1042 | after an async run whose pod input comes through an operator node, the function node is stale; a row read back from the store gets a fresh `datagram_uuid`, not the stored `__data_id` its own token names |
| `interleaved_reads_recompute.py` | ITL-1065 | two interleaved iterations of a pod stream behind the automatic select call the function 7 times for 6 rows; the first iteration's rows are not the stream's (a regression from #355) |
| `side_effect_stream_as_table.py` | ITL-1066 | `SideEffectPodStream.as_table` raises with `context` / `all_info` (a duplicate `_context_key`), ignores `content_hash` and `sort_by_tags` |
| `partial_read_then_head.py` | ITL-1065 | after a partial read of a pod stream fed by the automatic select, the stream is stale and `head(2)` shows no rows (a regression from #355) |
| `itl1043_loaded_operator_cache.py` | ITL-1043 | a loaded job cannot read a cached operator node: the loaded pipeline hash drops tag nullability, and the loaded content hash uses the stub source's identity |

Status on `main` at 5c7a058f (November 13, 2026): fixed are ITL-1042 and ITL-1052 (#355),
ITL-1043 (#352), ITL-1045 (#358), ITL-1046 (#353), ITL-1050 (#351), ITL-1060 (#356) and
ITL-1065 (#360); their scripts now show the fixed behaviour. ITL-1047 and ITL-1066 are not
fixed there; the suite marks only ITL-1066 (strict xfail). Later, at 41798e87, ITL-1066 is
fixed (#362) and the suite has no marks.
