# What a pipeline job keeps in memory (finding from ITL-186, input to ITL-858)

November 12, 2026. Read from the code on `main` at 25650c42 and measured on synthetic
spike_event-shaped rows (7 tag columns, 1 data column). Not a proposal.

## Measured

* One `(Tag, Data)` pair kept in a Python list costs about **5.3 KB**: 200,000 pairs add
  1,065 MB of peak RSS. The same rows as Arrow are 62 bytes each.
* `ArrowTableStream.iter_data()` produces about **9,000 rows per second**, and holds about
  10 KB per row of the table it is iterating (65,536-row table: +671 MB; 8,192-row table:
  +90 MB).

## Where rows are kept

| Place | What is kept | Until |
|---|---|---|
| `SyncPipelineOrchestrator` (`sync_orchestrator.py:91`) | `node.execute()` → `list(iter_data())`: every source row, then every node's output in `buffers` | end of run; released per node only with `materialize_results=False` |
| `AsyncPipelineOrchestrator._run_async` | `_CollectingWriter` copies every row of every node into `collectors` | end of run, unless `materialize_results=False` (default is `True`) |
| `OperatorNode.async_execute` (`operator_node.py:1457`) | `collected`: every output row, then `_cached_output_stream` (ITL-896, so `iter_data()` works after an async run) | lifetime of the node |
| `FunctionJobNode.async_execute` (`function_node.py:3071`, `3161`) | `_load_cached_entries()` loads every stored record of the node before the first input; every new output is added to `_cached_output_datas` | lifetime of the node |
| `Join._binary_streaming_join` (`join.py:564`) | `buffers`: every row of both inputs, also after one input has closed | end of the join |

Channels themselves are bounded (64 rows), so backpressure works; the memory is in the lists
above.

## Consequence

On either orchestrator a job over N rows holds roughly N × 5 KB for each node that keeps its
output. 100,000 rows: about 0.5 GB per node. The 8,087,906-row `spike_event`: about 43 GB per
node, and 15 minutes of row iteration before any pod runs. A source that streams in bounded
memory (ITL-186) does not change this.

## What a bounded-memory run would need (not designed here)

* Nodes that do not keep their output when the store already has it (function nodes), or
  when nothing asks for it (operator nodes with `cache_mode=OFF`).
* A function node that looks up stored records per input instead of loading all of them.
* A Join that keeps only the side it needs, or only the smaller side.
* Possibly channels that carry Arrow batches instead of one `(Tag, Data)` pair per row: the
  per-row objects are both the memory and the 9,000 rows/s limit.
