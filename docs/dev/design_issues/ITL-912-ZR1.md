## Cross-cutting

### ZR1 — Zero-row tables were typed from the Python schema
**Status:** resolved
**Severity:** high
**Issue:** ITL-912

The table a stream or node gives when it has no row was built from its Python output schema
(`arrow_utils.make_empty_table`): an operator node before its run, after `clear_cache()`, with
no stored table, or after a zero-row async run (`OperatorJobNode._make_empty_table`); the
sync orchestrator's input for an upstream that produced no row
(`SyncPipelineOrchestrator._materialize_as_stream`); and a function pod's stream or node with
no row (`_table_from_pairs([])`); and a side-effect pod's stream or node that delivered no row
(`side_effects._table_of_rows`); and the table of a `head()` with no rows to show
(`StreamBase._head_without_rows`). So `int8` read as `int64`, list items and source-info
columns changed nullability, system-tag columns went missing (operator nodes) and a function
pod lost its input's tag types and its context and source-info columns. Zero-row and
non-empty results must have the same types, and the developer's rule (November 13, 2026) is
that an Arrow schema is never computed: every pod derives it from its inputs' schemas and every
source has one.

**Fix:** one function, `streams/base.typed_zero_row_table(stream)`, asks a stream for
`_typed_zero_row_table()`: zero rows, user tags, system tags, data and source info, with the
types its rows have, or `None` when it cannot know them without rows. An `ArrowTableStream`
cuts its table; a source gives the table or schema it holds (`_stream`; cached sources
delegate to the wrapped source, a polling source to what it polled, a proxy to its delegate);
an operator node or `DynamicPodStream` applies its operator (`static_process`) to its inputs'
zero-row tables; a function pod's stream or node joins its input's tag columns to the zero-row
form of its output `Data` (`function_zero_row_table`, `Data.function_output_zero_row_table`,
built from the same converter call and field builders a computed row uses); a side-effect
pod's stream or node gives its input's (it passes rows through), with the context column. Inputs a job
builds with no rows use `StaticOutputOperatorPod._zero_row_input` (row-built column order,
`_rows_to_table`'s source-info nullability rule), through `_materialize_upstream` on both
orchestrators. A head with no rows is built in one place, `StreamBase._head_without_rows`,
from the stream's `_zero_row_table(config)`: `as_table(columns=config)` with no rows. Nothing is stored, so no hash, stored schema or
path changes; nothing is computed, read, polled or called. Regressions:
`tests/test_pipeline/test_operator_node_empty_table_types.py` and the un-marked xfails in
`tests/test_pipeline/test_operator_node_async_readback.py`.

Still built from the Python schema (no Arrow schema exists without rows): a `PollingSource`
before its first poll and an unbound input slot or `SourceProxy` (declared with a Python
`Schema` only; reported to the developer as a design gap), a `DerivedSource` (its
`_empty_origin_table` is left as it is), and a loaded node with no live upstream and no stored
table. `Index` and `Pick` over zero rows lack system-tag columns (ITL-985). These remaining
cases are tracked in ITL-1089.
