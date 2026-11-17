## `src/orcapod/utils/`

### U1 — Source-info column type hard-coded to `large_string`
**Status:** resolved (`tag_data.py` half), open (`arrow_utils.py` half)
**Severity:** critical

Two sibling call sites assume source-info values are always scalar strings.

**`Data._ensure_source_info_table()` (`core/datagrams/tag_data.py:330`)** builds the Arrow schema
as `pa.field(k, pa.large_string())` for every key, and `Data.schema()` (line ~384) reports `str`
for every `_source_*` column. Any operator that produces list-valued source info therefore fails
inside `job.run()`:

```
pyarrow.lib.ArrowTypeError: Expected bytes, got a 'list' object
  core/datagrams/tag_data.py:342  _ensure_source_info_table
```

Two operators hit this: `MergeJoin`, which carries source columns along as parallel lists when
merging colliding data columns (`merge_join.py:262`), and `Batch`, which list-wraps every column.
Both were reproduced against `966d759a`.

**Fix:** landed in NPIPE-204 (`1b570e68`, `1ed22ee4`). `_source_info_arrow_type` and
`_source_info_python_type` in `core/datagrams/tag_data.py` derive the Arrow and Python types
from the stored value — `str`/`None` → `large_string`, list → `large_list(<elem>)`
recursively. The `SourceInfoValue` alias lives in `types.py` next to `TagValue`/`DataValue`,
which lets `DataProtocol` and `ArrowTableStream` reference it without inverting the
`protocols/` → `core/` layering. No pipeline-DB schema bump: a node's source-column type is
fixed by its own output schema, so existing nodes keep `large_string`. Job-level regression
coverage is in `tests/test_pipeline/test_aggregation_job.py`. See
`superpowers/specs/2026-08-07-npipe-204-batch-group-by-design.md`.

A third site, `polars_data_utils.add_source_info` (line 119), forces `dtype=pl.String()`. It is
dead code — nothing in `src/` calls it, and the tests importing `add_source_info` import it from
`arrow_utils` — and it carries a latent shadowing bug where `source_column` is rebound to a
`pl.Series` inside the per-column loop. NPIPE-204 deletes it rather than fixing it.

**Still open:** in `add_source_info_to_table()` (`arrow_utils.py:604`), when source info is a
collection it is unconditionally cast to `pa.list_(pa.large_string())`:
```python
# TODO: this won't work other data types!!!
```
Any non-string collection values will fail or silently corrupt data. The logic also has an
unclear nested isinstance check (line ~602: `# TODO: clean up the logic here`).

Fix: inspect collection element types to select the appropriate Arrow type; refactor the
conditional logic.
