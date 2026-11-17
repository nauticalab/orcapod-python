## `src/orcapod/core/operators/group_by.py` — tags entering the data group (ITL-955)

### GB1 — `GroupBy` moved non-key tag values into data columns
**Status:** resolved
**Severity:** high
**Issue:** ITL-955

`GroupBy(by=[...])` built the right table (keys scalar, every other column list-valued) but
declared only the keys as the output's tag columns, so each non-key tag became a list-valued
**data** column with an all-null `_source_<tag>` column. A function pod could then read tag
content, which breaks the operator / function pod boundary. `Batch` kept its tags as tags.

The rule (developer, November 11, 2026): no data element may come from a tag value. An operator
may use tags, but tag content never becomes data.

**Fix:** the output's tag columns are the input's tag columns, in input order: keys scalar,
every other tag `list[T]`; `unary_output_schema` predicts the same. A grouped stream can now be
grouped again on a coarser key with list-valued non-key tags, so the member sort key
(`_null_last_key`) is recursive and sorts nulls last at every depth; for scalar tags the order is
unchanged. No pipeline hash or table path moves; each grouped row's data hash does (six pinned
`pipeline:group_by.row[...]` golden entries replaced, approved). Results stored by a pod that
followed `GroupBy` through `select_data_columns` are reused without recomputation
(`tests/test_pipeline/test_aggregation_job.py::TestStoreWrittenBeforeTagsStayedTags`).

Audit of the other operators against the rule (November 11, 2026): `Batch`, `Join`, `MergeJoin`,
`SemiJoin`, `SelectTagColumns`, `DropTagColumns`, `SelectDataColumns`, `DropDataColumns`,
`MapTags`, `MapData`, `PolarsFilter`, `Index` and `Pick` never put an input tag into the data
group.
