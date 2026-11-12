## `src/orcapod/core/sources/`

### F9 — `as_table()` crashes with `KeyError` on empty stream
**Status:** resolved
**Severity:** high
Both `FunctionPodStream.as_table()` and `FunctionPodNodeStream.as_table()` unconditionally call
`.drop([constants.CONTEXT_KEY])` on the tags table built from the accumulated data. When the
stream is empty (e.g. because the data function is inactive), `iter_data()` yields nothing,
`tag_schema` stays `None`, and `pa.Table.from_pylist([], schema=None)` produces a zero-column
table. The subsequent `.drop([constants.CONTEXT_KEY])` then raises `KeyError` because the column
does not exist.

**Fix:** Guarded both `.drop([constants.CONTEXT_KEY])` calls in `FunctionPodStream.as_table()` and
`FunctionPodNodeStream.as_table()` with a column-existence check. Also made the final
`output_table = self._cached_output_table.drop(drop_columns)` safe by filtering `drop_columns`
to only columns that exist in the table.
