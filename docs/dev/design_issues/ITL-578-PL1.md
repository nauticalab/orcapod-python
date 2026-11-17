## `src/orcapod/logical_types/polars_type.py` — Polars values

### PL1 — `pl.DataFrame` had no logical type, so a pod could not take or return one
**Status:** resolved
**Severity:** high
**Issue:** ITL-578

`@function_pod(output_keys="df") def f(n: int) -> pl.DataFrame` raised at decoration:
`TypeError: No LogicalType or LogicalTypeFactory registered for <class 'polars.dataframe.frame.DataFrame'>`.

**Fix:** `LogicalPolarsDataFrame` (`orcapod.polars.dataframe`) and `LogicalPolarsSeries`
(`orcapod.polars.series`, a one-column frame) store the value in `large_binary` with Polars' own
`write_ipc` (Polars' defaults, pinned) and read it with `read_ipc` from memory (developer's
decision, November 12, 2026: storage is the library's own serialization, the hash is defined
separately from the value's content). Every supported dtype reads back exactly; chunking and
sortedness flags are not stored. `Object`, `Int128`, `UInt128` and Polars extension dtypes
are rejected at write time, naming the column. The stored bytes are never hashed,
so `PolarsDataFrameHandler` / `PolarsSeriesHandler` hash
`sha256(Orcapod dtype descriptor + starfix digest of the values)` instead, under the labels
`polars_dataframe_v1` / `polars_series_v1` (developer's decision, November 10, 2026).
Everything a pod can observe through the public Polars API as the value's logical content is
identity (developer's decision, November 11, 2026, reversing the November 10 ruling that
order was not): the descriptor lists columns and struct fields in order at every depth, a
`Categorical` carries its `Categories` (name, namespace, physical width), and the values are
digested by starfix 0.5 in its ordered mode (`ordered_columns`, `ordered_struct_fields`,
ITL-898), which also tells a null struct from a struct of nulls. Physical properties stay
out: chunking, sortedness flags (not stored), physical `Categorical` codes, Arrow layout.
Both types carry the identifier metadata `{"ignore":[],"version":1}` (the ignore-list
mechanism of ITL-981; the hash is strict by default and can leave out `column_order`,
`struct_field_order` and `series_name`) and are registered, with their value handlers, in
`contexts/logical_types.json`. Golden values (value and data hashes, type and
`Schema` hashes, source identities, columns without values) are pinned in the shared golden
suite, `tests/test_hashing/golden` (the `polars_*` cases). Existing hashes are unchanged:
every pre-existing golden entry passes.
Open: SQLite stores reject every logical-typed column (ITL-877); the pandas handler ignores
column order, Categorical vs String and struct field order (ITL-897).
