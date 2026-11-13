## `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` — pandas value hash identity

### PV1 — pandas values that pandas tells apart hashed the same
**Status:** resolved
**Severity:** high
**Issue:** ITL-897, ITL-887

`PandasDataFrameHandler` and `PandasSeriesHandler` hashed `pa.Table.from_pandas(value)` with the
Arrow hasher, which ignores column order and struct field order. The hash also never saw what
pyarrow keeps only in its `pandas` schema metadata. So frames that `assert_frame_equal` tells
apart hashed the same when they differed only in column order, MultiIndex level order, struct
field order, label type (`0` vs `"0"`), `columns.name`, nullable vs NumPy dtype, `string` vs
`object`, or categorical vs string / categories / `ordered` (ITL-887). Storage kept every one of
them, so a cached pod returning `df.columns[0]` was handed the result computed for another
column order (sync; async once the cache is warm).

**Fix:** labels `pandas_dataframe_v1` / `pandas_series_v1`,
`sha256(descriptor + b"\x00" + ordered digest)`. The ordered digest is the Arrow hasher's new
`hash_table_ordered` of the positionally renamed `from_pandas` table: the same semantic
preprocessing as `hash_table`, then starfix 0.5's `ordered_columns` and
`ordered_struct_fields` options. Both entry points share one private starfix call. The
descriptor is an Orcapod-owned grammar of `attrs`, typed labels, `columns.names`, the
column-axis dtype, index levels and pandas dtypes (`orcapod.hashing.pandas_value_hash`; `docs/reference/hashing.md`
section 10). Storage and hashing are separate and the hash is strict (developer, November 11
and 12, 2026): every property of a value counts, `attrs` included, except what storage does
not restore (`flags`, RangeIndex kind, index `freq`, unused MultiIndex levels, string
storage, the `object` dtype of a column that reads back typed, the type of an index level
name, the types of labels of mixed type, an extension dtype of an index level), which never
counts, so
`hash(read(write(v))) == hash(v)`. `PANDAS_IDENTITY_PROPERTIES` is the one list of these in
code. The logical types were renamed `orcapod.pandas.dataframe` / `orcapod.pandas.series`
with the shared ignore-list metadata of library value types (`{"ignore":[],"version":1}`,
ITL-981; options `column_order`, `struct_field_order`, `series_name`, `attrs`, none ignored
by default); the old names are no longer registered. Every pandas value hash moved
once, before the v0.3 tag; stored bytes are unchanged. The investigation is in
`superpowers/specs/2026-11-11-itl-897-pandas-hash-identity.md`. `pa.Table` values (same
rule) are not included and remain order independent.
