## `src/orcapod/core/operators/merge_join.py` — Arrow types

### MJ2 — `MergeJoin` discards its inputs' Arrow types
**Status:** resolved
**Severity:** high
**Issue:** ITL-832 (with ITL-813; related: ITL-835, ITL-839)

`MergeJoin` built a merged (colliding) column with `pa.array(<python lists>)`, so Arrow inferred
the element type from the values instead of keeping the input's: `int8`/`int16`/`int32`/`uint8`
became `int64`, `float32` became `double`, nested lists widened their elements, and decimal
precision came from the data (`decimal128(5, 2)` became `decimal128(3, 2)`). PR #268 (ITL-813)
made the empty path mimic that inference with a hand-written table. When the two inputs gave a
column different Arrow types, the result depended on the values and on argument order, or the
Polars join raised.

**Fix:** No Arrow type is inferred from Python values. Every output column of `MergeJoin` and
`PolarsFilter` takes the large-normalized form of its input type
(`arrow_utils.normalize_to_large_types`: `string` to `large_string`, `list` to `large_list`,
recursively; hash-identical), keeping width, nullability and logical types. Merged columns,
and the list columns of `Batch` and `GroupBy`, are built by `arrow_utils.aggregated_list_type`:
`large_list<normalized input element>`, or for a logical element its logical list type from the
type converter (an error naming the column if the converter has none; the element is never
stripped to its storage type). Merged source-info columns are `large_list<large_string>`. A
column whose two inputs' types differ goes through `MergeJoin._resolve_disagreeing_types`:
equal after normalization proceeds, anything else raises `InputValidationError` naming the
column and both types, in `binary_static_process` before the join (`output_schema()` reports
Python types and cannot see it). Hashes: `list`/`large_list` and the other layout pairs hash
identically, so only a merged column narrower than `int64`/`double` changes its row hash (it no
longer widens). Rules recorded under "Arrow types" in `CLAUDE.md`.

`normalize_to_large_types` also dropped a list item's non-null flag (and map/union child
flags), which changes the Arrow schema hash of such a column; it now rebuilds every nested
field with `Field.with_type`, keeping name, nullability and metadata, so normalization is
hash-preserving (`tests/test_utils/test_normalize_to_large_types.py`). Measured on
`list<int64 not null>`, `struct<l: list<int8 not null>>` and
`list<list<string not null> not null>` columns: `Data.content_hash` (the stored
`input_data_hash`), `Tag.content_hash`, stream and source `content_hash` / `pipeline_hash`, and
`Batch` output hashes are all unchanged; only the Arrow types now keep the nested non-null
flags (a stored-schema change for such columns). The wider nullability audit across operators
and pods is ITL-842.

**Still open:** non-empty struct columns cannot be merged because values are sorted as Python
objects (ITL-839; pinned by a strict xfail).
