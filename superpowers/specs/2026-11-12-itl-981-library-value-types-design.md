# ITL-981: library value types (design note)

Status: SIGNED OFF by the developer on November 12, 2026 and implemented in PR #328. The
text below is the proposal as it was approved; the four "open decisions" at the end were
decided as proposed (refuse a column of another list; label `ndarray_v1`; keep refusing
unit-less datetimes and field titles; sorted metadata keys), and section (vii) records the
final extension names. Added in review of PR #328: padding bytes between structured fields
are written as zero, and a stored cell must hold exactly the element bytes its header
declares.
Measured with NumPy 2.2.6 and pyarrow 23.0.1 on a little-endian machine, writing with
`numpy.lib.format.write_array(version=(3, 0), allow_pickle=False)` and reading with
`numpy.lib.format.read_array(allow_pickle=False)`. "Measured" below means exactly that.

Rule applied (proposed by the torch and polars workers, adopted here): a property that
storage does not round-trip can never count in the hash, because `hash(read(write(v)))` must
equal `hash(v)` or every cached re-run recomputes.

## (i) numpy.ndarray

### A. Properties

Each line: option name; in the hash by default; does storage round-trip it.

Always in the hash, cannot be ignored:

1. `dtype`: yes / yes. Kind and width (`int32` is not `int64`), `datetime64` / `timedelta64`
   unit and multiple (`[ns]`, `[10ms]`), string and void widths (`U5`, `S3`, `V4`), and for a
   structured dtype the field names, declared field order, field types and subarray shapes,
   at every depth.
2. `shape`: yes / yes. Zero-dim `()` and empty shapes `(0, 3)`, `(3, 0)` are all distinct.
3. `element_bits`: yes / yes. Bit exact: `-0.0`, NaN payloads, NUL padding of strings, every
   byte of a void element.

In the hash by default, can be ignored (these are the three option names):

4. `memory_order`: yes / yes, as two values. `F` when the array is Fortran-contiguous and
   not C-contiguous, otherwise `C`. This is exactly the flag `.npy` keeps (measured: an F
   array and a transposed C array read back F-contiguous).
5. `byte_order`: yes / yes. Little or big, per scalar field (measured: `>f4`, `>U3`,
   `>M8[ns]` and a structured dtype with mixed orders read back unchanged). It is absolute,
   not "native or swapped", so a value hashes the same on every machine.
6. `struct_layout`: yes / yes. Field offsets and itemsize of a structured dtype, at every
   depth (measured: an aligned dtype, a nested aligned dtype with a subarray field, and
   trailing padding read back with identical offsets and itemsize).

Never in the hash (no option):

7. Strides beyond property 4 (step slices `a[:, ::2]`, reversed `a[::-1]`, broadcast views):
   never / no. Measured: `.npy` writes them C-contiguous. This is where the round-trip rule
   bites: a strided view hashes like its C-contiguous copy.
8. The `align=True` flag of a structured dtype: never / no (measured: `isalignedstruct`
   reads back `False`; the offsets it produced are kept, property 6).
9. Padding bytes between structured fields: never / not reliably (corrected after CI: on
   Linux a stored aligned array came back with zeroed padding). NumPy does not
   define them (`np.empty` leaves garbage, field assignment does not copy them), so they
   cannot be hashed deterministically.
10. Subclass (`np.matrix`, `np.recarray`, `np.memmap`): never / no (measured: all read back
    as plain `np.ndarray`; a recarray's `np.record` dtype reads back as the plain dtype).
11. `writeable` flag: never / no (measured: a read-only array reads back writeable).
12. dtype `metadata`: never / no (measured: reads back `None`).
13. Ownership (`base`, `OWNDATA`), `ALIGNED` flag, memory address: never / no. Not
    properties of the value.

Refused at write, as on main, with `UnsupportedValueError` naming the dtype:

14. Masked arrays (the mask would be lost; `.npy` silently drops it, measured), object
    dtype and structured dtypes with an object field, NumPy 2 `StringDType` (`.npy` itself
    refuses both without pickle, measured), `longdouble` / `clongdouble` (platform width).
15. Unit-less `datetime64` / `timedelta64` and structured field titles: `.npy` round-trips
    both (measured), but main refuses them and ITL-911 tracks them. Proposal: keep refusing
    in v0.3; accepting them later changes no existing hash.
16. New refusals forced by `.npy`: a structured dtype with overlapping or out-of-order
    field offsets (`np.save` raises, measured), and a dtype whose `.npy` header exceeds
    10,000 bytes (roughly 350 or more fields: `read_array(allow_pickle=False)` refuses to
    parse a longer header, measured at 400 fields). Checked at write so nothing unreadable
    is stored.

### B. Hash

Notation: `u8` one byte; `u64le` / `i64le` 8-byte little-endian; `lp(x) = u64le(len(utf8(x))) || utf8(x)`.

Step 1, element digest `E`. Unchanged from main (`array_format.array_digest_v1`, shared
with torch): take the array as C order, little-endian, structured fields packed in declared
order (`canonical_array`), then
`E = SHA-256(lp(tag) || u64le(ndim) || i64le(each dim) || u64le(len(bytes)) || bytes)`,
where `tag` is main's dtype tag (`float32`, `datetime64[ns]`, `ucs4[5]`, or the JSON field
list for a structured dtype). `E` covers properties 1 to 3 and nothing else.

Step 2, value hash `D = SHA-256(P)` with
`P = lp("orcapod.numpy.ndarray") || E (32 bytes) || u8(memory_order) || lp(byte_order_text) || lp(struct_layout_text)`.

- `memory_order`: 1 if `flags.f_contiguous and not flags.c_contiguous`, else 0.
- `byte_order_text`: one character per scalar leaf of the dtype, depth first in tag order (a
  subarray field counts once): `<` little-endian, `>` big-endian, `|` for a type without
  byte order (bool, 8-bit integers, `S`, `V`). NumPy's native marker `=` is resolved with
  `sys.byteorder`. A plain `float32` gives `<`.
- `struct_layout_text`: compact JSON (`separators=(",", ":")`) of `L(dtype)`, where `L` is
  `null` for an unstructured dtype and otherwise the list
  `[itemsize, [offset_1, L(field_1)], [offset_2, L(field_2)], ...]` in declared field order;
  a subarray field uses its base dtype. A plain `float32` gives `null`.

Ignoring a property: its field is always present and is written with the canonical value
instead of the real one. `memory_order` 0; `byte_order_text` with every `>` replaced by `<`;
`struct_layout_text` computed from the packed dtype `tag_to_dtype(tag)`. So two values that
differ only in an ignored property have equal `D`, and `D` with all three ignored equals the
strict `D` of the canonical array (C order, little-endian, packed). The ignore list is not
in `P`; it is in the extension metadata, so it is in every type identifier and cell token.

The result is `ContentHash("ndarray_v1", D)`. Proposal: keep the label `ndarray_v1`, because
nothing was released under it (v0.2 hashed the `.npy` file under another name); the
alternative is `ndarray_v2`.

Changes from main: main's hash is `D = E`: no leading type text, and it ignores all three
options. Every array hash moves. The leading text makes an ndarray never equal a tensor.

A cell of a stored table is hashed by decoding the cell to an array and computing `D` with
the ignore list of the cell's own column type. The stored bytes are never hashed.

### C. Storage

- Column: Arrow extension `orcapod.numpy.ndarray` over `large_binary`; one `.npy` file per cell.
  Null array is a null cell.
- Write: `numpy.lib.format.write_array(buffer, array, version=(3, 0), allow_pickle=False)`
  on the plain-`ndarray` view of the value (the user's array is not modified). `np.save`
  itself cannot pin the version (it picks 1.0 or 3.0 by content); format 3.0 is the only
  one that stores non-Latin-1 field names (measured: 1.0 and 2.0 raise on `β`).
- Read: check the magic is format 3.0, then
  `numpy.lib.format.read_array(buffer, allow_pickle=False)` (default `max_header_size`).
  The result is a fresh, writeable array.
- Round-trips (measured): dtype including byte order, units, widths, field names, offsets
  and itemsize; shape; bits; C or Fortran contiguity. Does not: properties 7, 8, 10 to 13.
- Size: 128 bytes of header per value for simple dtypes (a 6-element `float32` array is 152
  bytes), against a few dozen on main.
- Security: `allow_pickle=False` on both sides, so no pickle is ever read; an object array
  inside a stored file raises `ValueError` (measured). The header is parsed by NumPy with
  `ast.literal_eval` under the 10,000-byte limit. A truncated or garbage cell raises
  `ValueError` (measured). Orcapod never passes `allow_pickle=True` and never raises
  `max_header_size`.
- The stored bytes are deterministic within one NumPy version (measured) but NumPy does not
  promise them across versions. So `storage_sort_key` (MergeJoin sorts stored values) becomes
  the strict `D` of the decoded value, not the stored bytes.

## (ii) Shared mechanism

### Extension metadata

`json.dumps({"ignore": names, "version": N}, sort_keys=True, separators=(",", ":"), ensure_ascii=True)`
with `names` sorted by code point and unique. Exact bytes:

- default: `{"ignore":[],"version":1}`
- non-default: `{"ignore":["byte_order","memory_order"],"version":1}`

The `ignore` key is always present, also when empty. An option name matches
`[a-z][a-z0-9_]*`. On read the bytes are parsed and must re-serialise to the identical bytes;
keys other than `ignore` and `version` are an error.

New module `orcapod/logical_types/ignore_list.py` (no heavy imports), beside `identifier.py`
(`version_metadata` stays for types without options):

- `ignore_metadata(version: int, ignore: Iterable[str]) -> bytes`
- `parse_ignore_metadata(metadata: bytes) -> tuple[int, tuple[str, ...]]`
- `checked_ignore(ignore: Iterable[str], options: Collection[str]) -> tuple[str, ...]`:
  sorted and unique; an unknown name raises `ValueError` naming it and the allowed names.
- `class IgnoreListLogicalType(BaseLogicalType)`: class attributes `EXTENSION_NAME`,
  `STORAGE_TYPE`, `VERSION`, `OPTIONS`, `python_type`; `__init__(self, ignore=())`; property
  `ignore`; implements `get_arrow_extension_type`, `get_polars_extension_type` and
  `readable_extension_metadata`. A subclass adds `python_to_storage` / `storage_to_python`.
- `class IgnoreListHandler`: class attributes `OPTIONS`, `HASH_LABEL`;
  `__init__(self, ignore=())`; `handle(obj, hasher)` returns
  `hash_ignoring(obj, frozenset(self.ignore))`; `hash_ignoring(obj, ignore) -> ContentHash`;
  a subclass implements `digest(self, obj, ignore: frozenset[str]) -> bytes`.

Arrow type: one `pa.ExtensionType` class per (extension name, ignore list), so two lists are
unequal Arrow types everywhere (measured: `==`, schema equality, `concat_tables` and nested
`large_list` all tell them apart; IPC and Parquet round-trip the list). The class registered
with Arrow under the name deserialises any valid list to the class of that list; an unknown
name or non-canonical metadata raises `LogicalTypeIdentifierError`.

### Configuration

`contexts/logical_types.json` gets one new top-level object, the single place a list is
declared, keyed by extension name:

    "hash_ignore": {"orcapod.numpy.ndarray": []}

The context loader puts each entry into the reference table as `hash_ignore:<extension name>`
before it builds anything. The logical type entry and the value handler entry both take it:
`"_config": {"ignore": {"_ref": "hash_ignore:orcapod.numpy.ndarray"}}`. So:

- (a) the logical type's Arrow extension metadata is `ignore_metadata(VERSION, self.ignore)`;
- (b) a bare value: the semantic hasher finds the handler by Python type and calls
  `handle`, which uses the handler's configured list (the data context's);
- (c) a cell: `SemanticHashingVisitor.visit_extension` already has the cell's extension
  type. When the handler is an `IgnoreListHandler` it calls
  `handler.hash_ignoring(value, parse_ignore_metadata(column metadata)[1])`. The list comes
  from the column, never from the context. The cell token stays
  `<identifier text>::<D>`, and the identifier text now contains the list.

An unknown option name is an error at context build (`ContextValidationError` from the
`ValueError` of `checked_ignore`), for the logical type and for the handler.

There is no user API in v0.3; a different list needs a custom `logical_types.json`
(`DataContextRegistry(logical_types_file=...)`).

### Two lists meeting

- Join / MergeJoin of two columns whose Arrow types carry different lists:
  `InputValidationError` naming the column and both types, through the existing
  `require_shared_arrow_type` (the two Arrow types are unequal; the class name printed in the
  message carries the list).
- DECISION 1. A data context has exactly one list per type. Proposal: a column stored under
  another valid list is refused on read with `LogicalTypeIdentifierError` naming the stored
  and the readable metadata (the existing identifier rule; `readable_extension_metadata()`
  is the configured list only). Reason: the Python schema says only `np.ndarray`, and the
  schema hash and type hash take the identifier from the context's logical type, so a column
  read under a foreign list would have a schema hash that disagrees with its cells. Reading
  such a column needs a per-column Python type (the out-of-scope user API). The visitor
  still takes the list from the column, so allowing it later changes no hash.

## (iii) Golden entries

Moved (32 values), by cause. Cause H: the hash definition (`D`). Cause M: the identifier
metadata `{"version":1}` to `{"ignore":[],"version":1}`. Cause S: storage type struct to
`large_binary`.

- `semantic.json`: `value.ndarray.{int32,int64,float64,empty}` (H); `type.ndarray`,
  `type.structural_with_array` (M).
- `schema.json`: `single_field.ndarray.{arrow_type,schema_hash}`,
  `single_field.structural_with_array.{arrow_type,schema_hash}` (M, S).
- `arrow.json`: `extension_no_values.{all_null,zero_rows}.ndarray` (M).
- `datagram.json`: the 10 `ndarray.*` and 10 `structural_with_array.*` values (H, M; the
  system tag column names move because they embed the schema hash).

Added: `value.ndarray.{fortran,big_endian,aligned_struct,strided_view}` and pinned `D`
vectors in `test_golden_hashes.py`. No other entry moves; the regeneration diff is checked
for that.

## (iv) Deleted from ITL-888

- `array_format.py`: `ARRAY_STORAGE_TYPE`, `ARRAY_FORMAT_METADATA`, `ARRAY_FORMAT_VERSION`
  and the storage-struct text. `array_digest_v1` stays (it is `E`).
- `numpy_type.py`: the struct `python_to_storage` / `storage_to_python`, the
  `(tag, shape, data)` sort key. `dtype_to_tag`, `tag_to_dtype`, `canonical_array`,
  `array_bytes` stay (they produce `E`).
- `NumpyArrayHandler`: rewritten as an `IgnoreListHandler`.
- Docs: `arrays.md` sections "What counts as the same array" and "How an array is stored"
  and `hashing.md` section 8 are rewritten; `logical-type-identifiers.md` gains the ignore
  list. `superpowers/specs/2026-11-10-array-format-v0.3.md` gets a "superseded" line.
- Tests of the struct row (`test_array_value_format.py`, `test_array_format.py`,
  `test_numpy_type.py`, `test_numpy_handler.py`) are rewritten for the new behaviour.

## (v) Size and commit order

About 30 files: 8 source, 10 test, 4 golden data plus cases and generator, 4 docs,
changelog fragment, design-issue file, plan.

1. Plan under `superpowers/plans/`.
2. Failing tests through the public interface (hash distinguishes memory order, byte order,
   layout; ignoring makes them equal; exact round-trip; join error; unknown option name).
3. `ignore_list.py` and the context loader (`hash_ignore`).
4. NumPy storage (`.npy`) and logical type.
5. NumPy handler (`D`) and the visitor's cell path.
6. Golden regeneration for array entries, with the moved-entry list.
7. Docs, changelog fragment, design-issue file.

## (vi) How torch, polars and pandas plug in

Each branch:

1. adds its entry to `hash_ignore` in `logical_types.json` (`"orcapod.torch.tensor": []`
   ...), and `{"ignore": {"_ref": "hash_ignore:<name>"}}` to its logical type and handler
   entries (also inside a deferred registration);
2. subclasses `IgnoreListLogicalType` with `EXTENSION_NAME`, `STORAGE_TYPE`, `VERSION`,
   `OPTIONS`, `python_type`, and its two storage methods;
3. subclasses `IgnoreListHandler` with `OPTIONS`, `HASH_LABEL` and
   `digest(obj, ignore) -> bytes`, writing each optional property's field with its
   canonical value when its name is in `ignore`;
4. for torch, calls `array_format.array_digest_v1(tag, shape, data)` for `E`, unchanged.

Nothing else is needed: the visitor, the loader, the metadata and the Arrow type classes
are shared.

## (vii) Extension names are `orcapod.<library>.<type>` (developer, November 12, 2026, final)

First decision of the day: `<library>.<type>` (`numpy.ndarray`). Revised the same day,
because using the libraries' own namespaces is dangerous: prepend `orcapod.`. Final names:
`orcapod.numpy.ndarray`, `orcapod.torch.tensor`, `orcapod.pandas.dataframe`,
`orcapod.pandas.series`, `orcapod.polars.dataframe`, `orcapod.polars.series`;
`orcapod.file` and `orcapod.directory` are unchanged.

For ITL-981:

- The array type's extension name and logical type name are `orcapod.numpy.ndarray` (main:
  `orcapod.ndarray`). One constant (`NDARRAY_EXTENSION_NAME`) and the `hash_ignore` key;
  spelling in 8 test files, `arrays.md`, `hashing.md`, `logical-type-identifiers.md`.
- The leading type text of the value hash is the extension name (proposed by the
  coordinator, not yet confirmed): `lp("orcapod.numpy.ndarray")` in `P` of section (i) B, and
  `orcapod.torch.tensor` for torch. It is one constant; the tests take it from
  `LEADING_TYPE_TEXT`.
- Golden: no further entry. The same 32 values move once; cause M now includes the name.
- The clash with v0.2 does not arise: v0.2 stored arrays as `numpy.ndarray` (empty
  metadata), a name v0.3 does not register, so v0.2 tables are read as plain storage,
  counted and left unchanged, exactly as on main. (Measured while the name was briefly
  `numpy.ndarray`: registering that name made 2 of the 6 tests of
  `tests/test_arrays/test_array_v02_store.py` fail, because every pyarrow or Delta read of a
  v0.2 table then calls the registered class.) The v0.2 fixture script keeps its guard.
- The "foreign type" is therefore DROPPED. The mechanism does not need it: the class
  registered with Arrow deserialises any valid ignore list to the class of that list, and
  for anything else (non-canonical bytes, an unknown option name, another version) raises
  `LogicalTypeIdentifierError`, which is what every built-in type does on main.
- Note for ITL-897: the pandas rename is different. v0.2 stores used `pandas.dataframe`,
  and main reads their empty metadata through `also_reads`. That is ITL-897's question.

## Open decisions for the developer

1. Reading a column stored under another list: refuse (proposed) or read.
2. Hash label: keep `ndarray_v1` (proposed) or `ndarray_v2`.
3. Keep refusing unit-less datetimes and field titles in v0.3 (proposed), although `.npy`
   stores them.
4. Metadata key order: sorted keys, `{"ignore":[...],"version":1}` (proposed), so that
   `json.dumps(sort_keys=True)` reproduces it; the brief sketched version first.

## (viii) Scoping: storage type inside the Arrow schema hash (not part of ITL-981 unless decided)

Question from the coordinator: `StarfixArrowHasher.hash_schema` hashes a logical-type field
as its storage type plus the `ARROW:extension:*` keys, so that hash depends on the storage
type. Should a logical-type field be hashed as a fixed placeholder plus name and metadata?

Findings (read from the code and measured on main):

- No library code calls `hash_schema` (the only match under `src/` is its definition). It is
  reached only by tests and by 47 golden values in `arrow.json` (`schema.*`, 42 plain;
  `extension_schema.extension.*`, 5).
- Every schema or type hash Orcapod stores or embeds goes through the semantic hasher over
  Python types: the schema hash (Site 3) and the system tag column names built from it, the
  type hash, the pipeline hash and the paths built from it, the function signature. There a
  logical type contributes its identifier text only (`canonical_annotation_str`,
  `TypeObjectHandler`): name and metadata, never the storage type.
- `hash_table` (default source ids, row and datagram hashes) hashes a value-hashed column as
  `large_binary` plus the keys, also with zero rows (ITL-967): already independent of
  storage. Value-hashed today: UUID, File, Directory, ndarray, pandas DataFrame and Series,
  and `list` / `set` of them.
- Types without a value handler (Path, UPath, dataclass, pydantic, structural classes,
  lists of them, unregistered extension types) are hashed in `hash_table` as their stored
  values under the storage type. For them the stored struct is the value, so the storage
  type must stay in the hash.

So no stored or embedded hash depends on a value-hashed type's storage type today; only the
unused public method does. If it is to be made consistent, the change is to give
`hash_schema` the same field rule as `hash_table` (`SemanticHashingVisitor.hashed_field`):
about 10 lines, one test, and golden moves limited to `arrow.json`
`extension_schema.extension.*` entries of value-hashed types whose storage is not already
`large_binary` (none of the five there today: `uuid` is `large_binary`, `path` has no value
handler). Recommendation: not needed for the tag; leave it to ITL-980, or take the
10-line alignment in ITL-981 if the developer wants the public method consistent.
