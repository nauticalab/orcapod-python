# ITL-611: `set[primitive]` as a native container — design

Status: proposal, awaiting coordinator / developer decision (2026-11-14).
Measured on `main` 810d1b03 with a probe (job on Delta and in-memory, sync and async,
`as_source()`, a fresh-process `PipelineJob.load`), then with a throwaway prototype of
the proposal (not committed; kept as a patch outside the repo).

## (a) Measured on main

`set[T]` returned by a function pod, read back:

| `T` | stored Arrow type | identifier | read with schema: node / downstream pod `set[T]` | `as_source()` (same process) | loaded job, fresh process |
|---|---|---|---|---|---|
| `int` | `large_list<int64>` | none (plain) | set / **list** | **list**, schema says `set[int]` | schema `list[int]`, values list |
| `str` | `large_list<large_string>` | none | set / **list** | **list** | `list[str]` |
| `float` | `large_list<double>` | none | set / **list** | **list** | `list[float]` |
| `bool` | `large_list<bool>` | none | set / **list** | **list** | `list[bool]` |
| `bytes` | `large_list<large_binary>` | none | set / **list** | **list** | `list[bytes]` |
| `datetime` | `large_list<timestamp[us, tz=UTC]>` | none | set / **list** | **list** | `list[datetime]` |
| `date` | `large_list<date32[day]>` | none | set / **list** | **list** | `list[date]` |
| `Path` | `extension<set[orcapod.path]>` | `{"category": "set", "element_ext_name": "orcapod.path", "element_ext_metadata": "{\"version\":1}"}` | set / set | set | **schema cannot be restored** (warning); values `list[str]` |
| `UUID` | `extension<set[orcapod.uuid]>` | same shape | set / set | set | **cannot restore**; values `list[bytes]`; **row hash differs** from in-process |
| user dataclass | `extension<set[mod.Pt@<sha>]>` | same shape | set / set | set | **cannot restore**; values `list[dict]` |
| `tuple[int, str]` | (pod output refused: `Unsupported annotation: tuple[int, str]`) | | | | |
| `list[set[int]]` | `large_list<large_list<int64>>` | none | list of sets / **list of lists** | list of lists | list of lists |
| `set[int] \| None` | `large_list<int64>` | none | set / **list** | **list** | `list[int] \| None` |
| `frozenset[int]` | refused: `Unsupported annotation: frozenset[int]` | | | | |
| `list[int]` (control) | `large_list<int64>` | none | list / list | list | list |

Hashes on main:

- **Row hash of `{1, 2, 3, 10}` as `set[int]` equals the row hash of `[1, 2, 3, 10]` as
  `list[int]`** (`arrow_v1:0000028fcaa6…`): the set identity is in no hash.
- Type hash of `set[int]` (semantic, `GenericAliasHandler`) differs from `list[int]`, so
  schema and pipeline hashes already distinguish them.
- Semantic value hash of a Python set (`hash_object`) is order-independent (canonical JSON
  text order, ITL-942); `set[datetime]`/`set[date]`/`set[Path]` values have no
  `hash_object` handler (unrelated to storage).
- `set[UUID]` in a fresh process: the stored view loses the extension and the row hash
  changes (`…48f810…` in process, `…f5a773…` fresh). If the type is registered before
  `PipelineJob.load`, the fresh read is a set and the hash matches: the gap is only that
  `serialization.parse_arrow_type_string` resolves an extension name already registered
  in this process and never rebuilds a native container from its identifier.

Storage order on main: plain `set[T]` is ordered by `canonical_order.sort_set_elements`
(None first; numeric; str by code point; bytes unsigned bytewise; date/datetime by
instant; NaN, incomparable or duplicate storage values raise `CanonicalOrderError`).
`set[logical]` is ordered by `ListLogicalType.python_to_storage` (natural sort, `repr`
fallback; ITL-922 covers its NaN gap). Measured: `{3,1,2,10}` → `[1,2,3,10]`;
`{"b","a","10","2"}` → `["10","2","a","b"]`; `{b"y",b"x",b"\xff"}` → `[b"x",b"y",b"\xff"]`;
`{True, 2}` as `set[int]` → `[1, 2]`; `{1.0, nan}` → the row fails.

## (b) Proposal

`set[T]` where `T` has no logical type is stored as the same native container as
`set[logical]`: a `ListLogicalType` of container kind `set`, storage
`large_list<element>`, no `version` key. Its identifier is the container kind plus the
element's identifier; for a primitive element the element's identifier is its
large-normalized Arrow type text (`str(normalize_to_large_types(element_type))`):

| Python | extension name | extension metadata (exact bytes) |
|---|---|---|
| `set[int]` | `set[int64]` | `{"category": "set", "element_kind": "arrow"}` |
| `set[str]` | `set[large_string]` | `{"category": "set", "element_kind": "arrow"}` |
| `set[bytes]` | `set[large_binary]` | `{"category": "set", "element_kind": "arrow"}` |
| `set[float]` | `set[double]` | `{"category": "set", "element_kind": "arrow"}` |
| `set[datetime]` | `set[timestamp[us, tz=UTC]]` | `{"category": "set", "element_kind": "arrow"}` |
| `list[set[int]]` | `list[set[int64]]` | `{"category": "list", "element_ext_name": "set[int64]", "element_ext_metadata": "{\"category\": \"set\", \"element_kind\": \"arrow\"}"}` |

`element_kind: arrow` (not `element_ext_name`) marks a primitive element, so a
logical type can never be confused with an Arrow type name. Developer decision, November 15,
2026: the element type is in the name only (the registry key, unique per element type); the
metadata does not repeat it, and a reader takes the element from the storage. (The first
form, `element_arrow_type`, was never released.) JSON is written as the
existing container metadata is (`json.dumps`, insertion order).

- `list[primitive]` is unchanged (plain `large_list`); a regression test pins it.
- `set[T | None]` gets the same identifier as `set[T]`, as `set[Path | None]` does today
  (element nullability is not in a native container's identifier; see ITL-1136, which this
  does not change).
- Element order: unchanged, `sort_set_elements` on the converted storage values, so the
  stored bytes of every value are as before and only the type identity moves.
- Reading: `storage_to_python` converts each element with the converter and returns a
  `set`. `ListLogicalTypeFactory.reconstruct_from_arrow` rebuilds the type from the
  storage's element type and refuses (`LogicalTypeIdentifierError`) a storage that is not a
  list or whose set would have another name than the stored one.
- Hashing: the `SemanticHashingVisitor` value-hashes a native container only when its
  innermost element is a logical type; a container of a primitive element is hashed as
  stored, with its identifier in the field metadata (starfix `include_metadata`).
  Without this, `bytes` (which has a semantic value handler) would be value-hashed per
  element in process and hashed as stored in a fresh read: measured mismatch in the
  prototype.

## (c) What moves

Moves (measured with the prototype):

- Stored Arrow type of every `set[primitive]` column (top level, `Optional`, element of a
  `list`), sync and async, Delta and in-memory.
- Row (data) hash of every row holding such a column: all nine measured cases moved. So:
  function-node input hashes of rows that take a set input (cache miss), the default
  `source_id` and source content hash of an in-memory source with such a column.

Unchanged (measured):

- Type, schema and pipeline hashes (the Python type `set[int]` is unchanged and hashed as a
  generic alias): node pipeline hashes identical on main and prototype.
- Semantic value hash of a Python set; anything about `list[primitive]` and
  `set[logical]`.
- User-class identifiers whose fields are `set[...]` (fields stay plain storage inside the
  struct; identity record is built from Python types).
- **Golden: all 1282 entries pass unchanged with the prototype.** No golden entry pins a
  `set[primitive]` column. Related entries, none move: `semantic: value.set.ints`,
  `value.frozenset.ints`, `value.set.strs`, `value.set.mixed_str_equal` (Python-value
  hashes); `semantic: type.set_uuid`, `schema: single_field.set_uuid.*`,
  `datagram: set_uuid.*`, `arrow: extension_no_values.*.set_of_uuid` (logical element);
  `pipeline: column_selection.select_data_spelling.set` (a Python set of column names).
  Proposal: add golden entries (additions only) for `set[int]`, `set[str]`, `set[bytes]`
  arrow type, schema hash and one datagram row each.

Data context: CLAUDE.md says a change to how native containers are stored or hashed is a
data context change. `std:v1` is not released (`released_in: null`), so the change lands
inside `std:v1` with no new manifest and no key change.

## (d) A v0.3 development store holding `set[int]` as a plain list

Measured (main writes a Delta store, prototype code runs over it):

- Same input again: cache hit; the node serves the stored plain list. Values are `list`,
  declared type `set[int]`; downstream pod gets a `list`. No error.
- One new input: the run raises `ArrowInvalid: Schema ... was different` when the stored
  plain rows and the new extension rows are concatenated; the new row was already appended
  (Delta's append check compares storage only), so the table now mixes both forms.
- No `LogicalTypeIdentifierError`: the stored column has no extension metadata, so the
  logical type layout rule has nothing to refuse.

Options:

1. **Leave stores; document.** Only stores from unreleased v0.3 development builds hold
   this layout (v0.3 does not read 0.2.0 tables at all). Changelog "Breaking changes and
   migration": clear such nodes. Cost: none; leaves the silent `list` and the crash.
2. **Refuse explicitly.** Where a stored column is plain storage but the node declares a
   native container (read of stored results, Delta/connector append check), raise naming
   the column, the stored type and the expected identifier, telling the user to clear
   the node. Cost: two or three code paths, more tests.
3. Read the plain list as the set: forbidden by "never wrap a stored column by name alone".

Recommendation: 1, unless the developer wants 2.

## (e) `as_source()` / `DerivedSource` reading a set as a list

Fixed by the proposal in process (measured). In a fresh process / loaded job the change
would otherwise regress `set[int]` from "reads as `list[int]`" to "cannot restore" (as
`set[Path]` already fails). Proposal: in scope, make `parse_arrow_type_string` rebuild an
unregistered native container from its identifier through the registered factory
(`register_logical_type_from_arrow_metadata`). That also fixes the same pre-existing gap
for `set[Path]`, `set[UUID]`, `list[Path]`.

## (f) `frozenset[T]`

1. Leave refused (today: `ValueError: Unsupported annotation: frozenset[int]`). Recommended.
2. Own native container kind `frozenset` (`frozenset[int64]`), reads back a `frozenset`.
3. Store as `set`: a pod annotated `frozenset[int]` would receive a `set`; rejected by type
   strictness.

## (g) Order

See (a): unchanged, `sort_set_elements` per element type; NaN, incomparable and duplicate
storage values raise `CanonicalOrderError` (the row fails under the error policy). A
Python set cannot hold both `1` and `True` or both `0.0` and `-0.0`; in `set[int]` a
`bool` is converted to `int` before ordering. Two equal sets therefore store the same
list, and the row hash (starfix over the stored list plus the identifier) is equal.

## Out-of-scope findings

- `dict[K, set[primitive]]`: works today (plain). Under the proposal the value would be an
  extension inside a struct, which Arrow cannot hold; `dict[K, set[Path]]` already raises.
  Proposal: keep the plain storage inside a dict value (identity lost there, as today;
  PLT-1732 covers dict of logical types) rather than start raising.
- `tuple[int, str]` (heterogeneous tuple) as a pod output is refused by
  `register_python_class` though `python_type_to_arrow_type` supports it.
- Loaded-job reads of native containers of logical types (above, (e)).
