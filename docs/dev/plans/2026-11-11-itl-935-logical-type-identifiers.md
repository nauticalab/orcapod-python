# ITL-935: logical type identifiers (Arrow extension name + extension metadata)

Status: **implemented (November 11, 2026); PR open against main, stacked on #309.**
The Step 1 plan below is kept as written. Where it differs from what was built, the section
"Decisions and outcome" at the end governs.

Branch `eywalker/itl-935-design-make-the-logical-type-version`, stacked on PR #309
(`origin/eywalker/itl-910-adopt-starfix-05-hash-format-2-in-orcapod` at `b88f7eb3`).
Design it follows: ITL-938, section 5.5 of
`superpowers/specs/2026-11-11-itl-938-versioning-system-design.md` on
`origin/eywalker/itl-938-design-one-coherent-versioning-system-for` (re-read before finalising).

All facts below were obtained by running code on this branch (Python 3.12, starfix 0.5.0).

## (a) Inventory

There are two hashes that could be called "the schema hash":

- **Python schema hash** (label `semantic_v0.1`): `SchemaHandler` hashes `{field: hash of the
  Python type}`; `TypeObjectHandler` turns a logical type into the string
  `type:<logical_type_name>` (`hashing/semantic_hashing/builtin_handlers.py:115`). Function
  signatures use the same name through `canonical_annotation_str` (`hashing/hash_utils.py:77`).
  **This is the hash that is in stored paths** (`…/schema:semantic_v0.1:<hash>/pdb_v1`, and the
  function hash in `…/<label>/semantic_v0.1:<hash>/…`). It contains the name only.
- **Arrow schema / table hash** (starfix, label `arrow_v0.2`): Orcapod turns an extension column
  into its storage type plus `ARROW:extension:name` and `ARROW:extension:metadata` field
  metadata (`arrow_utils.normalize_extension_columns`) and calls starfix with
  `include_metadata=True`. starfix 0.5.0 **does** hash the metadata: the same column with
  metadata `b""`, `{"version":1}`, `{"version":2}` gives `…63a79763`, `…3e9c22e6`,
  `…707e3b90`. A missing metadata key and an empty one hash the same (`…63a79763`).

| Type | Extension name | Extension metadata today | Storage | Value hash of a cell |
|---|---|---|---|---|
| `pathlib.Path` | `orcapod.path` | empty | `large_string` | no value hasher: the string, hashed by starfix with name **and metadata** in the schema digest |
| `upath.UPath` | `orcapod.upath` | empty | `large_string` | same as path |
| `uuid.UUID` | `orcapod.uuid` | empty | `large_binary` | token `<name>::semantic_v0.1:<digest>` |
| `orcapod.File` | `orcapod.file` | empty | `large_string` | token `<name>::sha256:<digest>` |
| `orcapod.Directory` | `orcapod.directory` | empty | `large_string` | token `<name>::merkle_sha256:<digest>` |
| `numpy.ndarray` | `orcapod.ndarray` | `{"version":1}` | `struct<dtype, shape, data>` | token `<name>::ndarray_v1:<digest>` |
| `pandas.DataFrame` | `pandas.dataframe` | empty | `large_binary` | token `<name>::arrow_v0.2:<digest>` |
| `pandas.Series` | `pandas.series` | empty | `large_binary` | token, as DataFrame |
| `list[T]` / `set[T]` of a logical `T` | `list[<T name>]` / `set[<T name>]` | `{"category": "list", "element_ext_name": "<T name>", "element_ext_metadata": <T metadata as text or null>}` | `large_list<T storage>` | per element, as `T` |
| dataclass (factory) | module-qualified class name (PR #292: `name@fingerprint`) | `{"category": "orcapod.dataclass"}` | struct of the fields | struct fields, by starfix |
| pydantic model (factory) | same | `{"category": "orcapod.pydantic"}` | struct | same |
| `polars.Expr`, `pa.Table` | not logical types (value hashers only; not storable as a column) | — | — | — |

The token of a value-hashed cell contains the extension name and the hasher's label, **not**
the metadata.

Where the pair is stored: in memory it is the `pa.ExtensionType` (name, and
`__arrow_ext_serialize__()` for the metadata). In Parquet and Delta files it is the
`ARROW:extension:name` / `ARROW:extension:metadata` keys of the column (observed in the
result table of a `Path`-returning pod). Reads go through `ExtensionAwareDatabase`
(`register_discovered_logical_types`, then `apply_logical_types`).

Findings from the inventory:

1. **Silent misread exists today.** A column read as storage with field metadata
   `orcapod.path` + `{"version":9}` is wrapped as the current `orcapod.path` by
   `apply_logical_types`: it looks the type up by name and ignores the metadata.
2. When pyarrow itself resolves the type (name registered at read time), a metadata mismatch
   raises `ValueError: Arrow extension type 'orcapod.path': expected metadata b'' but got
   b'{"version":9}'`: explicit, but no column, and it means that **once a built-in writes
   `{"version":1}`, every existing file of that type (empty metadata) fails to read** unless the
   type says it also reads the empty form.
3. A dataclass output column in a function result table was stored as a plain
   `struct<x, y>` with no extension keys (probe: `-> Pt` pod, `_result/mk/...`). Out of scope
   (PR #292's area); reported, not fixed.

## (b) The mechanism

**Declaring.** A logical type's identifier is exactly what its Arrow extension type carries:
`get_arrow_extension_type().extension_name` and `.__arrow_ext_serialize__()`. That stays the
single place of declaration; no second field that could disagree. Orcapod reads it through
one helper, `logical_type_identifier(lt) -> LogicalTypeIdentifier(name: str, metadata: bytes)`.
Third-party types need nothing new to *have* an identifier.

**Reading older forms.** One new protocol member, with a default in `BaseLogicalType`:

```python
def readable_extension_metadata(self) -> tuple[bytes, ...]:
    """Every stored extension metadata value this type can read. Default: only its own."""
```

Orcapod's only rule is byte equality against that tuple; what the bytes mean is the
implementer's business. `make_arrow_extension_type(name, storage, metadata, also_reads=())`
accepts those values in `__arrow_ext_deserialize__` and returns the current type.

**Built-in convention.** `{"version":1}` (the exact bytes the array format already writes),
for path, upath, uuid, file, directory, pandas dataframe and pandas series. Not imposed on
external types. List/set wrappers gain `"version":1` in their own metadata (question 2).
Dataclass/pydantic are left to PR #292.

**Finding the type on read.** Lookup stays by extension name (one logical type per name), then
the stored metadata is checked against `readable_extension_metadata()`. On no match:
`LogicalTypeIdentifierError` naming the column, the stored `(name, metadata)` and the
identifiers this installation reads for that name. Both read channels get the check
(`apply_logical_types` knows the field; the pyarrow channel raises without a column and the
database read wraps it with the column it finds in the file schema). An unknown name with
no factory keeps today's explicit error. Never a fallback to the storage type or to the
current type.

**"No metadata" on read (recommended: readable as the first identifier).** The built-ins
that wrote empty metadata declare `readable_extension_metadata() == (b'{"version":1}', b"")`:
version 1 *is* the layout v0.2 wrote, byte for byte, so nothing is converted and nothing can
be misread. In memory the column has the current identifier. This matters for tables a user
points Orcapod at (a Delta or Parquet source written by v0.2 or a v0.3 pre-release); Orcapod's
own result tables of such types are at other paths (section d) and are simply not found:
recomputed and kept, as in #309.

## (c) The schema hash

- Python schema hash: `TypeObjectHandler` and `canonical_annotation_str` emit the whole
  identifier instead of the name. Proposed text: `<extension name>;<lowercase hex of the
  metadata bytes>` (so `type:orcapod.path;7b2276657273696f6e223a317d`), always, including
  empty metadata (`type:x.y;`). Hex because the metadata is opaque bytes; the last `;`
  splits unambiguously. `list[Path]` and friends follow because they hash their argument types.
- The extension name, not `logical_type_name`, is used (decision 1). They are equal for
  every type shipped.
- Arrow schema/table hash: already covers the pair (starfix `include_metadata`). No change to
  the algorithm, no new label. Nullability and large-type normalisation are untouched: the
  identifier is added next to them, and types with no logical type hash exactly as before.

## (d) What moves

- **Paths**: every table whose function signature or schema has a column of a logical type.
  Probe (`shift(p: Path) -> Path`), before:
  `probe/shift/semantic_v0.1:e7d5af65…/v0/python.function.v0/schema:semantic_v0.1:1eddb0e4…/pdb_v1`
  and `probe/_result/shift/semantic_v0.1:e7d5af65…/…`; after: both hashes differ (values go
  in this file once implemented). A pod over plain types (`plain(id2: int) -> int`,
  `…db3fd817…`, `schema:…84f155b4…`) must be byte-identical before and after: a test.
- **Stored column types**: the seven built-ins write `{"version":1}`; list/set wrappers of
  any logical type write the element's new metadata (and their own version).
- **Row hashes**: `Path`/`UPath` columns (starfix sees the metadata), list wrappers, and
  dataclass/pydantic fields only if they contain such a type. Value-hashed types (uuid, file,
  directory, ndarray, pandas) keep their cell tokens (question 5).
- **Golden values** (771 leaves in 5 files): estimate 60 to 120 entries, in `schema.json`,
  `semantic.json` (type and function hashes naming logical types), `datagram.json` (path
  columns), few in `arrow.json` and `pipeline.json`. One regeneration, per-type table in the PR.
- **Nothing moves** for pipelines without logical types.
- **A v0.3 pre-release store** seen by final v0.3: tables with logical-type columns are at old
  paths, so not found, recomputed, old tables kept untouched. Old files remain readable when
  opened explicitly.

## (e) Open PRs

- **#300 torch tensor**: already writes `{"version":1}` through `make_arrow_extension_type`;
  conforms. Must rebase on the `also_reads` signature and regenerate its own golden entries
  after this lands; its deferred-registration lookup by extension name must keep the metadata
  check.
- **#286 Polars DataFrame/Series**: `polars.dataframe` / `polars.series` are created with
  empty metadata. Must pass `{"version":1}` (new types, so no `also_reads`), and add the
  identifier tests.
- **ITL-897 branches (pandas column order, `pa.Table` value identity)**: they change what a
  pandas value hashes to. By decision 1 that is a change of identifier for
  `pandas.dataframe`/`pandas.series`. If they land before the v0.3 tag they define version 1
  and need no bump; after the tag they must become `{"version":2}` and decide whether to read
  version 1.
- **#292 user classes**: identity moves into the extension name (`name@fingerprint`), metadata
  `{"category", "identity"}`. That is a complete identifier under this mechanism and the
  fingerprint enters the schema hash through the name; the metadata will now also be hashed,
  so its metadata must be canonical JSON (stable key order). It should add `"version":1` for
  the struct layout, and rebase on the registry changes.

## (f) Risks, and what to defer (last first)

1. Golden churn collides with #309 and the PRs above: land in the same hash step, one
   regeneration.
2. The pyarrow read channel raises inside pyarrow; naming the column needs a wrap in the
   database read path. If time runs out: ship the explicit error without the column there.
3. Defer first: docs polish; then `"version"` on list/set wrappers; then the `readable_…`
   member (ship exact match plus the empty-metadata rule for built-ins only). Not deferrable:
   built-ins writing their identifier, and the pair in the Python schema hash.

## Questions sent to the developer

1. Built-in metadata is `{"version":1}`, same bytes as the array format. (Recommended.)
2. List/set wrappers also get `"version":1` in their own metadata. (Recommended.)
3. API: identifier declared only by the Arrow extension type; one new member
   `readable_extension_metadata()`, matched by byte equality. (Recommended; alternative: a
   predicate `reads(metadata) -> bool`, which cannot list what is readable in the error.)
4. Data with no metadata is read as version 1 for the seven built-ins. (Recommended.)
5. Value-hash tokens stay `<name>::<label>:<digest>` without the metadata. (Recommended: a
   hashing change changes the label or digest anyway; a layout-only change should not move a
   value hash.) Consequence: `Path`/`UPath` row hashes move, token types do not.
6. Text of the identifier in the Python schema hash: `<name>;<hex metadata>`, always.
7. No special handling of v0.3 pre-release stores.

## Step 2 (after approval)

Tests first (public interface): each built-in writes its identifier; metadata-only change
moves the schema hash and the path; unknown identifier on read raises the explicit error;
empty metadata handled per the approved rule; Delta round trip; plain pipeline unchanged.
Then implementation, one golden regeneration with a per-type table, CHANGELOG (0.3.0,
"Breaking changes and migration"), docs ("Logical type identifiers" for implementers),
DESIGN_ISSUES entry, CLAUDE.md + `.zed/rules` if a rule is added.

## Decisions and outcome (November 11, 2026)

Developer's answers, through the coordinator:

| Question | Answer |
|---|---|
| 1. Built-in metadata `{"version":1}` | yes |
| 2. List/set wrappers get `"version": 1` | **no** (final): the native containers `list[T]` / `set[T]` carry no version of their own; their identifier is the container kind plus the element's identifier; a change to them is a data context change |
| 3. `readable_extension_metadata()`, byte equality, `LogicalTypeIdentifierError` | yes |
| 4. Empty metadata read as version 1 for the seven built-ins | yes |
| 5. Cell token without metadata | **no**: the token carries the whole identifier, `utf8("<name>;<hex metadata>") + b"::" + tail`; the name is written as is (no `.` to `:`) |
| 6. Identifier text `<name>;<hex metadata>`, always | yes; an extension name may not contain `;` |
| 7. No special handling of v0.3 pre-release stores | yes |
| Wrapper identifier in the Python schema hash (found during implementation) | **no**: generics keep hashing as origin and arguments. Implemented once ("Option A"), then reverted on the developer's decision |
| `[]` vs `[None]` collision in wrapper cells (reported by the ITL-942 worker) | fixed here: element count, then per element a null marker or a length-prefixed token |

Measured path of the probe pod `shift(p: Path) -> Path`:

| | function hash | schema hash |
|---|---|---|
| before | `e7d5af653505598a…` | `1eddb0e41260879752…` |
| after | `5b5a49597b652bdd…` | `2d2936f8871f994f…` |
| `plain(id2: int) -> int`, before and after | `db3fd817dc7cf10d…` | `84f155b4a2ea98ab…` |

Golden values moved against the #309 base (`b88f7eb3`): 70 of 771 leaves (arrow 4, datagram 44
plus 32 keys whose name contains a moved hash, schema 14, semantic 8, pipeline 0), every one in
a group with a logical-type column. Outside the golden directory: 6 entries of
`tests/test_hashing/hash_samples/schema_hash_golden.json` and 3 pinned values in
`tests/test_hashing/test_canonical_type_names.py`.

Merge points with sibling branches:

- ITL-945 (labels out of hash inputs) edits the tail of the scalar cell token in
  `hashing/visitors.py` (`content_hash.to_prefixed_digest()`); this branch edits the prefix
  line (`type_name = arrow_type_identifier(extension_type).text`) and the container element
  encoding (`_encode_container_elements`).
- ITL-944 (logical-type list out of the data context file): this branch does not change
  `contexts/data/v0.1.json`.

Out-of-scope findings (not fixed): a dataclass output column of a function result table is
stored as a plain struct without extension keys; a `DataContext` object passed to a source is
not used by the source's inner stream (it resolves the registered context of the same key),
so a custom context with extra logical types cannot drive a pipeline; a third-party logical
type registered for a generic alias is not in the schema hash (origin and arguments only).
