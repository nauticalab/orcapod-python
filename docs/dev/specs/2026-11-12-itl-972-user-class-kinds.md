# Two kinds of user class: exact and structural (ITL-972)

**Status:** APPROVED with changes by the developer, November 12, 2026 (section "Answers
to the open points"). The sections below state the decided design.
**Issue:** ITL-972 (v0.3; tag target November 16, 2026; workshop November 17).
**Supersedes:** the identity model of PR #292 (ITL-881,
`superpowers/specs/2026-11-10-user-class-identity-v0.3.md`). PR #292 stays open and is not
merged; its branch is merged into this one and reworked.
**Base:** tip of `eywalker/itl-967-v03-hash-step-combine-the-hash-changing` (c4b1e4a7) with
`eywalker/itl-881-user-class-identity-impl` (c6837a15) merged in.

## Decided by the developer (November 12, 2026) — not reopened

| ID | Decision |
|---|---|
| D1 | An ordinary dataclass is named by its full module path plus class name. |
| D2 | An ordinary dataclass is specific to the class object. Only an Orcapod dataclass is independent of the class object. A notebook re-run of an ordinary class makes a new class that earlier pods refuse. |
| D3 | An Orcapod dataclass is identified by its name plus its fields and their types. The decorator optionally takes a name; the default is the class name. The logical type metadata holds name, fields and types, enough to rebuild the class. |
| D4 | The constructor of a structural class accepts an instance of any Orcapod dataclass or ordinary dataclass and yields a proper instance of the structural class. |
| D5 | Dataclasses only. Pydantic models are completely class based. A structural pydantic model is ITL-973. |
| D-order | Field order is part of identity (developer, November 11, 2026, for PR #292). Kept: I found no reason to change it. |

## Answers to the open points (developer, November 12, 2026)

The design was sent with seven open points. The developer changed three; the coordinator
read the rest as approved as recommended.

| ID | Question | Answer |
|---|---|---|
| O1 | Do an ordinary class's fields enter its identity? | **Yes** (as recommended). |
| O2 | Is a subclass column accepted at a parameter annotated with its ordinary base class? | **Accepted** (changed). "why refuse this? Per subclass rule of Python shouldn't this be accepted?" Exact class means no equivalence between *different* classes of the same description; it does not suspend subclassing. The subclass keeps its own identity and all its fields in storage. |
| O3 | Call form of the explicit conversion | **`P.from_instance(other)` only** (changed). The `P(other)` form is dropped: no `__init__` wrapper. |
| O4 | What `from_instance` accepts | Exactly the same field names and equal declared types; nested structural fields converted recursively (as recommended). |
| O5 | Is an ordinary dataclass column accepted at a structural parameter? | **No** (as recommended). |
| O6 | May `op.dataclass` add its marker and methods to the class it defines? | **Yes** (as recommended). |
| O7 | Do instances of two structural classes of one identity compare equal? | **Yes** (changed). "the whole point should be that structural classes identifies equality based on structural content as long as the two classes are of same structural identity." |
| — | Automatic conversion at pod inputs (proposed in §c.3) | **Removed** (changed). "let's not do auto conversion either — we should just check for structural identity as the basis of noting that they are compatible but simply pass it in as is without incurring some magical conversion." |

**Consequence, stated plainly.** Orcapod never converts a value from one class to
another. A function annotated with structural class `B` may receive an instance of
another class of the same identity (in memory: the producer's object) or the class bound
to the type or built from metadata (from a store). **A structural class promises its
fields, not its methods.** The test of the November 12 measurement therefore asserts
compatibility, field access and structural equality on both paths, not `type(p) is QNew`.

## Terms

- **Ordinary class:** a class made by `dataclasses.dataclass` (or
  `pydantic.dataclasses.dataclass`) and not by `orcapod.dataclass`; kind `dataclass`.
- **Pydantic model:** a `pydantic.BaseModel` subclass; kind `pydantic`.
- **Structural class:** a class made by `orcapod.dataclass`; kind `structural`.
- **Exact class:** the first two. Orcapod treats two exact classes as one type only when they
  are the same class object.

## Shared mechanics (all three kinds)

Carried over from PR #292, changed where marked.

**Identity record.** Canonical JSON (`sort_keys`, no whitespace, UTF-8, arrays keep order):

```json
{"fields": [["x", "int"], ["y", "float"]], "kind": "structural", "name": "Point"}
```

**Identifier (ITL-935 rule: extension name plus extension metadata).**

- Extension name and logical type name: `f"{name}@{sha256(canonical_json(record)).hexdigest()}"`.
  The digest stays in the name (developer, November 10, 2026, D-format of PR #292) for a
  technical reason that still holds: Arrow's and Polars' extension registries are keyed by
  name only, so two shapes of one name in one process (a class edited and its cell re-run)
  need two names. The digest is plain hex, no hash label.
- Extension metadata: `{"category": <category>, "identity": <record>}`, canonical JSON.
  Categories: `orcapod.dataclass` (ordinary), `orcapod.pydantic`, and new
  `orcapod.structural`.
- The whole identifier enters the type hash through `logical_type_identifier(lt).text`
  (ITL-967 code, unchanged). No user-class identifier has a `"version"` key: the record is
  the version. A change to the record format is a change of every user-class identifier.
- `readable_extension_metadata()` is the type's own metadata only.

**Type tokens (CHANGED from PR #292: structured, so a class can be rebuilt from them).**
A field's token is JSON, built from the annotation after `typing.get_type_hints`:

| Annotation | Token |
|---|---|
| `int`, `float`, `str`, `bool`, `bytes`, `None` | `"int"`, … `"None"` |
| stdlib class without a logical type (`datetime.datetime`) | its `canonical_type_name`, a string |
| type with a registered logical type (`Path`, `np.ndarray`, a third-party type) | `{"logical": "<LogicalTypeIdentifier.text>"}`: name **and** metadata, per the ITL-935 rule (PR #292 used the bare name) |
| `list[T]`, `set[T]`, `tuple[...]`, `dict[K, V]` | `{"generic": "list", "args": [<token>, …]}`, origin by canonical name |
| `Optional[T]`, unions | `{"union": [<token>, …]}`, members sorted by their canonical JSON, `"None"` last |
| `Literal[...]` | `{"literal": [<repr>, …]}`, sorted |
| another user class | `{"class": <that class's full identity record>}` |

A nested class contributes its **whole record**, not its identity string, so the outer
metadata describes every level without any class being importable.

**Stored fields:** dataclass fields with `init=True`; pydantic `model_fields`. Storage is a
struct of them in declared order. Not part of identity: defaults, methods, validators,
`init=False` fields, `ClassVar`s, dataclass options, base classes.

**Carried over unchanged from PR #292:** a subclass has its own identity and keeps its
fields (the registry never matches a user class by `issubclass`; audit finding R); field
order is identity; `TypeIdentityError` is not a `StoreReadError` and fails the run under
every `error_policy`; a nested class stored as `None` reads back; storage that differs only
in large versus non-large layout at any depth is accepted (Delta compaction);
`RecursiveTypeError` for a class that refers to itself; `tests/test_user_types`.

## a. Ordinary dataclass (exact class)

### a.1 Name

`f"{module}.{qualname}"`, the module through `canonical_module_name` (D1).

- A class in `__main__` is named `__main__.Point`. `__mp_main__` (a `multiprocessing` spawn
  child) is written `__main__`, so a worker process finds the same type. Consequence: a
  notebook class and the same class moved to `pkg/models.py` are different types
  (`__main__.Point` versus `pkg.models.Point`); two scripts that each define
  `__main__.Point` with the same fields share the type, as in v0.2.0.
- A local class (`<locals>` in the qualified name) is rejected, as today: it cannot be
  looked up by module path.

### a.2 Fields in or out (O1)

**Variant IN (DECIDED).** Record `{"kind": "dataclass", "name": "pkg.models.Point",
"fields": [...]}`.

- Hash: a field added, removed, retyped or reordered changes the identifier, so the schema
  hash, pipeline hash and table paths. The edited class computes into new tables; the old
  tables stay untouched and are no longer read by the pipeline.
- Storage: extension name `pkg.models.Point@<digest>`; metadata holds the record.
- It is what the ITL-935 rule requires: "change a type's identifier when, and only when, an
  existing value would be stored or hashed differently". A changed field changes the stored
  struct.
- One mechanism for all three kinds; the read check "stored fields equal the class's fields"
  is a comparison of two identifiers.

**Variant OUT (v0.2.0 behaviour; not chosen).** Record `{"kind", "name"}` only; extension name
`pkg.models.Point`, metadata `{"category":"orcapod.dataclass"}`, both exactly as on the
ITL-967 branch.

- Hash: the identifier does not move when fields change, so pipeline hash and table path
  stay. An edited class meets its old rows in the same table with another struct. Orcapod
  must then raise on write (Delta schema mismatch) and on read (`TypeIdentityError` naming
  the column) and the user must clear or move the store by hand. In v0.2.0 this case ended
  as a run that reported success with 0 rows (W6, W10 of the ITL-881 findings).
- Storage: one extension name for every shape of the class. Within one process (edit a
  field, re-run the cell) Arrow's registry holds the first shape; the second needs
  `pa.unregister_extension_type` and a replacement, after which tables of the first shape
  that pass through Polars or IPC are decoded with the wrong storage type. This is new,
  untested code.
- Golden: no existing dataclass or pydantic entry moves relative to the ITL-967 branch.
- It contradicts the ITL-935 rule quoted above.


### a.3 Rebinding and declared names: what remains of PR #292

**Removed, explicitly:**

- `op.register_type`, `orcapod/user_types.py`, declared names, `qualified_name=`.
- Name-only naming of `__main__` classes and the one-time `__main__` notice.
- Equivalence of two ordinary classes with the same identity in the schema check
  (`equivalent_hints` no longer applies to exact classes).
- The read lookup among classes "declared or seen under a name", and the `UserWarning`
  for the same identity from another module path.

**Kept, reduced to one rule:** when a class object with the same module path and name as an
earlier one is used (a cell re-run, `importlib.reload`, `%autoreload`), the registry binds
the identifier to the **newest** class, and the old class object keeps its own binding so
its instances still convert. Without this, re-running a cell raises "already bound" (the
W1 failure of v0.2.0). It gives no equivalence: the two classes remain different types for
every check, and an edited class has another identifier anyway.

**What the rule means for data already in memory (found while implementing).** An Arrow
table carries the identifier, not a class object, so the class of an Arrow-backed column
is whatever class the identifier is bound to *now*. After a class is defined again and
used, a source built before reports the new class as its column type and decodes its
values as the new class; a pod built for the old class refuses that source. That is the
module-path rule applied to memory as it applies to a store: data of `pkg.Point` is read
as `pkg.Point` as currently defined. Only a pod's own output schema holds a class object
(the annotation), so `A -> Old` feeding `B(p: New)` is refused naming the column.

### a.4 Enforcing the exact class

| Place | Rule |
|---|---|
| Schema compatibility (`schema_utils._accepts`, used at every pod input) | Two different exact classes are never equivalent, whatever their name and fields. Python's subtype rule still holds (O2): a column of a subclass is accepted at a parameter annotated with its base class, as `is_subhint` decides today. A class defined again is not a subclass of the earlier one, so a pod built for the earlier class refuses it; the error names the column. |
| Pod input | The value is passed as it is. No conversion. |
| Writing a value (`python_to_storage`) | `type(value) is python_type`, else `TypeError` naming both classes. A column's type is one class with one stored struct; a subclass instance has other fields, which would be lost (audit finding R). A subclass value belongs in a column of the subclass, which a base-class parameter accepts (O2). Found while implementing: `FunctionPodStream.as_table` converted a pod's output values by the types of the values, not by the pod's declared output types, so in memory a subclass instance returned for a base-class column was stored as the base without its own fields. It now converts the data columns by their declared types and raises like the stored path. |
| Reading stored values | The class is found by the stored name: the class bound in the reading context, else `sys.modules["__main__"]` for a `__main__.` name, else import by the longest module prefix. Its record, computed now, must equal the stored one. Not found: `ImportError`. Found with other fields: `TypeIdentityError` with the stored and the current fields. The database layer adds the column name (`register_discovered_logical_types` knows the column; the factory does not receive it). Never a fallback to the struct. |
| Subclass | Its own name, so its own identity, fields and tables. Never stored as its base. |
| Nested class | The outer record embeds the inner record (`{"class": …}`), so a change to the inner class changes the outer identifier. An ordinary class may have fields of any kind. |

**O2 has no storage or hashing problem.** A subclass column keeps the subclass's
identifier in the stream's schema, hashes and stored tables; the base-class parameter only
decides compatibility. The function receives subclass instances in memory and from a store
alike, because the stored column is read as the subclass.

The measured case (QOld, QNew; pod A `-> QOld`, pod B `p: QNew`) as ordinary classes: two
types, B refuses A's output at graph build, on both paths.

## b. `orcapod.dataclass` (structural)

### b.1 Public API

```python
import orcapod as op

@op.dataclass
class Point:
    x: int
    y: float

@op.dataclass(name="geo.Point", frozen=True)
class Point: ...
```

- Decorator only, bare or with keywords. It calls `dataclasses.dataclass` with the given
  options and returns a **real dataclass**: `dataclasses.is_dataclass`, `fields`, `asdict`,
  `replace`, pattern matching all work. It is declared with `typing.dataclass_transform`,
  so type checkers and editors see a dataclass.
- Options: every `dataclasses.dataclass` keyword is passed through (`repr`, `eq`, `order`,
  `unsafe_hash`, `frozen`, `match_args`, `kw_only`, `slots`, `weakref_slot`). `init=False`
  is rejected (`TypeError`): Orcapod builds instances through `__init__`.
- `name=`: optional. Default `cls.__name__`. Rules as in PR #292: non-empty, no `@`, `;` or
  whitespace, not starting with `orcapod.`, NFC-normalized.
- It raises `TypeError` for a class that is already a dataclass or a pydantic model: the
  kind is fixed where the class is defined, and there is no call form for classes you do
  not own (wrap them: define a structural class with the same fields and construct it).
- A local class is allowed: nothing is looked up by module path.
- What it adds to the class (O6): a marker attribute with the name, the classmethod
  `from_instance` (§c), and `__eq__` (§b.6). `__init__` is the one `dataclasses` generates.
- Field types allowed: scalars, types with a logical type, containers and unions of these,
  and other **structural** classes. A field typed by an ordinary dataclass or a pydantic
  model raises `TypeError` at first use, naming the field: a structural class must be
  rebuildable from its metadata, and an exact class is not. (The reverse is allowed.)

### b.2 Identifier

Record `{"kind": "structural", "name": <name>, "fields": [[field, token], …]}`, fields in
declared order (D-order). Extension name `Point@<digest>`, category `orcapod.structural`.
The module path is not part of it.

- Two structural classes with the same name, fields, types and order: **one type**. Same
  hashes, same tables, accepted for each other at every check.
- Different name, same fields: different types (D3: the name is part of it).
- Same name, different fields or order: different types that share a display name. They
  coexist in one process (two extension names).
- A structural class and an ordinary class with the same name and fields: different types
  (kind differs).

### b.3 Metadata layout and reconstruction

```json
{"category": "orcapod.structural",
 "identity": {"kind": "structural", "name": "Sample",
   "fields": [["id", "str"],
              ["where", {"class": {"kind": "structural", "name": "Point",
                                   "fields": [["x", "int"], ["y", "float"]]}}],
              ["tags", {"generic": "list", "args": ["str"]}],
              ["note", {"union": ["str", "None"]}],
              ["file", {"logical": "orcapod.path;7b2276657273696f6e223a317d"}]]}}
```

Rebuilding a Python annotation from a token: scalars by a fixed table; a stdlib canonical
name by import; `logical` through the reading context's registry (the extension name must
be registered and the metadata readable, else `LogicalTypeIdentifierError`); `generic`,
`union`, `literal` recursively; `class` with kind `structural` by the rule of §b.4. The
rebuilt class's record must equal the stored record, checked before it is used.

### b.4 Reading

1. The identifier is bound in the reading context: use the bound class.
2. A structural class with that name was defined in this process (a weak table filled by
   the decorator) and its record equals the stored one: use the most recently defined one.
3. Otherwise **synthesize** it: `dataclasses.make_dataclass` from the record, passed through
   `op.dataclass(name=<stored name>)`, module `orcapod.synthesized`. It is a normal
   structural class: attribute access, `asdict`, equality, and it converts to and from any
   class of the same shape. Nothing is imported and nothing can fail for a missing module.

If the user defines a matching class later, it becomes the bound class when first used
(same rebinding rule as §a.3). Which class a *read* returns therefore depends on what is
defined; what a *pod parameter* receives does not (§c.3).

### b.5 Schema compatibility

Two structural classes are accepted for each other when their records are equal, at the top
level and inside generics and unions. The value is then passed **as it is**: Orcapod checks
compatibility and converts nothing. Everything else about the check is as for any type.

### b.6 Instance equality (O7)

`op.dataclass` installs `__eq__`: two instances are equal when their classes are the same
class, or two structural classes with equal records, and their compared fields
(`field(compare=True)`) are equal. Fields typed by structural classes compare the same
way, also inside lists and other containers, because their own `__eq__` is used. An
instance of a class with another identity, or of any other type, is not equal
(`NotImplemented`).

- Not installed when `eq=False` is passed or the class body defines `__eq__`.
- `__hash__` stays what `dataclasses` generates (`frozen=True` or `unsafe_hash=True`): the
  hash of the field tuple, which does not depend on the class object, so equal instances of
  two classes have equal hashes. A class that is not hashable stays unhashable.
- One policy for every class of a type (coordinator, November 12, 2026, review round 2):
  equality and hashing cover exactly the stored fields. `op.dataclass` refuses
  `field(compare=False)`, `field(hash=...)` and an `init=False` field that takes part in
  comparison, with a `TypeError` naming the field. A class synthesized from metadata is
  hashable by its field values (`unsafe_hash=True`), so a defined and a synthesized class
  agree on `==` and `hash` by construction, and nothing about options enters the record.
- Ordering (`order=True`) is unchanged: `<` between two classes is not supported.
- A class synthesized from stored metadata has the same `__eq__` and `from_instance`.

## c. Explicit conversion (D4)

### c.1 Call form (O3)

`P.from_instance(other)`, a classmethod, is the only conversion. `P(...)` is the plain
field constructor that `dataclasses` generates. Nothing in Orcapod calls `from_instance`.

### c.2 What `from_instance` accepts (O4)

- `other` is an instance of a dataclass: ordinary or structural. A pydantic model, a dict
  or anything else raises `TypeError` (D5).
- `other`'s stored field names equal `P`'s exactly. A missing or an extra field raises
  `TypeError` naming the fields: dropping a field would lose data silently, and filling a
  default would invent it. Order is not compared (fields are copied by name).
- Declared field types must agree: equal annotations, or, for a field `P` types with a
  structural class, a dataclass instance that this rule converts recursively (also inside
  `list`, `set`, `tuple`, `dict` values and `Optional`). Anything else raises `TypeError`
  naming the field and both types. Values are not otherwise validated or coerced.
- The result is a new instance built by `P`'s own `__init__` (so `__post_init__` runs);
  `other` is not modified.

### c.3 No automatic conversion

Orcapod passes values as they are, at pod inputs and everywhere else. Writing a value to a
structural column requires an instance of a structural class with the column's identity
(else `TypeError` saying to build one with `from_instance`).

The measured case as structural classes: B accepts A's output on both paths. In memory it
receives the `QOld` object; from a store the class bound to the type. On both paths `p.x`
works and `p == QNew(p.x, p.y)` is true; `p.describe()` is not promised.

### c.4 Ordinary instance at a structural parameter (O5)

Not accepted by the schema check: the column's type is the ordinary class, which is another
type with another identifier. The user converts explicitly in a pod
(`def to_point(p: OldPoint) -> Point: return Point.from_instance(p)`).

## d. Pydantic

Exact class, the rules of §a with kind `pydantic` and category `orcapod.pydantic`: name is
module path plus class name; fields in the identity; no equivalence between different
classes in the schema check (subclassing as in §a.4); `type(value) is cls` on write; read by import and identifier comparison; a subclass is its
own type. No structural kind, no `op.register_type`. A pydantic instance is not accepted by
`from_instance`, and a structural class cannot have a pydantic field.

Kept from PR #292: the shared `UserClassLogicalType` base and factory, `model_fields` as
the stored fields in declared order, building values with the model's constructor (so
validators run on read), `None` for a null nested model, the `TypeIdentityError` handling.
`pydantic.dataclasses.dataclass` classes are ordinary dataclasses.

## e. What moves

Stored layouts (`pdb_v2`, `rdb_v2`, `tdb_v2`, `odb_v1`, `sdb_v1`), the data context key and
the released-manifest pin do not change: this is a change of logical type identifiers only.
The new category `orcapod.structural` is registered in `contexts/logical_types.json`.

**Relative to the ITL-967 branch** (dataclass and pydantic identifiers there: extension name
`module.qualname`, metadata `{"category": "orcapod.dataclass"}`):

| Cause | What moves |
|---|---|
| The record (fields) enters every exact class's identifier (O1) | Every pinned value that involves `GoldenPoint` or `GoldenModel`: in `semantic.json` `type.dataclass`, `type.pydantic`; in `schema.json` `multi_field.logical_mix` and `single_field.{dataclass,pydantic}.{arrow_type,schema_hash}`; in `datagram.json` `{dataclass,pydantic}.{content_hash,pipeline_hash,system_tag_columns}`, the two rows' `data_hash`, and the keys that embed the moved pipeline hash. That was 25 entries when measured for PR #292 on `main`; I re-measure against the ITL-967 tip before regenerating and list each entry in the PR. For users: every pipeline with a dataclass or pydantic column gets new pipeline hashes and table paths. |
| New fixtures | Added keys only: structural classes (default name, explicit name, nested, container and optional fields, field-order twins with different values, two classes of one identity with equal values), an ordinary subclass, an ordinary class with a structural field. |
| Stored extension names and metadata | Exact classes: `module.qualname` becomes `module.qualname@<digest>`, metadata gains `identity`. Structural: new. A column written by v0.2.0 or by the ITL-967 branch read directly raises `TypeIdentityError` (no identity record), as in PR #292. |

**Measured (November 12, 2026)**, golden files regenerated, compared key by key with the
golden files of the ITL-967 tip a0a3aeac (which contains `main` up to #311). Before the
first regeneration the golden test failed on exactly the 25 entries below (plus the
unpinned additions).

| File | Changed values | Replaced keys | Added keys |
|---|---|---|---|
| `semantic.json` | 2: `type.dataclass`, `type.pydantic` | 0 | 10: `type.{structural, structural_nested, structural_with_array, structural_twin_ab, structural_twin_ba, structural_same_identity, pydantic_optional, dataclass_subclass, dataclass_with_structural, list_dataclass}` |
| `schema.json` | 5: `multi_field.logical_mix`, `single_field.{dataclass,pydantic}.{arrow_type,schema_hash}` | 0 | 20: `single_field.<the 10 fixtures above>.{arrow_type,schema_hash}` |
| `datagram.json` | 10: `{dataclass,pydantic}.{content_hash, pipeline_hash, system_tag_columns}` and both rows' `data_hash` | 8: `{dataclass,pydantic}.row[r1/r2]._tag_record_id::<pipeline hash>` and `._tag_source_id::<pipeline hash>` (the key embeds the moved pipeline hash; 8 keys disappear, 8 appear) | 89: the 8 replacement keys and 9 new fixtures × 9 keys |
| `arrow.json` | 0 | 0 | 0 |
| `pipeline.json` | 0 | 0 | 0 |

Cause of all 25 moved entries: the identity record (kind, module-path name, ordered fields)
enters the identifier of `GoldenPoint` and `GoldenModel`. `structural` and
`structural_same_identity` (two classes of one identity) pin equal type, schema, pipeline
and row data hashes and equal system-tag column names (their source and record ids differ,
because each fixture's source has its own id); the field-order twins pin different values.

**Relative to PR #292** (never merged, so no stored data exists): every user-class
identifier moves, for three causes: `__main__` classes are named `__main__.X` instead of
`X`; tokens are structured, a nested class embeds its record, and a logical-type field uses
the whole identifier instead of the bare name; declared names no longer exist. PR #292's
golden fixtures that use `register_type` (`golden.Declared`, `golden.Inner`/`Outer`,
`golden.WithArray`, `golden.Twin`) are replaced by structural fixtures; none of PR #292's
golden values survives.

## Merge conflicts (ITL-967 tip + PR #292 branch)

Six files. Proposed resolutions:

| File | Conflict | Resolution |
|---|---|---|
| `databases/delta_lake_databases.py` | import list | union of both (obvious) |
| `logical_types/database_hooks.py` | ITL-967 walks per column and adds the column to `LogicalTypeIdentifierError`; PR #292 walks the whole schema and searches the columns afterwards for `TypeIdentityError`/`ImportError` | ITL-967's per-column loop, catching all three errors and adding the column there; PR #292's column search is dropped |
| `logical_types/registry.py` (`_deserialize`) | metadata mismatch: ITL-967 raises `LogicalTypeIdentifierError` against `readable_metadata`; PR #292 raises `TypeIdentityError` on inequality | ITL-967's check. PR #292's other change in that function (large/non-large tolerance at any depth; storage mismatch as `TypeIdentityError`) merged without conflict and is kept |
| `golden/data/{datagram,schema,semantic}.json` | both sides regenerated | take the ITL-967 side, regenerate once after implementation |

## f. Risks, and the order of cuts

Risks:

1. Four days to the tag for a new public API and a hash change. The identity mechanics are
   PR #292's tested code; the new parts are the tokens, the synthesized class, the
   constructor wrapper and the pod-input helper.
2. Operators that compare Python types across streams (`MergeJoin`, `Join` on shared keys)
   must use the same structural equivalence as the schema check, or two classes of one
   identity are refused there. I audit every Python-type comparison PR #292 touched.
3. `slots=True` makes `dataclasses` build a new class; the marker and methods are added to
   that class. Tested.
4. Writing is stricter than v0.2.0: a value is stored only as its own class (no duck-typed
   values, no subclass instance in a base-class column). Intended by D2 but can surprise.
5. With no conversion, a function annotated with a structural class can receive an instance
   without that class's methods. Documented as the contract of a structural class.
6. A synthesized class has no methods. Reading without the class defined gives data, not
   behaviour; documented.

Cuts, first cut first (each leaves a working, documented state):

1. `Literal` tokens in structural classes (rejected with a clear error).
2. Recursive conversion of nested structural fields in `from_instance` (the nested value
   must already be of the declared class).

(`P(other)` and the automatic conversion at pod inputs, first on this list in the proposal,
were removed by decision.)

Not cut: the two kinds, no equivalence of exact classes, the structural identifier and
metadata, structural equality, reading without the class (synthesis), the regression test
for the measured case, docs, changelog, golden list by cause.

Out of scope: structural pydantic (ITL-973); migration of v0.2 data; computed fields;
Enum, NamedTuple, TypedDict.
