# Identity of a user class in v0.3 (ITL-881)

**Status:** implemented on this branch (PR #292). Approved by the coordinator November 10,
2026; P1, the identity-string format (fold, full-length hash) and the dropped naming switch
decided by the developer the same day.
**Revised November 11, 2026 (developer):** declared field **order is part of the identity**,
for dataclasses and pydantic models. This reverses the November 10 ruling "not field order".
Criterion: take the type's own equality seriously (a dataclass compares its fields as an
ordered tuple; a pydantic model dumps, iterates and serializes in declared order); a finer
identity can cost a recompute but cannot be incorrect. The sections below state the revised
rule.
**Issue:** ITL-881 (v0.3, a public release; tag target November 16, 2026).
**Builds on:** the findings in PR #288 (`superpowers/specs/2026-11-10-user-class-identity.md`,
experiments E01–E08). "W1"–"W15" are the workshop scenarios from §3.4 of that document.

Each decision is marked **CONFIRMED** (the developer decided it, November 10, 2026) or
**PENDING** (the default this spec plans with; each pending item is a small, local change).

| ID | Decision | Status |
|---|---|---|
| D-name | Nominal identity with a declared name; `__main__` classes default to the name only | CONFIRMED: developer decision 1 |
| D-fields | Field names, field types and declared field order are part of the identity | CONFIRMED: developer decision 2, revised November 11, 2026 (order is identity) |
| D-computed | Computed fields are not part of the identity in v0.3 | CONFIRMED as deferred: developer decision 2 defers it; v0.3 keeps today's stored fields |
| D-kind | Dataclass and pydantic are distinct kinds | CONFIRMED: developer decision 3 |
| D-rebind | Same identity re-registered: rebind silently | CONFIRMED: developer answer to S1 |
| D-raise | Stored shape disagreeing with registered shape raises, in v0.3 | CONFIRMED: developer answer to S2 |
| D-doc | A page documenting identity | CONFIRMED: developer answer to S4 |
| P1 | A field typed by another user class carries that class's identity as that class was registered | CONFIRMED: developer decision, November 10, 2026 |
| D-cycle | A self-referential or cyclic class is rejected with a clear `TypeError` | CONFIRMED by the coordinator from this proposal: Arrow cannot store a recursive struct |
| D-format | Identity string = name + `@` + the full 64-hex SHA-256 of the record (the "fold") | CONFIRMED: developer, November 10, 2026; alternative B (fields only in metadata) not pursued, appendix A |
| D-naming | No process-wide naming switch: fixed location rule, per-class `register_type(name=...)` or `register_type(qualified_name=False)` | CONFIRMED: developer dropped `set_type_naming`; a project-wide choice would live in the data context, not in v0.3 |
| D-mutation | `register_type` never modifies the class; same name again is a no-op; another name raises | CONFIRMED: developer |
| P2 | Public API name `register_type` and argument `qualified_name` | CONFIRMED: developer, November 10, 2026 |
| D-hash | The 25 golden entries in §e move; nothing else | CONFIRMED by the coordinator: follows from decision 2; the PR lists every moved entry |
| D-legacy | Reading a pre-0.3 user-class column directly raises a clear error | CONFIRMED by the coordinator: a consequence of the identity check; nothing extra is built |
| D-notice | One-time notice when a `__main__` class is first registered (S3) | CONFIRMED in scope by the coordinator; last item before the cuts |

## a. The identity definition

A user class's identity is an **identity record** with three components:

```json
{"fields": [["x", "int"], ["y", "float"]], "kind": "dataclass", "name": "Point"}
```

### Name component — CONFIRMED (decision 1)

The name is a string chosen once per class, by the first rule that applies:

1. **Declared name.** The user declared one (§b). Used verbatim.
2. **Per-class choice without a name:** `register_type(qualified_name=False)` gives
   `cls.__qualname__`; `register_type(qualified_name=True)` gives `module.qualname`.
3. **`__main__` class** (`cls.__module__` is `"__main__"` or `"__mp_main__"`, the name a
   `multiprocessing` spawn child gives the main module, W14): `cls.__qualname__`
   (`"Point"`, or `"Outer.Point"` for a nested class).
4. **Module class:** `f"{module}.{qualname}"` (`"pkg.models.Point"`). The module is
   passed through `canonical_module_name`, which leaves user modules unchanged.

Rules 3 and 4 are fixed (D-naming): no process-wide switch changes them, so a class's
identity never depends on whether or when some setting was called.

A name must be non-empty, must not contain `@` or whitespace, and must not start with
`orcapod.` (reserved for built-in types such as `orcapod.path`). A declared name is
NFC-normalized, so one name typed in two Unicode forms is one name (Python already
normalizes class and field identifiers). With `qualified_name=True`, a `__main__` class is
named `__main__.<qualname>` in a `multiprocessing` spawn child too. A class whose
`__qualname__` contains `<locals>` stays rejected in v0.3, even with a declared name
(scope, §g).

### Kind component — CONFIRMED (decision 3)

`"dataclass"` (`dataclasses.is_dataclass`, which includes `pydantic.dataclasses.dataclass`)
or `"pydantic"` (`pydantic.BaseModel` subclass). A dataclass and a pydantic model with the
same name and fields have different identity records, so they are different types.

**Subclasses.** Each class has its own identity; a subclass never borrows its parent's.
Today the registry matches an unregistered class to a registered *parent* by
`issubclass`, so `class P3(Point)` with an extra field `z` is stored as `Point` and `z` is
silently dropped (reproduced on `main`, November 10, 2026). v0.3 matches dataclass and
pydantic logical types by exact class only.

### Field component — CONFIRMED (decision 2; nested classes: P1)

`fields` lists each **stored field's name** with its **type token**, as `[name, token]`
pairs in declared order. It is a JSON array, so field order enters the identity. The stored fields are exactly
the ones stored today: dataclass fields with `init=True`, pydantic `model_fields`. The
Arrow storage struct lists the fields in the same declared order, so two classes with the
same identity always have the same storage type.

Type token of an annotation (from `typing.get_type_hints`, which strips `Annotated`
extras):

| Annotation | Token | Example |
|---|---|---|
| builtin scalar | its name | `int`, `float`, `str`, `bool`, `bytes` |
| other class without a logical type (stdlib) | `canonical_type_name(cls)` | `datetime.datetime`, `decimal.Decimal` |
| type with a built-in logical type | its logical type name | `orcapod.path`, `orcapod.uuid`, `orcapod.file` |
| generic container | `origin[arg, …]`, origin by its canonical name, args in order | `list[int]`, `dict[str, float]`, `tuple[int, str]` (`typing.List[int]` gives `list[int]`) |
| `Optional` / union | member tokens sorted, `None` last, joined by ` \| ` | `int \| None` for `Optional[int]`, `int \| None` and `None \| int` |
| `Literal` | `Literal[...]` with the members' `repr`s sorted | `Literal['a', 'b']` |
| nested dataclass / pydantic model (P1) | that class's identity string (below), as that class is registered | `Inner@3c1d…` (full digest), `geo.Inner@…` |

**Nested classes (P1, CONFIRMED).** The token of a field typed by another user class is
that class's own registered identity string: its name component however *that* class was
registered (declared name, name only, or `module.qualname`), its kind, and its field
component, recursively through the fingerprint. It is never re-derived from
`module.qualname` and never reduced to the storage struct. So renaming the inner class,
declaring it under another name, or changing its fields changes the outer identity.

**Registration order does not matter.** `register_type` only records the declared name
and checks it (§b); the logical type, and so the identity, is built at first use, and the
outer class's build registers the inner class first (implicitly, by the location rule, if
it was never declared). Declaring every class before first use therefore gives the same
identities in any order: outer decorated before inner, inner declared later in the same
cell, inner registered implicitly by the outer's first use. Declaring a class after it
was used under a different name raises (§c). An identical notebook re-run of the inner
class rebinds it (§c) and leaves the outer identity unchanged.

**Cycles (D-cycle).** A class that refers to itself, directly (`next: Node | None`) or
through other classes, is rejected at first use with `RecursiveTypeError` (a `TypeError` subclass) naming the
path (`Node.next -> Node`, `CycleA.b -> CycleB.a -> CycleA`). Today's converter already
raises `TypeError: Circular type dependency detected…`; v0.3 keeps the rejection and names
the path. Encoding the
back-reference by name would give a well-defined identity, but Arrow cannot store a
recursive struct, so there is nothing to identify.

Deliberately **not** part of the identity: defaults and default factories, methods and
validators, properties and pydantic `computed_field`s, `init=False` dataclass fields,
`ClassVar`s, private attributes, docstrings, pydantic field aliases and
constraints (`Field(gt=0)`, `Annotated` metadata), `frozen`/`slots`/config flags, base
classes (only the resulting fields count). Computed fields are deferred (D-computed); a
later decision to include them adds a key that is absent when empty, so it would move
only classes that have computed fields.

A type with a logical type contributes its logical type *name*, so those names are part of
user-class identities: adding or renaming a built-in logical type (for example a new
`orcapod.ndarray` from PR #289, where today an `np.ndarray` field gives the token
`numpy.ndarray`) moves the identity of every class with such a field. Built-in logical
type names are already a stability commitment (`orcapod.path`); after #289 lands this
branch adds a golden fixture with an ndarray field.

The token is built from the Python annotation, not the Arrow type, because the developer
named field *types* and because the Arrow type cannot tell `str` from `orcapod.path`
(both `large_string`).

### Identity string

```
identity string = f"{name}@{fingerprint}"
fingerprint     = sha256(canonical_json(record)).hexdigest()      # full 64 hex digits
canonical_json  = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                  # hashed as UTF-8
```

`Point(x: int, y: float)` as a dataclass gives
`Point@76d961e40619a836535e3fdebdead7ea65aa9413955a889f3d5b51a7a94b65eb` (the SHA-256 of
`{"fields":[["x","int"],["y","float"]],"kind":"dataclass","name":"Point"}`); with `y: int`,
`Point@008496cf…`; as a pydantic model with `y: float`, `Point@8a0f5b42…`. SHA-256 of a
documented JSON string, not the Orcapod semantic hasher, so the identity can be reproduced
by hand and does not depend on a hasher version. The full digest is used (developer):
there is no truncation rule to explain, and the name is not meant for people to type.
The length lives in one constant (`FINGERPRINT_HEX_DIGITS`) and the format in one function
(`identity_string`).

Where the long name shows: the Arrow field metadata of stored columns, `extension<...>` in
Arrow type reprs, and Polars extension dtypes. It does not appear in table paths (those
use pipeline hashes) or in Orcapod's error messages, which describe a class as
`Point (dataclass) {x: int, y: float}`. A prefix-matching lookup for abbreviated names was
considered and not built: no API takes a logical type name, and the read path compares
full strings from stored metadata.

### Where each component enters

The identity string replaces today's `f"{module}.{qualname}"` as the class's logical
type name. Every place that used the old string now uses the new one; there is no
second mechanism.

| Place | What it carries | How |
|---|---|---|
| Logical type name | identity string | `DataclassLogicalType.logical_type_name` / pydantic equivalent |
| Arrow extension name (`ARROW:extension:name`) | identity string | same string; one Arrow/Polars extension registration per identity |
| Arrow extension metadata | category plus the full record: `{"category":"orcapod.dataclass","identity":{…}}`, canonical JSON | read path uses it to find the class and to print the stored shape |
| Arrow storage type | struct of the stored fields, in declared order | derived from the annotations |
| Schema hash, source `pipeline_hash`, system-tag column names | identity string | `TypeObjectHandler` hashes a registered class as `type:<logical type name>` (unchanged code) |
| Pod signature hash, node `pipeline_hash`, DB table paths | identity string | via the schema/signature hashes |
| Per-row data hash, `content_hash` | identity string + record + values | the extension column is hashed with its extension name and metadata (unchanged code) |
| `list[Point]`, `Point \| None`, nested fields | identity string | container logical names embed the element's logical name; nested fields via the token (P1) |

**Same name, different fields → different pipeline identity.** `Point(x: int, y: int)`
and `Point(x: int, y: float)` have different fingerprints, so different logical type
names, different schema hashes, different pod and node pipeline hashes, and different
table paths. They never share a table. That removes the "same table, different shape"
failure (W6, W10: today 0 rows plus log warnings): the changed class computes into its own
tables, and the old tables stay as they were. In one process the two shapes have two
extension names, so Arrow's and Polars' global extension registries (keyed by name) hold
both without conflict; that is why the fields must be in the extension *name*, not only
in its metadata.

## b. Public API — `register_type` (D-naming, D-mutation)

```python
import orcapod as op

@op.register_type                     # default name: __main__ → "Point", module → "pkg.models.Point"
@dataclass
class Point: ...

@op.register_type(name="Point")       # declared name, the same wherever the class is defined
@dataclass
class Point: ...

@op.register_type(qualified_name=False)    # a module class named by its class name: "Point"
@dataclass
class Point: ...

op.register_type(Point, name="Point") # call form, for classes you do not own
```

- `register_type` records the name for the class object in a process-wide weak-keyed
  table. It never modifies the class (no attributes, no dataclass parameters, no pydantic
  config) and returns the same object. It checks the kind, the name, `name` and
  `qualified_name` not both given, and that the class is not local. The logical type is built
  at first use, so declarations may come in any order (§a). Classes never declared are
  registered on first use under the location rule.
- Registering the same class again with the same name is a no-op. A different name raises
  `ValueError` naming both, including a declaration after the class was already used
  under its default name: one class object has one name per process, and a registration
  is never changed later.
- The public name lives in one place (`orcapod/user_types.py`, re-exported from
  `orcapod`).

**Name collisions (same kind, same name, two different class objects):**

| Case | What happens | What the user sees |
|---|---|---|
| Same fields, same `module.qualname` (cell re-run, `%autoreload`, W1/W3/W4) | Same identity: the identity is rebound to the new class object (§c) | nothing |
| Same fields, different `module.qualname` (two modules declare `"Point"`, or use `qualified_name=False`) | Same identity by definition: they are the same type; values read back are built as the most recently registered class | one `UserWarning` per rebinding naming both classes. Warranted: values come back as a different class object than may be expected |
| Different fields (W2 with changed fields, or two modules) | Different identities: two types that happen to share a display name; no clash | nothing at registration. Reading picks the class whose identity matches (§d) |

## c. Rebinding rule (S1) — CONFIRMED (developer answer to S1)

- **Same identity string, new class object → rebind silently.** The registry rebinds the
  identity (logical name and extension name) to a logical type built for the new class.
  Earlier class objects keep their mapping, so instances created before the re-run still
  convert. Arrow's global registration for that extension name is reused (same name,
  storage type and metadata by construction). This fixes W1, W3, W4.
- **Different identity under the same name → a different type.** Nothing is rebound;
  the new class gets its own logical type and extension name (W2: the next run computes
  into new tables).
- A pod defined before the class cell was re-run still accepts the re-defined class:
  schema compatibility treats two user classes with the same identity string as the same
  type (`user_class_identity.equivalent_hints`, used by `check_schema_compatibility`).
- **Still raises:**
  1. One class object registered under two names (§b), including a declaration after
     the class was already used under its default name.
  2. An invalid declared name, or `name` together with `qualified_name` (§a, §b).
  3. A local class (`<locals>`), as today.
  4. A shape disagreement on read (§d, S2).
  5. A self-referential or cyclic class (D-cycle).

## d. Read path — CONFIRMED behaviour (S2), lookup mechanism proposed

A stored column carries the extension name `Point@9117beaa…` (full digest) and the record
in its metadata. To build values, the reader resolves a class:

1. **Registered identity:** the extension name is registered in the reading context → use it.
2. **Declared or registered by name:** classes recorded under `(kind, name)` in this
   process (by `register_type` or by first use); take the one whose identity string
   matches.
3. **`__main__` lookup:** walk the name as an attribute path from
   `sys.modules["__main__"]` (`"Point"`, `"Outer.Point"`), accepted if its identity matches.
   This makes W12 (read notebook results from a script that defines the same `Point`)
   work without a declaration.
4. **Import** (dotted name): import by the longest module prefix, as today, accepted if
   its identity matches.

Outcomes:

- **No candidate found:** `ImportError` (kept), naming the declared name, the kind, the
  stored fields, and how to fix it: "define the class and register it with
  `op.register_type(Point, name='Point')` before reading".
- **Candidates found, none with a matching identity (S2):** `TypeIdentityError`
  (new, a `ValueError` subclass in `orcapod.errors`) naming the column, the class name and
  kind, the stored fields and the registered fields, for example "Column 'q': Stored
  values of Point (dataclass) {x: int, y: int} do not match the registered class: Point
  (dataclass) {x: int, y: float}. …". `ExtensionAwareDatabase` reads add the column name
  (`register_discovered_logical_types`); the `ImportError` gets it too.
- **Same identity, different Arrow storage** (possible only if two data contexts map one
  annotation to different Arrow types): `TypeIdentityError` naming the column, the class
  and both struct types. The existing check in the synthesized
  `__arrow_ext_deserialize__` raises it. Storage that differs only in large versus
  non-large layouts, at any depth, is accepted: Delta compaction rewrites a struct field
  `large_string` as `string` (found in review; on `main` this read failed into 0 rows).
- **Candidates are tested without registering them.** A class found by name that is not
  yet used in the process is described without fixing its name, so a failed read never
  stops the user from then declaring the class under the stored name. When a candidate
  has the stored fields under another name, the error says to declare it.
- **Extension metadata is compared byte for byte**, like the extension name. It is part
  of the per-row data hash already (`ARROW:extension:*` keys are hashed), so any change to
  it is a hash change and needs the same approval; it is not a separate format risk.
- These errors propagate. `DeltaTableDatabase` today catches every read error and logs
  "Error reading Delta table"; it re-raises `TypeIdentityError` in all five read paths
  (`get_all_records`, the record lookups, the schema read), so a run can no longer report
  success with 0 rows. Other read errors keep their current handling (out of
  scope). The misleading "data may have been deleted externally" message is untouched
  unless the path is reached for this reason (§g lists it as out of scope).
- **Pre-0.3 column** (D-legacy): an extension name without `@` and metadata
  without `identity`, read directly (pipelines never reach these tables, §e): raise
  `TypeIdentityError` saying the column was written by Orcapod before 0.3 under
  `pkg.models.Point`, that v0.3 identifies the class by name and fields, and that the
  pipeline recomputes into new tables. Not converted, not dropped.

## e. Intended hash changes — CONFIRMED list, measured

Every pinned value that involves a dataclass or pydantic type moves, because the identity
string changes for every user class (fields are now in it, for everyone). Measured on
`origin/main` (c5ab1e05) with E08 (`--ref origin/main`): changing the identity string of the
golden fixtures `GoldenPoint` and `GoldenModel` moves exactly these 25 entries, and the
`unchanged` control moves 0. Every identity-string change moves the same set (E08's
`__main__` and module-less variants list identical entries), so the v0.3 string moves
these and only these.

**Measured after implementation** (golden files regenerated, compared with `origin/main`
c5ab1e05): 17 values change and 8 keys are replaced, exactly the entries below; 71 keys
are added (the 8 replacements and the new fixtures); `arrow.json` and `pipeline.json` are
byte-identical to `main`. Before regenerating, the golden test failed on exactly these 25
entries plus the unpinned additions.

| File | Entries |
|---|---|
| `semantic.json` (2) | `type.dataclass`, `type.pydantic` |
| `schema.json` (5) | `multi_field.logical_mix`, `single_field.dataclass.arrow_type`, `single_field.dataclass.schema_hash`, `single_field.pydantic.arrow_type`, `single_field.pydantic.schema_hash` |
| `datagram.json` (10 change) | `{dataclass,pydantic}.content_hash`, `.pipeline_hash`, `.system_tag_columns`, `.row[{"k":"r1"}].data_hash`, `.row[{"k":"r2"}].data_hash` |
| `datagram.json` (8 replaced) | `{dataclass,pydantic}.row[{"k":"r1"/"r2"}]._tag_record_id::<pipeline hash>` and `._tag_source_id::<pipeline hash>`: the keys embed the moved pipeline hash, so these 8 keys disappear and 8 keys with the new hash appear |

`arrow.json` and `pipeline.json` have no user-class entries and do not move. Golden
additions (new keys only): a declared-name dataclass and a pydantic model with the same
name and fields (distinct values), a nested dataclass with a container field (P1),
`list[GoldenPoint]`, an `Optional` field, and field-order twins (`GoldenTwinAB`,
`GoldenTwinBA`, both `golden.Twin`) that pin **different** type, schema, data and pipeline
hashes, and a dataclass with a NumPy array field.

**For the CHANGELOG (stated plainly):** "Orcapod 0.3 identifies a dataclass or pydantic
model by its name, its kind and its fields. Every pipeline whose data includes such a
class gets new pipeline hashes and table paths, so the first run under 0.3 recomputes
those pod and operator results into new tables. Tables written by earlier versions are
left untouched and are not read by 0.3 pipelines. There is no migration tool. Classes
defined in a notebook or script (`__main__`) are now identified by their name only, so
results survive moving the class into a module if it keeps its name and is registered
with `op.register_type(name=...)`." Results that do not involve user classes are
unaffected.

## f. Interactions with PR #286 and PR #289

- **PR #286** (polars types) touches `builtin_handlers.py`, `logical_types/__init__.py`,
  `contexts/data/v0.1.json`, `docs/concepts/function-pods.md`, the golden `cases.py` and
  `semantic.json`/`schema.json`/`datagram.json`. This work does not touch
  `builtin_handlers.py` or `v0.1.json` (the identity reaches hashing through the
  logical type name), adds a new docs page instead of editing `function-pods.md` beyond a
  link, and edits the golden files only by regeneration.
- **PR #289** (`orcapod.ndarray`) adds `array_format.py` and tests; no file in common, but
  a field typed `np.ndarray` contributes its logical type name to the identity (§a), so
  after #289 lands the ndarray token is re-checked and pinned with a golden fixture.
- **Preferred order:** #286 and #289 merge first. This branch then merges `origin/main`,
  re-runs E08 against the new main to confirm the moved set, and regenerates the golden
  files; the PR's golden diff must then show only the §e entries plus the additions. If
  this PR merges first, #286 regenerates its own additive entries after merging main;
  its user-class-free entries do not move.

## g. Scope cuts, in order, if time runs short

None were needed: everything below is implemented. The order that applied:

1. The one-time `__main__` notice (S3, D-notice).
2. The collision `UserWarning` for same identity from a different `module.qualname`.
3. The `__main__` lookup on read (§d step 3).

(`set_type_naming` was dropped by decision, not cut.) Not cut: the identity record and string, rebinding, `register_type`, the S2 raise, the
read lookup by declared name, the docs page and the CHANGELOG entry.

Out of scope for v0.3: computed fields; local classes with a declared name; Enum,
NamedTuple and TypedDict support (D6 of the findings); the `DeltaTableDatabase` row-count
and shutdown-hang findings (§9 of the findings).

## Workshop scenarios after v0.3 (tested: `tests/test_user_types/test_workshop_scenarios.py`)

On `origin/main` 15 of the 19 scenario tests fail; all pass on this branch. The four that
pass on `main` pin behaviour v0.3 keeps (W5, W7, W12 with the same class, and a module
class without a declared name being a different type from the notebook class).

| # | Scenario | v0.3 |
|---|---|---|
| W1/W3 | Re-run the cell, same fields (dataclass / pydantic) | silent rebind; cache hit |
| W2 | Re-run with changed fields | new type; recompute into new tables; old tables untouched |
| W4 | `%autoreload` of a module class | silent rebind |
| W5/W9 | Restart / second notebook, identical class | cache hit |
| W6/W10 | Restart / second notebook, changed fields | clean recompute, new tables; no 0-row success |
| W7 | Script as `__main__` twice | cache hit |
| W8 | `python -m pkg.pipeline` then `import pkg.pipeline` | miss unless declared (`"Point"` vs `"pkg.pipeline.Point"`); hit with `@op.register_type(name="Point")` |
| W11 | Notebook → module | hit if the module class declares the same name (`"Point"`) |
| W12 | Read notebook results from a script | works if the script defines a matching `Point` (top level or declared); otherwise the `ImportError` of §d |
| W14 | spawn child (`__mp_main__`) | same identity as `__main__` |
| W15 | Local class | `ValueError`, as today |

## Appendix A. Alternative B (fields only in the extension metadata) — not pursued

The developer asked whether the fields must be folded into the name, or whether the name
could stay `Point` with kind and fields in `ARROW:extension:metadata` and a type hash
composed from name, kind and fields. The developer then confirmed the fold (November 10,
2026) and the investigation stopped. Evidence gathered before it stopped:

- Arrow's and Polars' extension registries are keyed by name only. Orcapod's registry
  swallows `ArrowKeyError` on a second registration under the same name
  (`registry.py`), so a second shape under one name would be read through the first
  shape's `__arrow_ext_deserialize__`, which raises on the storage mismatch. B would need
  a parametric extension type that accepts every shape and dispatches on the metadata;
  not built or tested.
- Whether every Polars round trip keeps extension metadata as it keeps the name was not
  verified.
- B would change the semantic hasher's type handler; the fold needs no hashing change.
