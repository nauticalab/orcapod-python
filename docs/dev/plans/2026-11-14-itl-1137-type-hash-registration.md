# ITL-1137: a type's hash must not depend on whether it was registered

Branch `eywalker/itl-1137-a-pipeline-hash-over-a-user-class-depends`, from `origin/main`
`e4b4319c`. Draft PR against `main`, `Fixes ITL-1137`.

## Status

- [x] Cause established (below), sent to the coordinator by `ask`.
- [ ] Coordinator approval of the proposal.
- [ ] Failing tests first.
- [ ] Fix.
- [ ] Changelog fragment, design_issues entry, full suite, PR.

## Cause

`TypeObjectHandler.handle` (`hashing/semantic_hashing/builtin_handlers.py`) is the one place a
type object becomes a hash input. It asks the type converter `get_logical_type(obj)`, which is
a registry lookup only:

- registered: `type:<LogicalTypeIdentifier.text>`, e.g. for an exact dataclass
  `type:__main__.A@1ee123…;7b2263617465676f7279223a226f726361706f642e64617461636c617373…`
  (extension name `name@sha256(record)` + metadata `{category, identity:{fields, kind, name}}`);
- not registered: `type:<canonical module>.<qualname>`, e.g. `type:__main__.A`.

So the unregistered value carries only the module path and qualified name: no kind, no fields.
Collisions (one process, nothing registered):

| class | hash input | slot pipeline hash |
|---|---|---|
| exact `A(u: int, v: str)` | `type:__main__.A` | `397c08159f373f49` |
| exact `A(w: float)` (other fields) | `type:__main__.A` | `397c08159f373f49` |
| structural `A(u: int, v: str)` | `type:__main__.A` | `397c08159f373f49` |
| exact `B(u: int, v: str)` | `type:__main__.B` | `741bb18fb6155218` |

After registration the four give `9d01e7b7…`, `0723367a…`, `6cddc2e5…`, `ca2138ba…`.

Why nothing registers first: `SchemaHandler` checks translatability with
`python_type_to_arrow_type`, which does not register (it raises `Unsupported Python type` for an
unregistered dataclass), and a `Schema` nested inside a structure (a node's
`pipeline_identity_structure()` is the tuple `(tag_schema, data_schema)`) never reaches
`SchemaHandler` at all: `_expand_element` expands it as a Mapping (`_expand_mapping`), which
hashes each field type through `TypeObjectHandler` with no check. A top-level
`hash_object(Schema({"p": A}))` of an unregistered class raises `TypeError`.

Registration happens only at: `FunctionPod.__init__` (`ensure_types_registered_for_schemas`
over input and output schemas), `UniversalTypeConverter.python_dicts_to_arrow_table`
(DictSource, ListSource, datagrams from Python values), reading Arrow data with extension types
(`register_storage_type` / `register_discovered_logical_types`, i.e. sources over Arrow tables,
database reads, loading a saved job's schema text when the class is already bound),
`output_conformance` (`get_logical_type_for_python_type`), and nested registration inside
`user_class_logical_type` / `user_class_identity`.

Not affected: built-in and library logical types (`Path`, `UUID`, `File`, `np.ndarray`,
`pd.DataFrame`, `pl.DataFrame`, `torch.Tensor`) are registered when the data context loads:
same hash before and after.

Affected in one process (before != after): exact dataclass, structural class, pydantic model,
nested exact and nested structural classes, `list[A]`, `A | None`.

## Realistic sequences

1. Pipeline with a `SourceNode` slot followed only by operators, job built before a source of
   the class exists: operator node pipeline hash `0a1f900ab2da6e57` is memoized and kept after
   bind and run; the same pipeline built after registration has `ea3272f4ec3bb58c`. Path
   component `schema:<pipeline hash>` differs.
2. Pipeline slot `(p: A, x: int)` -> function pod `h(x: int)` (pod never names `A`): run once
   with the pipeline built first, once with the source built first, on one Delta store. Two
   pipeline tables for one node:
   `demo/h/<uri>/v0/python.function.v0/schema:efce4ce5…/pdb_v2` and `schema:bd9c930f…/pdb_v2`.
   The result table (`_result/h/…/rdb_v2`) is shared, so the second run took `1 from store`;
   the pipeline records (the node's stored view) are split.
3. A function pod annotated with the class, defined before the slot is hashed: stable from the
   start (pod registers).
4. Save/load (DictSource -> MapData -> MapData, and DictSource -> pod; Delta store; load in a
   fresh process): operator and function job nodes take their hashes from the saved file:
   identical to the saving process, and `run(load_only=True)` after bind loads every stored
   row. The loaded `SourceJobNode` differs: the saved type text
   `extension<ucmod.A@…;…>` cannot be restored to a Python type unless the class is
   registered in the loading process before `load` (importing the module is not enough), so
   the slot's schema holds the text string and its pipeline hash is `8f42c0d1…` instead of the
   saved `c7e3da4d…` (structural: `57b3cbad…` vs `756682dc…`). Registered before load: equal.
   This is a separate mechanism (load cannot rebuild a user class from metadata); the
   proposed fix does not change it. Joined-source system-tag names in a loaded job come from
   the bound source, so they were equal in the test.

## Which value is the value

The registered one. Every golden case that hashes a user class calls
`register_logical_classes()` first (`semantic.json type.*`, `schema.json single_field.*`,
`datagram.json` dataclass / pydantic / structural groups). No golden entry holds an
unregistered value.

## Stored data under the unstable value

Paths that embed a pipeline hash: function pipeline tables (`pdb_v2`) and operator caches
(`odb_v1`) of nodes downstream of a slot hashed before registration, written by any main
build since the user-class identity (ITL-972). Not affected: result tables (`rdb_v2`, keyed by
the pod), and nodes over a source built from data (the source registers at construction). No
release holds this: v0.3.0 is not tagged, and v0.2 hashes differ from v0.3 anyway.

## Proposal (recommended: A)

A. `TypeObjectHandler` resolves the type through
   `type_converter.get_logical_type_for_python_type(obj)` (registry lookup, else register via
   the factory that covers it; `None` for primitives and classes no factory covers), and
   `SchemaHandler` hashes each field type before its translatability check (so the check sees
   the registered type). Registered classes hash exactly as today; the unregistered value can
   no longer be produced for a class a factory covers. Prototype: all probes equal, golden
   suite and `test_hash_label_rule.py` 1317 passed unchanged, hashing / user types / logical
   types / semantic types 3759 passed.
B. Register in `SourceNodeBase.__init__` (as `FunctionPod` does). Covers slots only; other
   schemas hashed before registration (pipeline load, operator predictions) stay exposed.
C. Route nested `Schema` objects through `SchemaHandler` and make it register. Fixes schemas
   only, and makes nested schemas with an untranslatable type raise where they hash today.

## Tests (after approval)

`tests/test_hashing/test_type_hash_registration.py`: in one process, the hash of a fresh class
taken before registration equals the hash after (`SourceNode` pipeline hash and schema hash),
for exact dataclass, structural class, pydantic model, nested class, `list[A]`, `A | None`;
two unregistered classes of the same qualified name and different fields hash differently; a
fresh-process subprocess test: a pipeline built before a source of the class gives the same
node pipeline hashes as one built after.
