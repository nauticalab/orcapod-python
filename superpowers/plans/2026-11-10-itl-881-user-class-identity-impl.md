# User class identity (v0.3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Identify a dataclass or pydantic model by `{kind, name, fields}` (spec
`superpowers/specs/2026-11-10-user-class-identity-v0.3.md`), with a declared-name API, silent
rebinding of identical re-registrations, and a raise on stored/registered shape mismatch.

**Architecture:** One new module, `orcapod/logical_types/user_class_identity.py`, owns the
naming policy, declarations, type tokens, the identity record and string (single-point
format), the process-wide `(kind, name)` table and the read-side class lookup. The two
factories share a new base (`user_class_logical_type.py`) that builds the identity string
and uses it as logical type name and Arrow extension name; the metadata carries the record.
`LogicalTypeRegistry` learns to rebind an identical identity to a new class object and stops
matching user classes by subclass. The public API (`register_type`, `set_type_naming`) lives
in `orcapod/user_types.py`. `TypeIdentityError` lives in `orcapod/errors.py`; the Delta read
path re-raises it.

**Tech Stack:** Python 3.11–3.13, pyarrow, polars, pydantic v2, deltalake, IPython (dev, for
the notebook scenarios), pytest via `uv run`.

**Changes after the plan was written (developer decisions, November 10, 2026):**
`set_type_naming` was dropped (fixed location rule plus `register_type(qualified_name=False)`),
the identity string uses the full 64-hex digest, and stored structs list fields in name
order. The spec is authoritative where this plan differs.

---

## File map

| File | Change |
|---|---|
| `src/orcapod/errors.py` | add `TypeIdentityError(ValueError)` |
| `src/orcapod/logical_types/user_class_identity.py` | **new**: kinds, naming policy, declarations, `type_token`, `identity_record`, `canonical_json`, `identity_string`, known-class table, `find_class`, `describe_fields`, S3 notice |
| `src/orcapod/logical_types/user_class_logical_type.py` | **new**: `UserClassLogicalType`, `UserClassLogicalTypeFactory` (shared write/read path) |
| `src/orcapod/logical_types/dataclass_logical_type_factory.py` | reduce to subclasses: kind, category, `supports_class`, stored fields |
| `src/orcapod/logical_types/pydantic_logical_type_factory.py` | same |
| `src/orcapod/logical_types/registry.py` | rebinding rule; exact-type match for user classes; `TypeIdentityError` from `_deserialize` |
| `src/orcapod/semantic_types/universal_converter.py` | cycle error names the class and says recursive classes cannot be stored |
| `src/orcapod/databases/delta_lake_databases.py` | re-raise `TypeIdentityError` where read errors are logged |
| `src/orcapod/logical_types/database_hooks.py` | add the column name to a `TypeIdentityError` raised while registering a discovered type |
| `src/orcapod/user_types.py` | **new**: `register_type`, `set_type_naming` |
| `src/orcapod/__init__.py` | re-export the two functions |
| tests | `tests/test_user_types/` (new): unit tests of identity, API, registry, read path; `test_workshop_scenarios.py` (subprocess + IPython `InteractiveShell`) |
| existing tests asserting `module.qualname` names | switch to the identity string |
| `tests/test_hashing/golden/cases.py` + `data/*.json` | add new fixture cases; regenerate (25 listed moves + additions) |
| docs | new `docs/concepts/your-own-data-types.md`; section in `docs/concepts/identity.md`; nav in `mkdocs.yml`; link from `function-pods.md` |
| `CHANGELOG.md`, `DESIGN_ISSUES.md` | entry each |

## Key definitions (used by every task)

```python
# user_class_identity.py
KIND_DATACLASS = "dataclass"
KIND_PYDANTIC = "pydantic"
_NAMING = {"main": "name", "module": "qualified"}          # set_type_naming
_MAIN_MODULES = frozenset({"__main__", "__mp_main__"})
_DECLARED: weakref.WeakKeyDictionary[type, str]             # register_type
_KNOWN: dict[tuple[str, str], list[weakref.ref[type]]]     # (kind, name) -> classes

def validate_name(name: str) -> None: ...                  # non-empty, no '@', no whitespace, not 'orcapod.'
def class_kind(cls: type) -> str | None: ...
def resolve_name(cls: type) -> str: ...                    # declared > main policy > module policy
def declare(cls: type, name: str | None) -> None: ...      # raises if already built under another name
def set_naming(main: str | None, module: str | None) -> None: ...
def type_token(annotation, converter) -> str: ...
def identity_record(kind, name, fields: dict[str, str]) -> dict: ...
def canonical_json(obj) -> str: ...                        # sort_keys, (",", ":")
def identity_string(record) -> str: ...                    # f"{name}@{sha256(canonical_json)[:16]}"  (single point)
def describe_fields(fields: dict[str, str]) -> str: ...    # "{x: int, y: float}"
def remember(kind, name, cls) -> None: ...                 # _KNOWN
def candidates(kind, name) -> list[type]: ...              # _KNOWN + declared + __main__ walk + import
```

Type tokens: builtins bare (`int`); `NoneType` → `None`; other classes without logical type →
`canonical_type_name`; class with logical type → `lt.logical_type_name` (built-ins
`orcapod.path`, user classes their identity string, P1); `list[...]`/`set[...]`/`dict[...]`
→ `origin[args]` with origin by canonical name minus `builtins.`; unions → sorted members
joined by ` | `; `Literal` → `Literal[...]` with sorted `repr`s.

---

### Task 1: Identity primitives (pure functions)

**Files:** create `src/orcapod/logical_types/user_class_identity.py`, modify
`src/orcapod/errors.py`; test `tests/test_user_types/test_identity_record.py`.

- [ ] Step 1: failing tests: `identity_string({"kind":"dataclass","name":"Point","fields":{"x":"int","y":"float"}}) == "Point@9117beaaf7ebe3ab"`; field-order independence; `type_token` for `int`, `int | None`, `Optional[int]`, `None | int`, `list[int]`, `typing.List[int]`, `dict[str, float]`, `Path` (`orcapod.path`), `datetime.datetime`, `Literal["b","a"]`; `validate_name` rejects `""`, `"a@b"`, `"a b"`, `"orcapod.x"`; `resolve_name` for a module class (`tests.…Point` qualified), a class with `__module__="__main__"` (`"Point"`), `"__mp_main__"` (same), after `set_naming(main="qualified")` (`"__main__.Point"`), after `set_naming(module="name")`.
- [ ] Step 2: run, expect ImportError (module missing).
- [ ] Step 3: implement the functions above.
- [ ] Step 4: run, pass. Commit `feat(types): identity record and string for user classes (ITL-881)`.

### Task 2: Shared user-class logical type; factories use the identity

**Files:** create `src/orcapod/logical_types/user_class_logical_type.py`; reduce both factory
modules to subclasses; test `tests/test_user_types/test_user_class_logical_type.py`.

`UserClassLogicalType(identity, record, python_type, storage_type, field_annotations, category)`:
logical name = Arrow extension name = identity string; metadata
`canonical_json({"category": category, "identity": record}).encode()`; conversions as today.
Class attribute `exact_python_type_only = True`.

`UserClassLogicalTypeFactory` (abstract: `kind`, `category`, `supports_class`, `stored_fields(cls, hints)`):
- `create_for_python_type(cls, converter)`: reject `<locals>`; name = `resolve_name(cls)`;
  for each stored field: `arrow = converter.register_python_class(annotation)` (wrap a
  circular `TypeError` as `TypeError(f"{cls.__qualname__}.{field}: recursive classes cannot be stored ({exc})")`);
  token = `type_token(annotation, converter)`; record; identity; `remember(kind, name, cls)`;
  S3 notice when the name came from the `__main__` policy (once per name per process).
- `reconstruct_from_arrow(ext_name, storage, metadata, converter)`: no `identity` key →
  `TypeIdentityError` (pre-0.3 column); else for each candidate class of (kind, name): get its
  logical type through `converter.register_python_class(cls)` +
  `converter.get_logical_type(cls)`; return the first whose `logical_type_name == ext_name`
  (and whose storage equals the stored storage after `normalize_to_large_types`, else
  `TypeIdentityError` naming both struct types). No candidate → `ImportError` naming name,
  kind, stored fields and `op.register_type(Point, name='Point')`. Candidates but no match →
  `TypeIdentityError` naming class, kind, stored fields, registered fields.

- [ ] Step 1: failing tests: logical name is the identity string; metadata contains the record; two classes with permuted fields share the identity; dataclass vs pydantic same name+fields differ; same name, `y: int` vs `y: float` differ; nested dataclass token is the inner identity string and changes when the inner fields change or the inner is declared under another name (P1); outer built before inner declared gives the same identity as inner-first (order); self-referential class → `TypeError` naming `Node.next`; `<locals>` → `ValueError`; subclass with an extra field gets its own identity (exact-type match).
- [ ] Step 2: run, fail. Step 3: implement. Step 4: update the existing factory/roundtrip tests that assert `module.qualname` (listed in the file map) to read the identity from the registry. Step 5: run `uv run pytest tests/test_logical_types tests/test_user_types -q`, pass. Commit.

### Task 3: Registry rebinding and exact-type match

**Files:** `src/orcapod/logical_types/registry.py`; test `tests/test_user_types/test_rebinding.py`.

- `register_logical_type`: when the logical name is bound to a different instance whose
  Arrow extension type has the same name, storage type and serialized metadata, and the
  Python type differs → rebind: replace the logical-name and Arrow-name bindings, add the new
  Python-type binding, keep the old Python-type binding. If the previous class's
  `module.qualname` differs from the new one's → one `UserWarning` naming both. Anything
  else keeps raising.
- `get_by_python_type`: skip the `issubclass` fallback for logical types with
  `exact_python_type_only`.
- `_deserialize`: raise `TypeIdentityError` (subclass of `ValueError`) for storage/metadata mismatch.

- [ ] Steps: failing tests (rebind same identity silently, `warnings` none; different module.qualname same identity → one `UserWarning`; one class object under two names → `ValueError`), implement, pass, commit.

### Task 4: Public API

**Files:** create `src/orcapod/user_types.py`; modify `src/orcapod/__init__.py`; test
`tests/test_user_types/test_register_type.py`.

```python
def register_type(cls=None, /, *, name=None):  # decorator, decorator factory, or call
def set_type_naming(*, main=None, module=None) -> None:
```

- [ ] Steps: failing tests through `op.register_type` / `op.set_type_naming` and a `DictSource` + `function_pod` pipeline: declared name gives the identity `Name@…`; `register_type(Point, name="Geo")` after first use under another name → `ValueError`; invalid names; non-dataclass → `TypeError`; returns the class. Implement, pass, commit.

### Task 5: Read path and S2 guard

**Files:** `delta_lake_databases.py`, `database_hooks.py`; test
`tests/test_user_types/test_read_path.py` (public interface: pipeline in a subprocess writes
to `DeltaTableDatabase`, a second subprocess reads with a different class).

- [ ] Steps: failing tests: reader with matching class (declared in another module) reads values; reader without the class → `ImportError` naming `Point` and `register_type`; reader whose `Point` has different fields reading the stored table directly → `TypeIdentityError` naming column, both field sets; re-run of the pipeline with changed fields computes into new tables (calls == rows), old table still readable by the old class. Implement re-raise, pass, commit.

### Task 6: Workshop scenarios W1–W12 as tests

**File:** `tests/test_user_types/test_workshop_scenarios.py` (each scenario in a fresh
subprocess; notebook cells through `IPython.core.interactiveshell.InteractiveShell`, as E02;
children end with `os._exit(0)` after printing JSON, as the E-harness does).

W1 re-run same fields (calls 0 on second run, no error); W2 changed fields (new tables:
calls 2, rows 2, no error); W3 pydantic re-run; W4 `importlib.reload`; W5 restart + re-run
(two processes, cache hit); W6 restart with changed fields (recompute, rows 2); W7 script as
`__main__` twice; W8 `-m` vs import with a declared name (hit); W11 notebook → module with a
declared name: identical pipeline hashes and table paths; W12 read notebook results from a
script defining the same `Point`. Plus: pydantic vs dataclass same name+fields are distinct
(different pod pipeline hashes).

- [ ] Steps: write, run (must fail before Tasks 1–5 on `main` for W1–W4, W6, W11, W12; record which), pass after, commit.

### Task 7: Golden values

- [ ] Add fixture cases to `cases.py` (new names only; nothing moved): declared-name dataclass, nested dataclass, `Optional` field, container field, permuted-order twin pair.
- [ ] Run `uv run pytest tests/test_hashing/golden -q`: exactly the 25 spec entries fail. Record the list.
- [ ] Regenerate; `git diff --stat` and a script comparing old/new JSON: changed keys == the 25, plus only added keys. Commit `test(hashing): golden values for v0.3 user class identity (ITL-881)`.

### Task 8: Docs, CHANGELOG, DESIGN_ISSUES

- [ ] `docs/concepts/your-own-data-types.md` (runnable samples: define, declare, re-run, what changes identity), identity section in `docs/concepts/identity.md`, nav entry, link from `function-pods.md`. Run `uv run pytest tests/test_docs -q`.
- [ ] CHANGELOG entry with the hash-change statement from spec §e.
- [ ] DESIGN_ISSUES entry appended at the end (resolved, with Fix note).

### Task 9: Verification and PR

- [ ] ruff check/format, basedpyright exit code, full `uv run pytest tests/` on 3.11/3.12/3.13 (`uv run --python 3.1x`).
- [ ] Merge `origin/main` (after #286/#289 land), re-run E08 on the new main, regenerate golden only for the listed entries.
- [ ] PR body: intended hash changes list; mark ready via sensei:create-pr.
