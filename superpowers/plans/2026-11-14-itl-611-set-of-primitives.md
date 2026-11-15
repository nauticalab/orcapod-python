# ITL-611 set of primitives — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `set[T]` of a primitive element type is stored as the native set container (an
Arrow extension type), so the set identity survives every read; `list[T]` is unchanged.

**Architecture:** `ListLogicalType` (kind `set`) gains a primitive-element form whose
element identifier is the large-normalized Arrow type text (metadata key
`element_arrow_type`). The converter returns it for `set[T]` when `T` has no logical type,
except inside a dict value (plain storage kept). The hash visitor value-hashes a native
container only when its innermost element is a logical type. The saved-schema parser
rebuilds an unregistered native container from its identifier through the registered
factory, so a loaded job in a fresh process restores it.

**Tech Stack:** Python, PyArrow extension types, Delta Lake, pytest (`uv run`).

Spec: `superpowers/specs/2026-11-14-itl-611-set-of-primitives-design.md`.

**Decision state (2026-11-14):** coordinator approved the recommendations
(dev stores: document only; loaded-job restore in scope; frozenset stays refused; golden
entries added; dict values keep plain storage). D1 (identifier format) and the row-hash
movement are waiting on the developer. **No library code until the coordinator's go.**

---

## File structure

- Modify `src/orcapod/logical_types/list_logical_type_factory.py`: primitive-element form
  of `ListLogicalType`; `reconstruct_from_arrow` handles `element_arrow_type`.
- Modify `src/orcapod/semantic_types/universal_converter.py`: both `set` branches
  (`_register_python_class_impl`, `_convert_python_to_arrow`) return the container for a
  primitive element; dict branch keeps plain storage for a primitive set value.
- Modify `src/orcapod/hashing/visitors.py`: `_hashes_extension` / `visit_extension`
  value-hash only a logical innermost element.
- Modify `src/orcapod/pipeline/serialization.py` (`parse_arrow_type_string`): rebuild an
  unregistered native container from its identifier.
- Test `tests/test_logical_types/test_set_of_primitives.py` (new): public-interface tests.
- Golden: `tests/test_hashing/golden/cases.py`: add `set_int`, `set_str`, `set_bytes` to
  `_semantic_types()` (new type and schema entries) and a datagram case; regenerate and
  check that the diff only adds lines.
- Docs: `docs/concepts/logical-type-identifiers.md` (native containers),
  `docs/reference/hashing.md` §1e, CLAUDE.md + `.zed/rules` (container rule),
  `changelog.d/ITL-611.md`, `design_issues/ITL-611-<id>.md`.

## Task 1: Failing tests (allowed now)

**Files:** Create `tests/test_logical_types/test_set_of_primitives.py`.

- [ ] Step 1: write tests (public interface only):
  - job round trip, parametrized over `int, str, float, bool, bytes, datetime, date` ×
    `InMemoryArrowDatabase`, `DeltaTableDatabase` × `run` / `run_async`: node rows are sets,
    a downstream pod annotated `set[T]` receives a `set`, `as_source()` rows are sets and its
    schema is `set[T]`.
  - stored Arrow type of `set[int]`, `set[str]`, `set[bytes]`: extension name and exact
    metadata bytes from the spec table; storage `large_list<elem>`.
  - `list[int]`, `list[str]`, `list[bytes]` pin: plain `large_list`, read as `list`.
  - row hash of a `set[int]` row differs from the `list[int]` row with the same elements;
    two equal sets built in different orders hash equal.
  - `list[set[int]]` and `set[int] | None` round trip through a downstream pod.
  - fresh process (subprocess): a saved job over Delta, loaded with `PipelineJob.load`,
    `as_source()` reads `set[int]` / `set[str]` / `set[bytes]` / `set[Path]` as sets with
    schema `set[T]`, and the row hash equals the in-process one.
  - `dict[str, set[int]]` still stores and reads (plain storage inside the dict value).
  - `frozenset[int]` stays refused.
- [ ] Step 2: run `uv run pytest tests/test_logical_types/test_set_of_primitives.py -q`;
  expect the set tests to FAIL on main and the pins (`list`, `dict`, `frozenset`) to pass.
- [ ] Step 3: commit `test(logical_types): failing tests for set of primitives (ITL-611)`.

## Task 2: Primitive-element `ListLogicalType` (after go)

- [ ] `ListLogicalType` accepts a primitive element: `ListLogicalType.of_arrow_element(
  element_python_type, element_arrow_type)` (set only). Name `set[<text>]`, metadata
  `json.dumps({"category": "set", "element_arrow_type": text})`, storage
  `large_list(element)`, `also_reads` = own metadata. `python_to_storage`: converter's
  element converter, then `sort_set_elements` (canonical, unchanged storage bytes).
  `storage_to_python`: converter's Arrow→Python element converter, `set(...)`.
- [ ] `reconstruct_from_arrow`: `element_arrow_type` present → element type is the
  normalized storage value type; its text must equal the metadata (else `ValueError`);
  Python element type from `converter.arrow_type_to_python_type`.
- [ ] Converter: `set` branches call a `_make_or_get_primitive_set_type`; idempotent by
  extension name (so `set[int | None]` reuses `set[int64]`). Dict branch: a value that is a
  primitive set container uses its storage type.
- [ ] Run Task 1 tests; commit `feat(logical_types): set of primitives is a native container`.

## Task 3: Hash visitor rule

- [ ] `_hashes_extension` and `visit_extension`: unwrap to the innermost element; value-hash
  only when that element type has a logical type registered (not merely a semantic handler).
- [ ] Run `uv run pytest tests/test_hashing -q` (golden unchanged) and the set tests;
  commit `fix(hashing): a container of a primitive element is hashed as stored`.

## Task 4: Loaded-job restore of native containers

- [ ] `parse_arrow_type_string`: unregistered extension name → if metadata is a native
  container (`category` in `list`/`set`), rebuild through
  `type_converter.register_logical_type_from_arrow_metadata(name, metadata, storage)`
  where storage is derived from the element (Arrow text, or the element logical type's
  storage after recursively resolving it); then `require_readable`.
- [ ] Run fresh-process tests and `tests/test_pipeline -q`; commit
  `fix(pipeline): a loaded job restores native containers from their identifier`.

## Task 5: Golden additions, docs, changelog, design issue

- [ ] Add golden cases for `set[int]`, `set[str]`, `set[bytes]`; regenerate; `git diff`
  shows added lines only.
- [ ] Docs, CLAUDE.md, `.zed/rules`, `changelog.d/ITL-611.md` (Changed + Breaking:
  dev stores holding a plain `set[...]` column must be cleared), `design_issues/ITL-611-SP1.md`.
- [ ] `uv run --with ruff ruff check .`, `ruff format --check .`, basedpyright (exit code),
  full `uv run pytest tests/`.

## Task 6: PR

- [ ] Merge `origin/main`, rerun suite if it moved; push; draft PR (`Fixes ITL-611`);
  `orca linear attach`; `orca worktree set --worktree current --pr <n>`; CodeRabbit rounds.

## Progress notes

- 2026-11-14: design sent; prototype patch measured (kept outside the repo); Task 1 next.

## Follow-up dispatch (2026-11-14, task_ac04b84f8f2a)

- [ ] Merge `origin/main` (834ee639, PR #391 ITL-1137: `TypeObjectHandler` and
  `canonical_annotation_str` register a type before hashing it). Check its interaction with
  the hash-visitor rule (Task 3) and native-container registration (Task 2).
- [ ] Targeted tests (3.12, one process): tests/test_logical_types, tests/test_hashing (golden,
  label rule), tests/test_semantic_types, tests/test_pipeline loaded-job tests, ITL-611 tests.
- [ ] Golden diff against main adds lines only.
- [ ] Push; CI green on 3.11/3.12/3.13. PR stays DRAFT (never `gh pr ready`).
- [ ] After the developer marks ready: CodeRabbit rounds (plan via coordinator `ask`).
- [ ] Reproduce: `hash_object` of a set of datetimes; is it reachable in a row hash via #393?

### Follow-up notes

- Merged origin/main 834ee639 cleanly (merge commit 7c1d48f0). #391 changes only
  `TypeObjectHandler`/`canonical_annotation_str` (lookups of plain `type` objects) and
  `SchemaHandler` order; the visitor's container rule reads the element type from a
  registered Arrow extension, so a lookup is enough there. Targeted suites: 3885 passed,
  31 skipped, 1 xfailed (3.12). Golden diff vs main: 53 insertions, 0 deletions.
- `hash_object` has no handler for `datetime` (bare or in a set): pre-existing, untouched.
  Not reachable in a row hash: `set[datetime]` elements are not a logical type, so the
  column is hashed as stored (verified: a pod returning an aware `set[datetime]` gives a row
  hash). Naive datetimes are refused as pod outputs for scalar/list/set alike (existing rule).

### CodeRabbit round 1 (review 5908025110 on b22cb6ff), plan approved by coordinator, fix done

- 4576307086 (Literal first wins) — ACCEPT. Reproduced: after a `set[Literal[1, 2]]` source,
  a `set[int]` source's schema is `set[Literal[1, 2]]` and its pipeline hash differs from the
  same source built alone. Fix in `_make_or_get_arrow_element_set_type`: the element Python
  type is `arrow_type_to_python_type(element Arrow type)` (what fresh-process reconstruction
  uses), never the first annotation. Test first (public: two DictSources, both orders).
- 4576307078 (None in `list[set[int] | None]`) — DECLINE as pre-existing: main reads the same
  column as `[[], [1], []]`; `list[set[UUID] | None]` reads `[set(), ...]` on main;
  `list[UUID | None]` crashes on read on main. Report out of scope.

### CodeRabbit round 2 (review 5908180501 on 8b75ccea): thread 4576412950, widths collide

- Reproduced. HEAD: set[np.int8] then set[int] -> set[int] gets the int8 container and a
  set[int] source raises "python_type 'set' is already bound to 'set[int8]'"; reverse order:
  set[np.int8] raises. b22cb6ff: same failure via a job loaded in a fresh process (reconstruct
  reads int8 as int and binds set[int]). main: plain lists, no collision.
- Root cause: the registry binds each Python type to one logical type; many element Arrow
  types read as one Python type (int8/int64, float/double, timestamp units/tz, date32/64).
- Proposed (A): plain-element set containers are registered by identifier only, never bound
  to a Python type; set[T] -> Arrow goes through T's Arrow type, as list[T] does. Pending
  coordinator decision. APPROVED (A) and implemented.

### Metadata format change (developer decision, November 15, 2026), task_261a2b5ec576

Decision: "make the logical extension name contain the element as is, but remove redundancy
for arrow type by simply using element_kind arrow". A set of a primitive keeps its name
(`set[int64]`); metadata becomes exactly `{"category": "set", "element_kind": "arrow"}`.
Containers of logical elements unchanged.

- [ ] Tests first (test_set_of_primitives.py): identifier/metadata for set[int], set[str],
  set[bytes], set[datetime]; list[set[int]] element_ext_metadata is the new inner metadata;
  fresh-process read rebuilds set[int]/set[str] from storage (existing fresh test covers it,
  with the new metadata); a stored column `set[...]` with `element_kind: arrow` over a
  non-list storage, and over a list whose element is not the one the name names, is refused
  with LogicalTypeIdentifierError (fresh process, so the name is not already registered).
- [ ] Writer `_init_arrow_element`: metadata `{"category": "set", "element_kind": "arrow"}`;
  constant `ELEMENT_KIND_KEY = "element_kind"`, `ARROW_ELEMENT_KIND = "arrow"`.
- [ ] Reader `reconstruct_from_arrow`: `element_kind == "arrow"` -> element from storage
  (`large_list<T>`, list accepted as before and normalized), rebuilt name must equal the
  stored name; otherwise LogicalTypeIdentifierError. Other element_kind values refused.
- [ ] Saved-schema rebuild (`serialization._native_container_storage`): element text comes
  from the extension name `set[<text>]` when element_kind is arrow (needs the name).
- [ ] Docs: logical-type-identifiers.md, CLAUDE.md + .zed/rules, changelog fragment,
  design_issues/ITL-611-NC1.md, spec table, PR description.
- [ ] Golden: regenerate only; diff vs origin/main must remain additions only.
- [ ] Targeted suites, push, CI, one signed `@coderabbitai review`.
- Done: writer/reader/serialization, docs, golden (6 set_* data hashes, vs main still 53+/0-),
  pushed 928482fc, CI green.

### CodeRabbit round 3 (review 5908556654 on 24cc3d9f), plan approved by coordinator

- 4576679626 (registered name: storage not checked) — ACCEPT, generic: the registry-hit
  branch of `register_logical_type_from_arrow_metadata` compares the stored storage with the
  registered type's (large-normalized) and raises `LogicalTypeIdentifierError` with a note.
  Was refused before too, by Arrow's `TypeError`. Hashing, logical types, semantic types,
  pipeline and database suites pass with it (8322 passed).
- ast-grep notes (jsonify x2, subprocess x2): DECLINE, false positives; one signed comment.

### CodeRabbit CLI (pre-push gate) on e3baffa8: 2 findings, both reproduced, fixed

- minor list_logical_type_factory.py:234 — list[set[np.int8]] then list[set[int]] raised
  (python_type 'list' already bound); main: plain lists, fine. Fix: a container takes its
  element's `binds_python_type`. Test: both orders, fresh process.
- major universal_converter.py:572 — dict[str, list[set[int]]] raised (extension in struct);
  main: plain storage. Fix: `_plain_set_storage` lowers a container whose innermost element is
  a plain set. Tuple part declined: tuple[int, list[int]] is unsupported on main too.

### Merged main 118b0379 (typecheck hotfix #396). CodeRabbit CLI on the merge (pre-push gate)

- Run 1: 0 findings, but 11 files `review_failed` (nothing reviewed); retried once.
- Run 2: 1 minor finding, list_logical_type_factory.py:493: the factory's write path for a
  set of a plain element kept the annotation (`Literal[1, 2]`) as the element Python type.
  Not reachable from the public converter path (generic sets take the converter's own set
  branch), but fixed for consistency with the read path; test calls the factory directly.
