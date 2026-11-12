# ITL-942 wrong-hash fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the wrong-hash findings of the ITL-938 hash-site audit (Appendix A.9) before the
v0.3 tag, each in its own commit with a failing public-interface test first, with one golden
regeneration at the end.

**Final scope (developer decisions, November 11, 2026; this plan is complete, PR #315):**
DONE: H (H1), I (I1), L1 (L1a), dict keys (D1 plus the '__type__' mechanism M2, `Encoded`),
F (F1, and the dead duplicate `SelectDataColumns` in `filters.py` deleted), G (G1), N (N1),
T (declared field order). NOT DONE BY DECISION: E is kept as intended (pinned by tests and
documented); L2 (function defaults) moved to ITL-953; R is fixed by #292 (ITL-881); the
list-of-logical-type fold collision found during I goes to ITL-935. The analysis below is the
Step 1 record and its "Proposed order" is historical, not an active task list.

**Architecture:** Each fix is local to one hash site (structure hasher, Arrow hasher pre-pass,
fold helper, operator identity, source record-id type, function-pod default). No new
dependencies. Hash values move only where the old value was wrong; every moved golden entry is
listed per finding.

**Tech Stack:** Python 3.11-3.13, pyarrow, starfix 0.5.0 (`arrow_v0.2`), pytest via `uv run`.

**Base:** branch `eywalker/itl-942-fix-the-wrong-hash-findings-of-the-hash`, created at the head
of PR #309 (`5363bfb8`, ITL-910). Stacked on #309.

**Status:** complete; PR #315. Merged #309 at b88f7eb3 (old-label rows are ignored on
whole-table reads, not refused); G re-checked: Delta stores one binary type, so G1 changes no
on-disk schema. Repro scripts: run on `5363bfb8`, Python 3.12, starfix 0.5.0.

---

## Step 1: per-finding analysis

Every finding below was reproduced on this base through the public interface (sources,
operators, `function_pod`, `FunctionPod.from_fn`, the context's `semantic_hasher` /
`arrow_hasher`). All eight are still real on starfix 0.5.0.

### H. Batch / GroupBy source-id fold — no member separator

- Repro: `fold_system_tag_values("_tag_source_id::…", ["ab","c"]) == (…["a","bc"])` → True;
  `[None] == [""]` → True (both folds map `None` to `""`).
- Site: `utils/arrow_utils.py:fold_system_tag_values` → `hashing/hash_utils.py:combine_hashes`
  (`"".join(...)`). The record-id fold joins hex with `::` (unambiguous) but also maps `None`
  and `b""` to `""`.
- Rule: a fold's preimage must decode back to the member list (injective encoding); member order
  stays identity (it matches the order of the list-valued data columns).
- Options: (H1) sha256 of the canonical JSON array of members, `None` → `null` **(recommended:
  unambiguous, handles `None`, no new format)**; (H2) length-prefix each member; (H3) hash each
  member, concatenate fixed-length digests. Record-id fold: keep `::`-joined hex, encode `None`
  as `null` (not valid hex, so unambiguous).
- Golden: `pipeline:batch` and `pipeline:group_by` `row[...]._tag_source_id::…` values. No
  column name, path or stored schema changes (values of system-tag columns only; they feed entry
  ids of downstream function nodes).
- Size: small. `combine_hashes` has no other caller; it is removed.

### I. Data-dependent Arrow hashing of nested types

- Repro (starfix 0.5): `struct<a: int64 not null>` vs `struct<a: int64>` — equal with one row,
  different with zero rows; `list<item: int64 not null>` vs nullable item — equal with data.
  `hash_schema` keeps nested nullability. List item names (`item` vs `element`) are equal both
  ways: starfix ignores them.
- Cause: `StarfixArrowHasher._process_table_columns` rebuilds every struct/list/extension
  column's type from the first non-null *value* (`visitors.py` builds `pa.field(name, t)` and
  `pa.large_list(t)`, which reset nullability, metadata and item names). A second data
  dependency: a top-level or nested logical column with a value hasher (`Path`, `File`, …)
  becomes `large_binary` when it has data but keeps the extension type when zero-row/all-null.
- Rule: the hashed type is a function of the column's declared type only. Nested nullability is
  always identity (consistent with "nullability is identity", `hash_schema`, and the zero-row
  case). Recommended definition: derive the pre-pass type with a type-only transform that
  replaces each extension type that has a value hasher by `large_binary`, keeping every field's
  name, nullability and metadata and the list kind; build the array with that type.
- Large-type normalisation: unaffected (starfix equates large and small variants;
  `normalize_to_large_types` keeps nested fields). starfix's "unobservable names" rule: list
  item names stay ignored by starfix itself, so `list<item>` and `list<element>` remain equal;
  pod code cannot observe the item name. Nested field *metadata* other than extension keys
  stays ignored, as it is at the top level.
- Golden: row/data hashes of rows with a struct or list column whose nested fields are
  non-nullable (arrow, datagram, pipeline groups), and zero-row/all-null logical columns (now
  `large_binary` instead of the extension type). Exact list from the regeneration. No paths, no
  stored schemas: these are key **values** (`__input_data_hash`, entry ids, `_record_hash`).
- Overlap: #292 also moves datagram golden entries of dataclass columns; dataclass columns pass
  through the visitor unchanged (no value hasher), so I does not touch them.
- Size: medium.

### L. Structure hasher: set order and function defaults

- Repro: `hash_object({1,"1",2,"2"})` changes with `PYTHONHASHSEED` (seed 4 differs from 1-3);
  a function with default `frozenset({"x","y","z","w"})` hashes differently under every seed;
  a default `object()` embeds its memory address via `repr()`.
- L1 set order. Rule: set items are sorted by a total order on their canonical expanded form.
  Recommended: sort by the canonical JSON text of each expanded item (the same serialiser as
  the digest). Alternative: sort by each item's own digest. Golden `set.ints`, `set.strs`,
  `frozenset.ints` keep their order under JSON-text sorting, so they do not move; only sets
  whose `str()` order differs from the JSON-text order move: mixed types, strings with JSON
  escapes, sets of tuples or dicts.
- L2 function defaults (`FunctionSignatureExtractor`, used for function **values** and for the
  stored-but-unused `__pf_var_function_signature_hash` column; a function pod's identity does
  **not** use it, see A.6). Options: (L2a) a default enters as the semantic hasher's hash of the
  value; a default the strict hasher cannot hash raises `TypeError` (the caller adds the
  parameter and function name) **(recommended: same rule as every other hashed value)**;
  (L2b) as L2a, but an unhashable default enters as its canonical type name (two such defaults
  then share a hash, which is the bug class we are fixing); (L2c) keep `repr()` for unregistered
  objects (process-dependent, today's bug). L2a risk: a pod whose function has an unregistered
  default object fails at construction, because `PythonDataFunction` computes the signature
  hash in `__init__`.
- Golden: none expected for L1; L2 moves only function values with defaults (none pinned that I
  found). Stored: `__pf_var_function_signature_hash` values for functions with defaults (value
  only, read by nothing).
- Size: L1 small, L2 small-medium.

### Dict keys coerced with `str()`

- Repro: `{1:"x"}` == `{"1":"x"}`; worse, `{1:"a","1":"b"}` silently collapses to `{"1":"b"}`
  (one key overwrites the other). Related, same class: `(1,2)` == the dict
  `{"__type__":"tuple","items":[1,2]}`.
- Rule: a dict's keys keep their type. Options: (D1) a dict whose keys are all `str` and none is
  `"__type__"` stays a plain JSON object (unchanged hash); any other dict becomes
  `{"__type__":"dict","items":[[key, value], …]}` sorted by the canonical JSON of the key
  **(recommended: zero movement for ordinary str-keyed dicts, fixes the tuple/dict collision
  too)**; (D2) every dict becomes the tagged pair list (moves every structure hash containing a
  dict: schemas, name maps, constraints, so every pipeline/content hash and every
  system-tag column name, i.e. every path).
- Golden (D1): none expected (golden dicts have str keys). Paths: only for pipelines whose
  identity holds a non-str-keyed dict.
- Overlap: #301 changes dict/set conversion to **Arrow** (row values), not the structure hasher;
  no code overlap, golden-file conflicts only.
- Size: small-medium.

### F. Column selection identity depends on the container

- Repro: `SelectDataColumns` / `DropDataColumns` with `["x"]`, `("x",)`, `{"x"}`, `["x","x"]`
  give four hashes (`"x"` equals `["x"]`). `["x","y"]` and `["y","x"]` differ, but the output
  column order is the stream's (`['k','x','y']` for both): order and duplicates are not
  observable.
- Rule: identity is what the operator does. Select/Drop act on a set of names, so identity is
  the set. Recommended canonical form: a sorted list of unique names, stored as an immutable
  tuple attribute at construction (no aliasing of the user's list; `to_config` becomes
  deterministic for a set argument). A sorted list keeps every already-sorted list spelling's
  hash (golden uses `["age"]`, `["weight"]`, `["cohort"]`, `["subject"]`), so no golden entry
  moves. Paths move only for pipelines written with a tuple, a set or an unsorted list.
- Also found: a dead duplicate `SelectDataColumns` in `core/operators/filters.py` (not exported,
  not imported). Proposed: delete it in the F commit.
- Size: small.

### E. Join / MergeJoin de-duplicate unordered inputs

- Repro: `MergeJoin(m1, m1b)` (same `source_id`, other data) equals `MergeJoin(m1, m1)` on both
  hashes (expected under the source-identity contract), but `MergeJoin(m1, m1)` pipeline hash
  differs from `MergeJoin(m1, m2)` although all inputs share one schema.
- Cause: `argument_symmetry` returns `frozenset(streams)`; stream `__eq__` compares content
  identity, so equal-content inputs collapse to one element before hashing. The pipeline hash
  then hashes a one-element set for `(m1, m1)` and a two-element set for `(m1, m2)`.
- Rule: commutative inputs are a multiset: order is not identity, multiplicity is. Recommended:
  `argument_symmetry` returns a dedicated `UnorderedArguments` (multiset) that the structure
  hasher expands like a set (`"__type__":"set"`, items sorted) but keeps duplicates. A multiset
  without duplicates hashes exactly as today's set, so only joins with equal-identity inputs
  move. (`Join(a, a)` is already rejected for overlapping data columns, so this is MergeJoin
  and any future commutative operator.)
- Golden: none expected (golden merge_join uses distinct sources). Paths: only for pipelines
  with a self-MergeJoin.
- Size: small-medium (hasher handles the new type; `Join`, `BinaryOperator` return it).

### G. Record-id system tag type drifts

- Repro: `_tag_record_id` is `fixed_size_binary[16]` out of a source, Batch and Select;
  `large_binary` from `Tag.as_table`, because `Tag` stores system tags as Python values and
  re-infers their Arrow type. Function pipeline tables store `large_binary`. The operator cache
  (`_record_hash`, `cache_mode` LOG/REPLAY) hashes the direct-stream row with
  `fixed_size_binary` and the orchestrator row with `large_binary`.
- Options: (G1) `large_binary` everywhere: sources emit it, the Batch/GroupBy fold emits it, and
  `Tag` keeps the Arrow type of the system-tag table it was given instead of re-inferring
  **(recommended: the type every stored pipeline table already has; entry ids unchanged; matches
  the UUID logical type, which uses `large_binary` for Polars round-trips)**; (G2)
  `fixed_size_binary[16]` everywhere: changes the stored type of `_tag_record_id` in **every**
  function pipeline table (a stored-schema change) and moves every entry id.
- Stored schema (G1), flagged: the `_tag_record_id` column of **CachedSource** tables and of
  operator cache tables written from a direct stream changes from `fixed_size_binary[16]` to
  `large_binary` under an unchanged path (cache path = source content hash / node pipeline
  hash). Their `_record_hash` row keys move. Mitigation already in #309: v0.3 refuses tables
  holding v0.2 hash formats (`StoreHashLabelError`), so no v0.2 table is appended to.
- Golden: none expected (`stream_summary` reads record ids as Python bytes; no cache keys pinned).
- Size: medium.

### N. Default function version differs by entry point

- Repro: `@function_pod` → uri major `v0`; `FunctionPod.from_fn` → `v1`
  (`side_effect_function_pod` also defaults to `v1.0`; `PythonDataFunction` to `v0.0`).
- Rule: one function, one identity, whatever the entry point. Options: (N1) `v0.0` everywhere
  **(recommended: the documented default — `docs/concepts/identity.md`,
  `function-pods.md` — of the main entry point and `PythonDataFunction`)**; (N2) `v1.0`
  everywhere (moves the path of every `@function_pod` pod without an explicit version); (N3) no
  default (API break).
- Stored PATHS, flagged: under N1, pods built with `FunctionPod.from_fn` or
  `side_effect_function_pod` without an explicit version move from `…/v1/…` to `…/v0/…` (result
  and pipeline tables), so their stored results are no longer found. Migration: pass
  `version="v1.0"` to keep the old path. Note that #309 already refuses v0.2 stores, so this
  bites only stores written by v0.3 pre-releases.
- Golden: none expected (golden pods use `@function_pod`).
- Size: small (two defaults, docstrings, CHANGELOG).

### R. A subclass's type hash depends on logical-type registration order (added by the coordinator)

- Repro (two processes, dataclasses `Base(a: int)`, `Sub(Base)(b: str)`): building a `-> Base` pod
  before a `-> Sub` pod makes `Sub` hash equal to `Base`, moves the `-> Sub` pod's output-schema hash
  (uri, so path), and stores the output as `rtypes.Base` with field `b` silently dropped.
- Cause: `LogicalTypeRegistry.get_by_python_type` falls back along the MRO to a registered base.
- #292 fixes it (user-class logical types are `exact_python_type_only`); verified by running the same
  repro against #292's source, and #292 has a regression test. **Not duplicated here.**

## Proposed order (most valuable and safest first)

1. H (collision by construction; values only)
2. F (no golden movement; small)
3. E (no golden movement for distinct inputs)
4. Dict keys D1 + L1 set order (structure hasher; expected no golden movement)
5. N (one-line defaults; path note in CHANGELOG)
6. I (medium; moves row hashes)
7. G (medium; stored-type note)
8. L2 function defaults (lowest value: feeds an unused column)

Then: one golden regeneration, CHANGELOG `[0.3.0] — unreleased` (breaking changes and
migration), `docs/reference/hashing.md`, DESIGN_ISSUES entry, PR stacked on #309.

## Step 2: implementation (done)

Executed after the developer's choices. Per finding: failing public-interface test →
`uv run pytest <test> -v` shows FAIL → fix → PASS → golden suite run (record moved entries
without regenerating) → commit `fix(<scope>): … (ITL-942)` listing moved golden entries.

## Worker report (November 11-12, 2026)

PR: https://github.com/metamorphic-labs/orcapod-python/pull/315 (stacked on #309), head
`43f88ccf` + this report. CI green on 3.11/3.12/3.13, ruff, typecheck. CodeRabbit: round 1
(a63896bb) answered; incremental review a63896bb..43f88ccf generated no actionable comments.

Fixed, one commit each, test first: L1, G1, N1, I1, H1, F1 (+ dead `SelectDataColumns`
deleted), dict keys D1, `'__type__'` forgery (M2, `Encoded`), T. E pinned as intended. Golden:
7 changed (3 G, 4 H), 32 added, 0 removed.

Merge points with #314 (ITL-945): remove the strict-xfail marker on
`test_hash_key_differs_from_its_string_spelling`; keep `GenericAliasHandler` and
`UnionTypeHandler` returning `Encoded`; `hash` is reserved in `encoded.HASHER_TAGS`.

CodeRabbit findings on #309's code (shown in #315 only because of the stack), to route to #309:
- `src/orcapod/core/nodes/operator_node.py:904` (major): a read-only `OperatorJobNode` from
  `from_descriptor()` has `_operator = None`; reading `self.data_context` raises
  `AttributeError`; use `_data_context` as `FunctionJobNode._fetch_joined_records()` does.
- `src/orcapod/errors.py:465` (major): the side-effect error advises a fresh store as a safe
  retry; a fresh store has no completion records, so delivered inputs are delivered again.
- `src/orcapod/hashing/stored_hash_labels.py:168` (major): docstrings missing on `_Digest`,
  `_TableHasher.hash_table`, `_RecordReader.get_all_records`.
- `src/orcapod/side_effects.py:816` (major): `_log_labels_checked` is cleared only by
  `attach_databases`; re-running a job object skips the log scan; clear it in `execute` and
  `async_execute`.
- `src/orcapod/side_effects.py:820` (major): the full invocation-log label scan runs
  synchronously inside `async_execute`, blocking the event loop.
- `tests/test_pipeline/test_stored_hash_labels.py:53` (major): docstrings missing.
- Nitpick `tests/test_arrays/test_array_v02_store.py:103-105`: check for duplicate rows
  before building `doubled`.
- Nitpick `tests/test_hashing/golden/cases.py:621`: `arrow_extension_schema_hashes` docstring.

Out of scope, reported: list of a value-hashed logical type folds `[]` and `[None]` alike
(`visitors.py`, routed to ITL-935); spec ITL-938 cut list still says ITL-942 changes MergeJoin
input identity (E was withdrawn); deltalake processes hang at exit in this sandbox (env only).
