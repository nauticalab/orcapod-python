# ITL-881 rebuild: field order is identity (PR #292)

Continues `2026-11-10-itl-881-user-class-identity-impl.md`. Worker notes are at the end, so a
successor can pick this up.

**Goal:** rebuild PR #292 on current `main` and change the identity of a user class so that
declared field order is part of it (developer, November 11, 2026, reversing "not order").

## State on arrival (November 11)

- Worktree clean, nothing unpushed; head `b930f629`, conflicting with `main`.
- Merged `origin/main` as `ac719294` (pushed). Conflicts: CHANGELOG, DESIGN_ISSUES, mkdocs,
  `delta_lake_databases.py`, `schema_utils.py`. Golden files merged cleanly and pass.
- Merge finding: main's registry MRO lookup (ITL-888) returned the base class's logical type
  for a dataclass subclass, bringing back the subclass-stored-as-base bug. Fixed in the merge
  commit; `test_subclass_has_its_own_identity_and_keeps_its_fields` caught it.
- Merge decision: main now wraps Delta read failures in `StoreReadError`. `TypeIdentityError`
  still propagates unwrapped (the table is intact; the class differs).

## Identity today (PR head)

Record `{"kind", "name", "fields": {name: token}}`, canonical JSON with sorted keys, identity
string `Name@<sha256>`. Fields are sorted by name in the record and in the stored struct, so
order never enters.

## Change

1. `identity_record`: `fields` becomes a list of `[name, token]` pairs in declared order
   (`dataclasses.fields` order for `init=True` fields; `model_fields` order for pydantic).
2. Stored struct lists fields in declared order (as on `main` and v0.2.0).
3. `describe_fields` prints declared order. Read-path comparisons use the ordered list.
4. Docs, spec, CHANGELOG, DESIGN_ISSUES UCI1: "in any order" becomes "in declared order".
5. Golden: twins `GoldenTwinAB`/`GoldenTwinBA` must pin different values. They share the
   declared name `golden.Twin`; with order in the identity that is two types under one name,
   which is allowed (same as two field sets under one name).

No label or version marker is introduced: the identity string is `Name@<sha256>` and the
metadata is `{"category", "identity"}`.

## Evidence (run on this branch, starfix 0.4.0)

- starfix 0.4.0 hashes a plain `struct<x, y>` and `struct<y, x>` with the same values to the
  same hash (order-insensitive).
- The Arrow extension name enters the hash: two extension types with different names over the
  same storage hash differently. Through a `DictSource`, two classes differing only in name
  have different row hash, content hash and pipeline hash, for a scalar column, a `list[...]`
  column and a nested class.
- So with order in the identity string, value hashes separate on starfix 0.4.0. The PR stays
  on `main`; `ordered_struct_fields` (starfix 0.5.0, PR #309) is not needed.
- Equality: a dataclass compares its fields as a tuple in declared order. Pydantic `__eq__`
  compares type and `__dict__` (order-insensitive), but two models differing in order are
  different types, and `model_dump`, `model_fields`, iteration and the JSON output all follow
  declared order, so pod code observes it.

## Tests first (must fail before the change)

In `tests/test_user_types/`, through `DictSource` / function pods:

- Same fields, other order: different type name, stored struct order follows the class, row
  hash, content hash and pipeline hash differ (dataclass and pydantic).
- Subclass: own identity and all its fields, with base-first and subclass-first registration
  run in two fresh processes; hash, pod uri and stored fields equal across the two orders
  (dataclass and pydantic).
- Class redefined with an added field: new type, new tables, old results untouched.
- Re-run with unchanged order still rebinds silently.

## Review findings still open

1. CodeRabbit (minor, unanswered thread): testing a read candidate registers its nested
   classes under their default names, so a later `register_type(Inner, name=...)` raises.
   Fix: make the probe leave no registration behind when the candidate is rejected; add the
   nested recovery test.
2. CodeRabbit nitpick (review body): assert that compaction rewrote the result table.
3. IPC read of non-large storage: answered, deferred to ITL-894 (strict xfail in place).

## Verification

Full suite on 3.11, 3.12, 3.13 one at a time; ruff; basedpyright; push; CI; CodeRabbit round.

## Worker notes

- Coordinator approved the plan (November 11): field order in identity, stay on `main`,
  `TypeIdentityError` unwrapped (pinned in `tests/test_databases/test_delta_read_failures.py`),
  defaults/methods/computed fields outside the identity (stated in the docs).
- Done and pushed: identity record with ordered fields; storage struct in declared order;
  golden regenerated (vs `main`: 17 changed + 8 replaced = the 25 approved entries, rest
  added; twins pin different values; ndarray-field fixture added); `probe_record` for read
  candidates (nested classes stay undeclared); compaction test asserts the rewrite; docs,
  spec, CHANGELOG, DESIGN_ISSUES UCI1.
- Remaining: full suite on 3.11/3.12/3.13, ruff, basedpyright, PR body, CI, CodeRabbit round.
- Out of scope, to report: the semantic hasher expands a namedtuple with fields sorted by
  name (`semantic_hasher._expand_namedtuple`), so namedtuple field order does not enter its
  hash, although a namedtuple compares as an ordered tuple.
- Second CodeRabbit round (plan approved by the coordinator): identity errors fatal under
  `error_policy="continue"` in both orchestrators (own commit); `None` nested class on read;
  stored record checked against its extension name; lazy candidate lookup; distinct scenario
  values; test docstrings; spec example. Declined: stale read converter after rebinding (not
  reproduced). Deferred: pydantic field alias fails on read (also on `main`).
- Full suite after these: 7794 passed, 96 skipped, 20 xfailed on 3.11, 3.12 and 3.13.
