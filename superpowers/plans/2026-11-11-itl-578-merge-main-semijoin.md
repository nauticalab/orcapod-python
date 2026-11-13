# ITL-578 — merge main after #291 (SemiJoin via Polars)

PR #286 conflicts with `main` (b197544e) after #291 (ITL-847) merged. Mechanical merge, no
design decisions.

## Steps

1. `git merge origin/main` (no rebase). Only conflict: `DESIGN_ISSUES.md`, where both sides
   appended a new section at the end. Keep main's SemiJoin SJ1 section, then ours (Polars PL1)
   last, each exactly once.
2. No CHANGELOG change: the PR has no CHANGELOG entry, and main has no `[0.3.0] — unreleased`
   section (brief: no other changes). Flag in worker_done.
3. Golden check: `git diff origin/main -- tests/test_hashing/golden/data` must be additions
   only (the `polars_*` entries). Run `tests/test_hashing/golden` without regenerating.
4. SemiJoin over a polars-typed column: #291 casts the result to the left table's schema, so a
   `polars.dataframe` logical column on the left must survive. Add one public-interface test
   in `tests/test_core/function_pod/test_polars_dataframe_values.py` if it adds coverage.
5. Full suite on 3.11/3.12/3.13, ruff check, ruff format --check, basedpyright.
6. Push, wait for CI, request CodeRabbit once, handle its round (test/doc/wording on merit;
   anything touching storage bytes, hash definition, dtype grammar, names, behaviour: ask).
7. worker_done with the final head.

## Progress notes

- (update as steps complete)
- Merge fbf2ed66, SemiJoin test 2abaf4c5; suite green 3.11/3.12/3.13; CI green.
- CodeRabbit round on 2abaf4c5: column-order cache-key finding declined (coordinator approved,
  developer decision Nov 10), docs caveat added in hashing.md and function-pods.md; body-only
  items answered as no-change in one top-level comment.

## Revision — developer decision, November 11, 2026: order is identity

Everything a pod can observe as a frame's logical content is identity (coordinator approved
the property table). Labels stay `polars_dataframe_v1` / `polars_series_v1` (unreleased).

1. Descriptor: `columns` and `struct` become ordered lists of `[name, dtype]` pairs, at every
   depth (column order, struct field order).
2. Struct validity: the hash projection turns each struct into
   `struct<valid: bool not null, fields: struct<positional children>>`, children nulled under a
   null parent, so a null struct differs from a struct of nulls (top level and nested).
   starfix root cause is on ITL-848; the Arrow table hash is not touched.
3. Categorical: `{"categorical": [name, namespace, physical]}` from its `Categories`.
4. OUT: chunking, rechunk, sortedness flags (not stored), physical codes / registry, Arrow
   layout, memory, `_PL_*` metadata. Stated in docs/reference/hashing.md.
5. Tests first: reordered columns / struct fields (nested) differ; null struct vs struct of
   nulls differ; Categories params differ, default `Categorical()` equal across processes;
   in-memory + Delta, sync + async preserve order; cached pod returning `df.columns[0]`
   recomputes for a reordered frame.
6. Golden: regenerate only the `polars_*` entries; diff must touch no other line.
7. Docs: remove the column-order caveat (9ed28caa), state the new rule + OUT list. CHANGELOG:
   add `[0.3.0] — unreleased` above `[0.2.0]` with the ITL-578 entry. PR description.
8. Reply on CodeRabbit thread 4536018197: accepted.
- Order-identity change done (4bb08754, 06f1682f); suite green 3.11/3.12/3.13; CI green on
  06f1682f. Threads 4536018197 and 4536193979 answered. CodeRabbit rate-limited on
  06f1682f at 09:43Z; retry 1 at ~10:05Z, retry 2 at ~10:25Z if needed.

## Prepared for starfix ordered mode (ITL-898) — hash NOT final

Developer decision, November 11, 2026: starfix gets an option to make column and struct
field order significant; #286 will move to it. The hash is unchanged for now; the switch
points are isolated in `logical_types/polars_type.py`:

- `_values_digest(df)` — the only call into starfix. Owns the type projection
  (`_hash_projection`) and the struct-validity workaround (`_with_table_struct_validity`).
- `_ordered_pairs(pairs)` — the only place column and struct field order enter the descriptor.

Under a starfix ordered mode:

| Part | Fate |
|---|---|
| Order in the descriptor (`_ordered_pairs`, columns and struct fields) | Redundant if starfix hashes order and names. Harmless to keep; dropping it changes the bytes. |
| Struct-validity wrapper (`_with_struct_validity`) | Redundant only if the new starfix spec also tells a null struct from a struct of nulls (ITL-848). Otherwise stays. |
| Top-level `Null` columns dropped from the projection; their name, position and dtype live only in the descriptor | Stays (unless starfix hashes `null` columns), so column order still needs the descriptor for them. |
| Nested `null` → all-null `bool`; `fixed_size_list` (Polars `Array`) → `large_list` | Stay unless starfix supports those types; the descriptor records the real dtypes. |
| Dictionary decode (Categorical / Enum) | Stays. |
| Descriptor dtype distinctions Arrow lacks: Enum categories and order, Categorical `Categories`, String vs Categorical vs Enum, Array size, nested Null | Stay. |
| Descriptor `kind`, `height`, Series `name` | Stay (`height` matters for zero-column and all-Null frames). |
| Descriptor Decimal / Datetime / Duration parameters, int / float width | Redundant with the Arrow schema starfix sees, harmless to keep. |
