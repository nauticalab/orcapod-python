# ITL-945 hash label rule: step 1 plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the nine-point hash label rule (ITL-938 design, section 5.6): labels
`<algorithm>_v1`, each defined once, never part of a hash input, absent from names, present on
stored hash values.

**Architecture:** Each label becomes one constant next to the algorithm it names. Every place
that feeds a nested hash into a parent hash feeds the digest only. Every place that builds a
name (path component, column name, token) uses the bare hex digest. The two bare row keys
become labelled bytes.

**Tech stack:** Python, pyarrow, starfix 0.5 (hash format 2), pytest.

**Branch:** `eywalker/itl-945-apply-the-hash-label-rule-one-format-labels`, from the head of
PR #309 (`b88f7eb3`). PR against `main`, first body line `Stacked on #309`, `Fixes ITL-945`.

**Status:** step 1 (this plan) sent to the coordinator; no library change before approval.

---

## (a) Site inventory

Method: read every use of `ContentHash.to_string`, `to_hex`, `to_prefixed_digest`, `__str__`,
`.digest`, `.method`, `display_name`, `from_string`, `from_prefixed_digest`; then ran the whole
test suite with `ContentHash.__getattribute__` and its dunder methods wrapped to record the
calling line (results in the section "Tracer results").

### A1. Label definitions today

| Label today | Becomes | Written in |
|---|---|---|
| `semantic_v0.1` | `semantic_v1` | `hashing/versioned_hashers.py:37`, `contexts/data/v0.1.json:132`, `contexts/data/schemas/context_schema.json:186` |
| `arrow_v0.2` | `arrow_v1` | `hashing/versioned_hashers.py:42`, `contexts/data/v0.1.json:141`, `contexts/data/schemas/context_schema.json:178`; also `hashing/stored_hash_labels.py:50` (`_LABEL_OF_FORMAT`) |
| `merkle_sha256` | `directory_v1` | `hashing/directory_hashers.py:215`, built as `f"merkle_{algorithm}"` |
| `ndarray_v1` | unchanged | `builtin_handlers.py:330` (`NumpyArrayHandler.HASH_LABEL`), once |
| `polars_expr_v1` | unchanged | `semantic_hashing/polars_expr.py:78` (`POLARS_EXPR_HASH_LABEL`), once |
| `sha256` (file) | unchanged, the stated exception | `hash_utils.py:173-213`: the name of the digest function the file hasher is configured with |

### A2. Hash inputs that contain a label today (rule 6)

| # | Site | Preimage today | Preimage after |
|---|---|---|---|
| 1 | `semantic_hasher.py:250` `_expand_structure`, a `ContentHash` leaf | JSON string `"<label>:<hex>"` | JSON string `"<hex>"` |
| 2 | `semantic_hasher.py:314` `_expand_element`, any non-primitive, non-container element (type, schema, function, bytes, pod, stream, value of a handler) | `"<label>:<hex>"` | `"<hex>"` |
| 3 | `builtin_handlers.py:138-152` `GenericAliasHandler`, `UnionTypeHandler` | `origin` and `args` as `"<label>:<hex>"` | `"<hex>"` |
| 4 | `builtin_handlers.py:251` `SchemaHandler` | `{field: "<label>:<hex>"}` | `{field: "<hex>"}` |
| 5 | `hashing/visitors.py:310` extension cell | `<ext name with . as :>` + `::` + `<label>:` + digest bytes | `<ext name with . as :>` + `::` + digest bytes |
| 6 | `core/nodes/function_node.py:119` entry-id preimage column `__input_data_hash` (used by `compute_base_entry_id`, `compute_pipeline_entry_id`, and the side-effect log key, `side_effects.py:234`) | `b"<arrow label>:"` + digest | digest bytes only (the stored column `__input_data_hash` stays labelled) |
| 7 | `core/nodes/operator_node.py:834-848` operator-cache row key: Arrow hash of the row including `__node_content_hash` | that column holds `b"<semantic label>:"` + digest | hash the row with that column holding the digest only; store the column labelled as today |
| 8 | `pipeline/job.py:861-866` run snapshot hash, `sha256("\n".join(leaf content hash strings))`, shown in the run URI `name@<16 hex>` | `"<label>:<hex>"` lines | `"<hex>"` lines |
| 9 | names that are themselves hash inputs: function `uri` (in the pod identity), default `DerivedSource.source_id`, `SourceNode` slot name | labelled, see A4 | bare, see A4 |

Sites 6, 7 and 8 are not in the brief or the design table. Without them, renaming the Arrow or
structure label moves every entry id, operator-cache key and run URI.

Already label-free: the directory hasher feeds `file_hash.digest` (`directory_hashers.py:96`);
record ids and folds use `uuid5` / `combine_hashes` over bare text.

### A3. Stored hash values (rule 4)

| Value | Form today | After |
|---|---|---|
| `__input_data_hash`, `__output_data_hash`, `__node_content_hash`, `__pipeline_base_entry_id`, pipeline `__record_id`, side-effect `record_id` / `record_id_hash`, `__pf_var_function_signature_hash`, `__pf_var_function_content_hash` | labelled bytes | keep |
| `_content_hash` column (`arrow_table_stream.py:223`) | labelled string | keep |
| file-hash cache value (`hash_cachers.py:317`, `postgres_hash_cacher.py:259`) | labelled bytes | keep |
| saved job JSON: node keys, edges, `content_hash`, `pipeline_hash`, `node_job_content_hashes` (`pipeline/graph.py`, `job.py`, `base.py`, `sources/base.py:125`), read back with `ContentHash.from_string` | labelled string | keep |
| observer `node_hash`, hook `pod_hash`, graph attribute `pipeline_hash` | labelled string | keep (reported values) |
| operator-cache row key `_record_hash` (`operator_node.py:847-853`) | bare hex text in `large_binary` | **add**: `to_prefixed_digest()` |
| cached-source row key (`cached_source.py:240-248`) | bare hex text in `large_binary` | **add**: `to_prefixed_digest()` |

### A4. Names (rule 5)

| Name | Site | Today | After |
|---|---|---|---|
| function uri component 2 (path, pod identity, source-info token of every function output) | `data_function.py:232` | `semantic_v0.1:<hex>` | `<hex>` |
| `schema:<pipeline hash>` | `function_node.py:358`, `operator_node.py:261`, `side_effects.py:852` | labelled | bare |
| `instance:<content hash>` | `function_node.py:360`, `operator_node.py:263` | labelled | bare |
| `node:<content hash>` | `cached_source.py:197` | labelled | bare |
| default `DerivedSource.source_id` | `derived_source.py:51` `derived:<hash>`; `function_node.py:1912`, `operator_node.py:1222` `<path>:<hash>` | labelled | bare |
| `SourceNode` slot name for a non-source stream | `source_node.py:452`, `:711` `<label>:<hash>` | labelled | bare |
| system-tag column names, operator uri, default table-hash `source_id`, list-source tag-function hash | `to_hex` already | bare | unchanged |
| `Join` input ordering key | `join.py:52` sorts by `to_string()` | labelled | `to_hex()` (no value change; same key as `merge_join.py:252`) |

### A5. Record ids (rule 7)

No change. Source record id (`uuid5`), folded record id (`uuid5`), folded source id
(`combine_hashes`), result row key and `__data_id` (`uuid7`) carry no label today.

### Tracer results

Suite on the base commit with the tracer: 7755 passed, 96 skipped, 18 xfailed (Python 3.12).
97 library lines turned a `ContentHash` into text or bytes. Every one is in the tables above
or in this list of sites that need no change:

- in-memory keys and comparisons on the labelled string: `pipeline/base.py` (node map, graph
  edges), `pipeline/job.py:173`, `:382`, `:568`, `data_function_proxy.py:308`,
  `source_proxy.py:110-119`;
- `__hash__` (`core/base.py:187`, `content_identifiable_mixin.py:182`) and the short display
  name (`content_identifiable_mixin.py:259`);
- an error message (`errors.py:208`, `repr`);
- old migrations (`migrations/pipeline_db.py`, `migrations/result_db.py`);
- `invocation.py:51`, the invocation hash handed to side-effect functions (open point 4).

The tracer found no site that the reading had missed. Sites the suite does not execute, found
by reading only: `derived_source.py:51`, `postgres_hash_cacher.py:259`.

---

## (b) Single definition

Each label is a module-level constant in the module that implements the algorithm:

| Constant | Module |
|---|---|
| `SEMANTIC_HASH_LABEL = "semantic_v1"` | `hashing/semantic_hashing/semantic_hasher.py` |
| `ARROW_HASH_LABEL = "arrow_v1"` | `hashing/arrow_hashers.py` |
| `DIRECTORY_HASH_LABEL = "directory_v1"` | `hashing/directory_hashers.py` |
| `NumpyArrayHandler.HASH_LABEL` | unchanged |
| `POLARS_EXPR_HASH_LABEL` | unchanged |

`hasher_id` stays a constructor argument of `SemanticAwarePythonHasher` and
`StarfixArrowHasher` and now defaults to the constant. The manifest refers to a label by naming
the class in `_class`; the two `"hasher_id": ...` lines are deleted from
`contexts/data/v0.1.json`, the two literals from the examples in `context_schema.json`, and the
two constants from `versioned_hashers.py`. A test reads every shipped file under
`contexts/data/` and fails if one spells a hasher label.

Fit with ITL-944: its branch adds `contexts/data/v1.json` as a copy that still has both
`hasher_id` lines and does not touch `v0.1.json`, so there is no textual conflict. At
integration the two lines are deleted from `v1.json`; until then the test above fails on the
merged tree, which is the intended signal.

## (c) Rule 6 test

`tests/test_hashing/test_hash_label_rule.py`, public interface only. Two default contexts are
built that differ only in labels (`SemanticAwarePythonHasher(hasher_id="renamed_v9", ...)`,
`StarfixArrowHasher(hasher_id="renamed_v9", ...)`). For each, compute: structure hashes of a
nested structure with types, a schema, bytes and an ndarray; the Arrow hash of a table with an
extension-typed column; the content and pipeline hash of a source, a join, a function pod
stream; a function node's `node_identity_path`; entry ids and the operator-cache row key
through a run on an in-memory store. Assert every digest and every name equal between the two
contexts, and every label different.

## (d) Reading older values

v0.2 tables are a skipped generation. Nothing in v0.3 parses a labelled name: the only parsers
of names are `cli/migrate.py:38` (`startswith("schema:")`, prefix unchanged), `join.py:444` and
`arrow_utils.py:1574` (system-tag names, bare already), and `registry.py:214` (context keys).
`ContentHash.from_string` / `from_prefixed_digest` are applied to stored values only.

## (e) Public API

Kept unchanged: `ContentHash.to_string`, `to_hex`, `to_prefixed_digest`, `from_string`,
`from_prefixed_digest`, `display_name`, `__str__`. Behaviour changes: every label string;
`BasicDirectoryHasher` label; function `uri[1]`, paths, default derived `source_id` and slot
names are bare; `get_versioned_*` defaults come from the constants.

## (f) Golden groups expected to move

706 entries in five files. Every entry that prints a label moves at least in its label text
(382 entries). Digests move for: every structure hash with a nested non-primitive leaf, every
schema hash and system-tag column name, every path, every Arrow hash over an extension cell,
every entry id and operator-cache key. Digests that must not move: Arrow hashes of plain tables
and schemas (`arrow.json` `table.*`, `schema.*`), flat structure hashes of primitives, value
hashes of ndarray, Polars expression, file and directory. Exact counts are reported per group
in the PR after implementation.

## (g) Commit order

1. `test(hashing)`: failing tests for the rule.
2. `refactor(hashing)`: rename labels.
3. `refactor(hashing)`: single definition per label.
4. `feat(hashing)!`: labels out of hash inputs.
5. `feat(core)!`: bare digests inside names.
6. `feat(store)!`: labelled row keys for the operator cache and cached source.
7. `test(golden)`: regenerate, diff described by group.
8. `docs`: `docs/reference/hashing.md`, CHANGELOG, DESIGN_ISSUES, CLAUDE.md and `.zed/rules`.

## (h) Open points

Each with my recommendation; answers are recorded under "Decisions".

1. **Three hash inputs outside the brief carry a label** (A2 sites 6, 7, 8: entry-id preimage,
   operator-cache row-key preimage, run snapshot hash). Recommend: feed the digest only at all
   three; the stored columns stay labelled.
2. **Form of a nested hash in the structure JSON.** Recommend the bare hex string. Consequence:
   a nested hash and a plain string equal to its hex give the same parent hash (the same kind
   of equality already exists between `b"a"` and `"61"`). Alternative: a tagged object
   `{"__type__":"hash","digest":"<hex>"}`.
3. **Extension cell token.** Recommend `<ext name>::` + raw digest bytes. ITL-935 changes the
   identifier part of the same line (`visitors.py:310`): a one-line overlap at integration.
4. **Invocation hash** (`InvocationContext.invocation_hash`, `"<label>:<hex>::<label>:<hex>"`,
   public, used by users as an idempotency key). It is an identifier built from two hashes, so
   recommend bare digests: `"<hex>::<hex>"`. This changes a documented public string.
5. **Where a label is defined.** The design says "defined in a single place, and is listed in
   the data context file". Recommend the constant in code and no literal in the manifest (the
   manifest names the class). The alternative, the manifest as the definition, keeps the
   literal in every manifest file and leaves the per-type labels in code anyway.
6. **`BasicDirectoryHasher(algorithm=...)`.** `directory_v1` names the SHA-256 Merkle
   procedure. Recommend: the constructor raises `ValueError` for any other algorithm; the
   manifest line `"algorithm": "sha256"` is untouched.
7. **Row-key readers.** With labelled row keys, `operator_node._read_stored_records` and
   `cached_source._read_cached_records` must filter on the label instead of the starfix format
   (one argument each). ITL-944 rewrites both readers; at integration its version wins.
8. **`contexts/data/v0.1.json` after ITL-944.** On this branch it is the only manifest, so it
   gets the `_v1` labels. What `std:v0.1:default` means once `v1.json` exists is ITL-944's
   question.
9. **Saved job JSON** keeps labelled hash strings as node keys and fields: they are stored
   values read back with `ContentHash.from_string`. Recommend keep.

## Decisions

Coordinator, November 11, 2026 (reply to the step 1 question):

1. Yes: digest only at the three extra hash inputs; stored columns stay labelled. List them in
   the PR notes for the design table.
2. **Not** the bare hex string. A nested hash enters as a tagged value with the digest and no
   label: `{"__type__": "hash", "digest": "<hex>"}`. The structure hasher has no escaping for a
   user dict with a `"__type__"` key today (a tuple and the dict
   `{"__type__":"tuple","items":[…]}` already hash alike); ITL-942 option D1 wraps any dict
   that has a `"__type__"` key as `{"__type__":"dict","items":[…]}`, which closes the collision
   for the `hash` tag as it does for `tuple`. Handlers return `ContentHash` objects inside
   their structures, so the tagged form is produced in one place (`_expand_structure`).
3. Yes. The line is `src/orcapod/hashing/visitors.py:309-311`.
4. Developer (via coordinator): leave as is for v0.3. `invocation_hash` keeps
   `<label>:<hex>::<label>:<hex>` with the new label names; the side-effect topic is deferred
   to after the tag. Checked: the library feeds `invocation_hash` into no hash computation (it
   is only handed to the user's function and hooks).
5. With the developer. Do not implement. Until answered: rename the labels where they are
   written today; add `DIRECTORY_HASH_LABEL`.
6. Yes: `ValueError` for any algorithm other than `sha256`.
7, 8, 9. Agreed as stated.

## Notes

- Tracer plugin: scratchpad `plug/chtrace.py`; not committed.

## Status (November 11, 2026)

Implemented and pushed; PR #314 against `main`, stacked on #309.

- Decision 5 (developer, via coordinator): each label is defined once in code; the data
  context file names the hasher and does not repeat the label. Implemented as in (b).
- Sites found during implementation, beyond section (a): the identity of a producer-less
  `ArrowTableStream` hashed its derived `_content_hash` column (labelled strings); a loaded
  job rebuilt its table path from stored labelled hash strings (`function_node.py`,
  `_bare_digest`); `migrations/pipeline_db.py` mirrors the entry-id recipe.
- Golden files regenerated on this branch: 225 label text only, 195 digest or name moved, 250
  entries renamed, 36 unchanged (706 entries).
- CodeRabbit round 1: tag sorting restored in the producer-less stream identity (a regression
  of this branch, no golden value affected); test and doc wording fixes; declined findings
  answered on the PR.
- Correction to decision 2: the `"__type__"` collision is not closed by ITL-942 option D1
  (non-str keys only) but by its mechanism M2 (in progress): only the hasher's own encodings
  may produce an object with a `"__type__"` key; plain mappings with that key take a pair-list
  form. `_nested_hash` is unchanged; `GenericAliasHandler` and `UnionTypeHandler` must return
  ITL-942's `Encoded` marker when the branches meet.
- Consequence raised with the coordinator: every v0.3 table path differs from v0.2's, so a
  v0.2 side-effect log is not found at the v0.3 path and the explicit failure of ITL-910 does
  not trigger for a v0.2 store. Documented; detection belongs to ITL-944.
