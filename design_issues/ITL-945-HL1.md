## Hash labels — one rule for every label (ITL-945)

### HL1 — Hash labels were inconsistent, part of hash inputs, and written in several places
**Status:** resolved
**Severity:** high
**Issue:** ITL-945 (design: ITL-938, section 5.6)

Labels came in three formats (`semantic_v0.1`, `ndarray_v1`, `merkle_sha256`), the core ones
were written in three places (`versioned_hashers.py`, the data context file, its schema), a
nested hash entered its parent hash as `"<label>:<hex>"` so that renaming a label moved every
hash above it, and names were mixed: path components and the function `uri` held labelled
hashes while system-tag column names and the operator `uri` held bare digests. The record ids
of operator-node and cached-source tables were bare hex text.

**Fix:** the rule decided by the developer on November 11, 2026, stated in
`docs/reference/hashing.md` ("The hash label rule"):

- Labels are `<algorithm>_v1` (`semantic_v1`, `arrow_v1`, `directory_v1`, `ndarray_v1`,
  `polars_expr_v1`); the file hash keeps `sha256`. Each is one constant beside its algorithm;
  the data context file no longer carries `hasher_id`.
- No hash input holds a label. Sites changed: nested hashes in the structure hasher
  (`{"__type__": "hash", "digest": "<hex>"}`), the type and schema handlers, the extension
  cell of the Arrow hasher, the entry-id preimage (`__input_data_hash` as raw digest), the
  operator-node record hash (`__node_content_hash` as raw digest), the run snapshot hash, and
  the identity of a producer-less `ArrowTableStream` (its derived `_content_hash` column is no
  longer hashed). The last four were found by tracing and by the test below, not by the
  design's site table.
- Names hold bare digests: function `uri`, `schema:` / `instance:` / `node:` path components,
  source-node slot names. Exception (developer, November 11, 2026): a source id is the
  source's name, so a default source id built from a hash keeps a source-kind prefix and the
  hash's label (`derived:semantic_v1:<hex>`, `<node path>:semantic_v1:<hex>`).
- Stored hash values are labelled, now including `_record_hash` of operator-node and
  cached-source tables.
- `tests/test_hashing/test_hash_label_rule.py` recomputes every golden value under renamed
  labels and requires every digest and name to be unchanged.

Known limits, follow-ups:
- A nested hash is the tagged object `{"__type__": "hash", ...}`. A user dict with a
  `"__type__"` key is not escaped by the structure hasher today (a tuple and the dict
  `{"__type__": "tuple", "items": [...]}` hash alike); ITL-942 closes that for every tag.
- A dict key that is itself hashed (a non-primitive key) becomes the `str()` of the tagged
  object. Dict keys are ITL-942's subject.
- Because a default derived source id holds a label and is used as it is in system-tag and
  source-info values, which are hashed as cell data, renaming the structure label would move
  the entry ids of rows that come from a `DerivedSource` with a default id. The rename test
  does not cover this (no golden case uses a derived source).
- `InvocationContext.invocation_hash` keeps `<label>:<hex>::<label>:<hex>` (developer: the
  side-effect topic is deferred to after the v0.3 tag). The library feeds it into no hash.
- `migrations/pipeline_db.py` (v0 to v1 pipeline tables) follows the new entry-id recipe; its
  other columns were not re-checked against the current recipe.
- `hashing/stored_hash_labels.py` still recognises v0.2 operator-node and cached-source rows
  by their bare hex digest; ITL-944 replaces that module with the data context column.

**Revision (ITL-1154, developer, November 15, 2026):** rule 6 is reversed. Before data
context `v1` was first released, the developer chose the from-scratch design: **a hash always
carries its label, except where it is written into a name, whose grammar fixes which
algorithm produced it.** Rule 6's argument ("the position and type of the value already tell
the two apart") held in Arrow cells but not in semantic structures, where a nested hash had no
type and no label, and several algorithms are plain SHA-256 over byte strings that can
overlap (a `File` whose bytes are a value's canonical JSON shared that value's digest). Every
site listed above now takes the hash with its label: the nested structure hash
(`{"__type__": "hash", "label": ..., "digest": ...}`), the logical-type cell
(`<identifier>::<label>:<digest>`), `__input_data_hash` in the entry-id preimage,
`__node_content_hash` in the operator-cache row hash, the run snapshot; and, found by the
ITL-1154 inventory, the `ListSource` content-mode tag function hash, the ordered Arrow digest
in the pandas value hash (now labelled `arrow_ordered_v1`) and the file hash in a directory
entry. `test_hash_label_rule.py` is flipped: a golden value moves under renamed labels if and
only if a label feeds it. The known limits above about `InvocationContext.invocation_hash` and
the derived default source id still apply; the latter is now the stated source-id rule
(`<kind>:<label>:<hex>`, and `<algorithm>_v<N>:<hex>` for a dedicated algorithm such as the
`batch_v1` / `group_v1` folds). The inventory of every hash site, with its class, is in
`docs/reference/hashing.md` ("Hash site inventory").
