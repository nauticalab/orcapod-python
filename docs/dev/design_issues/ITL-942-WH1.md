## Hash-site audit — wrong hashes fixed before v0.3 (ITL-942)

### WH1 — Hashes that collided or depended on data, process or spelling
**Status:** resolved
**Severity:** high
**Issue:** ITL-942

The ITL-938 hash-site audit (Appendix A.9 of its design) found values that were wrong: two
different things shared a hash, or one thing hashed differently depending on its data, the
process or its spelling. Fixed, one commit each, every moved golden value listed:

- **H** Batch/GroupBy source-id fold concatenated members with no separator
  (`["ab","c"]` == `["a","bc"]`, `None` == `""`). **Fix:** SHA-256 of the canonical JSON
  member list (`arrow_utils.fold_system_tag_values`); record-id fold encodes `None` as `null`.
- **I** Arrow hashing rebuilt nested types from the first non-null value, dropping nested
  nullability for columns with data only. **Fix:** `SemanticHashingVisitor.hashed_type`
  derives the hashed type from the declared type alone.
- **L1** Set items sorted by `str()`, so `{1, "1"}` hashed per process. **Fix:** sorted by
  canonical JSON. (L2, function defaults entering as `repr()`, moved to ITL-953.)
- **Dict keys** went through `str()`. **Fix:** non-`str` keys, and any `"__type__"` key, give
  the pair-list form `{"__type__":"dict","items":[...]}`; type handlers return `Encoded` for
  tagged objects, so data cannot forge a tagged value.
- **F** Column selection operators hashed the container type and order of `columns`.
  **Fix:** identity is the sorted list of unique names.
- **G** `_tag_record_id` was `fixed_size_binary[16]` from a source and `large_binary` after a
  `Tag`. **Fix:** `large_binary` everywhere.
- **N** Default function version `v0.0` (`@function_pod`) versus `v1.0` (`from_fn`,
  `side_effect_function_pod`). **Fix:** `v0.0` everywhere.
- **E** (reviewed, kept by decision): `MergeJoin(m1, m1)` differs from `MergeJoin(m1, m2)`;
  inputs with one source identity count as one. Pinned by tests.
- **R** (subclass hashing as its registered base, with fields dropped) is fixed by ITL-881.

- **T** a namedtuple's fields were sorted by name, so `Point(x=1, y=2)` and a namedtuple
  declared `(y, x)` holding `y=2, x=1` shared a hash. **Fix:** fields in declared order.
