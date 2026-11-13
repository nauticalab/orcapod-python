# ITL-981 plan: library value types (mechanism + NumPy)

Issue: https://linear.app/metamorphic/issue/ITL-981
Branch: `eywalker/itl-981-library-value-types-native-storage-strict` (from `origin/main` at `c8e9eb21`)
Design note: `superpowers/specs/2026-11-12-itl-981-library-value-types-design.md`

## State (keep this section current; a successor continues from here)

- November 12, 2026: design sent to the coordinator (one `ask`). Coordinator's reply: it goes
  to the developer with the torch, polars and pandas specs; NO library code until the
  coordinator replies again. Allowed meanwhile: this plan and the failing tests (steps 1-2).
- Open decisions (design note, last section): (1) refuse or read a column stored under
  another ignore list; (2) label `ndarray_v1` or `ndarray_v2`; (3) keep refusing unit-less
  datetimes and field titles; (4) metadata key order. The tests take (2) and (4) from two
  constants at the top of `tests/test_arrays/test_array_strict_hash.py`.
- November 12, later: (a) developer decision relayed: extension names are `<library>.<type>`,
  so the array type is `numpy.ndarray` (design note vii). The Arrow class registered under
  the name must never raise for metadata it does not know (v0.2 stored `numpy.ndarray` with
  empty metadata): it returns a "foreign" type keeping name and bytes; Orcapod refuses the
  column when it is used. Approved by the coordinator. No `also_reads` for v0.2.
  (b) Coordinator: take the 10-line alignment of `StarfixArrowHasher.hash_schema` with
  `hash_table` (`SemanticHashingVisitor.hashed_field`) inside ITL-981, with a test, AFTER
  sign-off; say in the PR body that no stored or golden value moves (design note viii).
  (c) Finding posted as a comment on Linear ITL-980.
  (d) FINAL naming (supersedes (a)): `orcapod.numpy.ndarray`. v0.2's `numpy.ndarray` is then
  not a registered name, so the "foreign type" is dropped: the registered Arrow class
  dispatches valid lists and raises `LogicalTypeIdentifierError` for anything else, as every
  built-in type does. The Linear comment on ITL-980 could not be posted from this session
  (permission denied); the coordinator posts it.
  (e) DEVELOPER SIGN-OFF received November 12, 2026 (through the coordinator): design approved
  as recommended. Label stays `ndarray_v1`; a column under another ignore list is refused on
  read; unit-less datetimes and field titles stay refused; metadata keys sorted; the leading
  type text of the value hash is the extension name; names are `orcapod.<library>.<type>`.
  (f) Mechanism pushed at `64c63bfe` for the torch, Polars and pandas workers; the public
  names of design note (vi) are stable from that commit.
  (g) Implemented and pushed: NumPy `.npy` storage and strict hash, tests, golden (22 values
  moved, 8 system-tag entries renamed, 4 added, array entries only), docs, changelog
  fragment, design-issue entry, CLAUDE.md and `.zed/rules`. Full suite on 3.12: 10092 passed,
  109 skipped, 23 xfailed; array, hashing, logical type and context tests also pass on 3.11
  and 3.13. Left: independent review findings, PR, CodeRabbit rounds.

## Steps

1. [x] Plan (this file).
2. [x] Failing tests through the public interface:
   `tests/test_arrays/test_array_strict_hash.py`. They fail on `main`'s code (the hash
   ignores memory order and byte order, storage is the struct).
3. [x] `src/orcapod/logical_types/ignore_list.py`: `ignore_metadata`,
   `parse_ignore_metadata`, `checked_ignore`, `IgnoreListLogicalType`, `IgnoreListHandler`;
   one Arrow extension class per (name, list), the registered class deserialising any valid
   list. Polars extension keeps `metadata_str`.
4. [x] Context loader (`contexts/registry.py`): `hash_ignore` of `logical_types.json` into
   the reference table as `hash_ignore:<extension name>` before anything is built.
   `logical_types.json`: `"hash_ignore": {"orcapod.numpy.ndarray": []}` and the two `_ref`s.
5. [x] `numpy_type.py`: storage `large_binary`, `.npy` format 3.0 write
   (`numpy.lib.format.write_array`, `allow_pickle=False`), read after checking the magic;
   refusals (masked, object, StringDType, longdouble, unit-less datetimes, titles,
   out-of-order or overlapping offsets, header over 10,000 bytes); `storage_sort_key` is the
   strict `D` of the decoded value. `array_format.py`: drop the storage struct and its
   metadata constant, keep `array_digest_v1`.
6. [x] `NumpyArrayHandler` as an `IgnoreListHandler` computing `D`;
   `SemanticHashingVisitor.visit_extension` passes the column's list
   (`handler.hash_ignoring`).
6b. [x] `StarfixArrowHasher.hash_schema`: apply `SemanticHashingVisitor.hashed_field` to each
   field before hashing (same rule as `hash_table`); test that a value-hashed type's schema
   hash does not depend on its storage type and that no golden value moves.
7. [x] Rewrite the ITL-888 tests that assert the struct row and layout independence
   (`tests/test_arrays/test_array_value_format.py`, `array_cases.py`
   (`assert_arrays_identical` requires C order today), `tests/test_logical_types/test_array_format.py`,
   `test_numpy_type.py`, `tests/test_hashing/test_numpy_handler.py`,
   `tests/test_hashing/test_hash_label_rule.py` if it names the constant).
8. [x] Golden: add `value.ndarray.{fortran,big_endian,aligned_struct,strided_view}` to
   `tests/test_hashing/golden/cases.py`; regenerate; check the diff moves only the 32 values
   listed in the design note (iii) and list each by cause in the PR.
9. [x] Docs: `docs/concepts/arrays.md`, `docs/reference/hashing.md` section 8,
   `docs/concepts/logical-type-identifiers.md`; "superseded" line in
   `superpowers/specs/2026-11-10-array-format-v0.3.md`; CLAUDE.md and `.zed/rules` if a rule
   changes (logical type identifiers: the ignore-list metadata shape).
10. [x] `changelog.d/ITL-981.md`, `design_issues/ITL-981-LV1.md`.
11. [x] Checks: `uv run ruff format .`, `uv run ruff check .`,
    `uv run --with basedpyright basedpyright` (exit code), `uv run pytest tests/`.
12. [ ] Merge `origin/main` if it moved, push, PR against `main` (`Fixes ITL-981`),
    `orca linear attach`, `orca worktree set --worktree current --pr <n>`, `worker_done`.

## Measurements behind the design

Scripts were run in the session scratchpad (not kept). NumPy 2.2.6, pyarrow 23.0.1. Results
are stated in the design note, section (i). Two facts that shape the code:

- `pa.ExtensionType` instances of one class compare equal whatever their serialised
  metadata, unless the class defines `__eq__`. Two different classes under one extension
  name compare unequal everywhere (`==`, schema equality, `concat_tables`, nested lists) and
  round-trip through IPC and Parquet when the registered class's
  `__arrow_ext_deserialize__` returns an instance of the class for the stored list.
- `read_array(allow_pickle=False)` refuses a header over 10,000 bytes (400 `f8` fields
  give 11,584).
