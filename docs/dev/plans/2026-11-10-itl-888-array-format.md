# Value-based array format (ITL-888) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store and hash `numpy.ndarray` values in the v0.3 value-based format of
`superpowers/specs/2026-11-10-array-format-v0.3.md`.

**Architecture:** A NumPy-free module `logical_types/array_format.py` owns the storage struct and
the value digest. `logical_types/numpy_type.py` maps NumPy dtypes to tags and canonicalises
arrays. `NumpyArrayHandler` hashes through the shared digest. Public-interface tests pin
behaviour across operators, orchestrators, databases and processes.

**Tech Stack:** numpy 2.2.6, pyarrow 23.0.1, polars 1.41.2, deltalake 1.0.2, starfix, pytest.

**Gating:** Phase 1 was approved by the coordinator on November 10. Phase 2 implements the
developer's decisions on the identity rules, the name `orcapod.ndarray`, the label `ndarray_v1`,
D-PATH and the refused dtypes (November 10 and 11, 2026; listed in the spec's Decisions).

---

## File structure

| File | Responsibility |
|---|---|
| `src/orcapod/logical_types/array_format.py` (new) | Storage struct, format version, `array_digest_v1`. No NumPy import. Shared with torch (ITL-871). |
| `src/orcapod/logical_types/numpy_type.py` | dtype to tag and back, canonicalisation, refusals, `LogicalNumpyArray` (Phase 2) |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | `NumpyArrayHandler` on the new digest and label (Phase 2) |
| `src/orcapod/core/function_pod.py` (or the converter it calls) | declared `np.ndarray` return check (Phase 2) |
| `src/orcapod/contexts/data/v0.1.json` | changelog line (Phase 2) |
| `tests/test_arrays/` (new package) | public-interface array tests (Phase 1 scaffolding, Phase 2 format tests) |
| `tests/test_logical_types/test_array_format.py` (new) | digest against an independent reference |

---

## Phase 1 (approved)

### Task 1: storage struct and value digest

**Files:**
- Create: `src/orcapod/logical_types/array_format.py`
- Test: `tests/test_logical_types/test_array_format.py`

- [ ] **Step 1: Write the failing test.** The reference is written independently with `struct`
  and `hashlib` only, framing per spec C1:

```python
import hashlib
import struct

import pyarrow as pa
import pytest

from orcapod.logical_types.array_format import (
    ARRAY_STORAGE_TYPE,
    array_digest_v1,
)


def reference_digest(tag: str, shape: tuple[int, ...], data: bytes) -> bytes:
    t = tag.encode("utf-8")
    p = struct.pack("<Q", len(t)) + t
    p += struct.pack("<Q", len(shape)) + b"".join(struct.pack("<q", d) for d in shape)
    p += struct.pack("<Q", len(data)) + data
    return hashlib.sha256(p).digest()


CASES = [
    ("float32", (2, 3), bytes(range(24))),
    ("float64", (), b"\x00" * 8),          # 0-d
    ("int8", (0, 3), b""),                 # empty
    ("int8", (3, 0), b""),
    ('[["a","int32",[2]],["b","float64"]]', (1,), bytes(16)),
    ("ucs4[2]", (1,), "hé".encode("utf-32-le")),
]


@pytest.mark.parametrize(("tag", "shape", "data"), CASES)
def test_digest_matches_reference(tag, shape, data):
    assert array_digest_v1(tag, shape, data) == reference_digest(tag, shape, data)


def test_digest_accepts_memoryview():
    data = bytes(range(24))
    assert array_digest_v1("float32", (6,), memoryview(data)) == array_digest_v1(
        "float32", (6,), data
    )


def test_digest_separates_fields():
    assert array_digest_v1("int8", (0, 3), b"") != array_digest_v1("int8", (3, 0), b"")
    assert array_digest_v1("int8", (), b"\x01") != array_digest_v1("int8", (1,), b"\x01")
    assert array_digest_v1("int8", (2,), b"\x01\x02") != array_digest_v1(
        "uint8", (2,), b"\x01\x02"
    )


def test_storage_type():
    # Children are nullable at the Arrow level (spec A1, finding F1): Polars drops
    # nested non-null flags, so the logical type enforces non-null instead.
    assert ARRAY_STORAGE_TYPE == pa.struct(
        [
            pa.field("dtype", pa.large_string()),
            pa.field("shape", pa.large_list(pa.int64())),
            pa.field("data", pa.large_binary()),
        ]
    )
```

- [ ] **Step 2:** `uv run pytest tests/test_logical_types/test_array_format.py -v`. Expected: FAIL
  (`ModuleNotFoundError: orcapod.logical_types.array_format`).
- [ ] **Step 3: Implement** `array_format.py`: `ARRAY_STORAGE_TYPE` as above;
  `array_digest_v1(tag, shape, data)` feeding `hashlib.sha256` incrementally (tag length and tag,
  ndim and dims, data length, then the buffer itself so a large array is not copied); validate
  that every dim is a non-negative int. Google-style docstrings carrying the byte-level
  definition.
- [ ] **Step 4:** rerun, expected PASS. `uv run ruff format . && uv run ruff check .`,
  `uv run --with basedpyright basedpyright` exit 0.
- [ ] **Step 5: Commit** `feat(arrays): shared array storage struct and value digest v1 (ITL-888)`.

### Task 2: public-interface scaffolding true under every option

**Files:** `tests/test_arrays/__init__.py`, `tests/test_arrays/conftest.py`,
`tests/test_arrays/test_array_round_trip.py`, `test_array_operators.py`,
`test_array_pipeline_stores.py`, `test_array_cached_rerun.py`, `test_array_hash_stability.py`.

Every test uses only arrays that are little-endian, C-ordered, plain `np.ndarray` and not
structured with unsorted fields, so it holds whatever the identity rules are. They pass on
today's code and must keep passing after Phase 2 (a regression harness, not TDD red tests).

- `conftest.py`: `ARRAYS` dict, one per dtype class: bool, int8, uint64, float16, float32,
  float64, complex64, complex128, `datetime64[ns]`, `datetime64[D]`, `timedelta64[s]`, `U5`,
  `S3`, `V4`, structured with sorted fields `[("a","<i4"),("b","<f8")]`, 0-d, `(0,3)`, `(3,0)`,
  3-D. `assert_arrays_identical(a, b)`: same type `np.ndarray`, dtype, shape, `array_equal`
  (with `equal_nan` for floats), and C-contiguous.
- Round trip: `DictSource` then `iter_data()`; a function pod `identity(x: np.ndarray) ->
  np.ndarray`; `np.ndarray | None` with a `None` row.
- Operators (called `op(stream)`, read through `iter_data`/`as_table`): `Join`, `SemiJoin`,
  `MapData`, `SelectDataColumns`, `DropDataColumns`, `PolarsFilter` on a tag, `Batch`,
  `GroupBy`, `MergeJoin` with a colliding array column (values preserved as a set, column is
  `list[np.ndarray]`).
- Pipeline stores: `PipelineJob` with `double` and `total`, in-memory and Delta, `job.run()` and
  `AsyncPipelineOrchestrator().run(job.dag)`; every node's arrays exact.
- Cached re-run: Delta store, two fresh subprocesses; the second calls no pod (call counter
  file) and returns identical arrays and identical node content hashes.
- Hash stability: semantic hash of each array, datagram content hash, and a source's
  `content_hash()` in three subprocesses with `PYTHONHASHSEED` 0, 1 and random: identical.

Steps: write each file, run it (`uv run pytest tests/test_arrays -v`), expect PASS on today's
code; fix the test, not the library, if a test is wrong; report any library failure as a
finding. Commit `test(arrays): public-interface array scaffolding (ITL-888)`.

### Task 3: draft PR

Push the branch, open a draft PR against `main` (body: `Fixes ITL-888`, `Fixes ITL-883`,
`Refs ITL-874`, the spec link, the trailer line), then
`orca linear attach` and `orca worktree set --worktree current --pr <n>`.

---

## Phase 2 (after decisions; expanded with the decided values before starting)

Each task is test-first: write the failing tests, show the failure, implement, pass, commit.

4. **Tag mapping and canonicalisation** (`numpy_type.py`): `dtype_to_tag`, `tag_to_dtype`,
   `canonicalise(arr)`. Tests: every row of spec A2, structured with unsorted fields, nested and
   subarray fields, refusals (object, `StringDType`, masked, plus the decided ones) naming the
   dtype. Structured copies go field by field (NumPy assigns structured dtypes by position).
   Byte order handled by one function, so preserved vs canonicalised is a local change.
5. **`LogicalNumpyArray` on the struct**: new extension name and metadata, polars extension,
   `python_to_storage`/`storage_to_python` (writeable copy). Tests: stored Arrow type, round
   trip per dtype, extension name `numpy.ndarray` no longer registered.
6. **Column name in conversion errors**: refusal raised through `DictSource` and a function pod
   names the column and the dtype.
7. **`NumpyArrayHandler`**: `ContentHash(<label>, array_digest_v1(...))`. Tests: invariances and
   differences of spec C3 under the decided identity rules; token prefix in a table hash.
8. **Declared `np.ndarray` return check**: a pod returning a NumPy scalar raises the decided
   error naming the output key.
9. **Context JSON changelog line; golden**: merge main once #284 is in. New array pins may be
   added, checked against the Task 1 reference. Existing entries are regenerated only under the
   golden rules in CLAUDE.md: the developer has approved the hash change (November 10, 2026),
   with the migration assessment in spec section E. Only the listed array entries may change,
   and each one is listed in the PR description.
10. **v0.2 store fixture test** (spec E): a small v0.2-format Delta store committed under
    `tests/test_arrays/data/`, generated by a script kept beside it; run the v0.3 pipeline on a
    copy; assert the decided behaviour (new tables, old tables byte-identical, nothing
    unreadable).
11. **Docs and CHANGELOG**: `docs/concepts/arrays.md` (format, dtype tags, identity, refusals,
    known consequence) added to `mkdocs.yml` nav; `docs/reference/hashing.md` array digest
    section with the byte-level definition and test vectors; CHANGELOG entry. Doc samples
    must pass `tests/test_docs`.
12. **Verification**: full suite on 3.11, 3.12, 3.13; ruff; basedpyright; merge origin/main;
    DESIGN_ISSUES.md entry; PR out of draft.

---

## Resume (2026-11-11, after machine restart; dispatch ctx_c41c7c0e38ed)

State on arrival: worktree clean at 9802a9ab, equal to origin; 3 CodeRabbit rounds answered.
main is b197544e (v0.2.0 tagged at 70f53f64; #291 SemiJoin via Polars, ITL-847, merged).

1. Merge origin/main (no rebase). Remove the strict xfail on `test_semijoin`
   (`tests/test_arrays/test_array_operators.py`); it must pass. Drop the "known gap"
   wording in the PR body, CHANGELOG (`[0.3.0]` section) and spec F2.
2. Rerun: full suite on 3.11/3.12/3.13, golden tests (only the ndarray entries listed in the
   PR body may differ from main), ruff check, ruff format --check, basedpyright.
3. Push, wait for CI, request CodeRabbit once, handle a round (test/doc/wording on merit;
   layout/digest/names/identity/behaviour → ask coordinator).
4. If #286 merges meanwhile: merge main again (golden: keep both sets; docs/reference/hashing.md:
   the hash label convention paragraph appears once).

### CodeRabbit round 4 (on 25b240ad), coordinator decisions

- Fixed (ad7a3ded): ReST `::` markers; fixture mkdir; tar extraction fallback for 3.11.0-3.11.3.
- Declined: test docstrings (as in rounds 1-3).
- A (None from a non-optional output): not array-specific; deferred to ITL-895; reply on thread.
- B: MergeJoin with colliding grouped array columns (`list[ndarray]`) raises TypeError. Extend
  the array ordering key recursively to lists of arrays; test merging two grouped array
  columns, sync and async; confirm no golden or existing hash moves.
- C: structured dtype tag entry with other than 2 or 3 parts raises ValueError naming the tag;
  test.

### Field order is identity (developer, November 11, 2026; relayed by the coordinator)

Reverses identity rule D3: a structured dtype's field order is identity at every depth, as
NumPy's dtype equality has it. Offsets, padding, alignment, itemsize and byte order stay layout.
Change: `_tag_value` walks `dtype.names` (declared order) instead of `sorted(dtype.fields)`;
read-back follows the tag. Tests: declared-order storage and read-back (nested too), field
order and nested field order change the hash and the stored value, padded and offset-swapped
layouts keep it, MergeJoin over arrays differing only in field order (both argument orders),
unsorted and nested structured arrays added to `ARRAYS` (round trip on in-memory and Delta,
sync and async, every operator). Golden: no structured entries, nothing moves. Docs, spec D3,
CHANGELOG, DESIGN_ISSUES, PR body and ITL-888 wording updated.

## Merge of origin/main after PR #287 (2026-11-11, task_0d716f1fc6fc)

Plan (mechanical, coordinator-scoped; no behaviour change beyond keeping both sides):
1. `git merge origin/main` (cf4f67c7) — merge commit, no rebase/force-push.
2. Resolve conflicts additively: CHANGELOG one `[0.3.0] — unreleased` section with both
   sides' entries; DESIGN_ISSUES both entries; code keeps both behaviours (ITL-878 store-write
   failure handling + ITL-888 ndarray output check / UnsupportedValueError); re-read merged code.
3. Golden: `git diff origin/main -- tests/test_hashing/golden/` must equal this PR's approved diff.
4. Verify: pytest on 3.11/3.12/3.13, ruff check, ruff format --check, basedpyright exit code.
5. Push; wait for CI + CodeRabbit on new head; read review bodies + inline threads.

Done (2026-11-11): merged cf4f67c7 (CHANGELOG conflict) and a4e4a3e5 (DESIGN_ISSUES conflict),
both additive; golden diff vs main unchanged. Decisions restated as decided (coordinator,
developer confirmation November 11; ITL-889, ITL-911 deferred).

### CodeRabbit round on 75f8959d (contained bug fixes, TDD)

1. `_dtype_from_value`: a structured entry whose name is not a string or whose shape is not
   a list of non-negative ints raised `TypeError` (or NumPy's own `ValueError`); raise the
   documented "malformed array dtype tag entry" `ValueError`. Test: extend the parametrized
   read-path test in `tests/test_arrays/test_array_value_format.py`.
2. `canonical_array`: use `np.ndarray.view(value, np.ndarray)` so a subclass overriding
   `view` cannot change the stored or hashed values (D7: subclasses are plain arrays). Test:
   a subclass whose `view` returns other values stores and hashes like its base array.
3. Body pre-merge checks: ITL-889 is deferred (referenced, not targeted); changelog entries
   from #287/#296 come from merging main; docstring coverage declined as in earlier rounds.

### CodeRabbit round on 1cf5eed4 (coordinator decisions, 2026-11-11)

- Plan's storage-type test: nullable children (done, 6ce706cf).
- (A) Path-move scope: pipeline hashes are Merkle-chained, so the pipeline/operator tables of
  every node downstream of an array column move too; result tables of array-free pods keep
  their path (no recompute). Verified v0.2.0 vs branch: source(x, n) -> DropDataColumns(['x'])
  -> plus_one(n) pipeline hash 7c13dce1 -> 9f5326e3; array-free lineage b2d5c5d7 unchanged.
  Fix wording in spec, CHANGELOG, docs/concepts/arrays.md, DESIGN_ISSUES, PR description.
  New fixture test: add `mixed` source -> DropDataColumns(['x']) -> `plus_two(n)` (label
  `after_drop`) to `v02_store_pods.build_job`, regenerate the archive with v0.2.0
  (`git archive v0.2.0`), then assert under v0.3: after_drop calls its function 0 times, its
  pipeline table is new, its result table is the v0.2 one; `total`'s shared result table holds
  the v0.2 rows unchanged plus new rows under new record ids, and reads back correctly.
- (B) Polars ext metadata: declined, follow-up ITL-917.
- (C) Windows: map ':' to '!' in archive member names on win32 (unverified on Windows).
