# ITL-871 torch.Tensor: native storage and strict hash Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the `torch.Tensor` value type of PR #300 on the design the developer signed
off on November 12, 2026 (storage `torch.save`, strict two-step hash with an ignore list), and
bring the PR to merge readiness.

**Architecture:** `logical_types/torch_type.py` owns three functions that do all the work:
`stored_form(value)` (the tensor that is written: CPU, no autograd history, cloned when it does
not cover its storage), `tensor_to_bytes` / `tensor_from_bytes` (`torch.save` / `torch.load`)
and `tensor_digest(value, ignore)` (the value hash `D`). The logical type and the value handler
are thin: after ITL-981 merges they subclass its `IgnoreListLogicalType` / `IgnoreListHandler`.
Until then they are wired with fixed metadata `{"ignore":[],"version":1}`.

**Tech Stack:** Python 3.11 to 3.13, uv, PyArrow, PyTorch (optional), pytest, ruff, basedpyright.

---

## Decisions

Developer sign-off (November 12, 2026):

1. A property storage cannot restore never counts in the hash and has no option name.
2. Storage: `torch.save` with its defaults written explicitly; read with
   `torch.load(buffer, map_location="cpu", weights_only=True)`, always explicit, never an
   unrestricted fallback. A tensor that does not cover its underlying storage exactly is saved
   as `value.clone(memory_format=torch.preserve_format)`, and `memory_layout` is hashed after
   that same clone.
3. Hash, strict by default. `E = array_format.array_digest_v1(tag, shape, bytes)` (unchanged).
   `D = SHA-256(lp(extension name) || E || u8(requires_grad) || u8(conj_bit) || u8(neg_bit)
   || u64le(ndim) || i64le(each stride) || lp(class name))`,
   `lp(x) = u64le(len(utf8(x))) || utf8(x)`. Options: `requires_grad`, `memory_layout`,
   `conj_bit`, `neg_bit`, `subclass`. An ignored property's slot holds its canonical value:
   0, C-contiguous strides of the shape, 0, 0, `torch.Tensor`.
4. Extension name `orcapod.torch.tensor`; it is the leading text in `D`. Label `torch_tensor_v1`.
5. Metadata `{"ignore":[...],"version":1}`; the list is configured per type in the data
   context; a stored column with another list is refused on read.
6. torch stays optional; `import orcapod` must not import torch.

Coordinator (ask 1, November 12, 2026):

- Merge conflicts: `errors.py` both sides; `data_function.py` drops the PR's helper for #299's
  `check_conformance`; `test_array_value_format.py` is `main`'s. Done in `5d6a3aa3`.
- Output rule gap (a fix to ITL-895's rule): #299 checks `isinstance` only for ndarray, so
  `-> nn.Parameter` returning a plain tensor is accepted. Fix generically in
  `semantic_types/output_conformance.py`: when the declared type is a class with a registered
  logical type and no more specific rule applies, require `isinstance(value, declared)`.
  Run the full suite; if any other type's outcome changes beyond "conversion failure becomes
  the standard `OutputTypeError`", stop and send the list.
- An `nn.Parameter` returned for a declared `torch.Tensor` is accepted, stored, read back and
  hashed as a Parameter. Correct `docs/concepts/function-pods.md` ("stored as the base"): the
  logical type decides; the tensor is the example.
- Minimum torch version: the developer decides (measured: `uint16/32/64` cannot be loaded with
  `weights_only=True` before torch 2.6). Do not change `pyproject.toml`; tests of those dtypes
  must hold under either answer.

Measured (torch 2.3.0, 2.4.1, 2.5.1, 2.6.0, 2.15.0):

- `TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD=1` does not override an explicit `weights_only=True`.
- `requires_grad`, `nn.Parameter`, strides, conj and neg bits round-trip.
- `clone()` resolves the conj and neg bits (2.15.0). So for a tensor that is cloned for
  storage those bits are hashed as 0, like its strides: every optional property is read from
  the stored form.

## File structure

- `src/orcapod/logical_types/torch_type.py`: `stored_form`, `tensor_to_bytes`,
  `tensor_from_bytes`, `tensor_digest`, `OPTIONS`, `canonical_tensor` (kept: source of `E`),
  `LogicalTorchTensor`.
- `src/orcapod/hashing/semantic_hashing/torch_handler.py`: `TorchTensorHandler`.
- `src/orcapod/utils/tensor_conversions.py`: what storing changes is now device and autograd
  history only.
- `src/orcapod/semantic_types/output_conformance.py`: the generic `isinstance` rule.
- `src/orcapod/logical_types/array_format.py`, `numpy_type.py`: back to `main` (the shared
  `array_storage_sort_key` is no longer used by the tensor type).
- Tests: `tests/test_torch/` (rewritten where they assert the struct row or the shared digest),
  new `tests/test_torch/test_tensor_strict_hash.py`, `tests/test_torch/test_tensor_storage.py`,
  `tests/test_torch/test_tensor_job_load_back.py`.

## Task 1: Output rule (ITL-895 gap)

- [ ] Failing tests (already in `tests/test_torch/test_tensor_value_format.py`):
      `test_declared_tensor_subclass_refuses_a_plain_tensor`,
      `test_declared_subclass_with_its_own_logical_type_requires_that_subclass`,
      `test_non_tensor_returned_for_declared_tensor_is_refused_at_the_pod` (expect
      `OutputTypeError` naming the function, output `'y'`, declared and returned types).
- [ ] In `_Checker.check`, after the `UserClassLogicalType` branch: a declared class with a
      registered logical type requires `isinstance(value, declared)`, else
      `Nonconformance("", declared, value)`.
- [ ] `uv run pytest tests/`; compare failures with the pre-change run; report any change for
      File, Directory, Path, pandas, polars, user classes.
- [ ] Commit `fix(output_conformance): ...`.

## Task 2: Storage

- [ ] Tests first (`tests/test_torch/test_tensor_storage.py`, through `DictSource(...).as_table()`
      and `iter_data()`): the column type is `orcapod.torch.tensor` over `large_binary`; each
      supported dtype reads back with equal dtype, shape, bits; `requires_grad`, `nn.Parameter`,
      transposed and channels-last strides, conj and neg bits read back; a 3-element slice of a
      1,000,000-element tensor stores under 10 KB and reads back equal and contiguous; the
      user's tensor is unchanged (same `requires_grad`, `grad_fn`, strides, device, version);
      another subclass is refused with `UnsupportedValueError`; a cell holding an arbitrary
      pickle raises on read and runs nothing (also with
      `TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD=1`); garbage bytes raise `ValueError`.
- [ ] `stored_form(value)`: `_check_storable`; class must be exactly `torch.Tensor` or
      `torch.nn.Parameter`; `t = value.detach()`; `.to("cpu")` if needed; clone with
      `preserve_format` unless `t.storage_offset() == 0`, the strides are dense and
      non-overlapping, and `t.untyped_storage().nbytes() == t.numel() * t.element_size()`;
      restore `requires_grad` on the new tensor object; wrap in `nn.Parameter` when the value
      is one.
- [ ] `tensor_to_bytes(value)`: `torch.save(stored_form(value), buffer, pickle_protocol=2,
      _use_new_zipfile_serialization=True)`.
- [ ] `tensor_from_bytes(data)`: `torch.load(io.BytesIO(data), map_location="cpu",
      weights_only=True)`; the result must be exactly a `torch.Tensor` or `nn.Parameter`
      (`ValueError` otherwise); torch's own errors are raised as `ValueError` from them.
- [ ] Commit.

## Task 3: Hash

- [ ] Tests first (`tests/test_torch/test_tensor_strict_hash.py`): pinned `D` vectors computed
      in the test from the formula with `hashlib` (independent of the implementation); each of
      the five properties changes the default hash; with its name passed to
      `tensor_digest(value, ignore)` the two hashes are equal; all five ignored equals the
      strict hash of the plain canonical tensor; dtype, shape and one bit always change the
      hash; device (MPS/CUDA when present), autograd history and `.grad` never do; a tensor
      never hashes like an equal ndarray, alone or inside `hash_object({"x": value})`;
      `hash(read(write(v))) == hash(v)` for every case in `tensor_cases.py`, including the
      step slice, the expanded view and a sliced conj view.
- [ ] `tensor_digest(value, ignore: frozenset[str]) -> bytes` reading every optional property
      from `stored_form(value)`; `OPTIONS = frozenset({"requires_grad", "memory_layout",
      "conj_bit", "neg_bit", "subclass"})`; class name through
      `canonical_type_names.canonical_type_name`.
- [ ] `TorchTensorHandler.handle` returns `ContentHash(TORCH_TENSOR_HASH_LABEL,
      tensor_digest(obj, frozenset()))` until Task 5.
- [ ] `storage_sort_key`: `None` first, then the strict `D` of the decoded value (stored bytes
      are not a stable order).
- [ ] Commit.

## Task 4: Conversion warning, existing tests, load-back test, docs

- [ ] `tensor_conversions`: reports a non-CPU device and autograd history (`grad_fn` is not
      `None`); `requires_grad` alone no longer warns. Update the messages in
      `core/data_function.py`, `tensor_conversions.warn_source_tensor_conversions` and
      `tests/test_torch/test_tensor_conversion_warning.py`.
- [ ] Rewrite the assertions of the struct row and the shared digest in
      `test_tensor_value_format.py`, `test_tensor_type_identity.py`,
      `test_tensor_round_trip.py`, `test_golden_hashes.py` (`TORCH_PINNED_ROWS`).
- [ ] `tests/test_torch/test_tensor_job_load_back.py`: save a `PipelineJob` with a tensor
      output on a Delta store, load it in a new job object, read the stored results (#303).
- [ ] Docs: `docs/concepts/arrays.md`, `docs/reference/hashing.md` section 8,
      `docs/concepts/logical-type-identifiers.md`, `docs/concepts/function-pods.md` (the
      "stored as the base" sentence), `changelog.d/ITL-871.md`, `design_issues/ITL-871-TT1.md`,
      `superpowers/specs/2026-11-11-torch-tensor-v0.3.md` (superseded note).
- [ ] Commit.

## Task 5: Wire to ITL-981 (after the coordinator sends its sha)

- [ ] `git fetch`, merge the sha locally.
- [ ] `LogicalTorchTensor(IgnoreListLogicalType)`: `EXTENSION_NAME`, `STORAGE_TYPE =
      pa.large_binary()`, `VERSION = 1`, `OPTIONS`, `python_type`, the two storage methods.
- [ ] `TorchTensorHandler(IgnoreListHandler)`: `OPTIONS`, `HASH_LABEL`, `digest(obj, ignore)`
      returning `tensor_digest(obj, ignore)`.
- [ ] `contexts/logical_types.json`: `"hash_ignore": {"orcapod.torch.tensor": []}` and
      `{"ignore": {"_ref": "hash_ignore:orcapod.torch.tensor"}}` in both deferred entries.
- [ ] Tests: a custom `logical_types.json` with `["memory_layout"]` gives equal cell hashes for
      a transposed tensor and its contiguous copy, and metadata
      `{"ignore":["memory_layout"],"version":1}`; a column stored under another list is refused
      on read (`LogicalTypeIdentifierError`); an unknown option name fails at context build;
      `torch` is absent from `sys.modules` after `import orcapod` and after building the default
      context.

## Task 6: Finish (only after ITL-981's PR is on `main`)

- [ ] Merge `origin/main`; regenerate `--group torch` only; `git diff --stat origin/main --
      tests/test_hashing/golden/data` shows `torch.json` only.
- [ ] `uv run --python 3.12 pytest tests/`, then 3.11, 3.13; ruff check, ruff format --check,
      basedpyright exit 0; `scripts/assemble_changelog.py --check`,
      `scripts/design_issues.py --check`.
- [ ] PR body; ONE push; CodeRabbit on the new head; every review thread answered, including
      `PRRT_kwDOOf7mTs62H5kR` and `PRRT_kwDOOf7mTs62MzFD`; `worker_done`.

## Resume notes

- `5d6a3aa3`: merge of `origin/main` at `25650c42`. Nothing is pushed; remote head `5f98283f`.
- Tasks 1 to 5 done. ITL-984 was fixed on `main` (#330): the xfail is removed. The extra is
  `torch>=2.6` (developer).
- Final merge of `origin/main` at `859106f7` (holds ITL-981, #328): every ITL-981 file is
  `main`'s; `contexts/logical_types.json` has one `hash_ignore` object with both types (git
  had merged it into two). Torch golden regenerated with `--group torch`: unchanged, 30
  entries; against `main` only `torch.json` is added.
- Verified on that merge: `pytest tests/` on 3.12, 3.11 and 3.13 each 10970 passed, 119
  skipped, 22 xfailed, 0 failed; ruff check, ruff format --check, basedpyright exit 0;
  changelog and design-issue checks pass.
- Later type PRs (Polars #286, pandas) touch `logical_types.json`, `docs/reference/hashing.md`
  and the golden files: expect one more mechanical merge of `main`.
