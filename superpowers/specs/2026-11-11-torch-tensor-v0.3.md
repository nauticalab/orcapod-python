# `torch.Tensor` as a value type in v0.3 (ITL-871)

> **SUPERSEDED (November 12, 2026).** The developer replaced the storage and the hash of this
> spec: a tensor is stored with `torch.save` (read with
> `torch.load(map_location="cpu", weights_only=True)`) under the extension name
> `orcapod.torch.tensor`, and hashed with a strict, Orcapod-defined hash with five optional
> properties. The current design is the module docstring of
> `src/orcapod/logical_types/torch_type.py`, `docs/concepts/arrays.md` and
> `docs/reference/hashing.md` section 8; the plan is
> `superpowers/plans/2026-11-12-itl-871-native-storage-strict-hash.md`. What still holds
> from this spec: the dtype list and the CI setup. Also changed: the extra is `torch>=2.6`
> (not 2.3), and `ValueConversionWarning` reports only a non-CPU device or autograd history
> (a `grad_fn`); `requires_grad` alone does not warn, because it is stored.

**Date:** 2026-11-11
**Issue:** ITL-871 (refs ITL-888 / PR #289, ITL-874, ITL-869, ITL-847 / PR #291)
**Basis:** the v0.3 array format (`superpowers/specs/2026-11-10-array-format-v0.3.md`,
`src/orcapod/logical_types/array_format.py`, `numpy_type.py`) and the ITL-874 storage review
(`superpowers/specs/2026-11-10-array-tensor-storage.md`, section B4, `b6_torch_mapping.py`).
**Status:** Decided (developer, November 11, 2026). The decisions below are final and override
the proposals in the body, which is kept as the design record.

**Developer decisions (November 11, 2026)**

- (a) Name `orcapod.torch_tensor`, label `torch_tensor_v1`: CONFIRMED.
- (b) Dtypes: the NumPy-shared ones plus `bfloat16`, `complex32` and the float8 family
  (`float8_e4m3fn`, `float8_e5m2`, `float8_e4m3fnuz`, `float8_e5m2fnuz`, `float8_e8m0fnu`),
  stored as raw bits under the torch dtype name (the table in section (b)). A dtype the
  installed torch lacks fails when a stored value of it is read, naming the dtype and the
  torch version, never at import. Quantized, sparse, nested and sub-byte types stay refused.
- (c) GPU and grad tensors accepted, stored from a detached CPU copy, never modified, with
  `orcapod.errors.ValueConversionWarning` (shared with PR #299): issued by the function pod
  output path (function, output) and by `DictSource` / `ListSource` (source, column, count),
  once per (function or source, output or column, conversion), explicitly deduplicated; a
  conversion is recorded only after `warnings.warn` returns, so under an error filter every
  row raises. Never raised for plain CPU tensors.
- (d) Packaging: the convenience extra `orcapod[torch] = ["torch>=2.3"]` (also in `all`).
  Support is driven by `torch` being importable, however installed; `import orcapod` never
  imports torch.
- (e) CI: the dev dependency group includes `orcapod[torch]`, so the org-required
  `uv run pytest tests/` (which syncs the dev group, not extras) installs torch. uv resolves
  torch from the PyTorch CPU index on Linux (`[tool.uv.sources]`, explicit index) and from
  PyPI elsewhere; `uv.lock` holds no CUDA package. `tests/test_torch/test_ci_has_torch.py`
  fails when `CI` is set and torch is missing. The no-torch path is tested in subprocesses
  with a `sys.meta_path` import blocker.

## Decided by the developer (CONFIRMED, not re-asked)

- A tensor is stored in the **same struct** as an array: `dtype` tag, `shape`, `data`
  (little-endian, C order, packed), `ARRAY_STORAGE_TYPE` with metadata `{"version":1}`.
- An ndarray and a tensor are **different data types**: their own logical type name and their
  own versioned hash label, `tensor_v1`, over the **same digest body** (`array_digest_v1`).
- Identity is dtype, shape and element values (hashed by their bits). Memory layout and byte
  order are not identity.
- "Extended" variants are deferred, and refused at write time with an error naming the column
  and the reason.
- User objects are never mutated.

## What today does (verified)

Nothing tensor-shaped can enter Orcapod yet: the semantic hasher raises (`strict`, no handler
for `torch.Tensor`), the type converter raises `Unsupported Python type`, and a function pod
annotated with `torch.Tensor` cannot be defined. So adding the type **moves no existing hash,
schema or table path**. Every hash below is new.

---

## (a) Name, storage version, hash label (decided: see the top)

| Item | Value |
|---|---|
| Logical type name = Arrow extension name | **`orcapod.tensor`** |
| Storage type | `ARRAY_STORAGE_TYPE` (shared; nullable children per F1 of the array spec) |
| Extension metadata (storage version) | `{"version":1}` (`ARRAY_FORMAT_METADATA`, shared) |
| Semantic hash | `ContentHash(method="tensor_v1", digest=array_digest_v1(tag, shape, data))` |
| Token in a table hash | `b"orcapod:tensor::tensor_v1:" + digest` (array: `orcapod:ndarray::ndarray_v1:`) |
| Python type | `torch.Tensor` (annotation); read back as a plain `torch.Tensor` |

`orcapod.tensor` follows `orcapod.ndarray`. Should JAX or TensorFlow arrays come later, they get
their own names (`orcapod.jax_array`, ...); the bare word "tensor" is taken by PyTorch because
`torch.Tensor` is the only type Python users call a tensor without a qualifier. Alternative:
`orcapod.torch_tensor` (more explicit, one-line change).

Consequences, all by construction of the confirmed decisions:

- An ndarray and a tensor with equal dtype, shape and values have **different hashes** (label
  and extension name differ) and the **same digest body** (tested).
- A pod declared `x: torch.Tensor` fed from an `np.ndarray` column (or the reverse) is a type
  mismatch at graph build, like any other pair of distinct types. No implicit conversion.

## (b) Dtype mapping (decided: see the top)

The tag vocabulary is the array format's (A2 of the array spec). Tags are the same strings for
the same element type, whichever library wrote them.

**Same tags as NumPy** (identical bytes and digest body to the equal ndarray):

| torch | tag |
|---|---|
| `bool` | `bool` (one byte, 0x00/0x01) |
| `int8 int16 int32 int64` | `int8 int16 int32 int64` |
| `uint8 uint16 uint32 uint64` | `uint8 uint16 uint32 uint64` (`uint16/32/64` exist from torch 2.3) |
| `float16 float32 float64` | `float16 float32 float64` |
| `complex64 complex128` | `complex64 complex128` |

**Torch-only dtypes: supported via raw bits with their own tag (my proposal).** Each is a
fixed-width, byte-aligned element. The bits are read through a same-width integer view
(`t.view(torch.int16)` for bfloat16, verified to round-trip exactly) and stored LE. The tag
spells the torch dtype name, which is the established cross-library name (ml_dtypes uses the
same ones), so a future NumPy reader can map them too.

| torch | tag | element bytes |
|---|---|---|
| `bfloat16` | `bfloat16` | 2, bfloat16 bits LE |
| `complex32` (`chalf`) | `complex32` | 4: real then imaginary, each IEEE binary16 LE |
| `float8_e4m3fn` | `float8_e4m3fn` | 1 |
| `float8_e5m2` | `float8_e5m2` | 1 |
| `float8_e4m3fnuz` | `float8_e4m3fnuz` | 1 |
| `float8_e5m2fnuz` | `float8_e5m2fnuz` | 1 |
| `float8_e8m0fnu` | `float8_e8m0fnu` | 1 (torch >= 2.7) |

bfloat16 is the one that matters for the workshop (model weights and activations). The rest
cost one table row each. If you prefer a smaller public commitment, ship **bfloat16 only** and
refuse the others (see g); adding a tag later is additive and changes no stored value or hash.

**Refused** (`UnsupportedValueError`, a `ValueError`, naming the column and the reason):

| Input | Why |
|---|---|
| quantized dtypes (`qint8`, `quint8`, `qint32`, `quint4x2`, `quint2x4`), `t.is_quantized` | scale and zero point are extended data |
| sparse layouts (`sparse_coo`, `sparse_csr/csc/bsr/bsc`), `mkldnn` | not a dense value; "convert with `.to_dense()`" |
| nested / jagged tensors (`t.is_nested`) | variable shape per element |
| sub-byte and packed dtypes (`int1`..`int7`, `uint1`..`uint7`, `float4_e2m1fn_x2`, `bits*`) | no defined element layout in core torch (their `itemsize` is a placeholder) |
| `bcomplex32` (new in torch 2.15) | no stable definition yet |
| `meta` device (and fake tensors without storage) | there is no data |
| named tensors (torch < 2.15 only; the API is gone in 2.15) | names would be dropped silently (refused) |

## (c) What is not identity and not stored; what reads back (decided: see the top)

| Property | Identity? | Stored? | On write |
|---|---|---|---|
| dtype, shape, element bits | yes | yes | |
| device (cpu, cuda, mps, ...) | no | no | copied to CPU with `.to("cpu")` (a new tensor) |
| `requires_grad`, `grad`, `grad_fn` | no | no | `.detach()` (a new view, no copy) |
| memory format, strides, storage offset, views (`t.T`, channels-last) | no | no | `.contiguous()` (copy only when needed) |
| conjugate and negative bits (`t.conj()` lazy views) | no | no | `.resolve_conj().resolve_neg()`; the value stored is the conjugated value. Needed: `.numpy()` raises on a conjugate-bit tensor (verified) |
| names (torch < 2.15) | no | no | refused (proposal); alternative: accept and drop |
| subclass (`nn.Parameter`, user subclasses) | no | no | treated as a plain tensor, like D7 for arrays |

Every step returns a new tensor or view; the user's tensor is never modified (in place
operations are never used). The bytes are read through `numpy()` of that (possibly copied)
tensor and copied into the stored row.

**GPU and grad-requiring tensors are accepted** (proposal): a pod that returns a CUDA or MPS
tensor, or one that still tracks gradients, is the common case in ML code, and the brief and
issue both state device and gradient state are not identity. The user sees no warning; the
docs say so. Alternative: refuse them, with an error telling the user to call
`.detach().cpu()`; that is stricter but makes nearly every training-loop pod fail on first try.

**Read back:** a CPU, C-contiguous, writeable `torch.Tensor`, `requires_grad=False`, with exactly
the stored dtype and shape (type strict: a bfloat16 tensor reads back bfloat16, never float32),
plain `torch.Tensor` class, not sharing memory with Arrow. A user who wants it on a GPU moves it
in the pod.

The **declared-output check** (D8 of the array spec) extends to tensors: a pod declared
`-> torch.Tensor` that returns an `np.ndarray`, a Python number or `t.item()` is refused at the
pod with a `TypeError` naming the output key, the declared type and the returned type. Never
wrapped implicitly.

## (d) Packaging and CI (decided: see the top)

**Optional, in the core package.** New module `orcapod/logical_types/torch_type.py` (imports
torch) and a `TensorHandler` in the hashing handlers. Not an extension package: the type shares
the struct, the digest and the operator support with `orcapod.ndarray`, and the SpikeInterface
pattern needs an explicit `op.register_extension(...)` call, which the brief rules out.

**Lazy registration, without importing torch** (proposal). A small "deferred type" table in the
data context names a type by its qualified class name and extension name:
`{"python_type": "torch.Tensor", "extension_name": "orcapod.tensor", "logical_type":
"orcapod.logical_types.torch_type.LogicalTorchTensor", "handler": "...TensorHandler"}`.
The three lookups that can meet a tensor consult it on a miss:

1. `LogicalTypeRegistry.get_by_python_type(t)`: a class in `t.__mro__` with module `torch` and
   qualname `Tensor` (comparing names, so this never imports torch; a tensor class can only
   exist if torch is already imported) registers the type into that registry, then retries.
2. `PythonTypeHandlerRegistry.get_handler(obj)`: the same check registers `TensorHandler`.
3. `LogicalTypeRegistry.get_by_arrow_extension_name("orcapod.tensor")` (reading a stored
   column, possibly in a fresh process): imports torch if it is installed and registers; if
   torch is **not** installed, raises an error naming the column type and the fix ("install
   PyTorch to read `orcapod.tensor` columns") instead of returning the raw struct.

`import orcapod`, building the default context, and hashing or running anything without tensors
never import torch (tested in a subprocess). Thread safety: registration under the registry's
lock. Context JSON: one new entry in `v0.1.json` plus a changelog line; nothing else in the
context moves (golden tests prove it).

**Install: choose one.** CI runs `uv sync --locked --all-extras --dev` in the required test job,
so **any** extra I add is installed in every required job (on Linux the default PyPI torch
wheel pulls the CUDA stack, about 2 GB). Options:

- **P1 (my pick): no extra; torch is a peer dependency.** `pip install orcapod torch` (or the
  wheel pytorch.org recommends for the user's CUDA version). PyTorch's own install matrix is
  the usual reason ML libraries do not pin torch. No `pyproject.toml` or `uv.lock` change, no
  required-workflow change. Docs state the tested version (PyTorch 2.15) and that the PyTorch build must support the installed NumPy (CodeRabbit, PR #300: some builds cannot use NumPy 2).
- **P2: extra `orcapod[torch] = ["torch>=2.3"]`**, plus a change to the required workflow:
  `uv sync --locked --all-extras --no-extra torch`. Needs your approval (required workflow), and
  adds torch for every platform to `uv.lock`.

**Tests in CI.** Torch tests live in `tests/test_torch/` and start with
`pytest.importorskip("torch")`, so the required `uv run pytest tests/` skips them (reported as
skips, by name). They run in **one new, non-required workflow** `run-torch-tests.yml` (Linux,
Python 3.12, CPU wheel):
`uv run --with torch --index https://download.pytorch.org/whl/cpu pytest tests/test_torch tests/test_hashing/golden`.
Adding a new workflow file touches no required workflow, but it is still CI configuration:
please confirm. Without it, the torch tests run only where someone runs that command (I will
run it locally and paste the result in the PR).

Two tests run in the required job without torch: `import orcapod` + a pipeline run leaves
`torch` out of `sys.modules` (meaningful in the torch job), and reading a hand-built
`orcapod.tensor` column with torch absent raises the install error (skipped when torch is
present).

## (e) Operators (decided: see the top)

Same struct, so the array work applies:

- `Batch` and `GroupBy` give `list[torch.Tensor]` columns through the list logical type, as
  for `list[np.ndarray]`.
- `MergeJoin` with a colliding tensor column orders by `storage_sort_key`, the stored
  `(dtype tag, shape, data)` tuple, as for arrays (F3).
- `SemiJoin` (PR #291, Polars semi join), `Join`, `PolarsFilter`, `Select`/`Drop`, `Map*`:
  pass the struct through.
- Sync and async paths, in-memory and Delta databases.

The test file mirrors `tests/test_arrays` for tensors.

## (f) Golden pins (decided: see the top)

Additions only, in a **new file** `tests/test_hashing/golden/data/tensor.json`, with cases in
`cases.py` that need torch (the same values as the `ndarray.*` cases, plus torch-only ones). Reason: the existing files must not lose or skip entries when the
generator or the test runs without torch. Entries: `value.tensor.{int32, int64, float64, empty, bool,
bfloat16, zero_dim, non_contiguous}`, `type.tensor`, `single_field.tensor.{arrow_type,
schema_hash}`, `tensor.{content_hash, pipeline_hash}`. The test module skips without torch and
runs in the torch workflow; the generator refuses to write `tensor.json` without torch. A
torch-free test pins the `tensor_v1` digests against an independent reference (`hashlib` +
`struct`) of the stored rows, and checks each shared case's digest body equals the pinned
`value.ndarray.*` body. No existing golden entry moves.

## (g) Cuts if time is short, in order

1. The float8 family, `complex32` (refuse them; keep `bfloat16`).
2. The new CI workflow (local torch run reported in the PR instead).
3. GPU tests beyond MPS (CUDA is environment-gated and unavailable here anyway).
4. Docs worked example (keep the identity table and the format note).

Not cuttable: storage, name, label, refusals, no torch import at `import orcapod`, lazy
registration on all three paths, the declared-output check, operator and database coverage,
fresh-process cached re-run, golden pins, CHANGELOG.

---

## Implementation outline (for the plan after approval)

1. `torch_type.py`: dtype <-> tag tables, `canonical_tensor(value) -> (tag, shape, bytes view)`,
   refusals, `LogicalTorchTensor` (name `orcapod.tensor`, `storage_sort_key`).
2. `TensorHandler` (`tensor_v1`).
3. Deferred-type table: context JSON entry, the three lookup hooks, install error.
4. Declared-output check generalised from ndarray to tensor.
5. Tests first (public interface): pod in/out, chained pods, operators, sync+async,
   in-memory+Delta, fresh-process cached re-run, cross-process stability, array vs tensor hash
   differ, digest body equal, every dtype, refusals, no-mutation, GPU (MPS here; CUDA gated),
   no torch import.
6. Docs (`docs/concepts/arrays.md` gains a Tensors section; hashing reference notes the label),
   CHANGELOG `[0.3.0]`, `DESIGN_ISSUES.md` entry.
