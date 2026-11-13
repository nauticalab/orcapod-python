## `src/orcapod/logical_types/torch_type.py` — PyTorch tensors

### TT1 — `torch.Tensor` could not be a data value
**Status:** resolved
**Severity:** medium
**Issue:** ITL-871 (design: the module docstring of `logical_types/torch_type.py`; history: `superpowers/specs/2026-11-11-torch-tensor-v0.3.md`)

A pod annotated with `torch.Tensor` could not be defined, and the semantic hasher refused
tensors. Users converted at every pod boundary by hand.

**Fix:** `LogicalTorchTensor` (`orcapod.torch.tensor`, `large_binary`) stores a tensor with
PyTorch's own serialization (`torch.save`; read with
`torch.load(map_location="cpu", weights_only=True)`, never unrestricted), and
`TorchTensorHandler` hashes it under `torch_tensor_v1` with a strict, Orcapod-defined hash:
the array digest of the elements, then `requires_grad`, the lazy conjugate and negative bits,
the strides and the class. Those five can be ignored through the type's ignore list (the
shared mechanism of ITL-981, `logical_types/ignore_list.py`). Both classes register on first
use through deferred entries in `contexts/logical_types.json`, so `import orcapod` never
imports PyTorch, and support depends only on `torch` being importable.

Developer decisions (November 12, 2026): storage and hash are decided separately; a property
storage cannot restore (device, autograd history) never counts in the hash; a tensor that does
not cover its buffer is stored as `clone(memory_format=torch.preserve_format)` and its
optional properties are hashed after that clone; extension name `orcapod.torch.tensor`, which
is also the leading text of the hash; label `torch_tensor_v1`; minimum `torch>=2.6`
(`uint16`/`uint32`/`uint64` cannot be loaded with `weights_only=True` before it). Earlier
decisions that stand (November 11, 2026): the dtype list (the NumPy-shared ones plus
`bfloat16`, `complex32` and the float8 family as raw bits); `ValueConversionWarning`, issued
once per function output or source column and conversion; the convenience extra
`orcapod[torch]`. The dev dependency group pulls the extra, with CPU-only wheels on Linux, so
the required CI always runs the torch tests. A guard test fails if torch is missing under CI.

Known limits: within one in-memory run a downstream pod receives the very tensor the upstream
pod returned (device and autograd history included); only stored or read-back values are in
the stored form. NumPy arrays behave the same. A lazy negative view made by public PyTorch
code (`x.conj().imag`) never covers its buffer, so its `neg_bit` is always stored and hashed
as 0. A non-default ignore list works at the type converter and hasher level only: datagrams
find their data context by key (ITL-981).
