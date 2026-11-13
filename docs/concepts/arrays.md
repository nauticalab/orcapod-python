# Arrays and Tensors

A `numpy.ndarray` can be a data value like any other: a source column, a function pod
parameter or a return value. Storage and hashing are two separate things:

- **Storage** is NumPy's own `.npy` format. An array reads back as it was given: dtype,
  byte order, shape, C or Fortran memory order and structured field offsets are kept.
- **The hash** is defined by Orcapod on the array's value, never on the stored bytes. It is
  strict: everything about the array that storage keeps is part of its identity.

```python
import numpy as np

from orcapod import function_pod
from orcapod.sources import DictSource

@function_pod(output_keys="scaled")
def scale(signal: np.ndarray) -> np.ndarray:
    return signal * 2

source = DictSource(
    data=[{"trial": "t1", "signal": np.arange(6, dtype=np.float32).reshape(2, 3)}],
    tag_columns=["trial"],
    source_id="trials",
)
for tag, data in scale.pod(source).iter_data():
    print(tag["trial"], data["scaled"].dtype, data["scaled"].shape)
# t1 float32 (2, 3)
```

!!! warning "v0.3 changed how arrays are stored and hashed"
    Orcapod v0.2 stored arrays as `.npy` files under the extension name `numpy.ndarray` and
    hashed the file. v0.3 stores them under `orcapod.numpy.ndarray`, hashes the value as
    described below, and does not migrate older stores. Every node that handles arrays, and
    every node downstream of an array column, gets new tables in v0.3. Because v0.3 also
    changed every hash of Arrow data, v0.3 reuses no result of a store written by v0.2: it
    ignores the v0.2 rows and computes every input again (see
    [A store written by an older version](function-pods.md#a-store-written-by-an-older-version)).
    Nodes that perform side effects are the exception: a side-effect pod, or a function pod
    that takes the invocation context, does not run again on such a store but raises
    `SideEffectLogDataContextError` before it delivers anything.
    The v0.2 tables and rows stay on disk unchanged, readable with v0.2. A migration is
    planned after v0.3 (ITL-889).

## What counts as the same array

Two arrays are the same value when everything below that counts is equal.

Always part of the hash:

- **Dtype**, including width: `int32` and `int64` arrays with equal values differ. So do
  `datetime64[s]` and `datetime64[ms]`, `U5` and `U6`. For a structured dtype the field
  names, the declared field order and the field types count, at every nesting depth:
  `[("a", "i4"), ("b", "f8")]` and `[("b", "f8"), ("a", "i4")]` are different arrays, as they
  are to NumPy's own dtype equality.
- **Shape**, including empty shapes: `(0, 3)` and `(3, 0)` differ, and so do a 0-d array and
  a one-element 1-d array.
- **Every element bit**: `0.0` and `-0.0` differ, and so do NaNs with different payloads.

Part of the hash by default; each has a name and can be put on the type's
[ignore list](#the-ignore-list):

| Name | What it is |
|---|---|
| `memory_order` | C or Fortran. An array counts as Fortran when it is Fortran-contiguous and not C-contiguous; every other array counts as C. |
| `byte_order` | Little- or big-endian, for each scalar field. A native array hashes as the byte order it actually has, never as "native": the same array bytes hash the same on every machine, and an array created natively on a big-endian machine is a big-endian array. |
| `struct_layout` | The field offsets and the item size of a structured dtype, at every nesting depth. An aligned (padded) dtype and the packed dtype with the same fields differ. |

Never part of the hash, because `.npy` does not keep it. A value read back from a store must
hash like the value that was written, or every cached run would compute again:

- **Strides other than C or Fortran order.** A step slice (`a[:, ::2]`), a reversed view or
  a broadcast view is stored C-contiguous, and hashes like its C-contiguous copy.
- **The subclass.** `np.matrix`, `np.recarray`, `np.memmap` and other subclasses are stored
  as their values and read back as plain `np.ndarray`. The exception is a subclass with its
  own registered logical type, which is used for that subclass and its own subclasses.
- **The `writeable` flag.** An array always reads back writeable.
- **Dtype `metadata`**, and the `align=True` flag of a structured dtype (the offsets that
  flag produced are kept and count, as `struct_layout`).
- **Padding bytes between structured fields.** NumPy does not define their content (an
  `np.empty` array holds whatever the memory held). They are not part of the value: they are
  written to the store as zero, so residual memory never reaches a store that may be shared,
  and they read back as zero. Every field reads back as written.

```python
from orcapod.contexts import get_default_context

hasher = get_default_context().semantic_hasher
a = np.arange(6, dtype=np.float32).reshape(2, 3)

print(hasher.hash_object(a) == hasher.hash_object(a.copy()))
# True
print(hasher.hash_object(a) == hasher.hash_object(np.asfortranarray(a)))
# False
print(hasher.hash_object(a) == hasher.hash_object(a.astype(">f4")))
# False
print(hasher.hash_object(a) == hasher.hash_object(a.astype(np.float64)))
# False
wide = np.arange(12, dtype=np.float32).reshape(2, 6)
view = wide[:, ::2]
print(hasher.hash_object(view) == hasher.hash_object(np.ascontiguousarray(view)))
# True
packed = np.zeros(1, dtype=[("a", "i1"), ("b", "f8")])
aligned = np.zeros(1, dtype=np.dtype([("a", "i1"), ("b", "f8")], align=True))
print(hasher.hash_object(packed) == hasher.hash_object(aligned))
# False
```

## The ignore list

What the hash ignores is a setting of the array type, not of a single call. The data context
holds one list of ignored property names for the type; the default list is empty. The list
is written into the type's Arrow extension metadata, so it is part of the
[logical type identifier](logical-type-identifiers.md): the array type with an empty list
and the array type that ignores `memory_order` are two different logical types, with
different schema hashes.

- A bare array (a function default, a value inside a Python structure) is hashed with the
  data context's list.
- An array in a table is hashed with the list of its own column.
- A data context reads only array columns stored under its own list. A column stored under
  another list raises `LogicalTypeIdentifierError`, naming the column and both lists.

When a property is ignored, arrays that differ only in it hash equal, and they hash like
the array in canonical form (C order, little-endian, packed fields) does under the empty
list. The list is configured in the `hash_ignore` object of Orcapod's logical type
registration file (`contexts/logical_types.json`); v0.3 has no other way to change it. A
name that is not one of the three above is an error when the data context is built.

!!! note "Known limitation in v0.3"
    A list other than the default cannot be used end to end in a pipeline yet. A data
    context built from your own registration file hashes bare values and Arrow tables with
    its list, but streams and datagrams find their data context by its key, which is the
    key of the default context, so their rows are read by the default context, and it
    refuses a column of another list (ITL-965). In v0.3 the default list, empty, is the
    one that works everywhere.

The same mechanism serves the other library value types, each with its own names.

## What cannot be stored

Some arrays are refused when they are written, with a `ValueError` (an
`orcapod.errors.UnsupportedValueError`) that names the column and the dtype:

| Array | Why | What to do |
|---|---|---|
| Masked arrays (`np.ma.MaskedArray`) | extended data: the mask would be lost | store the data and the mask as two arrays |
| `dtype=object`, or a structured dtype with an object field | extended data: would need pickling | convert to a fixed dtype |
| NumPy 2 `StringDType` | extended data: variable width | use a fixed-width string dtype such as `U10` |
| `longdouble`, `clongdouble` | width and format differ by platform | `.astype(np.float64)` or `.astype(np.complex128)` |
| `datetime64` or `timedelta64` without a unit | the integer has no defined meaning | give a unit, e.g. `datetime64[ns]` |
| Structured dtypes with field titles | titles are aliases of fields | drop the titles |
| Structured dtypes whose fields overlap or are not in offset order | `.npy` has no form for them | declare the fields in offset order |
| Structured dtypes with several hundred fields (a `.npy` header over 10,000 bytes) | NumPy does not read such a header without pickle support | split the array |

Extended data may be supported later, stored as several parts rather than as one array.
Whether `longdouble` and `clongdouble`, unit-less datetimes and timedeltas, and structured
dtypes with titles should be accepted in a later release is tracked in ITL-911; v0.3 refuses
them.

```python
try:
    DictSource(
        data=[{"trial": "t1", "signal": np.ma.array([1.0, 2.0], mask=[False, True])}],
        tag_columns=["trial"],
    )
except ValueError as exc:
    print(type(exc).__name__)
# UnsupportedValueError
```

A function pod that declares `-> np.ndarray` must return an array. A NumPy scalar, for
example `x * 2` on a 0-d array, raises `TypeError` that names the output. Return
`np.asarray(...)` instead.

## How an array is stored

An array column has the Arrow extension type `orcapod.numpy.ndarray` over `large_binary`.
Each cell is one `.npy` file, written by NumPy itself
(`numpy.lib.format.write_array`, format version 3.0, `allow_pickle=False`) and read with
`numpy.lib.format.read_array(..., allow_pickle=False)`. Format 3.0 is the one that stores
field names outside Latin-1. Pickle is never written and never read: a cell that holds an
object array, is not a complete `.npy` file of format 3.0, or does not hold exactly the
element bytes its header declares, raises a `ValueError` that names the column.

The extension metadata is `{"ignore":[],"version":1}`: the ignore list and the version of
the type. A null array is a null cell, allowed when the column is annotated
`np.ndarray | None`.

```python
import io

table = source.as_table()
print(table.schema.field("signal").type.extension_name)
# orcapod.numpy.ndarray
cell = table.column("signal").chunk(0).storage[0].as_py()
print(np.load(io.BytesIO(cell), allow_pickle=False).dtype)
# float32
```

What reads back:

| Property | Reads back |
|---|---|
| dtype, including byte order, units, string widths, field names, offsets and item size | as given |
| shape and element bits | as given |
| C or Fortran memory order | as given |
| any other strides (step slices, reversed or broadcast views) | C-contiguous |
| subclass | plain `np.ndarray` |
| `writeable` flag | writeable |
| dtype `metadata`, the `align` flag | not kept |
| padding bytes between structured fields | zero |

```python
big = np.asfortranarray(np.arange(6, dtype=">f4").reshape(2, 3))
kept = DictSource(
    data=[{"trial": "t1", "signal": big}], tag_columns=["trial"], source_id="kept"
)
for tag, data in kept.iter_data():
    print(data["signal"].dtype.str, data["signal"].flags.f_contiguous)
# >f4 True
```

A stored array costs at least 128 bytes of `.npy` header. The stored bytes are NumPy's and
may differ between NumPy versions; nothing in Orcapod depends on them. Where an operator has
to order arrays (`MergeJoin` sorts the values it merges), it orders them by their value hash.

## How an array is hashed

The hash of an array is labelled `ndarray_v1` and is computed in two steps. The first is the
*element digest* of the array's dtype, shape and elements, taken on the array in canonical
form (C order, little-endian, fields packed). The second adds the type's name and the three
properties of the table above. The byte-level definition, the dtype tags and a reference
implementation that does not use Orcapod are in the
[Hashing Reference](../reference/hashing.md#8-array-value-hash). A PyTorch tensor type shares
the element digest and has its own name and properties, so an array and a tensor with equal
values have different hashes.

## PyTorch tensors

A `torch.Tensor` can be a data value too. PyTorch is an optional dependency. Tensor support
works whenever PyTorch can be imported, however it was installed: `pip install "orcapod[torch]"`
installs it from PyPI, or install the build that suits your hardware yourself (see
pytorch.org; on Linux the PyPI build includes CUDA, and pytorch.org also offers smaller
CPU-only builds). Orcapod needs PyTorch 2.6 or later, the first version whose restricted
loader (below) reads `uint16`, `uint32` and `uint64` tensors, and is tested with PyTorch 2.15. Orcapod
hashes a tensor's elements through NumPy (`Tensor.numpy()`), so your PyTorch build must
support the NumPy version you have installed: some older builds cannot use NumPy 2 and fail
with "Numpy is not available". If you see that error, upgrade PyTorch or install a NumPy
version your PyTorch build supports.

Importing Orcapod never imports PyTorch. The tensor type is set up the first time Orcapod
meets a tensor or a `torch.Tensor` annotation, or reads a stored tensor column. Reading a
stored tensor column without PyTorch installed raises
`orcapod.errors.MissingOptionalDependencyError`, which names the missing package.

```python
import torch

from orcapod import function_pod
from orcapod.sources import DictSource

@function_pod(output_keys="scaled")
def scale_tensor(signal: torch.Tensor) -> torch.Tensor:
    return signal * 2

tensors = DictSource(
    data=[{"trial": "t1", "signal": torch.arange(6, dtype=torch.float32).reshape(2, 3)}],
    tag_columns=["trial"],
    source_id="tensor_trials",
)
for tag, data in scale_tensor.pod(tensors).iter_data():
    print(tag["trial"], data["scaled"].dtype, tuple(data["scaled"].shape))
# t1 torch.float32 (2, 3)
```

**A tensor is a different type from an array.** The column's extension name is
`orcapod.torch.tensor` and the hash label is `torch_tensor_v1`. A tensor and an ndarray with
equal dtype, shape and values never hash the same: not as values, not inside a `dict` or
`list`, not as columns, schemas or types. A pod that declares `x: torch.Tensor` cannot take an
`np.ndarray` column (or the reverse). Convert at the pod boundary when you need to:
`torch.from_numpy(a)` or `t.numpy()`.

### How a tensor is stored

Storage and hash are decided separately. A tensor is stored with PyTorch's own serialization:
one cell holds the bytes `torch.save` writes for that tensor, in a `large_binary` column. It
is read with `torch.load(..., map_location="cpu", weights_only=True)`. That is PyTorch's
restricted loader: it rebuilds tensors and runs no other pickled code. Orcapod always passes
both arguments, never falls back to an unrestricted load, and never adds a class to the
loader's allow list; the environment variable `TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD` does not
change that. A cell that is not a tensor archive raises an error naming the column.

What a stored tensor keeps:

| Property | Stored and read back? |
|---|---|
| dtype, shape, element bits | yes |
| `requires_grad` | yes |
| Memory layout: strides (transposed views, `channels_last`) | yes, for a tensor that covers its buffer (below) |
| Lazy conjugate and negative bits (`t.conj()`) | yes, for a tensor that covers its buffer |
| Class: `torch.Tensor` or `torch.nn.Parameter` | yes |
| Device (CUDA, MPS) | no: a stored tensor is read on the CPU |
| Autograd history: `grad_fn`, `.grad`, hooks | no: a stored tensor is read as a leaf |

**Only the elements you pass are stored.** `torch.save` writes the whole buffer under a
tensor, so a 3-element slice of a 1,000,000-element tensor would store all million elements.
When a tensor does not cover its underlying buffer exactly (a slice, a step slice such as
`t[::2]`, an expanded view), Orcapod stores `value.clone(memory_format=torch.preserve_format)`
instead. The clone holds just the elements of the view. It has PyTorch's strides for that
clone, and a lazy conjugate or negative bit is resolved into the elements: `x.conj().imag`,
which is a negative view of every second number of `x`, is stored as plain negated values.
A tensor that covers its buffer (`x.conj()`, `x.transpose(0, 1)`) is stored as it is.

Orcapod never modifies your tensor. One stored tensor costs about 1.5 KB more than its
elements, so a column of many very small tensors is larger than the same data in one tensor.

### What counts as the same tensor

The hash is defined by Orcapod and never reads the stored bytes. It is strict: by default two
tensors hash the same only when all of these are equal.

| Property | In the hash | Can be ignored (option name) |
|---|---|---|
| dtype, shape, element bits (`-0.0` and NaN payloads count) | always | no |
| `requires_grad` | by default | `requires_grad` |
| Memory layout (strides) | by default | `memory_layout` |
| Lazy conjugate bit | by default | `conj_bit` |
| Lazy negative bit | by default | `neg_bit` |
| Class (`torch.Tensor` or `torch.nn.Parameter`) | by default | `subclass` |
| Device, autograd history, `.grad`, position inside a larger buffer | never | |

A property that storage does not keep is never in the hash: otherwise a value read back from
a store would hash differently from the value that was written, and every cached result would
be computed again. For the same reason the optional properties are those of the *stored*
tensor: a step slice hashes like its contiguous clone.

The properties the hash ignores are the type's **ignore list**. It is empty by default. It is
configured per data context (`hash_ignore` in `orcapod/contexts/logical_types.json`) and is
written in the column's extension metadata, `{"ignore":[],"version":1}`, so it is part of the
type's [identifier](logical-type-identifiers.md): columns with different lists are different
types, and a data context refuses to read a tensor column stored under another list. An
ignored property is still stored and read back; it only stops counting in the hash. Orcapod
v0.3 has no user API for the list.

```python
from orcapod.contexts import get_default_context

hasher = get_default_context().semantic_hasher
t = torch.arange(6, dtype=torch.float32).reshape(2, 3)
print(hasher.hash_object(t) == hasher.hash_object(t.clone()))
# True
print(hasher.hash_object(t.T) == hasher.hash_object(t.T.contiguous()))
# False
print(hasher.hash_object(t) == hasher.hash_object(t.clone().requires_grad_(True)))
# False
print(hasher.hash_object(t) == hasher.hash_object(torch.nn.Parameter(t, requires_grad=False)))
# False
print(hasher.hash_object(t).digest == hasher.hash_object(t.numpy()).digest)
# False
print(hasher.hash_object(t).to_string())
# torch_tensor_v1:...
```

The byte-level definition is in the
[Hashing Reference](../reference/hashing.md#torch-tensor-value-hash).

### Device and autograd history

A pod may return a GPU tensor, or one computed from tensors that track gradients (it has a
`grad_fn`), and a source may hold one. Storage keeps neither the device nor the history, so
Orcapod stores a CPU leaf tensor and issues `orcapod.errors.ValueConversionWarning`, which
names the function and output (or the source and column), says what was converted, and how to
avoid it: return or pass `value.detach().cpu()`. A function warns once per output and
conversion, however many rows it processes; a source warns once per column, with the number
of values converted. A CPU tensor without a `grad_fn` never warns, also when it has
`requires_grad=True` or is an `nn.Parameter`: those are stored. To treat the conversion as an
error, call `warnings.simplefilter("error", ValueConversionWarning)` in your program (import
the class from `orcapod.errors`); every conversion then raises, and in a pipeline that row
fails. Python's `-W` command-line option and `PYTHONWARNINGS` cannot name this class: they are
read before installed packages can be imported, and Python ignores the filter.

```python
import warnings

from orcapod.errors import ValueConversionWarning

@function_pod(output_keys="weights")
def fit(signal: torch.Tensor) -> torch.Tensor:
    return signal.clone().requires_grad_(True) * 0.5

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    list(fit.pod(tensors).iter_data())
print(caught[0].category is ValueConversionWarning)
# True
print(caught[0].message)
# Function 'fit' output 'weights' returned a torch tensor with autograd history (a grad_fn); Orcapod stored a CPU tensor without autograd history, and the returned tensor is unchanged. Return value.detach().cpu() to avoid this warning.
```

**What comes back.** A tensor read from a store or a source is a CPU leaf tensor with the
stored dtype, shape, elements, strides, `requires_grad`, lazy bits and class (a `bfloat16`
tensor reads back as `bfloat16`, an `nn.Parameter` as an `nn.Parameter`). Within a single
in-memory run, though, the next pod receives the very object the previous pod returned, on its
device and with its autograd history. The same holds for NumPy arrays (a Fortran-ordered array
passes through in Fortran order). Write pods that do not depend on the device or the autograd
history of their inputs.

### Dtypes and refusals

`bool`, `int8` to `int64`, `uint8` to `uint64`, `float16`, `float32`, `float64`, `complex64`
and `complex128` are hashed with the NumPy tags and bytes from the table above. The dtypes
NumPy does not have are hashed as their raw bits, each element little-endian, under the
PyTorch dtype name as the tag:

| Dtype | Element bytes |
|---|---|
| `bfloat16` | 2: the bfloat16 bit pattern |
| `complex32` | 4: the real part, then the imaginary part, each a `float16` |
| `float8_e4m3fn`, `float8_e5m2`, `float8_e4m3fnuz`, `float8_e5m2fnuz`, `float8_e8m0fnu` | 1: the float8 bit pattern |

A stored dtype that your PyTorch does not have (`float8_e8m0fnu` needs PyTorch 2.7) raises an
error naming the column when it is read; everything else still works. These are refused when
they are written, with an `UnsupportedValueError` that names the column and the reason:

| Tensor | What to do |
|---|---|
| sparse or `mkldnn` layout | `.to_dense()` |
| quantized | `.dequantize()` |
| nested (jagged) | store the parts as separate tensors |
| on the `meta` device | it holds no data |
| named dimensions (PyTorch versions that have them) | `.rename(None)` |
| sub-byte and packed dtypes (`int1` to `int7`, `uint1` to `uint7`, `float4_e2m1fn_x2`, `bits*`), `bcomplex32` | convert to a supported dtype |
| a subclass other than `torch.nn.Parameter` | `.as_subclass(torch.Tensor)`, or register a logical type **and** a value handler for the class (PyTorch's restricted loader cannot read it back, and the tensor hash, which includes the class, is defined for `torch.Tensor` and `nn.Parameter` only) |

A function pod that declares `-> torch.Tensor` must return a tensor. An `np.ndarray` or a
Python number raises `OutputTypeError` that names the function and the output. Return
`torch.as_tensor(...)` instead. An `nn.Parameter` is a tensor, so it is accepted; it is
stored, read back and hashed as an `nn.Parameter`. A pod that declares `-> torch.nn.Parameter`
must return one.
