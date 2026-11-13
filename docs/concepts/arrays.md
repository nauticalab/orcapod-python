# NumPy Arrays

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
