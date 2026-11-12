# NumPy Arrays

A `numpy.ndarray` can be a data value like any other: a source column, a function pod
parameter or a return value. Orcapod stores each array by its **value**, its dtype, its shape
and its elements. How the array happens to sit in memory is not stored. The hash is defined on
that value, so two arrays that hold the same values hash the same, however each was laid out.

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

!!! warning "v0.3 changed the array format"
    Orcapod v0.2 stored arrays as `.npy` files under the extension name `numpy.ndarray` and
    hashed the file. v0.3 replaces both, as described below, and does not migrate older
    stores. Every node that handles arrays, and every node downstream of an array column, gets
    new tables in v0.3. Because v0.3 also changed every hash of Arrow data, v0.3 reuses no
    result of a store written by v0.2: it ignores the v0.2 rows and computes every input
    again (see [A store written by an older version](function-pods.md#a-store-written-by-an-older-version)).
    Nodes that perform side effects are the exception: a side-effect pod, or a function pod
    that takes the invocation context, does not run again on such a store but raises
    `SideEffectLogDataContextError` before it delivers anything.
    The v0.2 tables and rows stay on disk unchanged, readable with v0.2. A migration is
    planned after v0.3 (ITL-889).

## What counts as the same array

Two arrays are the same value when they have the same dtype, the same shape and the same
element bits.

- **Memory layout does not count.** C order, Fortran order, strided views and transposed views
  of equal values are the same array. An array always reads back in C order.
- **Byte order does not count.** Arrays are stored little-endian; a big-endian `>f4` array
  reads back as `<f4` with equal values.
- **Structured field order counts**, as do field names and field types, at every nesting
  depth: `[("a", "i4"), ("b", "f8")]` and `[("b", "f8"), ("a", "i4")]` are different arrays,
  as they are to NumPy's own dtype equality. Offsets, padding and alignment do not count:
  fields are stored packed in declared order, so an aligned `[("b", "f8"), ("a", "i2")]`
  reads back as the packed `[("b", "<f8"), ("a", "<i2")]`.
- **Dtype counts**, including width: `int32` and `int64` arrays with equal values differ.
- **Shape counts**, including empty shapes: `(0, 3)` and `(3, 0)` differ, and so do a 0-d
  array and a one-element 1-d array.
- **Every bit counts**: `0.0` and `-0.0` differ, and so do NaNs with different payloads.
- **Subclasses are plain arrays by default.** `np.matrix`, `np.recarray`, `np.memmap` and
  other subclasses use the array format: they are stored as their values and read back as
  plain `np.ndarray`. The exception is a subclass with its own registered logical type, which
  is used for that subclass and its own subclasses instead.

```python
from orcapod.contexts import get_default_context

hasher = get_default_context().semantic_hasher
a = np.arange(6, dtype=np.float32).reshape(2, 3)

print(hasher.hash_object(a) == hasher.hash_object(np.asfortranarray(a)))
# True
print(hasher.hash_object(a) == hasher.hash_object(a.astype(">f4")))
# True
print(hasher.hash_object(a) == hasher.hash_object(a.astype(np.float64)))
# False
ab = np.zeros(1, dtype=[("a", "i4"), ("b", "f8")])
ba = np.zeros(1, dtype=[("b", "f8"), ("a", "i4")])
print(hasher.hash_object(ab) == hasher.hash_object(ba))
# False
```

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

An array column has the Arrow extension type `orcapod.ndarray`, with extension metadata
`{"version":1}`. Its storage is one struct per row:

| Field | Arrow type | Holds |
|---|---|---|
| `dtype` | `large_string` | the Orcapod dtype tag (below) |
| `shape` | `large_list<int64>` | the dimensions, outermost first; empty for a 0-d array |
| `data` | `large_binary` | the elements in C order, little-endian, packed |

The struct's children are nullable in the Arrow schema only because Polars drops nested
non-null flags. A stored array never has a null child. A null array is a null struct, allowed
when the column is annotated `np.ndarray | None`.

```python
table = source.as_table()
print(table.schema.field("signal").type.extension_name)
# orcapod.ndarray
print(table.column("signal").chunk(0).storage[0].as_py()["dtype"])
# float32
```

### Dtype tags

The tag names the element type without a byte order. For a structured dtype, the tag is a JSON
list with one `[name, tag]` entry per field, or `[name, tag, [dims]]` for a subarray field,
in declared field order. It is written with no whitespace and with non-ASCII characters escaped
(`json.dumps(..., separators=(",", ":"), ensure_ascii=True)`).

| NumPy | Tag | Element bytes |
|---|---|---|
| `bool` | `bool` | one byte, 0 or 1 |
| `int8` ... `int64`, `uint8` ... `uint64` | `int8` ... `uint64` | two's complement or unsigned, little-endian |
| `float16`, `float32`, `float64` | same names | IEEE 754, little-endian |
| `complex64`, `complex128` | same names | real part, then imaginary part |
| `datetime64[ns]`, `datetime64[10ms]`, ... | `datetime64[ns]`, `datetime64[10ms]`, ... | `int64`, little-endian (NaT is the minimum) |
| `timedelta64[s]`, ... | `timedelta64[s]`, ... | `int64`, little-endian |
| `U5` | `ucs4[5]` | five UCS-4 code units, each `uint32` little-endian, NUL-padded |
| `S3` | `bytes[3]` | three raw bytes, NUL-padded |
| `V4` | `void[4]` | four raw bytes |
| `[("b", "f8"), ("a", "i4", (2,))]` | `[["b","float64"],["a","int32",[2]]]` | fields packed in tag order |

## How an array is hashed

The hash of an array is the version-1 array digest of its stored row, labelled `ndarray_v1`.
The byte-level definition, and a reference implementation that does not use Orcapod, are in
the [Hashing Reference](../reference/hashing.md#8-array-value-digest). A future PyTorch tensor
type will share the digest under its own label, so an array and a tensor with equal values
have different hashes.
