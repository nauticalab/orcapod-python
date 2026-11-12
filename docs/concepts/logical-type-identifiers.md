# Logical Type Identifiers

A *logical type* binds a Python class to the way its values are stored in an Arrow column:
`pathlib.Path` is stored as a string, `numpy.ndarray` as a struct, a dataclass as a struct
of its fields. Orcapod ships logical types for paths, files, directories, UUIDs, NumPy
arrays and pandas objects, and you can register your own.

Every logical type has an **identifier**: its Arrow extension name together with its
Arrow extension metadata.

```python
import pathlib

from orcapod.contexts import get_default_context
from orcapod.logical_types import logical_type_identifier

converter = get_default_context().type_converter
path_type = converter.get_logical_type(pathlib.Path)
identifier = logical_type_identifier(path_type)
print(identifier.name)
# orcapod.path
print(identifier.metadata)
# b'{"version":1}'
```

The identifier answers one question: *would a value be stored and hashed the same way?*
Two columns with the same identifier hold values in the same layout, hashed by the same
rule. Orcapod treats the identifier as opaque. It never interprets the metadata; it only
stores it, hashes it and compares it byte for byte.

## Where the identifier goes

- **Stored with the column.** The name and metadata are written as the column's
  `ARROW:extension:name` and `ARROW:extension:metadata`, in memory and in every store.
- **Part of the data type, so part of every hash the type takes part in.** The whole
  identifier enters the type hash, the schema hash, the identity of a function whose
  signature names the type, the pipeline hash, and the hash of every row that holds a
  value of the type. In those hashes it is written as the extension name, a `;`, and the
  metadata bytes in hexadecimal:

```python
print(identifier.text)
# orcapod.path;7b2276657273696f6e223a317d
```

- **A change of identifier is a schema change.** Stored tables are placed under their
  schema and function hashes, so when a type's identifier changes, every table with a
  column of that type gets a new location. Results stored under the old identifier are not
  found, are computed again, and stay where they were. Tables without a column of that
  type are not affected.

## When to change an identifier

Change the identifier **when, and only when, an existing value would be stored or hashed
differently**.

| Change to the type | Identifier |
|---|---|
| The stored layout of existing values changes (other storage type, other encoding) | must change |
| The hash of an existing value changes | must change |
| The type learns to store values it used to reject; every old value is stored and hashed as before | keep |
| Bug fix, speed-up or refactoring with identical stored bytes and hashes | keep |
| New release of the package that implements the type, nothing else | keep |

The identifier is therefore not a package version. A type can go through many releases
under one identifier.

## Built-in types

Orcapod's own types follow the same contract as yours. Their metadata is the JSON text
`{"version":N}`:

| Python type | Extension name | Extension metadata |
|---|---|---|
| `pathlib.Path` | `orcapod.path` | `{"version":1}` |
| `upath.UPath` | `orcapod.upath` | `{"version":1}` |
| `uuid.UUID` | `orcapod.uuid` | `{"version":1}` |
| `orcapod.File` | `orcapod.file` | `{"version":1}` |
| `orcapod.Directory` | `orcapod.directory` | `{"version":1}` |
| `numpy.ndarray` | `orcapod.ndarray` | `{"version":1}` |
| `pandas.DataFrame` | `pandas.dataframe` | `{"version":1}` |
| `pandas.Series` | `pandas.series` | `{"version":1}` |

`{"version":N}` is the convention of the built-in types only. Your types may use any bytes.

### Native containers: `list[T]` and `set[T]`

A column typed `list[T]` or `set[T]`, where `T` is a logical type, is stored under a wrapper
extension type named `list[<name of T>]` or `set[<name of T>]`. These two are the *native
containers*. They are logical types only because Arrow cannot hold an extension type inside
a list; conceptually they are an ordinary list or set of `T`. By design:

- **A native container has no version of its own.** Its metadata is JSON with `category`
  (`list` or `set`) and the element's extension name and metadata, and nothing else. Its
  identifier is the container kind plus the element's identifier, so it changes exactly when
  the element's identifier changes. Because the element's metadata is written into that
  JSON as text, a logical type used inside `list[T]` or `set[T]` must have extension
  metadata that is valid UTF-8, for its current identifier and for every older one it
  declares readable.
- **In the type and schema hash it is hashed like any other generic type**, `list[int]`
  included: as its origin (`list` or `set`) and its argument. The element's identifier enters
  through the argument.
- **A change to how native containers are stored or hashed is a change of Orcapod's core
  hashing**, recorded by the data context (the global hash version), never a change of a
  logical type.

A container type you implement yourself is not a native container. It is an ordinary logical
type with its own identifier, which you declare and change like that of any other type.

## Declaring the identifier of your own type

The Arrow extension type your logical type returns from `get_arrow_extension_type()` *is*
the declaration: its extension name and its serialized metadata. There is no second place
to keep in step. `make_arrow_extension_type` builds such an extension type:

```python
from typing import Any

import polars as pl
import pyarrow as pa

from orcapod.logical_types import (
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.logical_types.base_logical_type import BaseLogicalType


class Celsius:
    def __init__(self, degrees: float) -> None:
        self.degrees = degrees


class LogicalCelsius(BaseLogicalType):
    logical_type_name = "example.celsius"
    python_type = Celsius

    _arrow = make_arrow_extension_type(
        "example.celsius",
        pa.float64(),
        b"layout-2",              # the identifier's metadata: any bytes you choose
        also_reads=(b"layout-1",),  # older identifiers this version still decodes
    )()
    _polars = make_polars_extension_type("example.celsius", pa.float64(), "layout-2")()

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        return self._arrow

    def get_polars_extension_type(self) -> pl.BaseExtension:
        return self._polars

    def python_to_storage(self, value: Any, converter: Any = None) -> float:
        return value.degrees

    def storage_to_python(self, storage_value: Any, converter: Any = None) -> Celsius:
        return Celsius(storage_value)


celsius = LogicalCelsius()
print(logical_type_identifier(celsius).text)
# example.celsius;6c61796f75742d32
print(celsius.readable_extension_metadata())
# (b'layout-2', b'layout-1')
```

Rules for an identifier:

- The extension name must not contain `;` (registration raises `ValueError`).
- If the metadata is JSON, write it canonically (fixed key order and spacing): it is
  compared and hashed as bytes.

## Reading data stored under another identifier

`readable_extension_metadata()` returns every stored metadata value the type can read, the
current one first. `BaseLogicalType` takes it from `make_arrow_extension_type` (the type's
own metadata plus `also_reads`); override it if you build the extension type yourself.

When Orcapod reads a stored column it looks the logical type up by extension name, then
compares the stored metadata with that list:

- **Listed:** the column is read and given the type's *current* identifier. List an older
  value only if the current code decodes that layout correctly.
- **Not listed:** `LogicalTypeIdentifierError`, naming the column, the stored identifier
  and what this installation can read. Orcapod never falls back to the raw storage type or
  to the current version of the type.

```python
from orcapod.logical_types import LogicalTypeIdentifierError

stored = pa.table(
    [pa.array(["/data/a.txt"], pa.large_string())],
    schema=pa.schema(
        [
            pa.field(
                "location",
                pa.large_string(),
                metadata={
                    "ARROW:extension:name": "orcapod.path",
                    "ARROW:extension:metadata": '{"version":7}',
                },
            )
        ]
    ),
)
try:
    converter.load_logical_types(stored)
except LogicalTypeIdentifierError as error:
    print(error.column, error.stored.name, error.stored.metadata)
    print(error.readable)
# location orcapod.path b'{"version":7}'
# (b'{"version":1}', b'')
```

### Data written before identifiers existed

Before v0.3 the path, UPath, UUID, file, directory and pandas types wrote **empty**
metadata. Their layout was already version 1, so these seven types list the empty value as
readable (the `b''` above): a table written by an older Orcapod, opened as a source, is read
as version 1. The array type never wrote empty metadata and does not read it.
