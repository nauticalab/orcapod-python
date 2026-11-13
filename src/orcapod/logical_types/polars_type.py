"""``polars.DataFrame`` and ``polars.Series`` logical types for orcapod.

Storage
-------
Storage and hashing are separate (developer's decision, November 12, 2026). A value is
stored with Polars' own serialization and read back exactly as given; its hash is defined
by Orcapod from the value's content and is never a hash of the stored bytes.

``LogicalPolarsDataFrame`` maps ``pl.DataFrame`` <-> Arrow ``large_binary``: the cell holds
what ``DataFrame.write_ipc`` writes (the Arrow IPC file format) with Polars' defaults,
pinned explicitly in ``_frame_to_ipc_bytes``, and is read with ``pl.read_ipc`` from memory.
``LogicalPolarsSeries`` stores a ``pl.Series`` the same way, as the one-column frame
``series.to_frame()`` (a Polars Series name is always a ``str``, so no sentinel is needed).

A value reads back with exactly the Polars schema it was stored with: column order and
names, integer width and signedness, float width, ``Categorical`` with its ``Categories``
(name, namespace, physical width), ``Enum`` with its category order, ``Decimal`` precision
and scale, ``Datetime`` unit and time zone, ``Duration`` unit, ``List``, ``Array`` shape,
``Struct`` field order, and ``Null``. Chunking and sortedness flags are not stored: a value
comes back in one chunk with its flags cleared. The stored bytes depend on the Polars version
that wrote them (``compat_level`` is Polars' default, the newest), which no hash depends on.

Dtypes that cannot be stored and hashed exactly are rejected when the value is written,
naming the column and dtype: ``Object`` (``write_ipc`` cannot write it), Polars extension
dtypes (they read back as their storage type), and ``Int128`` / ``UInt128`` (stored
exactly by Polars, but the hash goes through an Arrow export that pyarrow cannot import).

Content hash
------------
The value hash is computed from the value itself by ``polars_frame_digest`` /
``polars_series_digest``, which the value handlers ``PolarsDataFrameHandler`` /
``PolarsSeriesHandler`` call and label ``polars_dataframe_v1`` / ``polars_series_v1``
(each label is defined on its handler). The digest is::

    sha256(descriptor + b"\\x00" + starfix_digest)

The hash is strict by default: every property of the value's logical content counts. Both
types carry an ignore list in their identifier (``IgnoreListLogicalType``, ITL-981), which
can leave out ``column_order`` and ``struct_field_order`` for a frame, ``series_name`` and
``struct_field_order`` for a Series; the list a data context uses is declared under
``hash_ignore`` in ``contexts/logical_types.json`` and is empty by default.

``starfix_digest`` is ``starfix.ArrowDigester.hash_table`` of a hash-only Arrow projection of
the values (see ``_hash_projection``), taken in starfix's ordered mode
(``ordered_columns=True, ordered_struct_fields=True``), so column order and struct field
order at every depth are part of the digest. starfix hashes a dictionary-encoded column by
its values and tells a null struct from a struct whose fields are all null.

``descriptor`` is canonical JSON (keys sorted, separators ``","`` and ``":"``, UTF-8) that
carries what starfix does not see, in a grammar Orcapod owns (never Polars' ``repr`` or its
``_PL_*`` metadata)::

    frame:  {"kind": "orcapod.polars.dataframe", "height": <rows>,
             "columns": [[<name>, <dtype>], ...]}   in column order
    series: {"kind": "orcapod.polars.series", "name": <name>, "height": <rows>,
             "dtype": <dtype>}

    <dtype> := "int8" | "int16" | "int32" | "int64"
             | "uint8" | "uint16" | "uint32" | "uint64"
             | "float16" | "float32" | "float64"
             | "boolean" | "string" | "binary" | "date" | "time" | "null"
             | {"categorical": [<name>, <namespace>, <physical>]}   its Categories
             | {"enum": [<category>, ...]}          every declared category, in order
             | {"decimal": [<precision>, <scale>]}
             | {"datetime": [<unit>, <time zone or null>]}
             | {"duration": <unit>}
             | {"list": <dtype>}
             | {"array": [<dtype>, <size>]}         a multi-dimensional array nests
             | {"struct": [[<field>, <dtype>], ...]}   in field order

The identity is everything a pod can observe through the public Polars API as the frame's
logical content (developer's decision, November 11, 2026): column order and struct field
order, at every depth, are part of it. Physical properties are not: chunking, sortedness
flags (which storage does not keep), physical ``Categorical`` codes, Arrow layout. An
Enum's full enumeration of choices is part of its dtype: its categories in declared order,
including categories no row holds, so ``Enum(["a", "b"])``, ``Enum(["b", "a"])`` and
``Enum(["a", "b", "z"])`` hash differently over the same values. Any change to the grammar or
the digest is a new definition under new labels (``polars_dataframe_v2``,
``polars_series_v2``). A dtype outside the grammar raises ``ValueError`` naming the column.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa
from starfix import ArrowDigester

from orcapod.logical_types.ignore_list import IgnoreListLogicalType

if TYPE_CHECKING:
    from polars._typing import PolarsDataType

    from orcapod.logical_types.protocols import TypeConverterProtocol


POLARS_DATAFRAME_EXTENSION_NAME = "orcapod.polars.dataframe"
"""Arrow extension name (and logical type name) of a stored ``pl.DataFrame``; also the
``"kind"`` of its hash descriptor, so the type has one name everywhere."""

POLARS_SERIES_EXTENSION_NAME = "orcapod.polars.series"
"""Arrow extension name (and logical type name) of a stored ``pl.Series``; also the
``"kind"`` of its hash descriptor."""

_VERSION = 1
"""Version of the stored layout and value hash of both types. Never stored under another
version, so there is no older layout to read."""

_AMBIGUOUS_PATH = re.compile(r"Ambiguous node path '(.*)': two schema nodes", re.DOTALL)
"""starfix's error for two schema nodes with one ``/``-joined path; group 1 is the path."""

_IPC_COMPRESSION = "uncompressed"
"""``compression`` of ``DataFrame.write_ipc``: Polars' default, pinned."""

_IPC_COMPAT_LEVEL: pl.CompatLevel | None = None
"""``compat_level`` of ``DataFrame.write_ipc``: Polars' default (``None``, the newest
level), pinned. The one place to change if stored values must be readable by older
readers (``pl.CompatLevel.oldest()``)."""

_UNSTORABLE_DTYPES: tuple[type[pl.DataType], ...] = (pl.Object, pl.Int128, pl.UInt128)

_SUPPORTED_DTYPES_TEXT = (
    "Supported dtypes are integers up to 64 bits, floats, String, Binary, Boolean, "
    "Categorical, Enum, Decimal, Date, Time, Datetime, Duration, Null, and List, Array or "
    "Struct of those."
)

# Grammar names of the dtypes that carry no parameters.
_PLAIN_DTYPE_NAMES: dict[object, str] = {
    pl.Int8: "int8",
    pl.Int16: "int16",
    pl.Int32: "int32",
    pl.Int64: "int64",
    pl.UInt8: "uint8",
    pl.UInt16: "uint16",
    pl.UInt32: "uint32",
    pl.UInt64: "uint64",
    pl.Float16: "float16",
    pl.Float32: "float32",
    pl.Float64: "float64",
    pl.Boolean: "boolean",
    pl.String: "string",
    pl.Binary: "binary",
    pl.Date: "date",
    pl.Time: "time",
    pl.Null: "null",
}


def _find_unstorable_dtype(dtype: PolarsDataType) -> PolarsDataType | None:
    """Return the first dtype inside ``dtype`` that cannot be stored and hashed exactly.

    Walks ``List``, ``Array`` and ``Struct`` children.

    Args:
        dtype: A Polars dtype instance or class, possibly nested.

    Returns:
        The offending (possibly nested) dtype, or ``None`` when every part of ``dtype`` can be
        stored losslessly.
    """
    if isinstance(dtype, type):
        # A bare dtype class has no children to walk.
        unstorable = issubclass(dtype, (pl.BaseExtension, *_UNSTORABLE_DTYPES))
        return dtype if unstorable else None
    if isinstance(dtype, (pl.BaseExtension, *_UNSTORABLE_DTYPES)):
        return dtype
    if isinstance(dtype, (pl.List, pl.Array)):
        return _find_unstorable_dtype(dtype.inner)
    if isinstance(dtype, pl.Struct):
        for field in dtype.fields:
            found = _find_unstorable_dtype(field.dtype)
            if found is not None:
                return found
    return None


def check_polars_schema_storable(schema: pl.Schema) -> None:
    """Raise if any column of ``schema`` cannot be stored and read back exactly.

    Args:
        schema: The schema of the frame about to be stored.

    Raises:
        ValueError: If a column's dtype is, or contains, ``Object``, ``Int128``, ``UInt128``
            or a Polars extension dtype. The message names the column and its dtype.
    """
    for name, dtype in schema.items():
        bad = _find_unstorable_dtype(dtype)
        if bad is None:
            continue
        nested = "" if bad is dtype else f" (contains {bad})"
        raise ValueError(
            f"Polars value: column {name!r} has dtype {dtype}{nested}, which cannot be "
            f"stored and read back exactly. {_SUPPORTED_DTYPES_TEXT} Convert the column "
            "explicitly before returning the value."
        )


def _ordered_pairs(
    pairs: Iterable[tuple[str, Any]], *, ignore_order: bool
) -> list[list[Any]]:
    """Encode named members (columns or struct fields) as a list of pairs.

    The one place where column order and struct field order enter the descriptor: a JSON
    list keeps its order under ``sort_keys``, a JSON object would not.

    Args:
        pairs: ``(name, dtype descriptor)`` pairs in the value's own order.
        ignore_order: When true the pairs are listed sorted by the UTF-8 bytes of their
            names, so the value's own order drops out.

    Returns:
        ``[[name, dtype descriptor], ...]`` in the value's order, or sorted by name.
    """
    members = [[name, descriptor] for name, descriptor in pairs]
    if ignore_order:
        members.sort(key=lambda member: member[0].encode("utf-8"))
    return members


def polars_dtype_descriptor(
    dtype: PolarsDataType, *, ignore_struct_field_order: bool = False
) -> Any:
    """Encode a Polars dtype in Orcapod's hash descriptor grammar.

    See the module docstring for the grammar.

    Args:
        dtype: The dtype to encode, possibly nested.
        ignore_struct_field_order: When true, the fields of every struct, at any depth,
            are listed sorted by name instead of in their own order.

    Returns:
        A JSON-serialisable value: a ``str`` for a plain dtype, a one-key ``dict`` for a
        parameterised one. Struct fields are a list of ``[name, dtype]`` pairs, in field
        order unless ``ignore_struct_field_order``.

    Raises:
        ValueError: If ``dtype`` (or a dtype nested in it) is outside the grammar. The
            message names the offending dtype; callers add the column.
    """
    if isinstance(dtype, pl.Enum):
        return {"enum": dtype.categories.to_list()}
    if dtype is pl.Categorical:  # the bare class means the default ``Categories``
        dtype = pl.Categorical()
    if isinstance(dtype, pl.Categorical):
        categories = dtype.categories
        return {
            "categorical": [
                categories.name(),
                categories.namespace(),
                _PLAIN_DTYPE_NAMES[categories.physical()],
            ]
        }
    if isinstance(dtype, pl.Decimal):
        return {"decimal": [dtype.precision, dtype.scale]}
    if isinstance(dtype, pl.Datetime):
        return {"datetime": [dtype.time_unit, dtype.time_zone]}
    if isinstance(dtype, pl.Duration):
        return {"duration": dtype.time_unit}
    if isinstance(dtype, pl.List):
        return {
            "list": polars_dtype_descriptor(
                dtype.inner, ignore_struct_field_order=ignore_struct_field_order
            )
        }
    if isinstance(dtype, pl.Array):
        inner = polars_dtype_descriptor(
            dtype.inner, ignore_struct_field_order=ignore_struct_field_order
        )
        return {"array": [inner, dtype.size]}
    if isinstance(dtype, pl.Struct):
        return {
            "struct": _ordered_pairs(
                (
                    (
                        field.name,
                        polars_dtype_descriptor(
                            field.dtype,
                            ignore_struct_field_order=ignore_struct_field_order,
                        ),
                    )
                    for field in dtype.fields
                ),
                ignore_order=ignore_struct_field_order,
            )
        }
    if not isinstance(dtype, pl.BaseExtension):
        dtype_class = dtype if isinstance(dtype, type) else type(dtype)
        name = _PLAIN_DTYPE_NAMES.get(dtype_class)
        if name is not None:
            return name
    raise ValueError(
        f"dtype {dtype} is outside Orcapod's hash grammar. {_SUPPORTED_DTYPES_TEXT}"
    )


def _hash_projection_type(arrow_type: pa.DataType) -> pa.DataType:
    """Map a nested Arrow type to the type starfix hashes it as.

    ``fixed_size_list`` (Polars ``Array``) becomes ``large_list``, a nested ``null`` becomes
    an all-null ``bool`` (starfix does not hash ``null``), and a dictionary becomes its value
    type. The descriptor records the real dtypes, so nothing is lost. Field names and
    nullability are kept.

    Args:
        arrow_type: The Arrow type of a column, as exported by Polars.

    Returns:
        The type to cast the column to before hashing.
    """
    if pa.types.is_null(arrow_type):
        return pa.bool_()
    if pa.types.is_dictionary(arrow_type):
        return _hash_projection_type(arrow_type.value_type)
    if (
        pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
    ):
        item = arrow_type.value_field
        return pa.large_list(item.with_type(_hash_projection_type(item.type)))
    if pa.types.is_struct(arrow_type):
        return pa.struct(
            [field.with_type(_hash_projection_type(field.type)) for field in arrow_type]
        )
    return arrow_type


def _hash_projection(df: pl.DataFrame) -> pa.Table:
    """Build the Arrow table whose starfix digest stands for the frame's values.

    Top-level ``Null`` columns are left out: their content is fully determined by their
    dtype and the row count, both in the descriptor.

    Args:
        df: The frame to hash. Its dtypes must already be inside the grammar.

    Returns:
        A table of the frame's non-``Null`` columns, cast with ``_hash_projection_type``.
    """
    table = df.to_arrow(compat_level=pl.CompatLevel.oldest())
    kept = [field for field in table.schema if not pa.types.is_null(field.type)]
    table = table.select([field.name for field in kept])
    target = pa.schema(
        [field.with_type(_hash_projection_type(field.type)) for field in kept]
    )
    return table.cast(target)


def _values_digest(
    df: pl.DataFrame, *, ordered_columns: bool, ordered_struct_fields: bool
) -> bytes:
    """The starfix digest of a frame's values: the only call into starfix.

    Args:
        df: The frame to hash. Its dtypes must already be inside the grammar.
        ordered_columns: Whether column order is part of the digest.
        ordered_struct_fields: Whether struct field order, at every depth, is part of the
            digest.

    Returns:
        ``ArrowDigester.hash_table`` of the projected table, in starfix's ordered or
        unordered mode for columns and for struct fields as asked.

    Raises:
        ValueError: If two column or struct field names form the same ``/`` path, which
            starfix cannot tell apart (a column ``"a/b"`` beside a struct column ``a``
            with a field ``b``). The message names the path.
    """
    try:
        return ArrowDigester.hash_table(
            _hash_projection(df),
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
        )
    except ValueError as error:
        ambiguous = _AMBIGUOUS_PATH.match(str(error))
        if ambiguous is None:
            raise
        raise ValueError(
            "Polars value cannot be hashed: two of its column or struct field names "
            f"form the same '/' path {ambiguous.group(1)!r} (a name containing '/' "
            "stands for the same path as a struct field). Rename one of them."
        ) from error


def _digest(
    descriptor: dict[str, Any],
    df: pl.DataFrame,
    *,
    ignore_column_order: bool,
    ignore_struct_field_order: bool,
) -> bytes:
    """Combine a descriptor and a frame's values into the value digest.

    The digest is ``sha256(descriptor_json + b"\x00" + starfix_digest)``, where
    ``descriptor_json`` is ``descriptor`` as canonical JSON (keys sorted, separators ``","``
    and ``":"``, UTF-8, non-ASCII kept) and ``starfix_digest`` is ``_values_digest(df)``.

    Args:
        descriptor: The frame or Series descriptor in the module's grammar, already built
            with the same two ``ignore`` settings.
        df: The frame whose values are hashed (a Series is passed as its one-column frame).
        ignore_column_order: Leave column order out of the values digest.
        ignore_struct_field_order: Leave struct field order out of the values digest.

    Returns:
        The 32 digest bytes.
    """
    descriptor_bytes = json.dumps(
        descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    values_digest = _values_digest(
        df,
        ordered_columns=not ignore_column_order,
        ordered_struct_fields=not ignore_struct_field_order,
    )
    return hashlib.sha256(descriptor_bytes + b"\x00" + values_digest).digest()


def polars_frame_digest(
    df: pl.DataFrame,
    *,
    ignore_column_order: bool = False,
    ignore_struct_field_order: bool = False,
) -> bytes:
    """Value digest of a ``pl.DataFrame`` (see the module docstring for the scheme).

    By default the digest is strict: every property of the frame's logical content counts.
    Each ``ignore`` parameter leaves one property out, so that frames differing only in
    it have equal digests.

    Args:
        df: The frame to hash.
        ignore_column_order: Leave column order out: the descriptor lists the columns
            sorted by name and starfix hashes them unordered.
        ignore_struct_field_order: Leave struct field order out, at every depth: the
            descriptor lists each struct's fields sorted by name and starfix hashes them
            unordered.

    Returns:
        The 32 digest bytes; ``PolarsDataFrameHandler`` labels them.

    Raises:
        ValueError: If a column's dtype is outside the descriptor grammar.
    """
    columns: list[tuple[str, Any]] = []
    for name, dtype in df.schema.items():
        try:
            dtype_descriptor = polars_dtype_descriptor(
                dtype, ignore_struct_field_order=ignore_struct_field_order
            )
        except ValueError as error:
            raise ValueError(
                f"Polars value: column {name!r} cannot be hashed: {error}"
            ) from error
        columns.append((name, dtype_descriptor))
    descriptor = {
        "kind": POLARS_DATAFRAME_EXTENSION_NAME,
        "height": df.height,
        "columns": _ordered_pairs(columns, ignore_order=ignore_column_order),
    }
    return _digest(
        descriptor,
        df,
        ignore_column_order=ignore_column_order,
        ignore_struct_field_order=ignore_struct_field_order,
    )


def polars_series_digest(
    series: pl.Series,
    *,
    ignore_name: bool = False,
    ignore_struct_field_order: bool = False,
) -> bytes:
    """Value digest of a ``pl.Series``: its name, dtype and values.

    Args:
        series: The Series to hash.
        ignore_name: Leave the Series name out: the descriptor's ``"name"`` and the name of
            the hashed column are both ``""``, whatever the Series is called.
        ignore_struct_field_order: Leave struct field order out, at every depth, as for a
            frame.

    Returns:
        The 32 digest bytes; ``PolarsSeriesHandler`` labels them. Never equal to the digest
        of the one-column frame holding the same Series (the descriptors' ``"kind"``
        differ).

    Raises:
        ValueError: If the dtype is outside the descriptor grammar.
    """
    try:
        dtype_descriptor = polars_dtype_descriptor(
            series.dtype, ignore_struct_field_order=ignore_struct_field_order
        )
    except ValueError as error:
        raise ValueError(
            f"Polars value: Series {series.name!r} cannot be hashed: {error}"
        ) from error
    hashed = series.alias("") if ignore_name else series
    descriptor = {
        "kind": POLARS_SERIES_EXTENSION_NAME,
        "name": hashed.name,
        "height": series.len(),
        "dtype": dtype_descriptor,
    }
    return _digest(
        descriptor,
        hashed.to_frame(),
        ignore_column_order=False,
        ignore_struct_field_order=ignore_struct_field_order,
    )


def _frame_to_ipc_bytes(df: pl.DataFrame) -> bytes:
    """Serialise a frame with Polars' own ``write_ipc`` after checking its dtypes.

    Args:
        df: The frame to store.

    Returns:
        The bytes ``df.write_ipc`` writes (Arrow IPC file format) with Polars' default
        compression and compatibility level, both pinned here. ``record_batch_size`` is
        left at its default without being named: the oldest supported Polars (1.36) has
        no such parameter.

    Raises:
        ValueError: If a column's dtype cannot be stored and hashed exactly (see
            ``check_polars_schema_storable``).
    """
    check_polars_schema_storable(df.schema)
    buffer = io.BytesIO()
    df.write_ipc(
        buffer,
        compression=_IPC_COMPRESSION,
        compat_level=_IPC_COMPAT_LEVEL,
    )
    return buffer.getvalue()


def _ipc_bytes_to_frame(storage_value: Any) -> pl.DataFrame:
    """Read stored bytes back into a frame with Polars' own ``read_ipc``.

    The bytes are read from memory: no file is mapped and nothing is fetched.

    Args:
        storage_value: The bytes (or a buffer) written by ``_frame_to_ipc_bytes``, as
            stored in ``large_binary``.

    Returns:
        The frame, with the schema it was stored with, in one chunk.
    """
    return pl.read_ipc(
        io.BytesIO(bytes(storage_value)),
        columns=None,
        n_rows=None,
        use_pyarrow=False,
        memory_map=False,
        rechunk=True,
    )


class LogicalPolarsDataFrame(IgnoreListLogicalType):
    """Logical type for ``pl.DataFrame``.

    Stores frames as Arrow ``large_binary`` holding what Polars' ``write_ipc`` writes (see
    the module docstring for the pinned settings and the rejected dtypes).

    The extension name is ``"orcapod.polars.dataframe"``: ``orcapod.`` followed by the
    library and the type, lower case (developer's decision, November 12, 2026), so the name
    stays in Orcapod's namespace and still says which Python type a stored column holds.

    Example:
        >>> lt = LogicalPolarsDataFrame()
        >>> df = pl.DataFrame({"x": [1, 2]}, schema={"x": pl.Int8})
        >>> dict(lt.storage_to_python(lt.python_to_storage(df)).schema)
        {'x': Int8}
    """

    EXTENSION_NAME = POLARS_DATAFRAME_EXTENSION_NAME
    STORAGE_TYPE = pa.large_binary()
    VERSION = _VERSION
    OPTIONS = frozenset({"column_order", "struct_field_order"})
    python_type: type = pl.DataFrame

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> bytes:
        """Serialise a ``pl.DataFrame`` to Polars IPC bytes.

        Args:
            value: A ``pl.DataFrame`` whose dtypes can all round-trip exactly.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The bytes ``DataFrame.write_ipc`` writes for the frame.

        Raises:
            TypeError: If ``value`` is not a ``pl.DataFrame``.
            ValueError: If a column's dtype cannot be stored and hashed exactly (see
                ``check_polars_schema_storable``).
        """
        if not isinstance(value, pl.DataFrame):
            raise TypeError(
                f"LogicalPolarsDataFrame: expected a pl.DataFrame, got {type(value)!r}"
            )
        return _frame_to_ipc_bytes(value)

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> pl.DataFrame:
        """Reconstruct a ``pl.DataFrame`` from Polars IPC bytes.

        Args:
            storage_value: Raw bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The ``pl.DataFrame`` with the schema it was stored with.

        """
        return _ipc_bytes_to_frame(storage_value)


class LogicalPolarsSeries(IgnoreListLogicalType):
    """Logical type for ``pl.Series``.

    Stores the Series as a one-column frame named after the Series, in the same form as
    ``LogicalPolarsDataFrame``, so its name, dtype and values round-trip exactly
    under the same rules.

    The extension name is ``"orcapod.polars.series"``, by the same rule as the frame's.

    Example:
        >>> lt = LogicalPolarsSeries()
        >>> s = pl.Series("m", [1, 2], dtype=pl.UInt8)
        >>> back = lt.storage_to_python(lt.python_to_storage(s))
        >>> back.name, back.dtype
        ('m', UInt8)
    """

    EXTENSION_NAME = POLARS_SERIES_EXTENSION_NAME
    STORAGE_TYPE = pa.large_binary()
    VERSION = _VERSION
    OPTIONS = frozenset({"series_name", "struct_field_order"})
    python_type: type = pl.Series

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> bytes:
        """Serialise a ``pl.Series`` to Polars IPC bytes.

        Args:
            value: A ``pl.Series`` whose dtype can round-trip exactly.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The bytes ``DataFrame.write_ipc`` writes for the Series as a one-column frame.

        Raises:
            TypeError: If ``value`` is not a ``pl.Series``.
            ValueError: If the dtype cannot be stored and hashed exactly (see
                ``check_polars_schema_storable``).
        """
        if not isinstance(value, pl.Series):
            raise TypeError(
                f"LogicalPolarsSeries: expected a pl.Series, got {type(value)!r}"
            )
        return _frame_to_ipc_bytes(value.to_frame())

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> pl.Series:
        """Reconstruct a ``pl.Series`` from Polars IPC bytes.

        Args:
            storage_value: Raw bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The ``pl.Series`` with the name, dtype and values it was stored with.

        Raises:
            TypeError: If the stored bytes do not hold a one-column frame.
        """
        frame = _ipc_bytes_to_frame(storage_value)
        if frame.width != 1:
            raise TypeError(
                "LogicalPolarsSeries: stored value has "
                f"{frame.width} columns, expected exactly 1"
            )
        return frame.to_series(0)
