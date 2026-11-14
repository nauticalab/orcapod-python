"""Content hashes of ``pandas.DataFrame`` and ``pandas.Series`` values.

A pandas value hashes as::

    sha256(canonical_json(descriptor) + b"\\x00" + arrow_hasher.hash_table_ordered(values_table))

under the ``ContentHash`` method label ``"pandas_dataframe_v1"`` (a frame) or
``"pandas_series_v1"`` (a Series); see ``orcapod.hashing.labelled_hash``. The label names this
whole procedure: the grammar, the values table and the ordered Arrow digest it embeds. It
changes only when the procedure would give an existing value a different digest (then
``pandas_dataframe_v2`` / ``pandas_series_v2``), and it never enters the computation: only the
digest bytes of ``hash_table_ordered`` are hashed, not that hash's own label. Such a change
also changes the identifier of the pandas logical types (``logical_types/pandas_type.py``).

The hash is strict (developer's decisions, November 11 and 12, 2026): everything about a
value that can be changed is part of its identity, ``attrs`` and ``-0.0`` versus ``0.0``
included. The one exception is a property the stored form does not restore, which never
counts, so that a value read back from a store hashes like the value that was written:
``flags``, the kind of a ``RangeIndex``, an index's ``freq``, unused MultiIndex levels, the
storage of a string dtype, and the ``object`` dtype of a column that reads back as another
dtype. ``PANDAS_IDENTITY_PROPERTIES`` lists every property and whether it is identity; it
is the one place that states the decision.

Ignore lists
------------
A data context can configure the hash of each type to ignore some properties
(``PANDAS_DATAFRAME_OPTIONS``, ``PANDAS_SERIES_OPTIONS``; the list is saved in the logical
type's extension metadata, ``orcapod.logical_types.ignore_list``). The preimage keeps its
shape: an ignored property is hashed with a fixed value. ``attrs`` becomes ``{}``,
``series_name`` becomes ``null``, and ``column_order`` hashes the columns sorted by the
canonical JSON of their labels. Nothing is ignored by default. (``struct_field_order`` is not
an option: it acted only on a ``pd.ArrowDtype`` struct column, which storage cannot read back
and so is refused, ITL-1054.)

Hashing what storage restores
-----------------------------
An ``object`` column (or index level) whose values are not strings does not come back as
``object``: ``[1, 2]`` reads back ``int64`` and ``[1, None]`` reads back ``float64``. Such a
column is hashed in the form it reads back in: its Arrow column is converted to pandas the
way storage does, and when the result is no longer ``object``, that dtype is described and
its Arrow form is digested. An ``object`` column of lists or dicts stays ``object``, but its
elements can change (a list ``[1, None]`` reads back ``[1.0, nan]``), so the Arrow form of
what was read back is digested. Any other ``object`` column that reads back as ``object``
(strings, bytes, decimals, dates) is hashed as it is.

A categorical column is described with the categories storage restores: they are read back
from the Arrow dictionary, so ``object`` categories ``[1, 2]`` are described as ``int64``.

The same holds for the axes. Storage writes the name of an index level as text, so the names
``1`` and ``"1"`` are one name once read back and hash equal; the name described is the one
storage restores. (``columns.names`` and a Series' name are restored with their types, so
they are described typed.) The dtype of each level of the column axis is restored, so it is
described: an ``int64`` and an ``object`` column axis with the same labels differ. Both are
read from the frame storage builds for the value's Arrow schema.

Values table
------------
``pa.Table.from_pandas(value, preserve_index=True)`` with its fields renamed by position:
``c0 .. cN-1`` for the columns in order, then ``i0 .. iM-1`` for the index levels in order.
Labels are not Arrow field names: they are in the descriptor, typed. A Series is hashed as its
one-column frame. The digest is the data context's Arrow hasher's ``hash_table_ordered``: the
same preprocessing as every Orcapod Arrow hash, with column order and struct field order at every
depth included (starfix's ordered options). It covers the Arrow types (with starfix's large /
non-large unification), the row count and the values; a categorical column is hashed by its
values.

Descriptor grammar
------------------
Canonical JSON, in a grammar Orcapod owns (never ``repr``, ``str(dtype)`` alone, or pyarrow's
``pandas`` schema metadata)::

    frame:  {"kind": "orcapod.pandas.dataframe", "height": <rows>, "attrs": <attrs>,
             "columns": [[<label>, <dtype>], ...],      in column order
             "columns_names": [<label>, ...],           columns.names, one per column level
             "columns_dtypes": [<dtype>, ...],          column axis, one per column level;
                                                        a pd.ArrowDtype level is
                                                        {"arrow": <Arrow type text>}
             "index": [[<name>, <dtype>], ...]}         index levels in order: name, dtype
    series: {"kind": "orcapod.pandas.series", "height": <rows>, "attrs": <attrs>,
             "name": <label>, "dtype": <dtype>,
             "index": [[<name>, <dtype>], ...]}

    <name>  := <str> | null                             an index level name as storage
                                                        restores it: its text
    <label> := <str> | null | {"bool": b} | {"int": n} | {"float": <float.hex()>}
             | {"tuple": [<label>, ...]}
             | {"timestamp": [<count since the epoch, in unit>, <unit>, <tz or null>]}
             | {"date": <iso date>}
             | {"timedelta": [<count, in unit>, <unit>]}
             | {"period": [<ordinal>, <period alias>]}
             | {"nat": "datetime" | "timedelta"}
    <dtype> := {"numpy": <numpy dtype name>}
             | {"masked": "Int8" | ... | "UInt64" | "Float32" | "Float64" | "boolean"}
             | "string"                                 pd.StringDtype, any storage
             | {"datetimetz": [<unit>, <tz>]}            <tz>: the text storage writes,
                                                        "UTC", "Europe/Berlin", "+01:00"
             | {"categorical": {"categories": [<dtype>, [<label>, ...]], "ordered": <bool>}}
             | {"period": <period alias>} | {"interval": [<subtype dtype>, <closed>]}
             | {"arrow": <Arrow type text>}             pd.ArrowDtype of a non-nested Arrow
                                                        type: str(type), e.g. "large_binary",
                                                        "decimal128(10, 2)", "null"

``<attrs>`` is ``attrs`` as the JSON object it is stored as: ``{1: "x"}`` is
``{"1": "x"}`` and a tuple is a list. ``attrs`` with an entry JSON cannot write (a ``set``,
a tuple as a key) are refused with ``ValueError`` naming the key, by the hash and by
storage alike. ``kind`` is the name of the
value's logical type.

A period alias is pandas' own period frequency string (``"M"``, ``"Q-DEC"``, as in
``Period.freqstr``). A label or dtype outside the grammar raises ``ValueError``.

What is refused
---------------
A value storage would write but not restore is refused with ``ValueError`` by the hash and by
storage alike, through one check, ``require_restorable`` (ITL-1054): MultiIndex levels that
would be written in another order (level names that are also level positions,
``names=[1, 0]``), and values storage cannot read back (a nested, dictionary or fixed-size
``pd.ArrowDtype`` column or index level; a categorical, period, timedelta, interval, date or
``pd.ArrowDtype`` column axis; tuple categories; a ``time64[ns]`` index level holding
sub-microsecond values; a MultiIndex level holding tuples or lists), column labels and index level names that would read back as another
label or name (``False`` reads back ``True``; ``"__index_level_0__"`` reads back unnamed), and
labels and names that collide once written as text.
A value Arrow cannot convert is refused by ``pandas_frame_to_arrow``. Both messages name the
column, index level or column-axis level.
"""

from __future__ import annotations

import datetime as dt
import json
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow.lib import tzinfo_to_string

from orcapod.hashing.labelled_hash import canonical_json, labelled_content_hash

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import ArrowHasherProtocol
    from orcapod.types import ContentHash


PANDAS_DATAFRAME_HASH_LABEL = "pandas_dataframe_v1"
"""``ContentHash.method`` of a ``pd.DataFrame`` value hash: the single definition."""

PANDAS_SERIES_HASH_LABEL = "pandas_series_v1"
"""``ContentHash.method`` of a ``pd.Series`` value hash: the single definition."""

PANDAS_UNNAMED_SERIES_COLUMN = "__pandas_series_unnamed__"
"""The column name storage gives a Series without a name: the single definition.

Storage writes a Series as a one-column frame whose column is the Series' name. The hash
builds the same frame to learn what storage restores of the index names.
"""


@dataclass(frozen=True)
class PandasIdentityProperty:
    """One property of a pandas value and whether the ``v1`` hash depends on it.

    Attributes:
        name: A stable name for the property.
        identity: Whether two values that differ only in this property hash differently.
        carried_by: Which part of the hash sees the property: the ordered Arrow digest of
            the values table, the descriptor, or both; empty when ``identity`` is false.
        description: What the property is, in pandas terms. For a property that is not
            identity, it says why.
        option: Whether the property may be named in a logical type's ignore list. Only
            identity properties can be options.
    """

    name: str
    identity: bool
    carried_by: Literal["digest", "descriptor", "digest and descriptor", ""]
    description: str
    option: bool = False


PANDAS_IDENTITY_PROPERTIES: tuple[PandasIdentityProperty, ...] = (
    PandasIdentityProperty(
        "column_order", True, "digest", "order of the columns", option=True
    ),
    PandasIdentityProperty(
        "index_level_order", True, "digest", "order of the levels of a MultiIndex"
    ),
    PandasIdentityProperty(
        "values", True, "digest", "the values, their row order and the row count"
    ),
    PandasIdentityProperty(
        "arrow_type",
        True,
        "digest",
        "a column's Arrow type: integer and float width, datetime unit and timezone",
    ),
    PandasIdentityProperty("index_values", True, "digest", "the index values"),
    PandasIdentityProperty(
        "index_names",
        True,
        "descriptor",
        "the name of each index level, as the text storage writes it",
    ),
    PandasIdentityProperty(
        "columns_dtype",
        True,
        "descriptor",
        "the dtype of each level of the column axis (an int64 vs an object Index of "
        "the same labels)",
    ),
    PandasIdentityProperty(
        "index_dtype",
        True,
        "digest and descriptor",
        "the dtype of each index level, as storage restores it",
    ),
    PandasIdentityProperty(
        "columns_names", True, "descriptor", "columns.names, one per column level"
    ),
    PandasIdentityProperty(
        "label_type",
        True,
        "descriptor",
        "the type of a column label where the labels have one type (0 vs '0'; a "
        "MultiIndex tuple vs its text)",
    ),
    PandasIdentityProperty(
        "series_name", True, "descriptor", "a Series' name, typed", option=True
    ),
    PandasIdentityProperty(
        "value_kind", True, "descriptor", "a frame vs its one-column Series"
    ),
    PandasIdentityProperty(
        "pandas_dtype",
        True,
        "descriptor",
        "the pandas dtype where the Arrow values coincide: masked vs NumPy "
        "(Int64 vs int64, boolean vs bool), string vs object, period frequency, "
        "interval closed side",
    ),
    PandasIdentityProperty(
        "categorical_dtype", True, "descriptor", "categorical vs its plain values"
    ),
    PandasIdentityProperty(
        "categorical_categories",
        True,
        "descriptor",
        "the category list, unused categories included, and its order",
    ),
    PandasIdentityProperty(
        "categorical_ordered", True, "descriptor", "a categorical's ordered flag"
    ),
    PandasIdentityProperty(
        "signed_zero",
        True,
        "digest",
        "-0.0 vs 0.0 (pandas calls them equal; the values are hashed as bytes)",
    ),
    PandasIdentityProperty(
        "attrs",
        True,
        "descriptor",
        "the attrs dict, as the JSON object storage keeps",
        option=True,
    ),
    PandasIdentityProperty(
        "flags", False, "", "flags.allows_duplicate_labels: storage does not restore it"
    ),
    PandasIdentityProperty(
        "mixed_label_type",
        False,
        "",
        "the types of column labels of mixed type (0 beside 'b'): storage writes such "
        "labels as text",
    ),
    PandasIdentityProperty(
        "index_extension_dtype",
        False,
        "",
        "a masked, string or Arrow dtype of an index level (Int64 vs int64): storage "
        "restores an index level as NumPy",
    ),
    PandasIdentityProperty(
        "index_name_type",
        False,
        "",
        "the type of an index level name (1 vs '1'): storage writes the name as text",
    ),
    PandasIdentityProperty(
        "index_kind",
        False,
        "",
        "RangeIndex vs an int64 Index with the same values: storage restores an Index",
    ),
    PandasIdentityProperty(
        "index_freq",
        False,
        "",
        "freq of a DatetimeIndex or TimedeltaIndex: storage does not restore it",
    ),
    PandasIdentityProperty(
        "index_unused_levels",
        False,
        "",
        "unused levels of a MultiIndex: storage does not restore them",
    ),
    PandasIdentityProperty(
        "string_storage",
        False,
        "",
        "string[python] vs string[pyarrow]: storage restores string[python]",
    ),
    PandasIdentityProperty(
        "object_dtype",
        False,
        "",
        "object dtype of a column whose values read back as another dtype "
        "(object ints read back int64)",
    ),
    PandasIdentityProperty(
        "dict_key_order",
        False,
        "",
        "key order of dicts in an object column: a dict is unordered",
    ),
)
"""Every property the ``v1`` hash was decided on, and whether it is identity.

This is the single statement of the decision (developer, November 11 and 12, 2026): the
hash is strict, so every property of a value counts, except a property the stored form does
not restore, which never counts and cannot be an option. Properties with ``option=True`` are
the ones a logical type's ignore list may name. The functions below implement exactly this
list; ``tests/test_hashing/test_pandas_value_identity.py`` checks every entry against a pair
of values. Changing an entry's ``identity`` changes existing hashes: it needs new labels and
a new logical type identifier.
"""

PANDAS_DATAFRAME_OPTIONS: frozenset[str] = frozenset({"attrs", "column_order"})
"""The properties a data context can configure a ``pd.DataFrame`` hash to ignore."""

PANDAS_SERIES_OPTIONS: frozenset[str] = frozenset({"attrs", "series_name"})
"""The properties a data context can configure a ``pd.Series`` hash to ignore."""

_NOTHING_IGNORED: frozenset[str] = frozenset()

_MASKED_DTYPE_NAMES = frozenset(
    {
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "boolean",
    }
)


def _timezone_text(tz: dt.tzinfo) -> str:
    """Return the text storage writes for a timezone: ``"UTC"``, ``"Europe/Berlin"``, ``"+01:00"``.

    It is Arrow's own text for the timezone, so it does not depend on which timezone
    class holds it: ``datetime.timezone(timedelta(hours=1))`` is read back from storage
    as another class of the same offset, and both are ``"+01:00"``.
    """
    return tzinfo_to_string(tz)


def _count_in_own_unit(moment: pd.Timestamp | pd.Timedelta) -> int:
    """Return the integer count of a timestamp or duration in its own unit.

    Not ``.value``, which converts to nanoseconds and overflows for a value that only a
    coarser unit can hold (``pd.Timestamp("2500-01-01")`` in seconds).
    """
    return int(moment.asm8.astype(np.int64))


def pandas_label_descriptor(label: Any) -> Any:
    """Describe a pandas label (column label, index or Series name, category) in the grammar.

    Args:
        label: The label.

    Returns:
        The label's grammar term; typed, so ``0`` and ``"0"`` differ.

    Raises:
        ValueError: If the label's type is outside the grammar.
    """
    if label is None or isinstance(label, str):
        return label
    if isinstance(label, (bool, np.bool_)):
        return {"bool": bool(label)}
    if isinstance(label, (pd.Timedelta, dt.timedelta, np.timedelta64)):
        # Before the integer test: ``np.timedelta64`` is a NumPy signed integer.
        delta = pd.Timedelta(label)
        if not isinstance(delta, pd.Timedelta):  # NaT
            return {"nat": "timedelta"}
        return {"timedelta": [_count_in_own_unit(delta), delta.unit]}
    if isinstance(label, (int, np.integer)):
        return {"int": int(label)}
    if isinstance(label, (float, np.floating)):
        return {"float": float(label).hex()}
    if isinstance(label, tuple):
        return {"tuple": [pandas_label_descriptor(part) for part in label]}  # pyright: ignore[reportUnknownVariableType]
    if isinstance(label, (pd.Timestamp, dt.datetime, np.datetime64)):
        timestamp = pd.Timestamp(label)
        if not isinstance(timestamp, pd.Timestamp):  # NaT
            return {"nat": "datetime"}
        tz = None if timestamp.tz is None else _timezone_text(timestamp.tz)
        return {"timestamp": [_count_in_own_unit(timestamp), timestamp.unit, tz]}
    if isinstance(label, dt.date):
        return {"date": label.isoformat()}
    if isinstance(label, pd.Period):
        return {"period": [label.ordinal, label.freqstr]}
    raise ValueError(
        f"pandas label {label!r} of type {type(label).__name__} is outside the "
        "pandas_dataframe_v1 label grammar"
    )


def pandas_dtype_descriptor(dtype: Any) -> Any:
    """Describe a pandas dtype in the grammar.

    Args:
        dtype: A numpy dtype or a pandas extension dtype.

    Returns:
        The dtype's grammar term.

    Raises:
        ValueError: If the dtype is outside the grammar.
    """
    if isinstance(dtype, pd.CategoricalDtype):
        categories = dtype.categories
        return {
            "categorical": {
                "categories": [
                    pandas_dtype_descriptor(categories.dtype),
                    [pandas_label_descriptor(c) for c in categories],
                ],
                "ordered": bool(dtype.ordered),
            }
        }
    if isinstance(dtype, pd.ArrowDtype):
        # Storage restores the exact Arrow type, but the values digest unifies large and
        # non-large variants, so the type text is the term (``null`` included: the digest sees an all-null ``large_string`` there, see
        # ``_restored_form``, and the term keeps it apart from a ``large_string`` column).
        # A nested, dictionary or run-end Arrow type never reaches a hash: storage cannot
        # read such a column back, so ``require_restorable`` refuses the value (ITL-1054).
        return {"arrow": str(dtype.pyarrow_dtype)}
    if isinstance(dtype, pd.StringDtype):
        # The storage (python / pyarrow) is not kept by the stored form, so it is not
        # part of the term.
        return "string"
    if isinstance(dtype, pd.DatetimeTZDtype):
        return {"datetimetz": [dtype.unit, _timezone_text(dtype.tz)]}
    if isinstance(dtype, pd.PeriodDtype):
        # The period alias ("M", "Q-DEC"), as in Period.freqstr; not the offset alias
        # ("ME", "QE-DEC") that dtype.freq.freqstr gives since pandas 2.2.
        return {"period": dtype.name.removeprefix("period[").removesuffix("]")}
    if isinstance(dtype, pd.IntervalDtype):
        return {"interval": [pandas_dtype_descriptor(dtype.subtype), dtype.closed]}
    if isinstance(dtype, np.dtype):
        return {"numpy": dtype.name}
    if getattr(dtype, "name", None) in _MASKED_DTYPE_NAMES:
        return {"masked": dtype.name}
    raise ValueError(
        f"pandas dtype {dtype!r} is outside the pandas_dataframe_v1 dtype grammar"
    )


def _column_descriptor(label: Any, dtype: Any) -> list[Any]:
    """Return the ``[<label>, <dtype>]`` term of one column.

    Args:
        label: The column's label, in the form storage restores.
        dtype: The column's pandas dtype, in the form storage restores.

    Returns:
        The term for the descriptor's ``columns`` list.

    Raises:
        ValueError: If the label or the dtype is outside the grammar; a dtype error names
            the column.
    """
    try:
        dtype_term = pandas_dtype_descriptor(dtype)
    except ValueError as exc:
        raise ValueError(f"column {label!r}: {exc}") from exc
    return [pandas_label_descriptor(label), dtype_term]


def pandas_attrs_as_stored(attrs: Any) -> Any:
    """Return ``attrs`` as the JSON object storage keeps, refusing what JSON cannot hold.

    Storage writes ``attrs`` as JSON, so a value read back has the JSON form:
    ``{1: "x"}`` reads back ``{"1": "x"}`` and a tuple reads back a list. An entry JSON
    cannot write (a ``set``, an arbitrary object, a tuple as a key) is refused here, by
    Orcapod, so that storing and hashing behave alike on every pyarrow version: pyarrow
    would drop the whole ``attrs`` with a warning, or raise ``TypeError``, depending on its
    version.

    Args:
        attrs: The ``attrs`` dict of a DataFrame or Series.

    Returns:
        ``attrs`` in the form it reads back from storage in.

    Raises:
        ValueError: If an entry cannot be written as JSON; the message names its key.
    """
    for key, value in attrs.items():
        try:
            json.dumps({key: value})
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"attrs[{key!r}] cannot be written as JSON, so storage cannot keep it "
                f"({exc}). Use JSON values (text, numbers, booleans, None, lists, "
                "dicts with text keys) or remove the entry."
            ) from exc
    return json.loads(json.dumps(attrs))


def _restored_form(
    dtype: Any, column: pa.ChunkedArray[Any], *, index_level: bool
) -> tuple[Any, pa.ChunkedArray[Any]]:
    """Return the dtype and the Arrow column to digest of a field read back from storage.

    The form is ``_restored_dtype_and_column``, except that a column whose Arrow type is
    ``null`` is digested as an all-null ``large_string`` column of the same length. Starfix
    refuses a null column, and storage cannot restore the type a column with no value had (an
    all-null ``pd.ArrowDtype`` decimal column reads back ``object`` ``[None]``, the same as
    a plain ``object`` column of ``None``), so every such column takes this one form, a
    value and its read-back alike. A cast of a null array holds no value, so nothing is
    inferred. The dtype is not changed, so the descriptor still tells the column from a
    real ``string`` column.

    Args:
        dtype: The field's pandas dtype.
        column: The field's Arrow column in the values table.
        index_level: Whether the field is an index level.

    Returns:
        The dtype to describe and the Arrow column to digest.
    """
    restored_dtype, restored = _restored_dtype_and_column(
        dtype, column, index_level=index_level
    )
    if pa.types.is_null(restored.type):
        restored = restored.cast(pa.large_string())
    return restored_dtype, restored


def _restored_dtype_and_column(
    dtype: Any, column: pa.ChunkedArray[Any], *, index_level: bool
) -> tuple[Any, pa.ChunkedArray[Any]]:
    """Return the dtype and Arrow column a field has once read back from storage.

    A column changes only when it is ``object`` or categorical (the dtype of the
    categories is the one the Arrow dictionary restores); an index level can change whatever its
    dtype, because storage restores an index level as NumPy (``Int64`` reads back ``int64``,
    or ``float64`` when it holds a missing value; ``string`` reads back ``object``). The
    field's Arrow column is converted to pandas the way storage does. When the result has
    another dtype (``[1, None]`` reads back ``float64``), that dtype and its Arrow form are
    returned. When it stays ``object`` but holds lists or dicts, whose elements can change
    (a list ``[1, None]`` reads back ``[1.0, nan]``), the Arrow form of what was read back
    is returned.

    Args:
        dtype: The field's pandas dtype.
        column: The field's Arrow column in the values table.
        index_level: Whether the field is an index level.

    Returns:
        The dtype to describe and the Arrow column to digest.
    """
    if isinstance(dtype, pd.CategoricalDtype):
        # The categories are restored from the Arrow dictionary, so their dtype can
        # change (``object`` categories ``[1, 2]`` read back ``int64``); the codes and
        # the dictionary that are digested do not.
        return column.to_pandas().dtype, column
    if not index_level and dtype != np.dtype(object):
        return dtype, column
    restored = column.to_pandas()
    if restored.dtype != dtype:
        return restored.dtype, pa.chunked_array([pa.Array.from_pandas(restored)])
    if dtype == np.dtype(object) and (
        pa.types.is_nested(column.type) or pa.types.is_decimal(column.type)
    ):
        # A list or dict cell can change inside: ``[1, None]`` reads back ``[1.0, nan]``.
        # Decimals read back as ``Decimal`` objects, whose Arrow precision is then taken
        # from the values, not from the type they were written with.
        return dtype, pa.chunked_array([pa.Array.from_pandas(restored)])
    return dtype, column


@dataclass(frozen=True)
class _StoredAxes:
    """The axes of a frame as storage restores them.

    Attributes:
        column_labels: The label of each column, in column order. Storage writes labels of
            mixed type as text, so ``0`` beside ``"b"`` is ``"0"`` here.
        columns_names: ``columns.names``, one per level of the column axis.
        column_axis_dtypes: The dtype of each level of the column axis.
        index_names: The name of each index level. Storage writes a name as text, so
            ``1`` and ``"1"`` are both ``"1"`` here.
        schema_dtypes: The dtype storage restores for each column as far as the schema
            alone decides it (a ``pd.ArrowDtype`` string column is restored as
            ``string``). It is right for every column that is neither ``object`` nor
            categorical; the dtype of those depends on the values.
    """

    column_labels: list[Any]
    columns_names: list[Any]
    column_axis_dtypes: list[Any]
    index_names: list[Any]
    schema_dtypes: list[Any]


@dataclass(frozen=True)
class _StoredForm:
    """What storage restores of a frame, beside its values.

    Attributes:
        column_dtypes: The dtype of each column, in column order.
        index_dtypes: The dtype of each index level, in level order.
        axes: The labels, names and dtypes of the two axes.
    """

    column_dtypes: list[Any]
    index_dtypes: list[Any]
    axes: _StoredAxes


def _level_dtypes(axis: pd.Index) -> list[Any]:
    """Return the dtype of each level of ``axis`` (one dtype for a flat ``Index``)."""
    if isinstance(axis, pd.MultiIndex):
        return [level.dtype for level in axis.levels]
    return [axis.dtype]


def _axes_of(frame: pd.DataFrame) -> _StoredAxes:
    """Return the axes of ``frame`` as they are."""
    return _StoredAxes(
        column_labels=list(frame.columns),
        columns_names=list(frame.columns.names),
        column_axis_dtypes=_level_dtypes(frame.columns),
        index_names=list(frame.index.names),
        schema_dtypes=list(frame.dtypes),
    )


def _level_by_position(axis: pd.Index, position: int) -> pd.Index:
    """Return level ``position`` of ``axis``, found by position only (``axis`` for a flat Index).

    ``MultiIndex.get_level_values`` resolves an integer as a level name first, so the levels are
    looked up with every name cleared. The level keeps its own name.
    """
    if not isinstance(axis, pd.MultiIndex):
        return axis
    unnamed = axis.set_names([None] * axis.nlevels)
    return unnamed.get_level_values(position).rename(axis.names[position])


def _parts(
    frame: pd.DataFrame, rows: int | None
) -> Iterator[tuple[str, Callable[[], pd.DataFrame]]]:
    """Yield each part of ``frame`` storage writes, with a function building it alone in a frame.

    The parts are the levels of the column axis, the columns, and the index levels, each by
    position. A part's frame holds the first ``rows`` rows of it (all rows when ``None``); a
    column-axis level is a zero-row frame with that level as its column labels. Building a
    part can itself fail (pandas cannot make every Index a column axis), so it is a function.
    """
    subset = frame if rows is None else frame.iloc[:rows]
    columns = frame.columns
    for position in range(columns.nlevels):
        level = _level_by_position(columns, position)
        where = (
            "column axis" if columns.nlevels == 1 else f"column axis level {position}"
        )
        yield (
            f"{where} (dtype {level.dtype})",
            partial(_column_axis_alone, level),
        )
    for position in range(subset.shape[1]):
        yield (
            f"column {columns[position]!r} (dtype {subset.dtypes.iloc[position]})",
            partial(_column_alone, subset, position),
        )
    index = subset.index
    for position in range(index.nlevels):
        level = _level_by_position(index, position)
        yield (
            f"index level {position} (name {index.names[position]!r}, dtype {level.dtype})",
            partial(pd.DataFrame, index=level),
        )


def _column_axis_alone(level: pd.Index) -> pd.DataFrame:
    """A zero-row frame whose column labels are the distinct labels of ``level``."""
    return pd.DataFrame(index=pd.RangeIndex(0), columns=level.unique())


def _column_alone(frame: pd.DataFrame, position: int) -> pd.DataFrame:
    """Column ``position`` of ``frame`` alone, with a plain column label and row index."""
    column = frame.iloc[:, [position]]
    return column.set_axis(pd.RangeIndex(1), axis=1).reset_index(drop=True)


def _failing_part(
    frame: pd.DataFrame, rows: int | None, attempt: Callable[[pd.DataFrame], object]
) -> str | None:
    """Return the description of the first part of ``frame`` on which ``attempt`` raises.

    Args:
        frame: The frame.
        rows: How many rows of each part to try (all when ``None``); see ``_parts``.
        attempt: The step that failed on the whole frame.

    Returns:
        The description of the part, or ``None`` when every part alone succeeds. A part
        that cannot even be built alone is the failing part.
    """
    for description, build in _parts(frame, rows):
        try:
            attempt(build())
        except Exception:  # noqa: BLE001 - any failure of the step locates the part
            return description
    return None


def _converted(frame: pd.DataFrame) -> pa.Table:
    """``pa.Table.from_pandas(frame, preserve_index=True)``: the table storage writes."""
    return pa.Table.from_pandas(frame, preserve_index=True)


def _read_back(frame: pd.DataFrame) -> pd.DataFrame:
    """The frame storage restores for ``frame`` (a zero-row part, see ``_parts``)."""
    return _converted(frame).to_pandas()


def _with_part(part: str | None, message: str) -> str:
    """``message`` prefixed by the part it is about, when the part is known."""
    return message if part is None else f"{part}: {message}"


def pandas_frame_to_arrow(frame: pd.DataFrame) -> pa.Table:
    """Return the Arrow table storage writes for ``frame``, refusing a frame Arrow cannot hold.

    Storage and the value hash convert a frame with this one function, so they refuse the same
    frames with the same message.

    Args:
        frame: The frame (a Series is converted as its one-column frame).

    Returns:
        ``pa.Table.from_pandas(frame, preserve_index=True)``.

    Raises:
        ValueError: If Arrow cannot convert the frame (mixed or non-Arrow values, a dtype or an
            index kind Arrow does not support). The message names the column, index level or
            column-axis level when converting it alone fails too.
    """
    try:
        return _converted(frame)
    except (pa.ArrowException, NotImplementedError, TypeError, ValueError) as exc:
        part = _failing_part(frame, None, _converted)
        raise ValueError(
            _with_part(
                part,
                "cannot convert to Arrow; check for columns with mixed or "
                f"non-Arrow-serialisable values. Original error: {exc}",
            )
        ) from exc


def _require_levels_in_place(index: pd.Index) -> None:
    """Raise if storage would write the levels of ``index`` in another order.

    pyarrow fetches index level ``i`` with ``index.get_level_values(i)``, and pandas resolves an
    integer there as a level *name* first: with the names ``[1, 0]``, level 0 is fetched as the
    level named 0 (position 1). The value would be written, and read back, with its levels in
    another order, and nothing would raise.

    Raises:
        ValueError: If a level would be written in another level's place; the message names it.
    """
    if not isinstance(index, pd.MultiIndex):
        return
    names = list(index.names)
    for position in range(index.nlevels):
        # pandas' own rule for an integer level argument: a matching name wins (by ==, so 1.0
        # and True match 1), else the position.
        fetched = names.index(position) if position in names else position
        if fetched != position:
            raise ValueError(
                f"index level {position} (name {names[position]!r}): storage would write "
                f"level {fetched} (name {names[fetched]!r}) in its place, because pyarrow "
                f"fetches level {position} by index.get_level_values({position}), which "
                f"pandas resolves as the level named {position}; the value would be read "
                "back with its levels in another order. Rename the index levels (integer "
                "level names that are also level positions are the cause)."
            )


def _require_labels_restored(columns: pd.Index, restored: pd.Index) -> None:
    """Raise if a column label would read back from storage as another label.

    Labels are compared as text, level by level and position by position: storage writes
    labels of mixed type as text (``0`` beside ``"b"`` reads back ``"0"``), and that is no
    change. A label whose text changes is: pyarrow restores a bool level by casting the
    text, so ``False`` reads back ``True``.

    Args:
        columns: The column axis of the frame.
        restored: The column axis of the frame storage restores, with as many levels.

    Raises:
        ValueError: If a label's text changes; the message names the level and the label.
    """
    for position in range(columns.nlevels):
        written = _level_by_position(columns, position)
        read = _level_by_position(restored, position)
        for label, back in zip(written, read):
            if str(label) != str(back):
                where = (
                    "column axis"
                    if columns.nlevels == 1
                    else f"column axis level {position}"
                )
                raise ValueError(
                    f"{where}: the label {label!r} would read back from storage as "
                    f"{back!r}. Rename the columns (pyarrow restores a bool label from "
                    "its text, so False reads back True)."
                )


def _name_as_written(name: Any) -> Any:
    """Return an index level name as storage writes it: its text, a tuple's parts each as text.

    ``None`` stays ``None``.
    """
    if name is None:
        return None
    if isinstance(name, tuple):
        return str(tuple(_name_as_written(part) for part in name))  # pyright: ignore[reportUnknownVariableType]
    return str(name)


def _require_index_names_restored(index: pd.Index, restored: pd.Index) -> None:
    """Raise if an index level name would read back from storage as another name.

    Storage writes a level name as text, so ``1`` reads back ``"1"``: that is no change. A
    name whose text changes is: pyarrow drops a name that looks like its own generated field
    name (``"__index_level_0__"``), and the level reads back unnamed.

    Args:
        index: The index of the frame.
        restored: The index of the frame storage restores, with as many levels.

    Raises:
        ValueError: If a level's name changes; the message names the level and both names.
    """
    for position, (name, back) in enumerate(zip(index.names, restored.names)):
        if _name_as_written(name) != back:
            raise ValueError(
                f"index level {position} (name {name!r}): the name would read back from "
                f"storage as {back!r}. Rename the index level (pyarrow drops a name of the "
                "form '__index_level_<n>__')."
            )


def _require_index_values_restored(table: pa.Table, frame: pd.DataFrame) -> None:
    """Raise if the index cannot be rebuilt from its stored values.

    Storage converts each index level from its Arrow column and, for a MultiIndex, assembles
    the levels with ``pd.MultiIndex.from_arrays``. Both steps can fail on the values alone,
    which a zero-row read never meets: a ``pd.ArrowDtype`` ``time64[ns]`` level holding a
    value that is not a whole number of microseconds cannot be converted, and a level holding
    tuples or lists reads back as arrays, which cannot be the values of a MultiIndex level.
    So each index level is converted once, in full, and a MultiIndex is assembled once.

    Args:
        table: ``pandas_frame_to_arrow(frame)``: the columns, then one field per index level.
        frame: The frame.

    Raises:
        ValueError: If an index level cannot be converted or assembled; the message names it.
    """

    def refuse(position: int, exc: Exception) -> ValueError:
        return ValueError(
            f"index level {position} (name {frame.index.names[position]!r}, dtype "
            f"{_level_by_position(frame.index, position).dtype}): storage would write "
            f"this value but it cannot be read back ({type(exc).__name__}: {exc})."
        )

    width = frame.shape[1]
    levels: list[Any] = []
    for position in range(frame.index.nlevels):
        try:
            levels.append(table.column(width + position).to_pandas())
        except Exception as exc:  # noqa: BLE001 - pandas and pyarrow raise many kinds here
            raise refuse(position, exc) from exc
    if len(levels) < 2:
        return
    try:
        pd.MultiIndex.from_arrays(levels)
    except Exception as exc:  # noqa: BLE001 - pandas raises many kinds here
        for position, level in enumerate(levels):
            try:
                pd.MultiIndex.from_arrays([level])
            except Exception as level_exc:  # noqa: BLE001 - locates the level
                raise refuse(position, level_exc) from exc
        raise ValueError(
            "the index: storage would write this value but its MultiIndex cannot be "
            f"rebuilt ({type(exc).__name__}: {exc})."
        ) from exc


def require_restorable(table: pa.Table, frame: pd.DataFrame) -> pd.DataFrame:
    """Return the zero-row frame storage restores for ``frame``, refusing what it would not.

    The one check, shared by storage (``python_to_storage``) and the value hash, of a value
    storage would write but not restore. Such a value is refused, before anything is written:

    - Two fields get one name (the index name ``1`` beside the column ``"1"``; integer names of
      MultiIndex levels): the frame read back would have lost its index.
    - The levels of a MultiIndex would be written in another order (level names that are also
      level positions, ``names=[1, 0]``): the frame read back would differ, silently.
    - The frame read back cannot be built: a nested, dictionary or fixed-size ``pd.ArrowDtype``
      column or index level; a categorical, period, timedelta, interval, date or
      ``pd.ArrowDtype`` column axis; a categorical whose categories cannot be restored
      (tuples read back as arrays, which cannot be categories).
    - An index level whose values cannot be converted back (a ``pd.ArrowDtype`` ``time64[ns]``
      level holding sub-microsecond values) or cannot be a MultiIndex level (tuples or lists,
      which read back as arrays).
    - The frame read back would have another number of columns or index levels, distinct
      column labels that read back equal, a column label that reads back as another label
      (``False`` reads back ``True``), or an index level name that reads back as another
      name (``"__index_level_0__"`` reads back unnamed).

    What storage restores is read from ``table.slice(0, 0)``, which keeps the schema and the
    dictionary (the categories) of each categorical column; the only rows converted are the
    index levels', once each.

    Args:
        table: ``pandas_frame_to_arrow(frame)``.
        frame: The frame about to be stored or hashed.

    Returns:
        The zero-row frame storage restores for ``table``: its axes, labels, names, the
        categories and the dtypes the schema decides.

    Raises:
        ValueError: If storage would not restore the frame; the message names the column,
            index level or column-axis level and the reason.
    """
    counts = Counter(table.schema.names)
    repeated = sorted(name for name, count in counts.items() if count > 1)
    if repeated:
        raise ValueError(
            f"column labels and index names collide once written as text: {repeated}. "
            "Storage could not restore the index. Rename the index levels (integer "
            "level names are a common cause) or the columns."
        )
    _require_levels_in_place(frame.index)
    try:
        restored = table.slice(0, 0).to_pandas()
    except Exception as exc:  # noqa: BLE001 - pandas and pyarrow raise many kinds here
        part = _failing_part(frame, 0, _read_back)
        raise ValueError(
            _with_part(
                part,
                "storage would write this value but it cannot be read back "
                f"({type(exc).__name__}: {exc}).",
            )
        ) from exc
    if (
        restored.shape[1] != frame.shape[1]
        or restored.index.nlevels != frame.index.nlevels
    ):
        raise ValueError(
            "the value read back from storage would have "
            f"{restored.shape[1]} column(s) and {restored.index.nlevels} index "
            f"level(s), not {frame.shape[1]} and {frame.index.nlevels}. Rename the "
            "columns or the index levels."
        )
    if frame.columns.is_unique and not restored.columns.is_unique:
        labels = sorted({repr(label) for label in restored.columns})
        raise ValueError(
            "distinct column labels would read back from storage as duplicates: "
            f"{labels}. Rename the columns (labels of mixed or bool type are written "
            "as text)."
        )
    _require_labels_restored(frame.columns, restored.columns)
    _require_index_names_restored(frame.index, restored.index)
    _require_index_values_restored(table, frame)
    return restored


def _restored_axes(table: pa.Table, df: pd.DataFrame) -> _StoredAxes:
    """Return the axes storage restores for a frame whose Arrow table is ``table``.

    They are read from the zero-row frame storage builds for the table (no rows are
    converted), so the rule is storage's own.

    Args:
        table: ``pandas_frame_to_arrow(df)``, or the same for a zero-row ``df``.
        df: The frame.

    Returns:
        The axes of the frame once read back from storage.

    Raises:
        ValueError: If storage would not restore the frame (``require_restorable``).
    """
    return _axes_of(require_restorable(table, df))


def _values_table(df: pd.DataFrame) -> tuple[pa.Table, _StoredForm]:
    """Return the frame's values as an Arrow table, with the form storage restores.

    The table has positional field names: ``c0 ..`` for the columns in order, then
    ``i0 ..`` for the index levels in order.

    Returns:
        The values table, and the dtypes, labels and names the frame has once read back
        from storage.

    Raises:
        ValueError: If a column's dtype is outside the grammar (the error names the column),
            the frame cannot be converted to Arrow, storage would not restore it
            (``require_restorable``), or a label or name is outside the grammar.
    """
    # Storage's refusals come first, with storage's own message, so the hash and
    # ``python_to_storage`` refuse a value alike (ITL-1054); the grammar checks follow.
    table = pandas_frame_to_arrow(df)
    axes = _restored_axes(table, df)
    for label, dtype in zip(df.columns, df.dtypes):
        if dtype != np.dtype(object):
            try:
                pandas_dtype_descriptor(dtype)
            except ValueError as exc:
                raise ValueError(f"column {label!r}: {exc}") from exc
    # The frame's own labels and names must be in the grammar, although the descriptor
    # holds them in the form storage restores: storage writes some as text, and the text of
    # an arbitrary object (its ``repr``, with a memory address) must never reach a hash.
    for label in (*df.columns, *df.columns.names, *df.index.names):
        pandas_label_descriptor(label)
    width = df.shape[1]
    index = df.index
    dtypes = [
        *df.dtypes,
        *(index.get_level_values(level).dtype for level in range(index.nlevels)),
    ]
    names = [f"c{position}" for position in range(width)] + [
        f"i{position}" for position in range(index.nlevels)
    ]
    for position, schema_dtype in enumerate(axes.schema_dtypes):
        own = dtypes[position]
        if own != np.dtype(object) and not isinstance(own, pd.CategoricalDtype):
            # The schema decides what such a column reads back as: a ``pd.ArrowDtype``
            # string column reads back ``string``, an Arrow decimal column ``object``.
            dtypes[position] = schema_dtype
    restored = [
        _restored_form(dtype, table.column(position), index_level=position >= width)
        for position, dtype in enumerate(dtypes)
    ]
    values = pa.table([column for _, column in restored], names=names)
    restored_dtypes = [dtype for dtype, _ in restored]
    return values, _StoredForm(
        column_dtypes=restored_dtypes[:width],
        index_dtypes=restored_dtypes[width:],
        axes=axes,
    )


def _axis_dtype_descriptor(dtype: Any) -> Any:
    """Return the dtype term of one level of the column axis.

    A ``pd.ArrowDtype`` is described with its Arrow type: storage restores it, and unlike
    a column's Arrow type it is not in the values digest (labels are not digested).
    """
    if isinstance(dtype, pd.ArrowDtype):
        return {"arrow": str(dtype.pyarrow_dtype)}
    return pandas_dtype_descriptor(dtype)


def _index_descriptor(stored: _StoredForm) -> list[Any]:
    """Return the ``index`` term: ``[<name>, <dtype>]`` per level, as storage restores it."""
    return [
        [pandas_label_descriptor(name), pandas_dtype_descriptor(dtype)]
        for name, dtype in zip(stored.axes.index_names, stored.index_dtypes)
    ]


def _frame_descriptor(
    df: pd.DataFrame, stored: _StoredForm, ignore: frozenset[str]
) -> dict[str, Any]:
    """Return the descriptor of a frame.

    Args:
        df: The frame, read for its row count and ``attrs``.
        stored: The dtypes, labels and names of the frame as storage restores them.
        ignore: The property names the hash ignores.

    Returns:
        The descriptor: every term describes the frame in the form it reads back in.
    """
    axes = stored.axes
    return {
        "kind": "orcapod.pandas.dataframe",
        "height": len(df),
        "attrs": {} if "attrs" in ignore else pandas_attrs_as_stored(df.attrs),
        "columns": [
            _column_descriptor(label, dtype)
            for label, dtype in zip(axes.column_labels, stored.column_dtypes)
        ],
        "columns_names": [pandas_label_descriptor(name) for name in axes.columns_names],
        "columns_dtypes": [
            _axis_dtype_descriptor(dtype) for dtype in axes.column_axis_dtypes
        ],
        "index": _index_descriptor(stored),
    }


def _series_descriptor(
    series: pd.Series, stored: _StoredForm, ignore: frozenset[str]
) -> dict[str, Any]:
    """Return the descriptor of a Series.

    Args:
        series: The Series, read for its row count, ``attrs`` and whether it has a name.
        stored: The dtypes, labels and names of the Series' one-column frame as storage
            restores them; the column's label is the Series' name.
        ignore: The property names the hash ignores.

    Returns:
        The descriptor: every term describes the Series in the form it reads back in.
    """
    named = series.name is not None and "series_name" not in ignore
    return {
        "kind": "orcapod.pandas.series",
        "height": len(series),
        "attrs": {} if "attrs" in ignore else pandas_attrs_as_stored(series.attrs),
        "name": (
            pandas_label_descriptor(stored.axes.column_labels[0]) if named else None
        ),
        "dtype": pandas_dtype_descriptor(stored.column_dtypes[0]),
        "index": _index_descriptor(stored),
    }


def _without_attrs(df: pd.DataFrame) -> pd.DataFrame:
    """Return a frame over the same data as ``df`` with empty ``attrs``; nothing is copied.

    ``attrs`` are described on their own and are not needed in the Arrow table. They must
    not travel with the frame the hash works on: pandas deep-copies ``attrs`` in almost
    every operation (``attrs`` holding a lock cannot be copied at all), and what pyarrow
    does with ``attrs`` it cannot write depends on its version. ``df`` is left as it is.
    """
    return pd.DataFrame(df, copy=False)


def _series_as_stored_frame(series: pd.Series) -> pd.DataFrame:
    """Return the one-column frame storage writes for a Series, with empty ``attrs``."""
    name = PANDAS_UNNAMED_SERIES_COLUMN if series.name is None else series.name
    return pd.Series(series, copy=False).to_frame(name=name)


def _in_canonical_column_order(df: pd.DataFrame) -> pd.DataFrame:
    """Return ``df`` with its columns sorted by the canonical JSON of their labels.

    This is the fixed column order a frame is hashed in when column order is ignored.
    The sort is on the terms of the labels as storage restores them, so a frame and the
    frame read back from storage are sorted alike.
    """
    # Only the labels are needed: a zero-row frame has them, and its conversion is cheap.
    empty = df.iloc[:0]
    labels = _restored_axes(pandas_frame_to_arrow(empty), empty).column_labels
    keys = [canonical_json(pandas_label_descriptor(label)) for label in labels]
    order = sorted(range(len(keys)), key=keys.__getitem__)
    return df.iloc[:, order]


def pandas_frame_descriptor(
    df: pd.DataFrame, ignore: frozenset[str] = _NOTHING_IGNORED
) -> dict[str, Any]:
    """Return the descriptor of a DataFrame (see the module docstring).

    Args:
        df: The DataFrame.
        ignore: The property names the hash ignores; a subset of
            ``PANDAS_DATAFRAME_OPTIONS``.

    Returns:
        The descriptor as a JSON-serialisable dict.

    Raises:
        ValueError: If a label or dtype is outside the grammar (a dtype error names its
            column), or a column cannot be converted to Arrow.
    """
    bare = _without_attrs(df)
    if "column_order" in ignore:
        bare = _in_canonical_column_order(bare)
    _, stored = _values_table(bare)
    return _frame_descriptor(df, stored, ignore)


def pandas_series_descriptor(
    series: pd.Series, ignore: frozenset[str] = _NOTHING_IGNORED
) -> dict[str, Any]:
    """Return the descriptor of a Series (see the module docstring).

    Args:
        series: The Series.
        ignore: The property names the hash ignores; a subset of
            ``PANDAS_SERIES_OPTIONS``.

    Returns:
        The descriptor as a JSON-serialisable dict.

    Raises:
        ValueError: If a label or the dtype is outside the grammar, or the values cannot be
            converted to Arrow.
    """
    _, stored = _values_table(_series_as_stored_frame(series))
    return _series_descriptor(series, stored, ignore)


def pandas_frame_content_hash(
    df: pd.DataFrame,
    arrow_hasher: ArrowHasherProtocol,
    ignore: frozenset[str] = _NOTHING_IGNORED,
) -> ContentHash:
    """Return the ``pandas_dataframe_v1`` content hash of a DataFrame.

    Args:
        df: The DataFrame.
        arrow_hasher: The data context's Arrow hasher, whose ``hash_table_ordered`` digests
            the values table.
        ignore: The property names the hash ignores; a subset of
            ``PANDAS_DATAFRAME_OPTIONS``. Each ignored property is hashed with a fixed
            value in place of the frame's own, so the preimage keeps one shape.

    Returns:
        A ``ContentHash`` with method ``"pandas_dataframe_v1"``.

    Raises:
        ValueError: If a label or dtype is outside the grammar, or a column cannot be converted
            to Arrow.
    """
    bare = _without_attrs(df)
    if "column_order" in ignore:
        bare = _in_canonical_column_order(bare)
    values, stored = _values_table(bare)
    descriptor = _frame_descriptor(df, stored, ignore)
    digest = arrow_hasher.hash_table_ordered(values).digest
    return labelled_content_hash(PANDAS_DATAFRAME_HASH_LABEL, descriptor, digest)


def pandas_series_content_hash(
    series: pd.Series,
    arrow_hasher: ArrowHasherProtocol,
    ignore: frozenset[str] = _NOTHING_IGNORED,
) -> ContentHash:
    """Return the ``pandas_series_v1`` content hash of a Series.

    Args:
        series: The Series.
        arrow_hasher: The data context's Arrow hasher, whose ``hash_table_ordered`` digests
            the values table.
        ignore: The property names the hash ignores; a subset of
            ``PANDAS_SERIES_OPTIONS``. Each ignored property is hashed with a fixed value
            in place of the Series' own, so the preimage keeps one shape.

    Returns:
        A ``ContentHash`` with method ``"pandas_series_v1"``.

    Raises:
        ValueError: If a label or the dtype is outside the grammar, or the values cannot be
            converted to Arrow.
    """
    values, stored = _values_table(_series_as_stored_frame(series))
    descriptor = _series_descriptor(series, stored, ignore)
    digest = arrow_hasher.hash_table_ordered(values).digest
    return labelled_content_hash(PANDAS_SERIES_HASH_LABEL, descriptor, digest)
