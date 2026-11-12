"""Claim 1: pyarrow's Python-to-Arrow conversion itself truncates float -> integer.

Pure pyarrow (plus numpy / stdlib values); no orcapod import. Shows:

1. The minimal reproduction of the exact orcapod call:
   ``pa.Table.from_pylist([{"y": 1.5}], schema=pa.schema([("y", pa.int64())]))``.
2. A matrix of Python value x declared Arrow type through the same call, so the
   asymmetry (float -> int truncates, the other mismatches raise) is visible.
3. The same call through ``pa.array(values, type=...)``, and the explicit safe cast
   (``pc.cast``), which does refuse 1.5 -> int64.

Why (from pyarrow's own C++ source, shipped in the wheel under
``pyarrow/src/arrow/python``):

* ``python_to_arrow.cc`` ``PyValue::Convert(IntegerType)`` calls ``CIntFromPython``.
* ``helpers.cc`` ``CIntFromPython`` rejects ``bool`` explicitly, then
  ``CIntFromPythonImpl``: if the object is not a ``PyLong`` it calls ``PyObjectToPyInt``,
  which tries ``__index__`` and, when that fails, FALLS BACK TO ``__int__``
  (``nb_int``). ``float.__int__`` truncates toward zero, so 1.5 -> 1, -1.5 -> -1.
  Range is checked afterwards (overflow raises). Anything with ``__int__`` is accepted
  the same way: ``Decimal``, ``Fraction``, ``numpy.float64``.
* ``Convert(DoubleType)`` accepts float, numpy float, and ints only through
  ``IntegerScalarToDoubleSafe`` (|v| <= 2**53, else raises).
* ``Convert(FloatType)`` (float32) does ``static_cast<float>(double)``: rounds silently.
* ``Convert(BooleanType)`` accepts only ``True``/``False``/``numpy.bool_``.
* String/binary converters reject non-str/bytes.
* Timestamp/date/time/duration converters fall through to ``CIntFromPython`` for
  non-datetime objects, so a float there truncates too (1.5 -> 1 tick).

No conversion option changes this: ``pa.array``/``from_pylist`` take no ``safe=`` for
Python sequences (``safe`` only applies to pandas/numpy inputs), and ``from_pandas``
does not change the integer converter.

Run: uv run python superpowers/specs/itl-895-float-to-int/01_pyarrow_minimal.py
     uv run --no-project --with pyarrow==20.0.0 --with numpy \
        python superpowers/specs/itl-895-float-to-int/01_pyarrow_minimal.py
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from fractions import Fraction

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import warnings

warnings.simplefilter("ignore", DeprecationWarning)

print(f"pyarrow {pa.__version__}, numpy {np.__version__}\n")

print("1. minimal reproduction of the orcapod call")
t = pa.Table.from_pylist([{"y": 1.5}], schema=pa.schema([("y", pa.int64())]))
print(
    f"   pa.Table.from_pylist([{{'y': 1.5}}], schema=int64) -> {t['y'].to_pylist()}\n"
)


def through(value, arrow_type) -> str:
    try:
        tbl = pa.Table.from_pylist(
            [{"v": value}], schema=pa.schema([("v", arrow_type)])
        )
        out = tbl["v"].to_pylist()[0]
        if (pa.types.is_temporal(arrow_type)) and not isinstance(
            value, (dt.date, dt.timedelta)
        ):
            tag = "REINTERPRETED"  # a number taken as ticks/days since the epoch
        elif out == value and type(out) is type(value):
            tag = "kept"
        elif out == value:
            tag = "kept, type changed"
        else:
            tag = "LOSSY"
        return f"{tag:18} -> {out!r}"
    except Exception as e:  # noqa: BLE001 - we want every error class
        return f"{'raises':18}    {type(e).__name__}: {str(e)[:70]}"


CASES = [
    # (label, value, arrow type)
    ("float 1.5        -> int64", 1.5, pa.int64()),
    ("float 2.9        -> int64", 2.9, pa.int64()),
    ("float -1.5       -> int64", -1.5, pa.int64()),
    ("float 1.5        -> int8", 1.5, pa.int8()),
    ("float 1.5        -> uint32", 1.5, pa.uint32()),
    ("float 2.0        -> int64", 2.0, pa.int64()),
    ("float nan        -> int64", float("nan"), pa.int64()),
    ("float inf        -> int64", float("inf"), pa.int64()),
    ("float 1e30       -> int64", 1e30, pa.int64()),
    ("np.float64(1.5)  -> int64", np.float64(1.5), pa.int64()),
    ("np.float32(1.5)  -> int64", np.float32(1.5), pa.int64()),
    ("Decimal('1.5')   -> int64", Decimal("1.5"), pa.int64()),
    ("Fraction(3, 2)   -> int64", Fraction(3, 2), pa.int64()),
    ("np.int64(3)      -> int64", np.int64(3), pa.int64()),
    ("np.int64(3)      -> int8", np.int64(3), pa.int8()),
    ("True             -> int64", True, pa.int64()),
    ("np.bool_(True)   -> int64", np.bool_(True), pa.int64()),
    ("'1'              -> int64", "1", pa.int64()),
    ("300              -> int8", 300, pa.int8()),
    ("-1               -> uint8", -1, pa.uint8()),
    ("1                -> bool", 1, pa.bool_()),
    ("1.0              -> bool", 1.0, pa.bool_()),
    ("np.bool_(True)   -> bool", np.bool_(True), pa.bool_()),
    ("1                -> float64", 1, pa.float64()),
    ("2**53+1          -> float64", 2**53 + 1, pa.float64()),
    ("True             -> float64", True, pa.float64()),
    ("Decimal('1.5')   -> float64", Decimal("1.5"), pa.float64()),
    ("np.int64(3)      -> float64", np.int64(3), pa.float64()),
    ("np.float32(0.1)  -> float64", np.float32(0.1), pa.float64()),
    ("0.1              -> float32", 0.1, pa.float32()),
    ("2**24+1          -> float32", 2**24 + 1, pa.float32()),
    ("1                -> large_string", 1, pa.large_string()),
    ("b'x'             -> large_string", b"x", pa.large_string()),
    ("'x'              -> large_binary", "x", pa.large_binary()),
    ("1.5              -> timestamp[us]", 1.5, pa.timestamp("us")),
    ("1                -> timestamp[us]", 1, pa.timestamp("us")),
    ("1.5              -> date32", 1.5, pa.date32()),
    ("1.5              -> duration[us]", 1.5, pa.duration("us")),
    ("date(2020,1,1)   -> timestamp[us]", dt.date(2020, 1, 1), pa.timestamp("us")),
    (
        "datetime naive   -> timestamp[us,UTC]",
        dt.datetime(2020, 1, 1, 12),
        pa.timestamp("us", "UTC"),
    ),
    (
        "datetime aware   -> timestamp[us]",
        dt.datetime(2020, 1, 1, 12, tzinfo=dt.timezone(dt.timedelta(hours=2))),
        pa.timestamp("us"),
    ),
    ("datetime         -> date32", dt.datetime(2020, 1, 1, 12), pa.date32()),
    ("1.5              -> decimal128(5,1)", 1.5, pa.decimal128(5, 1)),
    ("Decimal('1.25')  -> decimal128(5,1)", Decimal("1.25"), pa.decimal128(5, 1)),
    ("1                -> decimal128(5,1)", 1, pa.decimal128(5, 1)),
    ("[1.5]            -> large_list<int64>", [1.5], pa.large_list(pa.int64())),
    (
        "{'a': 1.5}       -> struct<a: int64>",
        {"a": 1.5},
        pa.struct([("a", pa.int64())]),
    ),
]

print("2. Table.from_pylist(rows, schema=...) for each (value, declared Arrow type)")
for label, value, typ in CASES:
    print(f"   {label:40} {through(value, typ)}")

print("\n3. other entry points")
print(
    f"   pa.array([1.5], type=int64)            -> {pa.array([1.5], type=pa.int64()).to_pylist()}"
)
print(
    f"   pa.scalar(1.5, type=int64)             -> {pa.scalar(1.5, type=pa.int64()).as_py()}"
)
try:
    pc.cast(pa.array([1.5]), pa.int64())
except pa.ArrowInvalid as e:
    print(f"   pc.cast(pa.array([1.5]), int64) [safe] -> raises ArrowInvalid: {e}")
print(
    f"   pc.cast(..., safe=False)               -> {pc.cast(pa.array([1.5]), pa.int64(), safe=False).to_pylist()}"
)
print(f"   int(1.5) (what nb_int does)            -> {int(1.5)}")
