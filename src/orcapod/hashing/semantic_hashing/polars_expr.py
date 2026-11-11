"""Identity of a Polars expression: the ``polars_expr_v1`` canonical encoding.

Polars' own serialization of an expression (``Expr.meta.serialize``) is not stable across
Polars versions, and ``str(expr)`` is lossy, so neither is hashed. Instead the JSON
serialization, the only public structured view of the expression tree, is parsed by a strict
parser into a tree in a grammar Orcapod owns, and that tree is hashed:

```text
ContentHash("polars_expr_v1", sha256(canonical JSON of the tree))
canonical JSON = json.dumps(tree, sort_keys=True, separators=(",", ":"),
                            ensure_ascii=False).encode("utf-8")

node    := {"col": <name>}
         | {"lit": <literal>}
         | {"binary": [<op>, <node>, <node>]}
         | {"fn": [<fname>, <options>, [<node>, ...]]}
         | {"cast": [<dtype>, "strict" | "non_strict", <node>]}
op      := "eq" | "ne" | "eq_missing" | "ne_missing" | "lt" | "le" | "gt" | "ge"
         | "and" | "or" | "xor" | "add" | "sub" | "mul" | "truediv" | "floordiv" | "mod"
fname   := "not" | "is_null" | "is_not_null" | "is_nan" | "is_not_nan" | "negate"
         | "all_horizontal" | "any_horizontal"                    options {}
         | "is_in"            options {"nulls_equal": <bool>}
         | "is_between"       options {"closed": "both" | "left" | "right" | "none"}
         | "str.contains"     options {"literal": <bool>, "strict": <bool>}
         | "str.starts_with" | "str.ends_with"                    options {}
literal := {"dyn_int": "<decimal>"}          a Python int (Polars adapts its type)
         | {"dyn_float": "<float.hex()>"}    a finite Python float
         | {"null": null} | {"boolean": <bool>} | {"string": <str>}
         | {"int8" | ... | "uint64": "<decimal>"}
         | {"float32" | "float64": "<float.hex()>"}   finite; float32 rounded to 32 bits
         | {"decimal": ["<unscaled decimal>", <precision>, <scale>]}
         | {"binary": "<lowercase hex>"}
         | {"date": <days since epoch>} | {"time": <ns since midnight>}
         | {"datetime": [<value>, <unit>, <time zone or null>]}
         | {"duration": [<value>, <unit>]}
         | {"list": [<elem>, [<value or null>, ...]]}    a Python list, as in is_in([...])
unit    := "ns" | "us" | "ms"
dtype   := "int8" | ... | "uint64" | "float32" | "float64" | "boolean" | "string" | "binary"
         | "date" | "time" | "null" | {"decimal": [<precision>, <scale>]}
         | {"datetime": [<unit>, <time zone or null>]} | {"duration": <unit>}
         | {"list": <dtype>}
elem    := "int8" | ... | "uint64" | "float32" | "float64" | "boolean" | "string" | "date"
         | "time" | "null" | {"datetime": [<unit>, <time zone or null>]} | {"duration": <unit>}
```

A list value is the payload of the element literal: a decimal string for an integer, a
``float.hex()`` string for a float, the string or bool itself, the integer physical value for a
temporal element, ``None`` for a null. Lists of decimals or bytes are not supported. The
dtype names are those of the Polars dtype descriptor grammar (``polars_dataframe_v1``).

The parser is strict: a node, key or value shape it does not know raises ``ValueError`` naming
the expression, so a Polars release that changes its serialization fails loudly instead of
moving a hash. The fix is to teach the parser the new layout and map it onto the same tree.
Expressions outside the grammar (Python functions, multi-column selectors, aggregations,
``alias``, ``when/then``, non-finite float literals, ``pl.Series`` literals, ...) are rejected
the same way. Supporting a new construct later adds to the grammar without changing the bytes
of any expression already supported; changing the encoding of a supported construct needs a
new label (``polars_expr_v2``).
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import struct
from typing import TYPE_CHECKING, Any

from orcapod.types import ContentHash
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
else:
    pl = LazyModule("polars")

POLARS_EXPR_HASH_LABEL = "polars_expr_v1"

_SUPPORTED_TEXT = (
    "Orcapod can hash column references, literals (numbers, strings, booleans, null, "
    "dates, times, datetimes, durations, decimals, bytes, and Python lists of integers, "
    "floats, strings, booleans, dates, times, datetimes or durations), "
    "comparisons, arithmetic, &, |, ^, ~, is_null, is_not_null, is_nan, is_not_nan, "
    "is_in, is_between, all_horizontal, any_horizontal, str.contains, str.starts_with, "
    "str.ends_with and cast."
)
_ADVICE_TEXT = (
    "Use keyword constraints (polars_filter(column=value)) for equality filters, or "
    "compute a boolean column in a function pod and filter on it."
)

_BINARY_OPS: dict[str, str] = {
    "Eq": "eq",
    "NotEq": "ne",
    "EqValidity": "eq_missing",
    "NotEqValidity": "ne_missing",
    "Lt": "lt",
    "LtEq": "le",
    "Gt": "gt",
    "GtEq": "ge",
    "And": "and",
    "Or": "or",
    "Xor": "xor",
    "Plus": "add",
    "Minus": "sub",
    "Multiply": "mul",
    "TrueDivide": "truediv",
    "FloorDivide": "floordiv",
    "Modulus": "mod",
}

_BOOLEAN_FUNCTIONS: dict[str, str] = {
    "Not": "not",
    "IsNull": "is_null",
    "IsNotNull": "is_not_null",
    "IsNan": "is_nan",
    "IsNotNan": "is_not_nan",
    "AllHorizontal": "all_horizontal",
    "AnyHorizontal": "any_horizontal",
}

_STRING_FUNCTIONS: dict[str, str] = {
    "StartsWith": "str.starts_with",
    "EndsWith": "str.ends_with",
}

_CLOSED: dict[str, str] = {
    "Both": "both",
    "Left": "left",
    "Right": "right",
    "None": "none",
}

_CAST_OPTIONS: dict[str, str] = {"Strict": "strict", "NonStrict": "non_strict"}

_TIME_UNITS: dict[str, str] = {
    "Nanoseconds": "ns",
    "Microseconds": "us",
    "Milliseconds": "ms",
}

_INTEGER_DTYPES: dict[str, str] = {
    "Int8": "int8",
    "Int16": "int16",
    "Int32": "int32",
    "Int64": "int64",
    "UInt8": "uint8",
    "UInt16": "uint16",
    "UInt32": "uint32",
    "UInt64": "uint64",
}

_PLAIN_DTYPES: dict[str, str] = {
    **_INTEGER_DTYPES,
    "Float32": "float32",
    "Float64": "float64",
    "Boolean": "boolean",
    "String": "string",
    "Binary": "binary",
    "Date": "date",
    "Time": "time",
    "Null": "null",
}

# The column-selection nodes Polars uses for an expression over a schema-dependent set of
# columns (pl.col("x", "y"), pl.all(), regex columns, pl.exclude, selectors, dtype columns).
_MULTI_COLUMN_NODES = frozenset(
    {"Selector", "Columns", "Wildcard", "Exclude", "DtypeColumn", "IndexColumn", "Nth"}
)
_PYTHON_FUNCTION_NODES = frozenset({"AnonymousFunction", "PythonUdf", "MapBatches"})


class _Unsupported(Exception):
    """Internal: the expression (or the serialization layout) is outside the grammar."""


def _fail(reason: str) -> _Unsupported:
    """Build the internal exception for a construct outside the grammar."""
    return _Unsupported(reason)


def _single_key(obj: Any) -> tuple[str, Any]:
    """Split a one-key JSON object (or a bare string tag) into its key and value."""
    if isinstance(obj, str):
        return obj, None
    if not isinstance(obj, dict) or len(obj) != 1:
        raise _fail(f"unexpected serialization layout {_short(obj)}")
    ((key, value),) = obj.items()
    return key, value


def _short(obj: Any) -> str:
    """Render a serialization fragment as compact JSON, truncated for messages."""
    text = json.dumps(obj, sort_keys=True)
    return text if len(text) <= 120 else text[:117] + "..."


def _require_keys(obj: Any, keys: set[str]) -> dict[str, Any]:
    """Return ``obj`` if it is a JSON object with exactly ``keys``."""
    if not isinstance(obj, dict) or set(obj) != keys:
        raise _fail(f"unexpected serialization layout {_short(obj)}")
    return obj


def _require_int(value: Any) -> int:
    """Return ``value`` if it is an integer (not a bool)."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise _fail(f"expected an integer, got {_short(value)}")
    return value


def _require_bool(value: Any) -> bool:
    """Return ``value`` if it is a bool."""
    if not isinstance(value, bool):
        raise _fail(f"expected a boolean, got {_short(value)}")
    return value


def _float_hex(value: Any, *, float32: bool = False) -> str:
    """Encode a finite float as ``float.hex()``, rounded to 32 bits when ``float32``."""
    if value is None:
        # Polars' JSON writes NaN, inf and -inf all as null, so they cannot be told apart.
        raise _fail("non-finite float literals (NaN, inf, -inf) are not supported")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _fail(f"unexpected float value {_short(value)}")
    result = float(value)
    if float32:
        result = struct.unpack("<f", struct.pack("<f", result))[0]
    if not math.isfinite(result):
        raise _fail("non-finite float literals (NaN, inf, -inf) are not supported")
    return result.hex()


def _time_unit(value: Any) -> str:
    """Map a Polars time unit name to ``ns``, ``us`` or ``ms``."""
    if not isinstance(value, str) or value not in _TIME_UNITS:
        raise _fail(f"unexpected time unit {_short(value)}")
    return _TIME_UNITS[value]


def _time_zone(value: Any) -> str | None:
    """Return the time zone name of a serialized datetime dtype, or ``None``."""
    if value is None:
        return None
    zone = _require_keys(value, {"inner"})["inner"]
    if not isinstance(zone, str):
        raise _fail(f"unexpected time zone {_short(value)}")
    return zone


def _dtype(obj: Any) -> Any:
    """Encode a dtype from Polars' JSON layout (without the ``Literal`` wrapper)."""
    key, value = _single_key(obj)
    if value is None and key in _PLAIN_DTYPES:
        return _PLAIN_DTYPES[key]
    if key == "Decimal":
        if not isinstance(value, list) or len(value) != 2:
            raise _fail(f"unexpected Decimal dtype {_short(value)}")
        return {
            "decimal": [
                _require_int(value[0]),
                _require_int(value[1]),
            ]
        }
    if key == "Datetime":
        if not isinstance(value, list) or len(value) != 2:
            raise _fail(f"unexpected Datetime dtype {_short(value)}")
        return {"datetime": [_time_unit(value[0]), _time_zone(value[1])]}
    if key == "Duration":
        return {"duration": _time_unit(value)}
    if key == "List":
        return {"list": _dtype(value)}
    raise _fail(f"the dtype {key} is not supported")


def _polars_dtype(dtype: Any) -> Any:
    """Encode a ``pl.DataType`` (a list literal's element type) in the dtype grammar."""
    plain = {
        pl.Int8: "int8",
        pl.Int16: "int16",
        pl.Int32: "int32",
        pl.Int64: "int64",
        pl.UInt8: "uint8",
        pl.UInt16: "uint16",
        pl.UInt32: "uint32",
        pl.UInt64: "uint64",
        pl.Float32: "float32",
        pl.Float64: "float64",
        pl.Boolean: "boolean",
        pl.String: "string",
        pl.Date: "date",
        pl.Time: "time",
        pl.Null: "null",
    }
    if isinstance(dtype, pl.Datetime):
        return {"datetime": [dtype.time_unit, dtype.time_zone]}
    if isinstance(dtype, pl.Duration):
        return {"duration": dtype.time_unit}
    name = plain.get(type(dtype))
    if name is None:
        raise _fail(f"list literals of dtype {dtype} are not supported")
    return name


def _list_literal(value: Any) -> Any:
    """Encode a Python-list literal (Arrow IPC bytes in the serialization) as a list literal."""
    if not isinstance(value, list) or not all(
        isinstance(b, int) and not isinstance(b, bool) and 0 <= b < 256 for b in value
    ):
        raise _fail(f"unexpected list literal layout {_short(value)}")
    frame = pl.read_ipc_stream(io.BytesIO(bytes(value)))
    if frame.width != 1:
        raise _fail("unexpected list literal layout (not one column)")
    series = frame.to_series()
    dtype = _polars_dtype(series.dtype)
    if series.dtype.is_float():
        payload = [
            None if v is None else _float_hex(v, float32=series.dtype == pl.Float32)
            for v in series.to_list()
        ]
    elif series.dtype.is_integer():
        payload = [None if v is None else str(v) for v in series.to_list()]
    elif series.dtype.is_temporal():
        payload = series.to_physical().to_list()
    else:
        payload = series.to_list()
    return {"list": [dtype, payload]}


def _scalar_literal(obj: Any) -> Any:
    """Encode a typed scalar literal."""
    key, value = _single_key(obj)
    if key == "Null":
        if value != "Null":
            raise _fail(f"unexpected null literal {_short(value)}")
        return {"null": None}
    if key == "Boolean":
        return {"boolean": _require_bool(value)}
    if key == "String":
        if not isinstance(value, str):
            raise _fail(f"unexpected string literal {_short(value)}")
        return {"string": value}
    if key in _INTEGER_DTYPES:
        return {_INTEGER_DTYPES[key]: str(_require_int(value))}
    if key == "Float32":
        return {"float32": _float_hex(value, float32=True)}
    if key == "Float64":
        return {"float64": _float_hex(value)}
    if key == "Decimal":
        if not isinstance(value, list) or len(value) != 3:
            raise _fail(f"unexpected decimal literal {_short(value)}")
        unscaled, precision, scale = value
        return {
            "decimal": [
                str(_require_int(unscaled)),
                _require_int(precision),
                _require_int(scale),
            ]
        }
    if key == "Binary":
        if not isinstance(value, list) or not all(
            isinstance(b, int) and not isinstance(b, bool) and 0 <= b < 256
            for b in value
        ):
            raise _fail(f"unexpected binary literal {_short(value)}")
        return {"binary": bytes(value).hex()}
    if key == "Date":
        return {"date": _require_int(value)}
    if key == "Time":
        return {"time": _require_int(value)}
    if key == "Datetime":
        if not isinstance(value, list) or len(value) != 3:
            raise _fail(f"unexpected datetime literal {_short(value)}")
        return {
            "datetime": [
                _require_int(value[0]),
                _time_unit(value[1]),
                _time_zone(value[2]),
            ]
        }
    if key == "Duration":
        if not isinstance(value, list) or len(value) != 2:
            raise _fail(f"unexpected duration literal {_short(value)}")
        return {
            "duration": [
                _require_int(value[0]),
                _time_unit(value[1]),
            ]
        }
    if key == "List":
        return _list_literal(value)
    raise _fail(f"{key} literals are not supported")


def _literal(obj: Any) -> Any:
    """Encode a literal node (dynamic Python number or typed scalar)."""
    key, value = _single_key(obj)
    if key == "Dyn":
        dyn_key, dyn_value = _single_key(value)
        if dyn_key == "Int":
            return {"dyn_int": str(_require_int(dyn_value))}
        if dyn_key == "Float":
            return {"dyn_float": _float_hex(dyn_value)}
        raise _fail(f"dynamic {dyn_key} literals are not supported")
    if key == "Scalar":
        return _scalar_literal(value)
    if key == "Series":
        raise _fail(
            "pl.Series literals are not supported (pass a Python list, as in is_in([...]))"
        )
    raise _fail(f"{key} literals are not supported")


def _function(obj: Any) -> Any:
    """Encode a function node (``is_in``, ``is_null``, ``str.contains``, ...)."""
    body = _require_keys(obj, {"input", "function"})
    inputs = body["input"]
    if not isinstance(inputs, list):
        raise _fail(f"unexpected function inputs {_short(inputs)}")
    nodes = [_node(i) for i in inputs]
    key, value = _single_key(body["function"])
    if key == "Negate" and value is None:
        return {"fn": ["negate", {}, nodes]}
    if key == "Boolean":
        name, options = _single_key(value)
        if options is None and name in _BOOLEAN_FUNCTIONS:
            return {"fn": [_BOOLEAN_FUNCTIONS[name], {}, nodes]}
        if name == "IsIn":
            opts = _require_keys(options, {"nulls_equal"})
            return {
                "fn": [
                    "is_in",
                    {"nulls_equal": _require_bool(opts["nulls_equal"])},
                    nodes,
                ]
            }
        if name == "IsBetween":
            closed = _require_keys(options, {"closed"})["closed"]
            if closed not in _CLOSED:
                raise _fail(f"unexpected is_between closed value {_short(closed)}")
            return {"fn": ["is_between", {"closed": _CLOSED[closed]}, nodes]}
        raise _fail(f"the function {name} is not supported")
    if key == "StringExpr":
        name, options = _single_key(value)
        if options is None and name in _STRING_FUNCTIONS:
            return {"fn": [_STRING_FUNCTIONS[name], {}, nodes]}
        if name == "Contains":
            opts = _require_keys(options, {"literal", "strict"})
            return {
                "fn": [
                    "str.contains",
                    {
                        "literal": _require_bool(opts["literal"]),
                        "strict": _require_bool(opts["strict"]),
                    },
                    nodes,
                ]
            }
        raise _fail(f"the function str.{name} is not supported")
    raise _fail(f"the function {key} is not supported")


def _node(obj: Any) -> Any:
    """Encode one serialized expression node in the canonical grammar."""
    key, value = _single_key(obj)
    if key in _PYTHON_FUNCTION_NODES:
        raise _fail(
            "it calls a Python function (map_elements, map_batches, ...), which cannot be "
            "hashed deterministically"
        )
    if key in _MULTI_COLUMN_NODES:
        raise _fail(
            "it selects several columns at once (pl.col with several names, pl.all, a "
            "regex, pl.exclude, a selector or a dtype), whose meaning depends on the "
            "schema; write one predicate per column"
        )
    if key == "Column":
        if not isinstance(value, str):
            raise _fail(f"unexpected column layout {_short(value)}")
        return {"col": value}
    if key == "Literal":
        return {"lit": _literal(value)}
    if key == "BinaryExpr":
        body = _require_keys(value, {"left", "op", "right"})
        op = body["op"]
        if op not in _BINARY_OPS:
            raise _fail(f"the operator {op} is not supported")
        return {"binary": [_BINARY_OPS[op], _node(body["left"]), _node(body["right"])]}
    if key == "Function":
        return _function(value)
    if key == "Cast":
        body = _require_keys(value, {"expr", "dtype", "options"})
        dtype_key, dtype_value = _single_key(body["dtype"])
        if dtype_key != "Literal":
            raise _fail(f"unexpected cast dtype layout {_short(body['dtype'])}")
        options = body["options"]
        if options not in _CAST_OPTIONS:
            raise _fail(f"the cast option {options} is not supported")
        return {
            "cast": [_dtype(dtype_value), _CAST_OPTIONS[options], _node(body["expr"])]
        }
    raise _fail(f"{key} expressions are not supported")


def polars_expr_canonical(expr: pl.Expr) -> Any:
    """Return the ``polars_expr_v1`` canonical tree of a Polars expression.

    Args:
        expr: The expression to encode.

    Returns:
        A JSON-serialisable tree in the grammar of this module's docstring.

    Raises:
        ValueError: If the expression is outside the grammar or Polars serializes it in a
            layout the parser does not know. The message names the expression, says why,
            lists what is supported and what to do instead.
    """
    try:
        try:
            serialized = expr.meta.serialize(format="json")
        except Exception as exc:  # noqa: BLE001 - any serialization failure is reported
            raise _fail(
                "Polars cannot serialize it (it probably calls a Python function, "
                f"which cannot be hashed deterministically): {exc}"
            ) from exc
        if isinstance(serialized, bytes):
            serialized = serialized.decode("utf-8")
        return _node(json.loads(serialized))
    except _Unsupported as exc:
        raise ValueError(
            f"Cannot hash the Polars expression {str(expr)!r} for an Orcapod identity: "
            f"{exc}. {_SUPPORTED_TEXT} {_ADVICE_TEXT}"
        ) from None


def polars_expr_content_hash(expr: pl.Expr) -> ContentHash:
    """Return the ``polars_expr_v1`` hash of a Polars expression.

    Args:
        expr: The expression to hash.

    Returns:
        ``ContentHash("polars_expr_v1", sha256(canonical JSON of the tree))``.

    Raises:
        ValueError: If the expression cannot be encoded (see ``polars_expr_canonical``).
    """
    canonical = json.dumps(
        polars_expr_canonical(expr),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return ContentHash(POLARS_EXPR_HASH_LABEL, hashlib.sha256(canonical).digest())
