"""Golden hash cases (ITL-626): every hash value Orcapod persists or embeds in a name.

Each case computes a flat ``{entry: value}`` map. ``compute_group`` prefixes each entry with
its case name, so a golden file entry reads ``<case>.<entry>``. The values are pinned in
``data/<group>.json`` by ``generate_golden_hashes.py`` and checked by
``test_golden_hashes.py``.

DO NOT MOVE OR RENAME anything defined in this module. Function pods, dataclasses and
pydantic models hash together with their defining module and qualified name, so moving them
changes pinned values even though no hashing code changed.

Imports of names that older releases lack are made inside the case that needs them, so the
generator can be run against an older checkout (``--allow-errors``) to list the values that
changed between releases. A case the old API cannot express records an ``ERROR:`` entry.

Values that are volatile by design are deliberately not pinned:

* the record ids of a function pod's result table, and the per-row token inside a function
  pod's output source info, are time-based UUIDv7 values;
* ``__pod_ts``, ``__pf_var_git_hash`` and ``__pf_exec_python_version`` record when, where and
  on which interpreter a result was computed.
"""

from __future__ import annotations

import dataclasses
import json
import collections
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pydantic

from orcapod import dataclass as structural_dataclass
from orcapod import function_pod

CaseFn = Callable[[], dict[str, Any]]

GROUPS = ("semantic", "schema", "arrow", "datagram", "pipeline")

_CASES: dict[str, list[tuple[str, CaseFn]]] = {group: [] for group in GROUPS}


def case(group: str, name: str) -> Callable[[CaseFn], CaseFn]:
    """Register a golden case under ``group``.

    Args:
        group: One of ``GROUPS``; selects the golden file the entries are written to.
        name: Case name, used as the prefix of every entry the case returns.

    Returns:
        A decorator that registers the case function and returns it unchanged.
    """

    def register(fn: CaseFn) -> CaseFn:
        _CASES[group].append((name, fn))
        return fn

    return register


def compute_group(group: str, *, allow_errors: bool = False) -> dict[str, Any]:
    """Compute every entry of one golden group.

    Args:
        group: One of ``GROUPS``.
        allow_errors: When true, a case that raises records a single
            ``"ERROR: <type>: <message>"`` entry instead of propagating. Used only when
            running the generator against an older release.

    Returns:
        A dict of ``<case>.<entry>`` names to JSON-compatible values, sorted by name.
    """
    out: dict[str, Any] = {}
    for name, fn in _CASES[group]:
        try:
            values = fn()
        except Exception as exc:
            if not allow_errors:
                raise
            out[name] = f"ERROR: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
            continue
        for key, value in values.items():
            full = f"{name}.{key}" if key else name
            if full in out:
                raise ValueError(f"duplicate golden entry {full!r}")
            out[full] = value
    return dict(sorted(out.items()))


def compute_all(*, allow_errors: bool = False) -> dict[str, dict[str, Any]]:
    """Compute every golden group.

    Args:
        allow_errors: Passed to ``compute_group``.

    Returns:
        A dict mapping each group name to its computed entries.
    """
    return {g: compute_group(g, allow_errors=allow_errors) for g in GROUPS}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _ctx() -> Any:
    from orcapod.contexts import get_default_context

    return get_default_context()


def _bytes_text(value: bytes) -> str:
    """Hex-encode bytes, keeping a stored hash's ASCII method prefix readable.

    Stored content hashes are ``b"<method>:<digest>"`` (e.g. ``b"arrow_v0.1:..."``); they are
    rendered as ``"<method>:<digest hex>"``. Any other bytes value is plain hex.
    """
    prefix, sep, digest = value.partition(b":")
    if sep and prefix and all(0x21 <= c <= 0x7E for c in prefix):
        return f"{prefix.decode('ascii')}:{digest.hex()}"
    return value.hex()


def _jsonable(value: Any) -> Any:
    """Convert a value to a JSON-compatible form (bytes become hex)."""
    if isinstance(value, (bytes, bytearray)):
        return _bytes_text(bytes(value))
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (Path, uuid.UUID)):
        return str(value)
    return value


def _row_key(tag: dict[str, Any]) -> str:
    return json.dumps(_jsonable(tag), sort_keys=True, separators=(",", ":"))


def stream_summary(stream: Any) -> dict[str, Any]:
    """Summarize a stream's identity through its public interface.

    Rows are keyed by their user tag values, so a failure names the row and the pinned
    values do not depend on emission order.

    Args:
        stream: Any Orcapod stream (source, operator output, function pod output, node).

    Returns:
        Entries for the stream's content hash, pipeline hash, system-tag column names, and
        per-row system-tag values (record ids as hex) and data content hashes.
    """
    tag_keys, _ = stream.keys()
    all_tag_keys, _ = stream.keys(columns={"system_tags": True})
    sys_cols = [k for k in all_tag_keys if k not in tag_keys]
    out: dict[str, Any] = {
        "content_hash": stream.content_hash().to_string(),
        "pipeline_hash": stream.pipeline_hash().to_string(),
        "system_tag_columns": sys_cols,
    }
    for tag, data in stream.iter_data():
        row = _row_key(tag.as_dict())
        tag_all = tag.as_dict(all_info=True)
        for col in sys_cols:
            out[f"row[{row}].{col}"] = _jsonable(tag_all[col])
        out[f"row[{row}].data_hash"] = data.content_hash().to_string()
    return out


def _prefixed(prefix: str, values: dict[str, Any]) -> dict[str, Any]:
    return {f"{prefix}.{k}": v for k, v in values.items()}


# ---------------------------------------------------------------------------
# Logical-type fixtures — DO NOT MOVE (module path is part of their identity)
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class GoldenPoint:
    """Dataclass whose logical type and values are pinned."""

    x: int
    y: float


class GoldenModel(pydantic.BaseModel):
    """Pydantic model whose logical type and values are pinned."""

    name: str
    count: int


@structural_dataclass(name="golden.Declared")
class GoldenDeclared:
    """Structural class (ITL-972) with an optional field."""

    label: str
    score: float | None


class GoldenDeclaredModel(pydantic.BaseModel):
    """Pydantic model with the same fields as ``GoldenDeclared``: another type."""

    label: str
    score: float | None


@structural_dataclass(name="golden.Inner")
class GoldenInner:
    """Structural class nested in ``GoldenOuter``; its record enters the outer one."""

    value: int


@structural_dataclass(name="golden.Outer")
class GoldenOuter:
    """Structural class with a nested structural field and a container field."""

    inner: GoldenInner
    tags: list[str]


@structural_dataclass(name="golden.WithArray")
class GoldenWithArray:
    """Structural class with a NumPy array field; the array's identifier enters its identity."""

    label: str
    values: np.ndarray  # pyright: ignore[reportMissingTypeArgument]  # the bare class is the annotation Orcapod stores


@structural_dataclass(name="golden.Twin")
class GoldenTwinAB:
    """Structural ``golden.Twin``; same fields as ``GoldenTwinBA`` in another order.

    Declared field order is part of the identity (ITL-881), so the twins are two types
    and must pin different values.
    """

    a: int
    b: str


@structural_dataclass(name="golden.Twin")
class GoldenTwinBA:
    """Structural ``golden.Twin``; must pin values different from ``GoldenTwinAB``'s."""

    b: str
    a: int


@structural_dataclass(name="golden.Declared")
class GoldenDeclaredAgain:
    """A second class of the type ``golden.Declared``: must pin ``GoldenDeclared``'s values."""

    label: str
    score: float | None


@dataclasses.dataclass
class GoldenChild(GoldenPoint):
    """Ordinary subclass of ``GoldenPoint``: its own type, with its own field."""

    z: int = 0


@dataclasses.dataclass
class GoldenHolder:
    """Ordinary dataclass with a structural field; the inner record enters its identity."""

    inner: GoldenInner
    note: str


def register_logical_classes() -> None:
    """Register the dataclass and pydantic fixtures with the default type converter."""
    tc = _ctx().type_converter
    tc.register_python_class(GoldenPoint)
    tc.register_python_class(GoldenModel)
    for cls in (
        GoldenDeclared,
        GoldenDeclaredModel,
        GoldenOuter,
        GoldenWithArray,
        GoldenTwinAB,
        GoldenTwinBA,
        GoldenDeclaredAgain,
        GoldenChild,
        GoldenHolder,
    ):
        tc.register_python_class(cls)


PATH_A = Path("/golden/data/a.txt")
PATH_B = Path("/golden/data/b.txt")
UUID_A = uuid.UUID("12345678-1234-5678-1234-567812345678")
UUID_B = uuid.UUID("87654321-4321-8765-4321-876543218765")


# ---------------------------------------------------------------------------
# Group: semantic — BaseSemanticHasher on Python values and type objects
# ---------------------------------------------------------------------------

# Added by ITL-942 (finding T): field order is part of a namedtuple's hash.
_POINT_XY = collections.namedtuple("Point", ["x", "y"])
_POINT_YX = collections.namedtuple("Point", ["y", "x"])

SEMANTIC_VALUES: dict[str, Any] = {
    "int.zero": 0,
    "int.one": 1,
    "int.negative": -1,
    "int.large": 2**62,
    "int.huge": 2**100,
    "float.zero": 0.0,
    "float.one_half": 1.5,
    "float.negative": -2.25,
    "float.tiny": 1e-300,
    "str.empty": "",
    "str.ascii": "orcapod",
    "str.unicode": "üñíçødé ✓",
    "bytes.empty": b"",
    "bytes.binary": b"\x00\x01\xfe\xff",
    "bytearray.binary": bytearray(b"\x00\x01\xfe\xff"),
    "bool.true": True,
    "bool.false": False,
    "none": None,
    "list.empty": [],
    "list.ints": [1, 2, 3],
    "list.mixed": [1, "a", None, 2.5],
    "tuple.ints": (1, 2, 3),
    "dict.empty": {},
    "dict.ab": {"a": 1, "b": 2},
    "dict.ba": {"b": 2, "a": 1},
    "set.ints": {3, 1, 2},
    "frozenset.ints": frozenset({3, 1, 2}),
    "set.strs": {"x", "y", "z"},
    "nested": {"a": [1, {"b": (2, 3)}], "c": {"d": None}},
    "uuid.a": UUID_A,
    "uuid.nil": uuid.UUID(int=0),
    "ndarray.int32": np.arange(6, dtype=np.int32).reshape(2, 3),
    "ndarray.int64": np.arange(6, dtype=np.int64).reshape(2, 3),
    "ndarray.float64": np.array([[0.5, 1.5], [2.5, 3.5]], dtype=np.float64),
    "ndarray.empty": np.zeros((0,), dtype=np.float32),
    # Added by ITL-981: the properties the strict array hash counts or never counts.
    "ndarray.fortran": np.asfortranarray(np.arange(6, dtype=np.int32).reshape(2, 3)),
    "ndarray.big_endian": np.arange(6, dtype=">i4").reshape(2, 3),
    "ndarray.aligned_struct": np.array(
        [(1, 0.5), (-2, 1.5)], dtype=np.dtype([("a", "i1"), ("b", "<f8")], align=True)
    ),
    "ndarray.strided_view": np.arange(12, dtype=np.int32).reshape(2, 6)[:, ::2],
    "dataframe.basic": pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}),
    # Added by ITL-942: set order, dict keys and '__type__' dicts.
    "set.mixed_str_equal": {1, "1", 2, "2"},
    "dict.int_keys": {1: "x", 2: "y"},
    "dict.str_keys_like_ints": {"1": "x", "2": "y"},
    "dict.type_key": {"__type__": "tuple", "items": [1, 2]},
    "namedtuple.xy": _POINT_XY(x=1, y=2),
    "namedtuple.yx": _POINT_YX(y=2, x=1),
}


@case("semantic", "value")
def semantic_values() -> dict[str, Any]:
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(v).to_string() for k, v in SEMANTIC_VALUES.items()}


def golden_fn_plain(a: int, b: str) -> float:
    """Function hashed as a value by the semantic group (ITL-953). DO NOT MOVE."""
    return float(a)


def golden_fn_defaults(
    a: int,
    b: str = "x",
    c: int | None = None,
    d: tuple[int, ...] = (1, 2),
    e: frozenset[str] = frozenset({"p", "q", "r"}),
    f: dict[str, list[int]] = {"k": [1, 2], "j": []},  # noqa: B006  (never mutated)
) -> float:
    """Function with default values, hashed as a value (ITL-953). DO NOT MOVE."""
    return float(a)


def golden_fn_kinds(a: int, /, b: int, *, c: int = 0) -> int:
    """Function with every parameter kind, hashed as a value (ITL-953). DO NOT MOVE."""
    return a + b + c


def golden_scaled(age: int, factor: float = 2.0) -> float:
    """Data function with a default, pinned by its stored signature hash. DO NOT MOVE."""
    return age * factor


@case("semantic", "function")
def semantic_functions() -> dict[str, Any]:
    """Pin the hash of a function used as a value and the stored signature hash.

    A default value enters as the semantic hasher's hash of the value and each
    parameter carries its kind (ITL-953).

    Returns:
        The function-value hashes, and the ``function_signature_hash`` a data
        function stores with every result row.
    """
    from orcapod.core.data_function import PythonDataFunction

    hasher = _ctx().semantic_hasher
    out: dict[str, Any] = {
        "value.plain": hasher.hash_object(golden_fn_plain).to_string(),
        "value.defaults": hasher.hash_object(golden_fn_defaults).to_string(),
        "value.kinds": hasher.hash_object(golden_fn_kinds).to_string(),
    }
    data_functions = {
        "ratio": golden_ratio.pod.data_function,
        "scaled": PythonDataFunction(golden_scaled, output_keys="scaled"),
    }
    for name, data_function in data_functions.items():
        variation = data_function.get_function_variation_data()
        out[f"stored_signature.{name}"] = _bytes_text(
            variation["function_signature_hash"]
        )
    return out


def _semantic_types() -> dict[str, Any]:
    return {
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
        "bytes": bytes,
        "optional_int": int | None,
        "list_int": list[int],
        "list_list_int": list[list[int]],
        "dict_str_int": dict[str, int],
        "tuple_int_str": tuple[int, str],
        "path": Path,
        "uuid": uuid.UUID,
        "ndarray": np.ndarray,
        "dataframe": pd.DataFrame,
        "list_path": list[Path],
        "set_uuid": set[uuid.UUID],
        "dataclass": GoldenPoint,
        "pydantic": GoldenModel,
        "structural": GoldenDeclared,
        "pydantic_optional": GoldenDeclaredModel,
        "structural_nested": GoldenOuter,
        "structural_with_array": GoldenWithArray,
        "structural_twin_ab": GoldenTwinAB,
        "structural_twin_ba": GoldenTwinBA,
        "structural_same_identity": GoldenDeclaredAgain,
        "dataclass_subclass": GoldenChild,
        "dataclass_with_structural": GoldenHolder,
        "list_dataclass": list[GoldenPoint],
    }


@case("semantic", "type")
def semantic_types() -> dict[str, Any]:
    register_logical_classes()
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(t).to_string() for k, t in _semantic_types().items()}


# ---------------------------------------------------------------------------
# Group: schema — Schema hashes, Arrow types, schema-hash helpers, SourceStreamBuilder
# ---------------------------------------------------------------------------


def _arrow_type_text(arrow_type: pa.DataType) -> str:
    """Render an Arrow type as stored: extension name plus storage type, not pyarrow's repr."""
    if isinstance(arrow_type, pa.ExtensionType):
        return (
            f"extension<{arrow_type.extension_name}>"
            f"[{_arrow_type_text(arrow_type.storage_type)}]"
        )
    return str(arrow_type)


@case("schema", "single_field")
def schema_single_field() -> dict[str, Any]:
    from orcapod.types import Schema

    register_logical_classes()
    ctx = _ctx()
    out: dict[str, Any] = {}
    for name, tp in _semantic_types().items():
        out[f"{name}.arrow_type"] = _arrow_type_text(
            ctx.type_converter.python_type_to_arrow_type(tp)
        )
        out[f"{name}.schema_hash"] = ctx.semantic_hasher.hash_object(
            Schema({"c": tp})
        ).to_string()
    return out


@case("schema", "multi_field")
def schema_multi_field() -> dict[str, Any]:
    from orcapod.types import Schema

    register_logical_classes()
    hasher = _ctx().semantic_hasher
    schemas = {
        "two_scalars": Schema({"a": int, "b": str}),
        "two_scalars_reordered": Schema({"b": str, "a": int}),
        "optional_field": Schema({"a": int, "b": str}, optional_fields=["b"]),
        "nullable_type": Schema({"a": int, "b": str | None}),
        "logical_mix": Schema(
            {"p": Path, "u": uuid.UUID, "pt": GoldenPoint, "m": GoldenModel}
        ),
        "empty": Schema({}),
    }
    return {k: hasher.hash_object(s).to_string() for k, s in schemas.items()}


def _tag_data_schemas() -> tuple[Any, Any]:
    from orcapod.types import Schema

    return Schema({"subject": str, "session": int}), Schema(
        {"weight": float, "image": Path}
    )


@case("schema", "compute_schema_hash")
def schema_compute_schema_hash() -> dict[str, Any]:
    from orcapod.utils.schema_utils import compute_schema_hash

    tag_schema, data_schema = _tag_data_schemas()
    hasher = _ctx().semantic_hasher
    return {
        "full": compute_schema_hash(tag_schema, data_schema, hasher, None),
        "chars_16": compute_schema_hash(tag_schema, data_schema, hasher, 16),
    }


@case("schema", "source_schema_hash")
def schema_source_schema_hash() -> dict[str, Any]:
    from orcapod import DEFAULT_CONFIG
    from orcapod.utils.arrow_utils import system_tag_column_names
    from orcapod.utils.schema_utils import compute_source_schema_hash

    tag_schema, data_schema = _tag_data_schemas()
    schema_hash = compute_source_schema_hash(
        tag_schema, data_schema, _ctx(), DEFAULT_CONFIG
    )
    source_id_col, record_id_col = system_tag_column_names(schema_hash)
    return {
        "schema_hash": schema_hash,
        "source_id_column": source_id_col,
        "record_id_column": record_id_col,
    }


def _builder_table() -> pa.Table:
    return pa.table(
        {
            "subject": pa.array(["s1", "s2", "s3"], pa.large_string()),
            "weight": pa.array([20.5, 22.25, 19.0], pa.float64()),
            "count": pa.array([1, 2, 3], pa.int32()),
        },
        schema=pa.schema(
            [
                pa.field("subject", pa.large_string(), nullable=False),
                pa.field("weight", pa.float64(), nullable=False),
                pa.field("count", pa.int32(), nullable=True),
            ]
        ),
    )


@case("schema", "source_stream_builder")
def schema_source_stream_builder() -> dict[str, Any]:
    # SourceStreamBuilder is not public API; the issue names it as a pinned call site.
    from orcapod import DEFAULT_CONFIG
    from orcapod.core.sources.stream_builder import SourceStreamBuilder

    builder = SourceStreamBuilder(_ctx(), DEFAULT_CONFIG)
    out: dict[str, Any] = {}
    for variant, kwargs in {
        "default_source_id": {"source_kind": "arrow_table"},
        "named_source_id": {"source_id": "golden_source"},
        "record_id_column": {
            "source_id": "golden_source",
            "record_id_column": "subject",
        },
    }.items():
        result = builder.build(_builder_table(), tag_columns=["subject"], **kwargs)
        out[f"{variant}.schema_hash"] = result.schema_hash
        out[f"{variant}.table_hash"] = result.table_hash.to_string()
        out[f"{variant}.source_id"] = result.source_id
        out[f"{variant}.system_tag_columns"] = list(result.system_tag_columns)
        out.update(_prefixed(variant, stream_summary(result.stream)))
    return out


@case("schema", "derived_source_tags_as_data")
def schema_derived_source_tags_as_data() -> dict[str, Any]:
    """Pin a source derived from another source with a tag also offered as data (ITL-939).

    Three variants: the default source id (``tags_as_data:<label>:<hex>``, the derivation
    hash), an explicit source id, and the default source id with a ``record_id_column``
    (which enters the derivation hash).

    Returns:
        Each variant's source id and stream summary.
    """
    from orcapod.core.sources import ArrowTableSource, DerivedSource

    origin = ArrowTableSource(
        _builder_table(),
        tag_columns=["subject"],
        source_id="golden_source",
        data_context=_ctx(),
    )
    out: dict[str, Any] = {}
    for variant, kwargs in {
        "default_source_id": {},
        "named_source_id": {"source_id": "golden_derived"},
        "record_id_column": {"record_id_column": "subject"},
    }.items():
        derived = DerivedSource(
            origin, tags_as_data={"subject": "subject_value"}, **kwargs
        )
        out[f"{variant}.source_id"] = derived.source_id
        out.update(_prefixed(variant, stream_summary(derived)))
    return out


# ---------------------------------------------------------------------------
# Group: arrow — StarfixArrowHasher table and schema hashes
# ---------------------------------------------------------------------------


class GoldenOpaqueType(pa.ExtensionType):
    """Extension type with no orcapod handler: exercises the passthrough leg."""

    def __init__(self) -> None:
        super().__init__(pa.int64(), "golden.opaque")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> GoldenOpaqueType:
        return cls()


def _arrow_tables() -> dict[str, pa.Table]:
    ints = [1, 2, 3]
    tables: dict[str, pa.Table] = {}
    for width in ("int8", "int16", "int32", "int64", "uint8"):
        tables[f"width.{width}"] = pa.table({"a": pa.array(ints, getattr(pa, width)())})
    tables["float.float32"] = pa.table({"a": pa.array([0.5, 1.5], pa.float32())})
    tables["float.float64"] = pa.table({"a": pa.array([0.5, 1.5], pa.float64())})
    tables["bool"] = pa.table({"a": pa.array([True, False, True], pa.bool_())})
    tables["layout.string"] = pa.table({"a": pa.array(["x", "yz"], pa.string())})
    tables["layout.large_string"] = pa.table(
        {"a": pa.array(["x", "yz"], pa.large_string())}
    )
    tables["layout.binary"] = pa.table({"a": pa.array([b"\x00", b"ab"], pa.binary())})
    tables["layout.large_binary"] = pa.table(
        {"a": pa.array([b"\x00", b"ab"], pa.large_binary())}
    )
    tables["layout.list"] = pa.table(
        {"a": pa.array([[1, 2], [3]], pa.list_(pa.int64()))}
    )
    tables["layout.large_list"] = pa.table(
        {"a": pa.array([[1, 2], [3]], pa.large_list(pa.int64()))}
    )
    tables["nulls.int64"] = pa.table({"a": pa.array([1, None, 3], pa.int64())})
    tables["nulls.string"] = pa.table(
        {"a": pa.array(["x", None, "z"], pa.large_string())}
    )
    tables["nullability.nullable"] = pa.table(
        {"a": pa.array(ints, pa.int64())},
        schema=pa.schema([pa.field("a", pa.int64(), nullable=True)]),
    )
    tables["nullability.non_nullable"] = pa.table(
        {"a": pa.array(ints, pa.int64())},
        schema=pa.schema([pa.field("a", pa.int64(), nullable=False)]),
    )
    tables["nested.list_list"] = pa.table(
        {"a": pa.array([[[1], [2, 3]], []], pa.large_list(pa.large_list(pa.int64())))}
    )
    struct_type = pa.struct([("x", pa.int64()), ("y", pa.large_string())])
    tables["nested.struct"] = pa.table(
        {"a": pa.array([{"x": 1, "y": "p"}, {"x": 2, "y": "q"}], struct_type)}
    )
    tables["nested.list_struct"] = pa.table(
        {"a": pa.array([[{"x": 1, "y": "p"}], []], pa.large_list(struct_type))}
    )
    col_a = pa.array(ints, pa.int64())
    col_b = pa.array(["x", "y", "z"], pa.large_string())
    tables["order.ab"] = pa.Table.from_arrays([col_a, col_b], names=["a", "b"])
    tables["order.ba"] = pa.Table.from_arrays([col_b, col_a], names=["b", "a"])
    tables["order.rows_reversed"] = pa.table(
        {
            "a": pa.array(ints[::-1], pa.int64()),
            "b": pa.array(["z", "y", "x"], pa.large_string()),
        }
    )
    tables["empty.zero_rows"] = pa.table({"a": pa.array([], pa.int64())})
    # Added with starfix hash format 2 (ITL-910): inputs format 1 could not hash or
    # hashed alike (ITL-848, ITL-927).
    tables["fixed_size_list.float32_2"] = pa.table(
        {"a": pa.array([[0.5, 1.5], [2.5, 3.5]], pa.list_(pa.float32(), 2))}
    )
    tables["fixed_size_list.null_slot"] = pa.table(
        {"a": pa.array([[1, 2], None], pa.list_(pa.int64(), 2))}
    )
    list3 = pa.large_list(pa.large_list(pa.large_list(pa.int64())))
    tables["collision.list_lengths_a"] = pa.table(
        {"a": pa.array([[[[1]], [[2], [3]]]], list3)}
    )
    tables["collision.list_lengths_b"] = pa.table(
        {"a": pa.array([[[[1], [2]], [[3]]]], list3)}
    )
    tables["collision.null_struct"] = pa.table(
        {"a": pa.array([None], pa.struct([("x", pa.int64())]))}
    )
    tables["collision.struct_of_nulls"] = pa.table(
        {"a": pa.array([{"x": None}], pa.struct([("x", pa.int64())]))}
    )
    tables["bool.chunked"] = pa.Table.from_batches(
        tables["bool"].to_batches(max_chunksize=1)
    )
    # Added by ITL-942 (finding I): nested nullability is part of the hash whether or
    # not the column holds data.
    non_null_struct = pa.struct([pa.field("x", pa.int64(), nullable=False)])
    nullable_struct = pa.struct([pa.field("x", pa.int64(), nullable=True)])
    non_null_list = pa.large_list(pa.field("item", pa.int64(), nullable=False))
    for label, struct_t, list_t in (
        ("non_null", non_null_struct, non_null_list),
        ("nullable", nullable_struct, pa.large_list(pa.int64())),
    ):
        tables[f"nested_nullability.struct_{label}.data"] = pa.table(
            {"a": pa.array([{"x": 1}], struct_t)}
        )
        tables[f"nested_nullability.struct_{label}.zero_rows"] = pa.table(
            {"a": pa.array([], struct_t)}
        )
        tables[f"nested_nullability.struct_{label}.all_null"] = pa.table(
            {"a": pa.array([None], struct_t)}
        )
        tables[f"nested_nullability.list_{label}.data"] = pa.table(
            {"a": pa.array([[1]], list_t)}
        )
        tables[f"nested_nullability.list_{label}.zero_rows"] = pa.table(
            {"a": pa.array([], list_t)}
        )
    return tables


def _extension_tables() -> dict[str, pa.Table]:
    from orcapod.types import Schema

    tc = _ctx().type_converter
    rows = {
        "path": [{"c": PATH_A}, {"c": PATH_B}],
        "uuid": [{"c": UUID_A}, {"c": UUID_B}],
    }
    types = {"path": Path, "uuid": uuid.UUID}
    tables = {
        f"extension.{name}": tc.python_dicts_to_arrow_table(
            rows[name], python_schema=Schema({"c": types[name]})
        )
        for name in rows
    }
    try:
        # The pyarrow stubs name PyExtensionType, but ExtensionType subclasses are the
        # documented way to register a custom extension type.
        pa.register_extension_type(GoldenOpaqueType())  # pyright: ignore[reportArgumentType]
    except pa.ArrowKeyError:
        pass  # already registered in this process
    storage = pa.array([1, 2, 3], pa.int64())
    tables["extension.unregistered_passthrough"] = pa.table(
        {"c": pa.ExtensionArray.from_storage(GoldenOpaqueType(), storage)}
    )
    # pyarrow's canonical extension types, hashable with their identity since ITL-910.
    tables["extension.canonical_uuid"] = pa.table(
        {
            "c": pa.ExtensionArray.from_storage(
                pa.uuid(), pa.array([UUID_A.bytes, UUID_B.bytes], pa.binary(16))
            )
        }
    )
    tables["extension.canonical_fixed_shape_tensor"] = pa.table(
        {
            "c": pa.ExtensionArray.from_storage(
                pa.fixed_shape_tensor(pa.float32(), [2]),
                pa.array([[0.5, 1.5], [2.5, 3.5]], pa.list_(pa.float32(), 2)),
            )
        }
    )
    return tables


@case("arrow", "table")
def arrow_table_hashes() -> dict[str, Any]:
    hasher = _ctx().arrow_hasher
    return {k: hasher.hash_table(t).to_string() for k, t in _arrow_tables().items()}


@case("arrow", "extension_table")
def arrow_extension_table_hashes() -> dict[str, Any]:
    hasher = _ctx().arrow_hasher
    return {k: hasher.hash_table(t).to_string() for k, t in _extension_tables().items()}


@case("arrow", "schema")
def arrow_schema_hashes() -> dict[str, Any]:
    hasher = _ctx().arrow_hasher
    return {
        k: hasher.hash_schema(t.schema).to_string() for k, t in _arrow_tables().items()
    }


@case("arrow", "dict_order")
def arrow_dict_order_hashes() -> dict[str, Any]:
    """Pin that an Arrow table is hashed exactly as stored (ITL-905).

    A key/value list that arrives in Arrow form keeps its order as identity (sorted and
    unsorted pin different values), like any other list of structs.
    """
    hasher = _ctx().arrow_hasher
    kv = pa.large_list(
        pa.struct([pa.field("key", pa.large_string()), pa.field("value", pa.int64())])
    )
    pairs = pa.large_list(
        pa.struct([pa.field("f0", pa.large_string()), pa.field("f1", pa.int64())])
    )
    ab = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
    tables = {
        "kv.sorted": pa.table({"m": pa.array([ab], kv)}),
        "kv.unsorted": pa.table({"m": pa.array([ab[::-1]], kv)}),
        "pairs.ab": pa.table(
            {"m": pa.array([[{"f0": "a", "f1": 1}, {"f0": "b", "f1": 2}]], pairs)}
        ),
        "pairs.ba": pa.table(
            {"m": pa.array([[{"f0": "b", "f1": 2}, {"f0": "a", "f1": 1}]], pairs)}
        ),
    }
    return {k: hasher.hash_table(t).to_string() for k, t in tables.items()}


@case("arrow", "extension_schema")
def arrow_extension_schema_hashes() -> dict[str, Any]:
    """Schema hashes of extension columns, hashed with their identity (ITL-910, ITL-846)."""
    hasher = _ctx().arrow_hasher
    return {
        k: hasher.hash_schema(t.schema).to_string()
        for k, t in _extension_tables().items()
    }


# ---------------------------------------------------------------------------
# Group: datagram — public sources carrying each logical type
# ---------------------------------------------------------------------------


def _datagram_rows() -> dict[str, list[dict[str, Any]]]:
    return {
        "scalars": [
            {"k": "r1", "i": 1, "f": 0.5, "s": "x", "b": True, "raw": b"\x00"},
            {"k": "r2", "i": -2, "f": 1.5, "s": "yz", "b": False, "raw": b"\xff"},
        ],
        "optional": [{"k": "r1", "v": 1}, {"k": "r2", "v": None}],
        "nested": [
            {"k": "r1", "li": [1, 2], "lli": [[1], [2, 3]], "d": {"a": 1}},
            {"k": "r2", "li": [], "lli": [[]], "d": {"b": 2, "c": 3}},
        ],
        "path": [{"k": "r1", "c": PATH_A}, {"k": "r2", "c": PATH_B}],
        "uuid": [{"k": "r1", "c": UUID_A}, {"k": "r2", "c": UUID_B}],
        "ndarray": [
            {"k": "r1", "c": np.arange(4, dtype=np.int64)},
            {"k": "r2", "c": np.array([[0.5, 1.5]], dtype=np.float64)},
        ],
        "dataframe": [
            {"k": "r1", "c": pd.DataFrame({"a": [1, 2]})},
            {"k": "r2", "c": pd.DataFrame({"a": [3], "b": ["x"]})},
        ],
        "dataclass": [
            {"k": "r1", "c": GoldenPoint(1, 0.5)},
            {"k": "r2", "c": GoldenPoint(2, 1.5)},
        ],
        "pydantic": [
            {"k": "r1", "c": GoldenModel(name="a", count=1)},
            {"k": "r2", "c": GoldenModel(name="b", count=2)},
        ],
        "list_path": [{"k": "r1", "c": [PATH_A, PATH_B]}, {"k": "r2", "c": []}],
        "set_uuid": [{"k": "r1", "c": {UUID_A, UUID_B}}, {"k": "r2", "c": {UUID_A}}],
        "structural": [
            {"k": "r1", "c": GoldenDeclared("a", 0.5)},
            {"k": "r2", "c": GoldenDeclared("b", None)},
        ],
        "pydantic_optional": [
            {"k": "r1", "c": GoldenDeclaredModel(label="a", score=0.5)},
            {"k": "r2", "c": GoldenDeclaredModel(label="b", score=None)},
        ],
        "structural_nested": [
            {"k": "r1", "c": GoldenOuter(GoldenInner(1), ["x", "y"])},
            {"k": "r2", "c": GoldenOuter(GoldenInner(2), [])},
        ],
        "structural_with_array": [
            {"k": "r1", "c": GoldenWithArray("a", np.arange(3, dtype=np.float32))},
            {"k": "r2", "c": GoldenWithArray("b", np.zeros((2, 2), dtype=np.int64))},
        ],
        "structural_twin_ab": [
            {"k": "r1", "c": GoldenTwinAB(1, "x")},
            {"k": "r2", "c": GoldenTwinAB(2, "y")},
        ],
        "structural_twin_ba": [
            {"k": "r1", "c": GoldenTwinBA("x", 1)},
            {"k": "r2", "c": GoldenTwinBA("y", 2)},
        ],
        "structural_same_identity": [
            {"k": "r1", "c": GoldenDeclaredAgain("a", 0.5)},
            {"k": "r2", "c": GoldenDeclaredAgain("b", None)},
        ],
        "dataclass_subclass": [
            {"k": "r1", "c": GoldenChild(1, 0.5, 7)},
            {"k": "r2", "c": GoldenChild(2, 1.5)},
        ],
        "dataclass_with_structural": [
            {"k": "r1", "c": GoldenHolder(GoldenInner(1), "a")},
            {"k": "r2", "c": GoldenHolder(GoldenInner(2), "b")},
        ],
    }


def _make_datagram_case(name: str) -> CaseFn:
    def run() -> dict[str, Any]:
        from orcapod.sources import DictSource

        source = DictSource(
            data=_datagram_rows()[name], tag_columns=["k"], source_id=f"golden_{name}"
        )
        return stream_summary(source)

    return run


for _name in _datagram_rows():
    case("datagram", _name)(_make_datagram_case(_name))


def _arrow_source(column: pa.Array[Any], nullable: bool = False) -> Any:
    from orcapod.sources import ArrowTableSource

    table = pa.table(
        {"k": pa.array(["r1", "r2", "r3"], pa.large_string()), "c": column},
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("c", column.type, nullable=nullable),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["k"], source_id="golden_arrow")


@case("datagram", "dict_order")
def datagram_dict_order() -> dict[str, Any]:
    """Pin equal hashes for rows holding equal dicts in different insertion orders (ITL-905)."""
    from orcapod.sources import DictSource

    rows: dict[str, tuple[Any, list[Any]]] = {
        "str_keys": (dict[str, int], [{"b": 2, "a": 1}, {"a": 1, "b": 2}]),
        "int_keys": (dict[int, str], [{10: "x", 9: "y"}, {9: "y", 10: "x"}]),
        "nested": (
            dict[str, dict[str, int]],
            [
                {"y": {"d": 4, "c": 3}, "x": {"a": 1}},
                {"x": {"a": 1}, "y": {"c": 3, "d": 4}},
            ],
        ),
    }
    out: dict[str, Any] = {}
    for name, (value_type, values) in rows.items():
        source = DictSource(
            data=[{"k": f"r{i}", "m": v} for i, v in enumerate(values)],
            tag_columns=["k"],
            data_schema={"k": str, "m": value_type},
            source_id=f"golden_dict_{name}",
        )
        out.update(_prefixed(name, stream_summary(source)))
    return out


@case("datagram", "arrow_source")
def datagram_arrow_source() -> dict[str, Any]:
    ints = [1, 2, 3]
    columns = {
        "int8": pa.array(ints, pa.int8()),
        "int32": pa.array(ints, pa.int32()),
        "int64": pa.array(ints, pa.int64()),
        "string": pa.array(["x", "y", "z"], pa.string()),
        "large_string": pa.array(["x", "y", "z"], pa.large_string()),
    }
    out: dict[str, Any] = {}
    for name, column in columns.items():
        out.update(_prefixed(name, stream_summary(_arrow_source(column))))
    out.update(
        _prefixed(
            "int64_nullable",
            stream_summary(_arrow_source(pa.array(ints, pa.int64()), nullable=True)),
        )
    )
    return out


# ---------------------------------------------------------------------------
# Group: pipeline — end-to-end identity through the public interface
# ---------------------------------------------------------------------------


@function_pod(output_keys="ratio")
def golden_ratio(age: int, weight: float) -> float:
    """Function pod pinned by the pipeline group. DO NOT MOVE."""
    return weight / age


@function_pod(output_keys="adjusted")
def golden_adjust(age: int, weight: float, score: float) -> float:
    """Function pod pinned by the pipeline group. DO NOT MOVE."""
    return weight * score + age


SUBJECT_ROWS = [
    {"subject": "s1", "age": 3, "weight": 20.5},
    {"subject": "s2", "age": 4, "weight": 22.25},
    {"subject": "s3", "age": 5, "weight": 19.0},
]
SCAN_ROWS = [
    {"subject": "s1", "score": 0.5},
    {"subject": "s2", "score": 1.5},
]
GROUPED_ROWS = [
    {"cohort": "a", "subject": "s1", "weight": 20.5},
    {"cohort": "a", "subject": "s2", "weight": 22.25},
    {"cohort": "b", "subject": "s3", "weight": 19.0},
]


def _subjects() -> Any:
    from orcapod.sources import DictSource

    return DictSource(data=SUBJECT_ROWS, tag_columns=["subject"], source_id="subjects")


def _scans() -> Any:
    from orcapod.sources import DictSource

    return DictSource(data=SCAN_ROWS, tag_columns=["subject"], source_id="scans")


def _grouped() -> Any:
    from orcapod.sources import DictSource

    return DictSource(
        data=GROUPED_ROWS, tag_columns=["cohort", "subject"], source_id="cohorts"
    )


@case("pipeline", "sources")
def pipeline_sources() -> dict[str, Any]:
    out = _prefixed("subjects", stream_summary(_subjects()))
    out.update(_prefixed("scans", stream_summary(_scans())))
    out.update(_prefixed("cohorts", stream_summary(_grouped())))
    return out


@case("pipeline", "pod")
def pipeline_pod() -> dict[str, Any]:
    return stream_summary(golden_ratio.pod(_subjects()))


@case("pipeline", "pod_default_version")
def pipeline_pod_default_version() -> dict[str, Any]:
    """Pin one uri for one function whatever the entry point (ITL-942 finding N).

    ``FunctionPod.from_fn`` and ``@function_pod`` both default to version ``v0.0``,
    so the same function gets the same uri, and therefore the same table paths.

    Returns:
        The uri of the pod built each way.
    """
    from orcapod import FunctionPod

    built = FunctionPod.from_fn(
        golden_ratio,
        output_keys="ratio",
        name="golden_ratio",
    )
    return {"from_fn.uri": list(built.uri), "decorator.uri": list(golden_ratio.pod.uri)}


@case("pipeline", "join_pod")
def pipeline_join_pod() -> dict[str, Any]:
    from orcapod.operators import Join

    joined = Join()(_subjects(), _scans())
    out = _prefixed("join", stream_summary(joined))
    out.update(_prefixed("pod", stream_summary(golden_adjust.pod(joined))))
    return out


@case("pipeline", "merge_join")
def pipeline_merge_join() -> dict[str, Any]:
    from orcapod.operators import MergeJoin, SelectDataColumns

    left = SelectDataColumns(columns=["weight"])(_subjects())
    from orcapod.sources import DictSource

    right = DictSource(
        data=[{"subject": "s1", "weight": 30.0}, {"subject": "s3", "weight": 31.5}],
        tag_columns=["subject"],
        source_id="reweigh",
    )
    return stream_summary(MergeJoin()(left, right))


@case("pipeline", "semi_join")
def pipeline_semi_join() -> dict[str, Any]:
    from orcapod.operators import SemiJoin

    return stream_summary(SemiJoin()(_subjects(), _scans()))


@case("pipeline", "batch")
def pipeline_batch() -> dict[str, Any]:
    from orcapod.operators import Batch

    return stream_summary(Batch(batch_size=2)(_subjects()))


@case("pipeline", "group_by")
def pipeline_group_by() -> dict[str, Any]:
    from orcapod.operators import GroupBy

    return stream_summary(GroupBy(by=["cohort"])(_grouped()))


@case("pipeline", "index_pick")
def pipeline_index_pick() -> dict[str, Any]:
    from orcapod.operators import Index, Pick
    from orcapod.sources import DictSource

    source = DictSource(
        data=[
            {"subject": "s1", "values": [1, 2, 3], "meta": {"k": 10, "j": 11}},
            {"subject": "s2", "values": [4, 5], "meta": {"k": 20, "j": 21}},
        ],
        tag_columns=["subject"],
        source_id="lists",
    )
    out = _prefixed("index", stream_summary(Index(column="values", i=0)(source)))
    out.update(_prefixed("pick", stream_summary(Pick(column="meta", key="k")(source))))
    return out


@case("pipeline", "map_data")
def pipeline_map_data() -> dict[str, Any]:
    from orcapod.operators import MapData

    return stream_summary(MapData(name_map={"age": "age_years"})(_subjects()))


@case("pipeline", "column_selection")
def pipeline_column_selection() -> dict[str, Any]:
    from orcapod.operators import (
        DropDataColumns,
        DropTagColumns,
        SelectDataColumns,
        SelectTagColumns,
    )

    out = _prefixed(
        "select_data", stream_summary(SelectDataColumns(columns=["age"])(_subjects()))
    )
    out.update(
        _prefixed(
            "drop_data", stream_summary(DropDataColumns(columns=["age"])(_subjects()))
        )
    )
    out.update(
        _prefixed(
            "select_tag",
            stream_summary(SelectTagColumns(columns=["cohort"])(_grouped())),
        )
    )
    out.update(
        _prefixed(
            "drop_tag", stream_summary(DropTagColumns(columns=["subject"])(_grouped()))
        )
    )
    # Added by ITL-942 (finding F): every spelling of one set of names has one
    # identity; these equal the sorted-list entries.
    for spelling, columns in {
        "tuple": ("weight", "age"),
        "set": {"weight", "age"},
        "unsorted": ["weight", "age"],
    }.items():
        stream = SelectDataColumns(columns=columns)(_subjects())
        out[f"select_data_spelling.{spelling}.pipeline_hash"] = (
            stream.pipeline_hash().to_string()
        )
    out["select_data_spelling.sorted.pipeline_hash"] = (
        SelectDataColumns(columns=["age", "weight"])(_subjects())
        .pipeline_hash()
        .to_string()
    )
    return out


@case("pipeline", "polars_filter")
def pipeline_polars_filter() -> dict[str, Any]:
    from orcapod.operators import PolarsFilter

    return stream_summary(PolarsFilter(constraints={"cohort": "a"})(_grouped()))


@case("pipeline", "job")
def pipeline_job() -> dict[str, Any]:
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.operators import Join

    db = InMemoryArrowDatabase()
    job = PipelineJob(name="golden_job", store=db)
    with job:
        joined = Join(label="joined")(_subjects(), _scans())
        golden_adjust.pod(joined, label="adjust")
    job.run()

    out: dict[str, Any] = {}
    for label in sorted(job.nodes):
        out.update(_prefixed(f"node[{label}]", stream_summary(job.nodes[label])))

    paths = sorted(db.list_sources())
    out["table_paths"] = ["/".join(p) for p in paths]
    for path in paths:
        name = "/".join(path)
        records = db.get_all_records(path, record_id_column="__golden_record_id")
        assert records is not None
        if "__input_data_hash" in records.column_names and "_result" in path:
            # Result-table record ids are UUIDv7 (volatile); pin the lookup key instead.
            out[f"table[{name}].input_data_hashes"] = sorted(
                _jsonable(v) for v in records.column("__input_data_hash").to_pylist()
            )
        else:
            out[f"table[{name}].record_ids"] = sorted(
                _jsonable(v) for v in records.column("__golden_record_id").to_pylist()
            )
    return out


# ---------------------------------------------------------------------------
# Polars values (ITL-578) — semantic, schema and datagram entries
# ---------------------------------------------------------------------------
#
# Each Polars case is separate from the groups' general cases, so running the generator
# against a release without the Polars logical types (``--allow-errors``) loses only these
# entries. The hash of a Polars value is a cache key carrying the label
# ``polars_dataframe_v1`` / ``polars_series_v1`` (``orcapod.logical_types.polars_type``); an
# intended change to it is a new definition under new labels, never new bytes under these.


def _polars_values() -> dict[str, Any]:
    """Polars values whose hashes are pinned: one per dtype family the grammar names."""
    import datetime as dt
    import decimal

    import polars as pl

    return {
        "simple": pl.DataFrame(
            {"x": pl.Series([1, 2, 3], dtype=pl.Int64), "y": ["a", "b", "c"]}
        ),
        "nulls_and_temporal": pl.DataFrame(
            {
                "i": pl.Series([1, None, -3], dtype=pl.Int8),
                "t": pl.Series(
                    [dt.datetime(2020, 1, 1, 12), None, dt.datetime(1999, 12, 31)],
                    dtype=pl.Datetime("ms", "America/New_York"),
                ),
                "d": pl.Series(
                    [dt.timedelta(seconds=1), dt.timedelta(0), None],
                    dtype=pl.Duration("us"),
                ),
                "m": pl.Series(
                    [decimal.Decimal("1.25"), None, decimal.Decimal("-0.50")],
                    dtype=pl.Decimal(10, 2),
                ),
            }
        ),
        "nested_and_categorical": pl.DataFrame(
            {
                "e": pl.Series(["x", "y", None], dtype=pl.Enum(["y", "x", "z"])),
                "c": pl.Series(["p", None, "q"], dtype=pl.Categorical),
                "a": pl.Series([[1, 2], [3, 4], None], dtype=pl.Array(pl.Int16, 2)),
                "s": pl.Series(
                    [{"u": 1, "l": [1.5]}, None, {"u": None, "l": []}],
                    dtype=pl.Struct({"u": pl.UInt16, "l": pl.List(pl.Float32)}),
                ),
                "n": pl.Series([None, None, None], dtype=pl.Null),
            }
        ),
        "empty_with_schema": pl.DataFrame(schema={"x": pl.Int32, "b": pl.Binary}),
        "series": pl.Series("metric", [1.5, None, 3.0], dtype=pl.Float32),
    }


def _polars_types() -> dict[str, Any]:
    """Polars classes whose type, stored Arrow type and ``Schema`` hashes are pinned."""
    import polars as pl

    return {"dataframe": pl.DataFrame, "series": pl.Series}


@case("semantic", "polars_value")
def semantic_polars_values() -> dict[str, Any]:
    """Semantic hash of each Polars value (the ``PolarsDataFrameHandler`` digest)."""
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(v).to_string() for k, v in _polars_values().items()}


@case("semantic", "polars_type")
def semantic_polars_types() -> dict[str, Any]:
    """Semantic hash of the ``pl.DataFrame`` and ``pl.Series`` type objects."""
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(t).to_string() for k, t in _polars_types().items()}


@case("schema", "polars_single_field")
def schema_polars_single_field() -> dict[str, Any]:
    """Stored Arrow type and single-field ``Schema`` hash of each Polars class."""
    from orcapod.types import Schema

    ctx = _ctx()
    out: dict[str, Any] = {}
    for name, tp in _polars_types().items():
        out[f"{name}.arrow_type"] = _arrow_type_text(
            ctx.type_converter.python_type_to_arrow_type(tp)
        )
        out[f"{name}.schema_hash"] = ctx.semantic_hasher.hash_object(
            Schema({"c": tp})
        ).to_string()
    return out


@case("datagram", "polars_data_hash")
def datagram_polars_data_hash() -> dict[str, Any]:
    """Content hash of the ``Data`` holding each Polars value in its only data column."""
    from orcapod.sources import DictSource

    out: dict[str, Any] = {}
    for name, value in _polars_values().items():
        source = DictSource(
            [{"k": "a", "v": value}],
            tag_columns=["k"],
            data_schema={"k": str, "v": type(value)},
        )
        ((_, data),) = list(source.iter_data())
        out[name] = data.content_hash().to_string()
    return out


def _polars_rows() -> dict[str, list[dict[str, Any]]]:
    """Rows of the Polars-valued sources whose identities are pinned."""
    import polars as pl

    return {
        "polars_dataframe": [
            {"k": "r1", "c": pl.DataFrame({"a": [1, 2]})},
            {"k": "r2", "c": pl.DataFrame({"a": [3], "b": ["x"]})},
        ],
        "polars_series": [
            {"k": "r1", "c": pl.Series("a", [1, 2])},
            {"k": "r2", "c": pl.Series("b", ["x"])},
        ],
    }


def _make_polars_datagram_case(name: str) -> CaseFn:
    """Build the case summarizing the Polars-valued ``DictSource`` named ``name``.

    Args:
        name: A key of ``_polars_rows()``.

    Returns:
        A case function returning the source's ``stream_summary``.
    """

    def run() -> dict[str, Any]:
        """Return the ``stream_summary`` of the Polars-valued source."""
        import polars as pl

        from orcapod.sources import DictSource

        column_type = pl.DataFrame if name == "polars_dataframe" else pl.Series
        source = DictSource(
            data=_polars_rows()[name],
            tag_columns=["k"],
            data_schema={"k": str, "c": column_type},
            source_id=f"golden_{name}",
        )
        return stream_summary(source)

    return run


for _name in ("polars_dataframe", "polars_series"):
    case("datagram", _name)(_make_polars_datagram_case(_name))


@case("arrow", "polars_extension_no_values")
def arrow_polars_extension_columns_without_values() -> dict[str, Any]:
    """Table hashes of Polars-typed columns with zero rows or nulls only.

    The Polars counterpart of ``arrow:extension_no_values``: such a column has no cell
    token, so its logical type enters the hash through the hashed field alone (ITL-967).
    """
    ctx = _ctx()
    entries: dict[str, Any] = {}
    for rows, label in ((0, "zero_rows"), (2, "all_null")):
        for name, python_type in _polars_types().items():
            arrow_type = ctx.type_converter.python_type_to_arrow_type(python_type)
            storage = pa.array([None] * rows, arrow_type.storage_type)
            column = pa.ExtensionArray.from_storage(arrow_type, storage)
            entries[f"{label}.{name}"] = ctx.arrow_hasher.hash_table(
                pa.table({"c": column})
            ).to_string()
    return entries


def _polars_exprs() -> dict[str, Any]:
    """Build representative Polars expression predicates, keyed by name."""
    from datetime import date, datetime, timezone

    import polars as pl

    return {
        "gt_dyn_int": pl.col("x") > 0,
        "gt_dyn_float": pl.col("x") > 0.5,
        "gt_int8": pl.col("x") > pl.lit(0, dtype=pl.Int8),
        "eq_string": pl.col("s") == "abc",
        "and_two_columns": (pl.col("x") > 0) & (pl.col("y") < 5),
        "is_in_strings": pl.col("s").is_in(["a", "b"]),
        "is_between_dates": pl.col("d").is_between(
            date(2024, 1, 1), date(2024, 12, 31)
        ),
        "gt_datetime_utc": pl.col("t") > datetime(2024, 1, 1, tzinfo=timezone.utc),
        "is_not_null_cast": pl.col("x").cast(pl.Float32).is_not_null(),
        "str_contains": pl.col("s").str.contains("^a"),
    }


@case("semantic", "polars_expr")
def semantic_polars_exprs() -> dict[str, Any]:
    """Record the ``polars_expr_v1`` semantic hash of each representative expression."""
    hasher = _ctx().semantic_hasher
    return {k: hasher.hash_object(e).to_string() for k, e in _polars_exprs().items()}


@case("pipeline", "polars_filter_expr")
def pipeline_polars_filter_expr() -> dict[str, Any]:
    """Record expression-only and expression-plus-keyword filter streams."""
    import polars as pl

    from orcapod.operators import PolarsFilter

    out = _prefixed(
        "expr",
        stream_summary(PolarsFilter(predicates=[pl.col("weight") > 20])(_grouped())),
    )
    out.update(
        _prefixed(
            "mixed",
            stream_summary(
                PolarsFilter(
                    predicates=[pl.col("weight") > 20], constraints={"cohort": "a"}
                )(_grouped())
            ),
        )
    )
    return out


@case("pipeline", "polars_filter_expr_job")
def pipeline_polars_filter_expr_job() -> dict[str, Any]:
    """Record a job with an expression filter upstream of a join and a function pod."""
    import polars as pl

    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.operators import Join

    db = InMemoryArrowDatabase()
    job = PipelineJob(name="golden_filter_job", store=db)
    with job:
        older = _subjects().polars_filter(pl.col("age") > 3, label="older")
        joined = Join(label="joined")(older, _scans())
        golden_adjust.pod(joined, label="adjust")
    job.run()

    out: dict[str, Any] = {}
    for label in sorted(job.nodes):
        out.update(_prefixed(f"node[{label}]", stream_summary(job.nodes[label])))
    paths = sorted(db.list_sources())
    out["table_paths"] = ["/".join(p) for p in paths]
    for path in paths:
        name = "/".join(path)
        records = db.get_all_records(path, record_id_column="__golden_record_id")
        assert records is not None
        if "__input_data_hash" in records.column_names and "_result" in path:
            # Result-table record ids are UUIDv7 (volatile); pin the lookup key instead.
            out[f"table[{name}].input_data_hashes"] = sorted(
                _jsonable(v) for v in records.column("__input_data_hash").to_pylist()
            )
        else:
            out[f"table[{name}].record_ids"] = sorted(
                _jsonable(v) for v in records.column("__golden_record_id").to_pylist()
            )
    return out


# ---------------------------------------------------------------------------
# Group: arrow — value-hashed logical types in a column that holds no value
# ---------------------------------------------------------------------------


@case("arrow", "extension_no_values")
def arrow_extension_columns_without_values() -> dict[str, Any]:
    """Table hashes of value-hashed logical-type columns with zero rows or nulls only.

    Such a column has no cell token, so its logical type enters the hash through
    the hashed field alone (ITL-967). Every entry differs from every other and from
    the plain ``large_binary`` entries.
    """
    import orcapod as op

    ctx = _ctx()
    types: dict[str, Any] = {
        "uuid": uuid.UUID,
        "file": op.File,
        "directory": op.Directory,
        "ndarray": np.ndarray,
        "dataframe": pd.DataFrame,
        "series": pd.Series,
        "list_of_file": list[op.File],
        "set_of_uuid": set[uuid.UUID],
    }
    entries: dict[str, Any] = {}
    for rows, label in ((0, "zero_rows"), (2, "all_null")):
        for name, python_type in types.items():
            arrow_type = ctx.type_converter.python_type_to_arrow_type(python_type)
            storage = pa.array([None] * rows, arrow_type.storage_type)
            column = pa.ExtensionArray.from_storage(arrow_type, storage)
            entries[f"{label}.{name}"] = ctx.arrow_hasher.hash_table(
                pa.table({"c": column})
            ).to_string()
        entries[f"{label}.plain_large_binary"] = ctx.arrow_hasher.hash_table(
            pa.table({"c": pa.array([None] * rows, pa.large_binary())})
        ).to_string()
    return entries


def _nullable_readings() -> Any:
    """A source with a nullable tag and two nullable data columns, each with a null."""
    from orcapod.sources import ArrowTableSource

    table = pa.table(
        {
            "k": pa.array(["r1", "r2", "r3", "r4"], pa.large_string()),
            "site": pa.array(["a", "b", "c", None], pa.large_string()),
            "x": pa.array([1, None, 3, 4], pa.int64()),
            "y": pa.array([1.5, 2.5, None, 4.5], pa.float64()),
        },
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("site", pa.large_string(), nullable=True),
                pa.field("x", pa.int64(), nullable=True),
                pa.field("y", pa.float64(), nullable=True),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["k", "site"], source_id="golden_nullable"
    )


@case("pipeline", "drop_nulls")
def pipeline_drop_nulls() -> dict[str, Any]:
    """Pin ``DropNulls``: the operator's own identity and the narrowed rows."""
    from orcapod.operators import DropNulls

    out = _prefixed("source", stream_summary(_nullable_readings()))
    out["operator[x].content_hash"] = DropNulls("x").content_hash().to_string()
    out["operator[x,y].content_hash"] = DropNulls(["y", "x"]).content_hash().to_string()
    out.update(_prefixed("data", stream_summary(DropNulls("x")(_nullable_readings()))))
    out.update(
        _prefixed(
            "data_many", stream_summary(DropNulls(["x", "y"])(_nullable_readings()))
        )
    )
    out.update(
        _prefixed("tag", stream_summary(_nullable_readings().drop_nulls("site")))
    )
    return out


@case("pipeline", "drop_nulls_all")
def pipeline_drop_nulls_all() -> dict[str, Any]:
    """Pin the no-argument ``DropNulls``: its fixed identity and the narrowed rows."""
    from orcapod.operators import DropNulls

    out: dict[str, Any] = {}
    out["operator[all].content_hash"] = DropNulls().content_hash().to_string()
    out["operator[all].pipeline_hash"] = DropNulls().pipeline_hash().to_string()
    out.update(_prefixed("all", stream_summary(DropNulls()(_nullable_readings()))))
    out.update(_prefixed("method", stream_summary(_nullable_readings().drop_nulls())))
    return out


@case("pipeline", "drop_nulls_job")
def pipeline_drop_nulls_job() -> dict[str, Any]:
    """Record a job with ``DropNulls`` upstream of a pod with non-optional parameters."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    db = InMemoryArrowDatabase()
    job = PipelineJob(name="golden_drop_null_job", store=db)
    with job:
        present = (
            _nullable_readings()
            .drop_nulls(["x", "y"], label="present")
            .map_data({"x": "age", "y": "weight"}, label="renamed")
        )
        golden_ratio.pod(present, label="ratio")
    job.run()

    out: dict[str, Any] = {}
    for label in sorted(job.nodes):
        out.update(_prefixed(f"node[{label}]", stream_summary(job.nodes[label])))
    paths = sorted(db.list_sources())
    out["table_paths"] = ["/".join(p) for p in paths]
    for path in paths:
        name = "/".join(path)
        records = db.get_all_records(path, record_id_column="__golden_record_id")
        assert records is not None
        if "__input_data_hash" in records.column_names and "_result" in path:
            # Result-table record ids are UUIDv7 (volatile); pin the lookup key instead.
            out[f"table[{name}].input_data_hashes"] = sorted(
                _jsonable(v) for v in records.column("__input_data_hash").to_pylist()
            )
        else:
            out[f"table[{name}].record_ids"] = sorted(
                _jsonable(v) for v in records.column("__golden_record_id").to_pylist()
            )
    return out


@case("pipeline", "default_source_ids")
def pipeline_default_source_ids() -> dict[str, Any]:
    """Pin the default source ids built from a hash (ITL-957).

    A source built with no ``source_id`` is named ``<source kind>:<algorithm
    label>:<hex>``; a source made with ``node.as_source()`` is named
    ``derived:<algorithm label>:<hex>``.
    """
    import polars as pl

    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.operators import Join
    from orcapod.sources import (
        ArrowTableSource,
        DataFrameSource,
        DictSource,
        ListSource,
    )

    sources: dict[str, Any] = {
        "arrow_table": ArrowTableSource(_builder_table(), tag_columns=["subject"]),
        "dict": DictSource(data=SUBJECT_ROWS, tag_columns=["subject"]),
        "list": ListSource("weight", [20.5, 22.25, 19.0]),
        "data_frame": DataFrameSource(
            pl.DataFrame(SUBJECT_ROWS), tag_columns=["subject"]
        ),
    }
    out: dict[str, Any] = {}
    for name, source in sources.items():
        out[f"{name}.source_id"] = source.source_id
        out[f"{name}.content_hash"] = source.content_hash().to_string()

    job = PipelineJob(name="golden_default_source_ids", store=InMemoryArrowDatabase())
    with job:
        joined = Join(label="joined")(_subjects(), _scans())
        golden_adjust.pod(joined, label="adjust")
    job.run()
    out["as_source[operator_node].source_id"] = (
        job.nodes["joined"].as_source().source_id
    )
    out["as_source[function_node].source_id"] = (
        job.nodes["adjust"].as_source().source_id
    )
    return out
