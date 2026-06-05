# tests/test_semantic_types/test_dataclass_encoding.py
from __future__ import annotations

import dataclasses
import os
import tempfile
import typing
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod.semantic_types.dataclass_encoding import (
    DATACLASS_TYPE_FIELD,
    DATACLASS_TYPE_PREFIX,
    _DATACLASS_REGISTRY,
    dataclass_to_arrow_struct_type,
    dataclass_to_struct_dict,
    has_dataclass_type_sentinel,
    register_dataclass,
    struct_dict_to_dataclass,
)
import orcapod.semantic_types.dataclass_encoding as _dc_enc
from orcapod.semantic_types.universal_converter import UniversalTypeConverter
from orcapod.types import Schema


@dataclasses.dataclass
class _Simple:
    a: int
    b: str


def test_constants():
    assert DATACLASS_TYPE_FIELD == "__dataclass."
    assert DATACLASS_TYPE_PREFIX == "dataclass:"


def test_register_explicit():
    register_dataclass(_Simple)
    key = f"{_Simple.__module__}.{_Simple.__qualname__}"
    assert _DATACLASS_REGISTRY[key] is _Simple


def test_register_returns_class():
    result = register_dataclass(_Simple)
    assert result is _Simple


def test_register_as_decorator():
    @register_dataclass
    @dataclasses.dataclass
    class _Decorated:
        x: float

    key = f"{_Decorated.__module__}.{_Decorated.__qualname__}"
    assert _DATACLASS_REGISTRY[key] is _Decorated


def test_register_non_dataclass_raises():
    with pytest.raises(TypeError, match="not a dataclass"):
        register_dataclass(int)


def test_sentinel_large_string():
    t = pa.struct([pa.field("__dataclass.", pa.large_string()), pa.field("a", pa.int64())])
    assert has_dataclass_type_sentinel(t) is True


def test_sentinel_string_compat():
    # older Arrow versions wrote pa.string() instead of pa.large_string()
    t = pa.struct([pa.field("__dataclass.", pa.string()), pa.field("a", pa.int64())])
    assert has_dataclass_type_sentinel(t) is True


def test_sentinel_missing_field():
    t = pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.large_string())])
    assert has_dataclass_type_sentinel(t) is False


def test_sentinel_non_struct():
    assert has_dataclass_type_sentinel(pa.int64()) is False


def test_struct_type_basic_fields():
    @dataclasses.dataclass
    class _Point:
        x: int
        y: float

    converter = UniversalTypeConverter()
    result = dataclass_to_arrow_struct_type(_Point, converter)

    assert pa.types.is_struct(result)
    # __dataclass. must be the first field
    assert result[0].name == "__dataclass."
    assert result[0].type == pa.large_string()
    assert result.field("x").type == pa.int64()
    assert result.field("y").type == pa.float64()


def test_struct_type_string_field():
    @dataclasses.dataclass
    class _Named:
        name: str

    converter = UniversalTypeConverter()
    result = dataclass_to_arrow_struct_type(_Named, converter)
    assert result.field("name").type == pa.large_string()


def test_struct_type_non_dataclass_raises():
    converter = UniversalTypeConverter()
    with pytest.raises(TypeError, match="not a dataclass"):
        dataclass_to_arrow_struct_type(int, converter)


def _build_field_converters(cls: type, converter: UniversalTypeConverter) -> dict:
    """Helper: build per-field Python-to-Arrow converters for a dataclass."""
    hints = typing.get_type_hints(cls)
    return {
        f.name: converter.get_python_to_arrow_converter(hints[f.name])
        for f in dataclasses.fields(cls)
    }


def test_struct_dict_simple():
    @dataclasses.dataclass
    class _Box:
        width: int
        label: str

    converter = UniversalTypeConverter()
    field_converters = _build_field_converters(_Box, converter)
    obj = _Box(width=10, label="big")
    result = dataclass_to_struct_dict(obj, field_converters)

    fqcn = f"{_Box.__module__}.{_Box.__qualname__}"
    assert result[DATACLASS_TYPE_FIELD] == f"dataclass:{fqcn}"
    assert result["width"] == 10
    assert result["label"] == "big"


def test_struct_dict_type_error_on_class():
    with pytest.raises(TypeError, match="not a dataclass instance"):
        dataclass_to_struct_dict(_Simple, {})


def test_struct_dict_type_error_on_non_dataclass():
    with pytest.raises(TypeError, match="not a dataclass instance"):
        dataclass_to_struct_dict(42, {})


@dataclasses.dataclass
class _TierOne:
    value: int


def test_tier1_import():
    """Tier 1: class is importable via importlib."""
    fqcn = f"{_TierOne.__module__}.{_TierOne.__qualname__}"
    struct_dict = {
        "__dataclass.": f"dataclass:{fqcn}",
        "value": 7,
    }
    field_converters = {"value": lambda v: v}
    cache: dict = {}

    # Patch importlib so tier 1 returns _TierOne
    module_path, _, class_attr = fqcn.rpartition(".")
    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        mock_mod = MagicMock()
        setattr(mock_mod, class_attr, _TierOne)
        mock_import.return_value = mock_mod

        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert isinstance(result, _TierOne)
    assert result.value == 7
    # Cache should be populated
    assert cache[fqcn] is _TierOne


def test_tier1_cache_hit():
    """Tier 1: cache hit skips importlib entirely."""
    fqcn = "some.module.SomeClass"
    cache = {fqcn: _TierOne}
    struct_dict = {"__dataclass.": f"dataclass:{fqcn}", "value": 3}
    field_converters = {"value": lambda v: v}

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)
        mock_import.assert_not_called()

    assert isinstance(result, _TierOne)
    assert result.value == 3


def test_tier2_registry(monkeypatch):
    """Tier 2: importlib fails, class found in registry."""
    @dataclasses.dataclass
    class _RegClass:
        score: float

    fqcn = "fake.module.RegClass"
    monkeypatch.setitem(_DATACLASS_REGISTRY, fqcn, _RegClass)

    struct_dict = {"__dataclass.": f"dataclass:{fqcn}", "score": 9.5}
    field_converters = {"score": lambda v: v}
    cache: dict = {}

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module", side_effect=ImportError("no module")):
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert isinstance(result, _RegClass)
    assert result.score == 9.5
    assert cache[fqcn] is _RegClass


def test_tier3_synthesize():
    """Tier 3: neither importable nor registered — synthesize a dataclass."""
    fqcn = "totally.unknown.Ghost"
    struct_dict = {"__dataclass.": f"dataclass:{fqcn}", "name": "phantom", "age": 99}
    field_converters = {"name": lambda v: v, "age": lambda v: v}
    cache: dict = {}

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module", side_effect=ImportError("no module")):
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert dataclasses.is_dataclass(result)
    assert result.name == "phantom"  # type: ignore[attr-defined]
    assert result.age == 99  # type: ignore[attr-defined]
    # Synthesized class cached under fqcn for future rows
    assert fqcn in cache


def test_missing_type_field_tier3():
    """Struct without __type falls through to tier 3 silently."""
    struct_dict = {"value": 42}
    field_converters = {"value": lambda v: v}
    cache: dict = {}

    result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert dataclasses.is_dataclass(result)
    assert result.value == 42  # type: ignore[attr-defined]
    # No cache entry — no valid fqcn to cache under
    assert len(cache) == 0


def test_malformed_type_field_tier3():
    """Invalid __dataclass. format (fails regex) falls through to tier 3."""
    struct_dict = {"__dataclass.": "not-valid!!!", "x": 1}
    field_converters = {"x": lambda v: v}
    cache: dict = {}

    result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert dataclasses.is_dataclass(result)
    assert result.x == 1  # type: ignore[attr-defined]


def test_utc_simple_round_trip():
    """Full encode->decode round-trip through UniversalTypeConverter."""
    @dataclasses.dataclass
    class _Color:
        r: int
        g: int
        b: int

    converter = UniversalTypeConverter()
    arrow_type = converter.python_type_to_arrow_type(_Color)
    assert has_dataclass_type_sentinel(arrow_type)

    obj = _Color(r=255, g=128, b=0)
    encode = converter.get_python_to_arrow_converter(_Color)
    encoded = encode(obj)
    assert encoded["__dataclass."] == f"dataclass:{_Color.__module__}.{_Color.__qualname__}"

    decode = converter.get_arrow_to_python_converter(arrow_type)
    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        mock_mod = MagicMock()
        setattr(mock_mod, "_Color", _Color)
        mock_import.return_value = mock_mod
        result = decode(encoded)

    assert isinstance(result, _Color)
    assert result.r == 255 and result.g == 128 and result.b == 0


def test_utc_nested_round_trip():
    """Nested dataclass encodes and decodes recursively."""
    @dataclasses.dataclass
    class _Inner:
        y: float

    @dataclasses.dataclass
    class _Outer:
        x: int
        inner: _Inner

    converter = UniversalTypeConverter()
    arrow_type = converter.python_type_to_arrow_type(_Outer)

    # Nested struct: inner field should itself be a __type-bearing struct
    inner_arrow = arrow_type.field("inner").type
    assert has_dataclass_type_sentinel(inner_arrow)

    obj = _Outer(x=1, inner=_Inner(y=3.14))
    encode = converter.get_python_to_arrow_converter(_Outer)
    encoded = encode(obj)

    assert encoded["inner"]["__dataclass."] == f"dataclass:{_Inner.__module__}.{_Inner.__qualname__}"
    assert encoded["inner"]["y"] == 3.14

    decode = converter.get_arrow_to_python_converter(arrow_type)

    inner_fqcn = f"{_Inner.__module__}.{_Inner.__qualname__}"
    outer_fqcn = f"{_Outer.__module__}.{_Outer.__qualname__}"
    inner_attr = inner_fqcn.rpartition(".")[2]
    outer_attr = outer_fqcn.rpartition(".")[2]

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        def fake_import(module_path):
            mod = MagicMock()
            setattr(mod, inner_attr, _Inner)
            setattr(mod, outer_attr, _Outer)
            return mod
        mock_import.side_effect = fake_import
        result = decode(encoded)

    assert isinstance(result, _Outer)
    assert result.x == 1
    assert isinstance(result.inner, _Inner)
    assert result.inner.y == 3.14


def test_utc_clear_cache_clears_dataclass_cache():
    """clear_cache() also clears the per-instance dataclass lookup cache."""
    converter = UniversalTypeConverter()

    @dataclasses.dataclass
    class _Temp:
        n: int

    fqcn = f"{_Temp.__module__}.{_Temp.__qualname__}"
    converter._dataclass_lookup_cache[fqcn] = _Temp
    converter.clear_cache()
    assert fqcn not in converter._dataclass_lookup_cache


def test_polymorphic_decode():
    """Two rows with different __type values each decode to their own class."""
    @dataclasses.dataclass
    class _Cat:
        name: str

    @dataclasses.dataclass
    class _Dog:
        name: str

    cat_fqcn = f"{_Cat.__module__}.{_Cat.__qualname__}"
    dog_fqcn = f"{_Dog.__module__}.{_Dog.__qualname__}"

    # Both have the same Arrow schema (name: large_string) plus __dataclass.
    arrow_type = pa.struct([
        pa.field("__dataclass.", pa.large_string()),
        pa.field("name", pa.large_string()),
    ])
    converter = UniversalTypeConverter()
    decode = converter.get_arrow_to_python_converter(arrow_type)

    cat_attr = cat_fqcn.rpartition(".")[2]
    dog_attr = dog_fqcn.rpartition(".")[2]

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        def fake_import(module_path):
            mod = MagicMock()
            setattr(mod, cat_attr, _Cat)
            setattr(mod, dog_attr, _Dog)
            return mod
        mock_import.side_effect = fake_import

        row0 = decode({"__dataclass.": f"dataclass:{cat_fqcn}", "name": "Whiskers"})
        row1 = decode({"__dataclass.": f"dataclass:{dog_fqcn}", "name": "Rex"})

    assert isinstance(row0, _Cat) and row0.name == "Whiskers"
    assert isinstance(row1, _Dog) and row1.name == "Rex"


@pytest.mark.integration
def test_parquet_round_trip():
    """Full round-trip: python_dicts_to_arrow_table -> Parquet -> arrow_table_to_python_dicts."""
    import pyarrow.parquet as pq

    @dataclasses.dataclass
    class _Record:
        score: float
        label: str

    converter = UniversalTypeConverter()

    python_dicts = [
        {"rec": _Record(score=0.9, label="good")},
        {"rec": _Record(score=0.1, label="bad")},
    ]
    python_schema = Schema({"rec": _Record})
    table = converter.python_dicts_to_arrow_table(python_dicts, python_schema=python_schema)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.parquet")
        pq.write_table(table, path)
        loaded = pq.read_table(path)

    rec_fqcn = f"{_Record.__module__}.{_Record.__qualname__}"
    rec_attr = rec_fqcn.rpartition(".")[2]

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        mod = MagicMock()
        setattr(mod, rec_attr, _Record)
        mock_import.return_value = mod
        results = converter.arrow_table_to_python_dicts(loaded)

    assert len(results) == 2
    assert isinstance(results[0]["rec"], _Record)
    assert results[0]["rec"].score == 0.9
    assert results[0]["rec"].label == "good"
    assert isinstance(results[1]["rec"], _Record)
    assert results[1]["rec"].score == 0.1
    assert results[1]["rec"].label == "bad"


# ---------------------------------------------------------------------------
# init=False field exclusion
# ---------------------------------------------------------------------------


def test_struct_type_excludes_init_false_fields():
    """dataclass_to_arrow_struct_type must not include fields with init=False."""
    @dataclasses.dataclass
    class _WithComputed:
        value: int
        cached: str = dataclasses.field(init=False, default="")

        def __post_init__(self) -> None:
            self.cached = f"v={self.value}"

    converter = UniversalTypeConverter()
    result = dataclass_to_arrow_struct_type(_WithComputed, converter)

    field_names = [result.field(i).name for i in range(result.num_fields)]
    assert "__dataclass." in field_names
    assert "value" in field_names
    assert "cached" not in field_names, "init=False field must be excluded from Arrow schema"


def test_struct_dict_excludes_init_false_fields():
    """dataclass_to_struct_dict must not include fields with init=False."""
    @dataclasses.dataclass
    class _WithComputed:
        value: int
        cached: str = dataclasses.field(init=False, default="")

        def __post_init__(self) -> None:
            self.cached = f"v={self.value}"

    obj = _WithComputed(value=42)
    result = dataclass_to_struct_dict(obj, {})

    assert "value" in result
    assert "cached" not in result, "init=False field must be excluded from encoded dict"


def test_utc_converter_excludes_init_false_fields():
    """UniversalTypeConverter converter closure must not include init=False fields."""
    @dataclasses.dataclass
    class _WithComputed:
        x: int
        derived: str = dataclasses.field(init=False, default="")

        def __post_init__(self) -> None:
            self.derived = str(self.x * 2)

    converter = UniversalTypeConverter()
    encode = converter.get_python_to_arrow_converter(_WithComputed)
    encoded = encode(_WithComputed(x=7))

    assert "x" in encoded
    assert "derived" not in encoded, "init=False field must not appear in encoded output"


def test_init_false_round_trip():
    """Full round-trip: init=False field is excluded from Arrow and reconstructed post-init."""
    @dataclasses.dataclass
    class _Computed:
        n: int
        doubled: int = dataclasses.field(init=False)

        def __post_init__(self) -> None:
            self.doubled = self.n * 2

    converter = UniversalTypeConverter()
    arrow_type = converter.python_type_to_arrow_type(_Computed)

    # Arrow schema must not contain 'doubled'
    field_names = [arrow_type.field(i).name for i in range(arrow_type.num_fields)]
    assert "doubled" not in field_names

    obj = _Computed(n=5)
    encode = converter.get_python_to_arrow_converter(_Computed)
    encoded = encode(obj)
    assert "doubled" not in encoded

    decode = converter.get_arrow_to_python_converter(arrow_type)
    fqcn = f"{_Computed.__module__}.{_Computed.__qualname__}"
    attr = fqcn.rpartition(".")[2]
    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as m:
        mod = MagicMock()
        setattr(mod, attr, _Computed)
        m.return_value = mod
        result = decode(encoded)

    assert isinstance(result, _Computed)
    assert result.n == 5
    # __post_init__ recomputes doubled
    assert result.doubled == 10


# ---------------------------------------------------------------------------
# Extra-field / superset-schema kwargs filtering in decoder
# ---------------------------------------------------------------------------


def test_decoder_extra_null_field_no_warning(caplog):
    """A NULL extra field (schema evolution — column present but empty for this row)
    is silently dropped without a warning."""
    @dataclasses.dataclass
    class _Narrow:
        name: str

    fqcn = f"{_Narrow.__module__}.{_Narrow.__qualname__}"
    struct_dict = {"__dataclass.": f"dataclass:{fqcn}", "name": "Alice", "age": None}
    field_converters = {"name": lambda v: v, "age": lambda v: v}
    cache: dict = {}

    attr = fqcn.rpartition(".")[2]
    import logging
    with caplog.at_level(logging.WARNING, logger="orcapod.semantic_types.dataclass_encoding"):
        with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as m:
            mod = MagicMock()
            setattr(mod, attr, _Narrow)
            m.return_value = mod
            result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert isinstance(result, _Narrow)
    assert result.name == "Alice"
    # No warning for a null extra field
    assert not any("age" in r.message for r in caplog.records)


def test_decoder_extra_nonnull_field_warns(caplog):
    """A non-null extra field being discarded must emit a WARNING — it signals a
    schema mismatch or encoding bug, not normal schema evolution."""
    @dataclasses.dataclass
    class _Narrow:
        name: str

    fqcn = f"{_Narrow.__module__}.{_Narrow.__qualname__}"
    # 'age' is non-null: real data being silently dropped is a bug signal
    struct_dict = {"__dataclass.": f"dataclass:{fqcn}", "name": "Alice", "age": 30}
    field_converters = {"name": lambda v: v, "age": lambda v: v}
    cache: dict = {}

    attr = fqcn.rpartition(".")[2]
    import logging
    with caplog.at_level(logging.WARNING, logger="orcapod.semantic_types.dataclass_encoding"):
        with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as m:
            mod = MagicMock()
            setattr(mod, attr, _Narrow)
            m.return_value = mod
            result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert isinstance(result, _Narrow)
    assert result.name == "Alice"
    assert not hasattr(result, "age")
    # Must emit a warning mentioning the dropped field
    assert any("age" in r.message and r.levelno == logging.WARNING for r in caplog.records)


# ---------------------------------------------------------------------------
# Tier-1 import gate (_TIER1_IMPORT_ENABLED)
# ---------------------------------------------------------------------------


def test_tier1_disabled_skips_to_tier2(monkeypatch):
    """When _TIER1_IMPORT_ENABLED is False, tier-1 import is skipped and tier-2 is used."""
    @dataclasses.dataclass
    class _GatedClass:
        val: int

    fqcn = "some.module.GatedClass"
    monkeypatch.setitem(_DATACLASS_REGISTRY, fqcn, _GatedClass)
    monkeypatch.setattr(_dc_enc, "_TIER1_IMPORT_ENABLED", False)

    struct_dict = {"__dataclass.": f"dataclass:{fqcn}", "val": 99}
    field_converters = {"val": lambda v: v}
    cache: dict = {}

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)
        mock_import.assert_not_called()

    assert isinstance(result, _GatedClass)
    assert result.val == 99


def test_tier1_disabled_falls_to_tier3(monkeypatch):
    """When _TIER1_IMPORT_ENABLED is False and class is unregistered, tier-3 synthesizes."""
    monkeypatch.setattr(_dc_enc, "_TIER1_IMPORT_ENABLED", False)

    fqcn = "totally.absent.UnknownClass"
    struct_dict = {"__dataclass.": f"dataclass:{fqcn}", "score": 7.5}
    field_converters = {"score": lambda v: v}
    cache: dict = {}

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)
        mock_import.assert_not_called()

    assert dataclasses.is_dataclass(result)
    assert result.score == 7.5  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# arrow_schema_to_python_schema for dataclass structs (Item 4 fix)
# ---------------------------------------------------------------------------


def test_arrow_schema_to_python_schema_dataclass_returns_concrete_type():
    """arrow_schema_to_python_schema returns a concrete dataclass type for sentinel structs.

    After the fix, converting a dataclass struct Arrow type back to a Python
    schema must return a proper @dataclass type rather than typing.Any, so
    that python_schema_to_arrow_schema can complete the round-trip.
    """
    @dataclasses.dataclass
    class _Point:
        x: int
        y: float

    converter = UniversalTypeConverter()
    arrow_type = converter.python_type_to_arrow_type(_Point)
    assert has_dataclass_type_sentinel(arrow_type)

    # Build a one-field Arrow schema wrapping the struct
    arrow_schema = pa.schema([pa.field("point", arrow_type, nullable=False)])
    python_schema = converter.arrow_schema_to_python_schema(arrow_schema)

    python_type = python_schema["point"]
    assert dataclasses.is_dataclass(python_type), (
        f"Expected a dataclass type, got {python_type!r}"
    )
    field_names = {f.name for f in dataclasses.fields(python_type)}
    assert "x" in field_names
    assert "y" in field_names
    assert DATACLASS_TYPE_FIELD not in field_names, (
        "Sentinel field must not appear among the synthesized dataclass fields"
    )


def test_arrow_schema_to_python_schema_dataclass_round_trip():
    """python_schema → arrow_schema → python_schema is lossless for dataclass fields.

    After the fix, the synthesized dataclass type is itself a proper dataclass,
    so python_schema_to_arrow_schema can convert it back to the original struct.
    """
    @dataclasses.dataclass
    class _Box:
        width: int
        label: str

    converter = UniversalTypeConverter()
    original_arrow = converter.python_type_to_arrow_type(_Box)

    # Round-trip via python schema
    schema = pa.schema([pa.field("box", original_arrow, nullable=False)])
    python_schema = converter.arrow_schema_to_python_schema(schema)
    synthesized_type = python_schema["box"]

    assert dataclasses.is_dataclass(synthesized_type)
    # Convert the synthesized type back to Arrow — must produce the same struct
    recovered_arrow = converter.python_type_to_arrow_type(synthesized_type)
    assert has_dataclass_type_sentinel(recovered_arrow)
    assert recovered_arrow.field("width").type == pa.int64()
    assert recovered_arrow.field("label").type == pa.large_string()


def test_arrow_schema_to_python_schema_dataclass_nullable_fields():
    """Nullable struct fields produce Optional[T] annotations in the synthesized dataclass.

    Regression guard for the nullability fix: when a dataclass-sentinel struct has a
    nullable field, the synthesized Python dataclass must annotate it as ``Optional[T]``
    so that:
    - The type correctly conveys that None is a valid value.
    - Round-trips through ``python_schema_to_arrow_schema`` preserve ``nullable=True``
      (because ``Optional[T]`` triggers ``_is_optional_type``).

    Non-nullable fields must remain plain ``T`` (not Optional).
    """
    converter = UniversalTypeConverter()

    # Build a raw dataclass-sentinel struct type manually with mixed nullability.
    import pyarrow as _pa
    struct_type = _pa.struct([
        _pa.field(DATACLASS_TYPE_FIELD, _pa.large_string()),     # sentinel (excluded)
        _pa.field("required_field", _pa.int64(), nullable=False),
        _pa.field("optional_field", _pa.int64(), nullable=True),
    ])
    assert has_dataclass_type_sentinel(struct_type)

    synthesized = converter.arrow_type_to_python_type(struct_type)
    assert dataclasses.is_dataclass(synthesized)

    field_map = {f.name: f.type for f in dataclasses.fields(synthesized)}

    # Non-nullable field must be plain int (or equivalent), not Optional.
    import typing as _typing
    required_type = field_map["required_field"]
    assert _typing.get_origin(required_type) is not _typing.Union, (
        "required_field (nullable=False) must not be Optional"
    )

    # Nullable field must be Optional[T].
    optional_type = field_map["optional_field"]
    assert _typing.get_origin(optional_type) is _typing.Union, (
        "optional_field (nullable=True) must be Optional[T]"
    )
    non_none_args = [a for a in _typing.get_args(optional_type) if a is not type(None)]
    assert len(non_none_args) == 1, "Optional[T] must wrap exactly one non-None type"

    # Sentinel must not appear in the synthesized dataclass fields.
    assert DATACLASS_TYPE_FIELD not in field_map


def test_two_distinct_dataclass_columns_no_collision():
    """Two dataclass columns with different schemas are synthesized as distinct types.

    Regression test for the hash-based naming fix: when an Arrow schema contains
    two struct columns that both have the dataclass sentinel but different fields,
    ``arrow_schema_to_python_schema`` must return two *different* Python types —
    one per column — rather than returning the same cached class for both.
    """
    @dataclasses.dataclass
    class _Alpha:
        x: int
        y: float

    @dataclasses.dataclass
    class _Beta:
        name: str
        count: int

    converter = UniversalTypeConverter()
    alpha_arrow = converter.python_type_to_arrow_type(_Alpha)
    beta_arrow = converter.python_type_to_arrow_type(_Beta)

    # Both columns carry the dataclass sentinel.
    assert has_dataclass_type_sentinel(alpha_arrow)
    assert has_dataclass_type_sentinel(beta_arrow)

    # Place both in the same Arrow schema (simulating two dataclass columns in one table).
    schema = pa.schema([
        pa.field("col_a", alpha_arrow, nullable=False),
        pa.field("col_b", beta_arrow, nullable=False),
    ])
    python_schema = converter.arrow_schema_to_python_schema(schema)

    type_a = python_schema["col_a"]
    type_b = python_schema["col_b"]

    # Both must be synthesized dataclasses …
    assert dataclasses.is_dataclass(type_a), f"col_a type is not a dataclass: {type_a!r}"
    assert dataclasses.is_dataclass(type_b), f"col_b type is not a dataclass: {type_b!r}"

    # … but they must be *different* types (no name collision in the lookup cache).
    assert type_a is not type_b, (
        "Both dataclass columns resolved to the same synthesized class — "
        "hash-based naming is required to prevent this collision."
    )

    # Verify that the field sets are correct for each synthesized type.
    fields_a = {f.name for f in dataclasses.fields(type_a)}
    fields_b = {f.name for f in dataclasses.fields(type_b)}
    assert fields_a == {"x", "y"}
    assert fields_b == {"name", "count"}
    # Sentinel must not leak into either synthesized type.
    assert DATACLASS_TYPE_FIELD not in fields_a
    assert DATACLASS_TYPE_FIELD not in fields_b
