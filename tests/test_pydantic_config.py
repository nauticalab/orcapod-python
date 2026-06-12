"""Tests for orcapod.pydantic_config (ENG-607)."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pydantic
import pytest

from orcapod.pydantic_config import OrcapodBaseConfig, PydanticModelConverter, load_pydantic_config


class SampleConfig(OrcapodBaseConfig):
    name: str
    threshold: float
    retries: int = 3


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(text, encoding="utf-8")
    return p


def test_loads_valid_config(tmp_path):
    path = _write(tmp_path, "name: run1\nthreshold: 6.0\n")
    cfg = load_pydantic_config(path, SampleConfig)
    assert isinstance(cfg, SampleConfig)
    assert cfg.name == "run1"
    assert cfg.threshold == 6.0
    assert cfg.retries == 3  # default applied


def test_wrong_type_raises_with_path(tmp_path):
    path = _write(tmp_path, "name: run1\nthreshold: not-a-number\n")
    with pytest.raises(ValueError) as exc:
        load_pydantic_config(path, SampleConfig)
    assert "threshold" in str(exc.value)
    assert str(path) in str(exc.value)


def test_unknown_key_raises(tmp_path):
    path = _write(tmp_path, "name: run1\nthreshold: 6.0\ntypo_key: 1\n")
    with pytest.raises(ValueError) as exc:
        load_pydantic_config(path, SampleConfig)
    assert "typo_key" in str(exc.value)


def test_missing_required_raises(tmp_path):
    path = _write(tmp_path, "threshold: 6.0\n")
    with pytest.raises(ValueError) as exc:
        load_pydantic_config(path, SampleConfig)
    assert "name" in str(exc.value)


def test_missing_file_raises_value_error(tmp_path):
    missing = tmp_path / "does_not_exist.yaml"
    with pytest.raises(ValueError) as exc:
        load_pydantic_config(missing, SampleConfig)
    assert str(missing) in str(exc.value)


def test_empty_file_raises_value_error(tmp_path):
    path = _write(tmp_path, "")
    with pytest.raises(ValueError) as exc:
        load_pydantic_config(path, SampleConfig)
    assert str(path) in str(exc.value)


def _converter() -> PydanticModelConverter:
    return PydanticModelConverter()


def test_converter_python_type_and_struct_signature():
    conv = _converter()
    assert conv.python_type is pydantic.BaseModel
    sig = conv.arrow_struct_type
    assert pa.types.is_struct(sig)
    assert {f.name for f in sig} == {"__pydantic_model__", "__pydantic_json__"}
    assert all(f.type == pa.large_string() for f in sig)


def test_converter_can_handle_model_subclass():
    conv = _converter()
    assert conv.can_handle_python_type(SampleConfig) is True
    assert conv.can_handle_python_type(int) is False


def test_converter_roundtrip_model_to_struct_to_model():
    conv = _converter()
    cfg = SampleConfig(name="run1", threshold=6.0, retries=5)
    struct = conv.python_to_struct_dict(cfg)
    assert set(struct.keys()) == {"__pydantic_model__", "__pydantic_json__"}
    assert struct["__pydantic_model__"].endswith(":SampleConfig")
    restored = conv.struct_dict_to_python(struct)
    assert isinstance(restored, SampleConfig)
    assert restored == cfg


def test_converter_can_handle_struct_type_and_is_semantic_struct():
    conv = _converter()
    assert conv.can_handle_struct_type(conv.arrow_struct_type) is True
    assert conv.can_handle_struct_type(pa.struct([pa.field("path", pa.large_string())])) is False
    cfg = SampleConfig(name="x", threshold=1.0)
    assert conv.is_semantic_struct(conv.python_to_struct_dict(cfg)) is True
    assert conv.is_semantic_struct({"path": "/tmp/x"}) is False
