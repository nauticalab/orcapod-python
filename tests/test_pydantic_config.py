"""Tests for orcapod.pydantic_config (ENG-607)."""

from __future__ import annotations

from pathlib import Path

import pytest

from orcapod.pydantic_config import OrcapodBaseConfig, load_pydantic_config


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
