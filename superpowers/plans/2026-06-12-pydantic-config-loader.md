# Pydantic Config Loader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reusable, pydantic-backed config facility to orcapod-python: validate a YAML config against a pydantic schema into a typed model at build time, and make a validated model a first-class, content-hashed orcapod value that pods receive already deserialized.

**Architecture:** A new module `orcapod/pydantic_config.py` provides `load_pydantic_config()` (YAML → validated model), an optional strict base `OrcapodBaseConfig`, and a `PydanticModelConverter` semantic-type converter modeled on `PythonPathStructConverter`. The converter maps any `pydantic.BaseModel` ⇄ an Arrow struct holding the model's fully-qualified class name plus canonical JSON, content-hashing the canonical JSON so identity tracks config *meaning*, not YAML formatting. The converter is registered in the production semantic registry (`contexts/data/v0.1.json`) so the existing `UniversalTypeConverter` and `StarfixArrowHasher` pick it up automatically.

**Tech Stack:** Python 3.12, pydantic v2, PyArrow, PyYAML, pytest, uv.

**Spec:** `superpowers/specs/2026-06-12-pydantic-config-loader-design.md` (ENG-607).

**Conventions:** Run everything via `uv run`. Google-style docstrings, no ReST roles. Conventional Commits. End commit messages with the `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` trailer.

---

## File Structure

- Create: `src/orcapod/pydantic_config.py` — `load_pydantic_config`, `OrcapodBaseConfig`, `PydanticModelConverter`.
- Create: `tests/test_pydantic_config.py` — loader + converter unit/integration tests.
- Modify: `pyproject.toml` — add `pydantic>=2` to `dependencies`.
- Modify: `src/orcapod/contexts/data/v0.1.json` — register the `pydantic` converter in `semantic_registry.converters` (production path).
- Modify: `src/orcapod/hashing/versioned_hashers.py:135-138` — register the converter in the standalone fallback registry for consistency.

---

### Task 1: Add the pydantic dependency

**Files:**
- Modify: `pyproject.toml` (the `dependencies` list, ~line 9-28)

- [ ] **Step 1: Add the dependency**

In `pyproject.toml`, add to the `dependencies` array (e.g. after the `"deltalake>=1.0.2",` line):

```toml
    "pydantic>=2",
```

- [ ] **Step 2: Sync the environment**

Run: `uv sync`
Expected: resolves and installs pydantic 2.x with no conflict.

- [ ] **Step 3: Verify import**

Run: `uv run python -c "import pydantic; print(pydantic.VERSION)"`
Expected: prints a `2.x` version string.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore(deps): add pydantic for typed config loading (ENG-607)"
```

---

### Task 2: `load_pydantic_config` + `OrcapodBaseConfig`

**Files:**
- Create: `src/orcapod/pydantic_config.py`
- Test: `tests/test_pydantic_config.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_pydantic_config.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pydantic_config.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'orcapod.pydantic_config'`.

- [ ] **Step 3: Implement the loader + base**

Create `src/orcapod/pydantic_config.py`:

```python
"""Pydantic-backed config loading for orcapod pipelines (ENG-601 / ENG-607).

Provides `load_pydantic_config` (validate a YAML file against a pydantic model)
and `OrcapodBaseConfig` (a strict base for config schemas). A companion
`PydanticModelConverter` (also in this module) makes a validated model a
first-class, content-hashed orcapod value.
"""

from __future__ import annotations

from pathlib import Path
from typing import TypeVar

import pydantic
import yaml

M = TypeVar("M", bound=pydantic.BaseModel)


class OrcapodBaseConfig(pydantic.BaseModel):
    """Recommended base for pipeline config schemas.

    Defaults to strict validation: unknown keys are rejected and instances are
    immutable. Subclass this for pipeline configs; subclass `pydantic.BaseModel`
    directly only when different semantics are required.
    """

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)


def load_pydantic_config(path: str | Path, model_cls: type[M]) -> M:
    """Read a YAML file and validate it against a pydantic model.

    Args:
        path: Path to the YAML config file.
        model_cls: The pydantic model class to validate against.

    Returns:
        A validated instance of `model_cls`.

    Raises:
        ValueError: If the YAML cannot be parsed or fails validation. The error
            message includes the file path and the underlying field-level detail.
    """
    path = Path(path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Could not parse YAML config {path}: {e}") from e

    try:
        return model_cls.model_validate(data)
    except pydantic.ValidationError as e:
        raise ValueError(f"Config validation failed for {path}:\n{e}") from e
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pydantic_config.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pydantic_config.py tests/test_pydantic_config.py
git commit -m "feat(pydantic_config): add load_pydantic_config and OrcapodBaseConfig (ENG-607)"
```

---

### Task 3: `PydanticModelConverter` — model ⇄ Arrow struct round-trip

**Files:**
- Modify: `src/orcapod/pydantic_config.py`
- Test: `tests/test_pydantic_config.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pydantic_config.py`:

```python
import pyarrow as pa

from orcapod.pydantic_config import PydanticModelConverter


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pydantic_config.py -q`
Expected: FAIL — `ImportError: cannot import name 'PydanticModelConverter'`.

- [ ] **Step 3: Implement the converter**

Append to `src/orcapod/pydantic_config.py`:

```python
import importlib
from typing import Any

from orcapod.semantic_types.semantic_struct_converters import (
    SemanticStructConverterBase,
)

# Arrow struct field names for the serialized config.
_MODEL_FIELD = "__pydantic_model__"  # fully-qualified "module:QualName"
_JSON_FIELD = "__pydantic_json__"    # canonical JSON of the model


def _qualified_name(cls: type) -> str:
    return f"{cls.__module__}:{cls.__qualname__}"


def _import_model(qualified_name: str) -> type[pydantic.BaseModel]:
    module_path, _, qualname = qualified_name.partition(":")
    module = importlib.import_module(module_path)
    obj: Any = module
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj


class PydanticModelConverter(SemanticStructConverterBase):
    """Semantic-type converter for pydantic models.

    Maps any `pydantic.BaseModel` instance to an Arrow struct holding the
    model's fully-qualified class name and its canonical JSON, and back. Content
    is hashed over (class name + canonical JSON), so identity tracks the config's
    meaning rather than source-file formatting. Modeled on `PythonPathStructConverter`.
    """

    def __init__(self) -> None:
        super().__init__("pydantic")
        import pyarrow as pa

        self._arrow_struct_type = pa.struct(
            [
                pa.field(_MODEL_FIELD, pa.large_string()),
                pa.field(_JSON_FIELD, pa.large_string()),
            ]
        )

    @property
    def python_type(self) -> type:
        return pydantic.BaseModel

    @property
    def arrow_struct_type(self) -> "Any":
        return self._arrow_struct_type

    def can_handle_python_type(self, python_type: type) -> bool:
        return isinstance(python_type, type) and issubclass(
            python_type, pydantic.BaseModel
        )

    def can_handle_struct_type(self, struct_type: "Any") -> bool:
        import pyarrow as pa

        if not pa.types.is_struct(struct_type):
            return False
        for field in self._arrow_struct_type:
            if (
                field.name not in struct_type.names
                or struct_type[field.name].type != field.type
            ):
                return False
        return True

    def is_semantic_struct(self, struct_dict: dict[str, Any]) -> bool:
        return set(struct_dict.keys()) == {_MODEL_FIELD, _JSON_FIELD}

    def python_to_struct_dict(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, pydantic.BaseModel):
            raise TypeError(f"Expected a pydantic BaseModel, got {type(value)}")
        return {
            _MODEL_FIELD: _qualified_name(type(value)),
            _JSON_FIELD: value.model_dump_json(),
        }

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> Any:
        qualified_name = struct_dict.get(_MODEL_FIELD)
        json_str = struct_dict.get(_JSON_FIELD)
        if qualified_name is None or json_str is None:
            raise ValueError(
                f"Missing '{_MODEL_FIELD}'/'{_JSON_FIELD}' in struct dict"
            )
        model_cls = _import_model(qualified_name)
        return model_cls.model_validate_json(json_str)

    def hash_struct_dict(
        self, struct_dict: dict[str, Any], add_prefix: bool = False
    ) -> str:
        qualified_name = struct_dict.get(_MODEL_FIELD)
        json_str = struct_dict.get(_JSON_FIELD)
        if qualified_name is None or json_str is None:
            raise ValueError(
                f"Missing '{_MODEL_FIELD}'/'{_JSON_FIELD}' in struct dict"
            )
        content = f"{qualified_name}\n{json_str}".encode("utf-8")
        content_hash = self._compute_content_hash(content)
        return self._format_hash_string(content_hash.digest, add_prefix=add_prefix)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pydantic_config.py -q`
Expected: PASS (all tests, including Task 2's).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pydantic_config.py tests/test_pydantic_config.py
git commit -m "feat(pydantic_config): add PydanticModelConverter semantic type (ENG-607)"
```

---

### Task 4: Hash stability — meaning, not formatting

**Files:**
- Test: `tests/test_pydantic_config.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pydantic_config.py`:

```python
def test_hash_equal_for_equal_values():
    conv = _converter()
    a = conv.python_to_struct_dict(SampleConfig(name="run1", threshold=6.0, retries=5))
    b = conv.python_to_struct_dict(SampleConfig(name="run1", threshold=6.0, retries=5))
    assert conv.hash_struct_dict(a) == conv.hash_struct_dict(b)


def test_hash_differs_for_different_values():
    conv = _converter()
    a = conv.python_to_struct_dict(SampleConfig(name="run1", threshold=6.0))
    b = conv.python_to_struct_dict(SampleConfig(name="run1", threshold=7.0))
    assert conv.hash_struct_dict(a) != conv.hash_struct_dict(b)


def test_hash_stable_across_yaml_formatting(tmp_path):
    # Two YAMLs that differ only in comments / key order / whitespace
    # must produce the same validated model and therefore the same hash.
    yaml_a = "name: run1\nthreshold: 6.0\nretries: 5\n"
    yaml_b = "# a comment\nretries: 5\nthreshold:   6.0\nname: run1\n"
    pa_path = _write(tmp_path, yaml_a)
    cfg_a = load_pydantic_config(pa_path, SampleConfig)
    pb_path = tmp_path / "b.yaml"
    pb_path.write_text(yaml_b, encoding="utf-8")
    cfg_b = load_pydantic_config(pb_path, SampleConfig)

    conv = _converter()
    ha = conv.hash_struct_dict(conv.python_to_struct_dict(cfg_a))
    hb = conv.hash_struct_dict(conv.python_to_struct_dict(cfg_b))
    assert ha == hb
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest tests/test_pydantic_config.py -q`
Expected: PASS — the implementation from Task 3 already satisfies these (no new code needed). If `test_hash_stable_across_yaml_formatting` fails, it indicates `model_dump_json()` is non-deterministic for this model; investigate before proceeding.

- [ ] **Step 3: Commit**

```bash
git add tests/test_pydantic_config.py
git commit -m "test(pydantic_config): assert hash tracks config meaning, not formatting (ENG-607)"
```

---

### Task 5: Register the converter in the production + standalone registries

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json` (the `semantic_registry` → `_config` → `converters` object)
- Modify: `src/orcapod/hashing/versioned_hashers.py:135-138`
- Test: `tests/test_pydantic_config.py`

- [ ] **Step 1: Write the failing integration test**

Append to `tests/test_pydantic_config.py`:

```python
from orcapod.contexts import get_default_context
from orcapod.types import Schema


def test_registered_in_default_context_roundtrip():
    ctx = get_default_context()
    converter = ctx.type_converter

    cfg = SampleConfig(name="run1", threshold=6.0, retries=5)
    table = converter.python_dicts_to_arrow_table(
        [{"config": cfg}], python_schema=Schema({"config": SampleConfig})
    )
    # Stored as the pydantic struct, not an opaque blob.
    assert pa.types.is_struct(table.schema.field("config").type)
    assert {f.name for f in table.schema.field("config").type} == {
        "__pydantic_model__",
        "__pydantic_json__",
    }

    restored = converter.arrow_table_to_python_dicts(table)
    assert isinstance(restored[0]["config"], SampleConfig)
    assert restored[0]["config"] == cfg


def test_default_context_hashes_model_stably():
    ctx = get_default_context()
    converter = ctx.type_converter
    schema = Schema({"config": SampleConfig})
    t1 = converter.python_dicts_to_arrow_table(
        [{"config": SampleConfig(name="r", threshold=6.0)}], python_schema=schema
    )
    t2 = converter.python_dicts_to_arrow_table(
        [{"config": SampleConfig(name="r", threshold=6.0)}], python_schema=schema
    )
    h1 = ctx.arrow_hasher.hash_table(t1)
    h2 = ctx.arrow_hasher.hash_table(t2)
    assert h1 == h2
```

Note: if `arrow_hasher` exposes a different method than `hash_table`, adjust the last two lines to the actual public hashing entry point (confirm by reading `ctx.arrow_hasher`'s class). The first test is the load-bearing one.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pydantic_config.py -k "default_context" -q`
Expected: FAIL — the converter is not yet registered, so `python_dicts_to_arrow_table` does not produce the pydantic struct (it errors or produces a non-struct column).

- [ ] **Step 3: Register in the production JSON registry**

In `src/orcapod/contexts/data/v0.1.json`, inside `semantic_registry._config.converters` (alongside `"path"` and `"upath"`), add:

```json
      "pydantic": {
        "_class": "orcapod.pydantic_config.PydanticModelConverter",
        "_config": {}
      }
```

(Place it as a sibling key; mind the trailing commas so the JSON stays valid.)

- [ ] **Step 4: Register in the standalone fallback registry**

In `src/orcapod/hashing/versioned_hashers.py`, after the existing `registry.register_converter("path", path_converter)` (line ~138), add:

```python
    from orcapod.pydantic_config import PydanticModelConverter

    registry.register_converter("pydantic", PydanticModelConverter())
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_pydantic_config.py -q`
Expected: PASS (all tests).

- [ ] **Step 6: Run the semantic-types + contexts suites for regressions**

Run: `uv run pytest tests/test_semantic_types tests/test_hashing -q`
Expected: PASS (no regressions from the new registration).

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/contexts/data/v0.1.json src/orcapod/hashing/versioned_hashers.py tests/test_pydantic_config.py
git commit -m "feat(pydantic_config): register PydanticModelConverter in default registries (ENG-607)"
```

---

### Task 6: Full-suite verification + DESIGN_ISSUES note

**Files:**
- Modify: `DESIGN_ISSUES.md` (optional — only if a matching issue exists; otherwise skip)

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -m "not postgres" -q`
Expected: PASS (no regressions). Note skip counts as normal.

- [ ] **Step 2: Type-check the new module (if the repo runs a type checker in CI)**

Run: `uv run python -c "import orcapod.pydantic_config"`
Expected: imports cleanly. (If the repo uses pyright/mypy in CI, run that on `src/orcapod/pydantic_config.py` and fix any issues.)

- [ ] **Step 3: Final commit (only if Step 2 required edits)**

```bash
git add -A
git commit -m "chore(pydantic_config): satisfy type checker (ENG-607)"
```

---

## Self-Review

**Spec coverage:**
- Reusable loader in orcapod-python → Task 2 (`load_pydantic_config`). ✓
- Validate at build time, clear field-located error → Task 2 tests (wrong type / unknown key / missing required, path in message). ✓
- Typed config is a first-class, content-hashed pod input → Task 3 (converter) + Task 5 (registration; round-trip + struct storage). ✓
- Pods receive the typed model with no per-pod deserialization → Task 5 `test_registered_in_default_context_roundtrip` proves automatic reconstruction via the type converter (the actual pod parameter wiring is the spike-sorting follow-up, out of scope here). ✓
- Hash over meaning, not formatting → Task 4 (`test_hash_stable_across_yaml_formatting`). ✓
- `OrcapodBaseConfig` strict base → Task 2. ✓
- Add pydantic dependency → Task 1. ✓

**Out of scope (correctly deferred):** spike-sorting `config/` models, source swap, pod annotations, enigma-ephys migration (separate follow-up per spec).

**Type consistency:** `PydanticModelConverter` uses `_MODEL_FIELD`/`_JSON_FIELD` consistently across `python_to_struct_dict`, `struct_dict_to_python`, `is_semantic_struct`, and `hash_struct_dict`. `python_type` returns `pydantic.BaseModel`; registry subclass-matching handles concrete subclasses (verified against `SemanticTypeRegistry.get_converter_for_python_type`).

**Known verification point:** Task 5 Step 1 notes the `arrow_hasher` hashing method name (`hash_table`) must be confirmed against the concrete hasher class; the primary round-trip assertion does not depend on it.
