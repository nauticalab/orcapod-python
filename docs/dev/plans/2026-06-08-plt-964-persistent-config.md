# PLT-964: Persistent OrcaPod Configuration System — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure `OrcapodConfig` into a nested sectioned config (`HashingConfig` + `DisplayConfig`), add a `from_dict()` classmethod, update all consumers, and add `load_config()` for TOML-backed persistent configuration.

**Architecture:** `config.py` is rewritten with three frozen dataclasses (`HashingConfig`, `DisplayConfig`, `OrcapodConfig`) and two new functions (`OrcapodConfig.from_dict()`, `load_config()`). Consumer files are updated in a single mechanical sweep to use new field paths. The file-loading layer uses Python's built-in `tomllib` (Python ≥3.11) — no new dependencies.

**Tech Stack:** Python 3.11+, `tomllib` (stdlib), `dataclasses`, `logging`, `uv run pytest`.

**Spec:** `superpowers/specs/2026-06-08-plt-964-persistent-config-design.md`

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/orcapod/config.py` | Full rewrite | `HashingConfig`, `DisplayConfig`, nested `OrcapodConfig`, `from_dict()`, `load_config()` |
| `src/orcapod/__init__.py` | Modify | Export `HashingConfig`, `DisplayConfig`, `load_config` |
| `src/orcapod/core/sources/stream_builder.py` | Modify | `.schema_hash_n_char` → `.hashing.schema_n_char`; `.path_hash_n_char` → `.hashing.path_n_char` |
| `src/orcapod/core/nodes/source_node.py` | Modify | `.schema_hash_n_char` → `.hashing.schema_n_char` |
| `src/orcapod/core/operators/join.py` | Modify | `.system_tag_hash_n_char` → `.hashing.system_tag_n_char` (×3) |
| `src/orcapod/core/operators/merge_join.py` | Modify | `.system_tag_hash_n_char` → `.hashing.system_tag_n_char` (×2) |
| `src/orcapod/core/nodes/function_node.py` | Modify | `OrcapodConfig(**config_dict)` → `OrcapodConfig.from_dict(config_dict)` (×2) |
| `src/orcapod/core/nodes/operator_node.py` | Modify | `OrcapodConfig(**config_dict)` → `OrcapodConfig.from_dict(config_dict)` (×2) |
| `src/orcapod/utils/schema_utils.py` | Modify | Update docstring reference from `schema_hash_n_char` to `hashing.schema_n_char` |
| `tests/test_orcapod_config.py` | Full rewrite | New structural tests, `from_dict()` tests, `load_config()` tests |
| `tests/test_core/operators/test_operators.py` | Modify | `.system_tag_hash_n_char` → `.hashing.system_tag_n_char` (×2) |
| `tests/test_core/operators/test_merge_join.py` | Modify | `.system_tag_hash_n_char` → `.hashing.system_tag_n_char` (×1) |
| `tests/test_utils/test_schema_utils.py` | Modify | `DEFAULT_CONFIG.schema_hash_n_char` → `DEFAULT_CONFIG.hashing.schema_n_char` |

---

## Task 1: Rewrite `config.py` and `test_orcapod_config.py`

**Files:**
- Rewrite: `src/orcapod/config.py`
- Rewrite: `tests/test_orcapod_config.py`

After this task the new config tests pass; tests that reference old flat field names in
other files will fail — that is expected and fixed in Task 2.

- [ ] **Step 1: Rewrite `tests/test_orcapod_config.py`**

Replace the entire file with:

```python
"""Tests for HashingConfig, DisplayConfig, OrcapodConfig, and load_config (PLT-964)."""
from __future__ import annotations


class TestHashingConfig:
    def test_defaults(self):
        from orcapod.config import HashingConfig

        cfg = HashingConfig()
        assert cfg.system_tag_n_char == 12
        assert cfg.schema_n_char == 12
        assert cfg.path_n_char == 20

    def test_is_frozen(self):
        import pytest

        from orcapod.config import HashingConfig

        cfg = HashingConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.system_tag_n_char = 99  # type: ignore[misc]

    def test_merge_non_default_wins(self):
        from orcapod.config import HashingConfig

        base = HashingConfig()
        other = HashingConfig(system_tag_n_char=8)
        merged = base.merge(other)
        assert merged.system_tag_n_char == 8
        assert merged.schema_n_char == 12  # unchanged

    def test_merge_default_does_not_override(self):
        from orcapod.config import HashingConfig

        base = HashingConfig(system_tag_n_char=6)
        other = HashingConfig()  # all defaults
        merged = base.merge(other)
        assert merged.system_tag_n_char == 6  # base value preserved


class TestDisplayConfig:
    def test_defaults(self):
        from orcapod.config import DisplayConfig

        cfg = DisplayConfig()
        assert cfg.max_rows is None
        assert cfg.show_meta_columns is False
        assert cfg.show_source_columns is False
        assert cfg.show_system_tag_columns is False
        assert cfg.show_context_columns is False

    def test_is_frozen(self):
        import pytest

        from orcapod.config import DisplayConfig

        cfg = DisplayConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.max_rows = 10  # type: ignore[misc]

    def test_merge_non_default_wins(self):
        from orcapod.config import DisplayConfig

        base = DisplayConfig()
        other = DisplayConfig(max_rows=100, show_source_columns=True)
        merged = base.merge(other)
        assert merged.max_rows == 100
        assert merged.show_source_columns is True
        assert merged.show_meta_columns is False  # unchanged

    def test_merge_default_does_not_override(self):
        from orcapod.config import DisplayConfig

        base = DisplayConfig(max_rows=50)
        other = DisplayConfig()  # all defaults
        merged = base.merge(other)
        assert merged.max_rows == 50  # base value preserved


class TestOrcapodConfig:
    def test_defaults(self):
        from orcapod.config import DisplayConfig, HashingConfig, OrcapodConfig

        cfg = OrcapodConfig()
        assert cfg.hashing == HashingConfig()
        assert cfg.display == DisplayConfig()

    def test_default_config_is_orcapod_config_instance(self):
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        assert isinstance(DEFAULT_CONFIG, OrcapodConfig)

    def test_with_updates_hashing_section(self):
        from orcapod.config import HashingConfig, OrcapodConfig

        cfg = OrcapodConfig()
        updated = cfg.with_updates(hashing=HashingConfig(system_tag_n_char=8))
        assert updated.hashing.system_tag_n_char == 8
        assert cfg.hashing.system_tag_n_char == 12  # original unchanged

    def test_with_updates_display_section(self):
        from orcapod.config import DisplayConfig, OrcapodConfig

        cfg = OrcapodConfig()
        updated = cfg.with_updates(display=DisplayConfig(max_rows=50))
        assert updated.display.max_rows == 50
        assert cfg.display.max_rows is None  # original unchanged

    def test_merge_hashing_non_default_wins(self):
        from orcapod.config import HashingConfig, OrcapodConfig

        base = OrcapodConfig()
        other = OrcapodConfig(hashing=HashingConfig(system_tag_n_char=8))
        merged = base.merge(other)
        assert merged.hashing.system_tag_n_char == 8

    def test_merge_hashing_default_does_not_override(self):
        from orcapod.config import HashingConfig, OrcapodConfig

        base = OrcapodConfig(hashing=HashingConfig(system_tag_n_char=6))
        other = OrcapodConfig()
        merged = base.merge(other)
        assert merged.hashing.system_tag_n_char == 6

    def test_merge_display_non_default_wins(self):
        from orcapod.config import DisplayConfig, OrcapodConfig

        base = OrcapodConfig()
        other = OrcapodConfig(display=DisplayConfig(max_rows=100))
        merged = base.merge(other)
        assert merged.display.max_rows == 100

    def test_merge_type_error(self):
        import pytest

        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig()
        with pytest.raises(TypeError):
            cfg.merge("not a config")  # type: ignore[arg-type]


class TestOrcapodConfigFromDict:
    def test_empty_dict_returns_default(self):
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig

        assert OrcapodConfig.from_dict({}) == DEFAULT_CONFIG

    def test_partial_hashing_section(self):
        from orcapod.config import OrcapodConfig

        cfg = OrcapodConfig.from_dict({"hashing": {"system_tag_n_char": 8}})
        assert cfg.hashing.system_tag_n_char == 8
        assert cfg.hashing.schema_n_char == 12  # default preserved

    def test_full_sections_round_trip(self):
        from orcapod.config import OrcapodConfig

        data = {
            "hashing": {"system_tag_n_char": 8, "schema_n_char": 6, "path_n_char": 16},
            "display": {"max_rows": 100, "show_source_columns": True},
        }
        cfg = OrcapodConfig.from_dict(data)
        assert cfg.hashing.system_tag_n_char == 8
        assert cfg.hashing.schema_n_char == 6
        assert cfg.hashing.path_n_char == 16
        assert cfg.display.max_rows == 100
        assert cfg.display.show_source_columns is True

    def test_unknown_section_warns(self, caplog):
        import logging

        from orcapod.config import OrcapodConfig

        with caplog.at_level(logging.WARNING, logger="orcapod.config"):
            OrcapodConfig.from_dict({"typo_section": {"foo": 1}})
        assert "typo_section" in caplog.text

    def test_unknown_field_within_section_warns(self, caplog):
        import logging

        from orcapod.config import OrcapodConfig

        with caplog.at_level(logging.WARNING, logger="orcapod.config"):
            OrcapodConfig.from_dict({"display": {"max_rwos": 50}})
        assert "max_rwos" in caplog.text

    def test_source_path_appears_in_warning(self, caplog):
        import logging
        from pathlib import Path

        from orcapod.config import OrcapodConfig

        with caplog.at_level(logging.WARNING, logger="orcapod.config"):
            OrcapodConfig.from_dict(
                {"bad_section": {}}, source_path=Path("/some/config.toml")
            )
        assert "/some/config.toml" in caplog.text


class TestLoadConfig:
    def test_no_files_returns_default(self, tmp_path):
        from orcapod.config import DEFAULT_CONFIG, load_config

        result = load_config(
            project_config_path=tmp_path / "nonexistent.toml",
            user_config_path=tmp_path / "nonexistent.toml",
        )
        assert result == DEFAULT_CONFIG

    def test_user_global_config_applied(self, tmp_path):
        from orcapod.config import load_config

        user_cfg = tmp_path / "config.toml"
        user_cfg.write_text("[hashing]\nsystem_tag_n_char = 8\n")
        result = load_config(
            user_config_path=user_cfg,
            project_config_path=tmp_path / "nonexistent.toml",
        )
        assert result.hashing.system_tag_n_char == 8

    def test_project_local_config_applied(self, tmp_path):
        from orcapod.config import load_config

        project_cfg = tmp_path / "orcapod_config.toml"
        project_cfg.write_text("[display]\nmax_rows = 50\n")
        result = load_config(
            project_config_path=project_cfg,
            user_config_path=tmp_path / "nonexistent.toml",
        )
        assert result.display.max_rows == 50

    def test_project_local_wins_over_user_global(self, tmp_path):
        from orcapod.config import load_config

        user_cfg = tmp_path / "user.toml"
        user_cfg.write_text("[hashing]\nsystem_tag_n_char = 6\n")
        project_cfg = tmp_path / "project.toml"
        project_cfg.write_text("[hashing]\nsystem_tag_n_char = 10\n")
        result = load_config(user_config_path=user_cfg, project_config_path=project_cfg)
        assert result.hashing.system_tag_n_char == 10

    def test_malformed_toml_raises_value_error(self, tmp_path):
        import pytest

        from orcapod.config import load_config

        bad_cfg = tmp_path / "bad.toml"
        bad_cfg.write_text("this is not valid toml ][[[")
        with pytest.raises(ValueError, match=str(bad_cfg)):
            load_config(
                project_config_path=bad_cfg,
                user_config_path=tmp_path / "nonexistent.toml",
            )

    def test_explicit_paths_used(self, tmp_path):
        from orcapod.config import load_config

        cfg_file = tmp_path / "custom.toml"
        cfg_file.write_text("[display]\nshow_source_columns = true\n")
        result = load_config(
            project_config_path=cfg_file,
            user_config_path=tmp_path / "nonexistent.toml",
        )
        assert result.display.show_source_columns is True


class TestOrcapodConfigTopLevelExport:
    def test_can_import_orcapod_config(self):
        from orcapod import OrcapodConfig  # noqa: F401

    def test_can_import_hashing_config(self):
        from orcapod import HashingConfig  # noqa: F401

    def test_can_import_display_config(self):
        from orcapod import DisplayConfig  # noqa: F401

    def test_can_import_load_config(self):
        from orcapod import load_config  # noqa: F401

    def test_orcapod_config_in_all(self):
        import orcapod

        assert "OrcapodConfig" in orcapod.__all__

    def test_hashing_config_in_all(self):
        import orcapod

        assert "HashingConfig" in orcapod.__all__

    def test_display_config_in_all(self):
        import orcapod

        assert "DisplayConfig" in orcapod.__all__

    def test_load_config_in_all(self):
        import orcapod

        assert "load_config" in orcapod.__all__
```

- [ ] **Step 2: Run new tests to confirm they fail**

```bash
uv run pytest tests/test_orcapod_config.py -v
```

Expected: `ImportError` on `HashingConfig`, `DisplayConfig`, `load_config` — the new names don't exist yet.

- [ ] **Step 3: Rewrite `src/orcapod/config.py`**

Replace the entire file with:

```python
# config.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Self

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HashingConfig:
    """Hash truncation length settings.

    Controls the number of hex characters used when truncating hashes for
    system-tag column names, schema hashes, and database path scoping.
    """

    system_tag_n_char: int = 12
    schema_n_char: int = 12
    path_n_char: int = 20

    def merge(self, other: "HashingConfig") -> "HashingConfig":
        """Merge with another ``HashingConfig``; other takes precedence for non-default values.

        Args:
            other: Config to merge in. Must be a ``HashingConfig`` instance.

        Returns:
            New ``HashingConfig`` with non-default values from ``other`` applied.

        Raises:
            TypeError: If ``other`` is not a ``HashingConfig`` instance.
        """
        if not isinstance(other, HashingConfig):
            raise TypeError("Can only merge with another HashingConfig instance")
        defaults = HashingConfig()
        updates = {
            f: getattr(other, f)
            for f in self.__dataclass_fields__
            if getattr(other, f) != getattr(defaults, f)
        }
        return replace(self, **updates)


@dataclass(frozen=True)
class DisplayConfig:
    """Display and preview preference settings.

    Controls default row limits and column visibility when rendering streams
    and tables. These values are consumed by output methods when no explicit
    override is supplied by the caller.
    """

    max_rows: int | None = None
    show_meta_columns: bool = False
    show_source_columns: bool = False
    show_system_tag_columns: bool = False
    show_context_columns: bool = False

    def merge(self, other: "DisplayConfig") -> "DisplayConfig":
        """Merge with another ``DisplayConfig``; other takes precedence for non-default values.

        Args:
            other: Config to merge in. Must be a ``DisplayConfig`` instance.

        Returns:
            New ``DisplayConfig`` with non-default values from ``other`` applied.

        Raises:
            TypeError: If ``other`` is not a ``DisplayConfig`` instance.
        """
        if not isinstance(other, DisplayConfig):
            raise TypeError("Can only merge with another DisplayConfig instance")
        defaults = DisplayConfig()
        updates = {
            f: getattr(other, f)
            for f in self.__dataclass_fields__
            if getattr(other, f) != getattr(defaults, f)
        }
        return replace(self, **updates)


@dataclass(frozen=True)
class OrcapodConfig:
    """Top-level immutable OrcaPod configuration.

    Groups all configuration into typed sections. Construct directly for
    programmatic configuration, or use ``load_config()`` to load from TOML files.
    """

    hashing: HashingConfig = field(default_factory=HashingConfig)
    display: DisplayConfig = field(default_factory=DisplayConfig)

    def with_updates(self, **kwargs: Any) -> Self:
        """Create a new ``OrcapodConfig`` with updated section values.

        Args:
            **kwargs: Section keyword arguments (e.g. ``hashing=HashingConfig(...)``).

        Returns:
            New ``OrcapodConfig`` with the given sections replaced.
        """
        return replace(self, **kwargs)

    def merge(self, other: "OrcapodConfig") -> "OrcapodConfig":
        """Merge with another ``OrcapodConfig``; other takes precedence for non-default values.

        Merging is performed section by section via each section's own ``merge()``.
        Within each section, fields that are non-default in ``other`` override the
        corresponding fields in ``self``; fields at their default in ``other`` are
        left unchanged.

        Args:
            other: Config to merge in. Must be an ``OrcapodConfig`` instance.

        Returns:
            New ``OrcapodConfig`` with merged sections.

        Raises:
            TypeError: If ``other`` is not an ``OrcapodConfig`` instance.
        """
        if not isinstance(other, OrcapodConfig):
            raise TypeError("Can only merge with another OrcapodConfig instance")
        return OrcapodConfig(
            hashing=self.hashing.merge(other.hashing),
            display=self.display.merge(other.display),
        )

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        source_path: Path | str | None = None,
    ) -> "OrcapodConfig":
        """Construct an ``OrcapodConfig`` from a plain dict.

        Unknown top-level section names and unknown field names within a known
        section are logged at WARNING level and otherwise ignored (forward-compat:
        a config written by a newer orcapod will not break an older version).

        Args:
            data: Mapping of section name to field dict (e.g. as produced by
                ``dataclasses.asdict()`` or parsed from a TOML file).
            source_path: Optional file path included in warning messages to help
                users locate typos.

        Returns:
            ``OrcapodConfig`` populated from ``data``; missing sections and fields
            fall back to built-in defaults.
        """
        known_sections = {"hashing", "display"}
        path_str = f" in {source_path}" if source_path is not None else ""

        for key in data:
            if key not in known_sections:
                logger.warning(
                    "Unknown config section %r%s — ignored", key, path_str
                )

        hashing_dict = data.get("hashing", {})
        known_hashing = set(HashingConfig.__dataclass_fields__)
        for key in hashing_dict:
            if key not in known_hashing:
                logger.warning(
                    "Unknown field %r in [hashing]%s — ignored", key, path_str
                )
        hashing = HashingConfig(
            **{k: v for k, v in hashing_dict.items() if k in known_hashing}
        )

        display_dict = data.get("display", {})
        known_display = set(DisplayConfig.__dataclass_fields__)
        for key in display_dict:
            if key not in known_display:
                logger.warning(
                    "Unknown field %r in [display]%s — ignored", key, path_str
                )
        display = DisplayConfig(
            **{k: v for k, v in display_dict.items() if k in known_display}
        )

        return cls(hashing=hashing, display=display)


# Module-level default config — created at import time.
DEFAULT_CONFIG = OrcapodConfig()


def load_config(
    project_config_path: Path | str | None = None,
    user_config_path: Path | str | None = None,
) -> OrcapodConfig:
    """Load and merge config from TOML files with precedence.

    Precedence (lowest to highest):
      built-in defaults
      → user-global config (``~/.orcapod/config.toml``)
      → project-local config (``./orcapod_config.toml`` in cwd)

    Missing files are silently skipped. Malformed TOML raises ``ValueError``
    with the offending file path included in the message.

    Args:
        project_config_path: Override the project-local config file path.
            Defaults to ``orcapod_config.toml`` in the current working directory.
        user_config_path: Override the user-global config file path.
            Defaults to ``~/.orcapod/config.toml``.

    Returns:
        Merged ``OrcapodConfig`` with all applicable overrides applied.

    Raises:
        ValueError: If a config file exists but contains invalid TOML.
    """
    import tomllib

    _user_path = (
        Path(user_config_path)
        if user_config_path is not None
        else Path.home() / ".orcapod" / "config.toml"
    )
    _project_path = (
        Path(project_config_path)
        if project_config_path is not None
        else Path.cwd() / "orcapod_config.toml"
    )

    config = DEFAULT_CONFIG

    for path in (_user_path, _project_path):
        if not path.exists():
            continue
        try:
            with open(path, "rb") as f:
                data = tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise ValueError(f"Malformed TOML in {path}: {e}") from e
        overlay = OrcapodConfig.from_dict(data, source_path=path)
        config = config.merge(overlay)

    return config
```

- [ ] **Step 4: Run the new config tests**

```bash
uv run pytest tests/test_orcapod_config.py -v
```

Expected: All tests in `TestHashingConfig`, `TestDisplayConfig`, `TestOrcapodConfig`, `TestOrcapodConfigFromDict`, `TestLoadConfig` PASS. `TestOrcapodConfigTopLevelExport` tests that import from `orcapod` will FAIL (exports not updated yet — fixed in Task 3).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/config.py tests/test_orcapod_config.py
git commit -m "feat(config): restructure OrcapodConfig into nested HashingConfig/DisplayConfig sections; add from_dict() and load_config()"
```

---

## Task 2: Update all consumer files to new field paths

**Files:**
- Modify: `src/orcapod/core/sources/stream_builder.py`
- Modify: `src/orcapod/core/nodes/source_node.py`
- Modify: `src/orcapod/core/operators/join.py`
- Modify: `src/orcapod/core/operators/merge_join.py`
- Modify: `src/orcapod/core/nodes/function_node.py`
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Modify: `src/orcapod/utils/schema_utils.py`
- Modify: `tests/test_core/operators/test_operators.py`
- Modify: `tests/test_core/operators/test_merge_join.py`
- Modify: `tests/test_utils/test_schema_utils.py`

This task is mechanical — no new tests. The existing tests for these files are the regression suite.

- [ ] **Step 1: Record baseline failures from old field names**

```bash
uv run pytest tests/ -x -q 2>&1 | head -30
```

Expected: Multiple failures referencing `AttributeError: 'HashingConfig' object has no attribute 'system_tag_hash_n_char'` and similar.

- [ ] **Step 2: Update `stream_builder.py` (2 changes)**

In `src/orcapod/core/sources/stream_builder.py`:

Line 134 — change:
```python
            self._config.schema_hash_n_char,
```
to:
```python
            self._config.hashing.schema_n_char,
```

Line 142 — change:
```python
            source_id = table_hash.to_hex(char_count=self._config.path_hash_n_char)
```
to:
```python
            source_id = table_hash.to_hex(char_count=self._config.hashing.path_n_char)
```

- [ ] **Step 3: Update `source_node.py` (1 change)**

In `src/orcapod/core/nodes/source_node.py`:

Line 130 — change:
```python
            self.orcapod_config.schema_hash_n_char,
```
to:
```python
            self.orcapod_config.hashing.schema_n_char,
```

- [ ] **Step 4: Update `join.py` (3 changes)**

In `src/orcapod/core/operators/join.py`, change all three occurrences of:
```python
        n_char = self.orcapod_config.system_tag_hash_n_char
```
to:
```python
        n_char = self.orcapod_config.hashing.system_tag_n_char
```

Lines 98, 127, and 241.

- [ ] **Step 5: Update `merge_join.py` (2 changes)**

In `src/orcapod/core/operators/merge_join.py`, change both occurrences of:
```python
        n_char = self.orcapod_config.system_tag_hash_n_char
```
to:
```python
        n_char = self.orcapod_config.hashing.system_tag_n_char
```

Lines 140 and 159.

- [ ] **Step 6: Update `function_node.py` deserialization (2 changes)**

In `src/orcapod/core/nodes/function_node.py`, change both occurrences of:
```python
            OrcapodConfig(**config_dict) if config_dict is not None else DEFAULT_CONFIG
```
to:
```python
            OrcapodConfig.from_dict(config_dict) if config_dict is not None else DEFAULT_CONFIG
```

Lines 518 and 838.

- [ ] **Step 7: Update `operator_node.py` deserialization (2 changes)**

In `src/orcapod/core/nodes/operator_node.py`, change both occurrences of:
```python
            OrcapodConfig(**config_dict) if config_dict is not None else DEFAULT_CONFIG
```
to:
```python
            OrcapodConfig.from_dict(config_dict) if config_dict is not None else DEFAULT_CONFIG
```

Lines 420 and 692.

- [ ] **Step 8: Update `schema_utils.py` docstring (1 change)**

In `src/orcapod/utils/schema_utils.py`, line 404 — change:
```python
            (``OrcapodConfig.schema_hash_n_char``).
```
to:
```python
            (``OrcapodConfig.hashing.schema_n_char``).
```

- [ ] **Step 9: Update `test_operators.py` (2 changes)**

In `tests/test_core/operators/test_operators.py`, change both occurrences of:
```python
        n_char = OrcapodConfig().system_tag_hash_n_char
```
to:
```python
        n_char = OrcapodConfig().hashing.system_tag_n_char
```

Lines 1320 and 1415.

- [ ] **Step 10: Update `test_merge_join.py` (1 change)**

In `tests/test_core/operators/test_merge_join.py`, line 647 — change:
```python
        n_char = OrcapodConfig().system_tag_hash_n_char
```
to:
```python
        n_char = OrcapodConfig().hashing.system_tag_n_char
```

- [ ] **Step 11: Update `test_schema_utils.py` (1 change)**

In `tests/test_utils/test_schema_utils.py`, line 19 — change:
```python
    return DEFAULT_CONFIG.schema_hash_n_char
```
to:
```python
    return DEFAULT_CONFIG.hashing.schema_n_char
```

- [ ] **Step 12: Run the full test suite to verify all passes**

```bash
uv run pytest tests/ -x -q
```

Expected: All tests PASS. Zero failures.

- [ ] **Step 13: Commit**

```bash
git add \
  src/orcapod/core/sources/stream_builder.py \
  src/orcapod/core/nodes/source_node.py \
  src/orcapod/core/operators/join.py \
  src/orcapod/core/operators/merge_join.py \
  src/orcapod/core/nodes/function_node.py \
  src/orcapod/core/nodes/operator_node.py \
  src/orcapod/utils/schema_utils.py \
  tests/test_core/operators/test_operators.py \
  tests/test_core/operators/test_merge_join.py \
  tests/test_utils/test_schema_utils.py
git commit -m "refactor(config): update all consumers to nested config field paths"
```

---

## Task 3: Update public API exports in `__init__.py`

**Files:**
- Modify: `src/orcapod/__init__.py`

- [ ] **Step 1: Run the export tests to confirm they fail**

```bash
uv run pytest tests/test_orcapod_config.py::TestOrcapodConfigTopLevelExport -v
```

Expected: All 8 tests FAIL — `HashingConfig`, `DisplayConfig`, `load_config` not yet exported.

- [ ] **Step 2: Update `src/orcapod/__init__.py`**

Replace the file with:

```python
from .config import DEFAULT_CONFIG, DisplayConfig, HashingConfig, OrcapodConfig, load_config
from .core.function_pod import (
    FunctionPod,
    function_pod,
)
from .core.nodes.source_node import SourceNode
from .pipeline import Pipeline, PipelineJob
from .semantic_types.dataclass_encoding import register_dataclass

# Subpackage re-exports for clean public API
from . import databases  # noqa: F401
from . import nodes  # noqa: F401
from . import operators  # noqa: F401
from . import sources  # noqa: F401
from . import streams  # noqa: F401
from . import types  # noqa: F401

__all__ = [
    "DEFAULT_CONFIG",
    "DisplayConfig",
    "HashingConfig",
    "OrcapodConfig",
    "load_config",
    "FunctionPod",
    "function_pod",
    "Pipeline",
    "PipelineJob",
    "SourceNode",
    "register_dataclass",
    "databases",
    "nodes",
    "operators",
    "sources",
    "streams",
    "types",
]
```

- [ ] **Step 3: Run the export tests**

```bash
uv run pytest tests/test_orcapod_config.py::TestOrcapodConfigTopLevelExport -v
```

Expected: All 8 tests PASS.

- [ ] **Step 4: Run the full test suite**

```bash
uv run pytest tests/ -q
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/__init__.py
git commit -m "feat(orcapod): export HashingConfig, DisplayConfig, load_config from top-level package"
```

---

## Task 4: Full regression run and PR

- [ ] **Step 1: Run the complete test suite one final time**

```bash
uv run pytest tests/ -v
```

Expected: All tests PASS, zero failures, zero errors.

- [ ] **Step 2: Draft the deferred save/load Linear issue**

Create a new issue in Linear (team: Platform, project: Orcapod Python v0.1 Stabilization Push) titled **"Add save/write support to OrcaPod config system"** with description:

```
## Overview

``load_config()`` (added in PLT-964) supports reading TOML config files, but there
is no way to programmatically write or update a config file. This issue adds
``save_config(config, path)`` so that config values set programmatically can be
persisted to disk.

## Goals & Success Criteria

* ``save_config(config: OrcapodConfig, path: Path | str)`` writes a TOML file
* Round-trip guarantee: ``load_config(project_config_path=p)`` after
  ``save_config(cfg, p)`` returns a config equal to ``cfg``
* Only non-default values are written (sparse output — default fields omitted)

## Scope & Boundaries

In scope:
* ``save_config()`` function in ``config.py``
* Export from ``orcapod.__init__``
* Tests for round-trip and sparse output

Out of scope:
* CLI interface for config management
* Wiring display config into stream output (tracked in PLT-1570)

## Dependencies & Risks

* Depends on PLT-964 landing first
* Requires ``tomlkit`` or similar for round-trip-safe TOML writing (``tomllib``
  is read-only). New dependency needed.
```

- [ ] **Step 3: Push the branch**

```bash
git push -u origin eywalker/plt-964-add-persistent-orcapod-configuration-system
```

- [ ] **Step 4: Open the PR**

```bash
gh pr create \
  --base dev \
  --title "feat(config): add persistent OrcaPod configuration system (PLT-964)" \
  --body "$(cat <<'EOF'
## Summary

- Restructures `OrcapodConfig` into a nested sectioned design (`HashingConfig` + `DisplayConfig`)
- Renames flat hashing fields to drop redundant `_hash_` infix (e.g. `system_tag_hash_n_char` → `hashing.system_tag_n_char`)
- Adds `OrcapodConfig.from_dict()` — fixes the broken `OrcapodConfig(**config_dict)` deserialization pattern in `function_node.py` and `operator_node.py`
- Adds `load_config()` — reads from `~/.orcapod/config.toml` (user-global) and `./orcapod_config.toml` (project-local) with precedence; uses built-in `tomllib`, no new deps
- Unknown config keys emit `logging.WARNING` (forward-compat: newer config files don't break older library versions)
- Exports `HashingConfig`, `DisplayConfig`, `load_config` from top-level `orcapod` package

Closes PLT-964

## Test plan

- [ ] `uv run pytest tests/test_orcapod_config.py -v` — all new structural, `from_dict`, and `load_config` tests pass
- [ ] `uv run pytest tests/ -q` — full suite passes, no regressions

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
