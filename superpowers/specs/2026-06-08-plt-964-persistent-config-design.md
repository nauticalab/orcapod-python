# PLT-964: Persistent OrcaPod Configuration System

**Date:** 2026-06-08
**Issue:** [PLT-964](https://linear.app/enigma-metamorphic/issue/PLT-964/add-persistent-orcapod-configuration-system)

---

## Problem

`OrcapodConfig` is a flat frozen dataclass with three hashing-specific fields. It has no
logical grouping, no display/preference settings, and no way to load values from a file.
Users must construct config objects programmatically or rely on built-in defaults; there is
no persistent per-user or per-project configuration.

---

## Design

### 1. New nested config structure

`config.py` gains two section dataclasses and `OrcapodConfig` becomes the top-level
container:

```python
@dataclass(frozen=True)
class HashingConfig:
    """Hash truncation length settings."""
    system_tag_n_char: int = 12
    schema_n_char: int = 12
    path_n_char: int = 20

@dataclass(frozen=True)
class DisplayConfig:
    """Display and preview preference settings."""
    max_rows: int | None = None          # None = no row limit
    show_meta_columns: bool = False
    show_source_columns: bool = False
    show_system_tag_columns: bool = False
    show_context_columns: bool = False

@dataclass(frozen=True)
class OrcapodConfig:
    """Top-level immutable OrcaPod configuration."""
    hashing: HashingConfig = field(default_factory=HashingConfig)
    display: DisplayConfig = field(default_factory=DisplayConfig)
```

`DEFAULT_CONFIG = OrcapodConfig()` stays at module level.

#### Field renames

The three existing fields are shortened by dropping the redundant `_hash_` infix — their
section context makes the purpose clear:

| Old (flat) | New (nested) |
|---|---|
| `config.system_tag_hash_n_char` | `config.hashing.system_tag_n_char` |
| `config.schema_hash_n_char` | `config.hashing.schema_n_char` |
| `config.path_hash_n_char` | `config.hashing.path_n_char` |

All call sites in `src/` and `tests/` are updated directly. No shims.

#### `with_updates()`

Works unchanged — callers now pass section instances:

```python
config.with_updates(hashing=HashingConfig(system_tag_n_char=8))
config.with_updates(display=DisplayConfig(max_rows=50))
```

#### `merge()`

Extended to deep-merge at the field level within each section. Each section dataclass
gets its own `merge()` with the same "other takes precedence for non-default values"
semantics as the current flat implementation. `OrcapodConfig.merge()` delegates to each
section's `merge()`:

```python
def merge(self, other: "OrcapodConfig") -> "OrcapodConfig":
    return OrcapodConfig(
        hashing=self.hashing.merge(other.hashing),
        display=self.display.merge(other.display),
    )
```

#### `from_dict()` classmethod

The existing deserialization pattern `OrcapodConfig(**config_dict)` breaks with a nested
structure. A `from_dict()` classmethod replaces it and also handles unknown-key warnings:

```python
@classmethod
def from_dict(
    cls,
    data: dict,
    source_path: Path | str | None = None,
) -> "OrcapodConfig":
    """Construct an ``OrcapodConfig`` from a plain dict.

    Unknown top-level section names and unknown field names within a known
    section are logged at WARNING level. Unknown keys are otherwise ignored
    (forward-compatibility: a config written by a newer version of orcapod
    will not break an older version).

    Args:
        data: Mapping of section name → field dict.
        source_path: Optional path to the originating file, used in warning
            messages to help users locate typos.

    Returns:
        An ``OrcapodConfig`` populated from the supplied data, with missing
        sections and fields falling back to their defaults.
    """
```

Unknown sections (e.g. `[typo_section]`) and unknown fields within a known section
(e.g. `max_rwos = 50`) are logged via Python's standard `logging` module:

```
WARNING:orcapod.config: Unknown config section 'typo_section' in <path> — ignored
WARNING:orcapod.config: Unknown field 'max_rwos' in [display] in <path> — ignored
```

If `source_path` is `None`, the `in <path>` clause is omitted.

All three deserialization sites (`function_node.py` ×2, `operator_node.py` ×2) are updated
from `OrcapodConfig(**config_dict)` to `OrcapodConfig.from_dict(config_dict)`.

The `dataclasses.asdict()` serialization in `graph.py` is unchanged — it naturally produces
the nested dict `{"hashing": {...}, "display": {...}}` that `from_dict()` expects. The
serialized format in the DB changes from flat to nested; no migration is needed (pre-v0.1).

---

### 2. TOML config file format

The persistent config file uses TOML. Python ≥3.11 ships `tomllib` for reading; no new
dependency is added.

**Example `orcapod_config.toml`:**

```toml
[hashing]
system_tag_n_char = 8

[display]
max_rows = 100
show_source_columns = true
```

Only sections and fields that differ from defaults need to be specified. Missing sections
and missing fields within a section all fall back to built-in defaults.

---

### 3. Config loading

A new module-level function `load_config()` in `config.py`:

```python
def load_config(
    project_config_path: Path | str | None = None,
    user_config_path: Path | str | None = None,
) -> OrcapodConfig:
    """Load and merge config from TOML files with precedence.

    Precedence (lowest to highest):
      built-in defaults
      → user-global config  (~/.orcapod/config.toml)
      → project-local config (./orcapod_config.toml in cwd)

    Missing files are silently skipped. Malformed TOML raises ``ValueError``
    with the offending file path included in the message.

    Args:
        project_config_path: Override the project-local config file path.
            Defaults to ``orcapod_config.toml`` in the current working directory.
        user_config_path: Override the user-global config file path.
            Defaults to ``~/.orcapod/config.toml``.

    Returns:
        Merged ``OrcapodConfig`` with all applicable overrides applied.
    """
```

**Discovery and merge algorithm:**

1. Start from `DEFAULT_CONFIG` (all built-in defaults).
2. If `~/.orcapod/config.toml` exists, parse it and merge via
   `DEFAULT_CONFIG.merge(OrcapodConfig.from_dict(data, source_path=...))`.
3. If `./orcapod_config.toml` exists, parse it and merge on top (project-local wins).
4. Return the merged result.

Programmatic overrides remain the caller's responsibility — pass the result of
`load_config()` to `OrcapodConfig.with_updates(...)` as needed.

Malformed TOML is re-raised as `ValueError`:

```
ValueError: Malformed TOML in /home/user/.orcapod/config.toml: <tomllib error message>
```

---

### 4. Public API exports

`src/orcapod/__init__.py` is updated to export the two new section types alongside
`OrcapodConfig`:

```python
from orcapod.config import DEFAULT_CONFIG, DisplayConfig, HashingConfig, OrcapodConfig
```

`load_config` is also exported so users can call `orcapod.load_config()`.

---

## Files changed

| File | Change |
|---|---|
| `src/orcapod/config.py` | Full rewrite: `HashingConfig`, `DisplayConfig`, nested `OrcapodConfig`; `from_dict()`; `load_config()` |
| `src/orcapod/__init__.py` | Export `HashingConfig`, `DisplayConfig`, `load_config` |
| `src/orcapod/core/base.py` | No field access changes (only imports `OrcapodConfig` / `DEFAULT_CONFIG`) |
| `src/orcapod/core/nodes/function_node.py` | `OrcapodConfig(**config_dict)` → `OrcapodConfig.from_dict(config_dict)` (×2) |
| `src/orcapod/core/nodes/operator_node.py` | Same deserialization fix (×2) |
| `src/orcapod/core/nodes/source_node.py` | `orcapod_config.schema_hash_n_char` → `orcapod_config.hashing.schema_n_char` |
| `src/orcapod/core/operators/join.py` | `orcapod_config.system_tag_hash_n_char` → `orcapod_config.hashing.system_tag_n_char` (×3) |
| `src/orcapod/core/operators/merge_join.py` | Same field rename (×2) |
| `src/orcapod/core/sources/stream_builder.py` | `_config.schema_hash_n_char` → `_config.hashing.schema_n_char`; `_config.path_hash_n_char` → `_config.hashing.path_n_char` |
| `src/orcapod/pipeline/graph.py` | `dataclasses.asdict(_cfg)` unchanged; `_cfg == _DEFAULT_CONFIG` unchanged; both work correctly with nested structure |
| `src/orcapod/utils/schema_utils.py` | Update docstring reference to `OrcapodConfig.schema_hash_n_char` |
| `tests/test_orcapod_config.py` | Rewrite for nested structure; add `load_config` tests |
| `tests/test_core/operators/test_operators.py` | `OrcapodConfig().system_tag_hash_n_char` → `OrcapodConfig().hashing.system_tag_n_char` (×2) |
| `tests/test_core/operators/test_merge_join.py` | Same field rename (×1) |
| `tests/test_utils/test_schema_utils.py` | `DEFAULT_CONFIG.schema_hash_n_char` → `DEFAULT_CONFIG.hashing.schema_n_char` |
| `tests/test_core/streams/test_streams.py` | No field accesses; unchanged |
| `tests/test_core/sources/test_stream_builder.py` | No field accesses; unchanged |

---

## Tests

### `tests/test_orcapod_config.py` — rewritten and extended

**Structural tests:**

| Test | What it verifies |
|---|---|
| `test_hashing_config_defaults` | `HashingConfig()` defaults are `12`, `12`, `20` |
| `test_display_config_defaults` | `DisplayConfig()` defaults are `None`, all `False` |
| `test_orcapod_config_defaults` | `OrcapodConfig().hashing` and `.display` are default instances |
| `test_default_config_is_orcapod_config_instance` | `DEFAULT_CONFIG` is `OrcapodConfig` |
| `test_with_updates_hashing_section` | `config.with_updates(hashing=HashingConfig(system_tag_n_char=8))` |
| `test_with_updates_display_section` | `config.with_updates(display=DisplayConfig(max_rows=50))` |
| `test_merge_hashing_non_default_wins` | Non-default hashing field in `other` overrides self |
| `test_merge_hashing_default_unchanged` | Default fields in `other` do not override self |
| `test_merge_display_non_default_wins` | Non-default display field in `other` overrides self |
| `test_merge_type_error` | `merge("not a config")` raises `TypeError` |

**`from_dict()` tests:**

| Test | What it verifies |
|---|---|
| `test_from_dict_empty` | `OrcapodConfig.from_dict({})` returns `DEFAULT_CONFIG` |
| `test_from_dict_partial_hashing` | Only specified fields are overridden |
| `test_from_dict_full_sections` | All fields in both sections round-trip correctly |
| `test_from_dict_unknown_section_warns` | Unknown top-level key emits `logging.WARNING` |
| `test_from_dict_unknown_field_warns` | Unknown field within known section emits `logging.WARNING` |
| `test_from_dict_with_source_path_in_warning` | Warning message includes the file path |

**`load_config()` tests (using `tmp_path`):**

| Test | What it verifies |
|---|---|
| `test_load_config_no_files` | Returns `DEFAULT_CONFIG` when no files exist |
| `test_load_config_user_global_only` | User-global file is applied |
| `test_load_config_project_local_only` | Project-local file is applied |
| `test_load_config_project_local_wins` | Project-local overrides user-global for the same field |
| `test_load_config_malformed_toml_raises` | `ValueError` with path in message |
| `test_load_config_explicit_paths` | Explicit `project_config_path` / `user_config_path` override defaults |

---

## Out of scope

- Saving / writing config to a TOML file (deferred — new Linear issue to be filed)
- CLI tool for inspecting or editing config
- Wiring `DisplayConfig` fields into stream output methods — tracked in
  [PLT-1570](https://linear.app/enigma-metamorphic/issue/PLT-1570/wire-displayconfig-into-stream-and-table-output-methods)
- Walking parent directories for project-local config (always uses `cwd`)
- Environment variable overrides
