# Design: Strongly-typed, schema-validated pipeline config loading (ENG-607)

**Status:** Approved (design)
**Date:** 2026-06-12
**Linear:** ENG-607 (project: Wrap other RawData → ETL repos in Orcapod)
**Related:** ENG-601 (config is a content-hashed broadcast pod input), PLT-964 (`OrcapodConfig` nested-config pattern)

## Overview

Pipelines wrapped in orcapod (starting with orcapod-spikesorting) are driven by large,
deeply-nested YAML config files — the spike-sorting config alone is ~9 top-level sections and
hundreds of parameters. Today such a config is loaded with `yaml.safe_load` into a plain `dict`
and accessed by string keys throughout the wrapper and its `enigma-ephys` pods. There is no
validation or typing: a typo'd key, a wrong type, or a missing nested field surfaces as a deep
failure mid-processing (often only on a Ray worker) or as a silently wrong result.

This design adds a **reusable, pydantic-backed config facility in orcapod-python** so a wrapped
pipeline can: (1) define its config **schema** once as pydantic models, (2) **validate** a YAML
config against that schema and load it into a **typed object** at pipeline-build time, and
(3) pass that validated object into function pods as a **first-class, content-hashed input**,
where pods receive it already deserialized and typed.

## Goals & Success Criteria

- Loading an invalid config fails **immediately at build time** (before any pod runs) with a
  clear, field-located error (wrong type, unknown key, out-of-range value, missing required).
- Pods receive the config as a **typed pydantic model** (attribute access, IDE/type-checker
  support), not an untyped dict — with **no per-pod deserialization boilerplate**.
- The config's pod-input **content hash is over its validated, canonical meaning**, so
  formatting-only YAML edits (comments, key order, whitespace) do **not** bust the cache; only
  meaningful value changes do. (An improvement over ENG-601's raw-file hashing.)
- The facility is **reusable** across every wrapped ETL repo, not specific to spike-sorting.

## Authoring model

Two artifacts, complementary (not redundant):

- **Schema** — pydantic model classes, written once by the pipeline developer, living in a
  `config/` subpackage of the wrapped repo (e.g. `orcapod_spikesorting/config/`). Defines
  structure, types, constraints, and defaults. Changes rarely.
- **Values** — the YAML file a scientist edits per run (e.g. `subset_data`, `cache_path`).
  YAML remains the human-authoring format; nobody hand-writes a pydantic object to configure a
  run.

## Architecture & data flow

```
Author (YAML)            Pipeline build (driver)                      Pod (worker)
─────────────            ───────────────────────                      ────────────
config.yaml  ──▶  load_pydantic_config(path, SpikeSortingConfig)      def preprocess(rec, config: SpikeSortingConfig):
                    → yaml.safe_load                                       config.kilosort.batch_size   # typed + validated
                    → model.model_validate(...)                            ...
                    → SpikeSortingConfig instance
                         │
                         ▼
                    broadcast as a pod input via a dict/list source.
                    A registered semantic-type converter maps the
                    model ⇄ Arrow struct<model qualname, canonical JSON>;
                    content hash = hash(qualname + canonical JSON).
                         │  identity = meaning of config, not file formatting
                         ▼
                    orcapod hashes + transports; worker reconstructs
                    the typed model from the struct automatically.
```

## Components & API

All new code lives in a new module `orcapod/pydantic_config.py` (loader + converter), kept
separate from `orcapod/config.py` (which is orcapod's *own* `OrcapodConfig` runtime settings —
a different concept). `pydantic` becomes a dependency of orcapod-python (pydantic v2).

### `load_pydantic_config`

```python
M = TypeVar("M", bound=pydantic.BaseModel)

def load_pydantic_config(path: str | Path, model_cls: type[M]) -> M:
    """Read a YAML file, validate it against `model_cls`, return the typed model.

    Raises a clear, file-located error on invalid YAML or schema violation.
    """
```

- Named `load_pydantic_config` (not `load_config`, which is already a top-level export for
  `OrcapodConfig`) to avoid collision and to be explicit that it is pydantic-backed.
- Reads YAML via `yaml.safe_load`, validates via `model_cls.model_validate(data)`, returns the
  instance. On `pydantic.ValidationError` (or YAML parse error), re-raise wrapped with the file
  path for context.

### `OrcapodBaseConfig` (optional base)

```python
class OrcapodBaseConfig(pydantic.BaseModel):
    """Recommended base for pipeline config schemas; strict by default."""
    model_config = ConfigDict(extra="forbid", frozen=True)
```

- `extra="forbid"` makes typo'd/unknown keys an error. `frozen=True` makes instances immutable
  (safer as a broadcast input). Use is recommended, not required — a schema author may subclass
  `pydantic.BaseModel` directly if they need different semantics.

### `PydanticModelConverter` (semantic-type converter)

Modeled directly on `PathStructConverter` (which maps `Path` ⇄ `struct<path: large_string>`
with file-content hashing). Registered in the `DataContext` semantic registry.

- `can_handle_python_type(t)`: `issubclass(t, pydantic.BaseModel)`.
- **python → arrow:** `struct<__pydantic_model__: large_string, __pydantic_json__: large_string>`
  where `__pydantic_model__` is the model's fully-qualified `module:qualname` and
  `__pydantic_json__` is canonical JSON (`model.model_dump_json()`; deterministic field order).
- **arrow → python:** import the class from the stored qualname and
  `model_cls.model_validate_json(json)`. Self-describing — no external type context needed.
- **content hash:** hash over `(__pydantic_model__, canonical JSON)`, so identity tracks config
  meaning + schema identity, independent of source-YAML formatting.

### Pipeline wiring (in the wrapped repo)

The broadcast config source is built from the **validated model instance** (via a dict/list
source, whose values route through the type converter) rather than a `Path` `DataFrameSource`.
Pods declare a parameter typed as the model (`config: SpikeSortingConfig`) and orcapod injects
the reconstructed, validated model — handling transport, hashing, and deserialization.

## Error handling

- **Invalid YAML / schema violation:** raised at build time, before any pod runs, with the file
  path and pydantic's field-level detail.
- **Unimportable model class on reconstruction (worker):** clear `ImportError` naming the stored
  qualname (e.g. the wrapped package isn't on the worker path).

## Testing

- **Loader:** valid config → model; wrong-type, unknown-key (with `extra="forbid"`), and
  missing-required configs → raise with clear messages including the file path.
- **Converter round-trip:** `model → arrow struct → model` equality.
- **Hash stability:** formatting-only YAML edits → identical content hash (cache still hits);
  any value change → different hash.
- **End-to-end:** a small pipeline where a pod consumes a typed config; confirm the pod receives
  the model and that a formatting-only config edit yields a cache hit.

## Scope & boundaries

**In scope (orcapod-python, this work):**
- `orcapod/pydantic_config.py`: `load_pydantic_config`, `OrcapodBaseConfig`,
  `PydanticModelConverter`.
- Register the converter in the `DataContext` semantic registry.
- Add `pydantic` (v2) as a dependency; tests; docs.

**Out of scope (orcapod-spikesorting follow-up):**
- Define a `config/` subpackage of pydantic models for the spike-sorting config.
- Swap the broadcast config source from a `Path` `DataFrameSource` to the validated model
  source; annotate pods with the model type.
- Migrate `enigma-ephys` dict-key call sites. Eased by handing existing functions
  `config.model_dump()` (a plain dict) during transition, so the migration can be incremental.

## Dependencies & risks

- Adds `pydantic` v2 as an orcapod-python dependency (intended).
- Content-hash semantics change for configs (meaning-based, not file-bytes). This is desired but
  means existing caches keyed on the old `Path`-file hash won't match — a one-time recompute when
  a pipeline migrates to the typed config. Document this.
- Reconstruction requires the model class to be importable on workers (already true for wrapped
  packages shipped via Ray `py_modules`).

## Deferred / not now (YAGNI)

- JSON-Schema export for docs/tooling — available for free via `model.model_json_schema()` if/when
  wanted; not built now.
- A standalone external schema file (XSD/JSON Schema) as the source of truth — rejected in favor
  of pydantic models as the single source of truth (avoids a duplicated, drift-prone schema).
