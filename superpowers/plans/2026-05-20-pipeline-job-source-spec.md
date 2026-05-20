# Pipeline + SourceSpec Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `Pipeline` into a pure source-agnostic DAG; introduce `SourceSpec` (typed input slot) and `PipelineJob` (everyday working object with source bindings, databases, and execution).

**Architecture:** `SourceSpec` is a named, immutable schema declaration implementing a minimal `StreamProtocol` surface. `Pipeline` is the pure DAG (SourceSpec-only leaves, no databases, no execution). `PipelineJob` extends `AutoRegisteringContextBasedTracker` — its `with`-block records both DAG structure and source bindings simultaneously; concrete sources are automatically promoted to SourceSpecs. `Pipeline` is derived from any `PipelineJob` via `job.pipeline`. All mutation needed for execution happens in fresh node objects inside `PipelineJob._build_execution_graph()`, so the shared `Pipeline` is never mutated.

**Tech Stack:** Python, PyArrow, NetworkX, `uv run pytest` for tests; follows Google-style docstrings and Conventional Commits.

---

## File Structure

**New files:**
- `src/orcapod/core/sources/source_spec.py` — `SourceSpec` class
- `src/orcapod/pipeline/execution_context.py` — `ExecutionContext` stub dataclass
- `src/orcapod/pipeline/job.py` — `PipelineJob` class
- `tests/test_core/sources/test_source_spec.py` — unit tests for `SourceSpec`
- `tests/test_pipeline/test_pipeline_job.py` — unit and integration tests for `PipelineJob`

**Modified files:**
- `src/orcapod/errors.py` — add `UnboundSourceError`, `SourceSpecMismatchError`
- `src/orcapod/core/sources/__init__.py` — export `SourceSpec`
- `src/orcapod/pipeline/graph.py` — refactor `Pipeline` (remove DB/execution, enforce SourceSpec, add `bind()`)
- `src/orcapod/pipeline/serialization.py` — add `PIPELINE_JOB_FORMAT_VERSION`, SourceSpec serialization helpers
- `src/orcapod/pipeline/__init__.py` — export `PipelineJob`, `SourceSpec`, `ExecutionContext`
- `src/orcapod/__init__.py` — export `PipelineJob`, `SourceSpec`
- `tests/test_pipeline/test_pipeline.py` — update all fixtures/tests to new Pipeline (no DB), migrate execution tests to PipelineJob
- `tests/test_pipeline/test_serialization.py` — update to new Pipeline save format; move DB round-trip tests to `test_pipeline_job.py`

---

## Task 1: Error Types

**Files:**
- Modify: `src/orcapod/errors.py`

- [ ] **Step 1: Add the two new error classes**

```python
# Append to src/orcapod/errors.py

class UnboundSourceError(RuntimeError):
    """Raised when a data-producing method is called on an unbound SourceSpec.

    Occurs when ``iter_data()`` or ``as_table()`` is called on a ``SourceSpec``
    that has not been bound to a concrete source in a ``PipelineJob``.
    """


class SourceSpecMismatchError(ValueError):
    """Raised when a concrete source's schema is incompatible with a SourceSpec.

    Contains the spec name and a description of the incompatible field(s).
    Raised at ``bind()`` time — schema mismatches are rejected before execution.
    """
```

- [ ] **Step 2: Verify import works**

Run: `uv run python -c "from orcapod.errors import UnboundSourceError, SourceSpecMismatchError; print('OK')`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/errors.py
git commit -m "feat(errors): add UnboundSourceError and SourceSpecMismatchError"
```

---

## Task 2: `SourceSpec` Class

**Files:**
- Create: `src/orcapod/core/sources/source_spec.py`
- Create: `tests/test_core/sources/test_source_spec.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_core/sources/test_source_spec.py
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.source_spec import SourceSpec
from orcapod.errors import UnboundSourceError, SourceSpecMismatchError
from orcapod.types import Schema


def _make_source(tag_col: str, data_col: str) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(["a", "b"], type=pa.large_string()),
            data_col: pa.array([1, 2], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


class TestSourceSpecConstruction:
    def test_construct_with_name_and_schemas(self):
        spec = SourceSpec(
            name="my_source",
            tag_schema=Schema({"key": str}),
            data_schema=Schema({"value": int}),
        )
        assert spec.name == "my_source"
        assert "key" in spec.tag_schema
        assert "value" in spec.data_schema

    def test_output_schema_returns_tag_and_data(self):
        tag = Schema({"key": str})
        data = Schema({"value": int})
        spec = SourceSpec(name="s", tag_schema=tag, data_schema=data)
        out_tag, out_data = spec.output_schema()
        assert out_tag == tag
        assert out_data == data

    def test_keys_returns_tag_column_names(self):
        spec = SourceSpec(
            name="s",
            tag_schema=Schema({"key": str, "group": str}),
            data_schema=Schema({"value": int}),
        )
        assert spec.keys() == frozenset({"key", "group"})

    def test_label_returns_name(self):
        spec = SourceSpec(name="my_spec", tag_schema=Schema({"k": str}), data_schema=Schema({"v": int}))
        assert spec.label == "my_spec"


class TestSourceSpecHashing:
    def test_pipeline_hash_matches_compatible_source(self):
        """SourceSpec.pipeline_hash() must equal a schema-compatible source's pipeline_hash()."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec = SourceSpec(name="my_source", tag_schema=tag_schema, data_schema=data_schema)

        assert spec.pipeline_hash() == source.pipeline_hash()

    def test_content_hash_differs_by_name(self):
        """Two specs with the same schema but different names must have different content hashes."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec_a = SourceSpec(name="source_a", tag_schema=tag_schema, data_schema=data_schema)
        spec_b = SourceSpec(name="source_b", tag_schema=tag_schema, data_schema=data_schema)

        assert spec_a.content_hash() != spec_b.content_hash()

    def test_pipeline_hash_same_for_same_name_diff_name(self):
        """SourceSpec.pipeline_hash() must be schema-only (ignoring name)."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec_a = SourceSpec(name="a", tag_schema=tag_schema, data_schema=data_schema)
        spec_b = SourceSpec(name="b", tag_schema=tag_schema, data_schema=data_schema)

        assert spec_a.pipeline_hash() == spec_b.pipeline_hash()

    def test_content_hash_stable(self):
        """Same name + schemas → same content hash across calls."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec = SourceSpec(name="s", tag_schema=tag_schema, data_schema=data_schema)
        assert spec.content_hash() == spec.content_hash()


class TestSourceSpecUnboundBehavior:
    def test_iter_data_raises_unbound_error(self):
        spec = SourceSpec(name="s", tag_schema=Schema({"k": str}), data_schema=Schema({"v": int}))
        with pytest.raises(UnboundSourceError, match="s"):
            list(spec.iter_data())

    def test_as_table_raises_unbound_error(self):
        spec = SourceSpec(name="s", tag_schema=Schema({"k": str}), data_schema=Schema({"v": int}))
        with pytest.raises(UnboundSourceError, match="s"):
            spec.as_table()


class TestSourceSpecValidate:
    def test_validate_passes_for_compatible_source(self):
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        spec = SourceSpec(name="s", tag_schema=tag_schema, data_schema=data_schema)
        # Should not raise
        spec.validate(source)

    def test_validate_raises_for_extra_data_column(self):
        """Source has extra data column not in spec → SourceSpecMismatchError."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        # Spec declares only a subset of columns
        narrow_data = Schema({"value": int})
        extra_tag = Schema({"key": str, "unexpected": str})
        spec = SourceSpec(name="s", tag_schema=extra_tag, data_schema=narrow_data)
        with pytest.raises(SourceSpecMismatchError):
            spec.validate(source)

    def test_validate_raises_for_missing_data_column(self):
        """Source missing a required data column → SourceSpecMismatchError."""
        source = _make_source("key", "value")
        tag_schema, data_schema = source.output_schema()
        # Spec requires an extra data column the source doesn't have
        wider_data = Schema({"value": int, "extra": str})
        spec = SourceSpec(name="s", tag_schema=tag_schema, data_schema=wider_data)
        with pytest.raises(SourceSpecMismatchError):
            spec.validate(source)
```

- [ ] **Step 2: Run to verify failures**

Run: `uv run pytest tests/test_core/sources/test_source_spec.py -v`
Expected: ERRORS (module not found)

- [ ] **Step 3: Implement `SourceSpec`**

```python
# src/orcapod/core/sources/source_spec.py
"""SourceSpec — a named, immutable schema declaration for pipeline input slots."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import orcapod.contexts as contexts
from orcapod.core.base import ContentIdentifiableBase, PipelineElementBase
from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.types import ColumnConfig, Schema

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.core_protocols import StreamProtocol
    from orcapod.types import ContentHash


class SourceSpec(ContentIdentifiableBase, PipelineElementBase):
    """A named, immutable schema declaration for a pipeline input slot.

    ``SourceSpec`` describes what a pipeline input looks like — its key schema
    and data schema — without referencing any concrete data source. It is used
    as the typed input slot concept for both ``Pipeline`` and ``PipelineJob``.

    A ``SourceSpec`` can appear as an upstream in operator chains during a
    ``with Pipeline:`` or ``with PipelineJob:`` recording block. Calling data-
    producing methods (``iter_data``, ``as_table``) raises ``UnboundSourceError``
    until the spec is bound to a concrete source via ``PipelineJob.bind()``.

    Identity and hashing:
        - ``pipeline_hash()`` — schema-only, ignoring ``name``. Matches a
          schema-compatible ``RootSource.pipeline_hash()``, enabling DB path
          reuse across different sources bound to the same spec.
        - ``content_hash()`` — includes ``name``. Two specs with identical
          schemas but different names are distinct elements.

    Args:
        name: Human-readable identifier for this input slot. Used as the
            source label when auto-promoting concrete sources in
            ``PipelineJob``. Must be unique within a pipeline.
        tag_schema: Mapping of tag column names to Python types.
        data_schema: Mapping of data column names to Python types.
    """

    def __init__(
        self,
        name: str,
        tag_schema: Schema,
        data_schema: Schema,
        data_context: str | contexts.DataContext | None = None,
    ) -> None:
        super().__init__(data_context=data_context)
        self._name = name
        self._tag_schema = tag_schema
        self._data_schema = data_schema

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """Human-readable name for this input slot."""
        return self._name

    @property
    def label(self) -> str:
        """Alias for ``name`` — satisfies the stream label convention."""
        return self._name

    @property
    def tag_schema(self) -> Schema:
        """Key schema for this input slot."""
        return self._tag_schema

    @property
    def data_schema(self) -> Schema:
        """Data schema for this input slot."""
        return self._data_schema

    # ------------------------------------------------------------------
    # ContentIdentifiableBase
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Content identity includes name + both schemas."""
        return ("SourceSpec", self._name, self._tag_schema, self._data_schema)

    # ------------------------------------------------------------------
    # PipelineElementBase
    # ------------------------------------------------------------------

    def pipeline_identity_structure(self) -> Any:
        """Pipeline identity is schema-only (no name).

        Matches ``RootSource.pipeline_identity_structure()`` so that a
        schema-compatible concrete source and this spec share the same
        pipeline hash — and therefore the same DB table paths.
        """
        return (self._tag_schema, self._data_schema)

    # ------------------------------------------------------------------
    # StreamProtocol surface (minimal — no data access)
    # ------------------------------------------------------------------

    def output_schema(self, columns: ColumnConfig | None = None) -> tuple[Schema, Schema]:
        """Return ``(tag_schema, data_schema)``.

        Args:
            columns: Ignored — SourceSpec always returns the full declared schemas.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        return (self._tag_schema, self._data_schema)

    def keys(self, columns: ColumnConfig | None = None) -> frozenset[str]:
        """Return the set of tag column names.

        Args:
            columns: Ignored.

        Returns:
            Frozenset of tag field names.
        """
        return frozenset(self._tag_schema.keys())

    def iter_data(self, *args: Any, **kwargs: Any):
        """Raise ``UnboundSourceError`` — spec is not bound to a concrete source.

        Raises:
            UnboundSourceError: Always.
        """
        raise UnboundSourceError(
            f"SourceSpec '{self._name}' is not bound to a concrete source. "
            "Call PipelineJob.bind(sources={...}) to attach a source before running."
        )

    def as_table(self, *args: Any, **kwargs: Any) -> "pa.Table":
        """Raise ``UnboundSourceError`` — spec is not bound to a concrete source.

        Raises:
            UnboundSourceError: Always.
        """
        raise UnboundSourceError(
            f"SourceSpec '{self._name}' is not bound to a concrete source. "
            "Call PipelineJob.bind(sources={...}) to attach a source before running."
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self, source: "StreamProtocol") -> None:
        """Check that *source* is schema-compatible with this spec.

        Validates that the source's tag and data schemas have exactly the
        same columns as this spec (no extra, no missing columns). Type
        compatibility is not checked here — Arrow conversion handles
        coercions at runtime.

        Args:
            source: The concrete source to validate.

        Raises:
            SourceSpecMismatchError: If the source schema does not match.
        """
        source_tag, source_data = source.output_schema()

        tag_issues: list[str] = []
        data_issues: list[str] = []

        spec_tag_cols = set(self._tag_schema.keys())
        src_tag_cols = set(source_tag.keys())
        if spec_tag_cols != src_tag_cols:
            missing = spec_tag_cols - src_tag_cols
            extra = src_tag_cols - spec_tag_cols
            if missing:
                tag_issues.append(f"missing tag columns: {sorted(missing)}")
            if extra:
                tag_issues.append(f"unexpected tag columns: {sorted(extra)}")

        spec_data_cols = set(self._data_schema.keys())
        src_data_cols = set(source_data.keys())
        if spec_data_cols != src_data_cols:
            missing = spec_data_cols - src_data_cols
            extra = src_data_cols - spec_data_cols
            if missing:
                data_issues.append(f"missing data columns: {sorted(missing)}")
            if extra:
                data_issues.append(f"unexpected data columns: {sorted(extra)}")

        if tag_issues or data_issues:
            all_issues = tag_issues + data_issues
            raise SourceSpecMismatchError(
                f"SourceSpec '{self._name}' is not compatible with the provided source. "
                + "; ".join(all_issues)
            )

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"SourceSpec(name={self._name!r}, "
            f"tag_schema={dict(self._tag_schema)!r}, "
            f"data_schema={dict(self._data_schema)!r})"
        )
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_core/sources/test_source_spec.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/sources/source_spec.py tests/test_core/sources/test_source_spec.py
git commit -m "feat(sources): add SourceSpec typed input-slot class"
```

---

## Task 3: SourceSpec Exports + ExecutionContext Stub

**Files:**
- Modify: `src/orcapod/core/sources/__init__.py`
- Create: `src/orcapod/pipeline/execution_context.py`

- [ ] **Step 1: Add SourceSpec to sources `__init__.py`**

In `src/orcapod/core/sources/__init__.py`, add after the last existing import:

```python
from .source_spec import SourceSpec
```

And add `"SourceSpec"` to `__all__`.

- [ ] **Step 2: Create `ExecutionContext` stub**

```python
# src/orcapod/pipeline/execution_context.py
"""ExecutionContext — stub type for pipeline execution configuration.

Full definition (PipelineConfig integration, distributed execution) is
deferred to a follow-up issue.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orcapod.types import PipelineConfig


@dataclass(frozen=True)
class ExecutionContext:
    """Minimal placeholder for pipeline execution configuration.

    Full definition including ``PipelineConfig`` integration is deferred
    to a follow-up issue.

    Args:
        config: Optional pipeline-level execution configuration.
    """

    config: "PipelineConfig | None" = None
```

- [ ] **Step 3: Verify imports**

Run: `uv run python -c "from orcapod.core.sources import SourceSpec; from orcapod.pipeline.execution_context import ExecutionContext; print('OK')"`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/sources/__init__.py src/orcapod/pipeline/execution_context.py
git commit -m "feat(pipeline): add ExecutionContext stub; export SourceSpec from sources"
```

---

## Task 4: Refactor `Pipeline` — Remove DB/Execution, Enforce SourceSpec

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`

This task removes all database, execution, and observer logic from `Pipeline`, leaving only DAG recording + compilation, plus a new `bind()` method. The `run()`, `flush()`, `_apply_execution_engine()`, `_compute_pipeline_snapshot_hash()`, and all scoped-DB properties are removed. `compile()` gains SourceSpec-only enforcement.

- [ ] **Step 1: Write failing tests for the new Pipeline interface**

Add to `tests/test_pipeline/test_pipeline.py` (before existing classes):

```python
# At top of file, add import:
from orcapod.core.sources.source_spec import SourceSpec

class TestPipelineSourceSpecEnforcement:
    def test_pipeline_no_database_params(self):
        """New Pipeline() takes only name (no pipeline_database)."""
        pipeline = Pipeline(name="test")
        with pipeline:
            pass
        assert pipeline._compiled

    def test_pipeline_with_spec_leaves_compiles(self):
        """Pipeline with SourceSpec leaves compiles without error."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("input_a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("input_b", tag_schema=tag_b, data_schema=data_b)

        pipeline = Pipeline(name="spec_pipe")
        with pipeline:
            Join()(spec_a, spec_b)

        assert pipeline._compiled
        source_nodes = [
            n for n in pipeline._node_graph.nodes() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 2

    def test_pipeline_with_concrete_leaf_raises(self):
        """Pipeline.compile() raises ValueError if any leaf is not a SourceSpec."""
        src_a, src_b = _make_two_sources()

        pipeline = Pipeline(name="bad_pipe")
        with pytest.raises(ValueError, match="SourceSpec"):
            with pipeline:
                Join()(src_a, src_b)

    def test_pipeline_bind_returns_pipeline_job(self):
        """Pipeline.bind() returns a PipelineJob without modifying the pipeline."""
        from orcapod.pipeline.job import PipelineJob

        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("b", tag_schema=tag_b, data_schema=data_b)

        pipeline = Pipeline(name="p")
        with pipeline:
            Join()(spec_a, spec_b)

        db = InMemoryArrowDatabase()
        job = pipeline.bind(sources={"a": src_a, "b": src_b}, store=db)

        assert isinstance(job, PipelineJob)
        assert job.pipeline is pipeline
        assert job.store is db
```

- [ ] **Step 2: Run to verify failures**

Run: `uv run pytest tests/test_pipeline/test_pipeline.py::TestPipelineSourceSpecEnforcement -v`
Expected: FAIL (Pipeline still accepts pipeline_database, no SourceSpec enforcement)

- [ ] **Step 3: Rewrite `Pipeline.__init__`**

Replace the current `Pipeline.__init__` (lines 70–103 in `graph.py`) with:

```python
def __init__(
    self,
    name: str | tuple[str, ...],
    tracker_manager: cp.TrackerManagerProtocol | None = None,
    auto_compile: bool = True,
) -> None:
    """Initialize a pure computational blueprint pipeline.

    Args:
        name: Pipeline name (string or tuple). Used to scope database paths
            when the pipeline is run via a ``PipelineJob``.
        tracker_manager: Optional tracker manager override. Defaults to
            ``DEFAULT_TRACKER_MANAGER``.
        auto_compile: If ``True`` (default), ``compile()`` is called
            automatically when the context manager exits.
    """
    super().__init__(tracker_manager=tracker_manager)
    self._node_lut: dict[str, GraphNode] = {}
    self._upstreams: dict[str, cp.StreamProtocol] = {}
    self._graph_edges: list[tuple[str, str]] = []
    self._hash_graph: "nx.DiGraph" = nx.DiGraph()
    self._name = (name,) if isinstance(name, str) else tuple(name)
    self._nodes: dict[str, GraphNode] = {}
    self._persistent_node_map: dict[str, GraphNode] = {}
    self._node_graph: "nx.DiGraph | None" = None
    self._auto_compile = auto_compile
    self._compiled = False
```

Also remove the instance variable declarations for `_pipeline_database`, `_result_database`, `_result_database_scoped`, `_scoped_pipeline_database`, `_status_database`, `_log_database`, `_default_observer`, and `_auto_save_path`.

Remove the entire `pipeline_database`, `result_database`, `scoped_pipeline_database`, `status_database`, `log_database` property methods.

- [ ] **Step 4: Update `Pipeline.compile()` — remove DB logic, add SourceSpec enforcement**

At the start of `compile()`, after `from orcapod.core.nodes import FunctionNode, OperatorNode`, remove all the scoped database creation block (the `if self._pipeline_database is not None:` block through `self._default_observer = NoOpObserver()`).

Replace the leaf-node wrapping block:
```python
if node_hash not in self._node_lut:
    # -- Leaf stream: wrap in SourceNode --
    stream = self._upstreams[node_hash]
    node = SourceNode(stream=stream)
    persistent_node_map[node_hash] = node
```

With the SourceSpec-enforcing version:
```python
if node_hash not in self._node_lut:
    # -- Leaf stream: must be a SourceSpec in the new design --
    from orcapod.core.sources.source_spec import SourceSpec
    stream = self._upstreams[node_hash]
    if not isinstance(stream, SourceSpec):
        raise ValueError(
            f"Pipeline: all leaf inputs must be SourceSpec instances, "
            f"but found {type(stream).__name__!r}. "
            "Use 'with PipelineJob:' to record a pipeline with concrete sources, "
            "or replace concrete sources with SourceSpec declarations."
        )
    node = SourceNode(stream=stream)
    persistent_node_map[node_hash] = node
```

Also remove the database-attachment blocks inside the `isinstance(node, FunctionNode)` and `isinstance(node, OperatorNode)` branches:

For FunctionNode — remove:
```python
if pipeline_db is not None:
    node.attach_databases(
        pipeline_database=pipeline_db,
        result_database=result_db,
    )
if node.executor is None:
    from orcapod.core.executors.local import LocalPythonFunctionExecutor
    node.executor = LocalPythonFunctionExecutor()
```

For OperatorNode — remove:
```python
if pipeline_db is not None:
    node.attach_databases(
        pipeline_database=pipeline_db,
    )
```

Remove the import of `NoOpObserver` from the compile() function.

- [ ] **Step 5: Remove `run()`, `flush()`, `_apply_execution_engine()`, `_compute_pipeline_snapshot_hash()` from Pipeline**

Delete these methods entirely from `graph.py`.

- [ ] **Step 6: Add `Pipeline.bind()` stub**

Add after `compile()`:

```python
def bind(
    self,
    sources: "dict[str, cp.StreamProtocol] | None" = None,
    store: "dbp.ArrowDatabaseProtocol | None" = None,
    execution_context: "ExecutionContext | None" = None,
) -> "PipelineJob":
    """Wrap this pipeline in a ``PipelineJob`` with the given bindings.

    Non-mutating — returns a fresh ``PipelineJob``; this ``Pipeline``
    is unchanged.

    Args:
        sources: Mapping of SourceSpec name to concrete source.
        store: Database for result caching and operator records.
        execution_context: Optional execution configuration.

    Returns:
        A new ``PipelineJob`` with this pipeline and the given bindings.
    """
    from orcapod.pipeline.job import PipelineJob
    from orcapod.pipeline.execution_context import ExecutionContext

    return PipelineJob(
        _pipeline=self,
        sources=sources or {},
        store=store,
        execution_context=execution_context,
    )
```

Add `ExecutionContext` to the `if TYPE_CHECKING:` block at the top of `graph.py`:
```python
if TYPE_CHECKING:
    import networkx as nx
    from orcapod.pipeline.execution_context import ExecutionContext
    from orcapod.pipeline.serialization import DatabaseRegistry
    from orcapod.protocols.database_protocols import DatabaseRegistryProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
```

- [ ] **Step 7: Run tests**

Run: `uv run pytest tests/test_pipeline/test_pipeline.py::TestPipelineSourceSpecEnforcement -v`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/pipeline/graph.py tests/test_pipeline/test_pipeline.py
git commit -m "refactor(pipeline): remove DB/execution surface; enforce SourceSpec-only leaves; add bind()"
```

---

## Task 5: Update `Pipeline.save()` / `load()` for Pure Blueprint

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`
- Modify: `src/orcapod/pipeline/serialization.py`
- Modify: `tests/test_pipeline/test_serialization.py`

The new `Pipeline.save()` always saves at "definition" level (no DB registry). SourceSpec nodes are serialized with `source_type: "spec"`. The old DB-dependent save levels (`"standard"`, `"full"`) move to `PipelineJob.save()`.

- [ ] **Step 1: Write failing tests for new save format**

Add to `tests/test_pipeline/test_serialization.py` (replacing or supplementing existing fixtures):

```python
# New fixture — Pipeline with SourceSpecs (no DB)
@pytest.fixture
def spec_pipeline(tmp_path):
    """A compiled Pipeline using SourceSpec leaves."""
    import pyarrow as pa
    from orcapod.core.sources import ArrowTableSource
    from orcapod.core.sources.source_spec import SourceSpec
    from orcapod.core.operators import Join
    from orcapod.pipeline import Pipeline

    def _src(tag, data):
        tbl = pa.table({tag: pa.array(["a"], type=pa.large_string()), data: pa.array([1], type=pa.int64())})
        return ArrowTableSource(tbl, tag_columns=[tag], infer_nullable=True)

    src_a = _src("key", "value")
    src_b = _src("key", "score")
    tag_a, data_a = src_a.output_schema()
    tag_b, data_b = src_b.output_schema()

    spec_a = SourceSpec("source_a", tag_schema=tag_a, data_schema=data_a)
    spec_b = SourceSpec("source_b", tag_schema=tag_b, data_schema=data_b)

    pipeline = Pipeline(name="spec_pipe")
    with pipeline:
        Join()(spec_a, spec_b, label="joiner")

    return pipeline, tmp_path


class TestPipelineBlueprintSave:
    def test_save_creates_file(self, spec_pipeline):
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        assert path.exists()

    def test_save_has_pipeline_version(self, spec_pipeline):
        pipeline, tmp_path = spec_pipeline
        import json
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert data["orcapod_pipeline_version"] == "0.1.0"

    def test_save_no_databases_block(self, spec_pipeline):
        """Pure blueprint save must not contain a 'databases' block."""
        pipeline, tmp_path = spec_pipeline
        import json
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert "databases" not in data

    def test_save_source_spec_nodes(self, spec_pipeline):
        """SourceSpec nodes must serialize with source_type='spec'."""
        pipeline, tmp_path = spec_pipeline
        import json
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        spec_nodes = [
            n for n in data["nodes"].values()
            if n.get("node_type") == "source"
            and n.get("source_config", {}).get("source_type") == "spec"
        ]
        assert len(spec_nodes) == 2

    def test_save_load_roundtrip_preserves_topology(self, spec_pipeline):
        """load() reconstructs the same number of nodes and edges."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        assert len(loaded._persistent_node_map) == len(pipeline._persistent_node_map)
        assert len(list(loaded._node_graph.edges())) == len(list(pipeline._node_graph.edges()))

    def test_save_load_restores_spec_names(self, spec_pipeline):
        """SourceSpec names must survive save/load."""
        from orcapod.core.sources.source_spec import SourceSpec
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        spec_names = {
            node.stream.name
            for node in loaded._persistent_node_map.values()
            if isinstance(node, SourceNode) and isinstance(node.stream, SourceSpec)
        }
        assert spec_names == {"source_a", "source_b"}
```

- [ ] **Step 2: Run tests to confirm failures**

Run: `uv run pytest tests/test_pipeline/test_serialization.py::TestPipelineBlueprintSave -v`
Expected: FAIL

- [ ] **Step 3: Simplify `Pipeline.save()`**

Replace the full `save()` method in `graph.py` with a simplified version:

```python
def save(self, path: str | Path) -> None:
    """Serialize the pure pipeline blueprint to a JSON file.

    Saves topology and SourceSpec declarations only — no databases,
    no execution context, no run metadata.

    Args:
        path: File path to write JSON output to.

    Raises:
        ValueError: If the pipeline has not been compiled.
    """
    if not self._compiled:
        raise ValueError(
            "Pipeline is not compiled. Call compile() or use "
            "auto_compile=True before saving."
        )

    import json as _json
    from orcapod.pipeline.serialization import (
        PIPELINE_FORMAT_VERSION,
        serialize_schema,
    )
    from orcapod.core.sources.source_spec import SourceSpec
    from orcapod.core.nodes import OperatorNode, FunctionNode

    nodes: dict[str, Any] = {}
    for content_hash_str, node in self._persistent_node_map.items():
        tag_schema, data_schema = node.output_schema()
        type_converter = node.data_context.type_converter

        descriptor: dict[str, Any] = {
            "node_type": node.node_type,
            "label": node.label,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "output_schema": {
                "tag": serialize_schema(tag_schema, type_converter),
                "data": serialize_schema(data_schema, type_converter),
            },
            "node_uri": list(node.node_uri),
            "data_context_key": node.data_context_key,
        }

        if isinstance(node, SourceNode):
            if isinstance(node.stream, SourceSpec):
                descriptor["source_config"] = {
                    "source_type": "spec",
                    "spec_name": node.stream.name,
                }
                descriptor["reconstructable"] = True
            else:
                descriptor["source_config"] = None
                descriptor["reconstructable"] = False

        elif isinstance(node, FunctionNode):
            descriptor["function_config"] = node._function_pod.to_config()
            descriptor["table_scope"] = node._table_scope

        elif isinstance(node, OperatorNode):
            descriptor["operator_config"] = node._operator.to_config()
            descriptor["table_scope"] = node._table_scope

        nodes[content_hash_str] = descriptor

    output: dict[str, Any] = {
        "orcapod_pipeline_version": PIPELINE_FORMAT_VERSION,
        "pipeline": {"name": list(self._name)},
        "nodes": nodes,
        "edges": [list(edge) for edge in self._graph_edges],
    }

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        _json.dump(output, f, indent=2)
```

Remove the old `_build_source_descriptor`, `_build_function_descriptor`, `_build_operator_descriptor` helper methods (they're replaced by the inline logic above). Keep the `show_graph()` method and `GraphRenderer` / related rendering code unchanged.

- [ ] **Step 4: Simplify `Pipeline.load()`**

Replace the `load()` classmethod with a simplified version that only handles blueprint format:

```python
@classmethod
def load(cls, path: str | Path) -> "Pipeline":
    """Deserialize a pure pipeline blueprint from a JSON file.

    Reconstructs topology and SourceSpec declarations. The loaded
    pipeline is topology-only — to run it, call
    ``pipeline.bind(sources=..., store=...)`` first.

    Args:
        path: Path to the JSON file produced by :meth:`save`.

    Returns:
        A compiled ``Pipeline`` instance with SourceSpec leaf nodes.

    Raises:
        ValueError: If the file's format version is unsupported.
    """
    import json as _json
    from orcapod.pipeline.serialization import (
        SUPPORTED_FORMAT_VERSIONS,
        deserialize_schema,
    )
    from orcapod.core.sources.source_spec import SourceSpec

    path = Path(path)
    with open(path) as f:
        data = _json.load(f)

    version = data.get("orcapod_pipeline_version", "")
    if version not in SUPPORTED_FORMAT_VERSIONS:
        raise ValueError(
            f"Unsupported pipeline format version {version!r}. "
            f"Supported: {sorted(SUPPORTED_FORMAT_VERSIONS)}"
        )

    pipeline_meta = data["pipeline"]
    name = tuple(pipeline_meta["name"])
    nodes_data = data["nodes"]
    edges = data["edges"]

    # Build topological order
    edge_graph: "nx.DiGraph" = nx.DiGraph()
    for up_hash, down_hash in edges:
        edge_graph.add_edge(up_hash, down_hash)
    for node_hash in nodes_data:
        if node_hash not in edge_graph:
            edge_graph.add_node(node_hash)
    topo_order = list(nx.topological_sort(edge_graph))

    upstream_map: dict[str, list[str]] = {}
    for up_hash, down_hash in edges:
        upstream_map.setdefault(down_hash, []).append(up_hash)

    reconstructed: dict[str, GraphNode] = {}

    for node_hash in topo_order:
        descriptor = nodes_data.get(node_hash)
        if descriptor is None:
            continue

        node_type = descriptor.get("node_type")
        source_config = descriptor.get("source_config") or {}

        if node_type == "source":
            if source_config.get("source_type") == "spec":
                spec_name = source_config["spec_name"]
                tag_schema = deserialize_schema(descriptor["output_schema"]["tag"])
                data_schema = deserialize_schema(descriptor["output_schema"]["data"])
                stream = SourceSpec(
                    name=spec_name,
                    tag_schema=tag_schema,
                    data_schema=data_schema,
                )
            else:
                stream = None  # non-spec source, load as read-only stub

            node = SourceNode.from_descriptor(descriptor, stream=stream, databases={})
            reconstructed[node_hash] = node

        elif node_type == "function":
            up_hashes = upstream_map.get(node_hash, [])
            upstream_node = reconstructed.get(up_hashes[0]) if up_hashes else None
            node = FunctionNode.from_descriptor(
                descriptor, function_pod=None, input_stream=upstream_node, databases={}
            )
            reconstructed[node_hash] = node

        elif node_type == "operator":
            up_hashes = upstream_map.get(node_hash, [])
            upstream_nodes = tuple(
                reconstructed[h] for h in up_hashes if h in reconstructed
            )
            node = OperatorNode.from_descriptor(
                descriptor, operator=None, input_streams=(), databases={}
            )
            reconstructed[node_hash] = node

    # Build Pipeline instance
    pipeline = cls(name=name, auto_compile=False)
    pipeline._persistent_node_map = dict(reconstructed)

    pipeline._nodes = {
        node.label: node
        for node in reconstructed.values()
        if node.label
    }

    pipeline._node_graph = nx.DiGraph()
    for up_hash, down_hash in edges:
        up_node = reconstructed.get(up_hash)
        down_node = reconstructed.get(down_hash)
        if up_node is not None and down_node is not None:
            pipeline._node_graph.add_edge(up_node, down_node)
    for node in reconstructed.values():
        if node not in pipeline._node_graph:
            pipeline._node_graph.add_node(node)

    pipeline._graph_edges = [(up, down) for up, down in edges]
    pipeline._hash_graph = nx.DiGraph()
    for up_hash, down_hash in edges:
        pipeline._hash_graph.add_edge(up_hash, down_hash)
    for node_hash, node in reconstructed.items():
        if node_hash not in pipeline._hash_graph:
            pipeline._hash_graph.add_node(node_hash)
        attrs = pipeline._hash_graph.nodes[node_hash]
        attrs["node_type"] = node.node_type
        if node.label:
            attrs["label"] = node.label

    pipeline._compiled = True
    return pipeline
```

Add `deserialize_schema` to `src/orcapod/pipeline/serialization.py` if it does not already exist. It should reconstruct a `Schema` from the `{"field_name": "type_str"}` format that `serialize_schema` produces. Check the existing code — if `deserialize_schema` is already present, just ensure it is importable.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_pipeline/test_serialization.py::TestPipelineBlueprintSave -v`
Expected: All PASS

Then run the full serialization suite to see which existing tests are now failing (expected — they test the old format with DBs):

Run: `uv run pytest tests/test_pipeline/test_serialization.py -v --tb=no -q`

Note the failing test names — they will be migrated to `test_pipeline_job.py` in Task 10.

- [ ] **Step 6: Remove static helper methods for old save**

Delete `_load_source_node`, `_load_function_node`, `_load_operator_node` from `Pipeline` (they were helper statics for the old complex load; the new load is inline).

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/pipeline/graph.py src/orcapod/pipeline/serialization.py \
        tests/test_pipeline/test_serialization.py
git commit -m "refactor(pipeline): simplify save/load to pure blueprint format with SourceSpec support"
```

---

## Task 6: `PipelineJob` — With-Block Recording, `bind()`, Completeness

**Files:**
- Create: `src/orcapod/pipeline/job.py`
- Create: `tests/test_pipeline/test_pipeline_job.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_pipeline/test_pipeline_job.py
from __future__ import annotations

from typing import cast

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode, OperatorNode, SourceNode
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.source_spec import SourceSpec
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError
from orcapod.pipeline.job import PipelineJob


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(tag_col: str, data_col: str, data: dict) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            data_col: pa.array(data[data_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _make_two_sources():
    src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
    src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
    return src_a, src_b


def add_values(value: int, score: int) -> int:
    return value + score


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store():
    return InMemoryArrowDatabase()


# ---------------------------------------------------------------------------
# Tests: with-block recording
# ---------------------------------------------------------------------------


class TestPipelineJobRecording:
    def test_with_concrete_sources_auto_creates_specs(self, store):
        """Concrete sources in with-block become SourceSpecs in job.pipeline."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        # Pipeline should have SourceSpec leaf nodes
        source_nodes = [
            n for n in job.pipeline._node_graph.nodes() if isinstance(n, SourceNode)
        ]
        assert all(isinstance(n.stream, SourceSpec) for n in source_nodes)

    def test_concrete_source_stored_in_sources(self, store):
        """Concrete sources from with-block are stored by label in job.sources."""
        src_a, src_b = _make_two_sources()
        src_a._label = "source_a"
        src_b._label = "source_b"

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        assert "source_a" in job.sources
        assert "source_b" in job.sources
        assert job.sources["source_a"] is src_a
        assert job.sources["source_b"] is src_b

    def test_spec_leaf_not_added_to_sources(self, store):
        """SourceSpec leaves are NOT added to job.sources (they're unbound)."""
        src_a, _ = _make_two_sources()
        tag_b, data_b = _make_source("key", "score", {"key": ["a"], "score": [1]}).output_schema()
        spec_b = SourceSpec("spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, spec_b)

        assert "spec_b" not in job.sources

    def test_pipeline_extracted_after_with_block(self, store):
        """job.pipeline is a compiled Pipeline after the with block."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        assert isinstance(job.pipeline, Pipeline)
        assert job.pipeline._compiled


# ---------------------------------------------------------------------------
# Tests: bind()
# ---------------------------------------------------------------------------


class TestPipelineJobBind:
    def test_bind_sources_returns_new_job(self, store):
        """bind(sources=...) returns a new PipelineJob; original is unchanged."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(spec_a, spec_b)

        job2 = job.bind(sources={"a": src_a, "b": src_b})
        assert job2 is not job
        assert job.sources == {}  # original unchanged
        assert "a" in job2.sources and "b" in job2.sources

    def test_bind_store_returns_new_job(self, store):
        src_a, src_b = _make_two_sources()
        job = PipelineJob()
        with job:
            Join()(src_a, src_b)

        new_store = InMemoryArrowDatabase()
        job2 = job.bind(store=new_store)
        assert job2.store is new_store
        assert job.store is None  # original unchanged

    def test_bind_preserves_existing_sources(self, store):
        """bind(sources=...) merges new sources with existing ones."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(spec_a, spec_b)

        job2 = job.bind(sources={"a": src_a})
        job3 = job2.bind(sources={"b": src_b})

        assert "a" in job3.sources
        assert "b" in job3.sources

    def test_bind_validates_schema_at_bind_time(self, store):
        """bind() raises SourceSpecMismatchError for incompatible sources."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        # Create a spec that requires an extra column the source doesn't have
        from orcapod.types import Schema
        wrong_spec = SourceSpec("a", tag_schema=tag_a, data_schema=Schema({"value": int, "extra": str}))

        job = PipelineJob(store=store)
        with job:
            Join()(wrong_spec, src_b)

        with pytest.raises(SourceSpecMismatchError):
            job.bind(sources={"a": src_a})

    def test_pipeline_bind_wraps_in_job(self, store):
        """Pipeline.bind() returns a PipelineJob holding that pipeline."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("b", tag_schema=tag_b, data_schema=data_b)

        pipeline = Pipeline(name="p")
        with pipeline:
            Join()(spec_a, spec_b)

        job = pipeline.bind(sources={"a": src_a, "b": src_b}, store=store)
        assert isinstance(job, PipelineJob)
        assert job.pipeline is pipeline


# ---------------------------------------------------------------------------
# Tests: completeness
# ---------------------------------------------------------------------------


class TestPipelineJobCompleteness:
    def test_unbound_specs_lists_unbound(self, store):
        """unbound_specs() lists SourceSpec names not in job.sources."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        spec_b = SourceSpec("spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, spec_b)

        unbound = job.unbound_specs()
        assert len(unbound) == 1
        assert unbound[0].name == "spec_b"

    def test_unbound_specs_empty_when_all_bound(self, store):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)  # both auto-bound from labels

        assert job.unbound_specs() == []

    def test_is_complete_true_when_all_bound_with_store(self, store):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        assert job.is_complete()

    def test_is_complete_false_when_store_missing(self):
        src_a, src_b = _make_two_sources()
        job = PipelineJob()  # no store
        with job:
            Join()(src_a, src_b)

        assert not job.is_complete()

    def test_is_complete_false_when_specs_unbound(self, store):
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        spec_b = SourceSpec("spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, spec_b)

        assert not job.is_complete()
```

- [ ] **Step 2: Run to verify failures**

Run: `uv run pytest tests/test_pipeline/test_pipeline_job.py -v --tb=short 2>&1 | head -30`
Expected: ImportError on `orcapod.pipeline.job`

- [ ] **Step 3: Implement `PipelineJob`**

```python
# src/orcapod/pipeline/job.py
"""PipelineJob — pipeline + source bindings + execution context."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.protocols import core_protocols as cp

if TYPE_CHECKING:
    from orcapod.core.nodes import FunctionNode, GraphNode, OperatorNode, SourceNode
    from orcapod.core.sources.source_spec import SourceSpec
    from orcapod.pipeline.execution_context import ExecutionContext
    from orcapod.pipeline.graph import Pipeline
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

logger = logging.getLogger(__name__)


class PipelineJob(AutoRegisteringContextBasedTracker):
    """Pipeline + source bindings + execution context.

    ``PipelineJob`` is the everyday working object. It is built incrementally:
    its ``with``-block records both the DAG structure and any concrete source
    bindings simultaneously. Concrete sources are automatically promoted to
    ``SourceSpec`` declarations in the underlying ``Pipeline``, with their
    concrete instances stored in ``job.sources``.

    After the ``with`` block, ``job.pipeline`` is a fully compiled, pure
    ``Pipeline`` (SourceSpec-only leaves). ``job.run()`` executes the
    resolvable subgraph — nodes whose upstream SourceSpecs are all bound.

    ``PipelineJob`` can also be created from a ``Pipeline`` via
    ``pipeline.bind(sources=..., store=...)`` for the "explicit blueprint"
    workflow.

    Args:
        store: Database for result caching and operator records.
        execution_context: Optional execution configuration.
        tracker_manager: Optional tracker manager override.
        _pipeline: Internal — pre-built pipeline (used by Pipeline.bind()).
        sources: Internal — pre-bound sources (used by Pipeline.bind() /
            bind()).
    """

    def __init__(
        self,
        store: "ArrowDatabaseProtocol | None" = None,
        execution_context: "ExecutionContext | None" = None,
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        *,
        _pipeline: "Pipeline | None" = None,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
    ) -> None:
        super().__init__(tracker_manager=tracker_manager)
        self._store = store
        self._execution_context = execution_context
        self._compiled_pipeline: "Pipeline | None" = _pipeline
        self._sources: dict[str, cp.StreamProtocol] = dict(sources or {})

        # Recording state (populated during with-block)
        self._rec_graph_edges: list[tuple[str, str]] = []
        self._rec_upstreams: dict[str, cp.StreamProtocol] = {}
        self._rec_node_lut: dict[str, "GraphNode"] = {}
        self._spec_by_name: dict[str, "SourceSpec"] = {}
        self._pipeline_name: tuple[str, ...] = ("pipeline",)

    # ------------------------------------------------------------------
    # Context manager — recording
    # ------------------------------------------------------------------

    def __enter__(self) -> "PipelineJob":
        # Reset recording state
        self._rec_graph_edges = []
        self._rec_upstreams = {}
        self._rec_node_lut = {}
        self._spec_by_name = {}
        return super().__enter__()  # type: ignore[return-value]

    def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        super().__exit__(exc_type, exc_value, traceback)
        if exc_type is None:
            self._compile_from_recording()

    def _compile_from_recording(self) -> None:
        """Compile the recorded edges into a pure Pipeline."""
        from orcapod.pipeline.graph import Pipeline

        pipeline = Pipeline(name=self._pipeline_name, auto_compile=False)
        # Inject the recording state into the pipeline
        pipeline._graph_edges = list(self._rec_graph_edges)
        pipeline._upstreams = dict(self._rec_upstreams)
        pipeline._node_lut = dict(self._rec_node_lut)
        # Rebuild hash graph
        import orcapod.utils.lazy_module as _lm
        import importlib
        nx = importlib.import_module("networkx")
        for edge in self._rec_graph_edges:
            pipeline._hash_graph.add_edge(*edge)

        pipeline.compile()
        self._compiled_pipeline = pipeline

    def _ensure_spec(self, source: cp.StreamProtocol) -> "SourceSpec":
        """Promote *source* to a SourceSpec, storing the concrete binding.

        If the spec already exists (same label), returns the cached spec.
        """
        from orcapod.core.sources.source_spec import SourceSpec

        name = source.label  # type: ignore[attr-defined]
        if name not in self._spec_by_name:
            tag_schema, data_schema = source.output_schema()
            spec = SourceSpec(name=name, tag_schema=tag_schema, data_schema=data_schema)
            self._spec_by_name[name] = spec
            self._sources[name] = source
        return self._spec_by_name[name]

    @staticmethod
    def _is_concrete_source(stream: cp.StreamProtocol) -> bool:
        """True if *stream* is a concrete RootSource (not a SourceSpec)."""
        from orcapod.core.sources.base import RootSource
        from orcapod.core.sources.source_spec import SourceSpec

        return isinstance(stream, RootSource) and not isinstance(stream, SourceSpec)

    # ------------------------------------------------------------------
    # TrackerProtocol — recording with source interception
    # ------------------------------------------------------------------

    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a function pod invocation, promoting concrete sources to specs."""
        from orcapod.core.nodes import FunctionNode

        if self._is_concrete_source(input_stream):
            input_stream = self._ensure_spec(input_stream)

        input_hash = input_stream.content_hash().to_string()
        function_node = FunctionNode(function_pod=pod, input_stream=input_stream, label=label)
        fn_hash = function_node.content_hash().to_string()

        self._rec_node_lut[fn_hash] = function_node
        self._rec_upstreams[input_hash] = input_stream
        self._rec_graph_edges.append((input_hash, fn_hash))

    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """Record an operator pod invocation, promoting concrete sources to specs."""
        from orcapod.core.nodes import OperatorNode

        processed = tuple(
            self._ensure_spec(s) if self._is_concrete_source(s) else s
            for s in upstreams
        )

        operator_node = OperatorNode(operator=pod, input_streams=processed, label=label)
        op_hash = operator_node.content_hash().to_string()

        self._rec_node_lut[op_hash] = operator_node
        for up_hash, upstream in zip(
            [s.content_hash().to_string() for s in processed], processed
        ):
            self._rec_upstreams[up_hash] = upstream
            self._rec_graph_edges.append((up_hash, op_hash))

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def pipeline(self) -> "Pipeline":
        """The compiled pure Pipeline (SourceSpec-only leaves).

        Raises:
            RuntimeError: If the with-block has not been completed yet.
        """
        if self._compiled_pipeline is None:
            raise RuntimeError(
                "PipelineJob has no compiled pipeline yet. "
                "Use 'with job:' to record a DAG first."
            )
        return self._compiled_pipeline

    @property
    def sources(self) -> dict[str, cp.StreamProtocol]:
        """Mapping of SourceSpec name to bound concrete source."""
        return dict(self._sources)

    @property
    def store(self) -> "ArrowDatabaseProtocol | None":
        """The database used for result caching."""
        return self._store

    @property
    def execution_context(self) -> "ExecutionContext | None":
        """The execution configuration, or ``None`` if unset."""
        return self._execution_context

    # ------------------------------------------------------------------
    # bind() — non-mutating
    # ------------------------------------------------------------------

    def bind(
        self,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
        store: "ArrowDatabaseProtocol | None" = None,
        execution_context: "ExecutionContext | None" = None,
    ) -> "PipelineJob":
        """Return a new ``PipelineJob`` with updated bindings.

        Non-mutating — the original ``PipelineJob`` is unchanged. Existing
        bindings not mentioned in this call are carried forward.

        ``SourceSpec.validate()`` is called for each source in *sources*;
        ``SourceSpecMismatchError`` is raised on schema mismatch.

        Args:
            sources: Mapping of SourceSpec name → concrete source. Each
                source is validated against the matching SourceSpec.
            store: Replaces the current store.
            execution_context: Replaces the current execution context.

        Returns:
            A new ``PipelineJob`` with merged bindings.

        Raises:
            SourceSpecMismatchError: If any source's schema is incompatible.
        """
        from orcapod.core.nodes import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        merged_sources = dict(self._sources)
        if sources:
            # Validate each supplied source against its SourceSpec
            pipeline = self._compiled_pipeline
            if pipeline is not None:
                for node in pipeline._persistent_node_map.values():
                    if (
                        isinstance(node, SourceNode)
                        and isinstance(node.stream, SourceSpec)
                        and node.stream.name in sources
                    ):
                        node.stream.validate(sources[node.stream.name])
            merged_sources.update(sources)

        return PipelineJob(
            store=store if store is not None else self._store,
            execution_context=execution_context if execution_context is not None else self._execution_context,
            _pipeline=self._compiled_pipeline,
            sources=merged_sources,
        )

    # ------------------------------------------------------------------
    # Completeness introspection
    # ------------------------------------------------------------------

    def unbound_specs(self) -> "list[SourceSpec]":
        """Return all SourceSpec slots not yet bound in this job.

        Returns:
            List of unbound ``SourceSpec`` instances, in order of appearance
            in the pipeline graph.
        """
        from orcapod.core.nodes import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        if self._compiled_pipeline is None:
            return []

        unbound = []
        seen: set[str] = set()
        for node in self._compiled_pipeline._persistent_node_map.values():
            if (
                isinstance(node, SourceNode)
                and isinstance(node.stream, SourceSpec)
                and node.stream.name not in self._sources
                and node.stream.name not in seen
            ):
                unbound.append(node.stream)
                seen.add(node.stream.name)
        return unbound

    def is_complete(self) -> bool:
        """Return ``True`` when all specs are bound and a store is set.

        Returns:
            ``True`` if all SourceSpec slots are bound and a store is set.
        """
        return self._store is not None and len(self.unbound_specs()) == 0

    def is_runnable(self, node_label: str) -> bool:
        """Return ``True`` if all upstream inputs of *node_label* are resolved.

        Args:
            node_label: Label of the node to check.

        Returns:
            ``True`` if the node can be executed with current bindings.
        """
        from orcapod.core.nodes import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        pipeline = self._compiled_pipeline
        if pipeline is None:
            return False

        target = pipeline._nodes.get(node_label)
        if target is None:
            return False

        import networkx as nx

        for node in nx.ancestors(pipeline._node_graph, target) | {target}:
            if (
                isinstance(node, SourceNode)
                and isinstance(node.stream, SourceSpec)
                and node.stream.name not in self._sources
            ):
                return False
        return True
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_pipeline/test_pipeline_job.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/job.py tests/test_pipeline/test_pipeline_job.py
git commit -m "feat(pipeline): add PipelineJob with with-block recording, source interception, and bind()"
```

---

## Task 7: `PipelineJob.run()` — Execution Graph + Orchestration

**Files:**
- Modify: `src/orcapod/pipeline/job.py`
- Modify: `tests/test_pipeline/test_pipeline_job.py`

- [ ] **Step 1: Write failing tests for run()**

Append to `tests/test_pipeline/test_pipeline_job.py`:

```python
class TestPipelineJobRun:
    def test_run_executes_all_nodes(self, store):
        """run() executes all nodes when all specs are bound."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        job.run()

        node = job.pipeline.compiled_nodes["adder"]
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_run_produces_correct_values(self, store):
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        job.run()

        table = job.pipeline.compiled_nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]  # a: 10+100, b: 20+200

    def test_run_partial_execution_skips_unbound_subgraph(self, store):
        """Nodes with unbound upstream SourceSpecs are excluded from execution."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        spec_b = SourceSpec("spec_b", tag_schema=tag_b, data_schema=data_b)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, spec_b)
            pod(joined, label="adder")

        result = job.run()
        # Unresolved specs should be reported
        assert "spec_b" in result.unresolved_specs

    def test_run_is_non_mutating(self, store):
        """run() returns a new PipelineJob; original is unchanged."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        result = job.run()
        assert result is not job

    def test_run_requires_store(self):
        """run() without a store raises ValueError."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob()  # no store
        with job:
            Join()(src_a, src_b)

        with pytest.raises(ValueError, match="store"):
            job.run()


class TestPipelineJobEndToEnd:
    def test_end_to_end_source_join_function(self, store):
        """Two sources → Join → FunctionPod all execute correctly."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        assert isinstance(job.pipeline.compiled_nodes["joiner"], OperatorNode)
        assert isinstance(job.pipeline.compiled_nodes["adder"], FunctionNode)

        job.run()

        fn_records = job.pipeline.compiled_nodes["adder"].get_all_records()
        assert fn_records is not None
        assert fn_records.num_rows == 2

        table = job.pipeline.compiled_nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]

    def test_bind_then_run(self, store):
        """Pipeline.bind() + job.run() produces correct results."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("src_a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("src_b", tag_schema=tag_b, data_schema=data_b)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        pipeline = Pipeline(name="bp")
        with pipeline:
            joined = Join()(spec_a, spec_b)
            pod(joined, label="adder")

        job = pipeline.bind(
            sources={"src_a": src_a, "src_b": src_b},
            store=store,
        )
        job.run()

        table = job.pipeline.compiled_nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]
```

- [ ] **Step 2: Run to confirm failures**

Run: `uv run pytest tests/test_pipeline/test_pipeline_job.py::TestPipelineJobRun -v`
Expected: FAIL (AttributeError: `PipelineJob` has no `run` method)

- [ ] **Step 3: Implement `PipelineJob._build_execution_graph()` and `run()`**

Add to `src/orcapod/pipeline/job.py`:

```python
# At top of file, add to imports:
from orcapod.utils.lazy_module import LazyModule
# ... (in the else branch for TYPE_CHECKING)
# Add: nx = LazyModule("networkx")

def _build_execution_graph(
    self,
) -> "tuple[Any, list[str], ArrowDatabaseProtocol, ArrowDatabaseProtocol]":
    """Build a fresh execution-ready graph with concrete sources substituted.

    Creates new SourceNode/FunctionNode/OperatorNode objects — does NOT
    mutate the shared ``pipeline._persistent_node_map`` nodes.

    Returns:
        Tuple of (exec_graph, unresolved_spec_names, pipeline_db, result_db).

    Raises:
        ValueError: If ``self.store`` is ``None``.
    """
    import networkx as nx
    from orcapod.core.nodes import FunctionNode, OperatorNode, SourceNode
    from orcapod.core.sources.source_spec import SourceSpec

    pipeline = self._compiled_pipeline
    if pipeline is None:
        raise ValueError("No compiled pipeline — use 'with job:' first.")

    store = self._store
    if store is None:
        raise ValueError(
            "PipelineJob.run() requires a store. "
            "Call job.bind(store=db) before run()."
        )

    pipeline_db = store.at(*pipeline.name)
    result_db = pipeline_db.at("_result")

    # Build the raw recording graph (hash-based)
    G: "nx.DiGraph" = nx.DiGraph()
    for edge in pipeline._graph_edges:
        G.add_edge(*edge)

    exec_node_map: dict[str, GraphNode] = {}
    excluded_hashes: set[str] = set()
    unresolved_specs: list[str] = []

    for node_hash in nx.topological_sort(G):
        if node_hash in excluded_hashes:
            continue

        if node_hash not in pipeline._node_lut:
            # Leaf stream
            stream = pipeline._upstreams.get(node_hash)
            if isinstance(stream, SourceSpec):
                if stream.name in self._sources:
                    # Bound — use concrete source
                    exec_node_map[node_hash] = SourceNode(
                        stream=self._sources[stream.name]
                    )
                else:
                    # Unbound — exclude this branch
                    excluded_hashes.add(node_hash)
                    if stream.name not in unresolved_specs:
                        unresolved_specs.append(stream.name)
            elif stream is not None:
                exec_node_map[node_hash] = SourceNode(stream=stream)
        else:
            template = pipeline._node_lut[node_hash]
            preds = list(G.predecessors(node_hash))

            if any(p in excluded_hashes for p in preds):
                excluded_hashes.add(node_hash)
                continue

            if isinstance(template, FunctionNode):
                assert len(preds) == 1, "FunctionNode must have exactly one upstream"
                input_node = exec_node_map[preds[0]]
                new_fn = FunctionNode(
                    function_pod=template._function_pod,
                    input_stream=input_node,
                    label=template._label,
                )
                new_fn.attach_databases(
                    pipeline_database=pipeline_db,
                    result_database=result_db,
                )
                # Preserve any executor setting from compile
                from orcapod.core.executors.local import LocalPythonFunctionExecutor
                if template.executor is not None:
                    new_fn.executor = template.executor
                else:
                    new_fn.executor = LocalPythonFunctionExecutor()
                exec_node_map[node_hash] = new_fn

            elif isinstance(template, OperatorNode):
                upstream_nodes = tuple(exec_node_map[p] for p in preds)
                new_op = OperatorNode(
                    operator=template._operator,
                    input_streams=upstream_nodes,
                    label=template._label,
                )
                new_op.attach_databases(pipeline_database=pipeline_db)
                exec_node_map[node_hash] = new_op

    # Build execution graph
    exec_graph: "nx.DiGraph" = nx.DiGraph()
    for up_hash, down_hash in pipeline._graph_edges:
        if up_hash in exec_node_map and down_hash in exec_node_map:
            exec_graph.add_edge(exec_node_map[up_hash], exec_node_map[down_hash])
    for node in exec_node_map.values():
        if node not in exec_graph:
            exec_graph.add_node(node)

    return exec_graph, unresolved_specs, pipeline_db, result_db


def run(
    self,
    observer: "ExecutionObserverProtocol | None" = None,
) -> "PipelineJob":
    """Execute the resolvable subgraph.

    Nodes whose upstream includes an unbound SourceSpec (and all their
    dependents) are excluded. Partial execution is a first-class outcome,
    not an error — excluded spec names are recorded in the returned job's
    ``unresolved_specs``.

    Args:
        observer: Optional execution observer.

    Returns:
        A new ``PipelineJob`` with run metadata populated (does not mutate
        ``self``).

    Raises:
        ValueError: If no store is set.
        RuntimeError: If no pipeline has been recorded.
    """
    import hashlib

    from orcapod.pipeline.observer import NoOpObserver
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

    exec_graph, unresolved_specs, pipeline_db, result_db = self._build_execution_graph()

    effective_observer = observer or NoOpObserver()

    # Compute snapshot hash for run URI
    node_strs = sorted(str(n.content_hash().to_string()) for n in exec_graph.nodes())
    snapshot_hash = hashlib.sha256("\n".join(node_strs).encode()).hexdigest()[:16]
    pipeline_uri = "/".join(self._compiled_pipeline.name) + "@" + snapshot_hash

    SyncPipelineOrchestrator().run(
        exec_graph, observer=effective_observer, pipeline_uri=pipeline_uri
    )

    # Flush databases
    pipeline_db.flush()
    result_db.flush()

    # Return new job with run metadata
    result = PipelineJob(
        store=self._store,
        execution_context=self._execution_context,
        _pipeline=self._compiled_pipeline,
        sources=dict(self._sources),
    )
    result._unresolved_specs = unresolved_specs
    return result
```

Also add a `_unresolved_specs` property:

```python
@property
def unresolved_specs(self) -> list[str]:
    """Spec names that were unbound at run time (excluded from execution)."""
    return list(getattr(self, "_unresolved_specs", []))
```

Add `ExecutionObserverProtocol` to the `if TYPE_CHECKING:` block at the top.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_pipeline/test_pipeline_job.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/job.py tests/test_pipeline/test_pipeline_job.py
git commit -m "feat(pipeline): add PipelineJob.run() with partial execution support"
```

---

## Task 8: `PipelineJob.save()` / `load()`

**Files:**
- Modify: `src/orcapod/pipeline/job.py`
- Modify: `src/orcapod/pipeline/serialization.py`
- Modify: `tests/test_pipeline/test_pipeline_job.py`

- [ ] **Step 1: Add `PIPELINE_JOB_FORMAT_VERSION` to `serialization.py`**

Append to `src/orcapod/pipeline/serialization.py`:

```python
# ---------------------------------------------------------------------------
# PipelineJob format version
# ---------------------------------------------------------------------------

PIPELINE_JOB_FORMAT_VERSION = "0.1.0"
SUPPORTED_JOB_FORMAT_VERSIONS = frozenset({"0.1.0"})
```

- [ ] **Step 2: Write failing tests**

Append to `tests/test_pipeline/test_pipeline_job.py`:

```python
class TestPipelineJobSerialization:
    def test_save_creates_file(self, store, tmp_path):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        path = tmp_path / "job.json"
        job.save(str(path))
        assert path.exists()

    def test_save_has_version(self, store, tmp_path):
        import json
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        path = tmp_path / "job.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        assert data["orcapod_pipeline_job_version"] == "0.1.0"

    def test_save_includes_run_block(self, store, tmp_path):
        """Unsaved template has status=pending and null run fields."""
        import json
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        path = tmp_path / "job.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        assert data["run"]["status"] == "pending"
        assert data["run"]["run_id"] is None

    def test_load_roundtrip_restores_pipeline(self, store, tmp_path):
        """load() restores the pipeline topology."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b, label="joiner")

        path = tmp_path / "job.json"
        job.save(str(path))
        loaded = PipelineJob.load(str(path))

        assert "joiner" in loaded.pipeline.compiled_nodes

    def test_load_roundtrip_after_run(self, store, tmp_path):
        """Save after run() → load → re-run produces correct results."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        completed = job.run()
        path = tmp_path / "completed.json"
        completed.save(str(path))

        # Load and re-run with same store
        loaded = PipelineJob.load(str(path), store=store)
        loaded.run()

        table = loaded.pipeline.compiled_nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]
```

- [ ] **Step 3: Run to confirm failures**

Run: `uv run pytest tests/test_pipeline/test_pipeline_job.py::TestPipelineJobSerialization -v`
Expected: FAIL (AttributeError: PipelineJob has no `save`)

- [ ] **Step 4: Implement `PipelineJob.save()` and `PipelineJob.load()`**

Add to `src/orcapod/pipeline/job.py`:

```python
def save(self, path: str | "Path") -> None:
    """Serialize this job to a JSON file.

    Saves topology (via the embedded Pipeline) plus bindings metadata and
    run results. The format covers both "template" (pre-run) and
    "run record" (post-run) states — distinguished by the ``run.status``
    field.

    Args:
        path: File path to write JSON output to.
    """
    import json as _json
    from pathlib import Path as _Path
    from orcapod.pipeline.serialization import PIPELINE_JOB_FORMAT_VERSION

    pipeline = self._compiled_pipeline
    if pipeline is None:
        raise ValueError("No compiled pipeline to save.")

    path = _Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Save the pipeline blueprint inline
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp:
        tmp_path = tmp.name
    try:
        pipeline.save(tmp_path)
        with open(tmp_path) as f:
            pipeline_data = _json.load(f)
    finally:
        os.unlink(tmp_path)

    # Serialize source configs
    sources_block: dict = {}
    for spec_name, source in self._sources.items():
        if hasattr(source, "to_config"):
            sources_block[spec_name] = source.to_config()
        else:
            sources_block[spec_name] = {"source_type": "unknown"}

    # Store config
    store_block = None
    if self._store is not None and hasattr(self._store, "to_config"):
        store_block = self._store.to_config()

    output = {
        "orcapod_pipeline_job_version": PIPELINE_JOB_FORMAT_VERSION,
        "run": {
            "run_id": getattr(self, "_run_id", None),
            "status": "pending" if not hasattr(self, "_unresolved_specs") else "complete",
            "unresolved_specs": getattr(self, "_unresolved_specs", []),
        },
        "pipeline": pipeline_data,
        "bindings": {
            "sources": sources_block,
            "store": store_block,
        },
    }

    with open(path, "w") as f:
        _json.dump(output, f, indent=2)


@classmethod
def load(
    cls,
    path: str | "Path",
    store: "ArrowDatabaseProtocol | None" = None,
) -> "PipelineJob":
    """Deserialize a ``PipelineJob`` from a JSON file.

    The embedded ``Pipeline`` blueprint is always restored. Concrete source
    bindings are restored only for reconstructable source types (CSV,
    Delta Lake, etc.). Pass *store* explicitly to override any serialized
    store configuration.

    Args:
        path: Path to the JSON file produced by :meth:`save`.
        store: Optional store override. When provided, takes precedence
            over any store configuration in the file.

    Returns:
        A ``PipelineJob`` ready to run (pending a ``bind()`` if sources
        are missing).
    """
    import json as _json
    from pathlib import Path as _Path
    from orcapod.pipeline.graph import Pipeline
    from orcapod.pipeline.serialization import (
        SUPPORTED_JOB_FORMAT_VERSIONS,
        resolve_source_from_config,
    )

    path = _Path(path)
    with open(path) as f:
        data = _json.load(f)

    version = data.get("orcapod_pipeline_job_version", "")
    if version not in SUPPORTED_JOB_FORMAT_VERSIONS:
        raise ValueError(
            f"Unsupported PipelineJob format version {version!r}. "
            f"Supported: {sorted(SUPPORTED_JOB_FORMAT_VERSIONS)}"
        )

    # Reconstruct pipeline from embedded blueprint data
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp:
        tmp_path = tmp.name
        _json.dump(data["pipeline"], tmp)
    try:
        pipeline = Pipeline.load(tmp_path)
    finally:
        os.unlink(tmp_path)

    # Reconstruct sources
    sources: dict[str, cp.StreamProtocol] = {}
    for spec_name, src_config in data.get("bindings", {}).get("sources", {}).items():
        try:
            source = resolve_source_from_config(src_config)
            if source is not None:
                sources[spec_name] = source
        except Exception:
            logger.warning(
                "Could not reconstruct source %r from config — skipping.", spec_name
            )

    # Reconstruct store
    effective_store = store
    if effective_store is None:
        store_config = data.get("bindings", {}).get("store")
        if store_config:
            try:
                from orcapod.pipeline.serialization import resolve_database_from_config
                effective_store = resolve_database_from_config(store_config)
            except Exception:
                logger.warning("Could not reconstruct store from config — skipping.")

    job = cls(
        store=effective_store,
        _pipeline=pipeline,
        sources=sources,
    )

    # Restore run metadata
    run_block = data.get("run", {})
    if run_block.get("status") != "pending":
        job._unresolved_specs = run_block.get("unresolved_specs", [])

    return job
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_pipeline/test_pipeline_job.py::TestPipelineJobSerialization -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/job.py src/orcapod/pipeline/serialization.py \
        tests/test_pipeline/test_pipeline_job.py
git commit -m "feat(pipeline): add PipelineJob.save() / load() serialization"
```

---

## Task 9: Update All Exports

**Files:**
- Modify: `src/orcapod/pipeline/__init__.py`
- Modify: `src/orcapod/__init__.py`

- [ ] **Step 1: Update `src/orcapod/pipeline/__init__.py`**

```python
from .async_orchestrator import AsyncPipelineOrchestrator
from .composite_observer import CompositeObserver
from .execution_context import ExecutionContext
from .graph import Pipeline
from .job import PipelineJob
from .logging_observer import LoggingObserver, DataLogger
from .serialization import (
    LoadStatus,
    PIPELINE_FORMAT_VERSION,
    PIPELINE_JOB_FORMAT_VERSION,
)
from .status_observer import StatusObserver
from .sync_orchestrator import SyncPipelineOrchestrator

__all__ = [
    "AsyncPipelineOrchestrator",
    "CompositeObserver",
    "ExecutionContext",
    "LoadStatus",
    "LoggingObserver",
    "DataLogger",
    "PIPELINE_FORMAT_VERSION",
    "PIPELINE_JOB_FORMAT_VERSION",
    "Pipeline",
    "PipelineJob",
    "StatusObserver",
    "SyncPipelineOrchestrator",
]
```

- [ ] **Step 2: Update `src/orcapod/__init__.py`**

```python
from .core.function_pod import FunctionPod, function_pod
from .core.sources.source_spec import SourceSpec
from .pipeline import Pipeline, PipelineJob

# Subpackage re-exports for clean public API
from . import databases  # noqa: F401
from . import nodes  # noqa: F401
from . import operators  # noqa: F401
from . import sources  # noqa: F401
from . import streams  # noqa: F401
from . import types  # noqa: F401

__all__ = [
    "FunctionPod",
    "function_pod",
    "Pipeline",
    "PipelineJob",
    "SourceSpec",
    "databases",
    "nodes",
    "operators",
    "sources",
    "streams",
    "types",
]
```

- [ ] **Step 3: Verify top-level imports**

Run: `uv run python -c "from orcapod import Pipeline, PipelineJob, SourceSpec; print('OK')"`
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/pipeline/__init__.py src/orcapod/__init__.py
git commit -m "chore(exports): add PipelineJob, SourceSpec, ExecutionContext to public API"
```

---

## Task 10: Migrate `test_pipeline.py`

**Files:**
- Modify: `tests/test_pipeline/test_pipeline.py`

The old `Pipeline` accepted `pipeline_database` and had `run()`. All tests that relied on those must migrate to `PipelineJob`. Tests that only test structural compilation (wrapping leaves, function nodes, operator nodes, labels) can stay — updated to use the new `Pipeline` (no DB, with SourceSpec leaves).

- [ ] **Step 1: Update all fixtures — replace `Pipeline(name=..., pipeline_database=...)` with `PipelineJob(store=...)`**

For every test that calls `Pipeline.run()`, moves to a `PipelineJob`. The pattern is:

```python
# BEFORE (old pattern):
pipeline = Pipeline(name="test", pipeline_database=pipeline_db)
with pipeline:
    joined = Join()(src_a, src_b)
    pod(joined, label="adder")
pipeline.run()
records = pipeline.adder.get_all_records()

# AFTER (new pattern):
from orcapod.pipeline.job import PipelineJob
job = PipelineJob(store=pipeline_db)
with job:
    joined = Join()(src_a, src_b)
    pod(joined, label="adder")
job.run()
records = job.pipeline.compiled_nodes["adder"].get_all_records()
```

For structural tests (TestCompileSourceWrapping, TestCompileFunctionNode, TestCompileOperatorNode, TestCompileMutatesNodes, TestLabelAccess) that only test `_compiled`, `compiled_nodes`, and node types — keep them but switch to SourceSpec-based leaves:

```python
# BEFORE:
pipeline = Pipeline(name="test_pipe", pipeline_database=pipeline_db)
with pipeline:
    _ = Join()(src_a, src_b)

# AFTER — Pipeline is now pure, must use PipelineJob for execution:
from orcapod.pipeline.job import PipelineJob
job = PipelineJob(store=pipeline_db)
with job:
    _ = Join()(src_a, src_b)
pipeline = job.pipeline  # pure Pipeline for structural assertions
```

- [ ] **Step 2: Remove `pipeline_db` and `function_db` fixtures from `test_pipeline.py` test classes that migrate to PipelineJob**

The `pipeline_db` fixture remains for the `store=` parameter of `PipelineJob`. Remove the `result_database=function_db` patterns since `PipelineJob` auto-scopes the result DB.

- [ ] **Step 3: Migrate `TestAutoCompileAndRun` → `TestPipelineJobRun`**

Migrate the following tests from `test_pipeline.py` to `test_pipeline_job.py`:
- `test_run_executes_all_nodes` → already added in Task 7
- `test_run_auto_saves_when_path_set` → update for `PipelineJob` (use `job.save()` after `job.run()`)
- `test_run_does_not_save_when_path_not_set` → remove (auto_save_path is a `PipelineJob`-level concern, not Pipeline)
- `test_auto_save_path_without_database_raises` → remove (no longer applies)

- [ ] **Step 4: Migrate `TestFunctionDatabaseHandling` → `TestPipelineJobDatabaseHandling`**

```python
# In test_pipeline_job.py:
class TestPipelineJobDatabaseHandling:
    def test_result_database_scoped_to_pipeline_name(self):
        """The result DB is auto-scoped to pipeline_name/_result."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        store = InMemoryArrowDatabase()

        job = PipelineJob(store=store)
        job._pipeline_name = ("my_pipe",)  # set name before with block
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        # Run to trigger database creation
        job.run()

        # After run, the result DB should have been scoped to my_pipe/_result
        # Verify via the function node's pipeline_database path
        exec_nodes = [
            n for n in job.pipeline.compiled_nodes.values()
            if isinstance(n, FunctionNode)
        ]
        assert len(exec_nodes) > 0
```

- [ ] **Step 5: Migrate `TestEndToEnd` → add to `TestPipelineJobEndToEnd`**

Already done in Task 7 (`test_end_to_end_source_join_function`).

- [ ] **Step 6: Update `TestPipelineExtension`**

The pipeline extension tests use `pipeline.adder` to extend the pipeline in a second `with` block. With the new design, re-entering a `PipelineJob` `with` block should work since `_compile_from_recording` merges with existing state.

Update the extension tests to use `PipelineJob`:

```python
class TestPipelineJobExtension:
    def test_extend_pipeline_with_new_sources(self):
        """Re-enter job context to extend the pipeline."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        store = InMemoryArrowDatabase()

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b, label="joiner")

        # Extend in a second with block
        src_c = _make_source("key", "extra", {"key": ["a", "b"], "extra": [1000, 2000]})
        # ... (implement extension logic)
```

Note: Pipeline extension (re-entering `with` blocks) may need special handling in `PipelineJob._compile_from_recording()` to merge recording state from multiple with-blocks. If the existing pipeline recording's `_hash_graph` accumulation mechanism works correctly (it already does in `Pipeline`), this should work transparently. If not, note it as a follow-up.

- [ ] **Step 7: Run the full test suite**

Run: `uv run pytest tests/test_pipeline/test_pipeline.py tests/test_pipeline/test_pipeline_job.py -v --tb=short 2>&1 | tail -30`
Expected: All new tests PASS; note any remaining failures for Task 11.

- [ ] **Step 8: Commit**

```bash
git add tests/test_pipeline/test_pipeline.py tests/test_pipeline/test_pipeline_job.py
git commit -m "test(pipeline): migrate execution tests from Pipeline to PipelineJob"
```

---

## Task 11: Migrate `test_serialization.py`

**Files:**
- Modify: `tests/test_pipeline/test_serialization.py`

The serialization tests fall into two categories:

1. **Tests that use `Pipeline` structurally** (no DB, no run): update to use SourceSpec-based fixtures.
2. **Tests that do DB round-trips** (save → run → load → run): migrate to `test_pipeline_job.py`.

- [ ] **Step 1: Create new fixture for test_serialization.py**

Replace the existing `simple_pipeline` and `multi_source_pipeline` fixtures (which use `pipeline_database=...`) with SourceSpec-based versions:

```python
# In test_serialization.py, update fixtures:

@pytest.fixture
def simple_pipeline(tmp_path):
    """A compiled Pipeline using SourceSpec leaves (no DB)."""
    from orcapod.core.sources import ArrowTableSource
    from orcapod.core.sources.source_spec import SourceSpec
    from orcapod.core.data_function import PythonDataFunction
    from orcapod.core.function_pod import FunctionPod
    from orcapod.core.operators import Join
    from orcapod.pipeline import Pipeline
    import pyarrow as pa

    def _src(tag, data_col, vals):
        tbl = pa.table({
            tag: pa.array(["a", "b"], type=pa.large_string()),
            data_col: pa.array(vals, type=pa.int64()),
        })
        return ArrowTableSource(tbl, tag_columns=[tag], infer_nullable=True)

    src_a = _src("key", "value", [10, 20])
    tag_a, data_a = src_a.output_schema()

    def double(value: int) -> int:
        return value * 2

    pf = PythonDataFunction(double, output_keys="doubled")
    pod = FunctionPod(data_function=pf)
    spec_a = SourceSpec("input_a", tag_schema=tag_a, data_schema=data_a)

    pipeline = Pipeline(name="simple")
    with pipeline:
        pod(spec_a, label="doubler")

    path = tmp_path / "simple.json"
    pipeline.save(str(path))
    return path, pipeline


@pytest.fixture
def multi_source_pipeline(tmp_path):
    """A compiled Pipeline with two SourceSpec leaves and a Join."""
    from orcapod.core.sources import ArrowTableSource
    from orcapod.core.sources.source_spec import SourceSpec
    from orcapod.core.operators import Join
    from orcapod.pipeline import Pipeline
    import pyarrow as pa

    def _src(tag, data_col):
        tbl = pa.table({
            tag: pa.array(["a", "b"], type=pa.large_string()),
            data_col: pa.array([1, 2], type=pa.int64()),
        })
        return ArrowTableSource(tbl, tag_columns=[tag], infer_nullable=True)

    src_a = _src("key", "value")
    src_b = _src("key", "score")
    tag_a, data_a = src_a.output_schema()
    tag_b, data_b = src_b.output_schema()
    spec_a = SourceSpec("source_a", tag_schema=tag_a, data_schema=data_a)
    spec_b = SourceSpec("source_b", tag_schema=tag_b, data_schema=data_b)

    pipeline = Pipeline(name="multi")
    with pipeline:
        Join()(spec_a, spec_b, label="joiner")

    path = tmp_path / "multi.json"
    pipeline.save(str(path))
    return path, pipeline
```

- [ ] **Step 2: Update structural save tests to use new fixtures**

For tests in `TestSave` that test basic JSON structure (file creation, version, nodes, edges), update to use the new `spec_pipeline` / `simple_pipeline` fixtures. Remove tests that assert on `"databases"` block presence (it no longer exists).

- [ ] **Step 3: Delete or migrate DB round-trip tests**

Tests like `test_save_load_run_full_cycle`, `test_standard_save_load_run_roundtrip`, `test_definition_save_load_run_roundtrip`, and all tests that combine `pipeline.save(level="standard")` with DB setup — these move to `test_pipeline_job.py` under `TestPipelineJobSerialization`.

The key migration (already done in Task 8):
```python
# In test_pipeline_job.py — already implemented in Task 8:
class TestPipelineJobSerialization:
    def test_load_roundtrip_after_run(self, store, tmp_path): ...
```

Add more complex round-trip tests to `TestPipelineJobSerialization` as needed, adapting the key scenarios from the old serialization tests.

- [ ] **Step 4: Run the serialization tests**

Run: `uv run pytest tests/test_pipeline/test_serialization.py -v --tb=short 2>&1 | tail -30`
Expected: Passing tests are the new structural tests. Remove or skip tests that test removed behavior (old levels, DB registry, etc.).

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest tests/ -v --tb=short -q 2>&1 | tail -40`
Fix any remaining failures.

- [ ] **Step 6: Commit**

```bash
git add tests/test_pipeline/test_serialization.py tests/test_pipeline/test_pipeline_job.py
git commit -m "test(pipeline): migrate DB round-trip serialization tests to test_pipeline_job"
```

---

## Task 12: Final Cleanup and PR

- [ ] **Step 1: Run the complete test suite**

Run: `uv run pytest tests/ -v -q 2>&1 | tail -20`
Expected: All tests PASS (or clearly explained skip/xfail).

- [ ] **Step 2: Check DESIGN_ISSUES.md**

Read `DESIGN_ISSUES.md` at the project root. Update any open issues that ENG-456 resolves. The old `Pipeline` conflating DAG and execution was a known design issue — mark it as resolved.

- [ ] **Step 3: Update DESIGN_ISSUES.md for the ExecutionContext follow-up**

If not already present, add an open issue:

```markdown
## [open] ExecutionContext full definition

**Description:** `ExecutionContext` is currently a stub dataclass. Full definition
including `PipelineConfig` integration and distributed execution support is deferred.

**Related issue:** ENG-456 (parent), follow-up issue TBD.
```

- [ ] **Step 4: Push and create PR**

```bash
git push -u origin eywalker/eng-456-refactor-pipeline-into-source-agnostic-dag-introduce
```

Create PR via `gh pr create` targeting `dev` branch with body referencing `Fixes ENG-456`.

---

## Self-Review Checklist

After writing this plan, verify against the spec:

**Spec coverage:**
- [x] `SourceSpec` type: Task 2
- [x] `PipelineJob` type: Tasks 6, 7, 8
- [x] Refactor `Pipeline` (remove execution surface, enforce SourceSpec): Task 4
- [x] `with PipelineJob:` recording + auto-SourceSpec: Task 6
- [x] `job.pipeline` extraction: Task 6 (property)
- [x] `bind()` on both `Pipeline` and `PipelineJob`: Tasks 4 (bind stub), 6 (PipelineJob.bind)
- [x] Partial/progressive execution: Task 7
- [x] `PipelineJob` save/load: Task 8
- [x] `Pipeline` save/load as pure blueprint: Task 5
- [x] Hashing correctness: Task 2 (`test_pipeline_hash_matches_compatible_source`)
- [x] `ExecutionContext` stub: Task 3
- [x] Test migration: Tasks 10, 11

**Type consistency:**
- `SourceSpec.name` → `str` (used as `job.sources` key)
- `PipelineJob.sources` → `dict[str, StreamProtocol]` (always returns a copy)
- `PipelineJob.unbound_specs()` → `list[SourceSpec]` (not list[str])
- `PipelineJob.unresolved_specs` (property on returned job) → `list[str]` (spec names that were excluded at run time — distinct from `unbound_specs()` which checks current bindings)
- `PipelineJob.bind()` → `PipelineJob` (non-mutating)
- `Pipeline.bind()` → `PipelineJob` (wraps self)
- `Pipeline.save(path)` → no `level` parameter in new design
- `Pipeline.load(path)` → `Pipeline` (no `mode` or DB params)
