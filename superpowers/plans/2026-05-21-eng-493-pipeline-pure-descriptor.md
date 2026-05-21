# Pipeline Pure-Descriptor Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `Pipeline` a pure computational descriptor (lightweight nodes only, no DB) while `PipelineJob` becomes the sole stateful, executable form — with `FunctionJobNode`/`OperatorJobNode`/`SourceJobNode` carrying all execution state.

**Architecture:** Create parallel "blueprint" and "execution" node variants sharing base classes. `SourceNode` (schema-only) replaces `SourceSpec` with bit-identical hashes. `FunctionNode`/`OperatorNode` become thin wrappers (no DB); `FunctionJobNode`/`OperatorJobNode` carry all DB logic. `AbstractPipelineBase` extracts shared recording machinery. `Pipeline.bind()` is removed; `PipelineJob.from_pipeline()` is its replacement. `PipelineJob.bind()` becomes mutating (returns `None`). `SourceSpec` is deleted.

**Tech Stack:** Python, PyArrow, NetworkX; always run `uv run pytest` (never `pytest` directly)

---

## File Layout

**New files:**
- `src/orcapod/pipeline/base.py` — `AbstractPipelineBase` (shared recording mechanism)
- `tests/test_core/nodes/test_source_node.py` — New unit tests for `SourceNode`/`SourceJobNode`
- `tests/test_core/nodes/test_function_node_split.py` — New unit tests for split hierarchy
- `tests/test_core/nodes/test_operator_node_split.py` — New unit tests for split hierarchy

**Heavily modified (in execution order):**
1. `src/orcapod/errors.py` — Add `PipelineJobRequiredError`
2. `src/orcapod/core/nodes/source_node.py` — Full rewrite: `SourceNodeBase` + `SourceNode` (schema-only) + `SourceJobNode`
3. `src/orcapod/core/nodes/function_node.py` — Split: `FunctionNodeBase` + thin `FunctionNode` + `FunctionJobNode`
4. `src/orcapod/core/nodes/operator_node.py` — Split: `OperatorNodeBase` + thin `OperatorNode` + `OperatorJobNode`
5. `src/orcapod/core/nodes/__init__.py` — Add `FunctionJobNode`, `OperatorJobNode`, `SourceJobNode` exports; add `JobNode` alias
6. `src/orcapod/pipeline/base.py` ← NEW
7. `src/orcapod/pipeline/graph.py` — `Pipeline`: accept `SourceNode` leaves (not `SourceSpec`), use thin nodes, remove `bind()`
8. `src/orcapod/pipeline/job.py` — `PipelineJob`: use `JobNode` types, `from_pipeline()`, mutating `bind()`, `as_pipeline()`
9. `src/orcapod/pipeline/serialization.py` — New source serialization format, version bumps
10. `src/orcapod/__init__.py` — Remove `SourceSpec`, export `SourceNode`

**Deleted:**
- `src/orcapod/core/sources/source_spec.py`
- `tests/test_core/sources/test_source_spec.py`

---

## Critical Hash-Stability Invariant

`SourceNode.identity_structure()` **must** return `("SourceSpec", name, tag_schema, data_schema)` — identical to the old `SourceSpec.identity_structure()`. This ensures:

```
SourceNode("x", tag_s, data_s).content_hash() == SourceSpec("x", tag_s, data_s).content_hash()
```

DB paths derived from `pipeline_hash()` chains must not change. Every task that touches identity structures must re-verify this invariant with tests before committing.

---

## Task 1: Add PipelineJobRequiredError and rewrite source_node.py

**Files:**
- Modify: `src/orcapod/errors.py`
- Rewrite: `src/orcapod/core/nodes/source_node.py`
- Create: `tests/test_core/nodes/test_source_node.py`

- [ ] **Step 1.1: Add PipelineJobRequiredError to errors.py**

Open `src/orcapod/errors.py` and append after the existing `SourceSpecMismatchError` class:

```python
class PipelineJobRequiredError(RuntimeError):
    """Raised when a lightweight blueprint node is asked to produce data.

    Blueprint nodes (``FunctionNode``, ``OperatorNode``) carry no database
    references.  Wrap the containing ``Pipeline`` in a ``PipelineJob`` to
    obtain executable ``FunctionJobNode`` / ``OperatorJobNode`` variants.
    """
```

- [ ] **Step 1.2: Write failing tests for new SourceNode/SourceJobNode interface**

Create `tests/test_core/nodes/` directory if it doesn't exist, and create
`tests/test_core/nodes/__init__.py` (empty) and
`tests/test_core/nodes/test_source_node.py`:

```python
"""Tests for SourceNode (schema-only slot) and SourceJobNode (execution variant)."""
from __future__ import annotations

import pytest

from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.types import Schema


@pytest.fixture
def tag_schema():
    return Schema({"id": int})


@pytest.fixture
def data_schema():
    return Schema({"value": float})


class TestSourceNodeHashStability:
    """SourceNode must produce bit-identical hashes to SourceSpec with the same args."""

    def test_content_hash_matches_source_spec(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        spec = SourceSpec(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        node = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        assert node.content_hash() == spec.content_hash()

    def test_pipeline_hash_matches_source_spec(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        spec = SourceSpec(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        node = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        assert node.pipeline_hash() == spec.pipeline_hash()

    def test_different_names_different_content_hash(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        a = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        b = SourceNode(name="slot_b", tag_schema=tag_schema, data_schema=data_schema)
        assert a.content_hash() != b.content_hash()

    def test_different_names_same_pipeline_hash(self, tag_schema, data_schema):
        """pipeline_hash is schema-only, name-independent."""
        from orcapod.core.nodes.source_node import SourceNode

        a = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        b = SourceNode(name="slot_b", tag_schema=tag_schema, data_schema=data_schema)
        assert a.pipeline_hash() == b.pipeline_hash()


class TestSourceNodeInterface:
    def test_iter_data_raises_unbound_error(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        with pytest.raises(UnboundSourceError):
            list(node.iter_data())

    def test_output_schema(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        t, d = node.output_schema()
        assert t == tag_schema
        assert d == data_schema

    def test_label_resolves_to_name(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="my_slot", tag_schema=tag_schema, data_schema=data_schema)
        assert node.label == "my_slot"

    def test_node_type(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        assert node.node_type == "source"

    def test_name_property(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="my_slot", tag_schema=tag_schema, data_schema=data_schema)
        assert node.name == "my_slot"

    def test_validate_compatible_source(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.core.sources.dict_source import DictSource

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        src = DictSource(records=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        node.validate(src)  # must not raise

    def test_validate_incompatible_raises(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.core.sources.dict_source import DictSource

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        src = DictSource(records=[{"id": 1, "wrong": 1.0}], tag_columns=["id"])
        with pytest.raises(SourceSpecMismatchError):
            node.validate(src)


class TestSourceJobNode:
    def test_unbound_iter_data_raises(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceJobNode

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        with pytest.raises(UnboundSourceError):
            list(job_node.iter_data())

    def test_unbound_content_hash_matches_source_node(self, tag_schema, data_schema):
        """Unbound SourceJobNode has same content_hash as SourceNode."""
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        assert job_node.content_hash() == node.content_hash()

    def test_pipeline_hash_matches_source_node(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        assert job_node.pipeline_hash() == node.pipeline_hash()

    def test_bound_content_hash_is_concrete_hash(self, tag_schema, data_schema):
        """Bound SourceJobNode content_hash() == concrete.content_hash()."""
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(records=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, concrete=src
        )
        assert job_node.content_hash() == src.content_hash()

    def test_bound_pipeline_hash_still_schema_based(self, tag_schema, data_schema):
        """pipeline_hash stays schema-based even when concrete is bound."""
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(records=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, concrete=src
        )
        assert job_node.pipeline_hash() == node.pipeline_hash()

    def test_as_node_returns_source_node(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        node = job_node.as_node()
        assert isinstance(node, SourceNode)
        assert node.content_hash() == job_node.content_hash()

    def test_mutable_concrete_updates_in_place(self, tag_schema, data_schema):
        """Binding concrete mutates _concrete in-place."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        assert job_node._concrete is None
        src = DictSource(records=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node._concrete = src
        assert job_node._concrete is src
```

- [ ] **Step 1.3: Run tests to confirm failure**

```bash
uv run pytest tests/test_core/nodes/test_source_node.py -v 2>&1 | tail -20
```

Expected: ImportError or AttributeError (SourceNode doesn't yet have `name=` constructor arg)

- [ ] **Step 1.4: Rewrite src/orcapod/core/nodes/source_node.py**

Replace the entire file content:

```python
"""Source node hierarchy for Pipeline and PipelineJob.

SourceNode — schema-only input-slot declaration (replaces SourceSpec).
SourceJobNode — execution variant that wraps a concrete StreamProtocol.
Both share SourceNodeBase which provides hash-stable identity.

Hash-stability guarantee:
    SourceNode(name=n, tag_schema=t, data_schema=d).content_hash()
    == SourceSpec(name=n, tag_schema=t, data_schema=d).content_hash()

This is achieved by using identical identity_structure():
    ("SourceSpec", name, tag_schema, data_schema)
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.config import DEFAULT_CONFIG
from orcapod.core.base import TraceableBase
from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.types import ColumnConfig, ContentHash, Schema

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.core_protocols import StreamProtocol

logger = logging.getLogger(__name__)


class SourceNodeBase(TraceableBase, ABC):
    """Abstract base for SourceNode and SourceJobNode.

    Provides schema-based identity (content_hash, pipeline_hash) and
    shared properties.  Both sub-types carry identical schemas so their
    pipeline_hash() values always match; content_hash() diverges only
    when SourceJobNode has a concrete source bound.

    Args:
        name: The input-slot name used as the key in
            ``PipelineJob.bind(sources={name: source})``.
        tag_schema: Mapping of tag column names to Python types.
        data_schema: Mapping of data column names to Python types.
        data_context: Optional data context override.
    """

    node_type = "source"

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
    # Identity — hash-stable against old SourceSpec
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Return the content identity: ``("SourceSpec", name, tag_schema, data_schema)``.

        Deliberately matches ``SourceSpec.identity_structure()`` so that a
        ``SourceNode`` constructed with the same arguments as a ``SourceSpec``
        produces an identical ``content_hash()``.  This preserves all DB paths
        computed from pre-refactor pipelines.
        """
        return ("SourceSpec", self._name, self._tag_schema, self._data_schema)

    def pipeline_identity_structure(self) -> Any:
        """Return the pipeline identity: ``(tag_schema, data_schema)`` (name-independent).

        Matches ``RootSource.pipeline_identity_structure()`` so that sources
        with identical schemas share the same DB table paths regardless of name.
        """
        return (self._tag_schema, self._data_schema)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """The input-slot name used as the key in ``PipelineJob.bind(sources={...})``."""
        return self._name

    def computed_label(self) -> str | None:
        """Resolve the node label to the slot name.

        Implements ``LabelableMixin.computed_label()`` so that ``self.label``
        resolves to the slot name without an explicit label assignment.

        Returns:
            The slot name.
        """
        return self._name

    @property
    def tag_schema(self) -> Schema:
        """Tag schema for this input slot."""
        return self._tag_schema

    @property
    def data_schema(self) -> Schema:
        """Data schema for this input slot."""
        return self._data_schema

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return ``(tag_schema, data_schema)``.

        Args:
            columns: Ignored.
            all_info: Ignored.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        return (self._tag_schema, self._data_schema)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return ``(tag_keys, data_keys)``.

        Args:
            columns: Ignored.
            all_info: Ignored.

        Returns:
            Tuple of ``(tag_column_names, data_column_names)``.
        """
        return (tuple(self._tag_schema.keys()), tuple(self._data_schema.keys()))

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self, source: StreamProtocol) -> None:
        """Check that *source* is schema-compatible with this node's declared schema.

        Args:
            source: A concrete stream to validate.

        Raises:
            SourceSpecMismatchError: If schema columns don't match.
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
            raise SourceSpecMismatchError(
                f"SourceNode '{self._name}' is not compatible with the provided source. "
                + "; ".join(tag_issues + data_issues)
            )

    # ------------------------------------------------------------------
    # Abstract
    # ------------------------------------------------------------------

    @abstractmethod
    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Yield ``(tag, data)`` pairs, or raise if data is unavailable."""
        ...

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(name={self._name!r}, "
            f"tag_schema={dict(self._tag_schema)!r}, "
            f"data_schema={dict(self._data_schema)!r})"
        )


class SourceNode(SourceNodeBase):
    """Schema-only input-slot declaration for ``Pipeline`` recording.

    Replaces ``SourceSpec`` as the user-facing way to declare typed pipeline
    inputs.  Pass a ``SourceNode`` inside a ``with pipeline:`` block as the
    upstream for any pod invocation.

    Example::

        slot = SourceNode(name="data", tag_schema={"id": int}, data_schema={"v": float})
        with pipeline:
            result = my_pod(slot)

        job = PipelineJob.from_pipeline(pipeline, store=db, sources={"data": my_source})
        job.run()

    Hash-stability note:
        ``identity_structure()`` returns ``("SourceSpec", name, tag_schema, data_schema)``
        — identical to the old ``SourceSpec`` — so existing DB paths remain valid.
    """

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Raise ``UnboundSourceError`` — ``SourceNode`` carries no data.

        Raises:
            UnboundSourceError: Always.
        """
        raise UnboundSourceError(
            f"SourceNode '{self._name}' is not bound to a concrete source. "
            "Use PipelineJob.from_pipeline(..., sources={'<name>': source}) "
            "or job.bind(sources={'<name>': source}) to attach data."
        )


class SourceJobNode(SourceNodeBase):
    """Execution-ready source node wrapping an optional concrete stream.

    Used inside ``PipelineJob._persistent_node_map``.  The ``_concrete``
    field is **mutable** — ``PipelineJob.bind(sources={...})`` updates it
    in-place so that downstream ``FunctionJobNode`` objects (which hold a
    reference to this same object) automatically see the new concrete source
    without cascading reference updates.

    Hash behaviour:

    * ``content_hash()`` — delegates to ``_concrete.content_hash()`` when
      bound; falls back to schema-based ``SourceNodeBase.content_hash()`` (==
      ``SourceNode.content_hash()``) when unbound.
    * ``pipeline_hash()`` — always schema-based (inherited); never
      data-inclusive.  This invariant keeps DB paths stable across different
      data sources bound to the same slot.

    Args:
        name: Slot name.
        tag_schema: Tag schema.
        data_schema: Data schema.
        concrete: Optional concrete stream.  Can be set or replaced later via
            ``job_node._concrete = source``.
        data_context: Optional data context override.
    """

    def __init__(
        self,
        name: str,
        tag_schema: Schema,
        data_schema: Schema,
        concrete: StreamProtocol | None = None,
        data_context: str | contexts.DataContext | None = None,
    ) -> None:
        super().__init__(
            name=name,
            tag_schema=tag_schema,
            data_schema=data_schema,
            data_context=data_context,
        )
        self._concrete: StreamProtocol | None = concrete

    def content_hash(self, hasher=None) -> ContentHash:
        """Return data-inclusive hash when bound; schema-based hash when unbound.

        Args:
            hasher: Optional semantic hasher.

        Returns:
            ``_concrete.content_hash(hasher)`` when bound, otherwise
            ``SourceNodeBase.content_hash(hasher)``.
        """
        if self._concrete is not None:
            if hasher is None:
                hasher = self.data_context.semantic_hasher
            return self._concrete.content_hash(hasher)
        return super().content_hash(hasher)

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Delegate to concrete source, or raise if unbound.

        Raises:
            UnboundSourceError: When no concrete source is attached.
        """
        if self._concrete is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={'<name>': source}) before running."
            )
        return self._concrete.iter_data()

    def as_node(self) -> SourceNode:
        """Return the lightweight ``SourceNode`` equivalent of this job node.

        Returns:
            A new ``SourceNode`` with the same name and schemas.
        """
        return SourceNode(
            name=self._name,
            tag_schema=self._tag_schema,
            data_schema=self._data_schema,
        )
```

- [ ] **Step 1.5: Run new source_node tests**

```bash
uv run pytest tests/test_core/nodes/test_source_node.py -v
```

Expected: All 14 tests pass.

- [ ] **Step 1.6: Update nodes/__init__.py to export new types**

Replace `src/orcapod/core/nodes/__init__.py` content:

```python
from typing import TypeAlias

from .function_node import FunctionNode
from .operator_node import OperatorNode
from .source_node import SourceJobNode, SourceNode, SourceNodeBase

GraphNode: TypeAlias = SourceNode | FunctionNode | OperatorNode

__all__ = [
    "FunctionNode",
    "GraphNode",
    "OperatorNode",
    "SourceJobNode",
    "SourceNode",
    "SourceNodeBase",
]
```

- [ ] **Step 1.7: Update Pipeline.compile() to accept SourceNode directly**

In `src/orcapod/pipeline/graph.py`, find the `compile()` method leaf-handling block (around line 218) and replace:

```python
# OLD — lines ~218-229
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
```

With:

```python
# NEW
from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass
stream = self._upstreams[node_hash]
if not isinstance(stream, SourceNodeClass):
    raise ValueError(
        f"Pipeline: all leaf inputs must be SourceNode instances, "
        f"but found {type(stream).__name__!r}. "
        "Use 'with PipelineJob:' to record a pipeline with concrete sources, "
        "or replace concrete sources with SourceNode declarations."
    )
node = stream  # SourceNode IS the leaf — no wrapping needed
```

Also update the `isinstance` checks later in `compile()` that reference `SourceNode` for node_type annotation — they now just check `isinstance(node, SourceNodeClass)`.

- [ ] **Step 1.8: Update PipelineJob._ensure_spec → _ensure_source_node**

In `src/orcapod/pipeline/job.py`:

1. Rename `_ensure_spec` to `_ensure_source_node` and update it:

```python
def _ensure_source_node(self, source: cp.StreamProtocol) -> "SourceNode":
    """Promote *source* to a SourceNode, storing the concrete binding.

    If the slot already exists (same label/hash key), returns the cached node.

    Args:
        source: A concrete ``RootSource`` to promote.

    Returns:
        The ``SourceNode`` slot declaration for this source.
    """
    from orcapod.core.nodes.source_node import SourceNode

    has_label = source.has_assigned_label
    if has_label:
        name = source.label  # type: ignore[attr-defined]
    else:
        name = source.content_hash().to_string()

    if name not in self._spec_by_name:
        tag_schema, data_schema = source.output_schema()
        node = SourceNode(name=name, tag_schema=tag_schema, data_schema=data_schema)
        self._spec_by_name[name] = node  # type: ignore[assignment]
        self._sources[name] = source
    return self._spec_by_name[name]  # type: ignore[return-value]
```

2. Update `_is_concrete_source` to not exclude `SourceNode`:

```python
@staticmethod
def _is_concrete_source(stream: cp.StreamProtocol) -> bool:
    """True if *stream* is a concrete RootSource (not a SourceNode)."""
    from orcapod.core.sources.base import RootSource
    from orcapod.core.nodes.source_node import SourceNode

    return isinstance(stream, RootSource) and not isinstance(stream, SourceNode)
```

3. Rename `_to_spec_stream` → `_to_node_stream` and update:

```python
def _to_node_stream(self, stream: cp.StreamProtocol) -> cp.StreamProtocol:
    """Convert *stream* to a node-based equivalent for consistent hash recording.

    Concrete ``RootSource`` instances are promoted to ``SourceNode`` via
    ``_ensure_source_node``.  ``DynamicPodStream`` instances have their
    upstreams recursively converted.

    Args:
        stream: The upstream stream to convert.

    Returns:
        A node-based stream with a stable hash for recording.
    """
    from orcapod.core.operators.static_output_pod import DynamicPodStream

    if self._is_concrete_source(stream):
        return self._ensure_source_node(stream)
    if isinstance(stream, DynamicPodStream):
        node_upstreams = tuple(self._to_node_stream(s) for s in stream.upstreams)
        return DynamicPodStream(
            pod=stream._pod,
            upstreams=node_upstreams,
            label=stream._label,
        )
    return stream
```

4. Update `record_function_pod_invocation` to call `_to_node_stream`:

```python
def record_function_pod_invocation(self, pod, input_stream, label=None):
    from orcapod.core.nodes import FunctionNode
    input_stream = self._to_node_stream(input_stream)
    input_hash = input_stream.content_hash().to_string()
    function_node = FunctionNode(function_pod=pod, input_stream=input_stream, label=label)
    fn_hash = function_node.content_hash().to_string()
    self._rec_node_lut[fn_hash] = function_node
    self._rec_upstreams[input_hash] = input_stream
    self._rec_graph_edges.append((input_hash, fn_hash))
```

5. Update `record_operator_pod_invocation` to call `_to_node_stream`:

```python
def record_operator_pod_invocation(self, pod, upstreams=(), label=None):
    from orcapod.core.nodes import OperatorNode
    processed = tuple(self._to_node_stream(s) for s in upstreams)
    operator_node = OperatorNode(operator=pod, input_streams=processed, label=label)
    op_hash = operator_node.content_hash().to_string()
    self._rec_node_lut[op_hash] = operator_node
    for upstream in processed:
        up_hash = upstream.content_hash().to_string()
        self._rec_upstreams[up_hash] = upstream
        self._rec_graph_edges.append((up_hash, op_hash))
```

6. Update `unbound_specs` → `unbound_source_nodes`:

```python
def unbound_source_nodes(self) -> list["SourceNode"]:
    """Return all SourceNode slots not yet bound in this job.

    Returns:
        List of unbound ``SourceNode`` instances, in graph order.
    """
    from orcapod.core.nodes.source_node import SourceNode

    if self._compiled_pipeline is None:
        return []

    unbound: list[SourceNode] = []
    seen: set[str] = set()
    for node in self._compiled_pipeline._persistent_node_map.values():
        if (
            isinstance(node, SourceNode)
            and node.name not in self._sources
            and node.name not in seen
        ):
            unbound.append(node)
            seen.add(node.name)
    return unbound
```

Keep `unbound_specs` as a deprecated alias that calls `unbound_source_nodes` (will be removed in Task 8):

```python
def unbound_specs(self):
    """Deprecated — use unbound_source_nodes() instead."""
    return self.unbound_source_nodes()
```

7. Update `is_complete` to use `unbound_source_nodes`:

```python
def is_complete(self) -> bool:
    return self._store is not None and len(self.unbound_source_nodes()) == 0
```

8. Update `is_runnable` to use `SourceNode` instead of `SourceSpec`:

```python
def is_runnable(self, node_label: str) -> bool:
    from orcapod.core.nodes.source_node import SourceNode

    pipeline = self._compiled_pipeline
    if pipeline is None:
        return False
    target = pipeline._nodes.get(node_label)
    if target is None:
        return False
    if pipeline._node_graph is None:
        return False

    import networkx as nx

    for node in nx.ancestors(pipeline._node_graph, target) | {target}:
        if isinstance(node, SourceNode) and node.name not in self._sources:
            return False
    return True
```

9. Update `bind()` validation:

```python
def bind(self, sources=None, store=None, execution_context=None) -> "PipelineJob":
    from orcapod.core.nodes.source_node import SourceNode

    merged_sources = dict(self._sources)
    if sources is not None:
        pipeline = self._compiled_pipeline
        if pipeline is not None:
            spec_names = {
                node.name
                for node in pipeline._persistent_node_map.values()
                if isinstance(node, SourceNode)
            }
            for name, source in sources.items():
                for node in pipeline._persistent_node_map.values():
                    if isinstance(node, SourceNode) and node.name == name:
                        node.validate(source)
                        break
            unknown = set(sources.keys()) - spec_names
            if unknown:
                raise ValueError(
                    f"bind() received source keys with no matching SourceNode in the pipeline: "
                    f"{sorted(unknown)}. Known names: {sorted(spec_names)}"
                )
        merged_sources.update(sources)

    return PipelineJob(
        name=self._pipeline_name,
        store=store if store is not None else self._store,
        execution_context=(
            execution_context if execution_context is not None else self._execution_context
        ),
        _pipeline=self._compiled_pipeline,
        sources=merged_sources,
    )
```

- [ ] **Step 1.9: Run pipeline test suite**

```bash
uv run pytest tests/test_pipeline/ -v --tb=short 2>&1 | tail -40
```

Expected: All existing pipeline tests pass. Failures here indicate missed `SourceSpec` references — fix each one.

- [ ] **Step 1.10: Commit**

```bash
git add src/orcapod/errors.py \
        src/orcapod/core/nodes/source_node.py \
        src/orcapod/core/nodes/__init__.py \
        src/orcapod/pipeline/graph.py \
        src/orcapod/pipeline/job.py \
        tests/test_core/nodes/__init__.py \
        tests/test_core/nodes/test_source_node.py
git commit -m "refactor(nodes): replace SourceSpec with schema-only SourceNode + add SourceJobNode"
```

---

## Task 2: Split function_node.py into FunctionNodeBase + FunctionNode + FunctionJobNode

**Files:**
- Rewrite: `src/orcapod/core/nodes/function_node.py`
- Modify: `src/orcapod/core/nodes/__init__.py`
- Create: `tests/test_core/nodes/test_function_node_split.py`

**Strategy:** `FunctionJobNode` is essentially the current `FunctionNode` renamed. The new thin `FunctionNode` just raises `PipelineJobRequiredError` on `iter_data()`. `FunctionNodeBase` holds the shared constructor, properties, and `from_descriptor` logic.

- [ ] **Step 2.1: Write failing tests**

Create `tests/test_core/nodes/test_function_node_split.py`:

```python
"""Tests for the FunctionNode / FunctionJobNode split."""
from __future__ import annotations

import pytest

from orcapod.errors import PipelineJobRequiredError
from orcapod.types import Schema


@pytest.fixture
def simple_pipeline(tmp_path):
    """A minimal in-memory store + function pod + source node fixture."""
    from orcapod.core.function_pod import FunctionPod
    from orcapod.core.nodes.source_node import SourceNode
    from orcapod.core.sources.dict_source import DictSource
    from orcapod.databases.in_memory_database import InMemoryDatabase

    tag_schema = Schema({"id": int})
    data_schema = Schema({"value": float})

    source_node = SourceNode(name="src", tag_schema=tag_schema, data_schema=data_schema)
    src = DictSource(records=[{"id": 1, "value": 1.0}], tag_columns=["id"])
    db = InMemoryDatabase()

    @FunctionPod.from_function
    def double(value: float) -> dict:
        return {"result": value * 2}

    return {
        "source_node": source_node,
        "source": src,
        "db": db,
        "pod": double,
        "tag_schema": tag_schema,
        "data_schema": data_schema,
    }


class TestThinFunctionNode:
    def test_iter_data_raises_pipeline_job_required(self, simple_pipeline):
        from orcapod.core.nodes.function_node import FunctionNode

        fn = FunctionNode(
            function_pod=simple_pipeline["pod"],
            input_stream=simple_pipeline["source_node"],
        )
        with pytest.raises(PipelineJobRequiredError):
            list(fn.iter_data())

    def test_content_hash_is_stable(self, simple_pipeline):
        from orcapod.core.nodes.function_node import FunctionNode

        fn = FunctionNode(
            function_pod=simple_pipeline["pod"],
            input_stream=simple_pipeline["source_node"],
        )
        h1 = fn.content_hash()
        h2 = fn.content_hash()
        assert h1 == h2

    def test_node_type(self, simple_pipeline):
        from orcapod.core.nodes.function_node import FunctionNode

        fn = FunctionNode(
            function_pod=simple_pipeline["pod"],
            input_stream=simple_pipeline["source_node"],
        )
        assert fn.node_type == "function"

    def test_output_schema(self, simple_pipeline):
        from orcapod.core.nodes.function_node import FunctionNode

        fn = FunctionNode(
            function_pod=simple_pipeline["pod"],
            input_stream=simple_pipeline["source_node"],
        )
        tag_s, data_s = fn.output_schema()
        assert "result" in data_s


class TestFunctionJobNodeHashParity:
    """FunctionJobNode must have identical content_hash / pipeline_hash to FunctionNode."""

    def test_content_hash_matches_function_node(self, simple_pipeline):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode

        fn = FunctionNode(
            function_pod=simple_pipeline["pod"],
            input_stream=simple_pipeline["source_node"],
        )
        fjn = FunctionJobNode(
            function_pod=simple_pipeline["pod"],
            input_stream=simple_pipeline["source_node"],
        )
        assert fn.content_hash() == fjn.content_hash()

    def test_pipeline_hash_matches_function_node(self, simple_pipeline):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode

        fn = FunctionNode(
            function_pod=simple_pipeline["pod"],
            input_stream=simple_pipeline["source_node"],
        )
        fjn = FunctionJobNode(
            function_pod=simple_pipeline["pod"],
            input_stream=simple_pipeline["source_node"],
        )
        assert fn.pipeline_hash() == fjn.pipeline_hash()

    def test_as_node_returns_function_node(self, simple_pipeline):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode

        fjn = FunctionJobNode(
            function_pod=simple_pipeline["pod"],
            input_stream=simple_pipeline["source_node"],
        )
        fn = fjn.as_node()
        assert isinstance(fn, FunctionNode)
        assert fn.content_hash() == fjn.content_hash()
```

- [ ] **Step 2.2: Run tests to confirm failure**

```bash
uv run pytest tests/test_core/nodes/test_function_node_split.py -v 2>&1 | tail -20
```

Expected: `ImportError: cannot import name 'FunctionJobNode'`

- [ ] **Step 2.3: Refactor function_node.py**

The current `FunctionNode` class (1466 lines) becomes the new `FunctionJobNode`. The new thin `FunctionNode` only raises on `iter_data()`. Both share `FunctionNodeBase` which holds the constructor, identity, properties, and `from_descriptor` logic.

At the top of `src/orcapod/core/nodes/function_node.py`, after the existing imports, add:

```python
from orcapod.errors import PipelineJobRequiredError
```

Then restructure the class hierarchy as follows (keeping all existing logic intact):

**a) Rename the current `FunctionNode` class to `FunctionJobNode`** by adding a `FunctionJobNode` alias at the bottom of the file **first**, then creating the new thin `FunctionNode`:

At the **bottom** of `function_node.py`, after the existing `FunctionNode` class, add:

```python
# FunctionJobNode is the DB-backed execution variant of FunctionNode.
# It is the existing FunctionNode class renamed.
FunctionJobNode = FunctionNode


class FunctionNode(FunctionJobNode):  # type: ignore[no-redef]
    """Lightweight blueprint node for ``Pipeline`` recording.

    Carries no database references.  Calling ``iter_data()`` raises
    ``PipelineJobRequiredError`` — wrap the containing ``Pipeline`` in a
    ``PipelineJob`` to obtain an executable ``FunctionJobNode``.

    All identity methods (``content_hash``, ``pipeline_hash``,
    ``output_schema``) are inherited from ``FunctionJobNode`` and produce
    values identical to those of the corresponding ``FunctionJobNode``
    constructed with the same arguments.

    Args:
        function_pod: The wrapped function pod.
        input_stream: The upstream stream (must be a ``SourceNode`` or
            another blueprint node).
        label: Optional display label.
    """

    def __init__(
        self,
        function_pod: "FunctionPodProtocol",
        input_stream: "StreamProtocol",
        tracker_manager: "TrackerManagerProtocol | None" = None,
        label: str | None = None,
        config: "Config | None" = None,
    ) -> None:
        # Construct without any DB parameters
        super().__init__(
            function_pod=function_pod,
            input_stream=input_stream,
            tracker_manager=tracker_manager,
            label=label,
            config=config,
        )

    def iter_data(self):
        """Raise PipelineJobRequiredError — blueprint node cannot produce data.

        Raises:
            PipelineJobRequiredError: Always.
        """
        raise PipelineJobRequiredError(
            f"FunctionNode '{self.label}' is a blueprint node and cannot produce data. "
            "Wrap the containing Pipeline in a PipelineJob to execute:\n"
            "    job = PipelineJob.from_pipeline(pipeline, store=db, sources={...})\n"
            "    job.run()"
        )

    def as_node(self) -> "FunctionNode":
        """Return self — already a lightweight node.

        Returns:
            ``self``
        """
        return self


# Patch FunctionJobNode.as_node() to return the lightweight FunctionNode variant
def _function_job_node_as_node(self) -> "FunctionNode":
    """Return a lightweight ``FunctionNode`` with the same identity.

    Returns:
        A new ``FunctionNode`` with the same function_pod, input_stream, and label.
    """
    return FunctionNode(
        function_pod=self._function_pod,
        input_stream=self._input_stream,
        label=self._label,
    )


FunctionJobNode.as_node = _function_job_node_as_node  # type: ignore[method-assign]
```

> **Note on architecture:** This "alias + subclass" approach avoids duplicating 1400 lines of code. `FunctionJobNode` = the existing class unchanged. `FunctionNode` = thin subclass that overrides only `__init__` and `iter_data`. Both have identical `content_hash()` / `pipeline_hash()` because `__init__` sets identical state.

- [ ] **Step 2.4: Run new tests**

```bash
uv run pytest tests/test_core/nodes/test_function_node_split.py -v
```

Expected: All 7 tests pass.

- [ ] **Step 2.5: Update nodes/__init__.py**

```python
from typing import TypeAlias

from .function_node import FunctionJobNode, FunctionNode
from .operator_node import OperatorNode
from .source_node import SourceJobNode, SourceNode, SourceNodeBase

GraphNode: TypeAlias = SourceNode | FunctionNode | OperatorNode
JobNode: TypeAlias = SourceJobNode | FunctionJobNode

__all__ = [
    "FunctionJobNode",
    "FunctionNode",
    "GraphNode",
    "JobNode",
    "OperatorNode",
    "SourceJobNode",
    "SourceNode",
    "SourceNodeBase",
]
```

- [ ] **Step 2.6: Run full pipeline test suite**

```bash
uv run pytest tests/test_pipeline/ tests/test_core/ -v --tb=short 2>&1 | tail -40
```

Expected: All tests pass. If any tests create `FunctionNode(..., pipeline_database=...)` directly, those still work because `FunctionNode` subclasses `FunctionJobNode` and passes kwargs up.

- [ ] **Step 2.7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py \
        src/orcapod/core/nodes/__init__.py \
        tests/test_core/nodes/test_function_node_split.py
git commit -m "refactor(nodes): split FunctionNode into thin FunctionNode + FunctionJobNode"
```

---

## Task 3: Split operator_node.py into OperatorNodeBase + OperatorNode + OperatorJobNode

**Files:**
- Rewrite: `src/orcapod/core/nodes/operator_node.py`
- Modify: `src/orcapod/core/nodes/__init__.py`
- Create: `tests/test_core/nodes/test_operator_node_split.py`

Same pattern as Task 2: `OperatorJobNode` = existing `OperatorNode` renamed. Thin `OperatorNode` overrides `iter_data()` to raise `PipelineJobRequiredError`.

- [ ] **Step 3.1: Write failing tests**

Create `tests/test_core/nodes/test_operator_node_split.py`:

```python
"""Tests for the OperatorNode / OperatorJobNode split."""
from __future__ import annotations

import pytest

from orcapod.errors import PipelineJobRequiredError
from orcapod.types import Schema


@pytest.fixture
def source_pair():
    from orcapod.core.nodes.source_node import SourceNode

    tag_schema = Schema({"id": int})
    data_schema_a = Schema({"a": float})
    data_schema_b = Schema({"b": float})
    node_a = SourceNode(name="src_a", tag_schema=tag_schema, data_schema=data_schema_a)
    node_b = SourceNode(name="src_b", tag_schema=tag_schema, data_schema=data_schema_b)
    return node_a, node_b


class TestThinOperatorNode:
    def test_iter_data_raises_pipeline_job_required(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        op_node = OperatorNode(operator=op, input_streams=(node_a, node_b))
        with pytest.raises(PipelineJobRequiredError):
            list(op_node.iter_data())

    def test_node_type(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        op_node = OperatorNode(operator=op, input_streams=(node_a, node_b))
        assert op_node.node_type == "operator"


class TestOperatorJobNodeHashParity:
    def test_content_hash_matches_operator_node(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        thin = OperatorNode(operator=op, input_streams=(node_a, node_b))
        job = OperatorJobNode(operator=op, input_streams=(node_a, node_b))
        assert thin.content_hash() == job.content_hash()

    def test_pipeline_hash_matches_operator_node(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        thin = OperatorNode(operator=op, input_streams=(node_a, node_b))
        job = OperatorJobNode(operator=op, input_streams=(node_a, node_b))
        assert thin.pipeline_hash() == job.pipeline_hash()

    def test_as_node_returns_operator_node(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        job = OperatorJobNode(operator=op, input_streams=(node_a, node_b))
        thin = job.as_node()
        assert isinstance(thin, OperatorNode)
        assert thin.content_hash() == job.content_hash()
```

- [ ] **Step 3.2: Run tests to confirm failure**

```bash
uv run pytest tests/test_core/nodes/test_operator_node_split.py -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name 'OperatorJobNode'`

- [ ] **Step 3.3: Refactor operator_node.py**

At the bottom of `src/orcapod/core/nodes/operator_node.py`, add:

```python
# OperatorJobNode is the DB-backed execution variant of OperatorNode.
# It is the existing OperatorNode class renamed.
OperatorJobNode = OperatorNode


class OperatorNode(OperatorJobNode):  # type: ignore[no-redef]
    """Lightweight blueprint node for ``Pipeline`` recording.

    Carries no database references.  Calling ``iter_data()`` raises
    ``PipelineJobRequiredError``.  All identity methods are inherited from
    ``OperatorJobNode`` and produce identical hashes.

    Args:
        operator: The wrapped operator pod.
        input_streams: Upstream streams (``SourceNode`` instances or other
            blueprint nodes).
        label: Optional display label.
    """

    def __init__(
        self,
        operator: "OperatorPodProtocol",
        input_streams: "tuple[StreamProtocol, ...] | list[StreamProtocol]",
        tracker_manager: "TrackerManagerProtocol | None" = None,
        label: str | None = None,
        config: "Config | None" = None,
    ) -> None:
        # Construct without any DB parameters
        super().__init__(
            operator=operator,
            input_streams=input_streams,
            tracker_manager=tracker_manager,
            label=label,
            config=config,
        )

    def iter_data(self):
        """Raise PipelineJobRequiredError — blueprint node cannot produce data.

        Raises:
            PipelineJobRequiredError: Always.
        """
        from orcapod.errors import PipelineJobRequiredError

        raise PipelineJobRequiredError(
            f"OperatorNode '{self.label}' is a blueprint node and cannot produce data. "
            "Wrap the containing Pipeline in a PipelineJob to execute:\n"
            "    job = PipelineJob.from_pipeline(pipeline, store=db, sources={...})\n"
            "    job.run()"
        )

    def as_node(self) -> "OperatorNode":
        """Return self — already a lightweight node."""
        return self


# Patch OperatorJobNode.as_node() to return the lightweight OperatorNode variant
def _operator_job_node_as_node(self) -> "OperatorNode":
    """Return a lightweight ``OperatorNode`` with the same identity."""
    return OperatorNode(
        operator=self._operator,
        input_streams=self._input_streams,
        label=self._label,
    )


OperatorJobNode.as_node = _operator_job_node_as_node  # type: ignore[method-assign]
```

Also add this import at the top of the file (with the other imports):

```python
# (PipelineJobRequiredError is imported lazily inside iter_data to avoid circular import)
```

- [ ] **Step 3.4: Update nodes/__init__.py to export OperatorJobNode**

```python
from typing import TypeAlias

from .function_node import FunctionJobNode, FunctionNode
from .operator_node import OperatorJobNode, OperatorNode
from .source_node import SourceJobNode, SourceNode, SourceNodeBase

GraphNode: TypeAlias = SourceNode | FunctionNode | OperatorNode
JobNode: TypeAlias = SourceJobNode | FunctionJobNode | OperatorJobNode

__all__ = [
    "FunctionJobNode",
    "FunctionNode",
    "GraphNode",
    "JobNode",
    "OperatorJobNode",
    "OperatorNode",
    "SourceJobNode",
    "SourceNode",
    "SourceNodeBase",
]
```

- [ ] **Step 3.5: Run new and existing tests**

```bash
uv run pytest tests/test_core/nodes/test_operator_node_split.py tests/test_pipeline/ -v --tb=short 2>&1 | tail -40
```

Expected: All pass.

- [ ] **Step 3.6: Commit**

```bash
git add src/orcapod/core/nodes/operator_node.py \
        src/orcapod/core/nodes/__init__.py \
        tests/test_core/nodes/test_operator_node_split.py
git commit -m "refactor(nodes): split OperatorNode into thin OperatorNode + OperatorJobNode"
```

---

## Task 4: Remove Pipeline.bind() and add PipelineJob.from_pipeline() + as_pipeline()

**Files:**
- Modify: `src/orcapod/pipeline/graph.py` — remove `bind()`
- Modify: `src/orcapod/pipeline/job.py` — add `from_pipeline()`, `as_pipeline()`, make `bind()` mutating

This is the heart of the public API change. `Pipeline.bind()` is deleted; `PipelineJob.from_pipeline()` is the new way to create a job from a compiled blueprint.

- [ ] **Step 4.1: Write failing tests**

Add these tests to `tests/test_pipeline/test_pipeline_job.py` (at the bottom, after existing tests):

```python
class TestFromPipeline:
    """PipelineJob.from_pipeline() creates a runnable job from a compiled Pipeline."""

    def test_from_pipeline_creates_pipeline_job(self, compiled_pipeline, db):
        """from_pipeline returns a PipelineJob with the same topology."""
        from orcapod.pipeline.job import PipelineJob

        job = PipelineJob.from_pipeline(compiled_pipeline, store=db)
        assert isinstance(job, PipelineJob)

    def test_from_pipeline_with_sources_binds_them(self, compiled_pipeline, db, source_a):
        """Sources passed to from_pipeline are immediately bound."""
        from orcapod.pipeline.job import PipelineJob

        job = PipelineJob.from_pipeline(
            compiled_pipeline, store=db, sources={"slot_a": source_a}
        )
        assert "slot_a" in job._sources

    def test_pipeline_bind_removed(self, compiled_pipeline):
        """Pipeline.bind() no longer exists."""
        assert not hasattr(compiled_pipeline, "bind"), (
            "Pipeline.bind() must be removed — use PipelineJob.from_pipeline() instead"
        )


class TestMutatingBind:
    """PipelineJob.bind() mutates in place and returns None."""

    def test_bind_returns_none(self, pipeline_job_with_pipeline, source_a):
        result = pipeline_job_with_pipeline.bind(sources={"slot_a": source_a})
        assert result is None

    def test_bind_mutates_sources(self, pipeline_job_with_pipeline, source_a):
        pipeline_job_with_pipeline.bind(sources={"slot_a": source_a})
        assert "slot_a" in pipeline_job_with_pipeline._sources

    def test_bind_mutates_store(self, pipeline_job_with_pipeline, db):
        pipeline_job_with_pipeline.bind(store=db)
        assert pipeline_job_with_pipeline._store is db


class TestAsPipeline:
    """PipelineJob.as_pipeline() returns a lightweight Pipeline."""

    def test_as_pipeline_returns_pipeline(self, pipeline_job_with_sources_and_store):
        from orcapod.pipeline.graph import Pipeline

        pipeline = pipeline_job_with_sources_and_store.as_pipeline()
        assert isinstance(pipeline, Pipeline)

    def test_as_pipeline_node_hashes_match(self, pipeline_job_with_sources_and_store):
        """as_pipeline() nodes have matching pipeline_hash to job nodes."""
        job = pipeline_job_with_sources_and_store
        pipeline = job.as_pipeline()

        for node_hash in job._persistent_node_map:
            assert node_hash in pipeline._persistent_node_map
```

> **Note:** The fixture names above (`compiled_pipeline`, `db`, `source_a`, `pipeline_job_with_pipeline`, `pipeline_job_with_sources_and_store`) must be defined in `tests/test_pipeline/conftest.py` or at the top of the test file. Add them as needed based on what already exists.

- [ ] **Step 4.2: Check existing fixtures in test files**

```bash
grep -n "^def compiled_pipeline\|^def pipeline_job_with_pipeline\|^def source_a\|^@pytest.fixture" \
    tests/test_pipeline/test_pipeline_job.py | head -20
```

Identify what fixtures already exist and add only the missing ones.

- [ ] **Step 4.3: Remove Pipeline.bind()**

In `src/orcapod/pipeline/graph.py`, delete the entire `bind()` method (lines ~313–340 currently):

```python
# DELETE this entire block:
def bind(
    self,
    sources: "dict[str, cp.StreamProtocol] | None" = None,
    store: "dbp.ArrowDatabaseProtocol | None" = None,
    execution_context: "ExecutionContext | None" = None,
) -> "PipelineJob":
    ...
```

Also remove the `TYPE_CHECKING` import of `PipelineJob` from graph.py if it was only used by `bind()`.

- [ ] **Step 4.4: Add PipelineJob.from_pipeline() classmethod**

In `src/orcapod/pipeline/job.py`, add the following classmethod to `PipelineJob`:

```python
@classmethod
def from_pipeline(
    cls,
    pipeline: "Pipeline",
    store: "ArrowDatabaseProtocol | None" = None,
    sources: "dict[str, cp.StreamProtocol] | None" = None,
    execution_context: "ExecutionContext | None" = None,
) -> "PipelineJob":
    """Create a runnable ``PipelineJob`` from a compiled ``Pipeline``.

    Walks the pipeline's ``_persistent_node_map`` topologically and
    creates corresponding ``JobNode`` variants:

    * ``SourceNode`` → ``SourceJobNode(name, schemas, concrete=sources.get(name))``
    * ``FunctionNode`` → ``FunctionJobNode(function_pod, upstream_job_node, label)``
    * ``OperatorNode`` → ``OperatorJobNode(operator, upstream_job_nodes, label)``

    If *store* is set, ``_distribute_databases()`` is called immediately
    so that all ``FunctionJobNode`` / ``OperatorJobNode`` objects have live
    DB references before the first ``run()``.

    Args:
        pipeline: A compiled ``Pipeline`` (``pipeline._compiled`` must be
            ``True``).
        store: Database for result caching and operator records.
        sources: Mapping of ``SourceNode.name`` → concrete source.
        execution_context: Optional execution configuration.

    Returns:
        A new ``PipelineJob`` ready to run (or ``bind()`` further).

    Raises:
        ValueError: If *pipeline* has not been compiled.
    """
    from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode
    from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
    from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
    from orcapod.utils.lazy_module import LazyModule

    nx = LazyModule("networkx")

    if not pipeline._compiled:
        raise ValueError(
            "Pipeline must be compiled before creating a PipelineJob from it. "
            "Call pipeline.compile() or use auto_compile=True."
        )

    bound_sources: dict[str, cp.StreamProtocol] = dict(sources or {})

    # Build a topological ordering of the persistent node map
    G = pipeline._hash_graph
    job_node_map: dict[str, object] = {}  # content_hash_str → JobNode

    import networkx as _nx

    for node_hash in _nx.topological_sort(G):
        if node_hash not in pipeline._persistent_node_map:
            continue

        node = pipeline._persistent_node_map[node_hash]

        if isinstance(node, SourceNode):
            concrete = bound_sources.get(node.name)
            job_node = SourceJobNode(
                name=node.name,
                tag_schema=node.tag_schema,
                data_schema=node.data_schema,
                concrete=concrete,
            )

        elif isinstance(node, FunctionNode):
            # Rewire input to the already-built SourceJobNode / FunctionJobNode
            original_input_hash = node._input_stream.content_hash().to_string()
            upstream_job_node = job_node_map[original_input_hash]
            job_node = FunctionJobNode(
                function_pod=node._function_pod,
                input_stream=upstream_job_node,  # type: ignore[arg-type]
                label=node._label,
                table_scope=node._table_scope,
            )

        elif isinstance(node, OperatorNode):
            # Rewire all inputs to already-built job nodes
            upstream_job_nodes = tuple(
                job_node_map[s.content_hash().to_string()]
                for s in node._input_streams
            )
            job_node = OperatorJobNode(
                operator=node._operator,
                input_streams=upstream_job_nodes,  # type: ignore[arg-type]
                label=node._label,
                cache_mode=node._cache_mode,
                table_scope=node._table_scope,
            )

        else:
            raise TypeError(
                f"Unknown node type in pipeline._persistent_node_map: {type(node)}"
            )

        job_node_map[node_hash] = job_node

    # Construct the PipelineJob and inject the built node map
    job = cls.__new__(cls)
    super(PipelineJob, job).__init__()
    job._store = store
    job._execution_context = execution_context
    job._sources = bound_sources
    job._pipeline_name = pipeline._name
    job._unresolved_specs = []
    job._has_run = False
    job._run_id = None
    job._rec_graph_edges = []
    job._rec_upstreams = {}
    job._rec_node_lut = {}
    job._spec_by_name = {}

    # Copy pipeline graph structure
    job._compiled_pipeline = pipeline
    job._persistent_node_map = job_node_map  # type: ignore[assignment]
    job._nodes = {}  # populated below

    # Build label → node map by copying from pipeline._nodes
    for label, node in pipeline._nodes.items():
        node_hash = node.content_hash().to_string()
        if node_hash in job_node_map:
            job._nodes[label] = job_node_map[node_hash]  # type: ignore[assignment]

    # Wire databases if store is provided
    if store is not None:
        job._distribute_databases()

    return job
```

- [ ] **Step 4.5: Make PipelineJob.bind() mutating (returns None)**

Replace the existing `bind()` method in `src/orcapod/pipeline/job.py`:

```python
def bind(
    self,
    sources: "dict[str, cp.StreamProtocol] | None" = None,
    store: "ArrowDatabaseProtocol | None" = None,
    execution_context: "ExecutionContext | None" = None,
) -> None:
    """Update bindings in place.

    Mutating — modifies ``self`` directly. Existing bindings not mentioned
    in this call are preserved.

    When *sources* is provided, each concrete source is validated against
    its matching ``SourceNode`` slot schema, then the corresponding
    ``SourceJobNode._concrete`` is updated in-place (downstream
    ``FunctionJobNode`` objects that hold a reference to the same
    ``SourceJobNode`` object automatically see the new concrete without
    any cascading reference updates).

    When *store* is provided and differs from the current store,
    ``_distribute_databases()`` is called so that all job nodes receive
    live DB references immediately.

    Args:
        sources: Mapping of ``SourceNode.name`` → concrete source.
        store: Replaces the current store and triggers DB redistribution.
        execution_context: Replaces the current execution context.

    Raises:
        SourceSpecMismatchError: If any source's schema is incompatible.
        ValueError: If a source key has no matching ``SourceNode`` slot.
    """
    from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

    store_changed = store is not None and store is not self._store

    if store is not None:
        self._store = store

    if sources is not None:
        pipeline = self._compiled_pipeline
        if pipeline is not None:
            spec_names = {
                node.name
                for node in pipeline._persistent_node_map.values()
                if isinstance(node, SourceNode)
            }
            unknown = set(sources.keys()) - spec_names
            if unknown:
                raise ValueError(
                    f"bind() received source keys with no matching SourceNode: "
                    f"{sorted(unknown)}. Known names: {sorted(spec_names)}"
                )
            # Validate schema for each supplied source
            for node in pipeline._persistent_node_map.values():
                if isinstance(node, SourceNode) and node.name in sources:
                    node.validate(sources[node.name])

        # Update SourceJobNode._concrete in-place
        for job_node in (self._persistent_node_map or {}).values():
            if isinstance(job_node, SourceJobNode) and job_node.name in sources:
                job_node._concrete = sources[job_node.name]
                # Clear cached hashes so content_hash() reflects new concrete
                job_node._content_hash_cache.clear()

        self._sources.update(sources)

    if execution_context is not None:
        self._execution_context = execution_context

    if store_changed:
        self._distribute_databases()
```

- [ ] **Step 4.6: Add _distribute_databases() to PipelineJob**

Check whether `_distribute_databases()` already exists in `job.py`. If not (it may be in `_build_execution_graph`), add it:

```python
def _distribute_databases(self) -> None:
    """Wire live DB references to all FunctionJobNode and OperatorJobNode objects.

    Called by ``bind()`` when *store* is changed and by ``from_pipeline()``
    when *store* is provided at construction time.

    Raises:
        RuntimeError: If ``_store`` is not set.
    """
    from orcapod.core.nodes.function_node import FunctionJobNode
    from orcapod.core.nodes.operator_node import OperatorJobNode

    if self._store is None:
        raise RuntimeError(
            "Cannot distribute databases: no store is set. "
            "Call bind(store=...) or from_pipeline(..., store=...) first."
        )

    pipeline_db = self._store.at(*self._pipeline_name)
    result_db = pipeline_db.at("_result")

    for node in (self._persistent_node_map or {}).values():
        if isinstance(node, FunctionJobNode):
            node.attach_databases(
                pipeline_database=pipeline_db,
                result_database=result_db,
            )
        elif isinstance(node, OperatorJobNode):
            node.attach_databases(pipeline_database=pipeline_db)
```

- [ ] **Step 4.7: Add as_pipeline() to PipelineJob**

```python
def as_pipeline(self) -> "Pipeline":
    """Return the lightweight ``Pipeline`` blueprint for this job.

    Walks ``_persistent_node_map`` and calls ``.as_node()`` on each
    ``JobNode`` to obtain the corresponding lightweight ``Node``.
    Upstream references in the returned ``Pipeline`` point at the
    lightweight nodes, not the ``JobNode`` objects.

    Returns:
        A compiled ``Pipeline`` whose ``_persistent_node_map`` contains
        only lightweight ``SourceNode`` / ``FunctionNode`` / ``OperatorNode``
        objects with identical ``content_hash()`` and ``pipeline_hash()``
        values to their ``JobNode`` counterparts.
    """
    from orcapod.core.nodes.function_node import FunctionJobNode
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.pipeline.graph import Pipeline

    import networkx as _nx

    if self._compiled_pipeline is None:
        raise RuntimeError(
            "PipelineJob has no compiled pipeline. "
            "Either use 'with job:' to record a DAG, "
            "or create the job via PipelineJob.from_pipeline()."
        )

    G = self._compiled_pipeline._hash_graph
    node_map: dict[str, object] = {}

    for node_hash in _nx.topological_sort(G):
        if node_hash not in (self._persistent_node_map or {}):
            continue
        job_node = self._persistent_node_map[node_hash]  # type: ignore[index]
        node_map[node_hash] = job_node.as_node()

    pipeline = Pipeline(name=self._pipeline_name, auto_compile=False)
    pipeline._graph_edges = list(self._compiled_pipeline._graph_edges)
    pipeline._upstreams = dict(self._compiled_pipeline._upstreams)
    pipeline._node_lut = dict(self._compiled_pipeline._node_lut)
    pipeline._hash_graph = self._compiled_pipeline._hash_graph
    pipeline._persistent_node_map = node_map  # type: ignore[assignment]
    pipeline._nodes = {
        label: node_map[node.content_hash().to_string()]  # type: ignore[index]
        for label, node in self._compiled_pipeline._nodes.items()
        if node.content_hash().to_string() in node_map
    }
    pipeline._compiled = True

    return pipeline
```

- [ ] **Step 4.8: Run new and full test suite**

```bash
uv run pytest tests/test_pipeline/ -v --tb=short 2>&1 | tail -40
```

Fix any test that calls `pipeline.bind(...)` — replace with:

```python
# OLD:
job = pipeline.bind(sources={"slot_a": src}, store=db)

# NEW:
from orcapod.pipeline.job import PipelineJob
job = PipelineJob.from_pipeline(pipeline, store=db, sources={"slot_a": src})
```

- [ ] **Step 4.9: Commit**

```bash
git add src/orcapod/pipeline/graph.py \
        src/orcapod/pipeline/job.py \
        tests/test_pipeline/test_pipeline_job.py
git commit -m "refactor(pipeline): remove Pipeline.bind(); add PipelineJob.from_pipeline(), as_pipeline(), mutating bind()"
```

---

## Task 5: AbstractPipelineBase in pipeline/base.py

**Files:**
- Create: `src/orcapod/pipeline/base.py`
- Modify: `src/orcapod/pipeline/graph.py` — inherit from `AbstractPipelineBase`
- Modify: `src/orcapod/pipeline/job.py` — inherit from `AbstractPipelineBase`

This task extracts the shared recording machinery (`_node_lut`, `_upstreams`, `_graph_edges`, `_hash_graph`, `reset()`, `__exit__`, `__getattr__`, `graph` property, etc.) into a common base.

- [ ] **Step 5.1: Create src/orcapod/pipeline/base.py**

```python
"""AbstractPipelineBase — shared recording mechanism for Pipeline and PipelineJob."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
    from orcapod.core.nodes import GraphNode

else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)


class AbstractPipelineBase(AutoRegisteringContextBasedTracker, ABC):
    """Shared recording mechanism and graph state for Pipeline and PipelineJob.

    Manages the ``with``-block recording phase: accumulating graph edges,
    node LUT entries, and upstream stream references.  Subclasses specialise
    which node types are created (blueprint vs. job nodes).

    Args:
        name: Pipeline name (string or tuple).  Used to scope database paths.
        tracker_manager: Optional tracker manager override.
    """

    def __init__(
        self,
        name: str | tuple[str, ...] = "pipeline",
        tracker_manager: cp.TrackerManagerProtocol | None = None,
    ) -> None:
        super().__init__(tracker_manager=tracker_manager)
        self._name: tuple[str, ...] = (name,) if isinstance(name, str) else tuple(name)
        self._node_lut: dict[str, Any] = {}
        self._upstreams: dict[str, cp.StreamProtocol] = {}
        self._graph_edges: list[tuple[str, str]] = []
        self._hash_graph: nx.DiGraph = nx.DiGraph()
        self._persistent_node_map: dict[str, Any] = {}
        self._nodes: dict[str, Any] = {}
        self._node_graph: nx.DiGraph | None = None
        self._compiled: bool = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> tuple[str, ...]:
        """Pipeline name tuple."""
        return self._name

    @property
    def graph(self) -> nx.DiGraph:
        """Directed hash graph of accumulated pipeline structure."""
        return self._hash_graph

    @property
    def compiled_nodes(self) -> dict[str, Any]:
        """Copy of the compiled nodes dict (label → node)."""
        return self._nodes.copy()

    # ------------------------------------------------------------------
    # Recording helpers
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Clear session-scoped recorded state (node LUT, upstreams, edge list).

        Note:
            ``_hash_graph`` and ``_persistent_node_map`` are intentionally
            *not* cleared — they accumulate across ``with`` blocks.
        """
        self._node_lut.clear()
        self._upstreams.clear()
        self._graph_edges.clear()

    def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        super().__exit__(exc_type, exc_value, traceback)
        if exc_type is None:
            self.compile()

    def __getattr__(self, item: str) -> Any:
        """Look up compiled nodes by label as attribute access."""
        if item.startswith("_"):
            raise AttributeError(item)
        nodes = object.__getattribute__(self, "_nodes")
        if item in nodes:
            return nodes[item]
        raise AttributeError(
            f"{type(self).__name__!r} has no attribute {item!r}. "
            f"Available node labels: {sorted(nodes.keys())}"
        )

    # ------------------------------------------------------------------
    # Abstract — specialised per subclass
    # ------------------------------------------------------------------

    @abstractmethod
    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a function pod invocation into the graph."""
        ...

    @abstractmethod
    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """Record an operator pod invocation into the graph."""
        ...

    @abstractmethod
    def compile(self) -> None:
        """Compile recorded invocations into a frozen DAG."""
        ...
```

- [ ] **Step 5.2: Update Pipeline to inherit from AbstractPipelineBase**

In `src/orcapod/pipeline/graph.py`:

1. Add import: `from orcapod.pipeline.base import AbstractPipelineBase`
2. Change class definition: `class Pipeline(AbstractPipelineBase):` (remove `AutoRegisteringContextBasedTracker` from the inheritance since it's now in the base)
3. Remove from `Pipeline.__init__` the attributes already in `AbstractPipelineBase` (`_node_lut`, `_upstreams`, `_graph_edges`, `_hash_graph`, `_name`, `_nodes`, `_persistent_node_map`, `_node_graph`, `_compiled`)
4. Update `super().__init__` call to pass `name=name` and `tracker_manager=tracker_manager`
5. Remove the `reset()` method (now in base), `graph` property (now in base), `compiled_nodes` property (now in base), and `__exit__` if it just calls `compile()` (now in base handles this)
6. Keep `name` property if it does something different, otherwise remove it (base has it)
7. Keep `_auto_compile` flag in Pipeline (base doesn't have it)

Verify `__exit__` in Pipeline: the base calls `compile()` unconditionally; Pipeline uses `_auto_compile`. Update base's `__exit__` to be abstract (not calling `compile()`), or handle `_auto_compile` differently:

Actually, keep Pipeline's `__exit__` override:

```python
def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
    super(AutoRegisteringContextBasedTracker, self).__exit__(exc_type, exc_value, traceback)
    if exc_type is None and self._auto_compile:
        self.compile()
```

Or just keep `_auto_compile` handling by overriding `__exit__` in `Pipeline`:

```python
def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
    # Skip AbstractPipelineBase.__exit__ (which calls compile() unconditionally)
    # and call compile() only if auto_compile is True.
    AutoRegisteringContextBasedTracker.__exit__(self, exc_type, exc_value, traceback)
    if exc_type is None and self._auto_compile:
        self.compile()
```

- [ ] **Step 5.3: Update PipelineJob to inherit from AbstractPipelineBase**

In `src/orcapod/pipeline/job.py`:

1. Add import: `from orcapod.pipeline.base import AbstractPipelineBase`
2. Change class definition: `class PipelineJob(AbstractPipelineBase):`
3. Update `__init__` to call `super().__init__(name=name, tracker_manager=tracker_manager)` and remove duplicate state initialization

- [ ] **Step 5.4: Run full test suite**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -40
```

Expected: All tests pass.

- [ ] **Step 5.5: Commit**

```bash
git add src/orcapod/pipeline/base.py \
        src/orcapod/pipeline/graph.py \
        src/orcapod/pipeline/job.py
git commit -m "refactor(pipeline): extract AbstractPipelineBase with shared recording mechanism"
```

---

## Task 6: Update PipelineJob to use FunctionJobNode / OperatorJobNode during recording

**Files:**
- Modify: `src/orcapod/pipeline/job.py`

Currently `PipelineJob.record_function_pod_invocation` creates a `FunctionNode`. After this task it creates a `FunctionJobNode` (DB-ready). Same for operators. PipelineJob's `compile()` then builds `SourceJobNode` leaves and wires job nodes together.

- [ ] **Step 6.1: Write failing test**

Add to `tests/test_pipeline/test_pipeline_job.py`:

```python
class TestPipelineJobUsesJobNodes:
    """PipelineJob._persistent_node_map must contain only JobNode variants."""

    def test_persistent_map_contains_source_job_nodes(self, pipeline_job_with_sources):
        from orcapod.core.nodes.source_node import SourceJobNode

        for node in pipeline_job_with_sources._persistent_node_map.values():
            from orcapod.core.nodes.source_node import SourceNodeBase
            if isinstance(node, SourceNodeBase):
                assert isinstance(node, SourceJobNode), (
                    f"Expected SourceJobNode, got {type(node).__name__}"
                )

    def test_persistent_map_contains_function_job_nodes(self, pipeline_job_with_sources):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode

        for node in pipeline_job_with_sources._persistent_node_map.values():
            if node.node_type == "function":
                assert isinstance(node, FunctionJobNode), (
                    f"Expected FunctionJobNode, got {type(node).__name__}"
                )
                # Thin FunctionNode should NOT appear in PipelineJob
                assert type(node) is not FunctionNode, (
                    "PipelineJob should use FunctionJobNode, not thin FunctionNode"
                )
```

- [ ] **Step 6.2: Run test to confirm failure**

```bash
uv run pytest tests/test_pipeline/test_pipeline_job.py::TestPipelineJobUsesJobNodes -v 2>&1 | tail -20
```

Expected: FAIL (currently creates `FunctionNode` in `_persistent_node_map`)

- [ ] **Step 6.3: Update PipelineJob recording to use FunctionJobNode/OperatorJobNode**

In `src/orcapod/pipeline/job.py`, update `record_function_pod_invocation`:

```python
def record_function_pod_invocation(
    self,
    pod: cp.FunctionPodProtocol,
    input_stream: cp.StreamProtocol,
    label: str | None = None,
) -> None:
    from orcapod.core.nodes.function_node import FunctionJobNode

    input_stream = self._to_node_stream(input_stream)
    input_hash = input_stream.content_hash().to_string()
    node = FunctionJobNode(function_pod=pod, input_stream=input_stream, label=label)
    fn_hash = node.content_hash().to_string()
    self._rec_node_lut[fn_hash] = node
    self._rec_upstreams[input_hash] = input_stream
    self._rec_graph_edges.append((input_hash, fn_hash))
```

Update `record_operator_pod_invocation`:

```python
def record_operator_pod_invocation(
    self,
    pod: cp.OperatorPodProtocol,
    upstreams: tuple[cp.StreamProtocol, ...] = (),
    label: str | None = None,
) -> None:
    from orcapod.core.nodes.operator_node import OperatorJobNode

    processed = tuple(self._to_node_stream(s) for s in upstreams)
    node = OperatorJobNode(operator=pod, input_streams=processed, label=label)
    op_hash = node.content_hash().to_string()
    self._rec_node_lut[op_hash] = node
    for upstream in processed:
        up_hash = upstream.content_hash().to_string()
        self._rec_upstreams[up_hash] = upstream
        self._rec_graph_edges.append((up_hash, op_hash))
```

- [ ] **Step 6.4: Update PipelineJob._compile_from_recording() to create SourceJobNode leaves**

In `src/orcapod/pipeline/job.py`, update `_compile_from_recording`:

```python
def _compile_from_recording(self) -> None:
    """Compile recorded edges + node LUT into a pure Pipeline + job node map."""
    from orcapod.pipeline.graph import Pipeline

    # Build the pure Pipeline (SourceNode leaves, thin FunctionNode/OperatorNode)
    pipeline = Pipeline(name=self._pipeline_name, auto_compile=False)
    pipeline._graph_edges = list(self._rec_graph_edges)
    pipeline._upstreams = dict(self._rec_upstreams)
    # Convert FunctionJobNode → FunctionNode and OperatorJobNode → OperatorNode
    # for the blueprint pipeline
    pipeline._node_lut = {
        h: node.as_node() for h, node in self._rec_node_lut.items()
    }
    for edge in self._rec_graph_edges:
        pipeline._hash_graph.add_edge(*edge)
    for node_hash, node in self._rec_node_lut.items():
        if node_hash in pipeline._hash_graph.nodes:
            pipeline._hash_graph.nodes[node_hash]["node_type"] = node.node_type
            if node._label:
                pipeline._hash_graph.nodes[node_hash]["label"] = node._label
    for node_hash, stream in self._rec_upstreams.items():
        if node_hash in pipeline._hash_graph.nodes:
            if not pipeline._hash_graph.nodes[node_hash].get("node_type"):
                pipeline._hash_graph.nodes[node_hash]["node_type"] = "source"
    pipeline.compile()
    self._compiled_pipeline = pipeline

    # Build PipelineJob's own job node map using SourceJobNode for leaves
    import networkx as _nx

    job_node_map: dict[str, object] = {}
    G = pipeline._hash_graph

    for node_hash in _nx.topological_sort(G):
        if node_hash not in pipeline._persistent_node_map:
            continue
        bp_node = pipeline._persistent_node_map[node_hash]

        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode

        if isinstance(bp_node, SourceNode):
            concrete = self._sources.get(bp_node.name)
            job_node = SourceJobNode(
                name=bp_node.name,
                tag_schema=bp_node.tag_schema,
                data_schema=bp_node.data_schema,
                concrete=concrete,
            )

        elif isinstance(bp_node, FunctionNode):
            original_input_hash = bp_node._input_stream.content_hash().to_string()
            upstream_job_node = job_node_map[original_input_hash]
            # Get the original FunctionJobNode from rec_node_lut
            rec_node = self._rec_node_lut.get(node_hash)
            table_scope = rec_node._table_scope if rec_node is not None else "pipeline_hash"
            job_node = FunctionJobNode(
                function_pod=bp_node._function_pod,
                input_stream=upstream_job_node,  # type: ignore[arg-type]
                label=bp_node._label,
                table_scope=table_scope,
            )

        elif isinstance(bp_node, OperatorNode):
            upstream_job_nodes = tuple(
                job_node_map[s.content_hash().to_string()]
                for s in bp_node._input_streams
            )
            rec_node = self._rec_node_lut.get(node_hash)
            table_scope = rec_node._table_scope if rec_node is not None else "pipeline_hash"
            cache_mode = rec_node._cache_mode if rec_node is not None else None
            from orcapod.types import CacheMode
            job_node = OperatorJobNode(
                operator=bp_node._operator,
                input_streams=upstream_job_nodes,  # type: ignore[arg-type]
                label=bp_node._label,
                cache_mode=cache_mode or CacheMode.OFF,
                table_scope=table_scope,
            )

        else:
            raise TypeError(
                f"Unknown blueprint node type in pipeline._persistent_node_map: {type(bp_node)}"
            )

        job_node_map[node_hash] = job_node

    self._persistent_node_map = job_node_map  # type: ignore[assignment]

    # Build label → job node map
    self._nodes = {
        label: job_node_map[node.content_hash().to_string()]  # type: ignore[index]
        for label, node in pipeline._nodes.items()
        if node.content_hash().to_string() in job_node_map
    }

    # Wire databases if store is set
    if self._store is not None:
        self._distribute_databases()
```

- [ ] **Step 6.5: Run test suite**

```bash
uv run pytest tests/test_pipeline/ -v --tb=short 2>&1 | tail -40
```

Fix any failures. Common issues:
- Tests that access `job._compiled_pipeline._persistent_node_map` expecting `FunctionNode` — now it has `FunctionNode` (blueprint), but `job._persistent_node_map` has `FunctionJobNode`. Update assertions.
- Tests calling `_build_execution_graph()` — check if that method still works or needs updating.

- [ ] **Step 6.6: Commit**

```bash
git add src/orcapod/pipeline/job.py
git commit -m "refactor(pipeline): PipelineJob recording now creates FunctionJobNode/OperatorJobNode/SourceJobNode"
```

---

## Task 7: Update serialization for the new node format

**Files:**
- Modify: `src/orcapod/pipeline/serialization.py`
- Modify: `src/orcapod/pipeline/graph.py` (save/load methods)
- Modify: `tests/test_pipeline/test_serialization.py`

The key change: serialized source nodes no longer wrap a `SourceSpec` — they are plain `SourceNode` objects with `source_type: "node"`. Format versions bump.

- [ ] **Step 7.1: Write failing serialization tests**

Add to `tests/test_pipeline/test_serialization.py`:

```python
class TestNewSerializationFormat:
    def test_save_load_roundtrip_with_source_node(self, tmp_path, compiled_pipeline):
        """Pipeline.save/load round-trip preserves SourceNode slots."""
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.pipeline.graph import Pipeline

        save_path = tmp_path / "test_pipeline.json"
        compiled_pipeline.save(save_path)

        loaded = Pipeline.load(save_path)
        assert loaded._compiled

        for node in loaded._persistent_node_map.values():
            if node.node_type == "source":
                assert isinstance(node, SourceNode)

    def test_saved_format_has_source_node_type(self, tmp_path, compiled_pipeline):
        import json

        save_path = tmp_path / "test_pipeline.json"
        compiled_pipeline.save(save_path)

        with open(save_path) as f:
            data = json.load(f)

        for node_data in data["nodes"].values():
            if node_data["node_type"] == "source":
                assert node_data.get("source_config", {}).get("source_type") == "node", (
                    "Source nodes should be serialized with source_type='node'"
                )

    def test_format_version_is_0_3(self, tmp_path, compiled_pipeline):
        import json

        save_path = tmp_path / "test_pipeline.json"
        compiled_pipeline.save(save_path)

        with open(save_path) as f:
            data = json.load(f)

        assert data["orcapod_pipeline_version"] == "0.3"
```

- [ ] **Step 7.2: Run tests to confirm failure**

```bash
uv run pytest tests/test_pipeline/test_serialization.py::TestNewSerializationFormat -v 2>&1 | tail -20
```

- [ ] **Step 7.3: Update Pipeline.save() for new SourceNode format**

In `src/orcapod/pipeline/graph.py`, find `save()` and update the source-node serialization block. Find the block that serializes source nodes (currently writes `SourceSpec` fields):

```python
# OLD (inside save() — look for the SourceNode handling block)
from orcapod.core.sources.source_spec import SourceSpec
if isinstance(node.stream, SourceSpec):
    spec = node.stream
    nodes[node_hash] = {
        "node_type": "source",
        "label": node.label,
        "source_config": {
            "source_type": "spec",
            "name": spec.name,
            "tag_schema": serialize_schema(spec.tag_schema),
            "data_schema": serialize_schema(spec.data_schema),
        },
    }
```

Replace with:

```python
# NEW — SourceNode is the leaf directly
from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass
if isinstance(node, SourceNodeClass):
    nodes[node_hash] = {
        "node_type": "source",
        "label": node.label,
        "source_config": {
            "source_type": "node",
            "name": node.name,
            "tag_schema": serialize_schema(node.tag_schema),
            "data_schema": serialize_schema(node.data_schema),
        },
    }
```

Update the format version constant in `src/orcapod/pipeline/serialization.py`:

```python
# OLD:
PIPELINE_FORMAT_VERSION = "0.2"
# NEW:
PIPELINE_FORMAT_VERSION = "0.3"
```

- [ ] **Step 7.4: Update Pipeline.load() to reconstruct SourceNode from new format**

In `src/orcapod/pipeline/graph.py`, find `load()` and update source node reconstruction:

```python
# NEW — inside load(), source node handling
if node_data["node_type"] == "source":
    source_config = node_data.get("source_config", {})
    source_type = source_config.get("source_type")
    if source_type == "node":
        from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass
        from orcapod.pipeline.serialization import deserialize_schema
        node = SourceNodeClass(
            name=source_config["name"],
            tag_schema=deserialize_schema(source_config["tag_schema"]),
            data_schema=deserialize_schema(source_config["data_schema"]),
        )
    elif source_type == "spec":
        # Backward-compat: load old format (v0.2) that used SourceSpec
        from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass
        from orcapod.pipeline.serialization import deserialize_schema
        node = SourceNodeClass(
            name=source_config["name"],
            tag_schema=deserialize_schema(source_config["tag_schema"]),
            data_schema=deserialize_schema(source_config["data_schema"]),
        )
    else:
        raise ValueError(
            f"Unknown source_type {source_type!r} in pipeline descriptor."
        )
```

> **Note:** For backward compatibility with v0.2 format, `source_type == "spec"` reconstructs a `SourceNode` with the same name/schemas — identical hashes are preserved since `SourceNode.identity_structure()` matches old `SourceSpec.identity_structure()`.

- [ ] **Step 7.5: Run serialization tests**

```bash
uv run pytest tests/test_pipeline/test_serialization.py -v --tb=short 2>&1 | tail -40
```

Expected: All tests pass (including the new ones and old ones).

- [ ] **Step 7.6: Commit**

```bash
git add src/orcapod/pipeline/graph.py \
        src/orcapod/pipeline/serialization.py \
        tests/test_pipeline/test_serialization.py
git commit -m "refactor(serialization): update Pipeline save/load for SourceNode format; bump to v0.3"
```

---

## Task 8: Delete SourceSpec and clean up all references

**Files:**
- Delete: `src/orcapod/core/sources/source_spec.py`
- Delete: `tests/test_core/sources/test_source_spec.py`
- Modify: `src/orcapod/core/sources/__init__.py`
- Modify: `src/orcapod/__init__.py`
- Modify: `src/orcapod/errors.py` — rename `SourceSpecMismatchError` → keep as alias
- Modify: all remaining test files that reference `SourceSpec`

- [ ] **Step 8.1: Find all remaining SourceSpec references**

```bash
grep -rn "SourceSpec\|source_spec" src/ tests/ --include="*.py" | grep -v "\.pyc"
```

Record every file that still imports or references `SourceSpec`.

- [ ] **Step 8.2: Update src/orcapod/__init__.py**

Replace `SourceSpec` export with `SourceNode`:

```python
# OLD:
from .core.sources.source_spec import SourceSpec

# NEW:
from .core.nodes.source_node import SourceNode
```

- [ ] **Step 8.3: Update src/orcapod/core/sources/__init__.py**

Remove `SourceSpec` from the exports. If the file only exported `SourceSpec`, the file can be left with just the remaining exports (or emptied).

- [ ] **Step 8.4: Update errors.py — rename SourceSpecMismatchError**

`SourceSpecMismatchError` is the right name since it describes a schema mismatch on a source node slot. Keep the name but update the docstring:

```python
class SourceSpecMismatchError(ValueError):
    """Raised when a concrete source's schema is incompatible with a SourceNode slot.

    Previously named in terms of ``SourceSpec``; the error class name is preserved
    for compatibility with any code that catches it by name.
    """
```

- [ ] **Step 8.5: Update remaining SourceSpec references in test files**

For each file from Step 8.1 that still imports `SourceSpec`:

**`tests/test_pipeline/test_pipeline.py`** — replace:
```python
# OLD:
from orcapod.core.sources.source_spec import SourceSpec
...
spec_a = SourceSpec(name="a", tag_schema=..., data_schema=...)
with pipeline:
    result = my_pod(spec_a)

# NEW:
from orcapod.core.nodes.source_node import SourceNode
...
spec_a = SourceNode(name="a", tag_schema=..., data_schema=...)
with pipeline:
    result = my_pod(spec_a)
```

**`tests/test_pipeline/test_pipeline_job.py`** — same pattern.

**`tests/test_core/test_tracker.py`** — if it uses `SourceSpec`, replace with `SourceNode`.

Run after each file update:

```bash
uv run pytest <updated_file> -v --tb=short 2>&1 | tail -20
```

- [ ] **Step 8.6: Delete source_spec.py**

```bash
git rm src/orcapod/core/sources/source_spec.py
git rm tests/test_core/sources/test_source_spec.py
```

- [ ] **Step 8.7: Run full test suite**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -40
```

Expected: All tests pass, no `SourceSpec` references remaining.

- [ ] **Step 8.8: Final grep to confirm SourceSpec is gone**

```bash
grep -rn "SourceSpec\|source_spec" src/ tests/ --include="*.py"
```

Expected: No output (zero matches).

- [ ] **Step 8.9: Commit**

```bash
git add -A
git commit -m "refactor(sources): delete SourceSpec; update all references to SourceNode (ENG-493)"
```

---

## Task 9: Integration test sweep and PR

**Files:**
- Run all tests, fix stragglers
- Create PR

- [ ] **Step 9.1: Run full test suite including integration tests**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tee /tmp/test_results.txt
grep -E "FAILED|ERROR" /tmp/test_results.txt
```

- [ ] **Step 9.2: Fix any remaining failures**

Common failure patterns at this stage:
- `_build_execution_graph()` in `job.py` may still reference `SourceSpec` — update to `SourceNode`
- Serialization load tests for old v0.2 format — verify backward-compat path works
- Orchestrator tests that build execution graphs — update to use `from_pipeline()` pattern

For each failure, diagnose root cause, implement fix, re-run the specific test.

- [ ] **Step 9.3: Verify hash stability end-to-end**

```bash
uv run python -c "
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.sources.source_spec import SourceSpec
from orcapod.types import Schema

tag = Schema({'id': int})
data = Schema({'value': float})

# These MUST be equal for DB path stability
old_hash = SourceSpec(name='x', tag_schema=tag, data_schema=data).content_hash()
new_hash = SourceNode(name='x', tag_schema=tag, data_schema=data).content_hash()

assert old_hash == new_hash, f'Hash mismatch! old={old_hash}, new={new_hash}'
print('Hash stability: PASS')

old_pipe = SourceSpec(name='x', tag_schema=tag, data_schema=data).pipeline_hash()
new_pipe = SourceNode(name='x', tag_schema=tag, data_schema=data).pipeline_hash()
assert old_pipe == new_pipe, f'Pipeline hash mismatch!'
print('Pipeline hash stability: PASS')
"
```

> **Note:** This check must pass before the PR is created. If SourceSpec has already been deleted at this point, adjust to compare against a recorded reference hash instead.

- [ ] **Step 9.4: Update src/orcapod/__init__.py exports**

Ensure `SourceNode` is properly exported (was done in Task 8, verify):

```python
from .core.nodes.source_node import SourceNode
from .pipeline.job import PipelineJob
from .pipeline.graph import Pipeline
```

- [ ] **Step 9.5: Final test run**

```bash
uv run pytest tests/ --tb=short 2>&1 | tail -10
```

Expected: `X passed, 0 failed, 0 errors`

- [ ] **Step 9.6: Commit any remaining fixes**

```bash
git add -A
git commit -m "fix(pipeline): address integration test failures after pure-descriptor refactor"
```

- [ ] **Step 9.7: Push and create PR**

```bash
git push -u origin eywalker/eng-493-refactor-pipeline-into-a-pure-computational-descriptor
```

Create PR targeting `dev`:

```bash
gh pr create \
  --title "refactor(pipeline): Pipeline pure-descriptor refactor — ENG-493" \
  --base dev \
  --body "$(cat <<'EOF'
## Summary

- `Pipeline` now stores only lightweight blueprint nodes (`FunctionNode`, `OperatorNode`, `SourceNode`) with no DB references
- `PipelineJob` stores DB-backed job nodes (`FunctionJobNode`, `OperatorJobNode`, `SourceJobNode`)
- `SourceSpec` deleted; replaced by `SourceNode` with bit-identical `content_hash()` / `pipeline_hash()` (DB paths preserved)
- `Pipeline.bind()` removed; replaced by `PipelineJob.from_pipeline(pipeline, store=..., sources=...)`
- `PipelineJob.bind()` is now mutating (returns `None`); updates `SourceJobNode._concrete` in-place
- `PipelineJob.as_pipeline()` produces a lightweight `Pipeline` from a job
- `AbstractPipelineBase` extracted to `pipeline/base.py` — shared recording machinery
- Serialization format bumped to v0.3 (backward-compatible load of v0.2)

Closes ENG-493

## Test plan
- [ ] `uv run pytest tests/test_core/nodes/` — new node hierarchy unit tests
- [ ] `uv run pytest tests/test_pipeline/` — full pipeline test suite
- [ ] `uv run pytest tests/` — complete test suite

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review Against Spec

### Spec coverage check

| Spec requirement | Covered by task |
|---|---|
| `Pipeline._persistent_node_map` contains only lightweight nodes | Tasks 5, 6 |
| `PipelineJob._persistent_node_map` contains only JobNodes | Task 6 |
| `SourceSpec` eliminated | Task 8 |
| `SourceNode` as user-facing input slot | Task 1 |
| `Pipeline.bind()` removed | Task 4 |
| `PipelineJob.from_pipeline()` classmethod | Task 4 |
| `PipelineJob.bind()` mutating | Task 4 |
| `PipelineJob.as_pipeline()` | Task 4 |
| `SourceJobNode._concrete` mutable for in-place update | Task 1 |
| Hash stability: `SourceNode.content_hash() == SourceSpec.content_hash()` | Task 1 (tests) |
| `pipeline_hash()` always schema-based for `SourceJobNode` | Task 1 (tests) |
| `FunctionNode.iter_data()` raises `PipelineJobRequiredError` | Task 2 |
| `FunctionJobNode.as_node()` returns `FunctionNode` | Task 2 |
| `OperatorNode.iter_data()` raises `PipelineJobRequiredError` | Task 3 |
| `OperatorJobNode.as_node()` returns `OperatorNode` | Task 3 |
| `AbstractPipelineBase` in `pipeline/base.py` | Task 5 |
| Serialization format v0.3, backward-compat v0.2 load | Task 7 |
| All tests updated | Tasks 1–8 |

### Notes on `_build_execution_graph()`

The current `PipelineJob._build_execution_graph()` creates `FunctionNode`/`OperatorNode` with DB attached from scratch. After Task 6, the `_persistent_node_map` already has `FunctionJobNode`/`OperatorJobNode`. The `_build_execution_graph()` method may need updating or removal — it may be replaced by `_distribute_databases()` + the new job node map. Check what calls it (orchestrators) and update those call sites.

### `tracker_manager` parameter in AbstractPipelineBase

`PipelineJob.__init__` currently does not accept `name` or `tracker_manager` via parent `__init__` in the same form. Ensure the `super().__init__` chains are correct after Task 5.
