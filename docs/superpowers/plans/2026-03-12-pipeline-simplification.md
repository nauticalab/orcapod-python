# Pipeline & Node Simplification Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify the pipeline architecture by merging redundant class hierarchies: GraphTracker → Pipeline, PersistentFunctionNode → FunctionNode, PersistentOperatorNode → OperatorNode, and refactor sources via a compositional builder pattern.

**Architecture:** Merge persistent node variants into their base classes with optional database params. Merge GraphTracker recording logic directly into Pipeline. Extract source enrichment logic into a `SourceStreamBuilder` class used by all table-backed sources (including ArrowTableSource), eliminating `_arrow_source` delegation boilerplate. Make source nodes first-class pipeline members.

**Tech Stack:** Python 3.12+, PyArrow, NetworkX, pytest, uv

---

## File Structure

### Source files modified/created:
- `src/orcapod/core/sources/stream_builder.py` — new: `SourceStreamBuilder`, `SourceStreamResult`
- `src/orcapod/core/sources/base.py` — add default stream delegation, default `identity_structure()`, `resolve_field()` → `NotImplementedError`, remove `computed_label()`
- `src/orcapod/core/sources/arrow_table_source.py` — use builder, remove enrichment logic and stream/identity methods
- `src/orcapod/core/sources/dict_source.py` — rewrite: use builder, remove delegation
- `src/orcapod/core/sources/data_frame_source.py` — rewrite: use builder, remove delegation
- `src/orcapod/core/sources/csv_source.py` — rewrite: use builder, remove delegation
- `src/orcapod/core/sources/delta_table_source.py` — rewrite: use builder, remove delegation
- `src/orcapod/core/sources/list_source.py` — rewrite: use builder, remove delegation (keep custom `identity_structure`)
- `src/orcapod/core/sources/cached_source.py` — remove `resolve_field()` delegation
- `src/orcapod/core/nodes/function_node.py` — merge PersistentFunctionNode in
- `src/orcapod/core/nodes/operator_node.py` — merge PersistentOperatorNode in
- `src/orcapod/core/nodes/__init__.py` — remove Persistent* exports
- `src/orcapod/__init__.py` — remove PersistentFunctionNode export
- `src/orcapod/core/tracker.py` — delete GraphTracker class
- `src/orcapod/pipeline/graph.py` — inherit from AutoRegisteringContextBasedTracker, inline GraphTracker, use attach_databases() in compile()
- `src/orcapod/core/sources/derived_source.py` — update type annotations
- `src/orcapod/pipeline/orchestrator.py` — update references

### Test files created:
- `tests/test_core/sources/test_stream_builder.py` — SourceStreamBuilder unit tests
- `tests/test_core/sources/test_source_builder_integration.py` — source builder integration + label tests
- `tests/test_core/function_pod/test_function_node_attach_db.py` — FunctionNode.attach_databases()
- `tests/test_core/operators/test_operator_node_attach_db.py` — OperatorNode.attach_databases()

### Test files updated (import renames):
- `tests/test_pipeline/test_pipeline.py`
- `tests/test_pipeline/test_node_descriptors.py`
- `tests/test_core/test_tracker.py`
- `tests/test_core/test_caching_integration.py`
- `tests/test_core/sources/test_derived_source.py`
- `tests/test_core/function_pod/test_function_pod_node.py`
- `tests/test_core/function_pod/test_function_pod_node_stream.py`
- `tests/test_core/function_pod/test_pipeline_hash_integration.py`
- `tests/test_core/operators/test_operator_node.py`
- `tests/test_channels/test_node_async_execute.py`
- `tests/test_channels/test_copilot_review_issues.py`
- `test-objective/` files referencing Persistent* or GraphTracker

---

## Chunk 1: Source Refactor (Compositional Builder) & Label Cleanup

**Strategy:** Extract the enrichment pipeline from `ArrowTableSource.__init__` into a
`SourceStreamBuilder` class. Add default stream delegation to `RootSource`. Refactor
`ArrowTableSource` to use the builder. Then refactor each delegating source to use
the builder directly (inheriting `RootSource`, no `_arrow_source`). Finally remove
`computed_label()` from `RootSource` and change `resolve_field()` to
`NotImplementedError` (deferred redesign).

**Note on hash stability:** `identity_structure()` includes `self.__class__.__name__`.
Sources that previously reported as `"ArrowTableSource"` (via delegation) will now
report their own class name. This is an acceptable one-time hash break (pre-v0.1.0).

**Note on CachedSource / DerivedSource:** These do NOT use `_arrow_source` delegation.
They override all stream methods themselves. Unaffected by this refactor except for
`resolve_field()` removal (CachedSource) and import updates (DerivedSource, later chunk).

### Task 1: Create SourceStreamBuilder

**Files:**
- Create: `src/orcapod/core/sources/stream_builder.py`
- Test: `tests/test_core/sources/test_stream_builder.py` (new)

The builder extracts enrichment logic from `ArrowTableSource.__init__` (lines 70-145)
into a reusable class. It takes a raw Arrow table + metadata and produces an enriched
`ArrowTableStream` plus artifacts (schema_hash, table_hash, source_id, tag_columns).

- [ ] **Step 1: Write the failing test**

Create `tests/test_core/sources/test_stream_builder.py`:

```python
"""Tests for SourceStreamBuilder."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.sources.stream_builder import SourceStreamBuilder, SourceStreamResult


class TestSourceStreamBuilder:
    @pytest.fixture
    def builder(self):
        from orcapod.contexts import resolve_context
        from orcapod.config import DEFAULT_CONFIG
        ctx = resolve_context(None)
        return SourceStreamBuilder(data_context=ctx, config=DEFAULT_CONFIG)

    def test_build_returns_source_stream_result(self, builder):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        result = builder.build(table, tag_columns=["id"])
        assert isinstance(result, SourceStreamResult)

    def test_build_stream_has_correct_row_count(self, builder):
        table = pa.table({"id": pa.array([1, 2, 3]), "x": pa.array([10, 20, 30])})
        result = builder.build(table, tag_columns=["id"])
        assert result.stream.as_table().num_rows == 3

    def test_build_source_id_defaults_to_table_hash(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"])
        assert result.source_id is not None
        assert len(result.source_id) > 0

    def test_build_source_id_explicit(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"], source_id="my_source")
        assert result.source_id == "my_source"

    def test_build_schema_hash_is_string(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"])
        assert isinstance(result.schema_hash, str)
        assert len(result.schema_hash) > 0

    def test_build_tag_columns_tuple(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"])
        assert result.tag_columns == ("id",)

    def test_build_validates_missing_tag_columns(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        with pytest.raises(ValueError, match="tag_columns not found"):
            builder.build(table, tag_columns=["nonexistent"])

    def test_build_validates_missing_record_id_column(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        with pytest.raises(ValueError, match="record_id_column"):
            builder.build(table, tag_columns=["id"], record_id_column="bad")

    def test_build_output_schema_has_tag_and_packet(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"])
        tag_schema, packet_schema = result.stream.output_schema()
        assert "id" in tag_schema
        assert "x" in packet_schema

    def test_build_with_record_id_column(self, builder):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        result = builder.build(
            table, tag_columns=["id"], record_id_column="id"
        )
        assert result.stream.as_table().num_rows == 2

    def test_build_drops_system_columns_from_input(self, builder):
        table = pa.table({
            "id": pa.array([1]),
            "x": pa.array([10]),
            "__system_col": pa.array(["sys"]),
        })
        result = builder.build(table, tag_columns=["id"])
        tag_schema, packet_schema = result.stream.output_schema()
        assert "__system_col" not in packet_schema
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_stream_builder.py -v`
Expected: FAIL — `stream_builder` module does not exist

- [ ] **Step 3: Implement SourceStreamBuilder**

Create `src/orcapod/core/sources/stream_builder.py`:

```python
"""Compositional builder for enriching raw Arrow tables into source streams.

Extracts the enrichment pipeline that was previously embedded in
``ArrowTableSource.__init__``: dropping system columns, validating tags,
computing schema/table hashes, adding source-info provenance, adding system
tag columns, and wrapping the result in an ``ArrowTableStream``.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.system_constants import constants
from orcapod.types import ContentHash
from orcapod.utils import arrow_data_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.config import Config
    from orcapod.contexts import DataContext
else:
    pa = LazyModule("pyarrow")


def _make_record_id(record_id_column: str | None, row_index: int, row: dict) -> str:
    """Build the record-ID token for a single row.

    When *record_id_column* is given the token is ``"{column}={value}"``,
    giving a stable, human-readable key that survives row reordering.
    When no column is specified the fallback is ``"row_{index}"``.
    """
    if record_id_column is not None:
        return f"{record_id_column}={row[record_id_column]}"
    return f"row_{row_index}"


@dataclass(frozen=True)
class SourceStreamResult:
    """Artifacts produced by ``SourceStreamBuilder.build()``."""

    stream: ArrowTableStream
    schema_hash: str
    table_hash: ContentHash
    source_id: str
    tag_columns: tuple[str, ...]
    system_tag_columns: tuple[str, ...]


class SourceStreamBuilder:
    """Builds an enriched ``ArrowTableStream`` from a raw Arrow table.

    Args:
        data_context: Provides type_converter, semantic_hasher, arrow_hasher.
        config: Orcapod config (controls hash character counts).
    """

    def __init__(self, data_context: "DataContext", config: "Config") -> None:
        self._data_context = data_context
        self._config = config

    def build(
        self,
        table: "pa.Table",
        tag_columns: Collection[str],
        source_id: str | None = None,
        record_id_column: str | None = None,
        system_tag_columns: Collection[str] = (),
    ) -> SourceStreamResult:
        """Run the full enrichment pipeline.

        Args:
            table: Raw Arrow table (system columns will be stripped).
            tag_columns: Column names forming the tag for each row.
            source_id: Canonical source name. Defaults to table hash.
            record_id_column: Column for stable record IDs in provenance.
            system_tag_columns: Additional system-level tag columns.

        Returns:
            SourceStreamResult with enriched stream and metadata.

        Raises:
            ValueError: If tag_columns or record_id_column are not in table.
        """
        tag_columns_tuple = tuple(tag_columns)
        system_tag_columns_tuple = tuple(system_tag_columns)

        # 1. Drop system columns from raw input.
        table = arrow_data_utils.drop_system_columns(table)

        # 2. Validate tag_columns.
        missing_tags = set(tag_columns_tuple) - set(table.column_names)
        if missing_tags:
            raise ValueError(
                f"tag_columns not found in table: {missing_tags}. "
                f"Available columns: {list(table.column_names)}"
            )

        # 3. Validate record_id_column.
        if record_id_column is not None and record_id_column not in table.column_names:
            raise ValueError(
                f"record_id_column {record_id_column!r} not found in table columns: "
                f"{table.column_names}"
            )

        # 4. Compute schema hash from tag/packet python schemas.
        non_sys = arrow_data_utils.drop_system_columns(table)
        tag_schema = non_sys.select(list(tag_columns_tuple)).schema
        packet_schema = non_sys.drop(list(tag_columns_tuple)).schema
        tag_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            tag_schema
        )
        packet_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            packet_schema
        )
        schema_hash = self._data_context.semantic_hasher.hash_object(
            (tag_python, packet_python)
        ).to_hex(char_count=self._config.schema_hash_n_char)

        # 5. Compute table hash for data identity.
        table_hash = self._data_context.arrow_hasher.hash_table(table)

        # 6. Default source_id to table hash.
        if source_id is None:
            source_id = table_hash.to_hex(
                char_count=self._config.path_hash_n_char
            )

        # 7. Build per-row source-info strings.
        rows_as_dicts = table.to_pylist()
        source_info = [
            f"{source_id}{constants.BLOCK_SEPARATOR}"
            f"{_make_record_id(record_id_column, i, row)}"
            for i, row in enumerate(rows_as_dicts)
        ]

        # 8. Add source-info provenance columns.
        table = arrow_data_utils.add_source_info(
            table, source_info, exclude_columns=tag_columns_tuple
        )

        # 9. Add system tag columns.
        record_id_values = [
            _make_record_id(record_id_column, i, row)
            for i, row in enumerate(rows_as_dicts)
        ]
        table = arrow_data_utils.add_system_tag_columns(
            table,
            schema_hash,
            source_id,
            record_id_values,
        )

        # 10. Wrap in ArrowTableStream.
        stream = ArrowTableStream(
            table=table,
            tag_columns=tag_columns_tuple,
            system_tag_columns=system_tag_columns_tuple,
        )

        return SourceStreamResult(
            stream=stream,
            schema_hash=schema_hash,
            table_hash=table_hash,
            source_id=source_id,
            tag_columns=tag_columns_tuple,
            system_tag_columns=system_tag_columns_tuple,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core/sources/test_stream_builder.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/sources/stream_builder.py tests/test_core/sources/test_stream_builder.py
git commit -m "feat(sources): add SourceStreamBuilder for compositional source enrichment"
```

---

### Task 2: Update RootSource Defaults and Refactor ArrowTableSource

**Files:**
- Modify: `src/orcapod/core/sources/base.py`
- Modify: `src/orcapod/core/sources/arrow_table_source.py`
- Test: `tests/test_core/sources/test_stream_builder.py` (append)

**Context:** Add default stream delegation methods and default `identity_structure()`
to `RootSource`. Change `resolve_field()` to raise `NotImplementedError`. Then
refactor `ArrowTableSource` to use the builder (removing inline enrichment logic).
CachedSource and DerivedSource already override all stream methods, so the defaults
do not affect them.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/sources/test_stream_builder.py`:

```python
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.base import RootSource


class TestArrowTableSourceUsesBuilder:
    def test_arrow_table_source_works(self):
        """ArrowTableSource should use SourceStreamBuilder internally."""
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        src = ArrowTableSource(table=table, tag_columns=["id"])
        assert src.as_table().num_rows == 2
        tag_schema, packet_schema = src.output_schema()
        assert "id" in tag_schema
        assert "x" in packet_schema

    def test_arrow_table_source_has_stream_attr(self):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        src = ArrowTableSource(table=table, tag_columns=["id"])
        assert hasattr(src, "_stream")

    def test_arrow_table_source_identity_uses_class_name(self):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        src = ArrowTableSource(table=table, tag_columns=["id"])
        identity = src.identity_structure()
        assert identity[0] == "ArrowTableSource"

    def test_resolve_field_raises_not_implemented(self):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        src = ArrowTableSource(table=table, tag_columns=["id"])
        with pytest.raises(NotImplementedError):
            src.resolve_field("row_0", "x")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_stream_builder.py::TestArrowTableSourceUsesBuilder -v`
Expected: FAIL — ArrowTableSource still has inline enrichment, resolve_field works

- [ ] **Step 3: Update RootSource with defaults**

In `src/orcapod/core/sources/base.py`:

1. Add default `identity_structure()`:
```python
    def identity_structure(self) -> Any:
        return (self.__class__.__name__, self.output_schema(), self.source_id)
```

2. Add default stream delegation methods:
```python
    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._stream.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._stream.keys(columns=columns, all_info=all_info)

    def iter_packets(self):
        return self._stream.iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._stream.as_table(columns=columns, all_info=all_info)
```

3. Change `resolve_field()` to raise `NotImplementedError`:
```python
    def resolve_field(self, record_id: str, field_name: str) -> Any:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement resolve_field. "
            f"Cannot resolve field {field_name!r} for record {record_id!r}."
        )
```

Add necessary imports: `ColumnConfig`, `Schema` from `orcapod.types`.

- [ ] **Step 4: Refactor ArrowTableSource to use SourceStreamBuilder**

Replace `src/orcapod/core/sources/arrow_table_source.py`. The new version:
- Calls `super().__init__()` first (sets up data_context)
- Creates `SourceStreamBuilder`, calls `build()`
- Stores `_stream`, `_schema_hash`, `_table_hash`, `_tag_columns`,
  `_system_tag_columns`, `_record_id_column` from the result
- Removes inline enrichment logic (steps 1-10 moved to builder)
- Removes `output_schema()`, `keys()`, `iter_packets()`, `as_table()`,
  `identity_structure()` (inherited from RootSource defaults)
- Removes `resolve_field()` (inherited NotImplementedError from RootSource)
- Keeps: `to_config()`, `from_config()`, `table` property

```python
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any, Self

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder

if TYPE_CHECKING:
    import pyarrow as pa


class ArrowTableSource(RootSource):
    """A source backed by an in-memory PyArrow Table.

    Uses ``SourceStreamBuilder`` to strip system columns, add per-row
    source-info provenance columns and a system tag column encoding the
    schema hash, then wraps the result in an ``ArrowTableStream``.
    """

    def __init__(
        self,
        table: "pa.Table",
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            table,
            tag_columns=tag_columns,
            source_id=self._source_id,
            record_id_column=record_id_column,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._schema_hash = result.schema_hash
        self._table_hash = result.table_hash
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        self._record_id_column = record_id_column

        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self) -> dict[str, Any]:
        """Serialize metadata-only config (in-memory table is not serializable)."""
        return {
            "source_type": "arrow_table",
            "tag_columns": list(self._tag_columns),
            "source_id": self.source_id,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Self:
        """Not supported — ArrowTableSource cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "ArrowTableSource cannot be reconstructed from config — "
            "the in-memory Arrow table is not serializable."
        )

    @property
    def table(self) -> "pa.Table":
        return self._stream.as_table(
            columns={"source": True, "system_tags": True}
        )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_core/sources/ -v --tb=short`
Expected: Most pass. Some tests may fail due to:
- `resolve_field` now raises `NotImplementedError` instead of `FieldNotResolvableError`
- Tests that directly accessed `ArrowTableSource._data_table` — update to use `table` property
- Hash values may change (no `_make_record_id` import from `arrow_table_source`)

Fix any failures.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/sources/base.py src/orcapod/core/sources/arrow_table_source.py tests/test_core/sources/test_stream_builder.py
git commit -m "refactor(sources): ArrowTableSource uses SourceStreamBuilder, RootSource gains defaults"
```

---

### Task 3: Refactor DictSource and DataFrameSource to Use Builder

**Files:**
- Modify: `src/orcapod/core/sources/dict_source.py`
- Modify: `src/orcapod/core/sources/data_frame_source.py`
- Test: `tests/test_core/sources/test_source_builder_integration.py` (new)

**Context:** Both sources currently create an internal `_arrow_source` and delegate ~7
methods. Replace with: `super().__init__()` → convert data → builder → store `_stream`.
All stream/identity methods inherited from `RootSource` defaults.

- [ ] **Step 1: Write the failing test**

Create `tests/test_core/sources/test_source_builder_integration.py`:

```python
"""Tests that sources use SourceStreamBuilder (no _arrow_source delegation)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.dict_source import DictSource
from orcapod.core.sources.data_frame_source import DataFrameSource


class TestDictSourceBuilder:
    def test_no_arrow_source_attr(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        assert hasattr(src, "_stream")

    def test_iter_packets(self):
        src = DictSource(
            data=[{"id": 1, "x": 10}, {"id": 2, "x": 20}],
            tag_columns=["id"],
        )
        assert len(list(src.iter_packets())) == 2

    def test_output_schema(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        tag_schema, packet_schema = src.output_schema()
        assert "id" in tag_schema
        assert "x" in packet_schema

    def test_to_config(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        config = src.to_config()
        assert config["source_type"] == "dict"
        assert config["tag_columns"] == ["id"]

    def test_identity_uses_class_name(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        identity = src.identity_structure()
        assert identity[0] == "DictSource"

    def test_source_id_defaults(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        assert src.source_id is not None


class TestDataFrameSourceBuilder:
    def test_no_arrow_source_attr(self):
        src = DataFrameSource(
            data={"id": [1, 2], "x": [10, 20]}, tag_columns=["id"]
        )
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self):
        src = DataFrameSource(
            data={"id": [1, 2], "x": [10, 20]}, tag_columns=["id"]
        )
        assert hasattr(src, "_stream")

    def test_iter_packets(self):
        src = DataFrameSource(
            data={"id": [1, 2], "x": [10, 20]}, tag_columns=["id"]
        )
        assert len(list(src.iter_packets())) == 2

    def test_identity_uses_class_name(self):
        src = DataFrameSource(
            data={"id": [1], "x": [10]}, tag_columns=["id"]
        )
        identity = src.identity_structure()
        assert identity[0] == "DataFrameSource"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_source_builder_integration.py -v`
Expected: FAIL — sources still have `_arrow_source`, identity reports "ArrowTableSource"

- [ ] **Step 3: Rewrite DictSource**

Replace `src/orcapod/core/sources/dict_source.py`:

```python
from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.types import DataValue, SchemaLike


class DictSource(RootSource):
    """A source backed by a collection of Python dictionaries.

    Each dict becomes one (tag, packet) pair in the stream. The dicts are
    converted to an Arrow table via the data-context type converter, then
    enriched by ``SourceStreamBuilder`` (source-info, schema-hash, system tags).
    """

    def __init__(
        self,
        data: Collection[Mapping[str, DataValue]],
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        data_schema: SchemaLike | None = None,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id=source_id, **kwargs)

        arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
            [dict(row) for row in data],
            python_schema=data_schema,
        )

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=tag_columns,
            source_id=self._source_id,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self) -> dict[str, Any]:
        """Serialize metadata-only config (data is not serializable)."""
        return {
            "source_type": "dict",
            "tag_columns": list(self._tag_columns),
            "source_id": self.source_id,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DictSource":
        """Not supported — DictSource data cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "DictSource cannot be reconstructed from config — "
            "original data is not serializable."
        )
```

- [ ] **Step 4: Rewrite DataFrameSource**

Replace `src/orcapod/core/sources/data_frame_source.py`:

```python
from __future__ import annotations

import logging
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.utils import polars_data_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    from polars._typing import FrameInitTypes
else:
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)


class DataFrameSource(RootSource):
    """A source backed by a Polars DataFrame (or any Polars-compatible data).

    The DataFrame is converted to an Arrow table and enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system tags).
    """

    def __init__(
        self,
        data: "FrameInitTypes",
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id=source_id, **kwargs)

        df = pl.DataFrame(data)

        # Convert any Object-dtype columns to Arrow-compatible types.
        object_columns = [c for c in df.columns if df[c].dtype == pl.Object]
        if object_columns:
            logger.info(
                f"Converting {len(object_columns)} object column(s) to Arrow format"
            )
            sub_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                df.select(object_columns).to_dicts()
            )
            df = df.with_columns([pl.from_arrow(c) for c in sub_table])

        if isinstance(tag_columns, str):
            tag_columns = [tag_columns]
        tag_columns = list(tag_columns)

        df = polars_data_utils.drop_system_columns(df)

        missing = set(tag_columns) - set(df.columns)
        if missing:
            raise ValueError(f"TagProtocol column(s) not found in data: {missing}")

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            df.to_arrow(),
            tag_columns=tag_columns,
            source_id=self._source_id,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self) -> dict[str, Any]:
        """Serialize metadata-only config (DataFrame is not serializable)."""
        return {
            "source_type": "data_frame",
            "tag_columns": list(self._tag_columns),
            "source_id": self.source_id,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DataFrameSource":
        """Not supported — DataFrameSource cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "DataFrameSource cannot be reconstructed from config — "
            "the original DataFrame is not serializable."
        )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_core/sources/ -v --tb=short`
Expected: PASS. Fix any failures from hash changes or `_arrow_source` references.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/sources/dict_source.py src/orcapod/core/sources/data_frame_source.py tests/test_core/sources/test_source_builder_integration.py
git commit -m "refactor(sources): DictSource and DataFrameSource use SourceStreamBuilder"
```

---

### Task 4: Refactor CSVSource and DeltaTableSource to Use Builder

**Files:**
- Modify: `src/orcapod/core/sources/csv_source.py`
- Modify: `src/orcapod/core/sources/delta_table_source.py`
- Test: `tests/test_core/sources/test_source_builder_integration.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/sources/test_source_builder_integration.py`:

```python
from orcapod.core.sources.csv_source import CSVSource
from orcapod.core.sources.delta_table_source import DeltaTableSource


class TestCSVSourceBuilder:
    def test_no_arrow_source_attr(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n2,20\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n2,20\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert hasattr(src, "_stream")

    def test_iter_packets(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n2,20\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert len(list(src.iter_packets())) == 2

    def test_round_trip_config(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n2,20\n")
        src = CSVSource(
            file_path=str(csv_file), tag_columns=["id"], record_id_column="id"
        )
        config = src.to_config()
        assert config["source_type"] == "csv"
        assert config["file_path"] == str(csv_file)
        src2 = CSVSource.from_config(config)
        assert src2.source_id == src.source_id

    def test_identity_uses_class_name(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert src.identity_structure()[0] == "CSVSource"


class TestDeltaTableSourceBuilder:
    @pytest.fixture
    def delta_path(self, tmp_path):
        from deltalake import write_deltalake
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        path = str(tmp_path / "delta_test")
        write_deltalake(path, table)
        return path

    def test_no_arrow_source_attr(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        assert hasattr(src, "_stream")

    def test_round_trip_config(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        config = src.to_config()
        assert config["source_type"] == "delta_table"
        src2 = DeltaTableSource.from_config(config)
        assert src2.source_id == src.source_id

    def test_identity_uses_class_name(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        assert src.identity_structure()[0] == "DeltaTableSource"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_source_builder_integration.py::TestCSVSourceBuilder tests/test_core/sources/test_source_builder_integration.py::TestDeltaTableSourceBuilder -v`
Expected: FAIL

- [ ] **Step 3: Rewrite CSVSource**

Replace `src/orcapod/core/sources/csv_source.py`:

```python
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class CSVSource(RootSource):
    """A source backed by a CSV file.

    The file is read once at construction time using PyArrow's CSV reader,
    converted to an Arrow table, and enriched by ``SourceStreamBuilder``
    (source-info, schema-hash, system tags).
    """

    def __init__(
        self,
        file_path: str,
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        import pyarrow.csv as pa_csv

        if source_id is None:
            source_id = file_path
        super().__init__(source_id=source_id, **kwargs)

        self._file_path = file_path
        table: pa.Table = pa_csv.read_csv(file_path)

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            table,
            tag_columns=tag_columns,
            source_id=self._source_id,
            record_id_column=record_id_column,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        self._record_id_column = record_id_column
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self) -> dict[str, Any]:
        """Serialize this source's configuration to a JSON-compatible dict."""
        return {
            "source_type": "csv",
            "file_path": self._file_path,
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_column": self._record_id_column,
            "source_id": self.source_id,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "CSVSource":
        """Reconstruct a CSVSource from a config dict."""
        return cls(
            file_path=config["file_path"],
            tag_columns=config.get("tag_columns", ()),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
        )
```

- [ ] **Step 4: Rewrite DeltaTableSource**

Replace `src/orcapod/core/sources/delta_table_source.py`:

```python
from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.types import PathLike
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class DeltaTableSource(RootSource):
    """A source backed by a Delta Lake table.

    The table is read once at construction time using ``deltalake``'s
    PyArrow integration. The resulting Arrow table is enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system tags).
    """

    def __init__(
        self,
        delta_table_path: PathLike,
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        from deltalake import DeltaTable
        from deltalake.exceptions import TableNotFoundError

        resolved = Path(delta_table_path).resolve()

        if source_id is None:
            source_id = resolved.name
        super().__init__(source_id=source_id, **kwargs)

        self._delta_table_path = resolved

        try:
            delta_table = DeltaTable(str(resolved))
        except TableNotFoundError:
            raise ValueError(f"Delta table not found at {resolved}")

        table: pa.Table = delta_table.to_pyarrow_dataset(
            as_large_types=True
        ).to_table()

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            table,
            tag_columns=tag_columns,
            source_id=self._source_id,
            record_id_column=record_id_column,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        self._record_id_column = record_id_column
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self) -> dict[str, Any]:
        """Serialize this source's configuration to a JSON-compatible dict."""
        return {
            "source_type": "delta_table",
            "delta_table_path": str(self._delta_table_path),
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_column": self._record_id_column,
            "source_id": self.source_id,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DeltaTableSource":
        """Reconstruct a DeltaTableSource from a config dict."""
        return cls(
            delta_table_path=config["delta_table_path"],
            tag_columns=config.get("tag_columns", ()),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
        )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_core/sources/ -v --tb=short`
Expected: PASS. Fix any hash or import failures.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/sources/csv_source.py src/orcapod/core/sources/delta_table_source.py tests/test_core/sources/test_source_builder_integration.py
git commit -m "refactor(sources): CSVSource and DeltaTableSource use SourceStreamBuilder"
```

---

### Task 5: Refactor ListSource to Use Builder

**Files:**
- Modify: `src/orcapod/core/sources/list_source.py`
- Test: `tests/test_core/sources/test_source_builder_integration.py` (append)

**Note:** ListSource keeps its custom `identity_structure()` (includes tag function
hash) and `_hash_tag_function()`. Since `super().__init__()` runs first,
`self.data_context` is available for the `"content"` hash mode.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/sources/test_source_builder_integration.py`:

```python
from orcapod.core.sources.list_source import ListSource


class TestListSourceBuilder:
    def test_no_arrow_source_attr(self):
        src = ListSource(name="val", data=[1, 2, 3])
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self):
        src = ListSource(name="val", data=[1, 2, 3])
        assert hasattr(src, "_stream")

    def test_iter_packets(self):
        src = ListSource(name="val", data=[1, 2, 3])
        assert len(list(src.iter_packets())) == 3

    def test_custom_identity_structure(self):
        src = ListSource(name="val", data=[1, 2, 3])
        identity = src.identity_structure()
        assert identity[0] == "ListSource"
        assert identity[1] == "val"
        assert identity[2] == (1, 2, 3)
        assert len(identity) == 4  # includes tag_function_hash

    def test_with_tag_function(self):
        src = ListSource(
            name="val",
            data=[10, 20],
            tag_function=lambda e, i: {"idx": i, "label": f"item_{i}"},
            expected_tag_keys=["idx", "label"],
        )
        results = list(src.iter_packets())
        assert len(results) == 2

    def test_source_id_defaults(self):
        src = ListSource(name="val", data=[1])
        assert src.source_id is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_source_builder_integration.py::TestListSourceBuilder -v`
Expected: FAIL — ListSource still has `_arrow_source`

- [ ] **Step 3: Rewrite ListSource**

Replace `src/orcapod/core/sources/list_source.py`:

```python
from __future__ import annotations

from collections.abc import Callable, Collection
from typing import TYPE_CHECKING, Any, Literal

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.protocols.core_protocols import TagProtocol

if TYPE_CHECKING:
    import pyarrow as pa


class ListSource(RootSource):
    """A source backed by a Python list.

    Each element in the list becomes one (tag, packet) pair. The element is
    stored as the packet under ``name``; the tag is either the element's index
    (default) or the dict returned by ``tag_function(element, index)``.
    """

    @staticmethod
    def _default_tag(element: Any, idx: int) -> dict[str, Any]:
        return {"element_index": idx}

    def __init__(
        self,
        name: str,
        data: list[Any],
        tag_function: Callable[[Any, int], dict[str, Any] | TagProtocol] | None = None,
        expected_tag_keys: Collection[str] | None = None,
        tag_function_hash_mode: Literal["content", "signature", "name"] = "name",
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id=source_id, **kwargs)

        self.name = name
        self._elements = list(data)
        self._tag_function_hash_mode = tag_function_hash_mode

        if tag_function is None:
            tag_function = self.__class__._default_tag
            if expected_tag_keys is None:
                expected_tag_keys = ["element_index"]

        self._tag_function = tag_function
        self._expected_tag_keys = (
            tuple(expected_tag_keys) if expected_tag_keys is not None else None
        )

        # Hash the tag function for identity purposes.
        self._tag_function_hash = self._hash_tag_function()

        # Build rows: each row is tag_fields merged with {name: element}.
        rows = []
        for idx, element in enumerate(self._elements):
            tag_fields = tag_function(element, idx)
            if hasattr(tag_fields, "as_dict"):
                tag_fields = tag_fields.as_dict()
            row = dict(tag_fields)
            row[name] = element
            rows.append(row)

        tag_columns = (
            list(self._expected_tag_keys)
            if self._expected_tag_keys is not None
            else [k for k in (rows[0].keys() if rows else []) if k != name]
        )

        arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(rows)

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=tag_columns,
            source_id=self._source_id,
        )

        self._stream = result.stream
        if self._source_id is None:
            self._source_id = result.source_id

    def _hash_tag_function(self) -> str:
        """Produce a stable hash string for the tag function."""
        if self._tag_function_hash_mode == "name":
            fn = self._tag_function
            return f"{fn.__module__}.{fn.__qualname__}"
        elif self._tag_function_hash_mode == "signature":
            import inspect
            return str(inspect.signature(self._tag_function))
        else:  # "content"
            import inspect
            src = inspect.getsource(self._tag_function)
            return self.data_context.semantic_hasher.hash_object(src).to_hex()

    def identity_structure(self) -> Any:
        try:
            elements_repr: Any = tuple(self._elements)
        except TypeError:
            elements_repr = tuple(str(e) for e in self._elements)
        return (
            self.__class__.__name__,
            self.name,
            elements_repr,
            self._tag_function_hash,
        )

    def to_config(self) -> dict[str, Any]:
        """Serialize metadata-only config (data is not serializable)."""
        return {
            "source_type": "list",
            "name": self.name,
            "source_id": self.source_id,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "ListSource":
        """Not supported — ListSource data cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "ListSource cannot be reconstructed from config — "
            "original list data is not serializable."
        )
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_core/sources/ -v --tb=short`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/sources/list_source.py tests/test_core/sources/test_source_builder_integration.py
git commit -m "refactor(sources): ListSource uses SourceStreamBuilder"
```

---

### Task 6: Remove `computed_label()` from RootSource, Remove CachedSource resolve_field, Full Verification

**Files:**
- Modify: `src/orcapod/core/sources/base.py`
- Modify: `src/orcapod/core/sources/cached_source.py`
- Test: `tests/test_core/sources/test_source_builder_integration.py` (append)

**Context:** After Tasks 1-5, all delegating sources use the builder. The only
remaining `computed_label()` is on `RootSource` (returns `self._source_id`). Removing
it causes sources without an explicit label to fall back to `self.__class__.__name__`
via `LabelableMixin.label`. Also remove `resolve_field` delegation from CachedSource
(it inherits `NotImplementedError` from RootSource).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/sources/test_source_builder_integration.py`:

```python
class TestSourceLabelDefaults:
    def test_dict_source_label_defaults_to_class_name(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        assert src.label == "DictSource"

    def test_dict_source_explicit_label_preserved(self):
        src = DictSource(
            data=[{"id": 1, "x": 10}],
            tag_columns=["id"],
            label="my_source",
        )
        assert src.label == "my_source"

    def test_arrow_table_source_label_defaults_to_class_name(self):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        src = ArrowTableSource(table=table, tag_columns=["id"])
        assert src.label == "ArrowTableSource"

    def test_list_source_label_defaults_to_class_name(self):
        src = ListSource(name="val", data=[1, 2])
        assert src.label == "ListSource"

    def test_csv_source_label_defaults_to_class_name(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert src.label == "CSVSource"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_source_builder_integration.py::TestSourceLabelDefaults -v`
Expected: FAIL — labels currently return source_id, not class name

- [ ] **Step 3: Remove computed_label() from RootSource**

In `src/orcapod/core/sources/base.py`, delete the `computed_label()` method:

```python
# DELETE these lines:
    def computed_label(self) -> str | None:
        """Return the source_id as the label."""
        return self._source_id
```

- [ ] **Step 4: Remove resolve_field delegation from CachedSource**

In `src/orcapod/core/sources/cached_source.py`, delete the `resolve_field()` method
(it now inherits `NotImplementedError` from RootSource):

```python
# DELETE these lines:
    def resolve_field(self, record_id: str, field_name: str) -> Any:
        return self._source.resolve_field(record_id, field_name)
```

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest tests/ -v --tb=short`

Fix any failures. Common issues:
- Tests that assert source labels equal source_id → update to class name
- Tests that call `resolve_field` expecting data → expect `NotImplementedError`
- Tests in `test_pipeline_hash_integration.py` with exact hash values → update
- Tests that accessed `_arrow_source` → should have been caught in Tasks 3-5

- [ ] **Step 6: Commit**

```bash
git add -u
git commit -m "refactor(sources): remove computed_label() from RootSource, resolve_field → NotImplementedError"
```

---

## Chunk 2: Merge PersistentFunctionNode into FunctionNode

### Task 7: Merge PersistentFunctionNode into FunctionNode

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Test: `tests/test_core/function_pod/test_function_node_attach_db.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_core/function_pod/test_function_node_attach_db.py`:

```python
"""Tests for FunctionNode with optional database backing."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase


def _make_pod():
    def double(x: int) -> int:
        return x * 2

    return FunctionPod(
        packet_function=PythonPacketFunction(double, output_keys="result"),
    )


def _make_stream(n=3):
    table = pa.table({
        "id": pa.array(list(range(n)), type=pa.int64()),
        "x": pa.array(list(range(n)), type=pa.int64()),
    })
    return ArrowTableStream(table, tag_columns=["id"])


class TestFunctionNodeWithoutDatabase:
    def test_construction_without_database(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        assert node._pipeline_database is None

    def test_iter_packets_without_database(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream(n=3))
        results = list(node.iter_packets())
        assert len(results) == 3
        assert results[0][1]["result"] == 0

    def test_get_all_records_without_database_returns_none(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        assert node.get_all_records() is None

    def test_as_source_without_database_raises(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        with pytest.raises(RuntimeError):
            node.as_source()


class TestFunctionNodeAttachDatabases:
    def test_attach_databases_sets_pipeline_db(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert node._pipeline_database is db

    def test_attach_databases_wraps_packet_function(self):
        from orcapod.core.packet_function import CachedPacketFunction

        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert isinstance(node._packet_function, CachedPacketFunction)

    def test_attach_databases_clears_caches(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        list(node.iter_packets())  # populate cache
        assert len(node._cached_output_packets) > 0
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert len(node._cached_output_packets) == 0

    def test_attach_databases_computes_pipeline_path(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert node.pipeline_path is not None
        assert len(node.pipeline_path) > 0

    def test_double_attach_does_not_double_wrap(self):
        from orcapod.core.packet_function import CachedPacketFunction

        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert isinstance(node._packet_function, CachedPacketFunction)
        # Second attach should not double-wrap
        node.attach_databases(pipeline_database=db, result_database=db)
        assert isinstance(node._packet_function, CachedPacketFunction)
        assert not isinstance(
            node._packet_function._packet_function, CachedPacketFunction
        )

    def test_iter_packets_after_attach_works(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream(n=2))
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        results = list(node.iter_packets())
        assert len(results) == 2


class TestFunctionNodeWithDatabase:
    def test_construction_with_database(self):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=_make_pod(),
            input_stream=_make_stream(),
            pipeline_database=db,
            result_database=db,
        )
        assert node._pipeline_database is db

    def test_pipeline_path_with_database(self):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=_make_pod(),
            input_stream=_make_stream(),
            pipeline_database=db,
            result_database=db,
        )
        assert len(node.pipeline_path) > 0

    def test_iter_packets_with_database(self):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=_make_pod(),
            input_stream=_make_stream(n=3),
            pipeline_database=db,
            result_database=db,
        )
        results = list(node.iter_packets())
        assert len(results) == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/function_pod/test_function_node_attach_db.py -v`
Expected: FAIL — `_pipeline_database` not on FunctionNode, `attach_databases` not found

- [ ] **Step 3: Merge PersistentFunctionNode into FunctionNode**

In `src/orcapod/core/nodes/function_node.py`:

1. Add optional DB params to `FunctionNode.__init__`:
   - `pipeline_database: ArrowDatabaseProtocol | None = None`
   - `result_database: ArrowDatabaseProtocol | None = None`
   - `result_path_prefix: tuple[str, ...] | None = None`
   - `pipeline_path_prefix: tuple[str, ...] | None = None`

2. When `pipeline_database is not None` in `__init__`, wrap packet function in `CachedPacketFunction` and compute `_pipeline_node_hash` and `_output_schema_hash` (same as old `PersistentFunctionNode.__init__`).

3. When `pipeline_database is None` in `__init__`, set `_pipeline_database = None` and skip wrapping.

4. Add `attach_databases()` method:
   ```python
   def attach_databases(self, pipeline_database, result_database,
                        result_path_prefix=None, pipeline_path_prefix=None):
       # 1. Set databases
       # 2. Clear all caches (hash, output, iterator)
       # 3. Guard against double-wrapping CachedPacketFunction
       # 4. Wrap packet function
       # 5. Compute pipeline_node_hash and output_schema_hash
   ```

5. Move all `PersistentFunctionNode` methods into `FunctionNode` with `if self._pipeline_database is None` guards:
   - `from_descriptor()` — classmethod
   - `load_status` — property
   - `content_hash()`, `pipeline_hash()` — read-only mode overrides
   - `output_schema()`, `keys()` — read-only mode overrides
   - `pipeline_path` — property
   - `process_packet()` — with DB recording
   - `async_process_packet()` — with DB recording
   - `add_pipeline_record()` — no-op when no DB
   - `get_all_records()` — returns None when no DB
   - `iter_packets()` — two-phase when DB, simple when not
   - `async_execute()` — two-phase when DB, simple when not
   - `run()` — eager iteration
   - `as_source()` — raises when no DB

6. Delete `PersistentFunctionNode` class entirely.

**Key `iter_packets()` logic:**
```python
def iter_packets(self):
    if self.is_stale:
        self.clear_cache()
    self._ensure_iterator()

    if self._pipeline_database is not None:
        # Two-phase: yield from DB first, then compute missing
        yield from self._iter_packets_persistent()
    else:
        # Simple: compute all
        if self._cached_input_iterator is not None:
            if _executor_supports_concurrent(self._packet_function):
                yield from self._iter_packets_concurrent(self._cached_input_iterator)
            else:
                yield from self._iter_packets_sequential(self._cached_input_iterator)
        else:
            for i in range(len(self._cached_output_packets)):
                tag, packet = self._cached_output_packets[i]
                if packet is not None:
                    yield tag, packet
```

**Key `process_packet()` logic:**
```python
def process_packet(self, tag, packet, skip_cache_lookup=False, skip_cache_insert=False):
    if self._pipeline_database is not None:
        # Persistent mode: use CachedPacketFunction + pipeline record
        output_packet = self._packet_function.call(
            packet, skip_cache_lookup=skip_cache_lookup,
            skip_cache_insert=skip_cache_insert,
        )
        if output_packet is not None:
            result_computed = bool(output_packet.get_meta_value(
                self._packet_function.RESULT_COMPUTED_FLAG, False
            ))
            self.add_pipeline_record(tag, packet,
                packet_record_id=output_packet.datagram_id,
                computed=result_computed)
        return tag, output_packet
    else:
        # In-memory mode: delegate to function pod
        return self._function_pod.process_packet(tag, packet)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core/function_pod/test_function_node_attach_db.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/function_pod/test_function_node_attach_db.py
git commit -m "refactor(nodes): merge PersistentFunctionNode into FunctionNode"
```

---

### Task 8: Update All PersistentFunctionNode Imports

**Files:**
- Modify: `src/orcapod/core/nodes/__init__.py` — remove PersistentFunctionNode export
- Modify: `src/orcapod/__init__.py` — remove PersistentFunctionNode export
- Modify: `src/orcapod/pipeline/graph.py` — update all references
- Modify: `src/orcapod/pipeline/orchestrator.py` — update references
- Modify: `src/orcapod/core/sources/derived_source.py` — update type annotation
- Modify: All test files that import PersistentFunctionNode

- [ ] **Step 1: Update `src/orcapod/core/nodes/__init__.py`**

```python
from typing import TypeAlias

from .function_node import FunctionNode
from .operator_node import OperatorNode, PersistentOperatorNode
from .source_node import SourceNode

GraphNode: TypeAlias = SourceNode | FunctionNode | OperatorNode

__all__ = [
    "FunctionNode",
    "GraphNode",
    "OperatorNode",
    "PersistentOperatorNode",
    "SourceNode",
]
```

- [ ] **Step 2: Update `src/orcapod/__init__.py`**

Remove `PersistentFunctionNode` from imports and `__all__`.

- [ ] **Step 3: Update `src/orcapod/pipeline/graph.py`**

Replace all `PersistentFunctionNode` → `FunctionNode`:
- Import statement (line 11)
- `compile()` method — instead of creating `PersistentFunctionNode`, the node is already a `FunctionNode`; call `node.attach_databases()`
- `save()` method — isinstance checks
- `_build_function_descriptor()` — type annotation
- `_load_function_node()` — return type, construction
- `_apply_execution_engine()` — isinstance check
- `load()` method — type annotations

- [ ] **Step 4: Update `src/orcapod/core/sources/derived_source.py`**

Replace `PersistentFunctionNode | PersistentOperatorNode` → `FunctionNode | PersistentOperatorNode` (OperatorNode will be updated in Task 10).

- [ ] **Step 5: Update all test files**

Search and replace `PersistentFunctionNode` → `FunctionNode` in:
- `tests/test_pipeline/test_pipeline.py`
- `tests/test_pipeline/test_node_descriptors.py`
- `tests/test_core/test_tracker.py`
- `tests/test_core/test_caching_integration.py`
- `tests/test_core/sources/test_derived_source.py`
- `tests/test_core/function_pod/test_function_pod_node.py`
- `tests/test_core/function_pod/test_function_pod_node_stream.py`
- `tests/test_core/function_pod/test_pipeline_hash_integration.py`
- `tests/test_channels/test_node_async_execute.py`
- `tests/test_channels/test_copilot_review_issues.py`
- `test-objective/` files

Note: Tests that construct `PersistentFunctionNode(... pipeline_database=db ...)` now construct `FunctionNode(... pipeline_database=db ...)` — same arguments, just different class name.

- [ ] **Step 6: Run full test suite**

Run: `uv run pytest tests/ -v --tb=short`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add -u
git commit -m "refactor(nodes): update all PersistentFunctionNode imports to FunctionNode"
```

---

## Chunk 3: Merge PersistentOperatorNode, Merge GraphTracker, Pipeline Improvements

### Task 9: Merge PersistentOperatorNode into OperatorNode

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Test: `tests/test_core/operators/test_operator_node_attach_db.py` (new)

Same pattern as FunctionNode merge.

- [ ] **Step 1: Write the failing test**

Create `tests/test_core/operators/test_operator_node_attach_db.py`:

```python
"""Tests for OperatorNode with optional database backing."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes import OperatorNode
from orcapod.core.operators.join import Join
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase


def _make_stream(name="x", n=3):
    return ArrowTableStream(
        pa.table({
            "id": pa.array(list(range(n)), type=pa.int64()),
            name: pa.array(list(range(n)), type=pa.int64()),
        }),
        tag_columns=["id"],
    )


class TestOperatorNodeWithoutDatabase:
    def test_construction_without_database(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        assert node._pipeline_database is None

    def test_iter_packets_without_database(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        results = list(node.iter_packets())
        assert len(results) == 3

    def test_get_all_records_without_database_returns_none(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        assert node.get_all_records() is None

    def test_as_source_without_database_raises(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        with pytest.raises(RuntimeError):
            node.as_source()


class TestOperatorNodeAttachDatabases:
    def test_attach_databases_sets_pipeline_db(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db)
        assert node._pipeline_database is db

    def test_attach_databases_computes_pipeline_path(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db)
        assert node.pipeline_path is not None
        assert len(node.pipeline_path) > 0

    def test_attach_databases_clears_caches(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        node.run()  # populate cache
        assert node._cached_output_stream is not None
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db)
        assert node._cached_output_stream is None


class TestOperatorNodeWithDatabase:
    def test_construction_with_database(self):
        db = InMemoryArrowDatabase()
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
            pipeline_database=db,
        )
        assert node._pipeline_database is db

    def test_iter_packets_with_database(self):
        db = InMemoryArrowDatabase()
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
            pipeline_database=db,
        )
        results = list(node.iter_packets())
        assert len(results) == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/operators/test_operator_node_attach_db.py -v`
Expected: FAIL

- [ ] **Step 3: Merge PersistentOperatorNode into OperatorNode**

In `src/orcapod/core/nodes/operator_node.py`:

1. Add optional DB params to `OperatorNode.__init__`:
   - `pipeline_database: ArrowDatabaseProtocol | None = None`
   - `cache_mode: CacheMode = CacheMode.OFF`
   - `pipeline_path_prefix: tuple[str, ...] = ()`

2. When `pipeline_database is not None` in `__init__`, compute `_pipeline_node_hash` using `content_hash()`.

3. Add `attach_databases()`:
   ```python
   def attach_databases(self, pipeline_database, pipeline_path_prefix=None):
       self._pipeline_database = pipeline_database
       self._pipeline_path_prefix = pipeline_path_prefix or ()
       self._content_hash_cache.clear()
       self._pipeline_hash_cache.clear()
       self._cached_output_stream = None
       self._cached_output_table = None
       self._pipeline_node_hash = self.content_hash().to_string()
   ```

4. Move all `PersistentOperatorNode` methods into `OperatorNode` with guards:
   - `from_descriptor()`, `load_status`, `content_hash()`, `pipeline_hash()`, `output_schema()`, `keys()`, `pipeline_path`
   - `_store_output_stream()`, `_compute_and_store()`, `_replay_from_cache()`
   - `run()` — cache-mode aware when DB present, simple when not
   - `get_all_records()` — returns None when no DB
   - `as_source()` — raises when no DB
   - `async_execute()` — cache-mode aware when DB present

5. Delete `PersistentOperatorNode` class entirely.

**Key distinction from FunctionNode:** OperatorNode uses `content_hash()` (data-inclusive) for `_pipeline_node_hash`, not `pipeline_hash()` (schema-only). This is preserved.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core/operators/test_operator_node_attach_db.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/operator_node.py tests/test_core/operators/test_operator_node_attach_db.py
git commit -m "refactor(nodes): merge PersistentOperatorNode into OperatorNode"
```

---

### Task 10: Update All PersistentOperatorNode Imports

**Files:**
- Modify: `src/orcapod/core/nodes/__init__.py` — remove PersistentOperatorNode
- Modify: `src/orcapod/pipeline/graph.py` — update references
- Modify: `src/orcapod/pipeline/orchestrator.py` — update references
- Modify: `src/orcapod/core/sources/derived_source.py` — final type annotation update
- Modify: All test files that import PersistentOperatorNode

- [ ] **Step 1: Update exports**

In `src/orcapod/core/nodes/__init__.py`:
```python
from typing import TypeAlias

from .function_node import FunctionNode
from .operator_node import OperatorNode
from .source_node import SourceNode

GraphNode: TypeAlias = SourceNode | FunctionNode | OperatorNode

__all__ = [
    "FunctionNode",
    "GraphNode",
    "OperatorNode",
    "SourceNode",
]
```

- [ ] **Step 2: Update all files**

Replace `PersistentOperatorNode` → `OperatorNode` in:
- `src/orcapod/pipeline/graph.py`
- `src/orcapod/pipeline/orchestrator.py`
- `src/orcapod/core/sources/derived_source.py` (now `FunctionNode | OperatorNode`)
- `tests/test_core/operators/test_operator_node.py`
- `tests/test_pipeline/test_pipeline.py`
- `tests/test_pipeline/test_node_descriptors.py`
- `tests/test_channels/test_node_async_execute.py`
- `tests/test_channels/test_copilot_review_issues.py`
- `test-objective/` files

In `graph.py`, update `compile()`: instead of creating `PersistentOperatorNode`, call `node.attach_databases()` on existing `OperatorNode`.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v --tb=short`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add -u
git commit -m "refactor(nodes): update all PersistentOperatorNode imports to OperatorNode"
```

---

### Task 11: Merge GraphTracker into Pipeline

**Files:**
- Modify: `src/orcapod/core/tracker.py` — delete GraphTracker class
- Modify: `src/orcapod/pipeline/graph.py` — change base class, inline GraphTracker state/methods
- Modify: `tests/test_core/test_tracker.py` — update or remove GraphTracker tests

- [ ] **Step 1: Write the test**

Update `tests/test_core/test_tracker.py` to test recording through Pipeline:

```python
class TestPipelineRecording:
    def test_pipeline_is_auto_registering_tracker(self):
        from orcapod.core.tracker import AutoRegisteringContextBasedTracker
        from orcapod.pipeline import Pipeline
        from orcapod.databases import InMemoryArrowDatabase

        p = Pipeline(name="test", pipeline_database=InMemoryArrowDatabase())
        assert isinstance(p, AutoRegisteringContextBasedTracker)

    def test_pipeline_records_invocations(self):
        from orcapod.pipeline import Pipeline
        from orcapod.databases import InMemoryArrowDatabase

        p = Pipeline(name="test", pipeline_database=InMemoryArrowDatabase())
        with p:
            # ... use pods — nodes should be recorded
            pass
        assert isinstance(p.nodes, list)
```

- [ ] **Step 2: Move GraphTracker logic into Pipeline**

In `src/orcapod/pipeline/graph.py`:
1. Change base class from `GraphTracker` to `AutoRegisteringContextBasedTracker`
2. Add to `Pipeline.__init__`:
   ```python
   self._node_lut: dict[str, GraphNode] = {}
   self._upstreams: dict[str, StreamProtocol] = {}
   self._graph_edges: list[tuple[str, str]] = []
   self._hash_graph: nx.DiGraph = nx.DiGraph()
   ```
3. Copy `record_function_pod_invocation()` and `record_operator_pod_invocation()` from GraphTracker into Pipeline
4. Copy `nodes`, `graph`, `reset()` properties/methods
5. Update `__exit__` to call `self.reset()` after compile (same behavior as before)

In `src/orcapod/core/tracker.py`:
1. Delete `GraphTracker` class (lines 120-238)
2. Keep `BasicTrackerManager`, `AutoRegisteringContextBasedTracker`, `DEFAULT_TRACKER_MANAGER`

- [ ] **Step 3: Update imports**

Remove `GraphTracker` from import in `graph.py`. Update any test files that reference `GraphTracker` directly.

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest tests/ -v --tb=short`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -u
git commit -m "refactor(pipeline): merge GraphTracker into Pipeline"
```

---

### Task 12: Update compile() to Mutate Nodes via attach_databases()

**Files:**
- Modify: `src/orcapod/pipeline/graph.py` — rewrite compile()

- [ ] **Step 1: Write the test**

Add to `tests/test_pipeline/test_pipeline.py`:

```python
class TestCompileMutatesNodes:
    def test_compile_reuses_recorded_node_objects(self):
        """compile() should mutate recorded nodes in place, not create new ones."""
        from orcapod.core.nodes import FunctionNode

        db = InMemoryArrowDatabase()
        pipeline = Pipeline(name="test", pipeline_database=db)

        # Record a function invocation
        with pipeline:
            stream = make_int_stream(n=3)
            result = double_pod.process(stream)

        # The FunctionNode in _persistent_node_map should be the same
        # object that was recorded in _node_lut
        fn_nodes = [
            n for n in pipeline._persistent_node_map.values()
            if isinstance(n, FunctionNode)
        ]
        assert len(fn_nodes) > 0
        for fn_node in fn_nodes:
            assert fn_node._pipeline_database is not None  # DB was attached
```

- [ ] **Step 2: Rewrite compile()**

The key change in `Pipeline.compile()`:

```python
# OLD: create new PersistentFunctionNode
persistent_node = PersistentFunctionNode(...)

# NEW: mutate existing node
node.upstreams = (rewired_input,)  # rewire first
node.attach_databases(             # then attach DB
    pipeline_database=self._pipeline_database,
    result_database=result_db,
    result_path_prefix=result_prefix,
    pipeline_path_prefix=self._pipeline_path_prefix,
)
persistent_node_map[node_hash] = node  # same object
```

**Ordering constraint:** `node.upstreams = ...` MUST happen before `node.attach_databases(...)` because `attach_databases()` computes `pipeline_path` which depends on `pipeline_hash()` which depends on the upstream identity chain.

For source nodes, no `attach_databases()` is needed — they don't have databases.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v --tb=short`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/pipeline/graph.py tests/test_pipeline/test_pipeline.py
git commit -m "refactor(pipeline): compile() mutates nodes via attach_databases()"
```

---

### Task 13: Make Source Nodes First-Class Pipeline Members

**Files:**
- Modify: `src/orcapod/pipeline/graph.py` — include sources in _nodes dict
- Test: `tests/test_pipeline/test_pipeline.py` (append)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_pipeline/test_pipeline.py`:

```python
class TestSourceNodesInPipeline:
    def test_source_nodes_in_compiled_nodes(self):
        db = InMemoryArrowDatabase()
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            stream = make_int_stream(n=3)
            result = double_pod.process(stream)

        source_nodes = [
            n for n in pipeline.compiled_nodes.values()
            if n.node_type == "source"
        ]
        assert len(source_nodes) > 0

    def test_source_node_accessible_by_label(self):
        db = InMemoryArrowDatabase()
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            src = ArrowTableSource(
                table=pa.table({"id": [1], "x": [10]}),
                tag_columns=["id"],
                label="my_source",
            )
            result = double_pod.process(src)

        assert "my_source" in pipeline.compiled_nodes or hasattr(pipeline, "my_source")
```

- [ ] **Step 2: Update compile() to include source nodes in _nodes**

In `Pipeline.compile()`, when creating `SourceNode` for leaf streams, include them in `name_candidates` for label assignment. The label comes from `persistent_node.label` falling back to `persistent_node.__class__.__name__` (since `SourceNode.computed_label()` delegates to `stream.label`, which is the source's class name after our refactor):

```python
if node_hash not in self._node_lut:
    stream = self._upstreams[node_hash]
    persistent_node = SourceNode(stream=stream)
    persistent_node_map[node_hash] = persistent_node
    # Include source nodes in label assignment
    label = persistent_node.label or "unnamed"
    name_candidates.setdefault(label, []).append(persistent_node)
```

Note: `SourceNode.computed_label()` returns `self.stream.label` which, after our refactor, returns the source class name (e.g. `"DictSource"`, `"ArrowTableStream"`). No need to call `computed_label()` explicitly — the `label` property already checks `_label` then `computed_label()`.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v --tb=short`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/pipeline/graph.py tests/test_pipeline/test_pipeline.py
git commit -m "feat(pipeline): source nodes are first-class pipeline members"
```

---

### Task 14: Final Cleanup and Full Verification

**Files:**
- Various: docstrings, dead code, unused imports, design docs

- [ ] **Step 1: Clean up docstrings**

- Remove references to `PersistentFunctionNode`, `PersistentOperatorNode`, `GraphTracker` from all docstrings
- Update `src/orcapod/pipeline/nodes.py` comment
- Update class docstrings in `FunctionNode` and `OperatorNode` (remove "Subclass PersistentFunctionNode adds..." text)

- [ ] **Step 2: Run ruff format and lint**

Run: `uv run ruff check src/ tests/ --fix && uv run ruff format src/ tests/`

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: ALL PASS

- [ ] **Step 4: Run test-objective suite**

Run: `uv run pytest test-objective/ -v --tb=short`
Expected: PASS or update references

- [ ] **Step 5: Update design spec status**

In `docs/superpowers/specs/2026-03-12-pipeline-simplification-design.md`, change status to "implemented".

- [ ] **Step 6: Commit**

```bash
git add -u
git commit -m "chore: clean up references to removed classes, update design spec"
```
