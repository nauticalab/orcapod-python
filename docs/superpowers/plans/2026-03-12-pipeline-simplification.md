# Pipeline & Node Simplification Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify the pipeline architecture by merging redundant class hierarchies: GraphTracker → Pipeline, PersistentFunctionNode → FunctionNode, PersistentOperatorNode → OperatorNode, and refactor source delegation to inheritance.

**Architecture:** Merge persistent node variants into their base classes with optional database params. Merge GraphTracker recording logic directly into Pipeline. Refactor delegating sources (DictSource, CSVSource, etc.) to inherit from ArrowTableSource instead of wrapping an internal `_arrow_source`. Make source nodes first-class pipeline members.

**Tech Stack:** Python 3.12+, PyArrow, NetworkX, pytest, uv

---

## File Structure

### Source files modified/created:
- `src/orcapod/core/sources/base.py` — remove `computed_label()` override
- `src/orcapod/core/sources/dict_source.py` — rewrite: inherit from ArrowTableSource
- `src/orcapod/core/sources/data_frame_source.py` — rewrite: inherit from ArrowTableSource
- `src/orcapod/core/sources/csv_source.py` — rewrite: inherit from ArrowTableSource
- `src/orcapod/core/sources/delta_table_source.py` — rewrite: inherit from ArrowTableSource
- `src/orcapod/core/sources/list_source.py` — rewrite: inherit from ArrowTableSource
- `src/orcapod/core/nodes/function_node.py` — merge PersistentFunctionNode in
- `src/orcapod/core/nodes/operator_node.py` — merge PersistentOperatorNode in
- `src/orcapod/core/nodes/__init__.py` — remove Persistent* exports
- `src/orcapod/__init__.py` — remove PersistentFunctionNode export
- `src/orcapod/core/tracker.py` — delete GraphTracker class
- `src/orcapod/pipeline/graph.py` — inherit from AutoRegisteringContextBasedTracker, inline GraphTracker, use attach_databases() in compile()
- `src/orcapod/core/sources/derived_source.py` — update type annotations
- `src/orcapod/pipeline/orchestrator.py` — update references

### Test files created:
- `tests/test_core/sources/test_source_label.py` — source label and inheritance tests
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

## Chunk 1: Source Inheritance Refactor & Label Cleanup

**Strategy:** Refactor each delegating source to inherit from ArrowTableSource first. Each refactored source naturally loses its `computed_label()` override and `_arrow_source` delegation. After all sources are refactored, remove `computed_label()` from `RootSource` as the final cleanup step.

**Note on hash stability:** `ArrowTableSource.identity_structure()` includes `self.__class__.__name__`. After inheritance, a `DictSource` will report `"DictSource"` instead of `"ArrowTableSource"`, changing content hashes. Per CLAUDE.md, this is a pre-v0.1.0 project with no backward-compatibility concerns.

**Note on CachedSource:** `CachedSource` wraps a `RootSource` and a cache database. It does NOT use `_arrow_source` delegation and is unaffected by this refactor.

### Task 1: Refactor DictSource to Inherit from ArrowTableSource

**Files:**
- Modify: `src/orcapod/core/sources/dict_source.py`
- Test: `tests/test_core/sources/test_source_label.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_core/sources/test_source_label.py`:

```python
"""Tests for source inheritance and label defaults."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.dict_source import DictSource


def _simple_table():
    return pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})


class TestDictSourceInheritance:
    def test_dict_source_is_arrow_table_source(self):
        src = DictSource(
            data=[{"id": 1, "x": 10}],
            tag_columns=["id"],
        )
        assert isinstance(src, ArrowTableSource)

    def test_dict_source_has_no_arrow_source_attr(self):
        src = DictSource(
            data=[{"id": 1, "x": 10}],
            tag_columns=["id"],
        )
        assert not hasattr(src, "_arrow_source")

    def test_dict_source_iter_packets(self):
        src = DictSource(
            data=[{"id": 1, "x": 10}, {"id": 2, "x": 20}],
            tag_columns=["id"],
        )
        results = list(src.iter_packets())
        assert len(results) == 2

    def test_dict_source_to_config(self):
        src = DictSource(
            data=[{"id": 1, "x": 10}],
            tag_columns=["id"],
        )
        config = src.to_config()
        assert config["source_type"] == "dict"
        assert "tag_columns" in config
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_source_label.py::TestDictSourceInheritance -v`
Expected: FAIL — DictSource is not an ArrowTableSource, has `_arrow_source`

- [ ] **Step 3: Rewrite DictSource to inherit from ArrowTableSource**

Replace `src/orcapod/core/sources/dict_source.py`:

```python
from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any

from orcapod import contexts
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.types import DataValue, SchemaLike


class DictSource(ArrowTableSource):
    """A source backed by a collection of Python dictionaries.

    Each dict becomes one (tag, packet) pair in the stream. The dicts are
    converted to an Arrow table via the data-context type converter, then
    handled by ``ArrowTableSource`` (including source-info and schema-hash
    annotation).
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
        # Resolve data_context before super().__init__ so type_converter is available
        data_context = kwargs.get("data_context")
        resolved_ctx = contexts.resolve_context(data_context)

        arrow_table = resolved_ctx.type_converter.python_dicts_to_arrow_table(
            [dict(row) for row in data],
            python_schema=data_schema,
        )
        super().__init__(
            table=arrow_table,
            tag_columns=tag_columns,
            system_tag_columns=system_tag_columns,
            source_id=source_id,
            **kwargs,
        )

    def to_config(self) -> dict[str, Any]:
        """Serialize metadata-only config (data is not serializable).

        Returns:
            Dict with source metadata. Cannot be used to reconstruct the source
            since the original data is not preserved.
        """
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

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core/sources/test_source_label.py::TestDictSourceInheritance -v`
Expected: PASS

- [ ] **Step 5: Run existing source tests**

Run: `uv run pytest tests/test_core/sources/ -v --tb=short`
Expected: PASS (some hash-dependent tests may need updating)

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/sources/dict_source.py tests/test_core/sources/test_source_label.py
git commit -m "refactor(sources): DictSource inherits from ArrowTableSource"
```

---

### Task 2: Refactor DataFrameSource to Inherit from ArrowTableSource

**Files:**
- Modify: `src/orcapod/core/sources/data_frame_source.py`
- Test: `tests/test_core/sources/test_source_label.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/sources/test_source_label.py`:

```python
from orcapod.core.sources.data_frame_source import DataFrameSource


class TestDataFrameSourceInheritance:
    def test_data_frame_source_is_arrow_table_source(self):
        src = DataFrameSource(
            data={"id": [1, 2], "x": [10, 20]},
            tag_columns=["id"],
        )
        assert isinstance(src, ArrowTableSource)

    def test_data_frame_source_has_no_arrow_source_attr(self):
        src = DataFrameSource(
            data={"id": [1, 2], "x": [10, 20]},
            tag_columns=["id"],
        )
        assert not hasattr(src, "_arrow_source")

    def test_data_frame_source_iter_packets(self):
        src = DataFrameSource(
            data={"id": [1, 2], "x": [10, 20]},
            tag_columns=["id"],
        )
        assert len(list(src.iter_packets())) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_source_label.py::TestDataFrameSourceInheritance -v`
Expected: FAIL

- [ ] **Step 3: Rewrite DataFrameSource**

Replace `src/orcapod/core/sources/data_frame_source.py`:

```python
from __future__ import annotations

import logging
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.utils import polars_data_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    from polars._typing import FrameInitTypes
else:
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)


class DataFrameSource(ArrowTableSource):
    """A source backed by a Polars DataFrame (or any Polars-compatible data).

    The DataFrame is converted to an Arrow table and then handled identically
    to ``ArrowTableSource``, including source-info provenance annotation and
    schema-hash system tags.
    """

    def __init__(
        self,
        data: "FrameInitTypes",
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        # Resolve data_context before super().__init__ so type_converter is available
        data_context = kwargs.get("data_context")
        resolved_ctx = contexts.resolve_context(data_context)

        df = pl.DataFrame(data)

        # Convert any Object-dtype columns to Arrow-compatible types.
        object_columns = [c for c in df.columns if df[c].dtype == pl.Object]
        if object_columns:
            logger.info(
                f"Converting {len(object_columns)} object column(s) to Arrow format"
            )
            sub_table = resolved_ctx.type_converter.python_dicts_to_arrow_table(
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

        super().__init__(
            table=df.to_arrow(),
            tag_columns=tag_columns,
            system_tag_columns=system_tag_columns,
            source_id=source_id,
            **kwargs,
        )

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

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_core/sources/ -v --tb=short`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/sources/data_frame_source.py tests/test_core/sources/test_source_label.py
git commit -m "refactor(sources): DataFrameSource inherits from ArrowTableSource"
```

---

### Task 3: Refactor CSVSource to Inherit from ArrowTableSource

**Files:**
- Modify: `src/orcapod/core/sources/csv_source.py`
- Test: `tests/test_core/sources/test_source_label.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/sources/test_source_label.py`:

```python
import csv


class TestCSVSourceInheritance:
    def test_csv_source_is_arrow_table_source(self, tmp_path):
        csv_path = str(tmp_path / "test.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "x"])
            writer.writeheader()
            writer.writerow({"id": 1, "x": 10})
        from orcapod.core.sources.csv_source import CSVSource

        src = CSVSource(file_path=csv_path, tag_columns=["id"])
        assert isinstance(src, ArrowTableSource)

    def test_csv_source_has_no_arrow_source_attr(self, tmp_path):
        csv_path = str(tmp_path / "test.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "x"])
            writer.writeheader()
            writer.writerow({"id": 1, "x": 10})
        from orcapod.core.sources.csv_source import CSVSource

        src = CSVSource(file_path=csv_path, tag_columns=["id"])
        assert not hasattr(src, "_arrow_source")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_source_label.py::TestCSVSourceInheritance -v`
Expected: FAIL

- [ ] **Step 3: Rewrite CSVSource**

Replace `src/orcapod/core/sources/csv_source.py`:

```python
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class CSVSource(ArrowTableSource):
    """A source backed by a CSV file.

    The file is read once at construction time using PyArrow's CSV reader,
    converted to an Arrow table, and then handled identically to
    ``ArrowTableSource``.

    Args:
        file_path: Path to the CSV file to read.
        tag_columns: Column names whose values form the tag for each row.
        system_tag_columns: Additional system-level tag columns.
        record_id_column: Column whose values serve as stable record identifiers.
        source_id: Canonical registry name for this source.
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
        if source_id is None:
            source_id = file_path

        import pyarrow.csv as pa_csv

        self._file_path = file_path
        table: pa.Table = pa_csv.read_csv(file_path)

        super().__init__(
            table=table,
            tag_columns=tag_columns,
            system_tag_columns=system_tag_columns,
            record_id_column=record_id_column,
            source_id=source_id,
            **kwargs,
        )

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

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_core/sources/ -v --tb=short`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/sources/csv_source.py tests/test_core/sources/test_source_label.py
git commit -m "refactor(sources): CSVSource inherits from ArrowTableSource"
```

---

### Task 4: Refactor DeltaTableSource to Inherit from ArrowTableSource

**Files:**
- Modify: `src/orcapod/core/sources/delta_table_source.py`

- [ ] **Step 1: Rewrite DeltaTableSource**

Replace `src/orcapod/core/sources/delta_table_source.py`:

```python
from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.types import PathLike
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class DeltaTableSource(ArrowTableSource):
    """A source backed by a Delta Lake table.

    The table is read once at construction time using ``deltalake``'s
    PyArrow integration. The resulting Arrow table is passed to
    ``ArrowTableSource`` which adds source-info provenance and schema-hash
    system tags.

    Args:
        delta_table_path: Filesystem path to the Delta table directory.
        tag_columns: Column names whose values form the tag for each row.
        system_tag_columns: Additional system-level tag columns.
        record_id_column: Column whose values serve as stable record identifiers.
        source_id: Canonical registry name for this source.
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

        self._delta_table_path = resolved

        try:
            delta_table = DeltaTable(str(resolved))
        except TableNotFoundError:
            raise ValueError(f"Delta table not found at {resolved}")

        table: pa.Table = delta_table.to_pyarrow_dataset(as_large_types=True).to_table()

        super().__init__(
            table=table,
            tag_columns=tag_columns,
            system_tag_columns=system_tag_columns,
            record_id_column=record_id_column,
            source_id=source_id,
            **kwargs,
        )

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

- [ ] **Step 2: Run existing DeltaTableSource tests**

Run: `uv run pytest tests/test_core/sources/test_sources_comprehensive.py -v --tb=short -k delta`
Expected: PASS (or skip if deltalake not installed)

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/core/sources/delta_table_source.py
git commit -m "refactor(sources): DeltaTableSource inherits from ArrowTableSource"
```

---

### Task 5: Refactor ListSource to Inherit from ArrowTableSource

**Files:**
- Modify: `src/orcapod/core/sources/list_source.py`
- Test: `tests/test_core/sources/test_source_label.py` (append)

ListSource is more complex: it has a custom `identity_structure()` including tag function hash, and needs `data_context` resolved before `super().__init__()` for `_hash_tag_function()`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/sources/test_source_label.py`:

```python
from orcapod.core.sources.list_source import ListSource


class TestListSourceInheritance:
    def test_list_source_is_arrow_table_source(self):
        src = ListSource(name="val", data=[1, 2, 3])
        assert isinstance(src, ArrowTableSource)

    def test_list_source_has_no_arrow_source_attr(self):
        src = ListSource(name="val", data=[1, 2, 3])
        assert not hasattr(src, "_arrow_source")

    def test_list_source_identity_includes_tag_function_hash(self):
        src1 = ListSource(name="val", data=[1, 2])
        src2 = ListSource(name="val", data=[1, 2])
        assert src1.identity_structure() == src2.identity_structure()

    def test_list_source_custom_tag_function(self):
        src = ListSource(
            name="val",
            data=[10, 20],
            tag_function=lambda elem, idx: {"my_tag": idx * 10},
            expected_tag_keys=["my_tag"],
        )
        tags, packets = src.keys()
        assert "my_tag" in tags
        assert "val" in packets

    def test_list_source_iter_packets(self):
        src = ListSource(name="val", data=[10, 20, 30])
        assert len(list(src.iter_packets())) == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_source_label.py::TestListSourceInheritance -v`
Expected: FAIL on isinstance check

- [ ] **Step 3: Rewrite ListSource**

Replace `src/orcapod/core/sources/list_source.py`:

```python
from __future__ import annotations

from collections.abc import Callable, Collection
from typing import TYPE_CHECKING, Any, Literal

from orcapod import contexts
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.protocols.core_protocols import TagProtocol

if TYPE_CHECKING:
    import pyarrow as pa


class ListSource(ArrowTableSource):
    """A source backed by a Python list.

    Each element in the list becomes one (tag, packet) pair. The element is
    stored as the packet under ``name``; the tag is either the element's index
    (default) or the dict returned by ``tag_function(element, index)``.

    Args:
        name: Packet column name under which each list element is stored.
        data: The list of elements.
        tag_function: Optional callable ``(element, index) -> dict[str, Any]``
            producing the tag fields for each element.
        expected_tag_keys: Explicit tag key names.
        tag_function_hash_mode: How to identify the tag function for content-hash.
        source_id: Canonical registry name for this source.
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
        # Resolve data_context before super().__init__ so semantic_hasher is available
        data_context = kwargs.get("data_context")
        resolved_ctx = contexts.resolve_context(data_context)

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

        # Hash the tag function for identity purposes (needs resolved_ctx).
        self._tag_function_hash = self._hash_tag_function(resolved_ctx)

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

        table = resolved_ctx.type_converter.python_dicts_to_arrow_table(rows)

        # Pass resolved context so super().__init__ doesn't re-resolve
        super().__init__(
            table=table,
            tag_columns=tag_columns,
            source_id=source_id,
            data_context=resolved_ctx,
            **{k: v for k, v in kwargs.items() if k != "data_context"},
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

    def _hash_tag_function(self, ctx: contexts.DataContext | None = None) -> str:
        """Produce a stable hash string for the tag function."""
        if self._tag_function_hash_mode == "name":
            fn = self._tag_function
            return f"{fn.__module__}.{fn.__qualname__}"
        elif self._tag_function_hash_mode == "signature":
            import inspect

            return str(inspect.signature(self._tag_function))
        else:  # "content"
            import inspect

            resolved_ctx = ctx or self.data_context
            src = inspect.getsource(self._tag_function)
            return resolved_ctx.semantic_hasher.hash_object(src).to_hex()

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
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_core/sources/ -v --tb=short`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/sources/list_source.py tests/test_core/sources/test_source_label.py
git commit -m "refactor(sources): ListSource inherits from ArrowTableSource"
```

---

### Task 6: Remove `computed_label()` from RootSource and Fix Regressions

Now that all delegating sources inherit from ArrowTableSource (no more `_arrow_source` delegation), remove the `computed_label()` override from `RootSource` and fix any test regressions.

**Files:**
- Modify: `src/orcapod/core/sources/base.py:117-119`
- Test: `tests/test_core/sources/test_source_label.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/sources/test_source_label.py`:

```python
class TestSourceLabelDefaults:
    def test_arrow_table_source_label_is_class_name(self):
        src = ArrowTableSource(table=_simple_table(), tag_columns=["id"])
        assert src.label == "ArrowTableSource"

    def test_dict_source_label_is_class_name(self):
        src = DictSource(
            data=[{"id": 1, "x": 10}, {"id": 2, "x": 20}],
            tag_columns=["id"],
        )
        assert src.label == "DictSource"

    def test_list_source_label_is_class_name(self):
        src = ListSource(name="val", data=[1, 2, 3])
        assert src.label == "ListSource"

    def test_data_frame_source_label_is_class_name(self):
        src = DataFrameSource(
            data={"id": [1, 2], "x": [10, 20]},
            tag_columns=["id"],
        )
        assert src.label == "DataFrameSource"

    def test_explicit_label_overrides_default(self):
        src = ArrowTableSource(
            table=_simple_table(), tag_columns=["id"], label="my_source"
        )
        assert src.label == "my_source"

    def test_source_id_is_not_label(self):
        """source_id should not leak into label."""
        src = ArrowTableSource(
            table=_simple_table(),
            tag_columns=["id"],
            source_id="custom_sid",
        )
        assert src.label == "ArrowTableSource"
        assert src.source_id == "custom_sid"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/sources/test_source_label.py::TestSourceLabelDefaults -v`
Expected: FAIL — `ArrowTableSource` still returns `source_id` as label via inherited `RootSource.computed_label()`

- [ ] **Step 3: Remove `computed_label()` from RootSource**

In `src/orcapod/core/sources/base.py`, delete lines 117-119:
```python
    def computed_label(self) -> str | None:
        """Return the source_id as the label."""
        return self._source_id
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/sources/test_source_label.py -v`
Expected: PASS

- [ ] **Step 5: Run full test suite to catch regressions**

Run: `uv run pytest tests/ -v --tb=short`

Fix any failures. Common issues:
- Tests in `test_sources.py` that accessed `_arrow_source` directly — update to use source attributes directly
- Tests in `test_source_protocol_conformance.py` that checked `_arrow_source` — remove those checks
- Tests in `test_pipeline_hash_integration.py` that compare exact hash values — update expected hashes (class name changed in identity_structure)
- Tests that assert source labels equal source_id — update to class name

- [ ] **Step 6: Commit**

```bash
git add -u
git commit -m "refactor(sources): remove computed_label() from RootSource, fix regressions"
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
