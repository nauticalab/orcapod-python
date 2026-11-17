# ENG-516: SourceNodeBase column_config / all_info forwarding — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `SourceNodeBase.keys()` and `output_schema()` to honour `column_config` / `all_info` — computing system-tag column names for unbound nodes and forwarding the arguments to `bound_source` for bound `SourceJobNode` instances.

**Architecture:** Extract two shared utility functions (`compute_schema_hash` in `schema_utils.py`, `system_tag_column_names` in `arrow_utils.py`), use them from both `stream_builder.py` (refactor) and `SourceNodeBase` (new behaviour). Then add `keys()` / `output_schema()` overrides to `SourceJobNode` that delegate to `bound_source` when set.

**Tech Stack:** Python, PyArrow, orcapod internal types (`Schema`, `ColumnConfig`), `uv run pytest`.

**Spec:** `superpowers/specs/2026-06-05-eng-516-source-node-column-config-forwarding.md`

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/orcapod/utils/schema_utils.py` | Modify | Add `compute_schema_hash()` |
| `src/orcapod/utils/arrow_utils.py` | Modify | Add `system_tag_column_names()`; use it inside `add_system_tag_columns()` |
| `src/orcapod/core/sources/stream_builder.py` | Modify | Call `compute_schema_hash()` instead of inlining hash logic |
| `src/orcapod/core/nodes/source_node.py` | Modify | Add `_schema_hash()` helper; fix `SourceNodeBase.keys()` and `output_schema()`; add `SourceJobNode.keys()` and `output_schema()` overrides |
| `tests/test_utils/test_schema_utils.py` | Create | Unit tests for `compute_schema_hash()` |
| `tests/test_utils/test_arrow_utils.py` | Modify | Add tests for `system_tag_column_names()` |
| `tests/test_core/nodes/test_source_node.py` | Modify | Add `TestSourceNodeColumnConfig` class |

---

## Task 1: Add `compute_schema_hash()` to `schema_utils.py`

**Files:**
- Create: `tests/test_utils/test_schema_utils.py`
- Modify: `src/orcapod/utils/schema_utils.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_utils/test_schema_utils.py`:

```python
"""Tests for schema_utils utility functions."""

from __future__ import annotations

import pytest

from orcapod.config import DEFAULT_CONFIG
from orcapod.contexts import resolve_context
from orcapod.types import Schema


@pytest.fixture
def semantic_hasher():
    return resolve_context(None).semantic_hasher


@pytest.fixture
def char_count():
    return DEFAULT_CONFIG.schema_hash_n_char


class TestComputeSchemaHash:
    def test_returns_string(self, semantic_hasher, char_count):
        from orcapod.utils.schema_utils import compute_schema_hash

        result = compute_schema_hash(
            Schema({"id": int}),
            Schema({"value": float}),
            semantic_hasher,
            char_count,
        )
        assert isinstance(result, str)

    def test_deterministic(self, semantic_hasher, char_count):
        from orcapod.utils.schema_utils import compute_schema_hash

        tag = Schema({"id": int})
        data = Schema({"value": float})
        assert compute_schema_hash(tag, data, semantic_hasher, char_count) == \
               compute_schema_hash(tag, data, semantic_hasher, char_count)

    def test_length_equals_char_count(self, semantic_hasher, char_count):
        from orcapod.utils.schema_utils import compute_schema_hash

        result = compute_schema_hash(
            Schema({"id": int}), Schema({"value": float}), semantic_hasher, char_count
        )
        assert len(result) == char_count

    def test_different_schemas_produce_different_hashes(self, semantic_hasher, char_count):
        from orcapod.utils.schema_utils import compute_schema_hash

        h1 = compute_schema_hash(Schema({"id": int}), Schema({"v": float}), semantic_hasher, char_count)
        h2 = compute_schema_hash(Schema({"id": int}), Schema({"v": int}), semantic_hasher, char_count)
        assert h1 != h2
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_utils/test_schema_utils.py -v
```

Expected: `ImportError` — `cannot import name 'compute_schema_hash' from 'orcapod.utils.schema_utils'`

- [ ] **Step 3: Add `compute_schema_hash()` to `schema_utils.py`**

Open `src/orcapod/utils/schema_utils.py`. After the existing imports block (after `from orcapod.types import Schema, SchemaLike`), add `Any` to the `typing` import if not already present (it is — line 7). Then append the new function at the end of the file:

```python
def compute_schema_hash(
    tag_schema: Schema,
    data_schema: Schema,
    semantic_hasher: Any,
    char_count: int,
) -> str:
    """Compute the schema hash used for system-tag column naming.

    This is the same hash that ``SourceStreamBuilder`` embeds in system-tag
    column names when building an ``ArrowTableStream`` from a source table.
    Extracting it here lets ``SourceNodeBase`` predict the system-tag column
    names from its declared schemas alone, without requiring a live source.

    Args:
        tag_schema: Python tag schema (``Schema`` mapping column names to types).
        data_schema: Python data schema.
        semantic_hasher: Hasher from the active ``DataContext``
            (``data_context.semantic_hasher``).
        char_count: Number of hex characters to use in the output string
            (``OrcapodConfig.schema_hash_n_char``).

    Returns:
        Hex string of length ``char_count``.
    """
    return semantic_hasher.hash_object(
        (tag_schema, data_schema)
    ).to_hex(char_count=char_count)
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
uv run pytest tests/test_utils/test_schema_utils.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_utils/test_schema_utils.py src/orcapod/utils/schema_utils.py
git commit -m "feat(schema_utils): add compute_schema_hash() utility"
```

---

## Task 2: Add `system_tag_column_names()` to `arrow_utils.py`

**Files:**
- Modify: `src/orcapod/utils/arrow_utils.py`
- Modify: `tests/test_utils/test_arrow_utils.py`

- [ ] **Step 1: Write the failing tests**

Open `tests/test_utils/test_arrow_utils.py` and append a new test class at the end:

```python
class TestSystemTagColumnNames:
    def test_returns_two_strings(self):
        from orcapod.utils.arrow_utils import system_tag_column_names

        src_col, rec_col = system_tag_column_names("abc123")
        assert isinstance(src_col, str)
        assert isinstance(rec_col, str)

    def test_source_id_col_starts_with_system_tag_prefix(self):
        from orcapod.system_constants import constants
        from orcapod.utils.arrow_utils import system_tag_column_names

        src_col, _ = system_tag_column_names("abc123")
        assert src_col.startswith(constants.SYSTEM_TAG_PREFIX)

    def test_record_id_col_starts_with_system_tag_prefix(self):
        from orcapod.system_constants import constants
        from orcapod.utils.arrow_utils import system_tag_column_names

        _, rec_col = system_tag_column_names("abc123")
        assert rec_col.startswith(constants.SYSTEM_TAG_PREFIX)

    def test_schema_hash_embedded_in_col_names(self):
        from orcapod.utils.arrow_utils import system_tag_column_names

        schema_hash = "deadbeef"
        src_col, rec_col = system_tag_column_names(schema_hash)
        assert schema_hash in src_col
        assert schema_hash in rec_col

    def test_different_hashes_produce_different_names(self):
        from orcapod.utils.arrow_utils import system_tag_column_names

        src1, rec1 = system_tag_column_names("aaa")
        src2, rec2 = system_tag_column_names("bbb")
        assert src1 != src2
        assert rec1 != rec2

    def test_col_names_match_add_system_tag_columns_output(self):
        """Column names from system_tag_column_names() must match those
        added to a table by add_system_tag_columns()."""
        import pyarrow as pa
        from orcapod.utils.arrow_utils import add_system_tag_columns, system_tag_column_names

        schema_hash = "testhash"
        table = pa.table({"id": pa.array([1]), "v": pa.array([1.0])})
        enriched = add_system_tag_columns(table, schema_hash, ["src_a"], ["row_0"])
        src_col, rec_col = system_tag_column_names(schema_hash)
        assert src_col in enriched.column_names
        assert rec_col in enriched.column_names
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/test_utils/test_arrow_utils.py::TestSystemTagColumnNames -v
```

Expected: `ImportError` — `cannot import name 'system_tag_column_names'`

- [ ] **Step 3: Add `system_tag_column_names()` and refactor `add_system_tag_columns()`**

Open `src/orcapod/utils/arrow_utils.py`. Find `add_system_tag_columns` (around line 940). Insert the new function directly above it, then update `add_system_tag_columns` to call it:

```python
def system_tag_column_names(schema_hash: str) -> tuple[str, str]:
    """Return the (source_id_col_name, record_id_col_name) system-tag column names.

    These are the two column names that ``add_system_tag_columns`` adds to every
    source table, and that ``ArrowTableStream`` exposes via ``keys(system_tags=True)``
    and ``output_schema(system_tags=True)``.

    Args:
        schema_hash: Hex schema hash produced by ``compute_schema_hash()``.

    Returns:
        Tuple of ``(source_id_column_name, record_id_column_name)``.
        Both start with ``constants.SYSTEM_TAG_PREFIX`` and have ``large_string``
        type in the Arrow table.
    """
    source_id_col = (
        f"{constants.SYSTEM_TAG_SOURCE_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"
    )
    record_id_col = (
        f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"
    )
    return source_id_col, record_id_col


def add_system_tag_columns(
    table: "pa.Table",
    schema_hash: str,
    source_ids: str | Collection[str],
    record_ids: Collection[str],
) -> "pa.Table":
    """Add paired source_id and record_id system tag columns to an Arrow table."""
    if not table.column_names:
        raise ValueError("Table is empty")

    # Normalize source_ids
    if isinstance(source_ids, str):
        source_ids = [source_ids] * table.num_rows
    else:
        source_ids = list(source_ids)
        if len(source_ids) != table.num_rows:
            raise ValueError(
                "Length of source_ids must match number of rows in the table."
            )

    record_ids = list(record_ids)
    if len(record_ids) != table.num_rows:
        raise ValueError("Length of record_ids must match number of rows in the table.")

    source_id_col_name, record_id_col_name = system_tag_column_names(schema_hash)

    source_id_array = pa.array(source_ids, type=pa.large_string())
    record_id_array = pa.array(record_ids, type=pa.large_string())

    # System tag columns are always computed, never null — declare nullable=False
    # explicitly so the schema intent is not lost in Polars round-trips.
    table = table.append_column(
        pa.field(source_id_col_name, pa.large_string(), nullable=False), source_id_array
    )
    table = table.append_column(
        pa.field(record_id_col_name, pa.large_string(), nullable=False), record_id_array
    )
    return table
```

- [ ] **Step 4: Run all tests to verify nothing broke**

```bash
uv run pytest tests/test_utils/test_arrow_utils.py -v
```

Expected: All tests including the new `TestSystemTagColumnNames` class PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/utils/arrow_utils.py tests/test_utils/test_arrow_utils.py
git commit -m "feat(arrow_utils): add system_tag_column_names(); use it in add_system_tag_columns()"
```

---

## Task 3: Refactor `stream_builder.py` to use `compute_schema_hash()`

**Files:**
- Modify: `src/orcapod/core/sources/stream_builder.py`

This is a pure refactor — no new tests. The existing `test_stream_builder.py` is the regression suite.

- [ ] **Step 1: Run existing stream_builder tests to establish baseline**

```bash
uv run pytest tests/test_core/sources/test_stream_builder.py -v
```

Expected: All tests PASS.

- [ ] **Step 2: Add import and replace inline hash computation**

Open `src/orcapod/core/sources/stream_builder.py`.

Add the import after the existing `from orcapod.types import ContentHash` line:

```python
from orcapod.utils.schema_utils import compute_schema_hash
```

Find the schema hash computation block inside `SourceStreamBuilder.build()` (currently around lines 117–131):

```python
        # 4. Compute schema hash from tag/data python schemas.
        # Nullable flags in the incoming table are trusted as-is — callers must
        # set them correctly before calling build().
        non_sys = arrow_utils.drop_system_columns(table)
        tag_schema = non_sys.select(list(tag_columns_tuple)).schema
        data_schema = non_sys.drop(list(tag_columns_tuple)).schema
        tag_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            tag_schema
        )
        data_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            data_schema
        )
        schema_hash = self._data_context.semantic_hasher.hash_object(
            (tag_python, data_python)
        ).to_hex(char_count=self._config.schema_hash_n_char)
```

Replace it with:

```python
        # 4. Compute schema hash from tag/data python schemas.
        # Nullable flags in the incoming table are trusted as-is — callers must
        # set them correctly before calling build().
        non_sys = arrow_utils.drop_system_columns(table)
        tag_schema = non_sys.select(list(tag_columns_tuple)).schema
        data_schema = non_sys.drop(list(tag_columns_tuple)).schema
        tag_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            tag_schema
        )
        data_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            data_schema
        )
        schema_hash = compute_schema_hash(
            tag_python,
            data_python,
            self._data_context.semantic_hasher,
            self._config.schema_hash_n_char,
        )
```

- [ ] **Step 3: Run the tests to verify nothing broke**

```bash
uv run pytest tests/test_core/sources/test_stream_builder.py -v
```

Expected: All tests PASS.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/sources/stream_builder.py
git commit -m "refactor(stream_builder): use compute_schema_hash() from schema_utils"
```

---

## Task 4: Fix `SourceNodeBase` — honour `system_tags` in `keys()` and `output_schema()`

**Files:**
- Modify: `tests/test_core/nodes/test_source_node.py`
- Modify: `src/orcapod/core/nodes/source_node.py`

- [ ] **Step 1: Write the failing tests**

Open `tests/test_core/nodes/test_source_node.py`. Append a new test class after the existing `TestSourceNodeAsTable` class:

```python
class TestSourceNodeColumnConfig:
    """SourceNodeBase must honour column_config / all_info in keys() and output_schema()."""

    def test_unbound_keys_default_unchanged(self, tag_schema, data_schema):
        """Regression: keys() with no args still returns plain schema keys."""
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        tag_keys, data_keys = node.keys()
        assert tag_keys == ("id",)
        assert data_keys == ("value",)

    def test_unbound_keys_system_tags_includes_both_system_cols(self, tag_schema, data_schema):
        """keys(system_tags=True) adds both system-tag column names to tag_keys."""
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.system_constants import constants
        from orcapod.types import ColumnConfig

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        tag_keys, data_keys = node.keys(columns=ColumnConfig(system_tags=True))

        assert data_keys == ("value",)
        assert len(tag_keys) == 3  # id + source_id_col + record_id_col
        assert tag_keys[0] == "id"
        # Both extra columns start with the system-tag prefix
        assert all(k.startswith(constants.SYSTEM_TAG_PREFIX) for k in tag_keys[1:])

    def test_unbound_keys_all_info_same_as_system_tags(self, tag_schema, data_schema):
        """keys(all_info=True) produces the same result as keys(system_tags=True)."""
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.types import ColumnConfig

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        via_system_tags = node.keys(columns=ColumnConfig(system_tags=True))
        via_all_info = node.keys(all_info=True)
        assert via_system_tags == via_all_info

    def test_unbound_output_schema_system_tags_adds_str_entries(self, tag_schema, data_schema):
        """output_schema(system_tags=True) tag schema includes two str-typed system-tag entries."""
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.system_constants import constants
        from orcapod.types import ColumnConfig

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        extended_tag_schema, data_schema_out = node.output_schema(
            columns=ColumnConfig(system_tags=True)
        )

        assert data_schema_out == data_schema
        assert "id" in extended_tag_schema
        system_tag_entries = {
            k: v for k, v in extended_tag_schema.items()
            if k.startswith(constants.SYSTEM_TAG_PREFIX)
        }
        assert len(system_tag_entries) == 2
        assert all(v is str for v in system_tag_entries.values())

    def test_unbound_output_schema_default_unchanged(self, tag_schema, data_schema):
        """Regression: output_schema() with no args still returns plain schemas."""
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        t, d = node.output_schema()
        assert t == tag_schema
        assert d == data_schema
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/test_core/nodes/test_source_node.py::TestSourceNodeColumnConfig -v
```

Expected: `test_unbound_keys_default_unchanged` PASSES (existing behaviour). All `system_tags` tests FAIL with wrong key counts or missing columns.

- [ ] **Step 3: Add imports to `source_node.py`**

Open `src/orcapod/core/nodes/source_node.py`. Find the existing imports block. Add these two lines after `from orcapod.types import ColumnConfig, Schema`:

```python
from orcapod.utils.arrow_utils import system_tag_column_names
from orcapod.utils.schema_utils import compute_schema_hash
```

- [ ] **Step 4: Add `_schema_hash()` helper to `SourceNodeBase`**

Inside `SourceNodeBase`, find the `# Properties` section (after line ~84). Add this private helper after the `data_schema` property:

```python
def _schema_hash(self) -> str:
    """Compute the schema hash used for system-tag column naming.

    Produces the same hash that ``SourceStreamBuilder`` embeds in system-tag
    column names, making it possible to predict those names from the declared
    schemas alone without requiring a live source.

    Returns:
        Hex string schema hash.
    """
    return compute_schema_hash(
        self._tag_schema,
        self._data_schema,
        self.data_context.semantic_hasher,
        self.orcapod_config.schema_hash_n_char,
    )
```

- [ ] **Step 5: Replace `SourceNodeBase.keys()` body**

Find `SourceNodeBase.keys()` (currently around line 159). Replace its body and docstring with:

```python
    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return ``(tag_keys, data_keys)``.

        When ``columns.system_tags`` is ``True`` (or ``all_info=True``), the two
        system-tag columns for this node's schema are appended to ``tag_keys``.
        Their names are deterministic from the declared schemas and match what a
        concrete source with the same schema would produce.

        Other ``ColumnConfig`` flags (``meta``, ``context``, ``source``,
        ``content_hash``, ``sort_by_tags``) are no-ops at the node level —
        consistent with ``ArrowTableStream.keys()`` which also ignores them.

        Args:
            columns: Column selection config.
            all_info: If ``True``, equivalent to ``ColumnConfig(system_tags=True)``
                for this method.

        Returns:
            Tuple of ``(tag_column_names, data_column_names)``.
        """
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        tag_keys = tuple(self._tag_schema.keys())
        if columns_config.system_tags:
            tag_keys += system_tag_column_names(self._schema_hash())
        return tag_keys, tuple(self._data_schema.keys())
```

- [ ] **Step 6: Replace `SourceNodeBase.output_schema()` body**

Find `SourceNodeBase.output_schema()` (currently around line 142). Replace its body and docstring with:

```python
    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return ``(tag_schema, data_schema)``.

        When ``columns.system_tags`` is ``True`` (or ``all_info=True``), the
        returned tag schema is extended with the two system-tag entries for this
        node's schema (both typed as ``str``). Their names are deterministic from
        the declared schemas and match what a concrete source with the same schema
        would produce.

        Other ``ColumnConfig`` flags (``meta``, ``context``, ``source``,
        ``content_hash``, ``sort_by_tags``) are no-ops at the node level —
        consistent with ``ArrowTableStream.output_schema()`` which also ignores them.

        Args:
            columns: Column selection config.
            all_info: If ``True``, equivalent to ``ColumnConfig(system_tags=True)``
                for this method.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        tag_schema = self._tag_schema
        if columns_config.system_tags:
            source_id_col, record_id_col = system_tag_column_names(self._schema_hash())
            tag_schema = Schema(
                {**dict(tag_schema), source_id_col: str, record_id_col: str}
            )
        return tag_schema, self._data_schema
```

- [ ] **Step 7: Run the new tests**

```bash
uv run pytest tests/test_core/nodes/test_source_node.py::TestSourceNodeColumnConfig -v
```

Expected: All 5 tests PASS.

- [ ] **Step 8: Run the full source_node test file to check for regressions**

```bash
uv run pytest tests/test_core/nodes/test_source_node.py -v
```

Expected: All tests PASS.

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/core/nodes/source_node.py tests/test_core/nodes/test_source_node.py
git commit -m "fix(source_node): SourceNodeBase.keys/output_schema honour system_tags column config"
```

---

## Task 5: Add `SourceJobNode.keys()` and `output_schema()` overrides (bound forwarding)

**Files:**
- Modify: `tests/test_core/nodes/test_source_node.py`
- Modify: `src/orcapod/core/nodes/source_node.py`

- [ ] **Step 1: Write the failing tests**

Open `tests/test_core/nodes/test_source_node.py`. Append to the `TestSourceNodeColumnConfig` class:

```python
    def test_unbound_job_node_keys_default_unchanged(self, tag_schema, data_schema):
        """Regression: unbound SourceJobNode.keys() still returns plain schema keys."""
        from orcapod.core.nodes.source_node import SourceJobNode

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        tag_keys, data_keys = job_node.keys()
        assert tag_keys == ("id",)
        assert data_keys == ("value",)

    def test_unbound_system_tag_names_match_bound(self, tag_schema, data_schema):
        """Unbound SJN.keys(system_tags=True) returns the same names as the equivalent bound source."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource
        from orcapod.types import ColumnConfig

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        unbound = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        bound = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, bound_source=src
        )

        unbound_keys = unbound.keys(columns=ColumnConfig(system_tags=True))
        bound_keys = bound.keys(columns=ColumnConfig(system_tags=True))
        assert unbound_keys == bound_keys

    def test_bound_keys_delegates_to_source(self, tag_schema, data_schema):
        """Bound SourceJobNode.keys() delegates to bound_source.keys()."""
        from unittest.mock import MagicMock
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.types import ColumnConfig

        mock_source = MagicMock()
        mock_source.output_schema.return_value = (tag_schema, data_schema)
        mock_source.keys.return_value = (("id",), ("value",))

        job_node = SourceJobNode(name="x", bound_source=mock_source)
        cfg = ColumnConfig(system_tags=True)
        job_node.keys(columns=cfg, all_info=False)

        mock_source.keys.assert_called_once_with(columns=cfg, all_info=False)

    def test_bound_keys_all_info_matches_source_directly(self, tag_schema, data_schema):
        """Bound SJN.keys(all_info=True) == bound_source.keys(all_info=True)."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, bound_source=src
        )

        assert job_node.keys(all_info=True) == src.keys(all_info=True)

    def test_bound_output_schema_delegates_to_source(self, tag_schema, data_schema):
        """Bound SourceJobNode.output_schema() delegates to bound_source.output_schema()."""
        from unittest.mock import MagicMock
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.types import ColumnConfig

        mock_source = MagicMock()
        mock_source.output_schema.return_value = (tag_schema, data_schema)

        job_node = SourceJobNode(name="x", bound_source=mock_source)
        cfg = ColumnConfig(system_tags=True)
        job_node.output_schema(columns=cfg, all_info=False)

        mock_source.output_schema.assert_called_once_with(columns=cfg, all_info=False)

    def test_bound_output_schema_all_info_matches_source_directly(self, tag_schema, data_schema):
        """Bound SJN.output_schema(all_info=True) == bound_source.output_schema(all_info=True)."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, bound_source=src
        )

        assert job_node.output_schema(all_info=True) == src.output_schema(all_info=True)
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/test_core/nodes/test_source_node.py::TestSourceNodeColumnConfig -v -k "bound"
```

Expected: The `test_bound_keys_delegates_to_source` and `test_bound_output_schema_delegates_to_source` tests FAIL because `SourceJobNode` has no override — the mock's `keys` is never called.

- [ ] **Step 3: Add `keys()` and `output_schema()` overrides to `SourceJobNode`**

Open `src/orcapod/core/nodes/source_node.py`. Inside `SourceJobNode`, find the `as_table()` override (currently around line 524). Add the two new overrides **before** `as_table()`:

```python
    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return ``(tag_keys, data_keys)``, delegating to ``bound_source`` when set.

        When ``bound_source`` is present, this is a transparent pass-through to
        ``bound_source.keys(columns=columns, all_info=all_info)`` so callers get
        the same result as querying the source directly.

        When unbound, delegates to ``SourceNodeBase.keys()`` which computes
        system-tag column names from the declared schemas.

        Args:
            columns: Column selection config.
            all_info: If ``True``, include all available column groups.

        Returns:
            Tuple of ``(tag_column_names, data_column_names)``.
        """
        if self._bound_source is None:
            return super().keys(columns=columns, all_info=all_info)
        return self._bound_source.keys(columns=columns, all_info=all_info)

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return ``(tag_schema, data_schema)``, delegating to ``bound_source`` when set.

        When ``bound_source`` is present, this is a transparent pass-through to
        ``bound_source.output_schema(columns=columns, all_info=all_info)`` so callers
        get the same result as querying the source directly.

        When unbound, delegates to ``SourceNodeBase.output_schema()`` which includes
        system-tag schema entries derived from the declared schemas.

        Args:
            columns: Column selection config.
            all_info: If ``True``, include all available column groups.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        if self._bound_source is None:
            return super().output_schema(columns=columns, all_info=all_info)
        return self._bound_source.output_schema(columns=columns, all_info=all_info)
```

- [ ] **Step 4: Run all new tests**

```bash
uv run pytest tests/test_core/nodes/test_source_node.py::TestSourceNodeColumnConfig -v
```

Expected: All 11 tests PASS.

- [ ] **Step 5: Run the entire test suite for the affected modules**

```bash
uv run pytest tests/test_core/nodes/ tests/test_core/sources/ tests/test_utils/ -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/source_node.py tests/test_core/nodes/test_source_node.py
git commit -m "feat(source_node): SourceJobNode.keys/output_schema forward column_config to bound_source"
```

---

## Task 6: Full regression run and PR

- [ ] **Step 1: Run the complete test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: All tests PASS, zero failures.

- [ ] **Step 2: Push branch and open PR**

```bash
git push -u origin eywalker/eng-516-sourcejobnode-and-sourcenode-should-accept-forward
```

Open a PR against the `dev` branch with title:
`fix(source_node): forward column_config/all_info in keys() and output_schema()`

PR description must include:
- `Fixes ENG-516`
- Summary of changes: shared utilities added, SourceNodeBase fixed, SourceJobNode overrides added
- Reference to ENG-576 and ENG-577 as related follow-ups
