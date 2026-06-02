# ENG-377: FunctionJobNode Fetch-Join Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the duplicated "fetch both DBs → content-hash-filter → inner-join → restore nullability" logic from `get_all_records` and `_load_cached_entries` into a single private helper `_fetch_joined_records`, eliminating the maintenance risk of two diverging copies.

**Architecture:** Add a module-level `_PIPELINE_ENTRY_ID_COL` constant and a `_JoinedRecords` NamedTuple. Add `_fetch_joined_records(entry_ids=None) -> _JoinedRecords | None` to `FunctionJobNode`. Refactor `_load_cached_entries` and `get_all_records` to delegate the fetch-and-join work to it. Update all four method docstrings to clarify the invocation chain.

**Tech Stack:** Python, PyArrow (`pa`), Polars (`pl`), `NamedTuple` from `typing`. All commands via `uv run`.

**Spec:** `superpowers/specs/2026-06-02-function-node-fetch-join-refactor-design.md`

---

## File Structure

| Action | Path | What changes |
|---|---|---|
| Modify | `src/orcapod/core/nodes/function_node.py` | Add `NamedTuple` import; add `_PIPELINE_ENTRY_ID_COL` constant; add `_JoinedRecords` NamedTuple; add `_fetch_joined_records`; refactor `_load_cached_entries`; refactor `get_all_records`; update 4 docstrings |
| Create | `tests/test_core/nodes/test_function_node_fetch_joined.py` | Unit tests for `_fetch_joined_records` |

---

## Task 1: Create Feature Branch

**Files:** none (git only)

- [ ] **Step 1.1: Check out the branch**

```bash
cd /home/kurouto/kurouto-jobs/711e18aa-7748-4686-8634-4f858cb0e9ed/orcapod-python
git checkout -b eywalker/eng-377-investigate-logic-redundancy-between-get_all_records-and
git branch --show-current
```

Expected output: `eywalker/eng-377-investigate-logic-redundancy-between-get_all_records-and`

---

## Task 2: Write Failing Tests for `_fetch_joined_records`

**Files:**
- Create: `tests/test_core/nodes/test_function_node_fetch_joined.py`

The tests call `_fetch_joined_records` which does not exist yet. They should fail with `AttributeError: 'FunctionJobNode' object has no attribute '_fetch_joined_records'`. They also import `_PIPELINE_ENTRY_ID_COL` which does not exist yet, so the import itself will fail. Both are the correct "red" state.

- [ ] **Step 2.1: Create the test file**

```python
# tests/test_core/nodes/test_function_node_fetch_joined.py
"""Tests for FunctionJobNode._fetch_joined_records."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode, _PIPELINE_ENTRY_ID_COL
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase


def double_value(value: int) -> int:
    return value * 2


@pytest.fixture
def node_without_db():
    """FunctionJobNode with no databases attached."""
    table = pa.table(
        {
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    return FunctionJobNode(pod, src)


@pytest.fixture
def node_with_empty_db():
    """FunctionJobNode with databases attached but no data executed."""
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    return FunctionJobNode(
        pod,
        src,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=InMemoryArrowDatabase(),
    )


@pytest.fixture
def executed_node(node_with_empty_db):
    """FunctionJobNode with databases attached and both input rows executed."""
    node = node_with_empty_db
    for tag, data in node._input_stream.iter_data():
        node.execute_data(tag, data)
    return node


class TestFetchJoinedRecords:
    def test_returns_none_when_no_db(self, node_without_db):
        """Returns None when no databases are attached."""
        assert node_without_db._fetch_joined_records() is None

    def test_returns_none_when_db_fetch_returns_none(self, node_with_empty_db):
        """Returns None when the pipeline DB returns None (no records written yet)."""
        assert node_with_empty_db._fetch_joined_records() is None

    def test_returned_table_includes_pipeline_entry_id_column(self, executed_node):
        """The returned table always contains __pipeline_entry_id."""
        result = executed_node._fetch_joined_records()
        assert result is not None
        assert _PIPELINE_ENTRY_ID_COL in result.table.column_names

    def test_taginfo_columns_present_in_result(self, executed_node):
        """taginfo_columns in the returned NamedTuple is a non-empty tuple of strings."""
        result = executed_node._fetch_joined_records()
        assert result is not None
        assert isinstance(result.taginfo_columns, tuple)
        assert len(result.taginfo_columns) > 0

    def test_no_entry_ids_returns_all_rows(self, executed_node):
        """Calling with entry_ids=None returns all executed rows."""
        result = executed_node._fetch_joined_records()
        assert result is not None
        assert result.table.num_rows == 2

    def test_entry_ids_filter_narrows_rows(self, executed_node):
        """Passing a single entry_id returns only that row."""
        node = executed_node
        input_pairs = list(node._input_stream.iter_data())
        entry_id_0 = node.compute_pipeline_entry_id(input_pairs[0][0], input_pairs[0][1])

        result = node._fetch_joined_records(entry_ids=[entry_id_0])
        assert result is not None
        assert result.table.num_rows == 1
        assert result.table.column(_PIPELINE_ENTRY_ID_COL)[0].as_py() == entry_id_0
```

- [ ] **Step 2.2: Run the tests and verify they fail at import**

```bash
uv run pytest tests/test_core/nodes/test_function_node_fetch_joined.py -v 2>&1 | head -30
```

Expected: `ImportError` or `AttributeError` mentioning `_PIPELINE_ENTRY_ID_COL` or `_fetch_joined_records`. The tests must be red before proceeding.

---

## Task 3: Implement `_PIPELINE_ENTRY_ID_COL`, `_JoinedRecords`, and `_fetch_joined_records`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

### Step 3.1 — Add `NamedTuple` to the `typing` import

- [ ] **Step 3.1: Edit line 22 of `function_node.py`**

Find:
```python
from typing import TYPE_CHECKING, Any, Literal, cast
```

Replace with:
```python
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, cast
```

### Step 3.2 — Add the `_PIPELINE_ENTRY_ID_COL` module-level constant

- [ ] **Step 3.2: Add constant after the `LazyModule` definitions (after line 68, before the `_executor_supports_concurrent` function)**

Find:
```python
if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")


def _executor_supports_concurrent(
```

Replace with:
```python
if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")

# Pipeline entry ID column name used when fetching records from the pipeline
# database. Always present in the table returned by _fetch_joined_records.
_PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"


def _executor_supports_concurrent(
```

### Step 3.3 — Add `_JoinedRecords` NamedTuple before `FunctionJobNode`

- [ ] **Step 3.3: Locate the `FunctionJobNode` class definition (around line 684) and add `_JoinedRecords` immediately before it**

Find the line that reads:
```python
class FunctionJobNode(FunctionNodeBase):
```

Insert the following block immediately before that line (one blank line separation):

```python
class _JoinedRecords(NamedTuple):
    """Internal result type returned by ``_fetch_joined_records``.

    Attributes:
        table: The joined ``pa.Table``, always including a
            ``__pipeline_entry_id`` column (the pipeline DB row key) and
            ``DATA_RECORD_ID``. Does not have ``ColumnConfig`` filtering
            applied — that is the caller's responsibility.
        taginfo_columns: Column names from the pipeline database fetch,
            captured before the join. Used by ``_load_cached_entries`` to
            derive tag keys in the CACHE_ONLY (``_input_stream is None``)
            fallback path, where the tag columns cannot be inferred from the
            input stream and must be identified by exclusion from the taginfo
            column set.
    """

    table: pa.Table
    taginfo_columns: tuple[str, ...]


```

### Step 3.4 — Add `_fetch_joined_records` to `FunctionJobNode`

- [ ] **Step 3.4: Add `_fetch_joined_records` inside `FunctionJobNode`, in the `# Cache-only helpers` section, immediately before `_load_cached_entries`**

Find the block that starts with:
```python
    # ------------------------------------------------------------------
    # Cache-only helpers (PLT-1156)
    # ------------------------------------------------------------------

    def _load_cached_entries(
```

Replace with:
```python
    # ------------------------------------------------------------------
    # Cache-only helpers (PLT-1156)
    # ------------------------------------------------------------------

    def _fetch_joined_records(
        self,
        entry_ids: list[str] | None = None,
    ) -> _JoinedRecords | None:
        """Internal primitive: fetch both DBs, content-hash-filter, and inner-join.

        Fetches ``taginfo`` from the pipeline database with
        ``_PIPELINE_ENTRY_ID_COL`` as the row-key column, fetches ``results``
        from the result database, applies ``_filter_by_content_hash``, and
        inner-joins the two tables on ``DATA_RECORD_ID`` via polars.

        If ``entry_ids`` is provided, the polars DataFrame is filtered to
        matching ``_PIPELINE_ENTRY_ID_COL`` values before conversion to Arrow,
        avoiding a round-trip.

        Does NOT apply ``ColumnConfig`` column dropping (that is
        ``get_all_records``'s job), convert rows to ``(tag, data)`` tuples
        (that is ``_load_cached_entries``'s job), or touch the in-memory cache
        (that is ``get_cached_results``'s job).

        Args:
            entry_ids: If given, return only rows whose
                ``_PIPELINE_ENTRY_ID_COL`` value is in this list.
                If ``None``, return all rows.

        Returns:
            A ``_JoinedRecords`` whose ``table`` always includes a
            ``_PIPELINE_ENTRY_ID_COL`` column, or ``None`` when either
            database is absent or either DB fetch returns ``None``. A 0-row
            table is returned (not ``None``) when both fetches succeed but the
            join finds no matching rows — callers check ``num_rows``
            themselves.
        """
        if self._cached_function_pod is None or self._pipeline_database is None:
            return None

        taginfo = self._pipeline_database.get_all_records(
            self.node_identity_path,
            record_id_column=_PIPELINE_ENTRY_ID_COL,
        )
        results = self._cached_function_pod._result_database.get_all_records(
            self._cached_function_pod.record_path,
            record_id_column=constants.DATA_RECORD_ID,
        )

        if taginfo is None or results is None:
            return None

        taginfo_columns = tuple(taginfo.column_names)
        taginfo = self._filter_by_content_hash(taginfo)
        taginfo_schema = taginfo.schema
        results_schema = results.schema

        joined_df = pl.DataFrame(taginfo).join(
            pl.DataFrame(results),
            on=constants.DATA_RECORD_ID,
            how="inner",
        )
        if entry_ids is not None:
            joined_df = joined_df.filter(
                pl.col(_PIPELINE_ENTRY_ID_COL).is_in(entry_ids)
            )
        joined = joined_df.to_arrow()
        joined = arrow_utils.restore_schema_nullability(
            joined, taginfo_schema, results_schema
        )
        return _JoinedRecords(table=joined, taginfo_columns=taginfo_columns)

    def _load_cached_entries(
```

- [ ] **Step 3.5: Run only the new tests to verify they pass**

```bash
uv run pytest tests/test_core/nodes/test_function_node_fetch_joined.py -v
```

Expected: all 6 tests **PASS**.

- [ ] **Step 3.6: Run the existing get_cached tests to confirm nothing broke**

```bash
uv run pytest tests/test_core/nodes/test_function_node_get_cached.py -v
```

Expected: all 5 tests **PASS**.

- [ ] **Step 3.7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py \
        tests/test_core/nodes/test_function_node_fetch_joined.py
git commit -m "$(cat <<'EOF'
refactor(function_node): add _fetch_joined_records primitive and tests

Extracts the fetch-and-join logic shared between get_all_records and
_load_cached_entries into a new _fetch_joined_records helper. Also adds
_PIPELINE_ENTRY_ID_COL module-level constant and _JoinedRecords NamedTuple.
Neither caller is wired up yet — that follows in the next two commits.

Closes ENG-377

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Refactor `_load_cached_entries` to Use `_fetch_joined_records`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (lines ~1442–1527)

The current body of `_load_cached_entries` duplicates the fetch-and-join logic. Replace the entire body with a delegation to `_fetch_joined_records`, keeping only the tuple-conversion step.

Note: `taginfo_columns` from `_JoinedRecords` is used in the `_input_stream is None` fallback path to derive `tag_keys` — this replaces the direct reference to `taginfo.column_names` that existed before the refactor.

- [ ] **Step 4.1: Replace the body of `_load_cached_entries`**

Find the entire current method body (from `def _load_cached_entries` through to just before `async def _async_execute_cache_only`):

```python
    def _load_cached_entries(
        self,
        entry_ids: list[str] | None = None,
    ) -> "dict[str, tuple[TagProtocol, DataProtocol]]":
        """Load (tag, data) pairs from pipeline DB + result DB.

        Args:
            entry_ids: If provided, load only these specific entry IDs.
                If ``None``, load all records for this node.

        Returns:
            dict mapping entry_id → (tag, data). Empty dict when either
            database is None, records are empty, or no rows match.

        Does NOT mutate ``_cached_output_datas``.
        Callers merge via ``self._cached_output_datas.update(loaded)``.
        """
        if self._cached_function_pod is None or self._pipeline_database is None:
            return {}

        PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"

        taginfo = self._pipeline_database.get_all_records(
            self.node_identity_path,
            record_id_column=PIPELINE_ENTRY_ID_COL,
        )
        results = self._cached_function_pod._result_database.get_all_records(
            self._cached_function_pod.record_path,
            record_id_column=constants.DATA_RECORD_ID,
        )

        if taginfo is None or results is None:
            return {}

        taginfo = self._filter_by_content_hash(taginfo)
        taginfo_schema = taginfo.schema
        results_schema = results.schema

        joined_df = pl.DataFrame(taginfo).join(
            pl.DataFrame(results),
            on=constants.DATA_RECORD_ID,
            how="inner",
        )
        if entry_ids is not None:
            joined_df = joined_df.filter(
                pl.col(PIPELINE_ENTRY_ID_COL).is_in(entry_ids)
            )
        joined = joined_df.to_arrow()
        joined = arrow_utils.restore_schema_nullability(
            joined, taginfo_schema, results_schema
        )

        if joined.num_rows == 0:
            return {}

        # Derive tag keys: prefer input_stream when available; fall back to
        # taginfo column exclusion for CACHE_ONLY / deserialized nodes.
        if self._input_stream is not None:
            tag_keys = self._input_stream.keys()[0]
        else:
            tag_keys = tuple(
                c
                for c in taginfo.column_names
                if not c.startswith(constants.META_PREFIX)
                and not c.startswith(constants.SOURCE_PREFIX)
                and not c.startswith(constants.SYSTEM_TAG_PREFIX)
                and c != PIPELINE_ENTRY_ID_COL
                and c != constants.NODE_CONTENT_HASH_COL
            )

        # Drop internal columns (SOURCE_PREFIX is kept — ArrowTableStream needs it)
        entry_ids_col = joined.column(PIPELINE_ENTRY_ID_COL).to_pylist()
        drop_cols = [
            c
            for c in joined.column_names
            if c.startswith(constants.META_PREFIX)
            or c == PIPELINE_ENTRY_ID_COL
            or c == constants.NODE_CONTENT_HASH_COL
        ]
        data_table = joined.drop([c for c in drop_cols if c in joined.column_names])
        stream = ArrowTableStream(data_table, tag_columns=tag_keys)

        loaded: dict[str, tuple[TagProtocol, DataProtocol]] = {}
        for eid, (tag, data) in zip(entry_ids_col, stream.iter_data()):
            loaded[eid] = (tag, data)
        return loaded
```

Replace with:

```python
    def _load_cached_entries(
        self,
        entry_ids: list[str] | None = None,
    ) -> "dict[str, tuple[TagProtocol, DataProtocol]]":
        """DB loader: fetch ``(tag, data)`` pairs from the pipeline and result databases.

        Calls ``_fetch_joined_records`` to obtain the raw joined table, then
        converts each row into a ``(tag, data)`` tuple keyed by pipeline entry ID.

        If ``entry_ids`` is given, only those entries are fetched from DB.
        If ``None``, all records for this node are loaded.

        Does NOT read from or write to the in-memory cache
        (``_cached_output_datas``). Callers that want to populate the cache
        must call ``self._cached_output_datas.update(loaded)`` themselves.

        Does NOT apply user-facing column filtering — see ``get_all_records``
        for that.

        Args:
            entry_ids: If provided, load only these specific entry IDs.
                If ``None``, load all records for this node.

        Returns:
            dict mapping entry_id → ``(tag, data)``. Empty dict when either
            database is absent, either DB fetch returns ``None``, or no rows
            match after joining.
        """
        fetched = self._fetch_joined_records(entry_ids=entry_ids)
        if fetched is None or fetched.table.num_rows == 0:
            return {}

        joined = fetched.table

        # Derive tag keys: prefer input_stream when available; fall back to
        # taginfo column exclusion for CACHE_ONLY / deserialized nodes.
        # taginfo_columns from _fetch_joined_records preserves the pipeline DB
        # column names before joining, which is the correct exclusion set.
        if self._input_stream is not None:
            tag_keys = self._input_stream.keys()[0]
        else:
            tag_keys = tuple(
                c
                for c in fetched.taginfo_columns
                if not c.startswith(constants.META_PREFIX)
                and not c.startswith(constants.SOURCE_PREFIX)
                and not c.startswith(constants.SYSTEM_TAG_PREFIX)
                and c != _PIPELINE_ENTRY_ID_COL
                and c != constants.NODE_CONTENT_HASH_COL
            )

        # Drop internal columns (SOURCE_PREFIX is kept — ArrowTableStream needs it)
        entry_ids_col = joined.column(_PIPELINE_ENTRY_ID_COL).to_pylist()
        drop_cols = [
            c
            for c in joined.column_names
            if c.startswith(constants.META_PREFIX)
            or c == _PIPELINE_ENTRY_ID_COL
            or c == constants.NODE_CONTENT_HASH_COL
        ]
        data_table = joined.drop([c for c in drop_cols if c in joined.column_names])
        stream = ArrowTableStream(data_table, tag_columns=tag_keys)

        loaded: dict[str, tuple[TagProtocol, DataProtocol]] = {}
        for eid, (tag, data) in zip(entry_ids_col, stream.iter_data()):
            loaded[eid] = (tag, data)
        return loaded
```

- [ ] **Step 4.2: Run all existing cached-results tests**

```bash
uv run pytest tests/test_core/nodes/ -v
```

Expected: all tests in `test_function_node_get_cached.py` and `test_function_node_fetch_joined.py` **PASS**.

- [ ] **Step 4.3: Run the broader function_node integration tests**

```bash
uv run pytest tests/test_core/function_pod/ tests/test_core/test_caching_integration.py -v
```

Expected: all tests **PASS**.

- [ ] **Step 4.4: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "$(cat <<'EOF'
refactor(function_node): wire _load_cached_entries to _fetch_joined_records

Removes the duplicated fetch-and-join block from _load_cached_entries.
The fallback CACHE_ONLY tag_keys derivation now uses
_JoinedRecords.taginfo_columns (the pre-join pipeline DB column names)
instead of a local taginfo reference, preserving correct semantics.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Refactor `get_all_records` to Use `_fetch_joined_records`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (lines ~1355–1415)

Two important notes:
1. The original `get_all_records` fetched taginfo without `record_id_column`, so `__pipeline_entry_id` was not in the table. Now it is. It starts with `__` (`META_PREFIX`), so the default column-config drop already removes it. But when `all_info=True`, meta columns are kept — so `__pipeline_entry_id` must be explicitly added to the always-drop list alongside `NODE_CONTENT_HASH_COL`.
2. The guard `if self._cached_function_pod is None` is dropped; `_fetch_joined_records` handles it.

- [ ] **Step 5.1: Replace the body of `get_all_records`**

Find the entire current `get_all_records` method:

```python
    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table | None:
        """Return all computed results joined with their pipeline tag records.

        Args:
            columns: Column configuration controlling which groups are included.
            all_info: Shorthand to include all info columns.

        Returns:
            A PyArrow table of joined results, or ``None`` if no database is
            attached or no records exist.
        """
        if self._cached_function_pod is None:
            return None

        results = self._cached_function_pod._result_database.get_all_records(
            self._cached_function_pod.record_path,
            record_id_column=constants.DATA_RECORD_ID,
        )
        taginfo = self._pipeline_database.get_all_records(self.node_identity_path)

        if results is None or taginfo is None:
            return None

        taginfo = self._filter_by_content_hash(taginfo)
        taginfo_schema = taginfo.schema
        results_schema = results.schema
        joined = (
            pl.DataFrame(taginfo)
            .join(pl.DataFrame(results), on=constants.DATA_RECORD_ID, how="inner")
            .to_arrow()
        )
        joined = arrow_utils.restore_schema_nullability(joined, taginfo_schema, results_schema)

        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        drop_columns = []
        # Always drop the node content hash column — it is an internal
        # row-level discriminator, not a user-facing column.
        drop_columns.append(constants.NODE_CONTENT_HASH_COL)
        if not column_config.meta and not column_config.all_info:
            drop_columns.extend(
                c for c in joined.column_names if c.startswith(constants.META_PREFIX)
            )
        if not column_config.source and not column_config.all_info:
            drop_columns.extend(
                c for c in joined.column_names if c.startswith(constants.SOURCE_PREFIX)
            )
        if not column_config.system_tags and not column_config.all_info:
            drop_columns.extend(
                c
                for c in joined.column_names
                if c.startswith(constants.SYSTEM_TAG_PREFIX)
            )
        if drop_columns:
            joined = joined.drop([c for c in drop_columns if c in joined.column_names])

        return joined if joined.num_rows > 0 else None
```

Replace with:

```python
    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table | None:
        """Public table view: return all computed results joined with their pipeline records.

        Calls ``_fetch_joined_records`` to obtain the raw joined table, then
        applies ``ColumnConfig``-driven column dropping to produce a
        user-facing result. ``_PIPELINE_ENTRY_ID_COL`` and
        ``NODE_CONTENT_HASH_COL`` are always dropped — they are internal
        discriminator columns, not user-facing data.

        Does NOT populate the in-memory cache — see ``get_cached_results``
        for that.

        Args:
            columns: Column configuration controlling which groups are
                included. Accepts a ``ColumnConfig`` instance or a dict
                shorthand (e.g. ``{"meta": True}``).
            all_info: If ``True``, equivalent to enabling all column groups.

        Returns:
            A ``pa.Table`` of joined results, or ``None`` if no database is
            attached, either DB fetch returns ``None``, or the join produces
            no rows.
        """
        fetched = self._fetch_joined_records()
        if fetched is None:
            return None

        joined = fetched.table
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        # Always drop internal discriminator columns regardless of column_config.
        # _PIPELINE_ENTRY_ID_COL starts with META_PREFIX so it is covered by
        # the meta drop in the default case, but must be listed explicitly
        # here so it is also dropped when all_info=True (which skips the
        # meta-prefix sweep).
        drop_columns = [constants.NODE_CONTENT_HASH_COL, _PIPELINE_ENTRY_ID_COL]
        if not column_config.meta and not column_config.all_info:
            drop_columns.extend(
                c for c in joined.column_names if c.startswith(constants.META_PREFIX)
            )
        if not column_config.source and not column_config.all_info:
            drop_columns.extend(
                c for c in joined.column_names if c.startswith(constants.SOURCE_PREFIX)
            )
        if not column_config.system_tags and not column_config.all_info:
            drop_columns.extend(
                c
                for c in joined.column_names
                if c.startswith(constants.SYSTEM_TAG_PREFIX)
            )
        if drop_columns:
            joined = joined.drop([c for c in drop_columns if c in joined.column_names])

        return joined if joined.num_rows > 0 else None
```

- [ ] **Step 5.2: Run the full node and function_pod test suites**

```bash
uv run pytest tests/test_core/nodes/ tests/test_core/function_pod/ tests/test_core/test_caching_integration.py -v
```

Expected: all tests **PASS**.

- [ ] **Step 5.3: Run the broader pipeline tests to catch any regressions**

```bash
uv run pytest tests/test_pipeline/ -v
```

Expected: all tests **PASS**.

- [ ] **Step 5.4: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "$(cat <<'EOF'
refactor(function_node): wire get_all_records to _fetch_joined_records

Removes the duplicated fetch-and-join block from get_all_records.
Adds _PIPELINE_ENTRY_ID_COL to the always-drop list so it is excluded
even when all_info=True (where the META_PREFIX sweep is skipped).

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Update Docstrings on `get_cached_results` and Final Test Run

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (lines ~1161–1194)

`get_all_records` and `_load_cached_entries` already received updated docstrings in Tasks 4–5. This task updates `get_cached_results` (the public cache façade) to describe its role in the chain. `_fetch_joined_records` already has a full docstring from Task 3.

- [ ] **Step 6.1: Update the `get_cached_results` docstring**

Find:
```python
    def get_cached_results(
        self, entry_ids: list[str]
    ) -> dict[str, tuple[TagProtocol, DataProtocol]]:
        """Retrieve cached results for specific pipeline entry IDs.

        Checks in-memory cache first. Loads only truly missing entries from DB.
        Add-only semantics: existing in-memory entries are never cleared or
        overwritten (overwrite is safe since in-memory and DB entries for the
        same entry_id are always semantically equivalent).

        Args:
            entry_ids: Pipeline entry IDs to look up.

        Returns:
            Mapping from entry_id to ``(tag, output_data)`` for found entries.
            Empty dict if no DB is attached or no matches found.
        """
```

Replace with:
```python
    def get_cached_results(
        self, entry_ids: list[str]
    ) -> dict[str, tuple[TagProtocol, DataProtocol]]:
        """Public cache façade: return already-computed results for the given entry IDs.

        Serves hits directly from the in-memory cache (``_cached_output_datas``).
        For IDs not yet cached, delegates to ``_load_cached_entries`` which calls
        ``_fetch_joined_records`` to load from the pipeline and result databases.
        Add-only semantics: existing in-memory entries are never cleared or
        overwritten (safe because in-memory and DB entries for the same entry_id
        are always semantically equivalent).

        Does NOT apply user-facing column filtering — see ``get_all_records``
        for that.

        Args:
            entry_ids: Pipeline entry IDs to look up.

        Returns:
            Mapping from entry_id to ``(tag, output_data)`` for found entries.
            Empty dict if no DB is attached, ``entry_ids`` is empty, or no
            matches are found.
        """
```

- [ ] **Step 6.2: Run the full test suite**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -30
```

Expected: all tests **PASS**. If any fail, read the traceback and fix before committing.

- [ ] **Step 6.3: Commit the docstring update**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "$(cat <<'EOF'
docs(function_node): update get_cached_results docstring to reflect invocation chain

Clarifies that get_cached_results delegates cache misses to _load_cached_entries,
which in turn calls _fetch_joined_records. Completes ENG-377 docstring pass.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task that covers it |
|---|---|
| Extract `_fetch_joined_records` | Task 3 |
| Module-level `_PIPELINE_ENTRY_ID_COL` constant | Task 3 |
| `_JoinedRecords` NamedTuple with `table` + `taginfo_columns` | Task 3 |
| `_load_cached_entries` delegates to `_fetch_joined_records` | Task 4 |
| `get_all_records` delegates to `_fetch_joined_records` | Task 5 |
| Guard normalization (both callers drop individual guards) | Tasks 4 + 5 |
| `_PIPELINE_ENTRY_ID_COL` always dropped in `get_all_records` | Task 5 |
| Docstrings on all four methods | Tasks 3, 4, 5, 6 |
| New `test_function_node_fetch_joined.py` | Task 2 |
| Existing `TestGetCachedResults` tests pass | Verified in Tasks 3, 4, 5 |

**Placeholder scan:** No TBDs, no "see above", no unresolved references. All code blocks are complete. ✓

**Type consistency:** `_JoinedRecords` defined in Task 3 with `.table: pa.Table` and `.taginfo_columns: tuple[str, ...]`. Both Task 4 (`fetched.table`, `fetched.taginfo_columns`) and Task 5 (`fetched.table`) use these exact attributes. `_PIPELINE_ENTRY_ID_COL` used consistently across Tasks 3, 4, and 5. ✓
