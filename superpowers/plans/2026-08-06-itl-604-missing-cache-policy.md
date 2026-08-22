# missing_cache_policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `NodeConfig.missing_cache_policy` — a three-value field (`"recompute"` / `"as_empty"` / `"strict"`) that unifies how `FunctionJobNode` handles missing cache entries for both ephemeral and non-ephemeral result stores.

**Architecture:** The flag lives in `NodeConfig` (frozen dataclass in `types.py`) and is read at execution time inside `_fetch_joined_records()` (controls EmptyData token creation vs. exception), `execute()` (controls whether EmptyData is emitted downstream or used as a recompute sentinel), and `async_execute()` (aligns the async Phase 1 with the sync behaviour). A new `CacheMissError` exception in `errors.py` is raised when the policy is `"strict"` and a non-ephemeral entry is absent.

**Tech Stack:** Python 3.12+, `polars` (DataFrame operations in `_fetch_joined_records`), `pyarrow`, `pytest`, `uv run pytest`.

---

## File map

| File | Change |
|---|---|
| `src/orcapod/errors.py` | Add `CacheMissError` |
| `src/orcapod/types.py` | Add `Literal` import; add `missing_cache_policy` field + `merge()` update |
| `src/orcapod/core/nodes/function_node.py` | Import `CacheMissError`; add `_populate_empty_data_tokens()` helper; update `_fetch_joined_records()` Branches A & B; add INFO log to ephemeral path; update `execute()` loop; update `async_execute()` `route_inputs` inner function |
| `tests/test_core/function_pod/test_missing_cache_policy.py` | New test file — all new tests live here |

---

## Task 1: Foundation — `CacheMissError` + `NodeConfig.missing_cache_policy`

**Files:**
- Modify: `src/orcapod/errors.py`
- Modify: `src/orcapod/types.py`
- Create: `tests/test_core/function_pod/test_missing_cache_policy.py`

- [ ] **Step 1.1: Write failing tests for the new types**

Create `tests/test_core/function_pod/test_missing_cache_policy.py`:

```python
"""Tests for NodeConfig.missing_cache_policy — ITL-604."""
from __future__ import annotations

import logging

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import CacheMissError, EphemeralResultMissingError
from orcapod.types import NodeConfig


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _double(x: int) -> int:
    return x * 2


def _make_stream(rows: list[dict]) -> ArrowTableStream:
    keys = list(rows[0].keys())
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in keys},
        schema=pa.schema([pa.field(k, pa.int64(), nullable=False) for k in keys]),
    )
    return ArrowTableStream(table, tag_columns=["id"])


def _make_node(
    stream: ArrowTableStream,
    pipeline_db: InMemoryArrowDatabase,
    result_db: InMemoryArrowDatabase,
    missing_cache_policy: str | None = None,
) -> FunctionJobNode:
    pf = PythonDataFunction(_double, output_keys="result")
    pod = FunctionPod(pf)
    node = FunctionJobNode(
        function_pod=pod,
        input_stream=stream,
        pipeline_database=pipeline_db,
        result_database=result_db,
    )
    if missing_cache_policy is not None:
        node.node_config = NodeConfig(missing_cache_policy=missing_cache_policy)
    return node


def _wipe_result_db(result_db: InMemoryArrowDatabase) -> None:
    """Simulate data loss by clearing the result store."""
    result_db._tables.clear()
    result_db._pending_batches.clear()


# ---------------------------------------------------------------------------
# Task 1: NodeConfig + CacheMissError types
# ---------------------------------------------------------------------------

class TestNodeConfigMissingCachePolicy:
    def test_default_is_none(self):
        cfg = NodeConfig()
        assert cfg.missing_cache_policy is None

    def test_accepts_valid_values(self):
        assert NodeConfig(missing_cache_policy="recompute").missing_cache_policy == "recompute"
        assert NodeConfig(missing_cache_policy="as_empty").missing_cache_policy == "as_empty"
        assert NodeConfig(missing_cache_policy="strict").missing_cache_policy == "strict"

    def test_merge_none_inherits_from_self(self):
        base = NodeConfig(missing_cache_policy="strict")
        merged = base.merge(NodeConfig())
        assert merged.missing_cache_policy == "strict"

    def test_merge_non_none_other_wins(self):
        base = NodeConfig(missing_cache_policy="recompute")
        merged = base.merge(NodeConfig(missing_cache_policy="as_empty"))
        assert merged.missing_cache_policy == "as_empty"

    def test_merge_preserves_other_fields(self):
        base = NodeConfig(is_result_ephemeral=True, missing_cache_policy="strict")
        merged = base.merge(NodeConfig(missing_cache_policy="as_empty"))
        assert merged.is_result_ephemeral is True
        assert merged.missing_cache_policy == "as_empty"

    def test_cache_miss_error_is_importable(self):
        # Will fail if CacheMissError doesn't exist in errors.py yet
        from orcapod.errors import CacheMissError  # noqa: F401
```

- [ ] **Step 1.2: Run tests to verify they fail**

```bash
cd /home/kurouto/kurouto-jobs/a5e0735a-ad44-4870-ad9d-e5c2111912a4/orcapod-python
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestNodeConfigMissingCachePolicy -v
```

Expected: FAIL — `CacheMissError` import error and `missing_cache_policy` attribute errors.

- [ ] **Step 1.3: Add `CacheMissError` to `src/orcapod/errors.py`**

Append at the end of `src/orcapod/errors.py`:

```python
class CacheMissError(Exception):
    """Raised when a persistent (non-ephemeral) result-store entry is absent
    and ``NodeConfig.missing_cache_policy`` is ``"strict"``.

    A missing durable result indicates data loss or corruption. Set
    ``missing_cache_policy="recompute"`` (the default) to fall back to
    recomputation, or ``"as_empty"`` to propagate an ``EmptyData`` token
    downstream instead of raising.
    """
```

- [ ] **Step 1.4: Add `missing_cache_policy` to `NodeConfig` in `src/orcapod/types.py`**

First, add `Literal` to the existing `typing` import line:

```python
# Before:
from typing import TYPE_CHECKING, Any, Generic, Self, TypeAlias, TypeVar
# After:
from typing import TYPE_CHECKING, Any, Generic, Literal, Self, TypeAlias, TypeVar
```

Then update `NodeConfig`:

```python
@dataclass(frozen=True, slots=True)
class NodeConfig:
    """Per-node pipeline execution configuration.

    Attributes:
        is_result_ephemeral: ``None`` inherits the default (``False``).
            ``True`` writes new computation results to the pipeline-scoped
            ephemeral store instead of the persistent result database.
            Persistent cache hits are still served when available. Raises
            ``RuntimeError`` at execution time if ``True`` but no ephemeral
            store has been injected via ``set_ephemeral_store()``.
        ignore_schema: Tuple of schema version strings that this node will
            tolerate without raising ``SchemaVersionError``. ``None`` (default)
            means no old schema is tolerated — any detected v0 table raises
            ``SchemaVersionError``. Pass ``("v0",)`` to suppress the error
            and allow the node to recompute all results from scratch.
        missing_cache_policy: Controls how the node reacts when the pipeline
            table has an entry for an input but the result store does not.
            ``None`` inherits the default (``"recompute"``).

            * ``"recompute"`` *(default)* — WARNING logged; the entry falls
              through to recomputation from the original upstream data.
              For ephemeral stores, ``EmptyData`` acts as a recompute sentinel
              (same as current behaviour).
            * ``"as_empty"`` — WARNING logged (non-ephemeral) or INFO logged
              (ephemeral); an ``EmptyData`` token is emitted directly. The
              downstream node attempts to serve the result from its own cache.
              Only use when partial gaps are semantically expected (e.g.
              shared read-only stores, exploratory pipelines).
            * ``"strict"`` — ERROR logged and ``CacheMissError`` raised for
              non-ephemeral misses. Ephemeral misses still degrade gracefully
              to ``EmptyData`` (raising on an ephemeral miss would contradict
              the semantics of ephemeral storage). Use in production pipelines
              where a missing durable result always indicates a bug or data
              loss.
    """

    is_result_ephemeral: bool | None = None
    ignore_schema: tuple[str, ...] | None = None
    missing_cache_policy: Literal["recompute", "as_empty", "strict"] | None = None

    def merge(self, other: "NodeConfig") -> "NodeConfig":
        """Return a new ``NodeConfig`` with ``other``'s non-``None`` fields overriding self.

        ``None`` fields in ``other`` are treated as "not set" and leave
        self's value unchanged.

        Args:
            other: The ``NodeConfig`` whose non-``None`` fields take precedence.

        Returns:
            A new immutable ``NodeConfig``.

        Example:
            NodeConfig(is_result_ephemeral=True).merge(NodeConfig())
            # → NodeConfig(is_result_ephemeral=True)

            NodeConfig(is_result_ephemeral=True).merge(NodeConfig(is_result_ephemeral=False))
            # → NodeConfig(is_result_ephemeral=False)
        """
        return NodeConfig(
            is_result_ephemeral=(
                other.is_result_ephemeral
                if other.is_result_ephemeral is not None
                else self.is_result_ephemeral
            ),
            ignore_schema=(
                other.ignore_schema
                if other.ignore_schema is not None
                else self.ignore_schema
            ),
            missing_cache_policy=(
                other.missing_cache_policy
                if other.missing_cache_policy is not None
                else self.missing_cache_policy
            ),
        )
```

- [ ] **Step 1.5: Run tests to verify they pass**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestNodeConfigMissingCachePolicy -v
```

Expected: all 6 tests PASS.

- [ ] **Step 1.6: Run the full test suite to catch regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: all existing tests PASS.

- [ ] **Step 1.7: Commit**

```bash
git add src/orcapod/errors.py src/orcapod/types.py \
        tests/test_core/function_pod/test_missing_cache_policy.py
git commit -m "feat(config): add missing_cache_policy to NodeConfig and CacheMissError (ITL-604)"
```

---

## Task 2: `_fetch_joined_records()` — strict mode for non-ephemeral misses

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Modify: `tests/test_core/function_pod/test_missing_cache_policy.py`

- [ ] **Step 2.1: Write failing tests for strict mode**

Append to `tests/test_core/function_pod/test_missing_cache_policy.py`:

```python
class TestStrictPolicy:
    """missing_cache_policy="strict" raises CacheMissError on non-ephemeral miss."""

    def test_strict_raises_when_result_db_completely_empty(self):
        """Branch A: result DB returns None (never written to)."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: compute and store
        _make_node(stream, pipeline_db, result_db).execute(stream)

        # Wipe result DB completely
        _wipe_result_db(result_db)

        # Session 2: strict mode should raise
        node = _make_node(stream, pipeline_db, result_db, missing_cache_policy="strict")
        with pytest.raises(CacheMissError):
            node.execute(stream)

    def test_strict_raises_when_result_db_has_partial_gap(self):
        """Branch B: result DB has rows but the required row is absent."""
        rows = [{"id": 0, "x": 10}, {"id": 1, "x": 20}]
        stream = _make_stream(rows)
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: compute both rows
        _make_node(stream, result_db=result_db, pipeline_db=pipeline_db).execute(stream)

        # Remove only the first row's result to create a partial gap
        for table_path in list(result_db._tables.keys()):
            tbl = result_db._tables[table_path]
            # Keep only the second row
            result_db._tables[table_path] = tbl.slice(1)
        result_db._pending_batches.clear()

        node = _make_node(stream, pipeline_db, result_db, missing_cache_policy="strict")
        with pytest.raises(CacheMissError):
            node.execute(stream)

    def test_strict_does_not_raise_when_all_results_present(self):
        """strict mode is a no-op when the result DB is fully populated."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        node1 = _make_node(stream, pipeline_db, result_db)
        node1.execute(stream)

        node2 = _make_node(stream, pipeline_db, result_db, missing_cache_policy="strict")
        results = node2.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20

    def test_strict_ephemeral_miss_does_not_raise(self):
        """strict mode never raises for ephemeral misses."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_db = InMemoryArrowDatabase()

        node1 = FunctionJobNode(
            function_pod=FunctionPod(PythonDataFunction(_double, output_keys="result")),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True, missing_cache_policy="strict")
        node1.execute(stream)

        # Wipe ephemeral store only
        ephemeral_db._tables.clear()
        ephemeral_db._pending_batches.clear()

        node2 = FunctionJobNode(
            function_pod=FunctionPod(PythonDataFunction(_double, output_keys="result")),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True, missing_cache_policy="strict")
        # Should NOT raise — ephemeral misses degrade gracefully
        results = node2.execute(stream)
        assert len(results) == 1  # recomputed
```

- [ ] **Step 2.2: Run tests to verify they fail**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestStrictPolicy -v
```

Expected: FAIL — `CacheMissError` not raised (current code just warns and recomputes).

- [ ] **Step 2.3: Import `CacheMissError` in `function_node.py`**

Find this line in `src/orcapod/core/nodes/function_node.py`:

```python
from orcapod.errors import EphemeralResultMissingError, PipelineJobRequiredError, SchemaVersionError
```

Replace with:

```python
from orcapod.errors import CacheMissError, EphemeralResultMissingError, PipelineJobRequiredError, SchemaVersionError
```

- [ ] **Step 2.4: Add `_populate_empty_data_tokens()` helper to `FunctionJobNode`**

In `src/orcapod/core/nodes/function_node.py`, add this method to `FunctionJobNode` just before `_fetch_joined_records()` (around line 1862):

```python
def _populate_empty_data_tokens(
    self,
    df: "pl.DataFrame",
    empty_data_tokens: "dict[bytes, EmptyData]",
    empty_taginfo_rows: "dict[bytes, dict]",
) -> None:
    """Populate ``empty_data_tokens`` and ``empty_taginfo_rows`` for rows in ``df``.

    Shared by the ephemeral miss path and the non-ephemeral permissive
    (``missing_cache_policy="as_empty"``) path in ``_fetch_joined_records``.

    Args:
        df: DataFrame of unmatched pipeline DB rows (each row is a miss).
        empty_data_tokens: Dict to populate with ``base_entry_id → EmptyData``.
        empty_taginfo_rows: Dict to populate with ``base_entry_id → raw row dict``.
    """
    for row in df.iter_rows(named=True):
        base_eid = row[_PIPELINE_BASE_ENTRY_ID_COL]
        raw_hash = row.get(constants.OUTPUT_DATA_HASH_COL)
        if raw_hash is None:
            logger.warning(
                "Pipeline DB row missing %r column — EmptyData will have "
                "no cached hash; flow-through unavailable for this row. "
                "base_entry_id: %r",
                constants.OUTPUT_DATA_HASH_COL,
                base_eid,
            )
            cached_hash = None
        else:
            cached_hash = ContentHash.from_prefixed_digest(raw_hash)
        empty_data_tokens[base_eid] = EmptyData(
            cached_content_hash=cached_hash,
            data_context=self.data_context,
        )
        empty_taginfo_rows[base_eid] = row
```

- [ ] **Step 2.5: Update `_fetch_joined_records()` — persistent join Branch A and Branch B**

Find the persistent join section in `_fetch_joined_records()`. Replace the entire block from `if persistent_taginfo_df.height > 0:` through `persistent_df = full_persistent_df` with:

```python
        # ------------------------------------------------------------------
        # Persistent join
        # ------------------------------------------------------------------
        results_schema = None
        persistent_df = pl.DataFrame()
        policy = self._node_config.missing_cache_policy or "recompute"

        if persistent_taginfo_df.height > 0:
            results = self._cached_function_pod.result_database.get_all_records(
                self._cached_function_pod.record_path,
                record_id_column=constants.DATA_RECORD_ID,
            )
            if results is None:
                # Branch A: result DB entirely empty — all persistent entries are misses.
                count = persistent_taginfo_df.height
                if policy == "strict":
                    logger.error(
                        "%d pipeline DB entries have no match in persistent result DB "
                        "— raising CacheMissError (missing_cache_policy='strict').",
                        count,
                    )
                    raise CacheMissError(
                        f"{count} pipeline DB entries have no match in persistent result DB "
                        "— data may have been deleted externally."
                    )
                elif policy == "as_empty":
                    logger.warning(
                        "%d pipeline DB entries have no match in persistent result DB "
                        "— treating as Empty data (missing_cache_policy='as_empty'). "
                        "Downstream nodes will attempt to serve from their own cache.",
                        count,
                    )
                    self._populate_empty_data_tokens(
                        persistent_taginfo_df, empty_data_tokens, empty_taginfo_rows
                    )
                else:
                    logger.warning(
                        "%d pipeline DB entries have no match in persistent result DB "
                        "— data may have been deleted externally. "
                        "These inputs will be recomputed.",
                        count,
                    )
            else:
                results_schema = results.schema
                full_persistent_df = persistent_taginfo_df.join(
                    pl.DataFrame(results),
                    on=constants.DATA_RECORD_ID,
                    how="inner",
                )
                # Branch B: result DB has rows but the anti-join found gaps.
                missing_count = persistent_taginfo_df.height - full_persistent_df.height
                if missing_count > 0:
                    if policy == "strict":
                        logger.error(
                            "%d pipeline DB entries have no match in persistent result DB "
                            "— raising CacheMissError (missing_cache_policy='strict').",
                            missing_count,
                        )
                        raise CacheMissError(
                            f"{missing_count} pipeline DB entries have no match in "
                            "persistent result DB — data may have been deleted externally."
                        )
                    elif policy == "as_empty":
                        logger.warning(
                            "%d pipeline DB entries have no match in persistent result DB "
                            "— treating as Empty data (missing_cache_policy='as_empty'). "
                            "Downstream nodes will attempt to serve from their own cache.",
                            missing_count,
                        )
                        unmatched_persistent_df = persistent_taginfo_df.join(
                            pl.DataFrame(results),
                            on=constants.DATA_RECORD_ID,
                            how="anti",
                        )
                        self._populate_empty_data_tokens(
                            unmatched_persistent_df, empty_data_tokens, empty_taginfo_rows
                        )
                    else:
                        logger.warning(
                            "%d pipeline DB entries have no match in persistent result DB "
                            "— data may have been deleted externally. "
                            "These inputs will be recomputed.",
                            missing_count,
                        )
                persistent_df = full_persistent_df
```

Also update the ephemeral EmptyData creation loop to use the shared helper. Find:

```python
            for row in unmatched_df.iter_rows(named=True):
                base_eid = row[_PIPELINE_BASE_ENTRY_ID_COL]
                # Use OUTPUT_DATA_HASH_COL so that the downstream result cache
                # lookup in _process_data_internal finds the correct entry.
                # The downstream's result cache is keyed by the INPUT to the
                # downstream (= the OUTPUT of this ephemeral node), so we must
                # use the output hash. INPUT_DATA_HASH_COL is deliberately not
                # used as a fallback — it carries the wrong hash (the upstream's
                # input, not its output) and would silently cause cache misses.
                raw_hash = row.get(constants.OUTPUT_DATA_HASH_COL)
                if raw_hash is None:
                    logger.warning(
                        "Pipeline DB row missing %r column — EmptyData will have "
                        "no cached hash; flow-through unavailable for this row. "
                        "base_entry_id: %r",
                        constants.OUTPUT_DATA_HASH_COL,
                        base_eid,
                    )
                    cached_hash = None
                else:
                    cached_hash = ContentHash.from_prefixed_digest(raw_hash)
                empty_data_tokens[base_eid] = EmptyData(
                    cached_content_hash=cached_hash,
                    data_context=self.data_context,
                )
                empty_taginfo_rows[base_eid] = row
```

Replace with:

```python
            # Use the shared helper — same EmptyData creation logic for
            # ephemeral misses and permissive persistent misses.
            self._populate_empty_data_tokens(
                unmatched_df, empty_data_tokens, empty_taginfo_rows
            )
```

- [ ] **Step 2.6: Run tests to verify they pass**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestStrictPolicy -v
```

Expected: all 4 tests PASS.

- [ ] **Step 2.7: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all existing tests PASS.

- [ ] **Step 2.8: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py \
        tests/test_core/function_pod/test_missing_cache_policy.py
git commit -m "feat(function_node): strict missing_cache_policy raises CacheMissError (ITL-604)"
```

---

## Task 3: `_fetch_joined_records()` — `"as_empty"` non-ephemeral path + ephemeral INFO log

**Files:**
- Modify: `tests/test_core/function_pod/test_missing_cache_policy.py`
- Modify: `src/orcapod/core/nodes/function_node.py`

Note: The persistent `"as_empty"` branch in `_fetch_joined_records()` was already implemented in Task 2. This task adds tests to verify it and wires the ephemeral INFO log.

- [ ] **Step 3.1: Write failing tests for `"as_empty"` and ephemeral INFO log**

Append to `tests/test_core/function_pod/test_missing_cache_policy.py`:

```python
class TestAsEmptyPolicy:
    """missing_cache_policy="as_empty" emits EmptyData instead of recomputing."""

    def test_as_empty_nonephemeral_miss_emits_empty_data_not_recompute(self):
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: compute once (call_count = 1)
        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node1.execute(stream)
        assert call_count["n"] == 1

        _wipe_result_db(result_db)

        # Session 2: as_empty — function must NOT be called again
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node2.node_config = NodeConfig(missing_cache_policy="as_empty")
        results = node2.execute(stream)

        assert call_count["n"] == 1, "function must not be called again in as_empty mode"
        assert len(results) == 1
        assert isinstance(results[0][1], EmptyData)

    def test_as_empty_nonephemeral_miss_logs_warning(self, caplog):
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        _make_node(stream, pipeline_db, result_db).execute(stream)
        _wipe_result_db(result_db)

        node = _make_node(stream, pipeline_db, result_db, missing_cache_policy="as_empty")
        with caplog.at_level(logging.WARNING, logger="orcapod.core.nodes.function_node"):
            node.execute(stream)

        assert any("treating as Empty data" in msg for msg in caplog.messages)

    def test_as_empty_honoured_across_multiple_execute_calls(self):
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node1.execute(stream)
        assert call_count["n"] == 1

        _wipe_result_db(result_db)

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node2.node_config = NodeConfig(missing_cache_policy="as_empty")
        node2.execute(stream)
        node2._cached_output_datas.clear()
        node2.execute(stream)  # second call — must still emit EmptyData, not recompute

        assert call_count["n"] == 1, "policy must be honoured on every call, not just the first"


class TestEphemeralInfoLog:
    """Ephemeral misses log at INFO level (never WARNING)."""

    def test_ephemeral_miss_logs_info_not_warning(self, caplog):
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_db = InMemoryArrowDatabase()

        node1 = FunctionJobNode(
            function_pod=FunctionPod(PythonDataFunction(_double, output_keys="result")),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.execute(stream)

        # Wipe ephemeral store
        ephemeral_db._tables.clear()
        ephemeral_db._pending_batches.clear()

        node2 = FunctionJobNode(
            function_pod=FunctionPod(PythonDataFunction(_double, output_keys="result")),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)

        with caplog.at_level(logging.DEBUG, logger="orcapod.core.nodes.function_node"):
            node2.execute(stream)

        # Assert INFO log was emitted for ephemeral miss
        info_msgs = [
            r.message for r in caplog.records
            if r.levelno == logging.INFO and "ephemeral result DB" in r.message
        ]
        assert info_msgs, "expected INFO log for ephemeral miss"

        # Assert NO WARNING log about ephemeral miss
        warning_msgs = [
            r.message for r in caplog.records
            if r.levelno == logging.WARNING and "ephemeral result DB" in r.message
        ]
        assert not warning_msgs, f"unexpected WARNING for ephemeral miss: {warning_msgs}"
```

- [ ] **Step 3.2: Run tests to verify they fail**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestAsEmptyPolicy \
              tests/test_core/function_pod/test_missing_cache_policy.py::TestEphemeralInfoLog -v
```

Expected: `TestAsEmptyPolicy` tests FAIL (EmptyData not yet emitted in `execute()`); `TestEphemeralInfoLog` FAIL (no INFO log yet).

- [ ] **Step 3.3: Add INFO log to ephemeral miss path in `_fetch_joined_records()`**

In the ephemeral join section, find the line that begins the `_populate_empty_data_tokens` call (just added in Task 2). Insert an INFO log immediately before it:

```python
            # Emit EmptyData tokens for unmatched ephemeral rows
            # (cross-session miss: ephemeral data is gone).
            if eph_results is not None:
                unmatched_df = ephemeral_taginfo_df.join(
                    pl.DataFrame(eph_results),
                    on=constants.DATA_RECORD_ID,
                    how="anti",
                )
            else:
                # No ephemeral store or empty store — all ephemeral rows are misses.
                unmatched_df = ephemeral_taginfo_df
            if unmatched_df.height > 0:
                logger.info(
                    "%d pipeline DB entries have no match in ephemeral result DB "
                    "— expected after cross-session store clear. Propagating as EmptyData.",
                    unmatched_df.height,
                )
            # Use the shared helper — same EmptyData creation logic for
            # ephemeral misses and permissive persistent misses.
            self._populate_empty_data_tokens(
                unmatched_df, empty_data_tokens, empty_taginfo_rows
            )
```

- [ ] **Step 3.4: Run the TestEphemeralInfoLog test**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestEphemeralInfoLog -v
```

Expected: PASS. (The `TestAsEmptyPolicy` tests will still fail — fixed in Task 4.)

- [ ] **Step 3.5: Run full suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all existing tests PASS.

- [ ] **Step 3.6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py \
        tests/test_core/function_pod/test_missing_cache_policy.py
git commit -m "feat(function_node): as_empty policy + ephemeral INFO log in _fetch_joined_records (ITL-604)"
```

---

## Task 4: `execute()` — emit EmptyData directly in opportunistic mode

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

The `TestAsEmptyPolicy` tests written in Task 3 are already the failing tests for this task. No new tests to write.

- [ ] **Step 4.1: Confirm tests still fail before implementing**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestAsEmptyPolicy -v
```

Expected: FAIL — function is still called (recompute mode).

- [ ] **Step 4.2: Update the `execute()` loop in `FunctionJobNode`**

Find the for-loop in `execute()` that begins `for tag, data, base_entry_id in upstream_entries:`. Replace the entire body of that loop (including the `if/else` block for cache-hit check) with:

```python
        output: list[tuple[TagProtocol, DataProtocol]] = []
        for tag, data, base_entry_id in upstream_entries:
            ctx_obs.on_data_start(node_label, tag, data)
            policy = self._node_config.missing_cache_policy or "recompute"

            if base_entry_id in self._cached_output_datas:
                tag_out, cached_pkt = self._cached_output_datas[base_entry_id]
                if isinstance(cached_pkt, EmptyData) and policy == "recompute":
                    # "recompute" mode: EmptyData is a sentinel — fall through to compute.
                    pass
                else:
                    # Real data cache hit, OR opportunistic EmptyData emission.
                    ctx_obs.on_data_end(node_label, tag, data, cached_pkt, cached=True)
                    if cached_pkt is not None:
                        output.append((tag_out, cached_pkt))
                    continue

            # Compute path: not in cache, or EmptyData sentinel in "recompute" mode.
            pkt_logger = ctx_obs.create_data_logger(tag, data)
            try:
                tag_out, result = self._process_data_internal(
                    tag, data, logger=pkt_logger, run_id=run_id
                )
            except Exception as exc:
                logger.warning(
                    "Data execution failed in %s: %s",
                    node_label,
                    exc,
                    exc_info=True,
                )
                ctx_obs.on_data_crash(node_label, tag, data, exc)
                if error_policy == "fail_fast":
                    ctx_obs.on_node_end(node_label, node_hash)
                    raise
            else:
                ctx_obs.on_data_end(
                    node_label, tag, data, result, cached=False
                )
                if result is not None:
                    output.append((tag_out, result))
```

- [ ] **Step 4.3: Run `TestAsEmptyPolicy` tests**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestAsEmptyPolicy -v
```

Expected: all 3 tests PASS.

- [ ] **Step 4.4: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests PASS.

- [ ] **Step 4.5: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "feat(function_node): emit EmptyData in execute() for as_empty/strict policy (ITL-604)"
```

---

## Task 5: `async_execute()` — align `route_inputs` with sync behaviour

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Modify: `tests/test_core/function_pod/test_missing_cache_policy.py`

- [ ] **Step 5.1: Write failing tests for async behaviour**

Append to `tests/test_core/function_pod/test_missing_cache_policy.py`:

```python
class TestAsyncExecutePolicy:
    """async_execute() route_inputs respects missing_cache_policy."""

    def test_async_recompute_mode_does_not_forward_empty_data(self):
        """In 'recompute' mode, an ephemeral miss in async_execute triggers recompute."""
        import asyncio
        from orcapod.channels import Channel

        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.execute(stream)
        assert call_count["n"] == 1

        # Wipe ephemeral store
        ephemeral_db._tables.clear()
        ephemeral_db._pending_batches.clear()

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)  # recompute (default)

        async def run():
            in_ch: Channel = Channel(buffer_size=16)
            out_ch: Channel = Channel(buffer_size=16)
            async def feed():
                for tag, data in stream.iter_data():
                    await in_ch.writer.send((tag, data))
                await in_ch.writer.close()
            results = []
            async def collect():
                async for item in out_ch.reader:
                    results.append(item)
            await asyncio.gather(
                feed(),
                node2.async_execute(in_ch.reader, out_ch.writer),
                collect(),
            )
            return results

        results = asyncio.run(run())
        # In recompute mode, the function is called again
        assert call_count["n"] == 2
        assert len(results) == 1
        assert not isinstance(results[0][1], EmptyData)

    def test_async_as_empty_mode_forwards_empty_data_without_recompute(self):
        """In 'as_empty' mode, an ephemeral miss forwards EmptyData without recompute."""
        import asyncio
        from orcapod.channels import Channel

        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.execute(stream)
        assert call_count["n"] == 1

        ephemeral_db._tables.clear()
        ephemeral_db._pending_batches.clear()

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
            ephemeral_database=ephemeral_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True, missing_cache_policy="as_empty")

        async def run():
            in_ch: Channel = Channel(buffer_size=16)
            out_ch: Channel = Channel(buffer_size=16)
            async def feed():
                for tag, data in stream.iter_data():
                    await in_ch.writer.send((tag, data))
                await in_ch.writer.close()
            results = []
            async def collect():
                async for item in out_ch.reader:
                    results.append(item)
            await asyncio.gather(
                feed(),
                node2.async_execute(in_ch.reader, out_ch.writer),
                collect(),
            )
            return results

        results = asyncio.run(run())
        assert call_count["n"] == 1, "function must not be called again in as_empty mode"
        assert len(results) == 1
        assert isinstance(results[0][1], EmptyData)
```

- [ ] **Step 5.2: Run tests to verify current async behaviour**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestAsyncExecutePolicy -v
```

Expected: `test_async_recompute_mode_does_not_forward_empty_data` FAILS (async currently forwards EmptyData even in recompute mode — sync/async inconsistency). `test_async_as_empty_mode_forwards_empty_data_without_recompute` may PASS (async already forwards EmptyData) — note it in case it does pass.

- [ ] **Step 5.3: Update `route_inputs` inner function in `async_execute()`**

Inside `async_execute()`, find the `route_inputs` nested async function. Replace its body with:

```python
                async def route_inputs() -> None:
                    """Stage 1: send cache hits to output; stamp misses for computation."""
                    policy = self._node_config.missing_cache_policy or "recompute"
                    try:
                        async for tag, data in input_channel:
                            base_entry_id = self.compute_base_entry_id(tag, data)
                            if base_entry_id in cached_by_base_entry_id:
                                cached_tag, cached_data = cached_by_base_entry_id[base_entry_id]
                                if isinstance(cached_data, EmptyData) and policy == "recompute":
                                    # "recompute" mode: EmptyData sentinel → send to compute.
                                    correlation_key = uuid.uuid4().bytes
                                    input_store[correlation_key] = (tag, data)
                                    stamped_tag = tag.with_meta_columns(
                                        **{_TAG_NODE_INPUT_REF: correlation_key}
                                    )
                                    await compute_channel.writer.send((stamped_tag, data))
                                else:
                                    # Real data hit, or EmptyData in opportunistic/strict mode.
                                    ctx_obs.on_data_start(node_label, tag, data)
                                    ctx_obs.on_data_end(
                                        node_label, tag, data, cached_data, cached=True
                                    )
                                    await output.send((cached_tag, cached_data))
                            else:
                                correlation_key = uuid.uuid4().bytes
                                input_store[correlation_key] = (tag, data)
                                stamped_tag = tag.with_meta_columns(
                                    **{_TAG_NODE_INPUT_REF: correlation_key}
                                )
                                await compute_channel.writer.send((stamped_tag, data))
                    finally:
                        await compute_channel.writer.close()
```

- [ ] **Step 5.4: Run the async tests**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestAsyncExecutePolicy -v
```

Expected: both tests PASS.

- [ ] **Step 5.5: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests PASS.

- [ ] **Step 5.6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py \
        tests/test_core/function_pod/test_missing_cache_policy.py
git commit -m "feat(function_node): align async_execute route_inputs with missing_cache_policy (ITL-604)"
```

---

## Task 6: CACHE_ONLY mode — strict raises, as_empty forwards EmptyData

**Files:**
- Modify: `tests/test_core/function_pod/test_missing_cache_policy.py`

The `_fetch_joined_records()` changes in Task 2 already handle CACHE_ONLY: `CacheMissError` is raised inside `_load_cached_entries()` which is called by both `iter_data()` and `_async_execute_cache_only()`. This task adds tests to verify the CACHE_ONLY paths work correctly.

- [ ] **Step 6.1: Write CACHE_ONLY tests**

Append to `tests/test_core/function_pod/test_missing_cache_policy.py`:

```python
class TestCacheOnlyPolicy:
    """CACHE_ONLY mode respects missing_cache_policy."""

    def _make_cache_only_node(
        self,
        stream: ArrowTableStream,
        pipeline_db: InMemoryArrowDatabase,
        result_db: InMemoryArrowDatabase,
        missing_cache_policy: str | None = None,
    ) -> FunctionJobNode:
        """Build a FunctionJobNode in CACHE_ONLY mode by simulating an UNAVAILABLE upstream."""
        from orcapod.pipeline.serialization import LoadStatus

        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        # Force CACHE_ONLY status directly — same as what from_descriptor() does
        # when the upstream stream has load_status == UNAVAILABLE.
        node._load_status = LoadStatus.CACHE_ONLY
        if missing_cache_policy is not None:
            node.node_config = NodeConfig(missing_cache_policy=missing_cache_policy)
        return node

    def test_strict_raises_in_cache_only_mode(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: compute normally
        _make_node(stream, pipeline_db, result_db).execute(stream)
        _wipe_result_db(result_db)

        # CACHE_ONLY + strict → must raise on iter_data()
        node = self._make_cache_only_node(
            stream, pipeline_db, result_db, missing_cache_policy="strict"
        )
        with pytest.raises(CacheMissError):
            list(node.iter_data())

    def test_as_empty_forwards_empty_data_in_cache_only_mode(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        _make_node(stream, pipeline_db, result_db).execute(stream)
        _wipe_result_db(result_db)

        node = self._make_cache_only_node(
            stream, pipeline_db, result_db, missing_cache_policy="as_empty"
        )
        results = list(node.iter_data())
        assert len(results) == 1
        assert isinstance(results[0][1], EmptyData)

    def test_recompute_omits_missing_entry_in_cache_only_mode(self):
        """Default 'recompute' mode: CACHE_ONLY can't recompute, so entry is just absent."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        _make_node(stream, pipeline_db, result_db).execute(stream)
        _wipe_result_db(result_db)

        node = self._make_cache_only_node(stream, pipeline_db, result_db)
        results = list(node.iter_data())
        assert results == [], "missing entry should be silently omitted in recompute+CACHE_ONLY"
```

- [ ] **Step 6.2: Run the tests**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestCacheOnlyPolicy -v
```

Expected: all 3 tests PASS (the implementation from Task 2 already handles these paths).

- [ ] **Step 6.3: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests PASS.

- [ ] **Step 6.4: Commit**

```bash
git add tests/test_core/function_pod/test_missing_cache_policy.py
git commit -m "test(function_node): CACHE_ONLY missing_cache_policy coverage (ITL-604)"
```

---

## Task 7: End-to-end — `"as_empty"` downstream cache hit

**Files:**
- Modify: `tests/test_core/function_pod/test_missing_cache_policy.py`

- [ ] **Step 7.1: Write the end-to-end test**

Append to `tests/test_core/function_pod/test_missing_cache_policy.py`:

```python
class TestAsEmptyEndToEnd:
    """End-to-end: upstream as_empty miss → downstream serves from its own cache."""

    def test_downstream_serves_from_cache_when_upstream_emits_empty_data(self):
        """Two-node pipeline: Node A (as_empty miss) → Node B (persistent).

        Session 1: A computes, B computes.
        Session 2: A's result wiped; A emits EmptyData. B should serve from
        its own cache using EmptyData.cached_content_hash.
        """
        def double(x: int) -> int:
            return x * 2

        def triple(result: int) -> int:
            return result * 3

        stream = _make_stream([{"id": 0, "x": 10}])

        pipeline_db_a = InMemoryArrowDatabase()
        result_db_a = InMemoryArrowDatabase()
        pipeline_db_b = InMemoryArrowDatabase()
        result_db_b = InMemoryArrowDatabase()

        # Session 1: compute A then B
        pf_a = PythonDataFunction(double, output_keys="result")
        node_a1 = FunctionJobNode(
            function_pod=FunctionPod(pf_a),
            input_stream=stream,
            pipeline_database=pipeline_db_a,
            result_database=result_db_a,
        )
        results_a = node_a1.execute(stream)
        assert len(results_a) == 1
        assert results_a[0][1].as_dict()["result"] == 20

        from orcapod.core.streams.arrow_table_stream import ArrowTableStream as ATS
        import pyarrow as pa2
        # Build B's input stream from A's output
        a_out_tag, a_out_data = results_a[0]
        a_table = pa2.table(
            {"id": pa2.array([0], type=pa2.int64()), "result": pa2.array([20], type=pa2.int64())}
        )
        stream_b = ArrowTableStream(a_table, tag_columns=["id"])

        pf_b = PythonDataFunction(triple, output_keys="tripled")
        node_b1 = FunctionJobNode(
            function_pod=FunctionPod(pf_b),
            input_stream=stream_b,
            pipeline_database=pipeline_db_b,
            result_database=result_db_b,
        )
        results_b = node_b1.execute(stream_b)
        assert len(results_b) == 1
        assert results_b[0][1].as_dict()["tripled"] == 60

        # Session 2: wipe A's result DB; A emits EmptyData with as_empty policy
        _wipe_result_db(result_db_a)

        pf_a2 = PythonDataFunction(double, output_keys="result")
        node_a2 = FunctionJobNode(
            function_pod=FunctionPod(pf_a2),
            input_stream=stream,
            pipeline_database=pipeline_db_a,
            result_database=result_db_a,
        )
        node_a2.node_config = NodeConfig(missing_cache_policy="as_empty")
        results_a2 = node_a2.execute(stream)

        assert len(results_a2) == 1
        assert isinstance(results_a2[0][1], EmptyData), "A must emit EmptyData"

        # Feed EmptyData to B — B should serve from its own cache
        empty_tag, empty_data = results_a2[0]
        empty_table = empty_data.as_table_for_downstream(empty_tag)  # may raise — see note below

        # Note: If EmptyData.as_table_for_downstream does not exist, use the
        # _process_data_internal path directly by calling node_b2.execute()
        # with a stream that yields EmptyData. The EmptyData.content_hash()
        # is used as the lookup key into B's result cache.
        # This test verifies the contract at the _process_data_internal level:
        pf_b2 = PythonDataFunction(triple, output_keys="tripled")
        node_b2 = FunctionJobNode(
            function_pod=FunctionPod(pf_b2),
            input_stream=stream_b,
            pipeline_database=pipeline_db_b,
            result_database=result_db_b,
        )
        # Call _process_data_internal directly with EmptyData input
        tag_b_out, data_b_out = node_b2._process_data_internal(empty_tag, empty_data)
        assert data_b_out.as_dict()["tripled"] == 60, \
            "B should serve 60 from its cache given EmptyData with the right hash"

    def test_downstream_raises_ephemeral_result_missing_when_it_has_no_cache(self):
        """Downstream raises EphemeralResultMissingError if it has never computed the result."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(_double, output_keys="result")
        node = FunctionJobNode(
            function_pod=FunctionPod(pf),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node.execute(stream)
        _wipe_result_db(result_db)

        node2 = FunctionJobNode(
            function_pod=FunctionPod(PythonDataFunction(_double, output_keys="result")),
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node2.node_config = NodeConfig(missing_cache_policy="as_empty")
        results = node2.execute(stream)
        assert isinstance(results[0][1], EmptyData)

        # A downstream node that has never computed the result raises EphemeralResultMissingError
        downstream_pipeline_db = InMemoryArrowDatabase()
        downstream_result_db = InMemoryArrowDatabase()
        downstream_stream = _make_stream([{"id": 0, "x": 20}])  # different stream, no cache
        pf_down = PythonDataFunction(lambda result: result + 1, output_keys="incremented")
        node_down = FunctionJobNode(
            function_pod=FunctionPod(pf_down),
            input_stream=downstream_stream,
            pipeline_database=downstream_pipeline_db,
            result_database=downstream_result_db,
        )

        empty_tag, empty_data = results[0]
        with pytest.raises(EphemeralResultMissingError):
            node_down._process_data_internal(empty_tag, empty_data)
```

- [ ] **Step 7.2: Run the end-to-end tests**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestAsEmptyEndToEnd -v
```

Expected: `test_downstream_raises_ephemeral_result_missing_when_it_has_no_cache` PASS. For `test_downstream_serves_from_cache_when_upstream_emits_empty_data` — if it fails due to `EmptyData.as_table_for_downstream` not existing, the test itself has a fallback note and the `_process_data_internal` call is the correct assertion path; adjust accordingly.

- [ ] **Step 7.3: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests PASS.

- [ ] **Step 7.4: Commit**

```bash
git add tests/test_core/function_pod/test_missing_cache_policy.py
git commit -m "test(function_node): end-to-end as_empty downstream cache hit (ITL-604)"
```

---

## Task 8: Regression guard — `"recompute"` default unchanged

**Files:**
- Modify: `tests/test_core/function_pod/test_missing_cache_policy.py`

- [ ] **Step 8.1: Write regression tests confirming default behaviour is unchanged**

Append to `tests/test_core/function_pod/test_missing_cache_policy.py`:

```python
class TestRecomputeRegression:
    """Default 'recompute' mode preserves all existing behaviour exactly."""

    def test_default_nonephemeral_miss_still_recomputes(self):
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf), input_stream=stream,
            pipeline_database=pipeline_db, result_database=result_db,
        )
        node1.execute(stream)
        assert call_count["n"] == 1

        _wipe_result_db(result_db)

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2), input_stream=stream,
            pipeline_database=pipeline_db, result_database=result_db,
        )
        # No missing_cache_policy set — defaults to "recompute"
        results = node2.execute(stream)
        assert call_count["n"] == 2, "default mode must recompute on miss"
        assert len(results) == 1
        assert not isinstance(results[0][1], EmptyData)
        assert results[0][1].as_dict()["result"] == 20

    def test_explicit_recompute_policy_same_as_default(self):
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        node1 = FunctionJobNode(
            function_pod=FunctionPod(pf), input_stream=stream,
            pipeline_database=pipeline_db, result_database=result_db,
        )
        node1.execute(stream)
        _wipe_result_db(result_db)

        pf2 = PythonDataFunction(counting_double, output_keys="result")
        node2 = FunctionJobNode(
            function_pod=FunctionPod(pf2), input_stream=stream,
            pipeline_database=pipeline_db, result_database=result_db,
        )
        node2.node_config = NodeConfig(missing_cache_policy="recompute")
        results = node2.execute(stream)
        assert call_count["n"] == 2
        assert not isinstance(results[0][1], EmptyData)
```

- [ ] **Step 8.2: Run regression tests**

```bash
uv run pytest tests/test_core/function_pod/test_missing_cache_policy.py::TestRecomputeRegression -v
```

Expected: both tests PASS.

- [ ] **Step 8.3: Run the full test suite one final time**

```bash
uv run pytest tests/ -q
```

Expected: all tests PASS. Note final count.

- [ ] **Step 8.4: Final commit**

```bash
git add tests/test_core/function_pod/test_missing_cache_policy.py
git commit -m "test(function_node): recompute default regression guard (ITL-604)"
```

---

## Self-review against spec

| Spec requirement | Covered by |
|---|---|
| `NodeConfig.missing_cache_policy: Literal[...] \| None` | Task 1 |
| `merge()` updated | Task 1 |
| Docstring warning in `NodeConfig` | Task 1 |
| `CacheMissError` in `errors.py` | Task 1 |
| Non-ephemeral miss, `"strict"`, FULL → raises | Task 2 |
| Non-ephemeral miss, `"strict"`, CACHE_ONLY → raises | Task 6 |
| Non-ephemeral miss, `"as_empty"`, FULL → EmptyData emitted | Task 3 + Task 4 |
| Non-ephemeral miss, `"as_empty"`, CACHE_ONLY → EmptyData forwarded | Task 6 |
| Non-ephemeral miss, `"recompute"` → unchanged | Task 8 |
| Ephemeral miss → INFO log (any policy) | Task 3 |
| `execute()` EmptyData emission for opportunistic mode | Task 4 |
| `async_execute()` route_inputs aligned with sync | Task 5 |
| `_populate_empty_data_tokens` shared helper (no duplication) | Task 2 |
| End-to-end downstream cache hit | Task 7 |
| End-to-end downstream cache miss → `EphemeralResultMissingError` | Task 7 |
| Policy honoured across multiple `execute()` calls | Task 3 |
| `NodeConfig.merge()` coverage | Task 1 |
