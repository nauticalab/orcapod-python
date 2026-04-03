# DAG Snapshot at Run Start Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write `dag_snapshot.json` at the pipeline-specific directory of the database at the start of every `Pipeline.run()` call, containing the full DAG structure with a stable `run_id` and `snapshot_time`, so observers and Portolan can interpret logs even if the run crashes.

**Architecture:** Extend `Pipeline.save()` with optional `run_id` / `snapshot_time` parameters that populate the currently-null fields in the JSON output. Add a `_write_dag_snapshot()` private method that derives the canonical path from the scoped pipeline database's local filesystem root, creates the directory, and calls `save()`. In `run()`, generate the `run_id` (UUID4) and `snapshot_time` (ISO UTC) at the very top — before any nodes execute — call `_write_dag_snapshot()`, then thread `run_id` into the orchestrator calls so observer `on_run_start` events carry the same ID as the snapshot.

**Tech Stack:** Python stdlib (`uuid`, `datetime`, `pathlib`), orcapod's existing `Pipeline.save()` / `DeltaTableDatabase`, pytest with `tmp_path` fixture.

---

## File Map

| File | Action | What changes |
|------|--------|-------------|
| `src/orcapod/pipeline/graph.py` | Modify | (1) `save()` gains `run_id` + `snapshot_time` params; (2) new `_write_dag_snapshot()`; (3) `run()` generates `run_id`/`snapshot_time`, calls `_write_dag_snapshot()`, passes `run_id` to orchestrators |
| `tests/test_pipeline/test_dag_snapshot.py` | Create | All new tests for the snapshot feature |

---

## Task 1: Extend `save()` to accept `run_id` and `snapshot_time`

**Files:**
- Modify: `src/orcapod/pipeline/graph.py` (the `save()` method, ~line 604)
- Test: `tests/test_pipeline/test_dag_snapshot.py`

Currently `pipeline_block["run_id"]` and `pipeline_block["snapshot_time"]` are always `None`. This task makes them injectable.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_pipeline/test_dag_snapshot.py`:

```python
"""Tests for Pipeline DAG snapshot (PLT-1161).

Verifies that Pipeline.save() populates run_id/snapshot_time when
provided, and that Pipeline.run() writes dag_snapshot.json to the
correct well-known path with a stable run_id.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

import pyarrow as pa
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source(tag_col: str, packet_col: str, data: dict) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            packet_col: pa.array(data[packet_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col])


def _make_two_sources():
    src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
    src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
    return src_a, src_b


def add_values(value: int, score: int) -> int:
    return value + score


def _build_and_run_pipeline(pipeline_db, run: bool = True) -> Pipeline:
    """Build a simple two-source + join + function pipeline.

    When ``run=False`` the pipeline is compiled but not executed, so
    that callers can safely invoke ``save()`` or ``_write_dag_snapshot()``,
    both of which require a compiled pipeline.
    """
    src_a, src_b = _make_two_sources()
    pf = PythonPacketFunction(add_values, output_keys="total")
    pod = FunctionPod(packet_function=pf)

    pipeline = Pipeline(name="snap_test", pipeline_database=pipeline_db)
    with pipeline:
        joined = Join()(src_a, src_b)
        pod(joined, label="adder")

    if run:
        pipeline.run()
    else:
        pipeline.compile()
    return pipeline


# ---------------------------------------------------------------------------
# Task 1: save() with run_id / snapshot_time
# ---------------------------------------------------------------------------

class TestSaveRunIdAndSnapshotTime:
    def test_save_populates_run_id_when_provided(self, tmp_path):
        """save() writes run_id into the pipeline block."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        out = tmp_path / "snap.json"
        pipeline.save(str(out), run_id="test-run-id-123")

        data = json.loads(out.read_text())
        assert data["pipeline"]["run_id"] == "test-run-id-123"

    def test_save_populates_snapshot_time_when_provided(self, tmp_path):
        """save() writes snapshot_time into the pipeline block."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        out = tmp_path / "snap.json"
        pipeline.save(str(out), snapshot_time="2026-01-01T00:00:00+00:00")

        data = json.loads(out.read_text())
        assert data["pipeline"]["snapshot_time"] == "2026-01-01T00:00:00+00:00"

    def test_save_run_id_defaults_to_none(self, tmp_path):
        """Existing callers get null run_id (backward compatible)."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        out = tmp_path / "snap.json"
        pipeline.save(str(out))

        data = json.loads(out.read_text())
        assert data["pipeline"]["run_id"] is None
        assert data["pipeline"]["snapshot_time"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/kurouto/kurouto-jobs/29f2e0ec-cea9-44c2-9b97-d2af58ff7281/orcapod-python
uv run pytest tests/test_pipeline/test_dag_snapshot.py::TestSaveRunIdAndSnapshotTime -v
```

Expected: FAIL — `save()` does not yet accept `run_id` or `snapshot_time`.

- [ ] **Step 3: Implement — update `save()` signature**

In `src/orcapod/pipeline/graph.py`, update the `save()` method signature and the `pipeline_block` construction.

Find the line:
```python
def save(self, path: str, level: Literal["minimal", "definition", "standard", "full"] = "standard") -> None:
```

Change to:
```python
def save(
    self,
    path: str,
    level: Literal["minimal", "definition", "standard", "full"] = "standard",
    *,
    run_id: str | None = None,
    snapshot_time: str | None = None,
) -> None:
```

Find the `pipeline_block` dict construction (currently ~line 690):
```python
        pipeline_block: dict[str, Any] = {
            "name": list(self._name),
            "run_id": None,
            "snapshot_time": None,
        }
```

Change to:
```python
        pipeline_block: dict[str, Any] = {
            "name": list(self._name),
            "run_id": run_id,
            "snapshot_time": snapshot_time,
        }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_pipeline/test_dag_snapshot.py::TestSaveRunIdAndSnapshotTime -v
```

Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/graph.py tests/test_pipeline/test_dag_snapshot.py
git commit -m "feat(pipeline): add run_id and snapshot_time params to Pipeline.save()"
```

---

## Task 2: Add `_write_dag_snapshot()` private method to `Pipeline`

**Files:**
- Modify: `src/orcapod/pipeline/graph.py` (add method after `_compute_pipeline_snapshot_hash`)
- Test: `tests/test_pipeline/test_dag_snapshot.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline/test_dag_snapshot.py`:

```python
# ---------------------------------------------------------------------------
# Task 2: _write_dag_snapshot() method
# ---------------------------------------------------------------------------

class TestWriteDagSnapshot:
    def test_write_dag_snapshot_creates_file_at_correct_path(self, tmp_path):
        """_write_dag_snapshot() creates dag_snapshot.json at
        {db_root}/{pipeline_name}/dag_snapshot.json."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        result = pipeline._write_dag_snapshot(
            run_id="my-run-id",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )

        expected = tmp_path / "db" / "snap_test" / "dag_snapshot.json"
        assert result == expected
        assert expected.exists()

    def test_write_dag_snapshot_content_is_valid_json(self, tmp_path):
        """The snapshot file contains valid JSON with pipeline/nodes/edges."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        pipeline._write_dag_snapshot(
            run_id="my-run-id",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )

        snap = json.loads((tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text())
        assert "pipeline" in snap
        assert "nodes" in snap
        assert "edges" in snap

    def test_write_dag_snapshot_embeds_run_id(self, tmp_path):
        """Snapshot includes the provided run_id."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        pipeline._write_dag_snapshot(
            run_id="embed-this-run-id",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )

        snap = json.loads((tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text())
        assert snap["pipeline"]["run_id"] == "embed-this-run-id"

    def test_write_dag_snapshot_embeds_snapshot_time(self, tmp_path):
        """Snapshot includes the provided snapshot_time."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=False)

        pipeline._write_dag_snapshot(
            run_id="my-run-id",
            snapshot_time="2026-04-03T12:00:00+00:00",
        )

        snap = json.loads((tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text())
        assert snap["pipeline"]["snapshot_time"] == "2026-04-03T12:00:00+00:00"

    def test_write_dag_snapshot_returns_none_for_in_memory_db(self):
        """_write_dag_snapshot() returns None when using InMemoryArrowDatabase
        (no local filesystem root)."""
        db = InMemoryArrowDatabase()
        pipeline = _build_and_run_pipeline(db, run=False)

        result = pipeline._write_dag_snapshot(
            run_id="my-run-id",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )

        assert result is None

    def test_write_dag_snapshot_returns_none_without_pipeline_database(self):
        """_write_dag_snapshot() returns None when no pipeline_database is set."""
        src_a, _ = _make_two_sources()
        pipeline = Pipeline(name="no_db", pipeline_database=None)
        with pipeline:
            _ = src_a  # just register the source

        result = pipeline._write_dag_snapshot(
            run_id="x",
            snapshot_time="2026-01-01T00:00:00+00:00",
        )
        assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pipeline/test_dag_snapshot.py::TestWriteDagSnapshot -v
```

Expected: FAIL — `_write_dag_snapshot` does not exist yet.

- [ ] **Step 3: Implement `_write_dag_snapshot()`**

In `src/orcapod/pipeline/graph.py`, add the following method after `_compute_pipeline_snapshot_hash()` (after line ~578):

```python
    def _write_dag_snapshot(
        self,
        run_id: str,
        snapshot_time: str,
    ) -> Path | None:
        """Write dag_snapshot.json to the pipeline-specific base directory.

        Derives the target path from the scoped pipeline database's local
        filesystem root.  Returns the :class:`~pathlib.Path` that was written,
        or ``None`` if the database has no local filesystem root (cloud or
        in-memory) or if no pipeline database is configured.

        Args:
            run_id: UUID string for this run (included in snapshot metadata).
            snapshot_time: ISO-8601 UTC timestamp string (included in snapshot).

        Returns:
            The :class:`~pathlib.Path` of the written file, or ``None`` if skipped.
        """
        from orcapod.databases.delta_lake_databases import DeltaTableDatabase

        scoped_db = self._scoped_pipeline_database
        if scoped_db is None:
            logger.debug("_write_dag_snapshot: no scoped pipeline database — skipping")
            return None

        if not isinstance(scoped_db, DeltaTableDatabase) or scoped_db._is_cloud:
            logger.debug(
                "_write_dag_snapshot: pipeline database has no local filesystem root — skipping"
            )
            return None

        # Build the snapshot directory: {db root}/{pipeline name components}
        snapshot_dir: Path = scoped_db._local_root
        for component in scoped_db._path_prefix:
            snapshot_dir = snapshot_dir / DeltaTableDatabase._sanitize_path_component(
                component
            )
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / "dag_snapshot.json"

        try:
            self.save(
                str(snapshot_path),
                level="standard",
                run_id=run_id,
                snapshot_time=snapshot_time,
            )
            logger.debug("dag_snapshot.json written to %s", snapshot_path)
            return snapshot_path
        except Exception as exc:
            logger.warning(
                "Failed to write dag_snapshot.json to %s: %s", snapshot_path, exc
            )
            return None
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_pipeline/test_dag_snapshot.py::TestWriteDagSnapshot -v
```

Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/graph.py tests/test_pipeline/test_dag_snapshot.py
git commit -m "feat(pipeline): add _write_dag_snapshot() method to Pipeline"
```

---

## Task 3: Call `_write_dag_snapshot()` at `run()` start; pass `run_id` to orchestrators

**Files:**
- Modify: `src/orcapod/pipeline/graph.py` (`run()` method, ~line 397)
- Test: `tests/test_pipeline/test_dag_snapshot.py`

This is the core PLT-1161 change: the snapshot is written before any node executes, using a `run_id` that is also passed to the observer's `on_run_start` via the orchestrator.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline/test_dag_snapshot.py`:

```python
# ---------------------------------------------------------------------------
# Task 3: run() writes snapshot at start, run_id is consistent
# ---------------------------------------------------------------------------

class TestRunWritesDagSnapshot:
    def test_run_writes_dag_snapshot_to_correct_path(self, tmp_path):
        """pipeline.run() creates dag_snapshot.json at the pipeline DB root."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=True)

        expected = tmp_path / "db" / "snap_test" / "dag_snapshot.json"
        assert expected.exists(), "dag_snapshot.json should exist after run()"

    def test_run_snapshot_contains_nodes_and_edges(self, tmp_path):
        """Snapshot written by run() contains nodes and edges entries."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert "nodes" in snap
        assert "edges" in snap
        assert len(snap["nodes"]) > 0

    def test_run_snapshot_has_non_null_run_id(self, tmp_path):
        """Snapshot written by run() has a non-null run_id."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert snap["pipeline"]["run_id"] is not None
        assert isinstance(snap["pipeline"]["run_id"], str)
        assert len(snap["pipeline"]["run_id"]) > 0

    def test_run_snapshot_has_non_null_snapshot_time(self, tmp_path):
        """Snapshot written by run() has a non-null snapshot_time."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert snap["pipeline"]["snapshot_time"] is not None
        assert isinstance(snap["pipeline"]["snapshot_time"], str)

    def test_run_snapshot_run_id_matches_observer_run_id(self, tmp_path):
        """The run_id in dag_snapshot.json matches the run_id
        received by the observer's on_run_start hook."""
        from orcapod.protocols.observability_protocols import ExecutionObserverProtocol

        class CapturingObserver:
            """Minimal observer that captures run_id from on_run_start."""
            def __init__(self):
                self.run_id: str | None = None

            def on_run_start(self, run_id: str, **kwargs) -> None:
                self.run_id = run_id

            def on_run_end(self, run_id: str) -> None:
                pass

            def on_node_start(self, *args, **kwargs) -> None:
                pass

            def on_node_end(self, *args, **kwargs) -> None:
                pass

            def on_packet_start(self, *args, **kwargs):
                pass

            def on_packet_end(self, *args, **kwargs) -> None:
                pass

            def on_packet_error(self, *args, **kwargs) -> None:
                pass

            def contextualize(self, *args, **kwargs):
                return self

            def create_packet_logger(self, *args, **kwargs):
                from orcapod.pipeline.observer import NoOpLogger
                return NoOpLogger()

        db = DeltaTableDatabase(base_path=tmp_path / "db")
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="snap_test", pipeline_database=db)
        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        observer = CapturingObserver()
        pipeline.run(observer=observer)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert snap["pipeline"]["run_id"] == observer.run_id, (
            "run_id in dag_snapshot.json must match the run_id passed to observer.on_run_start()"
        )

    def test_run_without_local_db_does_not_error(self):
        """pipeline.run() with InMemoryArrowDatabase completes without error
        (snapshot is silently skipped)."""
        db = InMemoryArrowDatabase()
        pipeline = _build_and_run_pipeline(db, run=True)
        # No exception = pass

    def test_run_snapshot_written_before_node_execution(self, tmp_path, monkeypatch):
        """dag_snapshot.json exists before any source or function node executes."""
        from orcapod.core.nodes import FunctionNode, SourceNode

        snapshot_existed_before_execute: list[bool] = []
        snap_path = tmp_path / "db" / "snap_test" / "dag_snapshot.json"

        original_fn_execute = FunctionNode.execute
        original_src_execute = SourceNode.execute

        def spy_fn_execute(self_node, *args, **kwargs):
            snapshot_existed_before_execute.append(snap_path.exists())
            return original_fn_execute(self_node, *args, **kwargs)

        def spy_src_execute(self_node, *args, **kwargs):
            snapshot_existed_before_execute.append(snap_path.exists())
            return original_src_execute(self_node, *args, **kwargs)

        monkeypatch.setattr(FunctionNode, "execute", spy_fn_execute)
        monkeypatch.setattr(SourceNode, "execute", spy_src_execute)

        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        # Every node execution should have seen the snapshot already present
        assert snapshot_existed_before_execute, "No node was executed (test setup issue)"
        assert all(snapshot_existed_before_execute), (
            "dag_snapshot.json must be written before any node executes"
        )

    def test_run_overwrites_snapshot_on_second_run(self, tmp_path):
        """A second run() overwrites dag_snapshot.json with a new run_id."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        pipeline = _build_and_run_pipeline(db, run=True)
        snap_path = tmp_path / "db" / "snap_test" / "dag_snapshot.json"

        first_run_id = json.loads(snap_path.read_text())["pipeline"]["run_id"]

        pipeline.run()
        second_run_id = json.loads(snap_path.read_text())["pipeline"]["run_id"]

        assert first_run_id != second_run_id, (
            "Second run() should produce a new run_id in dag_snapshot.json"
        )

    def test_run_snapshot_pipeline_name_matches(self, tmp_path):
        """Snapshot pipeline.name matches the Pipeline's name."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        assert snap["pipeline"]["name"] == ["snap_test"]

    def test_run_snapshot_contains_correct_node_types(self, tmp_path):
        """Snapshot nodes include source, operator, and function nodes."""
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        _build_and_run_pipeline(db, run=True)

        snap = json.loads(
            (tmp_path / "db" / "snap_test" / "dag_snapshot.json").read_text()
        )
        node_types = {v["node_type"] for v in snap["nodes"].values()}
        assert "source" in node_types
        assert "operator" in node_types
        assert "function" in node_types
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pipeline/test_dag_snapshot.py::TestRunWritesDagSnapshot -v
```

Expected: Most tests FAIL — `run()` does not yet call `_write_dag_snapshot()` and does not thread `run_id`.

- [ ] **Step 3: Implement — update `run()`**

In `src/orcapod/pipeline/graph.py`, update the `run()` method.

Add these two imports near the top of `run()` (after `from orcapod.types import ExecutorType, PipelineConfig`):

```python
        import uuid
        from datetime import datetime, timezone
```

Add these three lines immediately after `if not self._compiled: self.compile()` (and before the `_apply_execution_engine` call):

```python
        # Generate stable run identity at run start.
        run_id = str(uuid.uuid4())
        snapshot_time = datetime.now(timezone.utc).isoformat()

        # Write DAG snapshot before any node executes (PLT-1161).
        self._write_dag_snapshot(run_id, snapshot_time)
```

Then in each orchestrator call, add `run_id=run_id`. The three call sites in `run()` are:

**1. External orchestrator:**
```python
        if orchestrator is not None:
            orchestrator.run(
                self._node_graph,
                observer=effective_observer,
                pipeline_uri=pipeline_uri,
                run_id=run_id,
            )
```

**2. Async path:**
```python
                AsyncPipelineOrchestrator(
                    buffer_size=config.channel_buffer_size,
                ).run(
                    self._node_graph,
                    observer=effective_observer,
                    pipeline_uri=pipeline_uri,
                    run_id=run_id,
                )
```

**3. Sync path:**
```python
                orchestrator_sync = SyncPipelineOrchestrator()
                orchestrator_sync.run(
                    self._node_graph,
                    observer=effective_observer,
                    pipeline_uri=pipeline_uri,
                    run_id=run_id,
                )
```

Finally, update the `auto_save_path` call at the end of `run()` to also thread `run_id` and `snapshot_time`:

```python
        if self._auto_save_path is not None:
            self.save(str(self._auto_save_path), run_id=run_id, snapshot_time=snapshot_time)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_pipeline/test_dag_snapshot.py::TestRunWritesDagSnapshot -v
```

Expected: All 10 tests PASS.

- [ ] **Step 5: Run the full test suite to check for regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: All existing tests pass. No regressions.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/graph.py tests/test_pipeline/test_dag_snapshot.py
git commit -m "feat(pipeline): write dag_snapshot.json at run start with stable run_id (PLT-1161)"
```

---

## Task 4: Create PR against `dev`

- [ ] **Step 1: Create the feature branch and push**

```bash
cd /home/kurouto/kurouto-jobs/29f2e0ec-cea9-44c2-9b97-d2af58ff7281/orcapod-python
git checkout -b plt-1161/dag-snapshot-at-run-start
git push -u origin plt-1161/dag-snapshot-at-run-start
```

- [ ] **Step 2: Create the PR**

```bash
gh pr create \
  --base dev \
  --title "feat(pipeline): persist DAG snapshot as JSON at run start (PLT-1161)" \
  --body "$(cat <<'EOF'
## Summary

- Extends `Pipeline.save()` with `run_id` and `snapshot_time` keyword-only parameters, populating the previously-null fields in the JSON output
- Adds `Pipeline._write_dag_snapshot(run_id, snapshot_time)` which derives the canonical path `{db_root}/{pipeline_name}/dag_snapshot.json` from the scoped pipeline database and calls `save()` at `level="standard"`; silently skips for non-local (cloud, in-memory) databases
- Updates `Pipeline.run()` to generate a `run_id` (UUID4) and `snapshot_time` (ISO UTC) at run start, call `_write_dag_snapshot()` before any node executes, and thread `run_id` through to all orchestrator calls so `observer.on_run_start()` receives the same ID as the snapshot

## What this enables

Portolan and other log consumers can read `dag_snapshot.json` from a predictable path to reconstruct the exact DAG structure (nodes, edges, types, content hashes) for any run, even if the run crashed.

## Test plan

- `TestSaveRunIdAndSnapshotTime` — `save()` populates the new fields; null by default (backward compatible)
- `TestWriteDagSnapshot` — `_write_dag_snapshot()` creates the file at the correct path; returns `None` for in-memory/cloud databases
- `TestRunWritesDagSnapshot` — `run()` writes the snapshot before node execution; `run_id` in snapshot matches observer; second run overwrites with new `run_id`

Closes PLT-1161
EOF
)"
```

---

## Quick Reference

**Run all snapshot tests:**
```bash
uv run pytest tests/test_pipeline/test_dag_snapshot.py -v
```

**Run full test suite:**
```bash
uv run pytest tests/ -x -q
```

**Key files:**
- Implementation: `src/orcapod/pipeline/graph.py`
- Tests: `tests/test_pipeline/test_dag_snapshot.py`
