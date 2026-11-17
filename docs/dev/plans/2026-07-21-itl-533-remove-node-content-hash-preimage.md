# ITL-533: Remove NODE_CONTENT_HASH_COL from record_id preimage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `NODE_CONTENT_HASH_COL` (`_node_content_hash`) from the `record_id` preimage, stored pdb rows, and the side-effect tdb path — making the pdb v1 schema authoritative without the redundant column.

**Architecture:** Six targeted file changes, TDD throughout. Shared preimage helper extracted to `function_node.py` and imported by `side_effects.py`. `_filter_by_content_hash()` deleted entirely. pdb migration extended to drop the column and recompute hashes. tdb path gains formal versioning via `TRACKING_DB_SCHEMA_VERSION = "tdb_v1"`.

**Tech Stack:** Python, PyArrow, `InMemoryArrowDatabase`, `uv run pytest`

---

## File map

| File | Change |
|---|---|
| `src/orcapod/system_constants.py` | Add `TRACKING_DB_SCHEMA_VERSION = "tdb_v1"` |
| `src/orcapod/core/nodes/function_node.py` | Extract `_build_record_id_preimage()`, update `_build_entry_id_preimage()`, remove `NODE_CONTENT_HASH_COL` from `add_pipeline_record()`, delete `_filter_by_content_hash()`, clean up `get_all_records()`, remove `pc` lazy import |
| `src/orcapod/side_effects.py` | Remove `node_content_hash_str` from `_execute_side_effect_row()` and 4 call sites, import `_build_record_id_preimage`, update `attach_databases()` to append `TRACKING_DB_SCHEMA_VERSION` |
| `src/orcapod/migrations/pipeline_db.py` | Remove `NODE_CONTENT_HASH_COL` from `_PDB_HASH_COLS`, update `_v1_pdb_schema()` to drop the column, extend `_transform_pdb_batch()` to recompute `__pipeline_base_entry_id` and `__record_id` |
| `tests/test_migrations/test_pipeline_db.py` | Update 2 existing assertions, add 6 new test cases |
| `tests/test_core/function_pod/test_node_content_hash_redundancy.py` | Already has the pre-written failing test — no edits needed here |

---

### Task 1: Add `TRACKING_DB_SCHEMA_VERSION` to `system_constants.py`

**Files:**
- Modify: `src/orcapod/system_constants.py`

- [ ] **Step 1: Add the constant**

In `src/orcapod/system_constants.py`, after line 24 (`RESULT_DB_SCHEMA_VERSION = "rdb_v1"`), add:

```python
TRACKING_DB_SCHEMA_VERSION = "tdb_v1"
```

The module-level section (lines 23–25) becomes:

```python
PIPELINE_DB_SCHEMA_VERSION = "pdb_v1"
RESULT_DB_SCHEMA_VERSION = "rdb_v1"
TRACKING_DB_SCHEMA_VERSION = "tdb_v1"
```

- [ ] **Step 2: Verify import works**

```bash
cd /home/kurouto/kurouto-jobs/dbe06232-16a2-4ea3-a06c-8908d94b8161/orcapod-python
uv run python -c "from orcapod.system_constants import TRACKING_DB_SCHEMA_VERSION; print(TRACKING_DB_SCHEMA_VERSION)"
```

Expected output: `tdb_v1`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/system_constants.py
git commit -m "feat(system_constants): add TRACKING_DB_SCHEMA_VERSION = 'tdb_v1'"
```

---

### Task 2: Extract `_build_record_id_preimage()` and update preimage methods

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Test: `tests/test_core/function_pod/test_node_content_hash_redundancy.py` (already written — `test_node_content_hash_col_not_in_preimage_keys`)

- [ ] **Step 1: Confirm the red test**

```bash
uv run pytest tests/test_core/function_pod/test_node_content_hash_redundancy.py::TestPreimageShape::test_node_content_hash_col_not_in_preimage_keys -v
```

Expected: `FAILED` — `_node_content_hash` is currently in the preimage.

- [ ] **Step 2: Add `_build_record_id_preimage()` module-level function**

In `src/orcapod/core/nodes/function_node.py`, after line 81 (after `_PIPELINE_RECOMPUTATION_INDEX_COL = "__pipeline_recomputation_index"`), insert this new module-level private function:

```python
def _build_record_id_preimage(
    tag: "TagProtocol",
    input_data: "DataProtocol",
) -> "pa.Table":
    """Build the Arrow preimage table for record_id / base_entry_id computation.

    Combines the tag's system-tag columns with the binary input data hash into a
    single-row Arrow table.  ``NODE_CONTENT_HASH_COL`` is intentionally excluded —
    it is redundant (fully determined by the table path + system tags).

    Args:
        tag: The tag datagram for the input row.
        input_data: The data datagram for the input row.

    Returns:
        A single-row ``pa.Table`` with system-tag columns and
        ``INPUT_DATA_HASH_COL`` (``large_binary``).
    """
    return tag.as_table(columns={"system_tags": True}).append_column(
        constants.INPUT_DATA_HASH_COL,
        pa.array([input_data.content_hash().to_prefixed_digest()], type=pa.large_binary()),
    )
```

- [ ] **Step 3: Update `_build_entry_id_preimage()` to delegate**

`_build_entry_id_preimage` is the method on `FunctionJobNode` (around line 1559).
Replace its entire body with a delegation call:

Old:
```python
    def _build_entry_id_preimage(
        self,
        tag: TagProtocol,
        input_data: DataProtocol,
    ) -> pa.Table:
        """Builds the shared Arrow preimage used by both entry-ID methods.

        Combines the tag's system columns with the input data hash and
        node content hash into a single-row Arrow table.

        Args:
            tag: The tag datagram for the input row.
            input_data: The data datagram for the input row.

        Returns:
            A single-row ``pa.Table`` with system-tag columns,
            ``INPUT_DATA_HASH_COL``, and ``NODE_CONTENT_HASH_COL``.
        """
        return (
            tag.as_table(columns={"system_tags": True})
            .append_column(
                constants.INPUT_DATA_HASH_COL,
                pa.array([input_data.content_hash().to_string()], type=pa.large_string()),
            )
            .append_column(
                constants.NODE_CONTENT_HASH_COL,
                pa.array([self.content_hash().to_string()], type=pa.large_string()),
            )
        )
```

New:
```python
    def _build_entry_id_preimage(
        self,
        tag: TagProtocol,
        input_data: DataProtocol,
    ) -> pa.Table:
        """Builds the shared Arrow preimage used by both entry-ID methods.

        Delegates to the module-level ``_build_record_id_preimage()`` helper.
        The preimage contains system-tag columns and ``INPUT_DATA_HASH_COL``
        (``large_binary``).  ``NODE_CONTENT_HASH_COL`` is excluded — it is
        redundant and fully determined by the table path and system tags.

        Args:
            tag: The tag datagram for the input row.
            input_data: The data datagram for the input row.

        Returns:
            A single-row ``pa.Table`` with system-tag columns and
            ``INPUT_DATA_HASH_COL``.
        """
        return _build_record_id_preimage(tag, input_data)
```

- [ ] **Step 4: Update `compute_pipeline_entry_id()` docstring**

Find the docstring of `compute_pipeline_entry_id` (around line 1615). Remove the sentence that mentions `NODE_CONTENT_HASH_COL`. Replace:

```
        ``NODE_CONTENT_HASH_COL`` is always included so that two runs processing
        identical inputs each get a distinct entry ID, regardless of table scope.
```

With:

```
        The preimage is ``system_tags + INPUT_DATA_HASH_COL + recomputation_index``.
```

- [ ] **Step 5: Update `compute_base_entry_id()` docstring**

Find the docstring (around line 1589). Replace the sentence:

```
        This value is identical to the pre-ITL-508 ``compute_pipeline_entry_id`` output:
        it hashes the tag's system columns plus ``INPUT_DATA_HASH_COL`` and
        ``NODE_CONTENT_HASH_COL``.
```

With:

```
        Hashes the tag's system-tag columns plus ``INPUT_DATA_HASH_COL`` (binary).
```

- [ ] **Step 6: Run the previously-red test — it should now pass**

```bash
uv run pytest tests/test_core/function_pod/test_node_content_hash_redundancy.py::TestPreimageShape::test_node_content_hash_col_not_in_preimage_keys -v
```

Expected: `PASSED`

- [ ] **Step 7: Run the full redundancy test file to confirm no regressions**

```bash
uv run pytest tests/test_core/function_pod/test_node_content_hash_redundancy.py -v
```

Expected: 11 pass (the previously-red test now green), 0 fail.

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(function_node): extract _build_record_id_preimage, drop NODE_CONTENT_HASH_COL from preimage"
```

---

### Task 3: Remove `NODE_CONTENT_HASH_COL` from `add_pipeline_record()`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Test: `tests/test_core/function_pod/test_node_content_hash_redundancy.py` (new test)

- [ ] **Step 1: Write the failing test**

Add this test to `TestPreimageShape` in `tests/test_core/function_pod/test_node_content_hash_redundancy.py`:

```python
    def test_add_pipeline_record_does_not_store_node_content_hash(self, double_pf):
        """``add_pipeline_record()`` must not write ``NODE_CONTENT_HASH_COL`` to the DB.

        After ITL-533, the pdb_v1 schema excludes ``__node_content_hash``.
        """
        import uuid
        from orcapod.databases import InMemoryArrowDatabase

        db = InMemoryArrowDatabase()
        src = _make_source_stream([42])
        node = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=db,
        )
        tag, data = next(iter(src.iter_data()))
        node.add_pipeline_record(
            tag=tag,
            input_data=data,
            data_record_id=uuid.uuid4(),
            computed=True,
        )
        table = db.get_all_records(node._versioned_pipeline_path)
        assert table is not None, "Pipeline record was not written."
        assert constants.NODE_CONTENT_HASH_COL not in table.column_names, (
            f"``{constants.NODE_CONTENT_HASH_COL}`` must not be stored in pdb_v1 rows."
        )
```

- [ ] **Step 2: Run to confirm it fails**

```bash
uv run pytest "tests/test_core/function_pod/test_node_content_hash_redundancy.py::TestPreimageShape::test_add_pipeline_record_does_not_store_node_content_hash" -v
```

Expected: `FAILED`

- [ ] **Step 3: Remove `NODE_CONTENT_HASH_COL` from `add_pipeline_record()`**

In `src/orcapod/core/nodes/function_node.py`, find the `meta_table = pa.table({...})` block inside `add_pipeline_record()` (around line 1725). Remove these two lines:

```python
                constants.NODE_CONTENT_HASH_COL: pa.array(
                    [self.content_hash().to_prefixed_digest()], type=pa.large_binary()
                ),
```

Also update the `add_pipeline_record()` docstring. Remove the bullet:
```
        - Output data record ID (for joining with result records)
```

... wait, that one stays. Remove only the implicit NODE_CONTENT_HASH_COL mention — the docstring's bullet list doesn't actually list it explicitly, so no docstring change is needed here.

- [ ] **Step 4: Run the new test — it should pass**

```bash
uv run pytest "tests/test_core/function_pod/test_node_content_hash_redundancy.py::TestPreimageShape::test_add_pipeline_record_does_not_store_node_content_hash" -v
```

Expected: `PASSED`

- [ ] **Step 5: Run the full redundancy test file**

```bash
uv run pytest tests/test_core/function_pod/test_node_content_hash_redundancy.py -v
```

Expected: 12 pass, 0 fail.

- [ ] **Step 6: Commit**

```bash
git add tests/test_core/function_pod/test_node_content_hash_redundancy.py \
        src/orcapod/core/nodes/function_node.py
git commit -m "feat(function_node): remove NODE_CONTENT_HASH_COL from add_pipeline_record output"
```

---

### Task 4: Delete `_filter_by_content_hash()` and clean up references

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

- [ ] **Step 1: Delete `_filter_by_content_hash()` method**

Find and delete the entire method in `function_node.py` (around lines 1144–1163):

```python
    def _filter_by_content_hash(self, table: "pa.Table") -> "pa.Table":
        """Filter *table* to rows whose ``NODE_CONTENT_HASH_COL`` matches this node.

        Only applied when ``table_scope="pipeline_hash"`` because in that mode
        multiple runs share the same DB table and must be disambiguated at read
        time.  In ``"content_hash"`` mode every run has its own table so no
        filtering is needed.
        """
        if self._table_scope != "pipeline_hash":
            return table
        col_name = constants.NODE_CONTENT_HASH_COL
        if col_name not in table.column_names:
            raise ValueError(
                f"Cannot isolate records for table_scope='pipeline_hash': "
                f"required column {col_name!r} is missing from the stored table. "
                "This may indicate records written by an older version of the code."
            )
        own_hash = self.content_hash().to_prefixed_digest()
        mask = pc.equal(table.column(col_name), own_hash)
        return table.filter(mask)
```

Delete it entirely (20 lines).

- [ ] **Step 2: Remove the `_filter_by_content_hash()` call from `_fetch_joined_records()`**

Find this line in `_fetch_joined_records()` (around line 1915):

```python
        taginfo = self._filter_by_content_hash(taginfo)
```

Delete it. The two lines around it (`taginfo_columns = tuple(taginfo.column_names)` and `taginfo_schema = taginfo.schema`) stay.

- [ ] **Step 3: Remove `NODE_CONTENT_HASH_COL` from the `drop_columns` list in `get_all_records()`**

Find the `drop_columns = [...]` block in `get_all_records()` (around lines 1815–1820):

```python
        drop_columns = [
            constants.NODE_CONTENT_HASH_COL,
            _PIPELINE_ENTRY_ID_COL,
            _PIPELINE_BASE_ENTRY_ID_COL,
            _PIPELINE_RECOMPUTATION_INDEX_COL,
        ]
```

Remove `constants.NODE_CONTENT_HASH_COL,` so it becomes:

```python
        drop_columns = [
            _PIPELINE_ENTRY_ID_COL,
            _PIPELINE_BASE_ENTRY_ID_COL,
            _PIPELINE_RECOMPUTATION_INDEX_COL,
        ]
```

Also update the docstring of `get_all_records()`. Replace:

```
        ``_PIPELINE_ENTRY_ID_COL`` and
        ``NODE_CONTENT_HASH_COL`` are always dropped — they are internal
        discriminator columns, not user-facing data.
```

With:

```
        ``_PIPELINE_ENTRY_ID_COL``, ``_PIPELINE_BASE_ENTRY_ID_COL``, and
        ``_PIPELINE_RECOMPUTATION_INDEX_COL`` are always dropped — they are
        internal discriminator columns, not user-facing data.
```

- [ ] **Step 4: Remove `pc` from the lazy-import block**

`pc` (pyarrow.compute) was only used by `_filter_by_content_hash`. Remove it from both sides of the `TYPE_CHECKING` block (around lines 64 and 67):

```python
if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc   # ← delete this line
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")  # ← delete this line
    pl = LazyModule("polars")
```

Becomes:

```python
if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
```

- [ ] **Step 5: Update the stale comment that mentions `_filter_by_content_hash()`**

Find the comment block around line 1023 inside `FunctionJobNode.from_descriptor()`:

```python
        # match the _node_content_hash column written to the DB at run time.
        # The blueprint hash stored in descriptor["content_hash"] is computed
        # from schema-only upstreams and differs from the live hash, causing
        # _filter_by_content_hash() to return zero rows in get_all_records().
        node._stored_content_hash = (
```

Replace the comment with:

```python
        # Restore the live content hash from the serialised descriptor so that
        # ``content_hash()`` on the deserialised node matches the hash that was
        # computed when the original node ran.
        node._stored_content_hash = (
```

- [ ] **Step 6: Run the function_pod tests**

```bash
uv run pytest tests/test_core/function_pod/ -v
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(function_node): delete _filter_by_content_hash, remove NODE_CONTENT_HASH_COL from get_all_records drop list"
```

---

### Task 5: Update `side_effects.py`

**Files:**
- Modify: `src/orcapod/side_effects.py`
- Test: `tests/test_core/function_pod/test_node_content_hash_redundancy.py` (new test for tdb path)

- [ ] **Step 1: Write failing tests for tdb versioning**

Add a new test class at the end of `tests/test_core/function_pod/test_node_content_hash_redundancy.py`:

```python
# ---------------------------------------------------------------------------
# tdb versioning: SideEffectJobNode must use TRACKING_DB_SCHEMA_VERSION suffix
# ---------------------------------------------------------------------------


class TestTdbVersioning:
    """``SideEffectJobNode.attach_databases()`` must write to the tdb_v1 path."""

    def test_tdb_v1_path_used_after_attach(self):
        """After attaching a DB, ``_table_path`` ends with ``TRACKING_DB_SCHEMA_VERSION``."""
        import pyarrow as pa
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.side_effects import SideEffectJobNode, SideEffectPod
        from orcapod.system_constants import TRACKING_DB_SCHEMA_VERSION

        table = pa.table({"x": pa.array([1], type=pa.int64())})
        source = ArrowTableSource(table=table, tag_columns=[], infer_nullable=True)

        def noop(x: int, ctx=None) -> None:
            pass

        pod = SideEffectPod(fn=noop, ctx_arg_name="ctx")
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=source)
        db = InMemoryArrowDatabase()
        node.attach_databases(db)

        assert node._table_path is not None
        assert node._table_path[-1] == TRACKING_DB_SCHEMA_VERSION, (
            f"Expected _table_path to end with {TRACKING_DB_SCHEMA_VERSION!r}; "
            f"got: {node._table_path!r}"
        )

    def test_tdb_v0_entries_not_visible_to_new_code(self):
        """Entries written at the bare (tdb_v0) path are not seen by the new code.

        ``SideEffectJobNode`` now reads/writes at ``...(tdb_v1,)`` — any entries
        at the old bare path are orphaned and cause no error.
        """
        import pyarrow as pa
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.side_effects import SideEffectJobNode, SideEffectPod

        table = pa.table({"x": pa.array([1], type=pa.int64())})
        source = ArrowTableSource(table=table, tag_columns=[], infer_nullable=True)

        def noop(x: int, ctx=None) -> None:
            pass

        pod = SideEffectPod(fn=noop, ctx_arg_name="ctx")
        node = SideEffectJobNode(side_effect_pod=pod, input_stream=source)
        db = InMemoryArrowDatabase()

        # Write a fake "completed" entry at the OLD bare path (tdb_v0 location).
        old_path = node.node_uri + (f"schema:{node.pipeline_hash().to_string()}",)
        fake_record_id = b"\xde\xad" * 8
        fake_row = pa.table({"record_id_hash": pa.array(["old_hash"], type=pa.large_string())})
        db.add_record(old_path, fake_record_id, fake_row)

        # Attach — the new code uses the tdb_v1 path, not old_path.
        node.attach_databases(db)

        # The new _table_path is different from old_path → old entry is orphaned.
        assert node._table_path != old_path, (
            "New _table_path must differ from tdb_v0 bare path; old entries are orphaned."
        )
        # No error raised — orphaned entries are silently ignored.
```

- [ ] **Step 2: Run the new tests to confirm they fail**

```bash
uv run pytest "tests/test_core/function_pod/test_node_content_hash_redundancy.py::TestTdbVersioning" -v
```

Expected: `FAILED` (`_table_path` does not yet end with `"tdb_v1"`).

- [ ] **Step 3: Add `TRACKING_DB_SCHEMA_VERSION` import to `side_effects.py`**

In `src/orcapod/side_effects.py`, the existing import at the top is:

```python
from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
```

Add after the last `from orcapod` import at the top of the file (before the `if TYPE_CHECKING:` block):

```python
from orcapod.system_constants import TRACKING_DB_SCHEMA_VERSION
```

- [ ] **Step 4: Update `attach_databases()` to append `TRACKING_DB_SCHEMA_VERSION`**

Find `attach_databases()` in `SideEffectJobNode` (around line 914). Replace:

```python
        self._pipeline_database = pipeline_database
        if pipeline_database is not None:
            self._table_path = self.node_uri + (
                f"schema:{self.pipeline_hash().to_string()}",
            )
        else:
            self._table_path = None
```

With:

```python
        self._pipeline_database = pipeline_database
        if pipeline_database is not None:
            self._table_path = self.node_uri + (
                f"schema:{self.pipeline_hash().to_string()}",
                TRACKING_DB_SCHEMA_VERSION,
            )
        else:
            self._table_path = None
```

Also update the docstring of `attach_databases()`. Replace:

```
        The table path is
        ``self.node_uri + (f"schema:{self.pipeline_hash().to_string()}",)`` —
        the same scoping convention used by ``FunctionNode`` and
        ``OperatorNode``.
```

With:

```
        The table path is
        ``self.node_uri + (f"schema:{self.pipeline_hash().to_string()}", TRACKING_DB_SCHEMA_VERSION)``
        — scoped by pipeline hash and tdb schema version.
```

- [ ] **Step 5: Run tdb tests — should now pass**

```bash
uv run pytest "tests/test_core/function_pod/test_node_content_hash_redundancy.py::TestTdbVersioning" -v
```

Expected: `PASSED`

- [ ] **Step 6: Update `_execute_side_effect_row()` — remove `node_content_hash_str`**

Find `_execute_side_effect_row()` (around line 294). 

Remove `node_content_hash_str: str` from the parameter list:

```python
def _execute_side_effect_row(
    *,
    fn: Callable,
    tag: TagProtocol,
    data: DataProtocol,
    pod_config: SideEffectPodConfig,
    pipeline_hash_ch: ContentHash,
    node_content_hash_str: str,   # ← delete this line
    pod_name: str,
    run_id: str | None,
    arrow_hasher: Any,
    ctx_arg_name: str = "ctx",
    pipeline_database: ArrowDatabaseProtocol | None = None,
    table_path: tuple[str, ...] | None = None,
) -> tuple[TagProtocol, DataProtocol] | None:
```

Replace the preimage construction block (the `preimage = (tag.as_table(...) ...` block, around lines 348–362) with:

```python
    from orcapod.core.nodes.function_node import _build_record_id_preimage

    # 1. Build the preimage — delegates to the shared helper used by FunctionJobNode.
    #    Appends the recomputation index (fixed at 0; side effects never recompute).
    preimage = _build_record_id_preimage(tag, data).append_column(
        _SIDE_EFFECT_RECOMPUTATION_INDEX_COL,
        pa.array([0], type=pa.int32()),
    )
```

Update the docstring of `_execute_side_effect_row()`. Remove the `node_content_hash_str` parameter description and remove its mention from the preimage description. Replace:

```
    Computes a deterministic ``record_id`` from a preimage that matches
    ``FunctionNode._build_entry_id_preimage`` plus a recomputation index of
    ``0``.  The preimage covers:

    * tag system-tag columns,
    * ``INPUT_DATA_HASH_COL`` — ``data.content_hash().to_string()``,
    * ``NODE_CONTENT_HASH_COL`` (the pod's own content hash), and
    * ``_SIDE_EFFECT_RECOMPUTATION_INDEX_COL`` fixed at ``0`` (side effects
      never recompute).
```

With:

```
    Computes a deterministic ``record_id`` from a preimage built by
    ``_build_record_id_preimage(tag, data)`` plus a fixed recomputation index
    of ``0``.  The preimage covers:

    * tag system-tag columns,
    * ``INPUT_DATA_HASH_COL`` (binary ``data.content_hash().to_prefixed_digest()``), and
    * ``_SIDE_EFFECT_RECOMPUTATION_INDEX_COL`` fixed at ``0`` (side effects
      never recompute).
```

Also remove the `node_content_hash_str` entry from the Args section:

```
        node_content_hash_str: ``pod.content_hash().to_string()`` — included in
            the preimage as ``NODE_CONTENT_HASH_COL`` to scope the record ID
            to this specific pod version.
```

- [ ] **Step 7: Remove `node_content_hash_str=...` from the 4 call sites**

There are 4 calls to `_execute_side_effect_row`. In each, delete the `node_content_hash_str=self._pod.content_hash().to_string(),` kwarg line.

**Call site 1** — `SideEffectPodStream.iter_data()` (around line 242):
```python
            result = _execute_side_effect_row(
                fn=self._pod._fn,
                tag=tag,
                data=data,
                pod_config=self._pod.pod_config,
                pipeline_hash_ch=self.pipeline_hash(),
                node_content_hash_str=self._pod.content_hash().to_string(),  # ← delete
                pod_name=self._pod.label,
                run_id=None,
                arrow_hasher=self._pod.data_context.arrow_hasher,
```

**Call site 2** — `SideEffectNode.iter_data()` (around line 822):
Same pattern — delete the `node_content_hash_str=...` line.

**Call site 3** — `SideEffectJobNode.execute()` (around line 960):
Same pattern — delete the `node_content_hash_str=...` line.

**Call site 4** — async path inside `SideEffectJobNode` (around line 1020):
Same pattern — delete the `node_content_hash_str=...` line.

- [ ] **Step 8: Run side-effect tests**

```bash
uv run pytest tests/test_core/side_effect_pod/ tests/test_core/side_effect_function/ -v
```

Expected: all pass.

- [ ] **Step 9: Run full redundancy test file**

```bash
uv run pytest tests/test_core/function_pod/test_node_content_hash_redundancy.py -v
```

Expected: all pass (including the two new `TestTdbVersioning` tests).

- [ ] **Step 10: Commit**

```bash
git add src/orcapod/side_effects.py \
        tests/test_core/function_pod/test_node_content_hash_redundancy.py
git commit -m "feat(side_effects): remove node_content_hash_str from preimage, use tdb_v1 path"
```

---

### Task 6: Extend pdb migration

**Files:**
- Modify: `src/orcapod/migrations/pipeline_db.py`
- Modify: `tests/test_migrations/test_pipeline_db.py`

- [ ] **Step 1: Update existing migration tests that will break**

In `tests/test_migrations/test_pipeline_db.py`, two tests currently assert that `NODE_CONTENT_HASH_COL` is present in v1. Update them to assert it is **absent**.

**`test_v1_hash_columns_are_binary`**: replace the assertion that checks `NODE_CONTENT_HASH_COL` type:

Old:
```python
        assert v1_table.schema.field(constants.NODE_CONTENT_HASH_COL).type == pa.large_binary()
        assert v1_table.schema.field(constants.INPUT_DATA_HASH_COL).type == pa.large_binary()
        assert v1_table.schema.field(constants.OUTPUT_DATA_HASH_COL).type == pa.large_binary()
```

New:
```python
        assert constants.NODE_CONTENT_HASH_COL not in v1_table.schema.names, (
            "__node_content_hash must be dropped from pdb_v1 rows."
        )
        assert v1_table.schema.field(constants.INPUT_DATA_HASH_COL).type == pa.large_binary()
        assert v1_table.schema.field(constants.OUTPUT_DATA_HASH_COL).type == pa.large_binary()
```

**`test_v1_binary_values_decode_correctly`**: remove the `NODE_CONTENT_HASH_COL` decode assertion:

Old:
```python
        assert ContentHash.from_prefixed_digest(bytes(row_dict[constants.NODE_CONTENT_HASH_COL])) == _NODE_HASH
        assert ContentHash.from_prefixed_digest(bytes(row_dict[constants.INPUT_DATA_HASH_COL])) == _INPUT_HASH
        assert ContentHash.from_prefixed_digest(bytes(row_dict[constants.OUTPUT_DATA_HASH_COL])) == _OUTPUT_HASH
```

New:
```python
        assert constants.NODE_CONTENT_HASH_COL not in row_dict, (
            "__node_content_hash must be absent from migrated v1 rows."
        )
        assert ContentHash.from_prefixed_digest(bytes(row_dict[constants.INPUT_DATA_HASH_COL])) == _INPUT_HASH
        assert ContentHash.from_prefixed_digest(bytes(row_dict[constants.OUTPUT_DATA_HASH_COL])) == _OUTPUT_HASH
```

- [ ] **Step 2: Run the modified existing tests — confirm they fail**

```bash
uv run pytest tests/test_migrations/test_pipeline_db.py::TestMigratePipelineV0ToV1::test_v1_hash_columns_are_binary \
              tests/test_migrations/test_pipeline_db.py::TestMigratePipelineV0ToV1::test_v1_binary_values_decode_correctly -v
```

Expected: `FAILED` — `NODE_CONTENT_HASH_COL` is currently still written.

- [ ] **Step 3: Add a helper for v0 rows with system tags + add new failing tests**

Add at the top of `tests/test_migrations/test_pipeline_db.py`, after the existing `_write_v0_pdb_row` helper:

```python
_SYS_TAG_COL = f"{constants.SYSTEM_TAG_PREFIX}source_id::deadbeef"
_SYS_TAG_VAL = "source_abc"


def _write_v0_pdb_row_with_system_tags(
    db: InMemoryArrowDatabase,
    pdb_path: tuple,
    data_id: bytes,
    pdb_record_id: bytes,
) -> None:
    """Write a v0 pdb row that includes a system-tag column.

    Used to test that the extended migration correctly identifies system-tag
    columns and includes them in the recomputed preimage.
    """
    row = pa.table({
        _SYS_TAG_COL: pa.array([_SYS_TAG_VAL], type=pa.large_string()),
        constants.DATA_RECORD_ID: pa.array([data_id], type=pa.large_binary()),
        constants.NODE_CONTENT_HASH_COL: pa.array([_NODE_HASH.to_string()], type=pa.large_string()),
        constants.INPUT_DATA_HASH_COL: pa.array([_INPUT_HASH.to_string()], type=pa.large_string()),
        constants.OUTPUT_DATA_HASH_COL: pa.array([_OUTPUT_HASH.to_string()], type=pa.large_string()),
        f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": pa.array(["ctx"], type=pa.large_string()),
        f"{constants.META_PREFIX}computed": pa.array([True], type=pa.bool_()),
        constants.IS_EPHEMERAL_COL: pa.array([False], type=pa.bool_()),
        "__pipeline_base_entry_id": pa.array([b"old_base"], type=pa.large_binary()),
        "__pipeline_recomputation_index": pa.array([0], type=pa.int32()),
    })
    db.add_record(pdb_path, pdb_record_id, row)
    db.flush()
```

Then add a new test class at the end of the file:

```python
class TestMigratePipelineV0ToV1Extended:
    """Tests for the ITL-533 extensions to migrate_pipeline_v0_to_v1."""

    def test_migrate_v0_to_v1_drops_node_content_hash(self):
        """Migrated v1 rows must not contain ``__node_content_hash``."""
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        _write_v0_pdb_row_with_system_tags(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)

        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        assert v1_table is not None
        assert constants.NODE_CONTENT_HASH_COL not in v1_table.schema.names, (
            "``__node_content_hash`` must be dropped from pdb_v1 rows."
        )

    def test_migrate_v0_to_v1_recomputes_base_entry_id(self):
        """``__pipeline_base_entry_id`` is recomputed using only system_tags + INPUT_DATA_HASH_COL."""
        import pyarrow as pa
        from orcapod.hashing.defaults import get_default_arrow_hasher

        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        _write_v0_pdb_row_with_system_tags(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)

        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        assert v1_table is not None
        row = v1_table.to_pylist()[0]

        # Independently compute the expected base_entry_id.
        arrow_hasher = get_default_arrow_hasher()
        preimage = pa.table({
            _SYS_TAG_COL: pa.array([_SYS_TAG_VAL], type=pa.large_string()),
            constants.INPUT_DATA_HASH_COL: pa.array(
                [_INPUT_HASH.to_prefixed_digest()], type=pa.large_binary()
            ),
        })
        expected = arrow_hasher.hash_table(preimage).to_prefixed_digest()

        actual = bytes(row["__pipeline_base_entry_id"])
        assert actual == expected, (
            f"Recomputed base_entry_id mismatch.\n"
            f"Expected: {expected!r}\n"
            f"Actual:   {actual!r}"
        )

    def test_migrate_v0_to_v1_recomputes_record_id(self):
        """``__record_id`` is recomputed using system_tags + INPUT_DATA_HASH_COL + recomputation_index."""
        import pyarrow as pa
        from orcapod.hashing.defaults import get_default_arrow_hasher

        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        _write_v0_pdb_row_with_system_tags(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)

        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path, record_id_column="__record_id")
        assert v1_table is not None
        row = v1_table.to_pylist()[0]

        arrow_hasher = get_default_arrow_hasher()
        preimage = pa.table({
            _SYS_TAG_COL: pa.array([_SYS_TAG_VAL], type=pa.large_string()),
            constants.INPUT_DATA_HASH_COL: pa.array(
                [_INPUT_HASH.to_prefixed_digest()], type=pa.large_binary()
            ),
        })
        preimage_with_idx = preimage.append_column(
            "__pipeline_recomputation_index",
            pa.array([0], type=pa.int32()),
        )
        expected = arrow_hasher.hash_table(preimage_with_idx).to_prefixed_digest()

        actual = bytes(row["__record_id"])
        assert actual == expected, (
            f"Recomputed record_id mismatch.\n"
            f"Expected: {expected!r}\n"
            f"Actual:   {actual!r}"
        )

    def test_migrate_v0_to_v1_extended_idempotent(self):
        """Running the extended migration twice produces no duplicate v1 rows."""
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        _write_v0_pdb_row_with_system_tags(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)
        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)

        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        assert v1_table is not None
        assert v1_table.num_rows == 1, (
            f"Expected exactly 1 v1 row after two migration runs; got {v1_table.num_rows}."
        )
```

- [ ] **Step 4: Run all migration tests — confirm new tests fail**

```bash
uv run pytest tests/test_migrations/test_pipeline_db.py -v
```

Expected: `test_v1_hash_columns_are_binary` FAIL, `test_v1_binary_values_decode_correctly` FAIL, all 4 new `TestMigratePipelineV0ToV1Extended` tests FAIL, others PASS.

- [ ] **Step 5: Implement the extended migration in `pipeline_db.py`**

**5a. Update `_PDB_HASH_COLS`** — remove `NODE_CONTENT_HASH_COL` (it is dropped from v1, no longer converted):

```python
# pdb columns whose values are ContentHash strings in v0 and must become binary in v1.
# NODE_CONTENT_HASH_COL is intentionally excluded — it is dropped during migration.
_PDB_HASH_COLS = frozenset({
    constants.INPUT_DATA_HASH_COL,
    constants.OUTPUT_DATA_HASH_COL,
})
```

**5b. Add `get_default_arrow_hasher` import** at the top of `pipeline_db.py`, after the existing imports:

```python
from orcapod.hashing.defaults import get_default_arrow_hasher
```

**5c. Acquire the arrow hasher in `migrate_pipeline_v0_to_v1()`** — add one line before the batch loop:

```python
    rows_migrated = 0
    rows_skipped = 0
    rows_unresolvable = 0
    arrow_hasher = get_default_arrow_hasher()   # ← add this line

    for batch_start in range(0, rows_total, batch_size):
```

**5d. Pass `arrow_hasher` to `_transform_pdb_batch()`** — update the call:

```python
        transformed, batch_unresolvable = _transform_pdb_batch(new_rows, rdb_index, arrow_hasher)
```

**5e. Update `_transform_pdb_batch()` signature and body**:

Replace the full function with:

```python
def _transform_pdb_batch(
    batch: pa.Table,
    rdb_index: dict[bytes, dict],
    arrow_hasher: Any,
) -> tuple[pa.Table, int]:
    """Transform a batch of v0 pdb rows into v1 format.

    Per-row transformations (applied in order):

    1. Drop ``__node_content_hash`` — not stored in v1.
    2. Convert ``__input_data_hash`` from ``large_string`` → ``large_binary``
       (falls back to rdb index when the pdb value is ``None``).
    3. Convert ``__output_data_hash`` from ``large_string`` → ``large_binary``
       (no rdb fallback; ``None`` stays ``null``).
    4. Recompute ``__pipeline_base_entry_id`` and ``__record_id`` using the new
       preimage (``system_tag_cols + INPUT_DATA_HASH_COL``, without
       ``NODE_CONTENT_HASH_COL``).  Rows where ``__input_data_hash`` cannot be
       resolved are counted as unresolvable and their hash columns are left
       ``null``.

    Args:
        batch: Arrow table slice of v0 pdb rows (with ``_RECORD_ID_COL`` as first
            column, as returned by ``get_all_records(record_id_column=...)``).
        rdb_index: Dict mapping rdb record-ID bytes to row dicts.
        arrow_hasher: ``ArrowHasherProtocol`` used for recomputing the hash columns.

    Returns:
        Tuple of (transformed Arrow table, count of unresolvable rows).
    """
    node_hash_col = constants.NODE_CONTENT_HASH_COL
    input_hash_col = constants.INPUT_DATA_HASH_COL
    output_hash_col = constants.OUTPUT_DATA_HASH_COL
    data_id_col = constants.DATA_RECORD_ID
    sys_tag_prefix = constants.SYSTEM_TAG_PREFIX

    rows = batch.to_pylist()
    unresolvable = 0
    out_rows: list[dict] = []

    for row in rows:
        new_row = dict(row)

        # 1. Drop __node_content_hash — not stored in v1.
        new_row.pop(node_hash_col, None)

        # 2. Convert __input_data_hash from string → binary.
        val = new_row.get(input_hash_col)
        if val is not None and isinstance(val, str):
            new_row[input_hash_col] = ContentHash.from_string(val).to_prefixed_digest()
        elif val is None:
            # Missing in pdb — fall back to rdb index.
            data_id = row.get(data_id_col)
            if data_id is not None:
                data_id_bytes = bytes(data_id)
                rdb_row = rdb_index.get(data_id_bytes)
                if rdb_row is not None:
                    raw = rdb_row.get(input_hash_col)
                    if raw is not None and isinstance(raw, str):
                        new_row[input_hash_col] = ContentHash.from_string(raw).to_prefixed_digest()
                else:
                    unresolvable += 1

        # 3. Convert __output_data_hash from string → binary.
        val = new_row.get(output_hash_col)
        if val is not None and isinstance(val, str):
            new_row[output_hash_col] = ContentHash.from_string(val).to_prefixed_digest()

        # 4. Recompute __pipeline_base_entry_id and __record_id using new preimage.
        input_hash_bytes = new_row.get(input_hash_col)
        if input_hash_bytes is not None:
            sys_tag_cols = sorted(c for c in row if c.startswith(sys_tag_prefix))
            preimage_arrays: dict[str, pa.Array] = {}
            for col in sys_tag_cols:
                preimage_arrays[col] = pa.array([row.get(col)], type=pa.large_string())
            preimage_arrays[input_hash_col] = pa.array(
                [input_hash_bytes], type=pa.large_binary()
            )
            preimage = pa.table(preimage_arrays)

            new_base_entry_id = arrow_hasher.hash_table(preimage).to_prefixed_digest()
            new_row["__pipeline_base_entry_id"] = new_base_entry_id

            recomp_idx = new_row.get("__pipeline_recomputation_index") or 0
            preimage_with_idx = preimage.append_column(
                "__pipeline_recomputation_index",
                pa.array([recomp_idx], type=pa.int32()),
            )
            new_row[_RECORD_ID_COL] = arrow_hasher.hash_table(preimage_with_idx).to_prefixed_digest()

        out_rows.append(new_row)

    transformed = pa.Table.from_pylist(out_rows, schema=_v1_pdb_schema(batch))
    return transformed, unresolvable
```

**5f. Update `_v1_pdb_schema()` to exclude `NODE_CONTENT_HASH_COL`**:

```python
def _v1_pdb_schema(v0_batch: pa.Table) -> pa.Schema:
    """Derive the v1 pdb Arrow schema from a v0 batch.

    Drops ``__node_content_hash`` (removed in pdb_v1) and replaces the two
    remaining ContentHash columns (``__input_data_hash``, ``__output_data_hash``)
    with ``large_binary`` equivalents.  All other columns retain their original
    types.

    Args:
        v0_batch: A v0 pdb Arrow table (used to read non-hash column types).

    Returns:
        Arrow schema for the v1 pdb table.
    """
    node_hash_col = constants.NODE_CONTENT_HASH_COL
    fields = []
    for field in v0_batch.schema:
        if field.name == node_hash_col:
            continue  # Drop __node_content_hash from v1.
        if field.name in _PDB_HASH_COLS:
            fields.append(pa.field(field.name, pa.large_binary(), nullable=True))
        else:
            fields.append(field)
    # Ensure the remaining hash columns exist even if absent in v0.
    existing_names = {f.name for f in fields}
    for col in _PDB_HASH_COLS:
        if col not in existing_names:
            fields.append(pa.field(col, pa.large_binary(), nullable=True))
    return pa.schema(fields)
```

- [ ] **Step 6: Run all migration tests — all must pass**

```bash
uv run pytest tests/test_migrations/test_pipeline_db.py -v
```

Expected: all pass (both updated existing tests and all 4 new tests).

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/migrations/pipeline_db.py \
        tests/test_migrations/test_pipeline_db.py
git commit -m "feat(migrations): extend pdb v0→v1 to drop __node_content_hash and recompute hash columns"
```

---

### Task 7: Full suite verification

- [ ] **Step 1: Run the entire test suite**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -40
```

Expected: all tests pass. Key suites to watch:
- `tests/test_core/function_pod/` — all pass
- `tests/test_core/side_effect_pod/` — all pass
- `tests/test_core/side_effect_function/` — all pass
- `tests/test_migrations/` — all pass
- `tests/test_core/function_pod/test_node_content_hash_redundancy.py` — all 14 pass (was 12; 2 added in Task 3 + 2 `TestTdbVersioning`)

- [ ] **Step 2: If any test fails, diagnose and fix before committing**

For `function_pod` failures: check for any remaining `NODE_CONTENT_HASH_COL` reference in assertion messages or expected schema shapes.

For `side_effect_pod/side_effect_function` failures: check that all 4 call sites to `_execute_side_effect_row()` have `node_content_hash_str` removed.

For migration failures: check `_transform_pdb_batch` handles rows that have no system_tag columns (the existing test fixtures use rows without them — in that case `sys_tag_cols` is empty and the preimage is just `{INPUT_DATA_HASH_COL: ...}`).

- [ ] **Step 3: Final commit (if any fixes were needed)**

```bash
git add -p   # stage only the fix hunks
git commit -m "fix: address full-suite failures after NODE_CONTENT_HASH_COL removal"
```

---

## Self-review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| `_build_entry_id_preimage()` returns only `system_tags + INPUT_DATA_HASH_COL` | Task 2 |
| `_execute_side_effect_row()` uses same preimage; `node_content_hash_str` removed | Task 5 |
| `add_pipeline_record()` no longer writes `__node_content_hash` | Task 3 |
| `_filter_by_content_hash()` deleted; call site removed | Task 4 |
| `get_all_records()` stops referencing `__node_content_hash` | Task 4 |
| Shared `_build_record_id_preimage()` helper extracted | Task 2 |
| `PIPELINE_DB_SCHEMA_VERSION` remains `"pdb_v1"` | No change needed |
| `TRACKING_DB_SCHEMA_VERSION = "tdb_v1"` added | Task 1 |
| `SideEffectJobNode` appends `TRACKING_DB_SCHEMA_VERSION` to `_table_path` | Task 5 |
| `migrate_pipeline_v0_to_v1()` drops `__node_content_hash` and recomputes hashes | Task 6 |
| `test_node_content_hash_col_not_in_preimage_keys` turns green | Task 2 |
| New migration tests: drop, recompute_base, recompute_record, idempotent | Task 6 |
| `test_tdb_v1_path_used`, `test_tdb_v0_entries_orphaned` | Task 5 |

All spec requirements covered. ✓
