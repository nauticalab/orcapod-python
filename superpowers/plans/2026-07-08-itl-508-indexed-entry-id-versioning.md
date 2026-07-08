# ITL-508: Indexed Entry ID Versioning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single-`entry_id` pipeline DB key with a `(base_entry_id, recomputation_index)` versioning scheme so that miss-triggered Phase 2 recomputation always writes a fresh pipeline record rather than being silently blocked by a stale duplicate key.

**Architecture:** `compute_base_entry_id` (stable identity, used as in-memory cache key) + `compute_pipeline_entry_id(…, recomputation_index)` (versioned DB primary key). `add_pipeline_record` reads the max existing index for a `base_entry_id` and writes at `max+1`, so both single-threaded reruns and concurrent asyncio tasks eventually resolve to a valid pipeline record. Phase 1 filters the pipeline DB by `_PIPELINE_BASE_ENTRY_ID_COL` rather than the versioned key, so all recomputation-index versions are loaded and the inner-join naturally drops stale rows.

**Tech Stack:** Python 3.12, PyArrow, Polars (for joins), asyncio TaskGroup, pytest-asyncio.

---

## File Map

| Role | Path |
|---|---|
| **Primary implementation** | `src/orcapod/core/nodes/function_node.py` |
| **Protocol update** | `src/orcapod/protocols/pipeline_protocols.py` |
| **New entry-id tests** | `tests/test_core/function_pod/test_function_node_caching.py` |
| **Ephemeral / recomputation tests** | `tests/test_core/function_pod/test_ephemeral_result.py` |
| **Fetch-joined tests** | `tests/test_core/nodes/test_function_node_fetch_joined.py` |
| **Get-cached tests** | `tests/test_core/nodes/test_function_node_get_cached.py` |

---

## Task 1 — Add constants, `compute_base_entry_id`, and extend `compute_pipeline_entry_id`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (near line 73)
- Modify: `tests/test_core/function_pod/test_function_node_caching.py`

- [ ] **Step 1: Write failing tests for the new functions**

Add a `TestComputeBaseEntryId` class and a `TestVersionedEntryIdDiffersByIndex` class to `tests/test_core/function_pod/test_function_node_caching.py` immediately after the existing imports:

```python
# ---------------------------------------------------------------------------
# compute_base_entry_id
# ---------------------------------------------------------------------------


class TestComputeBaseEntryId:
    def test_returns_non_empty_bytes(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        data = Data({"x": 10})
        base_entry_id = node.compute_base_entry_id(tag, data)
        assert isinstance(base_entry_id, bytes)
        assert len(base_entry_id) > 0
        assert b":" in base_entry_id

    def test_same_inputs_produce_same_id(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        data = Data({"x": 10})
        assert node.compute_base_entry_id(tag, data) == node.compute_base_entry_id(tag, data)

    def test_different_tags_produce_different_ids(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        data = Data({"x": 10})
        assert node.compute_base_entry_id(Tag({"id": 0}), data) != node.compute_base_entry_id(Tag({"id": 1}), data)

    def test_different_data_produce_different_ids(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        assert node.compute_base_entry_id(tag, Data({"x": 10})) != node.compute_base_entry_id(tag, Data({"x": 99}))

    def test_differs_from_versioned_entry_id_at_index_zero(self):
        """After ITL-508, compute_pipeline_entry_id includes the index in the preimage,
        so compute_pipeline_entry_id(tag, data, 0) != compute_base_entry_id(tag, data)."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        data = Data({"x": 10})
        base_id = node.compute_base_entry_id(tag, data)
        versioned_id_0 = node.compute_pipeline_entry_id(tag, data, 0)
        assert base_id != versioned_id_0


# ---------------------------------------------------------------------------
# compute_pipeline_entry_id with recomputation_index
# ---------------------------------------------------------------------------


class TestVersionedEntryIdDiffersByIndex:
    def test_different_indices_produce_different_ids(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        data = Data({"x": 10})
        assert node.compute_pipeline_entry_id(tag, data, 0) != node.compute_pipeline_entry_id(tag, data, 1)

    def test_same_index_produces_same_id(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        data = Data({"x": 10})
        assert node.compute_pipeline_entry_id(tag, data, 1) == node.compute_pipeline_entry_id(tag, data, 1)

    def test_default_index_zero_is_consistent(self):
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag = Tag({"id": 0})
        data = Data({"x": 10})
        assert node.compute_pipeline_entry_id(tag, data) == node.compute_pipeline_entry_id(tag, data, 0)
```

- [ ] **Step 2: Run tests to confirm they fail**

```
uv run pytest tests/test_core/function_pod/test_function_node_caching.py::TestComputeBaseEntryId tests/test_core/function_pod/test_function_node_caching.py::TestVersionedEntryIdDiffersByIndex -v
```

Expected: `AttributeError: 'FunctionJobNode' object has no attribute 'compute_base_entry_id'`

- [ ] **Step 3: Add the two new constants and `compute_base_entry_id` to `function_node.py`**

Directly after line 73 (`_PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"`), add:

```python
# Column storing the stable base entry ID (hash without recomputation index).
# Used for filtering in _fetch_joined_records and as the in-memory cache key.
_PIPELINE_BASE_ENTRY_ID_COL = "__pipeline_base_entry_id"

# Column storing the recomputation chain index (pa.int32).
# 0 for the first computation, N+1 for each miss-triggered recompute.
_PIPELINE_RECOMPUTATION_INDEX_COL = "__pipeline_recomputation_index"
```

- [ ] **Step 4: Add `compute_base_entry_id` method to `FunctionJobNode`**

Add immediately **before** the existing `compute_pipeline_entry_id` method (around line 1373):

```python
def compute_base_entry_id(
    self, tag: TagProtocol, input_data: DataProtocol
) -> bytes:
    """Compute the stable base entry ID for a (tag, input_data, node) combination.

    Identical to the pre-ITL-508 ``compute_pipeline_entry_id`` implementation.
    Stable across all recomputation attempts for the same logical input.
    Used as the in-memory cache key (``_cached_output_datas``) and stored in
    ``_PIPELINE_BASE_ENTRY_ID_COL``.

    Args:
        tag: The tag (including system tags).
        input_data: The input data.

    Returns:
        Method-prefixed raw bytes (``b"{method}:{digest}"``) uniquely
        identifying this (tag, input_data, node) combination. Suitable
        for storage in a ``pa.large_binary()`` column.
    """
    tag_with_hash = (
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
    return self.data_context.arrow_hasher.hash_table(tag_with_hash).to_prefixed_digest()
```

- [ ] **Step 5: Extend `compute_pipeline_entry_id` signature with `recomputation_index`**

Replace the existing `compute_pipeline_entry_id` body (starting around line 1373) with:

```python
def compute_pipeline_entry_id(
    self,
    tag: TagProtocol,
    input_data: DataProtocol,
    recomputation_index: int = 0,
) -> bytes:
    """Compute a versioned pipeline entry ID from tag + system tags + input data hash + index.

    Appends a ``_PIPELINE_RECOMPUTATION_INDEX_COL`` column (value:
    ``recomputation_index``, type ``pa.int32()``) to the hash preimage so
    that each recomputation attempt receives a distinct DB primary key.

    At ``recomputation_index=0`` this produces a hash that differs from the
    pre-ITL-508 implementation (the index column is now part of the preimage).
    Existing pipeline DB records are implicitly invalidated — acceptable
    because the project is pre-v0.1.0.

    ``NODE_CONTENT_HASH_COL`` is always included so that two runs processing
    identical inputs each get a distinct entry ID, regardless of table scope.

    Args:
        tag: The tag (including system tags).
        input_data: The input data.
        recomputation_index: Position in the recomputation chain. ``0`` for
            the first computation, ``N+1`` for each miss-triggered recompute.

    Returns:
        Method-prefixed raw bytes (``b"{method}:{digest}"``) uniquely
        identifying this (tag, input_data, node run, recomputation attempt).
        Suitable for storage in a ``pa.large_binary()`` column.
    """
    tag_with_hash = (
        tag.as_table(columns={"system_tags": True})
        .append_column(
            constants.INPUT_DATA_HASH_COL,
            pa.array([input_data.content_hash().to_string()], type=pa.large_string()),
        )
        .append_column(
            constants.NODE_CONTENT_HASH_COL,
            pa.array([self.content_hash().to_string()], type=pa.large_string()),
        )
        .append_column(
            _PIPELINE_RECOMPUTATION_INDEX_COL,
            pa.array([recomputation_index], type=pa.int32()),
        )
    )
    return self.data_context.arrow_hasher.hash_table(tag_with_hash).to_prefixed_digest()
```

- [ ] **Step 6: Run tests to confirm they pass**

```
uv run pytest tests/test_core/function_pod/test_function_node_caching.py::TestComputeBaseEntryId tests/test_core/function_pod/test_function_node_caching.py::TestVersionedEntryIdDiffersByIndex -v
```

Expected: all PASS

- [ ] **Step 7: Confirm existing `TestComputePipelineEntryId` tests still pass**

```
uv run pytest tests/test_core/function_pod/test_function_node_caching.py::TestComputePipelineEntryId -v
```

Expected: all PASS (same-inputs-same-id, different-inputs-different-ids still hold at default index 0)

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/function_pod/test_function_node_caching.py
git commit -m "feat(ITL-508): add compute_base_entry_id and versioned compute_pipeline_entry_id"
```

---

## Task 2 — Redesign `add_pipeline_record` + remove `skip_cache_lookup` from protocol

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Modify: `src/orcapod/protocols/pipeline_protocols.py`
- Modify: `tests/test_core/function_pod/test_ephemeral_result.py`

- [ ] **Step 1: Write failing tests for the redesigned `add_pipeline_record`**

Replace the entire `TestAddPipelineRecordDeduplication` class in `tests/test_core/function_pod/test_ephemeral_result.py` (lines 779–809) with:

```python
# ---------------------------------------------------------------------------
# Task 2 tests: redesigned add_pipeline_record (indexed, no skip_cache_lookup)
# ---------------------------------------------------------------------------


class TestAddPipelineRecordIndexed:
    def test_first_call_writes_at_index_zero(self):
        """add_pipeline_record writes recomputation_index=0 on the first call."""
        from orcapod.core.nodes.function_node import _PIPELINE_RECOMPUTATION_INDEX_COL
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert all_records.num_rows == 1
        assert all_records.column(_PIPELINE_RECOMPUTATION_INDEX_COL)[0].as_py() == 0

    def test_second_call_writes_at_index_one(self):
        """Second add_pipeline_record call for the same base_entry_id writes at index 1."""
        from orcapod.core.nodes.function_node import _PIPELINE_RECOMPUTATION_INDEX_COL
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()
        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert all_records.num_rows == 2
        indices = all_records.column(_PIPELINE_RECOMPUTATION_INDEX_COL).to_pylist()
        assert sorted(indices) == [0, 1]

    def test_base_entry_id_column_written(self):
        """add_pipeline_record writes _PIPELINE_BASE_ENTRY_ID_COL to the pipeline DB row."""
        from orcapod.core.nodes.function_node import _PIPELINE_BASE_ENTRY_ID_COL
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert _PIPELINE_BASE_ENTRY_ID_COL in all_records.column_names
        expected_base_id = node.compute_base_entry_id(tag, data)
        assert all_records.column(_PIPELINE_BASE_ENTRY_ID_COL)[0].as_py() == expected_base_id

    def test_skip_cache_lookup_parameter_removed(self):
        """add_pipeline_record no longer accepts skip_cache_lookup — raises TypeError."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag, data = next(iter(stream.iter_data()))
        with pytest.raises(TypeError):
            node.add_pipeline_record(
                tag, data, data_record_id=uuid.uuid4(), computed=True,
                skip_cache_lookup=False,  # removed parameter
            )
```

- [ ] **Step 2: Run tests to confirm they fail**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestAddPipelineRecordIndexed -v
```

Expected: `FAILED` — `_PIPELINE_RECOMPUTATION_INDEX_COL` doesn't exist yet in pipeline DB rows; `skip_cache_lookup` still accepted.

- [ ] **Step 3: Redesign `add_pipeline_record` in `function_node.py`**

Replace the entire `add_pipeline_record` method (lines 1405–1483):

```python
def add_pipeline_record(
    self,
    tag: TagProtocol,
    input_data: DataProtocol,
    data_record_id: uuid.UUID,
    computed: bool,
    is_ephemeral: bool = False,
) -> None:
    """Add a pipeline record to the database for a processed data.

    Computes the next recomputation index by querying existing rows in the
    pipeline DB that share the same ``base_entry_id``, then writes at
    ``max_index + 1``. The write uses ``skip_duplicates=True`` so that
    concurrent asyncio coroutines competing for the same versioned entry ID
    are safely serialised: the first writer lands, subsequent writers whose
    compute happened to produce the same hash are silently no-oped.

    The pipeline record stores:

    - Tag columns (including system tags)
    - All source columns of the input data (provenance, not data values)
    - Output data record ID (for joining with result records)
    - Base entry ID (``_PIPELINE_BASE_ENTRY_ID_COL``)
    - Recomputation index (``_PIPELINE_RECOMPUTATION_INDEX_COL``)
    - Whether the result is stored in the ephemeral store
    - Input data context key
    - Whether the result was freshly computed or cached

    Args:
        tag: The tag associated with the input data.
        input_data: The input data that was processed.
        data_record_id: UUID of the result record in the result database.
        computed: Whether the result was freshly computed (``True``) or
            served from a cache (``False``).
        is_ephemeral: Whether the result is stored in the ephemeral store.
    """
    self._require_pipeline_database()
    base_entry_id = self.compute_base_entry_id(tag, input_data)

    # Determine the next recomputation index by reading all existing rows
    # that share this base_entry_id.  Steps 1–5 below are fully synchronous
    # (no await), so concurrent asyncio coroutines serialise naturally.
    existing = self._pipeline_database.get_records_with_column_value(
        self.node_identity_path,
        {_PIPELINE_BASE_ENTRY_ID_COL: base_entry_id},
    )
    if existing is None or existing.num_rows == 0:
        new_index = 0
    else:
        indices = existing.column(_PIPELINE_RECOMPUTATION_INDEX_COL).to_pylist()
        new_index = max(indices) + 1

    versioned_entry_id = self.compute_pipeline_entry_id(tag, input_data, new_index)

    # Extract source columns only (no data columns) from the input data
    input_table_with_source = input_data.as_table(columns={"source": True})
    source_col_names = [
        c
        for c in input_table_with_source.column_names
        if c.startswith(constants.SOURCE_PREFIX)
    ]
    input_source_table = input_table_with_source.select(source_col_names)

    # Build the meta columns table
    meta_table = pa.table(
        {
            constants.DATA_RECORD_ID: pa.array(
                [data_record_id.bytes], type=pa.large_binary()
            ),
            constants.NODE_CONTENT_HASH_COL: pa.array(
                [self.content_hash().to_string()], type=pa.large_string()
            ),
            f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": pa.array(
                [input_data.data_context_key], type=pa.large_string()
            ),
            f"{constants.META_PREFIX}computed": pa.array(
                [computed], type=pa.bool_()
            ),
            constants.IS_EPHEMERAL_COL: pa.array(
                [is_ephemeral], type=pa.bool_()
            ),
            _PIPELINE_BASE_ENTRY_ID_COL: pa.array(
                [base_entry_id], type=pa.large_binary()
            ),
            _PIPELINE_RECOMPUTATION_INDEX_COL: pa.array(
                [new_index], type=pa.int32()
            ),
        }
    )

    # Combine: tag (with system tags) + input source columns + meta columns
    combined_record = arrow_utils.hstack_tables(
        tag.as_table(columns={"system_tags": True}),
        input_source_table,
        meta_table,
    )

    self._pipeline_database.add_record(
        self.node_identity_path,
        versioned_entry_id,
        combined_record,
        skip_duplicates=True,
    )
```

- [ ] **Step 4: Remove `skip_cache_lookup` from `pipeline_protocols.py`**

In `src/orcapod/protocols/pipeline_protocols.py`, replace:

```python
    def add_pipeline_record(
        self,
        tag: cp.TagProtocol,
        input_data: cp.DataProtocol,
        data_record_id: str,
        retrieved: bool | None = None,
        skip_cache_lookup: bool = False,
    ) -> None: ...
```

with:

```python
    def add_pipeline_record(
        self,
        tag: cp.TagProtocol,
        input_data: cp.DataProtocol,
        data_record_id: str,
        retrieved: bool | None = None,
    ) -> None: ...
```

- [ ] **Step 5: Run tests to confirm they pass**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestAddPipelineRecordIndexed -v
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py src/orcapod/protocols/pipeline_protocols.py tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "feat(ITL-508): redesign add_pipeline_record with recomputation index; remove skip_cache_lookup"
```

---

## Task 3 — Update Phase 2 callers: `_process_data_internal` and `_async_process_data_internal`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

These two methods need two changes each:
1. Remove `skip_cache_lookup=True` from `add_pipeline_record` calls (parameter removed).
2. Switch `_cached_output_datas` key from `compute_pipeline_entry_id` to `compute_base_entry_id`.

- [ ] **Step 1: Verify that the pre-written TDD test currently FAILS**

The test at line 631 of `test_ephemeral_result.py` describes session 3 NOT recomputing after a persistent miss + recompute. With the old code it currently fails because the pipeline record at index 1 was never written.

```
uv run pytest "tests/test_core/function_pod/test_ephemeral_result.py::TestPersistentMissWarning::test_recompute_after_persistent_miss_appends_new_pipeline_record" -v
```

Expected: `FAILED` (session 3 recomputes; call_count reaches 3 instead of staying at 2)

Also verify the ephemeral-miss cycle test currently fails:

```
uv run pytest "tests/test_core/function_pod/test_ephemeral_result.py::TestEphemeralWritePath::test_recompute_after_ephemeral_miss_no_infinite_cycle" -v
```

Expected: `FAILED`

- [ ] **Step 2: Update `_process_data_internal`**

In `_process_data_internal`, make two sets of changes:

**a) Ephemeral branch** — remove `skip_cache_lookup=True`:

Replace:
```python
                if self._pipeline_database is not None:
                    self.add_pipeline_record(
                        tag,
                        data,
                        data_record_id=output_data.datagram_uuid,
                        computed=result_computed,
                        skip_cache_lookup=True,
                        is_ephemeral=True,
                    )
```

With:
```python
                if self._pipeline_database is not None:
                    self.add_pipeline_record(
                        tag,
                        data,
                        data_record_id=output_data.datagram_uuid,
                        computed=result_computed,
                        is_ephemeral=True,
                    )
```

**b) Persistent branch** — remove `skip_cache_lookup=True`:

Replace:
```python
                self.add_pipeline_record(
                    tag,
                    data,
                    data_record_id=output_data.datagram_uuid,
                    computed=result_computed,
                    skip_cache_lookup=True,
                )
```

With:
```python
                self.add_pipeline_record(
                    tag,
                    data,
                    data_record_id=output_data.datagram_uuid,
                    computed=result_computed,
                )
```

**c) Cache write** — switch key from versioned to base:

Replace:
```python
        # Store by entry_id and invalidate derived caches
        entry_id = self.compute_pipeline_entry_id(tag, data)
        self._cached_output_datas[entry_id] = (tag_out, output_data)
```

With:
```python
        # Store by base_entry_id (stable across recomputation cycles) and invalidate caches
        base_entry_id = self.compute_base_entry_id(tag, data)
        self._cached_output_datas[base_entry_id] = (tag_out, output_data)
```

- [ ] **Step 3: Update `_async_process_data_internal`** (identical changes)

**a) Ephemeral branch** — remove `skip_cache_lookup=True`:

Replace:
```python
                if self._pipeline_database is not None:
                    self.add_pipeline_record(
                        tag,
                        data,
                        data_record_id=output_data.datagram_uuid,
                        computed=result_computed,
                        skip_cache_lookup=True,
                        is_ephemeral=True,
                    )
```

With:
```python
                if self._pipeline_database is not None:
                    self.add_pipeline_record(
                        tag,
                        data,
                        data_record_id=output_data.datagram_uuid,
                        computed=result_computed,
                        is_ephemeral=True,
                    )
```

**b) Persistent branch** — remove `skip_cache_lookup=True`:

Replace:
```python
                self.add_pipeline_record(
                    tag,
                    data,
                    data_record_id=output_data.datagram_uuid,
                    computed=result_computed,
                    skip_cache_lookup=True,
                )
```

With:
```python
                self.add_pipeline_record(
                    tag,
                    data,
                    data_record_id=output_data.datagram_uuid,
                    computed=result_computed,
                )
```

**c) Cache write** — switch key from versioned to base:

Replace:
```python
        # Store by entry_id and invalidate derived caches
        entry_id = self.compute_pipeline_entry_id(tag, data)
        self._cached_output_datas[entry_id] = (tag_out, output_data)
```

With:
```python
        # Store by base_entry_id (stable across recomputation cycles) and invalidate caches
        base_entry_id = self.compute_base_entry_id(tag, data)
        self._cached_output_datas[base_entry_id] = (tag_out, output_data)
```

- [ ] **Step 4: Run the pre-written TDD tests to confirm they now pass**

```
uv run pytest "tests/test_core/function_pod/test_ephemeral_result.py::TestPersistentMissWarning::test_recompute_after_persistent_miss_appends_new_pipeline_record" "tests/test_core/function_pod/test_ephemeral_result.py::TestEphemeralWritePath::test_recompute_after_ephemeral_miss_no_infinite_cycle" -v
```

Expected: both PASS

- [ ] **Step 5: Run the full ephemeral test suite to verify no regressions**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py -v
```

Expected: all PASS (some tests that previously called `add_pipeline_record` with `skip_cache_lookup` will now fail — those are addressed in Task 5's regression fixes)

Note: `TestAddPipelineRecord` tests use `node.execute(stream)` not direct `add_pipeline_record` calls — they will still pass. The only broken test is the now-deleted `TestAddPipelineRecordDeduplication.test_duplicate_not_added_when_skip_cache_lookup_false` (replaced in Task 2).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "feat(ITL-508): switch Phase 2 cache key to base_entry_id; remove skip_cache_lookup from callers"
```

---

## Task 4 — Update Phase 1 internals: `get_cached_results`, `_fetch_joined_records`, `_load_cached_entries`, `get_all_records`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Modify: `tests/test_core/nodes/test_function_node_fetch_joined.py`
- Modify: `tests/test_core/nodes/test_function_node_get_cached.py`

- [ ] **Step 1: Write failing tests for the renamed parameter in `_fetch_joined_records`**

In `tests/test_core/nodes/test_function_node_fetch_joined.py`, update the import at line 11:

Replace:
```python
from orcapod.core.nodes.function_node import FunctionJobNode, _PIPELINE_ENTRY_ID_COL
```

With:
```python
from orcapod.core.nodes.function_node import (
    FunctionJobNode,
    _PIPELINE_ENTRY_ID_COL,
    _PIPELINE_BASE_ENTRY_ID_COL,
)
```

Then update `test_entry_ids_filter_narrows_rows` (currently lines 97–105):

Replace:
```python
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

With:
```python
    def test_entry_ids_filter_narrows_rows(self, executed_node):
        """Passing a single base_entry_id returns only that row."""
        node = executed_node
        input_pairs = list(node._input_stream.iter_data())
        base_entry_id_0 = node.compute_base_entry_id(input_pairs[0][0], input_pairs[0][1])

        result = node._fetch_joined_records(base_entry_ids=[base_entry_id_0])
        assert result is not None
        assert result.table.num_rows == 1
        assert result.table.column(_PIPELINE_BASE_ENTRY_ID_COL)[0].as_py() == base_entry_id_0
```

And update `test_empty_entry_ids_returns_zero_rows`:

Replace:
```python
        result = executed_node._fetch_joined_records(entry_ids=[])
```

With:
```python
        result = executed_node._fetch_joined_records(base_entry_ids=[])
```

- [ ] **Step 2: Write failing tests for the renamed parameter in `get_cached_results`**

In `tests/test_core/nodes/test_function_node_get_cached.py`, update all tests that call `compute_pipeline_entry_id` and `get_cached_results(entry_ids)`:

Replace the three tests `test_returns_cached_results_for_matching_entry_ids`, `test_filters_to_requested_entry_ids_only`, and `test_get_cached_results_populates_internal_cache` entirely:

```python
    def test_returns_cached_results_for_matching_entry_ids(self, function_node_with_db):
        node = function_node_with_db
        all_pairs = list(node._input_stream.iter_data())

        base_entry_ids = []
        for tag, data in all_pairs:
            node.execute_data(tag, data)
            base_entry_ids.append(node.compute_base_entry_id(tag, data))

        cached = node.get_cached_results(base_entry_ids)
        assert len(cached) == 2
        assert all(eid in cached for eid in base_entry_ids)

    def test_filters_to_requested_entry_ids_only(self, function_node_with_db):
        node = function_node_with_db
        all_pairs = list(node._input_stream.iter_data())

        base_entry_ids = []
        for tag, data in all_pairs:
            node.execute_data(tag, data)
            base_entry_ids.append(node.compute_base_entry_id(tag, data))

        cached = node.get_cached_results([base_entry_ids[0]])
        assert len(cached) == 1
        assert base_entry_ids[0] in cached
        assert base_entry_ids[1] not in cached

    def test_get_cached_results_populates_internal_cache(self, function_node_with_db):
        """get_cached_results should populate _cached_output_datas keyed by base_entry_id."""
        node = function_node_with_db
        all_pairs = list(node._input_stream.iter_data())

        base_entry_ids = []
        for tag, data in all_pairs:
            node.execute_data(tag, data)
            base_entry_ids.append(node.compute_base_entry_id(tag, data))

        # Clear internal cache
        node._cached_output_datas.clear()
        assert len(node._cached_output_datas) == 0

        # get_cached_results should repopulate
        node.get_cached_results(base_entry_ids)
        assert len(node._cached_output_datas) == 2
```

- [ ] **Step 3: Run tests to confirm they fail**

```
uv run pytest tests/test_core/nodes/test_function_node_fetch_joined.py tests/test_core/nodes/test_function_node_get_cached.py -v
```

Expected: several `FAILED` — parameter name mismatch or `_PIPELINE_BASE_ENTRY_ID_COL` not present in DB rows yet.

- [ ] **Step 4: Update `get_cached_results` in `function_node.py`**

Replace the method signature and body (lines ~1254–1293):

```python
def get_cached_results(
    self, base_entry_ids: list[bytes]
) -> dict[bytes, tuple[TagProtocol, DataProtocol]]:
    """Public cache façade: return already-computed results for the given base entry IDs.

    Serves hits directly from the in-memory cache (``_cached_output_datas``).
    For IDs not yet cached, delegates to ``_load_cached_entries`` which calls
    ``_fetch_joined_records`` to load from the pipeline and result databases.
    Add-only semantics: existing in-memory entries are never cleared or
    overwritten.

    Args:
        base_entry_ids: Stable base entry IDs (from ``compute_base_entry_id``)
            to look up.

    Returns:
        Mapping from base_entry_id to ``(tag, output_data)`` for found entries.
        Empty dict if no DB is attached, ``base_entry_ids`` is empty, or no
        matches are found.
    """
    if self._cached_function_pod is None or not base_entry_ids:
        return {}

    missing = [eid for eid in base_entry_ids if eid not in self._cached_output_datas]
    if missing:
        loaded = self._load_cached_entries(base_entry_ids=missing)
        self._cached_output_datas.update(loaded)
        if loaded:
            self._cached_output_table = None
            self._cached_content_hash_column = None

    return {
        eid: self._cached_output_datas[eid]
        for eid in base_entry_ids
        if eid in self._cached_output_datas
        and self._cached_output_datas[eid][1] is not None
    }
```

- [ ] **Step 5: Update `_fetch_joined_records` in `function_node.py`**

**a) Rename the parameter** (line ~1582):

Replace:
```python
    def _fetch_joined_records(
        self,
        entry_ids: list[bytes] | None = None,
    ) -> _JoinedRecords | None:
```

With:
```python
    def _fetch_joined_records(
        self,
        base_entry_ids: list[bytes] | None = None,
    ) -> _JoinedRecords | None:
```

Also update the docstring parameter description: change `entry_ids` → `base_entry_ids` and `_PIPELINE_ENTRY_ID_COL` → `_PIPELINE_BASE_ENTRY_ID_COL` in the description.

**b) Update the anti-join** to use `_PIPELINE_BASE_ENTRY_ID_COL` (lines ~1698–1704):

Replace:
```python
        if ephemeral_df.height > 0 and persistent_df.height > 0:
            ephemeral_only_df = ephemeral_df.join(
                persistent_df.select([_PIPELINE_ENTRY_ID_COL]),
                on=_PIPELINE_ENTRY_ID_COL,
                how="anti",
            )
```

With:
```python
        if ephemeral_df.height > 0 and persistent_df.height > 0:
            ephemeral_only_df = ephemeral_df.join(
                persistent_df.select([_PIPELINE_BASE_ENTRY_ID_COL]),
                on=_PIPELINE_BASE_ENTRY_ID_COL,
                how="anti",
            )
```

**c) Update the filter** to use `_PIPELINE_BASE_ENTRY_ID_COL` (lines ~1714–1718):

Replace:
```python
        # Apply entry_id filter if requested
        if entry_ids is not None:
            merged_df = merged_df.filter(
                pl.col(_PIPELINE_ENTRY_ID_COL).is_in(entry_ids)
            )
```

With:
```python
        # Apply base_entry_id filter if requested
        if base_entry_ids is not None:
            merged_df = merged_df.filter(
                pl.col(_PIPELINE_BASE_ENTRY_ID_COL).is_in(base_entry_ids)
            )
```

- [ ] **Step 6: Update `_load_cached_entries` in `function_node.py`**

**a) Rename the parameter** (line ~1728):

Replace:
```python
    def _load_cached_entries(
        self,
        entry_ids: list[bytes] | None = None,
    ) -> "dict[bytes, tuple[TagProtocol, DataProtocol]]":
```

With:
```python
    def _load_cached_entries(
        self,
        base_entry_ids: list[bytes] | None = None,
    ) -> "dict[bytes, tuple[TagProtocol, DataProtocol]]":
```

**b) Update the internal `_fetch_joined_records` call** (line ~1755):

Replace:
```python
        fetched = self._fetch_joined_records(entry_ids=entry_ids)
```

With:
```python
        fetched = self._fetch_joined_records(base_entry_ids=base_entry_ids)
```

**c) Update the column extraction and dict key** (lines ~1781–1793):

Replace:
```python
        entry_ids_col = joined.column(_PIPELINE_ENTRY_ID_COL).to_pylist()
        drop_cols = [
            c
            for c in joined.column_names
            if c.startswith(constants.META_PREFIX)
            or c == constants.NODE_CONTENT_HASH_COL
        ]
        data_table = joined.drop([c for c in drop_cols if c in joined.column_names])
        stream = ArrowTableStream(data_table, tag_columns=tag_keys)

        loaded: dict[bytes, tuple[TagProtocol, DataProtocol]] = {}
        for eid, (tag, data) in zip(entry_ids_col, stream.iter_data()):
            loaded[eid] = (tag, data)
        return loaded
```

With:
```python
        base_entry_ids_col = joined.column(_PIPELINE_BASE_ENTRY_ID_COL).to_pylist()
        drop_cols = [
            c
            for c in joined.column_names
            if c.startswith(constants.META_PREFIX)
            or c == constants.NODE_CONTENT_HASH_COL
        ]
        data_table = joined.drop([c for c in drop_cols if c in joined.column_names])
        stream = ArrowTableStream(data_table, tag_columns=tag_keys)

        loaded: dict[bytes, tuple[TagProtocol, DataProtocol]] = {}
        for base_eid, (tag, data) in zip(base_entry_ids_col, stream.iter_data()):
            loaded[base_eid] = (tag, data)
        return loaded
```

- [ ] **Step 7: Update `get_all_records` to always-drop the new internal columns**

In `get_all_records`, replace (lines ~1527–1528):

```python
        drop_columns = [constants.NODE_CONTENT_HASH_COL, _PIPELINE_ENTRY_ID_COL]
```

With:

```python
        drop_columns = [
            constants.NODE_CONTENT_HASH_COL,
            _PIPELINE_ENTRY_ID_COL,
            _PIPELINE_BASE_ENTRY_ID_COL,
            _PIPELINE_RECOMPUTATION_INDEX_COL,
        ]
```

- [ ] **Step 8: Run tests to confirm they pass**

```
uv run pytest tests/test_core/nodes/test_function_node_fetch_joined.py tests/test_core/nodes/test_function_node_get_cached.py -v
```

Expected: all PASS

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/nodes/test_function_node_fetch_joined.py tests/test_core/nodes/test_function_node_get_cached.py
git commit -m "feat(ITL-508): re-key Phase 1 internals to base_entry_id; update fetch/load/get_cached"
```

---

## Task 5 — Update `execute()` and `async_execute()` Phase 1 callers

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

- [ ] **Step 1: Update `execute()` to use `compute_base_entry_id`**

In `execute()` (around lines 1112–1131), replace:

```python
        # Collect upstream entries and resolve entry_ids
        upstream_entries: list[tuple[TagProtocol, DataProtocol, bytes]] = [
            (tag, data, self.compute_pipeline_entry_id(tag, data))
            for tag, data in input_stream.iter_data()
        ]
        entry_ids = [eid for _, _, eid in upstream_entries]

        # Hot-load any already-computed results from DB into _cached_output_datas.
        # get_cached_results() is called for its side effect (populating the
        # in-memory cache); the returned dict is intentionally discarded here so
        # that the per-data cache-hit check below uses _cached_output_datas
        # directly — which includes None-output entries (function returned None)
        # and prevents spurious recomputation of already-processed data.
        self.get_cached_results(entry_ids=entry_ids)

        output: list[tuple[TagProtocol, DataProtocol]] = []
        for tag, data, entry_id in upstream_entries:
            ctx_obs.on_data_start(node_label, tag, data)

            if entry_id in self._cached_output_datas:
                tag_out, result = self._cached_output_datas[entry_id]
```

With:

```python
        # Collect upstream entries and resolve base_entry_ids (stable across recomputation)
        upstream_entries: list[tuple[TagProtocol, DataProtocol, bytes]] = [
            (tag, data, self.compute_base_entry_id(tag, data))
            for tag, data in input_stream.iter_data()
        ]
        base_entry_ids = [eid for _, _, eid in upstream_entries]

        # Hot-load any already-computed results from DB into _cached_output_datas.
        # get_cached_results() is called for its side effect (populating the
        # in-memory cache); the returned dict is intentionally discarded here so
        # that the per-data cache-hit check below uses _cached_output_datas
        # directly — which includes None-output entries (function returned None)
        # and prevents spurious recomputation of already-processed data.
        self.get_cached_results(base_entry_ids=base_entry_ids)

        output: list[tuple[TagProtocol, DataProtocol]] = []
        for tag, data, base_entry_id in upstream_entries:
            ctx_obs.on_data_start(node_label, tag, data)

            if base_entry_id in self._cached_output_datas:
                tag_out, result = self._cached_output_datas[base_entry_id]
```

- [ ] **Step 2: Update `async_execute()` DB path to use `compute_base_entry_id`**

In `async_execute()` (around lines 1984–2004), replace:

```python
                cached_by_entry_id: dict[bytes, tuple[TagProtocol, DataProtocol]] = dict(loaded)

                # Phase 2: drive output from input channel — cached or compute
                async def _process_one_db(
                    tag: TagProtocol, data: DataProtocol
                ) -> None:
                    entry_id = self.compute_pipeline_entry_id(tag, data)
                    if entry_id in cached_by_entry_id:
                        tag_out, result_data = cached_by_entry_id[entry_id]
```

With:

```python
                cached_by_base_entry_id: dict[bytes, tuple[TagProtocol, DataProtocol]] = dict(loaded)

                # Phase 2: drive output from input channel — cached or compute
                async def _process_one_db(
                    tag: TagProtocol, data: DataProtocol
                ) -> None:
                    base_entry_id = self.compute_base_entry_id(tag, data)
                    if base_entry_id in cached_by_base_entry_id:
                        tag_out, result_data = cached_by_base_entry_id[base_entry_id]
```

- [ ] **Step 3: Run the full caching and ephemeral test suites**

```
uv run pytest tests/test_core/function_pod/test_function_node_caching.py tests/test_core/function_pod/test_ephemeral_result.py -v
```

Expected: all PASS

- [ ] **Step 4: Run the node protocol tests**

```
uv run pytest tests/test_pipeline/test_node_protocols.py -v
```

Expected: all PASS (no changes needed — mock classes don't test `add_pipeline_record` signature)

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "feat(ITL-508): update execute() and async_execute() Phase 1 to use base_entry_id"
```

---

## Task 6 — Add concurrent-miss asyncio test + run full suite

**Files:**
- Modify: `tests/test_core/function_pod/test_ephemeral_result.py`

- [ ] **Step 1: Add `TestConcurrentMissSerialization` class**

Add at the end of `tests/test_core/function_pod/test_ephemeral_result.py`:

```python
# ---------------------------------------------------------------------------
# Task 6 tests: concurrent asyncio Phase 2 serialisation
# ---------------------------------------------------------------------------


class TestConcurrentMissSerialization:
    @pytest.mark.asyncio
    async def test_two_concurrent_phase2_misses_produce_valid_pipeline_records(self):
        """Two asyncio coroutines that simultaneously execute Phase 2 for the same input
        each produce a valid pipeline record. A subsequent Phase 1 lookup finds a result
        and does NOT recompute."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )

        tag, data = next(iter(stream.iter_data()))

        # Two concurrent _async_process_data_internal calls on the same (tag, data).
        # In asyncio cooperative multitasking, these serialise at add_pipeline_record
        # (synchronous), so each gets a distinct recomputation_index.
        async with asyncio.TaskGroup() as tg:
            tg.create_task(node._async_process_data_internal(tag, data))
            tg.create_task(node._async_process_data_internal(tag, data))

        # At least one pipeline record must exist for this base_entry_id
        pipeline_db.flush()
        all_records = pipeline_db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert all_records.num_rows >= 1

        # Session 2: new node with the same DBs — Phase 1 must find a valid result
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        count_before_session2 = call_count["n"]
        results = node2.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == count_before_session2  # NOT recomputed

    @pytest.mark.asyncio
    async def test_sequential_add_pipeline_record_increments_index_each_time(self):
        """Two sequential add_pipeline_record calls for the same base_entry_id
        write at indices 0 and 1 respectively (not blocked by the existing row)."""
        from orcapod.core.nodes.function_node import (
            _PIPELINE_BASE_ENTRY_ID_COL,
            _PIPELINE_RECOMPUTATION_INDEX_COL,
        )

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert all_records.num_rows == 2

        from orcapod.core.nodes.function_node import _PIPELINE_BASE_ENTRY_ID_COL
        base_ids = all_records.column(_PIPELINE_BASE_ENTRY_ID_COL).to_pylist()
        assert base_ids[0] == base_ids[1]  # same base_entry_id

        indices = all_records.column(_PIPELINE_RECOMPUTATION_INDEX_COL).to_pylist()
        assert sorted(indices) == [0, 1]  # distinct indices
```

- [ ] **Step 2: Run the new concurrent tests**

```
uv run pytest tests/test_core/function_pod/test_ephemeral_result.py::TestConcurrentMissSerialization -v
```

Expected: both PASS

- [ ] **Step 3: Run the full test suite**

```
uv run pytest tests/ -v --tb=short 2>&1 | tail -40
```

Expected: all tests pass, no regressions.

- [ ] **Step 4: Commit**

```bash
git add tests/test_core/function_pod/test_ephemeral_result.py
git commit -m "test(ITL-508): add concurrent-miss asyncio serialisation tests"
```

---

## Self-Review

### Spec coverage check

| Spec section | Task that covers it |
|---|---|
| §1 New pipeline DB columns (`_PIPELINE_BASE_ENTRY_ID_COL`, `_PIPELINE_RECOMPUTATION_INDEX_COL`) | Task 2 Step 3 |
| §2 `compute_base_entry_id` | Task 1 Steps 3–4 |
| §2 Extended `compute_pipeline_entry_id` | Task 1 Step 5 |
| §3 In-memory cache re-keying (`_cached_output_datas` → `base_entry_id`) | Task 3 Steps 2–3 |
| §4 Phase 1 `execute()` and `async_execute()` | Task 5 Steps 1–2 |
| §4 `get_cached_results(base_entry_ids)` | Task 4 Step 4 |
| §4 `_fetch_joined_records(base_entry_ids)` + filter on BASE col | Task 4 Step 5 |
| §4 `_load_cached_entries` keyed by BASE col | Task 4 Step 6 |
| §5 `add_pipeline_record` redesign (remove `skip_cache_lookup`, max+1 index) | Task 2 Step 3 |
| §5 Phase 2 callers updated | Task 3 Steps 2–3 |
| §6 Anti-join on `_PIPELINE_BASE_ENTRY_ID_COL` (persistent wins over ephemeral) | Task 4 Step 5b |
| §7 Protocol update (remove `skip_cache_lookup`) | Task 2 Step 4 |
| Testing: single-threaded correctness | Pre-written in `test_ephemeral_result.py` (Task 3 Step 4) |
| Testing: concurrent-miss serialisation | Task 6 Step 1 |
| Testing: ephemeral + persistent coexistence | Existing `test_persistent_result_outcompetes_ephemeral` |
| Testing: regression | Task 6 Step 3 |

### Placeholder scan

No TBD or TODO markers. All steps contain exact code.

### Type consistency

- `compute_base_entry_id` returns `bytes` — consistent with `compute_pipeline_entry_id` return type.
- `_PIPELINE_BASE_ENTRY_ID_COL` is `pa.large_binary()` in the pipeline record — consistent with `base_entry_id: bytes` type used in `get_records_with_column_value` filter and `is_in` filter.
- `_PIPELINE_RECOMPUTATION_INDEX_COL` is `pa.int32()` — `max(indices)` where `indices` is a Python `list[int]` from `.to_pylist()` — consistent.
- `get_cached_results(base_entry_ids: list[bytes])` — callers in `execute()` and `async_execute()` pass `base_entry_ids` built from `compute_base_entry_id` which returns `bytes` — consistent.
- `_fetch_joined_records(base_entry_ids: list[bytes] | None)` — callers pass either `None` (load all) or a `list[bytes]` from `compute_base_entry_id` — consistent.
- `_load_cached_entries(base_entry_ids: list[bytes] | None)` — same as above — consistent.
