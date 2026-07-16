# EmptyData + Ephemeral Result Propagation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Propagate ephemeral result misses as `EmptyData` tokens through the pipeline, enabling downstream pods to recover via their own result cache rather than failing silently or blocking.

**Architecture:** Add `EmptyData(Data)` — a payload-free `Data` subclass carrying an optional cached content hash — then wire it into the pipeline DB read/write path and the downstream execution guard. `EmptyData.content_hash()` returns the stored hash, making `ResultCache.lookup()` work transparently; a guard in `_process_data_internal()` raises `EphemeralResultMissingError` on downstream cache miss rather than attempting computation.

**Tech Stack:** Python, PyArrow, Polars (for anti-join in `_fetch_joined_records`), `orcapod` internal APIs (`ResultCache`, `CachedFunctionPod`, `FunctionJobNode`, `ArrowTableStream`).

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/errors.py` | Modify | Add `EmptyDataAccessError`, `EmptyDataHashMissingError`, `EphemeralResultMissingError` |
| `src/orcapod/core/datagrams/tag_data.py` | Modify | Add `EmptyData(Data)` class |
| `src/orcapod/core/datagrams/__init__.py` | Modify | Export `EmptyData` |
| `src/orcapod/core/cached_function_pod.py` | Modify | Expose `result_cache` property |
| `src/orcapod/core/nodes/function_node.py` | Modify | Write path, read path, downstream guard |
| `tests/test_core/test_empty_data.py` | Create | Unit tests for `EmptyData` |
| `tests/test_core/test_result_cache.py` | Modify | Add lookup-via-`EmptyData` tests |
| `tests/test_core/nodes/test_function_node_empty_data.py` | Create | Integration tests for ephemeral-miss flow |

---

## Task 1: Exception types

**Files:**
- Modify: `src/orcapod/errors.py`
- Test: `tests/test_core/test_empty_data.py` (created here, extended in Task 2)

- [ ] **Step 1: Create the test file with exception smoke-tests**

```python
# tests/test_core/test_empty_data.py
"""Tests for EmptyData and its associated exception types."""
from __future__ import annotations

import pytest

from orcapod.errors import (
    EmptyDataAccessError,
    EmptyDataHashMissingError,
    EphemeralResultMissingError,
)


class TestExceptionTypes:
    def test_empty_data_access_error_is_exception(self):
        exc = EmptyDataAccessError("sentinel", "as_dict")
        assert isinstance(exc, Exception)
        assert exc.empty_data == "sentinel"
        assert exc.method_name == "as_dict"

    def test_empty_data_hash_missing_error_is_exception(self):
        exc = EmptyDataHashMissingError("sentinel")
        assert isinstance(exc, Exception)
        assert exc.empty_data == "sentinel"

    def test_ephemeral_result_missing_error_is_exception(self):
        exc = EphemeralResultMissingError(
            tag="tag",
            cached_content_hash=None,
            node_identity_path=("a", "b"),
            message="gone",
        )
        assert isinstance(exc, Exception)
        assert exc.tag == "tag"
        assert exc.cached_content_hash is None
        assert exc.node_identity_path == ("a", "b")
        assert "gone" in str(exc)
```

- [ ] **Step 2: Run the test to verify it fails**

```
uv run pytest tests/test_core/test_empty_data.py::TestExceptionTypes -v
```

Expected: `ImportError` — `EmptyDataAccessError` not found in `errors.py`.

- [ ] **Step 3: Add exception classes to `src/orcapod/errors.py`**

Append at the end of the file:

```python
class EmptyDataAccessError(Exception):
    """Raised when a payload-access method is called on an ``EmptyData`` instance.

    ``EmptyData`` carries no data payload. Any method that would access the
    underlying columns (``as_dict``, ``as_table``, ``keys``, ``schema``,
    ``arrow_schema``, ``identity_structure``) raises this exception instead of
    returning empty or ``None`` results.

    Attributes:
        empty_data: The ``EmptyData`` instance on which the method was called.
        method_name: The name of the method that was called.
    """

    def __init__(self, empty_data: object, method_name: str) -> None:
        self.empty_data = empty_data
        self.method_name = method_name
        super().__init__(
            f"Cannot call {method_name!r} on EmptyData — "
            "this instance carries no data payload. "
            "Check isinstance(data, EmptyData) before accessing the payload."
        )


class EmptyDataHashMissingError(Exception):
    """Raised when ``content_hash()`` is called on an ``EmptyData`` that has no cached hash.

    An ``EmptyData`` constructed without a ``cached_content_hash`` is in degraded
    mode (old pipeline DB row lacking the hash column). Any code path that tries
    to use the hash fails loudly here.

    Attributes:
        empty_data: The ``EmptyData`` instance on which ``content_hash()`` was called.
    """

    def __init__(self, empty_data: object) -> None:
        self.empty_data = empty_data
        super().__init__(
            "content_hash() called on EmptyData with no cached_content_hash. "
            "This EmptyData was constructed from a pipeline DB row that predates "
            "the INPUT_DATA_HASH_COL column. Flow-through is unavailable for this row."
        )


class EphemeralResultMissingError(Exception):
    """Raised when a downstream pod has a cache miss for an ``EmptyData`` input.

    Indicates that the upstream ephemeral result is gone AND the downstream pod
    has not yet computed a result for the same input content hash. No recovery
    is possible — the information has been lost.

    Attributes:
        tag: The tag associated with the missing input.
        cached_content_hash: The cached content hash from the ``EmptyData``, or
            ``None`` if the ``EmptyData`` itself lacked a hash.
        node_identity_path: Identity path of the downstream node raising this error.
    """

    def __init__(
        self,
        tag: object,
        cached_content_hash: object,
        node_identity_path: tuple[str, ...],
        message: str,
    ) -> None:
        self.tag = tag
        self.cached_content_hash = cached_content_hash
        self.node_identity_path = node_identity_path
        super().__init__(
            f"{message} "
            f"node={node_identity_path!r} "
            f"cached_content_hash={cached_content_hash!r}"
        )
```

- [ ] **Step 4: Run the test to verify it passes**

```
uv run pytest tests/test_core/test_empty_data.py::TestExceptionTypes -v
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/errors.py tests/test_core/test_empty_data.py
git commit -m "feat(errors): add EmptyDataAccessError, EmptyDataHashMissingError, EphemeralResultMissingError (ITL-534)"
```

---

## Task 2: `EmptyData` class

**Files:**
- Modify: `src/orcapod/core/datagrams/tag_data.py`
- Test: `tests/test_core/test_empty_data.py` (extend)

- [ ] **Step 1: Add failing tests for `EmptyData`**

Append to `tests/test_core/test_empty_data.py`:

```python
import uuid

import pyarrow as pa

from orcapod.core.datagrams import Data
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.types import ContentHash


def _make_hash(hex_str: str = "a" * 64) -> ContentHash:
    return ContentHash("arrow_v2.1", bytes.fromhex(hex_str))


class TestEmptyDataSubclass:
    def test_is_data_subclass(self):
        assert issubclass(EmptyData, Data)

    def test_instance_is_data(self):
        ed = EmptyData()
        assert isinstance(ed, Data)


class TestEmptyDataContentHash:
    def test_returns_cached_hash_when_set(self):
        h = _make_hash()
        ed = EmptyData(cached_content_hash=h)
        assert ed.content_hash() == h

    def test_raises_when_no_cached_hash(self):
        ed = EmptyData()
        with pytest.raises(EmptyDataHashMissingError):
            ed.content_hash()

    def test_cached_content_hash_property(self):
        h = _make_hash()
        ed = EmptyData(cached_content_hash=h)
        assert ed.cached_content_hash is h

    def test_cached_content_hash_property_none(self):
        ed = EmptyData()
        assert ed.cached_content_hash is None


class TestEmptyDataPayloadAccess:
    """All payload-access methods must raise EmptyDataAccessError."""

    def setup_method(self):
        self.ed = EmptyData(cached_content_hash=_make_hash())

    def test_as_dict_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.as_dict()

    def test_as_table_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.as_table()

    def test_keys_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.keys()

    def test_schema_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.schema()

    def test_arrow_schema_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.arrow_schema()

    def test_identity_structure_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.identity_structure()


class TestEmptyDataSourceInfo:
    def test_empty_source_info_none_by_default(self):
        ed = EmptyData()
        assert ed.empty_source_info is None

    def test_empty_source_info_stored(self):
        si = {"source_id": "abc", "record_id": None}
        ed = EmptyData(empty_source_info=si)
        assert ed.empty_source_info == si


class TestEmptyDataMetadata:
    def test_record_uuid_assigned(self):
        ed = EmptyData()
        assert ed.datagram_uuid is not None

    def test_custom_record_uuid(self):
        uid = uuid.uuid4()
        ed = EmptyData(record_uuid=uid)
        assert ed.datagram_uuid == uid
```

- [ ] **Step 2: Run the tests to verify they fail**

```
uv run pytest tests/test_core/test_empty_data.py -k "not TestExceptionTypes" -v
```

Expected: `ImportError` — `EmptyData` not found.

- [ ] **Step 3: Implement `EmptyData` in `tag_data.py`**

Add after the `Data` class (at the bottom of `src/orcapod/core/datagrams/tag_data.py`):

```python
# ---------------------------------------------------------------------------
# EmptyData
# ---------------------------------------------------------------------------


class EmptyData(Data):
    """A ``Data``-shaped token representing missing data.

    ``EmptyData`` is produced when an upstream pod's ephemeral result has
    expired or been pruned. It carries all normal datagram metadata (data
    context, record UUID) but has no data payload. Every payload-access
    method raises ``EmptyDataAccessError`` — callers must
    ``isinstance(data, EmptyData)`` before touching columns.

    ``content_hash()`` is overridden to return ``cached_content_hash`` (the
    hash of the original input data this token stands in for). This makes
    ``ResultCache.lookup(empty_data)`` work transparently without changes to
    the cache infrastructure. If ``cached_content_hash`` is ``None`` (old
    pipeline DB row lacking the hash column), ``content_hash()`` raises
    ``EmptyDataHashMissingError`` loudly.

    ``empty_source_info`` is an optional provenance field for the future
    tag-row reconstruction follow-up. This PR defines the field; the write
    logic is deferred.

    Args:
        cached_content_hash: The content hash of the original data this token
            represents. ``None`` for old-format rows lacking the hash column.
        empty_source_info: Optional provenance dict for tag-row reconstruction
            (follow-up). Keys match tag-row source columns; ``record_id`` may
            be ``None``.
        python_schema: Optional schema hint (passed to parent).
        data_context: Data context key or instance (passed to parent).
        record_uuid: Optional explicit UUID for this token.
    """

    def __init__(
        self,
        cached_content_hash: "ContentHash | None" = None,
        empty_source_info: "dict[str, str | None] | None" = None,
        python_schema: "SchemaLike | None" = None,
        data_context: "str | contexts.DataContext | None" = None,
        record_uuid: "uuid.UUID | None" = None,
    ) -> None:
        # Initialise the parent with an empty dict — no payload columns.
        super().__init__(
            {},
            python_schema=python_schema,
            data_context=data_context,
            record_uuid=record_uuid,
        )
        self._cached_content_hash = cached_content_hash
        self._empty_source_info = empty_source_info

    # ------------------------------------------------------------------
    # Content identity
    # ------------------------------------------------------------------

    def content_hash(self, hasher=None) -> "ContentHash":
        """Return the cached content hash or raise ``EmptyDataHashMissingError``.

        The returned hash is the hash of the original input data this token
        represents, enabling ``ResultCache.lookup(empty_data)`` to find the
        downstream cached result without accessing the missing payload.

        Args:
            hasher: Ignored — ``EmptyData`` uses the stored hash directly.

        Returns:
            The ``cached_content_hash`` set at construction.

        Raises:
            EmptyDataHashMissingError: If ``cached_content_hash`` is ``None``.
        """
        from orcapod.errors import EmptyDataHashMissingError

        if self._cached_content_hash is None:
            raise EmptyDataHashMissingError(self)
        return self._cached_content_hash

    def identity_structure(self) -> "Any":
        """Always raises ``EmptyDataAccessError`` — no payload to hash."""
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "identity_structure")

    # ------------------------------------------------------------------
    # Payload-access overrides — all raise EmptyDataAccessError
    # ------------------------------------------------------------------

    def as_dict(self, *, columns=None, all_info: bool = False) -> "dict":
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "as_dict")

    def as_table(self, *, columns=None, all_info: bool = False) -> "pa.Table":
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "as_table")

    def keys(self, *, columns=None, all_info: bool = False) -> "tuple[str, ...]":
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "keys")

    def schema(self, *, columns=None, all_info: bool = False) -> "Schema":
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "schema")

    def arrow_schema(self, *, columns=None, all_info: bool = False) -> "pa.Schema":
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "arrow_schema")

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------

    @property
    def cached_content_hash(self) -> "ContentHash | None":
        """The stored content hash, or ``None`` if absent (old-format row)."""
        return self._cached_content_hash

    @property
    def empty_source_info(self) -> "dict[str, str | None] | None":
        """Optional provenance dict for future tag-row reconstruction."""
        return self._empty_source_info
```

The import block at the top of `tag_data.py` already includes `Any`, `Schema`, `SchemaLike`, `contexts`, `uuid`, and `pa` (lazy). No new imports needed.

- [ ] **Step 4: Run the tests**

```
uv run pytest tests/test_core/test_empty_data.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/datagrams/tag_data.py tests/test_core/test_empty_data.py
git commit -m "feat(datagrams): add EmptyData subclass of Data (ITL-534)"
```

---

## Task 3: Export `EmptyData`

**Files:**
- Modify: `src/orcapod/core/datagrams/__init__.py`

- [ ] **Step 1: Add `EmptyData` to the datagrams package exports**

Edit `src/orcapod/core/datagrams/__init__.py`:

```python
from .datagram import Datagram
from .tag_data import Data, EmptyData, Tag

__all__ = [
    "Datagram",
    "Tag",
    "Data",
    "EmptyData",
]
```

- [ ] **Step 2: Verify import works**

```
uv run python -c "from orcapod.core.datagrams import EmptyData; print('ok')"
```

Expected: `ok`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/core/datagrams/__init__.py
git commit -m "feat(datagrams): export EmptyData from datagrams package (ITL-534)"
```

---

## Task 4: Expose `result_cache` on `CachedFunctionPod`

**Files:**
- Modify: `src/orcapod/core/cached_function_pod.py`
- Test: `tests/test_core/test_empty_data.py` (extend with ResultCache lookup test)

`_process_data_internal` (Task 8) will need to call `result_cache.lookup(empty_data)` directly, bypassing `process_data()` to avoid triggering computation. Expose `_cache` as a public `result_cache` property.

- [ ] **Step 1: Add a failing test**

Append to `tests/test_core/test_empty_data.py`:

```python
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.result_cache import ResultCache


class TestCachedFunctionPodResultCacheProperty:
    def test_result_cache_is_result_cache_instance(self):
        """CachedFunctionPod exposes result_cache as a ResultCache."""
        import pyarrow as pa
        from orcapod.core.data_function import PythonDataFunction
        from orcapod.core.function_pod import FunctionPod
        from orcapod.databases import InMemoryArrowDatabase

        pf = PythonDataFunction(lambda x: x * 2, output_keys="result")
        pod = FunctionPod(pf)
        cached_pod = CachedFunctionPod(pod, result_database=InMemoryArrowDatabase())
        assert isinstance(cached_pod.result_cache, ResultCache)
```

- [ ] **Step 2: Run to verify it fails**

```
uv run pytest tests/test_core/test_empty_data.py::TestCachedFunctionPodResultCacheProperty -v
```

Expected: `AttributeError: 'CachedFunctionPod' object has no attribute 'result_cache'`.

- [ ] **Step 3: Add `result_cache` property to `CachedFunctionPod`**

In `src/orcapod/core/cached_function_pod.py`, add after the `record_path` property:

```python
    @property
    def result_cache(self) -> ResultCache:
        """The underlying ``ResultCache`` instance.

        Exposed so that callers (e.g. ``FunctionJobNode._process_data_internal``)
        can perform a cache-only lookup without triggering computation — necessary
        when the input is an ``EmptyData`` token.
        """
        return self._cache
```

- [ ] **Step 4: Run the test to verify it passes**

```
uv run pytest tests/test_core/test_empty_data.py::TestCachedFunctionPodResultCacheProperty -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/cached_function_pod.py tests/test_core/test_empty_data.py
git commit -m "feat(cached_function_pod): expose result_cache property (ITL-534)"
```

---

## Task 5: Extend `ResultCache` lookup with `EmptyData`

**Files:**
- Test: `tests/test_core/test_result_cache.py`

No implementation change needed — `ResultCache.lookup(empty_data)` already works because `EmptyData.content_hash()` returns the cached hash. This task adds tests to prove it.

- [ ] **Step 1: Append tests to `tests/test_core/test_result_cache.py`**

```python
# --- Add at the bottom of tests/test_core/test_result_cache.py ---

from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.types import ContentHash


class TestLookupViaEmptyData:
    def test_empty_data_cache_hit(self):
        """ResultCache.lookup works for EmptyData with a matching cached hash."""
        cache, _ = _make_cache()
        pf = _make_pf()
        input_pkt = Data({"x": 10})
        _compute_and_store(cache, pf, input_pkt)

        # EmptyData with the same hash as input_pkt
        empty = EmptyData(cached_content_hash=input_pkt.content_hash())
        found = cache.lookup(empty)
        assert found is not None
        assert found.as_dict()["result"] == 20

    def test_empty_data_cache_miss(self):
        """ResultCache.lookup returns None for EmptyData with an unknown hash."""
        cache, _ = _make_cache()
        # Store a result so the DB is non-empty
        pf = _make_pf()
        _compute_and_store(cache, pf, Data({"x": 10}))

        # EmptyData with a hash that was never stored
        unknown_hash = ContentHash("arrow_v2.1", bytes(32))
        empty = EmptyData(cached_content_hash=unknown_hash)
        assert cache.lookup(empty) is None
```

- [ ] **Step 2: Run to verify they pass immediately**

```
uv run pytest tests/test_core/test_result_cache.py::TestLookupViaEmptyData -v
```

Expected: PASS (transparent — no implementation change needed).

- [ ] **Step 3: Commit**

```bash
git add tests/test_core/test_result_cache.py
git commit -m "test(result_cache): add EmptyData cache-lookup tests (ITL-534)"
```

---

## Task 6: Store `INPUT_DATA_HASH_COL` in the tag table write path

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (lines 1488–1615, `add_pipeline_record`)
- Test: `tests/test_core/nodes/test_function_node_empty_data.py` (created here)

- [ ] **Step 1: Create the test file with a failing test for the write path**

```python
# tests/test_core/nodes/test_function_node_empty_data.py
"""Tests for EmptyData integration in FunctionJobNode."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams import Data
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.system_constants import constants


def double_value(value: int) -> int:
    return value * 2


@pytest.fixture
def persistent_node():
    """FunctionJobNode with persistent databases, two input rows."""
    table = pa.table({
        "key": pa.array(["a", "b"], type=pa.large_string()),
        "value": pa.array([1, 2], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    return FunctionJobNode(
        pod, src,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=InMemoryArrowDatabase(),
    )


class TestAddPipelineRecordStoresInputHash:
    def test_pipeline_record_contains_input_data_hash(self, persistent_node):
        """add_pipeline_record now stores INPUT_DATA_HASH_COL in the pipeline DB."""
        node = persistent_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        all_records = node._pipeline_database.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert constants.INPUT_DATA_HASH_COL in all_records.column_names

    def test_stored_hash_matches_input_content_hash(self, persistent_node):
        """The stored INPUT_DATA_HASH_COL value matches input_data.content_hash()."""
        node = persistent_node
        input_pairs = list(node._input_stream.iter_data())
        tag0, data0 = input_pairs[0]
        node.execute_data(tag0, data0)

        all_records = node._pipeline_database.get_all_records(node.node_identity_path)
        assert all_records is not None
        stored_hashes = all_records.column(constants.INPUT_DATA_HASH_COL).to_pylist()
        assert data0.content_hash().to_string() in stored_hashes
```

- [ ] **Step 2: Run to verify tests fail**

```
uv run pytest tests/test_core/nodes/test_function_node_empty_data.py::TestAddPipelineRecordStoresInputHash -v
```

Expected: FAIL — `INPUT_DATA_HASH_COL` not in column names.

- [ ] **Step 3: Add `INPUT_DATA_HASH_COL` to `add_pipeline_record`**

In `src/orcapod/core/nodes/function_node.py`, find `add_pipeline_record` (around line 1488) and locate the `meta_table` construction (around line 1602). Add the new column:

```python
        meta_table = pa.table(
            {
                constants.DATA_RECORD_ID: pa.array(
                    [data_record_id.bytes], type=pa.large_binary()
                ),
                constants.NODE_CONTENT_HASH_COL: pa.array(
                    [self.content_hash().to_string()], type=pa.large_string()
                ),
                constants.INPUT_DATA_HASH_COL: pa.array(          # NEW
                    [input_data.content_hash().to_string()], type=pa.large_string()
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
```

- [ ] **Step 4: Run tests**

```
uv run pytest tests/test_core/nodes/test_function_node_empty_data.py::TestAddPipelineRecordStoresInputHash -v
```

Expected: PASS.

- [ ] **Step 5: Run the full test suite to check for regressions**

```
uv run pytest tests/ -x -q
```

Expected: All existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/nodes/test_function_node_empty_data.py
git commit -m "feat(function_node): store INPUT_DATA_HASH_COL in pipeline DB tag table (ITL-534)"
```

---

## Task 7: Extend `_JoinedRecords` and `_fetch_joined_records`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (lines 678–697 `_JoinedRecords`, lines 1709–1855 `_fetch_joined_records`)
- Test: `tests/test_core/nodes/test_function_node_empty_data.py` (extend)

- [ ] **Step 1: Add failing tests**

Append to `tests/test_core/nodes/test_function_node_empty_data.py`:

```python
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.core.nodes.function_node import _PIPELINE_BASE_ENTRY_ID_COL
from orcapod.types import NodeConfig


@pytest.fixture
def ephemeral_node():
    """FunctionJobNode with is_result_ephemeral=True and an ephemeral store."""
    table = pa.table({
        "key": pa.array(["a"], type=pa.large_string()),
        "value": pa.array([1], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    node = FunctionJobNode(
        pod, src,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=None,
        node_config=NodeConfig(is_result_ephemeral=True),
    )
    node.set_ephemeral_store(InMemoryArrowDatabase())
    return node


class TestFetchJoinedRecordsEmptyData:
    def test_returns_empty_data_tokens_field(self, ephemeral_node):
        """_JoinedRecords now has an empty_data_tokens field (even when empty)."""
        node = ephemeral_node
        # Execute so pipeline DB has a record
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)
        result = node._fetch_joined_records()
        assert result is not None
        assert hasattr(result, "empty_data_tokens")
        assert hasattr(result, "empty_taginfo_rows")

    def test_ephemeral_miss_produces_empty_data_token(self, ephemeral_node):
        """When the ephemeral store is cleared, _fetch_joined_records emits EmptyData."""
        node = ephemeral_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        # Simulate ephemeral result expiry by replacing the ephemeral store with an empty one
        node.set_ephemeral_store(InMemoryArrowDatabase())

        result = node._fetch_joined_records()
        assert result is not None
        assert len(result.empty_data_tokens) == 1

        token = next(iter(result.empty_data_tokens.values()))
        assert isinstance(token, EmptyData)
        assert token.cached_content_hash is not None

    def test_empty_data_token_hash_matches_input(self, ephemeral_node):
        """The EmptyData token's cached hash matches the original input's content hash."""
        node = ephemeral_node
        input_pairs = list(node._input_stream.iter_data())
        tag0, data0 = input_pairs[0]
        node.execute_data(tag0, data0)

        # Clear ephemeral store
        node.set_ephemeral_store(InMemoryArrowDatabase())

        result = node._fetch_joined_records()
        assert result is not None
        token = next(iter(result.empty_data_tokens.values()))
        assert token.cached_content_hash.to_string() == data0.content_hash().to_string()

    def test_old_format_row_warns_and_produces_none_hash(
        self, ephemeral_node, caplog
    ):
        """An ephemeral tag row without INPUT_DATA_HASH_COL logs a warning, hash=None."""
        import logging
        node = ephemeral_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        # Manually delete INPUT_DATA_HASH_COL from the pipeline DB to simulate old format
        path = node.node_identity_path
        all_records = node._pipeline_database.get_all_records(path)
        assert all_records is not None
        stripped = all_records.drop([constants.INPUT_DATA_HASH_COL])
        node._pipeline_database._tables[path] = stripped  # InMemoryArrowDatabase internals

        # Clear ephemeral store to force an ephemeral miss
        node.set_ephemeral_store(InMemoryArrowDatabase())

        with caplog.at_level(logging.WARNING):
            result = node._fetch_joined_records()

        assert result is not None
        assert len(result.empty_data_tokens) == 1
        token = next(iter(result.empty_data_tokens.values()))
        assert token.cached_content_hash is None
        assert constants.INPUT_DATA_HASH_COL in caplog.text
```

Note: the last test accesses `InMemoryArrowDatabase._tables` directly. Run the full test first — if `InMemoryArrowDatabase` doesn't expose `_tables`, look at its implementation and use whatever internal attribute it uses to store tables.

- [ ] **Step 2: Run to verify they fail**

```
uv run pytest tests/test_core/nodes/test_function_node_empty_data.py::TestFetchJoinedRecordsEmptyData -v
```

Expected: `AttributeError` on `empty_data_tokens` or similar.

- [ ] **Step 3: Extend `_JoinedRecords`**

In `function_node.py`, replace the existing `_JoinedRecords` definition (around line 678):

```python
class _JoinedRecords(NamedTuple):
    """Internal result type returned by ``_fetch_joined_records``.

    Attributes:
        table: The joined ``pa.Table`` for rows that matched in the result DB.
            Always includes a ``_PIPELINE_BASE_ENTRY_ID_COL`` column.
        taginfo_columns: Column names from the pipeline database fetch,
            captured before the join. Used by ``_load_cached_entries`` to
            derive tag keys in the CACHE_ONLY fallback path.
        empty_data_tokens: Mapping from ``base_entry_id`` bytes to an
            ``EmptyData`` token for each ephemeral tag row whose result
            was not found in either store (cross-session miss). Empty dict
            when no ephemeral misses occurred.
        empty_taginfo_rows: Mapping from ``base_entry_id`` bytes to the raw
            taginfo row dict for each entry in ``empty_data_tokens``. Used
            by ``_load_cached_entries`` to reconstruct the tag for each token.
    """

    table: "pa.Table"
    taginfo_columns: tuple[str, ...]
    empty_data_tokens: "dict[bytes, EmptyData]"
    empty_taginfo_rows: "dict[bytes, dict]"
```

Add `EmptyData` to the imports at the top of `function_node.py`. Find the existing `from orcapod.core.cached_function_pod import CachedFunctionPod` line and add below it:

```python
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.errors import EphemeralResultMissingError
```

- [ ] **Step 4: Update `_fetch_joined_records` to emit `EmptyData` tokens**

In `_fetch_joined_records` (around line 1709), find the ephemeral join section. Currently it ends with the comment `# Cross-session miss: eph_results is None → silently drop ephemeral entries`. Replace the entire ephemeral-join block with:

```python
        # ------------------------------------------------------------------
        # Ephemeral join
        # ------------------------------------------------------------------
        ephemeral_df = pl.DataFrame()
        empty_data_tokens: dict[bytes, EmptyData] = {}
        empty_taginfo_rows: dict[bytes, dict] = {}

        if ephemeral_taginfo_df.height > 0:
            eph_results = None
            if self._ephemeral_cached_pod is not None:
                eph_results = self._ephemeral_cached_pod.result_database.get_all_records(
                    self._ephemeral_cached_pod.record_path,
                    record_id_column=constants.DATA_RECORD_ID,
                )
            if eph_results is not None:
                if results_schema is None:
                    results_schema = eph_results.schema
                ephemeral_df = ephemeral_taginfo_df.join(
                    pl.DataFrame(eph_results),
                    on=constants.DATA_RECORD_ID,
                    how="inner",
                )

            # Emit EmptyData tokens for unmatched ephemeral rows
            # (cross-session miss: ephemeral data is gone).
            unmatched_df = ephemeral_taginfo_df.join(
                pl.DataFrame(eph_results) if eph_results is not None else pl.DataFrame(),
                on=constants.DATA_RECORD_ID,
                how="anti",
            )
            for row in unmatched_df.iter_rows(named=True):
                base_eid = row[_PIPELINE_BASE_ENTRY_ID_COL]
                raw_hash = row.get(constants.INPUT_DATA_HASH_COL)
                if raw_hash is None:
                    logger.warning(
                        "Pipeline DB row missing %r column — EmptyData will have "
                        "no cached hash; flow-through unavailable for this row. "
                        "base_entry_id: %r",
                        constants.INPUT_DATA_HASH_COL,
                        base_eid,
                    )
                    cached_hash = None
                else:
                    from orcapod.types import ContentHash as _ContentHash
                    cached_hash = _ContentHash.from_string(raw_hash)
                empty_data_tokens[base_eid] = EmptyData(
                    cached_content_hash=cached_hash,
                    data_context=self.data_context,
                )
                empty_taginfo_rows[base_eid] = row
```

Also update the empty-result return at the bottom of `_fetch_joined_records` to include the new fields:

```python
        # No results found in either store — return empty table preserving taginfo schema
        empty_table = taginfo.slice(0, 0)
        return _JoinedRecords(
            table=empty_table,
            taginfo_columns=taginfo_columns,
            empty_data_tokens=empty_data_tokens,
            empty_taginfo_rows=empty_taginfo_rows,
        )
```

And update the final return at the bottom of `_fetch_joined_records`:

```python
        return _JoinedRecords(
            table=joined,
            taginfo_columns=taginfo_columns,
            empty_data_tokens=empty_data_tokens,
            empty_taginfo_rows=empty_taginfo_rows,
        )
```

Also update the persistent-only-result early return (where `persistent_df` and `ephemeral_df` are both empty after the merge section):

Search for `return _JoinedRecords(table=empty_table, taginfo_columns=taginfo_columns)` and any other `_JoinedRecords(...)` calls in `_fetch_joined_records` — add `empty_data_tokens=empty_data_tokens, empty_taginfo_rows=empty_taginfo_rows` to each.

- [ ] **Step 5: Run the new tests**

```
uv run pytest tests/test_core/nodes/test_function_node_empty_data.py::TestFetchJoinedRecordsEmptyData -v
```

Expected: PASS. If the old-format-row test fails due to `InMemoryArrowDatabase._tables` not being accessible, read `src/orcapod/databases/in_memory.py` and adjust the test to use the correct internal attribute.

- [ ] **Step 6: Run existing fetch-joined tests to check for regressions**

```
uv run pytest tests/test_core/nodes/test_function_node_fetch_joined.py -v
```

Expected: All PASS (the new `empty_data_tokens` and `empty_taginfo_rows` fields default to empty dicts, which is backward-compatible).

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/nodes/test_function_node_empty_data.py
git commit -m "feat(function_node): emit EmptyData tokens for ephemeral misses in _fetch_joined_records (ITL-534)"
```

---

## Task 8: Update `_load_cached_entries` to merge `EmptyData` tokens

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (lines 1856–1920, `_load_cached_entries`)
- Test: `tests/test_core/nodes/test_function_node_empty_data.py` (extend)

- [ ] **Step 1: Add a failing test**

Append to `tests/test_core/nodes/test_function_node_empty_data.py`:

```python
class TestLoadCachedEntriesEmptyData:
    def test_empty_data_token_appears_in_loaded_entries(self, ephemeral_node):
        """_load_cached_entries yields (tag, EmptyData) for ephemeral miss rows."""
        node = ephemeral_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        # Simulate expiry
        node.set_ephemeral_store(InMemoryArrowDatabase())

        loaded = node._load_cached_entries()
        assert len(loaded) == 1
        base_eid = next(iter(loaded))
        tag_out, data_out = loaded[base_eid]
        assert isinstance(data_out, EmptyData)
        assert data_out.cached_content_hash is not None

    def test_normal_result_wins_over_empty_data_token(self, persistent_node):
        """Non-ephemeral rows produce real Data, not EmptyData."""
        node = persistent_node
        for tag, data in node._input_stream.iter_data():
            node.execute_data(tag, data)

        loaded = node._load_cached_entries()
        for base_eid, (tag, data) in loaded.items():
            assert not isinstance(data, EmptyData)
            assert isinstance(data, Data)
```

- [ ] **Step 2: Run to verify they fail**

```
uv run pytest tests/test_core/nodes/test_function_node_empty_data.py::TestLoadCachedEntriesEmptyData -v
```

Expected: FAIL — `_load_cached_entries` returns `{}` when `table.num_rows == 0`.

- [ ] **Step 3: Update `_load_cached_entries`**

In `function_node.py`, replace the `_load_cached_entries` method (around line 1856) with:

```python
    def _load_cached_entries(
        self,
        base_entry_ids: list[bytes] | None = None,
    ) -> "dict[bytes, tuple[TagProtocol, DataProtocol]]":
        """DB loader: fetch ``(tag, data)`` pairs from the pipeline and result databases.

        Calls ``_fetch_joined_records`` to obtain the raw joined table, then
        converts each row into a ``(tag, data)`` tuple keyed by base entry ID.
        Also merges ``EmptyData`` tokens for ephemeral miss rows.

        If ``base_entry_ids`` is given, only those entries are fetched from DB.
        If ``None``, all records for this node are loaded.

        Does NOT read from or write to the in-memory cache
        (``_cached_output_datas``). Callers that want to populate the cache
        must call ``self._cached_output_datas.update(loaded)`` themselves.

        Args:
            base_entry_ids: If provided, load only these specific base entry IDs.
                If ``None``, load all records for this node.

        Returns:
            dict mapping base_entry_id → ``(tag, data)``. Empty dict when
            either database is absent, both fetches return nothing, and no
            ``EmptyData`` tokens exist.
        """
        from orcapod.core.datagrams.tag_data import Tag

        fetched = self._fetch_joined_records(base_entry_ids=base_entry_ids)
        if fetched is None:
            return {}
        if fetched.table.num_rows == 0 and not fetched.empty_data_tokens:
            return {}

        joined = fetched.table

        # Derive tag keys: prefer input_stream when available; fall back to
        # taginfo column exclusion for CACHE_ONLY / deserialized nodes.
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

        loaded: "dict[bytes, tuple[TagProtocol, DataProtocol]]" = {}

        # --- Process normal (fully matched) rows ---
        if joined.num_rows > 0:
            base_entry_ids_col = joined.column(_PIPELINE_BASE_ENTRY_ID_COL).to_pylist()
            drop_cols = [
                c
                for c in joined.column_names
                if c.startswith(constants.META_PREFIX)
                or c == constants.NODE_CONTENT_HASH_COL
            ]
            data_table = joined.drop([c for c in drop_cols if c in joined.column_names])
            stream = ArrowTableStream(data_table, tag_columns=tag_keys)
            for base_eid, (tag, data) in zip(base_entry_ids_col, stream.iter_data()):
                loaded[base_eid] = (tag, data)

        # --- Process EmptyData tokens (ephemeral misses) ---
        for base_eid, empty_data in fetched.empty_data_tokens.items():
            if base_eid in loaded:
                # A persistent result exists for the same entry — it wins.
                continue
            raw_row = fetched.empty_taginfo_rows[base_eid]
            tag_data = {k: v for k, v in raw_row.items() if k in tag_keys}
            system_tags = {
                k: v
                for k, v in raw_row.items()
                if k.startswith(constants.SYSTEM_TAG_PREFIX)
            }
            tag = Tag(
                tag_data, system_tags=system_tags, data_context=self.data_context
            )
            loaded[base_eid] = (tag, empty_data)

        return loaded
```

- [ ] **Step 4: Run the new tests**

```
uv run pytest tests/test_core/nodes/test_function_node_empty_data.py::TestLoadCachedEntriesEmptyData -v
```

Expected: PASS.

- [ ] **Step 5: Run the full node test suite**

```
uv run pytest tests/test_core/nodes/ -v -q
```

Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "feat(function_node): merge EmptyData tokens in _load_cached_entries (ITL-534)"
```

---

## Task 9: Downstream guard in `_process_data_internal` and async counterpart

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (`_process_data_internal` line 1198, `_async_process_data_internal` line 1323)
- Test: `tests/test_core/nodes/test_function_node_empty_data.py` (extend)

- [ ] **Step 1: Add failing tests**

Append to `tests/test_core/nodes/test_function_node_empty_data.py`:

```python
from orcapod.errors import EphemeralResultMissingError


@pytest.fixture
def downstream_node(ephemeral_node):
    """Persistent downstream FunctionJobNode that follows ephemeral_node."""
    table = pa.table({
        "key": pa.array(["a"], type=pa.large_string()),
        "value": pa.array([1], type=pa.int64()),
    })
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)

    def triple_value(result: int) -> int:
        return result * 3

    pod = FunctionPod(PythonDataFunction(triple_value, output_keys="final"))
    return FunctionJobNode(
        pod, ephemeral_node,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=InMemoryArrowDatabase(),
    )


class TestProcessDataInternalEmptyData:
    def test_cache_hit_returns_cached_result(self, ephemeral_node, downstream_node):
        """Downstream cache hit on EmptyData input returns the cached result."""
        # Step 1: Run upstream and downstream together (both have live data)
        for tag, data in ephemeral_node._input_stream.iter_data():
            ephemeral_node.execute_data(tag, data)
        for tag, data in ephemeral_node.iter_data():
            downstream_node.execute_data(tag, data)

        # Step 2: Simulate upstream ephemeral expiry
        ephemeral_node.set_ephemeral_store(InMemoryArrowDatabase())
        ephemeral_node._cached_output_datas.clear()

        # Step 3: Downstream receives EmptyData from upstream's iter_data
        upstream_items = list(ephemeral_node.iter_data())
        assert len(upstream_items) == 1
        tag_in, data_in = upstream_items[0]
        assert isinstance(data_in, EmptyData)

        # Step 4: Downstream processes the EmptyData — should hit its result cache
        tag_out, result = downstream_node.execute_data(tag_in, data_in)
        assert result is not None
        assert result.as_dict()["final"] == 6  # double(1)=2, triple(2)=6

    def test_cache_miss_raises_ephemeral_result_missing_error(self, ephemeral_node):
        """Downstream cache miss on EmptyData input raises EphemeralResultMissingError."""
        # Run upstream but NOT downstream
        for tag, data in ephemeral_node._input_stream.iter_data():
            ephemeral_node.execute_data(tag, data)

        # Simulate expiry
        ephemeral_node.set_ephemeral_store(InMemoryArrowDatabase())
        ephemeral_node._cached_output_datas.clear()

        # Build a downstream node
        table = pa.table({
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)

        def triple_value(result: int) -> int:
            return result * 3

        pod = FunctionPod(PythonDataFunction(triple_value, output_keys="final"))
        downstream = FunctionJobNode(
            pod, ephemeral_node,
            pipeline_database=InMemoryArrowDatabase(),
            result_database=InMemoryArrowDatabase(),
        )

        upstream_items = list(ephemeral_node.iter_data())
        tag_in, data_in = upstream_items[0]
        assert isinstance(data_in, EmptyData)

        with pytest.raises(EphemeralResultMissingError) as exc_info:
            downstream.execute_data(tag_in, data_in)

        assert exc_info.value.cached_content_hash is not None
        assert exc_info.value.node_identity_path == downstream.node_identity_path
```

- [ ] **Step 2: Run to verify they fail**

```
uv run pytest tests/test_core/nodes/test_function_node_empty_data.py::TestProcessDataInternalEmptyData -v
```

Expected: FAIL — `EmptyDataAccessError` raised when downstream tries to compute with `EmptyData`.

- [ ] **Step 3: Add `EmptyData` guard at the top of `_process_data_internal`**

In `function_node.py`, find `_process_data_internal` (around line 1198). Insert an `isinstance` guard as the first action inside the method body, before the `ephemeral_result` check:

```python
    def _process_data_internal(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        # Guard: EmptyData inputs skip computation and go straight to cache lookup.
        # EmptyData.content_hash() returns the cached hash, so result_cache.lookup()
        # works transparently. On cache miss, fail loudly — the ephemeral data is
        # gone and the downstream has not yet computed a result for this input.
        if isinstance(data, EmptyData):
            if self._cached_function_pod is not None:
                cached = self._cached_function_pod.result_cache.lookup(data)
                if cached is not None:
                    cached = cached.with_meta_columns(
                        **{self._cached_function_pod.RESULT_COMPUTED_FLAG: False}
                    )
                    if self._pipeline_database is not None:
                        self.add_pipeline_record(
                            tag,
                            data,
                            data_record_id=cached.datagram_uuid,
                            computed=False,
                        )
                    base_entry_id = self.compute_base_entry_id(tag, data)
                    self._cached_output_datas[base_entry_id] = (tag, cached)
                    self._cached_output_table = None
                    self._cached_content_hash_column = None
                    return tag, cached
            raise EphemeralResultMissingError(
                tag=tag,
                cached_content_hash=data.cached_content_hash,
                node_identity_path=self.node_identity_path,
                message=(
                    "Downstream cache miss for EmptyData input — "
                    "ephemeral result is gone and downstream has not yet computed "
                    "a result for this input hash."
                ),
            )

        # ... rest of existing method unchanged ...
        ephemeral_result = self._node_config.is_result_ephemeral or False
        ...
```

- [ ] **Step 4: Add the identical guard to `_async_process_data_internal`**

Find `_async_process_data_internal` (around line 1323) and insert the same guard, but with `await` for the async path. The cache lookup itself is synchronous (ResultCache is sync), so no `await` is needed for the lookup:

```python
    async def _async_process_data_internal(
        self,
        tag: TagProtocol,
        data: DataProtocol,
        *,
        logger: DataExecutionLoggerProtocol | None = None,
    ) -> tuple[TagProtocol, DataProtocol | None]:
        # Guard: same as sync _process_data_internal.
        if isinstance(data, EmptyData):
            if self._cached_function_pod is not None:
                cached = self._cached_function_pod.result_cache.lookup(data)
                if cached is not None:
                    cached = cached.with_meta_columns(
                        **{self._cached_function_pod.RESULT_COMPUTED_FLAG: False}
                    )
                    if self._pipeline_database is not None:
                        self.add_pipeline_record(
                            tag,
                            data,
                            data_record_id=cached.datagram_uuid,
                            computed=False,
                        )
                    base_entry_id = self.compute_base_entry_id(tag, data)
                    self._cached_output_datas[base_entry_id] = (tag, cached)
                    self._cached_output_table = None
                    self._cached_content_hash_column = None
                    return tag, cached
            raise EphemeralResultMissingError(
                tag=tag,
                cached_content_hash=data.cached_content_hash,
                node_identity_path=self.node_identity_path,
                message=(
                    "Downstream cache miss for EmptyData input — "
                    "ephemeral result is gone and downstream has not yet computed "
                    "a result for this input hash."
                ),
            )

        # ... rest of existing async method unchanged ...
        ephemeral_result = self._node_config.is_result_ephemeral or False
        ...
```

- [ ] **Step 5: Run the new tests**

```
uv run pytest tests/test_core/nodes/test_function_node_empty_data.py::TestProcessDataInternalEmptyData -v
```

Expected: PASS.

- [ ] **Step 6: Run the full test suite**

```
uv run pytest tests/ -x -q
```

Expected: All PASS.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "feat(function_node): add EmptyData guard in _process_data_internal (ITL-534)"
```

---

## Task 10: Create follow-up Linear issues

Before creating the PR, create the three follow-up issues using the Linear MCP tool. Use the project ID `0f424f4d-74d3-4efd-aed6-b0364a7368fb` (Orcapod Python v0.2 Feature Sprint) and team `ITL`.

- [ ] **Step 1: Create issue — Configurable EmptyData relaxation**

```
mcp__claude_ai_Linear__save_issue(
  title: "EmptyData: configurable relaxation of strict upstream-ephemerality enforcement",
  team: "ITL",
  project: "0f424f4d-74d3-4efd-aed6-b0364a7368fb",
  description: """
## Overview
ITL-534 treats any EmptyData arriving at a downstream pod as a legitimate
ephemeral miss (option A). This follow-up adds per-pipeline opt-in to allow
graceful treatment of EmptyData from non-ephemeral upstreams (options B/C
from the original design brainstorm).

## Goals & Success Criteria
* A pipeline-level flag (or NodeConfig field) allows downstream pods to
  accept EmptyData from non-ephemeral upstreams without raising immediately.
* Default behavior (strict) is unchanged.
* Configurable relaxation is documented and tested.

## Scope & Boundaries
In scope:
* Per-pipeline or per-node config flag for EmptyData handling mode.
* Documentation of when to use each mode.
Out of scope:
* Changes to EmptyData data model (already done in ITL-534).

## Dependencies & Risks
* Depends on ITL-534 landing first.
"""
)
```

- [ ] **Step 2: Create issue — Rigorous upstream-ephemerality validation**

```
mcp__claude_ai_Linear__save_issue(
  title: "EmptyData: rigorous upstream-ephemerality validation in _process_data_internal",
  team: "ITL",
  project: "0f424f4d-74d3-4efd-aed6-b0364a7368fb",
  description: """
## Overview
ITL-534 treats any EmptyData as a legitimate ephemeral miss. This follow-up
adds a check that EmptyData only arrives from upstream pods declared with
is_result_ephemeral=True, failing loudly if EmptyData arrives from a
non-ephemeral upstream (indicating data corruption).

## Goals & Success Criteria
* _process_data_internal inspects the source of EmptyData.
* EmptyData from a non-ephemeral upstream raises a distinct exception.
* Legitimate ephemeral misses continue to work as before.

## Scope & Boundaries
In scope:
* Source-ephemerality check in _process_data_internal.
* Distinct exception for corruption vs. expected miss.
Out of scope:
* Changes to EmptyData data model.

## Dependencies & Risks
* Depends on ITL-534.
* Requires the empty_source_info field to carry ephemerality provenance.
"""
)
```

- [ ] **Step 3: Create issue — Tag-row reconstruction from downstream cache evidence**

```
mcp__claude_ai_Linear__save_issue(
  title: "EmptyData: tag-row reconstruction from downstream cache evidence",
  team: "ITL",
  project: "0f424f4d-74d3-4efd-aed6-b0364a7368fb",
  description: """
## Overview
ITL-534 defines the empty_source_info field on EmptyData but defers the
reconstruction logic. This follow-up implements tag-row reconstruction:
when a downstream cache hit is found for an EmptyData input, write a
reconstructed upstream tag row with record_id=None as a provenance marker.

## Goals & Success Criteria
* Downstream cache hit against EmptyData writes a reconstructed tag row.
* Reconstructed rows have record_id=None in the source-column set.
* Inspection tooling can distinguish reconstructed rows from normal rows.

## Scope & Boundaries
In scope:
* populate empty_source_info on EmptyData tokens.
* Write reconstructed tag rows with record_id=None.
* Resolve extension axes 3-6 from ITL-534 description before landing.
Out of scope:
* Cross-pipeline reconstruction (axis 6) — separate design required.

## Dependencies & Risks
* Depends on ITL-534.
* Extension axes 5 (orphaned reconstructed rows) and 6 (cross-pipeline
  trust) must be resolved before this lands.
"""
)
```

- [ ] **Step 4: Verify the three issues were created and note their IDs**

The tool response will include issue IDs. Record them.

---

## Task 11: Final verification and PR

- [ ] **Step 1: Run the full test suite**

```
uv run pytest tests/ -q
```

Expected: All PASS.

- [ ] **Step 2: Create the PR**

```bash
gh pr create \
  --title "feat(datagrams,function_node): add EmptyData + ephemeral result propagation (ITL-534)" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Adds `EmptyData(Data)` — a payload-free `Data` subclass carrying an optional cached content hash, used to represent expired/pruned ephemeral results in the pipeline.
- Extends the tag table (pipeline DB) to store `INPUT_DATA_HASH_COL` in every row written by `add_pipeline_record()`.
- `_fetch_joined_records()` now emits `EmptyData` tokens (via anti-join) for ephemeral miss rows instead of silently dropping them.
- `_load_cached_entries()` merges `EmptyData` tokens with normal rows, so `iter_data()` propagates them downstream.
- `_process_data_internal()` and `_async_process_data_internal()` add an `isinstance(data, EmptyData)` guard: cache-only lookup via `result_cache.lookup(empty_data)` on hit returns the cached result; on miss raises `EphemeralResultMissingError` loudly.
- Three new exception types: `EmptyDataAccessError`, `EmptyDataHashMissingError`, `EphemeralResultMissingError`.
- Old-format pipeline DB rows (lacking `INPUT_DATA_HASH_COL`) produce a WARNING and an `EmptyData` with `cached_content_hash=None`; no forced migration.

Closes ITL-534

## Test plan

- [ ] `uv run pytest tests/test_core/test_empty_data.py -v` — unit tests for `EmptyData` and exceptions
- [ ] `uv run pytest tests/test_core/test_result_cache.py -v` — lookup-via-`EmptyData` tests
- [ ] `uv run pytest tests/test_core/nodes/test_function_node_empty_data.py -v` — integration tests (write path, read path, downstream guard)
- [ ] `uv run pytest tests/ -q` — full suite, no regressions

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Confirm PR URL is returned and report it**

---

## Self-Review Checklist

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| `EmptyData(Data)` with `cached_content_hash` + `empty_source_info` | Task 2 |
| `EmptyDataAccessError`, `EmptyDataHashMissingError`, `EphemeralResultMissingError` | Task 1 |
| `INPUT_DATA_HASH_COL` in `add_pipeline_record()` | Task 6 |
| `_fetch_joined_records()` emits `EmptyData` tokens | Task 7 |
| `_process_data_internal()` + async guard | Task 9 |
| `_load_cached_entries()` merges tokens | Task 8 |
| `_JoinedRecords` extended | Task 7 |
| Warning log for old-format rows | Task 7 |
| Unit tests for `EmptyData` | Task 2 |
| `ResultCache.lookup` via `EmptyData` tests | Task 5 |
| Integration tests (happy path, miss, old-format) | Task 9 |

**Type consistency:** `EmptyData` defined in Task 2, used in Tasks 7–9. `EphemeralResultMissingError` defined in Task 1, raised in Task 9. `result_cache` property defined in Task 4, used in Task 9. All consistent.

**Placeholders:** None.
