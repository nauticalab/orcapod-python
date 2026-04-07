# ENG-374: Execution Context Schema Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make execution context metadata schema-driven and store it via Datagrams so that structured types like `dict[str, str]` are properly handled in Arrow storage.

**Architecture:** Executor returns rich metadata, PacketFunction stringifies it into a flat `dict[str, str]` and wraps it as a Datagram. `ResultCache.store()` accepts Datagrams instead of raw dicts, using `.as_table()` for Arrow conversion and adding column prefixes internally.

**Tech Stack:** Python, PyArrow, orcapod types/datagrams/semantic_types

**Spec:** `superpowers/specs/2026-04-07-execution-context-schema-design.md`

---

## Chunk 1: Abstract base class + PythonPacketFunction schema methods

### Task 1: Add abstract schema methods to PacketFunctionBase

**Files:**
- Modify: `src/orcapod/core/packet_function.py:234-242` (add abstract schema methods alongside existing abstract data methods)

- [ ] **Step 1: Add abstract `get_function_variation_data_schema` and `get_execution_data_schema` to `PacketFunctionBase`**

In `src/orcapod/core/packet_function.py`, after the existing abstract `get_execution_data` method (around line 242), add:

```python
@abstractmethod
def get_function_variation_data_schema(self) -> Schema:
    """Schema for the data returned by ``get_function_variation_data``."""
    ...

@abstractmethod
def get_execution_data_schema(self) -> Schema:
    """Schema for the data returned by ``get_execution_data``."""
    ...
```

- [ ] **Step 2: Verify tests fail as expected**

Run: `uv run pytest tests/test_core/packet_function/ -v --tb=short -x`
Expected: FAIL — `PythonPacketFunction` does not yet implement `get_execution_data_schema`, so instantiation raises `TypeError` for the missing abstract method. This is expected and will be fixed in Task 2. Do NOT commit yet — proceed directly to Task 2.

### Task 2: Update PythonPacketFunction.get_execution_data() and add get_execution_data_schema()

**Files:**
- Modify: `src/orcapod/core/packet_function.py:473-477` (replace current `get_execution_data`)

- [ ] **Step 1: Write test for new execution data shape**

In `tests/test_core/packet_function/test_packet_function.py`, add a test:

```python
class TestExecutionDataSchema:
    def test_execution_data_has_expected_keys(self):
        pf = PythonPacketFunction(lambda x: x, output_keys="result")
        data = pf.get_execution_data()
        assert set(data.keys()) == {
            "executor_type",
            "executor_info",
            "python_version",
            "extra_info",
        }

    def test_executor_type_is_string(self):
        pf = PythonPacketFunction(lambda x: x, output_keys="result")
        data = pf.get_execution_data()
        assert isinstance(data["executor_type"], str)

    def test_executor_info_is_dict_str_str(self):
        pf = PythonPacketFunction(lambda x: x, output_keys="result")
        data = pf.get_execution_data()
        assert isinstance(data["executor_info"], dict)
        for k, v in data["executor_info"].items():
            assert isinstance(k, str)
            assert isinstance(v, str)

    def test_python_version_matches_sys(self):
        import sys
        pf = PythonPacketFunction(lambda x: x, output_keys="result")
        data = pf.get_execution_data()
        expected = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        assert data["python_version"] == expected

    def test_extra_info_is_empty_dict(self):
        pf = PythonPacketFunction(lambda x: x, output_keys="result")
        data = pf.get_execution_data()
        assert data["extra_info"] == {}

    def test_execution_data_schema_matches_data_keys(self):
        pf = PythonPacketFunction(lambda x: x, output_keys="result")
        data = pf.get_execution_data()
        schema = pf.get_execution_data_schema()
        assert set(schema.keys()) == set(data.keys())

    def test_execution_data_schema_types(self):
        pf = PythonPacketFunction(lambda x: x, output_keys="result")
        schema = pf.get_execution_data_schema()
        assert schema["executor_type"] is str
        assert schema["executor_info"] == dict[str, str]
        assert schema["python_version"] is str
        assert schema["extra_info"] == dict[str, str]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function.py::TestExecutionDataSchema -v --tb=short`
Expected: FAIL (current `get_execution_data` returns different keys)

- [ ] **Step 3: Update `PythonPacketFunction.get_execution_data()` and add `get_execution_data_schema()`**

Replace the current `get_execution_data` method in `src/orcapod/core/packet_function.py` (around line 473) with:

```python
def get_execution_data(self) -> dict[str, Any]:
    """Raw data defining execution context."""
    import json

    executor = self._executor
    if executor is not None:
        executor_data = executor.get_executor_data()
        executor_type = str(executor_data.pop("executor_type", executor.executor_type_id))
        executor_info = {
            k: v if isinstance(v, str) else json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in executor_data.items()
        }
    else:
        executor_type = "none"
        executor_info = {}

    python_version_info = sys.version_info
    python_version_str = f"{python_version_info.major}.{python_version_info.minor}.{python_version_info.micro}"
    return {
        "executor_type": executor_type,
        "executor_info": executor_info,
        "python_version": python_version_str,
        "extra_info": {},
    }

def get_execution_data_schema(self) -> Schema:
    """Schema for the data returned by ``get_execution_data``."""
    return Schema({
        "executor_type": str,
        "executor_info": dict[str, str],
        "python_version": str,
        "extra_info": dict[str, str],
    })
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function.py::TestExecutionDataSchema -v --tb=short`
Expected: PASS

- [ ] **Step 5: Run full packet function test suite to check for regressions**

Run: `uv run pytest tests/test_core/packet_function/ -v --tb=short -x`
Expected: All PASS. Some tests that assert on `get_execution_data()` keys may fail — fix any that reference the old `execution_context` key.

- [ ] **Step 6: Commit Tasks 1 and 2 together**

Tasks 1 and 2 are committed together since Task 1 leaves the codebase in a broken state (abstract method without implementation).

```bash
git add src/orcapod/core/packet_function.py tests/test_core/packet_function/test_packet_function.py
git commit -m "feat(ENG-374): add abstract schema methods and update PythonPacketFunction execution data"
```

### Task 3: Add schema method delegation to PacketFunctionWrapper

**Files:**
- Modify: `src/orcapod/core/packet_function.py:755-758` (add delegation methods in `PacketFunctionWrapper`)
- Test: `tests/test_core/packet_function/test_cached_packet_function.py`

- [ ] **Step 1: Write tests for schema delegation**

In `tests/test_core/packet_function/test_cached_packet_function.py`, in the `TestPacketFunctionWrapperDelegation` class, add:

```python
def test_get_function_variation_data_schema_delegates(self, wrapper, inner_pf):
    assert (
        wrapper.get_function_variation_data_schema()
        == inner_pf.get_function_variation_data_schema()
    )

def test_get_execution_data_schema_delegates(self, wrapper, inner_pf):
    assert (
        wrapper.get_execution_data_schema()
        == inner_pf.get_execution_data_schema()
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/packet_function/test_cached_packet_function.py::TestPacketFunctionWrapperDelegation::test_get_function_variation_data_schema_delegates -v --tb=short`
Expected: FAIL (method not found on wrapper)

- [ ] **Step 3: Add delegation methods to `PacketFunctionWrapper`**

In `src/orcapod/core/packet_function.py`, in the `PacketFunctionWrapper` class, after the existing `get_execution_data` delegation (around line 759), add:

```python
def get_function_variation_data_schema(self) -> Schema:
    return self._packet_function.get_function_variation_data_schema()

def get_execution_data_schema(self) -> Schema:
    return self._packet_function.get_execution_data_schema()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/packet_function/test_cached_packet_function.py::TestPacketFunctionWrapperDelegation -v --tb=short`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/packet_function.py tests/test_core/packet_function/test_cached_packet_function.py
git commit -m "feat(ENG-374): add schema method delegation to PacketFunctionWrapper"
```

## Chunk 2: ResultCache datagram-based store + CachedPacketFunction helper

### Task 4: Update ResultCache.store() to accept Datagrams

**Files:**
- Modify: `src/orcapod/core/result_cache.py:138-204`
- Test: `tests/test_core/test_result_cache.py`

- [ ] **Step 1: Write tests for datagram-based store**

Update `tests/test_core/test_result_cache.py`. Change the `_compute_and_store` helper to construct Datagrams:

```python
from orcapod.core.datagrams import Datagram, Packet

def _compute_and_store(
    cache: ResultCache, pf: PythonPacketFunction, input_packet: Packet
):
    """Helper: compute output and store in cache."""
    output = pf.direct_call(input_packet)
    assert output is not None
    variation_datagram = Datagram(
        pf.get_function_variation_data(),
        python_schema=pf.get_function_variation_data_schema(),
        data_context=pf.data_context,
    )
    execution_datagram = Datagram(
        pf.get_execution_data(),
        python_schema=pf.get_execution_data_schema(),
        data_context=pf.data_context,
    )
    cache.store(
        input_packet,
        output,
        variation_datagram=variation_datagram,
        execution_datagram=execution_datagram,
    )
    return output
```

Also update `test_same_packet_different_record_path_is_miss` — replace its direct `cache_a.store(...)` call:

```python
def test_same_packet_different_record_path_is_miss(self):
    db = InMemoryArrowDatabase()
    cache_a = ResultCache(result_database=db, record_path=("path_a",))
    cache_b = ResultCache(result_database=db, record_path=("path_b",))
    pf = _make_pf()
    input_pkt = Packet({"x": 10})

    output = pf.direct_call(input_pkt)
    variation_datagram = Datagram(
        pf.get_function_variation_data(),
        python_schema=pf.get_function_variation_data_schema(),
        data_context=pf.data_context,
    )
    execution_datagram = Datagram(
        pf.get_execution_data(),
        python_schema=pf.get_execution_data_schema(),
        data_context=pf.data_context,
    )
    cache_a.store(input_pkt, output, variation_datagram, execution_datagram)

    assert cache_a.lookup(input_pkt) is not None
    assert cache_b.lookup(input_pkt) is None
```

Update the import at the top of `test_result_cache.py` — change `from orcapod.core.datagrams import Packet` to `from orcapod.core.datagrams import Datagram, Packet`.

Add a new test for dict-typed columns:

```python
class TestStoreDictColumns:
    def test_executor_info_column_stored(self):
        cache, db = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))

        records = db.get_all_records(cache.record_path)
        assert records is not None
        exec_info_col = f"{constants.PF_EXECUTION_PREFIX}executor_info"
        assert exec_info_col in records.column_names

    def test_extra_info_column_stored(self):
        cache, db = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))

        records = db.get_all_records(cache.record_path)
        assert records is not None
        extra_info_col = f"{constants.PF_EXECUTION_PREFIX}extra_info"
        assert extra_info_col in records.column_names
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_result_cache.py -v --tb=short -x`
Expected: FAIL (store() doesn't accept datagram kwargs yet)

- [ ] **Step 3: Update `ResultCache.store()` to accept and process Datagrams**

Replace the `store` method in `src/orcapod/core/result_cache.py`:

```python
def store(
    self,
    input_packet: PacketProtocol,
    output_packet: PacketProtocol,
    variation_datagram: "DatagramProtocol",
    execution_datagram: "DatagramProtocol",
    skip_duplicates: bool = False,
) -> None:
    """Store an output packet in the cache.

    Stores the output packet data alongside function variation and
    execution metadata (as Datagrams), input packet hash, and a timestamp.

    Args:
        input_packet: The input packet (used for its content hash).
        output_packet: The computed output packet to store.
        variation_datagram: Function variation metadata as a Datagram.
        execution_datagram: Execution environment metadata as a Datagram.
        skip_duplicates: If True, silently skip if a record with the
            same ID already exists.
    """
    data_table = output_packet.as_table(columns={"source": True, "context": True})

    # Add variation and execution columns with prefixes.
    # Use a running counter for insertion position since add_column shifts indices.
    col_idx = 0
    var_table = variation_datagram.as_table()
    for name in var_table.column_names:
        data_table = data_table.add_column(
            col_idx,
            f"{constants.PF_VARIATION_PREFIX}{name}",
            var_table.column(name),
        )
        col_idx += 1

    exec_table = execution_datagram.as_table()
    for name in exec_table.column_names:
        data_table = data_table.add_column(
            col_idx,
            f"{constants.PF_EXECUTION_PREFIX}{name}",
            exec_table.column(name),
        )
        col_idx += 1

    # Add input packet hash (position 0)
    data_table = data_table.add_column(
        0,
        constants.INPUT_PACKET_HASH_COL,
        pa.array([input_packet.content_hash().to_string()], type=pa.large_string()),
    )

    # Append timestamp
    timestamp = datetime.now(timezone.utc)
    data_table = data_table.append_column(
        constants.POD_TIMESTAMP,
        pa.array([timestamp], type=pa.timestamp("us", tz="UTC")),
    )

    self._result_database.add_record(
        self._record_path,
        output_packet.datagram_id,
        data_table,
        skip_duplicates=skip_duplicates,
    )

    if self._auto_flush:
        self._result_database.flush()
```

Also update the imports at the top of `result_cache.py`. Add `DatagramProtocol` inside the existing `TYPE_CHECKING` block (since it's only used in type annotations):

```python
if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.protocols.core_protocols.datagrams import DatagramProtocol
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/test_result_cache.py -v --tb=short`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/result_cache.py tests/test_core/test_result_cache.py
git commit -m "feat(ENG-374): update ResultCache.store() to accept Datagrams"
```

### Task 5: Add _build_metadata_datagrams to CachedPacketFunction and update callers

**Files:**
- Modify: `src/orcapod/core/packet_function.py:852-960` (`CachedPacketFunction`)
- Test: `tests/test_core/packet_function/test_cached_packet_function.py`

- [ ] **Step 1: Add `_build_metadata_datagrams` helper and update `call`, `async_call`, `record_packet`**

In `src/orcapod/core/packet_function.py`:

First, add the `Datagram` import near the top of the file (with the other core imports):

```python
from orcapod.core.datagrams import Datagram
```

Then in the `CachedPacketFunction` class, add a helper method and update the three callers.

Add after `__init__`:

```python
def _build_metadata_datagrams(self) -> tuple[Datagram, Datagram]:
    """Build variation and execution Datagrams for cache storage."""
    variation_datagram = Datagram(
        self.get_function_variation_data(),
        python_schema=self.get_function_variation_data_schema(),
        data_context=self.data_context,
    )
    execution_datagram = Datagram(
        self.get_execution_data(),
        python_schema=self.get_execution_data_schema(),
        data_context=self.data_context,
    )
    return variation_datagram, execution_datagram
```

Then update `call()` — replace:
```python
self._cache.store(
    packet,
    output_packet,
    variation_data=self.get_function_variation_data(),
    execution_data=self.get_execution_data(),
)
```
with:
```python
var_dg, exec_dg = self._build_metadata_datagrams()
self._cache.store(packet, output_packet, var_dg, exec_dg)
```

Apply the same replacement pattern to `async_call()`.

For `record_packet()`, replace:
```python
self._cache.store(
    input_packet,
    output_packet,
    variation_data=self.get_function_variation_data(),
    execution_data=self.get_execution_data(),
    skip_duplicates=skip_duplicates,
)
```
with:
```python
var_dg, exec_dg = self._build_metadata_datagrams()
self._cache.store(
    input_packet, output_packet, var_dg, exec_dg,
    skip_duplicates=skip_duplicates,
)
```

- [ ] **Step 2: Run CachedPacketFunction tests**

Run: `uv run pytest tests/test_core/packet_function/test_cached_packet_function.py -v --tb=short -x`
Expected: All PASS

- [ ] **Step 3: Run full test suite to catch any remaining callers of the old store() signature**

Run: `uv run pytest tests/ -v --tb=short -x`
Expected: All PASS. If any test calls `ResultCache.store()` with the old `variation_data`/`execution_data` kwargs, it will fail — fix those callers.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/packet_function.py
git commit -m "feat(ENG-374): add _build_metadata_datagrams and wire into CachedPacketFunction"
```

### Task 6: Update CachedFunctionPod to use Datagrams for store()

**Files:**
- Modify: `src/orcapod/core/cached_function_pod.py:96-103,130-136`

`CachedFunctionPod` also calls `_cache.store()` with the old raw-dict signature. It needs the same datagram conversion.

- [ ] **Step 1: Update `process_packet` and `async_process_packet` to build Datagrams**

In `src/orcapod/core/cached_function_pod.py`, add the import:

```python
from orcapod.core.datagrams import Datagram
```

In `process_packet()`, replace lines 96-103:
```python
        tag, output = self._function_pod.process_packet(tag, packet, logger=logger)
        if output is not None:
            pf = self._function_pod.packet_function
            self._cache.store(
                packet,
                output,
                variation_data=pf.get_function_variation_data(),
                execution_data=pf.get_execution_data(),
            )
```
with:
```python
        tag, output = self._function_pod.process_packet(tag, packet, logger=logger)
        if output is not None:
            pf = self._function_pod.packet_function
            var_dg = Datagram(
                pf.get_function_variation_data(),
                python_schema=pf.get_function_variation_data_schema(),
                data_context=pf.data_context,
            )
            exec_dg = Datagram(
                pf.get_execution_data(),
                python_schema=pf.get_execution_data_schema(),
                data_context=pf.data_context,
            )
            self._cache.store(packet, output, var_dg, exec_dg)
```

Apply the same replacement to `async_process_packet()` (lines 130-136).

- [ ] **Step 2: Run CachedFunctionPod tests**

Run: `uv run pytest tests/test_core/function_pod/ -v --tb=short -x`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/core/cached_function_pod.py
git commit -m "feat(ENG-374): update CachedFunctionPod to use Datagrams for store()"
```

### Task 7: Fix RayExecutor.get_executor_data_schema() immutable Schema bug

**Files:**
- Modify: `src/orcapod/core/executors/ray.py:8-9,347-352`

- [ ] **Step 1: Fix the import and schema construction**

In `src/orcapod/core/executors/ray.py`:

Fix the import on line 8 — change:
```python
from pyarrow import Schema
```
to:
```python
from orcapod.types import Schema
```

Replace `get_executor_data_schema` (lines 347-352):
```python
def get_executor_data_schema(self) -> Schema:
    return Schema({
        "executor_type": str,
        "ray_address": str,
        "remote_opts": dict[str, str],
        "runtime_env": bool,
    })
```

Note: `remote_opts` is typed as `dict[str, str]` even though `get_executor_data()` returns `dict[str, Any]` for that field. This mismatch is acceptable because the executor schema is not consumed by storage directly — the PacketFunction layer stringifies all values before storing.

- [ ] **Step 2: Verify the import and Schema construction**

Run: `uv run python -c "from orcapod.types import Schema; from orcapod.core.executors.ray import RayExecutor; print('import ok')"`
Expected: Prints `import ok` (confirming the import is correct and doesn't error).

Then verify the schema constructs correctly:
Run: `uv run python -c "from orcapod.types import Schema; s = Schema({'executor_type': str, 'ray_address': str, 'remote_opts': dict[str, str], 'runtime_env': bool}); print(s)"`
Expected: Prints the Schema repr without error.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/core/executors/ray.py
git commit -m "fix(ENG-374): fix RayExecutor schema import and immutable Schema mutation"
```

### Task 8: Final verification

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest tests/ -v --tb=short`
Expected: All PASS

- [ ] **Step 2: Verify execution data round-trips through cache**

Run a quick integration check:
```bash
uv run python -c "
from orcapod.core.datagrams import Packet
from orcapod.core.packet_function import PythonPacketFunction, CachedPacketFunction
from orcapod.databases import InMemoryArrowDatabase

def double(x: int) -> int:
    return x * 2

pf = PythonPacketFunction(double, output_keys='result')
db = InMemoryArrowDatabase()
cpf = CachedPacketFunction(pf, result_database=db)

inp = Packet({'x': 10})
out = cpf.call(inp)
print('Output:', out.as_dict())

records = cpf.get_all_cached_outputs(include_system_columns=False)
print('Stored columns:', records.column_names)
print('Num rows:', records.num_rows)
"
```
Expected: Output shows `{'result': 20}` and stored columns include prefixed variation and execution columns (including `executor_info` and `extra_info` as map-type columns).
