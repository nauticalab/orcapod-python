# UUID Arrow Type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all `pa.large_string()` UUID storage in OrcaPod with `pa.binary(16)` (`fixed_size_binary[16]`), introducing two named constants and a semantic struct converter.

**Architecture:** A single `UUID_ARROW_TYPE = pa.binary(16)` constant in `types.py` is used for all system UUID columns; a `UUID_STRUCT_ARROW_TYPE = pa.struct([pa.field("uuid", pa.binary(16))])` constant enables `uuid.UUID` Python objects to round-trip through the semantic type system. All UUID generation sites switch from `str(uuid7())` to `uuid7().bytes`.

**Tech Stack:** PyArrow 23, `uuid_utils` (uuid7), `uuid` stdlib, pytest

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/orcapod/types.py` | Modify | Add `UUID_ARROW_TYPE`, `UUID_STRUCT_ARROW_TYPE` constants |
| `src/orcapod/__init__.py` | Modify | Export both new constants |
| `src/orcapod/semantic_types/semantic_struct_converters.py` | Modify | Add `UUIDStructConverter` class |
| `src/orcapod/hashing/versioned_hashers.py` | Modify | Register `UUIDStructConverter` in default registry |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Modify | `UUIDHandler.handle` returns `obj.bytes` not `str(obj)` |
| `src/orcapod/utils/arrow_data_utils.py` | Modify | System UUID column field types → `UUID_ARROW_TYPE` |
| `src/orcapod/core/datagrams/datagram.py` | Modify | `str(uuid7())` → `uuid7().bytes`; `_datagram_id` type `bytes` |
| `src/orcapod/core/data_function.py` | Modify | `str(uuid7())` → `uuid7().bytes`; `record_id` type `bytes` |
| `src/orcapod/pipeline/logging_observer.py` | Modify | `str(uuid7())` → `uuid7().bytes` |
| `src/orcapod/pipeline/status_observer.py` | Modify | `str(uuid7())` → `uuid7().bytes` |
| `src/orcapod/databases/postgresql_connector.py` | Modify | `uuid` pg type → `UUID_ARROW_TYPE`; decode driver values to bytes |
| `tests/test_types.py` | Modify | Add constant assertions |
| `tests/test_semantic_types/test_uuid_struct_converter.py` | Create | Full converter test suite |
| `tests/test_semantic_types/test_semantic_registry.py` | Modify | Assert UUID type registered |
| `tests/` (various) | Modify | Fix tests asserting `pa.large_string()` for UUID columns |

---

### Task 1: UUID Arrow type constants

**Files:**
- Modify: `src/orcapod/types.py`
- Modify: `src/orcapod/__init__.py`
- Modify: `tests/test_types.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_types.py`:

```python
import pyarrow as pa

from orcapod.types import UUID_ARROW_TYPE, UUID_STRUCT_ARROW_TYPE


def test_uuid_arrow_type_is_binary16():
    assert UUID_ARROW_TYPE == pa.binary(16)


def test_uuid_struct_arrow_type_structure():
    assert UUID_STRUCT_ARROW_TYPE == pa.struct([pa.field("uuid", pa.binary(16))])


def test_uuid_struct_inner_type_matches_constant():
    assert UUID_STRUCT_ARROW_TYPE.field("uuid").type == UUID_ARROW_TYPE
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_types.py::test_uuid_arrow_type_is_binary16 tests/test_types.py::test_uuid_struct_arrow_type_structure -v
```

Expected: `ImportError: cannot import name 'UUID_ARROW_TYPE'`

- [ ] **Step 3: Add constants to `types.py`**

Find the import block at the top of `src/orcapod/types.py`. After the `if TYPE_CHECKING:` block (around line 22), add:

```python
if TYPE_CHECKING:
    import pyarrow as pa
    # ... existing ...
else:
    from orcapod.utils.lazy_module import LazyModule
    pa = LazyModule("pyarrow")
```

Then at module level, after the imports, before the first class definition, add:

```python
# ---------------------------------------------------------------------------
# UUID Arrow type constants
# ---------------------------------------------------------------------------

def _make_uuid_types() -> "tuple[pa.DataType, pa.StructType]":
    """Build UUID Arrow types (deferred so pyarrow is not imported at module level)."""
    import pyarrow as _pa
    arrow_type = _pa.binary(16)
    struct_type = _pa.struct([_pa.field("uuid", arrow_type)])
    return arrow_type, struct_type


# Canonical Arrow type for all UUID values in OrcaPod.
# Stored as fixed_size_binary[16] — 16 raw bytes, no hex encoding, no dashes.
UUID_ARROW_TYPE: "pa.DataType"
# Semantic struct type for Python uuid.UUID round-trips through the type system.
UUID_STRUCT_ARROW_TYPE: "pa.StructType"
UUID_ARROW_TYPE, UUID_STRUCT_ARROW_TYPE = _make_uuid_types()
del _make_uuid_types
```

> **Note on lazy import:** The rest of `types.py` uses `LazyModule("pyarrow")` to defer the heavy import. For module-level constants that need the actual `pa` object at definition time, the pattern is to call a helper function that imports `pyarrow` directly. The assignment happens at module import time but is deferred inside the helper.

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_types.py::test_uuid_arrow_type_is_binary16 tests/test_types.py::test_uuid_struct_arrow_type_structure tests/test_types.py::test_uuid_struct_inner_type_matches_constant -v
```

Expected: all PASS

- [ ] **Step 5: Export from `src/orcapod/__init__.py`**

Open `src/orcapod/__init__.py` and find the existing imports from `types`. Add `UUID_ARROW_TYPE` and `UUID_STRUCT_ARROW_TYPE` to the same import line:

```python
from orcapod.types import (
    # ... existing exports ...
    UUID_ARROW_TYPE,
    UUID_STRUCT_ARROW_TYPE,
)
```

- [ ] **Step 6: Verify import from top-level package**

```bash
uv run python -c "from orcapod import UUID_ARROW_TYPE, UUID_STRUCT_ARROW_TYPE; import pyarrow as pa; assert UUID_ARROW_TYPE == pa.binary(16); print('OK')"
```

Expected: `OK`

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/types.py src/orcapod/__init__.py tests/test_types.py
git commit -m "feat(types): add UUID_ARROW_TYPE and UUID_STRUCT_ARROW_TYPE constants (PLT-1162)"
```

---

### Task 2: UUIDStructConverter

**Files:**
- Modify: `src/orcapod/semantic_types/semantic_struct_converters.py`
- Create: `tests/test_semantic_types/test_uuid_struct_converter.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_semantic_types/test_uuid_struct_converter.py`:

```python
"""Tests for UUIDStructConverter."""
import uuid

import pyarrow as pa
import pytest

from orcapod.semantic_types.semantic_struct_converters import UUIDStructConverter
from orcapod.types import UUID_ARROW_TYPE, UUID_STRUCT_ARROW_TYPE


@pytest.fixture
def converter():
    return UUIDStructConverter()


@pytest.fixture
def sample_uuid():
    return uuid.UUID("550e8400-e29b-41d4-a716-446655440000")


def test_python_type(converter):
    assert converter.python_type is uuid.UUID


def test_arrow_struct_type(converter):
    assert converter.arrow_struct_type == UUID_STRUCT_ARROW_TYPE


def test_semantic_type_name(converter):
    assert converter.semantic_type_name == "uuid"


def test_python_to_struct_dict(converter, sample_uuid):
    result = converter.python_to_struct_dict(sample_uuid)
    assert result == {"uuid": sample_uuid.bytes}
    assert isinstance(result["uuid"], bytes)
    assert len(result["uuid"]) == 16


def test_python_to_struct_dict_rejects_non_uuid(converter):
    with pytest.raises(TypeError):
        converter.python_to_struct_dict("550e8400-e29b-41d4-a716-446655440000")  # type: ignore


def test_struct_dict_to_python(converter, sample_uuid):
    struct_dict = {"uuid": sample_uuid.bytes}
    result = converter.struct_dict_to_python(struct_dict)
    assert result == sample_uuid
    assert isinstance(result, uuid.UUID)


def test_struct_dict_to_python_from_bytearray(converter, sample_uuid):
    """Arrow may return binary fields as bytearray — must handle both."""
    struct_dict = {"uuid": bytearray(sample_uuid.bytes)}
    result = converter.struct_dict_to_python(struct_dict)
    assert result == sample_uuid


def test_struct_dict_to_python_missing_field(converter):
    with pytest.raises(ValueError, match="Missing 'uuid' field"):
        converter.struct_dict_to_python({})


def test_round_trip(converter, sample_uuid):
    struct_dict = converter.python_to_struct_dict(sample_uuid)
    recovered = converter.struct_dict_to_python(struct_dict)
    assert recovered == sample_uuid


def test_round_trip_all_versions():
    """Verify round-trip works for uuid4, uuid5, and uuid7 (uuid_utils)."""
    from uuid_utils import uuid7

    converter = UUIDStructConverter()
    for u in [uuid.uuid4(), uuid.uuid5(uuid.NAMESPACE_OID, "test"), uuid7()]:
        assert converter.struct_dict_to_python(converter.python_to_struct_dict(u)) == u


def test_arrow_array_round_trip(converter, sample_uuid):
    """Verify UUID survives a PyArrow array round-trip."""
    struct_dict = converter.python_to_struct_dict(sample_uuid)
    arr = pa.array([struct_dict], type=UUID_STRUCT_ARROW_TYPE)
    recovered_dict = arr[0].as_py()
    recovered_uuid = converter.struct_dict_to_python(recovered_dict)
    assert recovered_uuid == sample_uuid


def test_distinct_uuids_produce_distinct_struct_dicts(converter):
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    assert converter.python_to_struct_dict(u1) != converter.python_to_struct_dict(u2)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_uuid_struct_converter.py -v
```

Expected: `ImportError: cannot import name 'UUIDStructConverter'`

- [ ] **Step 3: Add `UUIDStructConverter` to `semantic_struct_converters.py`**

Add the following imports at the top of `src/orcapod/semantic_types/semantic_struct_converters.py` (after existing imports):

```python
import uuid as _uuid_module
from typing import Any

from orcapod.types import UUID_STRUCT_ARROW_TYPE
```

Then add the class after `UPathStructConverter`:

```python
class UUIDStructConverter(SemanticStructConverterBase):
    """Converts Python ``uuid.UUID`` objects to/from the OrcaPod UUID struct.

    Stores UUIDs as their raw 16-byte binary representation inside a
    single-field struct ``struct<uuid: fixed_size_binary[16]>``. This follows
    the same single-field struct pattern used by ``PythonPathStructConverter``
    and ``UPathStructConverter``.
    """

    def __init__(self) -> None:
        super().__init__("uuid")
        self._arrow_struct_type = UUID_STRUCT_ARROW_TYPE

    @property
    def python_type(self) -> type:
        """Python type handled by this converter."""
        return _uuid_module.UUID

    @property
    def arrow_struct_type(self) -> "pa.StructType":
        """Arrow struct type for UUID values."""
        return self._arrow_struct_type

    def python_to_struct_dict(self, value: Any) -> dict[str, bytes]:
        """Convert a ``uuid.UUID`` to a struct dictionary.

        Args:
            value: A ``uuid.UUID`` instance.

        Returns:
            ``{"uuid": <16 bytes>}``

        Raises:
            TypeError: If ``value`` is not a ``uuid.UUID``.
        """
        if not isinstance(value, _uuid_module.UUID):
            raise TypeError(
                f"Expected uuid.UUID, got {type(value).__name__}"
            )
        return {"uuid": value.bytes}

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> _uuid_module.UUID:
        """Convert a struct dictionary back to a ``uuid.UUID``.

        Args:
            struct_dict: Dict with a ``"uuid"`` key holding 16 bytes.

        Returns:
            The reconstructed ``uuid.UUID``.

        Raises:
            ValueError: If the ``"uuid"`` field is missing.
        """
        raw = struct_dict.get("uuid")
        if raw is None:
            raise ValueError("Missing 'uuid' field in struct dict")
        return _uuid_module.UUID(bytes=bytes(raw))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_uuid_struct_converter.py -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/semantic_types/semantic_struct_converters.py \
        tests/test_semantic_types/test_uuid_struct_converter.py
git commit -m "feat(semantic-types): add UUIDStructConverter for uuid.UUID Arrow struct round-trips (PLT-1162)"
```

---

### Task 3: Register UUIDStructConverter in the default semantic registry

**Files:**
- Modify: `src/orcapod/hashing/versioned_hashers.py`
- Modify: `tests/test_semantic_types/test_semantic_registry.py`

- [ ] **Step 1: Find the fixture name used in existing registry tests**

```bash
grep -n "def.*registry\|fixture.*registry" \
    tests/test_semantic_types/test_semantic_registry.py tests/conftest.py 2>/dev/null | head -10
```

Note the fixture name (e.g. `default_registry`, `registry`, `semantic_registry`). Use that exact name in the new tests below.

- [ ] **Step 1b: Write the failing tests**

Open `tests/test_semantic_types/test_semantic_registry.py` and add (replacing `default_registry` with the fixture name found above):

```python
import uuid

import pyarrow as pa

from orcapod.types import UUID_STRUCT_ARROW_TYPE


def test_uuid_type_registered_in_default_registry(default_registry):
    """uuid.UUID should be registered and map to UUID_STRUCT_ARROW_TYPE."""
    struct_type = default_registry.get_struct_for_python_type(uuid.UUID)
    assert struct_type == UUID_STRUCT_ARROW_TYPE


def test_uuid_struct_resolves_to_python_type(default_registry):
    """UUID_STRUCT_ARROW_TYPE should resolve back to uuid.UUID."""
    python_type = default_registry.get_python_type_for_struct(UUID_STRUCT_ARROW_TYPE)
    assert python_type is uuid.UUID


def test_uuid_semantic_type_name_registered(default_registry):
    """Converter registered under the name 'uuid'."""
    converter = default_registry.get_converter_for_semantic_type("uuid")
    assert converter is not None
    assert converter.python_type is uuid.UUID
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_semantic_registry.py::test_uuid_type_registered_in_default_registry -v
```

Expected: FAIL — `uuid.UUID` not registered

- [ ] **Step 3: Register the converter in `versioned_hashers.py`**

Open `src/orcapod/hashing/versioned_hashers.py`. Find the block that registers `PythonPathStructConverter` (around line 125-138):

```python
    from orcapod.semantic_types.semantic_struct_converters import PythonPathStructConverter

    registry: Any = SemanticTypeRegistry()
    file_hasher = BasicFileHasher(algorithm="sha256")
    path_converter: Any = PythonPathStructConverter(file_hasher=file_hasher)
    registry.register_converter("path", path_converter)
```

Extend it to also register `UUIDStructConverter`:

```python
    from orcapod.semantic_types.semantic_struct_converters import (
        PythonPathStructConverter,
        UUIDStructConverter,
    )

    registry: Any = SemanticTypeRegistry()
    file_hasher = BasicFileHasher(algorithm="sha256")
    path_converter: Any = PythonPathStructConverter(file_hasher=file_hasher)
    registry.register_converter("path", path_converter)
    uuid_converter: Any = UUIDStructConverter()
    registry.register_converter("uuid", uuid_converter)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_semantic_registry.py::test_uuid_type_registered_in_default_registry tests/test_semantic_types/test_semantic_registry.py::test_uuid_struct_resolves_to_python_type tests/test_semantic_types/test_semantic_registry.py::test_uuid_semantic_type_name_registered -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/hashing/versioned_hashers.py \
        tests/test_semantic_types/test_semantic_registry.py
git commit -m "feat(semantic-types): register UUIDStructConverter in default semantic registry (PLT-1162)"
```

---

### Task 4: Update UUIDHandler in semantic hashing

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`
- Modify: (existing test file for builtin handlers — find with `grep -rl "UUIDHandler" tests/`)

- [ ] **Step 1: Find the existing UUIDHandler test**

```bash
grep -rl "UUIDHandler\|uuid.*handler\|handler.*uuid" tests/ --include="*.py" -i
```

If a file is found, open it and locate the test asserting that handling a `uuid.UUID` returns its string form — update it per Step 2 below.

If **no file is found**, create `tests/test_hashing/test_uuid_handler.py`:

```python
"""Tests for UUIDHandler in semantic hashing."""
```

Then proceed to Step 2.

- [ ] **Step 2: Update the test to expect bytes**

In the test file found above, update the UUID handler test:

```python
import uuid as _uuid

def test_uuid_handler_returns_bytes():
    """UUIDHandler should return the 16-byte binary representation."""
    from orcapod.hashing.semantic_hashing.builtin_handlers import UUIDHandler

    handler = UUIDHandler()
    u = _uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
    result = handler.handle(u, hasher=None)  # type: ignore[arg-type]
    assert result == u.bytes
    assert isinstance(result, bytes)
    assert len(result) == 16


def test_uuid_handler_different_uuids_produce_different_bytes():
    from orcapod.hashing.semantic_hashing.builtin_handlers import UUIDHandler

    handler = UUIDHandler()
    u1 = _uuid.uuid4()
    u2 = _uuid.uuid4()
    assert handler.handle(u1, None) != handler.handle(u2, None)  # type: ignore[arg-type]
```

- [ ] **Step 3: Run to verify test fails**

```bash
uv run pytest tests/ -k "uuid_handler" -v
```

Expected: FAIL — result is a string, not bytes

- [ ] **Step 4: Update `UUIDHandler` in `builtin_handlers.py`**

Open `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`. Find `UUIDHandler` (around line 141):

```python
class UUIDHandler:
    """
    Handler for uuid.UUID objects.

    Converts the UUID to its canonical hyphenated string representation
    (e.g. ``"550e8400-e29b-41d4-a716-446655440000"``), which is stable,
    human-readable, and unambiguous.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        return str(obj)
```

Replace with:

```python
class UUIDHandler:
    """Handler for ``uuid.UUID`` objects.

    Returns the raw 16-byte binary representation of the UUID, consistent
    with OrcaPod's canonical ``pa.binary(16)`` Arrow storage format.
    The binary form is compact, unambiguous, and independent of string
    formatting conventions.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        return obj.bytes
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/ -k "uuid_handler" -v
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/builtin_handlers.py
git commit -m "fix(hashing): UUIDHandler returns bytes instead of str, consistent with binary Arrow storage (PLT-1162)"
```

---

### Task 5: Update system UUID column Arrow types in arrow_data_utils.py

**Files:**
- Modify: `src/orcapod/utils/arrow_data_utils.py`

- [ ] **Step 1: Locate all UUID column field definitions**

The UUID columns that need updating are **only** the following — all others (e.g. `source_id`, `pipeline_hash`, string metadata) are NOT UUIDs and must be left as `pa.large_string()`:

| Column name | File | Contains UUID? |
|---|---|---|
| `datagram_id` | `datagram.py`, `arrow_data_utils.py` | ✅ uuid7 |
| `record_id` / `record_id_col_name` | `arrow_data_utils.py`, `data_function.py` | ✅ uuid7 |
| `_log_id` | `logging_observer.py` | ✅ uuid7 |
| `_status_id` | `status_observer.py` | ✅ uuid7 |
| `source_id` | `arrow_data_utils.py`, `stream_builder.py` | ❌ hash string |
| `_source_*` columns | `arrow_data_utils.py` | ❌ provenance string |
| `pipeline_hash` | various | ❌ hash string |

Run this to confirm which lines in `arrow_data_utils.py` define UUID-carrying fields:

```bash
grep -n "record_id\|datagram_id\|_log_id\|_status_id" \
    src/orcapod/utils/arrow_data_utils.py | grep -i "field\|large_string\|array"
```

- [ ] **Step 2: Update imports in `arrow_data_utils.py`**

Add the import for `UUID_ARROW_TYPE` at the top of `src/orcapod/utils/arrow_data_utils.py`, alongside any existing `orcapod` imports:

```python
from orcapod.types import UUID_ARROW_TYPE
```

- [ ] **Step 3: Replace UUID column field types**

For every `pa.field(record_id_col_name, pa.large_string(), ...)` and `pa.array(..., type=pa.large_string())` that is used for UUID columns (identified in Step 1), replace `pa.large_string()` with `UUID_ARROW_TYPE`. Leave non-UUID `large_string` columns (e.g. `source_id` hash columns, string metadata) unchanged.

Example — if you see:

```python
pa.field(record_id_col_name, pa.large_string(), nullable=False)
```

Change to:

```python
pa.field(record_id_col_name, UUID_ARROW_TYPE, nullable=False)
```

And if you see an array constructor for record IDs:

```python
pa.array(record_ids, type=pa.large_string())
```

Change to:

```python
pa.array(record_ids, type=UUID_ARROW_TYPE)
```

- [ ] **Step 4: Run the relevant test suite to surface failures**

```bash
uv run pytest tests/test_core/ tests/test_utils/ -x -v 2>&1 | head -60
```

Fix any type assertion failures by updating tests that check `pa.large_string()` for UUID columns to check `pa.binary(16)` instead.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/utils/arrow_data_utils.py tests/
git commit -m "fix(arrow): update system UUID column Arrow types to pa.binary(16) (PLT-1162)"
```

---

### Task 6: Update UUID generation sites to produce bytes

**Files:**
- Modify: `src/orcapod/core/datagrams/datagram.py`
- Modify: `src/orcapod/core/data_function.py`
- Modify: `src/orcapod/pipeline/logging_observer.py`
- Modify: `src/orcapod/pipeline/status_observer.py`

- [ ] **Step 1: Update `datagram.py`**

Open `src/orcapod/core/datagrams/datagram.py`. Find the `datagram_id` property (around line 432):

```python
@property
def datagram_id(self) -> str:
    if self._datagram_id is None:
        self._datagram_id = str(uuid7())
    return self._datagram_id
```

Change to:

```python
@property
def datagram_id(self) -> bytes:
    if self._datagram_id is None:
        self._datagram_id = uuid7().bytes
    return self._datagram_id
```

Also update the `_datagram_id` instance variable type annotation (if present) from `str | None` to `bytes | None`, and update the `__init__` parameter type annotation for `datagram_id` from `str | None` to `bytes | None`.

- [ ] **Step 2: Update `data_function.py`**

Open `src/orcapod/core/data_function.py`. Find the `record_id` generation (around line 555):

```python
record_id = str(uuid7())
```

Change to:

```python
record_id = uuid7().bytes
```

Update any type annotation for `record_id` in the same method from `str` to `bytes`.

- [ ] **Step 3: Update `logging_observer.py`**

Open `src/orcapod/pipeline/logging_observer.py`. Find the `_log_id` generation (around line 103):

```bash
grep -n "uuid7\|log_id" src/orcapod/pipeline/logging_observer.py
```

Change all `str(uuid7())` used for Arrow-stored IDs to `uuid7().bytes`.

- [ ] **Step 4: Update `status_observer.py`**

Open `src/orcapod/pipeline/status_observer.py`. Find the `_status_id` generation (lines 273 and 400):

```bash
grep -n "uuid7\|status_id" src/orcapod/pipeline/status_observer.py
```

Change all `str(uuid7())` used for Arrow-stored IDs to `uuid7().bytes`.

- [ ] **Step 5: Run the core test suite**

```bash
uv run pytest tests/test_core/datagrams/ tests/test_core/data_function/ \
    tests/test_observability/ -x -v 2>&1 | head -80
```

Fix any type assertion failures — tests that previously checked `isinstance(datagram_id, str)` should now check `isinstance(datagram_id, bytes)` and `len(datagram_id) == 16`.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/datagrams/datagram.py \
        src/orcapod/core/data_function.py \
        src/orcapod/pipeline/logging_observer.py \
        src/orcapod/pipeline/status_observer.py \
        tests/
git commit -m "fix(core): UUID generation sites produce bytes instead of str (PLT-1162)"
```

---

### Task 7: Update PostgreSQL connector UUID mapping

**Files:**
- Modify: `src/orcapod/databases/postgresql_connector.py`
- Modify: `tests/test_databases/` (find PostgreSQL connector test)

- [ ] **Step 1: Locate and write the failing test**

```bash
grep -rl "PostgreSQL\|postgresql\|pg_type_to_arrow\|uuid" tests/test_databases/ --include="*.py" | head -5
```

Open the relevant test file and add:

```python
import pyarrow as pa
from orcapod.types import UUID_ARROW_TYPE


def test_pg_uuid_maps_to_binary16():
    """PostgreSQL 'uuid' type must map to UUID_ARROW_TYPE = pa.binary(16)."""
    from orcapod.databases.postgresql_connector import _pg_type_to_arrow

    result = _pg_type_to_arrow("uuid")
    assert result == UUID_ARROW_TYPE
    assert result == pa.binary(16)
```

- [ ] **Step 2: Run to verify test fails**

```bash
uv run pytest tests/test_databases/ -k "pg_uuid_maps" -v
```

Expected: FAIL — returns `pa.large_string()`

- [ ] **Step 3: Update `_pg_type_to_arrow` in `postgresql_connector.py`**

Open `src/orcapod/databases/postgresql_connector.py`. Add the import near the top of the file (or inside the function):

```python
from orcapod.types import UUID_ARROW_TYPE
```

Find the UUID branch in `_pg_type_to_arrow` (around line 95-100):

```python
    if t in ("text", "varchar", "character varying", "char", "bpchar",
             "name", "uuid", "json", "jsonb", "time", "timetz"):
        if t in ("time", "timetz"):
            logger.warning("PostgreSQL type %r mapped to pa.large_string() (known gap)", t)
        if t == "uuid":
            # TODO: revisit mapping once PLT-1162 decides on a canonical UUID Arrow type
            pass
        return _pa.large_string()
```

Restructure to handle `uuid` separately before the grouped string branch:

```python
    if t == "uuid":
        return UUID_ARROW_TYPE

    if t in ("text", "varchar", "character varying", "char", "bpchar",
             "name", "json", "jsonb", "time", "timetz"):
        if t in ("time", "timetz"):
            logger.warning("PostgreSQL type %r mapped to pa.large_string() (known gap)", t)
        return _pa.large_string()
```

- [ ] **Step 4: Handle bytes conversion in array construction**

The PostgreSQL driver (`psycopg2`) returns `uuid` column values as Python `uuid.UUID` objects. When building Arrow arrays from fetched rows the connector must convert these to `bytes` before passing to `pa.binary(16)`.

First, locate the array-construction code:

```bash
grep -n "pa\.array\|pa\.table\|pa\.record_batch\|pyarrow\|_coerce\|_build\|_rows_to" \
    src/orcapod/databases/postgresql_connector.py | head -30
```

The connector will have a loop that collects column values and calls `pa.array(column_values, type=arrow_type)`. Add the following module-level helper above that loop:

```python
def _coerce_pg_value(value: Any, arrow_type: "pa.DataType") -> Any:
    """Coerce a psycopg2 value to match the target Arrow type.

    Args:
        value: Raw value from the psycopg2 cursor.
        arrow_type: The Arrow type the value will be stored as.

    Returns:
        A Python value compatible with ``pa.array(..., type=arrow_type)``.
    """
    import pyarrow as _pa
    if value is None:
        return None
    # psycopg2 returns uuid columns as uuid.UUID objects; binary(16) needs bytes
    if arrow_type == _pa.binary(16) and hasattr(value, "bytes"):
        return value.bytes
    return value
```

Then in the column-building loop, wrap each value:

```python
# Before
column_values = [row[col_idx] for row in rows]

# After
column_values = [_coerce_pg_value(row[col_idx], arrow_type) for row in rows]
```

If the connector uses a different pattern (e.g. a single `pa.Table.from_pydict` call), apply the same coercion on the dict values for any `pa.binary(16)` column.

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/test_databases/ -x -v 2>&1 | head -60
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/databases/postgresql_connector.py tests/test_databases/
git commit -m "fix(databases): map PostgreSQL uuid columns to UUID_ARROW_TYPE (pa.binary(16)) (PLT-1162)"
```

---

### Task 8: Full test suite — fix remaining failures

- [ ] **Step 1: Run the full test suite**

```bash
uv run pytest tests/ -x --tb=short 2>&1 | tail -40
```

- [ ] **Step 2: For each failure, apply the fix pattern**

Most remaining failures will be one of these patterns:

**Pattern A — test asserts `pa.large_string()` for a UUID column:**
```python
# Before (failing)
assert schema.field("datagram_id").type == pa.large_string()
# After (fixed)
assert schema.field("datagram_id").type == pa.binary(16)
```

**Pattern B — test constructs a UUID value as a string:**
```python
# Before (failing)
datagram = Datagram(datagram_id="550e8400-e29b-41d4-a716-446655440000")
# After (fixed)
import uuid
datagram = Datagram(datagram_id=uuid.UUID("550e8400-e29b-41d4-a716-446655440000").bytes)
```

**Pattern C — test checks `isinstance(x, str)` for a UUID column value:**
```python
# Before (failing)
assert isinstance(datagram.datagram_id, str)
# After (fixed)
assert isinstance(datagram.datagram_id, bytes)
assert len(datagram.datagram_id) == 16
```

- [ ] **Step 3: Run the full suite again to confirm all pass**

```bash
uv run pytest tests/ --tb=short 2>&1 | tail -20
```

Expected: all tests PASS, no failures

- [ ] **Step 4: Commit**

```bash
git add tests/
git commit -m "test: update UUID column type assertions from large_string to binary(16) (PLT-1162)"
```

---

### Task 9: Post-implementation linear comment

- [ ] **Step 1: Post design note to PLT-1162**

Post a comment on [PLT-1162](https://linear.app/enigma-metamorphic/issue/PLT-1162) summarising the decision:

```
mcp__claude_ai_Linear__save_comment(
  issueId: "PLT-1162",
  body: "## Decision

Canonical UUID Arrow type: `pa.binary(16)` (`fixed_size_binary[16]`) for all UUID values in OrcaPod.

**Two layers:**
- `UUID_ARROW_TYPE = pa.binary(16)` — all system UUID columns (datagram_id, record_id, _log_id, _status_id)
- `UUID_STRUCT_ARROW_TYPE = pa.struct([pa.field(\"uuid\", pa.binary(16))])` — semantic type for uuid.UUID Python round-trips

**Alternatives rejected:**
- `pa.large_string()` — 2.75× larger, no type distinction from arbitrary strings
- `pa.uuid()` — Polars 1.31 hard incompatibility (ComputeError on conversion)
- struct with large_string inner type — UUID is fundamentally binary, not a string

**Known limitation:** Polars `write_json()` panics on Binary columns (pola-rs/polars#15410). OrcaPod has zero `write_json()` calls; unaffected in practice.

Full spec: `superpowers/specs/2026-06-11-plt-1162-uuid-arrow-type-design.md`"
)
```

---

### Task 10: Push and create PR

- [ ] **Step 1: Push branch**

```bash
git push -u origin eywalker/plt-1162-design-spike-uuid-arrow-type-mapping-large_string-vs
```

- [ ] **Step 2: Invoke `sensei:create-pr`**

Use the `sensei:create-pr` skill to create the pull request targeting `main`.
