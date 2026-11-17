# ExtensionTypeConverter Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `src/orcapod/extension_types/protocols.py` containing the `ExtensionTypeConverter` Protocol, establishing the new `extension_types/` subpackage.

**Architecture:** New code only — the existing `SemanticStructConverterProtocol` and all old system files are untouched (parallel-build strategy). The `extension_types/` subpackage is created fresh. Only two files are written: the package `__init__.py` (empty placeholder) and `protocols.py` containing `ExtensionTypeConverter`.

**Tech Stack:** Python `typing.Protocol` (Python ≥ 3.11), PyArrow (`pa.DataType`), `uv run pytest`

---

## File map

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/orcapod/extension_types/__init__.py` | Empty package marker (populated by PLT-1656) |
| Create | `src/orcapod/extension_types/protocols.py` | `ExtensionTypeConverter` protocol |
| Create | `tests/test_extension_types/__init__.py` | Empty test package marker |
| Create | `tests/test_extension_types/test_protocols.py` | All protocol conformance tests |

---

## Task 0: Checkout the feature branch

- [ ] **Step 0.1: Create and check out the branch**

```bash
git checkout -b eywalker/plt-1652-define-extensiontypeconverter-protocol-with-extension-name
```

Expected output: `Switched to a new branch 'eywalker/plt-1652-...'`

- [ ] **Step 0.2: Verify**

```bash
git branch --show-current
```

Expected: `eywalker/plt-1652-define-extensiontypeconverter-protocol-with-extension-name`

---

## Task 1: Scaffold the package structure

**Files:**
- Create: `src/orcapod/extension_types/__init__.py`
- Create: `tests/test_extension_types/__init__.py`

- [ ] **Step 1.1: Create the `extension_types` source package**

```bash
mkdir -p src/orcapod/extension_types
touch src/orcapod/extension_types/__init__.py
```

The `__init__.py` stays empty for now. PLT-1656 will populate it with import-time converter registration.

- [ ] **Step 1.2: Create the test package**

```bash
mkdir -p tests/test_extension_types
touch tests/test_extension_types/__init__.py
```

- [ ] **Step 1.3: Verify the directories exist**

```bash
ls src/orcapod/extension_types/ && ls tests/test_extension_types/
```

Expected:
```
__init__.py
__init__.py
```

---

## Task 2: Write failing tests

**Files:**
- Create: `tests/test_extension_types/test_protocols.py`

- [ ] **Step 2.1: Write the test file**

Create `tests/test_extension_types/test_protocols.py` with this exact content:

```python
"""Tests for ExtensionTypeConverter protocol."""

from __future__ import annotations

import pyarrow as pa

from orcapod.extension_types.protocols import ExtensionTypeConverter


class _StubConverter:
    """Minimal conforming implementation of ExtensionTypeConverter for use in tests."""

    @property
    def extension_name(self) -> str:
        return "test.module.MyType"

    @property
    def extension_metadata(self) -> bytes | None:
        return b"test.category"

    @property
    def storage_type(self) -> pa.DataType:
        return pa.large_string()

    @property
    def python_type(self) -> type:
        return str

    def python_to_storage(self, value):
        return str(value)

    def storage_to_python(self, storage_value):
        return storage_value


def test_protocol_is_importable():
    """ExtensionTypeConverter can be imported from extension_types.protocols."""
    assert ExtensionTypeConverter is not None


def test_protocol_defines_required_members():
    """Protocol defines all six required members."""
    required = {
        "extension_name",
        "extension_metadata",
        "storage_type",
        "python_type",
        "python_to_storage",
        "storage_to_python",
    }
    for member in required:
        assert hasattr(ExtensionTypeConverter, member), f"Protocol missing member: {member}"


def test_conforming_class_satisfies_protocol():
    """A class implementing all required members works correctly via the protocol interface."""
    converter: ExtensionTypeConverter = _StubConverter()
    assert converter.extension_name == "test.module.MyType"
    assert converter.extension_metadata == b"test.category"
    assert converter.storage_type == pa.large_string()
    assert converter.python_type is str
    assert converter.python_to_storage(42) == "42"
    assert converter.storage_to_python("hello") == "hello"


def test_extension_metadata_can_be_none():
    """extension_metadata is allowed to be None — it is bytes | None."""

    class NullMetadataConverter:
        @property
        def extension_name(self) -> str:
            return "test.NullMeta"

        @property
        def extension_metadata(self) -> bytes | None:
            return None

        @property
        def storage_type(self) -> pa.DataType:
            return pa.binary(16)

        @property
        def python_type(self) -> type:
            return bytes

        def python_to_storage(self, value):
            return value

        def storage_to_python(self, storage_value):
            return storage_value

    converter: ExtensionTypeConverter = NullMetadataConverter()
    assert converter.extension_metadata is None


def test_storage_type_not_constrained_to_struct():
    """storage_type accepts any pa.DataType — primitive types are valid, not only struct."""

    class BinaryConverter:
        @property
        def extension_name(self) -> str:
            return "uuid.UUID"

        @property
        def extension_metadata(self) -> bytes | None:
            return b"orcapod.builtin"

        @property
        def storage_type(self) -> pa.DataType:
            return pa.binary(16)

        @property
        def python_type(self) -> type:
            return bytes

        def python_to_storage(self, value):
            return value

        def storage_to_python(self, storage_value):
            return bytes(storage_value)

    converter: ExtensionTypeConverter = BinaryConverter()
    assert converter.storage_type == pa.binary(16)
    assert not pa.types.is_struct(converter.storage_type)


def test_protocol_does_not_include_old_members():
    """The new protocol must not define hashing or struct-dispatch members from the old protocol."""
    excluded = {
        "hash_struct_dict",
        "hasher_id",
        "can_handle_python_type",
        "can_handle_struct_type",
        "arrow_struct_type",
    }
    for member in excluded:
        assert not hasattr(ExtensionTypeConverter, member), (
            f"ExtensionTypeConverter must not define '{member}' — "
            "hashing and struct-shape dispatch are not part of the new protocol"
        )
```

- [ ] **Step 2.2: Run the tests to confirm they fail with ImportError**

```bash
uv run pytest tests/test_extension_types/test_protocols.py -v
```

Expected: all tests **FAIL** with:
```
ModuleNotFoundError: No module named 'orcapod.extension_types'
```
(or `ImportError: cannot import name 'ExtensionTypeConverter'` once the package exists but the file does not)

---

## Task 3: Implement ExtensionTypeConverter

**Files:**
- Create: `src/orcapod/extension_types/protocols.py`

- [ ] **Step 3.1: Write the protocol**

Create `src/orcapod/extension_types/protocols.py` with this exact content:

```python
"""Protocol definitions for the Arrow/Polars extension type system.

This module defines ``ExtensionTypeConverter`` — the contract for all
converters that map between Python objects and their Arrow extension type
storage representation.

Note:
    This module is part of the parallel-build phase. The old
    ``SemanticStructConverterProtocol`` in ``protocols/semantic_types_protocols.py``
    is untouched; it is removed in PLT-1660.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import pyarrow as pa


class ExtensionTypeConverter(Protocol):
    """Protocol for Arrow/Polars extension-type-backed converters.

    Declares the full contract for a converter that maps between Python
    objects and their Arrow extension type storage representation. This
    protocol is Arrow I/O only — hashing is not a converter responsibility.

    Attributes:
        extension_name: Fully-qualified Python class name used as the
            ``ARROW:extension:name`` metadata value (e.g. ``"pathlib.Path"``).
            Must be unique across all registered converters. By convention
            equals the FQCN, but any unique string is valid.
        extension_metadata: Category tag encoded as ``ARROW:extension:metadata``
            (e.g. ``b"orcapod.dataclass"``). Used by the registry to locate
            the right category handler at read time. May be ``None``.
        storage_type: The underlying Arrow ``pa.DataType`` used for physical
            storage (e.g. ``pa.large_string()``, ``pa.binary(16)``,
            ``pa.struct(...)``). Not used as an identity signal — identity
            is determined solely by ``extension_name``.
        python_type: The Python class this converter handles.
    """

    @property
    def extension_name(self) -> str:
        """Fully-qualified Python class name; stored as ``ARROW:extension:name``."""
        ...

    @property
    def extension_metadata(self) -> "bytes | None":
        """Category tag; stored as ``ARROW:extension:metadata``. May be ``None``."""
        ...

    @property
    def storage_type(self) -> "pa.DataType":
        """Underlying Arrow storage type. Any ``pa.DataType`` is valid."""
        ...

    @property
    def python_type(self) -> type:
        """The Python class this converter handles."""
        ...

    def python_to_storage(self, value: Any) -> Any:
        """Convert a Python value to its Arrow storage representation.

        Args:
            value: A Python object of type ``python_type``.

        Returns:
            A value suitable for use as an Arrow scalar or array element
            of type ``storage_type``.
        """
        ...

    def storage_to_python(self, storage_value: Any) -> Any:
        """Convert an Arrow storage value back to a Python object.

        Args:
            storage_value: A scalar or array element of type ``storage_type``.

        Returns:
            A Python object of type ``python_type``.
        """
        ...
```

- [ ] **Step 3.2: Run the full test suite to confirm tests pass and nothing regresses**

```bash
uv run pytest tests/test_extension_types/test_protocols.py -v
```

Expected: all 6 tests **PASS**:
```
tests/test_extension_types/test_protocols.py::test_protocol_is_importable PASSED
tests/test_extension_types/test_protocols.py::test_protocol_defines_required_members PASSED
tests/test_extension_types/test_protocols.py::test_conforming_class_satisfies_protocol PASSED
tests/test_extension_types/test_protocols.py::test_extension_metadata_can_be_none PASSED
tests/test_extension_types/test_protocols.py::test_storage_type_not_constrained_to_struct PASSED
tests/test_extension_types/test_protocols.py::test_protocol_does_not_include_old_members PASSED
```

- [ ] **Step 3.3: Run the full test suite to verify nothing is broken**

```bash
uv run pytest --ignore=tests/test_databases -x -q
```

Expected: all existing tests still pass. (Databases tests are excluded as they require live connections.)

---

## Task 4: Commit

- [ ] **Step 4.1: Stage and commit**

```bash
git add src/orcapod/extension_types/__init__.py \
        src/orcapod/extension_types/protocols.py \
        tests/test_extension_types/__init__.py \
        tests/test_extension_types/test_protocols.py \
        superpowers/specs/2026-06-13-plt-1652-extension-type-converter-protocol-design.md \
        docs/metamorphic/plans/2026-06-13-plt-1652-extension-type-converter-protocol.md

git commit -m "feat(extension_types): add ExtensionTypeConverter protocol and extension_types subpackage

Establishes the extension_types/ subpackage and defines ExtensionTypeConverter —
the new Arrow I/O-only protocol for extension-type-backed converters (PLT-1652).

Old SemanticStructConverterProtocol is not touched (parallel-build strategy).
Removal is deferred to PLT-1660."
```

- [ ] **Step 4.2: Push the branch**

```bash
git push -u origin eywalker/plt-1652-define-extensiontypeconverter-protocol-with-extension-name
```
