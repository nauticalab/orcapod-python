# PLT-1660: Hard Cut Extension Type Hashing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the old shape-based `SemanticTypeRegistry` system, wire the new extension-type system into Arrow hashing, and rename all protocol/registry/handler classes to cleaner names.

**Architecture:** `ArrowTypeDataVisitor` gains a `visit_extension()` hook (default: passthrough). `SemanticHashingVisitor` overrides it: for extension types whose Python counterpart has a registered semantic hasher, it converts the value to a Python object, hashes it, and stores the result as `pa.large_binary()` in the format `<type_name>::<method>:<digest>`. Unrecognized extension types pass through unmodified — starfix still sees their full metadata. All `TypeHandlerProtocol.handle()->Any` handlers are tightened to `PythonTypeSemanticHasherProtocol.hash()->ContentHash`.

**Tech Stack:** Python 3.10+, PyArrow extension types, starfix-python, uv/pytest

---

## File Map

**Modified source:**
- `src/orcapod/protocols/hashing_protocols.py` — rename `TypeHandlerProtocol`→`PythonTypeSemanticHasherProtocol`, `handle()`→`hash()->ContentHash`; rename `type_handler_registry`→`type_semantic_hasher_registry` on `SemanticHasherProtocol`
- `src/orcapod/hashing/semantic_hashing/type_handler_registry.py` — rename class + all methods
- `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` — rename 11 handler classes; `handle()`→`hash()->ContentHash`; rename `register_builtin_handlers`
- `src/orcapod/hashing/semantic_hashing/semantic_hasher.py` — rename `BaseSemanticHasher`→`SemanticAwarePythonHasher`; simplify dispatch; rename property
- `src/orcapod/hashing/semantic_hashing/content_identifiable_mixin.py` — update import + type annotations
- `src/orcapod/hashing/semantic_hashing/__init__.py` — update exports
- `src/orcapod/hashing/__init__.py` — update exports
- `src/orcapod/hashing/defaults.py` — rename function; update property access; remove broken `set_cacher` call
- `src/orcapod/hashing/visitors.py` — add `visit_extension` to base class + rewrite `SemanticHashingVisitor`
- `src/orcapod/hashing/arrow_hashers.py` — update `StarfixArrowHasher` constructor + short-circuit; delete `SemanticArrowHasher`
- `src/orcapod/hashing/versioned_hashers.py` — source `StarfixArrowHasher` from context; rename imports
- `src/orcapod/contexts/data/v0.1.json` — reorder components; remove `semantic_registry`; update class names and refs; add `type_converter`+`semantic_hasher` to `arrow_hasher`; remove `pa.Table` handlers (cycle-break)
- `src/orcapod/contexts/data/schemas/context_schema.json` — remove `semantic_registry` property; rename `type_handler_registry`→`python_type_semantic_hasher_registry`
- `src/orcapod/contexts/core.py` — update docstring for renamed property
- `src/orcapod/semantic_types/__init__.py` — remove `SemanticTypeRegistry` export
- `src/orcapod/protocols/semantic_types_protocols.py` — delete `SemanticStructConverterProtocol`

**Deleted source:**
- `src/orcapod/semantic_types/semantic_struct_converters.py`
- `src/orcapod/semantic_types/semantic_registry.py`

**Deleted tests:**
- `tests/test_semantic_types/` (all 9 files)
- `tests/test_hashing/test_file_hashing_consistency.py`

**New tests:**
- `tests/test_hashing/test_extension_type_hashing.py`

**Updated tests:**
- `tests/test_hashing/test_semantic_hasher.py`
- `tests/test_hashing/test_starfix_arrow_hasher.py`

---

## Task 1: Rename `TypeHandlerProtocol` → `PythonTypeSemanticHasherProtocol`

**Files:**
- Modify: `src/orcapod/protocols/hashing_protocols.py`

- [ ] **Step 1: Rewrite the protocol class and update surrounding references**

Replace the entire `TypeHandlerProtocol` class and update the `SemanticHasherProtocol`'s `type_handler_registry` property:

```python
# In src/orcapod/protocols/hashing_protocols.py

# Update TYPE_CHECKING import:
if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeSemanticHasherRegistry
    from orcapod.types import ContentHash  # already imported at module level, just noting

# Replace TypeHandlerProtocol with:
class PythonTypeSemanticHasherProtocol(Protocol):
    """Protocol for type-specific semantic hashers used by SemanticAwarePythonHasher.

    A PythonTypeSemanticHasherProtocol hashes a specific Python type to a ``ContentHash``.
    Implementations are registered with a ``PythonTypeSemanticHasherRegistry`` and looked
    up via MRO-aware resolution.

    Each implementation receives the full ``SemanticAwarePythonHasher`` so it can delegate
    hashing of sub-values (e.g. hashing a dict of function metadata) back to the outer
    hasher without coupling to a specific hasher instance.
    """

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        """Hash *obj* to a ContentHash.

        Args:
            obj:    The object to hash. Always matches the registered type.
            hasher: The active ``SemanticAwarePythonHasher``. Use
                    ``hasher.hash_object(sub_value)`` to hash sub-values.

        Returns:
            ContentHash: The content-addressed hash of *obj*.
        """
        ...


# Update SemanticHasherProtocol — rename the property:
class SemanticHasherProtocol(Protocol):
    # ... existing methods unchanged ...

    @property
    def type_semantic_hasher_registry(self) -> "PythonTypeSemanticHasherRegistry":
        """Return the PythonTypeSemanticHasherRegistry used by this hasher."""
        ...
```

The full updated `hashing_protocols.py` (only `TypeHandlerProtocol` is renamed and `SemanticHasherProtocol.type_handler_registry` → `type_semantic_hasher_registry`; everything else is unchanged):

```python
"""Hash strategy protocols for dependency injection."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.types import ContentHash, PathLike, Schema

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeSemanticHasherRegistry


@runtime_checkable
class DataContextAwareProtocol(Protocol):
    """Protocol for objects aware of their data context."""

    @property
    def data_context_key(self) -> str:
        """Return the data context key associated with this object."""
        ...


@runtime_checkable
class PipelineElementProtocol(Protocol):
    """Protocol for objects that have a stable identity as an element in a pipeline graph."""

    def pipeline_identity_structure(self) -> Any:
        """Return a structure representing this element's pipeline identity."""
        ...

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the pipeline-level hash of this element."""
        ...


@runtime_checkable
class ContentIdentifiableProtocol(Protocol):
    """Protocol for objects that can express their semantic identity as a plain Python structure."""

    def identity_structure(self) -> Any:
        """Return a structure that represents the semantic identity of this object."""
        ...

    def content_hash(self, hasher: "SemanticHasherProtocol | None" = None) -> ContentHash:
        """Returns the content hash."""
        ...


class PythonTypeSemanticHasherProtocol(Protocol):
    """Protocol for type-specific semantic hashers used by SemanticAwarePythonHasher.

    A ``PythonTypeSemanticHasherProtocol`` hashes a specific Python type to a
    ``ContentHash``. Implementations are registered with a
    ``PythonTypeSemanticHasherRegistry`` and looked up via MRO-aware resolution.

    Each implementation receives the full ``SemanticAwarePythonHasher`` so it can
    delegate hashing of sub-values back to the outer hasher without coupling to a
    specific hasher instance.
    """

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        """Hash *obj* to a ContentHash.

        Args:
            obj:    The object to hash. Always matches the registered type.
            hasher: The active ``SemanticAwarePythonHasher``. Use
                    ``hasher.hash_object(sub_value)`` to hash sub-values.

        Returns:
            ContentHash: The content-addressed hash of *obj*.
        """
        ...


class SemanticHasherProtocol(Protocol):
    """Protocol for the semantic content-based hasher."""

    def hash_object(
        self,
        obj: Any,
        resolver: Callable[[Any], ContentHash] | None = None,
    ) -> ContentHash:
        """Hash *obj* based on its semantic content."""
        ...

    @property
    def hasher_id(self) -> str:
        """Returns a unique identifier/name for this hasher instance."""
        ...

    @property
    def type_semantic_hasher_registry(self) -> "PythonTypeSemanticHasherRegistry":
        """Return the PythonTypeSemanticHasherRegistry used by this hasher."""
        ...


class FileContentHasherProtocol(Protocol):
    """Protocol for file-related hashing."""

    def hash_file(self, file_path: PathLike) -> ContentHash: ...


@runtime_checkable
class ArrowHasherProtocol(Protocol):
    """Protocol for hashing arrow data."""

    @property
    def hasher_id(self) -> str: ...

    def hash_table(self, table: "pa.Table | pa.RecordBatch") -> ContentHash: ...


class StringCacherProtocol(Protocol):
    """Protocol for caching string key value pairs."""

    def get_cached(self, cache_key: str) -> str | None: ...
    def set_cached(self, cache_key: str, value: str) -> None: ...
    def clear_cache(self) -> None: ...


class FunctionInfoExtractorProtocol(Protocol):
    """Protocol for extracting function information."""

    def extract_function_info(
        self,
        func: Callable[..., Any],
        function_name: str | None = None,
        input_typespec: Schema | None = None,
        output_typespec: Schema | None = None,
        exclude_function_signature: bool = False,
        exclude_function_body: bool = False,
    ) -> dict[str, Any]: ...


class SemanticTypeHasherProtocol(Protocol):
    """Abstract base class for semantic type-specific hashers."""

    @property
    def hasher_id(self) -> str:
        """Unique identifier for this semantic type hasher."""
        ...

    def hash_column(self, column: "pa.Array") -> "pa.Array":
        """Hash a column with this semantic type and return the hash bytes as an array."""
        ...

    def set_cacher(self, cacher: StringCacherProtocol) -> None:
        """Add a string cacher for caching hash values."""
        ...
```

- [ ] **Step 2: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add src/orcapod/protocols/hashing_protocols.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "refactor(hashing_protocols): rename TypeHandlerProtocol → PythonTypeSemanticHasherProtocol, tighten hash() → ContentHash"
```

---

## Task 2: Rename `TypeHandlerRegistry` → `PythonTypeSemanticHasherRegistry`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/type_handler_registry.py`

- [ ] **Step 1: Rename the class, subclass, and all methods**

Write the complete new file:

```python
"""
PythonTypeSemanticHasherRegistry — MRO-aware registry for PythonTypeSemanticHasherProtocol instances.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        PythonTypeSemanticHasherProtocol,
    )

logger = logging.getLogger(__name__)


class PythonTypeSemanticHasherRegistry:
    """Registry mapping Python types to PythonTypeSemanticHasherProtocol instances.

    Lookup is MRO-aware: when no hasher is registered for the exact type of
    an object, the registry walks the object's MRO (most-derived first) until
    it finds a match.

    Thread safety
    -------------
    Registration and lookup are protected by a reentrant lock so that the
    global singleton can be safely used from multiple threads.
    """

    def __init__(
        self, handlers: list[tuple[type, "PythonTypeSemanticHasherProtocol"]] | None = None
    ) -> None:
        """
        Args:
            handlers: Optional list of ``(target_type, hasher)`` pairs to
                register at construction time.
        """
        self._handlers: dict[type, "PythonTypeSemanticHasherProtocol"] = {}
        self._lock = threading.RLock()
        if handlers:
            for target_type, handler in handlers:
                self.register(target_type, handler)

    def register(self, target_type: type, handler: "PythonTypeSemanticHasherProtocol") -> None:
        """Register a hasher for a specific Python type.

        If a hasher is already registered for *target_type*, it is silently
        replaced by the new hasher.

        Args:
            target_type: The Python type (or class) for which the hasher should be used.
            handler: A ``PythonTypeSemanticHasherProtocol`` instance.

        Raises:
            TypeError: If ``target_type`` is not a ``type``.
        """
        if not isinstance(target_type, type):
            raise TypeError(
                f"target_type must be a type/class, got {type(target_type)!r}"
            )
        with self._lock:
            existing = self._handlers.get(target_type)
            if existing is not None and existing is not handler:
                logger.debug(
                    "PythonTypeSemanticHasherRegistry: replacing existing hasher for %s (%s -> %s)",
                    target_type.__name__,
                    type(existing).__name__,
                    type(handler).__name__,
                )
            self._handlers[target_type] = handler

    def unregister(self, target_type: type) -> bool:
        """Remove the hasher registered for *target_type*, if any.

        Args:
            target_type: The type whose hasher should be removed.

        Returns:
            True if a hasher was removed, False if none was registered.
        """
        with self._lock:
            if target_type in self._handlers:
                del self._handlers[target_type]
                return True
            return False

    def get_semantic_hasher(self, obj: Any) -> "PythonTypeSemanticHasherProtocol | None":
        """Look up the hasher for *obj* using MRO-aware resolution.

        Args:
            obj: The object for which a hasher is needed.

        Returns:
            The registered ``PythonTypeSemanticHasherProtocol``, or None.
        """
        obj_type = type(obj)
        with self._lock:
            handler = self._handlers.get(obj_type)
            if handler is not None:
                return handler
            for base in obj_type.__mro__[1:]:
                handler = self._handlers.get(base)
                if handler is not None:
                    logger.debug(
                        "PythonTypeSemanticHasherRegistry: resolved hasher for %s via base %s",
                        obj_type.__name__,
                        base.__name__,
                    )
                    return handler
        return None

    def get_semantic_hasher_for_type(
        self, target_type: type
    ) -> "PythonTypeSemanticHasherProtocol | None":
        """Look up the hasher for a *type object* (rather than an instance).

        Args:
            target_type: The type to look up.

        Returns:
            The registered ``PythonTypeSemanticHasherProtocol``, or None.
        """
        with self._lock:
            handler = self._handlers.get(target_type)
            if handler is not None:
                return handler
            for base in target_type.__mro__[1:]:
                handler = self._handlers.get(base)
                if handler is not None:
                    return handler
        return None

    def has_semantic_hasher(self, target_type: type) -> bool:
        """Return True if a hasher is registered for *target_type* or any MRO ancestor.

        Args:
            target_type: The type to check.
        """
        return self.get_semantic_hasher_for_type(target_type) is not None

    def registered_types(self) -> list[type]:
        """Return a list of all directly-registered types (no MRO expansion)."""
        with self._lock:
            return list(self._handlers.keys())

    def __repr__(self) -> str:
        with self._lock:
            names = [t.__name__ for t in self._handlers]
        return f"PythonTypeSemanticHasherRegistry(registered={names!r})"

    def __len__(self) -> int:
        with self._lock:
            return len(self._handlers)


def get_default_python_type_semantic_hasher_registry() -> "PythonTypeSemanticHasherRegistry":
    """Return the PythonTypeSemanticHasherRegistry from the default data context.

    This is a convenience wrapper; the registry is owned and versioned by the
    active ``DataContext``. Importing this function from
    ``orcapod.hashing.defaults`` or ``orcapod.hashing`` is equivalent.
    """
    from orcapod.hashing.defaults import (
        get_default_python_type_semantic_hasher_registry as _get,
    )
    return _get()


class BuiltinPythonTypeSemanticHasherRegistry(PythonTypeSemanticHasherRegistry):
    """A PythonTypeSemanticHasherRegistry pre-populated with all built-in hashers.

    Constructed via the data context JSON spec so that the default registry
    is versioned alongside the rest of the context components.
    """

    def __init__(self, arrow_hasher: "ArrowHasherProtocol | None" = None) -> None:
        super().__init__()
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            register_builtin_python_type_semantic_hashers,
        )
        register_builtin_python_type_semantic_hashers(self, arrow_hasher=arrow_hasher)
```

- [ ] **Step 2: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add src/orcapod/hashing/semantic_hashing/type_handler_registry.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "refactor(type_handler_registry): rename to PythonTypeSemanticHasherRegistry, rename methods"
```

---

## Task 3: Rename + tighten all builtin handlers

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`

- [ ] **Step 1: Write the complete updated file**

Key changes:
- 11 class renames (all `*Handler`/`*ContentHandler` → `*SemanticHasher`)
- `handle(obj, hasher) -> Any` → `hash(obj, hasher) -> ContentHash` on every class
- `UUIDSemanticHasher`, `BytesSemanticHasher`, `FunctionSemanticHasher`, `TypeObjectSemanticHasher`, `SpecialFormSemanticHasher`, `GenericAliasSemanticHasher`, `UnionTypeSemanticHasher` now call `hasher.hash_object(...)` to return `ContentHash` directly
- `register_builtin_handlers` → `register_builtin_python_type_semantic_hashers`
- Remove `SemanticArrowHasher` fallback construction (it will be deleted); when `arrow_hasher is None`, skip registering `pa.Table`/`pa.RecordBatch` handlers

```python
"""
Built-in PythonTypeSemanticHasherProtocol implementations.

  PathSemanticHasher       -- pathlib.Path: file content hash
  UPathSemanticHasher      -- upath.UPath: file content hash (remote-aware)
  UUIDSemanticHasher       -- uuid.UUID: 16-byte binary representation
  BytesSemanticHasher      -- bytes/bytearray: hex string representation
  FunctionSemanticHasher   -- callable with __code__: via FunctionInfoExtractorProtocol
  TypeObjectSemanticHasher -- type objects: stable "type:<module>.<qualname>" string
  SpecialFormSemanticHasher    -- typing._SpecialForm
  GenericAliasSemanticHasher   -- generic alias type annotations
  UnionTypeSemanticHasher      -- types.UnionType (Python 3.10+ X | Y syntax)
  ArrowTableSemanticHasher     -- pa.Table / pa.RecordBatch
  SchemaSemanticHasher         -- Schema objects

``register_builtin_python_type_semantic_hashers(registry)`` populates a registry
with all of the above.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from upath import UPath

from orcapod.types import ContentHash, PathLike, Schema

if TYPE_CHECKING:
    from orcapod.hashing.semantic_hashing.type_handler_registry import (
        PythonTypeSemanticHasherRegistry,
    )
    from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        FileContentHasherProtocol,
    )

logger = logging.getLogger(__name__)


class PathSemanticHasher:
    """Hasher for pathlib.Path objects — hashes file *content*.

    Args:
        file_hasher: Any object with a ``hash_file(path) -> ContentHash`` method.
    """

    def __init__(self, file_hasher: "FileContentHasherProtocol") -> None:
        self.file_hasher = file_hasher

    def hash(self, obj: PathLike, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        path: Path = Path(obj)
        if not path.exists():
            raise FileNotFoundError(
                f"PathSemanticHasher: path does not exist: {path!r}. "
                "Paths must refer to existing files for content-based hashing."
            )
        if path.is_dir():
            raise IsADirectoryError(
                f"PathSemanticHasher: path is a directory: {path!r}. "
                "Only regular files are supported for content-based hashing."
            )
        logger.debug("PathSemanticHasher: hashing file content at %s", path)
        return self.file_hasher.hash_file(path)


class UPathSemanticHasher:
    """Hasher for universal_pathlib.UPath objects — hashes file content.

    Args:
        file_hasher: Any object with a ``hash_file(path) -> ContentHash`` method.
    """

    def __init__(self, file_hasher: "FileContentHasherProtocol") -> None:
        self.file_hasher = file_hasher

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        if not isinstance(obj, UPath):
            raise TypeError(
                f"UPathSemanticHasher: expected a UPath, got {type(obj)!r}."
            )
        if not obj.exists():
            raise FileNotFoundError(
                f"UPathSemanticHasher: path does not exist: {obj!r}."
            )
        if obj.is_dir():
            raise IsADirectoryError(
                f"UPathSemanticHasher: path is a directory: {obj!r}."
            )
        logger.debug("UPathSemanticHasher: hashing file content at %s", obj)
        return self.file_hasher.hash_file(obj)


class UUIDSemanticHasher:
    """Hasher for ``uuid.UUID`` objects — hashes the raw 16-byte binary representation."""

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        return hasher.hash_object(obj.bytes)


class BytesSemanticHasher:
    """Hasher for bytes and bytearray objects — hashes the lowercase hex representation."""

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        if isinstance(obj, (bytes, bytearray)):
            return hasher.hash_object(obj.hex())
        raise TypeError(
            f"BytesSemanticHasher: expected bytes or bytearray, got {type(obj)!r}"
        )


class FunctionSemanticHasher:
    """Hasher for Python functions/callables with a ``__code__`` attribute.

    Args:
        function_info_extractor: Any object with an
            ``extract_function_info(func) -> dict`` method.
    """

    def __init__(self, function_info_extractor: Any) -> None:
        self.function_info_extractor = function_info_extractor

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        if not (callable(obj) and hasattr(obj, "__code__")):
            raise TypeError(
                f"FunctionSemanticHasher: expected a callable with __code__, got {type(obj)!r}"
            )
        func_name = getattr(obj, "__name__", repr(obj))
        logger.debug("FunctionSemanticHasher: extracting info for function %r", func_name)
        info: dict[str, Any] = self.function_info_extractor.extract_function_info(obj)
        return hasher.hash_object(info)


class TypeObjectSemanticHasher:
    """Hasher for type objects (classes passed as values).

    Returns a stable string of the form ``"type:<module>.<qualname>"``.
    """

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        if not isinstance(obj, type):
            raise TypeError(
                f"TypeObjectSemanticHasher: expected a type/class, got {type(obj)!r}"
            )
        module: str = obj.__module__ or "<unknown>"
        qualname: str = obj.__qualname__
        return hasher.hash_object(f"type:{module}.{qualname}")


class SpecialFormSemanticHasher:
    """Hasher for ``typing._SpecialForm`` objects such as ``typing.Union``."""

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        name = getattr(obj, "_name", None) or repr(obj)
        return hasher.hash_object(f"special_form:typing.{name}")


class GenericAliasSemanticHasher:
    """Hasher for generic alias type annotations (``dict[int, str]``, ``Optional[X]``, etc.)."""

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        import typing

        origin = getattr(obj, "__origin__", None)
        args = getattr(obj, "__args__", None) or ()
        if origin is None:
            return hasher.hash_object(f"generic_alias:{obj!r}")
        if origin is typing.Union:
            hashed_args = sorted(hasher.hash_object(arg).to_string() for arg in args)
            return hasher.hash_object({"__type__": "union", "args": hashed_args})
        return hasher.hash_object({
            "__type__": "generic_alias",
            "origin": hasher.hash_object(origin).to_string(),
            "args": [hasher.hash_object(arg).to_string() for arg in args],
        })


class UnionTypeSemanticHasher:
    """Hasher for ``types.UnionType`` objects (Python 3.10+ ``X | Y`` syntax)."""

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        args = getattr(obj, "__args__", None) or ()
        hashed_args = sorted(hasher.hash_object(arg).to_string() for arg in args)
        return hasher.hash_object({"__type__": "union", "args": hashed_args})


class ArrowTableSemanticHasher:
    """Hasher for ``pa.Table`` and ``pa.RecordBatch`` objects.

    Args:
        arrow_hasher: Any object satisfying ``ArrowHasherProtocol``.
    """

    def __init__(self, arrow_hasher: "ArrowHasherProtocol") -> None:
        self.arrow_hasher = arrow_hasher

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        import pyarrow as _pa

        if isinstance(obj, _pa.RecordBatch):
            obj = _pa.Table.from_batches([obj])
        if not isinstance(obj, _pa.Table):
            raise TypeError(
                f"ArrowTableSemanticHasher: expected pa.Table or pa.RecordBatch, got {type(obj)!r}"
            )
        return self.arrow_hasher.hash_table(obj)


class SchemaSemanticHasher:
    """Hasher for ``Schema`` objects."""

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        if not isinstance(obj, Schema):
            raise TypeError(
                f"SchemaSemanticHasher: expected a Schema, got {type(obj)!r}"
            )
        raise NotImplementedError("SchemaSemanticHasher is not yet implemented.")


def register_builtin_python_type_semantic_hashers(
    registry: "PythonTypeSemanticHasherRegistry",
    file_hasher: Any = None,
    function_info_extractor: Any = None,
    arrow_hasher: "ArrowHasherProtocol | None" = None,
) -> None:
    """Register all built-in semantic hashers into *registry*.

    When ``arrow_hasher`` is None, ``pa.Table`` and ``pa.RecordBatch`` handlers
    are **not** registered (to avoid circular dependency in the JSON context
    construction — the default context's ``python_type_semantic_hasher_registry``
    is built before ``arrow_hasher``).

    Args:
        registry: The ``PythonTypeSemanticHasherRegistry`` to populate.
        file_hasher: Optional ``FileContentHasherProtocol`` for path hashing.
            Defaults to ``BasicFileHasher(sha256)``.
        function_info_extractor: Optional ``FunctionInfoExtractorProtocol``.
            Defaults to ``FunctionSignatureExtractor``.
        arrow_hasher: Optional ``ArrowHasherProtocol`` for nested table hashing.
            When None, Arrow table handlers are skipped.
    """
    if file_hasher is None:
        from orcapod.hashing.file_hashers import BasicFileHasher
        file_hasher = BasicFileHasher(algorithm="sha256")

    if function_info_extractor is None:
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )
        function_info_extractor = FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
        )

    bytes_hasher = BytesSemanticHasher()
    registry.register(bytes, bytes_hasher)
    registry.register(bytearray, bytes_hasher)

    registry.register(Path, PathSemanticHasher(file_hasher))
    registry.register(UPath, UPathSemanticHasher(file_hasher))
    registry.register(UUID, UUIDSemanticHasher())

    import types as _types

    function_hasher = FunctionSemanticHasher(function_info_extractor)
    registry.register(_types.FunctionType, function_hasher)
    registry.register(_types.BuiltinFunctionType, function_hasher)
    registry.register(_types.MethodType, function_hasher)

    registry.register(type, TypeObjectSemanticHasher())
    registry.register(_types.UnionType, UnionTypeSemanticHasher())

    generic_alias_hasher = GenericAliasSemanticHasher()
    registry.register(_types.GenericAlias, generic_alias_hasher)
    try:
        import typing as _typing
        registry.register(_typing._GenericAlias, generic_alias_hasher)  # type: ignore[attr-defined]
        registry.register(_typing._SpecialForm, SpecialFormSemanticHasher())  # type: ignore[attr-defined]
    except AttributeError:
        pass

    registry.register(Schema, SchemaSemanticHasher())

    if arrow_hasher is not None:
        import pyarrow as _pa
        arrow_table_hasher = ArrowTableSemanticHasher(arrow_hasher)
        registry.register(_pa.Table, arrow_table_hasher)
        registry.register(_pa.RecordBatch, arrow_table_hasher)

    logger.debug(
        "register_builtin_python_type_semantic_hashers: registered %d hashers",
        len(registry),
    )
```

- [ ] **Step 2: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add src/orcapod/hashing/semantic_hashing/builtin_handlers.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "refactor(builtin_handlers): rename handler classes, tighten hash() → ContentHash"
```

---

## Task 4: Rename `BaseSemanticHasher` → `SemanticAwarePythonHasher`, simplify dispatch

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/semantic_hasher.py`

- [ ] **Step 1: Apply renames and simplify hash_object dispatch**

Changes:
1. Class name `BaseSemanticHasher` → `SemanticAwarePythonHasher`
2. `__init__` parameter `type_handler_registry` → `type_semantic_hasher_registry`
3. `self._registry = get_default_type_handler_registry()` → `get_default_python_type_semantic_hasher_registry()`
4. `type_handler_registry` property → `type_semantic_hasher_registry`
5. Return type annotation `TypeHandlerRegistry` → `PythonTypeSemanticHasherRegistry`
6. `hash_object` dispatch: `get_handler` → `get_semantic_hasher`; remove double-wrap (handler now returns `ContentHash` directly)

The dispatch block in `hash_object` changes from:
```python
handler = self._registry.get_handler(obj)
if handler is not None:
    return self.hash_object(handler.handle(obj, self), resolver=resolver)
```
to:
```python
semantic_hasher = self._registry.get_semantic_hasher(obj)
if semantic_hasher is not None:
    return semantic_hasher.hash(obj, self)
```

Full updated file (only showing the changed parts — keep everything else identical):

```python
# At top of file, update import:
from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeSemanticHasherRegistry

# Class rename:
class SemanticAwarePythonHasher:
    """
    Content-based recursive hasher.
    [same docstring, just update BaseSemanticHasher references to SemanticAwarePythonHasher]
    """

    def __init__(
        self,
        hasher_id: str,
        type_semantic_hasher_registry: PythonTypeSemanticHasherRegistry | None = None,
        strict: bool = True,
    ) -> None:
        self._hasher_id = hasher_id
        self._strict = strict

        if type_semantic_hasher_registry is None:
            from orcapod.hashing.defaults import get_default_python_type_semantic_hasher_registry
            self._registry = get_default_python_type_semantic_hasher_registry()
        else:
            self._registry = type_semantic_hasher_registry

    @property
    def hasher_id(self) -> str:
        return self._hasher_id

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def type_semantic_hasher_registry(self) -> PythonTypeSemanticHasherRegistry:
        """Return the ``PythonTypeSemanticHasherRegistry`` used by this hasher."""
        return self._registry

    def hash_object(self, obj, resolver=None):
        # ... keep all existing logic, EXCEPT replace the handler dispatch block:

        # Old:
        # handler = self._registry.get_handler(obj)
        # if handler is not None:
        #     return self.hash_object(handler.handle(obj, self), resolver=resolver)

        # New:
        # semantic_hasher = self._registry.get_semantic_hasher(obj)
        # if semantic_hasher is not None:
        #     return semantic_hasher.hash(obj, self)
        ...
```

The complete updated `hash_object` method (copy the full existing body, changing only the handler dispatch):

```python
def hash_object(
    self,
    obj: Any,
    resolver: Callable[[Any], ContentHash] | None = None,
) -> ContentHash:
    """Hash *obj* based on its semantic content."""
    # Terminal: already a hash -- return as-is.
    if isinstance(obj, ContentHash):
        return obj

    # Primitives: hash their direct JSON representation.
    if isinstance(obj, (type(None), bool, int, float, str)):
        return self._hash_to_content_hash(obj)

    # Structures: expand into a tagged tree, then hash the tree.
    if _is_structure(obj):
        expanded = self._expand_structure(
            obj, _visited=frozenset(), resolver=resolver
        )
        return self._hash_to_content_hash(expanded)

    # Semantic hasher dispatch: the hasher produces a ContentHash directly.
    semantic_hasher = self._registry.get_semantic_hasher(obj)
    if semantic_hasher is not None:
        logger.debug(
            "hash_object: dispatching %s to semantic hasher %s",
            type(obj).__name__,
            type(semantic_hasher).__name__,
        )
        return semantic_hasher.hash(obj, self)

    # ContentIdentifiableProtocol: use resolver if provided, else content_hash().
    if isinstance(obj, hp.ContentIdentifiableProtocol):
        if resolver is not None:
            logger.debug(
                "hash_object: resolving ContentIdentifiableProtocol %s via resolver",
                type(obj).__name__,
            )
            return resolver(obj)
        else:
            logger.debug(
                "hash_object: using ContentIdentifiableProtocol %s's content_hash",
                type(obj).__name__,
            )
            return obj.content_hash()

    # Fallback for unhandled types.
    fallback = self._handle_unknown(obj)
    return self._hash_to_content_hash(fallback)
```

- [ ] **Step 2: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add src/orcapod/hashing/semantic_hashing/semantic_hasher.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "refactor(semantic_hasher): rename BaseSemanticHasher → SemanticAwarePythonHasher, simplify dispatch"
```

---

## Task 5: Update `content_identifiable_mixin.py` and `contexts/core.py`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/content_identifiable_mixin.py`
- Modify: `src/orcapod/contexts/core.py`

- [ ] **Step 1: Update `content_identifiable_mixin.py`**

Three changes:
1. Line 68: `from orcapod.hashing.semantic_hashing.semantic_hasher import BaseSemanticHasher` → `SemanticAwarePythonHasher`
2. Line 97: parameter `semantic_hasher: BaseSemanticHasher | None` → `SemanticAwarePythonHasher | None`
3. Line 218 (approximately): `def _get_hasher(self) -> BaseSemanticHasher:` → `SemanticAwarePythonHasher`
4. Update the class docstring reference from `BaseSemanticHasher` to `SemanticAwarePythonHasher`

```python
# Old line 68:
from orcapod.hashing.semantic_hashing.semantic_hasher import BaseSemanticHasher

# New:
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
```

```python
# Old __init__ signature:
def __init__(
    self, *, semantic_hasher: BaseSemanticHasher | None = None, **kwargs: Any
) -> None:

# New:
def __init__(
    self, *, semantic_hasher: SemanticAwarePythonHasher | None = None, **kwargs: Any
) -> None:
```

Also update the `_get_hasher` return type annotation and any docstring mentions of `BaseSemanticHasher`.

- [ ] **Step 2: Update `contexts/core.py` docstring**

Update the `DataContext` docstring — replace `semantic_hasher.type_handler_registry` with `semantic_hasher.type_semantic_hasher_registry`:

```python
@dataclass
class DataContext:
    """Data context containing all versioned components needed for data interpretation.

    Attributes:
        context_key: Unique identifier (e.g., "std:v0.1:default")
        version: Version string (e.g., "v0.1")
        description: Human-readable description
        type_converter: Type converter for Python ↔ Arrow conversion and registration.
        arrow_hasher: Arrow table hasher for this context.
        semantic_hasher: General semantic hasher for this context. The
            ``PythonTypeSemanticHasherRegistry`` used for hashing is accessible via
            ``semantic_hasher.type_semantic_hasher_registry``.
    """
```

- [ ] **Step 3: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add src/orcapod/hashing/semantic_hashing/content_identifiable_mixin.py \
      src/orcapod/contexts/core.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "refactor: update BaseSemanticHasher → SemanticAwarePythonHasher refs in mixin and core"
```

---

## Task 6: Update `__init__.py` exports and `defaults.py`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/__init__.py`
- Modify: `src/orcapod/hashing/__init__.py`
- Modify: `src/orcapod/hashing/defaults.py`

- [ ] **Step 1: Update `semantic_hashing/__init__.py`**

```python
"""
orcapod.hashing.semantic_hashing
=================================
  SemanticAwarePythonHasher           -- content-based recursive object hasher
  PythonTypeSemanticHasherRegistry    -- MRO-aware registry mapping types → PythonTypeSemanticHasherProtocol
  BuiltinPythonTypeSemanticHasherRegistry  -- pre-populated registry with built-in hashers
  ContentIdentifiableMixin            -- convenience mixin for content-identifiable objects

Built-in PythonTypeSemanticHasherProtocol implementations:
  PathSemanticHasher          -- pathlib.Path  → file-content hash
  UUIDSemanticHasher          -- uuid.UUID     → canonical bytes
  BytesSemanticHasher         -- bytes/bytearray → hex string
  FunctionSemanticHasher      -- callable      → via FunctionInfoExtractorProtocol
  TypeObjectSemanticHasher    -- type objects  → "type:<module>.<qualname>"
  register_builtin_python_type_semantic_hashers -- populate a registry with all of the above

Function info extractors (used by FunctionSemanticHasher):
  FunctionNameExtractor
  FunctionSignatureExtractor
  FunctionInfoExtractorFactory
"""

from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesSemanticHasher,
    FunctionSemanticHasher,
    PathSemanticHasher,
    TypeObjectSemanticHasher,
    UUIDSemanticHasher,
    register_builtin_python_type_semantic_hashers,
)
from orcapod.hashing.semantic_hashing.content_identifiable_mixin import (
    ContentIdentifiableMixin,
)
from orcapod.hashing.semantic_hashing.function_info_extractors import (
    FunctionInfoExtractorFactory,
    FunctionNameExtractor,
    FunctionSignatureExtractor,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    BuiltinPythonTypeSemanticHasherRegistry,
    PythonTypeSemanticHasherRegistry,
)

__all__ = [
    "SemanticAwarePythonHasher",
    "PythonTypeSemanticHasherRegistry",
    "BuiltinPythonTypeSemanticHasherRegistry",
    "ContentIdentifiableMixin",
    "PathSemanticHasher",
    "UUIDSemanticHasher",
    "BytesSemanticHasher",
    "FunctionSemanticHasher",
    "TypeObjectSemanticHasher",
    "register_builtin_python_type_semantic_hashers",
    "FunctionNameExtractor",
    "FunctionSignatureExtractor",
    "FunctionInfoExtractorFactory",
]
```

- [ ] **Step 2: Update `hashing/__init__.py`**

```python
"""
OrcaPod hashing package.

Public API
----------
  SemanticAwarePythonHasher            -- content-based recursive object hasher
  SemanticHasherProtocol               -- protocol for semantic hashers
  PythonTypeSemanticHasherRegistry     -- registry mapping types to PythonTypeSemanticHasherProtocol instances
  get_default_semantic_hasher          -- global default SemanticHasherProtocol factory
  get_default_python_type_semantic_hasher_registry -- global default registry factory
  ContentIdentifiableMixin             -- convenience mixin for content-identifiable objects

Built-in hashers (importable for custom registry setup):
  PathSemanticHasher
  UUIDSemanticHasher
  BytesSemanticHasher
  FunctionSemanticHasher
  TypeObjectSemanticHasher
  register_builtin_python_type_semantic_hashers

Utility:
  FileContentHasherProtocol
  StringCacherProtocol
  FunctionInfoExtractorProtocol
  ArrowHasherProtocol
"""

from orcapod.hashing.defaults import (
    get_default_arrow_hasher,
    get_default_python_type_semantic_hasher_registry,
    get_default_semantic_hasher,
)
from orcapod.hashing.file_hashers import BasicFileHasher, CachedFileHasher
from orcapod.hashing.hash_utils import hash_file
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesSemanticHasher,
    FunctionSemanticHasher,
    PathSemanticHasher,
    TypeObjectSemanticHasher,
    UUIDSemanticHasher,
    register_builtin_python_type_semantic_hashers,
)
from orcapod.hashing.semantic_hashing.content_identifiable_mixin import (
    ContentIdentifiableMixin,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    BuiltinPythonTypeSemanticHasherRegistry,
    PythonTypeSemanticHasherRegistry,
)
from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    ContentIdentifiableProtocol,
    FileContentHasherProtocol,
    FunctionInfoExtractorProtocol,
    PythonTypeSemanticHasherProtocol,
    SemanticHasherProtocol,
    SemanticTypeHasherProtocol,
    StringCacherProtocol,
)

try:
    from orcapod.hashing.legacy_core import (
        HashableMixin,
        function_content_hash,
        get_function_signature,
        hash_function,
        hash_data,
        hash_pathset,
        hash_to_hex,
        hash_to_int,
        hash_to_uuid,
    )
except ImportError:
    HashableMixin = None  # type: ignore[assignment,misc]
    function_content_hash = None  # type: ignore[assignment]
    get_function_signature = None  # type: ignore[assignment]
    hash_function = None  # type: ignore[assignment]
    hash_data = None  # type: ignore[assignment]
    hash_pathset = None  # type: ignore[assignment]
    hash_to_hex = None  # type: ignore[assignment]
    hash_to_int = None  # type: ignore[assignment]
    hash_to_uuid = None  # type: ignore[assignment]

__all__ = [
    "SemanticAwarePythonHasher",
    "PythonTypeSemanticHasherRegistry",
    "BuiltinPythonTypeSemanticHasherRegistry",
    "get_default_python_type_semantic_hasher_registry",
    "get_default_semantic_hasher",
    "ContentIdentifiableMixin",
    "PathSemanticHasher",
    "UUIDSemanticHasher",
    "BytesSemanticHasher",
    "FunctionSemanticHasher",
    "TypeObjectSemanticHasher",
    "register_builtin_python_type_semantic_hashers",
    "SemanticHasherProtocol",
    "ContentIdentifiableProtocol",
    "PythonTypeSemanticHasherProtocol",
    "FileContentHasherProtocol",
    "ArrowHasherProtocol",
    "StringCacherProtocol",
    "FunctionInfoExtractorProtocol",
    "SemanticTypeHasherProtocol",
    "BasicFileHasher",
    "CachedFileHasher",
    "hash_file",
    "get_default_arrow_hasher",
    "HashableMixin",
    "hash_to_hex",
    "hash_to_int",
    "hash_to_uuid",
    "hash_function",
    "get_function_signature",
    "function_content_hash",
    "hash_pathset",
    "hash_data",
]
```

- [ ] **Step 3: Update `hashing/defaults.py`**

```python
# Default hasher accessors for the OrcaPod hashing system.

from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeSemanticHasherRegistry
from orcapod.protocols import hashing_protocols as hp


def get_default_python_type_semantic_hasher_registry() -> PythonTypeSemanticHasherRegistry:
    """Return the PythonTypeSemanticHasherRegistry from the default data context's semantic hasher.

    Returns:
        PythonTypeSemanticHasherRegistry: The registry from the default data context.
    """
    from orcapod.contexts import get_default_context
    return get_default_context().semantic_hasher.type_semantic_hasher_registry


def get_default_semantic_hasher() -> hp.SemanticHasherProtocol:
    """Return the SemanticHasherProtocol from the default data context."""
    from orcapod.contexts import get_default_context
    return get_default_context().semantic_hasher


def get_default_arrow_hasher() -> hp.ArrowHasherProtocol:
    """Return the ArrowHasherProtocol from the default data context.

    Note: file-hash caching (formerly via ``set_cacher``) has been removed.
    ``StarfixArrowHasher`` does not support per-path caching. Use
    ``CachedFileHasher`` when constructing a custom context if caching is needed.
    """
    from orcapod.contexts import get_default_context
    return get_default_context().arrow_hasher
```

- [ ] **Step 4: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add src/orcapod/hashing/semantic_hashing/__init__.py \
      src/orcapod/hashing/__init__.py \
      src/orcapod/hashing/defaults.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "refactor(hashing): update __init__.py exports and defaults for rename"
```

---

## Task 7: Update `test_semantic_hasher.py` → run tests

**Files:**
- Modify: `tests/test_hashing/test_semantic_hasher.py`

- [ ] **Step 1: Update imports at the top of the file**

```python
# Old:
from orcapod.hashing.semantic_hashing.builtin_handlers import register_builtin_handlers
from orcapod.hashing.semantic_hashing.semantic_hasher import (
    BaseSemanticHasher,
    _is_namedtuple,
)
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    TypeHandlerRegistry,
    get_default_type_handler_registry,
)

# New:
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    register_builtin_python_type_semantic_hashers,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import (
    SemanticAwarePythonHasher,
    _is_namedtuple,
)
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    PythonTypeSemanticHasherRegistry,
    get_default_python_type_semantic_hasher_registry,
)
```

- [ ] **Step 2: Update `make_hasher()` fixture and type annotations**

```python
def make_hasher(strict: bool = True) -> SemanticAwarePythonHasher:
    """Create a fresh SemanticAwarePythonHasher with an isolated registry."""
    registry = PythonTypeSemanticHasherRegistry()
    register_builtin_python_type_semantic_hashers(registry)
    return SemanticAwarePythonHasher(
        hasher_id="test_v1", type_semantic_hasher_registry=registry, strict=strict
    )


@pytest.fixture
def hasher() -> SemanticAwarePythonHasher:
    return make_hasher(strict=True)


@pytest.fixture
def lenient_hasher() -> SemanticAwarePythonHasher:
    return make_hasher(strict=False)
```

- [ ] **Step 3: Update `_DummyHandler` in `TestTypeHandlerRegistry` (near line 827)**

```python
# Old:
class _DummyHandler:
    def __init__(self, tag: str) -> None:
        self.tag = tag

    def handle(self, obj: Any, hasher: Any) -> Any:
        return f"{self.tag}:{obj}"

# New:
class _DummySemanticHasher:
    def __init__(self, tag: str) -> None:
        self.tag = tag

    def hash(self, obj: Any, hasher: Any) -> Any:
        # Returns a ContentHash by delegating to the outer hasher
        return hasher.hash_object(f"{self.tag}:{obj}")
```

- [ ] **Step 4: Update `TestTypeHandlerRegistry` class — rename class, method calls, and dummy handler**

Rename the test class to `TestPythonTypeSemanticHasherRegistry` and update every reference:
- `TypeHandlerRegistry()` → `PythonTypeSemanticHasherRegistry()`
- `_DummyHandler(...)` → `_DummySemanticHasher(...)`
- `reg.get_handler(...)` → `reg.get_semantic_hasher(...)`
- `reg.has_handler(...)` → `reg.has_semantic_hasher(...)`
- `reg.get_handler_for_type(...)` → `reg.get_semantic_hasher_for_type(...)`

Example of updated test methods:
```python
class TestPythonTypeSemanticHasherRegistry:
    def test_register_and_get_exact(self):
        reg = PythonTypeSemanticHasherRegistry()
        h = _DummySemanticHasher("base")
        reg.register(Base, h)
        assert reg.get_semantic_hasher(Base()) is h

    def test_mro_lookup_child(self):
        reg = PythonTypeSemanticHasherRegistry()
        h = _DummySemanticHasher("base")
        reg.register(Base, h)
        assert reg.get_semantic_hasher(Child()) is h

    def test_mro_lookup_grandchild(self):
        reg = PythonTypeSemanticHasherRegistry()
        h = _DummySemanticHasher("base")
        reg.register(Base, h)
        assert reg.get_semantic_hasher(GrandChild()) is h

    def test_more_specific_handler_wins(self):
        reg = PythonTypeSemanticHasherRegistry()
        h_base = _DummySemanticHasher("base")
        h_child = _DummySemanticHasher("child")
        reg.register(Base, h_base)
        reg.register(Child, h_child)
        assert reg.get_semantic_hasher(Child()) is h_child
        assert reg.get_semantic_hasher(GrandChild()) is h_child

    def test_unregistered_returns_none(self):
        reg = PythonTypeSemanticHasherRegistry()
        assert reg.get_semantic_hasher(Base()) is None

    def test_unregister_removes_handler(self):
        reg = PythonTypeSemanticHasherRegistry()
        h = _DummySemanticHasher("base")
        reg.register(Base, h)
        assert reg.unregister(Base) is True
        assert reg.get_semantic_hasher(Base()) is None

    def test_unregister_nonexistent_returns_false(self):
        reg = PythonTypeSemanticHasherRegistry()
        assert reg.unregister(Base) is False

    def test_replace_existing_handler(self):
        reg = PythonTypeSemanticHasherRegistry()
        h1 = _DummySemanticHasher("first")
        h2 = _DummySemanticHasher("second")
        reg.register(Base, h1)
        reg.register(Base, h2)
        assert reg.get_semantic_hasher(Base()) is h2

    def test_register_non_type_raises(self):
        reg = PythonTypeSemanticHasherRegistry()
        with pytest.raises(TypeError):
            reg.register("not_a_type", _DummySemanticHasher("x"))  # type: ignore[arg-type]

    def test_has_semantic_hasher_exact(self):
        reg = PythonTypeSemanticHasherRegistry()
        reg.register(Base, _DummySemanticHasher("b"))
        assert reg.has_semantic_hasher(Base) is True

    def test_has_semantic_hasher_via_mro(self):
        reg = PythonTypeSemanticHasherRegistry()
        reg.register(Base, _DummySemanticHasher("b"))
        assert reg.has_semantic_hasher(Child) is True

    def test_has_semantic_hasher_false(self):
        reg = PythonTypeSemanticHasherRegistry()
        assert reg.has_semantic_hasher(Base) is False

    def test_registered_types_snapshot(self):
        reg = PythonTypeSemanticHasherRegistry()
        reg.register(Base, _DummySemanticHasher("b"))
        reg.register(Child, _DummySemanticHasher("c"))
        types = reg.registered_types()
        assert Base in types
        assert Child in types

    def test_len(self):
        reg = PythonTypeSemanticHasherRegistry()
        assert len(reg) == 0
        reg.register(Base, _DummySemanticHasher("b"))
        assert len(reg) == 1
        reg.register(Child, _DummySemanticHasher("c"))
        assert len(reg) == 2

    def test_get_semantic_hasher_for_type(self):
        reg = PythonTypeSemanticHasherRegistry()
        h = _DummySemanticHasher("b")
        reg.register(Base, h)
        assert reg.get_semantic_hasher_for_type(Base) is h
        assert reg.get_semantic_hasher_for_type(Child) is h  # via MRO
        assert reg.get_semantic_hasher_for_type(int) is None
```

Also update any remaining references in the file body to `get_default_type_handler_registry` → `get_default_python_type_semantic_hasher_registry`, and any fixture type annotations.

- [ ] **Step 5: Run tests**

```bash
uv run --project /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  pytest tests/test_hashing/test_semantic_hasher.py -x -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add tests/test_hashing/test_semantic_hasher.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "test(semantic_hasher): update for registry rename and hash() protocol tightening"
```

---

## Task 8: Add `visit_extension` to `ArrowTypeDataVisitor` + rewrite `SemanticHashingVisitor`

**Files:**
- Modify: `src/orcapod/hashing/visitors.py`

- [ ] **Step 1: Write a failing test for `visit_extension` dispatch**

Create `tests/test_hashing/test_extension_type_hashing.py`:

```python
"""Tests for extension type column hashing via SemanticHashingVisitor."""

from __future__ import annotations

import pyarrow as pa
import pytest
from pathlib import Path

from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.contexts import get_default_context


@pytest.fixture
def ctx():
    return get_default_context()


class TestArrowTypeDataVisitorExtension:
    def test_visit_dispatches_to_visit_extension_for_extension_types(self, ctx):
        """visit() routes ExtensionType columns to visit_extension(), not visit_struct()."""
        arrow_type = ctx.type_converter.register_python_class(Path)
        assert isinstance(arrow_type, pa.ExtensionType), (
            "Path must be registered as an Arrow extension type"
        )

        calls = []

        class TrackingVisitor(SemanticHashingVisitor):
            def visit_extension(self, ext_type, storage_value):
                calls.append("visit_extension")
                return super().visit_extension(ext_type, storage_value)

            def visit_struct(self, struct_type, data):
                calls.append("visit_struct")
                return super().visit_struct(struct_type, data)

        visitor = TrackingVisitor(ctx.type_converter, ctx.semantic_hasher)
        # Any value is fine for this dispatch test — use a dummy string (storage for Path is str)
        visitor.visit(arrow_type, "/tmp/dummy")
        assert "visit_extension" in calls
        assert "visit_struct" not in calls


class TestSemanticHashingVisitorExtension:
    def test_path_column_hashed_to_large_binary(self, ctx, tmp_path):
        """Path extension columns are replaced with pa.large_binary() hash tokens."""
        file = tmp_path / "test.txt"
        file.write_text("hello")

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage_val = ctx.type_converter.python_to_storage(Path(file), Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(arrow_type, storage_val)

        assert new_type == pa.large_binary()
        assert isinstance(new_data, bytes)

    def test_same_content_same_hash(self, ctx, tmp_path):
        """Two paths pointing to files with identical content produce the same hash bytes."""
        file1 = tmp_path / "a.txt"
        file2 = tmp_path / "b.txt"
        file1.write_text("identical content")
        file2.write_text("identical content")

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage1 = ctx.type_converter.python_to_storage(Path(file1), Path)
        storage2 = ctx.type_converter.python_to_storage(Path(file2), Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, hash1 = visitor.visit(arrow_type, storage1)
        _, hash2 = visitor.visit(arrow_type, storage2)

        assert hash1 == hash2

    def test_different_content_different_hash(self, ctx, tmp_path):
        """Files with different content produce different hash bytes."""
        file1 = tmp_path / "x.txt"
        file2 = tmp_path / "y.txt"
        file1.write_text("content A")
        file2.write_text("content B")

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage1 = ctx.type_converter.python_to_storage(Path(file1), Path)
        storage2 = ctx.type_converter.python_to_storage(Path(file2), Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, hash1 = visitor.visit(arrow_type, storage1)
        _, hash2 = visitor.visit(arrow_type, storage2)

        assert hash1 != hash2

    def test_binary_encoding_format(self, ctx, tmp_path):
        """Hash bytes have format b'<type_name>::<method>:<digest>'."""
        file = tmp_path / "test.txt"
        file.write_text("test")

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage_val = ctx.type_converter.python_to_storage(Path(file), Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, hash_bytes = visitor.visit(arrow_type, storage_val)

        assert b"::" in hash_bytes
        type_prefix, hash_part = hash_bytes.split(b"::", 1)
        # Extension name "orcapod.path" → dots replaced with colons
        assert type_prefix == b"orcapod:path"
        # hash_part should be "method:digest" — at least one colon
        assert b":" in hash_part

    def test_null_value_passthrough(self, ctx):
        """Null storage values pass through as-is."""
        arrow_type = ctx.type_converter.register_python_class(Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(arrow_type, None)

        assert new_type == arrow_type
        assert new_data is None
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
uv run --project /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  pytest tests/test_hashing/test_extension_type_hashing.py -x -v
```

Expected: ImportError or AttributeError (methods don't exist yet).

- [ ] **Step 3: Rewrite `visitors.py`**

```python
"""
Generic visitor pattern for traversing Arrow types and data simultaneously.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter
    from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
else:
    pa = LazyModule("pyarrow")


class ArrowTypeDataVisitor(ABC):
    """Base visitor for traversing Arrow types and data simultaneously."""

    @abstractmethod
    def visit_struct(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Visit a struct type with its data."""
        pass

    @abstractmethod
    def visit_list(
        self, list_type: "pa.ListType", data: list | None
    ) -> tuple["pa.DataType", Any]:
        """Visit a list type with its data."""
        pass

    @abstractmethod
    def visit_map(
        self, map_type: "pa.MapType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Visit a map type with its data."""
        pass

    @abstractmethod
    def visit_primitive(
        self, primitive_type: "pa.DataType", data: Any
    ) -> tuple["pa.DataType", Any]:
        """Visit a primitive type with its data."""
        pass

    def visit_extension(
        self,
        extension_type: "pa.ExtensionType",
        storage_value: Any,
    ) -> tuple["pa.DataType", Any]:
        """Handle an Arrow extension type.

        Default implementation: passthrough — preserves the extension type and its
        storage value unchanged so that the downstream ``StarfixArrowHasher`` /
        ``ArrowDigester`` sees the full extension metadata when it receives the
        pre-processed table.

        Subclasses may override to convert recognised extension types to a hashed
        ``pa.large_binary()`` value.

        Args:
            extension_type: The Arrow extension type.
            storage_value: The storage-level value (result of ``to_pylist()`` on the column).

        Returns:
            Tuple of ``(new_arrow_type, new_data)``.
        """
        return extension_type, storage_value

    def visit(self, arrow_type: "pa.DataType", data: Any) -> tuple["pa.DataType", Any]:
        """Main dispatch method that routes to the appropriate visit method.

        Extension types are checked **first** — before the struct check — because
        extension types with struct storage would otherwise be incorrectly routed
        into ``visit_struct``.  After ``visit_extension``, the result is re-visited
        only if the type changed AND is no longer an extension type (enables
        composability, avoids infinite recursion).

        Args:
            arrow_type: Arrow data type to process.
            data: Corresponding data value.

        Returns:
            Tuple of ``(new_arrow_type, new_data)``.
        """
        if isinstance(arrow_type, pa.ExtensionType):
            new_type, new_data = self.visit_extension(arrow_type, data)
            if new_type is not arrow_type and not isinstance(new_type, pa.ExtensionType):
                return self.visit(new_type, new_data)
            return new_type, new_data

        if pa.types.is_struct(arrow_type):
            return self.visit_struct(arrow_type, data)
        elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            return self.visit_list(arrow_type, data)
        elif pa.types.is_fixed_size_list(arrow_type):
            return self.visit_list(arrow_type, data)
        elif pa.types.is_map(arrow_type):
            return self.visit_map(arrow_type, data)
        else:
            return self.visit_primitive(arrow_type, data)

    def _visit_struct_fields(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.StructType", dict]:
        """Recursively process struct fields. Default behavior for regular structs."""
        if data is None:
            return struct_type, None

        new_fields = []
        new_data = {}

        for field in struct_type:
            field_data = data.get(field.name)
            new_field_type, new_field_data = self.visit(field.type, field_data)
            new_fields.append(pa.field(field.name, new_field_type))
            new_data[field.name] = new_field_data

        return pa.struct(new_fields), new_data

    def _visit_list_elements(
        self, list_type: "pa.ListType", data: list | None
    ) -> tuple["pa.DataType", list]:
        """Recursively process list elements."""
        if data is None:
            return list_type, None

        element_type = list_type.value_type
        processed_elements = []
        new_element_type = None

        for item in data:
            current_element_type, processed_item = self.visit(element_type, item)
            processed_elements.append(processed_item)
            if new_element_type is None:
                new_element_type = current_element_type

        if new_element_type is None:
            new_element_type = element_type

        if pa.types.is_large_list(list_type):
            return pa.large_list(new_element_type), processed_elements
        elif pa.types.is_fixed_size_list(list_type):
            return pa.list_(new_element_type, list_type.list_size), processed_elements
        else:
            return pa.list_(new_element_type), processed_elements


class SemanticHashingError(Exception):
    """Exception raised when semantic hashing fails."""
    pass


class SemanticHashingVisitor(ArrowTypeDataVisitor):
    """Visitor that replaces extension-typed columns with their content hashes.

    For each Arrow column whose type is a ``pa.ExtensionType``:

    1. Look up the corresponding Python type via ``type_converter``.
    2. If the Python type has a semantic hasher registered in ``python_hasher``,
       convert the storage value to a Python object and hash it, replacing the
       column with a ``pa.large_binary()`` value of the form::

           <type_name_bytes> + b"::" + content_hash.to_prefixed_digest()

       where ``type_name`` is the extension name with dots replaced by colons
       (e.g. ``"orcapod.path"`` → ``"orcapod:path"``), and
       ``to_prefixed_digest()`` = ``method_bytes + b":" + digest``.
    3. If no hasher is registered (or the converter doesn't know the type),
       return the extension type and storage value unchanged. The downstream
       ``StarfixArrowHasher`` / ``ArrowDigester`` will see the full extension
       metadata intact and hash it in a type-aware way.

    Args:
        type_converter: The active ``UniversalTypeConverter`` for resolving
            extension type → Python type and storage → Python conversion.
        python_hasher: The active ``SemanticAwarePythonHasher`` for hashing
            Python objects.
    """

    def __init__(
        self,
        type_converter: "UniversalTypeConverter",
        python_hasher: "SemanticAwarePythonHasher",
    ) -> None:
        self._type_converter = type_converter
        self._python_hasher = python_hasher
        self._current_field_path: list[str] = []

    def visit_extension(
        self,
        extension_type: "pa.ExtensionType",
        storage_value: Any,
    ) -> tuple["pa.DataType", Any]:
        """Hash an extension type value to pa.large_binary(), or passthrough."""
        if storage_value is None:
            return extension_type, None

        from typing import Any as _Any

        # Resolve extension type → Python type.
        python_type = self._type_converter.arrow_type_to_python_type(extension_type)

        # If the converter couldn't resolve to a concrete class, passthrough.
        if python_type is _Any or not isinstance(python_type, type):
            return extension_type, storage_value

        # Only hash if a semantic hasher is registered for this Python type.
        if not self._python_hasher.type_semantic_hasher_registry.has_semantic_hasher(
            python_type
        ):
            return extension_type, storage_value

        # Convert storage value → Python object and hash it.
        python_obj = self._type_converter.storage_to_python(storage_value, python_type)
        content_hash = self._python_hasher.hash_object(python_obj)

        # Encode as binary: "<type_name>::<method>:<digest>"
        # Dots in the extension name → colons (e.g. "orcapod.path" → "orcapod:path").
        # The "::" separator is unambiguous because to_prefixed_digest() uses only ":".
        type_name = extension_type.extension_name.replace(".", ":")
        hash_bytes = (
            type_name.encode("ascii")
            + b"::"
            + content_hash.to_prefixed_digest()
        )
        return pa.large_binary(), hash_bytes

    def visit_struct(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Regular struct (no extension identity) — recurse into fields."""
        if data is None:
            return struct_type, None
        return self._visit_struct_fields(struct_type, data)

    def visit_list(
        self, list_type: "pa.ListType", data: list | None
    ) -> tuple["pa.DataType", Any]:
        """Recurse into list elements."""
        if data is None:
            return list_type, None
        self._current_field_path.append("[*]")
        try:
            return self._visit_list_elements(list_type, data)
        finally:
            self._current_field_path.pop()

    def visit_map(
        self, map_type: "pa.MapType", data: dict | None
    ) -> tuple["pa.DataType", Any]:
        """Pass map types through unchanged."""
        return map_type, data

    def visit_primitive(
        self, primitive_type: "pa.DataType", data: Any
    ) -> tuple["pa.DataType", Any]:
        """Pass primitive types through unchanged."""
        return primitive_type, data

    def _visit_struct_fields(
        self, struct_type: "pa.StructType", data: dict | None
    ) -> tuple["pa.StructType", dict]:
        """Override to add field path tracking for better error messages."""
        if data is None:
            return struct_type, None

        new_fields = []
        new_data = {}

        for field in struct_type:
            self._current_field_path.append(field.name)
            try:
                field_data = data.get(field.name)
                new_field_type, new_field_data = self.visit(field.type, field_data)
                new_fields.append(pa.field(field.name, new_field_type))
                new_data[field.name] = new_field_data
            finally:
                self._current_field_path.pop()

        return pa.struct(new_fields), new_data
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
uv run --project /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  pytest tests/test_hashing/test_extension_type_hashing.py -x -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add src/orcapod/hashing/visitors.py \
      tests/test_hashing/test_extension_type_hashing.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "feat(visitors): add visit_extension dispatch; rewrite SemanticHashingVisitor for extension types"
```

---

## Task 9: Update `StarfixArrowHasher`, delete `SemanticArrowHasher`

**Files:**
- Modify: `src/orcapod/hashing/arrow_hashers.py`

- [ ] **Step 1: Rewrite `arrow_hashers.py`**

Delete the entire `SemanticArrowHasher` class. Update `StarfixArrowHasher`:

```python
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa
from starfix import ArrowDigester

from orcapod.hashing.schema_cleaner import clean_schema_for_hashing, has_extension_metadata
from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.types import ContentHash
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter
    from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher


class StarfixArrowHasher:
    """Arrow table hasher backed by the starfix-python ``ArrowDigester``.

    Pipeline
    --------
    1. **Semantic pre-processing** — the ``SemanticHashingVisitor`` traverses
       every column. Extension-typed columns whose Python type has a registered
       semantic hasher are replaced with ``pa.large_binary()`` hash tokens
       (e.g. ``Path`` columns are replaced by their file-content hash).
       Extension-typed columns without a registered hasher pass through with
       their full extension metadata intact.
    2. **Starfix hashing** — ``ArrowDigester.hash_table`` produces a 35-byte
       versioned SHA-256 digest that is byte-for-byte identical to the Rust
       ``starfix`` crate output.

    Parameters
    ----------
    type_converter:
        ``UniversalTypeConverter`` used to resolve extension types to Python
        types and convert storage values back to Python objects.
    semantic_hasher:
        ``SemanticAwarePythonHasher`` used to hash Python objects extracted
        from extension-typed columns.
    hasher_id:
        String identifier embedded in every ``ContentHash`` produced by this
        hasher.
    """

    def __init__(
        self,
        type_converter: "UniversalTypeConverter",
        semantic_hasher: "SemanticAwarePythonHasher",
        hasher_id: str,
    ) -> None:
        self._type_converter = type_converter
        self._semantic_hasher = semantic_hasher
        self._hasher_id = hasher_id

    @property
    def hasher_id(self) -> str:
        return self._hasher_id

    def _process_table_columns(self, table: "pa.Table | pa.RecordBatch") -> "pa.Table":
        """Replace semantic-typed columns with their content-hash bytes."""
        new_columns: list[pa.Array] = []
        new_fields: list[pa.Field] = []

        for i, field in enumerate(table.schema):
            # Short-circuit: columns that cannot contain semantic types skip
            # the costly Python round-trip. Extension types must pass through
            # so visit_extension can process them.
            if not (
                isinstance(field.type, pa.ExtensionType)
                or pa.types.is_struct(field.type)
                or pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_fixed_size_list(field.type)
                or pa.types.is_map(field.type)
            ):
                new_columns.append(table.column(i))
                new_fields.append(field)
                continue

            column_data = table.column(i).to_pylist()
            visitor = SemanticHashingVisitor(self._type_converter, self._semantic_hasher)

            try:
                new_type: pa.DataType | None = None
                processed_data: list[Any] = []
                for value in column_data:
                    processed_type, processed_value = visitor.visit(field.type, value)
                    if new_type is None and processed_value is not None:
                        new_type = processed_type
                    processed_data.append(processed_value)

                if new_type is None:
                    new_type = field.type
                new_columns.append(pa.array(processed_data, type=new_type))
                new_fields.append(field.with_type(new_type))

            except Exception as exc:
                raise RuntimeError(
                    f"Failed to process column '{field.name}': {exc}"
                ) from exc

        return pa.table(
            new_columns,
            schema=pa.schema(new_fields, metadata=table.schema.metadata),
        )

    def hash_schema(self, schema: "pa.Schema") -> ContentHash:
        """Hash an Arrow schema using the starfix canonical algorithm."""
        include_meta = has_extension_metadata(schema)
        if include_meta:
            schema = clean_schema_for_hashing(schema)
        digest = ArrowDigester.hash_schema(schema, include_metadata=include_meta)
        return ContentHash(method=self._hasher_id, digest=digest)

    def hash_table(self, table: "pa.Table | pa.RecordBatch") -> ContentHash:
        """Hash an Arrow table (or ``RecordBatch``) using starfix."""
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])

        processed_table = self._process_table_columns(table)
        include_meta = has_extension_metadata(processed_table.schema)
        if include_meta:
            clean_schema = clean_schema_for_hashing(processed_table.schema)
            clean_table = pa.Table.from_arrays(
                processed_table.columns, schema=clean_schema
            )
        else:
            clean_table = processed_table
        digest = ArrowDigester.hash_table(clean_table, include_metadata=include_meta)
        return ContentHash(method=self._hasher_id, digest=digest)
```

- [ ] **Step 2: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add src/orcapod/hashing/arrow_hashers.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "refactor(arrow_hashers): update StarfixArrowHasher for extension types, delete SemanticArrowHasher"
```

---

## Task 10: Update `test_starfix_arrow_hasher.py`, run tests

**Files:**
- Modify: `tests/test_hashing/test_starfix_arrow_hasher.py`

- [ ] **Step 1: Update `_make_hasher()` and remove `SemanticTypeRegistry` import**

```python
# Remove this import:
# from orcapod.semantic_types import SemanticTypeRegistry

# Update _make_hasher():
def _make_hasher() -> StarfixArrowHasher:
    from orcapod.contexts import get_default_context
    ctx = get_default_context()
    return StarfixArrowHasher(
        type_converter=ctx.type_converter,
        semantic_hasher=ctx.semantic_hasher,
        hasher_id=HASHER_ID,
    )
```

- [ ] **Step 2: Run the hashing test suite**

```bash
uv run --project /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  pytest tests/test_hashing/ -x -v
```

Expected: all tests pass (golden digests unchanged for plain-schema tables; extension type tests pass).

- [ ] **Step 3: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add tests/test_hashing/test_starfix_arrow_hasher.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "test(starfix_arrow_hasher): update _make_hasher() for new constructor, remove SemanticTypeRegistry import"
```

---

## Task 11: Update `v0.1.json`, `context_schema.json`, and `versioned_hashers.py`

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`
- Modify: `src/orcapod/contexts/data/schemas/context_schema.json`
- Modify: `src/orcapod/hashing/versioned_hashers.py`

- [ ] **Step 1: Rewrite `v0.1.json`**

Key design note: `arrow_hasher` now depends on `semantic_hasher`, and `semantic_hasher` depends on `python_type_semantic_hasher_registry`. To avoid a circular dependency, the `pa.Table`/`pa.RecordBatch` handler entries are **removed** from the registry's handlers list (those entries previously referenced `arrow_hasher`). The JSON construction order is: `file_hasher` → `type_converter` → `function_info_extractor` → `python_type_semantic_hasher_registry` → `semantic_hasher` → `arrow_hasher`.

```json
{
    "context_key": "std:v0.1:default",
    "version": "v0.1",
    "description": "Initial stable release with extension type hashing support",
    "file_hasher": {
        "_class": "orcapod.hashing.file_hashers.BasicFileHasher",
        "_config": {
            "algorithm": "sha256"
        }
    },
    "type_converter": {
        "_class": "orcapod.semantic_types.universal_converter.UniversalTypeConverter",
        "_config": {
            "logical_type_registry": {
                "_class": "orcapod.extension_types.registry.LogicalTypeRegistry",
                "_config": {
                    "logical_types": [
                        {
                            "_class": "orcapod.extension_types.builtin_logical_types.LogicalPath",
                            "_config": {}
                        },
                        {
                            "_class": "orcapod.extension_types.builtin_logical_types.LogicalUPath",
                            "_config": {}
                        },
                        {
                            "_class": "orcapod.extension_types.builtin_logical_types.LogicalUUID",
                            "_config": {}
                        }
                    ],
                    "factories": [
                        {
                            "factory": {
                                "_class": "orcapod.extension_types.dataclass_logical_type_factory.DataclassLogicalTypeFactory",
                                "_config": {}
                            },
                            "category": "orcapod.dataclass",
                            "python_bases": [{"_type": "builtins.object"}]
                        },
                        {
                            "factory": {
                                "_class": "orcapod.extension_types.pydantic_logical_type_factory.PydanticLogicalTypeFactory",
                                "_config": {}
                            },
                            "category": "orcapod.pydantic",
                            "python_bases": [{"_type": "pydantic.BaseModel"}]
                        }
                    ]
                }
            }
        }
    },
    "function_info_extractor": {
        "_class": "orcapod.hashing.semantic_hashing.function_info_extractors.FunctionSignatureExtractor",
        "_config": {
            "include_module": true,
            "include_defaults": true
        }
    },
    "python_type_semantic_hasher_registry": {
        "_class": "orcapod.hashing.semantic_hashing.type_handler_registry.PythonTypeSemanticHasherRegistry",
        "_config": {
            "handlers": [
                [{"_type": "builtins.bytes"},         {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.BytesSemanticHasher",        "_config": {}}],
                [{"_type": "builtins.bytearray"},     {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.BytesSemanticHasher",        "_config": {}}],
                [{"_type": "pathlib.Path"},            {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.PathSemanticHasher",         "_config": {"file_hasher": {"_ref": "file_hasher"}}}],
                [{"_type": "upath.core.UPath"},        {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.UPathSemanticHasher",        "_config": {"file_hasher": {"_ref": "file_hasher"}}}],
                [{"_type": "uuid.UUID"},               {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.UUIDSemanticHasher",         "_config": {}}],
                [{"_type": "types.FunctionType"},      {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.FunctionSemanticHasher",     "_config": {"function_info_extractor": {"_ref": "function_info_extractor"}}}],
                [{"_type": "types.BuiltinFunctionType"},{"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.FunctionSemanticHasher",    "_config": {"function_info_extractor": {"_ref": "function_info_extractor"}}}],
                [{"_type": "types.MethodType"},        {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.FunctionSemanticHasher",     "_config": {"function_info_extractor": {"_ref": "function_info_extractor"}}}],
                [{"_type": "builtins.type"},           {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.TypeObjectSemanticHasher",   "_config": {}}],
                [{"_type": "types.GenericAlias"},      {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.GenericAliasSemanticHasher", "_config": {}}],
                [{"_type": "types.UnionType"},         {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.UnionTypeSemanticHasher",    "_config": {}}],
                [{"_type": "typing._GenericAlias"},    {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.GenericAliasSemanticHasher", "_config": {}}],
                [{"_type": "typing._SpecialForm"},     {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.SpecialFormSemanticHasher",  "_config": {}}]
            ]
        }
    },
    "semantic_hasher": {
        "_class": "orcapod.hashing.semantic_hashing.semantic_hasher.SemanticAwarePythonHasher",
        "_config": {
            "hasher_id": "semantic_v0.1",
            "type_semantic_hasher_registry": {
                "_ref": "python_type_semantic_hasher_registry"
            }
        }
    },
    "arrow_hasher": {
        "_class": "orcapod.hashing.arrow_hashers.StarfixArrowHasher",
        "_config": {
            "hasher_id": "arrow_v0.1",
            "type_converter": {"_ref": "type_converter"},
            "semantic_hasher": {"_ref": "semantic_hasher"}
        }
    },
    "metadata": {
        "created_date": "2026-06-24",
        "author": "OrcaPod Core Team",
        "changelog": [
            "Initial release with Path semantic type support",
            "Basic SHA-256 hashing for files and objects",
            "Arrow logical serialization method",
            "Introduced arrow_v0.1 StarfixArrowHasher using starfix ArrowDigester for cross-language-compatible Arrow hashing",
            "Hard cut: replaced shape-based SemanticTypeRegistry with extension-type hashing; renamed all hashing classes to clearer names"
        ]
    }
}
```

- [ ] **Step 2: Update `context_schema.json`**

Two changes:
1. Remove the `semantic_registry` property from `properties`.
2. Rename `type_handler_registry` → `python_type_semantic_hasher_registry` in `properties`.

```json
"python_type_semantic_hasher_registry": {
    "$ref": "#/$defs/objectspec",
    "description": "ObjectSpec for the PythonTypeSemanticHasherRegistry used by the semantic hasher"
},
```

Also update the `examples` section references and remove the `"semantic_registry"` entry.

- [ ] **Step 3: Update `versioned_hashers.py`**

```python
"""
Versioned hasher factories for OrcaPod.
"""

from __future__ import annotations

import logging
from typing import Any

from orcapod.protocols import hashing_protocols as hp

logger = logging.getLogger(__name__)

_CURRENT_SEMANTIC_HASHER_ID = "semantic_v0.1"
_CURRENT_ARROW_HASHER_ID = "arrow_v0.1"


def get_versioned_semantic_hasher(
    hasher_id: str = _CURRENT_SEMANTIC_HASHER_ID,
    strict: bool = True,
    type_semantic_hasher_registry: "Any | None" = None,
) -> hp.SemanticHasherProtocol:
    """Return a SemanticAwarePythonHasher configured for the current version.

    Parameters
    ----------
    hasher_id:
        Identifier embedded in every ContentHash produced by this hasher.
    strict:
        When True raises TypeError for unhandled types. When False falls back
        to a best-effort string representation.
    type_semantic_hasher_registry:
        Optional ``PythonTypeSemanticHasherRegistry`` to inject. When None the
        global default registry is used.
    """
    from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher

    if type_semantic_hasher_registry is None:
        from orcapod.hashing.semantic_hashing.type_handler_registry import (
            get_default_python_type_semantic_hasher_registry,
        )
        type_semantic_hasher_registry = get_default_python_type_semantic_hasher_registry()

    logger.debug(
        "get_versioned_semantic_hasher: creating SemanticAwarePythonHasher "
        "(hasher_id=%r, strict=%r)",
        hasher_id,
        strict,
    )
    return SemanticAwarePythonHasher(
        hasher_id=hasher_id,
        type_semantic_hasher_registry=type_semantic_hasher_registry,
        strict=strict,
    )


def get_versioned_semantic_arrow_hasher(
    hasher_id: str = _CURRENT_ARROW_HASHER_ID,
) -> hp.ArrowHasherProtocol:
    """Return a StarfixArrowHasher configured for the current version.

    Sources ``type_converter`` and ``semantic_hasher`` from the default
    ``DataContext`` so that the arrow hasher is consistent with all other
    versioned components.

    Parameters
    ----------
    hasher_id:
        Identifier embedded in every ContentHash produced by this hasher.
    """
    from orcapod.hashing.arrow_hashers import StarfixArrowHasher
    from orcapod.contexts import resolve_context

    ctx = resolve_context(None)  # default context
    logger.debug(
        "get_versioned_semantic_arrow_hasher: creating StarfixArrowHasher "
        "(hasher_id=%r)",
        hasher_id,
    )
    return StarfixArrowHasher(
        hasher_id=hasher_id,
        type_converter=ctx.type_converter,
        semantic_hasher=ctx.semantic_hasher,
    )
```

- [ ] **Step 4: Run the full test suite (except test_semantic_types)**

```bash
uv run --project /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  pytest tests/test_hashing/ tests/test_extension_types/ tests/test_core/ -x -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add src/orcapod/contexts/data/v0.1.json \
      src/orcapod/contexts/data/schemas/context_schema.json \
      src/orcapod/hashing/versioned_hashers.py
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "feat(v0.1): wire extension type hashing into default context; remove semantic_registry"
```

---

## Task 12: Delete old semantic type system + grep sweep + final test run

**Files:**
- Delete: `src/orcapod/semantic_types/semantic_struct_converters.py`
- Delete: `src/orcapod/semantic_types/semantic_registry.py`
- Delete: `tests/test_semantic_types/` (all 9 files)
- Delete: `tests/test_hashing/test_file_hashing_consistency.py`
- Modify: `src/orcapod/semantic_types/__init__.py`
- Modify: `src/orcapod/protocols/semantic_types_protocols.py`

- [ ] **Step 1: Delete old source files**

```bash
rm /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python/src/orcapod/semantic_types/semantic_struct_converters.py
rm /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python/src/orcapod/semantic_types/semantic_registry.py
```

- [ ] **Step 2: Update `semantic_types/__init__.py`** — remove `SemanticTypeRegistry` export

```python
from .universal_converter import UniversalTypeConverter
from .type_inference import infer_python_schema_from_pylist_data

__all__ = [
    "UniversalTypeConverter",
    "infer_python_schema_from_pylist_data",
]
```

- [ ] **Step 3: Remove `SemanticStructConverterProtocol` from `semantic_types_protocols.py`**

Delete the `SemanticStructConverterProtocol` class and any imports that only support it. Keep `TypeConverterProtocol` and all other classes.

- [ ] **Step 4: Delete old test files**

```bash
rm /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python/tests/test_hashing/test_file_hashing_consistency.py
rm -r /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python/tests/test_semantic_types/
```

- [ ] **Step 5: Grep sweep for stale references**

```bash
grep -rn \
  "SemanticTypeRegistry\|semantic_registry\|SemanticStructConverter\
\|BaseSemanticHasher\|TypeHandlerRegistry\|BuiltinTypeHandlerRegistry\
\|TypeHandlerProtocol\|PathContentHandler\|UPathContentHandler\
\|UUIDHandler\|BytesHandler\|FunctionHandler\|TypeObjectHandler\
\|SpecialFormHandler\|GenericAliasHandler\|UnionTypeHandler\|ArrowTableHandler\
\|SchemaHandler\|register_builtin_handlers\|get_default_type_handler_registry\
\|type_handler_registry\|get_handler\b\|has_handler\b\|SemanticArrowHasher" \
  /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python/src/ \
  /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python/tests/ \
  2>/dev/null
```

Expected: zero matches (fix any that appear before continuing).

- [ ] **Step 6: Run full test suite**

```bash
uv run --project /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  pytest tests/test_hashing/ tests/test_extension_types/ tests/test_core/ -x -v
```

Expected: all tests pass.

- [ ] **Step 7: Final commit**

```bash
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  add -u
git -C /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python \
  commit -m "feat(PLT-1660): hard cut — delete SemanticTypeRegistry and old struct-based hashing system"
```

---

## Self-Review

**Spec coverage:**
- ✅ §1 `visit_extension` added to `ArrowTypeDataVisitor`, `visit()` updated (Task 8)
- ✅ §2 `SemanticHashingVisitor` rewritten with binary encoding (Task 8)
- ✅ §3 `StarfixArrowHasher` constructor updated + short-circuit + `SemanticArrowHasher` deleted (Task 9)
- ✅ §4 `SemanticArrowHasher` deleted (Task 9)
- ✅ §5 All class/method renames applied (Tasks 1–6)
- ✅ §6 Protocol tightened: `hash() -> ContentHash` (Tasks 1, 3, 4)
- ✅ §7 `v0.1.json` updated (Task 11) — note: `pa.Table`/`pa.RecordBatch` handlers removed to break circular dep
- ✅ §8 `context_schema.json` updated (Task 11)
- ✅ §9 `DataContext.core` docstring updated (Task 5)
- ✅ §10 `versioned_hashers.py` sources from context (Task 11)
- ✅ Files to delete: all covered (Task 12)
- ✅ Files to update: covered across Tasks 1–11

**Circular dependency note (§7 deviation):** The spec says to add `"semantic_hasher": {"_ref": "semantic_hasher"}` to `arrow_hasher._config`. This is correct and implemented. However, to avoid a construction-order cycle (`arrow_hasher` → `semantic_hasher` → `registry` → `arrow_hasher` via `ArrowTableSemanticHasher`), the `pa.Table` and `pa.RecordBatch` handler entries are removed from the `python_type_semantic_hasher_registry` handlers list in `v0.1.json`. These handlers depended on `arrow_hasher` creating the cycle. The `register_builtin_python_type_semantic_hashers()` function still supports them when `arrow_hasher` is passed explicitly (e.g., for custom registry construction in tests).

**Type consistency check:**
- `SemanticAwarePythonHasher.__init__` takes `type_semantic_hasher_registry` → `v0.1.json` uses key `type_semantic_hasher_registry` ✅
- `SemanticHashingVisitor.__init__` takes `type_converter, python_hasher` → `_process_table_columns` passes `self._type_converter, self._semantic_hasher` ✅
- `StarfixArrowHasher.__init__` takes `type_converter, semantic_hasher, hasher_id` → `versioned_hashers.py` passes these by keyword ✅
- `PythonTypeSemanticHasherRegistry.get_semantic_hasher(obj)` → `SemanticAwarePythonHasher.hash_object()` calls this ✅
- `PythonTypeSemanticHasherRegistry.has_semantic_hasher(target_type)` → `SemanticHashingVisitor.visit_extension()` calls this ✅
