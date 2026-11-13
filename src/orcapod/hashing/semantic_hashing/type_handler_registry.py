"""
PythonTypeHandlerRegistry — MRO-aware registry for PythonTypeHandlerProtocol instances.

``PythonTypeHandlerProtocol`` is the protocol for type-specific handlers; this registry
provides MRO-aware lookup so subclasses inherit their parent's handler.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from orcapod.logical_types.provisional import warn_provisional
from orcapod.utils.deferred_types import load_object, qualified_name

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        PythonTypeHandlerProtocol,
    )

logger = logging.getLogger(__name__)


class PythonTypeHandlerRegistry:
    """Registry mapping Python types to PythonTypeHandlerProtocol instances.

    Lookup is MRO-aware: when no hasher is registered for the exact type of
    an object, the registry walks the object's MRO (most-derived first) until
    it finds a match.

    Thread safety
    -------------
    Registration and lookup are protected by a reentrant lock so that the
    global singleton can be safely used from multiple threads.
    """

    def __init__(
        self, handlers: list[tuple[type, "PythonTypeHandlerProtocol"]] | None = None
    ) -> None:
        """
        Args:
            handlers: Optional list of ``(target_type, hasher)`` pairs to
                register at construction time.
        """
        # Handlers loaded on first use, by qualified class name (``register_deferred``).
        self._deferred: dict[str, dict[str, Any]] = {}
        self._handlers: dict[type, "PythonTypeHandlerProtocol"] = {}
        self._lock = threading.RLock()
        # Extension names of the types whose value hash is provisional (ITL-1068).
        self._provisional: frozenset[str] = frozenset()
        if handlers:
            for entry in handlers:
                # Skip empty or incomplete pairs.  When a handler pair in the context
                # JSON carries "_optional": true on its elements and the backing module
                # is absent, parse_objectspec filters those elements out of the inner
                # list, leaving an empty list [] here.  Also skip pairs where either
                # element resolved to None.
                if entry and len(entry) == 2 and all(x is not None for x in entry):
                    target_type, handler = entry
                    self.register(target_type, handler)

    def register_deferred(
        self, python_type: str, handler: str, config: Mapping[str, Any] | None = None
    ) -> None:
        """Register a handler that is loaded on first use (ITL-871).

        Neither the type nor the handler is imported until a lookup meets a
        class whose MRO contains *python_type*. A deferred handler already
        named for *python_type* is replaced. See ``orcapod.utils.deferred_types``.

        Args:
            python_type: Qualified class name, for example ``"torch.Tensor"``.
            handler: Dotted path of the handler class.
            config: Keyword arguments the handler class is instantiated with
                (for example the ignore list of the value hash); none by default.
        """
        with self._lock:
            self._deferred[python_type] = {
                "python_type": python_type,
                "handler": handler,
                "config": dict(config or {}),
            }

    def register(self, target_type: type, handler: "PythonTypeHandlerProtocol") -> None:
        """Register a hasher for a specific Python type.

        If a hasher is already registered for *target_type*, it is silently
        replaced by the new hasher.

        Args:
            target_type: The Python type (or class) for which the hasher should be used.
            handler: A ``PythonTypeHandlerProtocol`` instance.

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
                    "PythonTypeHandlerRegistry: replacing existing hasher for %s (%s -> %s)",
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

    def declare_provisional(self, extension_names: Iterable[str]) -> None:
        """Declare the types whose value hash is provisional (ITL-1068).

        A handler whose ``EXTENSION_NAME`` is declared issues
        ``ProvisionalHashWarning`` the first time in the process that ``get_handler``
        returns it for a value. The data context loader passes the ``provisional``
        list of ``contexts/logical_types.json``; the declaration never enters a hash.

        Args:
            extension_names: The extension names declared provisional. They replace
                any earlier declaration.
        """
        with self._lock:
            self._provisional = frozenset(extension_names)

    def get_handler(self, obj: Any) -> "PythonTypeHandlerProtocol | None":
        """Look up the handler for *obj* using MRO-aware resolution.

        A handler of a type declared provisional (``declare_provisional``) issues
        ``ProvisionalHashWarning`` on its first use in the process.

        Args:
            obj: The object for which a handler is needed.

        Returns:
            The registered ``PythonTypeHandlerProtocol``, or None.
        """
        handler = self.get_handler_for_type(type(obj))
        extension_name = getattr(handler, "EXTENSION_NAME", None)
        if extension_name is not None and extension_name in self._provisional:
            warn_provisional(extension_name)
        return handler

    def get_handler_for_type(
        self, target_type: type
    ) -> "PythonTypeHandlerProtocol | None":
        """Look up the handler for a *type object* (rather than an instance).

        Walks ``target_type.__mro__`` once, most-derived class first. For each
        class a registered handler is taken first, then a deferred handler named
        for that class (``register_deferred``), which is loaded and registered. So
        the handler of the most-derived class wins, whether it is registered or
        deferred. Comparing qualified names imports nothing.

        Args:
            target_type: The type to look up.

        Returns:
            The registered ``PythonTypeHandlerProtocol``, or None.
        """
        with self._lock:
            for base in getattr(target_type, "__mro__", (target_type,)):
                handler = self._handlers.get(base)
                if handler is not None:
                    return handler
                entry = self._deferred.get(qualified_name(base))
                if entry is not None:
                    return self._load_deferred(entry)
            return None

    def _load_deferred(self, entry: dict[str, Any]) -> "PythonTypeHandlerProtocol":
        """Load and register a deferred handler. Callers hold ``self._lock``.

        Args:
            entry: The deferred entry (``register_deferred``).

        Returns:
            The newly registered handler.
        """
        handler = load_object(entry["handler"])(**entry["config"])
        self.register(load_object(entry["python_type"]), handler)
        del self._deferred[entry["python_type"]]
        return handler

    def has_handler(self, target_type: type) -> bool:
        """Return True if a handler is registered for *target_type* or any MRO ancestor.

        Args:
            target_type: The type to check.
        """
        return self.get_handler_for_type(target_type) is not None

    def registered_types(self) -> list[type]:
        """Return a list of all directly-registered types (no MRO expansion)."""
        with self._lock:
            return list(self._handlers.keys())

    def __repr__(self) -> str:
        with self._lock:
            names = [t.__name__ for t in self._handlers]
        return f"PythonTypeHandlerRegistry(registered={names!r})"

    def __len__(self) -> int:
        with self._lock:
            return len(self._handlers)


def get_default_python_type_handler_registry() -> "PythonTypeHandlerRegistry":
    """Return the PythonTypeHandlerRegistry from the default data context.

    This is a convenience wrapper; the registry is owned and versioned by the
    active ``DataContext``. Importing this function from
    ``orcapod.hashing.defaults`` or ``orcapod.hashing`` is equivalent.
    """
    from orcapod.hashing.defaults import (
        get_default_python_type_handler_registry as _get,
    )

    return _get()


class BuiltinPythonTypeHandlerRegistry(PythonTypeHandlerRegistry):
    """A PythonTypeHandlerRegistry pre-populated with all built-in hashers.

    Constructed via the data context JSON spec so that the default registry
    is versioned alongside the rest of the context components.
    """

    def __init__(self, arrow_hasher: "ArrowHasherProtocol | None" = None) -> None:
        super().__init__()
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            register_builtin_python_type_handlers,
        )

        register_builtin_python_type_handlers(self, arrow_hasher=arrow_hasher)
