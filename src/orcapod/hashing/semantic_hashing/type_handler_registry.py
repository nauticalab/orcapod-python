"""
Type Handler Registry for the SemanticHasher system.

Provides a registry through which TypeHandler implementations can be
registered for specific Python types. Lookup is MRO-aware: if no handler
is registered for an exact type, the registry walks the MRO of the object's
class to find the nearest ancestor for which a handler has been registered.

Usage
-----
# Register a handler for a specific type:
registry = TypeHandlerRegistry()
registry.register(Path, PathContentHandler())

# Or use the global default registry:
from orcapod.hashing.semantic_hashing.type_handler_registry import get_default_type_handler_registry
get_default_type_handler_registry().register(MyType, MyTypeHandler())

# Look up a handler (returns None if not found):
handler = registry.get_handler(some_object)
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import TypeHandler

logger = logging.getLogger(__name__)


class TypeHandlerRegistry:
    """
    Registry mapping Python types to TypeHandler instances.

    Lookup is MRO-aware: when no handler is registered for the exact type of
    an object, the registry walks the object's MRO (most-derived first) until
    it finds a match.  This means a handler registered for a base class is
    automatically inherited by all subclasses, unless a more specific handler
    has been registered for the subclass.

    Thread safety
    -------------
    Registration and lookup are protected by a reentrant lock so that the
    global singleton can be safely used from multiple threads.
    """

    def __init__(self) -> None:
        # Maps type -> handler; insertion order is preserved but lookup uses MRO.
        self._handlers: dict[type, "TypeHandler"] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, target_type: type, handler: "TypeHandler") -> None:
        """
        Register a handler for a specific Python type.

        If a handler is already registered for *target_type*, it is silently
        replaced by the new handler.

        Args:
            target_type: The Python type (or class) for which the handler
                         should be used.  Must be a ``type`` object.
            handler:     A TypeHandler instance whose ``handle()`` method will
                         be called when an object of ``target_type`` (or a
                         subclass with no more specific handler) is encountered
                         during structure resolution.

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
                    "TypeHandlerRegistry: replacing existing handler for %s (%s -> %s)",
                    target_type.__name__,
                    type(existing).__name__,
                    type(handler).__name__,
                )
            self._handlers[target_type] = handler

    def unregister(self, target_type: type) -> bool:
        """
        Remove the handler registered for *target_type*, if any.

        Args:
            target_type: The type whose handler should be removed.

        Returns:
            True if a handler was removed, False if none was registered.
        """
        with self._lock:
            if target_type in self._handlers:
                del self._handlers[target_type]
                return True
            return False

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get_handler(self, obj: Any) -> "TypeHandler | None":
        """
        Look up the handler for *obj* using MRO-aware resolution.

        The MRO of ``type(obj)`` is walked from most-derived to least-derived
        (i.e. the object's own class first, then its bases).  The first
        match found in the registry is returned.

        Args:
            obj: The object for which a handler is needed.

        Returns:
            The registered TypeHandler, or None if no handler is registered
            for the object's type or any of its base classes.
        """
        obj_type = type(obj)
        with self._lock:
            # Fast path: exact type match.
            handler = self._handlers.get(obj_type)
            if handler is not None:
                return handler

            # Slow path: walk the MRO, skipping the type itself (already
            # checked above) and skipping ``object`` as a last resort -- a
            # handler registered for ``object`` would match everything.
            for base in obj_type.__mro__[1:]:
                handler = self._handlers.get(base)
                if handler is not None:
                    logger.debug(
                        "TypeHandlerRegistry: resolved handler for %s via base %s",
                        obj_type.__name__,
                        base.__name__,
                    )
                    return handler

        return None

    def get_handler_for_type(self, target_type: type) -> "TypeHandler | None":
        """
        Look up the handler for a *type object* (rather than an instance).

        Useful when the caller already has the type and wants to check
        registration without constructing a dummy instance.

        Args:
            target_type: The type to look up.

        Returns:
            The registered TypeHandler, or None.
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

    def has_handler(self, target_type: type) -> bool:
        """
        Return True if a handler is registered for *target_type* or any of
        its MRO ancestors.

        Args:
            target_type: The type to check.
        """
        return self.get_handler_for_type(target_type) is not None

    def registered_types(self) -> list[type]:
        """
        Return a list of all directly-registered types (no MRO expansion).

        Returns:
            A snapshot list of types that have explicit handler registrations.
        """
        with self._lock:
            return list(self._handlers.keys())

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        with self._lock:
            names = [t.__name__ for t in self._handlers]
        return f"TypeHandlerRegistry(registered={names!r})"

    def __len__(self) -> int:
        with self._lock:
            return len(self._handlers)


# ---------------------------------------------------------------------------
# Pre-populated registry
# ---------------------------------------------------------------------------


def get_default_type_handler_registry() -> "TypeHandlerRegistry":
    """
    Return the TypeHandlerRegistry from the default data context.

    This is a convenience wrapper; the registry is owned and versioned by the
    active DataContext.  Importing this function from
    ``orcapod.hashing.defaults`` or ``orcapod.hashing`` is equivalent.
    """
    from orcapod.hashing.defaults import (
        get_default_type_handler_registry as _get,
    )  # stays in hashing/

    return _get()


class BuiltinTypeHandlerRegistry(TypeHandlerRegistry):
    """
    A TypeHandlerRegistry pre-populated with all built-in handlers.

    Constructed via the data context JSON spec so that the default registry
    is versioned alongside the rest of the context components.  The built-in
    handlers are registered in ``__init__`` so that no separate population
    step is required after construction.
    """

    def __init__(self) -> None:
        super().__init__()
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            register_builtin_handlers,
        )

        register_builtin_handlers(self)
