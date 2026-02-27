from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.core.sources.base import RootSource

logger = logging.getLogger(__name__)


class SourceRegistry:
    """
    Registry mapping canonical source IDs to live ``RootSource`` objects.

    A source ID is a stable, human-readable name (e.g. ``"delta_table:sales"``)
    that is independent of physical location.  The registry lets downstream
    code resolve a ``source_id`` token embedded in a provenance string back to
    the concrete source object that produced it, enabling ``resolve_field``
    calls without requiring a direct reference to the source object.

    Registration behaviour
    ----------------------
    - Registering the **same object** under the same ID is idempotent.
    - Registering a **different object** under an already-taken ID logs a
      warning and skips (rather than raising), so that sources constructed in
      different contexts don't crash each other via the global singleton.
    - Use ``replace`` when you explicitly want to overwrite an entry.

    The module-level ``GLOBAL_SOURCE_REGISTRY`` is the default registry used
    when no explicit registry is provided.
    """

    def __init__(self) -> None:
        self._sources: dict[str, "RootSource"] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(self, source_id: str, source: "RootSource") -> None:
        """
        Register *source* under *source_id*.

        If *source_id* is already taken by the same object, the call is a
        no-op.  If it is taken by a *different* object, a warning is emitted
        and the existing entry is left unchanged.
        """
        if not source_id:
            raise ValueError("source_id cannot be empty")
        if source is None:
            raise ValueError("source cannot be None")

        existing = self._sources.get(source_id)
        if existing is not None:
            if existing is source:
                logger.debug(
                    "Source '%s' already registered with the same object — skipping.",
                    source_id,
                )
                return
            logger.warning(
                "Source ID '%s' is already registered with a different %s object; "
                "keeping the existing registration.  Use replace() to overwrite.",
                source_id,
                type(existing).__name__,
            )
            return

        self._sources[source_id] = source
        logger.debug("Registered source '%s' -> %s", source_id, type(source).__name__)

    def replace(self, source_id: str, source: "RootSource") -> "RootSource | None":
        """
        Register *source* under *source_id*, unconditionally replacing any
        existing entry.  Returns the previous source if one existed.
        """
        if not source_id:
            raise ValueError("source_id cannot be empty")
        old = self._sources.get(source_id)
        self._sources[source_id] = source
        if old is not None and old is not source:
            logger.info(
                "Replaced source '%s': %s -> %s",
                source_id,
                type(old).__name__,
                type(source).__name__,
            )
        return old

    def unregister(self, source_id: str) -> "RootSource":
        """Remove and return the source registered under *source_id*."""
        if source_id not in self._sources:
            raise KeyError(f"No source registered under '{source_id}'")
        source = self._sources.pop(source_id)
        logger.debug("Unregistered source '%s'", source_id)
        return source

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get(self, source_id: str) -> "RootSource":
        """Return the source for *source_id*, raising ``KeyError`` if absent."""
        if source_id not in self._sources:
            raise KeyError(
                f"No source registered under '{source_id}'. "
                f"Available: {list(self._sources)}"
            )
        return self._sources[source_id]

    def get_optional(self, source_id: str) -> "RootSource | None":
        """Return the source for *source_id*, or ``None`` if not registered."""
        return self._sources.get(source_id)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def list_ids(self) -> list[str]:
        return list(self._sources)

    def clear(self) -> None:
        count = len(self._sources)
        self._sources.clear()
        logger.debug("Cleared %d source(s) from registry", count)

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __contains__(self, source_id: Any) -> bool:
        return source_id in self._sources

    def __len__(self) -> int:
        return len(self._sources)

    def __iter__(self) -> Iterator[str]:
        return iter(self._sources)

    def items(self) -> Iterator[tuple[str, "RootSource"]]:
        yield from self._sources.items()

    def __repr__(self) -> str:
        return f"SourceRegistry({len(self._sources)} source(s): {list(self._sources)})"


# Module-level global singleton — used as the default when no explicit
# registry is passed to DataContextMixin or RootSource.
GLOBAL_SOURCE_REGISTRY: SourceRegistry = SourceRegistry()
