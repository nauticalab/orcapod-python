"""
ContentIdentifiableMixin -- convenience base class for content-identifiable objects.

Any class that implements ``identity_structure()`` can inherit from this mixin
to gain a full suite of content-based identity helpers without having to wire
up a BaseSemanticHasher manually:

  - ``content_hash()``  -- returns a stable ContentHash for the object
  - ``__hash__()``      -- Python hash based on content (int)
  - ``__eq__()``        -- equality via content_hash comparison

The mixin uses the global default BaseSemanticHasher by default, but accepts an
injected hasher for testing or custom configurations.

Usage
-----
Simple usage with the global default hasher::

    class MyRecord(ContentIdentifiableMixin):
        def __init__(self, name: str, value: int) -> None:
            self.name = name
            self.value = value

        def identity_structure(self):
            return {"name": self.name, "value": self.value}

    r1 = MyRecord("foo", 42)
    r2 = MyRecord("foo", 42)
    assert r1 == r2
    assert hash(r1) == hash(r2)
    print(r1.content_hash())  # ContentHash(method='object_v0.1', digest=...)

With an injected hasher (e.g. in tests)::

    hasher = BaseSemanticHasher(hasher_id="test", strict=True)
    record = MyRecord("foo", 42)
    record._semantic_hasher = hasher
    print(record.content_hash())

Design notes
------------
- The mixin stores a lazily-computed ``_cached_content_hash`` to avoid
  recomputing the hash on every call.  The cache is invalidated by calling
  ``_invalidate_content_hash_cache()``, which subclasses should call whenever
  a mutation changes the semantic content of the object.

- ``__eq__`` compares ContentHash objects (not identity structures directly)
  for efficiency: if two objects have the same hash they are considered equal.
  This is a deliberate trade-off -- hash collisions are astronomically rare
  for SHA-256.

- The mixin deliberately does *not* inherit from ABC or impose any abstract
  method requirements.  ``identity_structure()`` is expected to be present on
  the concrete class; if it is missing a clear AttributeError will surface at
  call time.

- When used alongside other base classes in a multiple-inheritance chain,
  ensure that ``ContentIdentifiableMixin.__init__`` is cooperative (it calls
  ``super().__init__(**kwargs)``).  Pass ``semantic_hasher=`` as a keyword
  argument if needed.
"""

from __future__ import annotations

import logging
from typing import Any

from orcapod.hashing.semantic_hashing.semantic_hasher import BaseSemanticHasher
from orcapod.types import ContentHash

logger = logging.getLogger(__name__)


class ContentIdentifiableMixin:
    """
    Mixin that provides content-based identity to any class implementing
    ``identity_structure()``.

    Subclasses must implement::

        def identity_structure(self) -> Any:
            ...

    The returned structure is recursively resolved and hashed by the
    BaseSemanticHasher to produce a stable ContentHash.

    Parameters (passed as keyword arguments to ``__init__``)
    ---------------------------------------------------------
    semantic_hasher:
        Optional BaseSemanticHasher instance to use.  When omitted, the hasher
        is obtained from the default data context via
        ``orcapod.contexts.get_default_context().object_hasher``, which is
        the single source of truth for versioned component configuration.
    """

    def __init__(
        self, *, semantic_hasher: "BaseSemanticHasher | None" = None, **kwargs: Any
    ) -> None:
        # Cooperative MRO-friendly init -- forward remaining kwargs up the chain.
        super().__init__(**kwargs)
        # Store injected hasher (may be None; resolved lazily on first use).
        self._semantic_hasher: BaseSemanticHasher | None = semantic_hasher
        # Lazily populated content hash cache.
        self._cached_content_hash: ContentHash | None = None

    # ------------------------------------------------------------------
    # Core content-hash API
    # ------------------------------------------------------------------

    def content_hash(self) -> ContentHash:
        """
        Return a stable ContentHash based on the object's semantic content.

        The hash is computed once and cached.  To force recomputation (e.g.
        after a mutation), call ``_invalidate_content_hash_cache()`` first.

        Returns:
            ContentHash: Deterministic, content-based hash of this object.
        """
        if self._cached_content_hash is None:
            hasher = self._get_hasher()
            structure = self.identity_structure()  # type: ignore[attr-defined]
            logger.debug(
                "ContentIdentifiableMixin.content_hash: computing hash for %s",
                type(self).__name__,
            )
            self._cached_content_hash = hasher.hash_object(structure)
        return self._cached_content_hash

    def identity_structure(self) -> Any:
        """
        Return a structure representing the semantic identity of this object.

        Subclasses MUST override this method.  The default implementation
        raises NotImplementedError to make the missing override visible
        immediately rather than silently producing a meaningless hash.

        Returns:
            Any: A deterministic Python structure whose content fully captures
                 the semantic identity of this object.

        Raises:
            NotImplementedError: Always, unless overridden by a subclass.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement identity_structure() to use "
            "ContentIdentifiableMixin.  Override this method and return a "
            "deterministic Python structure representing the object's semantic "
            "content."
        )

    # ------------------------------------------------------------------
    # Python data model integration
    # ------------------------------------------------------------------

    def __hash__(self) -> int:
        """
        Return a Python integer hash derived from the content hash.

        Uses the first 16 hex characters (64 bits) of the SHA-256 digest
        converted to an integer.  This provides a good distribution while
        fitting within Python's hash range on all platforms.

        Returns:
            int: A stable, content-based hash integer.
        """
        return self.content_hash().to_int(hexdigits=16)

    def __eq__(self, other: object) -> bool:
        """
        Compare this object to *other* based on content hash equality.

        Two ContentIdentifiable objects are considered equal if and only if
        their content hashes are identical.  Objects of a different type that
        do not inherit ContentIdentifiableMixin are never equal to a mixin
        instance (returns NotImplemented to allow the other object to decide).

        Args:
            other: The object to compare against.

        Returns:
            bool: True if both objects have the same content hash.
            NotImplemented: If *other* does not implement content_hash().
        """
        if not isinstance(other, ContentIdentifiableMixin):
            return NotImplemented
        return self.content_hash() == other.content_hash()

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def _invalidate_content_hash_cache(self) -> None:
        """
        Invalidate the cached content hash.

        Call this after any mutation that changes the object's semantic
        content so that the next call to ``content_hash()`` recomputes from
        scratch.
        """
        self._cached_content_hash = None

    # ------------------------------------------------------------------
    # Hasher resolution
    # ------------------------------------------------------------------

    def _get_hasher(self) -> BaseSemanticHasher:
        """
        Return the BaseSemanticHasher to use for this object.

        Resolution order:
          1. The instance-level ``_semantic_hasher`` attribute (set at
             construction or injected directly).
          2. The object hasher from the default data context, obtained via
             ``orcapod.contexts.get_default_context().object_hasher``.
             The data context is the single source of truth for versioned
             component configuration; going through it ensures that the
             hasher is consistent with all other components (arrow hasher,
             type converter, etc.) that belong to the same context.

        Returns:
            BaseSemanticHasher: The hasher to use.
        """
        if self._semantic_hasher is not None:
            return self._semantic_hasher

        # Late import to avoid circular dependencies: contexts imports from
        # protocols and hashing, so we must not import it at module level.
        from orcapod.contexts import get_default_context

        return get_default_context().semantic_hasher  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Repr helper
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        """
        Return a human-readable representation including the short content hash.

        Uses only 8 hex characters to keep the repr concise.  Subclasses are
        encouraged to override this if they need a more informative repr.
        """
        try:
            short_hash = self.content_hash().to_hex(char_count=8)
        except Exception:
            short_hash = "<hash-unavailable>"
        return f"{type(self).__name__}(content_hash={short_hash!r})"
