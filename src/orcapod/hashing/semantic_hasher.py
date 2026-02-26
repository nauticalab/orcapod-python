"""
BaseSemanticHasher -- content-based recursive object hasher.

Algorithm
---------
``hash_object(obj)`` is the single public entry point.  It is mutually
recursive with ``_expand_structure``:

``hash_object(obj)``
  Produces a ContentHash for *any* Python object.

  - ContentHash        → terminal; returned as-is (already a hash)
  - Primitive          → JSON-serialise + SHA-256
  - Structure          → delegate to ``_expand_structure``, then
                         JSON-serialise the resulting tagged tree + SHA-256
  - Handler match      → call handler.handle(obj), recurse via hash_object
  - ContentIdentifiable→ call identity_structure(), recurse via hash_object
  - Fallback           → strict error or best-effort string, then hash

``_expand_structure(obj)``
  Structural expansion only -- called exclusively for container types
  (list, tuple, dict, set, frozenset, namedtuple).  Returns a
  JSON-serialisable tagged tree where:

  - Primitive elements  → passed through as-is (become leaves in the tree)
  - Nested structures   → recurse via ``_expand_structure``
  - Everything else     → call ``hash_object``, embed the resulting
                          ContentHash.to_string() as a string leaf

The boundary between the two functions encodes a key semantic distinction:
a ContentIdentifiable object X whose identity_structure returns [A, B]
embedded inside [X, C] contributes only its hash token to the parent --
it is NOT the same as [[A, B], C].  The parent's structure is opaque to
the expansion that produced X's hash.

Container type tagging
----------------------
Lists, tuples, dicts, sets, and namedtuples are represented as tagged
JSON objects so that structurally similar but type-distinct containers
produce different hashes:

  list       → {"__type__": "list",      "items": [...]}
  tuple      → {"__type__": "tuple",     "items": [...]}
  set        → {"__type__": "set",       "items": [...]}   # sorted by hash str
  dict       → {"__type__": "dict",      "items": {...}}   # sorted by key str
  namedtuple → {"__type__": "namedtuple","name": "T",
                "fields": {...}}                            # sorted by field name

Circular-reference detection
-----------------------------
Container ids are tracked in a ``_visited`` frozenset threaded through
``_expand_structure``.  When an already-visited id is encountered the
sentinel string ``"CircularRef"`` is embedded as the leaf value.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Mapping
from typing import Any

from orcapod.hashing.type_handler_registry import TypeHandlerRegistry
from orcapod.protocols import hashing_protocols as hp
from orcapod.types import ContentHash

logger = logging.getLogger(__name__)

_CIRCULAR_REF_SENTINEL = "CircularRef"
_MEMADDR_RE = re.compile(r" at 0x[0-9a-fA-F]+")


class BaseSemanticHasher:
    """
    Content-based recursive hasher.

    Parameters
    ----------
    hasher_id:
        A short string identifying this hasher version/configuration.
        Embedded in every ContentHash produced.
    type_handler_registry:
        TypeHandlerRegistry for MRO-aware lookup of TypeHandler instances.
        If None, the default registry from the active DataContext is used.
    strict:
        When True (default) raises TypeError for unhandled types.
        When False falls back to a best-effort string representation.
    """

    def __init__(
        self,
        hasher_id: str,
        type_handler_registry: TypeHandlerRegistry | None = None,
        strict: bool = True,
    ) -> None:
        self._hasher_id = hasher_id
        self._strict = strict

        if type_handler_registry is None:
            from orcapod.hashing.defaults import get_default_type_handler_registry

            self._registry = get_default_type_handler_registry()
        else:
            self._registry = type_handler_registry

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def hasher_id(self) -> str:
        return self._hasher_id

    @property
    def strict(self) -> bool:
        return self._strict

    def hash_object(
        self, obj: Any, process_identity_structure: bool = False
    ) -> ContentHash:
        """
        Hash *obj* based on its semantic content.

        This is the single recursive entry point for the hashing system.
        Returns a ContentHash for any Python object.

        - ContentHash        → terminal; returned as-is
        - Primitive          → JSON-serialised and hashed directly
        - Structure          → structurally expanded then hashed
        - Handler match      → handler produces a value, recurse
        - ContentIdentifiable→ identity_structure() produces a value, recurse
        - Unknown type       → TypeError in strict mode; best-effort otherwise

        Args:
            obj: The object to hash.
            process_identity_structure: If False(default), when hashing ContentIdentifiable object, its content_hash method is invoked.
                                        If True, ContentIdentifiable is hashed by hashing the identity_structure

        Returns:
            ContentHash: Stable, content-based hash of the object.
        """
        # Terminal: already a hash -- return as-is.
        if isinstance(obj, ContentHash):
            return obj

        # Primitives: hash their direct JSON representation.
        if isinstance(obj, (type(None), bool, int, float, str)):
            return self._hash_to_content_hash(obj)

        # Structures: expand into a tagged tree, then hash the tree.
        if _is_structure(obj):
            expanded = self._expand_structure(obj, _visited=frozenset())
            return self._hash_to_content_hash(expanded)

        # Handler dispatch: the handler produces a new value; recurse.
        handler = self._registry.get_handler(obj)
        if handler is not None:
            logger.debug(
                "hash_object: dispatching %s to handler %s",
                type(obj).__name__,
                type(handler).__name__,
            )
            return self.hash_object(handler.handle(obj, self))

        # ContentIdentifiable: expand via identity_structure(); recurse.
        if isinstance(obj, hp.ContentIdentifiable):
            if process_identity_structure:
                logger.debug(
                    "hash_object: hashing identity structure of ContentIdentifiable %s",
                    type(obj).__name__,
                )
                return self.hash_object(obj.identity_structure())
            else:
                logger.debug(
                    "hash_object: using ContentIdentifiable %s's content_hash",
                    type(obj).__name__,
                )
                return obj.content_hash()

        # Fallback for unhandled types.
        fallback = self._handle_unknown(obj)
        return self._hash_to_content_hash(fallback)

    # ------------------------------------------------------------------
    # Private: structural expansion
    # ------------------------------------------------------------------

    def _expand_structure(
        self,
        obj: Any,
        _visited: frozenset[int],
    ) -> Any:
        """
        Expand a container object into a JSON-serialisable tagged tree.

        Only called for structural types (list, tuple, dict, set, frozenset,
        namedtuple).  Within nested structures this function recurses into
        itself for container elements and calls ``hash_object`` for all
        non-container, non-primitive elements, embedding the resulting
        ContentHash.to_string() as a string leaf.

        Primitives are passed through as-is.

        Args:
            obj:      The object to expand.  Must be a structure or primitive.
            _visited: Set of container ids already on the current traversal
                      path, for circular-reference detection.

        Returns:
            A JSON-serialisable dict (with ``__type__`` tag) for containers,
            or the primitive value itself.
        """
        # Primitives are leaves -- pass through.
        if isinstance(obj, (type(None), bool, int, float, str)):
            return obj

        # ContentHash is a terminal leaf -- embed as its string form.
        if isinstance(obj, ContentHash):
            return obj.to_string()

        # Circular-reference guard for containers.
        obj_id = id(obj)
        if obj_id in _visited:
            logger.debug(
                "_expand_structure: circular reference detected for %s",
                type(obj).__name__,
            )
            return _CIRCULAR_REF_SENTINEL
        _visited = _visited | {obj_id}

        if _is_namedtuple(obj):
            return self._expand_namedtuple(obj, _visited)

        if isinstance(obj, (dict, Mapping)):
            return self._expand_mapping(obj, _visited)

        if isinstance(obj, list):
            return {
                "__type__": "list",
                "items": [self._expand_element(item, _visited) for item in obj],
            }

        if isinstance(obj, tuple):
            return {
                "__type__": "tuple",
                "items": [self._expand_element(item, _visited) for item in obj],
            }

        if isinstance(obj, (set, frozenset)):
            expanded_items = [self._expand_element(item, _visited) for item in obj]
            return {
                "__type__": "set",
                "items": sorted(expanded_items, key=str),
            }

        # Should not be reached if _is_structure() is consistent.
        raise TypeError(f"_expand_structure called on non-structure type {type(obj)!r}")

    def _expand_element(self, obj: Any, _visited: frozenset[int]) -> Any:
        """
        Expand a single element within a structure.

        - Primitives and ContentHash → handled by _expand_structure (leaf)
        - Nested structures → recurse via _expand_structure
        - Everything else → call hash_object, embed to_string() as leaf
        """
        if isinstance(obj, (type(None), bool, int, float, str, ContentHash)):
            return self._expand_structure(obj, _visited)

        if _is_structure(obj):
            return self._expand_structure(obj, _visited)

        # Non-structure, non-primitive: hash independently and embed token.
        return self.hash_object(obj).to_string()

    def _expand_mapping(
        self,
        obj: Mapping,
        _visited: frozenset[int],
    ) -> dict:
        """Expand a dict/Mapping into a tagged, sorted JSON object."""
        items: dict[str, Any] = {}
        for k, v in obj.items():
            str_key = str(self._expand_element(k, _visited))
            items[str_key] = self._expand_element(v, _visited)
        # Sort for determinism regardless of insertion order.
        sorted_items = dict(sorted(items.items()))
        return {"__type__": "dict", "items": sorted_items}

    def _expand_namedtuple(
        self,
        obj: Any,
        _visited: frozenset[int],
    ) -> dict:
        """Expand a namedtuple into a tagged dict preserving field names."""
        fields: tuple[str, ...] = obj._fields
        expanded_fields = {
            field: self._expand_element(getattr(obj, field), _visited)
            for field in fields
        }
        return {
            "__type__": "namedtuple",
            "name": type(obj).__name__,
            "fields": dict(sorted(expanded_fields.items())),
        }

    # ------------------------------------------------------------------
    # Private: hashing
    # ------------------------------------------------------------------

    def _hash_to_content_hash(self, obj: Any) -> ContentHash:
        """
        JSON-serialise *obj* and compute a SHA-256 ContentHash.

        *obj* must already be a JSON-serialisable primitive or tagged tree
        (the output of _expand_structure or a raw primitive).
        """
        try:
            json_bytes = json.dumps(
                obj,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"BaseSemanticHasher: failed to JSON-serialise object of type "
                f"{type(obj).__name__!r}. Ensure all TypeHandlers and "
                "identity_structure() implementations return JSON-serialisable "
                "primitives or structures."
            ) from exc

        digest = hashlib.sha256(json_bytes).digest()
        return ContentHash(self._hasher_id, digest)

    # ------------------------------------------------------------------
    # Private: fallback for unhandled types
    # ------------------------------------------------------------------

    def _handle_unknown(self, obj: Any) -> str:
        """
        Produce a best-effort string for an unregistered, non-ContentIdentifiable
        type.  Raises TypeError in strict mode.
        """
        class_name = type(obj).__name__
        module_name = getattr(type(obj), "__module__", "<unknown>")
        qualified = f"{module_name}.{class_name}"

        if self._strict:
            raise TypeError(
                f"BaseSemanticHasher (strict): no TypeHandler registered for type "
                f"'{qualified}' and it does not implement ContentIdentifiable. "
                "Register a TypeHandler via the TypeHandlerRegistry or implement "
                "identity_structure() on the class."
            )

        logger.warning(
            "SemanticHasher (non-strict): no handler for type '%s'. "
            "Falling back to best-effort string representation.",
            qualified,
        )

        if hasattr(obj, "__dict__"):
            attrs = sorted(
                (k, type(v).__name__)
                for k, v in obj.__dict__.items()
                if not k.startswith("_")
            )
            attr_str = ", ".join(f"{k}={t}" for k, t in attrs[:10])
            return f"{qualified}{{{attr_str}}}"
        else:
            raw = repr(obj)
            if len(raw) > 1000:
                raw = raw[:1000] + "..."
            scrubbed = _MEMADDR_RE.sub(" at 0xMEMADDR", raw)
            return f"{qualified}:{scrubbed}"


# ---------------------------------------------------------------------------
# Helper predicates
# ---------------------------------------------------------------------------


def _is_structure(obj: Any) -> bool:
    """Return True if *obj* is a container type handled by _expand_structure."""
    return isinstance(obj, (list, tuple, dict, set, frozenset, Mapping))


def _is_namedtuple(obj: Any) -> bool:
    """Return True if *obj* is an instance of a namedtuple class."""
    if not isinstance(obj, tuple):
        return False
    obj_type = type(obj)
    fields = getattr(obj_type, "_fields", None)
    if fields is None:
        return False
    return isinstance(fields, tuple) and all(isinstance(f, str) for f in fields)
