"""
SemanticAwarePythonHasher -- content-based recursive object hasher.

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
  - Semantic hasher match → handler.handle(obj, self) returns a representative
                            Python structure (or ContentHash as terminal); the result
                            is fed back into hash_object for final hashing
  - ContentIdentifiableProtocol→ call identity_structure(), recurse via hash_object
  - Fallback           → strict error or best-effort string, then hash

``_expand_structure(obj)``
  Structural expansion only -- called exclusively for container types
  (list, tuple, dict, set, frozenset, namedtuple).  Returns a
  JSON-serialisable value where:

  - Primitive elements  → passed through as-is (become leaves in the tree)
  - Nested structures   → recurse via ``_expand_structure``
  - Everything else     → call ``hash_object``, embed the resulting
                          the tagged label and digest of the ContentHash

The boundary between the two functions encodes a key semantic distinction:
a ContentIdentifiableProtocol object X whose identity_structure returns [A, B]
embedded inside [X, C] contributes only its hash token to the parent --
it is NOT the same as [[A, B], C].  The parent's structure is opaque to
the expansion that produced X's hash.

Container type serialisation
----------------------------
Native JSON container types (list and dict) are kept in their natural JSON
form.  Python-only container types that have no unambiguous JSON equivalent
are wrapped in a ``{"__type__": ..., ...}`` tagged object so that
structurally similar but type-distinct containers produce different hashes:

  list       → [...]                                        # native JSON array
  dict       → {...}                                        # str keys: native JSON object, keys sorted
  dict       → {"__type__": "dict",      "items": [[k, v], ...]}  # any other key; sorted pairs
  tuple      → {"__type__": "tuple",     "items": [...]}
  set        → {"__type__": "set",       "items": [...]}   # items sorted by canonical JSON
  frozenset  → {"__type__": "frozenset", "items": [...]}   # its own type; sorted as set
  namedtuple → {"__type__": "namedtuple","name": "T",
                "fields": [[name, value], ...]}             # declared field order

This means a ``list`` and a ``tuple`` with the same elements will hash
differently (the tuple carries a type tag), while a plain ``list`` and a
plain JSON array embedded anywhere in a structure are indistinguishable --
which is exactly the desired semantics for interoperability.

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
from collections.abc import Callable, Mapping
from typing import Any, cast

from orcapod.hashing.semantic_hashing.encoded import TYPE_KEY, Encoded
from orcapod.protocols import hashing_protocols as hp
from orcapod.types import ContentHash
from orcapod.utils.canonical_type_names import canonical_module_name

logger = logging.getLogger(__name__)

_CIRCULAR_REF_SENTINEL = "CircularRef"

SEMANTIC_HASH_LABEL = "semantic_v1"
"""``ContentHash.method`` of a structure hash: the single definition of the label.

It names the whole procedure of this module: the expansion of a Python structure
into a tagged JSON tree and the SHA-256 digest of its canonical JSON text. A change
that gives an existing structure a different digest takes a new label
(``semantic_v2``). The data context file names the hasher class and does not repeat
the label.
"""


def _nested_hash(content_hash: ContentHash) -> dict[str, str]:
    """Return the form in which a hash enters the structure that contains it.

    A hash fed into another hash carries its label (ITL-1154): the label names the
    algorithm that produced the digest, so two algorithms that happen to produce the
    same digest (a file's ``sha256`` over bytes that are some value's canonical JSON,
    and that value's ``semantic_v1``) never feed the same input into the enclosing
    hash. A label changes only when its algorithm's digests change, so this costs no
    stability. The tag keeps a nested hash apart from a string or a dict that happens
    to equal its fields.

    Args:
        content_hash: The hash of a nested element.

    Returns:
        ``{"__type__": "hash", "label": "<label>", "digest": "<hex digest>"}``.
    """
    return {
        "__type__": "hash",
        "label": content_hash.method,
        "digest": content_hash.to_hex(),
    }


_MEMADDR_RE = re.compile(r" at 0x[0-9a-fA-F]+")


class SemanticAwarePythonHasher:
    """Content-based recursive hasher.

    Args:
        hasher_id: The label (``ContentHash.method``) of every hash produced.
            Defaults to ``SEMANTIC_HASH_LABEL``, the label of this algorithm; pass
            another value only for a hasher that is not the data context's (a test
            hasher).
        type_handler_registry: ``HandlerRegistryProtocol`` for MRO-aware lookup of
            ``PythonTypeHandlerProtocol`` instances. If None, the default registry
            is used.
        strict: When True (default) raises TypeError for unhandled types. When
            False falls back to a best-effort string representation.
    """

    def __init__(
        self,
        hasher_id: str = SEMANTIC_HASH_LABEL,
        type_handler_registry: "hp.HandlerRegistryProtocol | None" = None,
        strict: bool = True,
    ) -> None:
        self._hasher_id = hasher_id
        self._strict = strict

        if type_handler_registry is None:
            from orcapod.hashing.defaults import (
                get_default_python_type_handler_registry,
            )

            self._registry = get_default_python_type_handler_registry()
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

    @property
    def type_handler_registry(self) -> "hp.HandlerRegistryProtocol":
        """Return the ``HandlerRegistryProtocol`` used by this hasher."""
        return self._registry

    def hash_object(
        self,
        obj: Any,
        resolver: Callable[[Any], ContentHash] | None = None,
    ) -> ContentHash:
        """
        Hash *obj* based on its semantic content.

        This is the single recursive entry point for the hashing system.
        Returns a ContentHash for any Python object.

        - ContentHash        → terminal; returned as-is
        - Primitive          → JSON-serialised and hashed directly
        - Structure          → structurally expanded then hashed
        - Semantic hasher match → handler.handle(obj, self) returns a representative Python
          structure (or ContentHash); result is fed back into hash_object for final hashing
        - ContentIdentifiableProtocol→ resolver(obj) if resolver provided, else obj.content_hash()
        - Unknown type       → TypeError in strict mode; best-effort otherwise

        Args:
            obj: The object to hash.
            resolver: Optional callable invoked for any ContentIdentifiableProtocol
                object encountered during hashing.  When provided it overrides the
                default ``obj.content_hash()`` call, allowing the caller to control
                which identity chain is used (e.g. pipeline_hash vs content_hash)
                and to propagate a consistent semantic hasher through the full
                recursive computation.

        Returns:
            ContentHash: Stable, content-based hash of the object.
        """
        # Terminal: already a hash -- return as-is.
        if isinstance(obj, ContentHash):
            return obj

        # Primitives: hash their direct JSON representation.
        if isinstance(obj, (type(None), bool, int, float, str)):
            return self._hash_to_content_hash(obj)

        # Registered handler dispatch (BEFORE _is_structure — ensures SchemaHandler
        # takes priority over _expand_mapping for Schema objects, which are Mappings).
        handler = self._registry.get_handler(obj)
        if handler is not None:
            logger.debug(
                "hash_object: dispatching %s to handler %s",
                type(obj).__name__,
                type(handler).__name__,
            )
            result = handler.handle(obj, self)
            if isinstance(result, Encoded):
                # The only route to a trusted tagged object: a handler's own result.
                return self._hash_to_content_hash(
                    self._expand_encoded(result, resolver=resolver)
                )
            return self.hash_object(result, resolver=resolver)

        # Structures: expand into a tagged tree, then hash the tree.
        if _is_structure(obj):
            expanded = self._expand_structure(
                obj, _visited=frozenset(), resolver=resolver
            )
            return self._hash_to_content_hash(expanded)

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

    # ------------------------------------------------------------------
    # Private: structural expansion
    # ------------------------------------------------------------------

    def _expand_structure(
        self,
        obj: Any,
        _visited: frozenset[int],
        resolver: Callable[[Any], ContentHash] | None = None,
    ) -> Any:
        """
        Expand a container object into a JSON-serialisable tagged tree.

        Only called for structural types (list, tuple, dict, set, frozenset,
        namedtuple).  Within nested structures this function recurses into
        itself for container elements and calls ``hash_object`` for all
        non-container, non-primitive elements, embedding the tagged label and
        digest of the resulting ContentHash.

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

        # ContentHash is a terminal leaf -- embed its label and digest.
        if isinstance(obj, ContentHash):
            return _nested_hash(obj)

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
            return self._expand_namedtuple(obj, _visited, resolver=resolver)

        if isinstance(obj, (dict, Mapping)):
            return self._expand_mapping(obj, _visited, resolver=resolver)

        if isinstance(obj, list):
            return [
                self._expand_element(item, _visited, resolver=resolver) for item in obj
            ]

        if isinstance(obj, tuple):
            return {
                "__type__": "tuple",
                "items": [
                    self._expand_element(item, _visited, resolver=resolver)
                    for item in obj
                ],
            }

        if isinstance(obj, (set, frozenset)):
            expanded_items = [
                self._expand_element(item, _visited, resolver=resolver) for item in obj
            ]
            # Sort by each item's canonical JSON text: a total order that never
            # ties two distinct items ({1, "1"} sorts as [1, "1"]), so the order,
            # and the hash, never depend on set iteration order (PYTHONHASHSEED).
            # A frozenset is a type of its own (ITL-1154): tagged apart from set.
            return {
                "__type__": "frozenset" if isinstance(obj, frozenset) else "set",
                "items": sorted(expanded_items, key=_canonical_json),
            }

        # Should not be reached if _is_structure() is consistent.
        raise TypeError(f"_expand_structure called on non-structure type {type(obj)!r}")

    def _expand_element(
        self,
        obj: Any,
        _visited: frozenset[int],
        resolver: Callable[[Any], ContentHash] | None = None,
    ) -> Any:
        """
        Expand a single element within a structure.

        - Primitives and ContentHash → handled by _expand_structure (leaf)
        - Nested structures → recurse via _expand_structure
        - Everything else → call hash_object, embed its tagged digest as leaf
        """
        if isinstance(obj, (type(None), bool, int, float, str, ContentHash)):
            return self._expand_structure(obj, _visited, resolver=resolver)

        if _is_structure(obj):
            return self._expand_structure(obj, _visited, resolver=resolver)

        # Non-structure, non-primitive: hash independently and embed the hash.
        return _nested_hash(self.hash_object(obj, resolver=resolver))

    def _expand_mapping(
        self,
        obj: Mapping,
        _visited: frozenset[int],
        resolver: Callable[[Any], ContentHash] | None = None,
    ) -> dict:
        """Expand a dict/Mapping into a JSON tree that keeps its keys' types.

        A mapping whose keys are all ``str``, none of them ``"__type__"``, becomes
        a native JSON object. Any other mapping becomes
        ``{"__type__": "dict", "items": [[key, value], ...]}``, so ``{1: "x"}``
        and ``{"1": "x"}`` differ, keys that share a ``str()`` never collapse into
        one, and no mapping can spell out the hasher's own encoding of another
        value (ITL-942); a type handler that wants a tagged object returns an
        ``Encoded``. Insertion order never matters: object keys are sorted by the
        serialiser, pairs by their canonical JSON text.
        """
        expanded = [
            (
                self._expand_element(k, _visited, resolver=resolver),
                self._expand_element(v, _visited, resolver=resolver),
            )
            for k, v in obj.items()
        ]
        if all(isinstance(k, str) and k != TYPE_KEY for k in obj):
            return dict(expanded)
        pairs = sorted(([k, v] for k, v in expanded), key=_canonical_json)
        return {"__type__": "dict", "items": pairs}

    def _expand_encoded(
        self,
        encoded: Encoded,
        resolver: Callable[[Any], ContentHash] | None = None,
    ) -> dict[str, Any]:
        """Expand a handler's ``Encoded`` result into ``{"__type__": tag, **fields}``."""
        tree: dict[str, Any] = {TYPE_KEY: encoded.tag}
        for key, value in encoded.fields.items():
            tree[key] = self._expand_element(value, frozenset(), resolver=resolver)
        return tree

    def _expand_namedtuple(
        self,
        obj: Any,
        _visited: frozenset[int],
        resolver: Callable[[Any], ContentHash] | None = None,
    ) -> dict[str, Any]:
        """Expand a namedtuple into a tagged dict of its fields in declared order.

        A namedtuple compares as an ordered tuple, so its field order is part of
        its identity: ``fields`` is a list of ``[name, value]`` pairs in declared
        order, never sorted by name (ITL-942 T).
        """
        fields: tuple[str, ...] = obj._fields
        return {
            "__type__": "namedtuple",
            "name": type(obj).__name__,
            "fields": [
                [
                    field,
                    self._expand_element(
                        getattr(obj, field), _visited, resolver=resolver
                    ),
                ]
                for field in fields
            ],
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
            json_bytes = _canonical_json(obj).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"SemanticAwarePythonHasher: failed to JSON-serialise object of type "
                f"{type(obj).__name__!r}. Ensure all PythonTypeHandlerProtocol "
                "implementations and identity_structure() return JSON-serialisable "
                "primitives or structures."
            ) from exc

        digest = hashlib.sha256(json_bytes).digest()
        return ContentHash(self._hasher_id, digest)

    # ------------------------------------------------------------------
    # Private: fallback for unhandled types
    # ------------------------------------------------------------------

    def _handle_unknown(self, obj: Any) -> str:
        """
        Produce a best-effort string for an unregistered, non-ContentIdentifiableProtocol
        type.  Raises TypeError in strict mode.
        """
        class_name = type(obj).__name__
        module_name = canonical_module_name(type(cast(object, obj))) or "<unknown>"
        qualified = f"{module_name}.{class_name}"

        if self._strict:
            raise TypeError(
                f"SemanticAwarePythonHasher (strict): no implementation of "
                f"PythonTypeHandlerProtocol registered for type '{qualified}' and it "
                "does not implement ContentIdentifiableProtocol. Register an "
                "implementation of PythonTypeHandlerProtocol via the "
                "HandlerRegistryProtocol or implement identity_structure() on the class."
            )

        logger.warning(
            "SemanticAwarePythonHasher (non-strict): no implementation of "
            "PythonTypeHandlerProtocol registered for type '%s'. "
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


def _canonical_json(obj: Any) -> str:
    """Return the canonical JSON text of an expanded tree: the hashed preimage."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


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
