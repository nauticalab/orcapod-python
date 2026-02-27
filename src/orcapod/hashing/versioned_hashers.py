"""
Versioned hasher factories for OrcaPod.

This module is the single source of truth for which concrete hasher
implementations correspond to each versioned context.  All code that
needs a "current" or "versioned" hasher should go through these factory
functions rather than constructing hashers directly, so that version
bumps happen in exactly one place.

Functions
---------
get_versioned_semantic_hasher()
    Return the current-version SemanticHasherProtocol (the new content-based
    recursive hasher that replaces BasicObjectHasher).

get_versioned_object_hasher()
    Alias for get_versioned_semantic_hasher(), kept so that the context
    registry JSON ("object_hasher" key) and any existing call-sites
    continue to work without modification.

get_versioned_semantic_arrow_hasher()
    Return the current-version SemanticArrowHasher (Arrow table hasher
    with semantic-type support).
"""

from __future__ import annotations

import logging
from typing import Any

from orcapod.protocols import hashing_protocols as hp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Version constants
# ---------------------------------------------------------------------------

# The hasher_id embedded in every ContentHash produced by the current
# semantic hasher.  Bump this string when the resolution/serialisation
# algorithm changes in a way that would alter hash outputs so that stored
# hashes can be distinguished from newly-computed ones.
_CURRENT_SEMANTIC_HASHER_ID = "semantic_v0.1"

# The hasher_id for the Arrow hasher.
_CURRENT_ARROW_HASHER_ID = "arrow_v0.1"


# ---------------------------------------------------------------------------
# SemanticHasherProtocol factory
# ---------------------------------------------------------------------------


def get_versioned_semantic_hasher(
    hasher_id: str = _CURRENT_SEMANTIC_HASHER_ID,
    strict: bool = True,
    type_handler_registry: "hp.TypeHandlerRegistry | None" = None,  # type: ignore[name-defined]
) -> hp.SemanticHasherProtocol:
    """
    Return a SemanticHasherProtocol configured for the current version.

    The returned hasher uses the global default TypeHandlerRegistry (which
    is pre-populated with all built-in handlers) unless an explicit registry
    is supplied.

    Parameters
    ----------
    hasher_id:
        Identifier embedded in every ContentHash produced by this hasher.
        Defaults to the current version constant.  Override only when
        producing hashes that must be tagged with a specific version string.
    strict:
        When True (the default) the hasher raises TypeError on encountering
        an object of an unhandled type.  When False it falls back to a
        best-effort string representation with a logged warning.
    type_handler_registry:
        Optional TypeHandlerRegistry to inject.  When None the global
        default registry is used (recommended for production code).

    Returns
    -------
    SemanticHasherProtocol
        A fully configured SemanticHasherProtocol instance.
    """
    from orcapod.hashing.semantic_hashing.semantic_hasher import BaseSemanticHasher

    if type_handler_registry is None:
        from orcapod.hashing.semantic_hashing.type_handler_registry import (
            get_default_type_handler_registry,
        )

        type_handler_registry = get_default_type_handler_registry()

    logger.debug(
        "get_versioned_semantic_hasher: creating BaseSemanticHasher "
        "(hasher_id=%r, strict=%r)",
        hasher_id,
        strict,
    )
    return BaseSemanticHasher(
        hasher_id=hasher_id,
        type_handler_registry=type_handler_registry,
        strict=strict,
    )


def get_versioned_object_hasher(
    hasher_id: str = _CURRENT_SEMANTIC_HASHER_ID,
    strict: bool = True,
    type_handler_registry: "hp.TypeHandlerRegistry | None" = None,  # type: ignore[name-defined]
) -> hp.SemanticHasherProtocol:
    """
    Return the current-version object hasher.

    This is a backward-compatible alias for ``get_versioned_semantic_hasher()``.
    It exists so that:

      * The context registry JSON file (which references "object_hasher") and
        the ``DataContext.object_hasher`` field continue to work without any
        changes.
      * Call-sites that were already using ``get_versioned_object_hasher()``
        transparently receive the new SemanticHasherProtocol implementation.

    All parameters are forwarded verbatim to ``get_versioned_semantic_hasher()``.
    """
    return get_versioned_semantic_hasher(
        hasher_id=hasher_id,
        strict=strict,
        type_handler_registry=type_handler_registry,
    )


# ---------------------------------------------------------------------------
# SemanticArrowHasher factory
# ---------------------------------------------------------------------------


def get_versioned_semantic_arrow_hasher(
    hasher_id: str = _CURRENT_ARROW_HASHER_ID,
) -> hp.ArrowHasherProtocol:
    """
    Return a SemanticArrowHasher configured for the current version.

    The arrow hasher handles Arrow table / RecordBatch hashing with
    semantic-type awareness (e.g. Path columns are hashed by file content).

    Parameters
    ----------
    hasher_id:
        Identifier embedded in every ContentHash produced by this hasher.

    Returns
    -------
    ArrowHasherProtocol
        A fully configured SemanticArrowHasher instance.
    """
    from orcapod.hashing.arrow_hashers import SemanticArrowHasher
    from orcapod.semantic_types.semantic_registry import SemanticTypeRegistry
    from orcapod.semantic_types.semantic_struct_converters import PathStructConverter

    # Build a default semantic registry populated with the standard converters.
    # We use Any-typed locals here to side-step type-checker false positives
    # that arise from the protocol definition of SemanticStructConverterProtocol having
    # a slightly different hash_struct_dict signature than the concrete class.
    registry: Any = SemanticTypeRegistry()
    path_converter: Any = PathStructConverter()
    registry.register_converter("path", path_converter)

    logger.debug(
        "get_versioned_semantic_arrow_hasher: creating SemanticArrowHasher "
        "(hasher_id=%r)",
        hasher_id,
    )
    hasher: Any = SemanticArrowHasher(
        hasher_id=hasher_id,
        semantic_registry=registry,
    )
    return hasher
