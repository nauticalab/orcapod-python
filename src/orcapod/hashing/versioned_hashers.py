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

get_versioned_semantic_arrow_hasher()
    Return the current-version StarfixArrowHasher (Arrow table hasher
    with extension-type semantic support).
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


# ---------------------------------------------------------------------------
# StarfixArrowHasher factory
# ---------------------------------------------------------------------------


def get_versioned_semantic_arrow_hasher(
    hasher_id: str = _CURRENT_ARROW_HASHER_ID,
) -> hp.ArrowHasherProtocol:
    """Return a StarfixArrowHasher configured for the current version.

    Sources ``type_converter`` and ``semantic_hasher`` from the default
    ``DataContext`` so that the arrow hasher is consistent with all other
    versioned components.
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
