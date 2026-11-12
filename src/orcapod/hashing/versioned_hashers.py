"""
Versioned hasher factories for OrcaPod.

Factories for the current structure hasher and Arrow hasher. Each hash label is
defined once, beside its algorithm (``SEMANTIC_HASH_LABEL`` in
``semantic_hashing/semantic_hasher.py``, ``ARROW_HASH_LABEL`` in
``arrow_hashers.py``); these factories take their defaults from there.

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

from orcapod.hashing.arrow_hashers import ARROW_HASH_LABEL
from orcapod.hashing.semantic_hashing.semantic_hasher import SEMANTIC_HASH_LABEL
from orcapod.protocols import hashing_protocols as hp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SemanticHasherProtocol factory
# ---------------------------------------------------------------------------


def get_versioned_semantic_hasher(
    hasher_id: str = SEMANTIC_HASH_LABEL,
    strict: bool = True,
    type_handler_registry: "hp.HandlerRegistryProtocol | None" = None,
) -> hp.SemanticHasherProtocol:
    """Return a SemanticHasherProtocol configured for the current version.

    Parameters
    ----------
    hasher_id:
        Identifier embedded in every ContentHash produced by this hasher.
    strict:
        When True raises TypeError for unhandled types. When False falls back
        to a best-effort string representation.
    type_handler_registry:
        Optional ``HandlerRegistryProtocol`` to inject. When None the
        global default registry is used.
    """
    from orcapod.hashing.semantic_hashing.semantic_hasher import (
        SemanticAwarePythonHasher,
    )

    if type_handler_registry is None:
        from orcapod.hashing.semantic_hashing.type_handler_registry import (
            get_default_python_type_handler_registry,
        )

        type_handler_registry = get_default_python_type_handler_registry()

    logger.debug(
        "get_versioned_semantic_hasher: creating SemanticAwarePythonHasher "
        "(hasher_id=%r, strict=%r)",
        hasher_id,
        strict,
    )
    return SemanticAwarePythonHasher(
        hasher_id=hasher_id,
        type_handler_registry=type_handler_registry,
        strict=strict,
    )


# ---------------------------------------------------------------------------
# StarfixArrowHasher factory
# ---------------------------------------------------------------------------


def get_versioned_semantic_arrow_hasher(
    hasher_id: str = ARROW_HASH_LABEL,
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
