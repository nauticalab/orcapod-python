# Default hasher accessors for the OrcaPod hashing system.
#
# All "default" hashers are obtained through the data context system, which is
# the single source of truth for versioned component configuration.  The
# functions below are thin convenience wrappers around the context system so
# that call-sites don't need to import from orcapod.contexts directly.
#
# DO NOT construct hashers directly here (e.g. via versioned_hashers).
# That is the job of the context registry when it instantiates a DataContext
# from its JSON spec.  Constructing them here would bypass versioning and
# produce hashers that are decoupled from the active data context.

from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
from orcapod.protocols import hashing_protocols as hp


def get_default_python_type_handler_registry() -> PythonTypeHandlerRegistry:
    """
    Return the ``PythonTypeHandlerRegistry`` from the default data context's
    semantic hasher.

    The registry is owned by the active ``SemanticAwarePythonHasher``, which is itself
    versioned inside the active ``DataContext``.

    Returns:
        PythonTypeHandlerRegistry: The type handler registry from the
            default data context.
    """
    from orcapod.contexts import get_default_context
    return get_default_context().semantic_hasher.type_handler_registry


def get_default_semantic_hasher() -> hp.SemanticHasherProtocol:
    """
    Return the SemanticHasherProtocol from the default data context.

    The hasher is owned by the active DataContext and is therefore consistent
    with all other versioned components (arrow hasher, type converter, etc.)
    that belong to the same context.

    Returns:
        SemanticHasherProtocol: The object hasher from the default data context.
    """
    # Late import to avoid circular dependencies: contexts imports from
    # protocols and hashing, so we must not import contexts at module level
    # inside the hashing package.
    from orcapod.contexts import get_default_context

    return get_default_context().semantic_hasher


def get_default_arrow_hasher() -> hp.ArrowHasherProtocol:
    """Return the ArrowHasherProtocol from the default data context.

    Note: file-hash caching (formerly via ``set_cacher``) has been removed.
    ``StarfixArrowHasher`` does not support per-path caching. Use
    ``CachedFileHasher`` when constructing a custom context if caching is needed.

    Returns:
        ArrowHasherProtocol: The arrow hasher from the default data context.
    """
    from orcapod.contexts import get_default_context
    return get_default_context().arrow_hasher
