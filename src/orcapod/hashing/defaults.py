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

from orcapod.hashing.semantic_hashing.type_handler_registry import TypeHandlerRegistry
from orcapod.protocols import hashing_protocols as hp


def get_default_type_handler_registry() -> TypeHandlerRegistry:
    """
    Return the TypeHandlerRegistry from the default data context.

    Returns:
        TypeHandlerRegistry: The type handler registry from the default data context.
    """
    from orcapod.contexts import get_default_context

    return get_default_context().type_handler_registry


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


def get_default_arrow_hasher(
    cache_file_hash: bool | hp.StringCacherProtocol = True,
) -> hp.ArrowHasherProtocol:
    """
    Return the ArrowHasherProtocol from the default data context.

    If ``cache_file_hash`` is True an in-memory StringCacherProtocol is attached to
    the hasher so that repeated hashes of the same file path are served from
    cache.  Pass a ``StringCacherProtocol`` instance to use a custom caching backend
    (e.g. SQLite-backed).

    Note: caching is applied on top of the context's arrow hasher each time
    this function is called.  If you need a single shared cached instance,
    obtain it once and store it yourself.

    Args:
        cache_file_hash: True to use an ephemeral in-memory cache, a
            StringCacherProtocol instance to use a custom cache, or False/None to
            disable caching.

    Returns:
        ArrowHasherProtocol: The arrow hasher from the default data context,
            optionally with file-hash caching attached.
    """
    from typing import Any

    from orcapod.contexts import get_default_context

    arrow_hasher: Any = get_default_context().arrow_hasher

    if cache_file_hash:
        from orcapod.hashing.string_cachers import InMemoryCacher

        if cache_file_hash is True:
            string_cacher: hp.StringCacherProtocol = InMemoryCacher(max_size=None)
        else:
            string_cacher = cache_file_hash

        # set_cacher is present on SemanticArrowHasher but not on the
        # ArrowHasherProtocol protocol, so we call it via Any to avoid a type error.
        arrow_hasher.set_cacher("path", string_cacher)

    return arrow_hasher
