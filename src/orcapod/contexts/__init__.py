"""
Orcapod Data Context System

This package manages versioned data contexts that define how data
should be interpreted and processed throughout the OrcaPod system.

A DataContext contains:
- Semantic type registry for handling structured data types
- Arrow hasher for hashing Arrow tables
- Semantic hasher for general Python object hashing
- Versioning information for reproducibility

Example usage:
    # Get default context
    context = resolve_context()

    # Get specific version
    context = resolve_context("v0.1")

    # Use context components
    registry = context.semantic_type_registry
    hasher = context.arrow_hasher

    # List available contexts
    versions = get_available_contexts()
"""

import logging
from typing import Any

from orcapod.protocols import hashing_protocols as hp
from orcapod.protocols import semantic_types_protocols as sp

logger = logging.getLogger(__name__)

from .core import ContextResolutionError, ContextValidationError, DataContext
from .registry import JSONDataContextRegistry

# Global registry instance (lazily initialized)
_registry: JSONDataContextRegistry | None = None


def _get_registry() -> JSONDataContextRegistry:
    """Get the global context registry, initializing if needed."""
    global _registry
    if _registry is None:
        _registry = JSONDataContextRegistry()
    return _registry


def get_default_context_key() -> str:
    return get_default_context().context_key


def resolve_context(context_info: str | DataContext | None = None) -> DataContext:
    """
    Resolve context information to a DataContext instance.

    Args:
        context_info: One of:
            - None: Use default context
            - str: Version string ("v0.1") or full key ("std:v0.1:default")
            - DataContext: Return as-is

    Returns:
        DataContext instance

    Raises:
        ContextResolutionError: If context cannot be resolved

    Examples:
        >>> context = resolve_context()  # Default
        >>> context = resolve_context("v0.1")  # Specific version
        >>> context = resolve_context("std:v0.1:default")  # Full key
        >>> context = resolve_context("latest")  # Latest version
    """
    # If already a DataContext, return as-is
    if isinstance(context_info, DataContext):
        return context_info

    # Use registry to resolve string/None to DataContext
    registry = _get_registry()
    return registry.get_context(context_info)


def get_available_contexts() -> list[str]:
    """
    Get list of all available context versions.

    Returns:
        Sorted list of version strings

    Example:
        >>> get_available_contexts()
        ['v0.1', 'v0.1-fast', 'v0.2']
    """
    registry = _get_registry()
    return registry.get_available_versions()


def get_context_info(version: str) -> dict[str, Any]:
    """
    Get metadata about a specific context version.

    Args:
        version: Context version string

    Returns:
        Dictionary with context metadata

    Example:
        >>> info = get_context_info("v0.1")
        >>> print(info['description'])
        'Initial stable release with basic Path semantic type support'
    """
    registry = _get_registry()
    return registry.get_context_info(version)


def set_default_context_version(version: str) -> None:
    """
    Set the default context version globally.

    Args:
        version: Version string to set as default

    Raises:
        ContextResolutionError: If version doesn't exist
    """
    registry = _get_registry()
    registry.set_default_version(version)


def validate_all_contexts() -> dict[str, str | None]:
    """
    Validate that all available contexts can be instantiated.

    Returns:
        Dict mapping version -> error message (None if valid)

    Example:
        >>> results = validate_all_contexts()
        >>> for version, error in results.items():
        ...     if error:
        ...         print(f"{version}: {error}")
        ...     else:
        ...         print(f"{version}: OK")
    """
    registry = _get_registry()
    return registry.validate_all_contexts()


def reload_contexts() -> None:
    """
    Reload context specifications from disk.

    Useful during development or when context files have been updated.
    Clears all cached contexts and reloads from JSON files.
    """
    registry = _get_registry()
    registry.reload_contexts()


def get_default_context() -> DataContext:
    """
    Get the default data context.

    Returns:
        DataContext instance for the default version
    """
    return resolve_context()


def get_default_semantic_hasher() -> hp.SemanticHasherProtocol:
    """
    Get the default semantic hasher.

    Returns:
        SemanticHasherProtocol instance for the default context
    """
    return get_default_context().semantic_hasher


def get_default_arrow_hasher() -> hp.ArrowHasherProtocol:
    """
    Get the default arrow hasher.

    Returns:
        ArrowHasherProtocol instance for the default context
    """
    return get_default_context().arrow_hasher


def get_default_type_converter() -> "sp.TypeConverterProtocol":
    """
    Get the default type converter.

    Returns:
        UniversalTypeConverter instance for the default context
    """
    return get_default_context().type_converter


# Convenience function for creating custom registries
def create_registry(
    contexts_dir: str | None = None,
    schema_file: str | None = None,
    default_version: str = "v0.1",
) -> JSONDataContextRegistry:
    """
    Create a custom context registry.

    Useful for testing or when you need to use a different set of contexts.

    Args:
        contexts_dir: Directory containing context JSON files
        schema_file: JSON schema file for validation
        default_version: Default version to use

    Returns:
        JSONDataContextRegistry instance

    Example:
        >>> # Create registry for testing
        >>> test_registry = create_registry("/path/to/test/contexts")
        >>> test_context = test_registry.get_context("test")
    """
    return JSONDataContextRegistry(contexts_dir, schema_file, default_version)


def enable_file_hash_caching(db_path: "Path | None" = None) -> None:
    """Enable SQLite-backed file hash caching on the default Orcapod context.

    Wraps the existing ``FileHandler``'s hasher in a ``CachedFileHasher``
    backed by a ``SqliteHashCacher`` and re-registers it for ``orcapod.File``
    in the default context's semantic hasher registry.

    Call once at application startup before any file hashing occurs.

    If the handler already wraps a ``CachedFileHasher`` (i.e. this function
    was already called), a warning is logged, all existing caching layers are
    unwrapped to reach the original base hasher, and the new cacher is applied
    around that base hasher. This keeps the system in a well-defined state
    (exactly one caching layer) regardless of how many times this function
    is called.

    For intentional multi-layer caching (e.g. in-memory L1 + SQLite L2),
    construct a ``CachedFileHasher`` manually and register it directly via
    the context's ``type_handler_registry`` instead.

    Args:
        db_path: Path to the SQLite cache database. Defaults to
            ``~/.orcapod/file_hash_cache.db`` or the
            ``ORCAPOD_HASH_CACHE_DB`` environment variable.
    """
    from orcapod.extension_types.file_type import File
    from orcapod.hashing.file_hashers import CachedFileHasher
    from orcapod.hashing.hash_cachers import SqliteHashCacher
    from orcapod.hashing.semantic_hashing.builtin_handlers import FileHandler

    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry

    existing_handler = registry.get_handler_for_type(File)
    if existing_handler is None:
        raise RuntimeError(
            "enable_file_hash_caching(): no FileHandler registered for "
            "orcapod.File in the default context. This should not happen "
            "with the standard v0.1 context."
        )

    base_hasher = existing_handler.file_hasher

    if isinstance(base_hasher, CachedFileHasher):
        logger.warning(
            "enable_file_hash_caching() called but the default FileHandler "
            "already has a CachedFileHasher. Unwrapping and replacing with "
            "the new cacher. If layered caching is intentional, construct a "
            "CachedFileHasher manually instead."
        )
        while isinstance(base_hasher, CachedFileHasher):
            cacher = base_hasher.cacher
            if hasattr(cacher, "close"):
                cacher.close()
            base_hasher = base_hasher.file_hasher

    registry.register(
        File,
        FileHandler(
            CachedFileHasher(
                file_hasher=base_hasher,
                cacher=SqliteHashCacher(db_path),
            )
        ),
    )


# Public API
__all__ = [
    # Core types
    "DataContext",
    "ContextValidationError",
    "ContextResolutionError",
    # Main functions
    "resolve_context",
    "get_available_contexts",
    "get_context_info",
    "get_default_context",
    # Management functions
    "set_default_context_version",
    "validate_all_contexts",
    "reload_contexts",
    # Caching
    "enable_file_hash_caching",
    # Advanced usage
    "create_registry",
    "JSONDataContextRegistry",
]
