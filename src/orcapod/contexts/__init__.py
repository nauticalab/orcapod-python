"""
Orcapod Data Context System

This package manages versioned data contexts that define how data
should be interpreted and processed throughout the OrcaPod system.

A DataContext contains:
- Semantic type registry for handling structured data types
- Arrow hasher for hashing Arrow tables
- Object hasher for general Python object hashing
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

from typing import Any

from orcapod.protocols import hashing_protocols as hp
from orcapod.protocols import semantic_types_protocols as sp

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


def get_default_object_hasher() -> hp.ObjectHasher:
    """
    Get the default object hasher.

    Returns:
        ObjectHasher instance for the default context
    """
    return get_default_context().object_hasher


def get_default_arrow_hasher() -> hp.ArrowHasher:
    """
    Get the default arrow hasher.

    Returns:
        ArrowHasher instance for the default context
    """
    return get_default_context().arrow_hasher


def get_default_type_converter() -> "sp.TypeConverter":
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
    # Advanced usage
    "create_registry",
    "JSONDataContextRegistry",
]
