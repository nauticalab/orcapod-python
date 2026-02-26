"""
Core data structures and exceptions for the OrcaPod context system.

This module defines the basic types and exceptions used throughout
the context management system.
"""

from dataclasses import dataclass

from orcapod.hashing.type_handler_registry import TypeHandlerRegistry
from orcapod.protocols.hashing_protocols import ArrowHasher, SemanticHasher
from orcapod.protocols.semantic_types_protocols import TypeConverter


@dataclass
class DataContext:
    """
    Data context containing all versioned components needed for data interpretation.

    A DataContext represents a specific version of the OrcaPod system configuration,
    including semantic type registries, hashers, and other components that affect
    how data is processed and interpreted.

    Attributes:
        context_key: Unique identifier (e.g., "std:v0.1:default")
        version: Version string (e.g., "v0.1")
        description: Human-readable description of this context
        semantic_type_registry: Registry of semantic type converters
        arrow_hasher: Arrow table hasher for this context
        object_hasher: General object hasher for this context
        type_handler_registry: Registry of TypeHandler instances for SemanticHasher
    """

    context_key: str
    version: str
    description: str
    type_converter: TypeConverter
    arrow_hasher: ArrowHasher
    semantic_hasher: SemanticHasher  # this is the currently the JSON hasher
    type_handler_registry: TypeHandlerRegistry


class ContextValidationError(Exception):
    """Raised when context validation fails."""

    pass


class ContextResolutionError(Exception):
    """Raised when context cannot be resolved."""

    pass
