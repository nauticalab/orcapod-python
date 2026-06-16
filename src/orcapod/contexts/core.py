"""Core data structures and exceptions for the OrcaPod context system."""

from dataclasses import dataclass

from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    SemanticHasherProtocol,
)
from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol


@dataclass
class DataContext:
    """Data context containing all versioned components needed for data interpretation.

    Attributes:
        context_key: Unique identifier (e.g., "std:v0.1:default")
        version: Version string (e.g., "v0.1")
        description: Human-readable description
        type_converter: Type converter for Python ↔ Arrow conversion and
            registration. This is the single public API for all type operations.
        arrow_hasher: Arrow table hasher for this context
        semantic_hasher: General semantic hasher for this context. The
            ``TypeHandlerRegistry`` used for hashing is accessible via
            ``semantic_hasher.type_handler_registry``.
    """

    context_key: str
    version: str
    description: str
    type_converter: TypeConverterProtocol
    arrow_hasher: ArrowHasherProtocol
    semantic_hasher: SemanticHasherProtocol

class ContextValidationError(Exception):
    """Raised when context validation fails."""

    pass


class ContextResolutionError(Exception):
    """Raised when context cannot be resolved."""

    pass
