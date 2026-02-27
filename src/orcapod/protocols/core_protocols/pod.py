from __future__ import annotations

from collections.abc import Collection
from typing import Any, Protocol, TypeAlias, runtime_checkable

from orcapod.protocols.core_protocols.streams import Stream
from orcapod.protocols.core_protocols.traceable import Traceable
from orcapod.types import ColumnConfig, Schema

# Core recursive types
ArgumentGroup: TypeAlias = "SymmetricGroup | OrderedGroup | Stream"

SymmetricGroup: TypeAlias = frozenset[ArgumentGroup]  # Order-independent
OrderedGroup: TypeAlias = tuple[ArgumentGroup, ...]  # Order-dependent


@runtime_checkable
class Pod(Traceable, Protocol):
    """
    The fundamental unit of computation in Orcapod.

    Pods are the building blocks of computational graphs, transforming
    zero, one, or more input streams into a single output stream. They
    encapsulate computation logic while providing consistent interfaces
    for validation, type checking, and execution.

    Key design principles:
    - Immutable: Pods don't change after creation
    - Composable: Pods can be chained and combined
    - Type-safe: Strong typing and validation throughout

    Execution modes:
    - __call__(): Full-featured execution with tracking, returns LiveStream
    - forward(): Pure computation without side effects, returns Stream

    The distinction between these modes enables both production use (with
    full tracking) and testing/debugging (without side effects).
    """

    @property
    def uri(self) -> tuple[str, ...]:
        """
        Unique identifier for the pod

        The URI is used for caching/storage and tracking purposes.
        As the name indicates, this is how data originating from the pod will be referred to.

        Returns:
            tuple[str, ...]: URI for this pod
        """
        ...

    def validate_inputs(self, *streams: Stream) -> None:
        """
        Validate input streams, raising exceptions if invalid.

        Should check:
        - Number of input streams
        - Stream types and schemas
        - Kernel-specific requirements
        - Business logic constraints

        Args:
            *streams: Input streams to validate

        Raises:
            PodInputValidationError: If inputs are invalid
        """
        ...

    def argument_symmetry(self, streams: Collection[Stream]) -> ArgumentGroup:
        """
        Describe symmetry/ordering constraints on input arguments.

        Returns a structure encoding which arguments can be reordered:
        - SymmetricGroup (frozenset): Arguments commute (order doesn't matter)
        - OrderedGroup (tuple): Arguments have fixed positions
        - Nesting expresses partial symmetry

        Examples:
            Full symmetry (Join):
                return frozenset([a, b, c])

            No symmetry (Concatenate):
                return (a, b, c)

            Partial symmetry:
                return (frozenset([a, b]), c)
                # a,b are interchangeable, c has fixed position
        """
        ...

    def output_schema(
        self,
        *streams: Stream,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Determine output schemas without triggering computation.

        This method performs type inference based on input stream types,
        enabling efficient type checking and stream property queries.
        It should be fast and not trigger any expensive computation.

        Used for:
        - Pre-execution type validation
        - Query planning and optimization
        - Schema inference in complex pipelines
        - IDE support and developer tooling

        Args:
            *streams: Input streams to analyze

        Returns:
            tuple[TypeSpec, TypeSpec]: (tag_types, packet_types) for output

        Raises:
            ValidationError: If input types are incompatible
            TypeError: If stream types cannot be processed
        """
        ...

    def process(self, *streams: Stream, label: str | None = None) -> Stream:
        """
        Executes the computation on zero or more input streams.
        This method contains the core computation logic and should be
        overridden by subclasses. It performs pure computation without:
        - Performing validation (caller's responsibility)
        - Guaranteeing result type (may return static or live streams)

        The returned stream must be accurate at the time of invocation but
        need not stay up-to-date with upstream changes. This makes forward()
        suitable for:
        - Testing and debugging
        - Batch processing where currency isn't required
        - Internal implementation details

        Args:
            *streams: Input streams to process

        Returns:
            Stream: Result of the computation (may be static or live)
        """
        ...
