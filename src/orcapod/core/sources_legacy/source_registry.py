import logging
from collections.abc import Iterator
from orcapod.protocols.core_protocols import Source


logger = logging.getLogger(__name__)


class SourceCollisionError(Exception):
    """Raised when attempting to register a source ID that already exists."""

    pass


class SourceNotFoundError(Exception):
    """Raised when attempting to access a source that doesn't exist."""

    pass


class SourceRegistry:
    """
    Registry for managing data sources.

    Provides collision detection, source lookup, and management of source lifecycles.
    """

    def __init__(self):
        self._sources: dict[str, Source] = {}

    def register(self, source_id: str, source: Source) -> None:
        """
        Register a source with the given ID.

        Args:
            source_id: Unique identifier for the source
            source: Source instance to register

        Raises:
            SourceCollisionError: If source_id already exists
            ValueError: If source_id or source is invalid
        """
        if not source_id:
            raise ValueError("Source ID cannot be empty")

        if not isinstance(source_id, str):
            raise ValueError(f"Source ID must be a string, got {type(source_id)}")

        if source is None:
            raise ValueError("Source cannot be None")

        if source_id in self._sources:
            existing_source = self._sources[source_id]
            if existing_source == source:
                # Idempotent - same source already registered
                logger.debug(
                    f"Source ID '{source_id}' already registered with the same source instance."
                )
                return
            raise SourceCollisionError(
                f"Source ID '{source_id}' already registered with {type(existing_source).__name__}. "
                f"Cannot register {type(source).__name__}. "
                f"Choose a different source_id or unregister the existing source first."
            )

        self._sources[source_id] = source
        logger.info(f"Registered source: '{source_id}' -> {type(source).__name__}")

    def get(self, source_id: str) -> Source:
        """
        Get a source by ID.

        Args:
            source_id: Source identifier

        Returns:
            Source instance

        Raises:
            SourceNotFoundError: If source doesn't exist
        """
        if source_id not in self._sources:
            available_ids = list(self._sources.keys())
            raise SourceNotFoundError(
                f"Source '{source_id}' not found. Available sources: {available_ids}"
            )

        return self._sources[source_id]

    def get_optional(self, source_id: str) -> Source | None:
        """
        Get a source by ID, returning None if not found.

        Args:
            source_id: Source identifier

        Returns:
            Source instance or None if not found
        """
        return self._sources.get(source_id)

    def unregister(self, source_id: str) -> Source:
        """
        Unregister a source by ID.

        Args:
            source_id: Source identifier

        Returns:
            The unregistered source instance

        Raises:
            SourceNotFoundError: If source doesn't exist
        """
        if source_id not in self._sources:
            raise SourceNotFoundError(f"Source '{source_id}' not found")

        source = self._sources.pop(source_id)
        logger.info(f"Unregistered source: '{source_id}'")
        return source

    # TODO: consider just using __contains__
    def contains(self, source_id: str) -> bool:
        """Check if a source ID is registered."""
        return source_id in self._sources

    def list_sources(self) -> list[str]:
        """Get list of all registered source IDs."""
        return list(self._sources.keys())

    # TODO: consider removing this
    def list_sources_by_type(self, source_type: type) -> list[str]:
        """
        Get list of source IDs filtered by source type.

        Args:
            source_type: Class type to filter by

        Returns:
            List of source IDs that match the type
        """
        return [
            source_id
            for source_id, source in self._sources.items()
            if isinstance(source, source_type)
        ]

    def clear(self) -> None:
        """Remove all registered sources."""
        count = len(self._sources)
        self._sources.clear()
        logger.info(f"Cleared {count} sources from registry")

    def replace(self, source_id: str, source: Source) -> Source | None:
        """
        Replace an existing source or register a new one.

        Args:
            source_id: Source identifier
            source: New source instance

        Returns:
            Previous source if it existed, None otherwise
        """
        old_source = self._sources.get(source_id)
        self._sources[source_id] = source

        if old_source:
            logger.info(f"Replaced source: '{source_id}' -> {type(source).__name__}")
        else:
            logger.info(
                f"Registered new source: '{source_id}' -> {type(source).__name__}"
            )

        return old_source

    def get_source_info(self, source_id: str) -> dict:
        """
        Get information about a registered source.

        Args:
            source_id: Source identifier

        Returns:
            Dictionary with source information

        Raises:
            SourceNotFoundError: If source doesn't exist
        """
        source = self.get(source_id)  # This handles the not found case

        info = {
            "source_id": source_id,
            "type": type(source).__name__,
            "reference": source.reference if hasattr(source, "reference") else None,
        }
        info["identity"] = source.identity_structure()

        return info

    def __len__(self) -> int:
        """Return number of registered sources."""
        return len(self._sources)

    def __contains__(self, source_id: str) -> bool:
        """Support 'in' operator for checking source existence."""
        return source_id in self._sources

    def __iter__(self) -> Iterator[str]:
        """Iterate over source IDs."""
        return iter(self._sources)

    def items(self) -> Iterator[tuple[str, Source]]:
        """Iterate over (source_id, source) pairs."""
        yield from self._sources.items()

    def __repr__(self) -> str:
        return f"SourceRegistry({len(self._sources)} sources)"

    def __str__(self) -> str:
        if not self._sources:
            return "SourceRegistry(empty)"

        source_summary = []
        for source_id, source in self._sources.items():
            source_summary.append(f"  {source_id}: {type(source).__name__}")

        return "SourceRegistry:\n" + "\n".join(source_summary)


# Global source registry instance
GLOBAL_SOURCE_REGISTRY = SourceRegistry()
