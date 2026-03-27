"""
JSON-based Context Registry implementation.

This module contains the core registry that loads and manages
data contexts from JSON files with validation and caching.
"""

from __future__ import annotations

import importlib.util
import json
import logging
from pathlib import Path
from typing import Any

_JSONSCHEMA_AVAILABLE = importlib.util.find_spec("jsonschema") is not None

from orcapod.contexts.core import (
    ContextResolutionError,
    ContextValidationError,
    DataContext,
)
from orcapod.utils.object_spec import parse_objectspec

logger = logging.getLogger(__name__)


class JSONDataContextRegistry:
    """
    Registry that loads data contexts from JSON files with validation.

    Features:
    - Loads context specs from JSON files in a directory
    - Validates JSON structure against schema
    - Lazy loading with caching
    - Robust error handling and logging
    - Context string resolution (e.g., "v0.1", "std:v0.1:default")
    """

    def __init__(
        self,
        contexts_dir: Path | str | None = None,
        schema_file: Path | str | None = None,
        default_version: str = "v0.1",
    ):
        """
        Initialize the context registry.

        Args:
            contexts_dir: Directory containing JSON context files
            schema_file: JSON schema file for validation (optional)
            default_version: Default context version to use
        """
        # Set up paths
        if contexts_dir is None:
            contexts_dir = self._get_default_contexts_dir()
        self.contexts_dir = Path(contexts_dir)

        if schema_file is None:
            schema_file = self.contexts_dir / "schemas" / "context_schema.json"
        self.schema_file = Path(schema_file) if schema_file else None

        # Internal state
        self._specs: dict[str, dict[str, Any]] = {}
        self._contexts: dict[str, DataContext] = {}
        self._schema: dict[str, Any] | None = None
        self._default_version = default_version

        # Load everything on initialization
        self._load_schema()
        self._load_all_specs()
        logger.info(f"Loaded {len(self._specs)} context specifications")

    def _get_default_contexts_dir(self) -> Path:
        """Get the default contexts directory from package data."""
        try:
            # Python 3.9+ preferred method
            import importlib.resources as resources

            contexts_path = resources.files("orcapod.contexts") / "data"
            return Path(str(contexts_path))
        except (ImportError, AttributeError):
            # Fallback for older Python versions
            return Path(__file__).parent / "data"

    def _load_schema(self) -> None:
        """Load JSON schema for validation if available."""
        if self.schema_file and self.schema_file.exists():
            try:
                with open(self.schema_file, "r") as f:
                    self._schema = json.load(f)
                logger.info(f"Loaded validation schema from {self.schema_file}")
            except Exception as e:
                logger.warning(f"Failed to load schema from {self.schema_file}: {e}")
                self._schema = None
        else:
            logger.info("No validation schema specified or found")
            self._schema = None

    def _load_all_specs(self) -> None:
        """Load all JSON context specifications from the contexts directory."""
        if not self.contexts_dir.exists():
            raise ContextValidationError(
                f"Contexts directory not found: {self.contexts_dir}"
            )

        json_files = list(self.contexts_dir.glob("*.json"))
        if not json_files:
            raise ContextValidationError(
                f"No JSON context files found in {self.contexts_dir}"
            )

        for json_file in json_files:
            try:
                self._load_spec_file(json_file)
            except Exception as e:
                logger.error(f"Failed to load context spec from {json_file}: {e}")
                raise ContextValidationError(f"Invalid context file {json_file}: {e}")

    def _load_spec_file(self, json_file: Path) -> None:
        """Load and validate a single context specification file."""
        version = json_file.stem  # e.g., "v0.1" from "v0.1.json"

        # Load JSON
        with open(json_file, "r") as f:
            spec = json.load(f)

        # Validate basic structure
        if not isinstance(spec, dict):
            raise ContextValidationError("Context spec must be a JSON object")

        # Check version consistency
        spec_version = spec.get("version")
        if spec_version != version:
            raise ContextValidationError(
                f"Version mismatch in {json_file}: filename suggests '{version}' "
                f"but spec contains '{spec_version}'"
            )

        # TODO: clean this up -- sounds redundant to the validation performed by schema check
        # Validate required fields
        required_fields = [
            "context_key",
            "version",
            "type_converter",
            "arrow_hasher",
            "semantic_hasher",
            "type_handler_registry",
        ]
        missing_fields = [field for field in required_fields if field not in spec]
        if missing_fields:
            raise ContextValidationError(f"Missing required fields: {missing_fields}")

        # Validate against JSON schema if available
        if self._schema:
            if not _JSONSCHEMA_AVAILABLE:
                logger.info("jsonschema not available, skipping schema validation")
            else:
                import jsonschema  # noqa: PLC0415 – deferred to keep startup fast
                try:
                    jsonschema.validate(spec, self._schema)
                except jsonschema.ValidationError as e:
                    raise ContextValidationError(f"Schema validation failed: {e.message}")

        # Store the validated spec
        self._specs[version] = spec
        logger.debug(f"Loaded context spec: {version} -> {spec.get('context_key')}")

    def get_available_versions(self) -> list[str]:
        """Get all available context versions, sorted."""
        return sorted(self._specs.keys())

    def get_context_info(self, version: str) -> dict[str, Any]:
        """Get context metadata without creating the full context."""
        if version not in self._specs:
            available = ", ".join(self.get_available_versions())
            raise ContextResolutionError(
                f"Unknown context version '{version}'. Available: {available}"
            )

        spec = self._specs[version]
        return {
            "version": spec["version"],
            "context_key": spec["context_key"],
            "description": spec.get("description", "No description provided"),
            "file_path": self.contexts_dir / f"{version}.json",
        }

    def resolve_context_string(self, context_string: str | None) -> str:
        """
        Resolve context string to a version identifier.

        Supports various formats:
        - None -> default version
        - "v0.1" -> "v0.1"
        - "std:v0.1:default" -> "v0.1" (extract version from full key)
        - "latest" -> highest version number
        """
        if context_string is None:
            return self._default_version

        # Handle special cases
        if context_string == "latest":
            versions = self.get_available_versions()
            return versions[-1] if versions else self._default_version

        # If it looks like a simple version (v0.1), use directly
        if context_string.startswith("v") and ":" not in context_string:
            return context_string

        # If it looks like a full context key (std:v0.1:default), extract version
        if ":" in context_string:
            parts = context_string.split(":")
            if len(parts) >= 2 and parts[1].startswith("v"):
                return parts[1]  # Extract version part

        # Fallback: treat as version string
        return context_string

    def get_context(self, context_string: str | None = None) -> DataContext:
        """
        Get DataContext instance, creating it lazily if needed.

        Args:
            context_string: Version string, full context key, or None for default

        Returns:
            DataContext instance

        Raises:
            ContextResolutionError: If context cannot be resolved or created
        """
        try:
            # Resolve to version
            version = self.resolve_context_string(context_string)

            # Return cached context if available
            if version in self._contexts:
                logger.debug(f"Returning cached context for version {version}")
                return self._contexts[version]

            # Validate version exists
            if version not in self._specs:
                available = ", ".join(self.get_available_versions())
                raise ContextResolutionError(
                    f"Unknown context version '{version}' (resolved from '{context_string}'). "
                    f"Available: {available}"
                )

            # Create context from spec
            logger.info(f"Creating new context for version {version}")
            spec = self._specs[version]
            context = self._create_context_from_spec(spec)

            # Cache and return
            self._contexts[version] = context
            return context

        except Exception as e:
            if isinstance(e, (ContextResolutionError, ContextValidationError)):
                raise
            else:
                raise ContextResolutionError(
                    f"Failed to resolve context '{context_string}': {e}"
                )

    # Top-level keys that are metadata, not instantiable components.
    _METADATA_KEYS = frozenset({"context_key", "version", "description", "metadata"})

    def _create_context_from_spec(self, spec: dict[str, Any]) -> DataContext:
        """Create DataContext instance from validated specification.

        All top-level keys whose value is a dict with a ``_class`` entry are
        built in JSON order and added to a shared ``ref_lut``.  This means
        new versioned components (e.g. ``file_hasher``, ``function_info_extractor``)
        can be added to the JSON without touching this method — they are
        instantiated automatically and become available as ``_ref`` targets for
        later components in the same file.
        """
        try:
            context_key = spec["context_key"]
            version = spec["version"]
            description = spec.get("description", "")
            ref_lut: dict[str, Any] = {}

            for key, value in spec.items():
                if key in self._METADATA_KEYS:
                    continue
                if isinstance(value, dict) and "_class" in value:
                    logger.debug(f"Creating {key} for context {version}")
                    ref_lut[key] = parse_objectspec(value, ref_lut=ref_lut)

            return DataContext(
                context_key=context_key,
                version=version,
                description=description,
                type_converter=ref_lut["type_converter"],
                arrow_hasher=ref_lut["arrow_hasher"],
                semantic_hasher=ref_lut["semantic_hasher"],
                type_handler_registry=ref_lut["type_handler_registry"],
            )

        except Exception as e:
            raise ContextValidationError(
                f"Failed to create context from spec: {e}"
            ) from e

    def set_default_version(self, version: str) -> None:
        """Set the default context version."""
        if version not in self._specs:
            available = ", ".join(self.get_available_versions())
            raise ContextResolutionError(
                f"Cannot set default to unknown version '{version}'. Available: {available}"
            )

        old_default = self._default_version
        self._default_version = version
        logger.info(f"Changed default context version from {old_default} to {version}")

    def reload_contexts(self) -> None:
        """Reload all context specifications from disk."""
        logger.info("Reloading context specifications from disk")

        # Clear caches
        self._specs.clear()
        self._contexts.clear()

        # Reload
        self._load_schema()
        self._load_all_specs()

        logger.info(f"Reloaded {len(self._specs)} context specifications")

    def validate_all_contexts(self) -> dict[str, str | None]:
        """
        Validate that all context specifications can be instantiated.

        Returns:
            Dict mapping version -> error message (None if valid)
        """
        results = {}

        for version in self.get_available_versions():
            try:
                # Try to create the context (don't cache it)
                spec = self._specs[version]
                self._create_context_from_spec(spec)
                results[version] = None  # Success
                logger.debug(f"Context {version} validates successfully")
            except Exception as e:
                results[version] = str(e)
                logger.error(f"Context {version} validation failed: {e}")

        return results
