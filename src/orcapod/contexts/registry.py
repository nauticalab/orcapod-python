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

from orcapod.contexts.core import (
    ContextResolutionError,
    ContextValidationError,
    DataContext,
)
from orcapod.system_constants import (
    PRE_V1_DATA_CONTEXT_KEY,
    PRE_V1_DATA_CONTEXT_VERSION,
)
from orcapod.utils.object_spec import parse_objectspec

logger = logging.getLogger(__name__)

# Check jsonschema availability once at import time so the "not available"
# message is emitted at most once per process (not once per spec file).
_JSONSCHEMA_AVAILABLE = importlib.util.find_spec("jsonschema") is not None
if not _JSONSCHEMA_AVAILABLE:
    logger.info("jsonschema not available, skipping schema validation")


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build one JSON object, refusing a key that occurs twice.

    ``json.load`` alone keeps the last of two equal keys and drops the others. A merge of
    two branches that each add the same object to a data context file can produce such a
    file without a conflict, so the entries of one branch would vanish silently.

    Args:
        pairs: The object's members in file order, as ``json`` passes them to an
            ``object_pairs_hook``.

    Returns:
        The object as a ``dict``.

    Raises:
        ValueError: If a key occurs more than once; the message names the key.
    """
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"a JSON object repeats the key {key!r}")
        result[key] = value
    return result


def _read_json_file(path: Path) -> Any:
    """Read a data context JSON file, refusing duplicate keys at any depth.

    Args:
        path: The file to read.

    Returns:
        The parsed document.

    Raises:
        ContextValidationError: If an object in the file repeats a key; the message
            names the file and the key.
    """
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f, object_pairs_hook=_object_without_duplicate_keys)
        except ValueError as error:
            if isinstance(error, json.JSONDecodeError):
                raise
            raise ContextValidationError(f"{path}: {error}") from error


class JSONDataContextRegistry:
    """
    Registry that loads data contexts from JSON files with validation.

    Features:
    - Loads context specs from JSON files in a directory
    - Validates JSON structure against schema
    - Lazy loading with caching
    - Robust error handling and logging
    - Context string resolution (e.g., "v1", "std:v1:default")
    """

    def __init__(
        self,
        contexts_dir: Path | str | None = None,
        schema_file: Path | str | None = None,
        default_version: str = "v1",
        logical_types_file: Path | str | None = None,
    ):
        """
        Initialize the context registry.

        Args:
            contexts_dir: Directory containing JSON context files
            schema_file: JSON schema file for validation (optional)
            default_version: Default context version to use
            logical_types_file: The logical type registration: the registered logical
                types and the value handlers that hash their Python values. Defaults
                to ``logical_types.json`` of the ``orcapod.contexts`` package. It is
                kept outside the context files because a data context file is frozen
                once released, while logical types are added independently.
        """
        # Set up paths
        if contexts_dir is None:
            contexts_dir = self._get_default_contexts_dir()
        self.contexts_dir = Path(contexts_dir)

        if schema_file is None:
            schema_file = self.contexts_dir / "schemas" / "context_schema.json"
        self.schema_file = Path(schema_file) if schema_file else None

        if logical_types_file is None:
            logical_types_file = Path(__file__).parent / "logical_types.json"
        self.logical_types_file = Path(logical_types_file)

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
                self._schema = _read_json_file(self.schema_file)
                logger.info(f"Loaded validation schema from {self.schema_file}")
            except ContextValidationError:
                # A schema file that repeats a key is refused like any other context file.
                raise
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
        version = json_file.stem  # e.g., "v1" from "v1.json"

        # Load JSON
        spec = _read_json_file(json_file)

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
        ]
        missing_fields = [field for field in required_fields if field not in spec]
        if missing_fields:
            raise ContextValidationError(f"Missing required fields: {missing_fields}")

        # Validate against JSON schema if available
        if self._schema and _JSONSCHEMA_AVAILABLE:
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
        - "v1" -> "v1"
        - "std:v1:default" -> "v1" (extract version from full key)
        - "latest" -> highest version number
        """
        if context_string is None:
            return self._default_version

        # Handle special cases
        if context_string == "latest":
            versions = self.get_available_versions()
            return versions[-1] if versions else self._default_version

        # If it looks like a simple version (v1), use directly
        if context_string.startswith("v") and ":" not in context_string:
            return context_string

        # If it looks like a full context key (std:v1:default), extract version
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

            # A full key selects its manifest by the version part only, so the
            # whole key must be the one that manifest defines.
            if (
                context_string is not None
                and ":" in context_string
                and version in self._specs
                and context_string != self._specs[version]["context_key"]
            ):
                raise ContextResolutionError(
                    f"Unknown data context {context_string!r}: version "
                    f"{version!r} is the data context "
                    f"{self._specs[version]['context_key']!r}."
                )

            # Return cached context if available
            if version in self._contexts:
                logger.debug(f"Returning cached context for version {version}")
                return self._contexts[version]

            # Validate version exists
            if version not in self._specs and version == PRE_V1_DATA_CONTEXT_VERSION:
                raise ContextResolutionError(
                    f"The data context {PRE_V1_DATA_CONTEXT_KEY!r} is the one of "
                    "Orcapod before v0.3. This version of Orcapod does not ship its "
                    "manifest and cannot hash under it; the old manifest and its "
                    "code are to be kept in orcapod-sanctuary. Use an Orcapod "
                    "version before v0.3 for stores and jobs written under it."
                )
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
    _METADATA_KEYS = frozenset(
        {
            "context_key",
            "version",
            "description",
            "metadata",
            "released_in",
            "previous",
            "definitions",
            "libraries",
            "golden",
            "changes",
        }
    )

    # A context file that takes its logical types from the registration file refers
    # to them by this name, as ``{"_ref": "logical_type_registry"}``.
    _LOGICAL_TYPE_REGISTRY_REF = "logical_type_registry"

    # The ignore list declared for an extension name in the registration file's
    # ``hash_ignore`` object is referred to as ``hash_ignore:<extension name>``.
    _HASH_IGNORE_REF_PREFIX = "hash_ignore:"

    # The registration file's ``provisional`` list (extension names of the types
    # whose value hash is provisional, ITL-1068) is referred to by this name.
    _PROVISIONAL_REF = "provisional"

    def _uses_logical_type_registration(self, spec: dict[str, Any]) -> bool:
        """Whether ``spec`` takes its logical types from the registration file.

        True when its type converter is configured with
        ``{"_ref": "logical_type_registry"}`` and the spec defines no component of
        that name itself. A spec that lists its logical types inline is built as is.
        """
        if self._LOGICAL_TYPE_REGISTRY_REF in spec:
            return False
        converter_config = spec["type_converter"].get("_config", {})
        return converter_config.get("logical_type_registry") == {
            "_ref": self._LOGICAL_TYPE_REGISTRY_REF
        }

    def _load_logical_type_registration(self) -> dict[str, Any]:
        """Read the logical type registration file.

        Returns:
            Its ``logical_type_registry`` object spec, its ``value_handlers`` list
            of ``[type, handler]`` object-spec pairs and, when present, its
            ``deferred_value_handlers`` list of handlers loaded on first use and
            its ``hash_ignore`` object: per extension name, the property names the
            value hash of that type ignores; and its ``provisional`` list: the
            extension names of the types whose value hash is provisional.
        """
        return _read_json_file(self.logical_types_file)

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

            # The registered logical types live outside the (frozen) context file.
            registration: dict[str, Any] | None = None
            if self._uses_logical_type_registration(spec):
                registration = self._load_logical_type_registration()
                # The ignore list of each library value type is declared once, by
                # extension name; its logical type and its value handler both take
                # it as ``{"_ref": "hash_ignore:<extension name>"}`` (ITL-981).
                for name, ignore in registration.get("hash_ignore", {}).items():
                    # A JSON object or string would iterate as names; only a list
                    # of names is a list of names.
                    if not isinstance(ignore, list) or not all(
                        isinstance(item, str) for item in ignore
                    ):
                        raise ValueError(
                            f"hash_ignore[{name!r}] must be a list of property "
                            f"names, got {ignore!r}"
                        )
                    ref_lut[f"{self._HASH_IGNORE_REF_PREFIX}{name}"] = list(ignore)
                # The types whose value hash is provisional are declared once, by
                # extension name; the logical type registry takes the list as
                # ``{"_ref": "provisional"}`` and the value handler registry below
                # is given it. It never enters an identifier or a hash (ITL-1068).
                provisional = registration.get(self._PROVISIONAL_REF, [])
                if not isinstance(provisional, list) or not all(
                    isinstance(item, str) for item in provisional
                ):
                    raise ValueError(
                        f"provisional must be a list of extension names, got "
                        f"{provisional!r}"
                    )
                ref_lut[self._PROVISIONAL_REF] = list(provisional)
                ref_lut[self._LOGICAL_TYPE_REGISTRY_REF] = parse_objectspec(
                    registration["logical_type_registry"], ref_lut=ref_lut
                )

            for key, value in spec.items():
                if key in self._METADATA_KEYS:
                    continue
                if isinstance(value, dict) and "_class" in value:
                    logger.debug(f"Creating {key} for context {version}")
                    ref_lut[key] = parse_objectspec(value, ref_lut=ref_lut)

            # The value handlers of the logical types join the core handlers of the
            # context file; they may refer to its components (``file_hasher`` ...).
            if registration is not None:
                handler_registry = ref_lut["python_type_handler_registry"]
                for pair_spec in registration["value_handlers"]:
                    # As for the handlers of the context file: a pair whose type or
                    # handler is optional and not installed is skipped.
                    pair = parse_objectspec(pair_spec, ref_lut=ref_lut)
                    if pair and len(pair) == 2 and all(x is not None for x in pair):
                        handler_registry.register(pair[0], pair[1])
                # Handlers of optional value types are named, not built: the
                # library is imported when a value of the type is first hashed.
                for entry in registration.get("deferred_value_handlers", []):
                    handler_registry.register_deferred(
                        entry["python_type"],
                        entry["handler"],
                        parse_objectspec(entry.get("_config", {}), ref_lut=ref_lut),
                    )
                handler_registry.declare_provisional(ref_lut[self._PROVISIONAL_REF])

            return DataContext(
                context_key=context_key,
                version=version,
                description=description,
                type_converter=ref_lut["type_converter"],
                arrow_hasher=ref_lut["arrow_hasher"],
                semantic_hasher=ref_lut["semantic_hasher"],
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
