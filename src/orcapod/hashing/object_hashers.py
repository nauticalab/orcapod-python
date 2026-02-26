import hashlib
import json
import logging
import uuid
from abc import ABC, abstractmethod
from collections.abc import Collection, Mapping
from pathlib import Path
from typing import Any
from uuid import UUID

from orcapod.protocols import hashing_protocols as hp
from orcapod.types import ContentHash

logger = logging.getLogger(__name__)


class ObjectHasherBase(ABC):
    @abstractmethod
    def hash_object(self, obj: object) -> ContentHash: ...

    @property
    @abstractmethod
    def hasher_id(self) -> str: ...

    def hash_to_hex(
        self, obj: Any, char_count: int | None = None, prefix_hasher_id: bool = False
    ) -> str:
        content_hash = self.hash_object(obj)
        hex_str = content_hash.to_hex()

        # TODO: clean up this logic, as char_count handling is messy
        if char_count is not None:
            if char_count > len(hex_str):
                raise ValueError(
                    f"Cannot truncate to {char_count} chars, hash only has {len(hex_str)}"
                )
            hex_str = hex_str[:char_count]
        if prefix_hasher_id:
            hex_str = self.hasher_id + "@" + hex_str
        return hex_str

    def hash_to_int(self, obj: Any, hexdigits: int = 16) -> int:
        """
        Hash an object to an integer.

        Args:
            obj (Any): The object to hash.
            hexdigits (int): Number of hexadecimal digits to use for the hash.

        Returns:
            int: The integer representation of the hash.
        """
        hex_hash = self.hash_to_hex(obj, char_count=hexdigits)
        return int(hex_hash, 16)

    def hash_to_uuid(
        self,
        obj: Any,
        namespace: uuid.UUID = uuid.NAMESPACE_OID,
    ) -> uuid.UUID:
        """Convert hash to proper UUID5."""
        # TODO: decide whether to use to_hex or digest here
        return uuid.uuid5(namespace, self.hash_object(obj).to_hex())


class BasicObjectHasher(ObjectHasherBase):
    """
    Default object hasher used throughout the codebase.
    """

    def __init__(
        self,
        hasher_id: str,
        function_info_extractor: hp.FunctionInfoExtractor | None = None,
    ):
        self._hasher_id = hasher_id
        self.function_info_extractor = function_info_extractor

    @property
    def hasher_id(self) -> str:
        return self._hasher_id

    def process_structure(
        self,
        obj: Any,
        visited: set[int] | None = None,
        force_hash: bool = True,
    ) -> Any:
        """
        Recursively process a structure to prepare it for hashing.

        Args:
            obj: The object or structure to process
            visited: Set of object ids already visited (to handle circular references)
            function_info_extractor: FunctionInfoExtractor to be used for extracting necessary function representation

        Returns:
            A processed version of the structure suitable for stable hashing
        """
        # Initialize the visited set if this is the top-level call
        if visited is None:
            visited = set()
        else:
            visited = visited.copy()  # Copy to avoid modifying the original set

        # Check for circular references - use object's memory address
        # NOTE: While id() is not stable across sessions, we only use it within a session
        # to detect circular references, not as part of the final hash
        obj_id = id(obj)
        if obj_id in visited:
            logger.debug(
                f"Detected circular reference for object of type {type(obj).__name__}"
            )
            return "CircularRef"  # Don't include the actual id in hash output

        # TODO: revisit the hashing of the ContentHash
        if isinstance(obj, ContentHash):
            return (obj.method, obj.digest.hex())

        # For objects that could contain circular references, add to visited
        if isinstance(obj, (dict, list, tuple, set)) or not isinstance(
            obj, (str, int, float, bool, type(None))
        ):
            visited.add(obj_id)

        # Handle None
        if obj is None:
            return None

        # TODO: currently using runtime_checkable on ContentIdentifiable protocol
        # Re-evaluate this strategy to see if a faster / more robust check could be used
        if isinstance(obj, hp.ContentIdentifiable):
            logger.debug(
                f"Processing ContentHashableBase instance of type {type(obj).__name__}"
            )
            return self._hash_object(obj.identity_structure(), visited=visited).to_hex()

        # Handle basic types
        if isinstance(obj, (str, int, float, bool)):
            return obj

        # Handle bytes and bytearray
        if isinstance(obj, (bytes, bytearray)):
            logger.debug(
                f"Converting bytes/bytearray of length {len(obj)} to hex representation"
            )
            return obj.hex()

        # Handle Path objects
        if isinstance(obj, Path):
            logger.debug(f"Converting Path object to string: {obj}")
            raise NotImplementedError(
                "Path objects are not supported in this hasher. Please convert to string."
            )
            return str(obj)

        # Handle UUID objects
        if isinstance(obj, UUID):
            logger.debug(f"Converting UUID to string: {obj}")
            raise NotImplementedError(
                "UUID objects are not supported in this hasher. Please convert to string."
            )
            return str(obj)

        # Handle named tuples (which are subclasses of tuple)
        if hasattr(obj, "_fields") and isinstance(obj, tuple):
            logger.debug(f"Processing named tuple of type {type(obj).__name__}")
            # For namedtuples, convert to dict and then process
            d = {field: getattr(obj, field) for field in obj._fields}  # type: ignore
            return self.process_structure(d, visited)

        # Handle mappings (dict-like objects)
        if isinstance(obj, Mapping):
            # Process both keys and values
            processed_items = [
                (
                    self.process_structure(k, visited),
                    self.process_structure(v, visited),
                )
                for k, v in obj.items()
            ]

            # Sort by the processed keys for deterministic order
            processed_items.sort(key=lambda x: str(x[0]))

            # Create a new dictionary with string keys based on processed keys
            # TODO: consider checking for possibly problematic values in processed_k
            # and issue a warning
            return {
                str(processed_k): processed_v
                for processed_k, processed_v in processed_items
            }

        # Handle sets and frozensets
        if isinstance(obj, (set, frozenset)):
            logger.debug(
                f"Processing set/frozenset of type {type(obj).__name__} with {len(obj)} items"
            )
            # Process each item first, then sort the processed results
            processed_items = [self.process_structure(item, visited) for item in obj]
            return sorted(processed_items, key=str)

        # Handle collections (list-like objects)
        if isinstance(obj, Collection):
            logger.debug(
                f"Processing collection of type {type(obj).__name__} with {len(obj)} items"
            )
            return [self.process_structure(item, visited) for item in obj]

        # For functions, use the function_content_hash
        if callable(obj) and hasattr(obj, "__code__"):
            logger.debug(f"Processing function: {getattr(obj, '__name__')}")
            if self.function_info_extractor is not None:
                # Use the extractor to get a stable representation
                function_info = self.function_info_extractor.extract_function_info(obj)
                logger.debug(
                    f"Extracted function info: {function_info} for {obj.__name__}"
                )

                # simply return the function info as a stable representation
                return function_info
            else:
                raise ValueError(
                    f"Function {obj} encountered during processing but FunctionInfoExtractor is missing"
                )

        # handle data types
        if isinstance(obj, type):
            logger.debug(f"Processing class/type: {obj.__name__}")
            return f"type:{obj.__name__}"

        # For other objects, attempt to create deterministic representation only if force_hash=True
        class_name = obj.__class__.__name__
        module_name = obj.__class__.__module__
        if force_hash:
            try:
                import re

                logger.debug(
                    f"Processing generic object of type {module_name}.{class_name}"
                )

                # Try to get a stable dict representation if possible
                if hasattr(obj, "__dict__"):
                    # Sort attributes to ensure stable order
                    attrs = sorted(
                        (k, v) for k, v in obj.__dict__.items() if not k.startswith("_")
                    )
                    # Limit to first 10 attributes to avoid extremely long representations
                    if len(attrs) > 10:
                        logger.debug(
                            f"Object has {len(attrs)} attributes, limiting to first 10"
                        )
                        attrs = attrs[:10]
                    attr_strs = [f"{k}={type(v).__name__}" for k, v in attrs]
                    obj_repr = f"{{{', '.join(attr_strs)}}}"
                else:
                    # Get basic repr but remove memory addresses
                    logger.debug(
                        "Object has no __dict__, using repr() with memory address removal"
                    )
                    obj_repr = repr(obj)
                    if len(obj_repr) > 1000:
                        logger.debug(
                            f"Object repr is {len(obj_repr)} chars, truncating to 1000"
                        )
                        obj_repr = obj_repr[:1000] + "..."
                    # Remove memory addresses which look like '0x7f9a1c2b3d4e'
                    obj_repr = re.sub(r" at 0x[0-9a-f]+", " at 0xMEMADDR", obj_repr)

                return f"{module_name}.{class_name}:{obj_repr}"
            except Exception as e:
                # Last resort - use class name only
                logger.warning(f"Failed to process object representation: {e}")
                try:
                    return f"object:{obj.__class__.__module__}.{obj.__class__.__name__}"
                except AttributeError:
                    logger.error(
                        "Could not determine object class, using UnknownObject"
                    )
                    return "UnknownObject"
        else:
            raise ValueError(
                f"Processing of {obj} of type {module_name}.{class_name} is not supported"
            )

    def _hash_object(
        self,
        obj: Any,
        visited: set[int] | None = None,
    ) -> hp.ContentHash:
        # Process the object to handle nested structures and HashableMixin instances
        processed = self.process_structure(obj, visited=visited)

        # Serialize the processed structure
        json_str = json.dumps(processed, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        logger.debug(
            f"Successfully serialized {type(obj).__name__} using custom serializer"
        )

        # Create the hash
        return hp.ContentHash(self.hasher_id, hashlib.sha256(json_str).digest())

    def hash_object(self, obj: object) -> hp.ContentHash:
        return self._hash_object(obj)
