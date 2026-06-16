"""
Universal Type Conversion Engine for Python ↔ Arrow type bidirectional conversion.

This provides a comprehensive, self-contained system that:
1. Converts Python type hints to Arrow types
2. Converts Arrow types back to Python type hints
3. Creates and caches conversion functions for optimal performance
4. Manages dynamic TypedDict creation for struct preservation
5. Integrates seamlessly with semantic type registries
"""

from __future__ import annotations

import hashlib
import logging
import types
import typing
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone

# Handle generic types
from typing import TYPE_CHECKING, Any, TypedDict, get_args, get_origin

from orcapod.contexts import DataContext, resolve_context
from orcapod.extension_types.type_utils import _extract_leaf_classes
from orcapod.semantic_types.type_inference import infer_python_schema_from_pylist_data
from orcapod.types import DataType, Schema, SchemaLike
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.extension_types.registry import LogicalTypeRegistry
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


# Basic type mapping for Python -> Arrow conversion.
# Built lazily on first use so that importing this module does not trigger a
# pyarrow (or numpy) import.  Call _get_python_to_arrow_map() instead of
# referencing _PYTHON_TO_ARROW_MAP directly.
_PYTHON_TO_ARROW_MAP: "dict | None" = None


def _get_python_to_arrow_map() -> dict:
    """Return the Python→Arrow type map, building it on first call."""
    global _PYTHON_TO_ARROW_MAP
    if _PYTHON_TO_ARROW_MAP is not None:
        return _PYTHON_TO_ARROW_MAP

    _PYTHON_TO_ARROW_MAP = {
        # Python built-ins
        int: pa.int64(),
        float: pa.float64(),
        str: pa.large_string(),  # Use large_string by default for Polars compatibility
        bool: pa.bool_(),
        bytes: pa.large_binary(),  # Use large_binary by default for Polars compatibility
        # typing.Any — used when element type is unknown (e.g. inferred from empty containers)
        Any: pa.null(),
        # String representations (for when we get type names as strings)
        "int": pa.int64(),
        "float": pa.float64(),
        "str": pa.large_string(),
        "bool": pa.bool_(),
        "bytes": pa.large_binary(),
        # Specific integer types
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "uint8": pa.uint8(),
        "uint16": pa.uint16(),
        "uint32": pa.uint32(),
        "uint64": pa.uint64(),
        # Specific float types
        "float32": pa.float32(),
        "float64": pa.float64(),
        # Date/time types
        "date": pa.date32(),
        "datetime": pa.timestamp("us", tz="UTC"),
        datetime: pa.timestamp("us", tz="UTC"),
    }

    # Add numpy types if available
    try:
        import numpy as np

        _PYTHON_TO_ARROW_MAP.update(
            {
                np.int8: pa.int8(),
                np.int16: pa.int16(),
                np.int32: pa.int32(),
                np.int64: pa.int64(),
                np.uint8: pa.uint8(),
                np.uint16: pa.uint16(),
                np.uint32: pa.uint32(),
                np.uint64: pa.uint64(),
                np.float32: pa.float32(),
                np.float64: pa.float64(),
                np.bool_: pa.bool_(),
            }
        )
    except ImportError:
        pass

    return _PYTHON_TO_ARROW_MAP


# Cache for the set of Python types that UniversalTypeConverter handles natively.
# Built lazily by get_native_python_types() from _get_python_to_arrow_map() so
# that datetime, numpy types, and any future additions are captured automatically.
_ARROW_NATIVE_TYPE_KEYS: frozenset[type] | None = None


def _is_optional_type(python_type: DataType) -> bool:
    """Return True if python_type is T | None (Optional[T]).

    Args:
        python_type: A Python type annotation.

    Returns:
        True if the type has ``None`` as one of its union arms,
        False otherwise (including for plain types and complex
        non-optional unions).
    """
    origin = get_origin(python_type)
    if origin is typing.Union or origin is types.UnionType:
        return type(None) in get_args(python_type)
    return False


class UniversalTypeConverter:
    """
    Universal engine for Python ↔ Arrow type conversion with cached conversion functions.

    This is a complete, self-contained system that handles:
    - Python type hint → Arrow type conversion
    - Arrow type → Python type hint conversion
    - Dynamic TypedDict creation for struct field preservation
    - Cached conversion function generation
    - Integration with semantic type registries
    """

    def __init__(
        self,
        datetime_timezone: typing.Literal["strict", "coerce_utc"] = "strict",
        logical_type_registry: LogicalTypeRegistry | None = None,
    ):
        """
        Args:
            datetime_timezone: How to handle naive (timezone-less) ``datetime``
                values when converting Python → Arrow.

                ``"strict"`` (default) — raise ``ValueError`` immediately so
                callers are forced to be explicit about timezone semantics.

                ``"coerce_utc"`` — silently attach ``timezone.utc`` to naive
                datetimes before writing to Arrow.  Use this when you know that
                all naive datetimes in your data represent UTC.
            logical_type_registry: Optional registry of ``LogicalType`` instances.
                When provided, extension-type identity takes priority over the
                shape-based logical type system at encoding time.
        """
        self._datetime_timezone = datetime_timezone
        self._logical_type_registry = logical_type_registry

        # Cache for created TypedDict classes
        self._struct_signature_to_typeddict: dict[pa.StructType, DataType] = {}
        self._typeddict_to_struct_signature: dict[DataType, pa.StructType] = {}
        self._created_type_names: set[str] = set()

        # Cache for conversion functions
        self._python_to_arrow_converters: dict[DataType, Callable] = {}
        self._arrow_to_python_converters: dict[pa.DataType, Callable] = {}

        # Cache for type mappings
        self._python_to_arrow_types: dict[DataType, pa.DataType] = {}
        self._arrow_to_python_types: dict[pa.DataType, DataType] = {}

        # Cycle detection for register_python_class
        self._in_progress: set[type] = set()

    @classmethod
    def get_native_python_types(cls) -> frozenset[type]:
        """Return the set of Python types that this converter handles natively.

        Derived lazily from ``_get_python_to_arrow_map()`` so that
        ``datetime.datetime``, numpy scalar types, and any future additions
        are captured without hard-coding them here. ``type(None)`` is always
        included because ``NoneType`` is produced by ``Optional[T]`` /
        ``T | None`` unwrapping but may not appear as a key in the map.

        Returns:
            Frozen set of Python ``type`` objects with built-in Arrow mappings.
        """
        global _ARROW_NATIVE_TYPE_KEYS
        if _ARROW_NATIVE_TYPE_KEYS is None:
            _ARROW_NATIVE_TYPE_KEYS = frozenset(
                k for k in _get_python_to_arrow_map() if isinstance(k, type)
            ) | {type(None)}
        return _ARROW_NATIVE_TYPE_KEYS

    def ensure_types_registered_for_schemas(self, *schemas: Schema) -> None:
        """Ensure a LogicalType is registered for every annotation in schemas.

        Calls ``register_python_class`` for each annotation, which recursively
        resolves nested types and synthesises via factory if needed.
        When no ``LogicalTypeRegistry`` is configured, this is a no-op.

        Args:
            *schemas: One or more ``Schema`` mappings (column name → Python type).

        Raises:
            TypeError: If a leaf class has no registered ``LogicalType`` and
                no registered factory covers it.
        """
        if self._logical_type_registry is None:
            return
        for schema in schemas:
            for annotation in schema.values():
                self.register_python_class(annotation)

    def register_python_class(self, annotation: Any) -> "pa.DataType":
        """Register a Python type annotation and return its Arrow type.

        Traverses generic annotations recursively. For each concrete class found,
        either returns from the primitive map or registry (cache hit), or
        synthesises via factory and registers the result.

        Args:
            annotation: A Python type or generic alias (e.g. ``list[str]``,
                ``Optional[uuid.UUID]``, a dataclass type).

        Returns:
            The Arrow ``pa.DataType`` corresponding to ``annotation``.

        Raises:
            TypeError: If a concrete class has no registered ``LogicalType`` and
                no factory covers it, or if a circular dependency is detected.
            ValueError: If a complex (non-Optional) union is encountered.
        """
        import types as _types_mod

        type_map = _get_python_to_arrow_map()

        # Primitive map hit
        if annotation in type_map:
            return type_map[annotation]

        origin = get_origin(annotation)
        args = get_args(annotation)

        # Optional[T] / T | None → strip None arm
        if origin is typing.Union or origin is _types_mod.UnionType:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return self.register_python_class(non_none[0])
            raise ValueError(
                f"Complex unions with multiple non-None types are not supported: "
                f"{annotation!r}. Only Optional[T] (T | None) is allowed."
            )

        # list[T] → pa.large_list(T)
        if origin is list:
            return pa.large_list(self.register_python_class(args[0]))

        # set[T] → pa.large_list(T)
        if origin is set:
            return pa.large_list(self.register_python_class(args[0]))

        # dict[K, V] → pa.large_list(struct{key: K, value: V})
        if origin is dict:
            key_arrow = self.register_python_class(args[0])
            val_arrow = self.register_python_class(args[1])
            return pa.large_list(
                pa.struct([pa.field("key", key_arrow), pa.field("value", val_arrow)])
            )

        # Concrete class — registry or factory dispatch
        if isinstance(annotation, type):
            if self._logical_type_registry is None:
                # No registry — return primitive Arrow type if available, else raise
                raise TypeError(
                    f"No LogicalTypeRegistry configured — cannot register {annotation!r}. "
                    f"Provide logical_type_registry at converter construction time."
                )

            # Registry hit (already synthesised)
            lt = self._logical_type_registry.get_by_python_type(annotation)
            if lt is not None:
                return lt.get_arrow_extension_type()

            # Cycle detection
            if annotation in self._in_progress:
                raise TypeError(
                    f"Circular type dependency detected while synthesising "
                    f"LogicalType for {annotation!r}."
                )

            # Factory dispatch via MRO walk
            factory = self._find_factory_for_class(annotation)
            if factory is None:
                raise TypeError(
                    f"No LogicalType or LogicalTypeFactory registered for {annotation!r}. "
                    f"Register a factory: converter.register_logical_type_factory(factory, "
                    f"python_bases=[<base of {annotation.__name__}>])"
                )

            self._in_progress.add(annotation)
            try:
                lt = factory.create_for_python_type(annotation, converter=self)
                self._logical_type_registry.register_logical_type(lt)
            finally:
                self._in_progress.discard(annotation)

            return lt.get_arrow_extension_type()

        raise ValueError(f"Unsupported annotation: {annotation!r}")

    def _find_factory_for_class(
        self,
        python_type: type,
    ) -> "LogicalTypeFactoryProtocol | None":
        """Find the most-specific registered factory for ``python_type``.

        Walks ``python_type.__mro__`` and returns the first factory in
        ``_python_class_factories`` whose ``supports_class(python_type)`` returns True.
        Falls back to an ``issubclass`` scan for ABC-registered factories.

        Args:
            python_type: Concrete Python class to find a factory for.

        Returns:
            The matching ``LogicalTypeFactoryProtocol``, or ``None`` if none found.
        """
        factories = self._logical_type_registry._python_class_factories

        # MRO walk — most-specific base first
        for base in python_type.__mro__:
            factory = factories.get(base)
            if factory is not None:
                if hasattr(factory, "supports_class") and factory.supports_class(python_type):
                    return factory
                elif not hasattr(factory, "supports_class"):
                    # Factories without supports_class are treated as unconditional matches
                    return factory

        # issubclass fallback for ABC-registered factories
        for base, factory in factories.items():
            try:
                if issubclass(python_type, base):
                    if hasattr(factory, "supports_class"):
                        if factory.supports_class(python_type):
                            return factory
                    else:
                        return factory
            except TypeError:
                continue

        return None

    def register_storage_type(self, arrow_type: "pa.DataType") -> "pa.DataType":
        """Register extension types found in ``arrow_type`` and return the resolved type.

        Traverses Arrow types recursively in a bottom-up manner:

        - Primitives are returned unchanged.
        - ``pa.ExtensionType`` instances that are already registered are returned as-is.
        - Unregistered extension types: the storage type is resolved first (bottom-up),
          then the factory dispatches on the ``"category"`` metadata key.
        - Structs: each field's type is resolved; a new struct with resolved fields is returned.
        - Lists: the value type is resolved; a new list type with the resolved value is returned.

        Args:
            arrow_type: An Arrow type to traverse and register.

        Returns:
            The resolved Arrow type with extension types embedded.
        """
        # Extension type
        if isinstance(arrow_type, pa.ExtensionType):
            ext_name = arrow_type.extension_name
            if self._logical_type_registry is not None:
                lt = self._logical_type_registry.get_by_arrow_extension_name(ext_name)
                if lt is not None:
                    return lt.get_arrow_extension_type()
            # Registry miss — extract info and register
            raw_meta = arrow_type.__arrow_ext_serialize__()
            ext_meta = raw_meta if raw_meta else None
            resolved_storage = self.register_storage_type(arrow_type.storage_type)
            return self._ensure_extension_type_info(ext_name, ext_meta, resolved_storage)

        # Struct type — recurse into each field
        if pa.types.is_struct(arrow_type):
            resolved_fields = []
            for i in range(arrow_type.num_fields):
                field = arrow_type.field(i)
                resolved_type = self.register_storage_type(field.type)
                resolved_fields.append(pa.field(field.name, resolved_type, nullable=field.nullable))
            return pa.struct(resolved_fields)

        # Large list type
        if pa.types.is_large_list(arrow_type):
            resolved_value = self.register_storage_type(arrow_type.value_type)
            return pa.large_list(resolved_value)

        # List type
        if pa.types.is_list(arrow_type):
            resolved_value = self.register_storage_type(arrow_type.value_type)
            return pa.list_(resolved_value)

        # All other types (primitives, timestamps, binary, etc.) — return as-is
        return arrow_type

    def _ensure_extension_type_info(
        self,
        arrow_extension_name: str,
        extension_metadata: bytes | None,
        storage_type: "pa.DataType",
    ) -> "pa.DataType":
        """Register an extension type from (name, metadata, storage_type) info.

        Called by ``register_storage_type`` for in-memory ``pa.ExtensionType`` objects,
        and by ``register_discovered_extensions`` for the field-metadata (Parquet) channel.
        The ``storage_type`` must already be resolved (nested extension types registered).

        Args:
            arrow_extension_name: Arrow extension name (``ARROW:extension:name``).
            extension_metadata: Raw metadata bytes, expected to be UTF-8 JSON with
                at least a ``"category"`` key. ``None`` or empty bytes if absent.
            storage_type: Underlying Arrow storage type (already bottom-up resolved).

        Returns:
            The Arrow extension type after registration.

        Raises:
            ValueError: If metadata is missing, malformed, lacks ``"category"``, or
                no factory is registered for the category.
        """
        import json as _json

        if self._logical_type_registry is None:
            raise ValueError(
                f"No LogicalTypeRegistry configured — cannot register extension type "
                f"{arrow_extension_name!r}."
            )

        # Registry hit — already registered
        lt = self._logical_type_registry.get_by_arrow_extension_name(arrow_extension_name)
        if lt is not None:
            return lt.get_arrow_extension_type()

        # Missing metadata — cannot auto-register
        if not extension_metadata:
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has no extension metadata. "
                f"Types without a metadata category tag cannot be auto-registered via a factory. "
                f"Pre-register them explicitly via converter.register_logical_type(lt)."
            )

        # Parse JSON metadata
        try:
            metadata_dict = _json.loads(extension_metadata.decode("utf-8"))
        except (UnicodeDecodeError, _json.JSONDecodeError) as exc:
            raise ValueError(
                f"Extension type {arrow_extension_name!r} has metadata that is not valid "
                f"UTF-8 JSON: {extension_metadata!r}. Parse error: {exc}."
            ) from exc

        if not isinstance(metadata_dict, dict):
            raise ValueError(
                f"Extension type {arrow_extension_name!r} metadata decoded to a non-object "
                f"JSON value: {metadata_dict!r}."
            )

        if "category" not in metadata_dict:
            raise ValueError(
                f"Extension type {arrow_extension_name!r} metadata has no \"category\" key: "
                f"{metadata_dict}."
            )

        category = metadata_dict["category"]
        if not isinstance(category, str):
            raise ValueError(
                f"Extension type {arrow_extension_name!r} metadata \"category\" is not a "
                f"string: {category!r}."
            )

        # Look up factory by category
        factory = self._logical_type_registry._category_factories.get(category)
        if factory is None:
            raise ValueError(
                f"No LogicalTypeFactory registered for category {category!r}. "
                f"Cannot register extension type {arrow_extension_name!r}."
            )

        # Reconstruct and register
        logical_type = factory.reconstruct_from_arrow(
            arrow_extension_name, storage_type, metadata_dict, converter=self
        )
        self._logical_type_registry.register_logical_type(logical_type)
        return logical_type.get_arrow_extension_type()

    def python_to_storage(self, value: Any, annotation: Any) -> Any:
        """Convert a Python value to its Arrow storage representation.

        Thin wrapper over ``get_python_to_arrow_converter`` for use by
        ``DataclassLogicalType`` and other logical types that delegate per-field
        conversion back to the converter.

        Args:
            value: A Python object.
            annotation: The Python type annotation for ``value``.

        Returns:
            A value in Arrow storage format.
        """
        converter_fn = self.get_python_to_arrow_converter(annotation)
        return converter_fn(value)

    def storage_to_python(self, storage_value: Any, annotation: Any) -> Any:
        """Convert an Arrow storage value back to a Python object.

        Args:
            storage_value: A scalar or element from an Arrow storage array.
            annotation: The Python type annotation to convert back to.

        Returns:
            A Python object of the type described by ``annotation``.
        """
        arrow_type = self.python_type_to_arrow_type(annotation)
        converter_fn = self.get_arrow_to_python_converter(arrow_type)
        return converter_fn(storage_value)

    def register_logical_type(self, lt: "LogicalTypeProtocol") -> None:
        """Register a ``LogicalTypeProtocol`` instance.

        Pass-through to the internal ``LogicalTypeRegistry``.

        Args:
            lt: The logical type to register.
        """
        if self._logical_type_registry is None:
            raise ValueError("No LogicalTypeRegistry configured on this converter.")
        self._logical_type_registry.register_logical_type(lt)

    def register_logical_type_factory(
        self,
        factory: "LogicalTypeFactoryProtocol",
        *,
        category: str | None = None,
        python_bases: Iterable[type] = (),
    ) -> None:
        """Register a ``LogicalTypeFactoryProtocol`` instance.

        Pass-through to the internal ``LogicalTypeRegistry``.

        Args:
            factory: The factory to register.
            category: If given, registers factory as the read-side handler for
                Arrow extension types with this ``"category"`` metadata value.
            python_bases: Zero or more Python base classes to register as write-side
                dispatch keys for this factory.
        """
        if self._logical_type_registry is None:
            raise ValueError("No LogicalTypeRegistry configured on this converter.")
        self._logical_type_registry.register_logical_type_factory(
            factory, category=category, python_bases=python_bases
        )

    def python_type_to_arrow_type(self, python_type: DataType) -> pa.DataType:
        """
        Convert Python type hint to Arrow type with caching.

        This is the main entry point for Python → Arrow type conversion.
        Results are cached for performance.
        """
        # Check cache first
        if python_type in self._python_to_arrow_types:
            return self._python_to_arrow_types[python_type]

        # Convert and cache result
        arrow_type = self._convert_python_to_arrow(python_type)
        self._python_to_arrow_types[python_type] = arrow_type

        return arrow_type

    def python_schema_to_arrow_schema(self, python_schema: SchemaLike) -> pa.Schema:
        """
        Convert a Python schema (dict of field names to data types) to an Arrow schema.

        Field nullability is derived from the Python type: ``T | None``
        (Optional[T]) maps to ``nullable=True``; plain ``T`` maps to
        ``nullable=False``.  This uses caches for type conversion.
        """
        fields = []
        for field_name, python_type in python_schema.items():
            arrow_type = self.python_type_to_arrow_type(python_type)
            nullable = _is_optional_type(python_type)
            fields.append(pa.field(field_name, arrow_type, nullable=nullable))
        return pa.schema(fields)

    def arrow_type_to_python_type(self, arrow_type: pa.DataType) -> DataType:
        """
        Convert Arrow type to Python type hint with caching.

        This is the main entry point for Arrow → Python type conversion.
        Results are cached for performance.
        """
        try:
            if arrow_type in self._arrow_to_python_types:
                return self._arrow_to_python_types[arrow_type]
        except TypeError:
            # ExtensionType instances are not always hashable — skip the cache.
            return self._convert_arrow_to_python(arrow_type)

        python_type = self._convert_arrow_to_python(arrow_type)
        try:
            self._arrow_to_python_types[arrow_type] = python_type
        except TypeError:
            pass  # Unhashable type — skip caching.
        return python_type

    def arrow_schema_to_python_schema(self, arrow_schema: pa.Schema) -> Schema:
        """
        Convert an Arrow schema to a Python Schema (mapping of field names to types).

        ``nullable=True`` fields are reconstructed as ``T | None``; ``nullable=False``
        fields are reconstructed as plain ``T``, completing the bidirectional
        round-trip with ``python_schema_to_arrow_schema``.

        Round-trip guarantee:
            - ``int``       → ``nullable=False`` → ``int``
            - ``int | None`` → ``nullable=True``  → ``int | None``
        """
        fields = {}
        for field in arrow_schema:
            python_type = self.arrow_type_to_python_type(field.type)
            if field.nullable and python_type is not Any:
                python_type = python_type | None
            fields[field.name] = python_type
        return Schema(fields)

    def python_dicts_to_struct_dicts(
        self,
        python_dicts: list[dict[str, Any]],
        python_schema: SchemaLike | None = None,
    ) -> list[dict[str, Any]]:
        """
        Convert a list of Python dictionaries to Arrow compatible list of structural dicts.

        This uses the main conversion logic and caches results for performance.
        """
        if python_schema is None:
            python_schema = infer_python_schema_from_pylist_data(python_dicts)

        # prepare a LUT of converters from Python to Arrow-compatible data type
        converters = {
            field_name: self.get_python_to_arrow_converter(python_type)
            for field_name, python_type in python_schema.items()
        }

        converted_data = []
        for record in python_dicts:
            converted_record = {}
            for field_name, converter in converters.items():
                if field_name in record:
                    converted_record[field_name] = converter(record[field_name])
                else:
                    converted_record[field_name] = None
            converted_data.append(converted_record)

        return converted_data

    def struct_dict_to_python_dict(
        self,
        struct_dict: list[dict[str, Any]],
        arrow_schema: pa.Schema,
    ) -> list[dict[str, Any]]:
        """
        Convert a list of Arrow-compatible structural dictionaries to Python dictionaries.

        This uses the main conversion logic and caches results for performance.
        """

        converters = {
            field.name: self.get_arrow_to_python_converter(field.type)
            for field in arrow_schema
        }

        converted_data = []
        for record in struct_dict:
            converted_record = {}
            for field_name, converter in converters.items():
                if field_name in record:
                    converted_record[field_name] = converter(record[field_name])
                else:
                    converted_record[field_name] = None
            converted_data.append(converted_record)

        return converted_data

    def python_dicts_to_arrow_table(
        self,
        python_dicts: list[dict[str, Any]],
        python_schema: SchemaLike | None = None,
        arrow_schema: "pa.Schema | None" = None,
    ) -> pa.Table:
        """
        Convert a list of Python dictionaries to an Arrow table.

        This uses the main conversion logic and caches results for performance.
        """
        if python_schema is not None and arrow_schema is not None:
            logger.warning(
                "Both Python and Arrow schemas are provided. If they are not compatible, this may lead to unexpected behavior."
            )
        if python_schema is None and arrow_schema is None:
            # Infer schema from data if not provided
            python_schema = infer_python_schema_from_pylist_data(python_dicts)

        if arrow_schema is None:
            # Convert to Arrow schema
            assert python_schema is not None, "Python schema should not be None here"
            arrow_schema = self.python_schema_to_arrow_schema(python_schema)

        if python_schema is None:
            assert arrow_schema is not None, (
                "Arrow schema should not be None if reaching here"
            )
            python_schema = self.arrow_schema_to_python_schema(arrow_schema)

        struct_dicts = self.python_dicts_to_struct_dicts(
            python_dicts, python_schema=python_schema
        )

        # TODO: add more helpful message here
        return pa.Table.from_pylist(struct_dicts, schema=arrow_schema)

    def arrow_table_to_python_dicts(
        self, arrow_table: pa.Table
    ) -> list[dict[str, Any]]:
        """
        Convert an Arrow table to a list of Python dictionaries.

        This uses the main conversion logic and caches results for performance.
        """
        # Prepare converters for each field
        converters = {
            field.name: self.get_arrow_to_python_converter(field.type)
            for field in arrow_table.schema
        }

        python_dicts = []
        for row in arrow_table.to_pylist():
            python_dict = {}
            for field_name, value in row.items():
                if value is not None:
                    python_dict[field_name] = converters[field_name](value)
                else:
                    python_dict[field_name] = None
            python_dicts.append(python_dict)

        return python_dicts

    def get_python_to_arrow_converter(
        self, python_type: DataType
    ) -> Callable[[Any], Any]:
        """
        Get cached conversion function for Python value → Arrow value.

        This creates and caches conversion functions for optimal performance
        during data conversion operations.
        """
        if python_type in self._python_to_arrow_converters:
            return self._python_to_arrow_converters[python_type]

        # Create conversion function
        converter = self._create_python_to_arrow_converter(python_type)
        self._python_to_arrow_converters[python_type] = converter

        return converter

    def get_arrow_to_python_converter(
        self, arrow_type: pa.DataType
    ) -> Callable[[Any], Any]:
        """
        Get cached conversion function for Arrow value → Python value.

        This creates and caches conversion functions for optimal performance
        during data conversion operations.
        """
        try:
            if arrow_type in self._arrow_to_python_converters:
                return self._arrow_to_python_converters[arrow_type]
        except TypeError:
            # Some pa.DataType subclasses (e.g. pa.ExtensionType instances) are not
            # hashable and will raise TypeError on dict lookup. Fall through to
            # create the converter without caching.
            return self._create_arrow_to_python_converter(arrow_type)

        # Create conversion function
        converter = self._create_arrow_to_python_converter(arrow_type)
        self._arrow_to_python_converters[arrow_type] = converter

        return converter

    def _convert_python_to_arrow(self, python_type: DataType) -> pa.DataType:
        """Core Python → Arrow type conversion logic."""

        type_map = _get_python_to_arrow_map()
        if python_type in type_map:
            return type_map[python_type]

        # Check LogicalTypeRegistry — extension-type identity takes priority over shape-based system.
        # Guard with isinstance(…, type) because get_by_python_type is keyed on concrete classes;
        # generic aliases (list[T], Optional[T], etc.) will never be registered there.
        if self._logical_type_registry is not None and isinstance(python_type, type):
            lt = self._logical_type_registry.get_by_python_type(python_type)
            if lt is not None:
                return lt.get_arrow_extension_type()

        # Handle typeddict look up
        if python_type in self._typeddict_to_struct_signature:
            return self._typeddict_to_struct_signature[python_type]

        # Check generic types
        origin = get_origin(python_type)
        args = get_args(python_type)

        if origin is None:
            # Handle string type names
            if hasattr(python_type, "__name__"):
                type_name = getattr(python_type, "__name__")
                if type_name in type_map:
                    return type_map[type_name]
            raise ValueError(f"Unsupported Python type: {python_type}.")

        # Handle list types
        if origin is list:
            if len(args) != 1:
                raise ValueError(
                    f"list type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            return pa.large_list(element_type)

        # Handle tuple types
        elif origin is tuple:
            if len(args) == 0:
                raise ValueError("Empty tuple type not supported")

            if len(set(args)) == 1:
                # Homogeneous tuple → fixed-size list
                element_type = self.python_type_to_arrow_type(args[0])
                return pa.list_(element_type, len(args))
            else:
                # Heterogeneous tuple → struct with indexed fields
                fields = []
                for i, arg_type in enumerate(args):
                    field_type = self.python_type_to_arrow_type(arg_type)
                    fields.append((f"f{i}", field_type))
                return pa.struct(fields)

        # Handle dict types
        elif origin is dict:
            if len(args) != 2:
                raise ValueError(
                    f"dict type must have exactly two type arguments, got: {args}"
                )
            key_type = self.python_type_to_arrow_type(args[0])
            value_type = self.python_type_to_arrow_type(args[1])
            key_value_struct = pa.struct([("key", key_type), ("value", value_type)])
            return pa.large_list(key_value_struct)

        # Handle Union/Optional types
        elif origin is typing.Union or origin is types.UnionType:
            non_none_types = [t for t in args if t is not type(None)]
            if len(non_none_types) == 1:
                # Optional[T] → just T (nullability handled at field level)
                return self.python_type_to_arrow_type(non_none_types[0])
            else:
                raise ValueError(
                    f"Complex unions with multiple non-None types are not supported: {python_type}. "
                    f"Only Optional[T] (i.e., T | None) is allowed."
                )

        # Handle set types → lists
        elif origin is set:
            if len(args) != 1:
                raise ValueError(
                    f"set type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            return pa.large_list(element_type)

        else:
            raise ValueError(f"Unsupported generic type: {origin}")

    def _convert_arrow_to_python(self, arrow_type: pa.DataType) -> type | Any:
        """Core Arrow → Python type conversion logic."""

        # Handle null type — maps to Any (unknown element type, e.g. from empty containers)
        if pa.types.is_null(arrow_type):
            return Any

        # Check LogicalTypeRegistry for extension types
        if isinstance(arrow_type, pa.ExtensionType) and self._logical_type_registry is not None:
            lt = self._logical_type_registry.get_by_arrow_extension_name(
                arrow_type.extension_name
            )
            if lt is not None:
                return lt.python_type

        # Handle basic types
        if pa.types.is_integer(arrow_type):
            return int
        elif pa.types.is_floating(arrow_type):
            return float
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return str
        elif pa.types.is_boolean(arrow_type):
            return bool
        elif (
            pa.types.is_binary(arrow_type)
            or pa.types.is_large_binary(arrow_type)
            or pa.types.is_fixed_size_binary(arrow_type)
        ):
            return bytes

        # Handle struct types
        elif pa.types.is_struct(arrow_type):
            # Check if it is heterogeneous tuple
            if len(arrow_type) > 0 and all(
                field.name.startswith("f") and field.name[1:].isdigit()
                for field in arrow_type
            ):
                # This is likely a heterogeneous tuple, extract digits and ensure it
                # is continuous
                field_digits = [int(field.name[1:]) for field in arrow_type]
                if field_digits == list(range(len(field_digits))):
                    return tuple[
                        tuple(
                            self.arrow_type_to_python_type(
                                arrow_type.field(f"f{pos}").type
                            )
                            for pos in range(len(arrow_type))
                        )
                    ]
                else:
                    # Non-continuous field names, treat as dynamic TypedDict
                    logger.info(
                        "Detected heterogeneous tuple with non-continuous field names, "
                        "treating as dynamic TypedDict"
                    )

            # Create dynamic TypedDict for unregistered struct
            # TODO: add check for heterogeneous tuple checking each field starts with f
            return self._get_or_create_typeddict_for_struct(arrow_type)

        # Handle list types
        elif (
            pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)
        ):
            element_type = arrow_type.value_type

            # Check if this is a dict representation: list<struct<key, value>>
            if pa.types.is_struct(element_type):
                field_names = [field.name for field in element_type]
                if set(field_names) == {"key", "value"}:
                    # This is a dict
                    key_field = next(f for f in element_type if f.name == "key")
                    value_field = next(f for f in element_type if f.name == "value")

                    key_python_type = self.arrow_type_to_python_type(key_field.type)
                    value_python_type = self.arrow_type_to_python_type(value_field.type)

                    return dict[key_python_type, value_python_type]

            # Regular list
            element_python_type = self.arrow_type_to_python_type(element_type)

            if pa.types.is_fixed_size_list(arrow_type):
                # Fixed-size list → homogeneous tuple
                size = arrow_type.list_size
                return tuple[tuple(element_python_type for _ in range(size))]
            else:
                # Variable-size list → list
                return list[element_python_type]

        # Handle map types
        elif pa.types.is_map(arrow_type):
            key_python_type = self.arrow_type_to_python_type(arrow_type.key_type)
            value_python_type = self.arrow_type_to_python_type(arrow_type.item_type)
            return dict[key_python_type, value_python_type]

        # Handle union types
        elif pa.types.is_union(arrow_type):
            import typing

            child_types = []
            for i in range(arrow_type.num_fields):
                child_field = arrow_type[i]
                child_types.append(self.arrow_type_to_python_type(child_field.type))

            if len(child_types) == 2 and type(None) in child_types:
                # Optional[T]
                non_none_type = next(t for t in child_types if t is not type(None))
                return typing.Optional[non_none_type]
            else:
                return typing.Union[tuple(child_types)]

        elif pa.types.is_timestamp(arrow_type):
            return datetime

        else:
            # Default case for unsupported types.
            # NOTE: this silent fallback to Any can cause cryptic errors
            # downstream when code tries to convert Any back to Arrow
            # (e.g. "Unsupported Python type: typing.Any"). If you hit that,
            # the root cause is likely an unmapped Arrow type here.
            # (pa.null() is intentionally excluded — it is handled above.)
            logger.warning(
                "arrow_type_to_python_type: no mapping for Arrow type %r, "
                "falling back to typing.Any. This may cause errors downstream "
                "when converting back to Arrow.",
                arrow_type,
            )
            return Any

    def _get_or_create_typeddict_for_struct(
        self, struct_type: pa.StructType
    ) -> DataType:
        """Get or create a TypedDict class for an Arrow struct type."""

        # Check cache first
        if struct_type in self._struct_signature_to_typeddict:
            return self._struct_signature_to_typeddict[struct_type]

        # Create field specifications for TypedDict
        field_specs: dict[str, DataType] = {}
        for field in struct_type:
            field_name = field.name
            python_type = self.arrow_type_to_python_type(field.type)
            field_specs[field_name] = python_type

        # Generate unique name
        type_name = self._generate_unique_type_name(field_specs)

        # Create TypedDict dynamically
        typeddict_class = TypedDict(type_name, field_specs)  # type: ignore[call-arg]

        # Cache the mapping
        self._struct_signature_to_typeddict[struct_type] = typeddict_class
        self._typeddict_to_struct_signature[typeddict_class] = struct_type

        return typeddict_class

    # TODO: consider setting type of field_specs to Schema
    def _generate_unique_type_name(self, field_specs: Mapping[str, DataType]) -> str:
        """Generate a unique name for TypedDict based on field specifications."""

        # Create deterministic signature that includes both names and types
        field_items = sorted(field_specs.items())
        signature_parts = []

        for field_name, field_type in field_items:
            type_name = getattr(field_type, "__name__", str(field_type))
            if type_name.startswith("typing."):
                type_name = type_name[7:]
            signature_parts.append(f"{field_name}_{type_name}")

        # Create base name from signature
        if len(signature_parts) <= 2:
            base_name = "Struct_" + "_".join(signature_parts)
        else:
            # Use hash-based approach for larger structs
            signature_str = "_".join(signature_parts)
            signature_hash = hashlib.md5(signature_str.encode()).hexdigest()[:8]
            field_names = [item[0] for item in field_items]

            if len(field_names) <= 3:
                base_name = f"Struct_{'_'.join(field_names)}_{signature_hash}"
            else:
                base_name = f"Struct_{len(field_names)}fields_{signature_hash}"

        # Clean up the name
        base_name = (
            base_name.replace("[", "_")
            .replace("]", "_")
            .replace(",", "_")
            .replace(" ", "")
        )

        self._created_type_names.add(base_name)
        return base_name

    def _create_python_to_arrow_converter(
        self, python_type: DataType
    ) -> Callable[[Any], Any]:
        """Create a cached conversion function for Python → Arrow values."""

        # Check LogicalTypeRegistry first — extension-type identity takes priority.
        # Guard with isinstance(…, type) because get_by_python_type is keyed on concrete classes;
        # generic aliases (list[T], Optional[T], etc.) will never be registered there.
        if self._logical_type_registry is not None and isinstance(python_type, type):
            lt = self._logical_type_registry.get_by_python_type(python_type)
            if lt is not None:
                _lt = lt
                _self = self
                return lambda value: _lt.python_to_storage(value, _self)

        # Get the Arrow type for this Python type
        # TODO: check if this step is necessary
        _ = self.python_type_to_arrow_type(python_type)

        # Create conversion function based on type

        # Without this guard, datetime would reach the `origin is None` catch-all
        # below and be returned as a no-op passthrough — silently allowing naive
        # datetimes to flow into PyArrow and fail with a cryptic ArrowInvalid error.
        if python_type is datetime:
            _tz_policy = self._datetime_timezone

            def _convert_datetime(dt: datetime) -> datetime:
                # Pass None through so PyArrow enforces nullability at the schema
                # level — consistent with all other primitive converters.
                if dt is None:
                    return None  # type: ignore[return-value]
                _is_naive = dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None
                if _is_naive:
                    if _tz_policy == "strict":
                        raise ValueError(
                            "Naive datetime (no timezone info) is not supported "
                            "under the current 'strict' timezone policy. "
                            "Use a timezone-aware datetime, "
                            f"e.g. datetime.now(timezone.utc). Got: {dt!r}"
                        )
                    # coerce_utc: attach UTC so the value is treated as UTC.
                    return dt.replace(tzinfo=timezone.utc)
                return dt

            return _convert_datetime

        origin = get_origin(python_type)
        args = get_args(python_type)

        if python_type in {int, float, str, bool, bytes} or origin is None:
            # Basic types - no conversion needed
            return lambda value: value

        elif origin is list:
            element_converter = self.get_python_to_arrow_converter(args[0])
            return (
                lambda value: [element_converter(item) for item in value]
                if value is not None
                else []
            )

        elif origin is dict:
            key_converter = self.get_python_to_arrow_converter(args[0])
            value_converter = self.get_python_to_arrow_converter(args[1])
            return (
                lambda value: [
                    {"key": key_converter(k), "value": value_converter(v)}
                    for k, v in value.items()
                ]
                if value is not None
                else []
            )

        elif origin is tuple:
            if len(set(args)) == 1:
                # Homogeneous tuple
                element_converter = self.get_python_to_arrow_converter(args[0])
                return lambda value: [element_converter(item) for item in value]
            else:
                # Heterogeneous tuple
                converters = [self.get_python_to_arrow_converter(arg) for arg in args]
                return lambda value: {
                    f"f{i}": converters[i](item) for i, item in enumerate(value)
                }

        # Handle Optional[T] unions; complex unions (e.g., A | B) are not currently supported
        elif origin is typing.Union or origin is types.UnionType:
            non_none_types = [t for t in args if t is not type(None)]
            if len(non_none_types) == 1:
                # Optional[T] - use converter for T, pass through None
                inner_converter = self.get_python_to_arrow_converter(non_none_types[0])
                return lambda value: inner_converter(value) if value is not None else None
            else:
                raise ValueError(
                    f"Complex unions with multiple non-None types are not supported: {python_type}. "
                    f"Only Optional[T] (i.e., T | None) is allowed."
                )

        else:
            # Default passthrough
            return lambda value: value

    def _create_arrow_to_python_converter(
        self, arrow_type: pa.DataType
    ) -> Callable[[Any], Any]:
        """Create a cached conversion function for Arrow → Python values."""

        # Check LogicalTypeRegistry for extension types
        if isinstance(arrow_type, pa.ExtensionType) and self._logical_type_registry is not None:
            lt = self._logical_type_registry.get_by_arrow_extension_name(
                arrow_type.extension_name
            )
            if lt is not None:
                _lt = lt
                _self = self
                return lambda storage_value: _lt.storage_to_python(storage_value, _self)

        # Get the Python type for this Arrow type
        python_type = self.arrow_type_to_python_type(arrow_type)

        # Handle basic types - no conversion needed
        if (
            pa.types.is_integer(arrow_type)
            or pa.types.is_floating(arrow_type)
            or pa.types.is_boolean(arrow_type)
            or pa.types.is_string(arrow_type)
            or pa.types.is_large_string(arrow_type)
            or pa.types.is_binary(arrow_type)
            or pa.types.is_large_binary(arrow_type)
            or pa.types.is_fixed_size_binary(arrow_type)
        ):
            return lambda value: value

        # Handle list types
        elif (
            pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)
        ):
            element_type = arrow_type.value_type

            # Check if this is a dict representation
            if pa.types.is_struct(element_type):
                field_names = [field.name for field in element_type]
                if set(field_names) == {"key", "value"}:
                    # Dict representation
                    key_field = next(f for f in element_type if f.name == "key")
                    value_field = next(f for f in element_type if f.name == "value")

                    key_converter = self.get_arrow_to_python_converter(key_field.type)
                    value_converter = self.get_arrow_to_python_converter(
                        value_field.type
                    )

                    return (
                        lambda value: {
                            key_converter(item["key"]): value_converter(item["value"])
                            for item in value
                            if item is not None
                        }
                        if value
                        else {}
                    )

            # Regular list
            element_converter = self.get_arrow_to_python_converter(element_type)

            if pa.types.is_fixed_size_list(arrow_type):
                # Fixed-size list → tuple
                return (
                    lambda value: tuple(element_converter(item) for item in value)
                    if value
                    else ()
                )
            else:
                # Variable-size list → list
                return (
                    lambda value: [element_converter(item) for item in value]
                    if value
                    else []
                )

        # Handle struct types - heterogeneous tuple or dynamic TypedDict
        elif pa.types.is_struct(arrow_type):
            # if python_type
            if python_type is tuple or get_origin(python_type) is tuple:
                n = len(get_args(python_type))
                # prepare list of converters
                converters = [
                    self.get_arrow_to_python_converter(arrow_type.field(f"f{i}").type)
                    for i in range(n)
                ]
                # this is a heterogeneous tuple
                return lambda value: tuple(
                    converter(value[f"f{i}"]) for i, converter in enumerate(converters)
                )

            # Create converters for each field
            field_converters = {}
            for field in arrow_type:
                field_converters[field.name] = self.get_arrow_to_python_converter(
                    field.type
                )

            return (
                lambda value: {
                    field_name: field_converters[field_name](value.get(field_name))
                    for field_name in field_converters
                }
                if value
                else {}
            )

        elif pa.types.is_timestamp(arrow_type):
            # PyArrow's .as_py() already returns the right Python type:
            # - tz-less timestamp  → naive datetime
            # - tz-bearing timestamp → aware datetime (UTC or localised)
            # No additional conversion is needed in either case.
            return lambda value: value

        else:
            # Default passthrough
            return lambda value: value

    def is_dynamic_typeddict(self, python_type: type) -> bool:
        """Check if a type is one of our dynamically created TypedDicts."""
        return python_type in self._typeddict_to_struct_signature

    def get_struct_signature_for_typeddict(
        self, python_type: type
    ) -> pa.StructType | None:
        """Get the Arrow struct signature for a dynamically created TypedDict."""
        return self._typeddict_to_struct_signature.get(python_type)

    def clear_cache(self) -> None:
        """Clear all caches (useful for testing or memory management)."""
        self._struct_signature_to_typeddict.clear()
        self._typeddict_to_struct_signature.clear()
        self._created_type_names.clear()
        self._python_to_arrow_converters.clear()
        self._arrow_to_python_converters.clear()
        self._python_to_arrow_types.clear()
        self._arrow_to_python_types.clear()

    def get_cache_stats(self) -> dict[str, int]:
        """Get statistics about cache usage (useful for debugging/optimization)."""
        return {
            "typeddict_count": len(self._struct_signature_to_typeddict),
            "python_to_arrow_converters": len(self._python_to_arrow_converters),
            "arrow_to_python_converters": len(self._arrow_to_python_converters),
            "type_mappings": len(self._python_to_arrow_types)
            + len(self._arrow_to_python_types),
        }


# Public API functions
def python_type_to_arrow_type(
    python_type: type, data_context: DataContext | str | None = None
) -> pa.DataType:
    """Convert Python type to Arrow type using the global converter."""
    data_context = resolve_context(data_context)
    converter = data_context.type_converter
    return converter.python_type_to_arrow_type(python_type)


def arrow_type_to_python_type(
    arrow_type: pa.DataType, data_context: DataContext | str | None = None
) -> type:
    """Convert Arrow type to Python type using the global converter."""
    data_context = resolve_context(data_context)
    converter = data_context.type_converter
    return converter.arrow_type_to_python_type(arrow_type)


def get_conversion_functions(
    python_type: type, data_context: DataContext | str | None = None
) -> tuple[Callable, Callable]:
    """Get both conversion functions for a Python type."""
    data_context = resolve_context(data_context)
    converter = data_context.type_converter
    arrow_type = converter.python_type_to_arrow_type(python_type)

    python_to_arrow = converter.get_python_to_arrow_converter(python_type)
    arrow_to_python = converter.get_arrow_to_python_converter(arrow_type)

    return python_to_arrow, arrow_to_python
