"""
Universal Type Conversion Engine for Python ↔ Arrow type bidirectional conversion.

This provides a comprehensive, self-contained system that:
1. Converts Python type hints to Arrow types
2. Converts Arrow types back to Python type hints
3. Creates and caches conversion functions for optimal performance
4. Manages dynamic TypedDict creation for struct preservation
5. Integrates seamlessly with semantic type registries
"""

import hashlib
import logging
import types
import typing
from collections.abc import Callable, Mapping

# Handle generic types
from typing import TYPE_CHECKING, Any, TypedDict, get_args, get_origin

from orcapod.contexts import DataContext, resolve_context
from orcapod.semantic_types.semantic_registry import SemanticTypeRegistry
from orcapod.semantic_types.type_inference import infer_python_schema_from_pylist_data
from orcapod.types import DataType, Schema, SchemaLike
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


logger = logging.getLogger(__name__)


# Basic type mapping for Python -> Arrow conversion
_PYTHON_TO_ARROW_MAP = {
    # Python built-ins
    int: pa.int64(),
    float: pa.float64(),
    str: pa.large_string(),  # Use large_string by default for Polars compatibility
    bool: pa.bool_(),
    bytes: pa.large_binary(),  # Use large_binary by default for Polars compatibility
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
    "datetime": pa.timestamp("us"),
    "timestamp": pa.timestamp("us"),
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

    def __init__(self, semantic_registry: SemanticTypeRegistry | None = None):
        self.semantic_registry = semantic_registry

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

        This uses the main conversion logic, using caches for known type conversion for
        an improved performance.
        """
        fields = []
        for field_name, python_type in python_schema.items():
            arrow_type = self.python_type_to_arrow_type(python_type)
            fields.append(pa.field(field_name, arrow_type))

        return pa.schema(fields)

    def arrow_type_to_python_type(self, arrow_type: pa.DataType) -> DataType:
        """
        Convert Arrow type to Python type hint with caching.

        This is the main entry point for Arrow → Python type conversion.
        Results are cached for performance.
        """
        # Check cache first
        if arrow_type in self._arrow_to_python_types:
            return self._arrow_to_python_types[arrow_type]

        # Convert and cache result
        python_type = self._convert_arrow_to_python(arrow_type)
        self._arrow_to_python_types[arrow_type] = python_type

        return python_type

    def arrow_schema_to_python_schema(self, arrow_schema: pa.Schema) -> Schema:
        """
        Convert an Arrow schema to a Python Schema (mapping of field names to types).

        This uses the main conversion logic, using caches for known type conversion for
        an improved performance.
        """
        fields = {}
        for field in arrow_schema:
            fields[field.name] = self.arrow_type_to_python_type(field.type)

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
        if arrow_type in self._arrow_to_python_converters:
            return self._arrow_to_python_converters[arrow_type]

        # Create conversion function
        converter = self._create_arrow_to_python_converter(arrow_type)
        self._arrow_to_python_converters[arrow_type] = converter

        return converter

    def _convert_python_to_arrow(self, python_type: DataType) -> pa.DataType:
        """Core Python → Arrow type conversion logic."""

        if python_type in _PYTHON_TO_ARROW_MAP:
            return _PYTHON_TO_ARROW_MAP[python_type]

        # Check semantic registry for registered types
        if self.semantic_registry:
            converter = self.semantic_registry.get_converter_for_python_type(
                python_type
            )
            if converter:
                return converter.arrow_struct_type

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
                if type_name in _PYTHON_TO_ARROW_MAP:
                    return _PYTHON_TO_ARROW_MAP[type_name]
            raise ValueError(f"Unsupported Python type: {python_type}")

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
            if len(args) == 2 and type(None) in args:
                # Optional[T] → just T (nullability handled at field level)
                non_none_type = args[0] if args[1] is type(None) else args[1]
                return self.python_type_to_arrow_type(non_none_type)
            else:
                # Complex unions → use first type as fallback
                return self.python_type_to_arrow_type(args[0])

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

        # Handle basic types
        if pa.types.is_integer(arrow_type):
            return int
        elif pa.types.is_floating(arrow_type):
            return float
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return str
        elif pa.types.is_boolean(arrow_type):
            return bool
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return bytes

        # Handle struct types
        elif pa.types.is_struct(arrow_type):
            # Check if it's a registered semantic type first
            if self.semantic_registry:
                python_type = self.semantic_registry.get_python_type_for_semantic_struct_signature(
                    arrow_type
                )
                if python_type:
                    return python_type

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

        else:
            # Default case for unsupported types
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

        # Get the Arrow type for this Python type
        # TODO: check if this step is necessary
        _ = self.python_type_to_arrow_type(python_type)

        # Check for semantic type first
        if self.semantic_registry:
            converter = self.semantic_registry.get_converter_for_python_type(
                python_type
            )
            if converter:
                return converter.python_to_struct_dict

        # Create conversion function based on type

        origin = get_origin(python_type)
        args = get_args(python_type)

        if python_type in {int, float, str, bool, bytes} or origin is None:
            # Basic types - no conversion needed
            return lambda value: value

        elif origin is list:
            element_converter = self.get_python_to_arrow_converter(args[0])
            return (
                lambda value: [element_converter(item) for item in value]
                if value
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
                if value
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

        else:
            # Default passthrough
            return lambda value: value

    def _create_arrow_to_python_converter(
        self, arrow_type: pa.DataType
    ) -> Callable[[Any], Any]:
        """Create a cached conversion function for Arrow → Python values."""

        # Get the Python type for this Arrow type
        python_type = self.arrow_type_to_python_type(arrow_type)

        # Check for semantic type first
        if self.semantic_registry and pa.types.is_struct(arrow_type):
            registered_python_type = (
                self.semantic_registry.get_python_type_for_semantic_struct_signature(
                    arrow_type
                )
            )
            if registered_python_type:
                converter = self.semantic_registry.get_converter_for_python_type(
                    registered_python_type
                )
                if converter:
                    return converter.struct_dict_to_python

        # Handle basic types - no conversion needed
        if (
            pa.types.is_integer(arrow_type)
            or pa.types.is_floating(arrow_type)
            or pa.types.is_boolean(arrow_type)
            or pa.types.is_string(arrow_type)
            or pa.types.is_large_string(arrow_type)
            or pa.types.is_binary(arrow_type)
            or pa.types.is_large_binary(arrow_type)
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
