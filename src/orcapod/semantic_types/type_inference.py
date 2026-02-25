from collections.abc import Collection, Mapping
from types import UnionType
from typing import Any, Union

from orcapod.types import DataType, Schema


def infer_python_schema_from_pylist_data(
    data: Collection[Mapping[str, Any]],
    default_type: type = str,
) -> Schema:
    """
    Infer schema from sample data (best effort).

    Args:
        data: List of sample dictionaries
        default_type: Default type to use for fields with no values

    Returns:
        Dictionary mapping field names to inferred Python types

    Note: This is best-effort inference and may not handle all edge cases.
    For production use, explicit schemas are recommended.
    """
    if not data:
        return Schema.empty()

    schema_data: dict[str, DataType] = {}

    # Get all possible field names
    all_fields = []
    for record in data:
        all_fields.extend(record.keys())

    all_fields = list(dict.fromkeys(all_fields))  # Remove duplicates

    # Infer type for each field
    for field_name in all_fields:
        # Get all values for this field (including None)
        all_field_values = [
            record.get(field_name) for record in data if field_name in record
        ]

        # Separate None and non-None values
        non_none_values = [v for v in all_field_values if v is not None]
        # check if there is at least one None value
        has_none = len(non_none_values) < len(all_field_values)

        if not non_none_values:
            # Handle case where all values are None
            schema_data[field_name] = default_type | None
            continue

        # Infer type from non-None values
        inferred_type = _infer_type_from_values(non_none_values)

        if inferred_type is None:
            schema_data[field_name] = default_type | None
        elif has_none:
            # Wrap with Optional if None values present
            # TODO: consider the case of Any
            schema_data[field_name] = inferred_type | None
        else:
            schema_data[field_name] = inferred_type

    return Schema(schema_data)


def infer_python_schema_from_pydict_data(
    data: dict[str, list[Any]],
    default_type: type = str,
) -> Schema:
    """
    Infer schema from columnar sample data (best effort).

    Args:
        data: Dictionary mapping field names to lists of values
        default_type: Default type to use for fields with no values

    Returns:
        Dictionary mapping field names to inferred Python types

    Note: This is best-effort inference and may not handle all edge cases.
    For production use, explicit schemas are recommended.
    """
    if not data:
        return Schema()

    schema_data = {}

    # Infer type for each field
    for field_name, field_values in data.items():
        if not field_values:
            # Handle case where field has empty list
            values = dict(schema_data)
            values[field_name] = default_type | None
            schema_data[field_name] = default_type | None
            continue

        # Separate None and non-None values
        non_none_values = [v for v in field_values if v is not None]
        has_none = len(non_none_values) < len(field_values)

        if not non_none_values:
            # Handle case where all values are None
            schema_data[field_name] = default_type | None
            continue

        # Infer type from non-None values
        inferred_type = _infer_type_from_values(non_none_values)

        if inferred_type is None:
            schema_data[field_name] = default_type | None
        elif has_none:
            # Wrap with Optional if None values present
            # TODO: consider the case of Any
            schema_data[field_name] = inferred_type | None
        else:
            schema_data[field_name] = inferred_type

    return Schema(schema_data)


# TODO: reconsider this type hint -- use of Any effectively renders this type hint useless
def _infer_type_from_values(values: list) -> DataType | None:
    """Infer type from a list of non-None values."""
    if not values:
        return None

    # Get types of all values
    value_types = {type(v) for v in values}

    if len(value_types) == 1:
        # All values have same type
        value_type = next(iter(value_types))
        return _infer_container_type(value_type, values)
    else:
        # Mixed types - handle common cases
        return _handle_mixed_types(value_types, values)


def _infer_container_type(value_type: type, values: list) -> type:
    """Infer container type with element types."""
    if value_type is list:
        return _infer_list_type(values)
    elif value_type is tuple:
        return _infer_tuple_type(values)
    elif value_type in {set, frozenset}:
        return _infer_set_type(values, value_type)
    elif value_type is dict:
        return _infer_dict_type(values)
    else:
        return value_type


def _infer_list_type(lists: list[list]) -> type:
    """Infer list element type."""
    all_elements = []
    for lst in lists:
        all_elements.extend(lst)

    if not all_elements:
        return list[Any]

    element_type = _infer_type_from_values(all_elements)
    return list[element_type]


def _infer_tuple_type(tuples: list[tuple]) -> type:
    """Infer tuple element types."""
    if not tuples:
        return tuple[Any, ...]

    # Check if all tuples have same length
    lengths = {len(t) for t in tuples}

    if len(lengths) == 1:
        # Fixed-length tuples - infer type for each position
        tuple_length = next(iter(lengths))
        if tuple_length == 0:
            return tuple[()]

        position_types = []
        for i in range(tuple_length):
            position_values = [t[i] for t in tuples if len(t) > i]
            position_type = _infer_type_from_values(position_values)
            position_types.append(position_type)

        # Always use fixed-length notation for same-length tuples
        return tuple[tuple(position_types)]
    else:
        # Variable-length tuples - infer common element type
        all_elements = []
        for t in tuples:
            all_elements.extend(t)

        if not all_elements:
            return tuple[Any, ...]

        element_type = _infer_type_from_values(all_elements)
        return tuple[element_type, ...]


def _infer_set_type(sets: list, set_type: type) -> type:
    """Infer set element type."""
    all_elements = []
    for s in sets:
        all_elements.extend(s)

    if not all_elements:
        return set_type[Any]  # type: ignore[return-value]

    element_type = _infer_type_from_values(all_elements)
    return set_type[element_type]  # type: ignore[return-value]


def _infer_dict_type(dicts: list[dict]) -> type:
    """Infer dictionary key and value types."""
    all_keys = []
    all_values = []

    for d in dicts:
        all_keys.extend(d.keys())
        all_values.extend(d.values())

    if not all_keys or not all_values:
        return dict[Any, Any]

    key_type = _infer_type_from_values(all_keys)
    value_type = _infer_type_from_values(all_values)

    return dict[key_type, value_type]


def _handle_mixed_types(value_types: set, values: list) -> UnionType | Any:
    """Handle mixed types by creating appropriate Union types."""

    # Handle common int/float mixing
    if value_types == {int, float}:
        return int | float

    # Handle numeric types with broader compatibility
    numeric_types = {int, float, complex}
    if value_types.issubset(numeric_types):
        if complex in value_types:
            return int | float | complex
        else:
            return int | float

    # For small number of types, create Union
    if len(value_types) <= 4:  # Arbitrary limit to avoid huge unions
        sorted_types = sorted(value_types, key=lambda t: t.__name__)
        return Union[tuple(sorted_types)]

    # Too many types, fall back to Any
    return Any


# Example usage and test function
def test_schema_inference():
    """Test the improved schema inference function."""

    test_data = [
        {
            "id": 1,
            "name": "Alice",
            "scores": [85, 92, 78],
            "coordinates": (10.5, 20.3),
            "tags": {"python", "data"},
            "metadata": {"created": "2023-01-01", "version": 1},
            "optional_field": "present",
        },
        {
            "id": 2,
            "name": "Bob",
            "scores": [88, 91],
            "coordinates": (15.2, 25.7),
            "tags": {"java", "backend"},
            "metadata": {"created": "2023-01-02", "version": 2},
            "optional_field": None,
        },
        {
            "id": 3,
            "name": "Charlie",
            "scores": [95, 87, 89, 92],
            "coordinates": (5.1, 30.9),
            "tags": {"javascript", "frontend"},
            "metadata": {"created": "2023-01-03", "version": 1},
            "mixed_field": 42,
        },
        {
            "id": 4,
            "name": "Diana",
            "scores": [],
            "coordinates": (0.0, 0.0),
            "tags": set(),
            "metadata": {},
            "mixed_field": "text",
        },
    ]

    schema = infer_python_schema_from_pylist_data(test_data)

    print("Inferred Schema:")
    for field, field_type in sorted(schema.items()):
        print(f"  {field}: {getattr(field_type, '__name__', field_type)}")

    return schema


if __name__ == "__main__":
    test_schema_inference()
