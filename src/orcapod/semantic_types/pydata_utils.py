# A collection of utility functions for working with Python lists of dictionaries and
# dictionary of lists

from types import UnionType
from typing import Any, Union

from orcapod.types import DataType, Schema


def pylist_to_pydict(pylist: list[dict]) -> dict:
    """
    Convert a list of dictionaries to a dictionary of lists (columnar format).

    This function transforms row-based data (list of dicts) to column-based data
    (dict of lists), similar to converting from records format to columnar format.
    Missing keys in individual dictionaries are filled with None values.

    Args:
        pylist: List of dictionaries representing rows of data

    Returns:
        Dictionary where keys are column names and values are lists of column data

    Example:
        >>> data = [{'a': 1, 'b': 2}, {'a': 3, 'c': 4}]
        >>> pylist_to_pydict(data)
        {'a': [1, 3], 'b': [2, None], 'c': [None, 4]}
    """
    result = {}
    known_keys = set()
    for i, d in enumerate(pylist):
        known_keys.update(d.keys())
        for k in known_keys:
            result.setdefault(k, [None] * i).append(d.get(k, None))
    return result


def pydict_to_pylist(pydict: dict) -> list[dict]:
    """
    Convert a dictionary of lists (columnar format) to a list of dictionaries.

    This function transforms column-based data (dict of lists) to row-based data
    (list of dicts), similar to converting from columnar format to records format.
    All arrays in the input dictionary must have the same length.

    Args:
        pydict: Dictionary where keys are column names and values are lists of column data

    Returns:
        List of dictionaries representing rows of data

    Raises:
        ValueError: If arrays in the dictionary have inconsistent lengths

    Example:
        >>> data = {'a': [1, 3], 'b': [2, None], 'c': [None, 4]}
        >>> pydict_to_pylist(data)
        [{'a': 1, 'b': 2, 'c': None}, {'a': 3, 'b': None, 'c': 4}]
    """
    if not pydict:
        return []

    # Check all arrays have same length
    lengths = [len(v) for v in pydict.values()]
    if not all(length == lengths[0] for length in lengths):
        raise ValueError(
            f"Inconsistent array lengths: {dict(zip(pydict.keys(), lengths))}"
        )

    num_rows = lengths[0]
    if num_rows == 0:
        return []

    result = []
    keys = pydict.keys()
    for i in range(num_rows):
        row = {k: pydict[k][i] for k in keys}
        result.append(row)
    return result


def infer_python_schema_from_pylist_data(
    data: list[dict],
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
        return Schema({})

    schema_data = {}

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
            schema_data[field_name] = (
                inferred_type | None if inferred_type != Any else Any
            )
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
        return Schema({})

    schema_data: dict[str, DataType] = {}

    # Infer type for each field
    for field_name, field_values in data.items():
        if not field_values:
            # Handle case where field has empty list
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
def _infer_type_from_values(values: list) -> type | UnionType | Any | None:
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
