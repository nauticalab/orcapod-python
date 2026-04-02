# Library of functions for working with Schemas and for extracting Schemas from a function's signature

import inspect
import logging
import sys
from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Any, get_args, get_origin, get_type_hints

from orcapod.types import Schema, SchemaLike

logger = logging.getLogger(__name__)


def verify_packet_schema(packet: dict, schema: SchemaLike) -> bool:
    """Verify that the dictionary's types match the expected types in the schema."""
    from beartype.door import is_bearable

    # verify that packet contains no keys not in schema
    if set(packet.keys()) - set(schema.keys()):
        logger.warning(
            f"PacketProtocol contains keys not in schema: {set(packet.keys()) - set(schema.keys())}. "
        )
        return False
    for key, type_info in schema.items():
        if key not in packet:
            logger.warning(
                f"Key '{key}' not found in packet. Assuming None but this behavior may change in the future"
            )
        if not is_bearable(packet.get(key), type_info):
            logger.warning(
                f"Type mismatch for key '{key}': expected {type_info}, got {packet.get(key)}."
            )
            return False
    return True


# TODO: is_subhint does not handle invariance properly
# so when working with mutable types, we have to make sure to perform deep copy
def check_schema_compatibility(
    incoming_types: SchemaLike, receiving_types: Schema
) -> bool:
    from beartype.door import is_subhint

    for key, type_info in incoming_types.items():
        if key not in receiving_types:
            logger.warning(f"Key '{key}' not found in parameter types.")
            return False
        if not is_subhint(type_info, receiving_types[key]):
            logger.warning(
                f"Type mismatch for key '{key}': expected {receiving_types[key]}, got {type_info}."
            )
            return False

    # Every receiving key must be present in incoming OR be optional (has a default)
    for key in receiving_types:
        if key not in incoming_types and key not in receiving_types.optional_fields:
            logger.warning(f"Required key '{key}' missing from incoming types.")
            return False

    return True


def extract_function_schemas(
    func: Callable,
    output_keys: Collection[str],
    input_typespec: SchemaLike | None = None,
    output_typespec: SchemaLike | Sequence[type] | None = None,
) -> tuple[Schema, Schema]:
    """
    Extract input and output data types from a function signature.

    This function analyzes a function's signature to determine the types of its parameters
    and return values. It combines information from type annotations, user-provided type
    specifications, and return key mappings to produce complete type specifications.

    Args:
        func: The function to analyze for type information.
        output_keys: Collection of string keys that will be used to map the function's
            return values. For functions returning a single value, provide a single key.
            For functions returning multiple values (tuple/list), provide keys matching
            the number of return items.
        input_types: Optional mapping of parameter names to their types. If provided,
            these types override any type annotations in the function signature for the
            specified parameters. If a parameter is not in this mapping and has no
            annotation, an error is raised.
        output_types: Optional type specification for return values. Can be either:
            - A dict mapping output keys to types (TypeSpec)
            - A sequence of types that will be mapped to output_keys in order
            These types override any inferred types from the function's return annotation.

    Returns:
        A tuple containing:
        - input_types_dict: Mapping of parameter names to their inferred/specified types
        - output_types_dict: Mapping of output keys to their inferred/specified types

    Raises:
        ValueError: In various scenarios:
            - Parameter has no type annotation and is not in input_types
            - Function has return annotation but no output_keys specified
            - Function has explicit None return but non-empty output_keys provided
            - Multiple output_keys specified but return annotation is not a sequence type
            - Return annotation is a sequence type but doesn't specify item types
            - Number of types in return annotation doesn't match number of output_keys
            - Output types sequence length doesn't match output_keys length
            - Output key not specified in output_types and has no type annotation

    Examples:
        >>> def add(x: int, y: int) -> int:
        ...     return x + y
        >>> input_types, output_types = extract_function_data_types(add, ['result'])
        >>> input_types
        {'x': <class 'int'>, 'y': <class 'int'>}
        >>> output_types
        {'result': <class 'int'>}

        >>> def process(data: str) -> tuple[int, str]:
        ...     return len(data), data.upper()
        >>> input_types, output_types = extract_function_data_types(
        ...     process, ['length', 'upper_data']
        ... )
        >>> input_types
        {'data': <class 'str'>}
        >>> output_types
        {'length': <class 'int'>, 'upper_data': <class 'str'>}

        >>> def legacy_func(x, y):  # No annotations
        ...     return x + y
        >>> input_types, output_types = extract_function_data_types(
        ...     legacy_func, ['sum'],
        ...     input_types={'x': int, 'y': int},
        ...     output_types={'sum': int}
        ... )
        >>> input_types
        {'x': <class 'int'>, 'y': <class 'int'>}
        >>> output_types
        {'sum': <class 'int'>}

        >>> def multi_return(data: list) -> tuple[int, float, str]:
        ...     return len(data), sum(data), str(data)
        >>> input_types, output_types = extract_function_data_types(
        ...     multi_return, ['count', 'total', 'repr'],
        ...     output_types=[int, float, str]  # Override with sequence
        ... )
        >>> output_types
        {'count': <class 'int'>, 'total': <class 'float'>, 'repr': <class 'str'>}
    """
    verified_output_types: Schema = {}
    if output_typespec is not None:
        if isinstance(output_typespec, dict):
            verified_output_types = output_typespec
        elif isinstance(output_typespec, Sequence):
            # If output_types is a collection, convert it to a dict with keys from return_keys
            if len(output_typespec) != len(output_keys):
                raise ValueError(
                    f"Output types collection length {len(output_typespec)} does not match return keys length {len(output_keys)}."
                )
            verified_output_types = {k: v for k, v in zip(output_keys, output_typespec)}

    # Use get_type_hints to resolve annotations that may be stored as strings
    # (e.g. when the defining module uses `from __future__ import annotations`).
    # Fall back to an empty dict if hints cannot be resolved (e.g. for built-ins).
    try:
        resolved_hints = get_type_hints(func)
    except Exception:
        resolved_hints = {}

    signature = inspect.signature(func)

    param_info: Schema = {}
    optional_params: set[str] = set()
    for name, param in signature.parameters.items():
        if input_typespec and name in input_typespec:
            param_info[name] = input_typespec[name]
        elif name in resolved_hints:
            param_info[name] = resolved_hints[name]
        elif param.annotation is not inspect.Parameter.empty:
            # annotation is already a live type (no __future__ postponement)
            param_info[name] = param.annotation
        else:
            raise ValueError(
                f"Parameter '{name}' has no type annotation and is not specified in input_types."
            )
        if param.default is not inspect.Parameter.empty:
            optional_params.add(name)

    # get_type_hints stores the return annotation under the key 'return'
    return_annot = resolved_hints.get("return", signature.return_annotation)
    inferred_output_types: Schema = {}
    if return_annot is not inspect.Signature.empty and return_annot is not None:
        output_item_types = []
        if len(output_keys) == 0:
            raise ValueError(
                "Function has a return type annotation, but no return keys were specified."
            )
        elif len(output_keys) == 1:
            # if only one return key, the entire annotation is inferred as the return type
            output_item_types = [return_annot]
        elif get_origin(return_annot) in (tuple, list) or (
            isinstance(get_origin(return_annot), type)
            and issubclass(get_origin(return_annot), Sequence)
        ):
            if get_origin(return_annot) is None:
                # right type was specified but did not specify the type of items
                raise ValueError(
                    f"Function return type annotation {return_annot} is a Sequence type but does not specify item types."
                )
            output_item_types = get_args(return_annot)
            if len(output_item_types) != len(output_keys):
                raise ValueError(
                    f"Function return type annotation {return_annot} has {len(output_item_types)} items, "
                    f"but output_keys has {len(output_keys)} items."
                )
        else:
            raise ValueError(
                f"Multiple return keys were specified but return type annotation {return_annot} is not a sequence type (list, tuple, Collection)."
            )
        for key, type_annot in zip(output_keys, output_item_types):
            inferred_output_types[key] = type_annot
    elif return_annot is None:
        if len(output_keys) != 0:
            raise ValueError(
                f"Function provides explicit return type annotation as None, but return keys of length {len(output_keys)} were specified."
            )
    else:
        inferred_output_types = {k: inspect.Signature.empty for k in output_keys}

    # TODO: simplify the handling here -- technically all keys should already be in return_types
    for key in output_keys:
        if key in verified_output_types:
            inferred_output_types[key] = verified_output_types[key]
        elif (
            key not in inferred_output_types
            or inferred_output_types[key] is inspect.Signature.empty
        ):
            raise ValueError(
                f"Type for return item '{key}' is not specified in output_types and has no type annotation in function signature."
            )
    # Reject bare container types (must have type parameters)
    _BARE_CONTAINER_TYPES = {dict, list, set, tuple}
    _BARE_CONTAINER_EXAMPLES = {
        dict: "dict[str, int]",
        list: "list[int]",
        set: "set[int]",
        tuple: "tuple[int, ...]",
    }
    for name, type_annot in {**param_info, **inferred_output_types}.items():
        if type_annot in _BARE_CONTAINER_TYPES:
            example = _BARE_CONTAINER_EXAMPLES[type_annot]
            raise ValueError(
                f"Type annotation for '{name}' is bare {type_annot.__name__} "
                f"without type parameters. Use e.g. {example} instead."
            )

    return Schema(param_info, optional_fields=optional_params), Schema(
        inferred_output_types
    )


def infer_schema_from_dict(
    data: Mapping, schema: SchemaLike | None = None, default=str
) -> Schema:
    """
    Returns a Schema for the given dictionary by inferring types from values.
    If schema is provided, it is used as a base when inferring types for the fields in dict.
    """
    if schema is None:
        schema = {}
    return Schema(
        {
            key: schema.get(key, type(value) if value is not None else default)
            for key, value in data.items()
        }
    )


# def get_compatible_type(type1: Any, type2: Any) -> Any:
#     if type1 is type2:
#         return type1
#     if issubclass(type1, type2):
#         return type2
#     if issubclass(type2, type1):
#         return type1
#     raise TypeError(f"Types {type1} and {type2} are not compatible")


def get_compatible_type(type1: Any, type2: Any) -> Any:
    # Handle identical types
    if type1 is type2:
        return type1

    # Handle equal types (e.g., two separate `int | None` union objects)
    if type1 == type2:
        return type1

    # Handle None/NoneType
    if type1 is type(None) or type2 is type(None):
        # You might want to handle Optional types here
        if type1 is type(None):
            return type2
        return type1

    # Get origins for generic types (e.g., list from list[int])
    origin1 = get_origin(type1) or type1
    origin2 = get_origin(type2) or type2

    # If origins are different, check basic subclass relationship
    if origin1 != origin2:
        try:
            if issubclass(origin1, origin2):
                return type2
            if issubclass(origin2, origin1):
                return type1
        except TypeError:
            # issubclass fails on some special forms
            pass
        raise TypeError(f"Types {type1} and {type2} are not compatible")

    # Same origin - check type arguments
    args1 = get_args(type1)
    args2 = get_args(type2)

    # If no type arguments, return the origin
    if not args1 and not args2:
        return origin1

    # If only one has type arguments, prefer the more specific one
    if not args1:
        return type2
    if not args2:
        return type1

    # Both have type arguments - recursively check compatibility
    if len(args1) != len(args2):
        raise TypeError(f"Types {type1} and {type2} have incompatible argument counts")

    compatible_args = []
    for arg1, arg2 in zip(args1, args2):
        try:
            compatible_args.append(get_compatible_type(arg1, arg2))
        except TypeError:
            raise TypeError(
                f"Types {type1} and {type2} have incompatible type arguments"
            )

    # Reconstruct the generic type
    if sys.version_info >= (3, 9):
        return origin1[tuple(compatible_args)]
    else:
        # For Python < 3.9, you might need to use typing._GenericAlias
        from typing import _GenericAlias

        return _GenericAlias(origin1, tuple(compatible_args))


def union_schemas(*schemas: SchemaLike) -> Schema:
    """Merge multiple schemas, raising an error if type conflicts are found."""
    merged = dict(schemas[0])
    for schema in schemas[1:]:
        for key, right_type in schema.items():
            merged[key] = (
                get_compatible_type(merged[key], right_type)
                if key in merged
                else right_type
            )
    return Schema(merged)


def intersection_schemas(*schemas: SchemaLike) -> Schema:
    """
    Returns the intersection of all schemas, only returning keys that are present in all schemas.
    If a key is present in multiple schemas, the types must be compatible.
    """
    common_keys = set(schemas[0].keys())
    for schema in schemas[1:]:
        common_keys.intersection_update(schema.keys())

    intersection = {k: schemas[0][k] for k in common_keys}
    for schema in schemas[1:]:
        for key in common_keys:
            try:
                intersection[key] = get_compatible_type(intersection[key], schema[key])
            except TypeError:
                raise TypeError(
                    f"Type conflict for key '{key}': {intersection[key]} vs {schema[key]}"
                )
    return Schema(intersection)
