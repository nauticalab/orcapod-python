import importlib
from typing import Any


def parse_objectspec(
    obj_spec: Any,
    ref_lut: dict[str, Any] | None = None,
    validate: bool = True,
) -> Any:
    """Enhanced ObjectSpec with better error handling and validation."""
    if ref_lut is None:
        ref_lut = {}

    if isinstance(obj_spec, dict):
        if "_class" in obj_spec:
            return _create_instance_from_spec(obj_spec, ref_lut, validate)
        elif "_ref" in obj_spec:
            ref_key = obj_spec["_ref"]
            if ref_key in ref_lut:
                return ref_lut[ref_key]
            else:
                raise ValueError(f"Unknown reference: {ref_key}")
        elif "_type" in obj_spec:
            return _resolve_type_from_spec(obj_spec)
        else:
            # Recursively process dict
            return {
                k: parse_objectspec(v, ref_lut, validate) for k, v in obj_spec.items()
            }

    elif isinstance(obj_spec, (list, tuple)):
        processed = [parse_objectspec(item, ref_lut, validate) for item in obj_spec]
        return tuple(processed) if isinstance(obj_spec, tuple) else processed

    else:
        return obj_spec


def _resolve_type_from_spec(spec: dict[str, Any]) -> type:
    """Resolve a ``{"_type": "module.ClassName"}`` spec to the actual Python type.

    Bare names without a dot (e.g. ``"bytes"``) are resolved from ``builtins``.
    """
    type_str: str = spec["_type"]
    if "." not in type_str:
        type_str = f"builtins.{type_str}"
    module_name, _, attr_name = type_str.rpartition(".")
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def _create_instance_from_spec(
    spec: dict[str, Any], ref_lut: dict[str, Any], validate: bool
) -> Any:
    """Create instance with better error handling."""
    try:
        class_path = spec["_class"]
        config = spec.get("_config", {})

        # Import and validate class exists
        module_name, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)

        # Process config recursively
        processed_config = parse_objectspec(config, ref_lut, validate)

        # Optional: validate config matches class signature
        if validate:
            _validate_config_for_class(cls, processed_config)

        return cls(**processed_config)

    except Exception as e:
        raise ValueError(f"Failed to create instance from spec {spec}: {e}") from e


def _validate_config_for_class(cls: type, config: dict[str, Any]) -> None:
    """Optional validation that config matches class signature."""
    import inspect

    try:
        sig = inspect.signature(cls.__init__)
        valid_params = set(sig.parameters.keys()) - {"self"}
        invalid_params = set(config.keys()) - valid_params

        if invalid_params:
            raise ValueError(f"Invalid parameters for {cls.__name__}: {invalid_params}")

    except Exception:
        # Skip validation if introspection fails
        pass


# def parse_objectspec(obj_spec: Any) -> Any:
#     if isinstance(obj_spec, dict):
#         if "_class" in obj_spec:
#             # if _class is specified, treat the dict as an object specification, looking for
#             # _config key to extract configuration parameters
#             module_name, class_name = obj_spec["_class"].rsplit(".", 1)
#             module = importlib.import_module(module_name)
#             cls = getattr(module, class_name)
#             configs = parse_objectspec(obj_spec.get("_config", {}))
#             return cls(**configs)
#         else:
#             # otherwise, parse through the dictionary recursively
#             parsed_object = obj_spec
#             for k, v in obj_spec.items():
#                 parsed_object[k] = parse_objectspec(v)
#             return parsed_object
#     elif isinstance(obj_spec, list):
#         # if it's a list, parse each item in the list
#         return [parse_objectspec(item) for item in obj_spec]
#     elif isinstance(obj_spec, tuple):
#         # if it's a tuple, parse each item in the tuple
#         return tuple(parse_objectspec(item) for item in obj_spec)
#     else:
#         # if it's neither a dict nor a list, return it as is
#         return obj_spec
