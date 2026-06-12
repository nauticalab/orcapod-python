"""Pydantic-backed config loading for orcapod pipelines (ENG-601 / ENG-607).

Provides `load_pydantic_config` (validate a YAML file against a pydantic model)
and `OrcapodBaseConfig` (a strict base for config schemas). A companion
`PydanticModelConverter` (also in this module) makes a validated model a
first-class, content-hashed orcapod value.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

import pydantic
import yaml

from orcapod.semantic_types.semantic_struct_converters import SemanticStructConverterBase

if TYPE_CHECKING:
    import pyarrow as pa

M = TypeVar("M", bound=pydantic.BaseModel)


class OrcapodBaseConfig(pydantic.BaseModel):
    """Recommended base for pipeline config schemas.

    Defaults to strict validation: unknown keys are rejected and instances are
    immutable. Subclass this for pipeline configs; subclass `pydantic.BaseModel`
    directly only when different semantics are required.
    """

    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)


def load_pydantic_config(path: str | Path, model_cls: type[M]) -> M:
    """Read a YAML file and validate it against a pydantic model.

    Args:
        path: Path to the YAML config file.
        model_cls: The pydantic model class to validate against.

    Returns:
        A validated instance of `model_cls`.

    Raises:
        ValueError: If the YAML cannot be parsed or fails validation. The error
            message includes the file path and the underlying field-level detail.
    """
    path = Path(path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Could not parse YAML config {path}: {e}") from e
    except OSError as e:
        raise ValueError(f"Could not read YAML config {path}: {e}") from e

    try:
        return model_cls.model_validate(data)
    except pydantic.ValidationError as e:
        raise ValueError(f"Config validation failed for {path}:\n{e}") from e


# Arrow struct field names for the serialized config.
_MODEL_FIELD = "__pydantic_model__"  # fully-qualified "module:QualName"
_JSON_FIELD = "__pydantic_json__"    # canonical JSON of the model


def _qualified_name(cls: type) -> str:
    return f"{cls.__module__}:{cls.__qualname__}"


def _import_model(qualified_name: str) -> type[pydantic.BaseModel]:
    module_path, _, qualname = qualified_name.partition(":")
    module = importlib.import_module(module_path)
    obj: Any = module
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj


class PydanticModelConverter(SemanticStructConverterBase):
    """Semantic-type converter for pydantic models.

    Maps any `pydantic.BaseModel` instance to an Arrow struct holding the
    model's fully-qualified class name and its canonical JSON, and back. Content
    is hashed over (class name + canonical JSON), so identity tracks the config's
    meaning rather than source-file formatting. Modeled on `PythonPathStructConverter`.
    """

    def __init__(self) -> None:
        super().__init__("pydantic")
        import pyarrow as pa

        self._arrow_struct_type = pa.struct(
            [
                pa.field(_MODEL_FIELD, pa.large_string()),
                pa.field(_JSON_FIELD, pa.large_string()),
            ]
        )

    @property
    def python_type(self) -> type:
        return pydantic.BaseModel

    @property
    def arrow_struct_type(self) -> Any:
        return self._arrow_struct_type

    def can_handle_python_type(self, python_type: type) -> bool:
        return isinstance(python_type, type) and issubclass(
            python_type, pydantic.BaseModel
        )

    def can_handle_struct_type(self, struct_type: Any) -> bool:
        import pyarrow as pa

        if not pa.types.is_struct(struct_type):
            return False
        for field in self._arrow_struct_type:
            if (
                field.name not in struct_type.names
                or struct_type[field.name].type != field.type
            ):
                return False
        return True

    def is_semantic_struct(self, struct_dict: dict[str, Any]) -> bool:
        return set(struct_dict.keys()) == {_MODEL_FIELD, _JSON_FIELD}

    def python_to_struct_dict(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, pydantic.BaseModel):
            raise TypeError(f"Expected a pydantic BaseModel, got {type(value)}")
        return {
            _MODEL_FIELD: _qualified_name(type(value)),
            _JSON_FIELD: value.model_dump_json(),
        }

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> Any:
        qualified_name = struct_dict.get(_MODEL_FIELD)
        json_str = struct_dict.get(_JSON_FIELD)
        if qualified_name is None or json_str is None:
            raise ValueError(
                f"Missing '{_MODEL_FIELD}'/'{_JSON_FIELD}' in struct dict"
            )
        model_cls = _import_model(qualified_name)
        return model_cls.model_validate_json(json_str)

    def hash_struct_dict(
        self, struct_dict: dict[str, Any], add_prefix: bool = False
    ) -> str:
        qualified_name = struct_dict.get(_MODEL_FIELD)
        json_str = struct_dict.get(_JSON_FIELD)
        if qualified_name is None or json_str is None:
            raise ValueError(
                f"Missing '{_MODEL_FIELD}'/'{_JSON_FIELD}' in struct dict"
            )
        content = f"{qualified_name}\n{json_str}".encode("utf-8")
        content_hash = self._compute_content_hash(content)
        return self._format_hash_string(content_hash.digest, add_prefix=add_prefix)
