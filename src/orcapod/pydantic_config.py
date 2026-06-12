"""Pydantic-backed config loading for orcapod pipelines (ENG-601 / ENG-607).

Provides `load_pydantic_config` (validate a YAML file against a pydantic model)
and `OrcapodBaseConfig` (a strict base for config schemas). A companion
`PydanticModelConverter` (also in this module) makes a validated model a
first-class, content-hashed orcapod value.
"""

from __future__ import annotations

from pathlib import Path
from typing import TypeVar

import pydantic
import yaml

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

    try:
        return model_cls.model_validate(data)
    except pydantic.ValidationError as e:
        raise ValueError(f"Config validation failed for {path}:\n{e}") from e
