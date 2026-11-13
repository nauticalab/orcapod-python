"""Helpers for value types that are registered on first use (ITL-871).

A registry can name an optional value type (such as ``torch.Tensor``) by the
qualified name of its class and by its Arrow extension name, without importing
the library. The type is loaded when a lookup first needs it:

* by Python type: a class in the looked-up type's MRO has the deferred
  qualified name. Comparing names never imports anything, and such a class can
  only exist once its library has been imported.
* by Arrow extension name: reading a stored column. The library is imported if
  it is installed; otherwise ``MissingOptionalDependencyError`` names it.
"""

from __future__ import annotations

import importlib
import importlib.util
from typing import Any


def qualified_name(cls: type) -> str:
    """Return ``module.qualname`` of a class.

    Args:
        cls: Any class.

    Returns:
        The class's module and qualified name joined by a dot, for example
        ``"torch.Tensor"``.
    """
    return f"{cls.__module__}.{cls.__qualname__}"


def load_object(dotted: str) -> Any:
    """Import and return the object named by a dotted path.

    Args:
        dotted: ``"package.module.Name"``.

    Returns:
        The attribute ``Name`` of the imported module.
    """
    module_name, _, attr = dotted.rpartition(".")
    return getattr(importlib.import_module(module_name), attr)


def module_available(module_name: str) -> bool:
    """Return whether a top-level module can be imported, without importing it.

    Args:
        module_name: A module name such as ``"torch"``.

    Returns:
        True when the import system can find the module.
    """
    try:
        return importlib.util.find_spec(module_name) is not None
    except (ImportError, ValueError):
        return False
