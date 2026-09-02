import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from orcapod.hashing.hash_utils import canonical_annotation_str
from orcapod.protocols.hashing_protocols import FunctionInfoExtractorProtocol
from orcapod.types import Schema

if TYPE_CHECKING:
    from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol


def _format_param(
    param: inspect.Parameter,
    canonical_annotation: str | None,
    include_defaults: bool,
) -> str:
    """Reconstruct a parameter string from its structured components.

    Produces output identical to ``str(inspect.Parameter)`` for normal inputs,
    but substitutes ``canonical_annotation`` for the raw annotation string without
    any string search/replace.  This avoids two failure modes of the substitution
    approach:

    1. Accidentally matching the annotation string inside a complex default
       value's ``repr`` (e.g. a dataclass whose repr embeds the type name).
    2. Truncating annotations that contain ``=`` (e.g. ``Literal["a=b"]``) when
       stripping defaults via ``str.split("=")``.

    The format follows CPython's ``inspect.Parameter.__str__`` exactly:

    - With annotation and default:  ``name: type = repr(default)``
    - With annotation, no default:  ``name: type``
    - No annotation, with default:  ``name=repr(default)``  (no spaces around ``=``)
    - No annotation, no default:    ``name``
    - ``*args`` / ``**kwargs`` get the corresponding prefix.

    Args:
        param: The parameter to format.
        canonical_annotation: Canonical string for the annotation, or ``None``
            when the parameter carries no annotation.
        include_defaults: Whether to include the default value.

    Returns:
        A formatted parameter string.
    """
    if param.kind == inspect.Parameter.VAR_POSITIONAL:
        prefix = "*"
    elif param.kind == inspect.Parameter.VAR_KEYWORD:
        prefix = "**"
    else:
        prefix = ""

    base = f"{prefix}{param.name}"
    has_default = include_defaults and param.default is not inspect.Parameter.empty

    if canonical_annotation is not None:
        # "name: type" or "name: type = default"
        if has_default:
            return f"{base}: {canonical_annotation} = {repr(param.default)}"
        return f"{base}: {canonical_annotation}"
    else:
        # "name" or "name=default"  (CPython omits spaces around = without annotation)
        if has_default:
            return f"{base}={repr(param.default)}"
        return base


class FunctionNameExtractor:
    """Extractor that only uses the function name for information extraction."""

    def extract_function_info(
        self,
        func: Callable[..., Any],
        function_name: str | None = None,
        input_typespec: Schema | None = None,
        output_typespec: Schema | None = None,
    ) -> dict[str, Any]:
        if not callable(func):
            raise TypeError("Provided object is not callable")
        function_name = function_name or getattr(func, "__name__", str(func))
        return {"name": function_name}


class FunctionSignatureExtractor:
    """Extractor that uses the function signature for information extraction.

    When a ``type_converter`` is provided, orcapod logical types in annotations
    are replaced with their stable ``logical_type_name``
    (e.g. ``"orcapod.file"``) rather than the full import path (e.g.
    ``"orcapod.logical_types.file_type.File"``).  This prevents internal
    module reorganisations from invalidating cached function-pod signatures.

    For **parameter** annotations each parameter string is reconstructed from
    its structured components (name, kind, canonical annotation, default) via
    ``_format_param``, avoiding any string search/replace.

    For **return** annotations the raw annotation object is stored unchanged.
    Canonicalisation happens at hash time: ``TypeObjectHandler`` (wired with the
    same ``type_converter``) resolves registered types to their stable name, so
    both registered and unregistered return types are handled consistently
    without any special casing here.
    """

    def __init__(
        self,
        include_module: bool = True,
        include_defaults: bool = True,
        type_converter: "TypeConverterProtocol | None" = None,
    ):
        self.include_module = include_module
        self.include_defaults = include_defaults
        self._type_converter = type_converter

    def extract_function_info(
        self,
        func: Callable[..., Any],
        function_name: str | None = None,
        input_typespec: Schema | None = None,
        output_typespec: Schema | None = None,
    ) -> dict[str, Any]:
        if not callable(func):
            raise TypeError("Provided object is not callable")

        # Use eval_str=True so that string annotations produced by
        # ``from __future__ import annotations`` (PEP 563) are resolved to live
        # type objects before we canonicalise them.
        try:
            sig = inspect.signature(func, eval_str=True)
        except (NameError, TypeError, AttributeError, SyntaxError):
            # Fall back to unresolved signatures when annotation evaluation fails
            # (e.g. forward references that cannot be resolved in the function's
            # module scope).
            sig = inspect.signature(func)

        parts: dict[str, Any] = {}

        # Add module if requested
        if self.include_module and hasattr(func, "__module__"):
            parts["module"] = func.__module__

        # Add function name
        parts["name"] = function_name or func.__name__

        tc = self._type_converter

        # Build each parameter string from structured components.
        # Using _format_param instead of str(param) + string substitution avoids
        # accidentally matching the annotation text inside a default value's repr,
        # and correctly handles annotations that contain '=' (e.g. Literal["a=b"]).
        param_strs = []
        for _, param in sig.parameters.items():
            annotation = param.annotation
            canonical_ann = (
                canonical_annotation_str(annotation, tc)
                if annotation is not inspect.Parameter.empty
                else None
            )
            param_strs.append(_format_param(param, canonical_ann, self.include_defaults))

        parts["params"] = ", ".join(param_strs)

        # Add return annotation if present.
        # The raw annotation object is stored here and hashed by the type
        # handler registry — TypeObjectHandler (configured with a type_converter)
        # canonicalises registered logical types to their stable
        # ``logical_type_name`` (e.g. ``"type:orcapod.file"``), so no special
        # casing is required here.
        ret_ann = sig.return_annotation
        if ret_ann is not inspect.Signature.empty:
            parts["returns"] = ret_ann

        return parts


class FunctionInfoExtractorFactory:
    """Factory for creating various extractor combinations."""

    @staticmethod
    def create_function_info_extractor(
        strategy: Literal["name", "signature"] = "signature",
    ) -> FunctionInfoExtractorProtocol:
        """Create a basic composite extractor."""
        if strategy == "name":
            return FunctionNameExtractor()
        elif strategy == "signature":
            return FunctionSignatureExtractor()
        else:
            raise ValueError(
                f"Unknown strategy: {strategy}. Use 'name' or 'signature'."
            )
