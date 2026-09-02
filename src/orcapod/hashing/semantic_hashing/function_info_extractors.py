import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from orcapod.hashing.hash_utils import canonical_annotation_str, is_union_annotation
from orcapod.protocols.hashing_protocols import FunctionInfoExtractorProtocol
from orcapod.types import Schema

if TYPE_CHECKING:
    from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol


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

    For **parameter** annotations the canonical string replaces the annotation
    substring in the ``str(param)`` representation (via ``canonical_annotation_str``).

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
        # type objects before we check for union types.
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

        # Add parameters, replacing annotation substrings with canonical forms
        param_strs = []
        for name, param in sig.parameters.items():
            param_str = str(param)
            annotation = param.annotation
            if annotation is not inspect.Parameter.empty:
                old_ann = inspect.formatannotation(annotation)
                new_ann = canonical_annotation_str(annotation, tc)
                if old_ann != new_ann:
                    # Replace ": <old_ann>" with ": <new_ann>" (first occurrence
                    # only).  The ": " prefix avoids accidentally replacing the
                    # annotation string inside a default value.
                    param_str = param_str.replace(f": {old_ann}", f": {new_ann}", 1)
            if not self.include_defaults and "=" in param_str:
                param_str = param_str.split("=")[0].strip()
            param_strs.append(param_str)

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
