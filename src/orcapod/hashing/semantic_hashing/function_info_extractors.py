import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from orcapod.hashing.hash_utils import canonical_annotation_str, is_union_annotation
from orcapod.protocols.hashing_protocols import FunctionInfoExtractorProtocol
from orcapod.types import Schema

if TYPE_CHECKING:
    from orcapod.logical_types.registry import LogicalTypeRegistry


def _annotation_contains_registered_type(annotation: object, registry: "LogicalTypeRegistry") -> bool:
    """Return ``True`` if *annotation* or any nested type argument is registered.

    This is used to decide whether a return annotation should be stored as a
    canonical string (when it contains an orcapod logical type) or as the raw
    type object (when it only contains builtins/user types, preserving existing
    hashes).

    Args:
        annotation: A type annotation to inspect.
        registry: The ``LogicalTypeRegistry`` to consult.

    Returns:
        ``True`` if any component of the annotation is registered.
    """
    if isinstance(annotation, type):
        return registry.get_by_python_type(annotation) is not None
    # Union types: check any member
    if is_union_annotation(annotation):
        args = getattr(annotation, "__args__", ()) or ()
        return any(_annotation_contains_registered_type(a, registry) for a in args)
    # Generic aliases (list[X], dict[K, V], etc.): check args
    origin = getattr(annotation, "__origin__", None)
    if origin is not None:
        args = getattr(annotation, "__args__", None) or ()
        return any(_annotation_contains_registered_type(a, registry) for a in args)
    return False


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

    When a ``logical_type_registry`` is provided (or resolvable from the
    default context), orcapod logical types in annotations are replaced with
    their stable ``logical_type_name`` (e.g. ``"orcapod.file"``) rather than
    the full import path (e.g.
    ``"orcapod.logical_types.file_type.File"``).  This prevents internal
    module reorganisations from invalidating cached function-pod signatures.

    For **parameter** annotations the canonical string replaces the
    annotation substring in the ``str(param)`` representation.

    For **return** annotations the canonical string replaces the raw type
    object *only when the annotation contains a registered type*.  When it
    contains only builtins or user types the raw type object is kept so that
    existing cached hashes are not invalidated.
    """

    def __init__(
        self,
        include_module: bool = True,
        include_defaults: bool = True,
        logical_type_registry: "LogicalTypeRegistry | None" = None,
    ):
        self.include_module = include_module
        self.include_defaults = include_defaults
        self._logical_type_registry = logical_type_registry

    def _get_registry(self) -> "LogicalTypeRegistry | None":
        """Return the logical type registry, resolving the lazy fallback if needed."""
        if self._logical_type_registry is not None:
            return self._logical_type_registry
        from orcapod.contexts import get_default_context

        ctx = get_default_context()
        return getattr(ctx.type_converter, "_logical_type_registry", None)

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

        registry = self._get_registry()

        # Add parameters, replacing annotation substrings with canonical forms
        param_strs = []
        for name, param in sig.parameters.items():
            param_str = str(param)
            annotation = param.annotation
            if annotation is not inspect.Parameter.empty:
                old_ann = inspect.formatannotation(annotation)
                new_ann = canonical_annotation_str(annotation, registry)
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
        # When the return type contains a registered logical type, store a
        # canonical string so that module relocations don't change the hash.
        # Otherwise keep the raw type object to avoid invalidating existing
        # cached hashes (a string and a type object hash to different values).
        ret_ann = sig.return_annotation
        if ret_ann is not inspect.Signature.empty:
            if registry is not None and _annotation_contains_registered_type(ret_ann, registry):
                parts["returns"] = canonical_annotation_str(ret_ann, registry)
            else:
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
