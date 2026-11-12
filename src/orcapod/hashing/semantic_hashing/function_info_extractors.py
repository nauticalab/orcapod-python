import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from orcapod.hashing.hash_utils import canonical_annotation_str
from orcapod.protocols.hashing_protocols import FunctionInfoExtractorProtocol
from orcapod.types import Schema

if TYPE_CHECKING:
    from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol


@dataclass(frozen=True)
class ParameterDefault:
    """A parameter's default value, not yet hashed.

    ``FunctionSignatureExtractor`` has no hasher, so it reports each default wrapped
    in this class and ``FunctionHandler`` replaces it with the semantic hasher's hash
    of the value. The structure hasher has no handler for this class: a description
    whose defaults were not replaced is refused instead of being hashed with a raw
    value in it.

    Args:
        value: The default value as written in the function's signature.
    """

    value: Any


def _parameter_entry(
    param: inspect.Parameter,
    canonical_annotation: str | None,
    include_default: bool,
) -> dict[str, Any]:
    """Describe one parameter as a structured entry.

    Args:
        param: The parameter to describe.
        canonical_annotation: Canonical text of the annotation, or ``None`` when the
            parameter carries no annotation.
        include_default: Whether to report the default value.

    Returns:
        ``{"name", "kind"}`` plus ``"annotation"`` when the parameter is annotated
        and ``"default"`` (a ``ParameterDefault``) when it has a default and
        ``include_default`` is true. ``kind`` is the name of the parameter's
        ``inspect`` kind (``"POSITIONAL_ONLY"``, ``"POSITIONAL_OR_KEYWORD"``,
        ``"VAR_POSITIONAL"``, ``"KEYWORD_ONLY"`` or ``"VAR_KEYWORD"``). An absent key
        is different from any value: a default of ``None`` is not "no default".
    """
    entry: dict[str, Any] = {"name": param.name, "kind": param.kind.name}
    if canonical_annotation is not None:
        entry["annotation"] = canonical_annotation
    if include_default and param.default is not inspect.Parameter.empty:
        entry["default"] = ParameterDefault(param.default)
    return entry


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
    are replaced with their logical type identifier text
    (``"orcapod.file;7b2276657273696f6e223a317d"``: Arrow extension name,
    ``;``, extension metadata in hex) rather than the full import path (e.g.
    ``"orcapod.logical_types.file_type.File"``).  Internal module
    reorganisations therefore do not invalidate cached function-pod signatures;
    a change of the type's identifier does.

    Each **parameter** is one structured entry (see ``_parameter_entry``): its
    name, its kind, the canonical text of its annotation and its default value. A
    default is reported as a ``ParameterDefault``; ``FunctionHandler`` replaces it
    with the semantic hasher's hash of the value, so a default never enters the
    hash as its ``repr()`` (ITL-953).

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

        # One structured entry per parameter, in declared order: name, kind,
        # canonical annotation and default. Nothing is formatted into text, so a
        # default's value can never be confused with part of an annotation.
        parameters = []
        for param in sig.parameters.values():
            annotation = param.annotation
            canonical_ann = (
                canonical_annotation_str(annotation, tc)
                if annotation is not inspect.Parameter.empty
                else None
            )
            parameters.append(
                _parameter_entry(param, canonical_ann, self.include_defaults)
            )

        parts["parameters"] = parameters

        # Add return annotation if present.
        # The raw annotation object is stored here and hashed by the type
        # handler registry — TypeObjectHandler (configured with a type_converter)
        # canonicalises registered logical types to their identifier
        # (``"type:<extension name>;<hex metadata>"``), so no special
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
