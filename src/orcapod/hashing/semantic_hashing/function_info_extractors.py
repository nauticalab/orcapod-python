from orcapod.protocols.hashing_protocols import FunctionInfoExtractor
from collections.abc import Callable
from typing import Any, Literal
from orcapod.types import Schema
import inspect


class FunctionNameExtractor:
    """
    Extractor that only uses the function name for information extraction.
    """

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
    """
    Extractor that uses the function signature for information extraction.
    """

    def __init__(self, include_module: bool = True, include_defaults: bool = True):
        self.include_module = include_module
        self.include_defaults = include_defaults

    # FIXME: Fix this implementation!!
    # BUG: Currently this is not using the input_types and output_types parameters
    def extract_function_info(
        self,
        func: Callable[..., Any],
        function_name: str | None = None,
        input_typespec: Schema | None = None,
        output_typespec: Schema | None = None,
    ) -> dict[str, Any]:
        if not callable(func):
            raise TypeError("Provided object is not callable")

        sig = inspect.signature(func)

        # Build the signature string
        parts = {}

        # Add module if requested
        if self.include_module and hasattr(func, "__module__"):
            parts["module"] = func.__module__

        # Add function name
        parts["name"] = function_name or func.__name__

        # Add parameters
        param_strs = []
        for name, param in sig.parameters.items():
            param_str = str(param)
            if not self.include_defaults and "=" in param_str:
                param_str = param_str.split("=")[0].strip()

            param_strs.append(param_str)

        parts["params"] = ", ".join(param_strs)

        # Add return annotation if present
        if sig.return_annotation is not inspect.Signature.empty:
            parts["returns"] = sig.return_annotation

        return parts


class FunctionInfoExtractorFactory:
    """Factory for creating various extractor combinations."""

    @staticmethod
    def create_function_info_extractor(
        strategy: Literal["name", "signature"] = "signature",
    ) -> FunctionInfoExtractor:
        """Create a basic composite extractor."""
        if strategy == "name":
            return FunctionNameExtractor()
        elif strategy == "signature":
            return FunctionSignatureExtractor()
        else:
            raise ValueError(
                f"Unknown strategy: {strategy}. Use 'name' or 'signature'."
            )
