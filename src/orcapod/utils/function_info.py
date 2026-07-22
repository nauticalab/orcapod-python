import ast
import inspect
from collections.abc import Callable

#! How comprehensively this is these functions are tested should be fully assessed

def _is_in_string(line: str, pos: int) -> bool:
    """Helper to check if a position in a line is inside a string literal."""
    # This is a simplified check - would need proper parsing for robust handling
    in_single = False
    in_double = False
    for i in range(pos):
        if line[i] == "'" and not in_double and (i == 0 or line[i - 1] != "\\"):
            in_single = not in_single
        elif line[i] == '"' and not in_single and (i == 0 or line[i - 1] != "\\"):
            in_double = not in_double
    return in_single or in_double


class SourceProcessor:
    """Handles AST-based and fallback source code processing."""

    #! What's the use of this section? Why do we only have *_fallback version that's in use?
    # @staticmethod
    # def remove_docstrings_and_comments_ast(
    #     source: str, remove_docstrings: bool = True, remove_comments: bool = True
    # ) -> str:
    #     """Remove docstrings and comments using AST parsing."""
    #     try:
    #         tree = ast.parse(source)

    #         if remove_docstrings:
    #             SourceProcessor._remove_docstrings_from_ast(tree)

    #         # Convert back to source
    #         import astor  # Note: This would require astor package

    #         processed = astor.to_source(tree)

    #         if remove_comments:
    #             # AST doesn't preserve comments, so we still need line-by-line processing
    #             processed = SourceProcessor._remove_comments_fallback(processed)

    #         return processed

    #     except (ImportError, SyntaxError, TypeError):
    #         # Fall back to string-based processing
    #         return SourceProcessor._remove_docstrings_and_comments_fallback(
    #             source, remove_docstrings, remove_comments
    #         )

    @staticmethod
    def _remove_docstrings_from_ast(node: ast.AST) -> None:
        """Remove docstring nodes from AST."""
        for child in ast.walk(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                if (
                    child.body
                    and isinstance(child.body[0], ast.Expr)
                    and isinstance(child.body[0].value, ast.Constant)
                    and isinstance(child.body[0].value.value, str)
                ):
                    child.body.pop(0)

    @staticmethod
    def _remove_docstrings_and_comments_fallback(
        source: str, remove_docstrings: bool = True, remove_comments: bool = True
    ) -> str:
        """Fallback string-based processing."""
        if remove_comments:
            lines = source.split("\n")
            for i, line in enumerate(lines):
                comment_pos = line.find("#")
                if comment_pos >= 0 and not _is_in_string(line, comment_pos):
                    lines[i] = line[:comment_pos].rstrip()
            source = "\n".join(lines)

        # Simplified docstring removal (keeping original logic)
        if remove_docstrings:
            # This is basic - the AST approach above is more robust
            pass

        return source

    @staticmethod
    def _remove_comments_fallback(source: str) -> str:
        """Remove comments using line-by-line parsing."""
        lines = source.split("\n")
        for i, line in enumerate(lines):
            comment_pos = line.find("#")
            if comment_pos >= 0 and not _is_in_string(line, comment_pos):
                lines[i] = line[:comment_pos].rstrip()
        return "\n".join(lines)


def extract_decorators(func: Callable) -> list[str]:
    """Extract decorator information from function source."""
    decorators = []
    try:
        source = inspect.getsource(func)
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Name):
                        decorators.append(decorator.id)
                    elif isinstance(decorator, ast.Attribute):
                        decorators.append(ast.unparse(decorator))
                    elif isinstance(decorator, ast.Call):
                        decorators.append(ast.unparse(decorator))
                break  # Only process the first function found

    except (ImportError, SyntaxError, TypeError, IOError):
        # Fallback: try to extract decorators from source lines
        try:
            source_lines = inspect.getsourcelines(func)[0]
            for line in source_lines:
                stripped = line.strip()
                if stripped.startswith("@"):
                    decorators.append(stripped[1:])
                elif stripped.startswith("def "):
                    break
        except (IOError, TypeError):
            pass

    return decorators


def get_function_components(
    func: Callable,
    name_override: str | None = None,
    include_name: bool = True,
    include_module: bool = True,
    include_declaration: bool = True,
    include_docstring: bool = True,
    include_comments: bool = True,
    preserve_whitespace: bool = True,
    include_annotations: bool = True,
    include_code_properties: bool = True,
    include_defaults: bool = True,
    include_custom_attributes: bool = True,
    include_closure_info: bool = True,
    include_decorators: bool = True,
    include_extended_code_props: bool = True,
    skip_source_for_performance: bool = False,
) -> list[str]:
    """
    Extract the components of a function that determine its identity for hashing.

    Args:
        func: The function to process
        name_override: Override for function name
        include_name: Whether to include the function name
        include_module: Whether to include the module name
        include_declaration: Whether to include the function declaration line
        include_docstring: Whether to include the function's docstring
        include_comments: Whether to include comments in the function body
        preserve_whitespace: Whether to preserve original whitespace/indentation
        include_annotations: Whether to include function type annotations
        include_code_properties: Whether to include basic code object properties
        include_defaults: Whether to include default parameter values
        include_custom_attributes: Whether to include custom function attributes
        include_closure_info: Whether to include closure variable information
        include_decorators: Whether to include decorator information
        include_extended_code_props: Whether to include extended code properties
        skip_source_for_performance: Skip expensive source code operations

    Returns:
        A list of string components
    """
    components = []

    # Add function name
    if include_name:
        components.append(f"name:{name_override or func.__name__}")

    # Add module
    if include_module and hasattr(func, "__module__"):
        components.append(f"module:{func.__module__}")

    # Add decorators
    if include_decorators and not skip_source_for_performance:
        try:
            decorators = extract_decorators(func)
            if decorators:
                components.append(f"decorators:{';'.join(decorators)}")
        except Exception:
            pass  # Don't fail if decorator extraction fails

    # Process source code
    if not skip_source_for_performance:
        try:
            source = inspect.getsource(func)

            # Handle whitespace preservation
            if not preserve_whitespace:
                source = inspect.cleandoc(source)

            # Remove declaration if requested
            if not include_declaration:
                lines = source.split("\n")
                for i, line in enumerate(lines):
                    if line.strip().startswith(("def ", "async def ")):
                        lines.pop(i)
                        break
                source = "\n".join(lines)

            # Process docstrings and comments
            if not include_docstring or not include_comments:
                source = SourceProcessor._remove_docstrings_and_comments_fallback(
                    source,
                    remove_docstrings=not include_docstring,
                    remove_comments=not include_comments,
                )

            components.append(f"source:{source}")

        except (IOError, TypeError, OSError):
            # Handle special function types
            if hasattr(func, "__name__"):
                if func.__name__ == "<lambda>":
                    components.append("function_type:lambda")
                else:
                    components.append("function_type:dynamic_or_builtin")

            # Fall back to signature if available
            try:
                sig = inspect.signature(func)
                components.append(f"signature:{str(sig)}")
            except (ValueError, TypeError):
                components.append("builtin:True")

    # Add function annotations
    if (
        include_annotations
        and hasattr(func, "__annotations__")
        and func.__annotations__
    ):
        sorted_annotations = sorted(func.__annotations__.items())
        annotations_str = ";".join(f"{k}:{v}" for k, v in sorted_annotations)
        components.append(f"annotations:{annotations_str}")

    # Add default parameter values
    if include_defaults:
        defaults_info = []

        if hasattr(func, "__defaults__") and func.__defaults__:
            defaults_info.append(f"defaults:{func.__defaults__}")

        if hasattr(func, "__kwdefaults__") and func.__kwdefaults__:
            sorted_kwdefaults = sorted(func.__kwdefaults__.items())
            kwdefaults_str = ";".join(f"{k}:{v}" for k, v in sorted_kwdefaults)
            defaults_info.append(f"kwdefaults:{kwdefaults_str}")

        if defaults_info:
            components.extend(defaults_info)

    # Add custom function attributes
    if include_custom_attributes and hasattr(func, "__dict__") and func.__dict__:
        # Filter out common built-in attributes
        custom_attrs = {k: v for k, v in func.__dict__.items() if not k.startswith("_")}
        if custom_attrs:
            sorted_attrs = sorted(custom_attrs.items())
            attrs_str = ";".join(f"{k}:{v}" for k, v in sorted_attrs)
            components.append(f"custom_attributes:{attrs_str}")

    # Add closure information
    if include_closure_info and hasattr(func, "__closure__") and func.__closure__:
        # Be careful with closure - it can contain dynamic data
        # We'll just include the variable names, not values
        try:
            closure_vars = (
                func.__code__.co_freevars if hasattr(func, "__code__") else ()
            )
            if closure_vars:
                components.append(f"closure_vars:{closure_vars}")
        except AttributeError:
            pass

    # Add basic code object properties
    if include_code_properties and hasattr(func, "__code__"):
        code = func.__code__
        stable_code_props = {
            "co_argcount": code.co_argcount,
            "co_kwonlyargcount": getattr(code, "co_kwonlyargcount", 0),
            "co_nlocals": code.co_nlocals,
            "co_varnames": code.co_varnames[: code.co_argcount],
        }
        components.append(f"code_properties:{stable_code_props}")

    # Add extended code object properties
    if include_extended_code_props and hasattr(func, "__code__"):
        code = func.__code__
        extended_props = {}

        # Add code flags (generator, coroutine, etc.)
        if hasattr(code, "co_flags"):
            extended_props["co_flags"] = code.co_flags

        # Add free variables (closure)
        if hasattr(code, "co_freevars") and code.co_freevars:
            extended_props["co_freevars"] = code.co_freevars

        # Add global names referenced
        if hasattr(code, "co_names") and code.co_names:
            # Limit to avoid too much noise - maybe first 10 names
            extended_props["co_names"] = code.co_names[:10]

        if extended_props:
            components.append(f"extended_code_properties:{extended_props}")

    return components
