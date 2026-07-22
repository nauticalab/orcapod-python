"""
Utility functions for handling names
"""

import re


#!? Stale TODO: these functions ARE already in utils/. Remove.
# TODO: move these functions to util
def escape_with_postfix(field: str, postfix=None, separator="_") -> str:
    """
    Escape the field string by doubling separators and optionally append a postfix.
    This function takes a field string and escapes any occurrences of the separator
    by doubling them, then optionally appends a postfix with a separator prefix.

    Args:
        field (str): The input string containing to be escaped.
        postfix (str, optional): An optional postfix to append to the escaped string.
                               If None, no postfix is added. Defaults to None.
        separator (str, optional): The separator character to escape and use for
                                 prefixing the postfix. Defaults to "_".
    Returns:
        str: The escaped string with optional postfix. Returns empty string if
             fields is provided but postfix is None.
    Examples:
        >>> escape_with_postfix("field1_field2", "suffix")
        'field1__field2_suffix'
        >>> escape_with_postfix("name_age_city", "backup", "_")
        'name__age__city_backup'
        >>> escape_with_postfix("data-info", "temp", "-")
        'data--info-temp'
        >>> escape_with_postfix("simple", None)
        'simple'
        >>> escape_with_postfix("no_separators", "end")
        'no__separators_end'
    """
    #!? BUG (verified): the postfix delimiter is hardcoded to "_" (`f"_{postfix}"`), NOT `separator`.
    #!? So docstring examples with separator="-" are WRONG: escape("data-info","temp","-") actually
    #!? returns 'data--info_temp' (not 'data--info-temp'), and unescape can't recover the postfix.
    #!? Worse, latent corruption when separator != "_" and a field contains "_":
    #!? unescape("field1--field2-meta", separator="-") → ('field1-field2-meta', None), not (...,'meta').
    #!? Fix: use `separator` for the postfix delimiter too (f"{separator}{postfix}"), or fix the docs
    #!? to state postfix is always "_"-delimited. Also the Returns line ("empty string if postfix is
    #!? None") is wrong — it returns the escaped field.
    return field.replace(separator, separator * 2) + (f"_{postfix}" if postfix else "")


def unescape_with_postfix(field: str, separator="_") -> tuple[str, str | None]:
    """
    Unescape a string by converting double separators back to single separators and extract postfix metadata.
    This function reverses the escaping process where single separators were doubled to avoid
    conflicts with metadata delimiters. It splits the input on double separators, then extracts
    any postfix metadata from the last part.

    Args:
        field (str): The escaped string containing doubled separators and optional postfix metadata
        separator (str, optional): The separator character used for escaping. Defaults to "_"
    Returns:
        tuple[str, str | None]: A tuple containing:
            - The unescaped string with single separators restored
            - The postfix metadata if present, None otherwise
    Examples:
        >>> unescape_with_postfix("field1__field2__field3")
        ('field1_field2_field3', None)
        >>> unescape_with_postfix("field1__field2_metadata")
        ('field1_field2', 'metadata')
        >>> unescape_with_postfix("simple")
        ('simple', None)
        >>> unescape_with_postfix("field1--field2", separator="-")
        ('field1-field2', None)
        >>> unescape_with_postfix("field1--field2-meta", separator="-")
        ('field1-field2', 'meta')
    """

    parts = field.split(separator * 2)
    parts[-1], *meta = parts[-1].split("_", 1)
    return separator.join(parts), meta[0] if meta else None


def find_noncolliding_name(name: str, lut: dict) -> str:
    """
    Generate a unique name that does not collide with existing keys in a lookup table (lut).

    If the given name already exists in the lookup table, a numeric suffix is appended
    to the name (e.g., "name_1", "name_2") until a non-colliding name is found.

    Parameters:
        name (str): The base name to check for collisions.
        lut (dict): A dictionary representing the lookup table of existing names.

    Returns:
        str: A unique name that does not collide with any key in the lookup table.

    Example:
        >>> lut = {"name": 1, "name_1": 2}
        >>> find_noncolliding_name("name", lut)
        'name_2'
    """
    if name not in lut:
        return name

    suffix = 1
    while f"{name}_{suffix}" in lut:
        suffix += 1

    return f"{name}_{suffix}"


def pascal_to_snake(name: str) -> str:
    # Convert PascalCase to snake_case
    # if already in snake_case, return as is
    # TODO: replace this crude check with a more robust one
    if "_" in name:
        # put everything into lowercase and return
        return name.lower()
    # Replace uppercase letters with underscore followed by lowercase letter
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def snake_to_pascal(name: str) -> str:
    # Convert snake_case to PascalCase
    # if already in PascalCase, return as is
    if "_" not in name:
        # capitalize the name and return
        return name.capitalize()
    # Split the string by underscores and capitalize each component
    components = name.split("_")
    return "".join(x.title() for x in components)


# def get_function_signature(func):
#     """
#     Returns a string representation of how the function arguments were defined.
#     Example output: f(a, b, c, d=0, **kwargs)
#     """
#     sig = inspect.signature(func)
#     function_name = func.__name__

#     param_strings = []

#     for name, param in sig.parameters.items():
#         # Handle different parameter kinds
#         if param.kind == param.POSITIONAL_ONLY:
#             formatted_param = f"{name}, /"
#         elif param.kind == param.POSITIONAL_OR_KEYWORD:
#             if param.default is param.empty:
#                 formatted_param = name
#             else:
#                 # Format the default value
#                 default = repr(param.default)
#                 formatted_param = f"{name}={default}"
#         elif param.kind == param.VAR_POSITIONAL:
#             formatted_param = f"*{name}"
#         elif param.kind == param.KEYWORD_ONLY:
#             if param.default is param.empty:
#                 formatted_param = f"*, {name}"
#             else:
#                 default = repr(param.default)
#                 formatted_param = f"{name}={default}"
#         elif param.kind == param.VAR_KEYWORD:
#             formatted_param = f"**{name}"

#         param_strings.append(formatted_param)

#     params_str = ", ".join(param_strings)
#     return f"{function_name}({params_str})"
