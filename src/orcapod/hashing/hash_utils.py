import hashlib
import inspect
import logging
import zlib
from collections.abc import Callable, Collection
from pathlib import Path

import xxhash

from orcapod.types import ContentHash

logger = logging.getLogger(__name__)


def combine_hashes(
    *hashes: str,
    order: bool = False,
    prefix_hasher_id: bool = False,
    hex_char_count: int | None = None,
) -> str:
    """
    Combine multiple hash strings into a single SHA-256 hash string.

    Args:
        *hashes: Hash strings to combine.
        order: If True, sort inputs before combining so the result is
               order-independent.  If False (default), insertion order
               is preserved.
        prefix_hasher_id: If True, prefix the result with ``"sha256@"``.
        hex_char_count: Number of hex characters to return.  None (default)
                        returns the full 64-character SHA-256 hex digest.

    Returns:
        A hex string (optionally truncated / prefixed).
    """
    prepared_hashes = sorted(hashes) if order else list(hashes)
    combined = "".join(prepared_hashes)
    combined_hash = hashlib.sha256(combined.encode()).hexdigest()
    if hex_char_count is not None:
        combined_hash = combined_hash[:hex_char_count]
    if prefix_hasher_id:
        return "sha256@" + combined_hash
    return combined_hash


def hash_file(file_path, algorithm="sha256", buffer_size=65536) -> ContentHash:
    """Calculate the hash of a file using the specified algorithm.

    Args:
        file_path: Path to the file to hash.
        algorithm: Hash algorithm to use — options include:
            'md5', 'sha1', 'sha256', 'sha512', 'xxh64', 'crc32', 'hash_path'.
        buffer_size: Size of chunks to read from the file at a time.

    Returns:
        A ContentHash with method set to the algorithm name and digest
        containing the raw hash bytes.
    """
    if not Path(file_path).is_file():
        raise FileNotFoundError(f"The file {file_path} does not exist")

    # Hash the path string itself rather than file content
    if algorithm == "hash_path":
        hasher = hashlib.sha256()
        hasher.update(str(file_path).encode("utf-8"))
        return ContentHash(method=algorithm, digest=hasher.digest())

    if algorithm == "xxh64":
        hasher = xxhash.xxh64()
        with open(file_path, "rb") as file:
            while True:
                data = file.read(buffer_size)
                if not data:
                    break
                hasher.update(data)
        return ContentHash(method=algorithm, digest=hasher.digest())

    if algorithm == "crc32":
        crc = 0
        with open(file_path, "rb") as file:
            while True:
                data = file.read(buffer_size)
                if not data:
                    break
                crc = zlib.crc32(data, crc)
        return ContentHash(
            method=algorithm,
            digest=(crc & 0xFFFFFFFF).to_bytes(4, byteorder="big"),
        )

    try:
        hasher = hashlib.new(algorithm)
    except ValueError:
        valid_algorithms = ", ".join(sorted(hashlib.algorithms_available))
        raise ValueError(
            f"Invalid algorithm: {algorithm}. Available algorithms: {valid_algorithms}, xxh64, crc32"
        )

    with open(file_path, "rb") as file:
        while True:
            data = file.read(buffer_size)
            if not data:
                break
            hasher.update(data)

    return ContentHash(method=algorithm, digest=hasher.digest())


def _is_in_string(line: str, pos: int) -> bool:
    """Helper to check if a position in a line is inside a string literal."""
    in_single = False
    in_double = False
    for i in range(pos):
        if line[i] == "'" and not in_double and (i == 0 or line[i - 1] != "\\"):
            in_single = not in_single
        elif line[i] == '"' and not in_single and (i == 0 or line[i - 1] != "\\"):
            in_double = not in_double
    return in_single or in_double


def get_function_signature(
    func: Callable,
    name_override: str | None = None,
    include_defaults: bool = True,
    include_module: bool = True,
    output_names: Collection[str] | None = None,
) -> str:
    """
    Get a stable string representation of a function's signature.

    Args:
        func: The function to process.
        name_override: Override the function name in the output.
        include_defaults: Whether to include default parameter values.
        include_module: Whether to include the module name.
        output_names: Unused; reserved for future use.

    Returns:
        A string representation of the function signature.
    """
    sig = inspect.signature(func)
    parts: dict[str, object] = {}

    if include_module and hasattr(func, "__module__"):
        parts["module"] = func.__module__

    parts["name"] = name_override or func.__name__

    param_strs = []
    for name, param in sig.parameters.items():
        param_str = str(param)
        if not include_defaults and "=" in param_str:
            param_str = param_str.split("=")[0].strip()
        param_strs.append(param_str)

    parts["params"] = f"({', '.join(param_strs)})"

    if sig.return_annotation is not inspect.Signature.empty:
        parts["returns"] = sig.return_annotation

    fn_string = (
        f"{parts['module'] + '.' if 'module' in parts else ''}"
        f"{parts['name']}{parts['params']}"
    )
    if "returns" in parts:
        fn_string += f"-> {parts['returns']}"
    return fn_string


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
) -> list:
    """
    Extract the components of a function that determine its identity for hashing.

    Args:
        func: The function to process.
        name_override: Override the function name in the output.
        include_name: Whether to include the function name.
        include_module: Whether to include the module name.
        include_declaration: Whether to include the function declaration line.
        include_docstring: Whether to include the function's docstring.
        include_comments: Whether to include comments in the function body.
        preserve_whitespace: Whether to preserve original whitespace/indentation.
        include_annotations: Whether to include function type annotations.
        include_code_properties: Whether to include code object properties.

    Returns:
        A list of string components.
    """
    components = []

    if include_name:
        components.append(f"name:{name_override or func.__name__}")

    if include_module and hasattr(func, "__module__"):
        components.append(f"module:{func.__module__}")

    try:
        source = inspect.getsource(func)

        if not preserve_whitespace:
            source = inspect.cleandoc(source)

        if not include_declaration:
            lines = source.split("\n")
            for i, line in enumerate(lines):
                if line.strip().startswith(("def ", "async def ")):
                    lines.pop(i)
                    break
            source = "\n".join(lines)

        if not include_docstring and func.__doc__:
            doc_str = inspect.getdoc(func)
            doc_lines = doc_str.split("\n") if doc_str else []
            doc_pattern = '"""' + "\\n".join(doc_lines) + '"""'
            if doc_pattern not in source:
                doc_pattern = "'''" + "\\n".join(doc_lines) + "'''"
            source = source.replace(doc_pattern, "")

        if not include_comments:
            lines = source.split("\n")
            for i, line in enumerate(lines):
                comment_pos = line.find("#")
                if comment_pos >= 0 and not _is_in_string(line, comment_pos):
                    lines[i] = line[:comment_pos].rstrip()
            source = "\n".join(lines)

        components.append(f"source:{source}")

    except (IOError, TypeError):
        components.append(f"name:{name_override or func.__name__}")
        try:
            sig = inspect.signature(func)
            components.append(f"signature:{str(sig)}")
        except ValueError:
            components.append("builtin:True")

    if (
        include_annotations
        and hasattr(func, "__annotations__")
        and func.__annotations__
    ):
        sorted_annotations = sorted(func.__annotations__.items())
        annotations_str = ";".join(f"{k}:{v}" for k, v in sorted_annotations)
        components.append(f"annotations:{annotations_str}")

    if include_code_properties:
        code = func.__code__
        stable_code_props = {
            "co_argcount": code.co_argcount,
            "co_kwonlyargcount": getattr(code, "co_kwonlyargcount", 0),
            "co_nlocals": code.co_nlocals,
            "co_varnames": code.co_varnames[: code.co_argcount],
        }
        components.append(f"code_properties:{stable_code_props}")

    return components
