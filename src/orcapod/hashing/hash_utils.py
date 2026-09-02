import hashlib
import inspect
import logging
import types as _types
import typing
import zlib
from collections.abc import Callable, Collection
from pathlib import Path
from typing import TYPE_CHECKING

import xxhash
from upath import UPath

from orcapod.types import ContentHash, PathLike

if TYPE_CHECKING:
    from orcapod.logical_types.registry import LogicalTypeRegistry

logger = logging.getLogger(__name__)


def is_union_annotation(annotation: object) -> bool:
    """Return ``True`` if *annotation* is a union type.

    Detects both PEP 604 ``X | Y`` (``types.UnionType``) and
    ``typing.Union[X, Y]`` / ``typing.Optional[X]``.

    Args:
        annotation: Any Python object (type annotation or otherwise).

    Returns:
        ``True`` if the annotation is a union; ``False`` otherwise.
    """
    if isinstance(annotation, _types.UnionType):
        return True
    return getattr(annotation, "__origin__", None) is typing.Union


def canonical_annotation_str(
    annotation: object,
    registry: "LogicalTypeRegistry | None" = None,
) -> str:
    """Return a stable, canonical string for a type annotation.

    Resolves types registered in *registry* (e.g. ``orcapod.File``) to their
    stable ``logical_type_name`` (e.g. ``"orcapod.file"``) so that internal
    module relocations do not change the string representation.

    For union types (both PEP 604 ``X | Y`` and ``typing.Union[X, Y]``),
    members are sorted byte-wise so that ``str | Path`` and ``Path | str``
    produce the same canonical string.

    For generic aliases (``list[X]``, ``dict[K, V]``), args are recursed with
    the same registry so nested orcapod types are also canonicalized.

    Non-union, non-generic types not found in the registry fall through to
    ``inspect.formatannotation``, preserving existing behaviour exactly.

    Args:
        annotation: A type annotation object.
        registry: Optional ``LogicalTypeRegistry``. When provided, registered
            logical types resolve to their stable ``logical_type_name``.

    Returns:
        A canonical string representation.
    """
    # Registered logical type: use stable canonical name (e.g. "orcapod.file")
    if registry is not None and isinstance(annotation, type):
        lt = registry.get_by_python_type(annotation)
        if lt is not None:
            return lt.logical_type_name

    # Union types (PEP 604 X | Y and typing.Union): sort members for order-independence
    if is_union_annotation(annotation):
        args = getattr(annotation, "__args__", ()) or ()
        member_strs = sorted(canonical_annotation_str(a, registry) for a in args)
        return " | ".join(member_strs)

    # Generic aliases (list[X], dict[K, V], etc.): recurse over args
    origin = getattr(annotation, "__origin__", None)
    if origin is not None and not is_union_annotation(annotation):
        args = getattr(annotation, "__args__", None) or ()
        origin_str = canonical_annotation_str(origin, registry)
        if args:
            args_str = ", ".join(canonical_annotation_str(a, registry) for a in args)
            return f"{origin_str}[{args_str}]"
        return origin_str

    return inspect.formatannotation(annotation)


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


def _to_path(file_path: PathLike) -> Path | UPath:
    """Convert a path-like to a Path, preserving UPath instances.

    If ``file_path`` is already a ``Path`` (including ``UPath`` subclasses),
    return it as-is so that remote-filesystem semantics are retained.
    Otherwise wrap it in ``Path()``.
    """
    # Check UPath first to preserve remote-filesystem semantics even if
    # the inheritance relationship with pathlib.Path ever changes.
    if isinstance(file_path, UPath):
        return file_path
    if isinstance(file_path, Path):
        return file_path
    return Path(file_path)


def hash_file(file_path: PathLike, algorithm="sha256", buffer_size=65536) -> ContentHash:
    """Calculate the hash of a file using the specified algorithm.

    Supports both local ``pathlib.Path`` and remote ``UPath`` objects.

    Args:
        file_path: Path to the file to hash.
        algorithm: Hash algorithm to use — options include:
            'md5', 'sha1', 'sha256', 'sha512', 'xxh64', 'crc32', 'hash_path'.
        buffer_size: Size of chunks to read from the file at a time.

    Returns:
        A ContentHash with method set to the algorithm name and digest
        containing the raw hash bytes.
    """
    path = _to_path(file_path)

    if not path.is_file():
        raise FileNotFoundError(f"The file {file_path} does not exist")

    # Hash the path string itself rather than file content
    if algorithm == "hash_path":
        hasher = hashlib.sha256()
        hasher.update(str(file_path).encode("utf-8"))
        return ContentHash(method=algorithm, digest=hasher.digest())

    if algorithm == "xxh64":
        hasher = xxhash.xxh64()
        with path.open("rb") as file:
            while True:
                data = file.read(buffer_size)
                if not data:
                    break
                hasher.update(data)
        return ContentHash(method=algorithm, digest=hasher.digest())

    if algorithm == "crc32":
        crc = 0
        with path.open("rb") as file:
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

    with path.open("rb") as file:
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
    parts: dict[str, object] = {}

    if include_module and hasattr(func, "__module__"):
        parts["module"] = func.__module__

    parts["name"] = name_override or func.__name__

    param_strs = []
    for name, param in sig.parameters.items():
        param_str = str(param)
        annotation = param.annotation
        if annotation is not inspect.Parameter.empty and is_union_annotation(annotation):
            old_ann = inspect.formatannotation(annotation)
            new_ann = canonical_annotation_str(annotation)
            # Replace ": <old_ann>" with ": <new_ann>" (first occurrence only).
            # The ": " prefix distinguishes the annotation from the default value.
            param_str = param_str.replace(f": {old_ann}", f": {new_ann}", 1)
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
        ret = parts["returns"]
        if is_union_annotation(ret):
            fn_string += f"-> {canonical_annotation_str(ret)}"
        else:
            fn_string += f"-> {ret}"
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
