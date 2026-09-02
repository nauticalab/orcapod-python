"""Generate pre-fix golden hash values for type annotation hashing.

Run once (before the ITL-638 fix) with:
    uv run python tests/test_hashing/generate_type_annotation_golden.py

Outputs: tests/test_hashing/hash_samples/type_annotation_golden.json
"""
from __future__ import annotations

import inspect
import json
import pathlib
import typing
from uuid import UUID

import orcapod as op
from orcapod.hashing.defaults import get_default_semantic_hasher
from orcapod.hashing.semantic_hashing.function_info_extractors import (
    FunctionSignatureExtractor,
)
from orcapod.logical_types.file_type import File
from orcapod.logical_types.directory_type import Directory

GOLDEN_PATH = pathlib.Path(__file__).parent / "hash_samples" / "type_annotation_golden.json"

# ---------------------------------------------------------------------------
# Annotation types to hash individually (bare type objects)
# ---------------------------------------------------------------------------
ANNOTATION_CASES: dict[str, object] = {
    # Builtins
    "int": int,
    "str": str,
    "float": float,
    "bytes": bytes,
    # orcapod logical types
    "op.File": op.File,
    "op.Directory": op.Directory,
    "op.Path": op.Path,
    "op.UUID": UUID,
    # Generic aliases
    "list[int]": list[int],
    "dict[str, int]": dict[str, int],
    "list[op.File]": list[op.File],
    "dict[str, op.File]": dict[str, op.File],
    # Unions
    "int | str": int | str,
    "op.File | None": op.File | None,
    "Optional[op.File]": typing.Optional[op.File],
}

# ---------------------------------------------------------------------------
# Functions whose full hash_object(func) and extract_function_info output
# are both captured.
# ---------------------------------------------------------------------------

def fn_no_annotations():
    return None

def fn_builtin_param(x: int, y: str) -> float:
    return float(x)

def fn_orcapod_param(f: op.File) -> str:
    return str(f)

def fn_orcapod_return(s: str) -> op.File:
    return op.File(s)  # type: ignore[arg-type]

def fn_generic_orcapod(files: list[op.File]) -> list[str]:
    return []

def fn_union_orcapod(f: op.File | None) -> op.File | None:
    return f

def fn_mixed(f: op.File, n: int) -> op.Directory:
    return op.Directory(str(f))  # type: ignore[arg-type]

FUNCTION_CASES: dict[str, object] = {
    "fn_no_annotations": fn_no_annotations,
    "fn_builtin_param": fn_builtin_param,
    "fn_orcapod_param": fn_orcapod_param,
    "fn_orcapod_return": fn_orcapod_return,
    "fn_generic_orcapod": fn_generic_orcapod,
    "fn_union_orcapod": fn_union_orcapod,
    "fn_mixed": fn_mixed,
}


def main() -> None:
    hasher = get_default_semantic_hasher()
    extractor = FunctionSignatureExtractor(include_module=True, include_defaults=True)

    result: dict = {"annotation_hashes": {}, "function_info_hashes": {}, "function_object_hashes": {}}

    # Hash bare annotations through TypeObjectHandler
    for key, ann in ANNOTATION_CASES.items():
        result["annotation_hashes"][key] = hasher.hash_object(ann).to_string()

    # Hash FunctionSignatureExtractor output dict, and full function object
    for key, func in FUNCTION_CASES.items():
        info = extractor.extract_function_info(func)
        result["function_info_hashes"][key] = hasher.hash_object(info).to_string()
        result["function_object_hashes"][key] = hasher.hash_object(func).to_string()

    GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    GOLDEN_PATH.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Wrote golden values to {GOLDEN_PATH}")


if __name__ == "__main__":
    main()
