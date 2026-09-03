"""Generate pre-fix golden hash values for type annotation hashing.

Run once (before the ITL-638 fix) with:
    uv run python tests/test_hashing/generate_type_annotation_golden.py

Outputs: tests/test_hashing/hash_samples/type_annotation_golden.json

Note: ANNOTATION_CASES and FUNCTION_CASES are imported from test_type_annotation_golden
so that function hashes are produced with the correct module path (the test module).
"""
from __future__ import annotations

import json
import pathlib
import sys

# Ensure the project root is on sys.path so that `tests.*` imports work when
# this script is run directly (e.g. `uv run python tests/test_hashing/...`).
_PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from orcapod.hashing.defaults import get_default_semantic_hasher
from orcapod.hashing.semantic_hashing.function_info_extractors import (
    FunctionSignatureExtractor,
)
from tests.test_hashing.test_type_annotation_golden import (
    ANNOTATION_CASES,
    FUNCTION_CASES,
)

GOLDEN_PATH = pathlib.Path(__file__).parent / "hash_samples" / "type_annotation_golden.json"


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
