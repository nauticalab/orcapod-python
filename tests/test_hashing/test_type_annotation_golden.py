"""Golden-value regression tests for type annotation hashing (ITL-638).

Three test classes:
  TestGoldenStability       -- builtins must be UNCHANGED after the fix.
  TestGoldenCanonical       -- orcapod logical types must produce NEW canonical hashes.
  TestSchemaHashStability   -- Schema hashes (including orcapod types) must stay stable
                               after any future code change.

The annotation/function golden JSON was generated pre-fix by generate_type_annotation_golden.py.
The schema golden JSON (hash_samples/schema_hash_golden.json) was generated post-fix and
locks in the stable canonical hash values.
"""
from __future__ import annotations

import json
import pathlib
import typing
from uuid import UUID

import pytest

import orcapod as op
from orcapod.hashing.defaults import get_default_semantic_hasher
from orcapod.hashing.semantic_hashing.function_info_extractors import (
    FunctionSignatureExtractor,
)
from orcapod.types import Schema

GOLDEN_PATH = (
    pathlib.Path(__file__).parent / "hash_samples" / "type_annotation_golden.json"
)
SCHEMA_GOLDEN_PATH = (
    pathlib.Path(__file__).parent / "hash_samples" / "schema_hash_golden.json"
)

# ---------------------------------------------------------------------------
# The same annotation and function cases as the generator — must stay in sync.
# ---------------------------------------------------------------------------

ANNOTATION_CASES: dict[str, object] = {
    "int": int,
    "str": str,
    "float": float,
    "bytes": bytes,
    "op.File": op.File,
    "op.Directory": op.Directory,
    "op.Path": op.Path,
    "op.UUID": UUID,  # UUID *class* as annotation; routes through TypeObjectHandler, not UUIDHandler
    "list[int]": list[int],
    "dict[str, int]": dict[str, int],
    "list[op.File]": list[op.File],
    "dict[str, op.File]": dict[str, op.File],
    "int | str": int | str,
    "op.File | None": op.File | None,
    "Optional[op.File]": typing.Optional[op.File],
}

# Annotation keys that are expected to change after the fix.
# Any key not in this set must have an UNCHANGED hash.
EXPECTED_CHANGED_KEYS: frozenset[str] = frozenset({
    "op.File",
    "op.Directory",
    "op.Path",
    "op.UUID",
    "list[op.File]",
    "dict[str, op.File]",
    "op.File | None",
    "Optional[op.File]",
})


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

# Functions expected to have different hashes after the fix.
EXPECTED_CHANGED_FUNCTIONS: frozenset[str] = frozenset({
    "fn_orcapod_param",
    "fn_orcapod_return",
    "fn_generic_orcapod",
    "fn_union_orcapod",
    "fn_mixed",
})


@pytest.fixture(scope="module")
def golden() -> dict:
    assert GOLDEN_PATH.exists(), (
        f"Golden file not found: {GOLDEN_PATH}. "
        "Run generate_type_annotation_golden.py first."
    )
    return json.loads(GOLDEN_PATH.read_text())


@pytest.fixture(scope="module")
def hasher():
    return get_default_semantic_hasher()


@pytest.fixture(scope="module")
def extractor():
    from orcapod.contexts import get_default_context
    return FunctionSignatureExtractor(
        include_module=True,
        include_defaults=True,
        type_converter=get_default_context().type_converter,
    )


class TestGoldenStability:
    """Builtins and non-orcapod annotations must hash identically before and after the fix."""

    def test_builtin_annotation_hashes_unchanged(self, golden, hasher):
        stable_keys = {k for k in ANNOTATION_CASES if k not in EXPECTED_CHANGED_KEYS}
        mismatches = {}
        for key in stable_keys:
            ann = ANNOTATION_CASES[key]
            current = hasher.hash_object(ann).to_string()
            expected = golden["annotation_hashes"][key]
            if current != expected:
                mismatches[key] = {"expected": expected, "current": current}
        assert not mismatches, (
            f"Unexpected hash changes in stable annotations:\n"
            + "\n".join(f"  {k}: {v}" for k, v in mismatches.items())
        )

    def test_builtin_function_hashes_unchanged(self, golden, hasher, extractor):
        stable_fns = {k for k in FUNCTION_CASES if k not in EXPECTED_CHANGED_FUNCTIONS}
        mismatches = {}
        for key in stable_fns:
            func = FUNCTION_CASES[key]
            info = extractor.extract_function_info(func)
            current = hasher.hash_object(info).to_string()
            expected = golden["function_info_hashes"][key]
            if current != expected:
                mismatches[key] = {"expected": expected, "current": current}
        assert not mismatches, (
            f"Unexpected hash changes in stable functions:\n"
            + "\n".join(f"  {k}: {v}" for k, v in mismatches.items())
        )


class TestGoldenCanonical:
    """orcapod logical types must produce NEW hashes (canonical name, not module path)."""

    def test_orcapod_annotation_hashes_changed(self, golden, hasher):
        """After the fix, orcapod type annotation hashes must DIFFER from golden."""
        unchanged = {}
        for key in EXPECTED_CHANGED_KEYS:
            if key not in ANNOTATION_CASES:
                continue
            ann = ANNOTATION_CASES[key]
            current = hasher.hash_object(ann).to_string()
            expected = golden["annotation_hashes"][key]
            if current == expected:
                unchanged[key] = current
        assert not unchanged, (
            f"Expected these annotation hashes to change after the fix, but they didn't:\n"
            + "\n".join(f"  {k}: {v}" for k, v in unchanged.items())
        )

    def test_orcapod_function_hashes_changed(self, golden, hasher, extractor):
        """After the fix, functions using orcapod types must DIFFER from golden."""
        unchanged = {}
        for key in EXPECTED_CHANGED_FUNCTIONS:
            func = FUNCTION_CASES[key]
            info = extractor.extract_function_info(func)
            current = hasher.hash_object(info).to_string()
            expected = golden["function_info_hashes"][key]
            if current == expected:
                unchanged[key] = current
        assert not unchanged, (
            f"Expected these function hashes to change after the fix, but they didn't:\n"
            + "\n".join(f"  {k}: {v}" for k, v in unchanged.items())
        )


# ---------------------------------------------------------------------------
# Schema hash cases — covers both builtin and orcapod logical types.
# All entries are POST-FIX canonical values and must remain stable forever.
# ---------------------------------------------------------------------------

SCHEMA_CASES: dict[str, Schema] = {
    "Schema({x: int})": Schema({"x": int}),
    "Schema({x: int, y: str})": Schema({"x": int, "y": str}),
    "Schema({f: op.File})": Schema({"f": op.File}),
    "Schema({d: op.Directory})": Schema({"d": op.Directory}),
    "Schema({p: op.Path})": Schema({"p": op.Path}),
    "Schema({x: int, f: op.File})": Schema({"x": int, "f": op.File}),
    "Schema({f: op.File, d: op.Directory})": Schema({"f": op.File, "d": op.Directory}),
    "Schema({f: op.File | None})": Schema({"f": op.File | None}),
}


@pytest.fixture(scope="module")
def schema_golden() -> dict:
    assert SCHEMA_GOLDEN_PATH.exists(), (
        f"Schema golden file not found: {SCHEMA_GOLDEN_PATH}. "
        "Regenerate it by running: "
        "uv run python -c \"<see generate_type_annotation_golden.py for pattern>\""
    )
    return json.loads(SCHEMA_GOLDEN_PATH.read_text())


class TestSchemaHashStability:
    """Schema hashes must stay stable after any code change.

    The golden file (hash_samples/schema_hash_golden.json) was generated
    post-fix (ITL-638) and captures the canonical hash for each schema.
    Every entry — including schemas that contain orcapod logical types such
    as ``op.File`` and ``op.Directory`` — must remain byte-identical across
    future refactors.

    If a hash changes unexpectedly, investigate whether:
    - ``TypeObjectHandler`` serialization changed for a logical type.
    - A logical type's ``logical_type_name`` was renamed.
    - The semantic hasher version was bumped (which intentionally changes all hashes).

    To intentionally update the golden values (e.g. after a deliberate hash-scheme
    change), recompute and overwrite ``hash_samples/schema_hash_golden.json``.
    """

    def test_schema_hashes_stable(self, schema_golden, hasher):
        """Every schema in the golden file must hash to the same value."""
        mismatches = {}
        for key, schema in SCHEMA_CASES.items():
            current = hasher.hash_object(schema).to_string()
            expected = schema_golden[key]
            if current != expected:
                mismatches[key] = {"expected": expected, "current": current}
        assert not mismatches, (
            "Schema hash values changed — this may indicate an unintended regression "
            "in TypeObjectHandler or canonical_annotation_str:\n"
            + "\n".join(
                f"  {k}:\n    expected: {v['expected']}\n    current:  {v['current']}"
                for k, v in mismatches.items()
            )
        )

    def test_all_golden_keys_covered(self, schema_golden):
        """Every key in the golden file must have a corresponding SCHEMA_CASES entry."""
        missing = set(schema_golden.keys()) - set(SCHEMA_CASES.keys())
        assert not missing, (
            f"Golden file contains keys not covered by SCHEMA_CASES: {missing}\n"
            "Add the missing schema(s) to SCHEMA_CASES or regenerate the golden file."
        )
