"""Cross-process hash stability tests.

Verifies that hash values produced by StarfixArrowHasher and the semantic
hasher are deterministic across independent Python interpreter processes,
immune to PYTHONHASHSEED randomisation, and therefore safe to persist in
databases or caches.

Coverage
--------
- Arrow schema hashes: identical across separately-spawned processes
- Arrow table data hashes: identical across separately-spawned processes
- Semantic (Python object) hashes: identical across processes
- PYTHONHASHSEED independence: hashes do not vary when Python's built-in
  hash() randomisation seed changes (critical for dicts and sets)
- Column-order independence confirmed cross-process
- Row-order sensitivity confirmed cross-process (rows matter, columns don't)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap

import pyarrow as pa
import pytest

from orcapod.hashing.versioned_hashers import (
    get_versioned_semantic_arrow_hasher,
    get_versioned_semantic_hasher,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_subprocess(script: str, pythonhashseed: str) -> dict[str, str]:
    """Run *script* in a fresh interpreter with the given PYTHONHASHSEED.

    Returns the JSON-decoded dict printed to stdout.
    Raises AssertionError if the subprocess exits non-zero.
    """
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = pythonhashseed
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"Subprocess failed (PYTHONHASHSEED={pythonhashseed}):\n{result.stderr}"
    )
    return json.loads(result.stdout.strip())


# ---------------------------------------------------------------------------
# Subprocess scripts (self-contained; no shared state with the test process)
# ---------------------------------------------------------------------------

_ARROW_SCHEMA_SCRIPT = textwrap.dedent("""\
    import json
    import pyarrow as pa
    from orcapod.hashing.versioned_hashers import get_versioned_semantic_arrow_hasher

    hasher = get_versioned_semantic_arrow_hasher()
    schemas = {
        "simple": pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("value", pa.float64(), nullable=True),
        ]),
        "string_types": pa.schema([
            pa.field("name", pa.utf8()),
            pa.field("data", pa.large_binary()),
        ]),
        "nested_struct": pa.schema([
            pa.field("point", pa.struct([
                pa.field("x", pa.float32()),
                pa.field("y", pa.float32()),
            ])),
        ]),
        # Same fields as "simple" but columns listed in reverse order —
        # must hash identically (column-order independence).
        "simple_reordered": pa.schema([
            pa.field("value", pa.float64(), nullable=True),
            pa.field("id", pa.int64(), nullable=False),
        ]),
    }
    print(json.dumps({name: hasher.hash_schema(s).digest.hex() for name, s in schemas.items()}))
""")

_ARROW_TABLE_SCRIPT = textwrap.dedent("""\
    import json
    import pyarrow as pa
    from orcapod.hashing.versioned_hashers import get_versioned_semantic_arrow_hasher

    hasher = get_versioned_semantic_arrow_hasher()
    tables = {
        "integers": pa.table({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "x":  pa.array([10, 20, 30], type=pa.int64()),
        }),
        "with_nulls": pa.table({
            "a": pa.array([1, None, 3], type=pa.int32()),
            "b": pa.array([None, 2.0, 3.0], type=pa.float64()),
        }),
        "strings": pa.table({
            "name": pa.array(["alice", "bob", "carol"], type=pa.large_utf8()),
        }),
        "empty": pa.table({
            "x": pa.array([], type=pa.int64()),
        }),
        # Same rows as "integers" but columns swapped — must hash identically.
        "integers_col_swap": pa.table({
            "x":  pa.array([10, 20, 30], type=pa.int64()),
            "id": pa.array([1, 2, 3], type=pa.int64()),
        }),
        # Same rows as "integers" but rows reversed — must hash DIFFERENTLY.
        "integers_row_reversed": pa.table({
            "id": pa.array([3, 2, 1], type=pa.int64()),
            "x":  pa.array([30, 20, 10], type=pa.int64()),
        }),
    }
    print(json.dumps({name: hasher.hash_table(t).digest.hex() for name, t in tables.items()}))
""")

_SEMANTIC_SCRIPT = textwrap.dedent("""\
    import json
    from orcapod.hashing.versioned_hashers import get_versioned_semantic_hasher

    hasher = get_versioned_semantic_hasher()
    objects = {
        "int":         42,
        "neg_int":     -7,
        "float":       3.14159,
        "string":      "hello world",
        "none":        None,
        "bool_true":   True,
        "bool_false":  False,
        "list":        [1, 2, 3],
        "nested_dict": {"a": 1, "b": [2, 3], "c": {"d": 4}},
        "set":         [1, 2, 3],   # hashed as set equivalent
        "tuple":       [1, 2, 3],   # distinguished by type tag inside hasher
        "empty_list":  [],
        "empty_dict":  {},
    }
    # sets/tuples need special treatment for JSON serialisation — hash via
    # the real Python types rather than JSON-roundtripped lists.
    typed_objects = {
        "set":   {1, 2, 3},
        "tuple": (1, 2, 3),
    }
    results = {name: hasher.hash_object(obj).digest.hex() for name, obj in objects.items()}
    results.update({name: hasher.hash_object(obj).digest.hex() for name, obj in typed_objects.items()})
    print(json.dumps(results))
""")


# ---------------------------------------------------------------------------
# Tests: Arrow schema hash cross-process stability
# ---------------------------------------------------------------------------


class TestArrowSchemaHashCrossProcess:
    """Arrow schema hashes are identical across independent processes."""

    def test_schema_hash_matches_subprocess(self):
        """Hash computed locally equals hash computed in a fresh subprocess."""
        hasher = get_versioned_semantic_arrow_hasher()
        local = {
            "simple": hasher.hash_schema(pa.schema([
                pa.field("id", pa.int64(), nullable=False),
                pa.field("value", pa.float64(), nullable=True),
            ])).digest.hex(),
            "string_types": hasher.hash_schema(pa.schema([
                pa.field("name", pa.utf8()),
                pa.field("data", pa.large_binary()),
            ])).digest.hex(),
            "nested_struct": hasher.hash_schema(pa.schema([
                pa.field("point", pa.struct([
                    pa.field("x", pa.float32()),
                    pa.field("y", pa.float32()),
                ])),
            ])).digest.hex(),
            "simple_reordered": hasher.hash_schema(pa.schema([
                pa.field("value", pa.float64(), nullable=True),
                pa.field("id", pa.int64(), nullable=False),
            ])).digest.hex(),
        }
        remote = _run_subprocess(_ARROW_SCHEMA_SCRIPT, pythonhashseed="42")
        assert local == remote

    def test_schema_hash_independent_of_pythonhashseed(self):
        """Two subprocesses with different PYTHONHASHSEED values agree."""
        hashes_a = _run_subprocess(_ARROW_SCHEMA_SCRIPT, pythonhashseed="0")
        hashes_b = _run_subprocess(_ARROW_SCHEMA_SCRIPT, pythonhashseed="999999")
        assert hashes_a == hashes_b

    def test_column_order_independence_cross_process(self):
        """Reordering schema fields yields the same hash in a subprocess."""
        hashes = _run_subprocess(_ARROW_SCHEMA_SCRIPT, pythonhashseed="random")
        assert hashes["simple"] == hashes["simple_reordered"]


# ---------------------------------------------------------------------------
# Tests: Arrow table data hash cross-process stability
# ---------------------------------------------------------------------------


class TestArrowTableHashCrossProcess:
    """Arrow table data hashes are identical across independent processes."""

    def test_table_hash_matches_subprocess(self):
        """Hash computed locally equals hash computed in a fresh subprocess."""
        hasher = get_versioned_semantic_arrow_hasher()
        local = {
            "integers": hasher.hash_table(pa.table({
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "x":  pa.array([10, 20, 30], type=pa.int64()),
            })).digest.hex(),
            "with_nulls": hasher.hash_table(pa.table({
                "a": pa.array([1, None, 3], type=pa.int32()),
                "b": pa.array([None, 2.0, 3.0], type=pa.float64()),
            })).digest.hex(),
            "strings": hasher.hash_table(pa.table({
                "name": pa.array(["alice", "bob", "carol"], type=pa.large_utf8()),
            })).digest.hex(),
            "empty": hasher.hash_table(pa.table({
                "x": pa.array([], type=pa.int64()),
            })).digest.hex(),
            "integers_col_swap": hasher.hash_table(pa.table({
                "x":  pa.array([10, 20, 30], type=pa.int64()),
                "id": pa.array([1, 2, 3], type=pa.int64()),
            })).digest.hex(),
            "integers_row_reversed": hasher.hash_table(pa.table({
                "id": pa.array([3, 2, 1], type=pa.int64()),
                "x":  pa.array([30, 20, 10], type=pa.int64()),
            })).digest.hex(),
        }
        remote = _run_subprocess(_ARROW_TABLE_SCRIPT, pythonhashseed="1337")
        assert local == remote

    def test_table_hash_independent_of_pythonhashseed(self):
        """Two subprocesses with different PYTHONHASHSEED values agree."""
        hashes_a = _run_subprocess(_ARROW_TABLE_SCRIPT, pythonhashseed="0")
        hashes_b = _run_subprocess(_ARROW_TABLE_SCRIPT, pythonhashseed="12345")
        assert hashes_a == hashes_b

    def test_column_order_independence_cross_process(self):
        """Column-reordered tables hash identically across processes."""
        hashes = _run_subprocess(_ARROW_TABLE_SCRIPT, pythonhashseed="random")
        assert hashes["integers"] == hashes["integers_col_swap"]

    def test_row_order_sensitivity_cross_process(self):
        """Row-reversed tables hash differently across processes (row order matters)."""
        hashes = _run_subprocess(_ARROW_TABLE_SCRIPT, pythonhashseed="random")
        assert hashes["integers"] != hashes["integers_row_reversed"]


# ---------------------------------------------------------------------------
# Tests: Semantic (Python object) hash cross-process stability
# ---------------------------------------------------------------------------


class TestSemanticHashCrossProcess:
    """Semantic Python-object hashes are identical across independent processes."""

    def test_semantic_hash_matches_subprocess(self):
        """Hash computed locally equals hash computed in a fresh subprocess."""
        hasher = get_versioned_semantic_hasher()
        local = {
            "int":         hasher.hash_object(42).digest.hex(),
            "neg_int":     hasher.hash_object(-7).digest.hex(),
            "float":       hasher.hash_object(3.14159).digest.hex(),
            "string":      hasher.hash_object("hello world").digest.hex(),
            "none":        hasher.hash_object(None).digest.hex(),
            "bool_true":   hasher.hash_object(True).digest.hex(),
            "bool_false":  hasher.hash_object(False).digest.hex(),
            "list":        hasher.hash_object([1, 2, 3]).digest.hex(),
            "nested_dict": hasher.hash_object({"a": 1, "b": [2, 3], "c": {"d": 4}}).digest.hex(),
            "set":         hasher.hash_object({1, 2, 3}).digest.hex(),
            "tuple":       hasher.hash_object((1, 2, 3)).digest.hex(),
            "empty_list":  hasher.hash_object([]).digest.hex(),
            "empty_dict":  hasher.hash_object({}).digest.hex(),
        }
        remote = _run_subprocess(_SEMANTIC_SCRIPT, pythonhashseed="42")
        assert local == remote

    def test_semantic_hash_independent_of_pythonhashseed(self):
        """Semantic hashes do not change when PYTHONHASHSEED differs.

        PYTHONHASHSEED affects dict and set iteration order in CPython.
        This test confirms the semantic hasher normalises that away.
        """
        hashes_seed0 = _run_subprocess(_SEMANTIC_SCRIPT, pythonhashseed="0")
        hashes_seed1 = _run_subprocess(_SEMANTIC_SCRIPT, pythonhashseed="1")
        hashes_random = _run_subprocess(_SEMANTIC_SCRIPT, pythonhashseed="random")
        assert hashes_seed0 == hashes_seed1
        assert hashes_seed1 == hashes_random
