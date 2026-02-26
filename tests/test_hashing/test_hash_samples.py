"""
Tests for hash samples consistency.

Verifies that BaseSemanticHasher produces identical hashes across runs for a
fixed set of recorded input values.  The sample file is generated (or
regenerated) by running generate_hash_examples.py.

Schema of each entry in the JSON sample file
--------------------------------------------
{
    "value": <JSON-serialisable representation of the original Python value>,
    "hash":  <ContentHash.to_string() produced by get_default_semantic_hasher()>
}

Value encoding conventions (mirrors generate_hash_examples.py)
---------------------------------------------------------------
  bytes / bytearray          → "bytes:<hex>"
  set                        → "set:[<sorted str repr of items>]"
  frozenset                  → "frozenset:[<sorted str repr of items>]"
  tuple                      → {"__type__": "tuple", "items": [...]}
  OrderedDict                → {"__type__": "OrderedDict", "items": {...}}
  everything else            → native JSON value (None, bool, int, float, str,
                               list, dict)
"""

import json
import os
from pathlib import Path

import pytest

from orcapod.hashing import get_default_semantic_hasher

# ---------------------------------------------------------------------------
# Helpers: locate and load the sample file
# ---------------------------------------------------------------------------


def get_latest_hash_samples() -> Path:
    """Return the path to the most-recently-generated sample file."""
    samples_dir = Path(__file__).parent / "hash_samples" / "data_structures"
    sample_files = list(samples_dir.glob("hash_examples_*.json"))

    if not sample_files:
        pytest.skip(f"No hash sample files found in {samples_dir}")

    sample_files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return sample_files[0]


def load_hash_samples(file_path: Path | None = None) -> list[dict]:
    """Load the list of sample entries from *file_path* (or the latest file)."""
    if file_path is None:
        file_path = get_latest_hash_samples()
    with open(file_path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Helpers: deserialise the stored value representation back to Python
# ---------------------------------------------------------------------------


def deserialize_value(serialized_value):
    """
    Convert the stored JSON representation back to the original Python value.

    Handles all the encoding conventions documented in the module docstring.
    """
    # --- bytes / bytearray ---
    if isinstance(serialized_value, str) and serialized_value.startswith("bytes:"):
        return bytes.fromhex(serialized_value.strip("bytes:"))

    # --- tagged dicts (set, frozenset, tuple, OrderedDict) ---
    if isinstance(serialized_value, dict) and "__type__" in serialized_value:
        type_tag = serialized_value["__type__"]
        if type_tag == "set":
            return set(serialized_value["items"])
        if type_tag == "frozenset":
            return frozenset(serialized_value["items"])
        if type_tag == "tuple":
            return tuple(serialized_value["items"])
        if type_tag == "OrderedDict":
            from collections import OrderedDict

            return OrderedDict(serialized_value["items"])

    # --- native JSON values (None, bool, int, float, str, list, dict) ---
    return serialized_value


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_hash_consistency():
    """
    For every entry in the latest sample file, re-hash the value with the
    current default SemanticHasher and assert it matches the recorded hash.
    """
    hasher = get_default_semantic_hasher()
    samples = load_hash_samples()

    mismatches = []
    for sample in samples:
        value = deserialize_value(sample["value"])
        expected = sample["hash"]
        actual = hasher.hash_object(value).to_string()
        if actual != expected:
            mismatches.append(
                f"  value={sample['value']!r}\n"
                f"    expected: {expected}\n"
                f"    actual:   {actual}"
            )

    assert not mismatches, f"{len(mismatches)} hash mismatch(es):\n" + "\n".join(
        mismatches
    )


def test_sample_file_is_non_empty():
    """Sanity check: the sample file must contain at least one entry."""
    samples = load_hash_samples()
    assert len(samples) > 0, "Hash sample file is empty — regenerate it."


def test_all_samples_have_required_keys():
    """Every entry must have both 'value' and 'hash' keys."""
    samples = load_hash_samples()
    for i, sample in enumerate(samples):
        assert "value" in sample, f"Entry {i} is missing 'value' key"
        assert "hash" in sample, f"Entry {i} is missing 'hash' key"


def test_hash_values_are_non_empty_strings():
    """All recorded hash strings must be non-empty."""
    samples = load_hash_samples()
    for i, sample in enumerate(samples):
        h = sample["hash"]
        assert isinstance(h, str) and h, f"Entry {i} has an invalid hash value: {h!r}"
