"""``hash_source_id``: the one definition of a hash-based default source id (ITL-957)."""

import pytest

from orcapod.core.sources.source_ids import hash_source_id
from orcapod.types import ContentHash

HASH = ContentHash("algo_v1", bytes.fromhex("00ab" * 16))


def test_format_is_kind_label_hex():
    assert hash_source_id("dict", HASH) == f"dict:{HASH.to_string()}"
    assert hash_source_id("dict", HASH).split(":") == ["dict", "algo_v1", "00ab" * 16]


def test_hexdigits_truncates_the_hex_only():
    assert hash_source_id("dict", HASH, hexdigits=6) == "dict:algo_v1:00ab00"


@pytest.mark.parametrize("kind", ["", "a:b"])
def test_kind_must_be_non_empty_without_a_colon(kind):
    with pytest.raises(ValueError, match="kind"):
        hash_source_id(kind, HASH)


@pytest.mark.parametrize("label", ["", "a:b"])
def test_label_must_be_non_empty_without_a_colon(label):
    with pytest.raises(ValueError, match="label"):
        hash_source_id("dict", ContentHash(label, b"\x00" * 32))
