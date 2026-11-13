"""Labelled per-type value hashes, and the order-dependent Arrow digest they build on.

A value type whose own equality depends on order (a pandas DataFrame's columns, a struct's fields)
hashes as::

    sha256(canonical_json(descriptor) + b"\\x00" + values_digest)

under a ``ContentHash`` method label ``<type>_v<N>`` (for example ``pandas_dataframe_v1``). The
descriptor carries what the Arrow values cannot express (label types, library dtypes); the
values digest is the data context's Arrow hasher's ``hash_table_ordered`` of the value's Arrow
form (order included, the same semantic preprocessing as every Orcapod Arrow hash). The label
names the whole procedure that produced the digest and nothing else (the label rule, ITL-938):
it changes only when the procedure would give an existing value a different digest, and it
never enters a hash computation.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from orcapod.types import ContentHash


def canonical_json(descriptor: Any) -> bytes:
    """Serialise a descriptor as canonical JSON.

    Keys are sorted, separators are ``","`` and ``":"``, and non-ASCII characters are kept as
    UTF-8, so equal descriptors always give equal bytes.

    Args:
        descriptor: A JSON-serialisable structure.

    Returns:
        The UTF-8 bytes of the canonical JSON text.
    """
    return json.dumps(
        descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def labelled_content_hash(
    method: str, descriptor: Any, values_digest: bytes
) -> ContentHash:
    """Combine a descriptor and a values digest into a labelled ``ContentHash``.

    Args:
        method: The versioned label of the definition, for example ``"pandas_dataframe_v1"``.
        descriptor: The JSON-serialisable descriptor of what the values digest does not see.
        values_digest: The digest of the value's contents.

    Returns:
        ``ContentHash(method, sha256(canonical_json(descriptor) + b"\\x00" + values_digest))``.
    """
    payload = canonical_json(descriptor) + b"\x00" + values_digest
    return ContentHash(method=method, digest=hashlib.sha256(payload).digest())
