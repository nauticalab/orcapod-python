"""The form of a default source id that is built from a hash."""

from __future__ import annotations

from orcapod.types import ContentHash


def hash_source_id(
    kind: str, content_hash: ContentHash, hexdigits: int | None = None
) -> str:
    """Return ``<kind>:<algorithm label>:<hex>``, a default source id built from a hash.

    A source id is the source's name. The kind says what sort of source it is, and
    the label and hex are the hash in its labelled form (``ContentHash.to_string``).
    The parts are joined by a single ``:`` and none contains one, so the id never
    contains the ``::`` that separates the parts of a source-info value.

    Args:
        kind: The source kind, for example ``dict`` or ``derived``.
        content_hash: The hash the id is built from.
        hexdigits: If given, keep only this many hex characters of the digest.

    Returns:
        The source id.

    Raises:
        ValueError: If the kind or the hash's label is empty or contains ``:``.
    """
    if not kind or ":" in kind:
        raise ValueError(f"source kind must be non-empty without ':', got {kind!r}")
    if not content_hash.method or ":" in content_hash.method:
        raise ValueError(
            f"hash label must be non-empty without ':', got {content_hash.method!r}"
        )
    return f"{kind}:{content_hash.to_string(hexdigits=hexdigits)}"
