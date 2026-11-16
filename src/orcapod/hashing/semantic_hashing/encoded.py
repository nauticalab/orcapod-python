"""The tagged value a type handler returns to the structure hasher.

The structure hasher writes each Python-only container as a JSON object carrying a
``"__type__"`` key (``{"__type__": "tuple", "items": [...]}``). Only the hasher's
own encodings may produce such an object: a plain mapping that has a ``"__type__"``
key is hashed as a list of key/value pairs, so a user dict can never spell out the
encoding of another value (ITL-942). A type handler that wants a tagged JSON object
returns an ``Encoded`` instead of a plain dict.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

TYPE_KEY = "__type__"
"""The key that carries the tag of an encoded value in the hashed JSON tree."""

HASHER_TAGS = frozenset({"tuple", "set", "frozenset", "dict", "namedtuple", "hash"})
"""Tags written by the structure hasher itself; a type handler may not use them.

``hash`` is the tag of a nested hash (ITL-945).
"""


@dataclass(frozen=True)
class Encoded:
    """A tagged value returned by a type handler: ``{"__type__": tag, **fields}``.

    The structure hasher trusts an ``Encoded`` only when a type handler returns
    it. An ``Encoded`` found anywhere else (inside a user value or an identity
    structure) is an ordinary object with no handler, which the strict hasher
    refuses, so data can never forge a tagged value.

    Args:
        tag: The value's tag. Must be non-empty and not one of ``HASHER_TAGS``.
        fields: The tagged object's other members. Keys are ``str`` and never
            ``"__type__"``; values are expanded like any other structure member.

    Raises:
        ValueError: If ``tag`` is empty or reserved, or a field key is invalid.
    """

    tag: str
    fields: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate ``tag`` and the field keys, then freeze ``fields``.

        ``fields`` is copied into a read-only mapping, so later changes to the
        mapping passed in do not reach this object.

        Raises:
            ValueError: If ``tag`` is empty or one of ``HASHER_TAGS``, or a field
                key is not a ``str`` or is ``"__type__"``.
        """
        if not self.tag:
            raise ValueError("Encoded tag must be a non-empty string.")
        if self.tag in HASHER_TAGS:
            raise ValueError(
                f"Encoded tag {self.tag!r} is reserved by the structure hasher "
                f"({sorted(HASHER_TAGS)})."
            )
        for key in self.fields:
            if not isinstance(key, str) or key == TYPE_KEY:
                raise ValueError(
                    f"Encoded field keys must be str and not {TYPE_KEY!r}; got {key!r}."
                )
        object.__setattr__(self, "fields", MappingProxyType(dict(self.fields)))
