"""A toy value type with an ignore list, for ``test_ignore_list.py`` (ITL-981).

``Labelled`` has a content (``amount``) and one optional property (``label``).
The classes live in a module of their own so that a logical type registration
file can name them.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from orcapod.logical_types.ignore_list import IgnoreListHandler, IgnoreListLogicalType
from orcapod.logical_types.protocols import TypeConverterProtocol

EXTENSION_NAME = "orcapod.test.labelled"


@dataclass(frozen=True)
class Labelled:
    """A value with a content and a label that the hash can ignore."""

    amount: int
    label: str


class LogicalLabelled(IgnoreListLogicalType):
    """Stores a ``Labelled`` as JSON text; the label is always stored."""

    EXTENSION_NAME = EXTENSION_NAME
    STORAGE_TYPE = pa.large_string()
    VERSION = 1
    OPTIONS = frozenset({"label"})
    python_type: type = Labelled

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> str:
        """Return the JSON text of a ``Labelled``."""
        return json.dumps([value.amount, value.label])

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> Labelled:
        """Return the ``Labelled`` of stored JSON text."""
        amount, label = json.loads(storage_value)
        return Labelled(amount, label)


class LabelledHandler(IgnoreListHandler):
    """Hashes the amount and the label; an ignored label is written as empty."""

    OPTIONS = frozenset({"label"})
    HASH_LABEL = "labelled_v1"

    def digest(self, obj: Any, ignore: frozenset[str]) -> bytes:
        """Return SHA-256 of the amount and the label field."""
        label = "" if "label" in ignore else obj.label
        return hashlib.sha256(json.dumps([obj.amount, label]).encode()).digest()
