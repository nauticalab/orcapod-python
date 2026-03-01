"""
Base class for legacy datagram implementations (ArrowDatagram, DictDatagram).

This is a verbatim copy of orcapod.core.datagrams.base kept exclusively for the
legacy classes so they remain self-contained once the main base.py is removed as
part of the Datagram unification.  Do not modify this file; it will be deleted
together with the legacy classes.
"""

import logging
from collections.abc import Mapping
from typing import Any

from uuid_utils import uuid7

from orcapod.core.base import ContentIdentifiableBase
from orcapod.types import DataValue
from orcapod.utils.lazy_module import LazyModule

logger = logging.getLogger(__name__)

if __import__("typing").TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


PacketLike = Mapping[str, DataValue]
"""Broad packet-like type: any mapping from string keys to DataValue."""


class BaseDatagram(ContentIdentifiableBase):
    """
    Minimal abstract base for legacy datagram implementations.

    Manages datagram identity (UUID) and the data context reference.
    Concrete subclasses are responsible for all data storage and access.
    """

    def __init__(self, datagram_id: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self._datagram_id = datagram_id

    @property
    def datagram_id(self) -> str:
        """Return (or lazily generate) the datagram's unique ID."""
        if self._datagram_id is None:
            self._datagram_id = str(uuid7())
        return self._datagram_id

    def identity_structure(self) -> Any:
        raise NotImplementedError()

    @property
    def converter(self):
        """Semantic type converter for this datagram's data context."""
        return self.data_context.type_converter

    def with_context_key(self, new_context_key: str):
        """Create a new datagram with a different data-context key."""
        from orcapod import contexts

        new_datagram = self.copy(include_cache=False)
        new_datagram._data_context = contexts.resolve_context(new_context_key)
        return new_datagram

    def copy(self, include_cache: bool = True, preserve_id: bool = True):
        """Shallow-copy skeleton used by subclass copy() implementations.

        Uses ``object.__new__`` to avoid calling ``__init__``, so all fields
        that are normally set by ``__init__`` must be initialized explicitly
        here or in the subclass ``copy()`` override.

        ``_content_hash_cache`` (owned by ``ContentIdentifiableBase.__init__``)
        is handled here so that subclasses do not need to manage it directly.
        """
        new_datagram = object.__new__(self.__class__)
        new_datagram._data_context = self._data_context
        new_datagram._datagram_id = self._datagram_id if preserve_id else None
        # Initialize the cache dict that ContentIdentifiableBase.__init__ normally sets.
        new_datagram._content_hash_cache = (
            dict(self._content_hash_cache) if include_cache else {}
        )
        return new_datagram
