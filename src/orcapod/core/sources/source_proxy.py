"""Proxy source that preserves identity hashes without requiring live data.

A ``SourceProxy`` stands in for a source that cannot be reconstructed from
its serialized config (e.g. an in-memory ``ArrowTableSource`` or ``DictSource``).
It returns the same ``content_hash``, ``pipeline_hash``, ``source_id``, and
``output_schema`` as the original, so downstream hash chains remain
consistent.  Any attempt to iterate or materialize data raises an error
unless a live source has been bound via `bind`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.protocols.core_protocols import SourceProtocol
from orcapod.types import ContentHash, Schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pyarrow as pa

    from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol
    from orcapod.types import ColumnConfig


class SourceProxy(RootSource):
    """A proxy source that preserves identity and optionally delegates.

    When created without a bound source, ``SourceProxy`` returns stored hashes
    and schemas but raises on data access.  A live source can be substituted
    in later via `bind` — if the source's identity matches, all data
    methods delegate to it.

    Args:
        source_id: The original source's canonical ID.
        content_hash_str: The original source's content hash string.
        pipeline_hash_str: The original source's pipeline hash string.
        tag_schema: The original source's tag schema.
        packet_schema: The original source's packet schema.
        expected_class_name: Class name of the original source (e.g.
            ``"ArrowTableSource"``).  Informational and used for validation
            in `bind`.
        source_config: The original source's serialized config (for to_config).
        label: Optional label.
    """

    def __init__(
        self,
        source_id: str,
        content_hash_str: str,
        pipeline_hash_str: str,
        tag_schema: Schema,
        packet_schema: Schema,
        expected_class_name: str | None = None,
        source_config: dict[str, Any] | None = None,
        label: str | None = None,
    ) -> None:
        super().__init__(source_id=source_id, label=label)
        self._content_hash_str = content_hash_str
        self._pipeline_hash_str = pipeline_hash_str
        self._tag_schema = tag_schema
        self._packet_schema = packet_schema
        self._expected_class_name = expected_class_name
        self._source_config = source_config or {}
        self._delegate: SourceProtocol | None = None

    # -------------------------------------------------------------------------
    # Binding a live source
    # -------------------------------------------------------------------------

    @property
    def expected_class_name(self) -> str | None:
        """The class name of the original source this proxy stands in for."""
        return self._expected_class_name

    @property
    def delegate(self) -> SourceProtocol | None:
        """The bound live source, or ``None`` if no source has been bound."""
        return self._delegate

    @property
    def is_bound(self) -> bool:
        """``True`` if a live source has been bound via `bind`."""
        return self._delegate is not None

    def bind(self, source: SourceProtocol) -> None:
        """Bind a live source to this proxy, enabling data access.

        The source must match this proxy's identity — same ``source_id``,
        ``content_hash``, ``pipeline_hash``, tag schema keys, and packet
        schema keys.  If ``expected_class_name`` is set, the source's class
        name must also match.

        Args:
            source: The live source to bind.

        Raises:
            ValueError: If the source's identity does not match.
        """
        errors: list[str] = []

        if source.source_id != self.source_id:
            errors.append(
                f"source_id mismatch: expected {self.source_id!r}, "
                f"got {source.source_id!r}"
            )

        if source.content_hash().to_string() != self._content_hash_str:
            errors.append(
                f"content_hash mismatch: expected {self._content_hash_str!r}, "
                f"got {source.content_hash().to_string()!r}"
            )

        if source.pipeline_hash().to_string() != self._pipeline_hash_str:
            errors.append(
                f"pipeline_hash mismatch: expected {self._pipeline_hash_str!r}, "
                f"got {source.pipeline_hash().to_string()!r}"
            )

        if (
            self._expected_class_name is not None
            and source.__class__.__name__ != self._expected_class_name
        ):
            errors.append(
                f"class mismatch: expected {self._expected_class_name!r}, "
                f"got {source.__class__.__name__!r}"
            )

        if errors:
            raise ValueError(f"Cannot bind source to SourceProxy: {'; '.join(errors)}")

        self._delegate = source

    def unbind(self) -> SourceProtocol | None:
        """Remove and return the bound source, reverting to proxy behavior.

        Returns:
            The previously bound source, or ``None`` if none was bound.
        """
        delegate = self._delegate
        self._delegate = None
        return delegate

    # -------------------------------------------------------------------------
    # Identity — return stored hashes (always, regardless of delegate)
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        return ContentHash.from_string(self._content_hash_str)

    def pipeline_identity_structure(self) -> Any:
        return ContentHash.from_string(self._pipeline_hash_str)

    # -------------------------------------------------------------------------
    # Schema — return stored schemas (always, regardless of delegate)
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return (self._tag_schema, self._packet_schema)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return (
            tuple(self._tag_schema.keys()),
            tuple(self._packet_schema.keys()),
        )

    # -------------------------------------------------------------------------
    # Data access — delegate if bound, raise otherwise
    # -------------------------------------------------------------------------

    def _require_delegate(self) -> SourceProtocol:
        """Return the delegate or raise if not bound."""
        if self._delegate is None:
            raise NotImplementedError(
                f"SourceProxy({self.source_id!r}) cannot provide data — "
                f"the original source ({self._expected_class_name or 'unknown'}) "
                f"was not reconstructable from config. "
                f"Use bind() to attach a live source."
            )
        return self._delegate

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        return self._require_delegate().iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        return self._require_delegate().as_table(columns=columns, all_info=all_info)

    def resolve_field(self, record_id: str, field_name: str) -> Any:
        """Delegate to bound source, or raise."""
        return self._require_delegate().resolve_field(record_id, field_name)

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Return the original source's config (preserves source_type)."""
        return dict(self._source_config)

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> SourceProxy:
        """Not supported — SourceProxy is created by the deserialization pipeline."""
        raise NotImplementedError(
            "SourceProxy cannot be reconstructed via from_config. "
            "It is created automatically when a source cannot be loaded."
        )
