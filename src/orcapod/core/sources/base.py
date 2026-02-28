from __future__ import annotations

from typing import Any

from orcapod.core.streams.base import StreamBase
from orcapod.errors import FieldNotResolvableError
from orcapod.protocols.core_protocols import StreamProtocol


class RootSource(StreamBase):
    """
    Abstract base class for all root sources in Orcapod.

    A RootSource is a pure stream — the root of a computational graph, producing
    data from an external source (file, database, in-memory data, etc.) with no
    upstream dependencies.

    As a StreamProtocol:
    - ``source`` returns ``None`` (no upstream source pod)
    - ``upstreams`` is always empty
    - ``keys``, ``output_schema``, ``iter_packets``, ``as_table`` are abstract
      and must be implemented by concrete subclasses

    As a PipelineElementProtocol:
    - ``pipeline_identity_structure()`` returns ``(tag_schema, packet_schema)``
      — schema-only, no data content — forming the base case of the pipeline
      identity Merkle chain.

    Source identity
    ---------------
    Every source has a ``source_id`` — a canonical name that can be used to
    register the source in a ``SourceRegistry`` so that provenance tokens
    embedded in downstream data can be resolved back to the originating source
    object.  Registration is an explicit external action; the source itself
    does not self-register.

    If ``source_id`` is not provided at construction it defaults to the content
    hash of the source (stable for fixed datasets).

    Field resolution
    ----------------
    All sources expose ``resolve_field(record_id, field_name)``.  The default
    implementation raises ``FieldNotResolvableError``; concrete subclasses
    that back addressable data should override it.

    Concrete subclasses must implement (all inherited as abstract from StreamBase):
    - ``identity_structure() -> Any``
    - ``pipeline_identity_structure()`` is provided here (schema-only)
    - ``iter_packets()``, ``keys()``, ``as_table()``, ``output_schema()``
    """

    def __init__(
        self,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._explicit_source_id = source_id

    # -------------------------------------------------------------------------
    # Source identity
    # -------------------------------------------------------------------------

    @property
    def source_id(self) -> str:
        """
        Canonical name for this source used in the registry and provenance
        strings.  Defaults to the content hash when not explicitly set.
        """
        if self._explicit_source_id is not None:
            return self._explicit_source_id
        return self.content_hash().to_hex(
            char_count=self.orcapod_config.path_hash_n_char
        )

    # -------------------------------------------------------------------------
    # Field resolution
    # -------------------------------------------------------------------------

    def resolve_field(self, record_id: str, field_name: str) -> Any:
        """
        Return the value of *field_name* for the record identified by
        *record_id*.

        The default implementation raises ``FieldNotResolvableError``.
        Subclasses that back addressable data should override this.

        Parameters
        ----------
        record_id:
            The record identifier as it appears in the source-info provenance
            string (e.g. ``"row_0"``, ``"user_id=abc123"``).
        field_name:
            The name of the field/column to retrieve.

        Raises
        ------
        FieldNotResolvableError
            When this source does not support field resolution.
        """
        raise FieldNotResolvableError(
            f"{self.__class__.__name__} (source_id={self.source_id!r}) does not "
            f"support field resolution.  Cannot resolve field {field_name!r} "
            f"for record {record_id!r}."
        )

    # -------------------------------------------------------------------------
    # PipelineElementProtocol — schema-only identity (base case of Merkle chain)
    # -------------------------------------------------------------------------

    def pipeline_identity_structure(self) -> Any:
        """
        Return (tag_schema, packet_schema) as the pipeline identity for this
        source.  Schema-only: no data content is included, so sources with
        identical schemas share the same pipeline hash and therefore the same
        pipeline database table.
        """
        tag_schema, packet_schema = self.output_schema()
        return (tag_schema, packet_schema)

    # -------------------------------------------------------------------------
    # StreamProtocol protocol
    # -------------------------------------------------------------------------

    @property
    def source(self) -> None:
        """Root sources have no upstream source pod."""
        return None

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        """Sources have no upstream dependencies."""
        return ()
