from __future__ import annotations

from abc import abstractmethod
from collections.abc import Collection, Iterator
from typing import TYPE_CHECKING, Any

from orcapod.core.base import TraceableBase
from orcapod.errors import FieldNotResolvableError
from orcapod.protocols.core_protocols import Stream
from orcapod.types import ColumnConfig, Schema

if TYPE_CHECKING:
    import pyarrow as pa


class RootSource(TraceableBase):
    """
    Abstract base class for all sources in Orcapod.

    A RootSource is a Pod that takes no input streams — it is the root of a
    computational graph, producing data from an external source (file, database,
    in-memory data, etc.).

    It simultaneously satisfies both the Pod protocol and the Stream protocol:

    - As a Pod:  ``process()`` is called with no input streams and returns a
      Stream.  ``validate_inputs`` rejects any provided streams.
      ``argument_symmetry`` always returns an empty ordered group.

    - As a Stream: all stream methods (``keys``, ``output_schema``,
      ``iter_packets``, ``as_table``) delegate straight through to
      ``self.process()``.  ``source`` returns ``self``; ``upstreams`` is always
      empty.  No caching is performed at this level — caching is the
      responsibility of concrete subclasses.

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

    Concrete subclasses must implement:
    - ``process(*streams, label=None) -> Stream``
    - ``output_schema(*streams, columns=..., all_info=...) -> tuple[Schema, Schema]``
    - ``identity_structure() -> Any``  (required by TraceableBase)
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
    # Pod protocol
    # -------------------------------------------------------------------------

    @property
    def uri(self) -> tuple[str, ...]:
        return (self.__class__.__name__, self.content_hash().to_hex())

    def validate_inputs(self, *streams: Stream) -> None:
        """Sources accept no input streams."""
        if streams:
            raise ValueError(
                f"{self.__class__.__name__} is a source and takes no input streams, "
                f"but {len(streams)} stream(s) were provided."
            )

    def argument_symmetry(self, streams: Collection[Stream]) -> tuple[()]:
        """Sources have no input arguments."""
        if streams:
            raise ValueError(
                f"{self.__class__.__name__} is a source and takes no input streams."
            )
        return ()

    @abstractmethod
    def output_schema(
        self,
        *streams: Stream,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Return the (tag_schema, packet_schema) for this source.

        Compatible with both the Pod protocol (which passes ``*streams``) and
        the Stream protocol (which passes no positional arguments).  Concrete
        implementations should ignore ``streams`` — it will always be empty for
        a source.
        """
        ...

    @abstractmethod
    def process(self, *streams: Stream, label: str | None = None) -> Stream:
        """
        Return a Stream representing the current state of this source.

        Concrete subclasses choose their own execution and caching model.
        This method is called with no input streams.
        """
        ...

    # -------------------------------------------------------------------------
    # Stream protocol — pure delegation to self.process()
    # -------------------------------------------------------------------------

    @property
    def source(self) -> "RootSource":
        """A source is its own source."""
        return self

    @property
    def upstreams(self) -> tuple[Stream, ...]:
        """Sources have no upstream dependencies."""
        return ()

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self.process().keys(columns=columns, all_info=all_info)

    def iter_packets(self) -> Iterator[tuple[Any, Any]]:
        return self.process().iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self.process().as_table(columns=columns, all_info=all_info)
