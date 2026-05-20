from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from orcapod.protocols.core_protocols.streams import StreamProtocol
from orcapod.types import Cursor

T = TypeVar("T")

if TYPE_CHECKING:
    from polars._typing import FrameInitTypes

    from orcapod.protocols.database_protocols import DatabaseRegistryProtocol


@runtime_checkable
class SourceProtocol(StreamProtocol, Protocol):
    """
    Protocol for root sources — streams with no upstream dependencies that
    expose provenance identity and optional field resolution.

    A SourceProtocol is a StreamProtocol where:
    - ``source`` is always ``None`` (no upstream pod)
    - ``upstreams`` is always empty
    - ``source_id`` provides a canonical name for registry and provenance
    - ``resolve_field`` enables lookup of individual field values by record id
    """

    @property
    def source_id(self) -> str: ...

    def resolve_field(self, record_id: str, field_name: str) -> Any: ...

    def to_config(
        self, db_registry: DatabaseRegistryProtocol | None = None
    ) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict.

        Args:
            db_registry: Optional registry for deduplicating embedded database
                configs at save time.  Sources that do not embed database
                references ignore this parameter.
        """
        ...

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        db_registry: DatabaseRegistryProtocol | None = None,
    ) -> "SourceProtocol":
        """Reconstruct a source instance from a config dict.

        Args:
            config: Dict as produced by ``to_config``.
            db_registry: Optional registry for resolving embedded database
                config keys at load time.  Sources that do not embed database
                references ignore this parameter.
        """
        ...


@runtime_checkable
class DynamicSourceProtocol(Protocol[T]):
    """User-supplied protocol for a polling data source.

    Implementations provide three async methods. The framework handles
    scheduling, cursor tracking, cache management, error handling, and
    lifecycle.

    Type parameter ``T`` is the cursor value type (e.g. ``datetime``, ``int``,
    ``str``).

    Cursor contract:
        ``poll()`` returns ``True`` if new data is available since the given
        cursor, ``False`` otherwise. Cursor advancement is tied to data
        reading — ``fetch()`` returns a ``(new_cursor, data)`` tuple, and the
        framework advances the cursor only after a successful fetch.

    Full-state invalidation:
        Raise ``CursorInvalidatedError`` from ``poll()`` or ``fetch()`` when
        previous state is no longer valid. This is a terminal condition —
        ``PollingSource`` will close its channel cleanly.

    Example::

        class MyDBSource:
            def __init__(self, db):
                self._db = db

            async def poll(
                self, cursor: Cursor[datetime] | None = None
            ) -> bool:
                latest = await self._db.latest_modified_at()
                return cursor is None or latest > cursor.value

            async def fetch(
                self, cursor: Cursor[datetime] | None = None
            ) -> tuple[Cursor[datetime], pl.DataFrame]:
                since = cursor.value if cursor else None
                df = await self._db.fetch_rows_since(since)
                latest = await self._db.latest_modified_at()
                return Cursor(value=latest, modified_at=latest), df

            async def close(self) -> None:
                await self._db.disconnect()
    """

    async def poll(self, cursor: Cursor[T] | None = None) -> bool:
        """Check whether new data is available.

        Args:
            cursor: The framework's current cursor position, or ``None`` on
                the first call.

        Returns:
            ``True`` if new data is available since *cursor*, ``False`` if
            nothing has changed.

        Raises:
            CursorInvalidatedError: If previous state is no longer valid.
        """
        ...

    async def fetch(
        self, cursor: Cursor[T] | None = None
    ) -> tuple[Cursor[T], "FrameInitTypes"]:
        """Fetch data from the given cursor position onward.

        Called only when ``poll()`` has returned ``True``. Returns both the
        new cursor position and the data so cursor advancement is always
        tied to a successful read.

        Args:
            cursor: The current cursor position, or ``None`` on the first
                call. Implementations that cannot filter by cursor may ignore
                this and return full state.

        Returns:
            A tuple ``(new_cursor, data)`` where *data* is anything accepted
            by ``pl.DataFrame()`` — polars DataFrame, pandas DataFrame,
            PyArrow Table, dict, list, etc.

        Raises:
            CursorInvalidatedError: If previous state is no longer valid.
        """
        ...

    async def close(self) -> None:
        """Release resources held by this source.

        Called on every termination path: normal duration expiry, pipeline
        cancellation, max error threshold exceeded, or
        ``CursorInvalidatedError``. The framework guarantees ``close()`` is
        awaited before the output channel is closed.
        """
        ...
