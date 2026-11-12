"""Utilities shared across database implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import TableCommittingDatabaseProtocol


def coerce_record_id(record_id: str | bytes) -> bytes:
    """Encode ``record_id`` to bytes if it is a ``str``.

    All database implementations store record IDs as ``bytes``
    (``pa.large_binary()``).  This helper lets callers pass plain strings
    without the caller needing to know the encoding — UTF-8 is used, which
    is a lossless round-trip for any ASCII or text-based ID.

    Args:
        record_id: A record identifier as either ``bytes`` or a ``str``.

    Returns:
        The record identifier as ``bytes``.
    """
    if isinstance(record_id, str):
        return record_id.encode()
    return record_id


def as_table_committing(database: object) -> TableCommittingDatabaseProtocol | None:
    """Return ``database`` if it commits table by table, else ``None`` (ITL-919).

    The one check for ``TableCommittingDatabaseProtocol``, used by ``PipelineJob.run``
    (the ``batch_size`` rule) and by function nodes (results committed before their
    pipeline records). It looks the attributes up on the object, so a wrapper that
    forwards them only when the database it wraps has them (``ExtensionAwareDatabase``)
    qualifies exactly when that database does; ``isinstance`` on the protocol would
    not, because it inspects the class.

    Args:
        database: Any database object.

    Returns:
        ``database``, typed as the protocol, or ``None``.
    """
    try:
        batch_size = getattr(database, "batch_size")  # noqa: B009  # AttributeError means "no"
        flush_batch = getattr(database, "flush_batch")  # noqa: B009
    except AttributeError:
        return None
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        return None
    if not callable(flush_batch):
        return None
    return cast("TableCommittingDatabaseProtocol", database)
