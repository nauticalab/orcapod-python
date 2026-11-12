"""Logical type identifiers.

A logical type's identifier is the pair (Arrow extension name, Arrow extension
metadata) of the extension type it returns from ``get_arrow_extension_type``.
The identifier is owned by the type's implementer and opaque to Orcapod: Orcapod
stores it with the column, hashes all of it wherever the type is hashed, and
compares stored metadata byte for byte with what the type says it can read.

The identifier must change whenever an existing value would be stored or hashed
differently, and only then. It is not a package version: a type that learns to
store new values keeps its identifier as long as every old value is stored and
hashed as before.

This module must stay free of heavy imports; it is used by the hashing code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.logical_types.protocols import LogicalTypeProtocol


@dataclass(frozen=True)
class LogicalTypeIdentifier:
    """The identifier of a logical type: Arrow extension name and metadata.

    Attributes:
        name: The Arrow extension name (``ARROW:extension:name``).
        metadata: The Arrow extension metadata bytes (``ARROW:extension:metadata``);
            empty when the type writes none.
    """

    name: str
    metadata: bytes

    @property
    def text(self) -> str:
        """The identifier as text, as it enters hashed inputs.

        ``<extension name>;<lowercase hex of the metadata bytes>``, for example
        ``orcapod.path;7b2276657273696f6e223a317d``. The metadata is opaque bytes,
        hence hex; the hex contains no ``;``, and an extension name may not
        contain one, so the text decodes to exactly one pair.
        """
        return f"{self.name};{self.metadata.hex()}"


class LogicalTypeIdentifierError(ValueError):
    """A stored column's logical type identifier cannot be read by this installation.

    A logical type is identified by its Arrow extension name and extension
    metadata. This error is raised when a stored column carries a name this
    installation knows with metadata the registered logical type does not
    declare as readable. Orcapod never falls back to the storage type or to the
    current version of the type, because that could misread the values.

    Attributes:
        stored: The identifier found with the stored column.
        readable: The extension metadata values this installation reads for
            that extension name.
        column: The column name, when the raising code knows it.
    """

    def __init__(
        self,
        stored: LogicalTypeIdentifier,
        readable: tuple[bytes, ...],
        column: str | None = None,
    ) -> None:
        """Build the error and its message.

        Args:
            stored: The identifier found with the stored column.
            readable: The extension metadata values this installation reads for
                the stored extension name; may be empty.
            column: Name of the column, if the raising code knows it. When
                ``None`` the message says "A column"; ``for_column`` adds the
                name later.
        """
        self.stored = stored
        self.readable = readable
        self.column = column
        subject = f"Column {column!r}" if column is not None else "A column"
        readable_text = ", ".join(repr(m) for m in readable) or "nothing"
        super().__init__(
            f"{subject} was stored as logical type {stored.name!r} with extension "
            f"metadata {stored.metadata!r}, which this installation cannot read. "
            f"For {stored.name!r} it reads extension metadata: {readable_text}. "
            f"The data was probably written by a different version of the type; "
            f"read it with a version that supports that identifier."
        )

    def for_column(self, column: str) -> "LogicalTypeIdentifierError":
        """Return the same error naming ``column``.

        Args:
            column: The name of the column the identifier was stored with.

        Returns:
            A new ``LogicalTypeIdentifierError`` with ``column`` set.
        """
        return LogicalTypeIdentifierError(self.stored, self.readable, column)


def version_metadata(version: int) -> bytes:
    """Return the extension metadata Orcapod's built-in types use for ``version``.

    The convention of the built-in types is canonical JSON ``{"version":N}``.
    Third-party types are free to use any bytes.

    Args:
        version: The version number of the type's stored layout and hashing.

    Returns:
        The metadata bytes, for example ``b'{"version":1}'``.
    """
    return b'{"version":%d}' % version


def arrow_type_identifier(extension_type: "pa.ExtensionType") -> LogicalTypeIdentifier:
    """Return the identifier carried by an Arrow extension type.

    Args:
        extension_type: A ``pa.ExtensionType`` instance.

    Returns:
        Its extension name and serialized metadata.
    """
    return LogicalTypeIdentifier(
        extension_type.extension_name, extension_type.__arrow_ext_serialize__()
    )


def logical_type_identifier(
    logical_type: "LogicalTypeProtocol",
) -> LogicalTypeIdentifier:
    """Return the identifier of a logical type.

    The Arrow extension type is the single place a logical type declares its
    identifier.

    Args:
        logical_type: The logical type.

    Returns:
        The identifier of its Arrow extension type.
    """
    return arrow_type_identifier(logical_type.get_arrow_extension_type())


def require_readable(
    logical_type: "LogicalTypeProtocol", stored_metadata: bytes
) -> None:
    """Raise unless ``logical_type`` reads data stored with ``stored_metadata``.

    Args:
        logical_type: The logical type registered for the stored extension name.
        stored_metadata: The extension metadata found with the stored column
            (empty bytes when none was stored).

    Raises:
        LogicalTypeIdentifierError: If ``stored_metadata`` is not one of the
            values in ``logical_type.readable_extension_metadata()``.
    """
    readable = tuple(logical_type.readable_extension_metadata())
    if stored_metadata not in readable:
        raise LogicalTypeIdentifierError(
            LogicalTypeIdentifier(
                logical_type.get_arrow_extension_type().extension_name,
                stored_metadata,
            ),
            readable,
        )
