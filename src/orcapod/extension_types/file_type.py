"""orcapod.File — content-identified, existence-validated file path.

``File`` wraps a ``upath.UPath`` and validates that the path points to a readable,
non-directory file at construction time. Use ``pathlib.Path`` / ``upath.UPath`` for
paths that may not yet exist.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Self

from upath import UPath
from upath.extensions import ProxyUPath

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol


class File(ProxyUPath):
    """A content-identified, existence-validated file path.

    Wraps a ``UPath`` and validates that the path refers to a readable, non-directory
    file at construction time. Works across local, S3, GCS, and any other
    fsspec-backed backend supported by ``upath``.

    Note:
        ``isinstance(file_instance, UPath)`` returns ``False`` because ``ProxyUPath``
        does not inherit from ``UPath``. Use ``isinstance(x, File)`` to type-check.

    Args:
        *args: Positional path arguments forwarded to ``UPath``.
        follow_symlinks: If ``True`` (default), symlinks are followed and the
            resolved target is validated and hashed. If ``False``, raises
            ``ValueError`` on construction when the path is a symlink.
        **kwargs: Keyword arguments forwarded to ``UPath``.

    Raises:
        FileNotFoundError: If the path does not exist.
        IsADirectoryError: If the path is a directory (or a symlink to one when
            ``follow_symlinks=True``).
        ValueError: If the path is a symlink and ``follow_symlinks=False``, or if
            the path exists but is not a regular file.

    Example:
        >>> f = File("/tmp/data.csv")
        >>> str(f)
        '/tmp/data.csv'
        >>> File("/tmp/nonexistent.csv")
        FileNotFoundError: ...
    """

    def __init__(self, *args: Any, follow_symlinks: bool = True, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if not follow_symlinks and self.__wrapped__.is_symlink():
            raise ValueError(
                f"File: path is a symlink and follow_symlinks=False: {self.__wrapped__!r}"
            )
        if not self.__wrapped__.exists():
            raise FileNotFoundError(
                f"File: path does not exist: {self.__wrapped__!r}"
            )
        if self.__wrapped__.is_dir():
            raise IsADirectoryError(
                f"File: path is a directory: {self.__wrapped__!r}"
            )
        if not self.__wrapped__.is_file():
            raise ValueError(
                f"File: path is not a regular file: {self.__wrapped__!r}"
            )

    @classmethod
    def _from_upath(cls, upath: UPath, /) -> Self:
        """Create a ``File`` from an existing ``UPath`` without validation.

        Used internally by ``ProxyUPath`` for derived paths (e.g. ``.parent``,
        ``/`` operator). Validation is intentionally skipped — derived paths from
        navigation may not exist yet. ``follow_symlinks`` defaults to ``True`` on
        all derived instances.
        """
        obj = object.__new__(cls)
        obj.__wrapped__ = upath
        return obj
