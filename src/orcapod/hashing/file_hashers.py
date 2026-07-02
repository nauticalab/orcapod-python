from dataclasses import dataclass

from upath import UPath

from orcapod.hashing.hash_utils import hash_file
from orcapod.protocols.hashing_protocols import (
    CacherProtocol,
    FileContentHasherProtocol,
)
from orcapod.types import ContentHash, PathLike


@dataclass(frozen=True)
class FileHashKey:
    """Cache lookup key for a file hash.

    The key captures the three file attributes that together identify a
    file's content without reading it: its absolute path, last-modified
    time in nanoseconds, and byte size.

    Attributes:
        path: Absolute, resolved ``UPath``. Any ``PathLike`` input is
            normalised to ``UPath`` before constructing this key.
        mtime_ns: Last-modified time in nanoseconds (``path.stat().st_mtime_ns``).
        size: File size in bytes (``path.stat().st_size``).
    """

    path: UPath
    mtime_ns: int
    size: int


class FileHasher:
    """Hash file content using a configurable algorithm.

    Args:
        algorithm: Hashing algorithm to use. Defaults to ``"sha256"``.
        buffer_size: Read buffer size in bytes. Defaults to 65536.
    """

    def __init__(
        self,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
    ):
        self.algorithm = algorithm
        self.buffer_size = buffer_size

    def hash_file(self, file_path: PathLike) -> ContentHash:
        """Hash the file at ``file_path`` and return its ``ContentHash``.

        Args:
            file_path: Path to the file to hash.

        Returns:
            ContentHash of the file's content.
        """
        return hash_file(
            file_path, algorithm=self.algorithm, buffer_size=self.buffer_size
        )


class CachedFileHasher:
    """File hasher that caches results to avoid redundant I/O.

    Wraps any ``FileContentHasherProtocol`` with a
    ``CacherProtocol[FileHashKey, ContentHash]``. On each call to
    ``hash_file``:

    1. Resolve the path to an absolute ``UPath`` and stat the file.
    2. Look up ``(path, mtime_ns, size)`` in the cacher.
    3. On hit: return the cached ``ContentHash`` directly.
    4. On miss: delegate to the inner hasher, store the result, return it.

    Both ``FileHasher`` and ``CachedFileHasher`` satisfy
    ``FileContentHasherProtocol`` — callers do not need to know which they
    have.

    Args:
        file_hasher: Inner hasher that performs the actual content hashing.
        cacher: Cache backend implementing
            ``CacherProtocol[FileHashKey, ContentHash]``.
    """

    def __init__(
        self,
        file_hasher: FileContentHasherProtocol,
        cacher: "CacherProtocol[FileHashKey, ContentHash]",
    ) -> None:
        self.file_hasher = file_hasher
        self.cacher = cacher

    def hash_file(self, file_path: PathLike) -> ContentHash:
        """Return the ``ContentHash`` for ``file_path``, using the cache.

        Args:
            file_path: Path to the file to hash.

        Returns:
            ContentHash of the file's content (from cache or computed).
        """
        path = file_path if isinstance(file_path, UPath) else UPath(file_path)
        path = path.resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)

        hit = self.cacher.get(key)
        if hit is not None:
            return hit

        result = self.file_hasher.hash_file(file_path)
        self.cacher.put(key, result)
        return result
