"""BasicDirectoryHasher — recursive Merkle tree hashing for directory trees."""

from __future__ import annotations

import fnmatch
import hashlib
import logging
import os
from collections.abc import Callable
from typing import Any

from upath import UPath

from orcapod.hashing.hash_utils import hash_file
from orcapod.types import ContentHash, PathLike

logger = logging.getLogger(__name__)


def _compile_ignore(
    ignore: Callable[[UPath], bool] | list[str] | None,
) -> Callable[[UPath], bool] | None:
    """Convert an ignore spec to a single callable filter.

    Args:
        ignore: ``None`` (no filtering), a list of ``fnmatch`` glob patterns matched
            against entry names, or a callable ``(UPath) -> bool``.

    Returns:
        A callable ``(UPath) -> bool`` returning ``True`` to exclude an entry, or
        ``None`` if no filtering is needed.
    """
    if ignore is None:
        return None
    if callable(ignore):
        return ignore
    patterns = list(ignore)

    def _glob_filter(entry: UPath) -> bool:
        return any(fnmatch.fnmatch(entry.name, pat) for pat in patterns)

    return _glob_filter


def _hash_dir(
    path: UPath,
    filter_fn: Callable[[UPath], bool] | None,
    algorithm: str,
    buffer_size: int,
) -> bytes:
    """Recursively compute the Merkle hash of a directory.

    Returns the raw 32-byte SHA-256 digest for the directory node.
    """
    entries: list[tuple[bytes, bytes]] = []

    for child in path.iterdir():
        if filter_fn is not None and filter_fn(child):
            continue

        name_bytes = child.name.encode("utf-8")

        if child.is_symlink():
            # Hash-as-symlink: record the link target, never dereference.
            target = os.readlink(child)
            entry_bytes = b"symlink\x00" + name_bytes + b"\x00" + target.encode("utf-8")
        elif child.is_file():
            file_hash = hash_file(child, algorithm=algorithm, buffer_size=buffer_size)
            entry_bytes = b"file\x00" + name_bytes + b"\x00" + file_hash.digest
        elif child.is_dir():
            subdir_digest = _hash_dir(child, filter_fn, algorithm, buffer_size)
            entry_bytes = b"dir\x00" + name_bytes + b"\x00" + subdir_digest
        else:
            # Special file (socket, device node, named pipe) — skip silently.
            logger.debug("BasicDirectoryHasher: skipping special file %s", child)
            continue

        entry_digest = hashlib.sha256(entry_bytes).digest()
        entries.append((name_bytes, entry_digest))

    # Sort byte-wise by name — locale-independent, deterministic.
    entries.sort(key=lambda x: x[0])

    h = hashlib.sha256()
    h.update(b"directory\x00")
    for _, entry_digest in entries:
        h.update(entry_digest)
    return h.digest()


class BasicDirectoryHasher:
    """Recursive Merkle tree hasher for directory trees.

    Computes a stable content hash of a directory tree using a recursive Merkle scheme:
    file leaves hash their content, subdirectory nodes hash their sorted children, and
    the root hash propagates the entire tree. Symlinks are recorded as ``(symlink, target)``
    without dereferencing — cycle-safe and deterministic.

    Args:
        algorithm: Hash algorithm for file-content leaves. Defaults to ``"sha256"``.
        buffer_size: Read buffer size in bytes for file content. Defaults to 65536.

    Example:
        >>> hasher = BasicDirectoryHasher()
        >>> result = hasher.hash_directory("/tmp/mydir")
        >>> result.method
        'merkle_sha256'
    """

    def __init__(self, algorithm: str = "sha256", buffer_size: int = 65536) -> None:
        self.algorithm = algorithm
        self.buffer_size = buffer_size

    def hash_directory(
        self,
        directory_path: PathLike,
        ignore: Callable[[UPath], bool] | list[str] | None = None,
    ) -> ContentHash:
        """Compute the recursive Merkle hash of a directory tree.

        Args:
            directory_path: Path to the directory to hash.
            ignore: Optional filter. A callable ``(UPath) -> bool`` returning ``True``
                to exclude an entry, or a list of glob patterns matched against entry
                names via ``fnmatch``. Applied at every level of recursion.

        Returns:
            A ``ContentHash`` with ``method="merkle_sha256"``.
        """
        path = UPath(directory_path)
        filter_fn = _compile_ignore(ignore)
        digest = _hash_dir(path, filter_fn, self.algorithm, self.buffer_size)
        return ContentHash(method="merkle_sha256", digest=digest)
