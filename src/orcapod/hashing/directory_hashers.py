"""BasicDirectoryHasher — recursive Merkle tree hashing for directory trees."""

from __future__ import annotations

import hashlib
import logging
import os
from collections.abc import Callable, Iterable
from pathlib import PurePosixPath

from upath import UPath

from orcapod.protocols.hashing_protocols import FileContentHasherProtocol
from orcapod.types import ContentHash, PathLike

logger = logging.getLogger(__name__)


def _compile_ignore(
    ignore: Callable[[PurePosixPath], bool] | Iterable[str] | None,
) -> Callable[[PurePosixPath], bool] | None:
    """Convert an ignore spec to a single callable filter.

    Args:
        ignore: ``None`` (no filtering), an iterable of glob patterns matched against
            the POSIX relative path from the root via ``pathlib.PurePosixPath.match()``
            (right-anchored: ``"*.pyc"`` matches at any depth; ``"sub/*.pyc"`` matches
            any entry named ``sub/*.pyc`` at any depth — to anchor to the root, prefix
            with ``"/"`` on Python 3.12+ or use a full relative path from the root),
            or a callable ``(PurePosixPath) -> bool``.

    Returns:
        A callable ``(PurePosixPath) -> bool`` returning ``True`` to exclude an entry,
        or ``None`` if no filtering is needed.
    """
    if ignore is None:
        return None
    if callable(ignore):
        return ignore
    patterns = list(ignore)

    def _glob_filter(relative: PurePosixPath) -> bool:
        return any(relative.match(pat) for pat in patterns)

    return _glob_filter


def _hash_dir(
    path: UPath,
    root: UPath,
    filter_fn: Callable[[PurePosixPath], bool] | None,
    algorithm: str,
    file_hasher: FileContentHasherProtocol,
) -> bytes:
    """Recursively compute the Merkle hash of a directory.

    Args:
        path: The directory to hash (current recursion node).
        root: The top-level directory passed to ``BasicDirectoryHasher.hash_directory``.
            Used to compute POSIX relative paths for filter evaluation.
        filter_fn: Optional filter callable; receives the POSIX relative path from
            ``root`` and returns ``True`` to exclude the entry. Excluding a directory
            entry also implicitly excludes its entire subtree (the recursive call is
            skipped for excluded directories).
        algorithm: Hash algorithm name used for structural (entry and node) hashing.
        file_hasher: Hasher used to compute ``ContentHash`` for each file leaf.

    Returns:
        The raw digest bytes for this directory node.
    """
    entries: list[tuple[bytes, bytes]] = []

    for child in path.iterdir():
        try:
            relative = PurePosixPath(child.relative_to(root))
        except ValueError as exc:
            raise RuntimeError(
                f"BasicDirectoryHasher: child path {child!r} is not relative to root {root!r}"
            ) from exc
        if filter_fn is not None and filter_fn(relative):
            continue

        name_bytes = child.name.encode("utf-8")

        if child.is_symlink():
            # Hash-as-symlink: record the link target, never dereference.
            target = os.readlink(child)
            entry_bytes = b"symlink\x00" + name_bytes + b"\x00" + target.encode("utf-8")
        elif child.is_file():
            file_hash = file_hasher.hash_file(child)
            entry_bytes = b"file\x00" + name_bytes + b"\x00" + file_hash.digest
        elif child.is_dir():
            subdir_digest = _hash_dir(child, root, filter_fn, algorithm, file_hasher)
            entry_bytes = b"dir\x00" + name_bytes + b"\x00" + subdir_digest
        else:
            # Special file (socket, device node, named pipe) — skip silently.
            logger.debug("BasicDirectoryHasher: skipping special file %s", child)
            continue

        entry_digest = hashlib.new(algorithm, entry_bytes).digest()
        entries.append((name_bytes, entry_digest))

    # Sort byte-wise by name — locale-independent, deterministic.
    entries.sort(key=lambda x: x[0])

    h = hashlib.new(algorithm)
    h.update(b"directory\x00")
    for _, entry_digest in entries:
        h.update(entry_digest)
    return h.digest()


class BasicDirectoryHasher:
    """Recursive Merkle tree hasher for directory trees.

    Computes a stable content hash of a directory tree using a recursive Merkle scheme:
    file leaves hash their content via the injected ``file_hasher``, subdirectory nodes
    hash their sorted children, and the root hash propagates the entire tree. Symlinks
    are recorded as ``(symlink, target)`` without dereferencing — cycle-safe and
    deterministic.

    Pass a ``CachedFileHasher`` as ``file_hasher`` to avoid re-reading unchanged files
    on repeated calls (e.g. across pipeline runs).

    Args:
        file_hasher: Hasher used to compute the ``ContentHash`` for each file leaf in
            the tree. Any object with a ``hash_file(path) -> ContentHash`` method.
        algorithm: Hash algorithm for structural (Merkle entry and node) hashing.
            Defaults to ``"sha256"``.
        buffer_size: Retained for context round-trip use. File content buffer sizing
            is owned by ``file_hasher``. Defaults to 65536.

    Example:
        >>> from orcapod.hashing.file_hashers import FileHasher
        >>> hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        >>> result = hasher.hash_directory("/tmp/mydir")
        >>> result.method
        'merkle_sha256'
    """

    def __init__(
        self,
        file_hasher: FileContentHasherProtocol,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
    ) -> None:
        self.file_hasher = file_hasher
        self.algorithm = algorithm
        self.buffer_size = buffer_size

    def hash_directory(
        self,
        directory_path: PathLike,
        ignore: Callable[[PurePosixPath], bool] | Iterable[str] | None = None,
    ) -> ContentHash:
        """Compute the recursive Merkle hash of a directory tree.

        Args:
            directory_path: Path to the directory to hash.
            ignore: Optional filter. An iterable of glob patterns matched against the
                **POSIX relative path from the root** via ``pathlib.PurePosixPath.match()``
                (right-anchored: ``"*.pyc"`` matches any ``.pyc`` at any depth;
                ``"sub/*.pyc"`` matches ``.pyc`` files in any directory named ``sub/``
                at any depth — right-anchored, not root-anchored),
                or a callable ``(pathlib.PurePosixPath) -> bool`` returning ``True``
                to exclude an entry. Applied at every level of recursion.

                Hash invariant: excluded entries are invisible to the hash. The result
                is identical to those entries never existing. The pattern string itself
                is not input to the hash. Excluding a directory entry also excludes its
                entire subtree.

        Returns:
            A ``ContentHash`` with ``method="merkle_{algorithm}"``.

        Raises:
            FileNotFoundError: If ``directory_path`` does not exist.
            NotADirectoryError: If ``directory_path`` is not a directory.
            PermissionError: If the directory is not traversable.

        Example:
            >>> from orcapod.hashing.file_hashers import FileHasher
            >>> hasher = BasicDirectoryHasher(file_hasher=FileHasher())
            >>> result = hasher.hash_directory("/tmp/mydir")
            >>> result.method
            'merkle_sha256'
            >>> # Exclude all .pyc files at any depth
            >>> result = hasher.hash_directory("/tmp/mydir", ignore=["*.pyc"])
            >>> # Exclude .pyc only inside build/
            >>> result = hasher.hash_directory("/tmp/mydir", ignore=["build/*.pyc"])
            >>> # Custom callable filter (receives PurePosixPath relative to root)
            >>> result = hasher.hash_directory("/tmp/mydir", ignore=lambda p: p.name.startswith("."))
        """
        path = UPath(directory_path)
        filter_fn = _compile_ignore(ignore)
        digest = _hash_dir(path, path, filter_fn, self.algorithm, self.file_hasher)
        return ContentHash(method=f"merkle_{self.algorithm}", digest=digest)
