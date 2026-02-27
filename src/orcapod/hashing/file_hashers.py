from orcapod.hashing.hash_utils import hash_file
from orcapod.protocols.hashing_protocols import (
    FileContentHasher,
    StringCacher,
)
from orcapod.types import ContentHash, PathLike


class BasicFileHasher:
    """Basic implementation for file hashing."""

    def __init__(
        self,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
    ):
        self.algorithm = algorithm
        self.buffer_size = buffer_size

    def hash_file(self, file_path: PathLike) -> ContentHash:
        return hash_file(
            file_path, algorithm=self.algorithm, buffer_size=self.buffer_size
        )


class CachedFileHasher:
    """File hasher with caching."""

    def __init__(
        self,
        file_hasher: FileContentHasher,
        string_cacher: StringCacher,
    ):
        self.file_hasher = file_hasher
        self.string_cacher = string_cacher

    def hash_file(self, file_path: PathLike) -> ContentHash:
        cache_key = f"file:{file_path}"
        cached_value = self.string_cacher.get_cached(cache_key)
        if cached_value is not None:
            return bytes.fromhex(cached_value)

        value = self.file_hasher.hash_file(file_path)
        self.string_cacher.set_cached(cache_key, value.hex())
        return value
