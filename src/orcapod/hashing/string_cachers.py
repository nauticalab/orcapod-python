import json
import logging
import random
import sqlite3
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from orcapod.protocols.hashing_protocols import StringCacher

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import redis


class TransferCacher(StringCacher):
    """
    Takes two string cachers as source and destination. Everytime a cached value is retrieved from source,
    the value is also set in the destination cacher.
    This is useful for transferring cached values between different caching mechanisms.
    """

    def __init__(self, source: StringCacher, destination: StringCacher):
        """
        Initialize the TransferCacher.

        Args:
            source: The source cacher to read from
            destination: The destination cacher to write to
        """
        self.source = source
        self.destination = destination

    def transfer(self, cache_key: str) -> str | None:
        """
        Transfer a cached value from source to destination.

        Args:
            cache_key: The key to transfer

        Returns:
            The cached value if found, otherwise None
        """
        # Try to get the cached value from the source
        value = self.source.get_cached(cache_key)
        if value is not None:
            # Set it in the destination cacher
            self.destination.set_cached(cache_key, value)
        return value

    def get_cached(self, cache_key: str) -> str | None:
        # try to get the cached value from the destination first
        value = self.destination.get_cached(cache_key)
        if value is not None:
            return value
        # if not found in destination, get it from source
        value = self.source.get_cached(cache_key)
        if value is not None:
            self.destination.set_cached(cache_key, value)
        return value

    def set_cached(self, cache_key: str, value: str) -> None:
        # Only set the value in the destination cacher
        self.destination.set_cached(cache_key, value)

    def clear_cache(self) -> None:
        self.destination.clear_cache()


class InMemoryCacher(StringCacher):
    """Thread-safe in-memory LRU cache."""

    def __init__(self, max_size: int | None = 1000):
        self.max_size = max_size
        self._cache = {}
        self._access_order = []
        self._lock = threading.RLock()

    def get_cached(self, cache_key: str) -> str | None:
        with self._lock:
            if cache_key in self._cache:
                self._access_order.remove(cache_key)
                self._access_order.append(cache_key)
                return self._cache[cache_key]
            return None

    def set_cached(self, cache_key: str, value: str) -> None:
        with self._lock:
            if cache_key in self._cache:
                self._access_order.remove(cache_key)
            elif self.max_size is not None and len(self._cache) >= self.max_size:
                if len(self._cache) < 1:
                    logger.warning(
                        "Cache is empty, cannot evict any items. "
                        "This may indicate an issue with cache size configuration."
                    )
                    return
                oldest = self._access_order.pop(0)
                del self._cache[oldest]

            self._cache[cache_key] = value
            self._access_order.append(cache_key)

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()
            self._access_order.clear()


class FileCacher(StringCacher):
    """File-based cacher with eventual consistency between memory and disk."""

    def __init__(
        self,
        file_path: str | Path,
        max_size: int | None = 1000,
        sync_probability: float = 0.1,
    ):
        """
        Initialize file-based cacher.

        Args:
            file_path: Path to the JSON file for persistence
            max_size: Maximum number of items to keep in memory (None for unlimited)
            sync_probability: Probability of syncing to disk on each write (0.0 to 1.0)
        """
        self.file_path = Path(file_path)
        self.max_size = max_size
        self.sync_probability = sync_probability
        self._cache = {}
        self._access_order = []
        self._lock = threading.RLock()
        self._sync_lock = threading.RLock()
        self._dirty = False

        # Load existing data from file
        self._load_from_file()

    def _load_from_file(self) -> None:
        """Load cache data from file if it exists."""
        if self.file_path.exists():
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._cache = data.get("cache", {})
                    self._access_order = data.get(
                        "access_order", list(self._cache.keys())
                    )
                    # Ensure access_order only contains keys that exist in cache
                    self._access_order = [
                        k for k in self._access_order if k in self._cache
                    ]
            except (json.JSONDecodeError, IOError) as e:
                logging.warning(f"Failed to load cache from {self.file_path}: {e}")
                self._cache = {}
                self._access_order = []

    def _sync_to_file(self) -> None:
        """
        Sync current cache state to file. Thread-safe with optimized locking.

        Uses a two-phase approach:
        1. Acquire lock, create snapshot of data, release lock
        2. Perform I/O operations without holding lock
        3. Re-acquire lock to update dirty flag

        This minimizes lock contention while ensuring thread safety.
        """
        # Quick dirty check without lock (optimization for common case)
        if not self._dirty:
            return

        # Phase 1: Create snapshot while holding lock
        data_snapshot = None
        should_sync = False

        with self._lock:
            # Double-check pattern - another thread might have synced already
            if not self._dirty:
                return

            try:
                # Create defensive copies of cache state
                data_snapshot = {
                    "cache": self._cache.copy(),
                    "access_order": self._access_order.copy(),
                }
                should_sync = True

            except Exception as e:
                logging.error(f"Failed to create cache snapshot for sync: {e}")
                return

        # Phase 2: Perform expensive I/O operations outside the lock
        if should_sync and data_snapshot:
            with self._sync_lock:
                sync_successful = False
                temp_path = None
                try:
                    # Ensure parent directory exists
                    self.file_path.parent.mkdir(parents=True, exist_ok=True)

                    # Write to temporary file first for atomic operation
                    temp_path = self.file_path.with_suffix(".tmp")

                    with open(temp_path, "w", encoding="utf-8") as f:
                        json.dump(data_snapshot, f, indent=2, ensure_ascii=False)

                    # Atomic rename - this is the critical moment where new data becomes visible
                    temp_path.replace(self.file_path)
                    sync_successful = True

                except (OSError, IOError, TypeError, ValueError, OverflowError) as e:
                    logging.error(f"Failed to sync cache to {self.file_path}: {e}")

                    # Clean up temp file if it exists
                    try:
                        if temp_path is not None and temp_path.exists():
                            temp_path.unlink()
                    except Exception:
                        pass  # Best effort cleanup

                except Exception as e:
                    # Catch any unexpected errors
                    logging.error(f"Unexpected error during cache sync: {e}")

            # Phase 3: Update dirty flag based on sync result
            with self._lock:
                if sync_successful:
                    self._dirty = False
                    logging.debug(f"Successfully synced cache to {self.file_path}")
                # If sync failed, leave _dirty = True so we'll retry later

    def get_cached(self, cache_key: str) -> str | None:
        with self._lock:
            if cache_key in self._cache:
                self._access_order.remove(cache_key)
                self._access_order.append(cache_key)
                self._dirty = True
                return self._cache[cache_key]
            return None

    def set_cached(self, cache_key: str, value: str) -> None:
        with self._lock:
            if cache_key in self._cache:
                self._access_order.remove(cache_key)
            elif self.max_size is not None and len(self._cache) >= self.max_size:
                oldest = self._access_order.pop(0)
                del self._cache[oldest]

            self._cache[cache_key] = value
            self._access_order.append(cache_key)
            self._dirty = True

            # Probabilistic sync to file
            if random.random() < self.sync_probability:
                self._sync_to_file()

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()
            self._access_order.clear()
            self._dirty = True
            self._sync_to_file()

    def force_sync(self) -> None:
        """Force synchronization to file."""
        with self._lock:
            self._sync_to_file()


class SQLiteCacher(StringCacher):
    """SQLite-based cacher with in-memory LRU and database persistence."""

    def __init__(
        self,
        db_path: str | Path,
        max_size: int | None = 1000,
        sync_probability: float = 0.1,
    ):
        """
        Initialize SQLite-based cacher.

        Args:
            db_path: Path to the SQLite database file
            max_size: Maximum number of items to keep in memory (None for unlimited)
            sync_probability: Probability of syncing to database on each write (0.0 to 1.0)
        """
        self.db_path = Path(db_path)
        self.max_size = max_size
        self.sync_probability = sync_probability
        self._cache: dict[str, str] = {}
        self._access_order: list[str] = []
        self._lock = threading.RLock()  # Main cache operations lock
        self._sync_lock = threading.Lock()  # Dedicated database sync lock
        self._dirty_keys: set = set()

        # Initialize database
        self._init_database()
        # Load existing data from database
        self._load_from_database()

    def _init_database(self) -> None:
        """Initialize SQLite database and create table if needed."""
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cache_entries (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        last_accessed REAL DEFAULT (strftime('%f', 'now'))
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_last_accessed
                    ON cache_entries(last_accessed)
                """)
                conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Failed to initialize database {self.db_path}: {e}")
            raise

    def _load_from_database(self) -> None:
        """Load cache data from database."""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute("""
                        SELECT key, value FROM cache_entries
                        ORDER BY last_accessed DESC
                    """)

                    for key, value in cursor:
                        if self.max_size is None or len(self._cache) < self.max_size:
                            self._cache[key] = value
                            self._access_order.append(key)
                        else:
                            break

            except sqlite3.Error as e:
                logging.error(f"Failed to load cache from database {self.db_path}: {e}")

    def _sync_to_database(self) -> None:
        """
        Sync dirty keys to database. Thread-safe with optimized locking.

        Uses a two-phase approach:
        1. Acquire cache lock, create snapshot of dirty data, release cache lock
        2. Perform database operations with dedicated sync lock
        3. Re-acquire cache lock to clear dirty flags
        """
        # Quick check without any locks
        if not self._dirty_keys:
            return

        # Phase 1: Create snapshot of dirty data while holding cache lock
        dirty_snapshot = {}
        keys_to_delete = set()
        should_sync = False

        with self._lock:
            # Double-check pattern
            if not self._dirty_keys:
                return

            try:
                # Create snapshot of dirty keys and their current values
                for key in self._dirty_keys:
                    if key in self._cache:
                        dirty_snapshot[key] = self._cache[key]
                    else:
                        # Key was removed from memory cache
                        keys_to_delete.add(key)

                should_sync = bool(dirty_snapshot or keys_to_delete)

            except Exception as e:
                logging.error(f"Failed to create dirty snapshot for database sync: {e}")
                return

        # Phase 2: Perform database operations with dedicated sync lock
        if should_sync:
            sync_successful = False

            # Use dedicated sync lock to prevent multiple threads from
            # hitting the database simultaneously
            with self._sync_lock:
                try:
                    with sqlite3.connect(self.db_path) as conn:
                        # Update/insert dirty keys
                        for key, value in dirty_snapshot.items():
                            conn.execute(
                                """
                                INSERT OR REPLACE INTO cache_entries (key, value, last_accessed)
                                VALUES (?, ?, strftime('%f', 'now'))
                            """,
                                (key, value),
                            )

                        # Delete removed keys
                        for key in keys_to_delete:
                            conn.execute(
                                "DELETE FROM cache_entries WHERE key = ?", (key,)
                            )

                        conn.commit()
                        sync_successful = True

                except sqlite3.Error as e:
                    logging.error(
                        f"Failed to sync cache to database {self.db_path}: {e}"
                    )
                except Exception as e:
                    logging.error(f"Unexpected error during database sync: {e}")

            # Phase 3: Clear dirty flags only for successfully synced keys
            with self._lock:
                if sync_successful:
                    # Remove synced keys from dirty set
                    self._dirty_keys -= set(dirty_snapshot.keys())
                    self._dirty_keys -= keys_to_delete

    def get_cached(self, cache_key: str) -> str | None:
        with self._lock:
            if cache_key in self._cache:
                # Update access order in memory
                self._access_order.remove(cache_key)
                self._access_order.append(cache_key)
                self._dirty_keys.add(cache_key)  # Mark for timestamp update
                return self._cache[cache_key]

            # Try loading from database if not in memory
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute(
                        "SELECT value FROM cache_entries WHERE key = ?", (cache_key,)
                    )
                    row = cursor.fetchone()
                    if row:
                        value = row[0]
                        # Add to memory cache with LRU eviction
                        self._add_to_memory_cache(cache_key, value)
                        self._dirty_keys.add(cache_key)  # Mark for timestamp update
                        return value

            except sqlite3.Error as e:
                logging.error(f"Failed to query database {self.db_path}: {e}")

            return None

    def _add_to_memory_cache(self, key: str, value: str) -> None:
        """Add item to memory cache with LRU eviction. Must be called under lock."""
        if self.max_size is not None and len(self._cache) >= self.max_size:
            # Evict oldest from memory (but keep in database)
            oldest = self._access_order.pop(0)
            del self._cache[oldest]

        self._cache[key] = value
        self._access_order.append(key)

    def set_cached(self, cache_key: str, value: str) -> None:
        with self._lock:
            if cache_key in self._cache:
                self._access_order.remove(cache_key)
            elif self.max_size is not None and len(self._cache) >= self.max_size:
                # Evict oldest from memory
                oldest = self._access_order.pop(0)
                del self._cache[oldest]

            self._cache[cache_key] = value
            self._access_order.append(cache_key)
            self._dirty_keys.add(cache_key)

            # Probabilistic sync to database
            if random.random() < self.sync_probability:
                # Safe to call without worrying about locks
                self._sync_to_database()

    def clear_cache(self) -> None:
        with self._lock:
            # Mark all current keys for deletion from database
            self._dirty_keys.update(self._cache.keys())
            self._cache.clear()
            self._access_order.clear()

        # Force immediate sync to clear database
        # Use dedicated method that ensures complete clearing
        self._clear_database()

    def _clear_database(self) -> None:
        """Clear all entries from database. Thread-safe."""
        with self._sync_lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("DELETE FROM cache_entries")
                    conn.commit()

                # Clear dirty keys since we've cleared everything
                with self._lock:
                    self._dirty_keys.clear()

            except sqlite3.Error as e:
                logging.error(f"Failed to clear database {self.db_path}: {e}")

    def force_sync(self) -> None:
        """Force synchronization to database. Always thread-safe."""
        self._sync_to_database()

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {
                "memory_cache_size": len(self._cache),
                "max_memory_size": self.max_size,
                "dirty_keys_count": len(self._dirty_keys),
                "db_path": str(self.db_path),
                "sync_probability": self.sync_probability,
            }

    def vacuum_database(self) -> None:
        """Vacuum the SQLite database to reclaim space. Expensive operation."""
        with self._sync_lock:  # Prevent concurrent database operations
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("VACUUM")
                    conn.commit()
                logging.info(f"Successfully vacuumed database {self.db_path}")
            except sqlite3.Error as e:
                logging.error(f"Failed to vacuum database {self.db_path}: {e}")

    def get_database_stats(self) -> dict[str, Any]:
        """Get database-level statistics."""
        with self._sync_lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    # Get total count
                    cursor = conn.execute("SELECT COUNT(*) FROM cache_entries")
                    total_count = cursor.fetchone()[0]

                    # Get database file size
                    db_size = (
                        self.db_path.stat().st_size if self.db_path.exists() else 0
                    )

                    return {
                        "total_database_entries": total_count,
                        "database_file_size_bytes": db_size,
                        "database_path": str(self.db_path),
                    }
            except (sqlite3.Error, OSError) as e:
                logging.error(f"Failed to get database stats: {e}")
                return {
                    "total_database_entries": -1,
                    "database_file_size_bytes": -1,
                    "database_path": str(self.db_path),
                }

    def close(self) -> None:
        """Close the cacher and perform final sync."""
        self.force_sync()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def __del__(self):
        """Destructor - ensure final sync."""
        try:
            self.close()
        except Exception:
            pass  # Avoid exceptions in destructor


class RedisCacher(StringCacher):
    """Redis-based cacher with graceful failure handling."""

    def __init__(
        self,
        connection: "redis.Redis | None" = None,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        key_prefix: str = "cache:",
        password: str | None = None,
        socket_timeout: float = 5.0,
    ):
        """
        Initialize Redis-based cacher.

        Args:
            connection: Existing Redis connection (if None, creates new connection)
            host: Redis host (used if connection is None)
            port: Redis port (used if connection is None)
            db: Redis database number (used if connection is None)
            key_prefix: Prefix for all cache keys (acts as namespace/topic)
            password: Redis password (used if connection is None)
            socket_timeout: Socket timeout in seconds
        """
        # TODO: cleanup the redis use pattern
        try:
            import redis
        except ImportError as e:
            raise ImportError(
                "Could not import Redis module. redis package is required for RedisCacher"
            ) from e

        self._redis_module = redis
        self.key_prefix = key_prefix
        self._connection_failed = False
        self._lock = threading.RLock()

        # Establish connection
        if connection is not None:
            self.redis = connection
        else:
            self.redis = self._redis_module.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_timeout,
                decode_responses=True,
            )

        # Test connection and topic access
        self._test_connection()

    def _test_connection(self) -> None:
        """Test Redis connection and ensure we can create/access the topic."""

        with self._lock:
            try:
                # Test basic connection
                self.redis.ping()

                # Test key creation/access with our prefix
                test_key = f"{self.key_prefix}__connection_test__"
                self.redis.set(test_key, "test", ex=10)  # 10 second expiry
                result = self.redis.get(test_key)
                self.redis.delete(test_key)

                if result != "test":
                    raise self._redis_module.RedisError("Failed to verify key access")

                logging.info(
                    f"Redis connection established successfully with prefix '{self.key_prefix}'"
                )

            except (
                self._redis_module.RedisError,
                self._redis_module.ConnectionError,
            ) as e:
                logging.error(f"Failed to establish Redis connection: {e}")
                raise RuntimeError(f"Redis connection test failed: {e}")

    def _get_prefixed_key(self, cache_key: str) -> str:
        """Get the full Redis key with prefix."""
        return f"{self.key_prefix}{cache_key}"

    def _handle_redis_error(self, operation: str, error: Exception) -> None:
        """Handle Redis errors by setting connection failed flag and logging."""
        if not self._connection_failed:
            logging.error(
                f"Redis {operation} failed: {error}. Cache will return None for all requests."
            )
            self._connection_failed = True

    def get_cached(self, cache_key: str) -> str | None:
        with self._lock:
            if self._connection_failed:
                return None

            try:
                result = self.redis.get(self._get_prefixed_key(cache_key))
                if result is None:
                    return None
                logger.info(f"Retrieved cached value from Redis for key {cache_key}")
                # Decode bytes to string if necessary
                if isinstance(result, bytes):
                    return result.decode("utf-8")

                return str(result)

            except (
                self._redis_module.RedisError,
                self._redis_module.ConnectionError,
            ) as e:
                self._handle_redis_error("get", e)
                return None

    def set_cached(self, cache_key: str, value: str) -> None:
        with self._lock:
            if self._connection_failed:
                logger.warning(
                    "Redis connection failed, cannot set cache. "
                    "Cache will not be updated."
                )
                return

            try:
                logger.info(f"Saving cached value to Redis for key {cache_key}")

                self.redis.set(self._get_prefixed_key(cache_key), value)

            except (
                self._redis_module.RedisError,
                self._redis_module.ConnectionError,
            ) as e:
                self._handle_redis_error("set", e)

    def clear_cache(self) -> None:
        with self._lock:
            if self._connection_failed:
                return

            try:
                pattern = f"{self.key_prefix}*"
                keys = self.redis.keys(pattern)
                if keys:
                    self.redis.delete(*list(keys))  # type: ignore[arg-type]

            except (
                self._redis_module.RedisError,
                self._redis_module.ConnectionError,
            ) as e:
                self._handle_redis_error("clear", e)

    def is_connected(self) -> bool:
        """Check if Redis connection is still active."""
        return not self._connection_failed

    def reset_connection(self) -> bool:
        """Attempt to reset the connection after failure."""
        with self._lock:
            try:
                self._test_connection()
                self._connection_failed = False
                logging.info("Redis connection successfully reset")
                return True
            except Exception as e:
                logging.error(f"Failed to reset Redis connection: {e}")
                return False
