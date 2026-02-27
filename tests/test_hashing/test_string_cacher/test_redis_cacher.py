# """Tests for RedisCacher using mocked Redis."""

# from typing import cast, TYPE_CHECKING
# from unittest.mock import patch, MagicMock

# import pytest

# from orcapod.hashing.string_cachers import RedisCacher

# if TYPE_CHECKING:
#     import redis


# # Mock Redis exceptions
# class MockRedisError(Exception):
#     """Mock for redis.RedisError"""

#     pass


# class MockConnectionError(Exception):
#     """Mock for redis.ConnectionError"""

#     pass


# class MockRedis:
#     """Mock Redis client for testing."""

#     def __init__(self, fail_connection=False, fail_operations=False):
#         self.data = {}
#         self.fail_connection = fail_connection
#         self.fail_operations = fail_operations
#         self.ping_called = False

#     def ping(self):
#         self.ping_called = True
#         if self.fail_connection:
#             raise MockConnectionError("Connection failed")
#         return True

#     def set(self, key, value, ex=None):
#         if self.fail_operations:
#             raise MockRedisError("Operation failed")
#         self.data[key] = value
#         return True

#     def get(self, key):
#         if self.fail_operations:
#             raise MockRedisError("Operation failed")
#         return self.data.get(key)

#     def delete(self, *keys):
#         if self.fail_operations:
#             raise MockRedisError("Operation failed")
#         deleted = 0
#         for key in keys:
#             if key in self.data:
#                 del self.data[key]
#                 deleted += 1
#         return deleted

#     def keys(self, pattern):
#         if self.fail_operations:
#             raise MockRedisError("Operation failed")
#         if pattern.endswith("*"):
#             prefix = pattern[:-1]
#             return [key for key in self.data.keys() if key.startswith(prefix)]
#         return [key for key in self.data.keys() if key == pattern]


# class MockRedisModule:
#     ConnectionError = MockConnectionError
#     RedisError = MockRedisError
#     Redis = MagicMock(return_value=MockRedis())  # Simple one-liner!


# def mock_get_redis():
#     return MockRedisModule


# def mock_no_redis():
#     return None


# class TestRedisCacher:
#     """Test cases for RedisCacher with mocked Redis."""

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_basic_operations(self):
#         """Test basic get/set/clear operations."""
#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="test:")

#         # Test empty cache
#         assert cacher.get_cached("nonexistent") is None

#         # Test set and get
#         cacher.set_cached("key1", "value1")
#         assert cacher.get_cached("key1") == "value1"

#         # Test overwrite
#         cacher.set_cached("key1", "new_value1")
#         assert cacher.get_cached("key1") == "new_value1"

#         # Test multiple keys
#         cacher.set_cached("key2", "value2")
#         assert cacher.get_cached("key1") == "new_value1"
#         assert cacher.get_cached("key2") == "value2"

#         # Test clear
#         cacher.clear_cache()
#         assert cacher.get_cached("key1") is None
#         assert cacher.get_cached("key2") is None

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_key_prefixing(self):
#         """Test that keys are properly prefixed."""
#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="myapp:")

#         cacher.set_cached("key1", "value1")

#         # Check that the key is stored with prefix
#         assert "myapp:key1" in mock_redis.data
#         assert mock_redis.data["myapp:key1"] == "value1"

#         # But retrieval should work without prefix
#         assert cacher.get_cached("key1") == "value1"

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_connection_initialization_success(self):
#         """Test successful connection initialization."""
#         mock_redis = MockRedis()

#         with patch("logging.info") as mock_log:
#             cacher = RedisCacher(connection=mock_redis, key_prefix="test:")
#             mock_log.assert_called_once()
#             assert "Redis connection established successfully" in str(
#                 mock_log.call_args
#             )

#         assert mock_redis.ping_called
#         assert cacher.is_connected()

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_connection_initialization_failure(self):
#         """Test connection initialization failure."""
#         mock_redis = MockRedis(fail_connection=True)

#         with pytest.raises(RuntimeError, match="Redis connection test failed"):
#             RedisCacher(connection=mock_redis, key_prefix="test:")

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_new_connection_creation(self):
#         """Test creation of new Redis connection when none provided."""
#         cacher = RedisCacher(host="localhost", port=6379, db=0, key_prefix="test:")

#         # Verify Redis was called with correct parameters
#         # Get the mock module to verify calls
#         mock_module = mock_get_redis()
#         mock_module.Redis.assert_called_with(
#             host="localhost",
#             port=6379,
#             db=0,
#             password=None,
#             socket_timeout=5.0,
#             socket_connect_timeout=5.0,
#             decode_responses=True,
#         )

#         assert cacher.is_connected()

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_graceful_failure_on_operations(self):
#         """Test graceful failure when Redis operations fail during use."""
#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="test:")

#         # Initially should work
#         cacher.set_cached("key1", "value1")
#         assert cacher.get_cached("key1") == "value1"
#         assert cacher.is_connected()

#         # Simulate Redis failure
#         mock_redis.fail_operations = True

#         with patch("logging.error") as mock_log:
#             # Operations should fail gracefully
#             result = cacher.get_cached("key1")
#             assert result is None
#             assert not cacher.is_connected()
#             mock_log.assert_called_once()
#             assert "Redis get failed" in str(mock_log.call_args)

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_set_failure_handling(self):
#         """Test handling of set operation failures."""
#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="test:")

#         # Simulate set failure
#         mock_redis.fail_operations = True

#         with patch("logging.error") as mock_log:
#             cacher.set_cached("key1", "value1")  # Should not raise
#             mock_log.assert_called_once()
#             assert "Redis set failed" in str(mock_log.call_args)
#             assert not cacher.is_connected()

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_clear_cache_failure_handling(self):
#         """Test handling of clear cache operation failures."""
#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="test:")

#         # Add some data first
#         cacher.set_cached("key1", "value1")

#         # Simulate clear failure
#         mock_redis.fail_operations = True

#         with patch("logging.error") as mock_log:
#             cacher.clear_cache()  # Should not raise
#             mock_log.assert_called_once()
#             assert "Redis clear failed" in str(mock_log.call_args)
#             assert not cacher.is_connected()

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_clear_cache_with_pattern_matching(self):
#         """Test that clear_cache only removes keys with the correct prefix."""
#         mock_redis = MockRedis()

#         # Manually add keys with different prefixes
#         mock_redis.data["test:key1"] = "value1"
#         mock_redis.data["test:key2"] = "value2"
#         mock_redis.data["other:key1"] = "other_value1"

#         cacher = RedisCacher(connection=mock_redis, key_prefix="test:")
#         cacher.clear_cache()

#         # Only keys with "test:" prefix should be removed
#         assert "test:key1" not in mock_redis.data
#         assert "test:key2" not in mock_redis.data
#         assert "other:key1" in mock_redis.data  # Should remain

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_connection_reset(self):
#         """Test connection reset functionality."""
#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="test:")

#         # Simulate connection failure
#         mock_redis.fail_operations = True
#         cacher.get_cached("key1")  # This should mark connection as failed
#         assert not cacher.is_connected()

#         # Reset connection
#         mock_redis.fail_operations = False  # Fix the "connection"

#         with patch("logging.info") as mock_log:
#             success = cacher.reset_connection()
#             assert success
#             assert cacher.is_connected()
#             # Check that the reset message was logged (it should be the last call)
#             mock_log.assert_called_with("Redis connection successfully reset")

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_connection_reset_failure(self):
#         """Test connection reset failure handling."""
#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="test:")

#         # Simulate connection failure
#         mock_redis.fail_operations = True
#         cacher.get_cached("key1")  # Mark connection as failed

#         # Keep connection broken for reset attempt
#         mock_redis.fail_connection = True

#         with patch("logging.error") as mock_log:
#             success = cacher.reset_connection()
#             assert not success
#             assert not cacher.is_connected()
#             # Check that the reset failure message was logged (should be the last call)
#             mock_log.assert_called_with(
#                 "Failed to reset Redis connection: Redis connection test failed: Connection failed"
#             )

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_error_logging_only_once(self):
#         """Test that errors are only logged once per failure."""
#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="test:")

#         # Simulate failure
#         mock_redis.fail_operations = True

#         with patch("logging.error") as mock_log:
#             # Multiple operations should only log error once
#             cacher.get_cached("key1")
#             cacher.get_cached("key2")
#             cacher.set_cached("key3", "value3")

#             # Should only log the first error
#             assert mock_log.call_count == 1

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_default_key_prefix(self):
#         """Test default key prefix behavior."""
#         mock_redis = MockRedis()
#         # Don't specify key_prefix, should use default
#         cacher = RedisCacher(connection=mock_redis)

#         cacher.set_cached("key1", "value1")

#         # Should use default prefix "cache:"
#         assert "cache:key1" in mock_redis.data
#         assert cacher.get_cached("key1") == "value1"

#     def test_redis_not_available(self):
#         """Test behavior when redis package is not available."""
#         with patch("orcapod.hashing.string_cachers._get_redis", mock_no_redis):
#             with pytest.raises(ImportError, match="redis package is required"):
#                 RedisCacher()

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_connection_test_key_access_failure(self):
#         """Test failure when connection test can't create/access test key."""

#         # Create a MockRedis that allows ping but fails key verification
#         class FailingKeyMockRedis(MockRedis):
#             def get(self, key):
#                 if key.endswith("__connection_test__"):
#                     return "wrong_value"  # Return wrong value for test key
#                 return super().get(key)

#         mock_redis = FailingKeyMockRedis()

#         with pytest.raises(RuntimeError, match="Redis connection test failed"):
#             RedisCacher(connection=mock_redis, key_prefix="test:")

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_thread_safety(self):
#         """Test thread safety of Redis operations."""
#         import threading

#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="thread_test:")

#         results = {}
#         errors = []

#         def worker(thread_id: int):
#             try:
#                 for i in range(50):
#                     key = f"thread{thread_id}_key{i}"
#                     value = f"thread{thread_id}_value{i}"
#                     cacher.set_cached(key, value)

#                     # Verify immediately
#                     result = cacher.get_cached(key)
#                     if result != value:
#                         errors.append(
#                             f"Thread {thread_id}: Expected {value}, got {result}"
#                         )

#                 # Final verification
#                 thread_results = []
#                 for i in range(50):
#                     key = f"thread{thread_id}_key{i}"
#                     result = cacher.get_cached(key)
#                     thread_results.append(result)

#                 results[thread_id] = thread_results

#             except Exception as e:
#                 errors.append(e)

#         # Start multiple threads
#         threads = []
#         for i in range(3):
#             t = threading.Thread(target=worker, args=(i,))
#             threads.append(t)
#             t.start()

#         # Wait for completion
#         for t in threads:
#             t.join()

#         # Check for errors
#         assert not errors, f"Thread safety errors: {errors}"

#         # Verify each thread's results
#         for thread_id in range(3):
#             thread_results = results[thread_id]
#             for i, result in enumerate(thread_results):
#                 expected = f"thread{thread_id}_value{i}"
#                 assert result == expected

#     @patch("orcapod.hashing.string_cachers._get_redis", mock_get_redis)
#     def test_operations_after_connection_failure(self):
#         """Test that operations return None/do nothing after connection failure."""
#         mock_redis = MockRedis()
#         cacher = RedisCacher(connection=mock_redis, key_prefix="test:")

#         # Add some data initially
#         cacher.set_cached("key1", "value1")
#         assert cacher.get_cached("key1") == "value1"

#         # Simulate connection failure
#         mock_redis.fail_operations = True

#         # This should mark connection as failed
#         result = cacher.get_cached("key1")
#         assert result is None
#         assert not cacher.is_connected()

#         # All subsequent operations should return None/do nothing without trying Redis
#         assert cacher.get_cached("key2") is None
#         cacher.set_cached("key3", "value3")  # Should do nothing
#         cacher.clear_cache()  # Should do nothing

#         # Redis should not receive any more calls after initial failure
#         call_count_before = len([k for k in mock_redis.data.keys()])
#         cacher.set_cached("key4", "value4")
#         call_count_after = len([k for k in mock_redis.data.keys()])
#         assert call_count_before == call_count_after  # No new calls to Redis
