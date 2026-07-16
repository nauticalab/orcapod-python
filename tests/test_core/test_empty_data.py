# tests/test_core/test_empty_data.py
"""Tests for EmptyData and its associated exception types."""
from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Data
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.errors import (
    EmptyDataAccessError,
    EmptyDataHashMissingError,
    EphemeralResultMissingError,
)
from orcapod.types import ContentHash


def _make_hash(hex_str: str = "a" * 64) -> ContentHash:
    return ContentHash("arrow_v2.1", bytes.fromhex(hex_str))


class TestExceptionTypes:
    def test_empty_data_access_error_is_exception(self):
        exc = EmptyDataAccessError("sentinel", "as_dict")
        assert isinstance(exc, Exception)
        assert exc.empty_data == "sentinel"
        assert exc.method_name == "as_dict"

    def test_empty_data_hash_missing_error_is_exception(self):
        exc = EmptyDataHashMissingError("sentinel")
        assert isinstance(exc, Exception)
        assert exc.empty_data == "sentinel"

    def test_ephemeral_result_missing_error_is_exception(self):
        exc = EphemeralResultMissingError(
            tag="tag",
            cached_content_hash=None,
            node_identity_path=("a", "b"),
            message="gone",
        )
        assert isinstance(exc, Exception)
        assert exc.tag == "tag"
        assert exc.cached_content_hash is None
        assert exc.node_identity_path == ("a", "b")
        assert "gone" in str(exc)


class TestEmptyDataSubclass:
    def test_is_data_subclass(self):
        assert issubclass(EmptyData, Data)

    def test_instance_is_data(self):
        ed = EmptyData()
        assert isinstance(ed, Data)


class TestEmptyDataContentHash:
    def test_returns_cached_hash_when_set(self):
        h = _make_hash()
        ed = EmptyData(cached_content_hash=h)
        assert ed.content_hash() == h

    def test_raises_when_no_cached_hash(self):
        ed = EmptyData()
        with pytest.raises(EmptyDataHashMissingError):
            ed.content_hash()

    def test_cached_content_hash_property(self):
        h = _make_hash()
        ed = EmptyData(cached_content_hash=h)
        assert ed.cached_content_hash is h

    def test_cached_content_hash_property_none(self):
        ed = EmptyData()
        assert ed.cached_content_hash is None


class TestEmptyDataPayloadAccess:
    """All payload-access methods must raise EmptyDataAccessError."""

    def setup_method(self):
        self.ed = EmptyData(cached_content_hash=_make_hash())

    def test_as_dict_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.as_dict()

    def test_as_table_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.as_table()

    def test_keys_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.keys()

    def test_schema_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.schema()

    def test_arrow_schema_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.arrow_schema()

    def test_identity_structure_raises(self):
        with pytest.raises(EmptyDataAccessError):
            self.ed.identity_structure()


class TestEmptyDataSourceInfo:
    def test_empty_source_info_none_by_default(self):
        ed = EmptyData()
        assert ed.empty_source_info is None

    def test_empty_source_info_stored(self):
        si = {"source_id": "abc", "record_id": None}
        ed = EmptyData(empty_source_info=si)
        assert ed.empty_source_info == si


class TestEmptyDataMetadata:
    def test_record_uuid_assigned(self):
        ed = EmptyData()
        assert ed.datagram_uuid is not None

    def test_custom_record_uuid(self):
        uid = uuid.uuid4()
        ed = EmptyData(record_uuid=uid)
        assert ed.datagram_uuid == uid


class TestCachedFunctionPodLookupCachedData:
    def test_lookup_returns_none_when_cache_empty(self):
        """lookup_cached_data returns None when no result is cached yet."""
        import pyarrow as pa
        from orcapod.core.cached_function_pod import CachedFunctionPod
        from orcapod.core.data_function import PythonDataFunction
        from orcapod.core.datagrams import Data, Tag
        from orcapod.core.function_pod import FunctionPod
        from orcapod.databases import InMemoryArrowDatabase

        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        cached_pod = CachedFunctionPod(pod, result_database=InMemoryArrowDatabase())
        data = Data({"x": 1})
        assert cached_pod.lookup_cached_data(data) is None

    def test_lookup_returns_data_after_process(self):
        """lookup_cached_data returns the cached result after process_data populates the cache."""
        from orcapod.core.cached_function_pod import CachedFunctionPod
        from orcapod.core.data_function import PythonDataFunction
        from orcapod.core.datagrams import Data, Tag
        from orcapod.core.function_pod import FunctionPod
        from orcapod.databases import InMemoryArrowDatabase

        def double(x: int) -> int:
            return x * 2

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        cached_pod = CachedFunctionPod(pod, result_database=InMemoryArrowDatabase())
        tag = Tag({})
        data = Data({"x": 1})
        cached_pod.process_data(tag, data)
        result = cached_pod.lookup_cached_data(data)
        assert result is not None
        assert result.as_dict()["result"] == 2
