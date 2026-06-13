"""Tests for UUIDStructConverter."""
import uuid

import pyarrow as pa
import pytest

from orcapod.semantic_types.semantic_struct_converters import UUIDStructConverter


@pytest.fixture
def converter():
    return UUIDStructConverter()


@pytest.fixture
def sample_uuid():
    return uuid.UUID("550e8400-e29b-41d4-a716-446655440000")


def test_python_type(converter):
    assert converter.python_type is uuid.UUID


def test_arrow_struct_type(converter):
    assert converter.arrow_struct_type == pa.struct([pa.field("uuid", pa.binary(16))])


def test_semantic_type_name(converter):
    assert converter.semantic_type_name == "uuid"


def test_python_to_struct_dict(converter, sample_uuid):
    result = converter.python_to_struct_dict(sample_uuid)
    assert result == {"uuid": sample_uuid.bytes}
    assert isinstance(result["uuid"], bytes)
    assert len(result["uuid"]) == 16


def test_python_to_struct_dict_rejects_non_uuid(converter):
    with pytest.raises(TypeError):
        converter.python_to_struct_dict("550e8400-e29b-41d4-a716-446655440000")  # type: ignore


def test_struct_dict_to_python(converter, sample_uuid):
    struct_dict = {"uuid": sample_uuid.bytes}
    result = converter.struct_dict_to_python(struct_dict)
    assert result == sample_uuid
    assert isinstance(result, uuid.UUID)


def test_struct_dict_to_python_from_bytearray(converter, sample_uuid):
    """Arrow may return binary fields as bytearray — must handle both."""
    struct_dict = {"uuid": bytearray(sample_uuid.bytes)}
    result = converter.struct_dict_to_python(struct_dict)
    assert result == sample_uuid


def test_struct_dict_to_python_missing_field(converter):
    with pytest.raises(ValueError, match="Missing 'uuid' field"):
        converter.struct_dict_to_python({})


def test_round_trip(converter, sample_uuid):
    struct_dict = converter.python_to_struct_dict(sample_uuid)
    recovered = converter.struct_dict_to_python(struct_dict)
    assert recovered == sample_uuid


def test_round_trip_all_versions():
    """Verify round-trip works for uuid4, uuid5, and uuid7 (uuid_utils).

    ``uuid_utils.UUID`` objects do not inherit from ``uuid.UUID`` and their
    ``__eq__`` does not cross-compare with ``uuid.UUID``, so we compare by
    the canonical string representation instead of direct equality.
    """
    from uuid_utils import uuid7

    converter = UUIDStructConverter()
    for u in [uuid.uuid4(), uuid.uuid5(uuid.NAMESPACE_OID, "test"), uuid7()]:
        recovered = converter.struct_dict_to_python(converter.python_to_struct_dict(u))
        assert str(recovered) == str(u)


def test_arrow_array_round_trip(converter, sample_uuid):
    """Verify UUID survives a PyArrow array round-trip."""
    struct_dict = converter.python_to_struct_dict(sample_uuid)
    arr = pa.array([struct_dict], type=pa.struct([pa.field("uuid", pa.binary(16))]))
    recovered_dict = arr[0].as_py()
    recovered_uuid = converter.struct_dict_to_python(recovered_dict)
    assert recovered_uuid == sample_uuid


def test_distinct_uuids_produce_distinct_struct_dicts(converter):
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    assert converter.python_to_struct_dict(u1) != converter.python_to_struct_dict(u2)


def test_can_handle_python_type_uuid(converter):
    assert converter.can_handle_python_type(uuid.UUID) is True


def test_can_handle_python_type_rejects_str(converter):
    assert converter.can_handle_python_type(str) is False


def test_can_handle_struct_type_uuid(converter):
    assert converter.can_handle_struct_type(pa.struct([pa.field("uuid", pa.binary(16))])) is True


def test_can_handle_struct_type_rejects_other(converter):
    import pyarrow as pa

    assert converter.can_handle_struct_type(pa.struct([pa.field("path", pa.large_string())])) is False


def test_hash_struct_dict_returns_string(converter, sample_uuid):
    struct_dict = converter.python_to_struct_dict(sample_uuid)
    result = converter.hash_struct_dict(struct_dict)
    assert isinstance(result, str)
    assert len(result) > 0


def test_hash_struct_dict_consistent(converter, sample_uuid):
    """Same UUID always produces the same hash."""
    struct_dict = converter.python_to_struct_dict(sample_uuid)
    assert converter.hash_struct_dict(struct_dict) == converter.hash_struct_dict(struct_dict)


def test_hash_struct_dict_different_uuids(converter):
    """Different UUIDs produce different hashes."""
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    d1 = converter.python_to_struct_dict(u1)
    d2 = converter.python_to_struct_dict(u2)
    assert converter.hash_struct_dict(d1) != converter.hash_struct_dict(d2)
