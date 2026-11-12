"""The v0.3 value-based array format: storage, canonical form, hash and refusals (ITL-888).

Specification: ``superpowers/specs/2026-11-10-array-format-v0.3.md``. Tests go
through sources, streams, function pods and the default data context's
semantic hasher.
"""

from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.contexts import get_default_context
from orcapod.operators import MergeJoin
from orcapod.sources import ArrowTableSource, DictSource
from tests.test_arrays.array_cases import ARRAYS, assert_arrays_identical


def reference_digest(tag: str, shape: tuple[int, ...], data: bytes) -> bytes:
    """Independent implementation of the v1 array digest (spec C1)."""
    t = tag.encode("utf-8")
    preimage = struct.pack("<Q", len(t)) + t + struct.pack("<Q", len(shape))
    preimage += b"".join(struct.pack("<q", d) for d in shape)
    preimage += struct.pack("<Q", len(data)) + data
    return hashlib.sha256(preimage).digest()


def stored(value: Any) -> dict[str, Any]:
    """The stored row of a one-row ``x`` column."""
    table = DictSource(data=[{"k": "r", "x": value}], tag_columns=["k"]).as_table()
    chunk = table.column("x").chunk(0)
    assert isinstance(chunk, pa.ExtensionArray)
    return chunk.storage[0].as_py()


def read_back(value: Any) -> Any:
    source = DictSource(data=[{"k": "r", "x": value}], tag_columns=["k"])
    return next(iter(source.iter_data()))[1].as_dict()["x"]


def semantic_hash(value: Any) -> Any:
    return get_default_context().semantic_hasher.hash_object(value)


def data_hash(value: Any) -> Any:
    source = DictSource(data=[{"k": "r", "x": value}], tag_columns=["k"])
    return next(iter(source.iter_data()))[1].content_hash()


# ---------------------------------------------------------------------------
# Column type
# ---------------------------------------------------------------------------


def test_column_type_is_orcapod_ndarray_over_the_array_struct() -> None:
    """An array column is ``orcapod.ndarray`` over the array struct with version metadata."""
    table = DictSource(
        data=[{"k": "r", "x": np.arange(3.0)}], tag_columns=["k"]
    ).as_table()
    ext = table.schema.field("x").type
    assert isinstance(ext, pa.ExtensionType)
    assert ext.extension_name == "orcapod.ndarray"
    assert ext.storage_type == pa.struct(
        [
            pa.field("dtype", pa.large_string()),
            pa.field("shape", pa.large_list(pa.int64())),
            pa.field("data", pa.large_binary()),
        ]
    )
    assert ext.__arrow_ext_serialize__() == b'{"version":1}'


def test_old_extension_name_is_not_registered() -> None:
    """Only ``orcapod.ndarray`` is registered; the old ``numpy.ndarray`` name is not."""
    converter: Any = get_default_context().type_converter
    assert converter.get_logical_type_by_arrow_extension_name("numpy.ndarray") is None
    assert (
        converter.get_logical_type_by_arrow_extension_name("orcapod.ndarray")
        is not None
    )


# ---------------------------------------------------------------------------
# Stored rows: dtype tags, shape, little-endian C-order data
# ---------------------------------------------------------------------------

TAG_CASES: list[tuple[str, npt.NDArray[Any], str]] = [
    ("bool", np.array([True, False]), "bool"),
    ("int8", np.array([1, -1], dtype=np.int8), "int8"),
    ("int16", np.array([1, -1], dtype="<i2"), "int16"),
    ("int32", np.array([1, -1], dtype="<i4"), "int32"),
    ("int64", np.array([1, -1], dtype="<i8"), "int64"),
    ("uint8", np.array([1, 255], dtype=np.uint8), "uint8"),
    ("uint16", np.array([1], dtype="<u2"), "uint16"),
    ("uint32", np.array([1], dtype="<u4"), "uint32"),
    ("uint64", np.array([1], dtype="<u8"), "uint64"),
    ("float16", np.array([0.5], dtype="<f2"), "float16"),
    ("float32", np.array([0.5], dtype="<f4"), "float32"),
    ("float64", np.array([0.5], dtype="<f8"), "float64"),
    ("complex64", np.array([1 + 2j], dtype="<c8"), "complex64"),
    ("complex128", np.array([1 + 2j], dtype="<c16"), "complex128"),
    ("datetime_ns", np.array([0], dtype="<M8[ns]"), "datetime64[ns]"),
    ("datetime_D", np.array([0], dtype="<M8[D]"), "datetime64[D]"),
    ("datetime_10ms", np.array([3], dtype="<M8[10ms]"), "datetime64[10ms]"),
    ("timedelta_s", np.array([5], dtype="<m8[s]"), "timedelta64[s]"),
    ("ucs4", np.array(["ab", "c"], dtype="<U3"), "ucs4[3]"),
    ("bytes", np.array([b"ab"], dtype="S4"), "bytes[4]"),
    ("void", np.array([b"\x01\x02"], dtype="V2"), "void[2]"),
    (
        "structured",
        np.zeros(2, dtype=[("a", "<i4"), ("b", "<f8")]),
        '[["a","int32"],["b","float64"]]',
    ),
    (
        "subarray_field",
        np.zeros(1, dtype=[("a", "<i4", (2, 3)), ("b", "u1")]),
        '[["a","int32",[2,3]],["b","uint8"]]',
    ),
    (
        "nested",
        np.zeros(1, dtype=[("p", [("y", "<f4"), ("x", "<f4")]), ("n", "<i8")]),
        '[["p",[["y","float32"],["x","float32"]]],["n","int64"]]',
    ),
    (
        "non_ascii_name",
        np.zeros(1, dtype=[("é", "<i2")]),
        '[["\\u00e9","int16"]]',
    ),
]


@pytest.mark.parametrize(
    ("array", "tag"), [c[1:] for c in TAG_CASES], ids=[c[0] for c in TAG_CASES]
)
def test_stored_row_holds_tag_shape_and_c_order_little_endian_data(
    array: npt.NDArray[Any], tag: str
) -> None:
    """The stored row holds the dtype tag, the shape and the packed element bytes."""
    row = stored(array)
    assert row["dtype"] == tag
    assert row["shape"] == list(array.shape)
    assert len(row["data"]) == array.size * np.dtype(read_back(array).dtype).itemsize


@pytest.mark.parametrize(
    "name", ["float32", "three_d", "complex128", "unicode", "datetime64_ns"]
)
def test_data_is_the_c_order_little_endian_buffer(name: str) -> None:
    """The stored data is the array's C-order, little-endian buffer."""
    array = ARRAYS[name]
    assert stored(array)["data"] == np.ascontiguousarray(array).tobytes()


def test_zero_d_and_empty_shapes() -> None:
    """Zero-dimensional and empty arrays store their exact shape and data."""
    assert stored(np.array(2.5))["shape"] == []
    assert stored(np.array(2.5))["data"] == struct.pack("<d", 2.5)
    assert stored(np.zeros((0, 3), np.float32)) == {
        "dtype": "float32",
        "shape": [0, 3],
        "data": b"",
    }
    assert stored(np.zeros((3, 0), np.float32))["shape"] == [3, 0]


def test_structured_data_is_packed_in_declared_field_order() -> None:
    """Structured data is stored little-endian and packed in declared field order."""
    array = np.array([(1.5, 7)], dtype=[("b", ">f8"), ("a", "<i4")])
    row = stored(array)
    assert row["dtype"] == '[["b","float64"],["a","int32"]]'
    assert row["data"] == struct.pack("<d", 1.5) + struct.pack("<i", 7)


# ---------------------------------------------------------------------------
# Canonical form on read
# ---------------------------------------------------------------------------


def test_fortran_order_reads_back_c_order() -> None:
    """A Fortran-ordered array reads back C-ordered with the same values."""
    array = np.asfortranarray(np.arange(6, dtype=np.float32).reshape(2, 3))
    assert_arrays_identical(read_back(array), np.ascontiguousarray(array))


def test_strided_view_reads_back_as_its_values() -> None:
    """A strided view reads back as a C-contiguous array of its values."""
    base = np.arange(24, dtype=np.int16).reshape(4, 6)
    view = base[::2, 1::2]
    assert_arrays_identical(read_back(view), np.ascontiguousarray(view))


@pytest.mark.parametrize("dtype", [">f4", ">i8", ">c16", ">M8[ns]", ">U3"])
def test_big_endian_reads_back_little_endian_with_equal_values(dtype: str) -> None:
    """A big-endian array reads back little-endian with equal values."""
    raw = (
        ["ab", "c"] if dtype.startswith(">U") else np.arange(2).astype(dtype[0] + "i8")
    )
    big = np.array(raw).astype(dtype)
    got = read_back(big)
    assert got.dtype == big.dtype.newbyteorder("<")
    assert (got == big).all()


def test_structured_reads_back_in_stored_field_order_and_packed() -> None:
    """An aligned structured array reads back in declared field order, little-endian and packed."""
    aligned = np.dtype([("b", ">f8"), ("a", "<i2")], align=True)
    array = np.array([(1.5, 7), (-2.0, 9)], dtype=aligned)
    got = read_back(array)
    assert got.dtype == np.dtype([("b", "<f8"), ("a", "<i2")])
    assert got.dtype.names == ("b", "a")
    assert got.dtype.itemsize == 10
    assert got["a"].tolist() == [7, 9]
    assert got["b"].tolist() == [1.5, -2.0]


def test_nested_structured_reads_back_in_stored_field_order() -> None:
    """A nested structured array reads back in declared field order at every depth."""
    inner = np.dtype([("y", ">i2"), ("x", "<i2")])
    array = np.array([((1, 2), 3.5)], dtype=[("s", inner), ("a", "<f8")])
    got = read_back(array)
    assert got.dtype == np.dtype([("s", [("y", "<i2"), ("x", "<i2")]), ("a", "<f8")])
    assert got["s"]["y"].tolist() == [1] and got["s"]["x"].tolist() == [2]


def test_reordered_fields_are_a_different_stored_value() -> None:
    """Arrays whose fields differ only in order are different stored values."""
    ab = np.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")])
    ba = np.array([(2.0, 1)], dtype=[("b", "<f8"), ("a", "<i4")])
    assert stored(ab)["dtype"] != stored(ba)["dtype"]
    assert stored(ab) != stored(ba)
    assert read_back(ab).dtype.names == ("a", "b")
    assert read_back(ba).dtype.names == ("b", "a")


def test_zero_d_structured_round_trip() -> None:
    """A zero-dimensional structured array round-trips."""
    array = np.array((3, 1.5), dtype=[("b", "<i4"), ("a", "<f8")])
    got = read_back(array)
    assert got.shape == ()
    assert got["a"] == 1.5 and got["b"] == 3


def test_read_back_is_writeable() -> None:
    """An array read back from storage is writeable."""
    got = read_back(np.arange(3.0))
    got[0] = 10.0
    assert got[0] == 10.0


# ---------------------------------------------------------------------------
# Subclasses: accepted and read back as plain ndarray
# ---------------------------------------------------------------------------


def test_matrix_and_recarray_read_back_as_plain_arrays(tmp_path: Path) -> None:
    """``np.matrix``, ``np.recarray`` and ``np.memmap`` read back and hash as plain arrays."""
    matrix = np.matrix([[1.0, 2.0], [3.0, 4.0]])
    rec = np.rec.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")])
    memmap = np.memmap(tmp_path / "m.dat", dtype=np.int32, mode="w+", shape=(3,))
    memmap[:] = [1, 2, 3]
    for sub in (matrix, rec, memmap):
        assert_arrays_identical(read_back(sub), np.array(np.asarray(sub)))
        assert semantic_hash(sub) == semantic_hash(np.array(np.asarray(sub)))


class _ViewChangesValues(np.ndarray[Any, Any]):
    """A subclass whose ``view`` override returns different values."""

    def view(self, *args: Any, **kwargs: Any) -> Any:
        return np.zeros(self.shape, dtype=self.dtype)


def test_subclass_view_override_does_not_change_stored_or_hashed_values() -> None:
    """A subclass's ``view`` override cannot change the stored or hashed values."""
    base = np.arange(1.0, 4.0)
    sub = base.view(_ViewChangesValues)
    assert_arrays_identical(read_back(sub), base)
    assert semantic_hash(sub) == semantic_hash(base)
    assert data_hash(sub) == data_hash(base)


# ---------------------------------------------------------------------------
# Hash: value digest under the ndarray_v1 label
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(ARRAYS))
def test_semantic_hash_is_the_v1_digest_of_the_stored_row(name: str) -> None:
    """The semantic hash is the ``ndarray_v1`` digest of the stored row."""
    row = stored(ARRAYS[name])
    h = semantic_hash(ARRAYS[name])
    assert h.method == "ndarray_v1"
    assert h.digest == reference_digest(row["dtype"], tuple(row["shape"]), row["data"])


def test_pinned_digests() -> None:
    """Pinned digests match the spec's byte rules."""
    # Pinned v1 digests; computed by ``reference_digest`` from the spec's byte rules.
    cases = {
        "float32_2x3": (np.arange(6, dtype=np.float32).reshape(2, 3), "float32"),
        "zero_d": (np.array(2.5), "float64"),
        "empty": (np.zeros((0, 3), np.int8), "int8"),
    }
    for array, tag in cases.values():
        expected = reference_digest(
            tag, array.shape, np.ascontiguousarray(array).tobytes()
        )
        assert semantic_hash(array).digest == expected
    assert (
        semantic_hash(cases["zero_d"][0]).digest.hex()
        == reference_digest("float64", (), struct.pack("<d", 2.5)).hex()
    )


def _equal_value_variants() -> list[tuple[str, npt.NDArray[Any], npt.NDArray[Any]]]:
    a = np.arange(24, dtype=np.float32).reshape(4, 6)
    base = np.zeros((4, 12), dtype=np.float32)
    base[:, ::2] = a
    rec_ab = np.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")])
    rec_ab_big = np.array([(1, 2.0)], dtype=[("a", ">i4"), ("b", ">f8")])
    aligned = np.array(
        [(1, 2.0)], dtype=np.dtype([("a", "<i4"), ("b", "<f8")], align=True)
    )
    padded = np.array(
        [(1, 2.0)],
        dtype=np.dtype(
            {
                "names": ["a", "b"],
                "formats": ["<i4", "<f8"],
                "offsets": [0, 8],
                "itemsize": 24,
            }
        ),
    )
    # Declared order a, b; in memory b comes first.
    offsets_swapped = np.array(
        [(1, 2.0)],
        dtype=np.dtype(
            {
                "names": ["a", "b"],
                "formats": ["<i4", "<f8"],
                "offsets": [8, 0],
                "itemsize": 12,
            }
        ),
    )
    return [
        ("fortran", a, np.asfortranarray(a)),
        ("strided", a, base[:, ::2]),
        ("transposed_twice", a, a.T.T),
        ("big_endian", a, a.astype(">f4")),
        ("structured_byte_order", rec_ab, rec_ab_big),
        ("aligned", rec_ab, aligned),
        ("padded", rec_ab, padded),
        ("offsets_swapped", rec_ab, offsets_swapped),
        (
            "matrix",
            np.arange(4.0).reshape(2, 2),
            np.matrix(np.arange(4.0).reshape(2, 2)),
        ),
    ]


@pytest.mark.parametrize(
    ("left", "right"),
    [v[1:] for v in _equal_value_variants()],
    ids=[v[0] for v in _equal_value_variants()],
)
def test_layout_byte_order_padding_and_subclass_do_not_change_the_hash(
    left: npt.NDArray[Any], right: npt.NDArray[Any]
) -> None:
    """Memory layout, byte order, padding and subclass do not change the hash."""
    assert semantic_hash(left) == semantic_hash(right)
    assert data_hash(left) == data_hash(right)


def _different_value_pairs() -> list[tuple[str, npt.NDArray[Any], npt.NDArray[Any]]]:
    nan_a = np.array([np.nan])
    nan_b = np.frombuffer(struct.pack("<Q", 0x7FF8000000000001), dtype="<f8").copy()
    return [
        ("dtype_width", np.arange(3, dtype=np.int32), np.arange(3, dtype=np.int64)),
        ("dtype_kind", np.arange(3, dtype=np.int64), np.arange(3, dtype=np.uint64)),
        ("shape", np.zeros((2, 3)), np.zeros((3, 2))),
        ("empty_shape", np.zeros((0, 3)), np.zeros((3, 0))),
        ("zero_d_vs_1", np.array(1.0), np.array([1.0])),
        ("value", np.array([1.0]), np.array([2.0])),
        ("negative_zero", np.array([0.0]), np.array([-0.0])),
        ("nan_payload", nan_a, nan_b),
        (
            "field_name",
            np.zeros(1, dtype=[("a", "<i4")]),
            np.zeros(1, dtype=[("b", "<i4")]),
        ),
        (
            "field_order",
            np.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")]),
            np.array([(2.0, 1)], dtype=[("b", "<f8"), ("a", "<i4")]),
        ),
        (
            "nested_field_order",
            np.zeros(1, dtype=[("s", [("x", "<i4"), ("y", "<i4")])]),
            np.zeros(1, dtype=[("s", [("y", "<i4"), ("x", "<i4")])]),
        ),
        ("unit", np.array([1], "M8[s]"), np.array([1], "M8[ms]")),
    ]


@pytest.mark.parametrize(
    ("left", "right"),
    [v[1:] for v in _different_value_pairs()],
    ids=[v[0] for v in _different_value_pairs()],
)
def test_dtype_shape_and_bits_change_the_hash(
    left: npt.NDArray[Any], right: npt.NDArray[Any]
) -> None:
    """Dtype, shape, field order and element bits change the hash."""
    assert semantic_hash(left) != semantic_hash(right)
    assert data_hash(left) != data_hash(right)


def test_table_token_carries_extension_name_and_digest() -> None:
    """A table token carries the extension name and the value digest, no label."""
    # The Arrow hasher folds the extension name in front of the handler's digest.
    from orcapod.hashing.arrow_hashers import StarfixArrowHasher

    hasher = get_default_context().arrow_hasher
    assert isinstance(hasher, StarfixArrowHasher)
    array = np.arange(3.0)
    table = DictSource(data=[{"k": "r", "x": array}], tag_columns=["k"]).as_table()
    # Non-public helper: this test is about the token the hasher builds for a cell.
    processed = hasher._process_table_columns(table.select(["x"]))  # pyright: ignore[reportPrivateUsage]
    token = processed.column("x")[0].as_py()
    assert (
        token
        == b"orcapod.ndarray;7b2276657273696f6e223a317d::" + semantic_hash(array).digest
    )


# ---------------------------------------------------------------------------
# Refusals: named column and dtype
# ---------------------------------------------------------------------------

REFUSED: list[tuple[str, Any, str]] = [
    ("object", np.array([1, None], dtype=object), "object"),
    ("object_field", np.zeros(1, dtype=[("a", "O")]), "object"),
    ("masked", np.ma.array([1.0, 2.0], mask=[False, True]), "masked"),
    ("longdouble", np.zeros(2, dtype=np.longdouble), "longdouble"),
    ("clongdouble", np.zeros(2, dtype=np.clongdouble), "longdouble"),
    ("generic_datetime", np.array(["NaT"], dtype="M8"), "unit"),
    ("generic_timedelta", np.array([0], dtype="m8"), "unit"),
    ("titles", np.zeros(1, dtype=[(("title", "a"), "<i4")]), "title"),
]
if hasattr(np, "dtypes") and hasattr(np.dtypes, "StringDType"):
    REFUSED.append(
        ("string_dtype", np.array(["a"], dtype=np.dtypes.StringDType()), "StringDType")
    )


@pytest.mark.parametrize(
    ("value", "reason"), [c[1:] for c in REFUSED], ids=[c[0] for c in REFUSED]
)
def test_unsupported_arrays_are_refused_naming_column_and_dtype(
    value: Any, reason: str
) -> None:
    """Unsupported arrays are refused at write time, naming the column and the dtype."""
    with pytest.raises(ValueError, match="'payload'") as info:
        DictSource(data=[{"k": "r", "payload": value}], tag_columns=["k"]).as_table()
    assert reason.lower() in str(info.value).lower()


def test_null_child_on_read_raises_naming_the_column() -> None:
    """A stored row with a null child raises on read, naming the column."""
    converter = get_default_context().type_converter
    ext = converter.python_type_to_arrow_type(np.ndarray)
    assert isinstance(ext, pa.ExtensionType)
    storage = pa.array(
        [{"dtype": "float64", "shape": None, "data": b""}], type=ext.storage_type
    )
    table = pa.table(
        {
            "k": pa.array(["r"], pa.large_string()),
            "x": pa.ExtensionArray.from_storage(ext, storage),
        }
    )
    # The first read of the value (hashing the source rows) must fail naming the column.
    with pytest.raises((ValueError, RuntimeError), match="'x'.*null field"):
        source = ArrowTableSource(table, tag_columns=["k"])
        for _, data in source.iter_data():
            data.as_dict()


@pytest.mark.parametrize(
    "entry",
    [
        ["a"],
        ["a", "int32", [2], "extra"],
        [1, "int32"],
        ["a", "int32", 2],
        ["a", "int32", ["2"]],
        ["a", "int32", [True]],
        ["a", "int32", [-1]],
    ],
    ids=[
        "one_part",
        "four_parts",
        "non_string_name",
        "shape_not_a_list",
        "shape_of_strings",
        "shape_of_bools",
        "negative_dimension",
    ],
)
def test_malformed_structured_tag_entry_raises_on_read(
    entry: list[Any],
) -> None:
    """A malformed structured tag entry raises the malformed-tag error on read."""
    converter = get_default_context().type_converter
    ext = converter.python_type_to_arrow_type(np.ndarray)
    assert isinstance(ext, pa.ExtensionType)
    tag = json.dumps([entry])
    storage = pa.array(
        [{"dtype": tag, "shape": [], "data": b"\x00" * 8}], type=ext.storage_type
    )
    table = pa.table(
        {
            "k": pa.array(["r"], pa.large_string()),
            "x": pa.ExtensionArray.from_storage(ext, storage),
        }
    )
    with pytest.raises((ValueError, RuntimeError), match="malformed array dtype tag"):
        source = ArrowTableSource(table, tag_columns=["k"])
        for _, data in source.iter_data():
            data.as_dict()


# ---------------------------------------------------------------------------
# Function pods: a declared np.ndarray return must be an ndarray
# ---------------------------------------------------------------------------


def test_scalar_returned_for_declared_ndarray_is_refused_at_the_pod() -> None:
    """A NumPy scalar returned for a declared ``np.ndarray`` output is refused at the pod."""

    @function_pod(output_keys="y")
    def double(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # the bare np.ndarray annotation is under test
        return x * 2  # a 0-d input gives a NumPy scalar

    source = DictSource(
        data=[{"k": "a", "x": np.array(7.5, np.float32)}], tag_columns=["k"]
    )
    with pytest.raises(TypeError, match="float32.*'y'.*ndarray"):
        list(double.pod(source).iter_data())


def test_none_returned_for_optional_ndarray_is_accepted() -> None:
    """``None`` returned for an optional ``np.ndarray`` output is accepted."""

    @function_pod(output_keys="y")
    def nothing(x: np.ndarray) -> np.ndarray | None:  # pyright: ignore[reportMissingTypeArgument]  # the bare np.ndarray annotation is under test
        return None

    source = DictSource(data=[{"k": "a", "x": np.arange(2.0)}], tag_columns=["k"])
    assert [d.as_dict()["y"] for _, d in nothing.pod(source).iter_data()] == [None]


# ---------------------------------------------------------------------------
# MergeJoin orders merged arrays by the stored (dtype tag, shape, data)
# ---------------------------------------------------------------------------


def test_merge_join_orders_arrays_by_stored_tag_shape_and_data() -> None:
    """``MergeJoin`` orders arrays by their stored dtype tag, shape and data."""
    small = np.array([7.5], dtype=np.float64)
    large = np.array([107.5], dtype=np.float64)
    other = np.array([1], dtype=np.int8)
    left = DictSource(
        data=[{"k": "a", "x": large}, {"k": "b", "x": other}],
        tag_columns=["k"],
        source_id="l",
    )
    right = DictSource(
        data=[{"k": "a", "x": small}, {"k": "b", "x": small}],
        tag_columns=["k"],
        source_id="r",
    )
    got: dict[Any, Any] = {
        t.as_dict()["k"]: d.as_dict()["x"]
        for t, d in MergeJoin()(left, right).iter_data()
    }

    def key(a: npt.NDArray[Any]) -> tuple[str, list[int], bytes]:
        row = stored(a)
        return (row["dtype"], row["shape"], row["data"])

    for k, pair in (("a", [large, small]), ("b", [other, small])):
        expected = sorted(pair, key=key)
        assert [x.tobytes() for x in got[k]] == [x.tobytes() for x in expected]
        assert [x.dtype for x in got[k]] == [x.dtype for x in expected]


def test_merge_join_orders_arrays_that_differ_only_in_field_order() -> None:
    """``MergeJoin`` orders arrays that differ only in field order the same way for either input order."""
    ab = np.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")])
    ba = np.array([(2.0, 1)], dtype=[("b", "<f8"), ("a", "<i4")])
    left = DictSource(data=[{"k": "a", "x": ba}], tag_columns=["k"], source_id="l")
    right = DictSource(data=[{"k": "a", "x": ab}], tag_columns=["k"], source_id="r")
    outs: list[Any] = [
        next(d.as_dict()["x"] for _, d in MergeJoin()(*pair).iter_data())
        for pair in ((left, right), (right, left))
    ]
    expected = sorted([ab, ba], key=lambda a: stored(a)["dtype"])
    for got in outs:
        assert [x.dtype.names for x in got] == [x.dtype.names for x in expected]
        for x, e in zip(got, expected, strict=True):
            assert_arrays_identical(x, e)


def test_stored_tag_is_valid_json_for_structured() -> None:
    """A structured dtype tag is valid JSON in declared field order."""
    tag = stored(np.zeros(1, dtype=[("b", "<f4"), ("a", "<i2", (2,))]))["dtype"]
    assert json.loads(tag) == [["b", "float32"], ["a", "int16", [2]]]


# ---------------------------------------------------------------------------
# Pick on an array column (ITL-883)
# ---------------------------------------------------------------------------


def test_pick_on_an_array_column_raises_input_validation_error() -> None:
    """``Pick`` on an array column raises ``InputValidationError`` naming the column."""
    from orcapod.errors import InputValidationError
    from orcapod.operators import Pick

    source = DictSource(data=[{"k": "r", "x": np.arange(3.0)}], tag_columns=["k"])
    with pytest.raises(InputValidationError, match="'x'.*does not support pick"):
        Pick("x", "dtype")(source)


def test_pick_on_an_array_column_raises_while_building_a_pipeline(
    tmp_path: Path,
) -> None:
    """``Pick`` on an array column raises while the pipeline is being built."""
    # Operators validate their inputs when the graph is built, before either
    # orchestrator (sync or async) runs, so both paths stop here.
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.errors import InputValidationError
    from orcapod.operators import Pick

    source = DictSource(data=[{"k": "r", "x": np.arange(3.0)}], tag_columns=["k"])
    job = PipelineJob(name="pick", store=InMemoryArrowDatabase())
    with pytest.raises(InputValidationError, match="'x'"), job:
        Pick("x", "dtype")(source)


def test_declared_ndarray_check_recognizes_parameterized_aliases() -> None:
    """The declared-output check recognizes ``np.ndarray`` aliases and subclasses."""
    # Non-public helper: NDArray[...] annotations are rejected when a pod is built
    # today (ITL-869), so the check is exercised directly for when they are allowed.
    import numpy.typing as npt_

    from orcapod.semantic_types.output_conformance import _declared_ndarray_type  # pyright: ignore[reportPrivateUsage]

    assert _declared_ndarray_type(np.ndarray) is np.ndarray
    assert _declared_ndarray_type(npt_.NDArray[np.float32]) is np.ndarray
    assert _declared_ndarray_type(npt_.NDArray[np.float32] | None) is np.ndarray
    assert _declared_ndarray_type(float) is None
