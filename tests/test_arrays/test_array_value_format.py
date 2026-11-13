"""Array values: ``.npy`` storage, what reads back, the hash and refusals (ITL-981).

Design: ``superpowers/specs/2026-11-12-itl-981-library-value-types-design.md``.
Tests go through sources, streams, function pods and the default data context's
semantic hasher. The strict hash and the ignore list have their own file,
``test_array_strict_hash.py``.
"""

from __future__ import annotations

import io
import struct
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.contexts import get_default_context
from orcapod.logical_types.numpy_type import dtype_to_tag
from orcapod.operators import MergeJoin
from orcapod.sources import ArrowTableSource, DictSource
from tests.test_arrays.array_cases import ARRAYS, assert_arrays_identical

IDENTIFIER_TEXT = (
    b"orcapod.numpy.ndarray;" + b'{"ignore":[],"version":1}'.hex().encode()
)
"""The identifier text of the default array type, as it enters a cell token."""


def stored(value: Any) -> bytes:
    """The stored cell of a one-row ``x`` column."""
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


def test_column_type_is_orcapod_numpy_ndarray_over_large_binary() -> None:
    """An array column is ``orcapod.numpy.ndarray`` over ``large_binary`` with the ignore list."""
    table = DictSource(
        data=[{"k": "r", "x": np.arange(3.0)}], tag_columns=["k"]
    ).as_table()
    ext = table.schema.field("x").type
    assert isinstance(ext, pa.ExtensionType)
    assert ext.extension_name == "orcapod.numpy.ndarray"
    assert ext.storage_type == pa.large_binary()
    assert ext.__arrow_ext_serialize__() == b'{"ignore":[],"version":1}'


def test_older_extension_names_are_not_registered() -> None:
    """Only ``orcapod.numpy.ndarray`` is registered; the names of v0.2 and of the first v0.3 format are not."""
    converter: Any = get_default_context().type_converter
    assert converter.get_logical_type_by_arrow_extension_name("numpy.ndarray") is None
    assert converter.get_logical_type_by_arrow_extension_name("orcapod.ndarray") is None
    assert (
        converter.get_logical_type_by_arrow_extension_name("orcapod.numpy.ndarray")
        is not None
    )


# ---------------------------------------------------------------------------
# Stored cells: one .npy file per value; dtype tags
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
def test_stored_cell_is_a_npy_file_numpy_reads(
    array: npt.NDArray[Any], tag: str
) -> None:
    """The stored cell is a ``.npy`` file of format 3.0 that plain NumPy loads back."""
    cell = stored(array)
    assert cell[:8] == b"\x93NUMPY\x03\x00"
    loaded = np.load(io.BytesIO(cell), allow_pickle=False)
    assert loaded.dtype == array.dtype
    assert loaded.shape == array.shape
    assert loaded.tobytes() == array.tobytes()


@pytest.mark.parametrize(
    ("array", "tag"), [c[1:] for c in TAG_CASES], ids=[c[0] for c in TAG_CASES]
)
def test_dtype_tag_of_the_element_digest(array: npt.NDArray[Any], tag: str) -> None:
    """The dtype tag that enters the element digest names the dtype without a byte order."""
    assert dtype_to_tag(array.dtype) == tag
    assert dtype_to_tag(array.dtype.newbyteorder(">")) == tag


def test_zero_d_and_empty_shapes() -> None:
    """Zero-dimensional and empty arrays keep their exact shape."""
    assert read_back(np.array(2.5)).shape == ()
    assert read_back(np.array(2.5)) == 2.5
    assert read_back(np.zeros((0, 3), np.float32)).shape == (0, 3)
    assert read_back(np.zeros((3, 0), np.float32)).shape == (3, 0)


def test_equal_arrays_are_stored_as_equal_bytes() -> None:
    """Within one NumPy version, equal arrays give equal stored bytes."""
    array = np.arange(6, dtype=np.float32).reshape(2, 3)
    assert stored(array) == stored(array.copy())


# ---------------------------------------------------------------------------
# What reads back
# ---------------------------------------------------------------------------


def test_fortran_order_reads_back_fortran_order() -> None:
    """A Fortran-ordered array reads back Fortran-ordered with the same values."""
    array = np.asfortranarray(np.arange(6, dtype=np.float32).reshape(2, 3))
    got = read_back(array)
    assert got.flags.f_contiguous and not got.flags.c_contiguous
    assert (got == array).all()


def test_strided_view_reads_back_as_its_values() -> None:
    """A strided view reads back as a C-contiguous array of its values."""
    base = np.arange(24, dtype=np.int16).reshape(4, 6)
    view = base[::2, 1::2]
    assert_arrays_identical(read_back(view), np.ascontiguousarray(view))


@pytest.mark.parametrize("dtype", [">f4", ">i8", ">c16", ">M8[ns]", ">U3"])
def test_big_endian_reads_back_big_endian(dtype: str) -> None:
    """A big-endian array reads back big-endian with equal values."""
    raw = (
        ["ab", "c"] if dtype.startswith(">U") else np.arange(2).astype(dtype[0] + "i8")
    )
    big = np.array(raw).astype(dtype)
    got = read_back(big)
    assert got.dtype.str == big.dtype.str
    assert got.tobytes() == big.tobytes()


def test_aligned_structured_reads_back_with_its_offsets() -> None:
    """An aligned structured array keeps field order, byte order, offsets and itemsize."""
    aligned = np.dtype([("b", ">f8"), ("a", "<i2")], align=True)
    array = np.array([(1.5, 7), (-2.0, 9)], dtype=aligned)
    got = read_back(array)
    assert got.dtype == aligned
    assert got.dtype.names == ("b", "a")
    assert got.dtype.itemsize == 16
    assert got.dtype["b"].str == ">f8"
    assert got.dtype.fields is not None and got.dtype.fields["a"][1] == 8
    assert got["a"].tolist() == [7, 9]
    assert got["b"].tolist() == [1.5, -2.0]
    # The ``align`` flag itself is not kept by ``.npy``; the layout it produced is.
    assert not got.dtype.isalignedstruct


def test_nested_structured_reads_back_as_given() -> None:
    """A nested structured array keeps field order and byte order at every depth."""
    inner = np.dtype([("y", ">i2"), ("x", "<i2")])
    array = np.array([((1, 2), 3.5)], dtype=[("s", inner), ("a", "<f8")])
    got = read_back(array)
    assert got.dtype == array.dtype
    assert got.dtype["s"]["y"].str == ">i2"
    assert got["s"]["y"].tolist() == [1] and got["s"]["x"].tolist() == [2]


def test_reordered_fields_are_a_different_stored_value() -> None:
    """Arrays whose fields differ only in order are different stored values."""
    ab = np.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")])
    ba = np.array([(2.0, 1)], dtype=[("b", "<f8"), ("a", "<i4")])
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
    """An array read back from storage is writeable, also when a read-only one was written."""
    frozen = np.arange(3.0)
    frozen.setflags(write=False)
    got = read_back(frozen)
    got[0] = 10.0
    assert got[0] == 10.0


def test_dtype_metadata_is_dropped_without_a_warning() -> None:
    """Dtype metadata is not stored and not hashed; writing it raises no warning."""
    plain = np.arange(3, dtype="<f4")
    with_metadata = plain.astype(np.dtype("<f4", metadata={"unit": "mV"}))
    assert with_metadata.dtype.metadata is not None
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        got = read_back(with_metadata)
    assert got.dtype.metadata is None
    assert got.tobytes() == plain.tobytes()
    assert semantic_hash(with_metadata) == semantic_hash(plain)


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
# Hash: the value digest under the ndarray_v1 label
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(ARRAYS))
def test_hash_is_labelled_and_survives_storage(name: str) -> None:
    """The hash is labelled ``ndarray_v1`` and is the same for the value read back."""
    given = semantic_hash(ARRAYS[name])
    assert given.method == "ndarray_v1"
    assert semantic_hash(read_back(ARRAYS[name])) == given


def _equal_value_variants() -> list[tuple[str, npt.NDArray[Any], npt.NDArray[Any]]]:
    a = np.arange(24, dtype=np.float32).reshape(4, 6)
    base = np.zeros((4, 12), dtype=np.float32)
    base[:, ::2] = a
    frozen = a.copy()
    frozen.setflags(write=False)
    return [
        ("strided", a, base[:, ::2]),
        ("reversed", np.ascontiguousarray(a[::-1]), a[::-1]),
        ("transposed_twice", a, a.T.T),
        ("read_only", a, frozen),
        (
            "matrix",
            np.arange(4.0).reshape(2, 2),
            np.matrix(np.arange(4.0).reshape(2, 2)),
        ),
        (
            "align_flag_only",
            np.zeros(2, dtype=[("a", "<i4"), ("b", "<i4")]),
            np.zeros(2, dtype=np.dtype([("a", "<i4"), ("b", "<i4")], align=True)),
        ),
    ]


@pytest.mark.parametrize(
    ("left", "right"),
    [v[1:] for v in _equal_value_variants()],
    ids=[v[0] for v in _equal_value_variants()],
)
def test_what_storage_does_not_keep_does_not_change_the_hash(
    left: npt.NDArray[Any], right: npt.NDArray[Any]
) -> None:
    """Strides, the writeable flag, the subclass and the ``align`` flag do not change the hash."""
    assert semantic_hash(left) == semantic_hash(right)
    assert data_hash(left) == data_hash(right)


def _different_value_pairs() -> list[tuple[str, npt.NDArray[Any], npt.NDArray[Any]]]:
    nan_a = np.array([np.nan])
    nan_b = np.frombuffer(struct.pack("<Q", 0x7FF8000000000001), dtype="<f8").copy()
    a = np.arange(24, dtype=np.float32).reshape(4, 6)
    rec_ab = np.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")])
    padded = np.dtype(
        {
            "names": ["a", "b"],
            "formats": ["<i4", "<f8"],
            "offsets": [0, 8],
            "itemsize": 24,
        }
    )
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
        ("fortran", a, np.asfortranarray(a)),
        ("big_endian", a, a.astype(">f4")),
        (
            "structured_byte_order",
            rec_ab,
            rec_ab.astype([("a", ">i4"), ("b", ">f8")]),
        ),
        (
            "aligned_offsets",
            rec_ab,
            rec_ab.astype(np.dtype([("a", "<i4"), ("b", "<f8")], align=True)),
        ),
        ("padded_itemsize", rec_ab, rec_ab.astype(padded)),
    ]


@pytest.mark.parametrize(
    ("left", "right"),
    [v[1:] for v in _different_value_pairs()],
    ids=[v[0] for v in _different_value_pairs()],
)
def test_dtype_shape_bits_and_kept_layout_change_the_hash(
    left: npt.NDArray[Any], right: npt.NDArray[Any]
) -> None:
    """Dtype, shape, element bits, memory order, byte order and offsets change the hash."""
    assert semantic_hash(left) != semantic_hash(right)
    assert data_hash(left) != data_hash(right)


def test_table_token_carries_the_identifier_and_the_digest() -> None:
    """A table token carries the type identifier (name and ignore list) and the value digest, no label."""
    # The Arrow hasher folds the identifier text in front of the handler's digest.
    from orcapod.hashing.arrow_hashers import StarfixArrowHasher

    hasher = get_default_context().arrow_hasher
    assert isinstance(hasher, StarfixArrowHasher)
    array = np.arange(3.0)
    table = DictSource(data=[{"k": "r", "x": array}], tag_columns=["k"]).as_table()
    # Non-public helper: this test is about the token the hasher builds for a cell.
    processed = hasher._process_table_columns(table.select(["x"]))  # pyright: ignore[reportPrivateUsage]
    token = processed.column("x")[0].as_py()
    assert token == IDENTIFIER_TEXT + b"::" + semantic_hash(array).digest


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
    (
        "out_of_order_offsets",
        np.zeros(
            1,
            dtype=np.dtype(
                {
                    "names": ["a", "b"],
                    "formats": ["<i4", "<f8"],
                    "offsets": [8, 0],
                    "itemsize": 12,
                }
            ),
        ),
        "out-of-order offsets",
    ),
    (
        "overlapping_fields",
        np.zeros(
            1,
            dtype=np.dtype(
                {
                    "names": ["a", "b"],
                    "formats": ["<i4", "<i2"],
                    "offsets": [0, 2],
                    "itemsize": 8,
                }
            ),
        ),
        "overlapping",
    ),
    (
        "too_many_fields",
        np.zeros(1, dtype=[(f"field_number_{i}", "<f8") for i in range(400)]),
        "header",
    ),
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


@pytest.mark.parametrize(
    "cell",
    [
        b"not a npy file",
        b"\x93NUMPY\x01\x00" + b"\x00" * 16,
        None,
    ],
    ids=["garbage", "other_format_version", "truncated"],
)
def test_a_cell_that_is_not_a_stored_array_raises_naming_the_column(
    cell: bytes | None,
) -> None:
    """A cell that is not a complete ``.npy`` file of format 3.0 raises on read, naming the column."""
    converter = get_default_context().type_converter
    ext = converter.python_type_to_arrow_type(np.ndarray)
    assert isinstance(ext, pa.ExtensionType)
    if cell is None:
        cell = stored(np.arange(4.0))[:-5]
    storage = pa.array([cell], type=ext.storage_type)
    table = pa.table(
        {
            "k": pa.array(["r"], pa.large_string()),
            "x": pa.ExtensionArray.from_storage(ext, storage),
        }
    )
    # The first read of the value (hashing the source rows) must fail naming the column.
    with pytest.raises((ValueError, RuntimeError), match="'x'"):
        source = ArrowTableSource(table, tag_columns=["k"])
        for _, data in source.iter_data():
            data.as_dict()


def _source_of_cell(cell: bytes) -> ArrowTableSource:
    converter = get_default_context().type_converter
    ext = converter.python_type_to_arrow_type(np.ndarray)
    assert isinstance(ext, pa.ExtensionType)
    storage = pa.array([cell], type=ext.storage_type)
    table = pa.table(
        {
            "k": pa.array(["r"], pa.large_string()),
            "x": pa.ExtensionArray.from_storage(ext, storage),
        }
    )
    return ArrowTableSource(table, tag_columns=["k"])


def test_a_cell_that_declares_more_than_it_holds_is_refused_before_allocation() -> None:
    """A small cell whose header declares a huge array is refused without allocating it."""
    buffer = io.BytesIO()
    np.lib.format.write_array_header_2_0(
        buffer, {"descr": "<f8", "fortran_order": False, "shape": (2**40,)}
    )
    lying = b"\x93NUMPY\x03\x00" + buffer.getvalue()[8:] + b"\x00" * 8
    with pytest.raises((ValueError, RuntimeError), match="'x'.*declares"):
        for _, data in _source_of_cell(lying).iter_data():
            data.as_dict()


def test_a_cell_with_trailing_bytes_is_refused() -> None:
    """A cell that holds more than the array its header declares is refused."""
    with pytest.raises((ValueError, RuntimeError), match="'x'.*declares"):
        for _, data in _source_of_cell(stored(np.arange(4.0)) + b"extra").iter_data():
            data.as_dict()


def test_padding_bytes_are_stored_as_zero_and_do_not_change_the_hash() -> None:
    """Garbage in structured padding never reaches the store, and is not part of the hash."""
    aligned = np.dtype([("a", "i1"), ("b", "<f8")], align=True)
    marker = b"\xa5" * 7
    dirty = np.frombuffer(
        b"\x01"
        + marker
        + struct.pack("<d", 0.5)
        + b"\x02"
        + marker
        + struct.pack("<d", 1.5),
        dtype=aligned,
    ).copy()
    clean = np.zeros(2, dtype=aligned)
    clean["a"], clean["b"] = dirty["a"], dirty["b"]
    before = dirty.tobytes()

    cell = stored(dirty)
    assert marker not in cell
    assert cell == stored(clean)
    assert dirty.tobytes() == before  # the given array is not modified
    got = read_back(dirty)
    assert got.dtype == aligned
    assert got["a"].tolist() == [1, 2] and got["b"].tolist() == [0.5, 1.5]
    assert got.tobytes() == clean.tobytes()
    assert semantic_hash(dirty) == semantic_hash(clean) == semantic_hash(got)
    # Fortran order and nesting are kept when padding is cleared.
    outer = np.dtype([("p", aligned), ("s", aligned, (2,))], align=True)
    nested = np.asfortranarray(np.ones((2, 3), dtype=outer))
    back = read_back(nested)
    assert back.dtype == outer and back.flags.f_contiguous
    assert semantic_hash(back) == semantic_hash(nested)


def test_a_stored_object_array_is_never_unpickled() -> None:
    """A ``.npy`` file holding a pickled object array is refused on read."""
    converter = get_default_context().type_converter
    ext = converter.python_type_to_arrow_type(np.ndarray)
    assert isinstance(ext, pa.ExtensionType)
    buffer = io.BytesIO()
    np.lib.format.write_array(
        buffer, np.array([1, "a"], dtype=object), version=(3, 0), allow_pickle=True
    )
    storage = pa.array([buffer.getvalue()], type=ext.storage_type)
    table = pa.table(
        {
            "k": pa.array(["r"], pa.large_string()),
            "x": pa.ExtensionArray.from_storage(ext, storage),
        }
    )
    with pytest.raises((ValueError, RuntimeError), match="allow_pickle=False"):
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
# MergeJoin orders merged arrays by their value hash
# ---------------------------------------------------------------------------


def _hash_order(arrays: list[npt.NDArray[Any]]) -> list[npt.NDArray[Any]]:
    return sorted(arrays, key=lambda a: semantic_hash(a).digest)


def test_merge_join_orders_arrays_by_their_value_hash() -> None:
    """``MergeJoin`` orders merged arrays by their strict value hash, never by the stored bytes."""
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
    for pair in ((left, right), (right, left)):
        got: dict[Any, Any] = {
            t.as_dict()["k"]: d.as_dict()["x"]
            for t, d in MergeJoin()(*pair).iter_data()
        }
        for k, values in (("a", [large, small]), ("b", [other, small])):
            expected = _hash_order(values)
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
    expected = _hash_order([ab, ba])
    for got in outs:
        assert [x.dtype.names for x in got] == [x.dtype.names for x in expected]
        for x, e in zip(got, expected, strict=True):
            assert_arrays_identical(x, e)


def test_merge_join_keeps_memory_order_and_byte_order_of_each_side() -> None:
    """Merged arrays read back as each side gave them."""
    base = np.arange(6, dtype=np.float32).reshape(2, 3)
    fortran_big = np.asfortranarray(base.astype(">f4"))
    left = DictSource(data=[{"k": "a", "x": base}], tag_columns=["k"], source_id="l")
    right = DictSource(
        data=[{"k": "a", "x": fortran_big}], tag_columns=["k"], source_id="r"
    )
    got: Any = next(d.as_dict()["x"] for _, d in MergeJoin()(left, right).iter_data())
    assert sorted(x.dtype.str for x in got) == ["<f4", ">f4"]
    assert sorted(bool(x.flags.f_contiguous) for x in got) == [False, True]


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
