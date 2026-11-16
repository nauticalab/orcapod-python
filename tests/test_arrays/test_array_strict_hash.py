"""Strict array hash, exact ``.npy`` storage and the per-type ignore list (ITL-981).

The hash label and the extension metadata bytes are two constants below: the
developer's decision on each changes only these lines.
"""

from __future__ import annotations

import hashlib
import json
import struct
import sys
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pytest

import orcapod.contexts as contexts_package
from orcapod.contexts import get_default_context
from orcapod.contexts.core import ContextValidationError, DataContext
from orcapod.contexts.registry import JSONDataContextRegistry
from orcapod.core.operators import MergeJoin
from orcapod.logical_types.identifier import LogicalTypeIdentifierError
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.utils.arrow_utils import shared_arrow_type

HASH_LABEL = "ndarray_v1"
"""Label of an array value hash (open decision 2 of the design note)."""


def ignore_metadata(names: list[str]) -> bytes:
    """Return the extension metadata for an ignore list (open decision 4: key order)."""
    return json.dumps(
        {"ignore": sorted(names), "version": 1},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()


EXTENSION_NAME = "orcapod.numpy.ndarray"
"""Arrow extension name of the array type (developer, November 12, 2026)."""

LEADING_TYPE_TEXT = EXTENSION_NAME
"""Leading text of the value hash preimage: the extension name (proposed)."""

OPTIONS = ("byte_order", "memory_order", "struct_layout")

BASE = np.arange(6, dtype=np.float32).reshape(2, 3)
PACKED = np.array([(1, 0.5), (-2, 1.5)], dtype=[("a", "i1"), ("b", "<f8")])
ALIGNED = PACKED.astype(np.dtype([("a", "i1"), ("b", "<f8")], align=True))

# (option that separates the pair, one value, the other value)
PAIRS: dict[str, tuple[str, npt.NDArray[Any], npt.NDArray[Any]]] = {
    "fortran": ("memory_order", BASE, np.asfortranarray(BASE)),
    "transposed_view": ("memory_order", np.ascontiguousarray(BASE.T), BASE.T),
    "big_endian": ("byte_order", BASE, BASE.astype(">f4")),
    "big_endian_field": (
        "byte_order",
        PACKED,
        PACKED.astype([("a", "i1"), ("b", ">f8")]),
    ),
    "aligned_struct": ("struct_layout", PACKED, ALIGNED),
}


def _lp(text: str) -> bytes:
    data = text.encode("utf-8")
    return struct.pack("<Q", len(data)) + data


def reference_digest(
    tag: str,
    shape: tuple[int, ...],
    data: bytes,
    memory_order: int,
    byte_order_text: str,
    struct_layout_text: str,
) -> str:
    """Reference implementation of the value hash with the standard library only."""
    p = _lp(tag) + struct.pack("<Q", len(shape))
    p += b"".join(struct.pack("<q", d) for d in shape)
    p += struct.pack("<Q", len(data)) + data
    element_digest = hashlib.sha256(p).digest()
    q = _lp(LEADING_TYPE_TEXT) + element_digest + bytes([memory_order])
    q += _lp(byte_order_text) + _lp(struct_layout_text)
    return hashlib.sha256(q).hexdigest()


def _context_ignoring(tmp_path: Path, names: list[str]) -> DataContext:
    """Build a data context whose ndarray ignore list is ``names``."""
    shipped = Path(contexts_package.__file__).parent / "logical_types.json"
    registration = json.loads(shipped.read_text())
    registration["hash_ignore"][EXTENSION_NAME] = names
    custom = tmp_path / f"logical_types_{'_'.join(names) or 'strict'}.json"
    custom.write_text(json.dumps(registration))
    return JSONDataContextRegistry(logical_types_file=custom).get_context()


def _source(arrays: dict[str, Any], column: str = "x") -> DictSource:
    return DictSource(
        data=[{"k": k, column: v} for k, v in arrays.items()],
        tag_columns=["k"],
        source_id="arrays",
        tag_only_columns=True,
    )


def _read(stream: Any, column: str = "x") -> dict[str, Any]:
    return {t.as_dict()["k"]: d.as_dict()[column] for t, d in stream.iter_data()}


def _layout(dtype: np.dtype[Any]) -> Any:
    if dtype.subdtype is not None:
        dtype = dtype.subdtype[0]
    if dtype.names is None:
        return None
    assert dtype.fields is not None
    fields: dict[str, Any] = {str(key): value for key, value in dtype.fields.items()}
    return [dtype.itemsize] + [
        [fields[name][1], _layout(fields[name][0])] for name in dtype.names
    ]


def _value_bytes(array: npt.NDArray[Any]) -> bytes:
    """The bytes of every scalar field in memory order, without padding bytes.

    Padding between structured fields is not compared: NumPy does not define its
    content and does not always copy it.
    """
    if array.dtype.names is None:
        return array.tobytes("A")
    return b"".join(_value_bytes(array[name]) for name in array.dtype.names)


# --- the hash is strict by default ------------------------------------------------


@pytest.mark.parametrize("name", sorted(PAIRS))
def test_default_hash_distinguishes(name: str) -> None:
    """By default memory order, byte order and structured layout are part of the hash."""
    _, one, other = PAIRS[name]
    hasher = get_default_context().semantic_hasher
    assert hasher.hash_object(one) != hasher.hash_object(other)


def test_hash_carries_the_label() -> None:
    """An array value hash is labelled with the array algorithm's label."""
    assert get_default_context().semantic_hasher.hash_object(BASE).method == HASH_LABEL


def test_strided_view_hashes_like_its_contiguous_copy() -> None:
    """A strided view is stored C-contiguous, so it hashes as its C-contiguous copy."""
    hasher = get_default_context().semantic_hasher
    wide = np.arange(12, dtype=np.float32).reshape(2, 6)
    view = wide[:, ::2]
    assert not view.flags.c_contiguous
    assert hasher.hash_object(view) == hasher.hash_object(np.ascontiguousarray(view))
    assert hasher.hash_object(BASE[::-1]) == hasher.hash_object(
        np.ascontiguousarray(BASE[::-1])
    )


def test_subclass_and_writeable_flag_do_not_count() -> None:
    """What storage does not keep (subclass, writeable flag) is never in the hash."""
    hasher = get_default_context().semantic_hasher
    ints = np.array([[1, 2], [3, 4]])
    assert hasher.hash_object(np.matrix(ints)) == hasher.hash_object(ints)
    frozen = BASE.copy()
    frozen.setflags(write=False)
    assert hasher.hash_object(frozen) == hasher.hash_object(BASE)


def test_reference_vectors() -> None:
    """The hash equals the reference implementation, field by field."""
    hasher = get_default_context().semantic_hasher
    data = struct.pack("<6f", 0, 1, 2, 3, 4, 5)
    assert hasher.hash_object(BASE).to_hex() == reference_digest(
        "float32", (2, 3), data, 0, "<", "null"
    )
    assert hasher.hash_object(np.asfortranarray(BASE)).to_hex() == reference_digest(
        "float32", (2, 3), data, 1, "<", "null"
    )
    assert hasher.hash_object(BASE.astype(">f4")).to_hex() == reference_digest(
        "float32", (2, 3), data, 0, ">", "null"
    )
    packed_bytes = struct.pack("<bd", 1, 0.5) + struct.pack("<bd", -2, 1.5)
    tag = '[["a","int8"],["b","float64"]]'
    assert hasher.hash_object(PACKED).to_hex() == reference_digest(
        tag, (2,), packed_bytes, 0, "|<", "[9,[0,null],[1,null]]"
    )
    assert hasher.hash_object(ALIGNED).to_hex() == reference_digest(
        tag, (2,), packed_bytes, 0, "|<", "[16,[0,null],[8,null]]"
    )


# --- storage reads back exactly as given --------------------------------------------


EXACT: dict[str, npt.NDArray[Any]] = {
    "c_order": BASE,
    "fortran": np.asfortranarray(BASE),
    "big_endian": BASE.astype(">f4"),
    "big_endian_unicode": np.array(["ab", "c"], dtype=">U3"),
    "aligned_struct": ALIGNED,
    "mixed_endian_struct": PACKED.astype([("a", "i1"), ("b", ">f8")]),
    "unicode_field_name": np.zeros(2, dtype=[("β", "<f8")]),
    "zero_d": np.array(2.5),
    "empty_3x0_fortran": np.zeros((3, 0), dtype=np.int8, order="F"),
}


@pytest.mark.parametrize("name", sorted(EXACT))
def test_array_reads_back_exactly(name: str) -> None:
    """Memory order, byte order and structured layout survive a source round trip."""
    expected = EXACT[name]
    got = _read(_source({name: expected}))[name]
    assert type(got) is np.ndarray
    assert got.dtype == expected.dtype
    assert got.dtype.str == expected.dtype.str
    assert _layout(got.dtype) == _layout(expected.dtype)
    assert got.shape == expected.shape
    assert got.flags.c_contiguous == expected.flags.c_contiguous
    assert got.flags.f_contiguous == expected.flags.f_contiguous
    assert _value_bytes(got) == _value_bytes(expected)
    assert got.flags.writeable


@pytest.mark.parametrize(
    "name", sorted(EXACT) + ["strided_view", "matrix", "read_only"]
)
def test_hash_is_unchanged_by_storage(name: str) -> None:
    """``hash(read(write(v))) == hash(v)``: a cached re-run never recomputes."""
    frozen = BASE.copy()
    frozen.setflags(write=False)
    extra = {
        "strided_view": np.arange(12, dtype=np.float32).reshape(2, 6)[:, ::2],
        "matrix": np.matrix([[1, 2], [3, 4]]),
        "read_only": frozen,
    }
    value = {**EXACT, **extra}[name]
    hasher = get_default_context().semantic_hasher
    got = _read(_source({name: value}))[name]
    assert hasher.hash_object(got) == hasher.hash_object(value)


def test_column_is_npy_bytes_under_the_default_identifier() -> None:
    """An array column is ``orcapod.numpy.ndarray`` over ``large_binary`` holding ``.npy`` files."""
    field = _source({"a": BASE}).as_table().schema.field("x")
    assert field.type.extension_name == EXTENSION_NAME
    assert field.type.storage_type == pa.large_binary()
    assert field.type.__arrow_ext_serialize__() == ignore_metadata([])
    chunk = _source({"a": BASE}).as_table().column("x").chunk(0)
    assert isinstance(chunk, pa.ExtensionArray)
    cell = chunk.storage[0].as_py()
    assert cell[:8] == b"\x93NUMPY\x03\x00"


def test_out_of_order_struct_offsets_are_refused() -> None:
    """A structured dtype ``.npy`` cannot store is refused when written."""
    dtype = np.dtype(
        {
            "names": ["a", "b"],
            "formats": ["i4", "f8"],
            "offsets": [8, 0],
            "itemsize": 16,
        }
    )
    with pytest.raises(ValueError, match="offset"):
        _source({"a": np.zeros(2, dtype=dtype)})


# --- the ignore list ----------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(PAIRS))
def test_ignoring_a_property_gives_equal_hashes(name: str, tmp_path: Path) -> None:
    """With a property in the ignore list, values differing only in it hash equal."""
    option, one, other = PAIRS[name]
    hasher = _context_ignoring(tmp_path, [option]).semantic_hasher
    assert hasher.hash_object(one) == hasher.hash_object(other)


@pytest.mark.parametrize("name", sorted(PAIRS))
def test_ignoring_another_property_keeps_them_apart(name: str, tmp_path: Path) -> None:
    """Ignoring one property leaves the other two in the hash."""
    option, one, other = PAIRS[name]
    others = [o for o in OPTIONS if o != option]
    hasher = _context_ignoring(tmp_path, others).semantic_hasher
    assert hasher.hash_object(one) != hasher.hash_object(other)


def test_everything_ignored_equals_strict_hash_of_canonical_array(
    tmp_path: Path,
) -> None:
    """The ignore list is not a hash input: only the canonical field values are."""
    lenient = _context_ignoring(tmp_path, list(OPTIONS)).semantic_hasher
    strict = get_default_context().semantic_hasher
    swapped_fortran = np.asfortranarray(BASE.astype(">f4"))
    assert lenient.hash_object(swapped_fortran) == strict.hash_object(BASE)


def test_ignore_list_is_in_the_extension_metadata(tmp_path: Path) -> None:
    """Two ignore lists are two logical types: the list is in the identifier."""
    context = _context_ignoring(tmp_path, ["memory_order", "byte_order"])
    arrow_type = context.type_converter.python_type_to_arrow_type(np.ndarray)
    assert isinstance(arrow_type, pa.ExtensionType)
    assert arrow_type.__arrow_ext_serialize__() == ignore_metadata(
        ["byte_order", "memory_order"]
    )
    default_type = get_default_context().type_converter.python_type_to_arrow_type(
        np.ndarray
    )
    assert arrow_type != default_type
    assert context.semantic_hasher.hash_object(
        np.ndarray
    ) != get_default_context().semantic_hasher.hash_object(np.ndarray)


def test_a_cell_is_hashed_with_its_columns_list(tmp_path: Path) -> None:
    """A table hash uses the ignore list of the column, so it follows the column type."""
    context = _context_ignoring(tmp_path, ["memory_order"])

    def table_hash(value: npt.NDArray[Any]) -> Any:
        table = context.type_converter.python_dicts_to_arrow_table(
            [{"x": value}], python_schema={"x": np.ndarray}
        )
        return context.arrow_hasher.hash_table(table)

    assert table_hash(BASE) == table_hash(np.asfortranarray(BASE))
    assert table_hash(BASE) != table_hash(BASE.astype(">f4"))


def test_unknown_option_name_is_an_error(tmp_path: Path) -> None:
    """A context that names an option the type does not have cannot be built."""
    with pytest.raises(ContextValidationError, match="memory_layout"):
        _context_ignoring(tmp_path, ["memory_layout"])


def test_merge_join_of_two_ignore_lists_is_an_error(tmp_path: Path) -> None:
    """Joining one column typed with two ignore lists names the column and both lists.

    A data context reads only columns of its own list, so the error is the read
    refusal, raised when the join inspects its inputs.
    """
    lenient = _context_ignoring(tmp_path, ["memory_order"])

    def stream(context: DataContext) -> ArrowTableSource:
        table = context.type_converter.python_dicts_to_arrow_table(
            [{"k": "r1", "x": BASE}], python_schema={"k": str, "x": np.ndarray}
        )
        return ArrowTableSource(
            table, tag_columns=["k"], source_id="s", tag_only_columns=True
        )

    with pytest.raises(LogicalTypeIdentifierError, match="'x'") as raised:
        MergeJoin()(stream(get_default_context()), stream(lenient)).as_table()
    message = str(raised.value)
    assert repr(ignore_metadata(["memory_order"])) in message
    assert repr(ignore_metadata([])) in message


def test_the_join_type_rule_tells_two_ignore_lists_apart(tmp_path: Path) -> None:
    """The rule joins use for a shared column finds no shared type for two lists."""
    lenient = _context_ignoring(tmp_path, ["memory_order"])
    strict_type = get_default_context().type_converter.python_type_to_arrow_type(
        np.ndarray
    )
    lenient_type = lenient.type_converter.python_type_to_arrow_type(np.ndarray)
    assert shared_arrow_type(strict_type, lenient_type) is None
    assert shared_arrow_type(strict_type, strict_type) == strict_type
    assert "memory_order" in str(lenient_type)


def test_native_byte_order_is_absolute() -> None:
    """A native array hashes as its explicit byte order, whatever the machine."""
    hasher = get_default_context().semantic_hasher
    explicit = "<f4" if sys.byteorder == "little" else ">f4"
    assert hasher.hash_object(BASE) == hasher.hash_object(BASE.astype(explicit))
