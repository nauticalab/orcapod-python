"""What the tensor hash ignores is an option of the type, per data context (ITL-871, ITL-981).

The five options are ``requires_grad``, ``memory_layout``, ``conj_bit``,
``neg_bit`` and ``subclass``. The first part exercises the hash function
(``torch_type.tensor_digest``) with each option; the second part configures a list
in a logical type registration file, the way a data context does.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("torch")

import pyarrow as pa  # noqa: E402
import torch  # noqa: E402

import orcapod.contexts as contexts_package  # noqa: E402
from orcapod.contexts.core import DataContext  # noqa: E402
from orcapod.contexts.registry import JSONDataContextRegistry  # noqa: E402
from orcapod.logical_types.identifier import LogicalTypeIdentifierError  # noqa: E402
from orcapod.logical_types.torch_type import OPTIONS, tensor_digest  # noqa: E402
from orcapod.sources import ArrowTableSource, DictSource  # noqa: E402
from tests.test_torch.tensor_cases import (  # noqa: E402
    EXTENSION_NAME,
    TENSOR_CLASS,
    c_strides,
    expected_digest,
    reference_value_digest,
)
from tests.test_torch.test_tensor_strict_hash import (  # noqa: E402
    _differ_in_one_property,  # pyright: ignore[reportPrivateUsage]  # shared cases
)

PAIRS = [
    (name.removesuffix("_channels_last"), a, b)
    for name, a, b in _differ_in_one_property()
]
"""``(option name, a, b)``: ``a`` and ``b`` differ only in that property."""


def test_the_options_are_exactly_the_five_properties() -> None:
    """The option names are fixed."""
    assert OPTIONS == {
        "requires_grad",
        "memory_layout",
        "conj_bit",
        "neg_bit",
        "subclass",
    }
    assert {name for name, _, _ in PAIRS} == OPTIONS


# ---------------------------------------------------------------------------
# The hash function with each option
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("option", "a", "b"), PAIRS, ids=[f"{p[0]}.{i}" for i, p in enumerate(PAIRS)]
)
def test_ignoring_a_property_makes_values_that_differ_only_in_it_equal(
    option: str, a: Any, b: Any
) -> None:
    """With the option ignored the two hash equal; ignoring any other keeps them apart."""
    assert tensor_digest(a) != tensor_digest(b)
    assert tensor_digest(a, frozenset({option})) == tensor_digest(
        b, frozenset({option})
    )
    for other in OPTIONS - {option}:
        assert tensor_digest(a, frozenset({other})) != tensor_digest(
            b, frozenset({other})
        ), other


@pytest.mark.parametrize(
    "value",
    [p[1] for p in PAIRS] + [p[2] for p in PAIRS],
    ids=[f"{p[0]}.a" for p in PAIRS] + [f"{p[0]}.b" for p in PAIRS],
)
def test_everything_ignored_is_the_strict_hash_of_the_plain_tensor(value: Any) -> None:
    """An ignored slot holds the canonical value: the slots of a plain contiguous tensor."""
    plain = value.detach().resolve_conj().resolve_neg().contiguous().clone()
    plain = plain.as_subclass(torch.Tensor)
    everything = tensor_digest(value, OPTIONS)
    assert everything == tensor_digest(plain)
    assert everything == reference_value_digest(
        expected_digest(value),
        requires_grad=False,
        conj_bit=False,
        neg_bit=False,
        strides=c_strides(value.shape),
        class_name=TENSOR_CLASS,
    )


def test_dtype_shape_and_elements_cannot_be_ignored() -> None:
    """With every option ignored, dtype, shape and one bit still change the hash."""
    base = torch.arange(6, dtype=torch.float32)
    digests = {
        tensor_digest(value, OPTIONS)
        for value in (
            base,
            base.to(torch.float64),
            base.reshape(2, 3),
            torch.cat([base[:5], torch.tensor([-5.0])]),
            torch.cat([torch.tensor([-0.0]), base[1:]]),
        )
    }
    assert len(digests) == 5


def test_an_unknown_option_name_is_an_error() -> None:
    """The hash function refuses a name that is not an option, naming it."""
    with pytest.raises(ValueError, match="device"):
        tensor_digest(torch.zeros(1), frozenset({"device"}))


# ---------------------------------------------------------------------------
# A list configured in the data context
# ---------------------------------------------------------------------------

STRICT = b'{"ignore":[],"version":1}'
LAYOUT = b'{"ignore":["memory_layout"],"version":1}'


def _context(tmp_path: Path, ignore: list[str]) -> DataContext:
    """Build a data context whose tensor type ignores ``ignore``."""
    shipped = Path(contexts_package.__file__).parent / "logical_types.json"
    registration = json.loads(shipped.read_text())
    assert registration["hash_ignore"][EXTENSION_NAME] == []
    registration["hash_ignore"][EXTENSION_NAME] = ignore
    custom = tmp_path / f"logical_types_{'_'.join(ignore) or 'strict'}.json"
    custom.write_text(json.dumps(registration))
    return JSONDataContextRegistry(logical_types_file=custom).get_context()


def _source(context: DataContext, value: Any) -> DictSource:
    """A one-row source of ``context`` with ``value`` in column ``x``."""
    return DictSource(
        data=[{"k": "r", "x": value}],
        tag_columns=["k"],
        source_id="s",
        data_context=context,
    )


def _cell_hash(context: DataContext, value: Any) -> Any:
    """The hash of the ``x`` column of ``_source``, by the context's own Arrow hasher.

    A row's ``content_hash()`` is not used here: the table handler of a custom
    context hashes with the default context's Arrow hasher, which refuses a
    column stored under another list (reported under ITL-981).
    """
    table = _source(context, value).as_table().select(["x"])
    return context.arrow_hasher.hash_table(table)


def _transposed() -> Any:
    """A transposed view that covers its storage."""
    return torch.arange(24, dtype=torch.float32).reshape(2, 3, 4).transpose(0, 2)


def test_the_configured_list_is_the_columns_extension_metadata(tmp_path: Path) -> None:
    """The list of the data context is written into the column's extension metadata."""
    for ignore, metadata in (([], STRICT), (["memory_layout"], LAYOUT)):
        context = _context(tmp_path, ignore)
        column_type = _source(context, _transposed()).as_table().schema.field("x").type
        assert isinstance(column_type, pa.ExtensionType)
        assert column_type.extension_name == EXTENSION_NAME
        assert column_type.__arrow_ext_serialize__() == metadata


def test_a_bare_value_and_a_cell_are_hashed_with_the_configured_list(
    tmp_path: Path,
) -> None:
    """Under ``["memory_layout"]`` a transposed view hashes like its contiguous copy."""
    strict = _context(tmp_path, [])
    lenient = _context(tmp_path, ["memory_layout"])
    view, copy = _transposed(), _transposed().contiguous()

    assert strict.semantic_hasher.hash_object(
        view
    ) != strict.semantic_hasher.hash_object(copy)
    assert lenient.semantic_hasher.hash_object(
        view
    ) == lenient.semantic_hasher.hash_object(copy)
    assert lenient.semantic_hasher.hash_object(view).digest == tensor_digest(
        view, frozenset({"memory_layout"})
    )

    assert _cell_hash(strict, view) != _cell_hash(strict, copy)
    assert _cell_hash(lenient, view) == _cell_hash(lenient, copy)
    # The list is in the identifier, so the same value hashes differently in a
    # strict column and in a lenient one.
    assert _cell_hash(strict, copy) != _cell_hash(lenient, copy)
    # Another property still counts under the lenient list.
    assert _cell_hash(lenient, copy) != _cell_hash(
        lenient, copy.clone().requires_grad_(True)
    )


def test_storage_does_not_depend_on_the_list(tmp_path: Path) -> None:
    """An ignored property is still stored and read back.

    The context's converter is used directly: a datagram finds its data context by
    key, which for a custom registration file is the default context (reported
    under ITL-981).
    """
    lenient = _context(tmp_path, ["memory_layout", "requires_grad"])
    value = (
        _transposed().clone(memory_format=torch.preserve_format).requires_grad_(True)
    )
    converter = lenient.type_converter
    table = converter.python_dicts_to_arrow_table(
        [{"x": value}], python_schema={"x": torch.Tensor}
    )
    assert table.schema.field("x").type.__arrow_ext_serialize__() == (
        b'{"ignore":["memory_layout","requires_grad"],"version":1}'
    )
    got = converter.arrow_table_to_python_dicts(table)[0]["x"]
    assert got.stride() == value.stride() == (1, 4, 12)
    assert got.requires_grad
    assert torch.equal(got.detach(), value.detach())


def test_a_column_stored_under_another_list_is_refused_on_read(tmp_path: Path) -> None:
    """A data context reads only tensor columns stored under its own list."""
    strict = _context(tmp_path, [])
    lenient = _context(tmp_path, ["memory_layout"])
    table = _source(lenient, _transposed()).as_table().select(["k", "x"])
    with pytest.raises(LogicalTypeIdentifierError) as raised:
        source = ArrowTableSource(
            table, tag_columns=["k"], source_id="stored", data_context=strict
        )
        for _, data in source.iter_data():
            data.as_dict()
    message = str(raised.value)
    assert repr(LAYOUT) in message and repr(STRICT) in message


def test_an_unknown_option_name_in_the_configuration_is_an_error(
    tmp_path: Path,
) -> None:
    """A configured name the tensor type does not have raises naming it.

    The tensor type is loaded on first use (building a context must not import
    torch), so the error is raised when the type is first needed.
    """
    context = _context(tmp_path, ["device"])
    with pytest.raises(ValueError, match="'device'"):
        _source(context, torch.zeros(1)).as_table()
    with pytest.raises(ValueError, match="'device'"):
        context.semantic_hasher.hash_object(torch.zeros(1))
