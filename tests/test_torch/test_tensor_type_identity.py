"""A tensor column keeps its logical type identity with and without values (ITL-871).

The Arrow hasher replaces a value-hashed logical type by ``large_binary`` tokens.
The identifier of ``orcapod.torch.tensor`` must stay on the hashed field, so a
column with no value (zero rows, or nulls only) is still hashed as a tensor
column: differently from an ndarray column, which shares its storage struct, and
from plain binary (the regression fixed for the other types in ITL-967).
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

pytest.importorskip("torch")

import numpy as np  # noqa: E402
import pyarrow as pa  # noqa: E402
import torch  # noqa: E402

from orcapod.contexts import get_default_context  # noqa: E402
from orcapod.logical_types.identifier import logical_type_identifier  # noqa: E402
from orcapod.types import Schema  # noqa: E402


def _null_column(python_type: Any, rows: int) -> pa.Table:
    """A one-column table of ``python_type`` holding ``rows`` nulls."""
    arrow_type = get_default_context().type_converter.python_type_to_arrow_type(
        python_type
    )
    assert isinstance(arrow_type, pa.ExtensionType)
    storage = pa.array([None] * rows, arrow_type.storage_type)
    return pa.table({"c": pa.ExtensionArray.from_storage(arrow_type, storage)})


def test_identifier_is_the_extension_name_and_the_ignore_list() -> None:
    """The identifier is ``orcapod.torch.tensor`` plus ``{"ignore":[],"version":1}``."""
    logical_type = get_default_context().type_converter.get_logical_type(torch.Tensor)
    assert logical_type is not None
    identifier = logical_type_identifier(logical_type)
    metadata = b'{"ignore":[],"version":1}'
    assert identifier.name == "orcapod.torch.tensor"
    assert identifier.metadata == metadata
    assert tuple(logical_type.readable_extension_metadata()) == (metadata,)


@pytest.mark.parametrize("rows", [0, 2], ids=["zero_rows", "all_null"])
def test_valueless_tensor_column_hashes_as_a_tensor_column(rows: int) -> None:
    """A tensor column with no value differs from an ndarray column and plain binary."""
    hasher = get_default_context().arrow_hasher
    hashes = {
        "tensor": hasher.hash_table(_null_column(torch.Tensor, rows)),
        "ndarray": hasher.hash_table(_null_column(np.ndarray, rows)),
        "plain_large_binary": hasher.hash_table(
            pa.table({"c": pa.array([None] * rows, pa.large_binary())})
        ),
    }
    assert len(set(hashes.values())) == len(hashes), hashes
    assert hashes["tensor"] == hasher.hash_table(_null_column(torch.Tensor, rows))


def test_hashed_type_is_the_same_with_and_without_data() -> None:
    """One declared tensor column is hashed under one type, whatever rows it holds."""
    context = get_default_context()
    with_data = context.type_converter.python_dicts_to_arrow_table(
        [{"c": torch.arange(3.0)}, {"c": None}],
        python_schema=Schema({"c": Optional[torch.Tensor]}),
    )
    # Non-public helper: the test is about the schema the hasher hashes under.
    hashed = context.arrow_hasher._process_table_columns  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
    assert hashed(with_data).schema == hashed(with_data.slice(0, 0)).schema
    assert hashed(with_data).schema == hashed(with_data.slice(1, 1)).schema


def test_tensor_and_equal_ndarray_are_told_apart_by_type() -> None:
    """With equal elements, the value, type, schema and table hashes all differ."""
    context = get_default_context()
    hasher = context.semantic_hasher
    tensor = torch.arange(6, dtype=torch.float32).reshape(2, 3)
    array = tensor.numpy().copy()
    assert hasher.hash_object(tensor).digest != hasher.hash_object(array).digest
    assert hasher.hash_object(tensor) != hasher.hash_object(array)
    assert hasher.hash_object(torch.Tensor) != hasher.hash_object(np.ndarray)
    assert hasher.hash_object(Schema({"c": torch.Tensor})) != hasher.hash_object(
        Schema({"c": np.ndarray})
    )

    def table_hash(python_type: Any, value: Any) -> Any:
        table = context.type_converter.python_dicts_to_arrow_table(
            [{"c": value}], python_schema=Schema({"c": python_type})
        )
        return context.arrow_hasher.hash_table(table)

    assert table_hash(torch.Tensor, tensor) != table_hash(np.ndarray, array)
