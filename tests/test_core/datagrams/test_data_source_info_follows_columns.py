"""A ``Data``'s source info follows its data columns in every view (ITL-1040, ITL-838).

``Data.drop`` and ``Data.select`` used to remove a data column but keep its source info,
which ``as_dict`` filtered out and ``as_table`` / ``arrow_schema`` did not. The async paths
of ``SelectDataColumns`` and ``DropDataColumns`` call them, so an async run stored an extra
``_source_<dropped column>`` that a sync run did not.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Data, Tag
from orcapod.system_constants import constants

SOURCE = {"x": "src::row_0::x", "s": "src::row_0::s", "m": "src::row_0::m"}


def data(backing: str) -> Data:
    """A ``Data`` with columns ``x``, ``s``, ``m`` and a source token for each."""
    values: dict[str, Any] = {"x": 1, "s": "p", "m": 2.5}
    if backing == "dict":
        return Data(values, source_info=SOURCE)
    table = pa.table(
        {
            "x": pa.array([1], pa.int64()),
            "s": pa.array(["p"], pa.large_string()),
            "m": pa.array([2.5], pa.float64()),
            **{
                f"{constants.SOURCE_PREFIX}{k}": pa.array([v], pa.large_string())
                for k, v in SOURCE.items()
            },
        }
    )
    return Data(table)


BACKINGS = ["dict", "arrow"]


def source_columns(names: list[str]) -> list[str]:
    """The source-info column names among ``names``, in order."""
    return [n for n in names if n.startswith(constants.SOURCE_PREFIX)]


def assert_source_follows(d: Data, expected_columns: list[str]) -> None:
    """Every view of ``d`` with source info names exactly ``expected_columns``."""
    expected = [f"{constants.SOURCE_PREFIX}{c}" for c in expected_columns]
    assert d.keys() == tuple(expected_columns)
    config = {"source": True}
    assert source_columns(d.as_table(columns=config).column_names) == expected
    assert source_columns(d.as_table(all_info=True).column_names) == expected
    assert source_columns(d.arrow_schema(all_info=True).names) == expected
    assert source_columns(d.arrow_schema(columns=config).names) == expected
    assert source_columns(list(d.as_dict(columns=config))) == expected
    assert source_columns(list(d.schema(columns=config))) == expected
    assert source_columns(list(d.keys(columns=config))) == expected
    assert list(d.source_info()) == expected_columns


@pytest.mark.parametrize("backing", BACKINGS)
def test_drop_removes_the_source_info_of_the_dropped_column(backing: str) -> None:
    dropped = data(backing).drop("s")
    assert_source_follows(dropped, ["x", "m"])
    assert dropped.source_info() == {"x": SOURCE["x"], "m": SOURCE["m"]}


@pytest.mark.parametrize("backing", BACKINGS)
def test_select_keeps_only_the_source_info_of_the_selected_columns(
    backing: str,
) -> None:
    selected = data(backing).select("x", "m")
    assert_source_follows(selected, ["x", "m"])


@pytest.mark.parametrize("backing", BACKINGS)
def test_select_orders_the_source_info_as_the_columns(backing: str) -> None:
    """Whatever column order ``select`` gives, the source info follows it."""
    reordered = data(backing).select("m", "x")
    assert set(reordered.keys()) == {"m", "x"}
    assert_source_follows(reordered, list(reordered.keys()))


@pytest.mark.parametrize("backing", BACKINGS)
def test_rename_renames_the_source_info(backing: str) -> None:
    renamed = data(backing).rename({"s": "t"})
    assert_source_follows(renamed, ["x", "t", "m"])
    assert renamed.source_info()["t"] == SOURCE["s"]


@pytest.mark.parametrize("backing", BACKINGS)
def test_update_and_copy_keep_the_source_info(backing: str) -> None:
    assert_source_follows(data(backing).update(x=7), ["x", "s", "m"])
    assert_source_follows(data(backing).copy(), ["x", "s", "m"])


@pytest.mark.parametrize("backing", BACKINGS)
def test_with_columns_adds_empty_source_info_for_the_new_column(backing: str) -> None:
    extended = data(backing).with_columns(n=3)
    assert_source_follows(extended, ["x", "s", "m", "n"])
    assert extended.source_info()["n"] is None


@pytest.mark.parametrize("backing", BACKINGS)
def test_a_column_dropped_then_added_again_has_no_stale_source_info(
    backing: str,
) -> None:
    again = data(backing).drop("s").with_columns(s="q")
    assert again.source_info()["s"] is None
    table = again.as_table(columns={"source": True})
    assert table.column(f"{constants.SOURCE_PREFIX}s").to_pylist() == [None]


def test_tag_drop_keeps_the_system_tags_which_belong_to_the_row() -> None:
    """System tags describe the row, not a tag column: dropping a tag keeps them."""
    system_tag = f"{constants.SYSTEM_TAG_SOURCE_ID_PREFIX}::abc"
    tag = Tag({"a": 1, "b": "q"}, system_tags={system_tag: "src"})

    dropped = tag.drop("b")

    assert dropped.keys() == ("a",)
    table = dropped.as_table(columns={"system_tags": True})
    assert table.column_names == ["a", system_tag]
    assert dropped.system_tags() == {system_tag: "src"}


@pytest.mark.parametrize("operation", ["construct", "drop", "select"])
def test_source_info_given_out_of_column_order_follows_the_columns(
    operation: str,
) -> None:
    """Source info given in another order than the columns is shown in column order."""
    d = Data(
        {"x": 1, "s": "p", "m": 2.5},
        source_info={"m": "src::row_0::m", "s": "src::row_0::s", "x": "src::row_0::x"},
    )
    if operation == "drop":
        d = d.drop("s")
    elif operation == "select":
        d = d.select("x", "m")
    expected = [c for c in ("x", "s", "m") if c in d.keys()]
    assert_source_follows(d, expected)
