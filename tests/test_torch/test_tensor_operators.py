"""Tensor columns pass through every operator unchanged (ITL-871).

Each source holds one row per entry of ``TENSORS``, so a single column carries
every dtype and shape class at once. Operators are called on streams and read
back through the stream's public methods.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("torch")

from orcapod.operators import (  # noqa: E402
    Batch,
    DropDataColumns,
    GroupBy,
    Join,
    MapData,
    MergeJoin,
    PolarsFilter,
    SelectDataColumns,
    SemiJoin,
)
from orcapod.protocols.core_protocols import StreamProtocol  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402
from tests.test_core.operators.test_async_sync_equivalence import (  # noqa: E402
    async_rows,
    sync_rows,
)
from tests.test_torch.tensor_cases import (  # noqa: E402
    TENSORS,
    assert_same_tensors_any_order,
    assert_tensors_identical,
    same_tensor,
)

NAMES = sorted(TENSORS)
GROUP = {name: f"g{i % 3}" for i, name in enumerate(NAMES)}


def tensors_source(source_id: str = "tensors", shift: int = 0) -> DictSource:
    """One row per tensor; ``shift`` rotates which tensor each tag gets."""
    return DictSource(
        data=[
            {
                "k": name,
                "g": GROUP[name],
                "x": TENSORS[NAMES[(i + shift) % len(NAMES)]],
                "n": i,
            }
            for i, name in enumerate(NAMES)
        ],
        tag_columns=["k", "g"],
        source_id=source_id,
    )


def expected_x(name: str, shift: int = 0) -> Any:
    """The tensor the row ``name`` holds in a source rotated by ``shift``."""
    return TENSORS[NAMES[(NAMES.index(name) + shift) % len(NAMES)]]


def fields(datagram: Any) -> dict[str, Any]:
    """A tag's or data's values as a plain dict."""
    return dict(datagram.as_dict())


def rows(stream: Any) -> dict[str, dict[str, Any]]:
    """Map each row's ``k`` tag to its tag and data values."""
    out: dict[str, dict[str, Any]] = {}
    for tag, data in stream.iter_data():
        out[str(tag.as_dict()["k"])] = {**tag.as_dict(), **data.as_dict()}
    return out


def other_source() -> DictSource:
    """A tensor-free source keyed by the same ``k`` tags."""
    return DictSource(
        data=[{"k": name, "y": i} for i, name in enumerate(NAMES)],
        tag_columns=["k"],
        source_id="other",
    )


def test_join() -> None:
    """Join keeps every tensor exact."""
    got = rows(Join()(tensors_source(), other_source()))
    assert sorted(got) == NAMES
    for name in NAMES:
        assert_tensors_identical(got[name]["x"], expected_x(name))


def test_semijoin() -> None:
    """SemiJoin keeps the matching rows' tensors exact."""
    keep = NAMES[::2]
    right = DictSource(
        data=[{"k": name, "y": 0} for name in keep], tag_columns=["k"], source_id="r"
    )
    got = rows(SemiJoin()(tensors_source(), right))
    assert sorted(got) == keep
    for name in keep:
        assert_tensors_identical(got[name]["x"], expected_x(name))


def test_map_data() -> None:
    """Renaming the tensor column keeps its values."""
    got = rows(MapData({"x": "z"})(tensors_source()))
    for name in NAMES:
        assert "x" not in got[name]
        assert_tensors_identical(got[name]["z"], expected_x(name))


def test_select_and_drop_data_columns() -> None:
    """Selecting and dropping columns keeps the tensors exact."""
    selected = rows(SelectDataColumns(["x"])(tensors_source()))
    dropped = rows(DropDataColumns(["n"])(tensors_source()))
    for got in (selected, dropped):
        for name in NAMES:
            assert "n" not in got[name]
            assert_tensors_identical(got[name]["x"], expected_x(name))


def test_polars_filter_on_tag() -> None:
    """Filtering on a tag keeps the remaining tensors exact."""
    got = rows(PolarsFilter(constraints={"g": "g1"})(tensors_source()))
    expected = [n for n in NAMES if GROUP[n] == "g1"]
    assert sorted(got) == expected
    for name in expected:
        assert_tensors_identical(got[name]["x"], expected_x(name))


def test_batch() -> None:
    """Batch collects tensors into lists, each exact."""
    out = list(Batch(batch_size=4)(tensors_source()).iter_data())
    seen: list[str] = []
    for tag, data in out:
        names = list(fields(tag)["k"])
        xs = fields(data)["x"]
        assert len(names) == len(xs)
        for name, x in zip(names, xs, strict=True):
            assert_tensors_identical(x, expected_x(name))
        seen.extend(names)
    assert sorted(seen) == NAMES


def test_group_by() -> None:
    """GroupBy collects each group's tensors into a list, each exact."""
    out = list(GroupBy(by=["g"])(tensors_source()).iter_data())
    assert sorted(fields(t)["g"] for t, _ in out) == ["g0", "g1", "g2"]
    for tag, data in out:
        # GroupBy drops the non-key tag ``k`` (ITL-1070); it still orders the
        # members, so the group's names come in ascending order.
        assert set(fields(tag)) == {"g"}
        names = sorted(n for n in NAMES if GROUP[n] == fields(tag)["g"])
        for name, x in zip(names, fields(data)["x"], strict=True):
            assert_tensors_identical(x, expected_x(name))


@pytest.mark.parametrize("shift", [0, 1])
def test_merge_join_colliding_tensor_column(shift: int) -> None:
    """MergeJoin merges a colliding tensor column into a list of both tensors."""
    left = tensors_source("left")
    right = tensors_source("right", shift=shift)
    got = rows(MergeJoin()(left, right))
    assert sorted(got) == NAMES
    for name in NAMES:
        assert_same_tensors_any_order(
            list(got[name]["x"]), [expected_x(name), expected_x(name, shift)]
        )


def test_merge_join_order_does_not_depend_on_input_order() -> None:
    """MergeJoin orders merged tensors the same whichever input comes first."""
    forward = rows(MergeJoin()(tensors_source("a"), tensors_source("b", shift=1)))
    backward = rows(MergeJoin()(tensors_source("b", shift=1), tensors_source("a")))
    for name in NAMES:
        f, b = list(forward[name]["x"]), list(backward[name]["x"])
        assert len(f) == len(b) == 2
        for x, y in zip(f, b, strict=True):
            assert_tensors_identical(x, y)


def grouped_tensors_source(source_id: str, shift: int = 0) -> StreamProtocol:
    """``tensors_source`` grouped on ``g``, so ``x`` becomes ``list[torch.Tensor]``."""
    return GroupBy(by=["g"])(tensors_source(source_id, shift))


def assert_merged_groups(got: dict[str, Any], shift: int) -> None:
    """Each group's merged ``x`` holds the left and the right member lists."""
    assert sorted(got) == ["g0", "g1", "g2"]
    for group, merged in got.items():
        names = [n for n in NAMES if GROUP[n] == group]
        assert len(merged) == 2
        expected = [
            [expected_x(n) for n in names],
            [expected_x(n, shift) for n in names],
        ]
        # One merged list from each input: match each list to a distinct expected one.
        remaining = list(expected)
        for members in merged:
            match = next(
                (
                    i
                    for i, e in enumerate(remaining)
                    if len(members) == len(e)
                    and all(same_tensor(g, x) for g, x in zip(members, e, strict=True))
                ),
                None,
            )
            assert match is not None, f"group {group}: unexpected member list"
            del remaining[match]
        assert not remaining


@pytest.mark.parametrize("shift", [0, 1])
def test_merge_join_colliding_grouped_tensor_column(shift: int) -> None:
    """MergeJoin merges colliding ``list[torch.Tensor]`` columns, in a stable order."""
    left = grouped_tensors_source("left")
    right = grouped_tensors_source("right", shift)
    out = MergeJoin()(left, right)
    got = {fields(t)["g"]: fields(d)["x"] for t, d in out.iter_data()}
    assert_merged_groups(got, shift)
    # The swapped call yields the same table. Source info is left out: on tied values
    # from inputs with one pipeline hash, its order follows the call.
    swapped = MergeJoin()(right, left)
    assert swapped.as_table().equals(out.as_table())


def test_merge_join_colliding_grouped_tensor_column_async_equals_sync() -> None:
    """The async MergeJoin of ``list[torch.Tensor]`` columns yields the sync data."""
    left = grouped_tensors_source("left")
    right = grouped_tensors_source("right", 1)
    expected = {fields(t)["g"]: d for t, d in sync_rows(MergeJoin(), left, right)}
    # The async path is under test: this helper drives ``async_execute`` over channels.
    actual = {fields(t)["g"]: d for t, d in async_rows(MergeJoin(), left, right)}
    # Data only: after GroupBy, MergeJoin's async system tag column names differ from
    # the sync ones for any column type (reported separately, as for arrays).
    assert sorted(actual) == sorted(expected)
    for group, data in actual.items():
        assert data.arrow_schema() == expected[group].arrow_schema()
        assert data.content_hash() == expected[group].content_hash()
        assert data.source_info() == expected[group].source_info()
    assert_merged_groups({g: fields(d)["x"] for g, d in actual.items()}, 1)
