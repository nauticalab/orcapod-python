"""SemiJoin keys on the shared tag columns only (ITL-1088).

Developer decision, November 13, 2026: "Semijoin should key on shared tags only". The
match keys are exactly the names that are tag columns of both inputs. Every other shared
name (data on both sides, or a tag on one side and data on the other) is ignored, and the
right input's data columns are never read. For v0.3 such a name raises
``SemiJoinIgnoredColumnWarning`` once per application (provisional: it may become an
error). With no shared tag, every left row is returned. Before, every shared name, tag or
data, was a match key.
"""

from __future__ import annotations

import asyncio
import warnings
from collections.abc import Callable

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.channels import Channel
from orcapod.core.operators import PolarsFilter, SemiJoin
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SemiJoinIgnoredColumnWarning
from orcapod.protocols.core_protocols import StreamProtocol
from tests.test_core.operators.test_async_sync_equivalence import (
    Row,
    _meta_columns,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    _source,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    _with_meta,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    assert_equivalent,
    assert_same_provenance,
    sync_rows,
)


def left_source() -> ArrowTableSource:
    """Tag ``id`` 1..3, data ``y`` 1..3 and ``x``."""
    return _source(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "y": pa.array([1, 2, 3], pa.int64()),
            "x": pa.array([10, 20, 30], pa.int64()),
        },
        tags=("id",),
        source_id="left",
    )


def right_same_tag_other_y() -> ArrowTableSource:
    """Tag ``id`` 1..3 and data ``y`` that equals the left's only for ``id`` 1 and 3."""
    return _source(
        {"id": pa.array([1, 2, 3], pa.int64()), "y": pa.array([1, 9, 3], pa.int64())},
        tags=("id",),
        source_id="right_y",
    )


def right_tag_y() -> ArrowTableSource:
    """Tag ``y`` (the left's data column name) and data ``z``."""
    return _source(
        {"y": pa.array([1, 3], pa.int64()), "z": pa.array([0, 0], pa.int64())},
        tags=("y",),
        source_id="right_tag_y",
    )


def right_nothing_shared() -> ArrowTableSource:
    """Tag ``other`` and data ``w``: no name in common with the left."""
    return _source(
        {"other": pa.array([7], pa.int64()), "w": pa.array([0], pa.int64())},
        tags=("other",),
        source_id="right_nothing",
    )


def right_unequal_data_of_another_type() -> ArrowTableSource:
    """Tag ``id`` 1 and 3; data ``y`` and ``x`` of other types and values than the left's."""
    return _source(
        {
            "id": pa.array([3, 1], pa.int64()),
            "y": pa.array(["no", "match"], pa.large_string()),
            "x": pa.array([None, 0.5], pa.float64()),
        },
        tags=("id",),
        source_id="right_other_data",
        nullable=("x",),
    )


def right_tags_only() -> ArrowTableSource:
    """Tag ``id`` 1 and 3 and data ``z``: only the tag is shared."""
    return _source(
        {"id": pa.array([1, 3], pa.int64()), "z": pa.array([0, 0], pa.int64())},
        tags=("id",),
        source_id="right_tags_only",
    )


def right_data_id() -> ArrowTableSource:
    """Tag ``other`` and data ``id``: the left's tag name is data here."""
    return _source(
        {"other": pa.array([7], pa.int64()), "id": pa.array([1], pa.int64())},
        tags=("other",),
        source_id="right_data_id",
    )


def left_ids(stream: StreamProtocol) -> list[int | None]:
    """The ``id`` of each result row, in order."""
    return stream.as_table().column("id").to_pylist()


# (right input, ids kept, ignored (name, left role, right role))
CASES = [
    pytest.param(
        right_same_tag_other_y,
        [1, 2, 3],
        [("y", "data", "data")],
        id="data_on_both_sides",
    ),
    pytest.param(
        right_tag_y, [1, 2, 3], [("y", "data", "tag")], id="data_left_tag_right"
    ),
    pytest.param(
        right_data_id, [1, 2, 3], [("id", "tag", "data")], id="tag_left_data_right"
    ),
    pytest.param(
        right_unequal_data_of_another_type,
        [1, 3],
        [("y", "data", "data"), ("x", "data", "data")],
        id="right_data_never_read",
    ),
    pytest.param(right_tags_only, [1, 3], [], id="tags_only"),
    pytest.param(right_nothing_shared, [1, 2, 3], [], id="nothing_shared"),
]

Ignored = list[tuple[str, str, str]]


def assert_warned_once(record: list[warnings.WarningMessage], ignored: Ignored) -> None:
    """Exactly one ``SemiJoinIgnoredColumnWarning`` naming each column and its roles,
    or none when nothing is ignored."""
    found = [w for w in record if issubclass(w.category, SemiJoinIgnoredColumnWarning)]
    assert len(found) == (1 if ignored else 0)
    if ignored:
        message = str(found[0].message)
        for name, left_role, right_role in ignored:
            assert (
                f"'{name}' ({left_role} on the left, {right_role} on the right)"
                in message
            )
        assert "only on tag columns shared by both inputs" in message
        # Attributed to this test file, the user's call.
        assert found[0].filename == __file__


@pytest.mark.parametrize(("make_right", "kept", "ignored"), CASES)
def test_semijoin_keys_on_shared_tags_only(
    make_right: Callable[[], ArrowTableSource], kept: list[int], ignored: Ignored
) -> None:
    """Only shared tags decide; other shared names warn once and play no part."""
    left = left_source()
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        result = SemiJoin()(left, make_right())
        assert left_ids(result) == kept
    assert_warned_once(record, ignored)
    assert result.output_schema() == left.output_schema()
    assert result.as_table().schema == left.as_table().schema


async def _async_rows(
    op: SemiJoin, left: StreamProtocol, right: StreamProtocol
) -> list[Row]:
    channels: list[Channel[Row]] = [Channel(buffer_size=64) for _ in range(2)]
    output: Channel[Row] = Channel(buffer_size=64)
    for stream, channel in zip((left, right), channels):
        for row in stream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    await op.async_execute(
        [channel.reader for channel in channels],
        output.writer,
        input_pipeline_hashes=[left.pipeline_hash(), right.pipeline_hash()],
    )
    return await output.reader.collect()


@pytest.mark.parametrize(("make_right", "kept", "ignored"), CASES)
def test_async_rows_equal_sync_rows(
    make_right: Callable[[], ArrowTableSource], kept: list[int], ignored: Ignored
) -> None:
    """The async path keys on the same columns; called without validation it warns once."""
    left, right = left_source(), make_right()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", SemiJoinIgnoredColumnWarning)
        expected = sync_rows(SemiJoin(), left, right)
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        actual = asyncio.run(_async_rows(SemiJoin(), left, right))
    found = [w for w in record if issubclass(w.category, SemiJoinIgnoredColumnWarning)]
    assert len(found) == (1 if ignored else 0)
    assert len(expected) == len(kept)
    assert_equivalent(expected, actual)
    assert_same_provenance(expected, actual)


def test_validated_async_run_does_not_warn_again() -> None:
    """An operator applied (and warned) once does not warn again on its async run."""
    left, right = left_source(), right_same_tag_other_y()
    op = SemiJoin()
    with pytest.warns(SemiJoinIgnoredColumnWarning):
        _ = op(left, right)
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        rows = asyncio.run(_async_rows(op, left, right))
    assert len(rows) == 3
    assert not [
        w for w in record if issubclass(w.category, SemiJoinIgnoredColumnWarning)
    ]


@pytest.mark.parametrize("run_async", [False, True], ids=["sync", "async"])
def test_job_warns_once(run_async: bool) -> None:
    """A semi join in a job warns once, at the call, not again when the job runs."""
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        job = PipelineJob(name="semijoin_warns_once", database=InMemoryArrowDatabase())
        with job:
            _ = SemiJoin(label="sj")(left_source(), right_same_tag_other_y())
        if run_async:
            from orcapod.pipeline import AsyncPipelineOrchestrator

            job.run(orchestrator=AsyncPipelineOrchestrator())
        else:
            job.run()
    assert_warned_once(record, [("y", "data", "data")])


def test_warning_promoted_to_an_error_raises_again_on_retry() -> None:
    """Under an error filter the warning raises, and a retry raises again."""
    op = SemiJoin()
    with warnings.catch_warnings():
        warnings.simplefilter("error", SemiJoinIgnoredColumnWarning)
        for _ in range(2):
            with pytest.raises(SemiJoinIgnoredColumnWarning):
                _ = op(left_source(), right_same_tag_other_y())


def test_filter_a_stream_by_a_subset_of_itself() -> None:
    """The commonest use: every data name is shared (and warned about); tags decide."""
    source = left_source()
    subset = PolarsFilter(constraints={"id": 2})(left_source())
    with pytest.warns(SemiJoinIgnoredColumnWarning, match="'y'.*'x'"):
        result = SemiJoin()(source, subset)
    assert left_ids(result) == [2]


def test_filter_without_the_warning() -> None:
    """Reduce the right input to its tags plus one renamed data column: no warning."""
    source = left_source()
    subset = PolarsFilter(constraints={"id": 2})(left_source())
    keys_only = subset.select_data_columns(["x"]).map_data({"x": "_x"})
    with warnings.catch_warnings():
        warnings.simplefilter("error", SemiJoinIgnoredColumnWarning)
        result = SemiJoin()(source, keys_only)
    assert left_ids(result) == [2]


@pytest.mark.parametrize("path", ["sync", "async_validated", "async_unvalidated"])
def test_shared_meta_column_is_neither_a_key_nor_warned_about(path: str) -> None:
    """A meta (``__``) column of the same name on both inputs is not a user column
    (ITL-1080): no match key, no ``SemiJoinIgnoredColumnWarning``, not in the output."""
    # Same meta names on both sides, with different values: a key would drop rows.
    left = _with_meta(left_source(), _meta_columns("m", 3))
    right = _with_meta(right_tags_only(), _meta_columns("m", 2))
    op = SemiJoin()
    with warnings.catch_warnings():
        warnings.simplefilter("error", SemiJoinIgnoredColumnWarning)
        if path == "sync":
            result = op(left, right)
            ids = left_ids(result)
            meta = [n for n in result.as_table(all_info=True).column_names if "__" in n]
        else:
            if path == "async_validated":
                _ = op(left, right)
            rows = asyncio.run(_async_rows(op, left, right))
            ids = [tag.as_dict()["id"] for tag, _ in rows]
            meta = [
                n
                for _, data in rows
                for n in data.as_table(all_info=True).column_names
                if "__" in n
            ]
    assert ids == [1, 3]
    assert meta == []
