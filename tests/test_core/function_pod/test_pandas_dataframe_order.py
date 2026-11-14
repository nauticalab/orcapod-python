"""A pandas frame's column order is kept in storage and is part of its cache key (ITL-897).

Through the public interface, for the in-memory and Delta stores and the sync and async
orchestrators. On v0.2.0 a cached pod returning ``df.columns[0]`` was handed the result
computed for the same frame with its columns in another order.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from pandas.testing import assert_frame_equal

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import DictSource


def _frame(reverse: bool) -> pd.DataFrame:
    """Columns ``a, b`` over a two-level index ``i, j``, or both orders reversed."""
    if reverse:
        index = pd.MultiIndex.from_tuples([("p", 0), ("q", 1)], names=["j", "i"])
        return pd.DataFrame({"b": ["x", "y"], "a": [1, 2]}, index=index)
    index = pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"])
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}, index=index)


@function_pod(output_keys="df")
def make_frame(reverse: bool) -> pd.DataFrame:
    """The frame of ``_frame`` in the asked order."""
    return _frame(reverse)


@function_pod(output_keys="leading")
def leading_names(df: pd.DataFrame) -> str:
    """The first column's name and the first index level's name."""
    return f"{df.columns[0]}/{df.index.names[0]}"


_COUNTED_CALLS: list[str] = []


@function_pod(output_keys="leading")
def counted_leading_names(df: pd.DataFrame) -> str:
    """As ``leading_names``, recording every call that actually computes."""
    leading = f"{df.columns[0]}/{df.index.names[0]}"
    _COUNTED_CALLS.append(leading)
    return leading


_VARIANT_CALLS: list[str] = []


@function_pod(output_keys="df")
def make_variant_frame(variant: str) -> pd.DataFrame:
    """``_frame(False)`` with one order changed: ``columns``, ``levels`` or neither."""
    frame = _frame(reverse=False)
    if variant == "columns":
        return frame.loc[:, ["b", "a"]]
    if variant == "levels":
        index = frame.index
        assert isinstance(index, pd.MultiIndex)
        return frame.set_index(index.swaplevel())
    return frame


@function_pod(output_keys="leading")
def counted_variant_leading_names(df: pd.DataFrame) -> str:
    """As ``leading_names``, recording every call that actually computes."""
    leading = f"{df.columns[0]}/{df.index.names[0]}"
    _VARIANT_CALLS.append(leading)
    return leading


_CONSUMER_CALLS: list[int] = []


@function_pod(output_keys="df")
def make_frame_storage_changes(n: int) -> pd.DataFrame:
    """A frame storage reads back in another form: object ints, pyarrow strings, attrs."""
    frame = pd.DataFrame(
        {
            "ints": pd.Series(list(range(n)), dtype=object),
            "text": pd.array(["x"] * n, dtype="string[pyarrow]"),
        },
        index=pd.RangeIndex(n),
    )
    frame.attrs = {1: "one"}
    return frame


@function_pod(output_keys="total")
def counted_total(df: pd.DataFrame) -> int:
    """The sum of ``ints``, recording every call that actually computes."""
    _CONSUMER_CALLS.append(len(df))
    return int(df["ints"].sum())


_STORES: dict[str, Callable[[Path], ArrowDatabaseProtocol]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda tmp: DeltaTableDatabase(tmp / "delta"),
}
_ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}
_EXPECTED = {"fwd": "a/i", "rev": "b/j"}


def _run(
    store: ArrowDatabaseProtocol, orchestrator_name: str, rows: list[tuple[str, bool]]
) -> PipelineJob:
    """Run ``make_frame -> leading_names`` over ``rows`` on ``store`` with the named orchestrator."""
    source = DictSource(
        [{"k": key, "reverse": reverse} for key, reverse in rows], tag_columns=["k"]
    )
    job = PipelineJob(name="pandas_order", database=store)
    with job:
        frames = make_frame.pod(source, label="make")
        leading_names.pod(frames, label="leading")
    job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())
    return job


@pytest.mark.parametrize("store_name", list(_STORES))
@pytest.mark.parametrize("orchestrator_name", list(_ORCHESTRATORS))
class TestPandasColumnOrder:
    """Column order of a frame in a pipeline, on every store and orchestrator."""

    def test_frames_stored_with_their_order(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """A stored frame reads back with its own column and level order."""
        job = _run(
            _STORES[store_name](tmp_path),
            orchestrator_name,
            [("fwd", False), ("rev", True)],
        )
        got = {tag["k"]: data["df"] for tag, data in job.nodes["make"].iter_data()}
        assert_frame_equal(got["fwd"], _frame(reverse=False))
        assert_frame_equal(got["rev"], _frame(reverse=True))

    def test_cached_pod_computes_for_each_order(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """Both orders in one run: each row gets the result for its own frame."""
        job = _run(
            _STORES[store_name](tmp_path),
            orchestrator_name,
            [("fwd", False), ("rev", True)],
        )
        got = {
            tag["k"]: data["leading"] for tag, data in job.nodes["leading"].iter_data()
        }
        assert got == _EXPECTED

    def test_warm_cache_does_not_serve_another_order(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """One order per run on one store, so the second run meets a warm cache."""
        store = _STORES[store_name](tmp_path)
        got: dict[str, str] = {}
        for key, reverse in (("fwd", False), ("rev", True)):
            job = _run(store, orchestrator_name, [(key, reverse)])
            got.update(
                {
                    tag["k"]: data["leading"]
                    for tag, data in job.nodes["leading"].iter_data()
                }
            )
        assert got == _EXPECTED

    def test_equal_frame_is_served_from_the_cache(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """An equal frame in a later run is a cache hit; a reordered one computes."""
        store = _STORES[store_name](tmp_path)
        _COUNTED_CALLS.clear()
        got: dict[str, str] = {}
        for key, reverse in (("first", False), ("equal", False), ("reordered", True)):
            source = DictSource([{"k": key, "reverse": reverse}], tag_columns=["k"])
            job = PipelineJob(name="pandas_order_counted", database=store)
            with job:
                frames = make_frame.pod(source, label="make")
                counted_leading_names.pod(frames, label="leading")
            job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())
            got.update(
                {
                    tag["k"]: data["leading"]
                    for tag, data in job.nodes["leading"].iter_data()
                }
            )
        assert got == {"first": "a/i", "equal": "a/i", "reordered": "b/j"}
        assert _COUNTED_CALLS == ["a/i", "b/j"]

    def test_each_order_alone_is_a_cache_miss(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """Column order alone, and index level order alone, each compute again."""
        store = _STORES[store_name](tmp_path)
        _VARIANT_CALLS.clear()
        got: dict[str, str] = {}
        for key, variant in (
            ("first", "none"),
            ("equal", "none"),
            ("columns", "columns"),
            ("levels", "levels"),
        ):
            source = DictSource([{"k": key, "variant": variant}], tag_columns=["k"])
            job = PipelineJob(name="pandas_order_variants", database=store)
            with job:
                frames = make_variant_frame.pod(source, label="make")
                counted_variant_leading_names.pod(frames, label="leading")
            job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())
            got.update(
                {
                    tag["k"]: data["leading"]
                    for tag, data in job.nodes["leading"].iter_data()
                }
            )
        assert got == {
            "first": "a/i",
            "equal": "a/i",
            "columns": "b/i",
            "levels": "a/j",
        }
        assert _VARIANT_CALLS == ["a/i", "b/i", "a/j"]

    def test_frame_read_back_from_the_store_keeps_its_cache_key(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """A frame that storage reads back in another form is still a cache hit.

        The store holds the frame in the form storage restores (``object`` ints as
        ``int64``, ``string[pyarrow]`` as ``string[python]``, ``attrs`` as JSON). A second
        run on the same store must not compute the consumer again. The hash-level
        guarantee, ``hash(read(write(v))) == hash(v)``, is tested in
        ``tests/test_hashing/test_pandas_value_identity.py``.
        """
        store = _STORES[store_name](tmp_path)
        _CONSUMER_CALLS.clear()
        totals: list[int] = []
        for _ in range(2):
            source = DictSource([{"k": "a", "n": 3}], tag_columns=["k"])
            job = PipelineJob(name="pandas_storage_form", database=store)
            with job:
                frames = make_frame_storage_changes.pod(source, label="make")
                counted_total.pod(frames, label="total")
            job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())
            totals += [data["total"] for _, data in job.nodes["total"].iter_data()]
        assert totals == [3, 3]
        assert _CONSUMER_CALLS == [3]


def _files(root: Path) -> dict[str, bytes]:
    """Every file under ``root`` with its bytes."""
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_v02_table_with_a_pandas_column_is_reported_not_read_and_left_alone(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A table v0.2 wrote with a ``pandas.dataframe`` column is counted and untouched.

    Before v0.3 a frame column was stored under the extension name ``pandas.dataframe``
    with no metadata, in a table at an older layout (``rdb_v1``). That name is no longer
    registered. The table is found by the function's name and reported; it is never
    read (reading the column would be refused), and its files stay byte-identical.
    """
    root = tmp_path / "delta"
    old = (
        "pandas_order",
        "_result",
        "make_frame",
        "0" * 64,
        "v0",
        "python.function.v0",
        "rdb_v1",
    )
    old_dir = root.joinpath(*old)
    old_dir.mkdir(parents=True)
    # Written with deltalake directly: the table must look like what v0.2 stored, which
    # this version's database would not write.
    column = pa.field(
        "df",
        pa.large_binary(),
        metadata={b"ARROW:extension:name": b"pandas.dataframe"},
    )
    write_deltalake(
        str(old_dir),
        pa.table(
            [pa.array([b"not read"], pa.large_binary())], schema=pa.schema([column])
        ),
    )
    stored = DeltaTable(str(old_dir)).to_pyarrow_table().schema.field("df")
    assert stored.metadata == {b"ARROW:extension:name": b"pandas.dataframe"}
    before = _files(old_dir)

    with caplog.at_level(logging.WARNING):
        job = _run(DeltaTableDatabase(root), "sync", [("fwd", False)])

    reported = " ".join(record.getMessage() for record in caplog.records)
    assert "written by an earlier version of Orcapod" in reported
    assert "/".join(old) in reported
    got = {tag["k"]: data["leading"] for tag, data in job.nodes["leading"].iter_data()}
    assert got == {"fwd": "a/i"}
    assert _files(old_dir) == before
