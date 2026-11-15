"""``pl.DataFrame`` values through the public interface (ITL-578).

Function pods take and return ``pl.DataFrame``; the frames survive operators, orchestrators
and database backends unchanged, dtype for dtype. Every comparison checks the Polars schema
as well as the values.
"""

from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal

from orcapod import PipelineJob, function_pod
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from orcapod.errors import OutputTypeError
from orcapod.logical_types import LogicalPolarsDataFrame
from orcapod.operators import Batch, GroupBy, Join, SemiJoin
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import DictSource


def _frame(n: int) -> pl.DataFrame:
    """A frame of height ``n`` whose dtypes would all be lost by careless conversion."""
    return pl.DataFrame(
        {
            "i": pl.Series(list(range(n)), dtype=pl.Int8),
            "e": pl.Series(["a"] * n, dtype=pl.Enum(["b", "a"])),
            "t": pl.Series(
                [dt.datetime(2020, 1, 1)] * n, dtype=pl.Datetime("ms", "UTC")
            ),
            "s": pl.Series(
                [{"u": k, "l": [k]} for k in range(n)],
                dtype=pl.Struct({"u": pl.UInt16, "l": pl.List(pl.Float32)}),
            ),
        }
    )


def _assert_exact(got: object, expected: pl.DataFrame) -> None:
    """Assert ``got`` is a frame with the schema and values of ``expected``."""
    assert isinstance(got, pl.DataFrame)
    assert got.schema == expected.schema
    assert_frame_equal(got, expected)


def _frame_list(value: object) -> list[pl.DataFrame]:
    """Narrow a list-valued data column to a list of frames."""
    assert isinstance(value, list)
    assert all(isinstance(item, pl.DataFrame) for item in value)
    return cast(list[pl.DataFrame], value)


@function_pod(output_keys="df")
def make_frame(n: int) -> pl.DataFrame:
    """Return the test frame of height ``n``."""
    return _frame(n)


@function_pod(output_keys="df")
def add_row_index(df: pl.DataFrame) -> pl.DataFrame:
    """Return ``df`` with a leading ``row`` index column."""
    return df.with_row_index("row", offset=0)


@function_pod(output_keys="height")
def frame_height(df: pl.DataFrame) -> int:
    """Return the height of ``df``."""
    return df.height


def _int_source() -> DictSource:
    """Two rows tagged ``a`` and ``b`` with ``n`` of 1 and 3."""
    return DictSource(
        [{"k": "a", "n": 1}, {"k": "b", "n": 3}],
        tag_columns=["k"],
    )


def _frame_source() -> DictSource:
    """Three frame-valued rows: two in group ``a``, one in group ``b``."""
    rows: list[dict[str, Any]] = [
        {"g": "a", "k": "1", "df": _frame(1)},
        {"g": "a", "k": "2", "df": _frame(2)},
        {"g": "b", "k": "3", "df": _frame(3)},
    ]
    return DictSource(
        rows,
        tag_columns=["g", "k"],
        data_schema={"g": str, "k": str, "df": pl.DataFrame},
    )


def test_pod_can_return_a_polars_dataframe() -> None:
    """Declaring a pod with a ``pl.DataFrame`` return type no longer raises."""

    @function_pod(output_keys="df")
    def make(n: int) -> pl.DataFrame:
        """Return a one-column frame of height ``n``."""
        return pl.DataFrame({"i": list(range(n))})

    assert make.pod is not None


class TestPodValues:
    """Pods that return and take ``pl.DataFrame`` values, alone and chained."""

    def test_pod_output(self) -> None:
        """A pod returns frames that read back with the exact schema and values."""
        stream = make_frame.pod(_int_source())
        got = {tag["k"]: data["df"] for tag, data in stream.iter_data()}
        _assert_exact(got["a"], _frame(1))
        _assert_exact(got["b"], _frame(3))

    def test_output_schema_names_the_python_type(self) -> None:
        """The output schema reports ``pl.DataFrame`` as the column's Python type."""
        _, data_schema = make_frame.pod(_int_source()).output_schema()
        assert data_schema["df"] is pl.DataFrame

    def test_pod_input(self) -> None:
        """A pod reads a frame-valued source column and sees each frame's height."""
        stream = frame_height.pod(_frame_source())
        got = {tag["k"]: data["height"] for tag, data in stream.iter_data()}
        assert got == {"1": 1, "2": 2, "3": 3}

    def test_chained_pods(self) -> None:
        """A frame passes through two pods and a third pod reads its height."""
        stream = frame_height.pod(add_row_index.pod(make_frame.pod(_int_source())))
        got = {tag["k"]: data["height"] for tag, data in stream.iter_data()}
        assert got == {"a": 1, "b": 3}

    def test_chained_frame_is_transformed_exactly(self) -> None:
        """A pod's change to an upstream pod's frame reads back exactly."""
        stream = add_row_index.pod(make_frame.pod(_int_source()))
        got = {tag["k"]: data["df"] for tag, data in stream.iter_data()}
        _assert_exact(got["b"], _frame(3).with_row_index("row", offset=0))


def _stored_frames(column: pa.ChunkedArray[Any]) -> list[pl.DataFrame]:
    """Decode a stored ``orcapod.polars.dataframe`` column with its logical type."""
    logical_type = LogicalPolarsDataFrame()
    assert column.type == logical_type.get_arrow_extension_type()
    extension_array = cast("pa.ExtensionArray[Any]", column.combine_chunks())
    return [
        logical_type.storage_to_python(value)
        for value in extension_array.storage.to_pylist()
    ]


class _FrameSubclass(pl.DataFrame):
    """A user subclass of ``pl.DataFrame``."""


class TestReturnedValueMustBeOfTheDeclaredType:
    """A pod's returned value is an instance of the declared type, or it is refused."""

    @pytest.mark.parametrize(
        "returned",
        [
            pd.DataFrame({"i": [1]}),
            pl.Series("i", [1]),
            pl.DataFrame({"i": [1]}).lazy(),
            pa.table({"i": [1]}),
            {"i": [1]},
        ],
        ids=["pandas_frame", "polars_series", "lazy_frame", "arrow_table", "dict"],
    )
    def test_wrong_type_for_dataframe_is_refused(self, returned: object) -> None:
        """A non-``pl.DataFrame`` value from a pod declared to return one is refused."""

        @function_pod(output_keys="df")
        def wrong(n: int) -> pl.DataFrame:
            """Return ``returned``, which is not a ``pl.DataFrame``."""
            return cast(pl.DataFrame, returned)

        with pytest.raises(OutputTypeError, match="DataFrame"):
            wrong.pod(_int_source()).as_table()

    @pytest.mark.parametrize(
        "returned",
        [pl.DataFrame({"i": [1]}), pd.Series([1], name="i"), [1, 2]],
        ids=["polars_frame", "pandas_series", "list"],
    )
    def test_wrong_type_for_series_is_refused(self, returned: object) -> None:
        """A non-``pl.Series`` value from a pod declared to return one is refused."""

        @function_pod(output_keys="column")
        def wrong(n: int) -> pl.Series:
            """Return ``returned``, which is not a ``pl.Series``."""
            return cast(pl.Series, returned)

        with pytest.raises(OutputTypeError, match="Series"):
            wrong.pod(_int_source()).as_table()

    def test_subclass_instance_is_accepted(self) -> None:
        """A ``pl.DataFrame`` subclass instance is stored as a frame column."""

        @function_pod(output_keys="df")
        def sub(n: int) -> pl.DataFrame:
            """Return a ``_FrameSubclass`` instance of height ``n``."""
            return _FrameSubclass({"i": pl.Series(list(range(n)), dtype=pl.Int8)})

        table = sub.pod(_int_source()).as_table()
        logical_type = LogicalPolarsDataFrame()
        assert table.schema.field("df").type == logical_type.get_arrow_extension_type()
        heights = sorted(frame.height for frame in _stored_frames(table.column("df")))
        assert heights == [1, 3]


class TestOperators:
    """Frame-valued columns pass through operators unchanged."""

    def test_batch_over_frame_column(self) -> None:
        """``Batch`` gathers the frames into lists, each member exact and in order."""
        stream = Batch(batch_size=2)(_frame_source())
        rows = [(tag["k"], data["df"]) for tag, data in stream.iter_data()]
        assert [keys for keys, _ in rows] == [["1", "2"], ["3"]]
        members = _frame_list(rows[0][1]) + _frame_list(rows[1][1])
        for got, n in zip(members, [1, 2, 3], strict=True):
            _assert_exact(got, _frame(n))

    def test_batch_output_type_is_a_list_of_frames(self) -> None:
        """``Batch`` reports the frame column's type as ``list[pl.DataFrame]``."""
        _, data_schema = Batch(batch_size=2)(_frame_source()).output_schema()
        assert data_schema["df"] == list[pl.DataFrame]

    def test_group_by_over_frame_column(self) -> None:
        """``GroupBy`` collects each group's frames into a list, each member exact."""
        stream = GroupBy(by=["g"])(_frame_source())
        got = {
            str(tag["g"]): _frame_list(data["df"]) for tag, data in stream.iter_data()
        }
        assert sorted(got) == ["a", "b"]
        for frame, n in zip(got["a"], [1, 2], strict=True):
            _assert_exact(frame, _frame(n))
        (only_b,) = got["b"]
        _assert_exact(only_b, _frame(3))

    def test_join_passes_frames_through(self) -> None:
        """``Join`` keeps the frames of matched rows exact beside the joined column."""
        weights = DictSource(
            [{"g": "a", "k": "1", "w": 10}, {"g": "b", "k": "3", "w": 30}],
            tag_columns=["g", "k"],
        )
        stream = Join()(_frame_source(), weights)
        got = {
            str(tag["k"]): (data["df"], data["w"]) for tag, data in stream.iter_data()
        }
        assert sorted(got) == ["1", "3"]
        _assert_exact(got["1"][0], _frame(1))
        _assert_exact(got["3"][0], _frame(3))
        assert (got["1"][1], got["3"][1]) == (10, 30)

    def test_semi_join_keeps_frames_of_matching_rows(self) -> None:
        """SemiJoin (a Polars semi join since ITL-847) returns the left frames exactly."""
        keys = DictSource(
            [{"g": "a", "k": "2", "w": 1}, {"g": "b", "k": "3", "w": 2}],
            tag_columns=["g", "k"],
        )
        stream = SemiJoin()(_frame_source(), keys)
        got = {str(tag["k"]): data["df"] for tag, data in stream.iter_data()}
        assert sorted(got) == ["2", "3"]
        _assert_exact(got["2"], _frame(2))
        _assert_exact(got["3"], _frame(3))
        _, data_schema = stream.output_schema()
        assert data_schema["df"] is pl.DataFrame


def _ordered_frame(*, swap_columns: bool, swap_fields: bool) -> pl.DataFrame:
    """Columns ``a, b`` with struct fields ``u, v``; each order can be reversed alone."""
    fields = {"u": pl.Int8, "v": pl.String}
    struct = pl.Struct(dict(reversed(fields.items())) if swap_fields else fields)
    frame = pl.DataFrame(
        {
            "a": pl.Series([{"u": 1, "v": "x"}], dtype=struct),
            "b": pl.Series([2], dtype=pl.Int8),
        }
    )
    return frame.select("b", "a") if swap_columns else frame


@function_pod(output_keys="df")
def make_ordered_frame(swap_columns: bool, swap_fields: bool) -> pl.DataFrame:
    """Return the ordered frame with the requested orders reversed."""
    return _ordered_frame(swap_columns=swap_columns, swap_fields=swap_fields)


@function_pod(output_keys="names")
def leading_names(df: pl.DataFrame) -> str:
    """The first column's name and the first field name of column ``a``."""
    first_field = df.schema["a"]
    assert isinstance(first_field, pl.Struct)
    return f"{df.columns[0]}/{first_field.fields[0].name}"


# Tag -> (swap_columns, swap_fields): each order varied alone and both together.
_ORDERS = {
    "none": (False, False),
    "columns": (True, False),
    "fields": (False, True),
    "both": (True, True),
}


def _order_source() -> DictSource:
    """One row per entry of ``_ORDERS``, tagged by its name."""
    return DictSource(
        [
            {"k": k, "swap_columns": columns, "swap_fields": fields}
            for k, (columns, fields) in _ORDERS.items()
        ],
        tag_columns=["k"],
    )


_NULLABLE_STRUCT = pl.Struct({"u": pl.Int8, "v": pl.String})


def _null_struct_frame(*, whole_struct_null: bool) -> pl.DataFrame:
    """One row: a null struct, or a struct whose two fields are null."""
    value = None if whole_struct_null else {"u": None, "v": None}
    return pl.DataFrame({"s": pl.Series([value], dtype=_NULLABLE_STRUCT)})


@function_pod(output_keys="df")
def make_null_struct_frame(whole_struct_null: bool) -> pl.DataFrame:
    """Return a one-row frame with a null struct or a struct of nulls."""
    return _null_struct_frame(whole_struct_null=whole_struct_null)


@function_pod(output_keys="is_null")
def struct_is_null(df: pl.DataFrame) -> bool:
    """Return whether the first value of column ``s`` is null."""
    return bool(df["s"].is_null()[0])


def _null_struct_source() -> DictSource:
    """Two rows: one for a null struct, one for a struct of nulls."""
    return DictSource(
        [
            {"k": "null_struct", "whole_struct_null": True},
            {"k": "struct_of_nulls", "whole_struct_null": False},
        ],
        tag_columns=["k"],
    )


_STORES: dict[str, Callable[[Path], ArrowDatabaseProtocol]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda tmp: DeltaTableDatabase(tmp / "delta"),
}
_ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


class TestPipelineStores:
    """Frames stored by a pipeline job read back exactly from each store."""

    @pytest.mark.parametrize("store_name", list(_STORES))
    @pytest.mark.parametrize("orchestrator_name", list(_ORCHESTRATORS))
    def test_frames_stored_and_read_back(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """Frames written by a job read back exactly, in each database and orchestrator."""
        job = PipelineJob(name="frames", database=_STORES[store_name](tmp_path))
        with job:
            frames = make_frame.pod(_int_source(), label="make")
            frame_height.pod(frames, label="height")
        job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())

        got = {tag["k"]: data["df"] for tag, data in job.nodes["make"].iter_data()}
        _assert_exact(got["a"], _frame(1))
        _assert_exact(got["b"], _frame(3))
        heights = {
            tag["k"]: data["height"] for tag, data in job.nodes["height"].iter_data()
        }
        assert heights == {"a": 1, "b": 3}

    @pytest.mark.parametrize("store_name", list(_STORES))
    @pytest.mark.parametrize("orchestrator_name", list(_ORCHESTRATORS))
    def test_column_and_struct_field_order_kept_and_distinguished(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """Frames differing only in order are stored exactly and are distinct cache keys.

        A cached pod that returns ``df.columns[0]`` computes for each order instead of
        reusing the other order's result.
        """
        job = PipelineJob(name="order", database=_STORES[store_name](tmp_path))
        with job:
            frames = make_ordered_frame.pod(_order_source(), label="make")
            leading_names.pod(frames, label="names")
        job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())

        got = {tag["k"]: data["df"] for tag, data in job.nodes["make"].iter_data()}
        for k, (columns, fields) in _ORDERS.items():
            _assert_exact(
                got[k], _ordered_frame(swap_columns=columns, swap_fields=fields)
            )
        names = {
            tag["k"]: data["names"] for tag, data in job.nodes["names"].iter_data()
        }
        assert names == {
            "none": "a/u",
            "columns": "b/u",
            "fields": "a/v",
            "both": "b/v",
        }

    @pytest.mark.parametrize("store_name", list(_STORES))
    @pytest.mark.parametrize("orchestrator_name", list(_ORCHESTRATORS))
    def test_null_struct_and_struct_of_nulls_are_distinct_cache_keys(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """A cached pod reading ``is_null()`` computes for each of the two frames."""
        job = PipelineJob(name="nulls", database=_STORES[store_name](tmp_path))
        with job:
            frames = make_null_struct_frame.pod(_null_struct_source(), label="make")
            struct_is_null.pod(frames, label="is_null")
        job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())

        got = {tag["k"]: data["df"] for tag, data in job.nodes["make"].iter_data()}
        for k, whole in (("null_struct", True), ("struct_of_nulls", False)):
            _assert_exact(got[k], _null_struct_frame(whole_struct_null=whole))
        is_null = {
            tag["k"]: data["is_null"] for tag, data in job.nodes["is_null"].iter_data()
        }
        assert is_null == {"null_struct": True, "struct_of_nulls": False}

    def test_saved_job_loaded_back_reads_the_frames(self, tmp_path: Path) -> None:
        """A saved job, loaded in place of the original, reads its stored frames exactly."""
        database_path = tmp_path / "delta"
        job = PipelineJob(name="frames", database=DeltaTableDatabase(database_path))
        with job:
            frames = make_frame.pod(_int_source(), label="make")
            frame_height.pod(frames, label="height")
        job.run()
        saved = tmp_path / "job.json"
        job.save(str(saved))

        loaded = PipelineJob.load(
            str(saved), database=DeltaTableDatabase(database_path)
        )
        node = loaded.nodes["make"]
        _, data_schema = node.output_schema()
        assert data_schema["df"] is pl.DataFrame
        # A loaded job's nodes are empty until it runs (ITL-1095); its stored results
        # are read through the stored view.
        stored_view = node.as_source()
        got = {tag["k"]: data["df"] for tag, data in stored_view.iter_data()}
        _assert_exact(got["a"], _frame(1))
        _assert_exact(got["b"], _frame(3))
        stored = dict(
            zip(
                stored_view.as_table().column("k").to_pylist(),
                _stored_frames(stored_view.as_table().column("df")),
                strict=True,
            )
        )
        _assert_exact(stored["a"], _frame(1))
        heights = {
            tag["k"]: data["height"]
            for tag, data in loaded.nodes["height"].as_source().iter_data()
        }
        assert heights == {"a": 1, "b": 3}

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "ITL-877: SQLiteConnector rejects every logical-typed column at write time "
            "('does not preserve Arrow metadata'), for pandas, numpy and Path too; "
            "not specific to pl.DataFrame."
        ),
    )
    def test_sqlite_store(self, tmp_path: Path) -> None:
        """Frames stored through ``SQLiteConnector`` read back exactly (ITL-877)."""
        connector = SQLiteConnector(str(tmp_path / "frames.sqlite"))
        job = PipelineJob(name="frames", database=ConnectorArrowDatabase(connector))
        with job:
            make_frame.pod(_int_source(), label="make")
        job.run()

        got = {tag["k"]: data["df"] for tag, data in job.nodes["make"].iter_data()}
        _assert_exact(got["a"], _frame(1))
        _assert_exact(got["b"], _frame(3))


# Runs one pipeline over a Delta database and prints how many times each pod body ran, plus
# the frames read back. The pod bodies append to a marker file so a cache hit (body not
# run) is observable from the parent process.
_CROSS_PROCESS_SCRIPT = textwrap.dedent(
    """
    import json, sys
    from pathlib import Path
    import polars as pl
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource

    database_path, marker = Path(sys.argv[1]), Path(sys.argv[2])

    @function_pod(output_keys="df")
    def make_frame(n: int) -> pl.DataFrame:
        with marker.open("a") as fh:
            fh.write("make\\n")
        return pl.DataFrame({
            "i": pl.Series(list(range(n)), dtype=pl.Int8),
            "e": pl.Series(["a"] * n, dtype=pl.Enum(["b", "a"])),
        })

    @function_pod(output_keys="height")
    def frame_height(df: pl.DataFrame) -> int:
        with marker.open("a") as fh:
            fh.write("height\\n")
        return df.height

    source = DictSource([{"k": "a", "n": 1}, {"k": "b", "n": 3}], tag_columns=["k"])
    job = PipelineJob(name="frames", database=DeltaTableDatabase(database_path))
    with job:
        frames = make_frame.pod(source, label="make")
        frame_height.pod(frames, label="height")
    job.run()
    frames = {t["k"]: d["df"] for t, d in job.nodes["make"].iter_data()}
    print(json.dumps({
        "frames": {k: f.write_json() for k, f in frames.items()},
        "schemas": {k: str(f.schema) for k, f in frames.items()},
        "heights": {t["k"]: d["height"] for t, d in job.nodes["height"].iter_data()},
    }))
    """
)


def _run_pipeline_process(database_path: Path, marker: Path) -> dict[str, Any]:
    """Run the pipeline script in a new process and return its printed JSON."""
    result = subprocess.run(
        [sys.executable, "-c", _CROSS_PROCESS_SCRIPT, str(database_path), str(marker)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_cached_rerun_in_new_process_hits_the_cache(tmp_path: Path) -> None:
    """A second process over the same Delta database recomputes nothing and reads equal frames."""
    database_path, marker = tmp_path / "store", tmp_path / "calls.txt"

    first = _run_pipeline_process(database_path, marker)
    calls_after_first = marker.read_text().splitlines()
    assert sorted(calls_after_first) == ["height", "height", "make", "make"]

    second = _run_pipeline_process(database_path, marker)
    assert marker.read_text().splitlines() == calls_after_first  # nothing re-ran
    assert second == first
    assert second["heights"] == {"a": 1, "b": 3}
    assert second["schemas"]["b"] == str(
        pl.Schema({"i": pl.Int8, "e": pl.Enum(["b", "a"])})
    )


@function_pod(output_keys="column")
def first_column(df: pl.DataFrame) -> pl.Series:
    """Return the first column of ``df`` as a ``pl.Series``."""
    return df.to_series(0)


@function_pod(output_keys="total")
def series_sum(column: pl.Series) -> int:
    """Return the sum of ``column`` as an ``int``."""
    return int(column.sum())


class TestSeriesValues:
    """``pl.Series`` values as pod outputs and inputs."""

    def test_pod_output_and_input(self, tmp_path: Path) -> None:
        """A stored ``pl.Series`` keeps name, dtype and values, and a pod reads it."""
        job = PipelineJob(name="series", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            columns = first_column.pod(make_frame.pod(_int_source()), label="first")
            series_sum.pod(columns, label="sum")
        job.run()

        got = {tag["k"]: data["column"] for tag, data in job.nodes["first"].iter_data()}
        for key, n in (("a", 1), ("b", 3)):
            series = got[key]
            assert isinstance(series, pl.Series)
            assert series.name == "i"
            assert series.dtype == pl.Int8
            assert series.to_list() == list(range(n))
        totals = {tag["k"]: data["total"] for tag, data in job.nodes["sum"].iter_data()}
        assert totals == {"a": 0, "b": 3}
