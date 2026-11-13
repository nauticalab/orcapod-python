"""SpiralDBTableSource reads no rows until asked and then reads them in batches (ITL-186).

Runs against the disk-backed pyspiral fake, whose ``scan_log`` records every scan a source
opens and how many batches were taken from it.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.databases.spiraldb_connector import _serialize_arrow_metadata
from orcapod.errors import SourceTooLargeError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.serialization import resolve_source_from_config
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_pyspiral

_PROJECT = "edi-sample"
_KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
)
_ROWS = 5


def _rows(timestamps: list[int], subject: str = "m1") -> pa.Table:
    """Return all-nullable ``event`` rows for ``timestamps``."""
    table = pa.table(
        {
            "subject_id": [subject] * len(timestamps),
            "timestamp": pa.array(timestamps, pa.uint64()),
            "amplitude": pa.array([t + 0.5 for t in timestamps], pa.float32()),
        }
    )
    return table.cast(pa.schema([pa.field(f.name, f.type, True) for f in table.schema]))


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fake project holding a 5-row ``event`` table read 2 rows per batch."""
    monkeypatch.setattr(fake_pyspiral, "scan_batch_rows", 2)
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        project.create_table("default.event", key_schema=_KEYS).write(
            _rows([1, 2, 3, 4, 5])
        )
        fake_pyspiral.scan_log.clear()
        yield project
        fake_pyspiral.scan_log.clear()


def _timestamps(source: SpiralDBTableSource) -> list[int]:
    """Return the ``timestamp`` key of every row ``source`` reads, in order."""
    return cast("list[int]", source.as_table().column("timestamp").to_pylist())


def _record_ids(source: SpiralDBTableSource) -> dict[int, bytes]:
    """Return each row's record ID by its ``timestamp`` key."""
    table = source.as_table(all_info=True)
    (column,) = [n for n in table.column_names if n.startswith("_tag_record_id::")]
    timestamps = cast("list[int]", table.column("timestamp").to_pylist())
    record_ids = cast("list[bytes]", table.column(column).to_pylist())
    return dict(zip(timestamps, record_ids, strict=True))


class TestNoRowIsReadUntilAsked:
    """A source reads no rows until they are asked for, then reads in batches."""

    def test_construction_and_identity_read_no_rows(self, project):
        """Building a source and reading its schema, hashes and config scan nothing."""
        source = SpiralDBTableSource(_PROJECT, "event")
        source.output_schema(all_info=True)
        source.keys(all_info=True)
        source.content_hash()
        source.pipeline_hash()
        source.to_config()
        assert source.row_count_estimate == _ROWS

        assert fake_pyspiral.scan_log == []

    def test_iter_data_reads_batches_only_as_rows_are_consumed(self, project):
        """``iter_data`` takes a batch from the scan only as its rows are consumed."""
        source = SpiralDBTableSource(_PROJECT, "event")
        rows = iter(source.iter_data())

        next(rows)
        assert [scan.batches_read for scan in fake_pyspiral.scan_log] == [1]

        assert sum(1 for _ in rows) == _ROWS - 1
        assert [scan.batches_read for scan in fake_pyspiral.scan_log] == [3]

    def test_batches_handed_on_hold_at_most_batch_rows(self, project, monkeypatch):
        """Batches handed on hold at most ``batch_rows`` rows, whatever is scanned."""
        monkeypatch.setattr(fake_pyspiral, "scan_batch_rows", 4)
        source = SpiralDBTableSource(_PROJECT, "event", batch_rows=3)

        # About the non-public batching: no public method shows the batch sizes.
        sizes = [s.as_table().num_rows for s in source._batch_streams()]  # pyright: ignore[reportPrivateUsage]

        assert sizes == [3, 1, 1]

    def test_a_read_that_matches_nothing_has_the_schema_of_one_that_does(self, project):
        """A read matching no row yields nothing, with the schema of a full read."""
        nothing = SpiralDBTableSource(_PROJECT, "event", where={"subject_id": "nobody"})
        rows = SpiralDBTableSource(_PROJECT, "event")

        assert list(nothing.iter_data()) == []
        empty = nothing.as_table(all_info=True)
        assert empty.num_rows == 0
        assert empty.schema == rows.as_table(all_info=True).schema

    def test_a_metadata_change_after_construction_is_refused(self, project):
        """Field metadata changed after construction fails the read: rebuild needed."""
        source = SpiralDBTableSource(_PROJECT, "event")
        marked = _rows([1]).cast(
            pa.schema(
                [
                    pa.field("subject_id", pa.string()),
                    pa.field("timestamp", pa.uint64()),
                    pa.field("amplitude", pa.float32(), metadata={"unit": "uV"}),
                ]
            )
        )
        # About the connector's non-public metadata encoding: it is how a table's
        # field metadata is stored, and the test changes only that.
        kv = _serialize_arrow_metadata(marked)  # pyright: ignore[reportPrivateUsage]
        assert kv is not None
        project.table("default.event").set_metadata(kv)

        with pytest.raises(
            ValueError, match="column 'amplitude'.*build the source again"
        ):
            source.as_table()

    def test_a_table_with_key_columns_only_is_refused(self, project):
        """A table never written, holding key columns only, is refused."""
        project.create_table("default.never_written", key_schema=_KEYS)

        with pytest.raises(ValueError, match="never_written.*data column"):
            SpiralDBTableSource(_PROJECT, "never_written")

    def test_schema_is_the_tables(self, project):
        """Keys are the tags and other columns data, all optional, in table order."""
        source = SpiralDBTableSource(_PROJECT, "event")

        assert source.keys() == (("subject_id", "timestamp"), ("amplitude",))
        tag_schema, data_schema = source.output_schema()
        assert dict(tag_schema) == {"subject_id": str | None, "timestamp": int | None}
        assert dict(data_schema) == {"amplitude": float | None}
        assert source.as_table().schema.names == [
            "subject_id",
            "timestamp",
            "amplitude",
        ]


class TestRows:
    """Every way of reading a source yields the same rows."""

    def test_streamed_rows_equal_as_table_rows(self, project):
        """``iter_data`` rows equal ``as_table`` rows, system columns included."""
        source = SpiralDBTableSource(_PROJECT, "event")

        streamed = [
            {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
            for tag, data in source.iter_data()
        ]
        table_rows = source.as_table(all_info=True).to_pylist()

        assert len(streamed) == _ROWS
        assert len(table_rows) == _ROWS
        for streamed_row, table_row in zip(streamed, table_rows, strict=True):
            shared = streamed_row.keys() & table_row.keys()
            assert {"subject_id", "timestamp", "amplitude"} <= shared
            assert any(name.startswith("_tag_record_id::") for name in shared)
            assert any(name.startswith("_source_") for name in shared)
            assert {k: streamed_row[k] for k in shared} == {
                k: table_row[k] for k in shared
            }

    def test_sorting_by_tags_sorts_across_batches(self, project):
        """``sort_by_tags`` orders the whole table by tag, not each batch alone."""
        keys = pa.schema([pa.field("k", pa.int64())])
        project.create_table("default.groups", key_schema=keys).write(
            pa.table({"k": [1, 2, 3, 4, 5], "g": [5, 4, 3, 2, 1], "v": [0.0] * 5})
        )
        source = SpiralDBTableSource(_PROJECT, "groups", tag_columns="g")

        table = source.as_table(columns={"sort_by_tags": True})

        assert table.column("g").to_pylist() == [1, 2, 3, 4, 5]
        assert table.column("k").to_pylist() == [5, 4, 3, 2, 1]

    def test_async_iteration_yields_the_same_rows(self, project):
        """``async_iter_data`` yields exactly the rows ``iter_data`` yields."""
        source = SpiralDBTableSource(_PROJECT, "event")

        async def collect() -> list[dict[str, object]]:
            """Return every row ``async_iter_data`` yields, tags and data merged."""
            return [
                {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
                async for tag, data in source.async_iter_data()
            ]

        expected = [
            {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
            for tag, data in source.iter_data()
        ]
        assert asyncio.run(collect()) == expected

    def test_dataframe_methods_return_the_rows(self, project):
        """``as_df``, ``as_lazy_frame`` and ``as_pandas_df`` return the rows read."""
        source = SpiralDBTableSource(_PROJECT, "event", limit=2)

        assert source.as_df().get_column("timestamp").to_list() == [1, 2]
        assert source.as_lazy_frame().collect().height == 2
        assert list(source.as_pandas_df()["timestamp"]) == [1, 2]


class TestRestrictions:
    """``where``, ``limit``, ``asof`` and the size arguments shape the read."""

    def test_limit_reads_the_first_rows_in_key_order(self, project):
        """``limit`` reads the first rows in key order and is passed to the scan."""
        source = SpiralDBTableSource(_PROJECT, "event", limit=2)

        assert _timestamps(source) == [1, 2]
        assert fake_pyspiral.scan_log[-1].kwargs["limit"] == 2

    def test_where_reads_only_matching_rows(self, project):
        """``where`` reads only the rows whose columns equal the given values."""
        project.table("default.event").write(_rows([7, 8], subject="m2"))
        source = SpiralDBTableSource(_PROJECT, "event", where={"subject_id": "m2"})

        assert _timestamps(source) == [7, 8]

    def test_where_and_limit_together(self, project):
        """``where`` with ``limit`` reads the first matching rows."""
        project.table("default.event").write(_rows([7, 8], subject="m2"))
        source = SpiralDBTableSource(
            _PROJECT, "event", where={"subject_id": "m2"}, limit=1
        )

        assert _timestamps(source) == [7]

    def test_where_on_an_unknown_column_is_refused(self, project):
        """A ``where`` on a column the table lacks is refused at construction."""
        with pytest.raises(ValueError, match="nope"):
            SpiralDBTableSource(_PROJECT, "event", where={"nope": 1})

    def test_sizes_must_be_positive(self, project):
        """A zero ``limit``, ``max_rows`` or ``batch_rows`` is refused."""
        with pytest.raises(ValueError, match="limit"):
            SpiralDBTableSource(_PROJECT, "event", limit=0)
        with pytest.raises(ValueError, match="max_rows"):
            SpiralDBTableSource(_PROJECT, "event", max_rows=0)
        with pytest.raises(ValueError, match="batch_rows"):
            SpiralDBTableSource(_PROJECT, "event", batch_rows=0)

    @pytest.mark.parametrize("name", ["limit", "max_rows", "batch_rows"])
    def test_a_bool_is_not_a_size(self, project, name):
        """``True`` is refused as ``limit``, ``max_rows`` or ``batch_rows``."""
        with pytest.raises(ValueError, match=name):
            # A bool where an int is declared: the call a type checker would refuse.
            sizes: dict[str, Any] = {name: True}
            SpiralDBTableSource(_PROJECT, "event", **sizes)

    def test_every_read_of_one_source_uses_the_version_it_was_built_at(self, project):
        """Every read scans the version current at construction, not a later write."""
        table = project.table("default.event")
        version = table.last_modified_at()
        source = SpiralDBTableSource(_PROJECT, "event")
        table.write(_rows([9]))

        source.as_table()
        list(source.iter_data())

        assert [scan.kwargs["asof"] for scan in fake_pyspiral.scan_log] == [version] * 2

    def test_a_given_version_is_used(self, project):
        """A given ``asof`` is passed to the scan and kept in the config."""
        source = SpiralDBTableSource(_PROJECT, "event", asof=1)

        source.as_table()

        assert fake_pyspiral.scan_log[-1].kwargs["asof"] == 1
        assert source.to_config()["asof"] == 1

    def test_a_given_version_zero_is_used(self, project):
        """An ``asof`` of ``0`` is passed to the scan and saved, not taken as unset."""
        source = SpiralDBTableSource(_PROJECT, "event", asof=0)

        source.as_table()

        assert fake_pyspiral.scan_log[-1].kwargs["asof"] == 0
        assert source.to_config()["asof"] == 0


class TestRowCap:
    """``max_rows`` refuses a read that could exceed it."""

    def test_a_read_over_the_cap_is_refused_before_scanning(self, project):
        """A read over ``max_rows`` fails before any scan, saying how to narrow it."""
        source = SpiralDBTableSource(_PROJECT, "event", max_rows=3)

        with pytest.raises(SourceTooLargeError) as raised:
            source.as_table()

        assert fake_pyspiral.scan_log == []
        message = str(raised.value)
        assert "up to 5 rows" in message and "max_rows=3" in message
        assert "records about 5 rows" in message
        assert "deleted rows not yet compacted" in message
        assert "limit=" in message and "where=" in message
        with pytest.raises(SourceTooLargeError):
            list(source.iter_data())

    def test_a_limit_within_the_cap_is_read(self, project):
        """A ``limit`` within ``max_rows`` lets a larger table be read."""
        source = SpiralDBTableSource(_PROJECT, "event", max_rows=3, limit=3)

        assert _timestamps(source) == [1, 2, 3]

    def test_a_limit_over_the_cap_is_refused(self, project):
        """A ``limit`` above ``max_rows`` is refused, naming the limit."""
        source = SpiralDBTableSource(_PROJECT, "event", max_rows=3, limit=4)

        with pytest.raises(SourceTooLargeError, match="4 rows"):
            source.as_table()

    def test_a_filtered_read_over_the_cap_stops_while_reading(self, project):
        """A ``where`` read that turns out over ``max_rows`` fails while reading."""
        source = SpiralDBTableSource(
            _PROJECT, "event", max_rows=3, where={"subject_id": "m1"}
        )

        with pytest.raises(SourceTooLargeError, match="max_rows=3"):
            list(source.iter_data())

    def test_a_filtered_read_within_the_cap_is_read(self, project):
        """A ``where`` read matching no more than ``max_rows`` rows is read."""
        project.table("default.event").write(_rows([7, 8], subject="m2"))
        source = SpiralDBTableSource(
            _PROJECT, "event", max_rows=3, where={"subject_id": "m2"}
        )

        assert _timestamps(source) == [7, 8]

    def test_no_cap(self, project):
        """``max_rows=None`` reads the whole table."""
        source = SpiralDBTableSource(_PROJECT, "event", max_rows=None)

        assert len(_timestamps(source)) == _ROWS


class TestIdentity:
    """A source's identity names its table, not the restrictions on the read."""

    def test_default_source_id_names_the_table_and_carries_its_spiral_id(self, project):
        """The default source id is ``<project>/<dataset>.<table>@<table id>``."""
        source = SpiralDBTableSource(_PROJECT, "event")
        table_id = project.table("default.event").table_id

        assert source.table_id == table_id
        assert source.source_id == f"{_PROJECT}/default.event@{table_id}"

    def test_a_given_source_id_is_used_as_it_is(self, project):
        """A ``source_id`` given by the user is used unchanged."""
        assert SpiralDBTableSource(_PROJECT, "event", source_id="mine").source_id == (
            "mine"
        )

    def test_a_recreated_table_is_a_different_source(self, project):
        """A recreated table is a new source; only its pipeline hash is unchanged."""
        before = SpiralDBTableSource(_PROJECT, "event")
        project.drop_table("default.event")
        project.create_table("default.event", key_schema=_KEYS).write(
            _rows([1, 2, 3, 4, 5])
        )
        after = SpiralDBTableSource(_PROJECT, "event")

        assert after.source_id != before.source_id
        assert after.content_hash() != before.content_hash()
        assert after.pipeline_hash() == before.pipeline_hash()
        assert _record_ids(after)[1] != _record_ids(before)[1]

    def test_restrictions_do_not_change_identity(self, project):
        """Restrictions on the read leave the source id and both hashes unchanged."""
        whole = SpiralDBTableSource(_PROJECT, "event")
        part = SpiralDBTableSource(
            _PROJECT, "event", where={"subject_id": "m1"}, limit=2, asof=1, max_rows=9
        )

        assert part.source_id == whole.source_id
        assert part.content_hash() == whole.content_hash()
        assert part.pipeline_hash() == whole.pipeline_hash()


class TestRecordIdentity:
    """A row's record identity comes from its primary key."""

    def test_the_row_token_is_the_labelled_hash_of_the_primary_key(self, project):
        """The source-info row token is the labelled Arrow hash of the primary key."""
        source = SpiralDBTableSource(_PROJECT, "event", limit=1)
        key = pa.table(
            {
                "subject_id": pa.array(["m1"], pa.large_string()),
                "timestamp": pa.array([1], pa.uint64()),
            },
            schema=pa.schema(
                [
                    pa.field("subject_id", pa.large_string(), nullable=False),
                    pa.field("timestamp", pa.uint64(), nullable=False),
                ]
            ),
        )
        token = source.data_context.arrow_hasher.hash_table(key).to_string()

        (info,) = source.as_table(columns={"source": True}).column("_source_amplitude")

        assert info.as_py() == f"{source.source_id}::{token}::amplitude"
        assert "::" not in token

    def test_a_row_has_the_same_record_id_in_every_read(self, project):
        """A row has the same record id in a whole, a limited and a filtered read."""
        whole = _record_ids(SpiralDBTableSource(_PROJECT, "event"))
        limited = _record_ids(SpiralDBTableSource(_PROJECT, "event", limit=2))
        filtered = _record_ids(
            SpiralDBTableSource(_PROJECT, "event", where={"timestamp": 4})
        )

        assert len(set(whole.values())) == _ROWS
        assert limited == {t: whole[t] for t in (1, 2)}
        assert filtered == {4: whole[4]}

    def test_a_row_keeps_its_record_id_when_an_earlier_key_is_added(self, project):
        """Adding a row with an earlier key leaves the other record ids unchanged."""
        before = _record_ids(SpiralDBTableSource(_PROJECT, "event"))
        project.table("default.event").write(_rows([0]))

        after = _record_ids(SpiralDBTableSource(_PROJECT, "event", max_rows=None))

        assert list(after) == [0, 1, 2, 3, 4, 5]
        assert {t: after[t] for t in before} == before

    def test_record_ids_do_not_depend_on_tag_columns(self, project):
        """Choosing other tag columns leaves the record ids unchanged."""
        by_key = _record_ids(SpiralDBTableSource(_PROJECT, "event"))
        by_subject = _record_ids(
            SpiralDBTableSource(_PROJECT, "event", tag_columns="subject_id")
        )

        assert by_subject == by_key


class TestSaveAndLoad:
    """A source saved to its config loads as the same source."""

    def test_config_round_trip_reads_no_rows(self, project):
        """A config round trip keeps the arguments and hash and scans nothing."""
        source = SpiralDBTableSource(
            _PROJECT,
            "event",
            where={"subject_id": "m1", "timestamp": 2},
            limit=3,
            max_rows=50,
            batch_rows=7,
        )
        config = source.to_config()

        loaded = resolve_source_from_config(config)

        assert fake_pyspiral.scan_log == []
        assert isinstance(loaded, SpiralDBTableSource)
        assert loaded.to_config() == config
        assert loaded.content_hash() == source.content_hash()
        assert config["table_id"] == source.table_id
        assert _timestamps(loaded) == [2]

    def test_a_date_and_time_in_where_survive_the_round_trip(self, project):
        """A datetime value in ``where`` survives the config round trip."""
        keys = pa.schema([pa.field("start", pa.timestamp("us"))])
        start = dt.datetime(2026, 3, 24, 17, 54, 17)
        project.create_table("default.session", key_schema=keys).write(
            pa.table({"start": pa.array([start], pa.timestamp("us")), "n": [1]})
        )
        source = SpiralDBTableSource(_PROJECT, "session", where={"start": start})

        loaded = SpiralDBTableSource.from_config(source.to_config())

        assert loaded.as_table().num_rows == 1

    def test_bytes_in_where_survive_a_json_round_trip(self, project):
        """A bytes value in ``where`` survives saving the config as JSON."""
        keys = pa.schema([pa.field("blob", pa.binary())])
        project.create_table("default.blobs", key_schema=keys).write(
            pa.table({"blob": pa.array([b"\x00\xff", b"\x01"]), "n": [1, 2]})
        )
        source = SpiralDBTableSource(_PROJECT, "blobs", where={"blob": b"\x00\xff"})

        saved = json.loads(json.dumps(source.to_config()))
        loaded = SpiralDBTableSource.from_config(saved)

        assert loaded.to_config() == saved
        assert loaded.as_table().column("n").to_pylist() == [1]

    def test_loading_refuses_a_name_that_now_leads_to_another_table(self, project):
        """Loading refuses a name that now opens a recreated table, naming both ids."""
        config = SpiralDBTableSource(_PROJECT, "event").to_config()
        project.drop_table("default.event")
        project.create_table("default.event", key_schema=_KEYS).write(_rows([1]))
        new_id = project.table("default.event").table_id

        with pytest.raises(ValueError) as raised:
            SpiralDBTableSource.from_config(config)

        assert config["table_id"] in str(raised.value)
        assert new_id in str(raised.value)


class TestInAJob:
    """A source in a ``PipelineJob`` has each row computed once."""

    def _job(
        self, store: Path, calls: list[float | None], max_rows: int | None = 100_000
    ) -> PipelineJob:
        """Build a job that doubles ``amplitude`` over the ``event`` table.

        Args:
            store: The Delta store directory.
            calls: Receives each amplitude the pod is called with.
            max_rows: The source's row cap.
        """

        @function_pod(output_keys="doubled")
        def double(amplitude: float | None) -> float | None:
            """Return twice ``amplitude``, recording the call."""
            calls.append(amplitude)
            return None if amplitude is None else 2 * amplitude

        job = PipelineJob(name="events", store=DeltaTableDatabase(store))
        with job:
            double.pod(
                SpiralDBTableSource(_PROJECT, "event", max_rows=max_rows),
                label="double",
            )
        return job

    @pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
    def test_run_rerun_and_an_appended_row(self, project, tmp_path, orchestrator):
        """A rerun computes nothing; then an appended row is the only one computed."""

        def run(calls: list[float | None]) -> PipelineJob:
            """Build and run the job, recording the pod's calls in ``calls``."""
            job = self._job(tmp_path / "store", calls)
            job.run(orchestrator=orchestrator() if orchestrator else None)
            return job

        calls: list[float | None] = []
        first = run(calls)
        assert set(calls) == {1.5, 2.5, 3.5, 4.5, 5.5} and len(calls) == _ROWS
        assert first.nodes["double"].as_table().num_rows == _ROWS

        calls.clear()
        run(calls)
        assert calls == []

        # A new row, with a key before every other, is the only one computed.
        project.table("default.event").write(_rows([0]))
        third = run(calls)
        assert calls == [0.5]
        assert third.nodes["double"].get_all_records().num_rows == _ROWS + 1

    def test_a_job_over_the_cap_fails_before_computing(self, project, tmp_path):
        """A job whose source is over its cap fails before the pod is called."""
        calls: list[float | None] = []
        job = self._job(tmp_path / "store", calls, max_rows=3)

        with pytest.raises(SourceTooLargeError):
            job.run()

        assert calls == []


class TestNotebookPreview:
    """A source shown at the end of a notebook cell reads a head of at most 10 rows."""

    @pytest.fixture()
    def big(self, project):
        """Add a 25-row ``big`` table and clear the scan log."""
        project.create_table("default.big", key_schema=_KEYS).write(
            _rows(list(range(1, 26)))
        )
        fake_pyspiral.scan_log.clear()
        return project

    @staticmethod
    def _scan_limits() -> list[object]:
        """Return the ``limit`` of every scan opened, oldest first."""
        return [scan.kwargs.get("limit") for scan in fake_pyspiral.scan_log]

    def test_display_reads_one_head_of_ten_rows(self, big):
        """The notebook display is ``head()``: one 10-row scan, headed with the size."""
        source = SpiralDBTableSource(_PROJECT, "big", label="big")

        html = source._repr_html_()

        assert self._scan_limits() == [10]
        assert "SpiralDBTableSource[big] — first 10 rows of about 25" in html
        assert "shape: (10, 3)" in html
        assert "tags: subject_id, timestamp" in html

    def test_head_reads_one_bounded_scan(self, big):
        """``head()`` reads one scan of at most 10 rows and says the table's size."""
        source = SpiralDBTableSource(_PROJECT, "big", label="big")

        head = source.head()

        assert self._scan_limits() == [10]
        assert head.header() == "SpiralDBTableSource[big] — first 10 rows of about 25"
        assert (head.total_rows, head.total_is_estimate) == (25, True)
        assert len(head) == 10

    def test_an_unlabelled_source_is_shown_by_its_table_name(self, big):
        """With no label, the header names the table; the label itself is unchanged."""
        source = SpiralDBTableSource(_PROJECT, "big")

        assert source.head().header().startswith("SpiralDBTableSource[big] — ")
        assert source.label == "SpiralDBTableSource"

    def test_a_table_above_the_cap_previews_without_raising(self, big):
        """A source refused by its cap still previews a 10-row head."""
        source = SpiralDBTableSource(_PROJECT, "big", max_rows=20)
        with pytest.raises(SourceTooLargeError):
            source.as_table()
        fake_pyspiral.scan_log.clear()

        html = source._repr_html_()

        assert self._scan_limits() == [10]
        assert "first 10 rows of about 25" in html

    def test_a_small_table_shows_all_its_rows(self, project):
        """A table of fewer than 10 rows is shown whole, saying so."""
        head = SpiralDBTableSource(_PROJECT, "event").head()

        assert self._scan_limits() == [10]
        assert head.header().endswith(" — all 5 rows")
        assert len(head) == 5

    def test_the_sources_own_limit_bounds_the_head(self, big):
        """A ``limit`` below 10 bounds the head, shown as all rows."""
        head = SpiralDBTableSource(_PROJECT, "big", limit=3).head()

        assert self._scan_limits() == [3]
        assert head.header().endswith(" — all 3 rows")

    def test_a_limit_above_the_head_is_the_row_count_shown(self, big):
        """A ``limit`` above 10 reads 10 rows and is shown as the row count."""
        head = SpiralDBTableSource(_PROJECT, "big", limit=15).head()

        assert self._scan_limits() == [10]
        assert head.header().endswith(" — first 10 rows of about 15")

    def test_where_is_applied_and_named(self, big):
        """A ``where`` restricts the head and is named in the header."""
        head = SpiralDBTableSource(_PROJECT, "big", where={"subject_id": "m1"}).head()

        assert self._scan_limits() == [10]
        assert head.header().endswith(
            " — first 10 matching rows (where subject_id='m1'); "
            "the table has about 25 rows"
        )

    def test_the_header_is_escaped_in_the_display(self, project):
        """A ``where`` value with markup characters is shown as text, not markup."""
        project.create_table("default.marked", key_schema=_KEYS).write(
            _rows(list(range(1, 26)), subject="<b>")
        )
        source = SpiralDBTableSource(_PROJECT, "marked", where={"subject_id": "<b>"})

        html = source._repr_html_()

        assert (
            "first 10 matching rows (where subject_id='&lt;b&gt;'); "
            "the table has about 25 rows"
        ) in html
        assert "<b>'" not in html

    def test_head_with_all_info_shows_system_columns(self, big):
        """``head(all_info=True)`` shows the system tag columns, from the same scan."""
        head = SpiralDBTableSource(_PROJECT, "big").head(all_info=True)

        assert self._scan_limits() == [10]
        assert any(c.startswith("_tag_record_id::") for c in head.table.column_names)

    def test_repr_reads_nothing(self, big):
        """``repr`` is the class name and scans nothing."""
        source = SpiralDBTableSource(_PROJECT, "big")

        assert repr(source) == "SpiralDBTableSource"
        assert fake_pyspiral.scan_log == []

    def test_switched_off_preview_shows_the_schema_without_a_scan(
        self, big, monkeypatch
    ):
        # About the non-public switch: the fallback kept for the ITL-991 ruling.
        """With the preview off, ``head()`` shows the empty schema, unscanned."""
        monkeypatch.setattr(SpiralDBTableSource, "_preview_reads_rows", False)
        source = SpiralDBTableSource(_PROJECT, "big", label="big")

        head = source.head()
        html = source._repr_html_()

        assert fake_pyspiral.scan_log == []
        assert head.header() == (
            "SpiralDBTableSource[big] — rows not read; the table has about 25 rows"
        )
        assert head.table.num_rows == 0
        assert head.table.num_columns == 3
        assert "tags: subject_id, timestamp" in html
