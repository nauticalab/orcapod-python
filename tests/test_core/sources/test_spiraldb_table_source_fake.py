"""SpiralDBTableSource end to end against the disk-backed pyspiral fake (no live SpiralDB).

Covers the workshop path (ITL-926) through the public interface: EDI-shaped tables (every
column nullable, composite keys with string, timestamp and uint32 columns) -> Join of two
Spiral sources -> function pod -> PipelineJob with a Delta store -> re-run -> save and load.
See ``tests/test_databases/fake_pyspiral.py`` for the storage behaviour modelled.
"""

from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InputValidationError, NullabilityRecordError
from orcapod.operators import Join, SelectDataColumns
from orcapod.databases import SpiralDBConnector
from orcapod.sources import SpiralDBTableSource
from orcapod.types import Schema
from tests.test_databases import fake_pyspiral

_PROJECT = "edi-sample"
_TS = pa.timestamp("us")
_SESSION_KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("session_start_datetime", _TS)]
)
_EXPERIMENT_KEYS = pa.schema(
    [*_SESSION_KEYS, pa.field("experiment_index", pa.uint32())]
)
_STARTS = [dt.datetime(2026, 1, 1, 9), dt.datetime(2026, 1, 2, 9)]


def _all_nullable(table: pa.Table) -> pa.Table:
    """Mark every column nullable, as the EDI sample tables are."""
    return table.cast(pa.schema([pa.field(f.name, f.type, True) for f in table.schema]))


@pytest.fixture()
def spiral_root(tmp_path: Path):
    """Provide a fake root with EDI-shaped, all-nullable ``session`` and ``experiment``."""
    root = tmp_path / "spiral"
    with fake_pyspiral.install(root):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        session = project.create_table("default.session", key_schema=_SESSION_KEYS)
        session.write(
            _all_nullable(
                pa.table(
                    {
                        "subject_id": ["m1", "m2"],
                        "session_start_datetime": pa.array(_STARTS, _TS),
                        "booth": ["b1", None],
                        "date_of_record": pa.array([dt.date(2026, 1, 1), None]),
                    }
                )
            )
        )
        experiment = project.create_table(
            "default.experiment", key_schema=_EXPERIMENT_KEYS
        )
        experiment.write(
            _all_nullable(
                pa.table(
                    {
                        "subject_id": ["m1", "m1", "m2"],
                        "session_start_datetime": pa.array(
                            [_STARTS[0], _STARTS[0], _STARTS[1]], _TS
                        ),
                        "experiment_index": pa.array([0, 1, 0], pa.uint32()),
                        "correct_trials": pa.array([5, 6, 7], pa.int64()),
                        "total_trials": pa.array([10, 10, None], pa.int64()),
                        "stop_timestamp": pa.array([2**63 + 1, 2, 3], pa.uint64()),
                        "eye_params": pa.array(
                            [[1.0], [], [2.0, 3.0]], pa.list_(pa.float32())
                        ),
                    }
                )
            )
        )
        yield root


class TestSchema:
    def test_nullable_spiral_columns_are_optional(self, spiral_root):
        tag, data = SpiralDBTableSource(_PROJECT, "session").output_schema()
        assert dict(tag) == {
            "subject_id": str | None,
            "session_start_datetime": dt.datetime | None,
        }
        assert dict(data) == {"booth": str | None, "date_of_record": dt.date | None}

    def test_arrow_types_are_kept(self, spiral_root):
        table = SpiralDBTableSource(_PROJECT, "experiment").as_table()
        assert {f.name: f.type for f in table.schema} == {
            "subject_id": pa.large_string(),
            "session_start_datetime": _TS,
            "experiment_index": pa.uint32(),
            "correct_trials": pa.int64(),
            "total_trials": pa.int64(),
            "stop_timestamp": pa.uint64(),
            "eye_params": pa.large_list(pa.float32()),
        }

    def test_dataframe_view_has_tags_and_data_only(self, spiral_root):
        frame = SpiralDBTableSource(_PROJECT, "session").as_df()
        assert frame.columns == [
            "subject_id",
            "session_start_datetime",
            "booth",
            "date_of_record",
        ]

    def test_plain_typed_pod_is_refused(self, spiral_root):
        @function_pod(output_keys="n")
        def plain(total_trials: int) -> int:
            return total_trials

        source = SpiralDBTableSource(_PROJECT, "experiment")
        selected = SelectDataColumns(["total_trials"])(source)
        with pytest.raises(ValueError, match="int \\| None"):
            plain.pod(selected)


class TestErrors:
    def test_missing_table_names_project_and_dataset(self, spiral_root):
        with pytest.raises(ValueError) as info:
            SpiralDBTableSource(_PROJECT, "no_such_table", dataset="default")
        message = str(info.value)
        assert "no_such_table" in message
        assert _PROJECT in message
        assert "default" in message


class TestWorkshopPath:
    def _job(self, store: Path, calls: list[int]) -> PipelineJob:
        """Build the workshop job (Join, select, counting pod) over the Delta ``store``."""

        @function_pod(output_keys="accuracy")
        def counted(
            correct_trials: int | None, total_trials: int | None
        ) -> float | None:
            calls.append(1)
            if correct_trials is None or not total_trials:
                return None
            return correct_trials / total_trials

        job = PipelineJob(name="workshop", store=DeltaTableDatabase(store))
        with job:
            joined = Join()(
                SpiralDBTableSource(_PROJECT, "session"),
                SpiralDBTableSource(_PROJECT, "experiment"),
                label="joined",
            )
            trials = SelectDataColumns(["correct_trials", "total_trials"])(
                joined, label="trials"
            )
            counted.pod(trials, label="accuracy")
        return job

    def test_run_rerun_save_load(self, spiral_root, tmp_path):
        store = tmp_path / "store"
        calls: list[int] = []
        job = self._job(store, calls)
        job.run()
        rows = job.nodes["accuracy"].as_table().to_pylist()
        assert sorted((r["subject_id"], r["experiment_index"]) for r in rows) == [
            ("m1", 0),
            ("m1", 1),
            ("m2", 0),
        ]
        assert sorted((r["accuracy"] for r in rows), key=lambda v: (v is None, v)) == [
            0.5,
            0.6,
            None,
        ]
        assert len(calls) == 3

        calls.clear()
        rerun = self._job(store, calls)
        rerun.run()
        assert calls == []

        path = tmp_path / "job.json"
        rerun.save(path)
        loaded = PipelineJob.load(path, store=DeltaTableDatabase(store))
        # Both Spiral sources were rebuilt from the saved config, not left unbound.
        assert loaded.unbound_sources == []
        records = loaded.nodes["accuracy"].get_all_records()
        assert records is not None
        assert records.num_rows == 3


def test_hashes_are_stable_across_processes(spiral_root):
    source = SpiralDBTableSource(_PROJECT, "experiment")
    script = (
        "import json\n"
        "from tests.test_databases import fake_pyspiral\n"
        "from orcapod.sources import SpiralDBTableSource\n"
        f"with fake_pyspiral.install({str(spiral_root)!r}):\n"
        f"    s = SpiralDBTableSource({_PROJECT!r}, 'experiment')\n"
        "print(json.dumps([s.content_hash().to_string(), s.pipeline_hash().to_string()]))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
        cwd=Path(__file__).resolve().parents[3],
    )
    other = json.loads(result.stdout.strip().splitlines()[-1])
    assert other == [
        source.content_hash().to_string(),
        source.pipeline_hash().to_string(),
    ]


class TestSourceId:
    """The default source_id is the fully qualified table (ITL-926, developer decision)."""

    _SUBJECT = pa.schema(
        [
            pa.field("subject_id", pa.string(), False),
            pa.field("sex", pa.string(), False),
        ]
    )

    @pytest.fixture()
    def two_projects(self, tmp_path: Path):
        """Provide two fake projects holding the same one-row ``subject`` table.

        Each is written through ``SpiralDBConnector``, which records its non-nullable
        columns (ITL-936); ``default.unrecorded`` holds the same row without a record.
        """
        with fake_pyspiral.install(tmp_path / "spiral"):
            rows = pa.table({"subject_id": ["m1"], "sex": ["F"]}, schema=self._SUBJECT)
            keys = pa.schema([pa.field("subject_id", pa.string(), False)])
            for project in ("edi-sample-a", "edi-sample-b"):
                fake = fake_pyspiral.FakeSpiral().project(project)
                fake.create_table("default.subject", key_schema=keys)
                SpiralDBConnector(project).upsert_records(
                    "subject", rows, id_column="subject_id"
                )
                fake.create_table("default.unrecorded", key_schema=keys).write(rows)
            yield

    @staticmethod
    def _identity(source: SpiralDBTableSource) -> tuple[str, str, str]:
        table = source.as_table(all_info=True)
        record_id = next(
            c for c in table.column_names if c.startswith("_tag_record_id")
        )
        return (
            source.content_hash().to_string(),
            table.column(record_id)[0].as_py().hex(),
            table.column("_source_sex")[0].as_py(),
        )

    def test_default_source_id_is_project_dataset_table(self, two_projects):
        source = SpiralDBTableSource("edi-sample-a", "subject")
        assert source.source_id == "edi-sample-a/default/subject"
        assert self._identity(source)[2] == "edi-sample-a/default/subject::row_0::sex"

    def test_same_table_in_two_projects_does_not_collide(self, two_projects):
        a = SpiralDBTableSource("edi-sample-a", "subject")
        b = SpiralDBTableSource("edi-sample-b", "subject")
        assert a.pipeline_hash() == b.pipeline_hash()  # schema only
        identity_a, identity_b = self._identity(a), self._identity(b)
        assert all(x != y for x, y in zip(identity_a, identity_b, strict=True))

    def test_unrecorded_table_hashes_are_unchanged(self, two_projects):
        # A table without a nullability record reads exactly as before ITL-936: every
        # column nullable, as SpiralDB reports it. Values pinned at 49956a6a, before the
        # record existed.
        source = SpiralDBTableSource("edi-sample-a", "unrecorded", source_id="subject")
        assert source.output_schema() == (
            Schema({"subject_id": str | None}),
            Schema({"sex": str | None}),
        )
        assert self._identity(source) == (
            "semantic_v0.1:0380725acb9073df747380b348f72a8a66bbe52f02e46fb3df795e7518e7f4f6",
            "18d1efa53fbc52b6a40f406eca35ca81",
            "subject::row_0::sex",
        )
        assert source.pipeline_hash().to_string() == (
            "semantic_v0.1:61796d9cdbd5afce45b2b99b1efe306a745ee63bdf4952e841772a04c450498a"
        )
        row_hash = source.as_table(columns={"content_hash": True}).column(
            "_content_hash"
        )
        assert row_hash.to_pylist() == [
            "arrow_v0.1:0000017a393c2f094ebfb6ecb72d0a0d5ef088243f38b709695c846dbd4a17b095caf9"
        ]

    def test_explicit_source_id_keeps_its_hashes(self, two_projects):
        # Values pinned on main (ebf2511b) before the default changed. The table was
        # written non-null; with the nullability record it reads back non-null, so the
        # hashes are those of the table as written.
        source = SpiralDBTableSource("edi-sample-a", "subject", source_id="subject")
        assert self._identity(source) == (
            "semantic_v0.1:1f4ecfe64566d1684d7cdf1319f0274a7048e0124f8026c815814de7d0e365d3",
            "18d1efa53fbc52b6a40f406eca35ca81",
            "subject::row_0::sex",
        )
        assert source.pipeline_hash().to_string() == (
            "semantic_v0.1:a771eabb35eac3fbd266b5751c75c6664556a2fab5d0d31977cefedc4b870a3d"
        )


class TestNullabilityErrors:
    """The source keeps the nullability error types (ITL-936)."""

    def test_unreadable_record_raises_its_own_type(self, tmp_path: Path):
        """A broken nullability record surfaces as NullabilityRecordError, naming the table."""
        with fake_pyspiral.install(tmp_path / "spiral"):
            project = fake_pyspiral.FakeSpiral().project(_PROJECT)
            keys = pa.schema([pa.field("subject_id", pa.string(), False)])
            project.create_table("default.subject", key_schema=keys)
            SpiralDBConnector(_PROJECT).upsert_records(
                "subject",
                pa.table({"subject_id": ["m1"]}, schema=keys),
                id_column="subject_id",
            )
            table = project.table("default.subject")
            header = json.loads(table.get_metadata()["__orcapod_nullability__"])
            header["version"] = 99
            table.set_metadata({"__orcapod_nullability__": json.dumps(header).encode()})
            with pytest.raises(NullabilityRecordError) as info:
                SpiralDBTableSource(_PROJECT, "subject")
            assert info.value.table == f"{_PROJECT}/default/subject"


class TestNonNullableColumns:
    """A column written non-null arrives as ``T``, not ``T | None`` (ITL-926 C1, ITL-936).

    SpiralDB reports every column and struct field nullable, so this holds only for tables
    that carry Orcapod's nullability record: here, tables written through
    ``SpiralDBConnector``. The same rows written without the record read as ``T | None``.
    """

    _KEYS = pa.schema(
        [
            pa.field("subject_id", pa.string(), False),
            pa.field("session_start_datetime", _TS, False),
        ]
    )
    _SESSION = pa.schema(
        [
            *_KEYS,
            pa.field("total_trials", pa.int64(), False),
            pa.field("weight", pa.float64(), False),
            pa.field("booth", pa.string(), True),
        ]
    )
    _ITEM = pa.field("item", pa.float32(), False)
    _QUALITY = pa.struct(
        [pa.field("snr", pa.float32(), False), pa.field("note", pa.string(), True)]
    )

    @pytest.fixture()
    def strict_root(self, tmp_path: Path):
        """Provide recorded ``session`` and ``unit`` tables and an unrecorded copy of session."""
        root = tmp_path / "spiral"
        with fake_pyspiral.install(root):
            project = fake_pyspiral.FakeSpiral().project(_PROJECT)
            connector = SpiralDBConnector(_PROJECT)
            session_rows = pa.table(
                {
                    "subject_id": ["m1", "m2"],
                    "session_start_datetime": pa.array(_STARTS, _TS),
                    "total_trials": pa.array([10, 20], pa.int64()),
                    "weight": pa.array([2.5, 4.0], pa.float64()),
                    "booth": ["b1", None],
                },
                schema=self._SESSION,
            )
            project.create_table("default.session", key_schema=self._KEYS)
            connector.upsert_records("session", session_rows, id_column="subject_id")
            project.create_table(
                "default.session_unrecorded", key_schema=self._KEYS
            ).write(session_rows)
            project.create_table("default.unit", key_schema=self._KEYS)
            connector.upsert_records(
                "unit",
                pa.table(
                    {
                        "subject_id": ["m1", "m2"],
                        "session_start_datetime": pa.array(_STARTS, _TS),
                        "spikes": pa.array([[1.0], [2.0, 3.0]], pa.list_(self._ITEM)),
                        "quality": pa.array(
                            [{"snr": 1.0, "note": None}, {"snr": 2.0, "note": "ok"}],
                            self._QUALITY,
                        ),
                    },
                    schema=pa.schema(
                        [
                            *self._KEYS,
                            pa.field("spikes", pa.list_(self._ITEM), False),
                            pa.field("quality", self._QUALITY, False),
                        ]
                    ),
                ),
                id_column="subject_id",
            )
            yield root

    def test_schema_keeps_declared_nullability(self, strict_root):
        """A recorded table's non-null columns arrive as T."""
        tag, data = SpiralDBTableSource(_PROJECT, "session").output_schema()
        assert dict(tag) == {"subject_id": str, "session_start_datetime": dt.datetime}
        assert dict(data) == {"total_trials": int, "weight": float, "booth": str | None}

    def test_unrecorded_table_is_all_optional(self, strict_root):
        """The same rows without a record arrive as T | None."""
        tag, data = SpiralDBTableSource(_PROJECT, "session_unrecorded").output_schema()
        assert dict(tag) == {
            "subject_id": str | None,
            "session_start_datetime": dt.datetime | None,
        }
        assert dict(data) == {
            "total_trials": int | None,
            "weight": float | None,
            "booth": str | None,
        }

    def test_arrow_fields_keep_nullability_at_every_depth(self, strict_root):
        """Struct fields and list items keep their recorded nullability."""
        schema = SpiralDBTableSource(_PROJECT, "unit").as_table().schema
        assert not schema.field("subject_id").nullable
        assert not schema.field("spikes").nullable
        assert schema.field("spikes").type == pa.large_list(self._ITEM)
        assert not schema.field("spikes").type.value_field.nullable
        quality = schema.field("quality")
        assert not quality.nullable
        assert not quality.type.field("snr").nullable
        assert quality.type.field("note").nullable

    def test_plain_float_pod_is_refused_on_an_unrecorded_table(self, strict_root):
        """A pod taking x: float refuses an unrecorded column."""

        @function_pod(output_keys="half")
        def halve(weight: float) -> float:
            return weight / 2

        source = SpiralDBTableSource(_PROJECT, "session_unrecorded")
        with pytest.raises(ValueError, match="float \\| None"):
            halve.pod(SelectDataColumns(["weight"])(source))

    def test_join_of_recorded_and_unrecorded_keys_is_refused(self, strict_root):
        # Type strictness: a key that is T on one side and T | None on the other.
        """A T key joined with a T | None key is refused."""
        with pytest.raises(InputValidationError, match="str \\| None"):
            Join()(
                SpiralDBTableSource(_PROJECT, "unit"),
                SpiralDBTableSource(_PROJECT, "session_unrecorded"),
            )

    def test_plain_typed_pod_runs_through_join_and_save_load(
        self, strict_root, tmp_path
    ):
        """Plain-typed pods run over recorded tables through Join, a store and save/load."""

        @function_pod(output_keys="score")
        def score(total_trials: int, weight: float) -> float:
            return total_trials * weight

        store = tmp_path / "store"
        job = PipelineJob(name="strict", store=DeltaTableDatabase(store))
        with job:
            joined = Join()(
                SpiralDBTableSource(_PROJECT, "session"),
                SpiralDBTableSource(_PROJECT, "unit"),
                label="joined",
            )
            trials = SelectDataColumns(["total_trials", "weight"])(
                joined, label="trials"
            )
            score.pod(trials, label="score")
        job.run()
        result = job.nodes["score"].as_table()
        assert sorted(result.column("score").to_pylist()) == [25.0, 80.0]
        assert not result.schema.field("subject_id").nullable
        assert job.nodes["joined"].output_schema()[0]["subject_id"] is str

        path = tmp_path / "job.json"
        job.save(path)
        loaded = PipelineJob.load(path, store=DeltaTableDatabase(store))
        assert loaded.unbound_sources == []
        assert (
            loaded.nodes["score"].pipeline_hash() == job.nodes["score"].pipeline_hash()
        )
        records = loaded.nodes["score"].get_all_records()
        assert records is not None
        assert sorted(records.column("score").to_pylist()) == [25.0, 80.0]
