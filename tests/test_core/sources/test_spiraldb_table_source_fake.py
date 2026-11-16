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
from orcapod.errors import NullabilityRecordError
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
    """Spiral tables arrive with their nullability and Arrow types."""

    def test_nullable_spiral_columns_are_optional(self, spiral_root):
        """Nullable Spiral columns arrive as ``T | None`` tags and data."""
        tag, data = SpiralDBTableSource(
            _PROJECT, "session", tag_only_columns=True
        ).output_schema()
        assert dict(tag) == {
            "subject_id": str | None,
            "session_start_datetime": dt.datetime | None,
        }
        assert dict(data) == {"booth": str | None, "date_of_record": dt.date | None}

    def test_arrow_types_are_kept(self, spiral_root):
        """Arrow types are kept, only normalized to their large variants."""
        table = SpiralDBTableSource(
            _PROJECT, "experiment", tag_only_columns=True
        ).as_table()
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
        """``as_df`` holds the tag and data columns only."""
        frame = SpiralDBTableSource(_PROJECT, "session", tag_only_columns=True).as_df()
        assert frame.columns == [
            "subject_id",
            "session_start_datetime",
            "booth",
            "date_of_record",
        ]

    def test_plain_typed_pod_is_refused(self, spiral_root):
        """A pod taking ``int`` is refused over a nullable column."""

        @function_pod(output_keys="n")
        def plain(total_trials: int) -> int:
            """Return ``total_trials`` unchanged."""
            return total_trials

        source = SpiralDBTableSource(_PROJECT, "experiment", tag_only_columns=True)
        selected = SelectDataColumns(["total_trials"])(source)
        with pytest.raises(ValueError, match="int \\| None"):
            plain.pod(selected)


class TestErrors:
    """A table that cannot be found is reported by name."""

    def test_missing_table_names_project_and_dataset(self, spiral_root):
        """A missing table is refused, naming the table, project and dataset."""
        with pytest.raises(ValueError) as info:
            SpiralDBTableSource(
                _PROJECT, "no_such_table", dataset="default", tag_only_columns=True
            )
        message = str(info.value)
        assert "no_such_table" in message
        assert _PROJECT in message
        assert "default" in message


class TestWorkshopPath:
    """The workshop job runs, reruns from its store, and saves and loads."""

    def _job(self, store: Path, calls: list[int]) -> PipelineJob:
        """Build the workshop job (Join, select, counting pod) over the Delta ``store``."""

        @function_pod(output_keys="accuracy")
        def counted(
            correct_trials: int | None, total_trials: int | None
        ) -> float | None:
            """Return the share of correct trials, or ``None``; counts the call."""
            calls.append(1)
            if correct_trials is None or not total_trials:
                return None
            return correct_trials / total_trials

        job = PipelineJob(name="workshop", database=DeltaTableDatabase(store))
        with job:
            joined = Join()(
                SpiralDBTableSource(_PROJECT, "session", tag_only_columns=True),
                SpiralDBTableSource(_PROJECT, "experiment", tag_only_columns=True),
                label="joined",
            )
            trials = SelectDataColumns(["correct_trials", "total_trials"])(
                joined, label="trials"
            )
            counted.pod(trials, label="accuracy")
        return job

    def test_run_rerun_save_load(self, spiral_root, tmp_path):
        """Each row is computed once, a rerun computes nothing, and the job reloads."""
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
        loaded = PipelineJob.load(path, database=DeltaTableDatabase(store))
        # Both Spiral sources were rebuilt from the saved config, not left unbound.
        assert loaded.unbound_sources == []
        records = loaded.nodes["accuracy"].get_all_records()
        assert records is not None
        assert records.num_rows == 3


def test_hashes_are_stable_across_processes(spiral_root):
    """A second Python process computes the same content and pipeline hashes."""
    source = SpiralDBTableSource(_PROJECT, "experiment", tag_only_columns=True)
    script = (
        "import json\n"
        "from tests.test_databases import fake_pyspiral\n"
        "from orcapod.sources import SpiralDBTableSource\n"
        f"with fake_pyspiral.install({str(spiral_root)!r}):\n"
        f"    s = SpiralDBTableSource({_PROJECT!r}, 'experiment', tag_only_columns=True)\n"
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
        """Return the content hash, first record id and first ``_source_sex`` token."""
        table = source.as_table(all_info=True)
        record_id = next(
            c for c in table.column_names if c.startswith("_tag_record_id")
        )
        return (
            source.content_hash().to_string(),
            table.column(record_id)[0].as_py(),
            table.column("_source_sex")[0].as_py(),
        )

    # The record id of subject "m1": the canonical JSON of its primary key (ITL-1154).
    _M1 = '{"subject_id":"m1"}'

    def test_default_source_id_is_project_dataset_table_and_table_id(
        self, two_projects
    ):
        """The default source id is the project, dataset, table and table id."""
        source = SpiralDBTableSource("edi-sample-a", "subject", tag_only_columns=True)
        expected = f"spiraldb_table:edi-sample-a.default.subject@{source.table_id}"
        assert source.source_id == expected
        assert self._identity(source)[2] == f"{expected}::{self._M1}::sex"

    def test_same_table_in_two_projects_does_not_collide(self, two_projects):
        """One table in two projects shares the pipeline hash only."""
        a = SpiralDBTableSource("edi-sample-a", "subject", tag_only_columns=True)
        b = SpiralDBTableSource("edi-sample-b", "subject", tag_only_columns=True)
        assert a.pipeline_hash() == b.pipeline_hash()  # schema only
        (hash_a, rid_a, token_a), (hash_b, rid_b, token_b) = (
            self._identity(a),
            self._identity(b),
        )
        # The record id names the row by its key; the source id tells them apart.
        assert rid_a == rid_b
        assert hash_a != hash_b
        assert token_a != token_b

    def test_unrecorded_table_hashes_are_unchanged(self, two_projects):
        # A table without a nullability record reads exactly as before ITL-936: every
        # column nullable, as SpiralDB reports it. The values were pinned at 49956a6a,
        # before the record existed, and re-recorded once for the v0.3 hash step
        # (ITL-967: starfix 0.5 and the hash label rule move every one of them).
        # ITL-186 moved the record id and the row token only (from the row's position to
        # the hash of its primary key); the token is the same with and without a
        # nullability record, because key fields are hashed as non-nullable.
        """An unrecorded table reads all-optional, with its pinned hashes."""
        source = SpiralDBTableSource(
            "edi-sample-a", "unrecorded", source_id="subject", tag_only_columns=True
        )
        assert source.output_schema() == (
            Schema({"subject_id": str | None}),
            Schema({"sex": str | None}),
        )
        assert self._identity(source) == (
            "semantic_v1:742aca7f6359844b4999e5bce29fda0769dca57e737d585e9ab4020c6dc6147c",
            self._M1,
            f"subject::{self._M1}::sex",
        )
        assert source.pipeline_hash().to_string() == (
            "semantic_v1:657ec561c0408591d01265e8e3a4e70223d1301634f8c3fc4edced2be6f34948"
        )
        row_hash = source.as_table(columns={"content_hash": True}).column(
            "_content_hash"
        )
        assert row_hash.to_pylist() == [
            "arrow_v1:000002bbde92209120af87be19987a1cfd0cb8a3aa4ab4562941c225530861c74d360e"
        ]

    def test_explicit_source_id_keeps_its_hashes(self, two_projects):
        # Values pinned on main (ebf2511b) before the default changed, re-recorded once
        # for the v0.3 hash step (ITL-967). The table was
        # written non-null; with the nullability record it reads back non-null, so the
        # hashes are those of the table as written.
        """A recorded table with a given source id keeps its pinned hashes."""
        source = SpiralDBTableSource(
            "edi-sample-a", "subject", source_id="subject", tag_only_columns=True
        )
        assert self._identity(source) == (
            "semantic_v1:6f4e8a26863d5efe7ccdc6d0859624960c9a6c6e36a0ca94ce62e48b68149dcc",
            self._M1,
            f"subject::{self._M1}::sex",
        )
        assert source.pipeline_hash().to_string() == (
            "semantic_v1:762a06a391948ba159c51df90475d609d745b9bf054e2b040f42495513b5498a"
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
                SpiralDBTableSource(_PROJECT, "subject", tag_only_columns=True)
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
        tag, data = SpiralDBTableSource(
            _PROJECT, "session", tag_only_columns=True
        ).output_schema()
        assert dict(tag) == {"subject_id": str, "session_start_datetime": dt.datetime}
        assert dict(data) == {"total_trials": int, "weight": float, "booth": str | None}

    def test_unrecorded_table_is_all_optional(self, strict_root):
        """The same rows without a record arrive as T | None."""
        tag, data = SpiralDBTableSource(
            _PROJECT, "session_unrecorded", tag_only_columns=True
        ).output_schema()
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
        schema = (
            SpiralDBTableSource(_PROJECT, "unit", tag_only_columns=True)
            .as_table()
            .schema
        )
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
            """Return half of ``weight``."""
            return weight / 2

        source = SpiralDBTableSource(
            _PROJECT, "session_unrecorded", tag_only_columns=True
        )
        with pytest.raises(ValueError, match="float \\| None"):
            halve.pod(SelectDataColumns(["weight"])(source))

    def test_join_of_recorded_and_unrecorded_keys_gives_the_recorded_type(
        self, strict_root
    ):
        """A T key joined with a T | None key is accepted and comes out T (ITL-829).

        A null key matches nothing, so no joined row can hold a null there.
        """
        unit = SpiralDBTableSource(_PROJECT, "unit", tag_only_columns=True)
        unrecorded = SpiralDBTableSource(
            _PROJECT, "session_unrecorded", tag_only_columns=True
        )
        assert unrecorded.output_schema()[0]["subject_id"] == str | None
        joined = Join()(unit, unrecorded)
        tag_schema, _ = joined.output_schema()
        assert tag_schema["subject_id"] is str
        assert not joined.as_table().schema.field("subject_id").nullable

    def test_plain_typed_pod_runs_through_join_and_save_load(
        self, strict_root, tmp_path
    ):
        """Plain-typed pods run over recorded tables through Join, a store and save/load."""

        @function_pod(output_keys="score")
        def score(total_trials: int, weight: float) -> float:
            """Return ``total_trials`` times ``weight``."""
            return total_trials * weight

        store = tmp_path / "store"
        job = PipelineJob(name="strict", database=DeltaTableDatabase(store))
        with job:
            joined = Join()(
                SpiralDBTableSource(_PROJECT, "session", tag_only_columns=True),
                SpiralDBTableSource(_PROJECT, "unit", tag_only_columns=True),
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
        loaded = PipelineJob.load(path, database=DeltaTableDatabase(store))
        assert loaded.unbound_sources == []
        assert (
            loaded.nodes["score"].pipeline_hash() == job.nodes["score"].pipeline_hash()
        )
        records = loaded.nodes["score"].get_all_records()
        assert records is not None
        assert sorted(records.column("score").to_pylist()) == [25.0, 80.0]
