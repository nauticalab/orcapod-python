"""Tests for ``DerivedSource(origin, tags_as_data=...)`` (ITL-939).

A derived source built with ``tags_as_data`` is a new root source over another source or a
job node. Its tags are the origin's tags; its data columns are the origin's data columns plus
a copy of each mapped tag column under the given name. It has source provenance of its own.

Fixtures are shaped like the EDI ``session`` and ``experiment`` tables, where
``session_start_datetime`` is a key that a pod also needs as an input for time arithmetic.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from contextlib import closing
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import DerivedSource, RootSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.operators import Join
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.types import ColumnConfig
from orcapod.sources import ArrowTableSource, DictSource, SQLiteTableSource

UTC = dt.timezone.utc
TS = pa.timestamp("us", tz="UTC")
S1 = dt.datetime(2026, 1, 5, 9, 0, tzinfo=UTC)
S2 = dt.datetime(2026, 1, 6, 9, 30, tzinfo=UTC)
S3 = dt.datetime(2026, 1, 5, 14, 0, tzinfo=UTC)
STARTS = [S1, S1, S2, S2, S3]
ONSETS_S = [60.0, 900.0, 120.0, 1500.0, 30.0]
EXPERIMENT_KEYS = ["subject_id", "session_start_datetime", "experiment_index"]
SESSION_KEYS = ["subject_id", "session_start_datetime"]
COPY = {"session_start_datetime": "session_start"}


def _micros(t: dt.datetime) -> int:
    """Return ``t`` as microseconds since the epoch."""
    return int(t.timestamp() * 1_000_000)


def _experiment_table(*, nullable_keys: bool = False) -> pa.Table:
    """Return the EDI-shaped ``experiment`` table (two experiments per session)."""
    schema = pa.schema(
        [
            pa.field("subject_id", pa.large_string(), nullable=nullable_keys),
            pa.field("session_start_datetime", TS, nullable=nullable_keys),
            pa.field("experiment_index", pa.uint32(), nullable=nullable_keys),
            pa.field("experiment_start_timestamp", pa.uint64(), nullable=False),
            pa.field("task_id", pa.large_string(), nullable=False),
        ]
    )
    return pa.table(
        {
            "subject_id": ["m1", "m1", "m1", "m1", "m2"],
            "session_start_datetime": STARTS,
            "experiment_index": [0, 1, 0, 1, 0],
            "experiment_start_timestamp": [
                _micros(s) + int(o * 1_000_000) for s, o in zip(STARTS, ONSETS_S)
            ],
            "task_id": ["fv", "fv", "fv", "gz", "fv"],
        },
        schema=schema,
    )


def _session_table() -> pa.Table:
    """Return the EDI-shaped ``session`` table (subject ``m1`` has two sessions)."""
    schema = pa.schema(
        [
            pa.field("subject_id", pa.large_string(), nullable=False),
            pa.field("session_start_datetime", TS, nullable=False),
            pa.field("booth", pa.large_string(), nullable=False),
        ]
    )
    return pa.table(
        {
            "subject_id": ["m1", "m1", "m2"],
            "session_start_datetime": [S1, S2, S3],
            "booth": ["A", "B", "A"],
        },
        schema=schema,
    )


def _experiment(source_id: str = "experiment", **kwargs) -> ArrowTableSource:
    """Return the ``experiment`` table as a source with its keys as tags."""
    return ArrowTableSource(
        _experiment_table(**kwargs),
        tag_columns=EXPERIMENT_KEYS,
        tag_only_columns=True,
        source_id=source_id,
    )


def _session() -> ArrowTableSource:
    """Return the ``session`` table as a source with its keys as tags."""
    return ArrowTableSource(
        _session_table(),
        tag_columns=SESSION_KEYS,
        tag_only_columns=True,
        source_id="session",
    )


def _prefixed(table: pa.Table, prefix: str) -> list[str]:
    """Return the names of the table's columns that start with ``prefix``."""
    return [c for c in table.column_names if c.startswith(prefix)]


@function_pod(output_keys="onset_s")
def onset_s(session_start: dt.datetime, experiment_start_timestamp: int) -> float:
    """Return seconds from the session start to the experiment start."""
    return experiment_start_timestamp / 1e6 - session_start.timestamp()


@function_pod(output_keys="n")
def name_length(name_copy: str) -> int:
    """Return the length of the copied key."""
    return len(name_copy)


# ---------------------------------------------------------------------------
# Columns, types and values
# ---------------------------------------------------------------------------


class TestColumns:
    def test_is_a_root_source_with_no_upstreams(self):
        derived = DerivedSource(_experiment(), tags_as_data=COPY)
        assert isinstance(derived, RootSource)
        assert derived.upstreams == ()
        assert derived.producer is None

    def test_tags_are_the_origins_and_copy_is_appended_to_data(self):
        origin = _experiment()
        derived = DerivedSource(origin, tags_as_data=COPY)
        assert derived.keys() == (
            tuple(EXPERIMENT_KEYS),
            ("experiment_start_timestamp", "task_id", "session_start"),
        )
        tag_schema, data_schema = derived.output_schema()
        assert dict(tag_schema) == dict(origin.output_schema()[0])
        assert dict(data_schema) == {
            **dict(origin.output_schema()[1]),
            "session_start": dt.datetime,
        }

    def test_copy_values_equal_the_tag_values(self):
        table = DerivedSource(_experiment(), tags_as_data=COPY).as_table()
        assert table["session_start"].equals(table["session_start_datetime"])
        assert table.num_rows == 5

    def test_iter_data_carries_the_value_in_tag_and_data(self):
        derived = DerivedSource(_experiment(), tags_as_data=COPY)
        pairs = list(derived.iter_data())
        assert len(pairs) == 5
        for tag, data in pairs:
            assert data["session_start"] == tag["session_start_datetime"]

    def test_several_copies_follow_map_order(self):
        derived = DerivedSource(
            _experiment(),
            tags_as_data={"experiment_index": "idx", "subject_id": "subject"},
        )
        assert derived.keys()[1] == (
            "experiment_start_timestamp",
            "task_id",
            "idx",
            "subject",
        )
        table = derived.as_table()
        assert table.schema.field("idx").type == pa.uint32()
        assert table["subject"].to_pylist() == ["m1", "m1", "m1", "m1", "m2"]

    def test_origin_is_not_changed(self):
        origin = _experiment()
        before = (
            origin.content_hash(),
            origin.pipeline_hash(),
            origin.keys(),
            origin.as_table(all_info=True),
        )
        DerivedSource(origin, tags_as_data=COPY).as_table()
        assert origin.content_hash() == before[0]
        assert origin.pipeline_hash() == before[1]
        assert origin.keys() == before[2]
        assert origin.as_table(all_info=True).equals(before[3])


class TestArrowTypesPreserved:
    def test_not_null_tag_gives_not_null_copy_of_same_type(self):
        origin = _experiment()
        table = DerivedSource(origin, tags_as_data=COPY).as_table()
        tag_field = origin.as_table().schema.field("session_start_datetime")
        copy_field = table.schema.field("session_start")
        assert copy_field.type == tag_field.type == TS
        assert copy_field.nullable is False

    def test_nullable_tag_gives_nullable_copy(self):
        derived = DerivedSource(_experiment(nullable_keys=True), tags_as_data=COPY)
        assert derived.as_table().schema.field("session_start").nullable is True
        assert derived.output_schema()[1]["session_start"] == dt.datetime | None

    def test_narrow_integer_width_is_kept(self):
        derived = DerivedSource(_experiment(), tags_as_data={"experiment_index": "idx"})
        assert derived.as_table().schema.field("idx").type == pa.uint32()

    def test_logical_type_is_kept(self):
        origin = DictSource(
            data=[
                {"file": Path("/data/a.bin"), "size": 1},
                {"file": Path("/data/b.bin"), "size": 2},
            ],
            tag_columns=["file"],
            tag_only_columns=True,
            source_id="files",
        )
        derived = DerivedSource(origin, tags_as_data={"file": "file_path"})
        table = derived.as_table()
        assert (
            table.schema.field("file_path").type
            == origin.as_table().schema.field("file").type
        )
        assert derived.output_schema()[1]["file_path"] is Path
        assert [d["file_path"] for _, d in derived.iter_data()] == [
            Path("/data/a.bin"),
            Path("/data/b.bin"),
        ]

    def test_field_metadata_is_kept(self):
        schema = pa.schema(
            [
                pa.field("k", pa.int8(), nullable=False, metadata={b"unit": b"s"}),
                pa.field("x", pa.int64(), nullable=False),
            ]
        )
        origin = ArrowTableSource(
            pa.table({"k": [1, 2], "x": [10, 20]}, schema=schema),
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="meta",
        )
        table = DerivedSource(origin, tags_as_data={"k": "k_copy"}).as_table()
        copy_field = table.schema.field("k_copy")
        assert copy_field.type == pa.int8()
        assert copy_field.metadata == origin.as_table().schema.field("k").metadata


# ---------------------------------------------------------------------------
# Provenance and identity
# ---------------------------------------------------------------------------


def _expected_default_source_id(origin, tags_as_data, record_id_column=None) -> str:
    """The documented definition: ``tags_as_data:`` plus the labelled derivation hash.

    The derivation hash is the content hash an operator node doing the derivation
    would have.
    """
    hasher = origin.data_context.semantic_hasher
    parameters = ("DerivedSource", tuple(sorted(tags_as_data.items())))
    if record_id_column is not None:
        parameters += (("record_id_column", record_id_column),)
    derivation_hash = hasher.hash_object(
        (parameters, (origin,)),
        resolver=lambda obj: obj.content_hash(hasher),
    )
    return f"tags_as_data:{derivation_hash.to_string()}"


class TestProvenance:
    def test_default_source_id_is_kind_label_and_derivation_hash(self):
        origin = _experiment()
        derived = DerivedSource(origin, tags_as_data=COPY)
        assert derived.source_id == _expected_default_source_id(origin, COPY)
        kind, label, digest = derived.source_id.split(":")
        assert kind == DerivedSource.TAGS_AS_DATA_KIND == "tags_as_data"
        # The config key names the class to rebuild, not the operation.
        assert derived.to_config()["source_type"] == DerivedSource.SOURCE_KIND
        assert DerivedSource.SOURCE_KIND == "derived"
        assert label == origin.data_context.semantic_hasher.hasher_id
        assert len(digest) == 64

    def test_default_source_id_is_used_as_is_in_values_and_tokens(self):
        derived = DerivedSource(_experiment(), tags_as_data=COPY)
        table = derived.as_table(all_info=True)
        (sid_col,) = _prefixed(table, "_tag_source_id")
        assert set(table[sid_col].to_pylist()) == {derived.source_id}
        token = table["_source_session_start"][0].as_py()
        assert token.split("::") == [derived.source_id, "row_0", "session_start"]

    def test_default_source_id_follows_origin_and_mapping_only(self):
        base = DerivedSource(_experiment(), tags_as_data=COPY).source_id
        assert DerivedSource(_experiment(), tags_as_data=COPY).source_id == base
        assert (
            DerivedSource(
                _experiment(), tags_as_data={"session_start_datetime": "t0"}
            ).source_id
            != base
        )
        assert (
            DerivedSource(_experiment("experiment2"), tags_as_data=COPY).source_id
            != base
        )

    def test_record_id_column_enters_the_default_source_id(self):
        origin = _experiment()
        by_position = DerivedSource(origin, tags_as_data=COPY)
        by_value = DerivedSource(
            origin, tags_as_data=COPY, record_id_column="experiment_start_timestamp"
        )
        assert by_value.source_id == _expected_default_source_id(
            origin, COPY, "experiment_start_timestamp"
        )
        assert by_value.source_id != by_position.source_id
        assert by_value.content_hash() != by_position.content_hash()
        assert by_value.pipeline_hash() == by_position.pipeline_hash()

    def test_explicit_source_id_replaces_the_default(self):
        derived = DerivedSource(_experiment(), tags_as_data=COPY, source_id="exp_t0")
        assert derived.source_id == "exp_t0"
        table = derived.as_table(all_info=True)
        (sid_col,) = _prefixed(table, "_tag_source_id")
        assert set(table[sid_col].to_pylist()) == {"exp_t0"}
        assert table["_source_session_start"][0].as_py().startswith("exp_t0::")

    def test_system_tags_are_its_own(self):
        origin = _experiment()
        derived = DerivedSource(origin, tags_as_data=COPY)
        o_table = origin.as_table(all_info=True)
        d_table = derived.as_table(all_info=True)
        o_cols = _prefixed(o_table, "_tag_")
        d_cols = _prefixed(d_table, "_tag_")
        assert len(d_cols) == 2
        assert set(d_cols).isdisjoint(o_cols)
        (sid_col,) = _prefixed(d_table, "_tag_source_id")
        assert set(d_table[sid_col].to_pylist()) == {derived.source_id}
        (rid_col,) = _prefixed(d_table, "_tag_record_id")
        assert len(set(d_table[rid_col].to_pylist())) == 5

    def test_every_data_column_has_a_token_naming_the_derived_source(self):
        derived = DerivedSource(_experiment(), tags_as_data=COPY)
        table = derived.as_table(all_info=True)
        assert set(_prefixed(table, "_source_")) == {
            "_source_experiment_start_timestamp",
            "_source_task_id",
            "_source_session_start",
        }
        assert (
            table["_source_session_start"][0].as_py()
            == f"{derived.source_id}::row_0::session_start"
        )
        assert (
            table["_source_task_id"][4].as_py()
            == f"{derived.source_id}::row_4::task_id"
        )

    def test_record_id_column_gives_value_based_tokens(self):
        derived = DerivedSource(
            _experiment(),
            tags_as_data=COPY,
            source_id="exp",
            record_id_column="experiment_start_timestamp",
        )
        table = derived.as_table(all_info=True)
        first = table["experiment_start_timestamp"][0].as_py()
        assert (
            table["_source_task_id"][0].as_py()
            == f"exp::experiment_start_timestamp={first}::task_id"
        )

    def test_equals_the_hand_written_recipe_with_the_same_source_id(self):
        raw = _experiment_table()
        raw = raw.append_column(
            raw.schema.field("session_start_datetime").with_name("session_start"),
            raw["session_start_datetime"],
        )
        recipe = ArrowTableSource(
            raw, tag_columns=EXPERIMENT_KEYS, tag_only_columns=True, source_id="exp"
        )
        derived = DerivedSource(_experiment(), tags_as_data=COPY, source_id="exp")
        r_table = recipe.as_table(columns={"source": True, "system_tags": True})
        d_table = derived.as_table(columns={"source": True, "system_tags": True})
        assert d_table.column_names == r_table.column_names
        assert d_table.equals(r_table)
        assert derived.pipeline_hash() == recipe.pipeline_hash()


class TestIdentity:
    def test_pipeline_hash_is_schema_only(self):
        a = DerivedSource(_experiment("a"), tags_as_data=COPY)
        b = DerivedSource(_experiment("b"), tags_as_data=COPY)
        assert a.pipeline_hash() == b.pipeline_hash()
        assert a.pipeline_hash() != _experiment("a").pipeline_hash()

    def test_content_hash_follows_origin_and_mapping(self):
        base = DerivedSource(_experiment(), tags_as_data=COPY)
        assert (
            DerivedSource(_experiment(), tags_as_data=COPY).content_hash()
            == base.content_hash()
        )
        assert (
            DerivedSource(_experiment("other"), tags_as_data=COPY).content_hash()
            != base.content_hash()
        )
        assert base.content_hash() != _experiment().content_hash()

    def test_content_hash_is_the_root_source_rule_plus_the_mapping(self):
        """The identity structure is the root source rule followed by the sorted mapping."""
        derived = DerivedSource(_experiment(), tags_as_data=COPY)
        # identity_structure is the non-public entry point this test is about: the
        # structure is the root source rule followed by the sorted mapping items.
        assert derived.identity_structure() == (
            "DerivedSource",
            derived.output_schema(),
            derived.source_id,
            ("tags_as_data", tuple(sorted(COPY.items()))),
        )

    def test_mappings_with_one_schema_have_different_content_hashes(self):
        """Two mappings with one schema and one source_id still differ in content hash (ITL-1000)."""
        # ITL-1000: two tag columns of one type copied to one name give one schema
        # but different values, so with one explicit source_id the mapping must tell
        # the sources apart.
        table = pa.table(
            {
                "a": pa.array([1, 2], pa.int64()),
                "b": pa.array([3, 4], pa.int64()),
                "x": pa.array([0.5, 1.5], pa.float64()),
            }
        )
        base = ArrowTableSource(
            table, tag_columns=["a", "b"], tag_only_columns=True, source_id="two_tags"
        )
        from_a = DerivedSource(base, source_id="same", tags_as_data={"a": "k"})
        from_b = DerivedSource(base, source_id="same", tags_as_data={"b": "k"})
        assert from_a.output_schema() == from_b.output_schema()
        assert from_a.source_id == from_b.source_id
        assert from_a.pipeline_hash() == from_b.pipeline_hash()
        assert from_a.content_hash() != from_b.content_hash()
        # The copies hold different values, and record ids (from the source id and
        # row position) are equal.
        ta, tb = from_a.as_table(), from_b.as_table()
        assert ta.column("k").to_pylist() != tb.column("k").to_pylist()
        ca, cb = (
            from_a.as_table(columns=ColumnConfig(system_tags=True)),
            from_b.as_table(columns=ColumnConfig(system_tags=True)),
        )
        (rid_a,) = _prefixed(ca, "_tag_record_id")
        (rid_b,) = _prefixed(cb, "_tag_record_id")
        assert ca.column(rid_a).to_pylist() == cb.column(rid_b).to_pylist()

    def test_mapping_order_does_not_change_content_hash(self):
        """The mapping enters the identity sorted, so its order does not matter."""
        a = DerivedSource(
            _experiment(),
            source_id="s",
            tags_as_data={"subject_id": "p", "experiment_index": "q"},
        )
        b = DerivedSource(
            _experiment(),
            source_id="s",
            tags_as_data={"experiment_index": "q", "subject_id": "p"},
        )
        assert a.content_hash() == b.content_hash()


# ---------------------------------------------------------------------------
# Downstream use
# ---------------------------------------------------------------------------


class TestDownstream:
    def test_elapsed_time_pod_then_join_with_session(self):
        derived = DerivedSource(_experiment(), tags_as_data=COPY)
        onsets = onset_s.pod(
            derived.select_data_columns(["session_start", "experiment_start_timestamp"])
        )
        joined = Join()(onsets, _session())
        tag_keys, data_keys = joined.keys()
        assert tag_keys == tuple(EXPERIMENT_KEYS)
        assert set(data_keys) == {"onset_s", "booth"}
        table = joined.as_table()
        assert table.num_rows == 5
        got = {
            (r["subject_id"], r["session_start_datetime"], r["experiment_index"]): (
                r["onset_s"],
                r["booth"],
            )
            for r in table.to_pylist()
        }
        assert got == {
            ("m1", S1, 0): (60.0, "A"),
            ("m1", S1, 1): (900.0, "A"),
            ("m1", S2, 0): (120.0, "B"),
            ("m1", S2, 1): (1500.0, "B"),
            ("m2", S3, 0): (30.0, "A"),
        }

    def test_join_with_the_original_needs_distinct_data_columns(self):
        origin = _experiment()
        derived = DerivedSource(origin, tags_as_data=COPY)
        with pytest.raises(InputValidationError):
            Join()(derived, origin)
        joined = Join()(derived.select_data_columns(["session_start"]), origin)
        assert joined.as_table().num_rows == 5

    def test_copy_is_plain_data_downstream(self):
        derived = DerivedSource(_experiment(), tags_as_data=COPY)
        renamed = derived.map_data({"session_start": "t0"})
        assert renamed.keys()[0] == tuple(EXPERIMENT_KEYS)
        assert "t0" in renamed.keys()[1]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_unknown_tag(self):
        with pytest.raises(InputValidationError, match="no_such_tag"):
            DerivedSource(_experiment(), tags_as_data={"no_such_tag": "x"})

    def test_a_data_column_is_not_a_tag(self):
        with pytest.raises(InputValidationError, match="task_id"):
            DerivedSource(_experiment(), tags_as_data={"task_id": "x"})

    def test_new_name_equal_to_a_tag(self):
        with pytest.raises(InputValidationError, match="subject_id"):
            DerivedSource(
                _experiment(),
                tags_as_data={"session_start_datetime": "subject_id"},
            )

    def test_new_name_equal_to_a_data_column(self):
        with pytest.raises(InputValidationError, match="task_id"):
            DerivedSource(
                _experiment(), tags_as_data={"session_start_datetime": "task_id"}
            )

    def test_two_tags_copied_to_one_name(self):
        with pytest.raises(InputValidationError, match="'x'"):
            DerivedSource(
                _experiment(),
                tags_as_data={"session_start_datetime": "x", "subject_id": "x"},
            )

    def test_reserved_name(self):
        with pytest.raises(InputValidationError, match="_hidden"):
            DerivedSource(
                _experiment(), tags_as_data={"session_start_datetime": "_hidden"}
            )

    def test_source_origin_needs_tags_as_data(self):
        with pytest.raises(TypeError, match="tags_as_data"):
            DerivedSource(_experiment())

    def test_empty_mapping_re_roots_a_source_unchanged(self):
        origin = _experiment()
        derived = DerivedSource(origin, tags_as_data={})
        assert derived.keys() == origin.keys()
        assert derived.as_table().equals(origin.as_table())
        assert derived.source_id.startswith("tags_as_data:")

    def test_record_id_column_needs_tags_as_data(self):
        job = _upper_job()
        with pytest.raises(ValueError, match="record_id_column"):
            DerivedSource(job.nodes["upper"], record_id_column="task_upper")


# ---------------------------------------------------------------------------
# PipelineJob, orchestrators, save and load, stores
# ---------------------------------------------------------------------------


def _make_probe_db(path: Path) -> str:
    """Create a SQLite file with a three-row ``probe`` table and return its path."""
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(
            "CREATE TABLE probe (probe_id TEXT NOT NULL, name TEXT NOT NULL, "
            "channels INTEGER, PRIMARY KEY (probe_id, name))"
        )
        conn.executemany(
            "INSERT INTO probe VALUES (?, ?, ?)",
            [("p1", "alpha", 32), ("p2", "beta", 64), ("p3", "gamma", None)],
        )
        conn.commit()
    return str(path)


def _probe_job(db_path: str, name: str = "probes") -> PipelineJob:
    """Return a job that copies the ``name`` key into data and measures its length."""
    job = PipelineJob(name=name, database=InMemoryArrowDatabase())
    with job:
        derived = DerivedSource(
            SQLiteTableSource(db_path, "probe", tag_only_columns=True),
            tags_as_data={"name": "name_copy"},
        )
        name_length.pod(derived.select_data_columns(["name_copy"]), label="len")
    return job


class TestPipelineJob:
    @pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator()])
    def test_sync_and_async_runs_give_the_same_rows(self, tmp_path, orchestrator):
        job = _probe_job(_make_probe_db(tmp_path / "edi.db"))
        job.run(orchestrator=orchestrator)
        node = job.nodes["len"]
        assert node.keys() == (("probe_id", "name"), ("n",))
        rows = {r["name"]: r["n"] for r in node.as_table().to_pylist()}
        assert rows == {"alpha": 5, "beta": 4, "gamma": 5}

    def test_origin_and_derived_source_in_one_job(self, tmp_path):
        db_path = _make_probe_db(tmp_path / "edi.db")
        job = PipelineJob(name="both", database=InMemoryArrowDatabase())
        with job:
            probe = SQLiteTableSource(db_path, "probe", tag_only_columns=True)
            derived = DerivedSource(probe, tags_as_data={"name": "name_copy"})
            lengths = name_length.pod(
                derived.select_data_columns(["name_copy"]), label="len"
            )
            Join()(lengths, probe, label="back")
        job.run()
        assert set(job.sources) == {"probe", derived.source_id}
        assert job.nodes["back"].as_table().num_rows == 3

    def test_saved_job_reloads_the_derived_source(self, tmp_path):
        job = _probe_job(_make_probe_db(tmp_path / "edi.db"))
        job.run()
        (source_name,) = job.sources
        original = job.sources[source_name]
        assert isinstance(original, DerivedSource)
        path = tmp_path / "job.json"
        job.save(path)

        loaded = PipelineJob.load(path, database=InMemoryArrowDatabase())
        assert loaded.unbound_sources == []
        reloaded = loaded.sources[source_name]
        assert isinstance(reloaded, DerivedSource)
        assert reloaded.keys() == (("probe_id", "name"), ("channels", "name_copy"))
        assert reloaded.source_id == original.source_id
        assert reloaded.content_hash() == original.content_hash()
        assert reloaded.as_table(all_info=True).equals(original.as_table(all_info=True))
        assert loaded.nodes["len"].pipeline_hash() == job.nodes["len"].pipeline_hash()
        # The node identities include the mapping through the source's content hash.
        assert loaded.nodes["len"].content_hash() == job.nodes["len"].content_hash()

    def test_config_round_trip_keeps_an_explicit_source_id(self, tmp_path):
        db_path = _make_probe_db(tmp_path / "edi.db")
        derived = DerivedSource(
            SQLiteTableSource(db_path, "probe", tag_only_columns=True),
            tags_as_data={"name": "name_copy"},
            source_id="probe_named",
            record_id_column="name",
        )
        config = derived.to_config()
        assert config["source_type"] == "derived"
        assert config["tags_as_data"] == {"name": "name_copy"}
        rebuilt = DerivedSource.from_config(config)
        assert rebuilt.source_id == "probe_named"
        assert rebuilt.content_hash() == derived.content_hash()
        assert rebuilt.as_table(all_info=True).equals(derived.as_table(all_info=True))

    def test_not_reloadable_when_the_origin_is_not(self, tmp_path):
        job = PipelineJob(name="memory", database=InMemoryArrowDatabase())
        with job:
            derived = DerivedSource(_experiment(), tags_as_data=COPY)
            onset_s.pod(
                derived.select_data_columns(
                    ["session_start", "experiment_start_timestamp"]
                ),
                label="onset",
            )
        job.run()
        path = tmp_path / "job.json"
        job.save(path)
        loaded = PipelineJob.load(path, database=InMemoryArrowDatabase())
        assert loaded.unbound_sources == [derived.source_id]


class TestStore:
    def test_job_store_holds_nothing_for_the_source_and_cached_saves_it(self):
        derived = DerivedSource(_experiment(), tags_as_data=COPY)
        db = InMemoryArrowDatabase()
        cached = derived.cached(db)
        # A hash inside a path is the bare digest (ITL-945); the layout is last (ITL-944).
        assert cached.cache_path == (
            "source",
            f"node:{derived.content_hash().to_hex()}",
            "sdb_v1",
        )
        assert db.get_all_records(cached.cache_path) is None
        table = cached.as_table()
        assert table.num_rows == 5
        assert table["session_start"].equals(table["session_start_datetime"])
        stored = db.get_all_records(cached.cache_path)
        assert stored is not None and stored.num_rows == 5
        assert cached.keys() == derived.keys()


# ---------------------------------------------------------------------------
# Job-node origins and zero rows
# ---------------------------------------------------------------------------


@function_pod(output_keys="task_upper")
def task_upper(task_id: str) -> str:
    """Return the task id in upper case."""
    return task_id.upper()


def _upper_job() -> PipelineJob:
    """Return a job with one function node, ``upper``, over the ``experiment`` table."""
    job = PipelineJob(name="stage1", database=InMemoryArrowDatabase())
    with job:
        task_upper.pod(_experiment().select_data_columns(["task_id"]), label="upper")
    return job


class TestNodeOrigin:
    def test_zero_rows_with_the_full_schema_before_the_node_has_run(self):
        job = _upper_job()
        derived = job.nodes["upper"].as_source(tags_as_data=COPY)
        assert derived.keys() == (
            tuple(EXPERIMENT_KEYS),
            ("task_upper", "session_start"),
        )
        table = derived.as_table(all_info=True)
        assert table.num_rows == 0
        assert table.schema.field("session_start").type == TS
        assert len(_prefixed(table, "_tag_")) == 2

    def test_rows_appear_on_the_same_object_after_the_run(self):
        job = _upper_job()
        derived = job.nodes["upper"].as_source(tags_as_data=COPY)
        assert derived.as_table().num_rows == 0
        job.run()
        table = derived.as_table()
        assert table.num_rows == 5
        assert table["session_start"].equals(table["session_start_datetime"])
        assert sorted(table["task_upper"].to_pylist()) == ["FV", "FV", "FV", "FV", "GZ"]

    def test_zero_row_and_filled_tables_have_the_same_columns(self):
        # Column names only: before a node has run, its Arrow types are known only
        # through its Python schema (as for the plain form), so a narrow type such
        # as uint32 shows as int64 until the node has rows. Tracked with ITL-956.
        job = _upper_job()
        node = job.nodes["upper"]
        empty = node.as_source(tags_as_data=COPY).as_table(all_info=True)
        job.run()
        filled = node.as_source(tags_as_data=COPY).as_table(all_info=True)
        assert empty.column_names == filled.column_names
        assert filled.schema.field("experiment_index").type == pa.uint32()

    def test_default_source_id_is_the_derivation_hash_over_the_node(self):
        job = _upper_job()
        node = job.nodes["upper"]
        derived = node.as_source(tags_as_data=COPY)
        assert derived.source_id == _expected_default_source_id(node, COPY)

    def test_feeds_a_second_job(self):
        job = _upper_job()
        job.run()
        stage2 = PipelineJob(name="stage2", database=InMemoryArrowDatabase())
        with stage2:
            derived = job.nodes["upper"].as_source(tags_as_data=COPY)
            Join()(
                derived.select_data_columns(["session_start"]), _session(), label="j"
            )
        stage2.run()
        assert stage2.nodes["j"].as_table().num_rows == 5

    def test_plain_as_source_is_unchanged(self):
        job = _upper_job()
        job.run()
        node = job.nodes["upper"]
        plain = node.as_source()
        assert plain.keys() == node.keys()
        assert plain.identity_structure() == (node.content_hash(),)
        # No source_id is passed: the plain form takes the DerivedSource default,
        # ``derived:<algorithm label>:<hex>`` of the node's content hash (ITL-957).
        assert plain.source_id == f"derived:{node.content_hash().to_string()}"
        assert _prefixed(plain.as_table(all_info=True), "_tag_") == []


class TestZeroRowOrigin:
    def test_zero_row_source_origin(self):
        schema = pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        )
        origin = DictSource(
            data=[],
            tag_columns=["k"],
            tag_only_columns=True,
            data_schema=schema,
            source_id="empty",
        )
        derived = DerivedSource(origin, tags_as_data={"k": "k_copy"})
        assert derived.keys() == (("k",), ("x", "k_copy"))
        table = derived.as_table(all_info=True)
        assert table.num_rows == 0
        copy_field = table.schema.field("k_copy")
        assert copy_field.type == pa.large_string()
        assert copy_field.nullable is False
        assert list(derived.iter_data()) == []
