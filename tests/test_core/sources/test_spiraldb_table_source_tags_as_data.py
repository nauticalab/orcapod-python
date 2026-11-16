"""SpiralDBTableSource copies named key columns into data as rows are read (ITL-996).

Runs against the disk-backed pyspiral fake, whose ``scan_log`` records every scan a source
opens. The ``spike`` table mirrors the EDI ``spike_event`` table: its key holds
``timestamp`` before ``unit_id``, and it is read 2 rows per batch.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InputValidationError, SourceTooLargeError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.serialization import resolve_source_from_config
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_pyspiral

_PROJECT = "edi-sample"
_KEYS = pa.schema(
    [
        pa.field("subject_id", pa.string()),
        pa.field("timestamp", pa.uint64()),
        pa.field("unit_id", pa.uint64()),
    ]
)
# (subject, timestamp, unit), written out of time order.
_SPIKES = [
    ("m1", 50, 2),
    ("m1", 10, 1),
    ("m1", 40, 1),
    ("m1", 20, 2),
    ("m1", 30, 1),
    ("m2", 15, 1),
    ("m2", 5, 1),
]
_COPY = {"timestamp": "spike_time_us"}


def _spike_rows(spikes: list[tuple[str, int, int]]) -> pa.Table:
    """Return all-nullable ``spike`` rows, one per ``(subject, timestamp, unit)``."""
    table = pa.table(
        {
            "subject_id": [s for s, _, _ in spikes],
            "timestamp": pa.array([t for _, t, _ in spikes], pa.uint64()),
            "unit_id": pa.array([u for _, _, u in spikes], pa.uint64()),
            "amplitude": pa.array([t + 0.5 for _, t, _ in spikes], pa.float32()),
        }
    )
    return table.cast(pa.schema([pa.field(f.name, f.type, True) for f in table.schema]))


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fake project holding the 7-row ``spike`` table, read 2 rows per batch."""
    monkeypatch.setattr(fake_pyspiral, "scan_batch_rows", 2)
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        project.create_table("default.spike", key_schema=_KEYS).write(
            _spike_rows(_SPIKES)
        )
        fake_pyspiral.scan_log.clear()
        yield project
        fake_pyspiral.scan_log.clear()


def _source(**kwargs: Any) -> SpiralDBTableSource:
    """Return a source over the ``spike`` table."""
    return SpiralDBTableSource(_PROJECT, "spike", tag_only_columns=True, **kwargs)


def _column(name: str, table: pa.Table) -> list[Any]:
    """Return the values of column ``name`` of ``table``."""
    return table.column(name).to_pylist()


def _system_column(prefix: str, table: pa.Table) -> str:
    """Return the name of the one column of ``table`` that starts with ``prefix``."""
    (name,) = [n for n in table.column_names if n.startswith(prefix)]
    return name


def _spiral_configs(saved: object) -> list[dict[str, Any]]:
    """Return every saved Spiral source config found anywhere in a saved job."""
    if isinstance(saved, dict):
        found = [saved] if saved.get("source_type") == "spiraldb_table" else []
        return found + [c for v in saved.values() for c in _spiral_configs(v)]
    if isinstance(saved, list):
        return [c for v in saved for c in _spiral_configs(v)]
    return []


class TestTheCopy:
    """Each named key column is also a data column under the given name."""

    def test_the_schema_has_the_copy_and_construction_reads_no_rows(self, project):
        """The copy is a data column after the table's own, known without a scan."""
        source = _source(tags_as_data=_COPY)

        tag_keys, data_keys = source.keys()
        _, data_schema = source.output_schema()

        assert fake_pyspiral.scan_log == []
        assert tag_keys == ("subject_id", "timestamp", "unit_id")
        assert data_keys == ("amplitude", "spike_time_us")
        assert data_schema["spike_time_us"] is int

    def test_the_copy_has_the_key_type_not_nullable_and_the_key_values(self, project):
        """The copy is the key column's Arrow type, non-nullable, with its values."""
        table = _source(tags_as_data=_COPY).as_table()

        field = table.schema.field("spike_time_us")
        assert field.type == pa.uint64()
        assert not field.nullable
        assert _column("spike_time_us", table) == _column("timestamp", table)
        assert _column("timestamp", table) == [10, 20, 30, 40, 50, 5, 15]

    def test_copies_follow_in_mapping_order(self, project):
        """Several copies follow the table's data columns in the mapping's order."""
        source = _source(tags_as_data={"unit_id": "unit", "subject_id": "subject"})

        assert source.keys()[1] == ("amplitude", "unit", "subject")
        table = source.as_table()
        assert table.schema.field("subject").type == pa.large_string()
        assert _column("subject", table) == _column("subject_id", table)

    def test_iteration_and_as_table_agree(self, project):
        """``iter_data``, ``async_iter_data`` and ``as_table`` give the same copies."""
        source = _source(tags_as_data=_COPY)

        async def collect() -> list[tuple[int, int]]:
            """Return ``(timestamp, copy)`` of every row the async iteration yields."""
            return [
                (cast(int, tag["timestamp"]), cast(int, data["spike_time_us"]))
                async for tag, data in source.async_iter_data()
            ]

        synced = [
            (cast(int, tag["timestamp"]), cast(int, data["spike_time_us"]))
            for tag, data in source.iter_data()
        ]
        table = source.as_table()
        whole = list(
            zip(
                _column("timestamp", table),
                _column("spike_time_us", table),
                strict=True,
            )
        )

        assert synced == asyncio.run(collect()) == whole
        assert all(t == c for t, c in synced)

    def test_the_copy_is_offered_to_a_function_pod(self, project):
        """A function pod gets the copy as an argument: the point of the option."""

        @function_pod(output_keys="ms")
        def to_ms(spike_time_us: int) -> float:
            """Return the spike time in milliseconds."""
            return spike_time_us / 1000

        stream = to_ms.pod(_source(tags_as_data=_COPY, limit=2))

        assert _column("ms", stream.as_table()) == [0.01, 0.02]

    def test_limit_where_and_max_rows_apply_unchanged(self, project):
        """``limit``, ``where`` and ``max_rows`` work as without the option."""
        limited = _source(tags_as_data=_COPY, limit=2).as_table()
        filtered = _source(tags_as_data=_COPY, where={"subject_id": "m2"}).as_table()

        assert _column("spike_time_us", limited) == [10, 20]
        assert _column("spike_time_us", filtered) == [5, 15]
        with pytest.raises(SourceTooLargeError):
            _source(tags_as_data=_COPY, max_rows=3).as_table()

    def test_the_notebook_preview_shows_the_copy(self, project):
        """The bounded preview reads one head and shows the copied column."""
        html = _source(tags_as_data=_COPY)._repr_html_()

        assert [scan.kwargs.get("limit") for scan in fake_pyspiral.scan_log] == [10]
        assert "spike_time_us" in html
        assert "shape: (7, 5)" in html


class TestRefusedMappings:
    """A mapping that cannot be applied is refused at construction, naming the column."""

    @pytest.mark.parametrize(
        ("mapping", "named"),
        [
            ({"missing": "copy"}, "'missing' is not a column"),
            ({"amplitude": "copy"}, "'amplitude' is a data column"),
            ({"timestamp": "amplitude"}, "'amplitude', which is already a column"),
            ({"timestamp": "unit_id"}, "'unit_id', which is already a column"),
            ({"timestamp": "t", "unit_id": "t"}, "'t' is given to more than one tag"),
            ({"timestamp": "__t"}, "'__t'"),
        ],
    )
    def test_refused(self, project, mapping, named):
        """Each refusal names the table and the column."""
        with pytest.raises(InputValidationError, match=named) as raised:
            _source(tags_as_data=mapping)

        assert "'spike'" in str(raised.value)
        assert fake_pyspiral.scan_log == []

    def test_a_tag_that_is_not_a_key_column_is_refused(self, project):
        """Only a key column, which is never null, can be copied."""
        with pytest.raises(InputValidationError, match="'amplitude' is not a key"):
            _source(
                tag_columns=["subject_id", "amplitude"], tags_as_data={"amplitude": "a"}
            )

    def test_a_key_column_that_is_not_a_tag_is_refused(self, project):
        """A key column the source offers as data already is not a tag to copy."""
        with pytest.raises(InputValidationError, match="'timestamp' is a data column"):
            _source(tag_columns=["subject_id", "unit_id"], tags_as_data=_COPY)


class TestIdentity:
    """The copies are data columns, so they are in the schema and in identity."""

    def test_source_id_record_ids_and_tokens_do_not_move(self, project):
        """The source id, each row's record id and its other tokens are unchanged."""
        plain = _source()
        copied = _source(tags_as_data=_COPY)
        before = plain.as_table(all_info=True)
        after = copied.as_table(all_info=True)

        assert copied.source_id == plain.source_id
        for prefix in ("_tag_record_id::", "_tag_source_id::"):
            assert _column(_system_column(prefix, after), after) == _column(
                _system_column(prefix, before), before
            )
        assert _column("_source_amplitude", after) == _column(
            "_source_amplitude", before
        )

    def test_the_copys_token_has_the_form_of_every_data_column(self, project):
        """A copy's source-info value is ``<source id>::<row token>::<copy name>``."""
        table = _source(tags_as_data=_COPY).as_table(columns={"source": True})

        for amplitude, copy in zip(
            _column("_source_amplitude", table),
            _column("_source_spike_time_us", table),
            strict=True,
        ):
            assert copy == amplitude.removesuffix("amplitude") + "spike_time_us"

    def test_content_and_pipeline_hash_move_with_the_schema(self, project):
        """The schema gains a column, so both hashes differ from the plain source."""
        plain = _source()
        copied = _source(tags_as_data=_COPY)

        assert copied.content_hash() != plain.content_hash()
        assert copied.pipeline_hash() != plain.pipeline_hash()

    def test_two_mappings_with_one_schema_are_different_sources(self, project):
        """The mapping is in the content hash; the pipeline hash is schema-only."""
        by_time = _source(tags_as_data={"timestamp": "copy"})
        by_unit = _source(tags_as_data={"unit_id": "copy"})

        assert by_time.output_schema() == by_unit.output_schema()
        assert by_time.content_hash() != by_unit.content_hash()
        assert by_time.pipeline_hash() == by_unit.pipeline_hash()

    def test_no_mapping_is_the_source_without_the_option(self, project):
        """``None`` and an omitted argument give the same identity as before."""
        plain = _source()
        for same in (_source(tags_as_data=None), _source(tags_as_data={})):
            assert same.content_hash() == plain.content_hash()
            assert same.pipeline_hash() == plain.pipeline_hash()
            assert same.keys() == plain.keys()

    def test_a_rows_data_hash_moves_and_its_tag_hash_does_not(self, project):
        """The row has one more data value; its tag is the same."""
        ((plain_tag, plain_data), *_) = _source().iter_data()
        ((tag, data), *_) = _source(tags_as_data=_COPY).iter_data()

        assert tag.content_hash() == plain_tag.content_hash()
        assert data.content_hash() != plain_data.content_hash()


class TestSaveAndLoad:
    """The mapping is saved and loaded with the source."""

    def test_config_round_trip(self, project):
        """A config round trip keeps the mapping and the content hash, scanning nothing."""
        source = _source(tags_as_data={"timestamp": "t", "unit_id": "u"})
        config = source.to_config()

        loaded = resolve_source_from_config(json.loads(json.dumps(config)))

        assert fake_pyspiral.scan_log == []
        assert config["tags_as_data"] == {"timestamp": "t", "unit_id": "u"}
        assert isinstance(loaded, SpiralDBTableSource)
        assert loaded.to_config() == config
        assert loaded.content_hash() == source.content_hash()
        assert loaded.keys() == source.keys()

    def test_a_config_without_the_mapping_loads_the_plain_source(self, project):
        """A config with no ``tags_as_data`` entry loads with no copies."""
        config = _source().to_config()
        assert config["tags_as_data"] is None
        del config["tags_as_data"]

        loaded = resolve_source_from_config(config)

        assert loaded.content_hash() == _source().content_hash()

    @pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
    def test_a_saved_and_loaded_job_reads_its_results(
        self, project, tmp_path, orchestrator
    ):
        """A job saved after its run keeps the mapping and, loaded, reads its results."""
        calls: list[int] = []

        @function_pod(output_keys="ms")
        def to_ms(spike_time_us: int) -> float:
            """Return the spike time in milliseconds, recording the call."""
            calls.append(spike_time_us)
            return spike_time_us / 1000

        def build() -> PipelineJob:
            """Build the job over the source with the copy."""
            job = PipelineJob(
                name="spikes", database=DeltaTableDatabase(tmp_path / "store")
            )
            with job:
                to_ms.pod(_source(tags_as_data=_COPY), label="to_ms")
            return job

        job = build()
        job.run(orchestrator=orchestrator() if orchestrator else None)
        assert sorted(calls) == sorted(t for _, t, _ in _SPIKES)
        path = tmp_path / "job.json"
        job.save(str(path))
        (saved,) = _spiral_configs(json.loads(path.read_text()))
        assert saved["tags_as_data"] == _COPY

        loaded = PipelineJob.load(str(path))
        stored = loaded.nodes["to_ms"].get_all_records()
        assert stored is not None
        assert sorted(_column("ms", stored)) == sorted(t / 1000 for _, t, _ in _SPIKES)

        # The same job built again finds every row's result.
        calls.clear()
        build().run(orchestrator=orchestrator() if orchestrator else None)
        assert calls == []


class TestTheWorkshopCase:
    """Grouping on the unit key gives each unit's spike times in time order."""

    def test_group_by_the_unit_key_orders_spike_times_by_time(self, project):
        """The only tag left to order members is the time, so the lists are in time order.

        ``group_by`` drops the non-key tag ``timestamp`` (ITL-1070); its copy
        ``spike_time_us`` is data and is kept.
        """
        spikes = _source(tags_as_data=_COPY)

        per_unit = spikes.group_by(["subject_id", "unit_id"])

        assert per_unit.keys() == (
            ("subject_id", "unit_id"),
            ("amplitude", "spike_time_us"),
        )
        rows = per_unit.as_table().to_pylist()
        assert [(r["subject_id"], r["unit_id"], r["spike_time_us"]) for r in rows] == [
            ("m1", 1, [10, 30, 40]),
            ("m1", 2, [20, 50]),
            ("m2", 1, [5, 15]),
        ]
        assert [r["amplitude"] for r in rows] == [
            [10.5, 30.5, 40.5],
            [20.5, 50.5],
            [5.5, 15.5],
        ]
