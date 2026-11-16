"""Tests for SpiralDBTableSource's interface as a source.

SpiralDB needs network access and credentials, so these tests run against the
disk-backed pyspiral fake (``tests/test_databases/fake_pyspiral.py``): the source goes
through the real ``SpiralDBConnector``. Reading in batches, restrictions, the row cap and
record identity are in ``test_spiraldb_table_source_streaming.py``; the workshop path is in
``test_spiraldb_table_source_fake.py``.

Test sections:
 1. Import / export sanity
 2. Protocol conformance
 3. PK as default tag columns (single and composite)
 4. Explicit tag column override
 5. Error cases (missing table, no key schema)
 6. The connector (arguments forwarded, closed after use)
 7. Stream behaviour
 8. Deterministic hashing
 9. Config round-trip (to_config / from_config)
10. Integration: SpiralDBTableSource in a simple pipeline
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.core.sources import SpiralDBTableSource
from orcapod.databases import SpiralDBConnector
from tests.test_databases import fake_pyspiral

_TABLE_NAME = "spike_data"
_PROJECT_ID = "test-project-123456"
_CONNECTOR = "orcapod.core.sources.spiraldb_table_source.SpiralDBConnector"


def _make_table(
    name: str = _TABLE_NAME,
    pk_columns: tuple[str, ...] = ("session_id",),
    rows: pa.Table | None = None,
    project_id: str = _PROJECT_ID,
    dataset: str = "default",
) -> None:
    """Create a table in the fake project and write ``rows`` to it."""
    if rows is None:
        rows = pa.table(
            {
                "session_id": pa.array(["s1", "s2", "s3"], type=pa.large_string()),
                "firing_rate": pa.array([0.1, 0.2, 0.3], type=pa.float64()),
            }
        )
    keys = pa.schema([rows.schema.field(name) for name in pk_columns])
    project = fake_pyspiral.FakeSpiral().project(project_id)
    project.create_table(f"{dataset}.{name}", key_schema=keys).write(rows)


def _make_composite_pk_table() -> None:
    """Create a table with composite PK (session_id, probe_id) and data."""
    _make_table(
        pk_columns=("session_id", "probe_id"),
        rows=pa.table(
            {
                "session_id": pa.array(["s1", "s1"], type=pa.large_string()),
                "probe_id": pa.array([1, 2], type=pa.int64()),
                "neuron_count": pa.array([100, 200], type=pa.int64()),
            }
        ),
    )


@pytest.fixture(autouse=True)
def fake_spiral(tmp_path: Path):
    """Route every source in this module to a fresh fake SpiralDB."""
    with fake_pyspiral.install(tmp_path / "spiral"):
        yield


# ===========================================================================
# 1. Import / export sanity
# ===========================================================================


def test_import_from_core_sources():
    """``SpiralDBTableSource`` is importable from ``orcapod.core.sources``."""
    from orcapod.core.sources import SpiralDBTableSource

    assert SpiralDBTableSource is not None


def test_import_from_orcapod_sources():
    """``SpiralDBTableSource`` is importable from ``orcapod.sources``."""
    from orcapod.sources import SpiralDBTableSource

    assert SpiralDBTableSource is not None


def test_in_core_sources_all():
    """``SpiralDBTableSource`` is listed in ``orcapod.core.sources.__all__``."""
    import orcapod.core.sources as m

    assert "SpiralDBTableSource" in m.__all__


# ===========================================================================
# 2. Protocol conformance
# ===========================================================================


class TestProtocolConformance:
    """A ``SpiralDBTableSource`` satisfies the source, stream and hashing protocols."""

    def test_is_source_protocol(self):
        """The source satisfies ``SourceProtocol``."""
        from orcapod.protocols.core_protocols import SourceProtocol

        _make_table()
        assert isinstance(
            SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True),
            SourceProtocol,
        )

    def test_is_stream_protocol(self):
        """The source satisfies ``StreamProtocol``."""
        from orcapod.protocols.core_protocols import StreamProtocol

        _make_table()
        assert isinstance(
            SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True),
            StreamProtocol,
        )

    def test_is_pipeline_element_protocol(self):
        """The source satisfies ``PipelineElementProtocol``."""
        from orcapod.protocols.hashing_protocols import PipelineElementProtocol

        _make_table()
        assert isinstance(
            SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True),
            PipelineElementProtocol,
        )


# ===========================================================================
# 3. PK as default tag columns
# ===========================================================================


class TestPKAsDefaultTags:
    """By default the primary key columns are the tag columns."""

    def test_single_pk_is_tag_column(self):
        """A one-column primary key is the only tag; the other column is data."""
        _make_table()
        tag_schema, data_schema = SpiralDBTableSource(
            _PROJECT_ID, _TABLE_NAME, tag_only_columns=True
        ).output_schema()
        assert list(tag_schema) == ["session_id"]
        assert list(data_schema) == ["firing_rate"]

    def test_composite_pk_all_columns_are_tags(self):
        """Every column of a composite primary key is a tag."""
        _make_composite_pk_table()
        tag_schema, data_schema = SpiralDBTableSource(
            _PROJECT_ID, _TABLE_NAME, tag_only_columns=True
        ).output_schema()
        assert list(tag_schema) == ["session_id", "probe_id"]
        assert list(data_schema) == ["neuron_count"]


# ===========================================================================
# 4. Explicit tag column override
# ===========================================================================


class TestExplicitTagOverride:
    """Given ``tag_columns`` replace the primary key as the tags."""

    def test_explicit_tag_columns_override_pk(self):
        """Given ``tag_columns`` are the tags; key columns left out become data."""
        _make_composite_pk_table()
        src = SpiralDBTableSource(
            _PROJECT_ID, _TABLE_NAME, tag_columns=["probe_id"], tag_only_columns=True
        )
        tag_schema, data_schema = src.output_schema()
        assert list(tag_schema) == ["probe_id"]
        assert set(data_schema) == {"session_id", "neuron_count"}

    def test_a_bare_string_names_one_tag_column(self):
        """A bare string names a single tag column."""
        _make_composite_pk_table()
        src = SpiralDBTableSource(
            _PROJECT_ID, _TABLE_NAME, tag_columns="session_id", tag_only_columns=True
        )
        assert list(src.output_schema()[0]) == ["session_id"]

    def test_unknown_tag_column_raises(self):
        """A ``tag_columns`` name the table lacks is refused."""
        _make_table()
        with pytest.raises(ValueError, match="tag_columns not found"):
            SpiralDBTableSource(
                _PROJECT_ID, _TABLE_NAME, tag_columns=["nope"], tag_only_columns=True
            )


# ===========================================================================
# 5. Error cases
# ===========================================================================


class TestErrorCases:
    """Construction refuses a table it cannot read as a source."""

    def test_missing_table_raises_value_error(self):
        """A missing table is refused, naming the project and dataset."""
        _make_table()
        with pytest.raises(ValueError, match="not found in database") as raised:
            SpiralDBTableSource(
                _PROJECT_ID, "no_such_table", dataset="default", tag_only_columns=True
            )
        assert _PROJECT_ID in str(raised.value)
        assert "default" in str(raised.value)

    @pytest.mark.parametrize("tag_columns", [None, ["session_id"]])
    def test_a_table_without_a_key_schema_raises(self, tag_columns):
        """Rows are identified by their primary key, so a key schema is required."""
        _make_table()
        with patch.object(SpiralDBConnector, "get_pk_columns", return_value=[]):
            with pytest.raises(ValueError, match="no primary key"):
                SpiralDBTableSource(
                    _PROJECT_ID,
                    _TABLE_NAME,
                    tag_columns=tag_columns,
                    tag_only_columns=True,
                )


# ===========================================================================
# 6. The connector
# ===========================================================================


class TestConnector:
    """The source forwards its arguments to the connector and closes it."""

    def test_dataset_and_overrides_forwarded_to_connector(self):
        """``dataset`` and ``overrides`` are forwarded to ``SpiralDBConnector``."""
        overrides = {"server.url": "http://api.spiraldb.dev"}
        _make_table(dataset="prod")
        with patch(_CONNECTOR, wraps=SpiralDBConnector) as connector_class:
            SpiralDBTableSource(
                _PROJECT_ID,
                _TABLE_NAME,
                dataset="prod",
                overrides=overrides,
                tag_only_columns=True,
            )
        connector_class.assert_called_once_with(
            project_id=_PROJECT_ID, dataset="prod", overrides=overrides
        )

    def test_every_connector_is_closed(self):
        """Every connector is closed: build, full read, unfinished read, error."""
        _make_table()
        opened: list[SpiralDBConnector] = []

        def connect(**kwargs):
            """Open a real connector with ``kwargs`` and record it in ``opened``."""
            opened.append(SpiralDBConnector(**kwargs))
            return opened[-1]

        with patch(_CONNECTOR, side_effect=connect):
            src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True)
            src.as_table()
            # iter_data is a generator function, so the read can be closed.
            unfinished = cast("Generator[Any, None, None]", src.iter_data())
            next(unfinished)  # a read that is not finished
            unfinished.close()
            with pytest.raises(ValueError):
                SpiralDBTableSource(_PROJECT_ID, "no_such_table", tag_only_columns=True)
        assert len(opened) == 4
        # About the connector's own closed flag: nothing public reports it.
        assert [c._closed for c in opened] == [True] * 4  # pyright: ignore[reportPrivateUsage]


# ===========================================================================
# 7. Stream behaviour
# ===========================================================================


class TestStreamBehaviour:
    """The source is a root stream whose rows are the table's."""

    def test_producer_is_none_and_upstreams_empty(self):
        """A source has no producer and no upstreams."""
        _make_table()
        src = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True)
        assert src.producer is None
        assert src.upstreams == ()

    def test_iter_data_yields_one_pair_per_row(self):
        """``iter_data`` yields one tag and data pair per row, in key order."""
        _make_table()
        pairs = list(
            SpiralDBTableSource(
                _PROJECT_ID, _TABLE_NAME, tag_only_columns=True
            ).iter_data()
        )
        assert [tag.as_dict()["session_id"] for tag, _ in pairs] == ["s1", "s2", "s3"]
        assert [data.as_dict()["firing_rate"] for _, data in pairs] == pytest.approx(
            [0.1, 0.2, 0.3]
        )

    def test_as_table_returns_the_rows(self):
        """``as_table`` returns the table's rows as a ``pa.Table``."""
        _make_table()
        table = SpiralDBTableSource(
            _PROJECT_ID, _TABLE_NAME, tag_only_columns=True
        ).as_table()
        assert isinstance(table, pa.Table)
        assert table.to_pydict() == {
            "session_id": ["s1", "s2", "s3"],
            "firing_rate": [0.1, 0.2, 0.3],
        }


# ===========================================================================
# 8. Deterministic hashing
# ===========================================================================


class TestDeterministicHashing:
    """Hashes depend on the table and its schema, not on the source object."""

    def test_hashes_are_deterministic(self):
        """Two sources of one table have equal pipeline and content hashes."""
        _make_table()
        src1 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True)
        src2 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True)
        assert src1.pipeline_hash() == src2.pipeline_hash()
        assert src1.content_hash() == src2.content_hash()

    def test_different_tag_columns_yield_different_pipeline_hash(self):
        """Other tag columns give another pipeline hash."""
        _make_composite_pk_table()
        src1 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True)
        src2 = SpiralDBTableSource(
            _PROJECT_ID, _TABLE_NAME, tag_columns=["probe_id"], tag_only_columns=True
        )
        assert src1.pipeline_hash() != src2.pipeline_hash()

    def test_different_schemas_yield_different_pipeline_hash(self):
        """pipeline_hash is schema-only; different column schemas → different hash."""
        _make_table()
        _make_table(
            name="counts",
            rows=pa.table(
                {
                    "session_id": pa.array(["s1", "s2"], type=pa.large_string()),
                    "neuron_count": pa.array([10, 20], type=pa.int64()),
                }
            ),
        )
        src1 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True)
        src2 = SpiralDBTableSource(_PROJECT_ID, "counts", tag_only_columns=True)
        assert src1.pipeline_hash() != src2.pipeline_hash()

    def test_same_schema_in_two_tables_shares_the_pipeline_hash_only(self):
        """Two tables of one schema share the pipeline hash, not the content hash."""
        _make_table()
        _make_table(name="other")
        src1 = SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True)
        src2 = SpiralDBTableSource(_PROJECT_ID, "other", tag_only_columns=True)
        assert src1.pipeline_hash() == src2.pipeline_hash()
        assert src1.content_hash() != src2.content_hash()


# ===========================================================================
# 9. Config round-trip (to_config / from_config)
# ===========================================================================


class TestConfigSerialization:
    """``to_config`` and ``from_config`` round-trip a source."""

    def test_to_config_fields(self):
        """``to_config`` records the table, its id, tags, overrides, id and hashes."""
        _make_table(dataset="prod")
        overrides = {"server.url": "http://api.spiraldb.dev"}
        src = SpiralDBTableSource(
            _PROJECT_ID,
            _TABLE_NAME,
            dataset="prod",
            overrides=overrides,
            tag_only_columns=True,
        )
        cfg = src.to_config()
        assert cfg["source_type"] == "spiraldb_table"
        assert "connector" not in cfg
        assert cfg["project_id"] == _PROJECT_ID
        assert cfg["dataset"] == "prod"
        assert cfg["table_name"] == _TABLE_NAME
        assert cfg["table_id"] == src.table_id
        assert cfg["tag_columns"] == ["session_id"]
        assert cfg["overrides"] == overrides
        assert cfg["source_id"] == src.source_id
        for field in ("content_hash", "pipeline_hash", "tag_schema", "data_schema"):
            assert field in cfg

    def test_to_config_defaults(self):
        """``to_config`` records the default restrictions, cap and batch size."""
        _make_table()
        cfg = SpiralDBTableSource(
            _PROJECT_ID, _TABLE_NAME, tag_only_columns=True
        ).to_config()
        assert cfg["overrides"] is None
        assert cfg["where"] == {}
        assert cfg["limit"] is None
        assert cfg["asof"] is None
        assert cfg["max_rows"] == 100_000
        assert cfg["batch_rows"] == 8_192

    def test_from_config_hashes_match(self):
        """A source rebuilt by ``from_config`` keeps its hashes and reads the rows."""
        _make_table(dataset="prod")
        src = SpiralDBTableSource(
            _PROJECT_ID, _TABLE_NAME, dataset="prod", tag_only_columns=True
        )
        src2 = SpiralDBTableSource.from_config(src.to_config())
        assert src.pipeline_hash() == src2.pipeline_hash()
        assert src.content_hash() == src2.content_hash()
        assert src2.as_table().num_rows == 3

    def test_resolve_source_from_config_works(self):
        """``resolve_source_from_config`` rebuilds a ``SpiralDBTableSource``."""
        from orcapod.pipeline.serialization import resolve_source_from_config

        _make_table()
        cfg = SpiralDBTableSource(
            _PROJECT_ID, _TABLE_NAME, tag_only_columns=True
        ).to_config()
        assert isinstance(resolve_source_from_config(cfg), SpiralDBTableSource)


# ===========================================================================
# 10. Integration: SpiralDBTableSource in a simple pipeline
# ===========================================================================


@pytest.mark.integration
class TestPipelineIntegration:
    """A ``SpiralDBTableSource`` feeds a function pod in a ``PipelineJob``."""

    def test_spiraldb_source_in_pipeline(self):
        """Verify SpiralDBTableSource drives a full pipeline end-to-end."""
        from orcapod import PipelineJob, function_pod
        from orcapod.databases import InMemoryArrowDatabase

        @function_pod(output_keys="doubled")
        def double_rate(firing_rate: float | None) -> float | None:
            """Return twice ``firing_rate``, or ``None`` for a null."""
            return None if firing_rate is None else firing_rate * 2.0

        _make_table()
        job = PipelineJob(name="spiraldb_integration", database=InMemoryArrowDatabase())
        with job:
            double_rate.pod(
                SpiralDBTableSource(_PROJECT_ID, _TABLE_NAME, tag_only_columns=True),
                label="doubler",
            )
        job.run()

        rows = job.nodes["doubler"].as_table().sort_by("session_id").to_pydict()
        assert rows["session_id"] == ["s1", "s2", "s3"]
        assert rows["doubled"] == pytest.approx([0.2, 0.4, 0.6])
