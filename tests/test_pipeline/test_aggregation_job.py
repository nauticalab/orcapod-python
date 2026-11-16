"""Job-level tests for aggregating operators (NPIPE-204).

Operator-level tests (`op.process(stream)` then `as_table()`) never reach
`StaticOutputOperatorPod._materialize_to_stream`, which is where list-valued
provenance used to crash.  These run the full `job.run()` path against a real
Delta Lake store.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Batch, GroupBy, MergeJoin, SelectDataColumns
from orcapod import File
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import PipelineJob
from orcapod.types import Schema

# ---------------------------------------------------------------------------
# Invocation recording
#
# `PythonDataFunction` hashes the wrapped callable via `inspect.getsource` and
# `func.__code__`, so the callable must be a real function -- a callable class
# instance is rejected.  The invocation log therefore lives at module level
# rather than on a per-test callable object, which has the useful side effect
# of keeping the function identity byte-for-byte identical across the two runs
# a memoization test performs.
# ---------------------------------------------------------------------------

_CALLS: dict[str, list] = {"path": [], "v": [], "cfg": []}


def count_paths(path: list[str]) -> int:
    """Record the members' `path` values and return how many there are.

    `Batch` keeps `probe` as a list-valued *tag* and `GroupBy` drops it, so
    `path` is the only data column reaching the pod.
    """
    _CALLS["path"].append(list(path))
    return len(path)


def count_v(v: list[str]) -> int:
    """Record the merged `v` members and return the merged-list size."""
    _CALLS["v"].append(list(v))
    return len(v)


@pytest.fixture(autouse=True)
def _reset_calls() -> Iterator[None]:
    """Clear the shared invocation log before every test."""
    _CALLS["path"].clear()
    _CALLS["v"].clear()
    yield


@pytest.fixture
def store(tmp_path: Path) -> DeltaTableDatabase:
    return DeltaTableDatabase(base_path=tmp_path / "store")


@pytest.fixture
def session_source_factory():
    """Build a 2-group source; `paths` lets a test mutate one group's data."""

    def _make(paths: list[str] | None = None) -> ArrowTableSource:
        table = pa.table(
            {
                "subject": ["G", "G", "G", "G"],
                "date": ["d1", "d1", "d2", "d2"],
                "probe": [0, 1, 0, 1],
                "path": paths or ["a", "b", "c", "d"],
            }
        )
        return ArrowTableSource(
            table,
            tag_columns=["subject", "date", "probe"],
            tag_only_columns=True,
            infer_nullable=True,
        )

    return _make


def _run(store, source, operator, name, function=count_paths):
    """Record and run `source -> operator -> function` as a PipelineJob."""
    pod = FunctionPod(PythonDataFunction(function, output_keys="n"))
    job = PipelineJob(name=name, database=store)
    with job:
        pod(operator(source, label="agg"), label="counter")
    _ = job.run()
    return job


class TestGroupByInJob:
    def test_group_by_completes(self, store, session_source_factory):
        _run(store, session_source_factory(), GroupBy(by=["subject", "date"]), "gb")
        assert len(_CALLS["path"]) == 2
        assert sorted(_CALLS["path"]) == [["a", "b"], ["c", "d"]]

    def test_stored_records_hold_only_the_key_tags(self, store, session_source_factory):
        """The pod after ``GroupBy`` stores the keys, not the dropped tag (ITL-1070)."""
        job = _run(
            store, session_source_factory(), GroupBy(by=["subject", "date"]), "gb"
        )
        counter = job.nodes["counter"]
        assert counter.keys() == (("subject", "date"), ("n",))
        records = counter.get_all_records()
        assert records is not None
        assert "probe" not in records.column_names
        assert sorted(records.select(["date", "n"]).to_pylist(), key=str) == [
            {"date": "d1", "n": 2},
            {"date": "d2", "n": 2},
        ]

    def test_batch_completes(self, store, session_source_factory):
        """The provenance fix is independent of grouping."""
        _run(
            store,
            session_source_factory(),
            Batch(batch_size=2),
            "b",
            function=count_paths,
        )
        assert len(_CALLS["path"]) == 2


class TestGroupByMemoization:
    def test_identical_runs_hit_cache(self, store, session_source_factory):
        _run(store, session_source_factory(), GroupBy(by=["subject", "date"]), "m")
        assert len(_CALLS["path"]) == 2

        _CALLS["path"].clear()
        result = _run(
            store, session_source_factory(), GroupBy(by=["subject", "date"]), "m"
        )
        assert _CALLS["path"] == [], "second identical run must not recompute"
        # A cache hit must still surface the results, not an empty stream.
        table = result.nodes["counter"].as_table()
        assert table.num_rows == 2
        assert sorted(table.column("n").to_pylist()) == [2, 2]

    def test_fresh_store_recomputes(self, tmp_path, session_source_factory):
        """Control for `test_identical_runs_hit_cache`.

        Same two runs, but the second points at a *different* store.  If this
        did not recompute, the cache-hit assertion above would be vacuous --
        it would be passing because the pod never runs, not because the record
        was found.
        """
        store_a = DeltaTableDatabase(base_path=tmp_path / "store_a")
        store_b = DeltaTableDatabase(base_path=tmp_path / "store_b")

        _run(store_a, session_source_factory(), GroupBy(by=["subject", "date"]), "m")
        assert len(_CALLS["path"]) == 2

        _CALLS["path"].clear()
        _run(store_b, session_source_factory(), GroupBy(by=["subject", "date"]), "m")
        assert len(_CALLS["path"]) == 2, (
            "a fresh database has no cached records, so both groups must recompute"
        )

    def test_changed_member_invalidates_only_its_group(
        self, store, session_source_factory
    ):
        """Two groups; change one member of the first only.

        With a single group this assertion would be vacuous -- it must show
        that the untouched group stays cached.
        """
        _run(store, session_source_factory(), GroupBy(by=["subject", "date"]), "i")
        assert len(_CALLS["path"]) == 2

        _CALLS["path"].clear()
        changed = session_source_factory(["a", "B_CHANGED", "c", "d"])
        result = _run(store, changed, GroupBy(by=["subject", "date"]), "i")

        assert _CALLS["path"] == [["a", "B_CHANGED"]], (
            f"only the changed group should recompute; got {_CALLS['path']}"
        )
        # Both groups must still be present in the output — the recomputed one
        # and the one served from cache.
        table = result.nodes["counter"].as_table()
        assert table.num_rows == 2
        assert sorted(table.column("n").to_pylist()) == [2, 2]


class _LegacyGroupBy(GroupBy):
    """``GroupBy`` with the output shape of v0.2.0, before ITL-955 and ITL-1070.

    Non-key tags are list-valued data columns, which is what stores written by
    v0.2.0 hold.  It keeps the identity of ``GroupBy`` so that it occupies the
    same place in the pipeline.
    """

    def identity_structure(self):
        """Return the identity of ``GroupBy``, so stored tables are at the same paths."""
        return ("GroupBy", self.by)

    def unary_static_process(self, stream):
        """Group as ``GroupBy`` does, then add the non-key tags as data columns.

        ``GroupBy`` drops the non-key tags (ITL-1070); v0.2.0 kept each as a
        list-valued data column, in member order.  The members of a group are
        ordered by their non-key tag values, so each such list is its group's
        values sorted (the fixture's tags hold no nulls or ties).

        Args:
            stream: The stream to group.

        Returns:
            The grouped stream in the legacy shape.
        """
        grouped = super().unary_static_process(stream)
        table = grouped.as_table(columns={"source": True, "system_tags": True})
        tag_columns, _ = stream.keys()
        non_key = [c for c in tag_columns if c not in self.by]
        input_table = stream.as_table()
        members: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in input_table.to_pylist():
            members.setdefault(tuple(row[c] for c in self.by), []).append(row)
        keys = [
            tuple(r[c] for c in self.by)
            for r in table.select(list(self.by)).to_pylist()
        ]
        # v0.2.0 placed the moved tags right after the keys, before the data.
        for position, column in enumerate(non_key, start=len(self.by)):
            values = [sorted(m[column] for m in members[key]) for key in keys]
            list_type = pa.large_list(input_table.schema.field(column).type)
            table = table.add_column(
                position,
                pa.field(column, list_type, nullable=False),
                pa.array(values, type=list_type),
            )
        return ArrowTableStream(
            table,
            tag_columns=self.by,
            data_context=grouped.data_context,  # pyright: ignore[reportAttributeAccessIssue]  # not on StreamProtocol, as in GroupBy itself (ITL-844)
        )

    def unary_output_schema(self, stream, *, columns=None, all_info=False):
        """Predict the legacy schema: non-key tags are list-valued data columns.

        Args:
            stream: The stream to group.
            columns: Column groups to include, as for ``GroupBy``.
            all_info: Whether to include every column group.

        Returns:
            The ``(tag_schema, data_schema)`` pair of the legacy shape.
        """
        tag_schema, data_schema = super().unary_output_schema(
            stream, columns=columns, all_info=all_info
        )
        in_tags, _ = stream.output_schema()
        moved = {k: list[v] for k, v in in_tags.items() if k not in self.by}
        return tag_schema, Schema({**moved, **data_schema})


class TestStoreWrittenByV020:
    """Results stored by v0.2.0 are still found (ITL-955, ITL-1070).

    In v0.2.0 a pod could only follow ``GroupBy`` by selecting the real data
    columns (or by taking the moved tags as parameters).  Neither later change
    moves a pipeline hash or the pod's input data, and the dropped tags make
    the stored shape after the select equal to today's, so a database written by
    the old shape serves the new one without recomputing.
    """

    @staticmethod
    def _run(store, source, group_by):
        """Run ``source -> group_by -> select path -> counting pod`` on ``store``.

        Args:
            store: The database the job writes to and reads from.
            source: The source stream.
            group_by: The grouping operator to use.

        Returns:
            The job after its run.
        """
        pod = FunctionPod(
            PythonDataFunction(count_paths, output_keys="n")  # pyright: ignore[reportArgumentType]  # protocol mismatch baselined elsewhere (ITL-844)
        )
        job = PipelineJob(name="legacy", database=store)
        with job:
            grouped = group_by(source, label="agg")
            selected = SelectDataColumns(columns=["path"])(grouped, label="sel")
            pod(selected, label="counter")
        _ = job.run()
        return job

    def test_legacy_fixture_has_the_old_shape(self, session_source_factory):
        """Guard the fixture -- if this stops holding, the next test is vacuous."""
        out = _LegacyGroupBy(by=["subject", "date"])(session_source_factory())
        assert out.keys() == (("subject", "date"), ("probe", "path"))

    def test_rerun_on_old_store_does_not_recompute(self, store, session_source_factory):
        """A store written with the legacy shape serves the new one without recomputing."""
        before = self._run(
            store, session_source_factory(), _LegacyGroupBy(by=["subject", "date"])
        )
        assert len(_CALLS["path"]) == 2
        old_counter = before.nodes["counter"]
        assert old_counter.keys()[0] == ("subject", "date")

        _CALLS["path"].clear()
        after = self._run(
            store, session_source_factory(), GroupBy(by=["subject", "date"])
        )
        counter = after.nodes["counter"]

        assert counter.pipeline_hash() == old_counter.pipeline_hash()
        assert _CALLS["path"] == [], "stored results must be reused, not recomputed"
        assert after.nodes["agg"].keys() == (("subject", "date"), ("path",))
        # The stored shape of v0.2.0 is the shape of today (ITL-1070).
        assert counter.keys() == old_counter.keys() == (("subject", "date"), ("n",))
        rows = sorted(counter.as_table().to_pylist(), key=lambda r: r["date"])
        assert rows == [
            {"subject": "G", "date": "d1", "n": 2},
            {"subject": "G", "date": "d2", "n": 2},
        ]


class TestMergeJoinRegression:
    def test_merge_join_completes_in_job(self, store):
        """MergeJoin carries source columns as parallel lists.

        It crashed with the same ArrowTypeError before the Data fix.
        """
        left = ArrowTableSource(
            pa.table({"id": ["a", "b"], "v": ["l1", "l2"]}),
            tag_columns=["id"],
            tag_only_columns=True,
            infer_nullable=True,
        )
        right = ArrowTableSource(
            pa.table({"id": ["a", "b"], "v": ["r1", "r2"]}),
            tag_columns=["id"],
            tag_only_columns=True,
            infer_nullable=True,
        )

        pod = FunctionPod(PythonDataFunction(count_v, output_keys="n"))
        job = PipelineJob(name="mj", database=store)
        with job:
            pod(MergeJoin()(left, right, label="mj"), label="counter")
        job.run()

        assert len(_CALLS["v"]) == 2
        # MergeJoin merges colliding `v` columns into a sorted 2-element list.
        assert sorted(_CALLS["v"]) == [["l1", "r1"], ["l2", "r2"]]


# ---------------------------------------------------------------------------
# list[File] content hashing through a reduction — ITL-627 Defect 2
# ---------------------------------------------------------------------------


def reduce_configs(cfg: list[File]) -> int:
    """Record each member's file *contents* so a stale cache is visible."""
    _CALLS["cfg"].append([Path(c).read_text().strip() for c in cfg])
    return len(cfg)


def read_config(cfg: File) -> int:
    """Per-row control: scalar File columns are known to hash by content."""
    _CALLS["cfg"].append(Path(cfg).read_text().strip())
    return 1


class TestFileContentHashingThroughReduction:
    """A `list[File]` column must invalidate on content change, like a scalar one.

    A `File` column exists so that orcapod hashes the file's *contents* — editing
    a file re-runs its consumer even though the path is unchanged. ITL-627
    Defect 2 was that list-wrapping lost this: `build_aggregated_table` produced
    a `list[orcapod.file]` hashed by its storage values (JSON path strings), so a
    content edit silently failed to invalidate.

    The unit-level hashing contract is covered by
    `tests/test_hashing/test_extension_type_hashing.py::TestListExtensionHashing`.
    This test covers the job level, which is where the failure was observed and
    where it fails *silently* rather than raising.
    """

    @staticmethod
    def _source(cfg_paths):
        return DictSource(
            [
                {"date": "d1", "probe": i, "cfg": File(p)}
                for i, p in enumerate(cfg_paths)
            ],
            tag_columns=["date", "probe"],
            tag_only_columns=True,
            data_schema={"date": str, "probe": int, "cfg": File},
            source_id="cfgsrc",
        )

    def _run(self, store, cfg_paths, *, grouped):
        _CALLS["cfg"].clear()
        source = self._source(cfg_paths)
        job = PipelineJob(name="filehash", database=store)
        with job:
            if grouped:
                pod = FunctionPod(PythonDataFunction(reduce_configs, output_keys="n"))
                pod(GroupBy(by=["date"])(source, label="agg"), label="cc")
            else:
                pod = FunctionPod(PythonDataFunction(read_config, output_keys="n"))
                pod(source, label="cc")
        job.run()
        return list(_CALLS["cfg"])

    @pytest.mark.parametrize("grouped", [False, True], ids=["per_row", "grouped"])
    def test_content_edit_invalidates(self, tmp_path, grouped):
        store = DeltaTableDatabase(base_path=tmp_path / "store")
        a = tmp_path / "a.toml"
        b = tmp_path / "b.toml"
        a.write_text("entities = 'A'\n")
        b.write_text("entities = 'B'\n")

        assert self._run(store, [a, b], grouped=grouped), "cold run must execute"
        assert self._run(store, [a, b], grouped=grouped) == [], (
            "identical re-run must hit the cache"
        )

        # Same path, new contents.
        a.write_text("entities = 'A_CHANGED'\n")
        after = self._run(store, [a, b], grouped=grouped)

        assert after, (
            "a content edit must invalidate; an empty call log means the "
            "File column was hashed by path rather than by contents (ITL-627)"
        )
        flattened = after[0] if grouped else after
        assert "entities = 'A_CHANGED'" in flattened
