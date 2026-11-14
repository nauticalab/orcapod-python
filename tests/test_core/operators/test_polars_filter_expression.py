"""``polars_filter`` with a Polars expression predicate in a pipeline (ITL-828).

An expression filter used to raise ``TypeError`` as soon as its identity was needed: upstream
of a join, and anywhere inside a ``PipelineJob``. These tests drive the public interface only.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import polars as pl
import pytest

from orcapod import function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import Join, PolarsFilter
from orcapod.pipeline import AsyncPipelineOrchestrator, PipelineJob
from orcapod.sources import DictSource


def _readings() -> DictSource:
    return DictSource(
        data=[
            {"id": "a", "x": 1, "y": 10},
            {"id": "b", "x": -2, "y": 20},
            {"id": "c", "x": 3, "y": 30},
        ],
        tag_columns=["id"],
        label="readings",
    )


def _names() -> DictSource:
    return DictSource(
        data=[
            {"id": "a", "name": "alpha"},
            {"id": "b", "name": "beta"},
            {"id": "c", "name": "gamma"},
        ],
        tag_columns=["id"],
        label="names",
    )


def _ids(stream) -> list[str]:
    return sorted(str(v) for v in stream.as_table().column("id").to_pylist())


class TestHashes:
    def test_expression_filter_has_stable_hashes(self):
        first = _readings().polars_filter(pl.col("x") > 0)
        second = _readings().polars_filter(pl.col("x").gt(0))
        assert first.pipeline_hash() == second.pipeline_hash()
        assert first.content_hash() == second.content_hash()

    def test_different_expressions_hash_differently(self):
        first = _readings().polars_filter(pl.col("x") > 0)
        second = _readings().polars_filter(pl.col("x") > 1)
        assert first.pipeline_hash() != second.pipeline_hash()
        assert first.content_hash() != second.content_hash()

    def test_mixed_keyword_and_expression(self):
        mixed = _readings().polars_filter(pl.col("x") > 0, y=30)
        assert _ids(mixed) == ["c"]
        assert mixed.pipeline_hash() != _readings().polars_filter(y=30).pipeline_hash()
        assert (
            mixed.pipeline_hash()
            == _readings().polars_filter(pl.col("x") > 0, y=30).pipeline_hash()
        )

    def test_list_and_tuple_predicates_hash_equal(self):
        expr = pl.col("x") > 0
        from_list = PolarsFilter(predicates=[expr])(_readings())
        from_stream = _readings().polars_filter(expr)
        assert from_list.pipeline_hash() == from_stream.pipeline_hash()
        assert from_list.content_hash() == from_stream.content_hash()

    def test_unsupported_expression_names_it_when_hashed(self):
        expr = pl.col("x").map_elements(lambda v: v > 0, return_dtype=pl.Boolean)
        filtered = _readings().polars_filter(expr)
        # Never hashed: the in-memory result still works.
        assert _ids(filtered) == ["a", "c"]
        with pytest.raises(ValueError, match="keyword constraints"):
            filtered.pipeline_hash()


def test_expression_filter_upstream_of_join():
    filtered = _readings().polars_filter(pl.col("x") > 0)
    joined = Join()(filtered, _names())
    table = joined.as_table()
    assert sorted(str(v) for v in table.column("name").to_pylist()) == [
        "alpha",
        "gamma",
    ]
    assert joined.pipeline_hash() is not None


@function_pod(output_keys="score")
def score(x: int, y: int, name: str) -> int:
    """Combine a filtered reading with its joined name."""
    return x * y + len(name)


_FILTERS = {
    "expression": lambda src: src.polars_filter(
        (pl.col("x") > 0) & pl.col("y").is_in([10, 30]), label="positive"
    ),
    "keyword": lambda src: src.polars_filter(constraint_map={"x": 1}, label="positive"),
    "mixed": lambda src: src.polars_filter(pl.col("x") > 0, y=10, label="positive"),
}


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("kind", sorted(_FILTERS))
def test_filter_upstream_of_join_inside_pipeline_job(kind, orchestrator):
    expected = {
        "expression": (["a", "c"], [15, 95]),
        "keyword": (["a"], [15]),
        "mixed": (["a"], [15]),
    }
    job = PipelineJob(name="filter_job", database=InMemoryArrowDatabase())
    with job:
        filtered = _FILTERS[kind](_readings())
        joined = Join(label="joined")(filtered, _names())
        score.pod(joined, label="score")
    if orchestrator == "async":
        job.run(orchestrator=AsyncPipelineOrchestrator())
    else:
        job.run()
    # Read through the function pod's node: after an async run an operator node reads back
    # empty for every operator (pre-existing, reported separately), while a function node
    # serves its stored results on both paths.
    table = job.nodes["score"].as_table()
    ids, scores = expected[kind]
    assert sorted(table.column("id").to_pylist()) == ids
    assert sorted(table.column("score").to_pylist()) == scores


def test_filter_keeps_every_column_nullability():
    source = _readings()
    filtered = source.polars_filter(pl.col("x") > 0)
    columns = {"source": True, "system_tags": True}
    assert (
        filtered.as_table(columns=columns).schema
        == source.as_table(columns=columns).schema
    )


def test_filtered_row_keeps_its_source_data_hash():
    source = _readings()
    filtered = source.polars_filter(pl.col("x") > 0)
    source_hashes = {tag["id"]: data.content_hash() for tag, data in source.iter_data()}
    for tag, data in filtered.iter_data():
        assert data.content_hash() == source_hashes[tag["id"]]


_JOB_SCRIPT = textwrap.dedent("""\
    import json
    import sys

    import polars as pl

    from orcapod import function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import Join
    from orcapod.pipeline import PipelineJob
    from orcapod.pipeline.observer import NoOpObserver
    from orcapod.sources import DictSource


    @function_pod(output_keys="score")
    def score(x: int, y: int, name: str) -> int:
        return x * y + len(name)


    class Recorder(NoOpObserver):
        def __init__(self):
            self.events = []

        def contextualize(self, *identity_path):
            return self

        def on_data_end(self, node_label, tag, input_data, output_data, cached):
            self.events.append([node_label, bool(cached)])


    readings = DictSource(
        data=[
            {"id": "a", "x": 1, "y": 10},
            {"id": "b", "x": -2, "y": 20},
            {"id": "c", "x": 3, "y": 30},
        ],
        tag_columns=["id"],
        label="readings",
    )
    names = DictSource(
        data=[
            {"id": "a", "name": "alpha"},
            {"id": "b", "name": "beta"},
            {"id": "c", "name": "gamma"},
        ],
        tag_columns=["id"],
        label="names",
    )

    job = PipelineJob(name="expr_filter_cache", database=DeltaTableDatabase(base_path=sys.argv[1]))
    with job:
        filtered = readings.polars_filter(pl.col("x") > 0, label="positive")
        joined = Join(label="joined")(filtered, names)
        score.pod(joined, label="score")
    recorder = Recorder()
    job.run(observer=recorder)
    scores = sorted(job.nodes["score"].as_table().column("score").to_pylist())
    print(json.dumps({
        "pipeline_hash": job.nodes["score"].pipeline_hash().to_string(),
        "scores": scores,
        "score_events": [cached for label, cached in recorder.events if label == "score"],
    }))
""")


def test_two_runs_in_fresh_processes_hit_the_cache(tmp_path: Path):
    script = tmp_path / "run_job.py"
    script.write_text(_JOB_SCRIPT)
    store = tmp_path / "store"
    results = []
    for seed in ("0", "4242"):
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = seed
        proc = subprocess.run(
            [sys.executable, str(script), str(store)],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        results.append(json.loads(proc.stdout.strip().splitlines()[-1]))
    first, second = results
    assert first["pipeline_hash"] == second["pipeline_hash"]
    assert first["scores"] == second["scores"] == [15, 95]
    assert first["score_events"] == [False, False]
    assert second["score_events"] == [True, True]
