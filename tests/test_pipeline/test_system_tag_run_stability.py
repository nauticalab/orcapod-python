"""Run-to-run stability of system tags and sink completion tracking (ITL-817).

A ``sink_pod`` keys a row's completion record on the row's system tags plus the
content hash of its data. For a repeated run to skip a delivery, a row must
carry the same system tags every time. These tests pin that for a two-source
join feeding a function pod and a ``sink_pod``, run several times against one
on-disk ``DeltaTableDatabase`` with the input row order varied and with a mix
of cached and freshly computed rows.
"""

from __future__ import annotations

import random
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest

from orcapod import contexts

import orcapod as op
from orcapod import side_effects
from orcapod.core.datagrams import Data, Tag
from orcapod.core.operators import Join
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod, sink_pod

Key = tuple[tuple[str, Any], ...]


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def sink_rows(monkeypatch: pytest.MonkeyPatch) -> dict[Key, dict[str, Any]]:
    """Record the system tags of every row that reaches a side-effect node."""
    seen: dict[Key, dict[str, Any]] = {}
    original = side_effects._execute_side_effect_row

    def spy(**kwargs: Any) -> Any:
        tag = kwargs["tag"]
        key = tuple(sorted(tag.as_dict().items()))
        assert key not in seen, f"row {key} reached the sink twice in one run"
        seen[key] = tag.system_tags()
        return original(**kwargs)

    monkeypatch.setattr(side_effects, "_execute_side_effect_row", spy)
    return seen


def _table(rows: list[dict[str, Any]]) -> pa.Table:
    """Build a table whose columns are all non-nullable ``int64``."""
    schema = pa.schema([pa.field(name, pa.int64(), nullable=False) for name in rows[0]])
    return pa.Table.from_pylist(rows, schema=schema)


def _inputs(
    n_probes: int, n_methods: int, shared_key: bool
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return probe rows and method rows for a cross-product or shared-key join.

    ``w`` is unique per method row in both shapes so it can serve as the
    method source's ``record_id_column``.
    """
    probes = [{"probe": p, "x": 10 + p} for p in range(n_probes)]
    if shared_key:
        methods = [
            {"probe": p, "method": m, "w": 10 * p + m + 2}
            for p in range(n_probes)
            for m in range(n_methods)
        ]
    else:
        methods = [{"method": m, "w": m + 2} for m in range(n_methods)]
    return probes, methods


def _run_once(
    db_path: str,
    probes: list[dict[str, Any]],
    methods: list[dict[str, Any]],
    shared_key: bool,
) -> list[int]:
    """Run the pipeline once in a fresh ``PipelineJob`` and return what was delivered."""
    delivered: list[int] = []

    @op.function_pod(output_keys=["metric"])
    def scale(x: int, w: int) -> int:
        return x * w

    def deliver(metric: int, ctx: Any) -> None:
        delivered.append(metric)

    method_tags = ["probe", "method"] if shared_key else ["method"]
    with PipelineJob(name="itl817", database=DeltaTableDatabase(db_path)) as job:
        probe_source = ArrowTableSource(
            _table(probes),
            tag_columns=["probe"],
            source_id="probes",
            record_id_column="probe",
            tag_only_columns=True,
        )
        method_source = ArrowTableSource(
            _table(methods),
            tag_columns=method_tags,
            source_id="methods",
            record_id_column="w",
            tag_only_columns=True,
        )
        sink = sink_pod(deliver)
        assert isinstance(sink, SideEffectPod)
        sink.process(scale.pod(Join()(probe_source, method_source)))
    job.run()
    return delivered


# ---------------------------------------------------------------------------
# Buffer materialization
# ---------------------------------------------------------------------------


def _rows_with_swapped_tag_order() -> list[tuple[Tag, Data]]:
    """Return two rows whose tags list the same columns in opposite order."""
    return [
        (Tag({"a": 1, "b": 2}), Data({"v": 10})),
        (Tag({"b": 4, "a": 3}), Data({"v": 20})),
    ]


class TestMaterializationAlignsColumnsByName:
    """Rows in one buffer may list the same tag columns in a different order.

    A ``FunctionJobNode`` serves cached rows with the compile-time tag order
    and fresh rows with whatever order its upstream emitted.
    """

    def test_operator_materialize_to_stream(self) -> None:
        """The operator materializer emits rows in the first row's column order."""
        stream = StaticOutputOperatorPod._materialize_to_stream(
            _rows_with_swapped_tag_order()
        )
        rows = stream.as_table().to_pylist()
        assert rows == [{"a": 1, "b": 2, "v": 10}, {"a": 3, "b": 4, "v": 20}]

    def test_orchestrator_materialize_as_stream(self) -> None:
        """The orchestrator materializer emits rows in the first row's column order."""
        rows = _rows_with_swapped_tag_order()
        # The rebuilt stream takes the upstream's data context (ITL-1080) and, for an
        # upstream with no producer, its pipeline hash (ITL-1094): here a plain
        # stream's.
        upstream = SimpleNamespace(
            producer=None,
            upstreams=(),
            data_context_key=contexts.get_default_context_key(),
            pipeline_hash=StaticOutputOperatorPod._materialize_to_stream(
                rows
            ).pipeline_hash,
        )
        stream = SyncPipelineOrchestrator._materialize_as_stream(rows, upstream)
        rows = stream.as_table().to_pylist()
        assert rows == [{"a": 1, "b": 2, "v": 10}, {"a": 3, "b": 4, "v": 20}]

    def test_different_column_sets_still_raise(self) -> None:
        """Rows whose column names differ still raise ``ArrowInvalid``."""
        rows = [
            (Tag({"a": 1}), Data({"v": 10})),
            (Tag({"b": 2}), Data({"v": 20})),
        ]
        with pytest.raises(pa.ArrowInvalid):
            StaticOutputOperatorPod._materialize_to_stream(rows)


# ---------------------------------------------------------------------------
# End-to-end completion tracking
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "shared_key", [False, True], ids=["cross_product", "shared_key"]
)
class TestSinkCompletionIsRunStable:
    """A ``sink_pod`` fed by a two-source join skips rows it already delivered."""

    def test_second_run_delivers_nothing(
        self, tmp_path: Any, sink_rows: dict[Key, dict[str, Any]], shared_key: bool
    ) -> None:
        """Repeating the run with identical inputs delivers nothing and keeps system tags."""
        probes, methods = _inputs(3, 3, shared_key)

        assert len(_run_once(str(tmp_path), probes, methods, shared_key)) == 9
        first = dict(sink_rows)
        sink_rows.clear()

        assert _run_once(str(tmp_path), probes, methods, shared_key) == []
        assert sink_rows == first

    def test_reordered_inputs_keep_system_tags_and_skip_delivery(
        self, tmp_path: Any, sink_rows: dict[Key, dict[str, Any]], shared_key: bool
    ) -> None:
        """With ``record_id_column`` set, lineage follows the row, not its position."""
        probes, methods = _inputs(3, 3, shared_key)

        assert len(_run_once(str(tmp_path), probes, methods, shared_key)) == 9
        first = dict(sink_rows)
        assert len(first) == 9

        for seed in (1, 2):
            random.Random(seed).shuffle(probes)
            random.Random(seed + 10).shuffle(methods)
            sink_rows.clear()
            assert _run_once(str(tmp_path), probes, methods, shared_key) == []
            assert sink_rows == first

    def test_cached_and_fresh_rows_mix(
        self, tmp_path: Any, sink_rows: dict[Key, dict[str, Any]], shared_key: bool
    ) -> None:
        """Growing inputs deliver only the new rows and never move old lineage."""
        known: dict[Key, dict[str, Any]] = {}
        for n_probes, n_methods in [(1, 2), (2, 3), (3, 3), (3, 3)]:
            probes, methods = _inputs(n_probes, n_methods, shared_key)
            random.Random(n_probes * 7 + n_methods).shuffle(probes)
            random.Random(n_probes + n_methods * 7).shuffle(methods)
            sink_rows.clear()

            delivered = _run_once(str(tmp_path), probes, methods, shared_key)

            assert len(sink_rows) == n_probes * n_methods
            new_keys = set(sink_rows) - set(known)
            assert len(delivered) == len(new_keys)
            for key, system_tags in known.items():
                assert sink_rows[key] == system_tags, f"system tags moved for {key}"
            known.update(sink_rows)
