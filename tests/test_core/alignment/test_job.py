"""A job's run summary and tables belong to the rows that produced them (ITL-1034).

``gated_fragile`` raises for every ``x`` with ``fails(x)`` and the run goes on (the default
error policy). The run summary must count exactly the failed and computed rows, and its
first error must be the error of the first row to fail in the order the rows completed.
The node's table must hold exactly the rows that succeeded, each aligned. Every entry
point (sync ``run``, async ``run``, ``run_async``, ``run`` inside a running loop), every
forced completion order on the async ones, both stores and every tie scenario.

A job's ``SideEffectPod`` node builds its tables with its own tag/data concatenation
(``SideEffectNode.as_table``), not a stream's: its iterated rows and its tables under every
configuration must hold every input row once, each aligned. After the run its preview
shows the rows the run passed on, each aligned, and no read delivers again (ITL-1091).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from orcapod import PipelineJob
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import RowFailuresWarning
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod

from .checker import Layout, assert_requested_columns, assert_rows_aligned, rows_table
from .expectations import rows
from .fixtures import (
    COLUMN_CONFIGS,
    SCENARIOS,
    content_hash_column,
    keys_a,
    make_sources,
)
from .gate import ORDERS, fails
from .jobs import MODES, STORES, function_tokens, make_run

ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


def job_params() -> list[Any]:
    """Every entry point, its orders, store and scenario."""
    params = []
    for scenario in SCENARIOS:
        for store_kind in STORES:
            for mode in MODES:
                for order in ["natural"] if mode == "sync" else list(ORDERS):
                    params.append(
                        pytest.param(
                            scenario,
                            store_kind,
                            mode,
                            order,
                            id=f"{scenario}-{store_kind}-{mode}-{order}",
                        )
                    )
    return params


@pytest.mark.parametrize(("scenario", "store_kind", "mode", "order"), job_params())
def test_run_summary_and_table_belong_to_the_rows_that_ran(
    scenario: str, store_kind: str, mode: str, order: str, tmp_path: Path
) -> None:
    keys = keys_a(scenario, "shuffled")
    completion = keys if mode == "sync" else cast(list[int], ORDERS[order](keys))
    with pytest.warns(RowFailuresWarning, match="rows failed in node 'scaled'"):
        run = make_run(
            scenario,
            "shuffled",
            mode,
            None if mode == "sync" else completion,
            store_kind,
            tmp_path,
            fragile=True,
        )
    assert run.completed == completion, "the gate did not realise the order"

    failing = [x for x in completion if fails(x)]
    succeeding = [x for x in completion if not fails(x)]
    assert failing and succeeding, "the fixture must have both kinds of row"
    summary = run.summary.nodes["scaled"]
    assert summary.failed == len(failing)
    assert summary.computed == len(succeeding)
    assert summary.reused == 0
    assert str(summary.first_error) == f"gated_fragile fails for x={failing[0]}"

    node = run.job.nodes["scaled"]
    table = node.as_table(all_info=True)
    tags, data = node.keys()
    assert_rows_aligned(
        table,
        Layout(tag_keys=tuple(tags), data_keys=tuple(data)),
        run.sources.oracle,
        node.data_context,
        function_tokens=function_tokens(run.job),
    )
    computed_ys = sorted(table.column("y").to_pylist())
    assert computed_ys == sorted(10 * x for x in succeeding)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_a_side_effect_node_gives_every_row_once_aligned(
    scenario: str, orchestrator: str
) -> None:
    """A job's side-effect node: rows, tables and previews aligned; delivered once.

    The run delivers each input row once. The node's own iteration, its tables and its
    previews under every configuration hold every row of A exactly once, each aligned:
    after the run the node holds the rows the run passed on (ITL-1091), and showing it
    never delivers.
    """
    sources = make_sources(scenario)
    delivered: list[int] = []

    def deliver(x: int, s: str, m: int | None, ctx: Any) -> None:
        """Record the delivered row's ``x``."""
        delivered.append(x)

    job = PipelineJob(name="side_effects", database=InMemoryArrowDatabase())
    with job:
        SideEffectPod(deliver)(sources.a, label="delivered")
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert sorted(delivered) == sorted(keys_a(scenario, "shuffled")), (
        "the run did not deliver every row once"
    )

    node = job.nodes["delivered"]
    tags, data = node.keys()
    expected = rows(sources.oracle, "A")
    assert_rows_aligned(
        rows_table(list(node.iter_data())),
        Layout(tag_keys=tuple(tags), data_keys=tuple(data)),
        sources.oracle,
        node.data_context,
        expected_rows=expected,
    )
    failures = []
    for name, config in COLUMN_CONFIGS.items():
        layout = Layout(
            tag_keys=tuple(tags),
            data_keys=tuple(data),
            content_hash_column=content_hash_column(config),
        )
        try:
            table = node.as_table(**config)
            assert_requested_columns(table, config, layout)
            assert_rows_aligned(
                table, layout, sources.oracle, node.data_context, expected_rows=expected
            )
            before = len(delivered)
            preview = node.head(100, **config).as_table()
            assert_requested_columns(preview, config, layout)
            assert_rows_aligned(
                preview,
                layout,
                sources.oracle,
                node.data_context,
                expected_rows=expected,
            )
            # Showing the node must never deliver.
            assert len(delivered) == before, "the preview delivered side effects"
        except AssertionError as exc:
            failures.append(f"[{name}] {exc}")
    assert not failures, "\n".join(failures)


def test_a_read_of_a_side_effect_node_after_its_run_delivers_nothing() -> None:
    """After ``job.run()``, iterating or tabling the side-effect node delivers nothing.

    A job node keeps the rows ``run()`` gave and its ``as_table()`` only reads
    (docs/concepts/streams.md); the run already delivered every row once.
    """
    sources = make_sources("unique")
    delivered: list[int] = []

    def deliver(x: int, s: str, m: int | None, ctx: Any) -> None:
        """Record the delivered row's ``x``."""
        delivered.append(x)

    job = PipelineJob(name="side_effects", database=InMemoryArrowDatabase())
    with job:
        SideEffectPod(deliver)(sources.a, label="delivered")
    job.run()
    once = sorted(keys_a("unique", "shuffled"))
    assert sorted(delivered) == once
    node = job.nodes["delivered"]
    list(node.iter_data())
    node.as_table(all_info=True)
    assert sorted(delivered) == once, (
        f"{len(delivered)} deliveries after the reads, {len(once)} rows"
    )
