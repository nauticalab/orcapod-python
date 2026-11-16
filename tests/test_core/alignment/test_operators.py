"""Every operator's output keeps each derived column with its own row (ITL-1034).

Each case applies one operator (``core/operators``) to the alignment sources, and every
column configuration of its ``as_table`` and ``head`` is checked by recomputation. The
product: operator case x tie scenario x input order x read pattern (a fresh read; a
partial ``iter_data`` then a full read; a repeated read) x every ``ColumnConfig``, on the
stream an operator returns (the sync path). The same cases also run as operator nodes of a
job on both orchestrators, so each operator's async path is checked, and its table must
equal the sync one row by row, keyed on record ids. ``head(n, cfg)`` takes every
configuration ``as_table`` takes, sorting included, and must equal
``as_table(cfg).slice(0, n)`` (ITL-1053).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import polars as pl
import pytest

from orcapod.core.operators.batch import BATCH_FOLD_LABEL
from orcapod.core.operators.group_by import GROUP_BY_FOLD_LABEL
from orcapod import PipelineJob
from orcapod.core.operators import (
    Batch,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

from .checker import (
    Layout,
    assert_members_cover,
    assert_requested_columns,
    assert_rows_aligned,
    assert_same_rows_by_identity,
)
from .fixtures import (
    COLUMN_CONFIGS,
    INPUT_ORDERS,
    SCENARIOS,
    Sources,
    content_hash_column,
    make_sources,
)
from .checker import RowIdentity
from .expectations import groups, no_nulls, pairs, rows, semi
from .oracle import Origin


@dataclass(frozen=True)
class OpCase:
    """One operator applied to the alignment sources.

    Attributes:
        name: The test id.
        build: The operator's output stream from the sources (called inside a job for
            the job variant, so the streams it makes become job nodes).
        members: Columns that hold one element per member row (``Batch``, ``GroupBy``).
        dropped_tags: Input tags the output must not have, in its keys, its schema or
            any table (``GroupBy`` drops every tag not in ``by``, ITL-1070).
        merged: Columns that hold one element per input (``MergeJoin``).
        prepare: Tells the oracle about renamed or derived columns.
        keyed_by_record_ids: Whether sync and async give the same rows, so the two
            tables can be matched by record ids. ``Batch`` groups rows in arrival
            order, which the async path does not fix.
        covers: ``(column, source id)`` for a many-to-one operator: the flattened
            member values of ``column`` must be that source's rows, each once.
        expected: The rows the output must hold, from the fixtures by the operator's
            rule (``expectations.py``); ``None`` where they depend on arrival order
            (``Batch``'s boundaries), which ``covers`` checks instead.
        fold_label: The label of the operator's source-id fold (``Batch``, ``GroupBy``).
    """

    name: str
    build: Callable[[Sources], Any]
    members: Callable[[tuple[str, ...], tuple[str, ...]], frozenset[str]] = (
        lambda tags, data: frozenset()
    )
    merged: frozenset[str] = frozenset()
    prepare: Callable[[Sources], None] = lambda sources: None
    keyed_by_record_ids: bool = True
    covers: tuple[str, str] | None = None
    expected: Callable[[Sources], list[RowIdentity]] | None = None
    dropped_tags: tuple[str, ...] = ()
    fold_label: str | None = None


def every_column(tags: tuple[str, ...], data: tuple[str, ...]) -> frozenset[str]:
    """Every tag and data column holds members (``Batch``)."""
    return frozenset(tags + data)


def data_only(tags: tuple[str, ...], data: tuple[str, ...]) -> frozenset[str]:
    """Only data columns hold members (``GroupBy``: its keys are scalar and every
    other tag is dropped, ITL-1070)."""
    return frozenset(data)


def rename(new: str, old: str) -> Callable[[Sources], None]:
    """Tell the oracle that ``new`` is ``old`` renamed."""

    def prepare(sources: Sources) -> None:
        """Register the rename."""
        sources.oracle.rename(new, old)

    return prepare


def derive_from_d(
    column: str, compute: Callable[[Origin], Any], token: Callable[[Origin], str]
) -> Callable[[Sources], None]:
    """Tell the oracle that ``column`` is computed from a row of source D."""

    def prepare(sources: Sources) -> None:
        """Register the derivation."""
        sources.oracle.derive(column, "D", compute, token)

    return prepare


CASES = [
    OpCase(
        "join", lambda s: Join()(s.a, s.b), expected=lambda s: pairs(s.oracle, "A", "B")
    ),
    OpCase(
        "merge_join",
        lambda s: MergeJoin()(SelectDataColumns(["x"])(s.a), s.c),
        merged=frozenset({"x"}),
        expected=lambda s: pairs(s.oracle, "A", "C"),
    ),
    OpCase(
        "semi_join",
        lambda s: SemiJoin()(s.a, s.b),
        expected=lambda s: semi(s.oracle, "A", "B"),
    ),
    OpCase(
        "batch",
        lambda s: Batch(4)(s.a),
        members=every_column,
        fold_label=BATCH_FOLD_LABEL,
        keyed_by_record_ids=False,
        covers=("x", "A"),
    ),
    OpCase(
        "group_by_g",
        lambda s: GroupBy(by=["g"])(s.a),
        members=data_only,
        fold_label=GROUP_BY_FOLD_LABEL,
        covers=("x", "A"),
        expected=lambda s: groups(s.oracle, "A", ["g"]),
        dropped_tags=("n",),
    ),
    OpCase(
        "group_by_all_tags",
        lambda s: GroupBy(by=["g", "n"])(s.a),
        members=data_only,
        fold_label=GROUP_BY_FOLD_LABEL,
        covers=("x", "A"),
        expected=lambda s: groups(s.oracle, "A", ["g", "n"]),
    ),
    OpCase(
        "select_data",
        lambda s: SelectDataColumns(["m", "x"])(s.a),
        expected=lambda s: rows(s.oracle, "A"),
    ),
    OpCase(
        "drop_data",
        lambda s: DropDataColumns(["s"])(s.a),
        expected=lambda s: rows(s.oracle, "A"),
    ),
    OpCase(
        "select_tags",
        lambda s: SelectTagColumns(["g"])(s.a),
        expected=lambda s: rows(s.oracle, "A"),
    ),
    OpCase(
        "drop_tags",
        lambda s: DropTagColumns(["n"])(s.a),
        expected=lambda s: rows(s.oracle, "A"),
    ),
    OpCase(
        "map_tags",
        lambda s: MapTags({"g": "gg"})(s.a),
        prepare=rename("gg", "g"),
        expected=lambda s: rows(s.oracle, "A"),
    ),
    OpCase(
        "map_data",
        lambda s: MapData({"s": "t"})(s.a),
        prepare=rename("t", "s"),
        expected=lambda s: rows(s.oracle, "A"),
    ),
    OpCase(
        "polars_filter",
        lambda s: PolarsFilter([pl.col("x") > 1])(s.a),
        expected=lambda s: rows(s.oracle, "A", lambda o: o.values["x"] > 1),
    ),
    OpCase(
        "drop_nulls_named",
        lambda s: DropNulls(["m"])(s.a),
        expected=lambda s: rows(s.oracle, "A", no_nulls("m")),
    ),
    OpCase(
        "drop_nulls_all",
        lambda s: DropNulls()(s.a),
        expected=lambda s: rows(s.oracle, "A", no_nulls("g", "n", "x", "s", "m")),
    ),
    OpCase(
        "pick",
        lambda s: Pick("d", "p", out="dp")(s.d),
        prepare=derive_from_d(
            "dp",
            lambda o: o.values["d"][0]["value"],
            lambda o: f"D::{o.token}::d['p']",
        ),
        expected=lambda s: rows(s.oracle, "D"),
    ),
    OpCase(
        "index_new_column",
        lambda s: Index("seq", 1, out="second")(s.d),
        prepare=derive_from_d(
            "second", lambda o: o.values["seq"][1], lambda o: f"D::{o.token}::seq[1]"
        ),
        expected=lambda s: rows(s.oracle, "D"),
    ),
    OpCase(
        "index_in_place",
        lambda s: Index("seq", 0)(s.d),
        prepare=derive_from_d(
            "seq", lambda o: o.values["seq"][0], lambda o: f"D::{o.token}::seq[0]"
        ),
        expected=lambda s: rows(s.oracle, "D"),
    ),
]
CASE_IDS = [c.name for c in CASES]


def case_layout(case: OpCase, stream: Any, config: dict[str, Any]) -> Layout:
    """How the columns of ``stream``'s table relate to the origin rows."""
    tags, data = stream.keys()
    return Layout(
        tag_keys=tuple(tags),
        data_keys=tuple(data),
        members=case.members(tuple(tags), tuple(data)),
        merged=case.merged,
        content_hash_column=content_hash_column(config),
        fold_label=case.fold_label,
    )


def check_tables(
    case: OpCase, stream: Any, sources: Sources, *, with_head: bool
) -> list[str]:
    """Check ``as_table`` (and ``head``) under every configuration; return the failures.

    Every configuration must give the rows the default one gives (``head`` at most 100)
    and the per-row columns it asks for, and ``head(100, cfg)`` must be the first rows of
    ``as_table(cfg)``. An empty default table (a filter that keeps nothing) must stay
    empty.
    """
    failures = []
    if case.dropped_tags:
        tags, data = stream.keys()
        schema_tags, _ = stream.output_schema()
        kept = set(tags) | set(data) | set(schema_tags)
        for tag in case.dropped_tags:
            if tag in kept:
                failures.append(f"the dropped tag {tag!r} is in keys() or the schema")
    baseline = stream.as_table().num_rows
    for name, config in COLUMN_CONFIGS.items():
        full = stream.as_table(**config)
        tables = [("as_table", full, baseline)]
        if with_head:
            tables.append(
                ("head", stream.head(100, **config).as_table(), min(baseline, 100))
            )
        for call, table, expected_rows in tables:
            layout = case_layout(case, stream, config)
            try:
                present = [t for t in case.dropped_tags if t in table.column_names]
                assert not present, f"the dropped tags {present} are in the table"
                assert table.num_rows == expected_rows, (
                    f"{table.num_rows} rows, the default configuration gives "
                    f"{expected_rows}"
                )
                if call == "head":
                    assert table.equals(full.slice(0, expected_rows)), (
                        "head(100) is not the first rows of as_table"
                    )
                # Every configured table, not only the default one: a configuration
                # could repeat one whole group and leave out another, or give none.
                if (covered := member_values(case, sources)) is not None:
                    assert_members_cover(table, *covered)
                # Requested columns are names only: an empty table has them too.
                assert_requested_columns(table, config, layout)
                # An empty table is right only when the fixture-derived rows are none.
                if (
                    table.num_rows == 0
                    and case.expected is not None
                    and not case.expected(sources)
                ):
                    continue
                assert_rows_aligned(
                    table,
                    layout,
                    sources.oracle,
                    stream.data_context,
                    expected_rows=case.expected(sources) if case.expected else None,
                )
            except AssertionError as exc:
                failures.append(f"{call} [{name}] {exc}")
    return failures


def member_values(case: OpCase, sources: Sources) -> tuple[str, list[Any]] | None:
    """The member column of a many-to-one case and the input values it must cover."""
    if case.covers is None:
        return None
    column, source_id = case.covers
    return column, [
        o.values[column] for o in sources.oracle.origins if o.source_id == source_id
    ]


def check_stream(case: OpCase, stream: Any, sources: Sources) -> None:
    """Check ``as_table`` and ``head`` of ``stream`` under every column configuration."""
    failures = check_tables(case, stream, sources, with_head=True)
    assert not failures, f"{case.name}: " + "\n".join(failures)


READ_PATTERNS = ("fresh", "partial_then_full", "repeated")


def read(stream: Any, pattern: str) -> None:
    """Read ``stream`` in ``pattern`` before its tables are checked."""
    if pattern == "partial_then_full":
        partial = iter(stream.iter_data())
        next(partial, None)
        del partial
    elif pattern == "repeated":
        stream.as_table(all_info=True)
        list(stream.iter_data())


@pytest.mark.parametrize("pattern", READ_PATTERNS)
@pytest.mark.parametrize("input_order", INPUT_ORDERS)
@pytest.mark.parametrize("scenario", list(SCENARIOS))
@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_every_row_of_an_operator_output_is_aligned(
    case: OpCase, scenario: str, input_order: str, pattern: str
) -> None:
    sources = make_sources(scenario, input_order)
    case.prepare(sources)
    stream = case.build(sources)
    read(stream, pattern)
    total = stream.as_table().num_rows
    check_stream(case, stream, sources)
    assert total or case.name.startswith(("polars_filter", "drop_nulls", "join")), (
        "an unfiltered operator gave no rows: the check checked nothing"
    )


# ---------------------------------------------------------------------------
# The same operators as job nodes, on both orchestrators
# ---------------------------------------------------------------------------

ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


def run_case(case: OpCase, scenario: str, input_order: str, orchestrator: str) -> Any:
    """Run ``case`` as the operator node ``op`` of a job; return the node and sources."""
    sources = make_sources(scenario, input_order)
    case.prepare(sources)
    job = PipelineJob(name="ops", database=InMemoryArrowDatabase())
    with job:
        output = case.build(sources)
    label = output.label
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    return job.nodes[label], sources


def job_case_params() -> list[Any]:
    """Every case, scenario and input order."""
    return [
        pytest.param(
            case,
            scenario,
            input_order,
            id=f"{case.name}-{scenario}-{input_order}",
        )
        for case in CASES
        for scenario in SCENARIOS
        for input_order in INPUT_ORDERS
    ]


@pytest.mark.parametrize(("case", "scenario", "input_order"), job_case_params())
def test_operator_nodes_are_aligned_and_async_equals_sync(
    case: OpCase, scenario: str, input_order: str
) -> None:
    """Both orchestrators give aligned rows, and the same rows keyed on record ids."""
    nodes = {}
    for orchestrator in ORCHESTRATORS:
        node, sources = run_case(case, scenario, input_order, orchestrator)
        nodes[orchestrator] = node
        failures = check_tables(case, node, sources, with_head=True)
        assert not failures, f"{orchestrator}: " + "\n".join(failures)
    if case.keyed_by_record_ids:
        sync, async_ = nodes["sync"], nodes["async"]
        assert_same_rows_by_identity(
            sync.as_table(all_info=True), async_.as_table(all_info=True)
        )
