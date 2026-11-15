"""Sources and column configurations shared by the row-alignment tests (ITL-1034).

Three tie scenarios of six rows, each given in three input orders (``INPUT_ORDERS``):
shuffled (neither tag order nor data order, the default), tag-sorted, and reverse
tag-sorted. The tie scenarios:

* ``unique``: every tag tuple appears once;
* ``duplicate``: tag tuples repeat (ties under ``sort_by_tags``);
* ``null``: tags are null in some rows, and repeat.

Every data value is unique within its column and across sources sharing the column, so a
value names its origin row. Source ``A`` identifies rows by ``record_id_column="x"``; source
``C`` by position (``row_<i>`` tokens), so both provenance token forms are covered.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from orcapod.sources import ArrowTableSource
from orcapod.system_constants import constants

from .oracle import Oracle

TAG_COLUMNS = ("g", "n")

# Each scenario's rows of A as (g, n, x), in a "shuffled" order that is neither tag order
# nor data order.
SCENARIOS: dict[str, list[tuple[str | None, int | None, int]]] = {
    "unique": [
        ("d", 4, 5),
        ("b", 2, 2),
        ("f", 6, 0),
        ("a", 1, 4),
        ("e", 5, 1),
        ("c", 3, 3),
    ],
    "duplicate": [
        ("b", 1, 5),
        ("a", 1, 2),
        ("b", 1, 0),
        ("a", 2, 4),
        ("c", 1, 1),
        ("b", 1, 3),
    ],
    "null": [
        (None, 1, 5),
        ("a", None, 2),
        (None, 1, 0),
        ("b", 2, 4),
        (None, None, 1),
        ("a", 1, 3),
    ],
}

# The order the input rows are given in. Existing tests almost always feed rows already in
# tag order, which hides any order assumption; here "shuffled" is the default.
INPUT_ORDERS = ("shuffled", "sorted", "reversed")


def _tag_sort_key(row: tuple[str | None, int | None, int]) -> tuple[Any, ...]:
    """Tag order, nulls first, then the data key (a stable order for tied tags)."""
    g, n, x = row
    return (g is not None, g or "", n is not None, n or 0, x)


def rows_a(scenario: str, input_order: str) -> list[tuple[str | None, int | None, int]]:
    """The rows of A of ``scenario`` in ``input_order``."""
    rows = SCENARIOS[scenario]
    if input_order == "shuffled":
        return list(rows)
    ordered = sorted(rows, key=_tag_sort_key)
    if input_order == "sorted":
        return ordered
    if input_order == "reversed":
        return list(reversed(ordered))
    raise ValueError(f"unknown input order {input_order!r}")


def keys_a(scenario: str, input_order: str) -> list[int]:
    """The data key ``x`` of A's rows in input order: the keys of the completion orders."""
    return [x for _, _, x in rows_a(scenario, input_order)]


TAG_FIELDS = [pa.field("g", pa.large_string()), pa.field("n", pa.int64())]


def table_a(scenario: str, input_order: str = "shuffled") -> pa.Table:
    """Source A: tags ``g``, ``n``; data ``x`` (unique), ``s`` (unique), ``m`` (nullable)."""
    rows = rows_a(scenario, input_order)
    xs = [x for _, _, x in rows]
    return pa.table(
        {
            "g": [g for g, _, _ in rows],
            "n": [n for _, n, _ in rows],
            "x": xs,
            "s": [f"s{x}" for x in xs],
            "m": [None if x % 2 else 1000 + x for x in xs],
        },
        schema=pa.schema(
            [
                *TAG_FIELDS,
                pa.field("x", pa.int64(), nullable=False),
                pa.field("s", pa.large_string(), nullable=False),
                pa.field("m", pa.int64()),
            ]
        ),
    )


def table_b(scenario: str) -> pa.Table:
    """Source B: the tags of A in another order; data ``z`` (unique)."""
    rows = SCENARIOS[scenario]
    order = [3, 0, 5, 1, 4, 2]
    return pa.table(
        {
            "g": [rows[i][0] for i in order],
            "n": [rows[i][1] for i in order],
            "z": [100 + i for i in order],
        },
        schema=pa.schema([*TAG_FIELDS, pa.field("z", pa.int64(), nullable=False)]),
    )


def table_c(scenario: str) -> pa.Table:
    """Source C: the tags of A reversed; data ``x`` from another range, positional ids."""
    rows = SCENARIOS[scenario]
    order = [5, 4, 3, 2, 1, 0]
    return pa.table(
        {
            "g": [rows[i][0] for i in order],
            "n": [rows[i][1] for i in order],
            "x": [200 + i for i in order],
        },
        schema=pa.schema([*TAG_FIELDS, pa.field("x", pa.int64(), nullable=False)]),
    )


DICT_TYPE = pa.large_list(
    pa.struct(
        [
            pa.field("key", pa.large_string(), nullable=False),
            pa.field("value", pa.int64()),
        ]
    )
)


def table_d(scenario: str, input_order: str = "shuffled") -> pa.Table:
    """Source D (for ``Pick`` and ``Index``): A's tags; ``x`` from another range, a list
    column ``seq`` and a ``dict[str, int]`` column ``d``."""
    rows = rows_a(scenario, input_order)
    xs = [300 + x for _, _, x in rows]
    return pa.table(
        {
            "g": [g for g, _, _ in rows],
            "n": [n for _, n, _ in rows],
            "x": xs,
            "seq": [[x, x + 1] for x in xs],
            "d": pa.array([[{"key": "p", "value": x}] for x in xs], type=DICT_TYPE),
        },
        schema=pa.schema(
            [
                *TAG_FIELDS,
                pa.field("x", pa.int64(), nullable=False),
                pa.field("seq", pa.large_list(pa.int64()), nullable=False),
                pa.field("d", DICT_TYPE, nullable=False),
            ]
        ),
    )


@dataclass
class Sources:
    """The four sources of a scenario and the oracle that knows their rows."""

    a: ArrowTableSource
    b: ArrowTableSource
    c: ArrowTableSource
    d: ArrowTableSource
    oracle: Oracle
    oracle_tables: dict[str, pa.Table] = field(default_factory=dict)
    expected_source: str | None = None


def schema_hash_of(source: Any) -> str:
    """The schema hash naming the system tag columns of the root ``source``.

    Read from the source's own source-id column, ``_tag_source_id::<schema hash>``: the
    base every downstream column holding its rows extends.
    """
    prefix = constants.SYSTEM_TAG_SOURCE_ID_PREFIX + constants.BLOCK_SEPARATOR
    tags, _ = source.keys(columns={"system_tags": True})
    (name,) = [t for t in tags if t.startswith(prefix)]
    return name[len(prefix) :]


def make_sources(scenario: str, input_order: str = "shuffled") -> Sources:
    """Build sources A, B and C of ``scenario`` and register their rows in an oracle.

    ``input_order`` orders the rows of A; B and C keep their own orders.
    """
    oracle = Oracle()
    built: dict[str, ArrowTableSource] = {}
    tables: dict[str, pa.Table] = {}
    for name, table, record_id_column in (
        ("A", table_a(scenario, input_order), "x"),
        ("B", table_b(scenario), "z"),
        ("C", table_c(scenario), None),
        ("D", table_d(scenario, input_order), "x"),
    ):
        built[name] = ArrowTableSource(
            table,
            tag_columns=list(TAG_COLUMNS),
            source_id=name,
            record_id_column=record_id_column,
        )
        oracle.add_source(
            name,
            table,
            tag_columns=TAG_COLUMNS,
            schema_hash=schema_hash_of(built[name]),
            record_id_column=record_id_column,
        )
        tables[name] = table
    return Sources(
        a=built["A"],
        b=built["B"],
        c=built["C"],
        d=built["D"],
        oracle=oracle,
        oracle_tables=tables,
    )


ALL_GROUPS = {
    "meta": True,
    "context": True,
    "source": True,
    "system_tags": True,
    "content_hash": True,
}

# Every ColumnConfig that changes which columns a table has, their order, or its row order.
COLUMN_CONFIGS: dict[str, dict[str, Any]] = {
    "default": {},
    "sort_by_tags": {"columns": {"sort_by_tags": True}},
    "content_hash": {"columns": {"content_hash": True}},
    "content_hash_named": {"columns": {"content_hash": "h"}},
    "content_hash_sorted": {"columns": {"content_hash": True, "sort_by_tags": True}},
    "source": {"columns": {"source": True}},
    "system_tags": {"columns": {"system_tags": True}},
    "context": {"columns": {"context": True}},
    "meta": {"columns": {"meta": True}},
    "all_groups_unsorted": {"columns": {**ALL_GROUPS, "sort_by_tags": False}},
    "all_info": {"all_info": True},
}


def content_hash_column(config: dict[str, Any]) -> str:
    """The name of the content hash column ``config`` asks for (default when none)."""
    value = config.get("columns", {}).get("content_hash")
    return value if isinstance(value, str) else "_content_hash"
