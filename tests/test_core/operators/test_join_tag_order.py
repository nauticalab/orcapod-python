"""Column order produced by Join and MergeJoin.

Both operators are commutative (``argument_symmetry`` returns a ``frozenset``), so
their column order must not depend on the order of the operands: tag and data
columns are listed by first appearance across the inputs taken in canonical
``pipeline_hash`` order. The order must also not depend on ``PYTHONHASHSEED``:
both operators used to build ``tag_columns`` from a ``set``, and rows loaded
from a ``FunctionJobNode`` cache use the compile-time order, so a buffer mixing
cached and freshly computed rows could not be concatenated (ITL-817).

Each check runs in a subprocess so that ``PYTHONHASHSEED`` can vary.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.operators import Join, MergeJoin
from orcapod.core.operators.base import BinaryOperator, NonZeroInputOperator
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol

_SCRIPT = """
import json
import pyarrow as pa
from orcapod.core.operators import Join, MergeJoin
from orcapod.core.sources.arrow_table_source import ArrowTableSource

def source(columns, tags, source_id):
    return ArrowTableSource(pa.table(columns), tag_columns=tags, tag_only_columns=True, source_id=source_id)

def describe(operator, first, second):
    stream = operator(first, second)
    static = operator.static_process(first, second)
    tag, data = next(iter(stream.iter_data()))
    table = stream.as_table(all_info=True)
    rows = sorted(json.dumps(row, sort_keys=True, default=str) for row in table.to_pylist())
    predicted = operator.output_schema(first, second)
    predicted_sys = operator.output_schema(first, second, columns={{"system_tags": True}})
    actual = static.output_schema()
    actual_sys = static.output_schema(columns={{"system_tags": True}})
    return {{
        "keys": [list(k) for k in stream.keys()],
        "schema": [list(s) for s in predicted],
        "schema_system_tags": [list(s) for s in predicted_sys],
        "static_schema": [list(s) for s in actual],
        "static_schema_system_tags": [list(s) for s in actual_sys],
        "table": table.column_names,
        "visible_table": stream.as_table().column_names,
        "row_tag": list(tag.keys()),
        "row_data": list(data.keys()),
        "rows": rows,
    }}

operator = {operator}
a = source({a_columns}, ["probe"], "a")
b = source({b_columns}, ["method"], "b")
print(json.dumps({{"ab": describe(operator, a, b), "ba": describe(operator, b, a)}}))
"""


@dataclass(frozen=True)
class _Case:
    """Operator under test and the columns of its two input sources."""

    operator: str
    a_columns: dict[str, list[int]]
    b_columns: dict[str, list[int]]

    def make_operator(self) -> BinaryOperator | NonZeroInputOperator:
        return Join() if self.operator == "Join()" else MergeJoin()


_CASES = {
    "join": _Case(
        operator="Join()",
        a_columns={"probe": [0, 1], "x": [1, 2]},
        b_columns={"method": [0, 1], "w": [3, 4]},
    ),
    "merge_join": _Case(
        operator="MergeJoin()",
        a_columns={"probe": [0, 1], "x": [1, 2], "y": [5, 6]},
        b_columns={"method": [0, 1], "x": [3, 4], "z": [7, 8]},
    ),
}

_SEEDS = range(1, 7)

# {"ab": description of op(a, b), "ba": description of op(b, a)}; each
# description maps a field name to a JSON value (see ``_SCRIPT``).
Description = dict[str, dict[str, object]]


def _describe(case: str, seed: int) -> Description:
    """Run the description script for ``case`` under ``PYTHONHASHSEED=seed``."""
    spec = _CASES[case]
    env = {**os.environ, "PYTHONHASHSEED": str(seed)}
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _SCRIPT.format(
                operator=spec.operator,
                a_columns=spec.a_columns,
                b_columns=spec.b_columns,
            ),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    # json.loads is typed as returning Any; the script prints a Description.
    return json.loads(result.stdout.strip().splitlines()[-1])  # pyright: ignore[reportAny]


@pytest.fixture(scope="module", params=sorted(_CASES))
def descriptions(request: pytest.FixtureRequest) -> dict[int, Description]:
    """Descriptions of ``op(a, b)`` and ``op(b, a)`` for every seed, per operator."""
    case = cast(str, request.param)
    return {seed: _describe(case, seed) for seed in _SEEDS}


def test_operand_order_does_not_change_columns(
    descriptions: dict[int, Description],
) -> None:
    """``op(a, b)`` and ``op(b, a)`` agree on every column order and on the rows."""
    for seed, result in descriptions.items():
        for field, value in result["ab"].items():
            assert result["ba"][field] == value, f"PYTHONHASHSEED={seed} {field}"


def test_column_order_is_independent_of_hash_seed(
    descriptions: dict[int, Description],
) -> None:
    """Every process lists the columns in the same order."""
    first = descriptions[_SEEDS[0]]
    for seed, result in descriptions.items():
        assert result == first, f"PYTHONHASHSEED={seed}"


def test_prediction_matches_static_process(
    descriptions: dict[int, Description],
) -> None:
    """The predicted schema order is the order ``static_process`` emits."""
    for seed, result in descriptions.items():
        for order in ("ab", "ba"):
            described = result[order]
            context = f"PYTHONHASHSEED={seed} {order}"
            assert described["schema"] == described["static_schema"], context
            assert (
                described["schema_system_tags"]
                == described["static_schema_system_tags"]
            ), context
            assert described["keys"] == described["schema"], context
            tags, data = cast(list[list[str]], described["schema"])
            assert described["row_tag"] == tags, context
            assert described["row_data"] == data, context
            assert described["visible_table"] == tags + data, context


# ---------------------------------------------------------------------------
# Async execution
# ---------------------------------------------------------------------------

Row = tuple[TagProtocol, DataProtocol]


def _source(
    columns: dict[str, list[int]], tag: str, source_id: str
) -> ArrowTableSource:
    return ArrowTableSource(
        pa.Table.from_pydict(columns),
        tag_columns=[tag],
        tag_only_columns=True,
        source_id=source_id,
    )


async def _async_first_row(
    operator: BinaryOperator | NonZeroInputOperator,
    first: ArrowTableSource,
    second: ArrowTableSource,
) -> list[list[str]]:
    """Run ``operator.async_execute`` on two inputs and return the first row's keys."""
    channels: list[Channel[Row]] = [Channel(buffer_size=16), Channel(buffer_size=16)]
    output: Channel[Row] = Channel(buffer_size=16)
    for stream, channel in zip((first, second), channels):
        for row in stream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    await operator.async_execute(
        [channel.reader for channel in channels],
        output.writer,
        input_pipeline_hashes=[first.pipeline_hash(), second.pipeline_hash()],
    )
    rows = await output.reader.collect()
    tag, data = rows[0]
    return [list(tag.keys()), list(data.keys())]


@pytest.mark.parametrize("case", sorted(_CASES))
def test_async_matches_sync_column_order(case: str) -> None:
    """The async two-input path lists columns in the sync stream's order."""
    spec = _CASES[case]
    operator = spec.make_operator()
    a = _source(spec.a_columns, "probe", "a")
    b = _source(spec.b_columns, "method", "b")
    expected = [list(keys) for keys in operator.process(a, b).keys()]
    for first, second in ((a, b), (b, a)):
        assert asyncio.run(_async_first_row(operator, first, second)) == expected
