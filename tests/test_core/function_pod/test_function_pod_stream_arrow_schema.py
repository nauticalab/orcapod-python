"""``FunctionPodStream.as_table`` keeps every column of its input rows (ITL-834).

``FunctionPodStream.as_table`` builds its tag and data tables with the first
row's ``arrow_schema()``. A stale cached schema on a mutated dict-born row used
to make it silently drop the columns the mutation added.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pyarrow as pa

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams import Data, Tag
from orcapod.core.function_pod import FunctionPod
from orcapod.core.streams import ArrowTableStream


class _DictBornStream(ArrowTableStream):
    """Yields dict-born rows whose tag gained a column after its schema was cached."""

    def iter_data(self, *args: Any, **kwargs: Any) -> Iterator[tuple[Tag, Data]]:
        """Yield dict-born rows, each tag extended after its schema was cached."""
        for row in self.as_table().to_pylist():
            tag = Tag({"id": row["id"]})
            _ = tag.arrow_schema(all_info=True)  # cache the schema
            tag = tag.with_columns(extra=row["extra"])
            yield tag, Data({"x": row["x"]})


def _inc(x: int) -> int:
    return x + 1


def test_function_pod_stream_keeps_column_added_by_with_columns():
    """A tag column added by ``with_columns`` survives ``FunctionPodStream.as_table``."""
    table = pa.table({"id": [1, 2], "extra": [10, 20], "x": [5, 6]})
    table = table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))
    stream = _DictBornStream(table, tag_columns=["id", "extra"])
    function = PythonDataFunction(_inc, output_keys="y")
    out = FunctionPod(function)(stream).as_table()  # pyright: ignore[reportArgumentType]
    assert out.column_names == ["id", "extra", "y"]
    assert out.to_pylist() == [
        {"id": 1, "extra": 10, "y": 6},
        {"id": 2, "extra": 20, "y": 7},
    ]
