"""An operator stream recomputes after an upstream changes (ITL-991, finding F1).

``DynamicPodStream`` keeps the result of its operator. When an upstream is modified after
that, the kept result is stale and the next read must run the operator again.
"""

from __future__ import annotations

from unittest import mock

import pyarrow as pa

from orcapod import function_pod
from orcapod.core.function_pod import FunctionPodStream
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources import ArrowTableSource


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Double ``x``."""
    return 2 * x


def _source() -> ArrowTableSource:
    table = pa.table(
        {"id": [1, 2, 3], "x": [1, 2, 3]},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["id"], tag_only_columns=True, source_id="s"
    )


def test_operator_stream_reruns_after_upstream_is_modified() -> None:
    """A kept operator result is recomputed once an upstream changed."""
    fn_stream = double.pod(_source())
    assert isinstance(fn_stream, FunctionPodStream)
    out = SelectDataColumns(["y"])(fn_stream)
    original = SelectDataColumns.static_process
    # A spy on the non-public static_process: the test is about whether it runs again.
    with mock.patch.object(
        SelectDataColumns, "static_process", autospec=True, side_effect=original
    ) as spy:
        first = out.as_table()
        assert spy.call_count == 1
        fn_stream.clear_cache()  # the upstream is modified after the result was kept
        second = out.as_table()
        assert spy.call_count == 2
    assert first.equals(second)
