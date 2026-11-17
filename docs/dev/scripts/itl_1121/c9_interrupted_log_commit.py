"""PR #381 summary finding: a standalone side-effect job node (input outside a job, store
attached) holds its rows before _run_alone commits the invocation log. A KeyboardInterrupt
during that commit skips the Exception-only cleanup, so the node keeps a current run: the
next run() is held and never retries the commit."""

from typing import Any
from unittest import mock

import pyarrow as pa

from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.side_effects import SideEffectJobNode, tap_pod

DELIVERED: list[int] = []


def deliver(x: int, ctx: Any) -> None:
    DELIVERED.append(x)


schema = pa.schema(
    [
        pa.field("k", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ]
)
stream = ArrowTableStream(
    pa.table({"k": [1, 2], "x": [1, 2]}, schema=schema), tag_columns=["k"]
)
db = InMemoryArrowDatabase()
tap: Any = tap_pod(deliver)
node = SideEffectJobNode(tap, stream)
node.attach_databases(db)

with mock.patch.object(InMemoryArrowDatabase, "flush", side_effect=KeyboardInterrupt):
    try:
        node.run()
    except KeyboardInterrupt:
        print("run(): KeyboardInterrupt during the log commit")
print(
    f"after the interrupted commit: holds_current_run={node.holds_current_run(load_only=False)} is_stale={node.is_stale} delivered={len(DELIVERED)}"
)
flushes: list[int] = []
real_flush = InMemoryArrowDatabase.flush


def counting_flush(self: Any) -> None:
    flushes.append(1)
    real_flush(self)


with mock.patch.object(InMemoryArrowDatabase, "flush", counting_flush):
    node.run()
print(
    f"next run(): commits attempted={len(flushes)} delivered in total={len(DELIVERED)}"
)
