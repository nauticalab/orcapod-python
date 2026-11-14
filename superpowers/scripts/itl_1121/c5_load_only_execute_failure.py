"""Comment 5: a sync load-only execute() whose input raises does not clear the node
(the async load-only branch and the full-run branch do)."""

import asyncio
from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.side_effects import tap_pod
from orcapod.sources import DictSource


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


@function_pod(output_keys="z")
def boom(y: int) -> int:
    raise RuntimeError("input failed")


def deliver(y: int, ctx: Any) -> None:
    pass


def build() -> PipelineJob:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        rows = inc.pod(
            DictSource(
                [{"k": i, "x": i} for i in range(3)], tag_columns=["k"], source_id="s"
            ),
            label="rowwise",
        )
        tap: Any = tap_pod(deliver)
        tapped: Any = tap.process(rows, label="tap")
        tapped.select_data_columns(["y"], label="down")
    return job


schema = pa.schema(
    [
        pa.field("k", pa.int64(), nullable=False),
        pa.field("y", pa.int64(), nullable=False),
    ]
)
failing = boom.pod(
    ArrowTableStream(pa.table({"k": [1], "y": [1]}, schema=schema), tag_columns=["k"])
)


class FailingChannel:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise RuntimeError("input failed")


for mode in ("sync full", "sync load_only", "async load_only"):
    job = build()
    job.run()
    tap, down = job.nodes["tap"], job.nodes["down"]
    try:
        if mode == "sync full":
            tap.execute(failing)
        elif mode == "sync load_only":
            tap.execute(failing, load_only=True)
        else:
            out = Channel()
            asyncio.run(
                tap.async_execute([FailingChannel()], out.writer, load_only=True)
            )
    except Exception as exc:
        print(f"{mode}: raised {type(exc).__name__}: {exc}")
    print(
        f"  tap: is_stale={tap.is_stale} rows={tap.as_table().num_rows}; "
        f"down (ran before): is_stale={down.is_stale} rows={down.as_table().num_rows}"
    )
