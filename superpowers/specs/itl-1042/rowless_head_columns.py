"""Compare the columns of a head with no rows and a head with rows (ITL-1042, item 4).

Measurement kept for ITL-985 / ITL-1002 (coordinator decision of November 12, 2026: rowless
heads keep the declared columns for now). For each stream kind and column configuration it
prints the columns a rowless head lacks, or has with another type, compared with the head of
the same stream once it has rows, and the count of mismatches. Run with
``uv run python superpowers/specs/itl-1042/rowless_head_columns.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa

from orcapod import function_pod
from orcapod.core.operators import Batch, GroupBy, Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.streams import StreamHead
from orcapod.types import CacheMode

CONFIGS: dict[str, dict[str, Any]] = {
    "default": {},
    "content_hash": {"columns": {"content_hash": True}},
    "content_hash_named": {"columns": {"content_hash": "h"}},
    "source": {"columns": {"source": True}},
    "system_tags": {"columns": {"system_tags": True}},
    "context": {"columns": {"context": True}},
    "meta": {"columns": {"meta": True}},
    "all_info": {"all_info": True},
}


@function_pod(output_keys="y")
def scale(x: int) -> int:
    """Return ``2 * x``."""
    return 2 * x


def _table(columns: dict[str, list[int]]) -> pa.Table:
    """A table of non-nullable ``int64`` columns."""
    return pa.Table.from_arrays(
        [pa.array(values, type=pa.int64()) for values in columns.values()],
        schema=pa.schema(
            [pa.field(name, pa.int64(), nullable=False) for name in columns]
        ),
    )


def _sources() -> tuple[ArrowTableSource, ArrowTableSource]:
    """Sources ``a`` (tags ``id``, ``g``; data ``x``) and ``b`` (tag ``id``; data ``z``)."""
    a = ArrowTableSource(
        _table({"id": [3, 1, 2], "g": [0, 1, 0], "x": [30, 10, 20]}),
        tag_columns=["id", "g"],
        source_id="a",
        label="a",
    )
    b = ArrowTableSource(
        _table({"id": [2, 3, 1], "z": [200, 300, 100]}),
        tag_columns=["id"],
        source_id="b",
        label="b",
    )
    return a, b


def _job(store: InMemoryArrowDatabase) -> PipelineJob:
    """``scaled`` over ``a``, then ``joined`` (with ``b``, stored) and ``grouped``."""
    a, b = _sources()
    job = PipelineJob(name="r", database=store)
    with job:
        scaled = scale.pod(a, label="scaled")
        Join()(scaled, b, label="joined")
        GroupBy(by=["g"])(scaled, label="grouped")
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at("r"), cache_mode=CacheMode.LOG
    )
    return job


def _schema(head: StreamHead) -> dict[str, str]:
    """The head's columns and their Arrow types."""
    return {field.name: str(field.type) for field in head.table.schema}


def _report(kind: str, config: str, empty: StreamHead, full: StreamHead) -> bool:
    """Print how ``empty``'s columns differ from ``full``'s; True when they differ."""
    empty_schema, full_schema = _schema(empty), _schema(full)
    if empty_schema == full_schema:
        return False
    missing = [name for name in full_schema if name not in empty_schema]
    extra = [name for name in empty_schema if name not in full_schema]
    retyped = [
        (name, empty_schema[name], full_schema[name])
        for name in full_schema
        if name in empty_schema and empty_schema[name] != full_schema[name]
    ]
    print(
        f"{kind:18} {config:18} ({empty.status.value}) missing={missing} "
        f"extra={extra} retyped={retyped}"
    )
    return True


def main() -> None:
    """Print every mismatch and their count."""
    mismatches = 0
    before = _job(InMemoryArrowDatabase())
    after = _job(InMemoryArrowDatabase())
    after.run(orchestrator=SyncPipelineOrchestrator())
    for label in ("scaled", "joined", "grouped"):
        for config_name, config in CONFIGS.items():
            empty = before.nodes[label].head(10, **config)
            full = after.nodes[label].head(10, **config)
            mismatches += _report(f"node {label}", config_name, empty, full)

    a, b = _sources()
    streams: dict[str, Callable[[], Any]] = {
        "FunctionPodStream": lambda: scale.pod(a),
        "Join stream": lambda: Join()(scale.pod(a), b),
        "GroupBy stream": lambda: GroupBy(by=["g"])(scale.pod(a)),
        "Batch stream": lambda: Batch(2)(a),
    }
    for kind, make in streams.items():
        for config_name, config in CONFIGS.items():
            empty = make().head(10, **config)
            computed = make()
            _ = computed.as_table()
            full = computed.head(10, **config)
            mismatches += _report(kind, config_name, empty, full)
    print("mismatches", mismatches)

    print("--- source-info types: head with rows vs the zero-row as_table before a run")
    for label in ("joined", "grouped"):
        with_rows = after.nodes[label].head(10, all_info=True).table.schema
        zero_rows = before.nodes[label].as_table(all_info=True).schema
        print(
            label,
            [(f.name, str(f.type)) for f in with_rows if f.name.startswith("_source")],
        )
        print(
            label,
            [(f.name, str(f.type)) for f in zero_rows if f.name.startswith("_source")],
        )


if __name__ == "__main__":
    main()
