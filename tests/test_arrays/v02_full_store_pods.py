"""Pods, sources and job of the full v0.2 store fixture (ITL-954).

``data/v02_full_store.tar.gz`` was written by Orcapod v0.2.0 (tag ``v0.2.0``) running
``write_store`` below; ``data/make_v02_full_store.py`` regenerates it. It holds every
kind of table v0.2 stored: function pipeline and result tables, the tables of a
function pod that takes the invocation context, a side-effect log, an operator cache
and a cached source.

This module runs unchanged under v0.2.0 and under the current version. The module path
and the function names are part of every pod's identity, so they must stay as they are.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

JOB_NAME = "v02full"
ROWS = [{"k": "a", "n": 1}, {"k": "b", "n": 2}, {"k": "c", "n": 3}]

# What each function did, so a test can tell what ran again.
COMPUTED: list[int] = []
NOTIFIED: list[int] = []
DELIVERED: list[int] = []


def reset() -> None:
    """Forget what the pods did so far."""
    COMPUTED.clear()
    NOTIFIED.clear()
    DELIVERED.clear()


def deliver(m: int, ctx: Any) -> None:
    """The side effect of the sink."""
    DELIVERED.append(m)


def build_job(
    store: Any,
    *,
    cache_mode: Any = None,
    with_notify: bool = True,
    with_sink: bool = True,
) -> Any:
    """Build the fixture's job on ``store``.

    Nodes: ``plus_one`` and ``plus_two`` (function pods), ``notify`` (a function pod
    that takes the invocation context), ``select`` (an operator whose output is
    stored) and ``sink`` (a side-effect pod). The fixture was written with all of
    them.

    Args:
        store: The database the job stores its tables in.
        cache_mode: Cache mode of the ``select`` operator; ``LOG`` when ``None``.
        with_notify: Whether the job has the ``notify`` node.
        with_sink: Whether the job has the ``sink`` node.
    """
    from orcapod import PipelineJob, function_pod
    from orcapod.core.operators import SelectDataColumns
    from orcapod.side_effects import sink_pod
    from orcapod.sources import DictSource
    from orcapod.types import CacheMode

    @function_pod(output_keys="m")
    def plus_one(n: int) -> int:
        """Return ``n + 1`` and record ``n`` in ``COMPUTED``."""
        COMPUTED.append(n)
        return n + 1

    @function_pod(output_keys="p")
    def plus_two(m: int) -> int:
        """Return ``m + 2``."""
        return m + 2

    @function_pod(output_keys="sent", ctx_arg="ctx")
    def notify(m: int, ctx: Any) -> int:
        """Record ``m`` in ``NOTIFIED`` and return it; takes the invocation context."""
        NOTIFIED.append(m)
        return m

    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        ints = DictSource(data=ROWS, tag_columns=["k"], source_id="ints")
        ones = plus_one.pod(ints, label="plus_one")
        plus_two.pod(ones, label="plus_two")
        if with_notify:
            notify.pod(ones, label="notify")
        SelectDataColumns(["m"])(ones, label="select")
        if with_sink:
            sink: Any = sink_pod(deliver)
            sink.process(ones, label="sink")
    job.nodes["select"].attach_databases(
        pipeline_database=store.at(JOB_NAME),
        cache_mode=CacheMode.LOG if cache_mode is None else cache_mode,
    )
    return job


def build_cached_source(store: Any) -> Any:
    """Build the fixture's cached source, stored under the job's name in ``store``."""
    import pyarrow as pa

    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.sources import ArrowTableSource

    table = pa.table(
        {
            "k": pa.array(["a", "b"], type=pa.large_string()),
            "x": pa.array([1, 2], type=pa.int64()),
        }
    )
    return CachedSource(
        ArrowTableSource(table, tag_columns=["k"], source_id="fixed"),
        cache_database=store,
        cache_path_prefix=(JOB_NAME,),
    )


def write_store(store_root: Path) -> None:
    """Run the job and fill the cached source on a Delta store at ``store_root``."""
    from orcapod.databases import DeltaTableDatabase

    store = DeltaTableDatabase(base_path=store_root)
    build_job(store).run()
    build_cached_source(store).as_table()
    store.flush()
