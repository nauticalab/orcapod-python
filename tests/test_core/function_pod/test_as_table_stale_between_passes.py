"""``as_table`` returns the current rows with their own content hashes (ITL-1045).

A table is built from one pass over the rows and its content hashes from another. If the
upstream is reset between a cached ``as_table()`` and ``as_table(content_hash=True)``, the
call must return the rows a fresh read gives, each with the hash of its own data.
"""

from __future__ import annotations

import itertools
from typing import Any, cast

import pyarrow as pa

from orcapod import function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource

COUNTER = itertools.count()


@function_pod(output_keys="y")
def stamp(x: int) -> int:
    """Not deterministic: a call counter."""
    return 100 * next(COUNTER) + x


@function_pod(output_keys="z")
def plus_one(y: int) -> int:
    """Return ``y + 1``."""
    return y + 1


def source() -> ArrowTableSource:
    """Three rows, not in tag order."""
    table = pa.table(
        {"k": ["c", "a", "b"], "x": [3, 1, 2]},
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["k"], source_id="src", tag_only_columns=True
    )


def chain(cached: bool = False) -> tuple[Any, Any]:
    s1: Any = stamp.pod(source())
    if cached:
        pod = CachedFunctionPod(
            function_pod=plus_one.pod,  # pyright: ignore[reportArgumentType]  # FunctionPod lacks from_config, as in CachedFunctionPod.process
            result_database=InMemoryArrowDatabase(),
        )
        return s1, pod.process(s1)
    return s1, plus_one.pod(s1)


def assert_rows_carry_their_own_hash(stream: Any, table: pa.Table) -> None:
    hasher = stream.data_context.arrow_hasher
    for i in range(table.num_rows):
        own = hasher.hash_table(table.slice(i, 1).select(["z"])).to_string()
        assert table.column("_content_hash")[i].as_py() == own, f"row {i}"


def by_tag(table: pa.Table) -> dict[str, int]:
    keys = cast("list[str]", table.column("k").to_pylist())
    values = cast("list[int]", table.column("z").to_pylist())
    return dict(zip(keys, values, strict=True))


def test_stale_between_two_calls_returns_current_rows_with_their_own_hashes() -> None:
    s1, s2 = chain()
    s2.as_table()
    s1.clear_cache()
    assert s2.is_stale

    table = s2.as_table(columns={"content_hash": True})

    assert_rows_carry_their_own_hash(s2, table)
    # the rows are the current ones: a read now gives the same values
    assert by_tag(table) == by_tag(s2.as_table())


def test_cached_pod_stream_stale_between_two_calls() -> None:
    s1, s2 = chain(cached=True)
    s2.as_table()
    s1.clear_cache()

    table = s2.as_table(columns={"content_hash": True})

    assert_rows_carry_their_own_hash(s2, table)
    assert by_tag(table) == by_tag(s2.as_table())


def test_head_and_iter_data_show_the_current_rows_after_the_reset() -> None:
    s1, s2 = chain()
    s2.as_table()
    s1.clear_cache()
    table = s2.as_table(columns={"content_hash": True})
    current = by_tag(table)

    s1.clear_cache()
    rows = {t.as_dict()["k"]: d.as_dict()["z"] for t, d in s2.iter_data()}
    assert rows == by_tag(s2.as_table())
    assert rows != current  # the reset recomputed: the counter moved on

    s1.clear_cache()
    head = s2.head(10, columns={"content_hash": True})
    assert head.table.num_rows in (0, 3)
    if head.table.num_rows:
        assert_rows_carry_their_own_hash(s2, head.table)


def test_job_node_stale_between_two_calls_keeps_rows_and_hashes_aligned() -> None:
    from orcapod import PipelineJob

    job = PipelineJob(name="itl1045", database=InMemoryArrowDatabase())
    with job:
        plus_one.pod(stamp.pod(source(), label="st"), label="n")
    _ = job.run()
    node = job.nodes["n"]
    _ = node.as_table()
    job.nodes["st"].clear_cache()  # public: the node goes stale
    assert node.is_stale

    table = node.as_table(columns={"content_hash": True})

    assert_rows_carry_their_own_hash(node, table)
    assert by_tag(table) == by_tag(node.as_table())
