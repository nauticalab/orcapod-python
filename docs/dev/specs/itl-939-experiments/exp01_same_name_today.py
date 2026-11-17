"""ITL-939 experiment 01: what Orcapod does TODAY when a tag and a data column share a name.

Throwaway spike script; public interface only. Run:

    uv run python superpowers/specs/itl-939-experiments/exp01_same_name_today.py

Every probe prints OK (with what it saw) or the exception type and message.
"""

from __future__ import annotations

import datetime as dt
import traceback

import pyarrow as pa

from orcapod import function_pod
from orcapod.operators import Join, MapData, MapTags, MergeJoin, SemiJoin
from orcapod.sources import ArrowTableSource

US = pa.timestamp("us", tz="UTC")
T0 = dt.datetime(2026, 1, 5, 9, 0, 0, tzinfo=dt.timezone.utc)
T1 = dt.datetime(2026, 1, 6, 9, 30, 0, tzinfo=dt.timezone.utc)


def probe(label: str, fn) -> None:
    """Run ``fn`` and print its result, or the exception and where Orcapod raised it."""
    print(f"\n--- {label}")
    try:
        out = fn()
        print("OK:", out)
    except Exception as exc:  # noqa: BLE001 - a probe reports every failure
        print(f"RAISES {type(exc).__name__}: {exc}")
        frames = traceback.extract_tb(exc.__traceback__)
        inner = [f for f in frames if "orcapod" in f.filename]
        if inner:
            f = inner[-1]
            print(f"   raised at {f.filename.split('src/')[-1]}:{f.lineno} ({f.name})")


def session_table() -> pa.Table:
    """Return a two-row EDI-shaped ``session`` table."""
    return pa.table(
        {
            "subject_id": pa.array(["m1", "m2"], pa.large_string()),
            "session_start_datetime": pa.array([T0, T1], US),
            "session_end_datetime": pa.array(
                [T0 + dt.timedelta(hours=2), T1 + dt.timedelta(hours=1)], US
            ),
        }
    )


# 1. An Arrow table that physically carries the name twice (Arrow allows duplicate
#    field names), with the name declared as a tag.
def dup_name_source():
    """Build a source from a table that holds ``session_start_datetime`` twice."""
    t = session_table()
    dup = pa.Table.from_arrays(
        [t["subject_id"], t["session_start_datetime"], t["session_start_datetime"]],
        names=["subject_id", "session_start_datetime", "session_start_datetime"],
    )
    src = ArrowTableSource(
        dup,
        tag_columns=["subject_id", "session_start_datetime"],
        source_id="session",
        infer_nullable=True,
    )
    return src.keys()


probe("1. ArrowTableSource on a table with a duplicated field name", dup_name_source)


# 2. tag_columns naming a column -> it simply leaves the data group.
def baseline():
    """Build the ``session`` source with its primary keys as tags."""
    src = ArrowTableSource(
        session_table(),
        tag_columns=["subject_id", "session_start_datetime"],
        source_id="session",
    )
    return src.keys()


probe("2. Baseline: a key column is a tag and nothing else", baseline)

base = ArrowTableSource(
    session_table(),
    tag_columns=["subject_id", "session_start_datetime"],
    source_id="session",
    infer_nullable=True,
)

# 3. MapData renaming a data column onto a tag name.
probe(
    "3. MapData renames data 'session_end_datetime' -> tag name 'session_start_datetime'",
    lambda: MapData({"session_end_datetime": "session_start_datetime"})(base).keys(),
)

# 4. MapTags renaming a tag onto a data name.
probe(
    "4. MapTags renames tag 'session_start_datetime' -> data name 'session_end_datetime'",
    lambda: MapTags({"session_start_datetime": "session_end_datetime"})(base).keys(),
)


# 5. A function pod whose output key equals a tag name.
@function_pod(output_keys="session_start_datetime")
def echo_end(session_end_datetime: dt.datetime) -> dt.datetime:
    """Return the end time under the output key ``session_start_datetime``."""
    return session_end_datetime


probe(
    "5. function pod output key == tag name: building the stream",
    lambda: echo_end.pod(base).keys(),
)
probe(
    "5b. ... and materialising it (as_table)",
    lambda: echo_end.pod(base).as_table().column_names,
)


# 6. Join: stream A has the name as a tag, stream B has the same name as data.
def b_stream():
    """Build a stream with ``session_start_datetime`` as a DATA column."""
    t = pa.table(
        {
            "subject_id": pa.array(["m1", "m2"], pa.large_string()),
            "session_start_datetime": pa.array([T0, T1], US),
            "weight_g": pa.array([25.0, 27.5], pa.float64()),
        }
    )
    return ArrowTableSource(
        t, tag_columns=["subject_id"], source_id="weights", infer_nullable=True
    )


probe(
    "6. Join(A[tag: session_start_datetime], B[data: session_start_datetime]) keys",
    lambda: Join()(base, b_stream()).keys(),
)
probe(
    "6b. ... and materialising it",
    lambda: Join()(base, b_stream()).as_table().column_names,
)
probe(
    "7. MergeJoin(A, B) with the same shapes",
    lambda: MergeJoin()(base, b_stream()).keys(),
)
probe(
    "7b. ... and materialising it",
    lambda: MergeJoin()(base, b_stream()).as_table().column_names,
)
probe(
    "8. SemiJoin(A, B) with the same shapes",
    lambda: SemiJoin()(base, b_stream()).keys(),
)
probe(
    "8b. ... and materialising it",
    lambda: SemiJoin()(base, b_stream()).as_table().column_names,
)


# 9. A pod parameter named after a tag: can a pod read a tag today?
@function_pod(output_keys="elapsed_s")
def elapsed(
    session_start_datetime: dt.datetime, session_end_datetime: dt.datetime
) -> float:
    """Return seconds from session start to session end."""
    return (session_end_datetime - session_start_datetime).total_seconds()


probe(
    "9. pod with a parameter named after a TAG column",
    lambda: elapsed.pod(base).keys(),
)


# 10. Per-row view: Tag and Data are separate datagrams, so a shared name may survive
#     iter_data() even though as_table() cannot hold it.
def iter_pod_rows():
    """Return the (tag, data) dicts of the pod whose output key is a tag name."""
    return [(t.as_dict(), d.as_dict()) for t, d in echo_end.pod(base).iter_data()]


probe("10. iter_data() of the pod whose output key == tag name", iter_pod_rows)


def iter_join_rows():
    """Return the (tag, data) dicts of the colliding Join."""
    return [(t.as_dict(), d.as_dict()) for t, d in Join()(base, b_stream()).iter_data()]


probe("10b. iter_data() of Join(A[tag], B[data]) sharing the name", iter_join_rows)
probe(
    "10c. output_schema() of that Join (graph-build time, no error)",
    lambda: Join()(base, b_stream()).output_schema(),
)
