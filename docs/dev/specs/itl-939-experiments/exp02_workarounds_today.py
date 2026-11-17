"""ITL-939 experiment 02: every way to get a tag value into a function pod TODAY.

Throwaway spike script; public interface only. EDI-shaped fixtures:

* ``session``    keys (subject_id, session_start_datetime); data session_end_datetime, booth
* ``experiment`` keys (subject_id, session_start_datetime, experiment_index);
  data experiment_start_timestamp (u64, microseconds since the epoch), task_id

Subject m1 has two sessions, each with experiment_index 0, so experiment_index alone does
not identify an experiment (as in the real EDI tables).

The computation: elapsed seconds from session start to experiment start, then a Join with
``session`` on the shared keys, as a workshop pipeline would do.

Run:

    uv run python superpowers/specs/itl-939-experiments/exp02_workarounds_today.py
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa

from orcapod import function_pod
from orcapod.operators import Join, MapData
from orcapod.sources import ArrowTableSource

UTC = dt.timezone.utc
TS = pa.timestamp("us", tz="UTC")
S1 = dt.datetime(2026, 1, 5, 9, 0, tzinfo=UTC)
S2 = dt.datetime(2026, 1, 6, 9, 30, tzinfo=UTC)
S3 = dt.datetime(2026, 1, 5, 14, 0, tzinfo=UTC)


def us(t: dt.datetime) -> int:
    """Return ``t`` as microseconds since the epoch."""
    return int(t.timestamp() * 1_000_000)


def session_table() -> pa.Table:
    """Return the EDI-shaped ``session`` table (m1 has two sessions)."""
    return pa.table(
        {
            "subject_id": pa.array(["m1", "m1", "m2"], pa.large_string()),
            "session_start_datetime": pa.array([S1, S2, S3], TS),
            "session_end_datetime": pa.array(
                [
                    S1 + dt.timedelta(hours=2),
                    S2 + dt.timedelta(hours=2),
                    S3 + dt.timedelta(hours=1),
                ],
                TS,
            ),
            "booth": pa.array(["A", "B", "A"], pa.large_string()),
        }
    )


def experiment_table() -> pa.Table:
    """Return the EDI-shaped ``experiment`` table (two experiments per session)."""
    starts = [S1, S1, S2, S2, S3]
    offsets_s = [60, 900, 120, 1500, 30]
    return pa.table(
        {
            "subject_id": pa.array(["m1", "m1", "m1", "m1", "m2"], pa.large_string()),
            "session_start_datetime": pa.array(starts, TS),
            "experiment_index": pa.array([0, 1, 0, 1, 0], pa.uint32()),
            "experiment_start_timestamp": pa.array(
                [us(s) + o * 1_000_000 for s, o in zip(starts, offsets_s)], pa.uint64()
            ),
            "task_id": pa.array(["fv", "fv", "fv", "gz", "fv"], pa.large_string()),
        }
    )


KEYS_SESSION = ["subject_id", "session_start_datetime"]
KEYS_EXPERIMENT = ["subject_id", "session_start_datetime", "experiment_index"]


def src(table: pa.Table, tags: list[str], source_id: str) -> ArrowTableSource:
    """Build an ``ArrowTableSource`` with nullability inferred from the data."""
    return ArrowTableSource(
        table, tag_columns=tags, source_id=source_id, infer_nullable=True
    )


session = src(session_table(), KEYS_SESSION, "session")


def show(label: str, stream) -> None:
    """Print a stream's keys, system-tag columns and hashes."""
    print(f"\n=== {label}")
    tags, data = stream.keys()
    print("keys():", tags, "|", data)
    sys_cols = [
        c for c in stream.as_table(all_info=True).column_names if c.startswith("_tag_")
    ]
    print("system tag columns:", sys_cols)
    print("pipeline_hash:", stream.pipeline_hash().to_hex(16))
    print("content_hash: ", stream.content_hash().to_hex(16))


def rows(stream, cols: list[str]) -> None:
    """Print the given columns of a stream as a table."""
    df = stream.as_table().select(cols).to_pandas()
    print(df.to_string(index=False))


# --- baseline: the DB-source default (primary keys become the tags) --------------------
baseline = src(experiment_table(), KEYS_EXPERIMENT, "experiment")
show("baseline: experiment, primary keys as tags", baseline)


# ----------------------------------------------------------------------------------------
# W1. Choose tag_columns so session_start_datetime stays data.
# ----------------------------------------------------------------------------------------
@function_pod(output_keys="elapsed_s")
def elapsed_s(
    session_start_datetime: dt.datetime, experiment_start_timestamp: int
) -> float:
    """Return seconds from session start (read as data) to experiment start."""
    return experiment_start_timestamp / 1e6 - session_start_datetime.timestamp()


w1_src = src(experiment_table(), ["subject_id", "experiment_index"], "experiment")
show("W1 source: tag_columns without session_start_datetime", w1_src)
w1 = elapsed_s.pod(
    w1_src.select_data_columns(["session_start_datetime", "experiment_start_timestamp"])
)
print("W1 pod output:")
rows(w1, ["subject_id", "experiment_index", "elapsed_s"])
# Downstream: Join with session on the shared keys. W1's tags lack session_start_datetime.
try:
    j = Join()(w1, session)
    print("W1 Join keys():", j.keys())
    print(
        "W1 Join rows:", j.as_table().num_rows, "(5 experiments; correct answer is 5)"
    )
    rows(j, ["subject_id", "experiment_index", "session_start_datetime", "elapsed_s"])
except Exception as exc:  # noqa: BLE001
    print(f"W1 Join RAISES {type(exc).__name__}: {exc}")
# The pod's output no longer carries session_start_datetime at all, so the result has lost
# the session identity: m1 experiment 0 appears twice with nothing to tell them apart.


# ----------------------------------------------------------------------------------------
# W2. Duplicate the column under another name BEFORE building the source.
#     (A DB source such as SpiralDBTableSource runs SELECT * with no column hook, so for a
#     DB table this means: read it, copy the column, build an ArrowTableSource.)
# ----------------------------------------------------------------------------------------
@function_pod(output_keys="elapsed_s")
def elapsed_s2(session_start: dt.datetime, experiment_start_timestamp: int) -> float:
    """Return seconds from the copied session start to experiment start."""
    return experiment_start_timestamp / 1e6 - session_start.timestamp()


t = experiment_table()
t = t.append_column("session_start", t["session_start_datetime"])  # the one extra line
w2_src = src(t, KEYS_EXPERIMENT, "experiment")
show("W2 source: column copied to 'session_start' before the source", w2_src)
w2 = elapsed_s2.pod(
    w2_src.select_data_columns(["session_start", "experiment_start_timestamp"])
)
print("W2 pod output:")
rows(w2, ["subject_id", "session_start_datetime", "experiment_index", "elapsed_s"])
j2 = Join()(w2, session)
print("W2 Join keys():", j2.keys())
print("W2 Join rows:", j2.as_table().num_rows, "(correct answer is 5)")


# ----------------------------------------------------------------------------------------
# W2'. Same, but mid-pipeline (after an operator), by re-rooting: as_table() + new source.
# ----------------------------------------------------------------------------------------
mid = Join()(baseline, session)  # some upstream work
mt = mid.as_table()
mt = mt.append_column("session_start", mt["session_start_datetime"])
w2b_src = ArrowTableSource(
    mt, tag_columns=KEYS_EXPERIMENT, source_id="rerooted", infer_nullable=True
)
show("W2' re-rooted mid-pipeline (lineage to 'experiment'/'session' is cut)", w2b_src)
print("W2' upstreams of the new source:", w2b_src.upstreams)


# ----------------------------------------------------------------------------------------
# W3. Read the source twice: once keyed normally, once with the column as data, then join.
# ----------------------------------------------------------------------------------------
second = src(
    experiment_table(), ["subject_id", "experiment_index"], "experiment_as_data"
)
second = MapData({"session_start_datetime": "session_start"}, drop_unmapped=True)(
    second
)
try:
    j3 = Join()(baseline, second)
    print("\n=== W3: Join(keys-as-tags read, column-as-data read)")
    print("W3 Join keys():", j3.keys())
    print(
        "W3 Join rows:",
        j3.as_table().num_rows,
        "(5 experiments; >5 means a wrong pairing)",
    )
    rows(
        j3,
        ["subject_id", "session_start_datetime", "experiment_index", "session_start"],
    )
except Exception as exc:  # noqa: BLE001
    print(f"W3 Join RAISES {type(exc).__name__}: {exc}")


# ----------------------------------------------------------------------------------------
# W4. Operators that could copy a tag into data today? None: MapTags renames (moves) a tag
#     within the tag group; MapData renames within the data group; Select/Drop only remove;
#     Pick/Index read data columns only; a pod never receives tags.
# ----------------------------------------------------------------------------------------
from orcapod.operators import MapTags  # noqa: E402

try:
    MapTags({"session_start_datetime": "session_start"})(baseline).keys()
    print(
        "\nW4 MapTags keys():",
        MapTags({"session_start_datetime": "session_start"})(baseline).keys(),
    )
except Exception as exc:  # noqa: BLE001
    print(f"W4 MapTags RAISES {type(exc).__name__}: {exc}")
