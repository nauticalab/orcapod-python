"""ITL-939 experiment 04: the inverse direction, a DATA column wanted as a TAG (foreign key).

EDI ``penetration`` is keyed (subject_id, session_start_datetime, recording_index,
probe_index) and carries ``probe_id`` as data; ``probe`` is keyed by ``probe_id``. Joining
them needs ``probe_id`` as a tag on both sides. Throwaway spike script, public interface only.

Run:

    uv run python superpowers/specs/itl-939-experiments/exp04_foreign_key_promotion.py
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pa

from orcapod import function_pod
from orcapod.operators import Join
from orcapod.sources import ArrowTableSource

UTC = dt.timezone.utc
TS = pa.timestamp("us", tz="UTC")
S1 = dt.datetime(2026, 1, 5, 9, 0, tzinfo=UTC)

penetration = pa.table(
    {
        "subject_id": pa.array(["m1", "m1"], pa.large_string()),
        "session_start_datetime": pa.array([S1, S1], TS),
        "recording_index": pa.array([0, 0], pa.uint32()),
        "probe_index": pa.array([0, 1], pa.uint32()),
        "probe_id": pa.array(["P-17", "P-22"], pa.large_string()),
        "probe_plan_chamber_z_um": pa.array([3100.0, 2950.0], pa.float64()),
    }
)
probe = pa.table(
    {
        "probe_id": pa.array(["P-17", "P-22"], pa.large_string()),
        "probe_model": pa.array(["NP1.0", "NP2.0"], pa.large_string()),
    }
)
PK = ["subject_id", "session_start_datetime", "recording_index", "probe_index"]

pen_pk = ArrowTableSource(
    penetration, tag_columns=PK, source_id="penetration", infer_nullable=True
)
probe_src = ArrowTableSource(
    probe, tag_columns=["probe_id"], source_id="probe", infer_nullable=True
)

print("=== F1. primary keys only: Join(penetration, probe) shares no tag")
try:
    j = Join()(pen_pk, probe_src)
    print("keys():", j.keys())
    print("rows:", j.as_table().num_rows, "(2 penetrations; 4 means a cross product)")
except Exception as exc:  # noqa: BLE001
    print(f"RAISES {type(exc).__name__}: {exc}")

print("\n=== F2. today's recipe: list probe_id among the tag_columns at the source")
pen_fk = ArrowTableSource(
    penetration,
    tag_columns=[*PK, "probe_id"],
    source_id="penetration",
    infer_nullable=True,
)
j2 = Join()(pen_fk, probe_src)
print("keys():", j2.keys())
print("rows:", j2.as_table().num_rows, "(correct answer is 2)")


print("\n=== F3. after a pod, a data column cannot become a tag (no operator does it)")


@function_pod(output_keys=["probe_id", "depth_mm"])
def plan(probe_id: str, probe_plan_chamber_z_um: float) -> tuple[str, float]:
    """Pass the probe id through and convert the planned depth to millimetres."""
    return probe_id, probe_plan_chamber_z_um / 1000.0


out = plan.pod(pen_pk.select_data_columns(["probe_id", "probe_plan_chamber_z_um"]))
print("pod output keys():", out.keys())
print("Join(pod output, probe) keys():", Join()(out, probe_src).keys())
print("-> probe_id stays data; joining on it needs the column as a tag, which only")
print("   re-rooting (as_table() + a new source) can do today.")
