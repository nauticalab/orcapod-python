"""ITL-939 experiment 08: does ``GroupBy`` put tag values into the data group?

The developer's criterion (November 11, 2026) separating an operator from a derived source:
does any data element come from a tag value, or from anywhere other than the input data
elements? This script shows, on the EDI-shaped ``experiment`` table, where the non-key tag
values go after ``GroupBy``, and after ``Batch`` for contrast.

Run:

    uv run python superpowers/specs/itl-939-experiments/exp08_group_by_tags_into_data.py
"""

from __future__ import annotations

import contextlib
import importlib
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
with contextlib.redirect_stdout(io.StringIO()):  # reuse fixtures, not their output
    fx = importlib.import_module("exp02_workarounds_today")

from orcapod import function_pod
from orcapod.operators import Batch, GroupBy

experiment = fx.src(fx.experiment_table(), fx.KEYS_EXPERIMENT, "experiment")
print("input   keys():", experiment.keys())

grouped = GroupBy(by=["subject_id"])(experiment)
print("\n=== GroupBy(by=['subject_id'])")
print("grouped keys():", grouped.keys())
tag_schema, data_schema = grouped.output_schema()
print("tag schema :", dict(tag_schema))
print("data schema:", dict(data_schema))
table = grouped.as_table(columns={"source": True})
for name in ("subject_id", "experiment_index", "session_start_datetime", "task_id"):
    print(f"{name}: {table[name].to_pylist()}")
for name in ("_source_experiment_index", "_source_session_start_datetime"):
    print(f"{name}: {table[name].to_pylist()}")
print("_source_task_id[0]:", table["_source_task_id"][0].as_py())


@function_pod(output_keys="last_index")
def last_index(experiment_index: list[int]) -> int:
    """Return the largest experiment index of the group."""
    return max(experiment_index)


out = last_index.pod(grouped.select_data_columns(["experiment_index"]))
print(
    "a pod reads the former tag as data:",
    [(t["subject_id"], d["last_index"]) for t, d in out.iter_data()],
)

batched = Batch(batch_size=2)(experiment)
print("\n=== Batch(batch_size=2), for contrast")
print("batched keys():", batched.keys())
