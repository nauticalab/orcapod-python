"""ITL-939 experiment 05: hashes of the CopyTagsToData route vs the W2 recipe, and divergence.

Compares, for the same elapsed-time computation on the EDI-shaped ``experiment`` table:

* route W2:   copy the column before building the source (today's recipe)
* route copy: the out-of-tree ``CopyTagsToData`` prototype from exp03

what each gives for per-row input data hashes, stream ``content_hash``, ``pipeline_hash``, and
the record-id preimage a ``FunctionNode`` would derive for the pod's input rows. Then shows
that the copied value can diverge from the tag once a pod or ``MapData`` touches it.

Run:

    uv run python superpowers/specs/itl-939-experiments/exp05_hash_comparison.py
"""

from __future__ import annotations

import contextlib
import importlib
import datetime as dt
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
with contextlib.redirect_stdout(io.StringIO()):  # reuse fixtures, not their output
    fx = importlib.import_module("exp02_workarounds_today")
    CopyTagsToData = importlib.import_module("exp03_copy_tag_prototype").CopyTagsToData

from orcapod import contexts, function_pod
from orcapod.core.nodes.function_node import _build_record_id_preimage
from orcapod.operators import MapData

COLS = ["session_start", "experiment_start_timestamp"]
w2_in = fx.w2_src.select_data_columns(COLS)
copy_in = CopyTagsToData({"session_start_datetime": "session_start"})(
    fx.baseline
).select_data_columns(COLS)
w2_out = fx.elapsed_s2.pod(w2_in)
copy_out = fx.elapsed_s2.pod(copy_in)


def h(x) -> str:
    """Return the first 16 hex characters of a ``ContentHash``."""
    return x.to_hex(16)


print("=== per-row input data content_hash (the pod's result-cache key)")
print(
    "equal:",
    [h(d.content_hash()) for _, d in w2_in.iter_data()]
    == [h(d.content_hash()) for _, d in copy_in.iter_data()],
)

print("\n=== per-row OUTPUT data content_hash")
print(
    "equal:",
    [h(d.content_hash()) for _, d in w2_out.iter_data()]
    == [h(d.content_hash()) for _, d in copy_out.iter_data()],
)

print("\n=== stream content_hash / pipeline_hash (pod input stream, pod output stream)")
print(
    f"W2   input : content {h(w2_in.content_hash())}  pipeline {h(w2_in.pipeline_hash())}"
)
print(
    f"copy input : content {h(copy_in.content_hash())}  pipeline {h(copy_in.pipeline_hash())}"
)
print(
    f"W2   output: content {h(w2_out.content_hash())}  pipeline {h(w2_out.pipeline_hash())}"
)
print(
    f"copy output: content {h(copy_out.content_hash())}  pipeline {h(copy_out.pipeline_hash())}"
)

print(
    "\n=== record-id preimage of the pod's first input row (system tags + input data hash)"
)
# Non-public helper: this comparison is specifically about the FunctionNode record-id
# preimage, which has no public accessor.
for name, stream in (("W2", w2_in), ("copy", copy_in)):
    tag, data = next(iter(stream.iter_data()))
    pre = _build_record_id_preimage(tag, data)
    print(
        f"{name:5s} columns:",
        [
            c.split("::")[0] + "::" + c.split("::")[1][:8] if "::" in c else c
            for c in pre.column_names
        ],
    )
    vals = {c.split("::")[0]: pre.column(c)[0].as_py() for c in pre.column_names}
    print(
        f"{name:5s} values: source_id={vals['_tag_source_id']!r}",
        f"record_id={vals['_tag_record_id'].hex()[:16]}",
        f"input_data_hash=...{vals['__input_data_hash'].hex()[-16:]}",
    )
    # How FunctionNode.compute_base_entry_id hashes the preimage.
    entry = contexts.get_default_context().arrow_hasher.hash_table(pre).to_hex(16)
    print(f"{name:5s} base entry id (arrow hash of the preimage): {entry}")

print("\n=== divergence: the copy is ordinary data once created")


@function_pod(output_keys="session_start")
def to_day(session_start: dt.datetime) -> dt.datetime:
    """Truncate the copied session start to midnight."""
    return session_start.replace(hour=0, minute=0)


copied = CopyTagsToData({"session_start_datetime": "session_start"})(fx.baseline)
day = to_day.pod(copied.select_data_columns(["session_start"]))
for tag, data in list(day.iter_data())[:2]:
    print(
        "tag:",
        tag.as_dict()["session_start_datetime"],
        "| data session_start:",
        data.as_dict()["session_start"],
    )
renamed = MapData({"session_start": "t0"})(copied)
print("after MapData the tag is unchanged:", renamed.keys())
