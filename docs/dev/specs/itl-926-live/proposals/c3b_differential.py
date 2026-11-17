"""C3b differential: hash every source output byte for byte, before and after the change.

The change is in this branch, so compare the revision before it (``ebf2511b``, main when
ITL-926 started) with this branch. Copy the script out of the tree first, because the
older revision does not have it, then run it from the repository root on each revision
with the same environment and compare the two files:

    cp superpowers/specs/itl-926-live/proposals/c3b_differential.py /tmp/c3b_diff.py
    git switch --detach ebf2511b
    uv run python /tmp/c3b_diff.py before.json
    git switch -
    uv run python /tmp/c3b_diff.py after.json
    cmp before.json after.json

Each case records the SHA-256 of the IPC bytes of ``as_table(all_info=True)``,
``content_hash`` and ``pipeline_hash``.
"""

import datetime as dt
import decimal
import hashlib
import json
import sys
from typing import Any
import pyarrow as pa
from orcapod.sources import ArrowTableSource

utc = dt.timezone.utc
cols: dict[str, Any] = {
    "k_str": pa.array(["b", "a", None, "c"], pa.string()),
    "k_int": pa.array([3, 1, 2, None], pa.int64()),
    "k_u64": pa.array([2**63 + 5, 1, 2, 3], pa.uint64()),
    "k_ts": pa.array(
        [dt.datetime(2026, 1, 1, 9, 0, 0, 123456)] * 3 + [None], pa.timestamp("us")
    ),
    "k_tstz": pa.array(
        [dt.datetime(2026, 1, 1, 9, tzinfo=utc)] * 4, pa.timestamp("us", "UTC")
    ),
    "k_date": pa.array([dt.date(2026, 1, 1)] * 4, pa.date32()),
    "k_float": pa.array([1.5, float("nan"), -0.0, 1e300], pa.float64()),
    "k_f32": pa.array([0.1, 0.2, 0.3, None], pa.float32()),
    "k_bool": pa.array([True, False, None, True]),
    "k_bin": pa.array([b"\x00\xff", b"a", None, b""], pa.binary()),
    "k_dec": pa.array(
        [decimal.Decimal("1.10"), None, decimal.Decimal("0"), decimal.Decimal("-2.5")],
        pa.decimal128(5, 2),
    ),
    "v": pa.array([1.0, 2.0, 3.0, 4.0]),
}
table = pa.table(cols)
out = {}
for tag in [c for c in cols if c != "v"]:
    for rid in (None, tag):
        for sid in (None, "s"):
            src = ArrowTableSource(
                table, tag_columns=[tag], record_id_column=rid, source_id=sid
            )
            t = src.as_table(all_info=True)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, t.schema) as w:
                w.write_table(t)
            out[f"{tag}|{rid}|{sid}"] = [
                hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest(),
                src.content_hash().to_string(),
                src.pipeline_hash().to_string(),
            ]
json.dump(out, open(sys.argv[1], "w"), indent=1, sort_keys=True)
print(len(out), "cases")
