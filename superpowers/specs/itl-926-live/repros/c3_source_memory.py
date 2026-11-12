"""C3 repro: memory used to build a source over a spike_event-shaped table.

Run from the repository root:

    uv run python superpowers/specs/itl-926-live/repros/c3_source_memory.py [rows]

No SpiralDB needed (``SpiralDBTableSource`` builds through the same ``SourceStreamBuilder``).
Observed on main (ebf2511b), 200,000 rows: 8.6 MB of Arrow data, build 0.9 s, peak RSS up
about 210 MB.
"""

import resource
import sys
import time

import numpy as np
import pyarrow as pa

from orcapod.sources import ArrowTableSource

n = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000
table = pa.table(
    {
        "spike_extraction_hash": pa.array(["h"] * n),
        "subject_id": pa.array(["m1"] * n),
        "session_start_datetime": pa.array(
            np.full(n, np.datetime64("2026-01-01T09:00:00", "us"))
        ),
        "recording_index": pa.array(np.zeros(n, np.uint32)),
        "probe_index": pa.array(np.zeros(n, np.uint32)),
        "timestamp": pa.array(np.arange(n, dtype=np.uint64)),
        "unit_id": pa.array((np.arange(n) % 300).astype(np.uint32)),
        "spike_amplitude_uv": pa.array(np.random.rand(n).astype(np.float32)),
    }
)
scale = (
    1e6 if sys.platform == "darwin" else 1e3
)  # ru_maxrss: bytes on macOS, KiB on Linux
before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / scale
start = time.time()
ArrowTableSource(table, tag_columns=table.column_names[:7], source_id="spike_event")
after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / scale
print(
    f"rows={n} arrow={table.nbytes / 1e6:.1f} MB build={time.time() - start:.1f}s "
    f"peak_rss {before:.0f} -> {after:.0f} MB"
)
