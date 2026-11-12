"""C2 repro: a ``fixed_size_list`` column cannot be hashed, so no source can be built on it.

Run from the repository root:

    uv run python superpowers/specs/itl-926-live/repros/c2_fixed_size_list_cannot_hash.py

No SpiralDB needed. Observed with starfix 0.4.0 on main (ebf2511b): ``NotImplementedError:
Unsupported leaf type: fixed_size_list<item: uint8>[3]`` from ``arrow_hasher.hash_table``.
Since PR #305 the source names the column: ``cannot hash column 'rgb' of type
fixed_size_list<item: uint8>[3]: ...``. EDI sample tables
with such columns: experiment_display_params, experiment_freeviewing_params,
experiment_screen_capture_event, lfp_timeseries (``fixed_size_list(f32)[384]``).
"""

import pyarrow as pa

from orcapod.sources import ArrowTableSource

table = pa.table({"k": ["a"], "rgb": pa.array([[1, 2, 3]], pa.list_(pa.uint8(), 3))})
try:
    ArrowTableSource(table, tag_columns=["k"])
    print("built")
except NotImplementedError as exc:
    print(f"NotImplementedError: {exc}")
