"""H2 repro: record identity of a Spiral source is positional, and a composite key can't be used.

Run from the repository root:

    uv run python superpowers/specs/itl-926-live/repros/h2_positional_record_ids.py

Uses the pyspiral fake (``tests/test_databases/fake_pyspiral.py``). Observed on main
(ebf2511b): inserting a row whose key sorts first shifts every later row's provenance token
(``row_0`` → ``row_1`` ...), so each gets a new record ID; ``record_id_column`` accepts one
column only, and a non-unique one gives two rows the same token.
"""

import sys
import tempfile
from typing import Any

import pyarrow as pa

sys.path.insert(0, "tests/test_databases")
import fake_pyspiral  # noqa: E402  # pyright: ignore[reportMissingImports]  (path set above)

from orcapod.sources import SpiralDBTableSource  # noqa: E402


def tokens(source: SpiralDBTableSource) -> list[Any]:
    """Return the source-info tokens of column ``v``."""
    return source.as_table(columns={"source": True}).column("_source_v").to_pylist()


with fake_pyspiral.install(tempfile.mkdtemp()):
    project = fake_pyspiral.FakeSpiral().project("p")
    table = project.create_table(
        "default.t", key_schema=[("s", pa.string()), ("n", pa.int32())]
    )
    table.write(
        pa.table({"s": ["b", "b"], "n": pa.array([1, 2], pa.int32()), "v": [1.0, 2.0]})
    )
    print("before insert:", tokens(SpiralDBTableSource("p", "t")))
    table.write(pa.table({"s": ["a"], "n": pa.array([0], pa.int32()), "v": [0.0]}))
    print("after insert: ", tokens(SpiralDBTableSource("p", "t")))
    print(
        "record_id_column='s':",
        tokens(SpiralDBTableSource("p", "t", record_id_column="s")),
    )
    try:
        SpiralDBTableSource("p", "t", record_id_column=["s", "n"])  # pyright: ignore[reportArgumentType]  (deliberate)
    except ValueError as exc:
        print(f"composite record_id_column: ValueError: {exc}")
