"""C1 repro: an all-nullable Spiral table makes a plain-typed function pod refuse the stream.

Run from the repository root:

    uv run python superpowers/specs/itl-926-live/repros/c1_nullable_columns_refuse_plain_pods.py

Uses the pyspiral fake (``tests/test_databases/fake_pyspiral.py``); every column is written
nullable, as in the EDI sample tables. Observed on main (ebf2511b): ``total(n: int)`` raises
``ValueError: ... column 'n' has type int | None, the function expects int``;
``total(n: int | None)`` works.
"""

import sys
import tempfile

import pyarrow as pa

sys.path.insert(0, "tests/test_databases")
import fake_pyspiral  # noqa: E402  # pyright: ignore[reportMissingImports]  (path set above)

from orcapod import function_pod  # noqa: E402
from orcapod.sources import SpiralDBTableSource  # noqa: E402

with fake_pyspiral.install(tempfile.mkdtemp()):
    project = fake_pyspiral.FakeSpiral().project("edi")
    table = project.create_table(
        "default.experiment", key_schema=[("subject_id", pa.string())]
    )
    table.write(
        pa.table({"subject_id": ["m1", "m2"], "n": pa.array([5, 6], pa.int64())})
    )
    source = SpiralDBTableSource("edi", "experiment")
print("source schema:", source.output_schema())


@function_pod(output_keys="m")
def plain(n: int) -> int:
    """A pod whose parameter does not accept ``None``."""
    return n


@function_pod(output_keys="m")
def optional(n: int | None) -> int | None:
    """A pod whose parameter accepts ``None``."""
    return n


for pod in (plain, optional):
    try:
        print(pod.pod(source).as_table().to_pylist())
    except ValueError as exc:
        print(f"ValueError: {exc}")
