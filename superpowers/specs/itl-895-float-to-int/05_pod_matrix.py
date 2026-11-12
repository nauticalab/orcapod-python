"""Claim 5: declared output type x returned value, through a real pod, today.

For each pair, one pod is built and run two ways:

* ``standalone``: ``f.pod(src).as_table()`` (no store)
* ``job``: ``PipelineJob`` (sync orchestrator, in-memory store), reading the stored record

Each cell is ``kept``, ``LOSSY`` (stored value differs from the returned one),
``converted`` (equal value, different Python type), or ``raises <Error>``. The last
section shows the standalone ``as_table()`` "Complex unions" error from the issue.

Run: uv run python superpowers/specs/itl-895-float-to-int/05_pod_matrix.py
"""

from __future__ import annotations

import datetime as dt
import logging
import warnings
from decimal import Decimal
from typing import Any

import numpy as np

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource

warnings.simplefilter("ignore")
logging.disable(logging.CRITICAL)

UTC = dt.timezone.utc
PLUS2 = dt.timezone(dt.timedelta(hours=2))

CASES: list[tuple[Any, Any]] = [
    (int, 1.5),
    (int, 2.0),
    (int, np.float64(1.5)),
    (int, Decimal("1.5")),
    (int, np.int64(3)),
    (int, True),
    (int, np.bool_(True)),
    (int, "1"),
    (int, None),
    (int, 2**63),
    (np.int8, 1.5),
    (np.int8, 300),
    (float, 1),
    (float, True),
    (float, 2**53 + 1),
    (float, np.float32(0.1)),
    (float, Decimal("1.5")),
    (float, "1.0"),
    (float, None),
    (bool, 1),
    (bool, np.bool_(True)),
    (bool, 1.0),
    (str, 1),
    (str, b"x"),
    (bytes, "x"),
    (dt.datetime, dt.datetime(2020, 1, 1, 12, tzinfo=PLUS2)),
    (dt.datetime, dt.datetime(2020, 1, 1, 12)),
    (dt.datetime, dt.date(2020, 1, 1)),
    (dt.datetime, 1.5),
    (dt.date, dt.datetime(2020, 1, 1, 12)),
    (dt.date, 1.5),
    (list[int], [1.5]),
    (list[int], (1, 2)),
    (int | None, 1.5),
]


def make_pod(declared, value, label):
    def fn(x: int):
        return value

    fn.__annotations__["return"] = declared
    fn.__name__ = fn.__qualname__ = label
    return function_pod(output_keys="y")(fn)


def classify(stored: Any, value: Any) -> str:
    if stored == value and type(stored) is type(value):
        return f"kept {stored!r}"
    try:
        equal = stored == value
        if isinstance(equal, np.ndarray):
            equal = bool(equal.all())
    except Exception:  # noqa: BLE001
        equal = False
    if equal and not isinstance(value, dt.datetime):
        return f"converted {stored!r}"
    return f"LOSSY {stored!r}"


def run(declared, value, i) -> tuple[str, str]:
    f = make_pod(declared, value, f"f{i}")
    src = DictSource(data=[{"k": "a", "x": 1}], tag_columns=["k"], source_id="src")
    out = []
    try:
        out.append(classify(f.pod(src).as_table()["y"].to_pylist()[0], value))
    except Exception as e:  # noqa: BLE001
        out.append(f"raises {type(e).__name__}")
    try:
        job = PipelineJob(name=f"m{i}", store=InMemoryArrowDatabase())
        with job:
            f.pod(src, label="f")
        job.run()
        recs = job.nodes["f"].get_all_records()
        out.append(
            classify(recs["y"].to_pylist()[0], value)
            if recs is not None and recs.num_rows
            else "no row stored"
        )
    except Exception as e:  # noqa: BLE001
        out.append(f"raises {type(e).__name__}")
    return out[0], out[1]


def name(t) -> str:
    return getattr(t, "__name__", None) or str(t)


print(
    f"{'declared':10} {'returned':42} {'standalone as_table':34} {'PipelineJob stored'}"
)
for i, (declared, value) in enumerate(CASES):
    s, j = run(declared, value, i)
    print(f"{name(declared):10} {repr(value)[:40]:42} {s[:34]:34} {j[:40]}")

print(
    "\n== the issue's 'Complex unions' error: standalone as_table() over mixed results =="
)


@function_pod(output_keys="y")
def g(x: int) -> int:
    return x if x == 1 else x + 0.5  # pyright: ignore[reportReturnType]  # deliberate wrong return type: reproduces ITL-895


src2 = DictSource(data=[{"k": "a", "x": 1}, {"k": "b", "x": 2}], tag_columns=["k"])
print("  iter_data:", [d.as_dict() for _, d in g.pod(src2).iter_data()])
try:
    g.pod(src2).as_table()
except Exception as e:  # noqa: BLE001
    print(f"  as_table raises {type(e).__name__}: {e}")
