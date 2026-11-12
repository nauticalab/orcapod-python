"""Claim 8: ``None`` returned for a non-optional output (coordinator follow-up from PR #289).

Same family as float -> int: the returned value is never checked against the declared
output type. ``-> int`` maps to ``int64 not null``; ``pa.Table.from_pylist`` writes a
NULL into a ``not null`` field without error, and ``Table.validate(full=True)`` does not
check field nullability either. What happens next depends on the path:

* standalone ``as_table()``: returns a table whose ``not null`` field holds a NULL;
* in-memory store: the run completes; the NULL is stored; reading the node back fails
  in ``restore_schema_nullability`` (``Table.cast``: "Casting field 'y' with null values
  to non-nullable");
* Delta store: the Delta writer refuses the batch at flush ("declared as non-nullable
  but contains null values"); today that is logged as a row failure and the final flush
  raises ``RuntimeError`` (ITL-878 / PR #287 turns it into ``StoreWriteError``).

Run: uv run python superpowers/specs/itl-895-float-to-int/08_none_output.py
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.datagrams import Data
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import DictSource

logging.disable(logging.CRITICAL)

print("== pyarrow only ==")
t = pa.Table.from_pylist(
    [{"y": None}], schema=pa.schema([pa.field("y", pa.int64(), nullable=False)])
)
print(
    f"  from_pylist(None, 'y: int64 not null') -> {t.to_pylist()}  field={t.schema.field('y')}  "
    f"null_count={t['y'].null_count}"
)
t.validate(full=True)
print("  Table.validate(full=True) -> passes (Arrow does not check field nullability)")

print("\n== orcapod Data ==")
d = Data({"y": None}, python_schema={"y": int})
print(
    f"  Data({{'y': None}}, python_schema={{'y': int}}).as_table() -> {d.as_table().to_pylist()} "
    f"field={d.as_table().schema.field('y')}"
)


@function_pod(output_keys="y")
def f(x: int) -> int:
    return None  # pyright: ignore[reportReturnType]  # deliberate wrong return type: reproduces ITL-895


def src() -> DictSource:
    return DictSource(data=[{"k": "a", "x": 1}], tag_columns=["k"], source_id="s")


print("\n== standalone pod ==")
st = f.pod(src()).as_table()
print(f"  as_table() -> y={st['y'].to_pylist()} field={st.schema.field('y')}")

print("\n== PipelineJob ==")
for label, kind, orch in [
    ("in-memory, sync", "mem", None),
    ("in-memory, async", "mem", AsyncPipelineOrchestrator()),
    ("Delta, sync", "delta", None),
    ("Delta, async", "delta", AsyncPipelineOrchestrator()),
]:
    with tempfile.TemporaryDirectory() as tmp:
        store = (
            DeltaTableDatabase(Path(tmp) / "db")
            if kind == "delta"
            else InMemoryArrowDatabase()
        )
        job = PipelineJob(name="none_out", store=store)
        with job:
            f.pod(src(), label="f")
        try:
            job.run(orchestrator=orch)
            run = "run completes"
        except Exception as e:  # noqa: BLE001
            run = f"run raises {type(e).__name__}: {str(e)[:80]}"
        try:
            recs = job.nodes["f"].get_all_records()
            read = "no rows" if recs is None else f"stored y={recs['y'].to_pylist()}"
        except Exception as e:  # noqa: BLE001
            read = f"read raises {type(e).__name__}: {str(e)[:70]}"
        print(f"  {label:18} {run}\n  {'':18} {read}")
