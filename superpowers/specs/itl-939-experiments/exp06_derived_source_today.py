"""ITL-939 experiment 06: what today's ``DerivedSource`` can and cannot do.

The developer chose the DerivedSource form for "a tag value also available as data"
(November 11, 2026). This script probes the class as it is today:

1. where a ``DerivedSource`` comes from and what it needs (database, completed run);
2. whether its tag/data assignment can differ from the origin's;
3. whether it can derive from a plain source or an in-memory stream;
4. its provenance columns (system tags, source info) compared with the origin's;
5. its ``content_hash``, ``pipeline_hash`` and ``source_id``;
6. its behaviour inside a second ``PipelineJob`` (sync and async) and with save/load.

Run:

    uv run python superpowers/specs/itl-939-experiments/exp06_derived_source_today.py
"""

from __future__ import annotations

import contextlib
import importlib
import io
import sys
import tempfile
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
with contextlib.redirect_stdout(io.StringIO()):  # reuse fixtures, not their output
    fx = importlib.import_module("exp02_workarounds_today")

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import DerivedSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator


def attempt(label: str, fn) -> object:
    """Run ``fn`` and print its result or the error it raises."""
    try:
        out = fn()
        print(f"{label}: OK -> {out}")
        return out
    except Exception as exc:  # noqa: BLE001 - the experiment records any failure
        last = traceback.extract_tb(exc.__traceback__)[-1]
        print(
            f"{label}: {type(exc).__name__}: {str(exc)[:200]} "
            f"[{Path(last.filename).name}:{last.lineno}]"
        )
        return None


def sys_cols(table) -> list[str]:
    """Return the system-tag and source-info column names of a table."""
    return [c for c in table.column_names if c.startswith(("_tag_", "_source_"))]


@function_pod(output_keys="task_upper")
def task_upper(task_id: str) -> str:
    """Return the task id in upper case (a stand-in computation)."""
    return task_id.upper()


experiment = fx.src(fx.experiment_table(), fx.KEYS_EXPERIMENT, "experiment")

print("=== 1. where a DerivedSource comes from")
attempt("RootSource.as_source", lambda: experiment.as_source())  # pyright: ignore[reportAttributeAccessIssue]
attempt(
    "in-memory pod stream .as_source",
    lambda: task_upper.pod(experiment.select_data_columns(["task_id"])).as_source(),  # pyright: ignore[reportAttributeAccessIssue]
)
attempt(
    "DerivedSource(origin=<a source>)", lambda: DerivedSource(experiment).as_table()
)  # pyright: ignore[reportArgumentType]

db = InMemoryArrowDatabase()
job = PipelineJob(name="stage1", database=db)
with job:
    e = fx.src(fx.experiment_table(), fx.KEYS_EXPERIMENT, "experiment")
    task_upper.pod(e.select_data_columns(["task_id"]), label="upper")
node = job.nodes["upper"]
derived = node.as_source()
print("type:", type(derived).__name__, "| origin:", type(node).__name__)

print("\n=== 2. before the origin has run")
print("rows:", derived.as_table().num_rows, "| keys():", derived.keys())
job.run()
print(
    "after run, same object rows:", derived.as_table().num_rows, "(cached empty table)"
)
derived = node.as_source()
print("after run, new as_source() rows:", derived.as_table().num_rows)

print("\n=== 3. tag/data assignment")
print("origin keys(): ", node.keys())
print("derived keys():", derived.keys())
attempt(
    "DerivedSource(origin, tag_columns=...)",
    lambda: DerivedSource(node, tag_columns=["subject_id"]),  # pyright: ignore[reportCallIssue]
)

print("\n=== 4. provenance columns: origin node vs derived source")
node_t = node.as_table(all_info=True)
der_t = derived.as_table(all_info=True)
print("origin :", sys_cols(node_t))
print("derived:", sys_cols(der_t))
print("derived.source_id:", derived.source_id)
print("origin all columns :", [c[:40] for c in node_t.column_names])
print("derived all columns:", [c[:40] for c in der_t.column_names])
print("derived _source_task_upper[0]:", der_t["_source_task_upper"][0].as_py())

print("\n=== 5. identity")
print("origin  content_hash :", node.content_hash().to_hex(16))
print("derived content_hash :", derived.content_hash().to_hex(16))
print("origin  pipeline_hash:", node.pipeline_hash().to_hex(16))
print("derived pipeline_hash:", derived.pipeline_hash().to_hex(16))
print("derived.upstreams:", derived.upstreams, "| producer:", derived.producer)


@function_pod(output_keys="n")
def length(task_upper: str) -> int:
    """Return the length of the string."""
    return len(task_upper)


print("\n=== 6. a DerivedSource inside a second PipelineJob")
for name, orch in (("sync", None), ("async", AsyncPipelineOrchestrator())):
    job2 = PipelineJob(name=f"stage2_{name}", database=InMemoryArrowDatabase())
    with job2:
        length.pod(node.as_source(), label="len")
    attempt(f"{name} run", lambda job=job2, o=orch: job.run(orchestrator=o) and None)
    attempt(f"{name} rows", lambda job=job2: job.nodes["len"].as_table().num_rows)

with tempfile.TemporaryDirectory() as tmp:
    path = Path(tmp) / "stage2.json"
    attempt("to_config", lambda: derived.to_config())  # pyright: ignore[reportAttributeAccessIssue]
    attempt("job2.save", lambda: job2.save(path))
    loaded = attempt("PipelineJob.load", lambda: PipelineJob.load(path))
    if loaded is not None:
        print("loaded unbound sources:", loaded.unbound_sources)  # pyright: ignore[reportAttributeAccessIssue]

print("\n=== 7. downstream of today's DerivedSource (it carries no system tags)")
store = InMemoryArrowDatabase()
for attempt_no in (1, 2):
    job3 = PipelineJob(name="stage2_records", database=store)
    with job3:
        length.pod(node.as_source(), label="len")
    job3.run()
    stored = job3.nodes["len"].get_all_records()
    print(
        f"run {attempt_no}: rows out {job3.nodes['len'].as_table().num_rows},",
        "stored records",
        None if stored is None else stored.num_rows,
        "| input task_upper values:",
        sorted(node.as_source().as_table()["task_upper"].to_pylist()),
    )
