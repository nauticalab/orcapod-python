"""ITL-897 claim (b): stored pandas frames keep column order; the cache key does not.

Through the public interface, for the in-memory and Delta stores and the sync and async
orchestrators:

1. ``make`` returns ``df`` with columns ``a, b`` (row ``fwd``) or ``b, a`` (row ``rev``).
   Reading the node back gives each frame with its own column order (storage round trip).
2. ``first`` returns ``df.columns[0]`` from the stored frame. A correct cache gives
   ``{"fwd": "a", "rev": "b"}``; on ``main`` one row reuses the other's cached result.

Also prints what the Arrow IPC storage envelope records for a reordered frame.

Run: ``uv run python superpowers/scripts/itl_897/claim_b_storage_roundtrip.py``
"""

from __future__ import annotations

import json
import tempfile
import warnings
from pathlib import Path

import pandas as pd
import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.logical_types.pandas_type import LogicalPandasDataFrame
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.sources import DictSource

warnings.simplefilter("ignore")


def _frame(reverse: bool) -> pd.DataFrame:
    if reverse:
        return pd.DataFrame({"b": ["x", "y"], "a": [1, 2]})
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


@function_pod(output_keys="df")
def make(reverse: bool) -> pd.DataFrame:
    return _frame(reverse)


@function_pod(output_keys="first")
def first(df: pd.DataFrame) -> str:
    return str(df.columns[0])


def _run(store_name: str, orchestrator_name: str, tmp: Path) -> None:
    store = (
        InMemoryArrowDatabase()
        if store_name == "in_memory"
        else DeltaTableDatabase(tmp / f"delta_{orchestrator_name}")
    )
    orchestrator = (
        SyncPipelineOrchestrator()
        if orchestrator_name == "sync"
        else AsyncPipelineOrchestrator()
    )
    source = DictSource(
        [{"k": "fwd", "reverse": False}, {"k": "rev", "reverse": True}],
        tag_columns=["k"],
    )
    job = PipelineJob(name="order", database=store)
    with job:
        frames = make.pod(source, label="make")
        first.pod(frames, label="first")
    job.run(orchestrator=orchestrator)

    stored = {t["k"]: list(d["df"].columns) for t, d in job.nodes["make"].iter_data()}
    firsts = {t["k"]: d["first"] for t, d in job.nodes["first"].iter_data()}
    order_kept = stored == {"fwd": ["a", "b"], "rev": ["b", "a"]}
    cache_ok = firsts == {"fwd": "a", "rev": "b"}
    print(
        f"| {store_name} | {orchestrator_name} | {stored} | "
        f"{'yes' if order_kept else 'NO'} | {firsts} | {'yes' if cache_ok else 'NO'} |"
    )


def _run_sequential(store_name: str, orchestrator_name: str, tmp: Path) -> None:
    """Run ``fwd`` alone, then ``rev`` alone, on one store: a warm cache, no race."""
    store = (
        InMemoryArrowDatabase()
        if store_name == "in_memory"
        else DeltaTableDatabase(tmp / f"seq_{orchestrator_name}")
    )
    got: dict[str, str] = {}
    for key, reverse in (("fwd", False), ("rev", True)):
        orchestrator = (
            SyncPipelineOrchestrator()
            if orchestrator_name == "sync"
            else AsyncPipelineOrchestrator()
        )
        source = DictSource([{"k": key, "reverse": reverse}], tag_columns=["k"])
        job = PipelineJob(name="order_seq", database=store)
        with job:
            frames = make.pod(source, label="make")
            first.pod(frames, label="first")
        job.run(orchestrator=orchestrator)
        got.update({t["k"]: d["first"] for t, d in job.nodes["first"].iter_data()})
    ok = got.get("rev") == "b"
    print(f"| {store_name} | {orchestrator_name} | {got} | {'yes' if ok else 'NO'} |")


def _envelope() -> None:
    lt = LogicalPandasDataFrame()
    table = pa.ipc.open_stream(lt.python_to_storage(_frame(True))).read_all()
    meta = json.loads(table.schema.metadata[b"pandas"])
    print("\nIPC envelope of the reversed frame:")
    print(f"  Arrow field order : {table.schema.names}")
    print(f"  pandas metadata   : columns={[c['name'] for c in meta['columns']]}")
    print(f"  index_columns     : {meta['index_columns']}")


def _prototype_hash_survives_storage() -> None:
    """Under the prototype hash, a value and its stored round trip hash the same."""
    import sys

    sys.path.insert(0, str(Path(__file__).parent))
    import importlib

    prototype_v1 = importlib.import_module("prototype_v1")
    claim_a = importlib.import_module("claim_a_equality_vs_hash")
    FRAME_CASES, SERIES_CASES = claim_a.FRAME_CASES, claim_a.SERIES_CASES

    from orcapod.contexts import get_default_context
    from orcapod.logical_types.pandas_type import LogicalPandasSeries

    prototype_v1.install()
    hasher = get_default_context().semantic_hasher
    lt_df, lt_s = LogicalPandasDataFrame(), LogicalPandasSeries()
    bad: list[str] = []
    unstorable: list[str] = []
    values = [(n, v, lt_df) for n, a, b, _ in FRAME_CASES for v in (a, b)]
    values += [(n, v, lt_s) for n, a, b, _ in SERIES_CASES for v in (a, b)]
    for name, value, lt in values:
        try:
            back = lt.storage_to_python(lt.python_to_storage(value))
        except Exception as exc:  # noqa: BLE001 - a storage failure is reported, not a hash
            unstorable.append(f"{name}: {type(exc).__name__}")
            continue
        if hasher.hash_object(value) != hasher.hash_object(back):
            bad.append(name)
    print(
        f"\nPrototype hash equal before and after storage: {len(values) - len(unstorable) - len(bad)}"
        f" of {len(values) - len(unstorable)} storable values; differ: {bad or 'none'};"
        f" not storable on main: {unstorable or 'none'}"
    )


if __name__ == "__main__":
    print(
        "| store | orchestrator | stored column order | order kept | first column | cache correct |"
    )
    print("|---|---|---|---|---|---|")
    with tempfile.TemporaryDirectory() as tmp:
        for store_name in ("in_memory", "delta"):
            for orchestrator_name in ("sync", "async"):
                _run(store_name, orchestrator_name, Path(tmp))
        print("\nSequential runs on one store (fwd, then rev):\n")
        print("| store | orchestrator | first column seen across runs | rev correct |")
        print("|---|---|---|---|")
        for store_name in ("in_memory", "delta"):
            for orchestrator_name in ("sync", "async"):
                _run_sequential(store_name, orchestrator_name, Path(tmp))
    _envelope()
    _prototype_hash_survives_storage()
