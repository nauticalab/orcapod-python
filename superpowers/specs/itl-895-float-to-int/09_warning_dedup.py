"""How many ValueConversionWarnings does a long run show? (ITL-895)

Each scenario runs in a fresh interpreter and counts what ``warnings.showwarning`` is asked
to show, under Python's default filters and under ``-W always`` (``simplefilter("always")``,
which disables the warning registry). A pod with an output declared ``list[int]`` returns
a tuple (D5) and an output declared ``datetime.date`` returns a datetime (D6) on every row.
The function instance deduplicates explicitly, so the expected count is two per function
instance, under either filter: one per output x conversion.

The standalone scenarios use 10,000 rows. The ``PipelineJob`` scenarios use 1,000 rows,
because a ``PipelineJob`` run grows faster than linearly with the row count on main too
(1,000 rows took minutes), which is not what this script measures; the deduplication does not
depend on the row count.

Run: ``uv run python superpowers/specs/itl-895-float-to-int/09_warning_dedup.py``
"""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
import warnings

SCENARIOS = ("standalone", "standalone-twice", "sync", "async")


def run(scenario: str, always: bool) -> None:
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
    from orcapod.sources import DictSource

    shown: list[str] = []
    original = warnings.showwarning

    def count(message, category, filename, lineno, file=None, line=None):  # noqa: ANN001, ANN202
        shown.append(f"{category.__name__}: {message}")

    warnings.showwarning = count
    if always:
        warnings.simplefilter("always")
    rows = 10_000 if scenario.startswith("standalone") else 1_000

    @function_pod(output_keys=("pair", "day"))
    def near_miss(x: int) -> tuple[list[int], dt.date]:
        return (x, x), dt.datetime(2020, 1, 1, x % 24)  # pyright: ignore[reportReturnType]

    source = DictSource(
        data=[{"k": f"r{x}", "x": x} for x in range(rows)], tag_columns=["k"]
    )
    stored = 0
    if scenario.startswith("standalone"):
        for _ in range(2 if scenario == "standalone-twice" else 1):
            stored = near_miss.pod(source).as_table().num_rows
    else:
        job = PipelineJob(name="dedup", store=InMemoryArrowDatabase())
        with job:
            near_miss.pod(source, label="near_miss")
        orchestrator = (
            SyncPipelineOrchestrator()
            if scenario == "sync"
            else AsyncPipelineOrchestrator()
        )
        job.run(orchestrator=orchestrator)
        stored = job.nodes["near_miss"].as_table().num_rows
    warnings.showwarning = original
    filters = "always" if always else "default"
    print(f"{scenario} ({filters} filter): {stored} rows, {len(shown)} warnings shown")
    for line in shown:
        print(f"    {line}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run(sys.argv[1], always=sys.argv[2] == "always")
    else:
        for name in SCENARIOS:
            for filters in ("default", "always"):
                _ = subprocess.run(
                    [sys.executable, __file__, name, filters], check=True
                )
