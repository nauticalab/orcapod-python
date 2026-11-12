"""ITL-926 live verification of ``SpiralDBTableSource`` against a real SpiralDB project.

Run after ``spiral login`` (or with ``SPIRAL_WORKLOAD_ID`` set in CI), from the repository
root, with the ``spiraldb`` extra installed (``uv sync --all-extras``).

Two modes.

**Read-only (default)** -- for an existing project such as the workshop's EDI sample
(``edi-sample-v1-855632``). Nothing is written to the project: no table creation, no writes,
no maintenance. Results go to a local Delta store in a temporary directory:

    uv run python superpowers/specs/itl-926-live/live_verify.py \
        --project edi-sample-v1-855632 --tables session,experiment \
        --value-column total_trials [--report ro.json]

It builds ``SpiralDBTableSource`` on each of ``--tables`` (keep them small: the source reads
the whole table into memory), records schemas, row counts and hashes, then joins the first
two on their shared keys, feeds ``--value-column`` to a function pod, runs twice (the second
run must make no function calls), saves the job, and in a fresh process checks that both
sources' ``content_hash`` / ``pipeline_hash`` are unchanged and that the loaded job's
``get_all_records()`` returns every row.

**Write mode** -- only for a dedicated test project, never an ``edi-*`` project. It must be
asked for by repeating the project id:

    uv run python superpowers/specs/itl-926-live/live_verify.py \
        --project <test-project> --allow-writes <test-project> [--keep]

It works in scratch tables named ``itl926_<run>_*`` in ``--dataset`` (dropped at the end
unless ``--keep``):

1. **Storage behaviour.** Writes one table per awkward column type straight through pyspiral
   (the way a workshop table is written: not through Orcapod) and records, per column, the
   type and nullability that were written, that ``Table.schema().to_arrow()`` reports, that a
   scan returns, and what ``SpiralDBTableSource`` exposes (Arrow field and Python type). This
   settles the behaviours that the test fake (``tests/test_databases/fake_pyspiral.py``)
   only assumes: value-column nullability, nested string types, binary type, dictionary
   decoding, decimal, timezone, extension types, struct-level nulls.
2. **Workshop path.** ``SpiralDBTableSource`` (composite key) -> ``PipelineJob`` with ``Join``
   against a ``DictSource`` and a function pod -> ``DeltaTableDatabase`` -> second run must
   make no function calls -> ``save`` -> ``PipelineJob.load`` in a fresh process ->
   ``get_all_records()`` of the function node, and the source's ``content_hash`` /
   ``pipeline_hash`` must equal the first process's.
3. **Row insert.** Inserts a row whose key sorts first and runs again: exactly one function
   call and four result rows are expected. Because record IDs are positional by default, the
   insert re-keys the rows after it; the report records ``get_all_records()``'s row count.

Nothing here needs the developer's interaction once logged in. Exit status is 0 when every
check passed, 1 otherwise; the JSON report has the details.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import os
import shutil
import subprocess
import sys
import tempfile
import traceback
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa


def _type_cases() -> dict[str, tuple[pa.Array[Any], bool]]:
    """Column cases: name -> (values, written non-null)."""
    utc = dt.timezone.utc
    return {
        "string": (pa.array(["a", None, "c"], pa.string()), False),
        "large_string_nonnull": (pa.array(["a", "b", "c"], pa.large_string()), True),
        "binary": (pa.array([b"a", None, b"c"], pa.binary()), False),
        "large_binary_nonnull": (pa.array([b"a", b"b", b"c"], pa.large_binary()), True),
        "int8_nonnull": (pa.array([1, 2, 3], pa.int8()), True),
        "float32": (pa.array([1.5, None, 3.0], pa.float32()), False),
        "float64_nonnull": (pa.array([1.5, 2.5, 3.0], pa.float64()), True),
        "list_string": (pa.array([["a"], [], ["b", "c"]], pa.list_(pa.string())), True),
        "large_list_int32": (
            pa.array([[1], [2, 3], []], pa.large_list(pa.int32())),
            True,
        ),
        "list_nonnull_item": (
            pa.array([[1], [2], []], pa.list_(pa.field("item", pa.int64(), False))),
            True,
        ),
        "struct": (
            pa.array(
                [{"x": 1, "s": "a"}, {"x": 2, "s": None}, {"x": 3, "s": "c"}],
                pa.struct(
                    [pa.field("x", pa.int16(), False), pa.field("s", pa.string())]
                ),
            ),
            True,
        ),
        "dictionary": (pa.array(["u", "v", "u"]).dictionary_encode(), True),
        "timestamp_utc": (
            pa.array(
                [dt.datetime(2026, 11, 17, 9, tzinfo=utc)] * 3,
                pa.timestamp("us", "UTC"),
            ),
            True,
        ),
        "timestamp_naive_ms": (
            pa.array([dt.datetime(2026, 11, 17, 9)] * 3, pa.timestamp("ms")),
            True,
        ),
        "date32": (pa.array([dt.date(2026, 11, 17)] * 3, pa.date32()), True),
        "decimal": (
            pa.array(
                [decimal.Decimal("1.23"), None, decimal.Decimal("4.56")],
                pa.decimal128(10, 2),
            ),
            False,
        ),
        "bool": (pa.array([True, False, None], pa.bool_()), False),
    }


def _field_desc(f: pa.Field[Any]) -> str:
    """Describe a field as its type, marking a non-null one ``not null``."""
    return f"{f.type}{'' if f.nullable else ' not null'}"


_CHILD_TIMEOUT_S = 600


def _run_child(cmd: list[str]) -> dict[str, Any]:
    """Run a fresh-process phase of this script and return the JSON it prints last.

    Args:
        cmd: The argument list (no shell), built from this script's own arguments.

    Returns:
        The decoded JSON object, or ``{"error": ...}`` when the child times out after
        ``_CHILD_TIMEOUT_S`` seconds (for example waiting at a login prompt) or prints no
        JSON.
    """
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, check=False, timeout=_CHILD_TIMEOUT_S
        )
    except subprocess.TimeoutExpired:
        return {"error": f"fresh-process phase timed out after {_CHILD_TIMEOUT_S}s"}
    try:
        return json.loads(proc.stdout.strip().splitlines()[-1])
    except Exception:  # noqa: BLE001 - the child's output is reported as is
        return {"error": proc.stdout[-2000:] + proc.stderr[-4000:]}


class Run:
    """Write mode: scratch tables in a dedicated test project, dropped by ``cleanup``.

    Args:
        args: The parsed command line; ``args.project`` is the test project.
    """

    def __init__(self, args: argparse.Namespace) -> None:
        """Connect to the test project; no table is touched yet."""
        import spiral as sp  # pyright: ignore[reportMissingImports]  (spiraldb extra, absent in the type-check job)

        self.args = args
        self.overrides = {"server.url": args.server_url} if args.server_url else None
        self.sp = sp.Spiral(overrides=self.overrides)
        self.project = self.sp.project(args.project)
        self.prefix = f"itl926_{args.run_id}"
        self.created: list[str] = []
        self.report: dict[str, Any] = {"run_id": args.run_id, "checks": {}, "types": {}}

    def ident(self, name: str) -> str:
        """Return the dataset-qualified identifier of scratch table ``name``."""
        return f"{self.args.dataset}.{self.prefix}_{name}"

    def create(self, name: str, key_schema: Any) -> Any:
        """Create scratch table ``name`` and remember it for ``cleanup``."""
        t = self.project.create_table(self.ident(name), key_schema=key_schema)
        self.created.append(self.ident(name))
        return t

    def check(self, name: str, ok: bool, detail: Any) -> None:
        """Record and print one pass/fail check."""
        self.report["checks"][name] = {"ok": bool(ok), "detail": detail}
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")

    # 1. storage behaviour ------------------------------------------------------------
    def storage_matrix(self) -> None:
        """Write each awkward column type through pyspiral and record how it reads back.

        A case that raises, or whose values come back different while the stored type
        equals the written one, is a failed check. A changed type (for example a dropped
        time zone) is recorded as an observation.
        """
        from orcapod.sources import SpiralDBTableSource

        for name, (values, nonnull) in _type_cases().items():
            entry: dict[str, Any] = {}
            self.report["types"][name] = entry
            try:
                schema = pa.schema(
                    [
                        pa.field("id", pa.int64(), False),
                        pa.field("v", values.type, not nonnull),
                    ]
                )
                table = pa.Table.from_arrays(
                    [pa.array(range(len(values)), pa.int64()), values], schema=schema
                )
                entry["written"] = _field_desc(schema.field("v"))
                t = self.create(f"t_{name}", [("id", pa.int64())])
                t.write(table)
                entry["table_schema"] = _field_desc(t.schema().to_arrow().field("v"))
                scanned = self.sp.scan(t.select()).to_table()
                entry["scan"] = _field_desc(scanned.schema.field("v"))
                entry["values_equal"] = (
                    scanned.column("v").to_pylist() == values.to_pylist()
                )
                src = SpiralDBTableSource(
                    self.args.project,
                    f"{self.prefix}_t_{name}",
                    dataset=self.args.dataset,
                    overrides=self.overrides,
                )
                entry["source_arrow"] = _field_desc(src.as_table().schema.field("v"))
                entry["source_python"] = str(src.output_schema()[1]["v"])
            except Exception as exc:  # noqa: BLE001 - every outcome is recorded
                entry["error"] = f"{type(exc).__name__}: {exc}"
            print(f"  {name:22} {entry}")
            same_type = (
                entry.get("scan", "").split(" not null")[0]
                == entry.get("written", "").split(" not null")[0]
            )
            self.check(
                f"type {name}: written and read back",
                "error" not in entry
                and (bool(entry.get("values_equal")) or not same_type),
                entry,
            )

        # struct-level null: SpiralDB documents that it refuses these without push_down_nulls
        try:
            t = self.create("t_struct_null", [("id", pa.int64())])
            t.write(
                pa.table(
                    {
                        "id": pa.array([0, 1], pa.int64()),
                        "v": pa.array([{"x": 1}, None], pa.struct([("x", pa.int64())])),
                    }
                )
            )
            self.report["types"]["struct_null"] = {"write": "accepted"}
        except Exception as exc:  # noqa: BLE001
            self.report["types"]["struct_null"] = {
                "write": f"{type(exc).__name__}: {exc}"
            }

        # extension type written by Orcapod (Path logical type)
        try:
            from orcapod.sources import DictSource

            ext = (
                DictSource(
                    data=[{"id": 1, "p": Path("/a")}, {"id": 2, "p": Path("/b")}],
                    tag_columns=["id"],
                )
                .as_table()
                .select(["id", "p"])
            )
            entry = {"written": str(ext.schema.field("p").type)}
            t = self.create("t_extension", [("id", pa.int64())])
            t.write(ext)
            entry["scan"] = str(
                self.sp.scan(t.select()).to_table().schema.field("p").type
            )
            self.report["types"]["extension_path"] = entry
        except Exception as exc:  # noqa: BLE001
            self.report["types"]["extension_path"] = {
                "error": f"{type(exc).__name__}: {exc}"
            }

    # 2. workshop path ----------------------------------------------------------------
    def workshop_table(self) -> str:
        """Create the composite-key workshop table and return its plain name."""
        t = self.create(
            "recordings", [("subject_id", pa.string()), ("session", pa.int32())]
        )
        t.write(
            pa.table(
                {
                    "subject_id": pa.array(["m2", "m1", "m1"], pa.string()),
                    "session": pa.array([1, 2, 1], pa.int32()),
                    "firing_rate": pa.array([1.5, 2.5, 3.5], pa.float64()),
                    "notes": pa.array(["x", None, "z"], pa.string()),
                },
                schema=pa.schema(
                    [
                        pa.field("subject_id", pa.string(), False),
                        pa.field("session", pa.int32(), False),
                        pa.field("firing_rate", pa.float64(), False),
                        pa.field("notes", pa.string()),
                    ]
                ),
            )
        )
        return f"{self.prefix}_recordings"

    def workshop_path(self, workdir: Path) -> None:
        """Run the workshop path, the fresh-process load, and the row-insert check."""
        table_name = self.workshop_table()
        first = _workshop_job(self.args, table_name, workdir, save=True)
        self.report["workshop_first"] = first
        self.check(
            "run1 computes every joined row",
            first["run1_calls"] == 3,
            first["run1_calls"],
        )
        self.check(
            "run2 makes no function calls",
            first["run2_calls"] == 0,
            first["run2_calls"],
        )
        self.check(
            "source schema: firing_rate is non-null float",
            first["data_schema"].get("firing_rate") == "<class 'float'>",
            first["data_schema"],
        )

        cmd = [
            sys.executable,
            __file__,
            "--phase",
            "load",
            "--table",
            table_name,
            "--workdir",
            str(workdir),
            "--project",
            self.args.project,
            "--dataset",
            self.args.dataset,
            "--run-id",
            self.args.run_id,
        ]
        if self.args.server_url:
            cmd += ["--server-url", self.args.server_url]
        second = _run_child(cmd)
        self.report["workshop_load"] = second
        self.check(
            "fresh process: same content_hash",
            second.get("content_hash") == first["content_hash"],
            [first["content_hash"], second.get("content_hash")],
        )
        self.check(
            "fresh process: same pipeline_hash",
            second.get("pipeline_hash") == first["pipeline_hash"],
            [first["pipeline_hash"], second.get("pipeline_hash")],
        )
        self.check(
            "loaded job: get_all_records returns the 3 scores",
            second.get("score_rows") == 3,
            second,
        )

        # 3. row insert
        t = self.project.table(self.ident("recordings"))
        t.write(
            pa.table(
                {
                    "subject_id": pa.array(["m1"], pa.string()),
                    "session": pa.array([0], pa.int32()),
                    "firing_rate": pa.array([9.0], pa.float64()),
                    "notes": pa.array(["new"], pa.string()),
                }
            )
        )
        third = _workshop_job(self.args, table_name, workdir, save=False, runs=1)
        self.report["workshop_insert"] = third
        self.check(
            "insert one row: one function call",
            third["run1_calls"] == 1,
            third["run1_calls"],
        )
        self.check(
            "insert one row: the score node shows 4 rows",
            len(third["scores"]) == 4,
            third["scores"],
        )
        # Recorded, not checked: on main, get_all_records() after a source change returns
        # the previous run's rows (seen with the fake, outside ITL-926's scope).
        print("insert: get_all_records rows =", third["pipeline_records"])

    def cleanup(self) -> None:
        """Drop every scratch table this run created, unless ``--keep``."""
        if self.args.keep:
            print("kept tables:", self.created)
            return
        for ident in reversed(self.created):
            try:
                self.project.drop_table(ident)
            except Exception as exc:  # noqa: BLE001
                print(f"could not drop {ident}: {exc}", file=sys.stderr)


def _workshop_job(
    args: argparse.Namespace,
    table_name: str,
    workdir: Path,
    *,
    save: bool,
    runs: int = 2,
) -> dict[str, Any]:
    """Build and run the write-mode workshop job; return its counts, schemas and hashes."""
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import Join
    from orcapod.sources import DictSource, SpiralDBTableSource

    overrides = {"server.url": args.server_url} if args.server_url else None
    calls: list[int] = []

    @function_pod(output_keys="score")
    def score(firing_rate: float, age: int, notes: str | None) -> float:
        """Workshop function pod: a firing rate scaled by age."""
        calls.append(1)
        return firing_rate * age

    def build() -> tuple[Any, Any]:
        """Build the source and the job over the shared Delta store."""
        src = SpiralDBTableSource(
            args.project, table_name, dataset=args.dataset, overrides=overrides
        )
        subjects = DictSource(
            data=[{"subject_id": "m1", "age": 10}, {"subject_id": "m2", "age": 20}],
            tag_columns=["subject_id"],
            source_id="subjects",
        )
        job = PipelineJob(
            name="itl926_workshop", store=DeltaTableDatabase(workdir / "store")
        )
        with job:
            joined = Join()(src, subjects, label="joined")
            score.pod(joined, label="score")
        return src, job

    out: dict[str, Any] = {}
    src, job = build()
    out["content_hash"] = src.content_hash().to_string()
    out["pipeline_hash"] = src.pipeline_hash().to_string()
    out["tag_schema"] = {k: str(v) for k, v in src.output_schema()[0].items()}
    out["data_schema"] = {k: str(v) for k, v in src.output_schema()[1].items()}
    job.run()
    out["run1_calls"] = len(calls)
    out["scores"] = job.nodes["score"].as_table().to_pylist()
    records = job.nodes["score"].get_all_records()
    out["pipeline_records"] = None if records is None else records.num_rows
    if runs > 1:
        calls.clear()
        _, job2 = build()
        job2.run()
        out["run2_calls"] = len(calls)
        if save:
            job2.save(workdir / "job.json")
    return out


def _load_phase(args: argparse.Namespace) -> None:
    """Fresh-process phase of write mode: rebuild the source, load the job, print JSON."""
    from orcapod import PipelineJob
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import SpiralDBTableSource

    workdir = Path(args.workdir)
    overrides = {"server.url": args.server_url} if args.server_url else None
    out: dict[str, Any] = {}
    src = SpiralDBTableSource(
        args.project, args.table, dataset=args.dataset, overrides=overrides
    )
    out["content_hash"] = src.content_hash().to_string()
    out["pipeline_hash"] = src.pipeline_hash().to_string()
    loaded = PipelineJob.load(
        workdir / "job.json", store=DeltaTableDatabase(workdir / "store")
    )
    out["unbound"] = list(loaded.unbound_sources)
    records = loaded.nodes["score"].get_all_records()
    out["score_rows"] = None if records is None else records.num_rows
    print(json.dumps(out))


# ---------------------------------------------------------------------------------------
# Read-only mode: an existing project (the workshop's EDI sample). No writes of any kind.
# ---------------------------------------------------------------------------------------


def _ro_job(args: argparse.Namespace, workdir: Path) -> tuple[Any, Any, Any, list[int]]:
    """Build the read-only workshop job: Join(table A, table B) -> one-column function pod."""
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import Join, MapData, SelectDataColumns
    from orcapod.sources import SpiralDBTableSource

    overrides = {"server.url": args.server_url} if args.server_url else None
    left_name, right_name = args.tables.split(",")[:2]
    left = SpiralDBTableSource(
        args.project, left_name, args.dataset, overrides=overrides
    )
    right = SpiralDBTableSource(
        args.project, right_name, args.dataset, overrides=overrides
    )
    value_type = right.output_schema()[1][args.value_column]
    calls: list[int] = []

    def touch(value: Any) -> Any:
        """Return the value unchanged, counting the call."""
        calls.append(1)
        return value

    # The parameter and return type must equal the column's Python type exactly (a column
    # every value of which may be null arrives as ``T | None``).
    touch.__annotations__ = {"value": value_type, "return": value_type}
    pod = function_pod(output_keys="echoed")(touch)
    job = PipelineJob(
        name="itl926_readonly", store=DeltaTableDatabase(workdir / "store")
    )
    with job:
        joined = Join()(left, right, label="joined")
        only = SelectDataColumns([args.value_column])(joined, label="selected")
        renamed = MapData({args.value_column: "value"})(only, label="renamed")
        pod.pod(renamed, label="echo")
    return left, right, job, calls


def read_only(args: argparse.Namespace) -> dict[str, Any]:
    """Inspect ``--tables`` and run the workshop path on the first two; writes nothing remote."""
    import time

    from orcapod.sources import SpiralDBTableSource

    overrides = {"server.url": args.server_url} if args.server_url else None
    report: dict[str, Any] = {"tables": {}, "checks": {}}

    def check(name: str, ok: bool, detail: Any) -> None:
        """Record and print one pass/fail check."""
        report["checks"][name] = {"ok": bool(ok), "detail": detail}
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")

    for name in args.tables.split(","):
        entry: dict[str, Any] = {}
        report["tables"][name] = entry
        t0 = time.time()
        try:
            src = SpiralDBTableSource(
                args.project, name, args.dataset, overrides=overrides
            )
            entry["seconds"] = round(time.time() - t0, 2)
            table = src.as_table()
            entry["rows"] = table.num_rows
            entry["arrow"] = {f.name: _field_desc(f) for f in table.schema}
            tag, data = src.output_schema()
            entry["tag_schema"] = {k: str(v) for k, v in tag.items()}
            entry["data_schema"] = {k: str(v) for k, v in data.items()}
            entry["content_hash"] = src.content_hash().to_string()
            entry["pipeline_hash"] = src.pipeline_hash().to_string()
        except Exception as exc:  # noqa: BLE001 - every outcome is recorded
            entry["error"] = f"{type(exc).__name__}: {exc}"
        print(f"  {name}: {entry}")

    owned_workdir = args.workdir is None
    workdir = Path(tempfile.mkdtemp()) if owned_workdir else Path(args.workdir)
    left, right, job, calls = _ro_job(args, workdir)
    job.run()
    first_calls = len(calls)
    echoed = job.nodes["echo"].as_table()
    rows = echoed.num_rows
    # The function cache is keyed on the input value, so equal values compute once.
    # json text is hashable for list and struct values too.
    distinct = len(
        {
            json.dumps(v, sort_keys=True, default=str)
            for v in echoed.column("echoed").to_pylist()
        }
    )
    check(
        "run 1 computes each distinct value once",
        first_calls == distinct and rows > 0,
        {"calls": first_calls, "distinct_values": distinct, "rows": rows},
    )
    _, _, job2, calls2 = _ro_job(args, workdir)
    job2.run()
    check("run 2 makes no function calls", len(calls2) == 0, len(calls2))
    job2.save(workdir / "job.json")
    cmd = [
        sys.executable,
        __file__,
        "--phase",
        "ro-load",
        "--project",
        args.project,
        "--dataset",
        args.dataset,
        "--tables",
        args.tables,
        "--value-column",
        args.value_column,
        "--workdir",
        str(workdir),
    ]
    if args.server_url:
        cmd += ["--server-url", args.server_url]
    second = _run_child(cmd)
    report["fresh_process"] = second
    for label, src in (("left", left), ("right", right)):
        check(
            f"fresh process: same content_hash ({label})",
            second.get(f"{label}_content_hash") == src.content_hash().to_string(),
            second.get(f"{label}_content_hash"),
        )
        check(
            f"fresh process: same pipeline_hash ({label})",
            second.get(f"{label}_pipeline_hash") == src.pipeline_hash().to_string(),
            second.get(f"{label}_pipeline_hash"),
        )
    check(
        "loaded job: get_all_records returns every row",
        second.get("echo_rows") == rows,
        [rows, second.get("echo_rows")],
    )
    if owned_workdir:
        shutil.rmtree(workdir, ignore_errors=True)
    else:
        report["workdir"] = str(workdir)
    return report


def _ro_load_phase(args: argparse.Namespace) -> None:
    """Fresh-process phase of read-only mode: rebuild both sources, load the job, print JSON."""
    from orcapod import PipelineJob
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import SpiralDBTableSource

    overrides = {"server.url": args.server_url} if args.server_url else None
    workdir = Path(args.workdir)
    out: dict[str, Any] = {}
    for label, name in zip(("left", "right"), args.tables.split(",")[:2], strict=True):
        src = SpiralDBTableSource(args.project, name, args.dataset, overrides=overrides)
        out[f"{label}_content_hash"] = src.content_hash().to_string()
        out[f"{label}_pipeline_hash"] = src.pipeline_hash().to_string()
    loaded = PipelineJob.load(
        workdir / "job.json", store=DeltaTableDatabase(workdir / "store")
    )
    records = loaded.nodes["echo"].get_all_records()
    out["echo_rows"] = None if records is None else records.num_rows
    print(json.dumps(out))


def main() -> int:
    """Parse the command line, run the chosen mode, write the report, return the exit code."""
    parser = argparse.ArgumentParser(description="ITL-926 live SpiralDB source check")
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", default="default")
    parser.add_argument("--server-url", default=os.environ.get("SPIRAL_SERVER_URL"))
    parser.add_argument(
        "--allow-writes",
        default=None,
        metavar="PROJECT",
        help="write mode: create, write and drop scratch tables in PROJECT, which must "
        "equal --project and must not be an edi-* project. Without it nothing is written.",
    )
    parser.add_argument(
        "--tables",
        default="session,experiment",
        help="read-only mode: tables to inspect; the first two are joined",
    )
    parser.add_argument(
        "--value-column",
        default="total_trials",
        help="read-only mode: data column of the second table fed to the pod",
    )
    parser.add_argument(
        "--keep", action="store_true", help="write mode: keep scratch tables"
    )
    parser.add_argument("--report", default=None, help="write the JSON report here")
    parser.add_argument("--phase", default="all", choices=["all", "load", "ro-load"])
    parser.add_argument("--run-id", default=uuid.uuid4().hex[:8])
    parser.add_argument("--table", default=None)
    parser.add_argument("--workdir", default=None)
    args = parser.parse_args()

    if args.phase == "ro-load":
        _ro_load_phase(args)
        return 0
    if args.phase == "load":
        _load_phase(args)
        return 0

    if args.allow_writes is None:
        report = read_only(args)
    else:
        if args.allow_writes != args.project:
            parser.error("--allow-writes must name the same project as --project")
        if args.project.startswith("edi-"):
            parser.error("refusing to write to an edi-* project; they are read-only")
        run = Run(args)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                try:
                    run.storage_matrix()
                except Exception:  # noqa: BLE001
                    run.check("storage matrix", False, traceback.format_exc())
                try:
                    run.workshop_path(Path(tmp))
                except Exception:  # noqa: BLE001
                    run.check("workshop path", False, traceback.format_exc())
        finally:
            run.cleanup()
        report = run.report
    text = json.dumps(report, indent=2, default=str)
    if args.report:
        Path(args.report).write_text(text)
    print(text)
    return 0 if all(c["ok"] for c in report["checks"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
