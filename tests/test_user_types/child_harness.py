"""Run workshop scenarios in fresh Python processes (ITL-881 tests).

A user class's identity depends on process-wide state (where the class is defined, what
is registered, Arrow's global extension registry), so the scenarios run each "kernel" or
"script" in its own interpreter. A child reports by printing one ``JSON:<payload>`` line
through ``report``.

``WSJOB`` is a module the scenarios write next to their scripts: it runs one function pod
over a ``DictSource`` in a ``PipelineJob`` stored in a ``DeltaTableDatabase``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

JSON_MARKER = "JSON:"
HARNESS_DIR = Path(__file__).parent


def write_files(root: Path, files: Mapping[str, str]) -> None:
    """Write ``files`` (relative path to dedented content) under ``root``."""
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        _ = path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")


def run_child(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
    timeout: float = 300.0,
) -> Any:
    """Run ``python <argv>`` in ``cwd`` and return the payload it reported.

    Args:
        argv: Arguments after the interpreter, e.g. ``["-c", code]``.
        cwd: Working directory; also put on ``PYTHONPATH`` with this harness.
        env: Extra environment variables.
        timeout: Seconds before the child is killed.

    Returns:
        The reported payload.

    Raises:
        AssertionError: If the child exits non-zero or reports nothing.
    """
    child_env = dict(os.environ)
    child_env["PYTHONPATH"] = os.pathsep.join([str(cwd), str(HARNESS_DIR)])
    child_env.update(env or {})
    proc = subprocess.run(
        [sys.executable, *argv],
        cwd=cwd,
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"child failed (exit {proc.returncode}):\n{proc.stderr[-4000:]}"
        )
    for line in proc.stdout.splitlines():
        if line.startswith(JSON_MARKER):
            return json.loads(line[len(JSON_MARKER) :])
    raise AssertionError(
        f"child reported nothing (exit {proc.returncode}):\n{proc.stderr[-4000:]}"
    )


def report(payload: Any) -> None:
    """Print ``payload`` as the JSON line the parent reads, then exit at once.

    ``os._exit`` skips interpreter shutdown: a process that read Delta tables sometimes
    hangs in Arrow's thread-pool destructor at shutdown (seen in the ITL-881
    experiments; out of scope here).
    """
    print(JSON_MARKER + json.dumps(payload, default=str))
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


def error_text(exc: BaseException) -> str:
    """Return ``"<Type>: <first line>"`` for an exception."""
    first = str(exc).splitlines()[0] if str(exc) else ""
    return f"{type(exc).__name__}: {first}"


#: Job module written next to the scenario scripts.
WSJOB = '''
import os
from pathlib import Path

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource


def table_paths(root):
    """Return the sorted relative paths of the Delta tables under ``root``."""
    return sorted(
        str(p.parent.relative_to(root)) for p in Path(root).rglob("_delta_log") if p.is_dir()
    )


def stored_fields(root, column):
    """Return the field names of each stored struct column ``column``, as written."""
    import deltalake
    import pyarrow as pa

    found = []
    for rel in table_paths(root):
        schema = pa.schema(deltalake.DeltaTable(str(Path(root) / rel)).schema().to_arrow())
        if column in schema.names:
            stored = schema.field(column).type
            stored = getattr(stored, "storage_type", stored)
            found.append([field.name for field in stored])
    return sorted(found)


def make_pod(point_cls):
    """Return ``(pod, calls)`` for ``shift(p: point_cls) -> point_cls``."""
    calls = []

    def shift(p):
        """Record the call and return ``p`` unchanged."""
        calls.append(p)
        return p

    shift.__annotations__ = {"p": point_cls, "return": point_cls}
    return function_pod(output_keys="q")(shift), calls


def run_job(point_cls, pod_and_calls=None, rows=None):
    """Run the shift pod over two rows; report calls, rows and identity."""
    pod, calls = pod_and_calls or make_pod(point_cls)
    if rows is None:
        names = list(getattr(point_cls, "model_fields", None) or point_cls.__dataclass_fields__)
        # A distinct value per field, so that swapped fields would be noticed.
        rows = [{n: 10 * i + j for j, n in enumerate(names)} for i in (1, 2)]
    data = [{"k": f"r{i}", "p": point_cls(**row)} for i, row in enumerate(rows, 1)]
    before = len(calls)
    db_root = os.environ["WS_DB"]
    job = PipelineJob(name="ws", database=DeltaTableDatabase(Path(db_root)))
    with job:
        pod.pod(DictSource(data=data, tag_columns=["k"], source_id="s"), label="shift")
    job.run()
    node = job.nodes["shift"]
    values = [d.as_dict()["q"] for _, d in node.iter_data()]
    return {
        "calls": len(calls) - before,
        "rows": node.as_table().num_rows,
        "q_class_is_point": all(type(v) is point_cls for v in values),
        "values": sorted(repr(v) for v in values),
        "stored_fields": stored_fields(db_root, "q"),
        "pod_pipeline_hash": pod.pod.pipeline_hash().to_string(),
        "pod_uri": list(pod.pod.uri),
        "node_identity_path": list(node.node_identity_path),
        "table_paths": table_paths(db_root),
        "extension_name": getattr(
            node.as_table().schema.field("q").type, "extension_name", None
        ),
    }
'''
