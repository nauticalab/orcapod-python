"""Run a command many times and count the runs that do not exit before a deadline.

Usage:
    harness.py --label LABEL -n 40 -t 20 -j 4 -- <command ...>

Each run is a child process in its own session. A run that misses the deadline is
counted as hung; the first hung child of a cell has its native stack sampled with
macOS ``sample`` (or ``eu-stack``/``gdb`` on Linux when present) before it is killed.
A summary line is printed and appended as JSON to ``results.jsonl`` next to this file.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
SAMPLES = HERE / "samples"


def _sample_stack(pid: int, label: str) -> str | None:
    """Capture the native stack of a hung process, returning the file path."""
    SAMPLES.mkdir(exist_ok=True)
    out = SAMPLES / f"{label.replace('/', '_').replace(' ', '_')}.txt"
    if sys.platform == "darwin" and shutil.which("sample"):
        cmd = ["sample", str(pid), "1", "-file", str(out)]
    elif shutil.which("gdb"):
        cmd = ["gdb", "-p", str(pid), "-batch", "-ex", "thread apply all bt"]
    elif shutil.which("eu-stack"):
        cmd = ["eu-stack", "-p", str(pid)]
    else:
        return None
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except Exception:  # noqa: BLE001 - diagnostics only
        return None
    if not out.exists():
        out.write_text(res.stdout + res.stderr)
    return str(out.relative_to(HERE))


def _run_one(cmd: list[str], timeout: float, env: dict[str, str]) -> dict[str, Any]:
    """Run ``cmd`` once and classify the outcome.

    Args:
        cmd: The command to run.
        timeout: Seconds before the run counts as hung.
        env: Environment for the child.

    Returns:
        The outcome. A hung run also carries the live process, for the caller to sample
        and kill.
    """
    t0 = time.monotonic()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        env=env,
    )
    try:
        out, err = proc.communicate(timeout=timeout)
        return {
            "hung": False,
            "rc": proc.returncode,
            "done": "done" in out,
            "secs": round(time.monotonic() - t0, 2),
            "err": err[-400:] if proc.returncode else "",
        }
    except subprocess.TimeoutExpired:
        return {"hung": True, "proc": proc, "secs": timeout}


def main() -> int:
    """Run one cell of the matrix and append its summary to ``results.jsonl``.

    Returns:
        The process exit code.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("-n", type=int, default=40)
    ap.add_argument("-t", type=float, default=20.0)
    ap.add_argument("-j", type=int, default=4)
    ap.add_argument("--no-sample", action="store_true")
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    args = ap.parse_args()
    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    env = dict(os.environ)

    results: list[dict[str, Any]] = []
    sampled: str | None = None
    with ThreadPoolExecutor(max_workers=args.j) as ex:
        for r in ex.map(lambda _: _run_one(cmd, args.t, env), range(args.n)):
            if r["hung"]:
                proc = r.pop("proc")
                if sampled is None and not args.no_sample:
                    sampled = _sample_stack(proc.pid, args.label) or "unavailable"
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (PermissionError, ProcessLookupError):
                    # macOS refuses killpg once the leader is a zombie; it already exited.
                    proc.kill()
                out, _ = proc.communicate()
                r["done"] = "done" in (out or "")
            results.append(r)

    hung = sum(r["hung"] for r in results)
    rcs = Counter("HUNG" if r["hung"] else r["rc"] for r in results)
    hung_after_done = sum(r["hung"] and r["done"] for r in results)
    errors = sorted({r["err"] for r in results if r.get("err")})[:2]
    summary = {
        "label": args.label,
        "n": args.n,
        "hung": hung,
        "hung_after_done": hung_after_done,
        "rc": {str(k): v for k, v in rcs.items()},
        "timeout_s": args.t,
        "jobs": args.j,
        "platform": f"{platform.system()} {platform.machine()}",
        "stack": sampled,
        "errors": errors,
        "cmd": " ".join(cmd),
    }
    print(
        f"{args.label}: hung {hung}/{args.n} (after 'done': {hung_after_done}) rc={dict(rcs)}"
        + (f" stack={sampled}" if sampled else "")
        + (f" ERR={errors[0][-200:]!r}" if errors else ""),
        flush=True,
    )
    with open(HERE / "results.jsonl", "a") as fh:
        fh.write(json.dumps(summary) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
