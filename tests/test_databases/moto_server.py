"""A moto S3 server in a subprocess, for the S3 tests (ITL-1079).

It is a subprocess, not ``moto.server.ThreadedMotoServer``, because the threaded server
runs in the test process and deadlocks with deltalake: deltalake holds the GIL while it
works on a table (``DeltaTable.update_incremental`` during an append), so the server
thread never answers and the client times out after 30 s. Reproduction: start a
``ThreadedMotoServer``, ``write_deltalake`` a table to it, then append to the same
table.
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator

S3_ACCESS_KEY = "testing"
S3_SECRET_KEY = "testing"
START_TIMEOUT_SECONDS = 60.0


def free_localhost_port() -> int:
    """Return a TCP port on 127.0.0.1 that is free right now."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop_process(
    proc: subprocess.Popen[bytes], terminate_timeout: float = 10.0
) -> None:
    """Stop a child process and reap it.

    Sends SIGTERM and waits ``terminate_timeout`` seconds, then SIGKILL; either way the
    child has been waited for when this returns.

    Args:
        proc: The child process.
        terminate_timeout: Seconds to wait after SIGTERM before killing.
    """
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=terminate_timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@contextlib.contextmanager
def moto_server() -> Iterator[str]:
    """Run a moto S3 server in a subprocess and yield its endpoint URL.

    A subprocess, not ``ThreadedMotoServer``: deltalake holds the GIL while it works
    on a table, so a server thread in the test process cannot answer it and an append
    hangs. Raises ``RuntimeError`` with the server's output if it does not come up.
    """
    port = free_localhost_port()
    endpoint = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
    }
    with tempfile.TemporaryFile() as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        try:
            deadline = time.monotonic() + START_TIMEOUT_SECONDS
            while True:
                if proc.poll() is not None:
                    break
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=1):
                        pass
                except OSError:
                    if time.monotonic() > deadline:
                        break
                    time.sleep(0.1)
                    continue
                yield endpoint
                return
            log.seek(0)
            output = log.read().decode(errors="replace")
            raise RuntimeError(
                f"The moto S3 server did not start on {endpoint}:\n{output}"
            )
        finally:
            stop_process(proc)
