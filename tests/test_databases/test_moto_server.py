"""The moto server fixture helper starts a server and leaves no process behind."""

from __future__ import annotations

import socket
import subprocess
import sys

import pytest

from tests.test_databases.moto_server import moto_server, stop_process


def _refuses_connections(endpoint: str) -> bool:
    host, port = endpoint.removeprefix("http://").split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=1):
            return False
    except OSError:
        return True


def test_server_answers_while_open_and_is_gone_after():
    with moto_server() as endpoint:
        assert not _refuses_connections(endpoint)
    assert _refuses_connections(endpoint)


def test_server_is_stopped_when_the_body_raises():
    endpoint = ""
    with pytest.raises(ZeroDivisionError):
        with moto_server() as endpoint:
            raise ZeroDivisionError
    assert endpoint
    assert _refuses_connections(endpoint)


def test_stop_process_reaps_a_child_that_ignores_sigterm():
    code = (
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print('ready', flush=True)\n"
        "time.sleep(60)\n"
    )
    proc = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE)
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == b"ready"  # SIGTERM is now ignored
    stop_process(proc, terminate_timeout=0.5)
    proc.stdout.close()
    assert proc.returncode is not None
