"""Capture infrastructure for observability logging.

Provides context-variable-local capture of stdout, stderr, and Python logging
for use in FunctionNode execution.  Thread-safe and asyncio-task-safe via
``contextvars.ContextVar`` — captures from concurrent packets never intermingle.

CapturedLogs travel as part of the return type through the call chain
(``direct_call`` → ``call`` → ``process_packet`` → FunctionNode) so there
is no ContextVar side-channel for logs.  Each executor's ``execute_callable``
returns ``(raw_result, CapturedLogs)``, and ``direct_call`` returns
``(output_packet, CapturedLogs)`` — catching user-function exceptions
internally rather than re-raising.

Typical usage
-------------
Call ``install_capture_streams()`` once when a logging Observer is created.
The executor or ``direct_call`` wraps function execution in
``LocalCaptureContext`` and returns CapturedLogs alongside the result::

    result, captured = packet_function.call(packet)
    pkt_logger.record(captured)
"""

from __future__ import annotations

import contextvars
import io
import logging
import sys
from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# CapturedLogs
# ---------------------------------------------------------------------------


@dataclass
class CapturedLogs:
    """I/O captured from a single packet function execution."""

    stdout_log: str = ""
    stderr_log: str = ""
    python_logs: str = ""
    traceback: str | None = None
    success: bool = True

    def as_dict(self) -> dict[str, Any]:
        """Return fields as a plain dict (for passing to ``logger.record(**d)``)."""
        return {
            "stdout_log": self.stdout_log,
            "stderr_log": self.stderr_log,
            "python_logs": self.python_logs,
            "traceback": self.traceback,
            "success": self.success,
        }


# ---------------------------------------------------------------------------
# Context variables
# ---------------------------------------------------------------------------
# Each asyncio task and thread gets its own copy of these variables, so
# captures from concurrent packets never intermingle.

_stdout_capture: contextvars.ContextVar[io.StringIO | None] = contextvars.ContextVar(
    "_stdout_capture", default=None
)
_stderr_capture: contextvars.ContextVar[io.StringIO | None] = contextvars.ContextVar(
    "_stderr_capture", default=None
)
_log_capture: contextvars.ContextVar[list[str] | None] = contextvars.ContextVar(
    "_log_capture", default=None
)


# ---------------------------------------------------------------------------
# ContextLocalTeeStream
# ---------------------------------------------------------------------------


class ContextLocalTeeStream:
    """A stream that writes to the original *and* a per-context capture buffer.

    All writes go to *original* (terminal output is preserved) and also to a
    ``StringIO`` buffer active for the current asyncio task / thread (selected
    via ``capture_var``).  Concurrent tasks each have their own buffer and do
    not interfere with each other.
    """

    def __init__(
        self,
        original: Any,
        capture_var: contextvars.ContextVar[io.StringIO | None],
    ) -> None:
        self._original = original
        self._capture_var = capture_var

    def write(self, s: str) -> int:
        buf = self._capture_var.get()
        if buf is not None:
            buf.write(s)
        return self._original.write(s)

    def flush(self) -> None:
        buf = self._capture_var.get()
        if buf is not None:
            buf.flush()
        self._original.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._original, name)


# ---------------------------------------------------------------------------
# ContextVarLoggingHandler
# ---------------------------------------------------------------------------


class ContextVarLoggingHandler(logging.Handler):
    """A logging handler that captures records into a per-context buffer.

    When a capture buffer is active for the current context (asyncio task or
    thread), log records are formatted and appended to it.  When no buffer is
    active the record is silently discarded (not duplicated to other handlers).
    """

    def emit(self, record: logging.LogRecord) -> None:
        buf = _log_capture.get()
        if buf is not None:
            buf.append(self.format(record))


# ---------------------------------------------------------------------------
# Global installation (idempotent)
# ---------------------------------------------------------------------------

_installed = False
_logging_handler: ContextVarLoggingHandler | None = None


def install_capture_streams() -> None:
    """Install tee streams and the logging handler globally.

    Idempotent — safe to call multiple times.  Should be called once when a
    concrete logging Observer is instantiated.

    After installation:

    * ``sys.stdout`` / ``sys.stderr`` tee writes to per-context buffers while
      also forwarding to the original stream (terminal output preserved).
    * The root logger gains a ``ContextVarLoggingHandler`` that captures
      records to per-context buffers (covering Python ``logging`` calls).

    Note:
        Subprocess and C-extension output bypasses Python's stream objects and
        goes directly to file descriptors 1/2.  For local execution these are
        *not* captured (but are still visible in the terminal).  Ray remote
        execution uses fd-level capture via
        ``RayExecutor._make_capture_wrapper``.

        The stream check runs on every call so that if something (e.g. a test
        harness) replaces ``sys.stdout``/``sys.stderr`` between calls we
        re-wrap the new stream.  The logging handler is only added once.
    """
    global _installed, _logging_handler

    # Always re-check in case sys.stdout/stderr was replaced (e.g. by pytest).
    if not isinstance(sys.stdout, ContextLocalTeeStream):
        sys.stdout = ContextLocalTeeStream(sys.stdout, _stdout_capture)
    if not isinstance(sys.stderr, ContextLocalTeeStream):
        sys.stderr = ContextLocalTeeStream(sys.stderr, _stderr_capture)

    if _installed:
        return

    _logging_handler = ContextVarLoggingHandler()
    _logging_handler.setFormatter(
        logging.Formatter("%(levelname)s:%(name)s:%(message)s")
    )
    logging.getLogger().addHandler(_logging_handler)

    _installed = True


# ---------------------------------------------------------------------------
# LocalCaptureContext
# ---------------------------------------------------------------------------


class LocalCaptureContext:
    """Context manager that activates per-context capture for one packet.

    Requires ``install_capture_streams()`` to have been called; without it the
    ContextVars are set but nothing tees into them, so captured strings will be
    empty (acceptable when no logging Observer is configured).

    Example::

        ctx = LocalCaptureContext()
        try:
            with ctx:
                result = call_something()
        except Exception:
            captured = ctx.get_captured(success=False, tb=traceback.format_exc())
        else:
            captured = ctx.get_captured(success=True)
    """

    def __init__(self) -> None:
        self._stdout_buf = io.StringIO()
        self._stderr_buf = io.StringIO()
        self._log_buf: list[str] = []
        self._tokens: list[contextvars.Token] = []

    def __enter__(self) -> LocalCaptureContext:
        self._tokens = [
            _stdout_capture.set(self._stdout_buf),
            _stderr_capture.set(self._stderr_buf),
            _log_capture.set(self._log_buf),
        ]
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        for token, var in zip(
            self._tokens,
            [_stdout_capture, _stderr_capture, _log_capture],
        ):
            var.reset(token)
        return False  # do not suppress exceptions

    def get_captured(
        self,
        success: bool,
        tb: str | None = None,
    ) -> CapturedLogs:
        """Return a `CapturedLogs` from what was captured in this context."""
        return CapturedLogs(
            stdout_log=self._stdout_buf.getvalue(),
            stderr_log=self._stderr_buf.getvalue(),
            python_logs="\n".join(self._log_buf) if self._log_buf else "",
            traceback=tb,
            success=success,
        )

