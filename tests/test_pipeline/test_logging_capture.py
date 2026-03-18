"""Tests for logging_capture — CapturedLogs, tee streams, LocalCaptureContext,
and the Ray worker-side wrapper."""

from __future__ import annotations

import asyncio
import concurrent.futures
import io
import logging
import sys

import pytest

from orcapod.pipeline.logging_capture import (
    CapturedLogs,
    ContextLocalTeeStream,
    ContextVarLoggingHandler,
    LocalCaptureContext,
    _log_capture,
    _stderr_capture,
    _stdout_capture,
    install_capture_streams,
)


# ---------------------------------------------------------------------------
# CapturedLogs
# ---------------------------------------------------------------------------


class TestCapturedLogs:
    def test_defaults(self):
        c = CapturedLogs()
        assert c.stdout == ""
        assert c.stderr == ""
        assert c.python_logs == ""
        assert c.traceback is None
        assert c.success is True

    def test_fields(self):
        c = CapturedLogs(stdout="out", stderr="err", traceback="tb", success=False)
        assert c.stdout == "out"
        assert c.stderr == "err"
        assert c.traceback == "tb"
        assert c.success is False


# ---------------------------------------------------------------------------
# ContextLocalTeeStream
# ---------------------------------------------------------------------------


class TestContextLocalTeeStream:
    def test_tees_to_buffer_and_original(self):
        buf = io.StringIO()
        original = io.StringIO()
        token = _stdout_capture.set(buf)
        tee = ContextLocalTeeStream(original, _stdout_capture)
        try:
            tee.write("hello")
            tee.flush()
        finally:
            _stdout_capture.reset(token)

        assert buf.getvalue() == "hello"
        assert original.getvalue() == "hello"

    def test_no_buffer_writes_only_original(self):
        original = io.StringIO()
        tee = ContextLocalTeeStream(original, _stdout_capture)
        # No ContextVar buffer set — should just write to original
        tee.write("world")
        assert original.getvalue() == "world"

    def test_isolation_between_contexts(self):
        """Two concurrent contexts each capture only their own writes."""
        buf_a = io.StringIO()
        buf_b = io.StringIO()
        original = io.StringIO()
        tee = ContextLocalTeeStream(original, _stdout_capture)

        token_a = _stdout_capture.set(buf_a)
        tee.write("from-A")
        _stdout_capture.reset(token_a)

        token_b = _stdout_capture.set(buf_b)
        tee.write("from-B")
        _stdout_capture.reset(token_b)

        assert buf_a.getvalue() == "from-A"
        assert buf_b.getvalue() == "from-B"
        assert original.getvalue() == "from-Afrom-B"

    def test_proxy_attributes(self):
        original = io.StringIO()
        tee = ContextLocalTeeStream(original, _stdout_capture)
        # Attribute delegation should work
        assert tee.writable() == original.writable()


# ---------------------------------------------------------------------------
# ContextVarLoggingHandler
# ---------------------------------------------------------------------------


class TestContextVarLoggingHandler:
    def test_captures_to_active_buffer(self):
        handler = ContextVarLoggingHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        buf: list[str] = []
        token = _log_capture.set(buf)
        try:
            record = logging.LogRecord(
                name="test", level=logging.INFO,
                pathname="", lineno=0, msg="hello log", args=(), exc_info=None
            )
            handler.emit(record)
        finally:
            _log_capture.reset(token)

        assert buf == ["hello log"]

    def test_discards_when_no_buffer(self):
        handler = ContextVarLoggingHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        # No ContextVar buffer — emit should be a no-op
        record = logging.LogRecord(
            name="test", level=logging.INFO,
            pathname="", lineno=0, msg="ignored", args=(), exc_info=None
        )
        handler.emit(record)  # should not raise


# ---------------------------------------------------------------------------
# LocalCaptureContext
# ---------------------------------------------------------------------------


class TestLocalCaptureContext:
    def test_captures_nothing_without_install(self):
        """Without install_capture_streams(), capture returns empty strings."""
        ctx = LocalCaptureContext()
        with ctx:
            print("not captured")
        captured = ctx.get_captured(success=True)
        # Since streams are not installed, buffer is empty
        assert captured.stdout == ""
        assert captured.success is True

    def test_captures_after_install(self):
        install_capture_streams()
        ctx = LocalCaptureContext()
        with ctx:
            print("captured output")
        captured = ctx.get_captured(success=True)
        assert "captured output" in captured.stdout
        assert captured.success is True

    def test_captures_stderr_after_install(self):
        install_capture_streams()
        ctx = LocalCaptureContext()
        with ctx:
            print("error output", file=sys.stderr)
        captured = ctx.get_captured(success=True)
        assert "error output" in captured.stderr

    def test_exception_does_not_suppress(self):
        install_capture_streams()
        ctx = LocalCaptureContext()
        with pytest.raises(ValueError, match="boom"):
            with ctx:
                print("before error")
                raise ValueError("boom")

    def test_captures_partial_output_on_exception(self):
        install_capture_streams()
        ctx = LocalCaptureContext()
        try:
            with ctx:
                print("before error")
                raise ValueError("boom")
        except ValueError:
            pass
        captured = ctx.get_captured(success=False, tb="traceback text")
        assert "before error" in captured.stdout
        assert captured.success is False
        assert captured.traceback == "traceback text"

    def test_isolation_between_concurrent_asyncio_tasks(self):
        """Each asyncio task captures only its own output."""
        install_capture_streams()

        async def run_task(label: str) -> str:
            ctx = LocalCaptureContext()
            with ctx:
                await asyncio.sleep(0)  # yield to other tasks
                print(label)
                await asyncio.sleep(0)
            return ctx.get_captured(success=True).stdout

        async def main():
            results = await asyncio.gather(
                run_task("task-A"), run_task("task-B")
            )
            return results

        a_out, b_out = asyncio.run(main())
        assert "task-A" in a_out
        assert "task-B" not in a_out
        assert "task-B" in b_out
        assert "task-A" not in b_out

    def test_isolation_between_threads(self):
        """Each thread captures only its own output."""
        install_capture_streams()
        results: dict[str, str] = {}

        def worker(label: str) -> None:
            ctx = LocalCaptureContext()
            with ctx:
                print(label)
            results[label] = ctx.get_captured(success=True).stdout

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futs = [pool.submit(worker, lbl) for lbl in ("thread-X", "thread-Y")]
            for f in futs:
                f.result()

        assert "thread-X" in results["thread-X"]
        assert "thread-Y" not in results["thread-X"]
        assert "thread-Y" in results["thread-Y"]
        assert "thread-X" not in results["thread-Y"]

    def test_python_logging_captured(self):
        install_capture_streams()
        test_logger = logging.getLogger("test.capture")
        ctx = LocalCaptureContext()
        with ctx:
            test_logger.warning("log message")
        captured = ctx.get_captured(success=True)
        assert "log message" in captured.python_logs

    def test_resets_contextvar_after_exit(self):
        install_capture_streams()
        ctx = LocalCaptureContext()
        with ctx:
            pass
        # ContextVars should be None after context exits
        assert _stdout_capture.get() is None
        assert _stderr_capture.get() is None
        assert _log_capture.get() is None


# ---------------------------------------------------------------------------
# RayExecutor._make_capture_wrapper (local process — no Ray cluster needed)
# ---------------------------------------------------------------------------


class TestRayCaptureWrapper:
    """Tests exercise :meth:`RayExecutor._make_capture_wrapper` directly.

    The wrapper runs in the same process (no Ray cluster needed).
    On success it returns a 4-tuple; on failure it raises a
    ``_CapturedTaskError`` carrying the captured I/O.
    """

    @staticmethod
    def _call_wrapper(fn, kwargs):
        """Helper: call the Ray capture wrapper and return (raw, CapturedLogs)."""
        from orcapod.core.executors.ray import RayExecutor

        wrapper = RayExecutor._make_capture_wrapper()
        try:
            raw, stdout, stderr, python_logs = wrapper(fn, kwargs)
        except Exception as exc:
            if hasattr(exc, "captured_stdout"):
                return None, CapturedLogs(
                    stdout=exc.captured_stdout,
                    stderr=exc.captured_stderr,
                    python_logs=exc.captured_python_logs,
                    traceback=exc.captured_traceback,
                    success=False,
                )
            raise
        return raw, CapturedLogs(
            stdout=stdout, stderr=stderr, python_logs=python_logs,
            success=True,
        )

    def test_captures_stdout(self):
        def fn(x):
            print(f"result={x}")
            return x * 2

        raw, captured = self._call_wrapper(fn, {"x": 3})
        assert raw == 6
        assert "result=3" in captured.stdout
        assert captured.success is True
        assert captured.traceback is None

    def test_captures_stderr(self):
        def fn():
            import sys
            print("err line", file=sys.stderr)
            return 1

        raw, captured = self._call_wrapper(fn, {})
        assert raw == 1
        assert "err line" in captured.stderr

    def test_captures_exception(self):
        def fn():
            raise RuntimeError("worker blew up")

        raw, captured = self._call_wrapper(fn, {})
        assert raw is None
        assert captured.success is False
        assert "RuntimeError" in captured.traceback
        assert "worker blew up" in captured.traceback

    def test_captures_python_logging(self):
        def fn():
            logging.getLogger("ray_wrapper_test").error("log from worker")
            return True

        raw, captured = self._call_wrapper(fn, {})
        assert raw is True
        assert "log from worker" in captured.python_logs

    def test_restores_fds_after_exception(self):
        """File descriptors 1/2 must be restored even when fn raises."""
        import os

        original_stdout_fd = os.dup(1)
        try:
            def fn():
                raise ValueError("oops")

            self._call_wrapper(fn, {})

            # Write to fd 1 — should succeed (not be pointing at a closed temp file)
            os.write(1, b"")
        finally:
            os.close(original_stdout_fd)
