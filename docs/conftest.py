"""Conftest for pytest-codeblocks doc tests.

Patches the pytest-codeblocks exec namespace to include ``__name__`` so that
functions defined inside code blocks get a proper ``__module__`` attribute.
Without this, orcapod's ``get_function_signature`` fails because
``function.__module__`` is ``None``.
"""

from __future__ import annotations

try:
    from pytest_codeblocks import plugin as _codeblocks_plugin

    _original_runtest = _codeblocks_plugin.TestBlock.runtest

    def _patched_runtest(self):
        """Run code blocks with __name__ set in the exec globals."""
        import contextlib
        import io
        import re as re_mod
        import subprocess

        assert self.obj is not None
        output = None

        if self.obj.importorskip is not None:
            import pytest

            try:
                __import__(self.obj.importorskip)
            except (ImportError, ModuleNotFoundError):
                pytest.skip()

        if self.obj.syntax == "python":
            with contextlib.redirect_stdout(io.StringIO()) as s:
                try:
                    exec(
                        self.obj.code,
                        {"__MODULE__": "__main__", "__name__": "__main__"},
                    )
                except Exception as e:
                    raise RuntimeError(
                        f"{self.name}, line {self.obj.lineno}:\n```\n"
                        + self.obj.code
                        + "```\n\n"
                        + f"{e}"
                    )
            output = s.getvalue()
        else:
            assert self.obj.syntax in ["sh", "bash"]
            executable = {
                "sh": None,
                "bash": "/bin/bash",
                "zsh": "/bin/zsh",
            }[self.obj.syntax]
            ret = subprocess.run(
                self.obj.code,
                shell=True,
                check=True,
                stdout=subprocess.PIPE,
                executable=executable,
            )
            output = ret.stdout.decode()

        if output is not None and self.obj.expected_output is not None:
            str0 = self.obj.expected_output
            str1 = output
            if getattr(self.obj, "expected_output_ignore_whitespace", False):
                str0 = re_mod.sub(r"\s+", "", str0)
                str1 = re_mod.sub(r"\s+", "", str1)
            if str0 != str1:
                raise RuntimeError(
                    f"{self.name}, line {self.obj.lineno}:\n```\n"
                    + f"Expected output\n```\n{self.obj.expected_output}```\n"
                    + f"but got\n```\n{output}```"
                )

    _codeblocks_plugin.TestBlock.runtest = _patched_runtest
except ImportError:
    pass
