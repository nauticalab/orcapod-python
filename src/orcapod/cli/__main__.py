"""Run the Orcapod CLI as a module: ``python -m orcapod.cli``.

Equivalent to the ``orcapod`` console script, but invocable with an explicit
interpreter — which lets tests drive the CLI in a subprocess without depending
on which console scripts happen to be on ``PATH``.
"""

from __future__ import annotations

from orcapod.cli import app

if __name__ == "__main__":
    app()
