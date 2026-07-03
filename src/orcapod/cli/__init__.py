"""Orcapod command-line interface.

Provides the ``orcapod`` entry point. Sub-commands are registered below.

Usage::

    orcapod warm-cache /data/recordings --min-size 500
"""

from __future__ import annotations

import typer

from orcapod.cli.warm_cache import warm_cache

app = typer.Typer(
    name="orcapod",
    help="Orcapod pipeline utilities.",
    no_args_is_help=True,
)


@app.callback()
def _main() -> None:
    """Orcapod pipeline utilities."""


app.command("warm-cache")(warm_cache)
