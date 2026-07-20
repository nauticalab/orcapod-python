"""Orcapod command-line interface.

Provides the ``orcapod`` entry point. Sub-commands are registered below.
Run ``orcapod --help`` for usage.
"""

from __future__ import annotations

import typer

from orcapod.cli.warm_cache import warm_cache
from orcapod.cli.migrate import migrate_app

app = typer.Typer(
    name="orcapod",
    help="Orcapod pipeline utilities.",
    no_args_is_help=True,
)


@app.callback()
def _main() -> None:
    """Orcapod pipeline utilities."""


app.command("warm-cache")(warm_cache)
app.add_typer(migrate_app, name="migrate")
