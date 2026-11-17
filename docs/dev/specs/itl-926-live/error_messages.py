"""READ-ONLY: what an attendee sees for common mistakes with ``SpiralDBTableSource``.

Run after ``spiral login``, from the repository root:

    uv run python superpowers/specs/itl-926-live/error_messages.py edi-sample-v1-855632

Each case only lists tables or reads a table's schema; nothing is written. The made-up
project id is random, so it cannot name a real project (in particular, never a full EDI
project). Prints the exception type and message for each case, and how long it took.
"""

from __future__ import annotations

import signal
import sys
import time
import uuid

from orcapod.sources import SpiralDBTableSource


def attempt(label: str, **kwargs: str) -> None:
    """Build a source with ``kwargs`` and print the error it raises, if any."""
    start = time.time()
    try:
        SpiralDBTableSource(**kwargs)  # pyright: ignore[reportArgumentType]
        outcome = "constructed (no error)"
    except Exception as exc:  # noqa: BLE001 - the message is what we are after
        outcome = f"{type(exc).__module__}.{type(exc).__name__}: {exc}"
    print(f"[{label}] ({time.time() - start:.1f}s)\n    {outcome}\n")


def main() -> None:
    """Try each common mistake against the project named on the command line."""
    signal.alarm(300)
    project = sys.argv[1]
    attempt("wrong table name", project_id=project, table_name="no_such_table")
    attempt(
        "table name with dataset prefix",
        project_id=project,
        table_name="default.subject",
    )
    attempt(
        "wrong dataset",
        project_id=project,
        table_name="subject",
        dataset="no_such_dataset",
    )
    attempt(
        "project that does not exist",
        project_id=f"no-such-project-{uuid.uuid4().hex[:6]}",
        table_name="subject",
    )


if __name__ == "__main__":
    main()
