"""Helpers that keep a ``StoreError`` intact on its way out of a run.

A failed store read or write always fails the run (ITL-878, ITL-884). User callbacks that run while
the error propagates (a post-run hook, an observer callback) may fail too; their
failure is logged and noted on the store error, never raised in its place.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

from orcapod.errors import StoreError

logger = logging.getLogger(__name__)


def note_callback_failure(
    store_error: StoreError, during: str, failure: Exception
) -> None:
    """Log a callback failure and record it on the propagating store error.

    Args:
        store_error: The store error on its way out of the run.
        during: What was running, for the log and the note, such as
            ``"Observer on_data_crash"``.
        failure: The exception the callback raised.
    """
    logger.warning(
        "%s raised while a store failure was propagating, which is raised "
        + "instead: %s",
        during,
        failure,
        exc_info=failure,
    )
    store_error.add_note(
        f"{during} also raised while this error propagated: "
        + f"{type(failure).__name__}: {failure}"
    )


@contextmanager
def keep_store_error(store_error: StoreError | None, during: str) -> Iterator[None]:
    """Run a callback so that it cannot replace a propagating store error.

    With ``store_error`` set, an ``Exception`` the block raises is logged and noted on
    ``store_error`` (see ``note_callback_failure``) and not re-raised, so the caller's
    ``raise`` propagates the store error. With ``store_error=None`` the block runs
    unchanged and its exceptions propagate.

    Args:
        store_error: The store error on its way out, or ``None`` when none is.
        during: What the block runs, for the log and the note.

    Yields:
        Nothing; the block runs inside the context.
    """
    if store_error is None:
        yield
        return
    try:
        yield
    except Exception as failure:
        note_callback_failure(store_error, during, failure)
