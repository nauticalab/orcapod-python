"""Helpers that keep a ``OrcapodDatabaseError`` intact on its way out of a run.

A failed database read or write always fails the run (ITL-878, ITL-884), and so does a
stored row that cannot be decoded as its class (``TypeIdentityError``, ITL-881). User
callbacks that run while the error propagates (a post-run hook, an observer callback)
may fail too; their failure is logged and noted on the error, never raised in its place.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from orcapod.errors import OrcapodDatabaseError, TypeIdentityError

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

logger = logging.getLogger(__name__)


def note_callback_failure(
    database_error: OrcapodDatabaseError | TypeIdentityError,
    during: str,
    failure: Exception,
) -> None:
    """Log a callback failure and record it on the propagating database error.

    Args:
        database_error: The database error (or identity error) on its way out of the run.
        during: What was running, for the log and the note, such as
            ``"Observer on_data_crash"``.
        failure: The exception the callback raised.
    """
    logger.warning(
        "%s raised while a database failure was propagating, which is raised "
        + "instead: %s",
        during,
        failure,
        exc_info=failure,
    )
    database_error.add_note(
        f"{during} also raised while this error propagated: "
        + f"{type(failure).__name__}: {failure}"
    )


def database_error_of(
    error: BaseException,
) -> OrcapodDatabaseError | TypeIdentityError | None:
    """Return the database error a failure carries: itself, or the first inside its group.

    Args:
        error: A run's failure, possibly an exception group from a task group.

    Returns:
        The ``OrcapodDatabaseError`` or ``TypeIdentityError`` that is ``error`` or is the first
        one found inside it, or ``None`` when it carries none.
    """
    if isinstance(error, (OrcapodDatabaseError, TypeIdentityError)):
        return error
    if isinstance(error, BaseExceptionGroup):
        for inner in error.exceptions:
            found = database_error_of(inner)
            if found is not None:
                return found
    return None


@contextmanager
def keep_database_error(
    database_error: OrcapodDatabaseError | TypeIdentityError | None, during: str
) -> Iterator[None]:
    """Run a callback so that it cannot replace a propagating database error.

    With ``database_error`` set, an ``Exception`` the block raises is logged and noted on
    ``database_error`` (see ``note_callback_failure``) and not re-raised, so the caller's
    ``raise`` propagates the database error. With ``database_error=None`` the block runs
    unchanged and its exceptions propagate.

    Args:
        database_error: The database error (or identity error) on its way out, or ``None``
            when none is.
        during: What the block runs, for the log and the note.

    Yields:
        Nothing; the block runs inside the context.
    """
    if database_error is None:
        yield
        return
    try:
        yield
    except Exception as failure:
        note_callback_failure(database_error, during, failure)


def commit_best_effort(run_error: BaseException, commit: Callable[[], None]) -> None:
    """Commit pending writes after a run failed, noting instead of raising a failure.

    The run's own error is the one to report, and a second failure while committing
    the writes that did succeed must not replace it. A failed commit is logged and
    added as a note to ``run_error``, so the caller learns that pending results may
    not have been stored.

    Args:
        run_error: The error the run failed with, which the caller re-raises.
        commit: Commits the pending writes, such as a database's ``flush``.
    """
    try:
        commit()
    except Exception as flush_error:
        logger.exception(
            "Could not flush pending writes after the run failed; "
            "the rows they hold were not stored"
        )
        run_error.add_note(
            "Flushing pending writes after this failure also failed "
            + f"({type(flush_error).__name__}: {flush_error}); results that were "
            + "still pending may not have been stored."
        )


def flush_best_effort(
    run_error: BaseException, *databases: ArrowDatabaseProtocol
) -> None:
    """Flush each database after a run failed, noting instead of raising a failure.

    See ``commit_best_effort``.

    Args:
        run_error: The error the run failed with, which the caller re-raises.
        *databases: The databases to flush.
    """
    for database in databases:
        commit_best_effort(run_error, database.flush)
