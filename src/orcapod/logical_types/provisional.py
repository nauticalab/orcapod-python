"""The warning that a value type's hash is provisional (ITL-1068).

The library value types are declared provisional in the ``provisional`` list of
``orcapod/contexts/logical_types.json``; the logical type registry and the value
handler registry of a data context hold that list and call ``warn_provisional``
when they resolve a declared type. This module only keeps the once-per-type
bookkeeping, which is process-wide.
"""

from __future__ import annotations

import sys
import threading
import warnings
from types import FrameType

from orcapod.errors import ProvisionalHashWarning

_lock = threading.RLock()
_issued: set[str] = set()


def _outside_orcapod_stacklevel() -> int:
    """Return the stacklevel of the first frame outside Orcapod, as seen from the caller.

    Level 1 is the caller of this function (``warn_provisional``). When every frame
    is Orcapod's (a worker thread), the caller of ``warn_provisional`` is used.
    """
    level = 1
    frame: FrameType | None = sys._getframe(1)  # pyright: ignore[reportPrivateUsage]  # as the warnings module does
    while frame is not None:
        module = str(frame.f_globals.get("__name__", ""))
        if module != "orcapod" and not module.startswith("orcapod."):
            return level
        frame = frame.f_back
        level += 1
    return 2


def warn_provisional(extension_name: str) -> None:
    """Issue ``ProvisionalHashWarning`` for a type, once per process.

    The type is recorded only after ``warnings.warn`` returns, so under an error
    filter every use raises. The warning is attributed to the first frame outside
    Orcapod, the code that used the type.

    Args:
        extension_name: The Arrow extension name of a type declared provisional.
    """
    if extension_name in _issued:
        return
    with _lock:
        if extension_name in _issued:
            return
        warnings.warn(
            f"{extension_name} values are provisional: their value hash and stored "
            "identity may change in Orcapod v0.4, and results computed from them "
            "before then may be recomputed after the upgrade (nothing stored is lost "
            "or wrong). See 'Provisional value hashes' in docs/concepts/arrays.md. To "
            "silence: warnings.filterwarnings('ignore', "
            "category=orcapod.errors.ProvisionalHashWarning).",
            ProvisionalHashWarning,
            stacklevel=_outside_orcapod_stacklevel(),
        )
        _issued.add(extension_name)


def reset_provisional_warnings() -> None:
    """Forget which types have warned, so each warns again on its next use.

    A test hook: a test that checks the warning starts as a new process would.
    """
    with _lock:
        _issued.clear()
