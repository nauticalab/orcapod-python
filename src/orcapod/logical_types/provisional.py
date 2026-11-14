"""The warning that a value type's hash is provisional (ITL-1068).

The library value types are declared provisional in the ``provisional`` list of
``orcapod/contexts/logical_types.json``; the logical type registry and the value
handler registry of a data context hold that list and call ``warn_provisional``
when they resolve a declared type. This module only keeps the once-per-type
bookkeeping, which is process-wide.
"""

from __future__ import annotations

import threading
import warnings

from orcapod.errors import ProvisionalHashWarning
from orcapod.utils.warning_utils import outside_orcapod_stacklevel

_lock = threading.RLock()
_issued: set[str] = set()


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
            stacklevel=outside_orcapod_stacklevel(),
        )
        _issued.add(extension_name)


def reset_provisional_warnings() -> None:
    """Forget which types have warned, so each warns again on its next use.

    A test hook: a test that checks the warning starts as a new process would.
    """
    with _lock:
        _issued.clear()
