"""Attribute an Orcapod warning to the user's code."""

from __future__ import annotations

import sys
from types import FrameType


def outside_orcapod_stacklevel() -> int:
    """Return the stacklevel of the first frame outside Orcapod, as seen from the caller.

    Level 1 is the caller of this function, the function that calls
    ``warnings.warn``. When every frame is Orcapod's (a worker thread), the caller
    of that function is used.

    Returns:
        The ``stacklevel`` to pass to ``warnings.warn``.
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
