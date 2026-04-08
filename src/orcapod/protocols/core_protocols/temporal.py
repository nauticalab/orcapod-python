from datetime import datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class TemporalProtocol(Protocol):
    """
    Protocol for objects that track temporal state.

    Objects implementing TemporalProtocol carries a computed property to
    report when their content was last modified, enabling time-sensitive
    actions such as cache invalidation, incremental processing, and
    dependency staleness tracking.
    """

    @property
    def last_modified(self) -> datetime | None:
        """
        When this object's content was last modified.

        Returns:
            datetime: Content last modified timestamp (timezone-aware)
            None: Modification time unknown (assume always changed)
        """
        ...
