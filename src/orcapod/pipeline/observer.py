"""No-op implementations of the observability protocols.

Provides ``NoOpLogger`` and ``NoOpObserver`` — the defaults used
when no observability is configured.  Every method is a zero-cost no-op.
"""

from __future__ import annotations

from typing import Any

from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.types import SchemaLike
from orcapod.protocols.observability_protocols import (  # noqa: F401  (re-exported for convenience)
    ExecutionObserverProtocol,
    DataExecutionLoggerProtocol,
)


# ---------------------------------------------------------------------------
# NoOpLogger
# ---------------------------------------------------------------------------


class NoOpLogger:
    """Logger that discards all captured output.

    Returned by ``NoOpObserver`` when no logging sink is configured.
    """

    def record(self, **kwargs: Any) -> None:
        pass


# Singleton — NoOpLogger carries no state so one instance is enough.
_NOOP_LOGGER = NoOpLogger()


# ---------------------------------------------------------------------------
# NoOpObserver
# ---------------------------------------------------------------------------


class NoOpObserver:
    """Observer that does nothing.

    Satisfies ``ExecutionObserverProtocol`` and is the default when no
    observability is configured.  ``create_data_logger`` returns the
    shared ``_NOOP_LOGGER`` singleton.
    """

    def contextualize(
        self, *identity_path: str
    ) -> NoOpObserver:
        return self

    def on_run_start(
        self,
        run_id: str,
        pipeline_uri: str = "",
    ) -> None:
        pass

    def on_run_end(self, run_id: str) -> None:
        pass

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: SchemaLike | None = None
    ) -> None:
        pass

    def on_node_end(
        self, node_label: str, node_hash: str
    ) -> None:
        pass

    def on_data_start(
        self,
        node_label: str,
        tag: TagProtocol,
        data: DataProtocol,
    ) -> None:
        pass

    def on_data_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_data: DataProtocol,
        output_data: DataProtocol | None,
        cached: bool,
    ) -> None:
        pass

    def on_data_crash(
        self,
        node_label: str,
        tag: TagProtocol,
        data: DataProtocol,
        error: Exception,
    ) -> None:
        pass

    def create_data_logger(
        self,
        tag: TagProtocol,
        data: DataProtocol,
    ) -> NoOpLogger:
        return _NOOP_LOGGER
