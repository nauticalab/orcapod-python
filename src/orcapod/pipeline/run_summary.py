"""What a job run did with each row: the summary ``PipelineJob.run`` returns.

With the default ``error_policy="continue"``, a row whose function raises is skipped and
the run goes on. ``RunSummary`` tells the user, per function node, how many rows were
computed, how many were read from the store, and how many failed with which first error.
"""

from __future__ import annotations

import html
import json
import threading
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from orcapod.core.result_cache import ResultCache
from orcapod.pipeline.observer import NoOpLogger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
    from orcapod.types import SchemaLike

_NOOP_LOGGER = NoOpLogger()


@dataclass(frozen=True)
class NodeRunSummary:
    """What one function node did with its rows in a run.

    Attributes:
        label: The node's label, its key in ``job.nodes``.
        computed: Rows whose function ran and returned.
        reused: Rows whose result was read from the store instead.
        failed: Rows whose function raised; the run skipped them.
        first_error: The exception of the first failed row, with its traceback, or
            ``None`` when no row failed.
    """

    label: str
    computed: int
    reused: int
    failed: int
    first_error: BaseException | None

    @property
    def rows(self) -> int:
        """All rows the node received: computed, reused and failed."""
        return self.computed + self.reused + self.failed


class RunSummary:
    """What a job run did, per function node; returned by ``PipelineJob.run``.

    Shown at the end of a notebook cell, it lists every function node with its counts and,
    for a node with failed rows, the first error and how to see its full traceback. The job
    also keeps it as ``job.last_run``.

    Attributes:
        job_name: The job's name.
        nodes: Per function node, in run order, keyed by label.
    """

    def __init__(self, job_name: str, nodes: Sequence[NodeRunSummary]) -> None:
        """Build the summary.

        Args:
            job_name: The job's name.
            nodes: One entry per function node of the run, in run order.
        """
        self.job_name: str = job_name
        self.nodes: Mapping[str, NodeRunSummary] = {node.label: node for node in nodes}

    @property
    def failed_rows(self) -> int:
        """The number of rows that failed, over every node."""
        return sum(node.failed for node in self.nodes.values())

    @property
    def ok(self) -> bool:
        """``True`` when no row failed."""
        return self.failed_rows == 0

    def failures_message(self) -> str:
        """One sentence per node with failed rows, for the run's warning.

        Returns:
            The sentences joined, or ``""`` when no row failed.
        """
        return " ".join(
            f"{node.failed} of {_rows(node.rows)} failed in node {node.label!r} "
            f"(first error: {_error_line(node.first_error)}). "
            f"Full traceback: {_traceback_call(node.label)}."
            for node in self.nodes.values()
            if node.failed
        )

    def print_traceback(self, label: str | None = None) -> None:
        """Print the full traceback of a node's first failed row to standard error.

        Args:
            label: The node; by default the first node with a failed row.

        Raises:
            KeyError: If no function node of the run has that label.
        """
        if label is not None and label not in self.nodes:
            raise KeyError(
                f"No function node {label!r} in this run; its function nodes are "
                f"{sorted(self.nodes)}."
            )
        candidates = [self.nodes[label]] if label is not None else self.nodes.values()
        for node in candidates:
            if node.first_error is not None:
                traceback.print_exception(node.first_error)
                return
        where = f" in node {label!r}" if label is not None else ""
        print(f"No rows failed{where} in this run.")

    def _headline(self) -> str:
        """The first line: the job and whether any row failed."""
        if self.failed_rows:
            outcome = f"{_rows(self.failed_rows)} failed."
        else:
            outcome = "all rows succeeded."
        return f"Run of job {self.job_name!r}: {outcome}"

    def __repr__(self) -> str:
        """The summary as text: one line per node, and the first error of a failing one."""
        lines = [self._headline()]
        for node in self.nodes.values():
            lines.append(
                f"  {node.label}: {_rows(node.rows)}: {node.computed} computed, "
                f"{node.reused} from store, {node.failed} failed"
            )
            if node.failed:
                lines.append(f"    first error: {_error_line(node.first_error)}")
                lines.append(f"    Full traceback: {_traceback_call(node.label)}")
        return "\n".join(lines)

    def _repr_html_(self) -> str:
        """The notebook display: the same content as ``repr``, as a table."""
        header = "".join(
            f"<th style='text-align:left'>{name}</th>"
            for name in (
                "node",
                "rows",
                "computed",
                "from store",
                "failed",
                "first error",
            )
        )
        body = []
        for node in self.nodes.values():
            failed_style = (
                " style='color:#c62828;font-weight:bold'" if node.failed else ""
            )
            error = ""
            if node.failed:
                error = (
                    f"{html.escape(_error_line(node.first_error))}<br>"
                    f"<code>{html.escape(_traceback_call(node.label))}</code>"
                )
            body.append(
                "<tr>"
                f"<td><code>{html.escape(node.label)}</code></td>"
                f"<td>{node.rows}</td><td>{node.computed}</td><td>{node.reused}</td>"
                f"<td{failed_style}>{node.failed}</td><td>{error}</td>"
                "</tr>"
            )
        return (
            f"<p>{html.escape(self._headline())}</p>"
            f"<table><thead><tr>{header}</tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table>"
        )


def _rows(n: int) -> str:
    """``1 row`` or ``n rows``."""
    return f"{n} row" if n == 1 else f"{n} rows"


def _error_line(error: BaseException | None) -> str:
    """``Type: first line of the message``."""
    if error is None:
        return ""
    message = str(error)
    lines = message.splitlines()
    first = lines[0] if lines else ""
    if len(lines) > 1:
        first += " ..."
    return f"{type(error).__name__}: {first}" if first else type(error).__name__


def _traceback_call(label: str) -> str:
    """The call that prints a node's traceback, with the label as a valid string literal.

    A JSON string is a valid Python string literal, and keeps double quotes.
    """
    return f"job.last_run.print_traceback({json.dumps(label)})"


class RunCountingObserver:
    """Counts, per node label, the rows computed, reused and failed in one run.

    ``PipelineJob.run`` runs it beside the user's observer. Every contextualized copy is
    this observer itself, so all nodes count into one place. Hooks may be called from
    several threads, so the counts are updated under a lock.
    """

    def __init__(self) -> None:
        """Start with no counts."""
        self._lock: threading.Lock = threading.Lock()
        self._computed: dict[str, int] = {}
        self._reused: dict[str, int] = {}
        self._failed: dict[str, int] = {}
        self._first_error: dict[str, BaseException] = {}

    def node_summary(self, label: str) -> NodeRunSummary:
        """The counts of node ``label``; zeros when it saw no rows.

        Args:
            label: The node's label.

        Returns:
            The node's summary.
        """
        with self._lock:
            return NodeRunSummary(
                label=label,
                computed=self._computed.get(label, 0),
                reused=self._reused.get(label, 0),
                failed=self._failed.get(label, 0),
                first_error=self._first_error.get(label),
            )

    def contextualize(self, *identity_path: str) -> RunCountingObserver:
        """Return this observer: counts are keyed by node label, not identity path."""
        return self

    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        """Nothing to count."""

    def on_run_end(self, run_id: str) -> None:
        """Nothing to count."""

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: SchemaLike | None = None
    ) -> None:
        """Nothing to count."""

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Nothing to count."""

    def on_data_start(
        self, node_label: str, tag: TagProtocol, data: DataProtocol
    ) -> None:
        """Nothing to count until the row ends."""

    def on_data_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_data: DataProtocol,
        output_data: DataProtocol | None,
        cached: bool,
    ) -> None:
        """Count the row as reused (from the pipeline or result store) or computed."""
        from_store = cached or (
            output_data is not None
            and output_data.get_meta_value(ResultCache.RESULT_COMPUTED_FLAG, True)
            is False
        )
        counts = self._reused if from_store else self._computed
        with self._lock:
            counts[node_label] = counts.get(node_label, 0) + 1

    def on_data_crash(
        self, node_label: str, tag: TagProtocol, data: DataProtocol, error: Exception
    ) -> None:
        """Count the row as failed; keep the node's first error."""
        with self._lock:
            self._failed[node_label] = self._failed.get(node_label, 0) + 1
            _ = self._first_error.setdefault(node_label, error)

    def create_data_logger(self, tag: TagProtocol, data: DataProtocol) -> Any:
        """No logging of its own."""
        return _NOOP_LOGGER
