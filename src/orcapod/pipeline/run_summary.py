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
    from types import FrameType

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
        not_stored: Inputs a load-only run skipped because no result is stored for
            them (``job.run(load_only=True)``; always 0 in a full run).
        inputs_incomplete: The labels of the function nodes upstream of this one that
            skipped inputs in a load-only run, in run order: this node's inputs lack
            the rows those results would have given (a group may be partial).
        held: The rows of a node the run did not run because it held a current run
            from its previous run (ITL-1095): it computed, read and wrote nothing.
    """

    label: str
    computed: int
    reused: int
    failed: int
    first_error: BaseException | None
    not_stored: int = 0
    inputs_incomplete: tuple[str, ...] = ()
    held: int = 0

    @property
    def rows(self) -> int:
        """All rows of the node: computed, reused, failed, not stored and held."""
        return self.computed + self.reused + self.failed + self.not_stored + self.held


class RunSummary:
    """What a job run did, per function node; returned by ``PipelineJob.run``.

    Shown at the end of a notebook cell, it lists every function node with its counts and,
    for a node with failed rows, the first error and how to see its full traceback. The job
    also keeps it as ``job.last_run``.

    Attributes:
        job_name: The job's name.
        nodes: Per function node, in run order, keyed by label.
        load_only: Whether the run was a load-only run (``job.run(load_only=True)``):
            it computed nothing; ``reused`` counts the rows it loaded.
        target: For the run of one node and the nodes it depends on
            (``node.run()``), that node's label; ``None`` for a job run.
    """

    def __init__(
        self,
        job_name: str,
        nodes: Sequence[NodeRunSummary],
        *,
        load_only: bool = False,
        target: str | None = None,
    ) -> None:
        """Build the summary.

        Args:
            job_name: The job's name.
            nodes: One entry per function node of the run, in run order.
            load_only: Whether the run loaded stored results only.
            target: The label of the node whose ``run()`` this was, if any.
        """
        self.job_name: str = job_name
        self.nodes: Mapping[str, NodeRunSummary] = {node.label: node for node in nodes}
        self.load_only: bool = load_only
        self.target: str | None = target

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
            f"Full traceback: {_traceback_call(node.label, self.target)}."
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
        """The first line: the job (or node) and whether any row failed."""
        subject = (
            f"job {self.job_name!r}"
            if self.target is None
            else f"node {self.target!r} and the nodes it depends on"
        )
        if self.nodes and all(
            node.held and not (node.computed or node.reused or node.failed)
            for node in self.nodes.values()
        ):
            run = "Load-only run" if self.load_only else "Run"
            return f"{run} of {subject}: nothing to do; every node holds a current run."
        if self.load_only:
            skipped = sum(node.not_stored for node in self.nodes.values())
            outcome = (
                f"{_rows(skipped)} without a stored result skipped."
                if skipped
                else "every input had a stored result."
            )
            return f"Load-only run of {subject}: {outcome}"
        if self.failed_rows:
            outcome = f"{_rows(self.failed_rows)} failed."
        else:
            outcome = "all rows succeeded."
        return f"Run of {subject}: {outcome}"

    def __repr__(self) -> str:
        """The summary as text: one line per node, and the first error of a failing one."""
        lines = [self._headline()]
        for node in self.nodes.values():
            lines.append(
                f"  {node.label}: {_rows(node.rows)}: {_counts(node, self.load_only)}"
            )
            if node.failed:
                lines.append(f"    first error: {_error_line(node.first_error)}")
                lines.append(
                    f"    Full traceback: {_traceback_call(node.label, self.target)}"
                )
        return "\n".join(lines)

    def _repr_html_(self) -> str:
        """The notebook display: the same content as ``repr``, as a table."""
        if self.load_only:
            return self._load_only_html()
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
                    f"<code>{html.escape(_traceback_call(node.label, self.target))}</code>"
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

    def _load_only_html(self) -> str:
        """The notebook display of a load-only run: rows loaded and skipped per node."""
        header = "".join(
            f"<th style='text-align:left'>{name}</th>"
            for name in ("node", "rows", "loaded", "not stored", "inputs incomplete")
        )
        body = [
            "<tr>"
            f"<td><code>{html.escape(node.label)}</code></td>"
            f"<td>{node.rows}</td><td>{node.reused}</td><td>{node.not_stored}</td>"
            f"<td>{html.escape(', '.join(node.inputs_incomplete))}</td>"
            "</tr>"
            for node in self.nodes.values()
        ]
        return (
            f"<p>{html.escape(self._headline())}</p>"
            f"<table><thead><tr>{header}</tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table>"
        )


def _counts(node: NodeRunSummary, load_only: bool) -> str:
    """A node's counts as text: what a full run did, or what a load-only run loaded."""
    if node.held and not (node.computed or node.reused or node.failed):
        text = "held from the previous run"
        if node.not_stored:
            text += f" ({node.not_stored} inputs without a stored result)"
        return text
    if not load_only:
        return (
            f"{node.computed} computed, {node.reused} from store, {node.failed} failed"
        )
    text = f"{node.reused} loaded"
    if node.not_stored:
        text += f", {node.not_stored} not stored (skipped)"
    if node.inputs_incomplete:
        text += f" (inputs incomplete: {', '.join(node.inputs_incomplete)})"
    return text


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


def _traceback_call(label: str, target: str | None = None) -> str:
    """The call that prints a node's traceback, with the label as a valid string literal.

    A JSON string is a valid Python string literal, and keeps double quotes. A node
    run's summary is kept on its node (``target``), a job run's on the job.
    """
    owner = "job" if target is None else f"job.nodes[{json.dumps(target)}]"
    return f"{owner}.last_run.print_traceback({json.dumps(label)})"


def warn_row_failures(summary: RunSummary, caller: FrameType) -> None:
    """Issue the run's ``RowFailuresWarning``, shown after every run with failed rows.

    Python's default filter shows a warning once per text and line, so a second identical
    failing run from the same line (re-running a notebook cell) would show nothing. A
    fresh registry for each run keeps every run's warning, while the user's filters
    (``ignore``, ``error``) still apply.

    Args:
        summary: The run's summary, with failed rows.
        caller: The frame that called ``run``; the warning is shown at its line.
    """
    import warnings

    from orcapod.errors import RowFailuresWarning

    warnings.warn_explicit(
        summary.failures_message(),
        RowFailuresWarning,
        caller.f_code.co_filename,
        caller.f_lineno,
        module=caller.f_globals.get("__name__"),
        registry=None,
        module_globals=caller.f_globals,
    )


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


def run_node_summaries(
    dag: Any, counter: RunCountingObserver, held: Mapping[Any, int] | None = None
) -> list[NodeRunSummary]:
    """One summary per function node of a finished run, in run order.

    The counts come from ``counter``; a load-only run's skipped inputs come from each
    node (``last_run_not_stored``), and every node below a function node that skipped
    inputs names it in ``inputs_incomplete`` (ITL-1095).

    Args:
        dag: The run's node graph (``topological_sort``, ``predecessors``).
        counter: The observer that counted the run's rows.
        held: Nodes of ``dag`` the run did not run because they hold a current run,
            with their row counts: reported as held (ITL-1095). Inputs they skipped in
            an earlier load-only run still make the nodes below them incomplete.

    Returns:
        The summaries of the run's function nodes, in run order.
    """
    from dataclasses import replace

    from orcapod.protocols.node_protocols import (
        is_function_node,
        is_side_effect_function_node,
    )

    order = list(dag.topological_sort())
    position = {id(node): index for index, node in enumerate(order)}
    skipping_above: dict[int, set[int]] = {}
    summaries: list[NodeRunSummary] = []
    for node in order:
        above: set[int] = set()
        for upstream in dag.predecessors(node):
            above |= skipping_above.get(id(upstream), set())
            if getattr(upstream, "last_run_not_stored", 0):
                above.add(id(upstream))
        skipping_above[id(node)] = above
        # A stream outside the job (in a node.run() graph) is no node of the run.
        if getattr(node, "node_type", None) is None:
            continue
        if not (is_function_node(node) or is_side_effect_function_node(node)):
            continue
        held_rows = next(
            (count for other, count in (held or {}).items() if other is node), None
        )
        labels = tuple(
            order[index].label for index in sorted(position[key] for key in above)
        )
        label: str = getattr(node, "label")  # every graph node is labelled
        if held_rows is not None:
            # A held node keeps what its run skipped: a load-only run's missing
            # inputs are still missing.
            summaries.append(
                NodeRunSummary(
                    label,
                    0,
                    0,
                    0,
                    None,
                    not_stored=getattr(node, "last_run_not_stored", 0),
                    inputs_incomplete=labels,
                    held=held_rows,
                )
            )
            continue
        summaries.append(
            replace(
                counter.node_summary(label),
                not_stored=getattr(node, "last_run_not_stored", 0),
                inputs_incomplete=labels,
            )
        )
    return summaries
