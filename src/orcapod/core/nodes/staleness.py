"""Staleness of job nodes across the job graph (ITL-1095).

A stream is stale when an upstream or its producer was modified after it
(``StreamBase.is_stale``), which looks one level up only. A job node holds the rows of
its current run, so it is stale also when any job node it depends on is stale: a source
rebound two nodes up makes every node below it stale, not only its direct consumer.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


class RunHoldingNode:
    """Mixin of the job nodes that hold the rows of their current run.

    ``FunctionJobNode``, ``OperatorJobNode`` and ``SideEffectJobNode`` mix it in ahead of
    their stream base, so their ``is_stale`` covers their whole ancestry.
    """

    # The summary of the node's last ``run()`` (with the nodes it ran first); a class
    # default so nodes rebuilt from a saved descriptor start without one.
    _last_run: Any = None

    @property
    def last_run(self) -> Any:
        """The ``RunSummary`` of this node's last ``run()``, or ``None``.

        ``node.run()`` runs the node and the job nodes it depends on (ITL-1095); this is
        what that run did, per function node, as ``job.last_run`` is for ``job.run()``.
        A ``run()`` that found the node current ran nothing and leaves it unchanged.
        """
        return self._last_run

    @property
    def is_stale(self) -> bool:
        """Whether the rows this node holds are out of date, or it holds none.

        True before the node's first run in this process and after ``clear_cache()``
        (no modified time), when an input or its producer was modified after the
        node's run (``StreamBase.is_stale``), or when any job node it depends on is
        stale (ITL-1095). A stale node reads empty until it runs again. Each node of
        the ancestry is checked once, however many paths lead to it.
        """
        return _stale(self, {})

    def _own_staleness(self) -> bool:
        """``StreamBase.is_stale`` of this node alone: its inputs and producer."""
        own: bool = super().is_stale  # pyright: ignore[reportAttributeAccessIssue]  # the stream base after this mixin in the MRO defines it
        return own

    def holds_current_run(self, *, load_only: bool = False) -> bool:
        """Whether the node holds the rows of a run that ``run()`` would not redo.

        A node that is not stale and holds the rows of a completed run in this process
        is current: ``run()`` returns them without running it (developer, November 13,
        2026, ITL-1095). A node filled by a load-only run is current only for another
        load-only run: a full run computes the inputs it skipped. So is every node
        whose rows came from such a node: a node is current only if every job node
        above it is current for the same kind of run. Each node of the ancestry is
        checked once.

        Args:
            load_only: Whether the question is for a load-only run.

        Returns:
            ``True`` if the node holds a current run for such a run.
        """
        return _current(self, load_only, {}, {})

    def _holds_run(self, *, load_only: bool) -> bool:
        """Whether the node holds the rows of a completed run good for such a run."""
        raise NotImplementedError


def _job_upstreams(node: Any) -> list[RunHoldingNode]:
    """The direct upstreams of ``node`` that are job nodes holding run rows."""
    upstreams: tuple[Any, ...] = getattr(node, "upstreams", ())
    return [upstream for upstream in upstreams if isinstance(upstream, RunHoldingNode)]


def _stale(node: RunHoldingNode, memo: dict[int, bool]) -> bool:
    """Whether ``node`` is stale; ``memo`` keeps each node's answer for one traversal."""
    key = id(node)
    if key not in memo:
        memo[key] = node._own_staleness() or any(  # pyright: ignore[reportPrivateUsage]  # the mixin's own helper
            _stale(upstream, memo) for upstream in _job_upstreams(node)
        )
    return memo[key]


def _current(
    node: RunHoldingNode,
    load_only: bool,
    memo: dict[int, bool],
    stale_memo: dict[int, bool],
) -> bool:
    """Whether ``node`` holds a current run; memoized for one traversal."""
    key = id(node)
    if key not in memo:
        memo[key] = (
            node._holds_run(load_only=load_only)  # pyright: ignore[reportPrivateUsage]  # the mixin's own hook
            and not _stale(node, stale_memo)
            and all(
                _current(upstream, load_only, memo, stale_memo)
                for upstream in _job_upstreams(node)
            )
        )
    return memo[key]


def upstream_job_node_stale(upstreams: Iterable[Any]) -> bool:
    """Return whether any job node in ``upstreams`` that holds run rows is stale.

    Each such node answers with its own ``is_stale``, which applies this rule to its
    own upstreams in turn, so the whole ancestry is covered.

    Args:
        upstreams: The direct upstream streams of a job node.

    Returns:
        ``True`` if any upstream ``RunHoldingNode`` is stale.
    """
    memo: dict[int, bool] = {}
    return any(
        isinstance(upstream, RunHoldingNode) and _stale(upstream, memo)
        for upstream in upstreams
    )
