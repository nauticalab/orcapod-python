"""Async pipeline orchestrator for push-based channel execution.

Walks a compiled pipeline's node graph and launches all nodes concurrently
via ``asyncio.TaskGroup``, wiring them together with bounded channels.
Uses TypeGuard dispatch with tightened per-type async_execute signatures.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import threading
import uuid
import warnings
from collections import defaultdict
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from orcapod.channels import BroadcastChannel, Channel
from orcapod.errors import StoreError, StoreWriteError, TypeIdentityError
from orcapod.pipeline.result import OrchestratorResult
from orcapod.utils.store_errors import keep_store_error
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_side_effect_function_node,
    is_side_effect_node,
    is_source_node,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from orcapod.channels import WritableChannel
    from orcapod.pipeline.dag import GraphProtocol
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# How often a caller waiting for a run on its own thread checks for an interrupt.
_INTERRUPT_CHECK_SECONDS = 0.1


class AsyncPipelineOrchestrator:
    """Execute a compiled pipeline asynchronously using channels.

    After compilation, the orchestrator:

    1. Walks the node graph in topological order.
    2. Creates bounded channels (or broadcast channels for fan-out)
       between connected nodes.
    3. Launches every node's ``async_execute`` concurrently via
       ``asyncio.TaskGroup``, using TypeGuard dispatch for per-type
       signatures.

    Args:
        buffer_size: Channel buffer size. Defaults to 64.
    """

    def __init__(
        self,
        buffer_size: int = 64,
        error_policy: Literal["continue", "fail_fast"] = "continue",
    ) -> None:
        self._buffer_size = buffer_size
        self._error_policy: Literal["continue", "fail_fast"] = error_policy

    def run(
        self,
        graph: "GraphProtocol[Any]",
        *,
        observer: ExecutionObserverProtocol | None = None,
        materialize_results: bool = True,
        run_id: str | None = None,
        pipeline_uri: str = "",
        load_only: bool = False,
        held_rows: Mapping[Any, list[tuple[TagProtocol, DataProtocol]]] | None = None,
    ) -> OrchestratorResult:
        """Synchronous entry point — runs the async pipeline to completion.

        Without a running event loop on the calling thread (a script), the run uses
        ``asyncio.run``. With one (a Jupyter kernel, or a call from async code), the
        same coroutine runs to completion on a separate thread with its own event loop
        while the caller waits, so the call works the same in both places. The caller's
        loop is blocked until the run ends; async code should await ``run_async``
        instead. A ``KeyboardInterrupt`` while waiting (the notebook's interrupt button)
        cancels the run, waits for the functions still running to return, and is
        re-raised. A second interrupt during that wait returns at once, with a
        ``RuntimeWarning`` naming the nodes whose functions have not returned; when they
        do, nothing more is written to the store.

        Args:
            graph: A ``GraphProtocol`` DAG with GraphNode objects as vertices.
            observer: Optional execution observer forwarded to nodes.
            materialize_results: If True, collect all node outputs into
                the result. If False, return empty node_outputs.
            run_id: Optional run identifier.  If not provided, a UUID is
                generated automatically.
            pipeline_uri: Opaque URI string identifying this pipeline run
                (e.g. ``"my_pipeline@a1b2c3d4e5f6a1b2"``).  Forwarded
                verbatim to ``observer.on_run_start``.

        Returns:
            OrchestratorResult with node outputs.
        """
        try:
            _ = asyncio.get_running_loop()
        except RuntimeError:
            loop_running = False
        else:
            loop_running = True
        # Run outside the handler, so the run's own errors carry no context from it.
        if not loop_running:
            return asyncio.run(
                self._run_async(
                    graph,
                    materialize_results,
                    observer=observer,
                    run_id=run_id,
                    pipeline_uri=pipeline_uri,
                    load_only=load_only,
                    held_rows=held_rows,
                )
            )

        from orcapod.pipeline.composite_observer import CompositeObserver

        rows_in_flight = _RowsInFlight(_async_function_labels(graph))
        watched: Any = (
            rows_in_flight
            if observer is None
            else CompositeObserver(observer, rows_in_flight)
        )

        def make_run() -> Coroutine[Any, Any, OrchestratorResult]:
            """Build the run's coroutine, observed for rows still in flight."""
            return self._run_async(
                graph,
                materialize_results,
                observer=watched,
                run_id=run_id,
                pipeline_uri=pipeline_uri,
                load_only=load_only,
                held_rows=held_rows,
            )

        return _run_on_own_thread(make_run, rows_in_flight)

    async def run_async(
        self,
        graph: "GraphProtocol[Any]",
        *,
        observer: ExecutionObserverProtocol | None = None,
        materialize_results: bool = True,
        run_id: str | None = None,
        pipeline_uri: str = "",
        load_only: bool = False,
        held_rows: Mapping[Any, list[tuple[TagProtocol, DataProtocol]]] | None = None,
    ) -> OrchestratorResult:
        """Async entry point for callers already inside an event loop.

        Args:
            graph: A ``GraphProtocol`` DAG with GraphNode objects as vertices.
            observer: Optional execution observer forwarded to nodes.
            materialize_results: If True, collect all node outputs.
            run_id: Optional run identifier.  If not provided, a UUID is
                generated automatically.
            pipeline_uri: Opaque URI string identifying this pipeline run.
                Forwarded verbatim to ``observer.on_run_start``.
            load_only: As for ``SyncPipelineOrchestrator.run``.
            held_rows: As for ``SyncPipelineOrchestrator.run``: nodes not run, whose
                rows feed their successors.

        Returns:
            OrchestratorResult with node outputs.
        """
        return await self._run_async(
            graph,
            materialize_results,
            observer=observer,
            run_id=run_id,
            pipeline_uri=pipeline_uri,
            load_only=load_only,
            held_rows=held_rows,
        )

    async def _run_async(
        self,
        graph: "GraphProtocol[Any]",
        materialize_results: bool,
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
        pipeline_uri: str = "",
        load_only: bool = False,
        held_rows: Mapping[Any, list[tuple[TagProtocol, DataProtocol]]] | None = None,
    ) -> OrchestratorResult:
        """Core async logic: wire channels, launch tasks, collect results."""
        from orcapod.pipeline.observer import NoOpObserver

        run_id = run_id or str(uuid.uuid4())
        effective_observer = observer if observer is not None else NoOpObserver()
        effective_observer.on_run_start(run_id, pipeline_uri=pipeline_uri)

        topo_order: list[Any] = []
        store_error: StoreError | TypeIdentityError | None = None
        # id of each exception a node raised -> that node's label.
        failed_nodes: dict[int, str] = {}
        try:
            topo_order = list(graph.topological_sort())
            buf = self._buffer_size

            # Build edge maps
            out_edges: dict[Any, list[Any]] = defaultdict(list)
            in_edges: dict[Any, list[Any]] = defaultdict(list)
            for upstream_node, downstream_node in graph.edges():
                out_edges[upstream_node].append(downstream_node)
                in_edges[downstream_node].append(upstream_node)

            # Create channels for each edge
            node_output_channels: dict[Any, Channel | BroadcastChannel] = {}
            edge_readers: dict[tuple[Any, Any], Any] = {}

            for node, downstreams in out_edges.items():
                if len(downstreams) == 1:
                    ch = Channel(buffer_size=buf)
                    node_output_channels[node] = ch
                    edge_readers[(node, downstreams[0])] = ch.reader
                else:
                    bch = BroadcastChannel(buffer_size=buf)
                    node_output_channels[node] = bch
                    for ds in downstreams:
                        edge_readers[(node, ds)] = bch.add_reader()

            # Terminal nodes need sink channels
            terminal_channels: list[Channel] = []
            for node in topo_order:
                if node not in node_output_channels:
                    ch = Channel(buffer_size=buf)
                    node_output_channels[node] = ch
                    terminal_channels.append(ch)

            # Result collection
            collectors: dict[Any, list[tuple[TagProtocol, DataProtocol]]] = {}
            if materialize_results:
                for node in topo_order:
                    collectors[node] = []

            # Launch all nodes concurrently
            async with asyncio.TaskGroup() as tg:
                for node in topo_order:
                    writer = node_output_channels[node].writer

                    if materialize_results:
                        collector = collectors[node]
                        writer = _CollectingWriter(writer, collector)

                    if held_rows is not None and node in held_rows:
                        # Not run: the rows of its current run feed its successors. Its
                        # own inputs are read and discarded, so no producer blocks on
                        # a channel nobody reads.
                        for upstream in in_edges.get(node, []):
                            tg.create_task(_drain(edge_readers[(upstream, node)]))
                        _start_node(
                            tg, failed_nodes, node, _send_held(held_rows[node], writer)
                        )
                    elif is_source_node(node):
                        _start_node(
                            tg,
                            failed_nodes,
                            node,
                            node.async_execute(writer, observer=effective_observer),
                        )
                    elif is_function_node(node) or is_side_effect_function_node(node):
                        predecessors = in_edges.get(node, [])
                        if len(predecessors) != 1:
                            raise ValueError(
                                f"FunctionNode expects exactly 1 upstream, got {len(predecessors)}"
                            )
                        input_reader = edge_readers[(predecessors[0], node)]
                        _start_node(
                            tg,
                            failed_nodes,
                            node,
                            node.async_execute(
                                input_reader,
                                writer,
                                observer=effective_observer,
                                error_policy=self._error_policy,
                                run_id=run_id,
                                load_only=load_only,
                            ),
                        )
                    elif is_operator_node(node):
                        predecessors = in_edges.get(node, [])
                        # Sort by node.upstreams order for non-commutative operators
                        upstream_order = {
                            id(s): i for i, s in enumerate(node.upstreams)
                        }
                        sorted_preds = sorted(
                            predecessors,
                            key=lambda p: upstream_order.get(id(p), 0),
                        )
                        input_readers = [
                            edge_readers[(upstream, node)] for upstream in sorted_preds
                        ]
                        _start_node(
                            tg,
                            failed_nodes,
                            node,
                            node.async_execute(
                                input_readers,
                                writer,
                                observer=effective_observer,
                                load_only=load_only,
                            ),
                        )
                    elif is_side_effect_node(node):
                        predecessors = in_edges.get(node, [])
                        if not predecessors:
                            raise ValueError(
                                "SideEffectNode expects exactly 1 upstream, got 0"
                            )
                        input_reader = edge_readers[(predecessors[0], node)]
                        # In a load-only run the node passes its rows on and delivers
                        # nothing.
                        _start_node(
                            tg,
                            failed_nodes,
                            node,
                            node.async_execute(
                                [input_reader],
                                writer,
                                observer=effective_observer,
                                run_id=run_id,
                                load_only=load_only,
                            ),
                        )
                    else:
                        raise TypeError(
                            f"Unknown node type: {getattr(node, 'node_type', None)!r}"
                        )

                # Drain terminal channels concurrently
                for ch in terminal_channels:
                    tg.create_task(ch.reader.collect())

            return OrchestratorResult(
                node_outputs=collectors if materialize_results else {}
            )
        except ExceptionGroup as group:
            # A failed store read or write stops the run. Raise it on its own, as the
            # sync orchestrator does, rather than inside the TaskGroup's exception
            # group.
            store_error = _single_store_error(group, topo_order)
            if store_error is not None:
                raise store_error
            # A stored row that cannot be decoded as its class stops the run too,
            # raised on its own as the sync orchestrator raises it (ITL-881).
            identity_errors = [
                exc for exc in _leaves(group) if isinstance(exc, TypeIdentityError)
            ]
            if identity_errors:
                store_error = identity_errors[0]
                raise store_error
            # Any other failure is raised on its own too, as the sync orchestrator
            # raises it: a row's error under fail_fast, a side effect's delivery error.
            raise _single_failure(group, topo_order, failed_nodes)
        finally:
            # A failing on_run_end must not replace a failed store read or write.
            with keep_store_error(store_error, "Observer on_run_end"):
                effective_observer.on_run_end(run_id)


def _run_on_own_thread(
    make_run: Callable[[], Coroutine[Any, Any, _T]], rows_in_flight: _RowsInFlight
) -> _T:
    """Run a coroutine to completion on a new thread with its own event loop.

    For a caller whose thread already runs an event loop, where ``asyncio.run`` is
    refused. The caller blocks until the run ends and gets its result, or its exception
    unchanged. The thread runs in a copy of the caller's context variables.

    A first ``KeyboardInterrupt`` in the caller (the notebook's interrupt button)
    cancels the run and waits for it to stop: a function already running on the loop's
    thread pool cannot be interrupted, so the wait lasts until it returns. A second
    interrupt during that wait returns control at once: it warns that a function has
    not returned yet, naming the nodes with rows in flight, and is raised. The cancelled
    run never resumes, so when that function returns nothing more is written.

    Args:
        make_run: Builds the coroutine; called on the new thread, and not at all when
            the caller is interrupted before the thread starts it.
        rows_in_flight: Observes the run, to name in the warning the nodes whose
            functions have not returned.

    Returns:
        What the coroutine returned.
    """
    loop = asyncio.new_event_loop()
    # Guards the hand-over between caller and thread: once the caller has cancelled,
    # the thread does not start the run; once the thread has started it, the caller
    # cancels the task.
    state_lock = threading.Lock()
    cancelled = threading.Event()
    tasks: list[asyncio.Task[_T]] = []
    finished = threading.Event()
    results: list[_T] = []
    errors: list[BaseException] = []

    def run_loop() -> None:
        """Run the coroutine on this thread's loop, unless the caller cancelled first."""
        asyncio.set_event_loop(loop)
        try:
            with state_lock:
                if cancelled.is_set():
                    return
                task = loop.create_task(make_run())
                tasks.append(task)
            results.append(loop.run_until_complete(task))
        except BaseException as exc:  # noqa: BLE001  # handed to the waiting caller
            errors.append(exc)
        finally:
            try:
                _cancel_leftover_tasks(loop)
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
            finally:
                asyncio.set_event_loop(None)
                loop.close()
                finished.set()

    context = contextvars.copy_context()
    thread = threading.Thread(
        target=context.run, args=(run_loop,), name="orcapod-async-run", daemon=True
    )
    try:
        thread.start()
        _wait_until(finished)
    except KeyboardInterrupt:
        with state_lock:
            cancelled.set()
            started = tasks[0] if tasks else None
        if started is not None:
            # The loop may have closed after the run ended on its own.
            with contextlib.suppress(RuntimeError):
                _ = loop.call_soon_threadsafe(started.cancel)
            try:
                _wait_until(finished)
            except KeyboardInterrupt:
                _warn_function_still_running(
                    rows_in_flight.labels(), rows_in_flight.async_labels
                )
                raise
        raise
    if errors:
        raise errors[0]
    return results[0]


def _cancel_leftover_tasks(loop: asyncio.AbstractEventLoop) -> None:
    """Cancel the tasks still pending on ``loop`` and let them finish, as ``asyncio.run`` does.

    A task a pod started and left running would otherwise lose its cancellation
    cleanup when the loop closes.

    Args:
        loop: The bridge thread's loop, no longer running.
    """
    leftover = asyncio.all_tasks(loop)
    if not leftover:
        return
    for task in leftover:
        _ = task.cancel()
    _ = loop.run_until_complete(asyncio.gather(*leftover, return_exceptions=True))


def _wait_until(event: threading.Event) -> None:
    """Block until ``event`` is set, letting a ``KeyboardInterrupt`` through.

    Waits on an event, not ``Thread.join``: on Python 3.12 an interrupt inside join
    marks a thread that is still running as stopped. Waits in short slices, so a pending
    interrupt is raised between them even when the signal reached another thread.

    Args:
        event: The event to wait for.
    """
    while not event.wait(_INTERRUPT_CHECK_SECONDS):
        pass


def _warn_function_still_running(
    labels: list[str], async_labels: frozenset[str]
) -> None:
    """Warn that a cancelled run's function has not returned, naming its nodes.

    A sync function runs on the loop's thread pool: the run is already cancelled, so
    when it returns nothing more is written. An async function that has not returned is
    blocking the loop's own thread, so the cancellation cannot run until it returns, and
    rows already in progress may still be stored then.

    Args:
        labels: The nodes with rows whose function has not returned.
        async_labels: The nodes whose function is an async function.
    """
    blocking = [label for label in labels if label in async_labels]
    if blocking:
        message = (
            f"The run was cancelled, but an async function in {_nodes(blocking)} is "
            "blocking the event loop and has not returned yet. When it returns, rows "
            "already in progress may still be stored before the run stops. To stop it "
            "now, restart the kernel (or end the process)."
        )
    else:
        which = f"a function in {_nodes(labels)}" if labels else "a function"
        message = (
            f"The run was cancelled, but {which} has not returned yet. Nothing more "
            "will be written to the store when it does. To stop it, restart the kernel "
            "(or end the process)."
        )
    # Shown every time, whatever filters hide repeated warnings.
    with warnings.catch_warnings():
        warnings.simplefilter("always", RuntimeWarning)
        warnings.warn(message, RuntimeWarning, stacklevel=2)


def _nodes(labels: list[str]) -> str:
    """``node 'a'`` or ``nodes 'a', 'b'``."""
    names = ", ".join(repr(label) for label in labels)
    return f"node {names}" if len(labels) == 1 else f"nodes {names}"


def _async_function_labels(graph: GraphProtocol[Any]) -> frozenset[str]:
    """The labels of the graph's function nodes whose function is an async function.

    Args:
        graph: The run's graph.

    Returns:
        The labels.
    """
    labels: set[str] = set()
    for node in graph.nodes():
        label: str = node.label
        if not (is_function_node(node) or is_side_effect_function_node(node)):
            continue
        # Only a Python data function has a kind; a loaded node has no pod.
        data_function = getattr(getattr(node, "producer", None), "data_function", None)
        if getattr(data_function, "is_async", False) is True:
            labels.add(label)
    return frozenset(labels)


class _RowsInFlight:
    """Observes a run: per node label, the rows started whose function has not returned.

    Rows already handed to the thread pool but not started are cancelled without an
    end event, so a node can count rows that were only queued. Every contextualized copy
    is this observer itself. Hooks may be called from several threads, so the counts are
    updated under a lock.

    Attributes:
        async_labels: The nodes whose function is an async function.
    """

    def __init__(self, async_labels: frozenset[str]) -> None:
        """Start with no rows in flight.

        Args:
            async_labels: The nodes whose function is an async function.
        """
        self.async_labels: frozenset[str] = async_labels
        self._lock: threading.Lock = threading.Lock()
        self._open: dict[str, int] = {}

    def labels(self) -> list[str]:
        """The nodes with rows in flight, in the order their first row started.

        Returns:
            The node labels.
        """
        with self._lock:
            return [label for label, n in self._open.items() if n > 0]

    def _change(self, node_label: str, delta: int) -> None:
        """Add ``delta`` to the rows in flight of ``node_label``."""
        with self._lock:
            self._open[node_label] = self._open.get(node_label, 0) + delta

    def contextualize(self, *identity_path: str) -> _RowsInFlight:
        """Return this observer: rows are keyed by node label."""
        return self

    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        """Nothing to track."""

    def on_run_end(self, run_id: str) -> None:
        """Nothing to track."""

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: Any = None
    ) -> None:
        """Nothing to track."""

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Nothing to track."""

    def on_data_start(self, node_label: str, tag: Any, data: Any) -> None:
        """A row's function starts."""
        self._change(node_label, 1)

    def on_data_end(
        self,
        node_label: str,
        tag: Any,
        input_data: Any,
        output_data: Any,
        cached: bool,
    ) -> None:
        """A row's function returned (or its result came from the store)."""
        self._change(node_label, -1)

    def on_data_crash(
        self, node_label: str, tag: Any, data: Any, error: Exception
    ) -> None:
        """A row's function raised."""
        self._change(node_label, -1)

    def create_data_logger(self, tag: Any, data: Any) -> Any:
        """No logging of its own."""
        from orcapod.pipeline.observer import NoOpLogger

        return NoOpLogger()


async def _drain(reader: Any) -> None:
    """Read ``reader`` to its end, keeping nothing."""
    async for _ in reader:
        pass


async def _send_held(
    rows: list[tuple[TagProtocol, DataProtocol]],
    writer: WritableChannel[tuple[TagProtocol, DataProtocol]],
) -> None:
    """Send the rows of a node the run holds rather than runs, then close."""
    try:
        for row in rows:
            await writer.send(row)
    finally:
        await writer.close()


def _start_node(
    tg: asyncio.TaskGroup,
    failed_nodes: dict[int, str],
    node: Any,
    work: Coroutine[Any, Any, None],
) -> None:
    """Start a node's coroutine in ``tg``, recording which node each failure came from.

    A store error raised on its own or inside an exception group (a node that runs
    its own task group) gets the node's label when it has none. Every exception the
    node raises, each one inside a group included, is recorded in ``failed_nodes``
    under the node's label, so ``_single_failure`` can order the run's failures as
    the sync orchestrator meets them.

    Args:
        tg: The run's task group.
        failed_nodes: The run's record, by exception id, of the node that raised it.
        node: The node the coroutine runs.
        work: The node's ``async_execute`` coroutine.
    """

    async def run() -> None:
        try:
            await work
        except StoreError as exc:
            # Name the node the failure belongs to, if the raising site did not,
            # so _single_store_error can order it like the sync orchestrator.
            exc.add_context(node_label=node.label)
            failed_nodes[id(exc)] = node.label
            raise
        except BaseExceptionGroup as group:
            # A node that runs its own task group raises its store errors inside
            # a group; name the node on each and re-raise the group unchanged.
            for store_error in StoreError.in_group(group):
                store_error.add_context(node_label=node.label)
            for leaf in _leaves(group):
                failed_nodes[id(leaf)] = node.label
            raise
        except Exception as exc:
            failed_nodes[id(exc)] = node.label
            raise

    tg.create_task(run())


def _single_failure(
    group: BaseExceptionGroup, topo_order: list[Any], failed_nodes: dict[int, str]
) -> BaseException:
    """Pick the one exception an async run raises when no store error is among its failures.

    The caller gets one exception, as from the sync orchestrator, never the task
    group's exception group: the one whose node comes first in topological order (the
    one the sync orchestrator would meet first, when both failures happen), with ties
    and exceptions of no node broken by their order in the group, which is the order
    the failures happened in. Every other exception is noted on the chosen one; the
    whole group becomes its ``__context__`` when it is raised.

    Args:
        group: The exception group the run's ``TaskGroup`` raised.
        topo_order: The run's nodes in topological order.
        failed_nodes: The node label of each exception a node raised, by exception id.

    Returns:
        The exception to raise.
    """
    leaves = _leaves(group)
    rank = {getattr(node, "label", None): i for i, node in enumerate(topo_order)}
    chosen = min(leaves, key=lambda exc: rank.get(failed_nodes.get(id(exc)), len(rank)))
    for other in leaves:
        if other is chosen:
            continue
        label = failed_nodes.get(id(other))
        where = f"Node {label!r}" if label is not None else "Another task"
        chosen.add_note(
            f"{where} also failed in the same run: {type(other).__name__}: {other}"
        )
    return chosen


def _single_store_error(
    group: ExceptionGroup, topo_order: list[Any]
) -> StoreError | None:
    """Pick the one ``StoreError`` an async run raises from its exception group.

    Concurrent node tasks can fail together, so the ``TaskGroup``'s group may hold
    several store errors, read and write ones alike, and other exceptions. The
    caller gets a single ``StoreError``, as from the sync orchestrator: the one
    whose node comes first in topological order (the one the sync orchestrator
    would meet first, when both failures happen), with ties and node-less errors
    broken by their order in the group. Every other exception stays attached for
    diagnosis: each is noted on the chosen error, other store errors are also
    logged at WARNING, and the whole group is the chosen error's ``__context__``.
    Its ``__cause__`` stays the backend error.

    Args:
        group: The exception group the run's ``TaskGroup`` raised.
        topo_order: The run's nodes in topological order.

    Returns:
        The store error to raise, or ``None`` when the group holds none.
    """
    store_errors = StoreError.in_group(group)
    if not store_errors:
        return None
    rank = {getattr(node, "label", None): i for i, node in enumerate(topo_order)}
    chosen = min(store_errors, key=lambda err: rank.get(err.node_label, len(rank)))
    for other in _leaves(group):
        if other is chosen:
            continue
        if isinstance(other, StoreError):
            kind = "write" if isinstance(other, StoreWriteError) else "read"
            logger.warning(
                "Another store %s failed in the same run: %s",
                kind,
                other,
                exc_info=other,
            )
            chosen.add_note(f"Another store {kind} failed in the same run: {other}")
        else:
            chosen.add_note(
                f"Another node failed in the same run: {type(other).__name__}: {other}"
            )
    return chosen


def _leaves(group: BaseExceptionGroup) -> list[BaseException]:
    """Return every non-group exception in ``group``, nested groups flattened."""
    found: list[BaseException] = []
    for exc in group.exceptions:
        if isinstance(exc, BaseExceptionGroup):
            found.extend(_leaves(exc))
        else:
            found.append(exc)
    return found


class _CollectingWriter:
    """Wrapper that collects items while forwarding to real writer."""

    def __init__(self, writer: Any, collector: list) -> None:
        self._writer = writer
        self._collector = collector

    async def send(self, item: Any) -> None:
        self._collector.append(item)
        await self._writer.send(item)

    async def close(self) -> None:
        await self._writer.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._writer, name)
