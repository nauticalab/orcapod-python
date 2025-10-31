from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable
from orcapod.types import DataValue


@runtime_checkable
class ExecutionEngine(Protocol):
    """
    Abstract execution backend responsible for running user functions.

    ExecutionEngine defines the minimal contract that any execution backend
    must satisfy to be used by Orcapod. Concrete implementations may execute
    work in the current process (synchronously), on background threads or
    processes, or on remote/distributed systems (e.g., Ray, Dask, Slurm).

    Responsibilities
    - Accept a Python callable plus arguments and execute it.
    - Provide both a synchronous API (blocking) and an asynchronous API
      (awaitable) with consistent error semantics.
    - Surface the original exception from the user function without
      wrapping where practical, while preserving traceback information.
    - Be safe to construct/read concurrently from the pipeline orchestration.

    Contract
    - Inputs: a Python callable and its positional/keyword arguments.
    - Outputs: the callable's return value (or a coroutine result when awaited).
    - Errors: exceptions raised by the callable must be propagated to the
      caller of submit_sync/submit_async.
    - Cancellation: implementations may optionally support task cancellation
      in submit_async via standard asyncio cancellation; submit_sync is
      expected to block until completion.

    Notes
    - Serialization: Distributed engines may require the function and its
      arguments to be serializable (pickle/cloudpickle). Local engines have
      no such requirement beyond normal Python callability.
    - Resource usage: Engines may schedule work with resource hints
      (CPU/GPU/memory) outside this minimal protocol; higher-level APIs can
      extend this interface if needed.
    - Naming: ``name`` should be a short, human-friendly identifier such as
      "local", "threadpool", "processpool", or "ray" and is used for logging
      and diagnostics.
    """

    @property
    def name(self) -> str:
        """Return a short, human-friendly identifier for the engine.

        Examples: "local", "threadpool", "processpool", "ray".
        Used for logging, metrics, and debugging output.
        """
        ...

    def submit_sync(
        self,
        func: Callable[..., Any],
        /,
        *,
        fn_args: tuple[Any, ...] = (),
        fn_kwargs: dict[str, Any] | None = None,
        **engine_opts: Any,
    ) -> Any:
        """
        Execute a callable and return its result (blocking).

        This call is blocking. Engines may choose where/how the function
        executes (same thread, worker thread/process, remote node), but the
        call does not return until the work completes or fails.

        Parameters
        - func: Python callable to execute.
        - fn_args: Tuple of positional arguments to pass to ``func``.
        - fn_kwargs: Mapping of keyword arguments to pass to ``func``.
        - **engine_opts: Engine-specific options (e.g., resources, priority),
          never forwarded to ``func``.

        Returns:
            Any: The return value of ``func``.

        Raises:
            Exception: Any exception raised by ``func`` must be propagated to
            the caller. Engines should preserve the original traceback whenever
            practical.

        Notes
        - This API separates function inputs from engine configuration.
          ``fn_args``/``fn_kwargs`` are always applied to ``func``;
          ``engine_opts`` configures the engine and is never forwarded.
        """
        ...

    async def submit_async(
        self,
        func: Callable[..., Any],
        /,
        *,
        fn_args: tuple[Any, ...] = (),
        fn_kwargs: dict[str, Any] | None = None,
        **engine_opts: Any,
    ) -> Any:
        """
        Asynchronously execute a callable and return the result when awaited.

        The returned awaitable resolves to the callable's return value or
        raises the callable's exception. Implementations should integrate with
        asyncio semantics: if the awaiting task is cancelled, the engine may
        attempt to cancel the underlying work when supported.

        Parameters
        - func: Python callable to execute.
        - fn_args: Tuple of positional arguments to pass to ``func``.
        - fn_kwargs: Mapping of keyword arguments to pass to ``func``.
        - **engine_opts: Engine-specific options (e.g., resources, priority),
          never forwarded to ``func``.

        Returns:
            Any: The return value of ``func`` when awaited.

        Raises:
            asyncio.CancelledError: If the awaiting task is cancelled and the
            implementation propagates cancellation.
            Exception: Any exception raised by ``func`` must be propagated to
            the awaiting caller, with traceback preserved where possible.

        Notes
        - Mirrors the sync API: ``fn_args``/``fn_kwargs`` target ``func``;
          ``engine_opts`` configures the engine and is never forwarded.
        """
        ...

    # TODO: consider adding batch submission


@runtime_checkable
class PodFunction(Protocol):
    """
    A function suitable for use in a FunctionPod.

    PodFunctions define the computational logic that operates on individual
    packets within a Pod. They represent pure functions that transform
    data values without side effects.

    These functions are designed to be:
    - Stateless: No dependency on external state
    - Deterministic: Same inputs always produce same outputs
    - Serializable: Can be cached and distributed
    - Type-safe: Clear input/output contracts

    PodFunctions accept named arguments corresponding to packet fields
    and return transformed data values.
    """

    def __call__(self, **kwargs: DataValue) -> None | DataValue:
        """
        Execute the pod function with the given arguments.

        The function receives packet data as named arguments and returns
        either transformed data or None (for filtering operations).

        Args:
            **kwargs: Named arguments mapping packet fields to data values

        Returns:
            None: Filter out this packet (don't include in output)
            DataValue: Single transformed value

        Raises:
            TypeError: If required arguments are missing
            ValueError: If argument values are invalid
        """
        ...


@runtime_checkable
class Labelable(Protocol):
    """
    Protocol for objects that can have a human-readable label.

    Labels provide meaningful names for objects in the computational graph,
    making debugging, visualization, and monitoring much easier. They serve
    as human-friendly identifiers that complement the technical identifiers
    used internally.

    Labels are optional but highly recommended for:
    - Debugging complex computational graphs
    - Visualization and monitoring tools
    - Error messages and logging
    - User interfaces and dashboards
    """

    @property
    def label(self) -> str:
        """
        Return the human-readable label for this object.

        Labels should be descriptive and help users understand the purpose
        or role of the object in the computational graph.

        Returns:
            str: Human-readable label for this object
            None: No label is set (will use default naming)
        """
        ...

    @label.setter
    def label(self, label: str | None) -> None:
        """
        Set the human-readable label for this object.

        Labels should be descriptive and help users understand the purpose
        or role of the object in the computational graph.

        Args:
            value (str): Human-readable label for this object
        """
        ...
