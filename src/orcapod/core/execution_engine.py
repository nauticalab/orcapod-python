from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class ExecutionEngine(Protocol):
    @property
    def name(self) -> str: ...

    def submit_sync(self, function: Callable, *args, **kwargs) -> Any:
        """
        Run the given function with the provided arguments.
        This method should be implemented by the execution engine.
        """
        ...

    async def submit_async(self, function: Callable, *args, **kwargs) -> Any:
        """
        Asynchronously run the given function with the provided arguments.
        This method should be implemented by the execution engine.
        """
        ...
