from orcapod.utils.lazy_module import LazyModule
from typing import TYPE_CHECKING, Any, TypeVar
import logging
from collections.abc import Callable

if TYPE_CHECKING:
    import asyncio
    import ray
else:
    asyncio = LazyModule("asyncio")
    ray = LazyModule("ray")

logger = logging.getLogger(__name__)


T = TypeVar("T")


class RayEngine:
    """
    Ray execution engine using native asyncio support.

    This approach uses Ray's built-in async capabilities:
    1. For tasks: Uses ObjectRef.future() + asyncio.wrap_future()
    2. For batch operations: Uses ray's native async support
    3. No polling needed - Ray handles async integration
    """

    @property
    def supports_async(self) -> bool:
        """Indicate that this engine supports async execution."""
        return True

    def __init__(self, ray_address: str | None = None, **ray_init_kwargs):
        """Initialize Ray with native async support."""

        if not self.is_initialized():
            ray.init(address=ray_address, **ray_init_kwargs)
            # track whether ray engine was initialized in this engine
            self._ray_initialized_here = True
            logger.info("Native Ray async engine initialized")
        else:
            self._ray_initialized_here = False
            logger.info("Working with an existing Ray async engine")

        logger.info(f"Cluster resources: {ray.cluster_resources()}")

    def is_initialized(self) -> bool:
        """Check if Ray is initialized."""
        return ray.is_initialized()

    @property
    def name(self) -> str:
        return "ray"

    def submit_sync(
        self,
        func: Callable[..., T],
        /,
        *,
        fn_args: tuple[Any, ...] = (),
        fn_kwargs: dict[str, Any] | None = None,
        **engine_opts: Any,
    ) -> T:
        """
        Submit a function synchronously using Ray.

        Arguments destined for the function must be provided via
        ``fn_args``/``fn_kwargs``. Engine-specific options (e.g., resources)
        should be passed through ``engine_opts`` and are applied via
        ``.options(**engine_opts)``.
        """
        if fn_kwargs is None:
            fn_kwargs = {}

        # Create remote function and submit
        remote_func = ray.remote(func)
        if engine_opts:
            remote_func = remote_func.options(**engine_opts)  # type: ignore[arg-type]
        object_ref = remote_func.remote(*fn_args, **fn_kwargs)

        # Wait for the result - this is blocking
        result = ray.get(object_ref)
        return result

    async def submit_async(
        self,
        func: Callable[..., T],
        /,
        *,
        fn_args: tuple[Any, ...] = (),
        fn_kwargs: dict[str, Any] | None = None,
        **engine_opts: Any,
    ) -> T:
        """
        Submit a function using Ray's native async support.

        Arguments destined for the function must be provided via
        ``fn_args``/``fn_kwargs``. Engine-specific options (e.g., resources)
        should be passed through ``engine_opts`` and are applied via
        ``.options(**engine_opts)``.
        """
        if fn_kwargs is None:
            fn_kwargs = {}

        # Create remote function and submit
        remote_func = ray.remote(func)
        if engine_opts:
            remote_func = remote_func.options(**engine_opts)  # type: ignore[arg-type]
        object_ref = remote_func.remote(*fn_args, **fn_kwargs)

        # Use Ray's native async support - this is the key insight!
        # ObjectRef.future() returns a concurrent.futures.Future that works with asyncio
        future = object_ref.future()  # type: ignore
        asyncio_future = asyncio.wrap_future(future)

        # This is truly non-blocking and integrates with asyncio event loop
        result = await asyncio_future
        return result

    async def submit_batch_async(
        self,
        func: Callable[..., T],
        args_list: list[tuple],
        kwargs_list: list[dict] | None = None,
    ) -> list[T]:
        """
        Submit batch using Ray's native async support.

        This is much more efficient than individual submissions.
        """
        if kwargs_list is None:
            kwargs_list = [{}] * len(args_list)

        # Create remote function once
        remote_func = ray.remote(func)

        # Submit all tasks and get ObjectRefs
        object_refs = [
            remote_func.remote(*args, **kwargs)
            for args, kwargs in zip(args_list, kwargs_list)
        ]

        # Convert all ObjectRefs to asyncio futures
        asyncio_futures = [
            asyncio.wrap_future(obj_ref.future())  # type: ignore
            for obj_ref in object_refs
        ]

        # Use asyncio.gather for efficient concurrent execution
        results = await asyncio.gather(*asyncio_futures)
        return results

    def get_cluster_info(self) -> dict[str, Any]:
        """Get Ray cluster information."""
        return {
            "cluster_resources": ray.cluster_resources(),
            "available_resources": ray.available_resources(),
            "nodes": ray.nodes(),
        }

    def shutdown(self) -> None:
        """Shutdown Ray if we initialized it."""
        if self._ray_initialized_here and ray.is_initialized():
            ray.shutdown()
            logger.info("Native Ray async engine shut down")
