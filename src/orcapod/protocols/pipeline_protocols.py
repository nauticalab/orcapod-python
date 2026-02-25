# Protocols for pipeline and nodes
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from orcapod.protocols import core_protocols as cp

if TYPE_CHECKING:
    import pyarrow as pa


class Node(cp.Source, Protocol):
    # def record_pipeline_outputs(self):
    #     pass
    ...


@runtime_checkable
class PodNode(cp.CachedPod, Protocol):
    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all tag and packet processed by this Pod.

        This method returns a table containing all packets processed by the Pod,
        including metadata and system columns if requested. It is useful for:
        - Debugging and analysis
        - Auditing and data lineage tracking
        - Performance monitoring

        Args:
            include_system_columns: Whether to include system columns in the output

        Returns:
            pa.Table | None: A table containing all processed records, or None if no records are available
        """
        ...

    def flush(self):
        """
        Flush any in-memory data to persistent storage.

        This method ensures that all buffered data is written to the underlying
        storage system, making it durable and consistent. It is useful for:
        - Ensuring data integrity before shutdown or restart
        - Committing changes after a batch of operations
        - Reducing memory usage by clearing buffers

        """
        ...

    def add_pipeline_record(
        self,
        tag: cp.Tag,
        input_packet: cp.Packet,
        packet_record_id: str,
        retrieved: bool | None = None,
        skip_cache_lookup: bool = False,
    ) -> None: ...
