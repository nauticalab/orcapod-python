from typing import Protocol, runtime_checkable

from orcapod.protocols.core_protocols.pod import PodProtocol
from orcapod.protocols.core_protocols.streams import StreamProtocol


@runtime_checkable
class SourcePodProtocol(PodProtocol, StreamProtocol, Protocol):
    """
    Entry point for data into the computational graph.

    Sources are special objects that serve dual roles:
    - As Kernels: Can be invoked to produce streams
    - As Streams: Directly provide data without upstream dependencies

    Sources represent the roots of computational graphs and typically
    interface with external data sources. They bridge the gap between
    the outside world and the Orcapod computational model.

    Common source types:
    - File readers (CSV, JSON, Parquet, etc.)
    - Database connections and queries
    - API endpoints and web services
    - Generated data sources (synthetic data)
    - Manual data input and user interfaces
    - Message queues and event streams

    Sources have unique properties:
    - No upstream dependencies (upstreams is empty)
    - Can be both invoked and iterated
    - Serve as the starting point for data lineage
    - May have their own refresh/update mechanisms
    """
