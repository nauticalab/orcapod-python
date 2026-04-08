from typing import Protocol

from orcapod.protocols.core_protocols.labelable import LabelableProtocol
from orcapod.protocols.core_protocols.temporal import TemporalProtocol
from orcapod.protocols.hashing_protocols import (
    ContentIdentifiableProtocol,
    DataContextAwareProtocol,
)


class TraceableProtocol(
    DataContextAwareProtocol,
    ContentIdentifiableProtocol,
    LabelableProtocol,
    TemporalProtocol,
    Protocol,
):
    """
    Base protocol for objects that can be traced.
    """

    pass
