from typing import Protocol

from orcapod.protocols.core_protocols.labelable import Labelable
from orcapod.protocols.core_protocols.temporal import Temporal
from orcapod.protocols.hashing_protocols import ContentIdentifiable, DataContextAware


class OrcapodObject(
    DataContextAware, ContentIdentifiable, Labelable, Temporal, Protocol
):
    pass
