from .datagram import Datagram
from .tag_packet import Packet, Tag

# Legacy classes — scheduled for removal once all callers migrate
from .legacy.arrow_datagram import ArrowDatagram
from .legacy.arrow_tag_packet import ArrowPacket, ArrowTag
from .legacy.dict_datagram import DictDatagram
from .legacy.dict_tag_packet import DictPacket, DictTag

__all__ = [
    # New unified classes (preferred)
    "Datagram",
    "Tag",
    "Packet",
    # Legacy classes (scheduled for removal)
    "ArrowDatagram",
    "ArrowTag",
    "ArrowPacket",
    "DictDatagram",
    "DictTag",
    "DictPacket",
]
