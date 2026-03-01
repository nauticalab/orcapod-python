"""
Legacy datagram implementations — scheduled for removal.

These classes are the original Arrow-backed and dict-backed implementations that
predated the unified Datagram/Tag/Packet hierarchy.  They are preserved here for
reference while the codebase migrates to the new classes; they will be deleted once
migration is complete.
"""

from .arrow_datagram import ArrowDatagram
from .arrow_tag_packet import ArrowPacket, ArrowTag
from .dict_datagram import DictDatagram
from .dict_tag_packet import DictPacket, DictTag

__all__ = [
    "ArrowDatagram",
    "ArrowTag",
    "ArrowPacket",
    "DictDatagram",
    "DictTag",
    "DictPacket",
]
