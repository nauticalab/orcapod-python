from collections.abc import Callable, Collection, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from deltalake import DeltaTable, write_deltalake
from pyarrow.lib import Table

from orcapod.core.datagrams import DictTag
from orcapod.core.executable_pod import TrackedKernelBase
from orcapod.core.streams import (
    ArrowTableStream,
    KernelStream,
    StatefulStreamBase,
)
from orcapod.errors import DuplicateTagError
from orcapod.protocols import core_protocols as cp
from orcapod.types import DataValue, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.contexts.system_constants import constants
from orcapod.semantic_types import infer_python_schema_from_pylist_data

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa
else:
    pl = LazyModule("polars")
    pd = LazyModule("pandas")
    pa = LazyModule("pyarrow")

from orcapod.core.sources.base import SourceBase


class ListSource(SourceBase):
    """
    A stream source that sources data from a list of elements.
    For each element in the list, yields a tuple containing:
    - A tag generated either by the provided tag_function or defaulting to the element index
    - A packet containing the element under the provided name key
    Parameters
    ----------
    name : str
        The key name under which each list element will be stored in the packet
    data : list[Any]
        The list of elements to source data from
    tag_function : Callable[[Any, int], TagProtocol] | None, default=None
        Optional function to generate a tag from a list element and its index.
        The function receives the element and the index as arguments.
        If None, uses the element index in a dict with key 'element_index'
    tag_function_hash_mode : Literal["content", "signature", "name"], default="name"
        How to hash the tag function for identity purposes
    expected_tag_keys : Collection[str] | None, default=None
        Expected tag keys for the stream
    label : str | None, default=None
        Optional label for the source
    Examples
    --------
    >>> # Simple list of file names
    >>> file_list = ['/path/to/file1.txt', '/path/to/file2.txt', '/path/to/file3.txt']
    >>> source = ListSource('file_path', file_list)
    >>>
    >>> # Custom tag function using filename stems
    >>> from pathlib import Path
    >>> source = ListSource(
    ...     'file_path',
    ...     file_list,
    ...     tag_function=lambda elem, idx: {'file_name': Path(elem).stem}
    ... )
    >>>
    >>> # List of sample IDs
    >>> samples = ['sample_001', 'sample_002', 'sample_003']
    >>> source = ListSource(
    ...     'sample_id',
    ...     samples,
    ...     tag_function=lambda elem, idx: {'sample': elem}
    ... )
    """

    @staticmethod
    def default_tag_function(element: Any, idx: int) -> cp.TagProtocol:
        return DictTag({"element_index": idx})

    def __init__(
        self,
        name: str,
        data: list[Any],
        tag_function: Callable[[Any, int], cp.TagProtocol] | None = None,
        label: str | None = None,
        tag_function_hash_mode: Literal["content", "signature", "name"] = "name",
        expected_tag_keys: Collection[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(label=label, **kwargs)
        self.name = name
        self.elements = list(data)  # Create a copy to avoid external modifications

        if tag_function is None:
            tag_function = self.__class__.default_tag_function
            # If using default tag function and no explicit expected_tag_keys, set to default
            if expected_tag_keys is None:
                expected_tag_keys = ["element_index"]

        self.expected_tag_keys = expected_tag_keys
        self.tag_function = tag_function
        self.tag_function_hash_mode = tag_function_hash_mode

    def forward(self, *streams: SyncStream) -> SyncStream:
        if len(streams) != 0:
            raise ValueError(
                "ListSource does not support forwarding streams. "
                "It generates its own stream from the list elements."
            )

        def generator() -> Iterator[tuple[TagProtocol, PacketProtocol]]:
            for idx, element in enumerate(self.elements):
                tag = self.tag_function(element, idx)
                packet = {self.name: element}
                yield tag, packet

        return SyncStreamFromGenerator(generator)

    def __repr__(self) -> str:
        return f"ListSource({self.name}, {len(self.elements)} elements)"

    def identity_structure(self, *streams: SyncStream) -> Any:
        hash_function_kwargs = {}
        if self.tag_function_hash_mode == "content":
            # if using content hash, exclude few
            hash_function_kwargs = {
                "include_name": False,
                "include_module": False,
                "include_declaration": False,
            }

        tag_function_hash = hash_function(
            self.tag_function,
            function_hash_mode=self.tag_function_hash_mode,
            hash_kwargs=hash_function_kwargs,
        )

        # Convert list to hashable representation
        # Handle potentially unhashable elements by converting to string
        try:
            elements_hashable = tuple(self.elements)
        except TypeError:
            # If elements are not hashable, convert to string representation
            elements_hashable = tuple(str(elem) for elem in self.elements)

        return (
            self.__class__.__name__,
            self.name,
            elements_hashable,
            tag_function_hash,
        ) + tuple(streams)

    def keys(
        self, *streams: SyncStream, trigger_run: bool = False
    ) -> tuple[Collection[str] | None, Collection[str] | None]:
        """
        Returns the keys of the stream. The keys are the names of the packets
        in the stream. The keys are used to identify the packets in the stream.
        If expected_keys are provided, they will be used instead of the default keys.
        """
        if len(streams) != 0:
            raise ValueError(
                "ListSource does not support forwarding streams. "
                "It generates its own stream from the list elements."
            )

        if self.expected_tag_keys is not None:
            return tuple(self.expected_tag_keys), (self.name,)
        return super().keys(trigger_run=trigger_run)

    def claims_unique_tags(
        self, *streams: "SyncStream", trigger_run: bool = True
    ) -> bool | None:
        if len(streams) != 0:
            raise ValueError(
                "ListSource does not support forwarding streams. "
                "It generates its own stream from the list elements."
            )
        # Claim uniqueness only if the default tag function is used
        if self.tag_function == self.__class__.default_tag_function:
            return True
        # Otherwise, delegate to the base class
        return super().claims_unique_tags(trigger_run=trigger_run)
