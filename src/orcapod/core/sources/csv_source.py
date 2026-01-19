from typing import TYPE_CHECKING, Any


from orcapod.core.streams import (
    TableStream,
)
from orcapod.protocols import core_protocols as cp
from orcapod.types import Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa
else:
    pl = LazyModule("polars")
    pd = LazyModule("pandas")
    pa = LazyModule("pyarrow")

from orcapod.core.sources.base import SourceBase


class CSVSource(SourceBase):
    """Loads data from a CSV file."""

    def __init__(
        self,
        file_path: str,
        tag_columns: list[str] | None = None,
        source_id: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.tag_columns = tag_columns or []
        if source_id is None:
            source_id = self.file_path

    def source_identity_structure(self) -> Any:
        return (self.__class__.__name__, self.source_id, tuple(self.tag_columns))

    def forward(self, *streams: cp.Stream) -> cp.Stream:
        """
        Load data from file and return a static stream.

        This is called by forward() and creates a fresh snapshot each time.
        """
        import pyarrow.csv as csv

        # Load current state of the file
        table = csv.read_csv(self.file_path)

        return TableStream(
            table=table,
            tag_columns=self.tag_columns,
            source=self,
            upstreams=(),
        )

    def source_output_types(
        self, include_system_tags: bool = False
    ) -> tuple[Schema, Schema]:
        """Infer types from the file (could be cached)."""
        # For demonstration - in practice you might cache this
        sample_stream = self.forward()
        return sample_stream.types(include_system_tags=include_system_tags)
