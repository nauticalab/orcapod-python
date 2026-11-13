"""``LogicalTypeRegistry.get_by_python_type`` picks the most specific registered type (ITL-888).

Array subclasses are treated as arrays unless a more specific subclass has its own
logical type; then that type wins for the subclass and everything below it,
whatever the registration order.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl
import pyarrow as pa

from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.numpy_type import LogicalNumpyArray
from orcapod.logical_types.registry import (
    LogicalTypeRegistry,
    make_arrow_extension_type,
    make_polars_extension_type,
)


class Labeled(np.ndarray):  # pyright: ignore[reportMissingTypeArgument]  # a toy subclass; type arguments are irrelevant here
    """Toy ndarray subclass with its own logical type."""


class SubLabeled(Labeled):
    """A subclass of the toy subclass, with no logical type of its own."""


class LogicalLabeled(BaseLogicalType):
    """Toy logical type for ``Labeled``; storage is irrelevant to the lookup."""

    _arrow_ext_class = make_arrow_extension_type(
        "test.labeled_ndarray", pa.large_binary()
    )
    _polars_ext_class = make_polars_extension_type(
        "test.labeled_ndarray", pa.large_binary()
    )
    logical_type_name: str = "test.labeled_ndarray"
    python_type: type = Labeled

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        return self._arrow_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments

    def get_polars_extension_type(self) -> pl.BaseExtension:
        return self._polars_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments

    def python_to_storage(self, value: Any, converter: Any = None) -> bytes:
        return b""

    def storage_to_python(self, storage_value: Any, converter: Any = None) -> Any:
        return np.zeros(0).view(Labeled)


def test_ndarray_subclasses_resolve_to_the_array_type() -> None:
    """NumPy's own ndarray subclasses resolve to the array logical type."""
    registry = LogicalTypeRegistry(logical_types=[LogicalNumpyArray()])
    for sub in (np.matrix, np.recarray, np.memmap):
        lt = registry.get_by_python_type(sub)
        assert lt is not None and lt.logical_type_name == "orcapod.numpy.ndarray"


def test_registered_subclass_wins_for_itself_and_its_subclasses() -> None:
    """A registered subclass wins for itself and its subclasses, in any registration order."""
    for order in ("array_first", "subclass_first"):
        array_lt, labeled_lt = LogicalNumpyArray(), LogicalLabeled()
        types = (
            [array_lt, labeled_lt] if order == "array_first" else [labeled_lt, array_lt]
        )
        registry = LogicalTypeRegistry(logical_types=types)  # pyright: ignore[reportArgumentType]  # toy logical type implements the protocol structurally
        assert registry.get_by_python_type(Labeled) is labeled_lt, order
        assert registry.get_by_python_type(SubLabeled) is labeled_lt, order
        assert registry.get_by_python_type(np.ndarray) is array_lt, order
        assert registry.get_by_python_type(np.matrix) is array_lt, order
