"""Handler factory for Python dataclasses.

Implements ``DataclassHandlerFactory`` — a ``LogicalTypeFactoryProtocol`` that
constructs ``DataclassLogicalType`` instances for any Python dataclass on both
the write path (annotation-driven) and read path (Arrow schema metadata).

Registration example::

    factory = DataclassHandlerFactory()
    registry.register_logical_type_factory(
        factory,
        category="orcapod.dataclass",
        python_bases=[object],
    )

Note:
    Nested dataclasses are stored as plain Arrow sub-structs (not extension
    types). Only the outermost column is self-describing via the extension
    type metadata. Supporting nested extension types inside struct sub-fields
    is tracked as a v0.2 issue (PLT-1700).

    Registered logical types (e.g. ``pathlib.Path``, ``uuid.UUID``) used as
    dataclass field annotations are not supported in this version and will
    raise ``TypeError``. A follow-up issue will add registry-lookup support.
"""

from __future__ import annotations

import dataclasses
import importlib
from typing import TYPE_CHECKING, Any, Callable

from orcapod.extension_types.protocols import ResolutionContext
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from orcapod.extension_types.protocols import LogicalTypeProtocol
    from orcapod.extension_types.registry import LogicalTypeRegistry
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

_DATACLASS_CATEGORY_METADATA = b'{"category": "orcapod.dataclass"}'

# Mapping from primitive Python type → Arrow type.
_PRIMITIVE_ARROW: dict[type, Any] = {}


def _primitive_arrow_map() -> dict[type, Any]:
    """Return (and lazily build) the primitive-type → Arrow-type lookup table."""
    global _PRIMITIVE_ARROW
    if not _PRIMITIVE_ARROW:
        _PRIMITIVE_ARROW = {
            int: pa.int64(),
            float: pa.float64(),
            str: pa.large_string(),
            bool: pa.bool_(),
            bytes: pa.large_binary(),
        }
    return _PRIMITIVE_ARROW


class DataclassLogicalType:
    """Concrete ``LogicalTypeProtocol`` for a specific Python dataclass.

    Constructed once by ``DataclassHandlerFactory``; holds no registry reference —
    all conversion logic is baked in at construction time via pre-built field
    converters.

    Args:
        logical_name: Fully qualified class name (e.g. ``"my.module.Data1"``).
        python_type: The dataclass class.
        storage_type: ``pa.struct([...])`` describing the Arrow layout.
        field_converters: Ordered list of ``(field_name, to_storage_fn,
            from_storage_fn)`` tuples. Primitive fields use identity functions;
            nested dataclass fields use their own ``python_to_storage`` /
            ``storage_to_python`` methods; ``list[T]`` fields use element-wise
            converters.
    """

    def __init__(
        self,
        logical_name: str,
        python_type: type,
        storage_type: pa.DataType,
        field_converters: list[tuple[str, Callable[..., Any], Callable[..., Any]]],
    ) -> None:
        self._logical_name = logical_name
        self._python_type = python_type
        self._storage_type = storage_type
        self._field_converters = field_converters
        self._arrow_ext_class = make_arrow_extension_type(
            logical_name, storage_type, _DATACLASS_CATEGORY_METADATA
        )
        self._polars_ext_class = make_polars_extension_type(logical_name, storage_type)
        self._arrow_ext: pa.ExtensionType | None = None
        self._polars_ext: pl.BaseExtension | None = None

    @property
    def logical_type_name(self) -> str:
        """Fully qualified class name used as the Arrow extension name."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """The dataclass class this logical type represents."""
        return self._python_type

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return a cached Arrow extension type instance for this dataclass.

        Returns:
            A ``pa.ExtensionType`` with extension name equal to the FQCN and
            storage type ``pa.struct([...])``. Metadata bytes encode
            ``{"category": "orcapod.dataclass"}`` for read-path dispatch.
        """
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()
        return self._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return a cached Polars extension type instance for this dataclass.

        Returns:
            A ``pl.BaseExtension`` registered under the FQCN.
        """
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()
        return self._polars_ext

    def python_to_storage(self, value: Any) -> dict[str, Any]:
        """Convert a dataclass instance to a Python dict for Arrow struct storage.

        Args:
            value: A Python instance of ``python_type``.

        Returns:
            A dict mapping field names to their storage-converted values.
        """
        return {
            name: to_fn(getattr(value, name))
            for name, to_fn, _ in self._field_converters
        }

    def storage_to_python(self, storage_value: Any) -> Any:
        """Reconstruct the dataclass from an Arrow struct ``.as_py()`` dict.

        Args:
            storage_value: A Python dict as returned by ``scalar.as_py()`` for
                a struct-storage Arrow scalar.

        Returns:
            A fully reconstructed instance of ``python_type``.
        """
        return self._python_type(**{
            name: from_fn(storage_value[name])
            for name, _, from_fn in self._field_converters
        })
