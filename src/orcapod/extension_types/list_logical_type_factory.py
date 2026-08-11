"""ListLogicalType: wraps list[T]/set[T] (where T is an Arrow extension type) as a
top-level Arrow extension type with storage ``large_list(<T storage>)``.

This solves the ET1 constraint (Arrow cannot embed extension types inside list value
positions): by promoting the extension metadata to the outermost field level, the
metadata IS preserved through Parquet round-trips while the list values remain plain
storage types.

Design decisions:
- Extension name: ``list[orcapod.uuid]`` or ``set[orcapod.uuid]`` (based on element name)
- Storage type: ``pa.large_list(element_storage)`` — NO extension type inside list value
- Metadata JSON: ``{"category": "list"|"set", "element_ext_name": "...", "element_ext_metadata": null_or_string}``
- ``python_type`` returns the full generic alias ``list[uuid.UUID]`` (not bare ``list``)
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from orcapod.extension_types.protocols import TypeConverterProtocol
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)

LIST_CATEGORY = "list"
SET_CATEGORY = "set"


class ListLogicalType(BaseLogicalType):
    """Logical type for ``list[T]`` or ``set[T]`` where T maps to an Arrow extension type.

    Wraps a homogeneous list or set of elements whose Python type corresponds to
    a registered Arrow extension type (e.g. ``list[uuid.UUID]`` where ``uuid.UUID``
    maps to ``orcapod.uuid``). The resulting Arrow extension type has:

    - Extension name: ``list[<element_ext_name>]`` or ``set[<element_ext_name>]``
    - Storage type: ``pa.large_list(<element_storage_type>)`` — ET1 safe (no nested extension)
    - Metadata: JSON with ``category``, ``element_ext_name``, and ``element_ext_metadata``

    Args:
        element_python_type: The Python type of individual list elements (e.g. ``uuid.UUID``).
        element_ext_type: The Arrow extension type for the element (e.g. the ``orcapod.uuid``
            extension type instance).
        is_set: If ``True``, uses ``set[T]`` semantics (``storage_to_python`` returns a
            ``set``). Defaults to ``False`` (``list[T]`` semantics).

    Example:
        >>> from orcapod.extension_types.builtin_logical_types import LogicalUUID
        >>> import uuid
        >>> uuid_ext = LogicalUUID().get_arrow_extension_type()
        >>> lt = ListLogicalType(uuid.UUID, uuid_ext, is_set=False)
        >>> lt.logical_type_name
        'list[orcapod.uuid]'
        >>> lt.python_type
        list[uuid.UUID]
    """

    def __init__(
        self,
        element_python_type: type,
        element_ext_type: pa.ExtensionType,
        *,
        is_set: bool = False,
    ) -> None:
        self._element_python_type = element_python_type
        self._element_ext_type = element_ext_type
        self._is_set = is_set
        self._arrow_ext: pa.ExtensionType | None = None
        self._polars_ext: pl.BaseExtension | None = None

        # Build metadata bytes for Arrow extension serialization.
        element_ext_name = element_ext_type.extension_name
        raw_meta_bytes: bytes = element_ext_type.__arrow_ext_serialize__()
        element_ext_metadata: str | None = raw_meta_bytes.decode("utf-8") if raw_meta_bytes else None

        category = SET_CATEGORY if is_set else LIST_CATEGORY
        meta_dict = {
            "category": category,
            "element_ext_name": element_ext_name,
            "element_ext_metadata": element_ext_metadata,
        }
        self._metadata_bytes: bytes = json.dumps(meta_dict).encode("utf-8")

        # Derive storage type: large_list of the element's storage type (not extension type).
        element_storage = element_ext_type.storage_type
        self._storage_type = pa.large_list(element_storage)

        # Compose the extension name from the category and element extension name.
        self._logical_type_name = f"{category}[{element_ext_name}]"

    @property
    def logical_type_name(self) -> str:
        """Unique orcapod identifier for this logical type.

        Returns:
            A string like ``"list[orcapod.uuid]"`` or ``"set[orcapod.uuid]"``.
        """
        return self._logical_type_name

    @property
    def python_type(self) -> type:
        """The Python generic alias this logical type represents.

        Returns:
            A generic alias like ``list[uuid.UUID]`` or ``set[uuid.UUID]``.
        """
        if self._is_set:
            return set[self._element_python_type]  # type: ignore[valid-type]
        return list[self._element_python_type]  # type: ignore[valid-type]

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for this list/set logical type.

        The extension type has:
        - ``extension_name``: ``"list[<element_ext_name>]"`` or ``"set[...]"``
        - ``storage_type``: ``pa.large_list(<element_storage_type>)``
        - Serialized metadata: JSON with ``category``, ``element_ext_name``,
          and ``element_ext_metadata``

        Returns:
            A cached ``pa.ExtensionType`` instance.
        """
        if self._arrow_ext is None:
            arrow_ext_class = make_arrow_extension_type(
                self._logical_type_name,
                self._storage_type,
                metadata=self._metadata_bytes,
            )
            self._arrow_ext = arrow_ext_class()
        return self._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for this list/set logical type.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under the logical
            type name.
        """
        if self._polars_ext is None:
            polars_ext_class = make_polars_extension_type(
                self._logical_type_name,
                self._storage_type,
            )
            self._polars_ext = polars_ext_class()
        return self._polars_ext

    def index_element(self) -> type:
        """Return the Python element type for positional list access.

        Returns:
            The Python type of individual elements (e.g. ``uuid.UUID``).
        """
        return self._element_python_type

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None) -> list:
        """Convert a Python list or set to its Arrow storage representation.

        Delegates per-element conversion to ``converter.python_to_storage`` using
        the element Python type. If ``value`` is ``None``, returns an empty list.

        Args:
            value: A Python list (or set) of elements of type ``element_python_type``,
                or ``None``.
            converter: The active ``TypeConverterProtocol`` for per-element delegation.

        Returns:
            A plain Python list of storage values suitable for Arrow large_list storage.
        """
        if value is None:
            return []
        if converter is None:
            return list(value)
        return [converter.python_to_storage(item, self._element_python_type) for item in value]

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None) -> list | set:
        """Reconstruct a Python list or set from its Arrow storage representation.

        Delegates per-element conversion to ``converter.storage_to_python`` using
        the element Python type. If ``storage_value`` is ``None``, returns an empty
        list (or empty set for set semantics).

        Args:
            storage_value: An iterable of element storage values from Arrow large_list
                storage, or ``None``.
            converter: The active ``TypeConverterProtocol`` for per-element delegation.

        Returns:
            A Python ``list`` (or ``set`` if ``is_set=True``) of ``element_python_type``
            instances.
        """
        if storage_value is None:
            return set() if self._is_set else []
        if converter is None:
            elements = list(storage_value)
        else:
            elements = [
                converter.storage_to_python(item, self._element_python_type)
                for item in storage_value
            ]
        return set(elements) if self._is_set else elements


class ListLogicalTypeFactory:
    """Stateless factory that reconstructs ``ListLogicalType`` instances from Arrow metadata.

    Registered for categories ``"list"`` and ``"set"`` in the ``LogicalTypeRegistry``.
    No ``python_bases`` are registered — write-path dispatch is handled explicitly in
    ``UniversalTypeConverter._register_python_class_impl`` and ``_convert_python_to_arrow``.

    Read path only (``reconstruct_from_arrow``). ``create_for_python_type`` raises
    ``NotImplementedError`` because explicit dispatch makes it unnecessary.
    """

    def supports_class(self, python_type: type) -> bool:
        """Always ``False`` — write-path dispatch is explicit, not via base-class matching.

        Args:
            python_type: Ignored.

        Returns:
            ``False``.
        """
        return False

    def create_for_python_type(
        self,
        python_type: type,
        converter: "TypeConverterProtocol",
    ) -> ListLogicalType:
        """Not implemented — list/set types are registered directly in the converter.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "ListLogicalTypeFactory does not implement create_for_python_type. "
            "list[T] and set[T] logical types are created explicitly in "
            "UniversalTypeConverter._register_python_class_impl."
        )

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: "pa.DataType",
        metadata: dict,
        converter: "TypeConverterProtocol",
    ) -> ListLogicalType:
        """Reconstruct a ``ListLogicalType`` from Arrow schema metadata (read path).

        Recursively calls ``converter.register_arrow_extension`` for the element type,
        ensuring the element logical type is registered before constructing the outer
        ``ListLogicalType``. Handles arbitrary nesting depth via recursion.

        Args:
            arrow_extension_name: Extension name (e.g. ``"list[orcapod.uuid]"``).
            storage_type: Outer storage type (``large_list(<element storage>)``).
            metadata: Parsed metadata dict; must contain ``"category"`` and
                ``"element_ext_name"``; ``"element_ext_metadata"`` may be ``None``.
            converter: Active converter for recursive element registration.

        Returns:
            A ``ListLogicalType`` ready for registration.

        Raises:
            ValueError: If ``storage_type`` is not a list type, or required metadata
                keys are missing.
        """
        if not (pa.types.is_large_list(storage_type) or pa.types.is_list(storage_type)):
            raise ValueError(
                f"ListLogicalTypeFactory.reconstruct_from_arrow: expected a list storage "
                f"type for {arrow_extension_name!r}, got {storage_type!r}."
            )

        element_ext_name = metadata.get("element_ext_name")
        if not element_ext_name:
            raise ValueError(
                f"ListLogicalTypeFactory.reconstruct_from_arrow: missing 'element_ext_name' "
                f"in metadata for {arrow_extension_name!r}. metadata={metadata!r}."
            )

        element_meta_str = metadata.get("element_ext_metadata")
        element_meta_bytes = (
            element_meta_str.encode("utf-8") if element_meta_str else b""
        )
        # Element storage is the value type of the outer list storage.
        element_storage_type = storage_type.value_type

        # Recursively register the element logical type (handles nesting).
        element_ext_arrow_type = converter.register_arrow_extension(
            element_ext_name, element_meta_bytes, element_storage_type
        )

        # Recover element Python type from the now-registered extension type.
        element_python_type = converter.arrow_type_to_python_type(element_ext_arrow_type)

        is_set = metadata.get("category") == SET_CATEGORY
        logger.debug(
            "ListLogicalTypeFactory: reconstructed %r from Arrow (is_set=%s)",
            arrow_extension_name,
            is_set,
        )
        return ListLogicalType(element_python_type, element_ext_arrow_type, is_set=is_set)
