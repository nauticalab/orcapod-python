"""DataclassLogicalType and DataclassLogicalTypeFactory.

An ordinary dataclass is an exact class: it is identified by its module path, qualified
name and fields, and is one type only with itself (ITL-972). The shared write and read
paths live in ``user_class_logical_type``. A class made by ``orcapod.dataclass`` is a
dataclass too; this factory hands it to ``StructuralLogicalTypeFactory``.

Category tag: ``"orcapod.dataclass"``
"""

from __future__ import annotations


import orcapod.logical_types.user_class_identity as uci
from typing import TYPE_CHECKING

from orcapod.logical_types.user_class_logical_type import (
    UserClassLogicalType,
    UserClassLogicalTypeFactory,
)

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol

#: Category tag embedded in Arrow extension metadata. Used as the factory dispatch key.
DATACLASS_CATEGORY = "orcapod.dataclass"


class DataclassLogicalType(UserClassLogicalType):
    """Logical type binding a dataclass to its Arrow extension type.

    Example:
        >>> lt = DataclassLogicalType(
        ...     "Point@76d961e4...", Point,
        ...     pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())]),
        ...     [("x", int), ("y", int)],
        ... )
        >>> lt.python_to_storage(Point(1, 2), converter)
        {"x": 1, "y": 2}
    """

    category = DATACLASS_CATEGORY


class DataclassLogicalTypeFactory(UserClassLogicalTypeFactory):
    """Factory for dataclass logical types.

    Stored fields are the dataclass fields with ``init=True``.

    Register with::

        converter.register_logical_type_factory(
            DataclassLogicalTypeFactory(),
            category="orcapod.dataclass",
            python_bases=[object],
        )
    """

    kind = uci.KIND_DATACLASS
    category = DATACLASS_CATEGORY
    logical_type_class = DataclassLogicalType

    def supports_class(self, python_type: type) -> bool:
        """Return True for every dataclass, ordinary or structural."""
        return uci.class_kind(python_type) in (uci.KIND_DATACLASS, uci.KIND_STRUCTURAL)

    def create_for_python_type(
        self,
        python_type: type,
        converter: TypeConverterProtocol,
    ) -> UserClassLogicalType:
        """Build the logical type of a dataclass (write path).

        Args:
            python_type: An ordinary dataclass or a structural class.
            converter: The active converter for field-type resolution.

        Returns:
            A ``DataclassLogicalType``, or a ``StructuralLogicalType`` for a class made
            by ``orcapod.dataclass``.
        """
        if uci.is_structural(python_type):
            from orcapod.logical_types.structural_logical_type_factory import (
                StructuralLogicalTypeFactory,
            )

            return StructuralLogicalTypeFactory().create_for_python_type(
                python_type, converter
            )
        return super().create_for_python_type(python_type, converter)
