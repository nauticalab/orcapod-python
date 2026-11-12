"""StructuralLogicalType and StructuralLogicalTypeFactory (ITL-972).

A structural class is made by ``orcapod.dataclass``. It is identified by its declared
name and its fields in declared order, not by the class object: two structural classes
with the same name and fields are one type, and a stored column can be read with no
class defined, because the Arrow extension metadata holds the whole identity record.

Category tag: ``"orcapod.structural"``
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import orcapod.logical_types.user_class_identity as uci
from orcapod.logical_types.user_class_logical_type import (
    UserClassLogicalType,
    UserClassLogicalTypeFactory,
)

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol

#: Category tag embedded in Arrow extension metadata. Used as the factory dispatch key.
STRUCTURAL_CATEGORY = "orcapod.structural"


class StructuralLogicalType(UserClassLogicalType):
    """Logical type binding a structural class to its Arrow extension type."""

    category = STRUCTURAL_CATEGORY

    instance_requirement = (
        "a structural column stores instances of an orcapod.dataclass with the same "
        "name and fields. Build one explicitly, for example "
        "<StructuralClass>.from_instance(value)."
    )

    def accepts_instance(self, value: Any, converter: TypeConverterProtocol) -> bool:
        """Return True if ``value`` is an instance of a structural class of this type.

        Args:
            value: The value offered.
            converter: The active converter.

        Returns:
            Whether the value's class is this type's class, or another structural
            class with the same identity record.
        """
        cls = type(value)
        if isinstance(value, self._python_type):
            return True
        if not uci.is_structural(cls):
            return False
        return uci.class_record(cls, converter) == self._identity_record


class StructuralLogicalTypeFactory(UserClassLogicalTypeFactory):
    """Factory for structural class logical types.

    On the write path a structural class is a dataclass, so the dataclass factory,
    which is registered for every class, hands it to this factory. On the read path
    the registry dispatches on the category.

    Register with::

        converter.register_logical_type_factory(
            StructuralLogicalTypeFactory(), category="orcapod.structural"
        )
    """

    kind = uci.KIND_STRUCTURAL
    category = STRUCTURAL_CATEGORY
    logical_type_class = StructuralLogicalType
