"""PydanticLogicalType and PydanticLogicalTypeFactory.

Pydantic v2 models are user classes: their logical type name and Arrow extension name
are the class's identity string (name, kind ``"pydantic"`` and field types). A pydantic
model and a dataclass with the same name and fields are different types. The shared
write and read paths live in ``user_class_logical_type``.

Category tag: ``"orcapod.pydantic"``
"""

from __future__ import annotations

import orcapod.logical_types.user_class_identity as uci
from orcapod.logical_types.user_class_logical_type import (
    UserClassLogicalType,
    UserClassLogicalTypeFactory,
)

#: Category tag embedded in Arrow extension metadata. Used as the factory dispatch key.
PYDANTIC_CATEGORY = "orcapod.pydantic"


class PydanticLogicalType(UserClassLogicalType):
    """Logical type binding a pydantic ``BaseModel`` subclass to its Arrow extension type.

    ``storage_to_python`` constructs the model, so pydantic validation runs on read.
    """

    category = PYDANTIC_CATEGORY


class PydanticLogicalTypeFactory(UserClassLogicalTypeFactory):
    """Factory for pydantic model logical types.

    Stored fields are the model's ``model_fields``; computed fields and private
    attributes are not stored.

    Register with::

        from pydantic import BaseModel

        converter.register_logical_type_factory(
            PydanticLogicalTypeFactory(),
            category="orcapod.pydantic",
            python_bases=[BaseModel],
        )
    """

    kind = uci.KIND_PYDANTIC
    category = PYDANTIC_CATEGORY
    logical_type_class = PydanticLogicalType
