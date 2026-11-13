"""Fixture classes for the deferred-registration tests (ITL-871).

``Thing`` plays the role of an optional library's class (such as ``torch.Tensor``);
``LogicalThing`` and ``ThingHandler`` play the logical type and the semantic
handler that are only loaded on first use. Their ``instances`` counters show
when the registries instantiate them.
"""

from __future__ import annotations

from typing import Any, ClassVar

import polars as pl
import pyarrow as pa

from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.registry import (
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.types import ContentHash

THING_EXTENSION_NAME = "orcapod_test.deferred_thing"


class Thing:
    """A value type registered lazily."""

    def __init__(self, text: str) -> None:
        """Hold the text."""
        self.text = text


class SubThing(Thing):
    """A subclass, resolved through the MRO."""


class LogicalThing(BaseLogicalType):
    """Logical type for ``Thing`` over ``large_string``."""

    instances: ClassVar[int] = 0
    _arrow_ext_class = make_arrow_extension_type(
        THING_EXTENSION_NAME, pa.large_string()
    )
    _polars_ext_class = make_polars_extension_type(
        THING_EXTENSION_NAME, pa.large_string()
    )
    logical_type_name: str = THING_EXTENSION_NAME
    python_type: type = Thing

    def __init__(self) -> None:
        """Count the instance."""
        LogicalThing.instances += 1

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type over ``large_string``."""
        return self._arrow_ext_class()  # pyright: ignore[reportCallIssue]  # the generated class takes no arguments

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type over ``large_string``."""
        return self._polars_ext_class()  # pyright: ignore[reportCallIssue]  # the generated class takes no arguments

    def python_to_storage(self, value: Any, converter: Any = None) -> str:
        """Store a ``Thing`` as its text."""
        return value.text

    def storage_to_python(self, storage_value: Any, converter: Any = None) -> Thing:
        """Rebuild a ``Thing`` from its text."""
        return Thing(storage_value)


class ThingHandler:
    """Semantic handler for ``Thing``."""

    instances: ClassVar[int] = 0

    def __init__(self) -> None:
        """Count the instance."""
        ThingHandler.instances += 1

    def handle(self, obj: Any, hasher: Any) -> ContentHash:
        """Hash a ``Thing`` by its text."""
        return ContentHash(method="thing_v1", digest=obj.text.encode())


SLOW_EXTENSION_NAME = "orcapod_test.deferred_slow_thing"


class SlowThing:
    """A value type whose logical type is slow to construct (widens race windows)."""


class LogicalSlowThing(LogicalThing):
    """Logical type for ``SlowThing``; its constructor sleeps."""

    _arrow_ext_class = make_arrow_extension_type(SLOW_EXTENSION_NAME, pa.large_string())
    _polars_ext_class = make_polars_extension_type(
        SLOW_EXTENSION_NAME, pa.large_string()
    )
    logical_type_name: str = SLOW_EXTENSION_NAME
    python_type: type = SlowThing

    def __init__(self) -> None:
        """Sleep, so that concurrent first lookups overlap."""
        import time

        time.sleep(0.05)
        super().__init__()


SUB_EXTENSION_NAME = "orcapod_test.deferred_sub_thing"


class LogicalSubThing(LogicalThing):
    """Logical type for ``SubThing``, deferred while ``Thing``'s is registered."""

    _arrow_ext_class = make_arrow_extension_type(SUB_EXTENSION_NAME, pa.large_string())
    _polars_ext_class = make_polars_extension_type(
        SUB_EXTENSION_NAME, pa.large_string()
    )
    logical_type_name: str = SUB_EXTENSION_NAME
    python_type: type = SubThing


EXACT_EXTENSION_NAME = "orcapod_test.deferred_exact_thing"


class ExactThing:
    """A value type whose logical type matches the class exactly, never a subclass."""


class SubExactThing(ExactThing):
    """A subclass: it must not resolve to ``LogicalExactThing``."""


class LogicalExactThing(LogicalThing):
    """Deferred logical type for ``ExactThing`` only."""

    _arrow_ext_class = make_arrow_extension_type(
        EXACT_EXTENSION_NAME, pa.large_string()
    )
    _polars_ext_class = make_polars_extension_type(
        EXACT_EXTENSION_NAME, pa.large_string()
    )
    logical_type_name: str = EXACT_EXTENSION_NAME
    python_type: type = ExactThing
    exact_python_type_only: bool = True
