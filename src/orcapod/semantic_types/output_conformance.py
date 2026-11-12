"""Function-output conformance (ITL-895, ITL-901).

the returned value must be an instance of the declared type, or an exact numeric
equivalent; a subclass stands in for its base and is stored as the base.

Logical types retain ownership of their compatibility rules. The caller adds function
and output context to a mismatch; this module never formats returned values.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import types
import typing
from collections.abc import Mapping
from dataclasses import dataclass

from orcapod.errors import type_text
from orcapod.logical_types import user_class_identity
from orcapod.logical_types.user_class_logical_type import UserClassLogicalType

if typing.TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol


class Treatment(enum.Enum):
    """Whether a classified near miss is accepted or rejected."""

    ACCEPT = "accept"
    REJECT = "reject"


class NearMiss(enum.Enum):
    """The explicit exceptions and rejected near misses of the output contract."""

    NUMPY_SCALAR_SAME_KIND = "numpy_scalar_same_kind"
    INT_FOR_FLOAT = "int_for_float"
    BOOL_FOR_FLOAT = "bool_for_float"
    NONE_FOR_NON_OPTIONAL = "none_for_non_optional"
    ITERABLE_FOR_LIST = "iterable_for_list"
    FLOAT_NARROWING = "float_narrowing"


OUTPUT_CONFORMANCE: Mapping[NearMiss, Treatment] = types.MappingProxyType(
    {
        case: Treatment.ACCEPT
        if case is NearMiss.NUMPY_SCALAR_SAME_KIND
        else Treatment.REJECT
        for case in NearMiss
    }
)


@dataclass(frozen=True)
class Nonconformance:
    """A mismatch, with structural location and an explicit-conversion hint."""

    path: str
    expected: object
    value: object
    hint: str | None = None

    def describe(self) -> str | None:
        """Describe nested types without exposing any returned content."""
        if not self.path:
            return None
        return f"{self.path} is {type_text(type(self.value))}, expected {type_text(self.expected)}"


def _at(mismatch: Nonconformance | None, path: str) -> Nonconformance | None:
    """Attach the structural location of a nested mismatch."""
    return (
        dataclasses.replace(mismatch, path=path + mismatch.path) if mismatch else None
    )


def _numpy_scalar(value: object) -> tuple[str, int] | None:
    """Return a NumPy scalar's dtype kind and byte width without importing NumPy eagerly."""
    if not type(value).__module__.startswith("numpy"):
        return None
    import numpy as np

    if isinstance(value, np.generic):
        return value.dtype.kind, value.dtype.itemsize
    return None


def _numpy_type(declared: object) -> tuple[str, int] | None:
    """Return the kind and byte width of a declared NumPy scalar type."""
    if not isinstance(declared, type) or not declared.__module__.startswith("numpy"):
        return None
    import numpy as np

    if issubclass(declared, np.generic):
        dtype = np.dtype(declared)
        return dtype.kind, dtype.itemsize
    return None


def _declared_ndarray_type(declared: object) -> type | None:
    """Resolve an ndarray annotation or parameterized alias without importing NumPy."""
    candidates = (
        typing.get_args(declared)
        if typing.get_origin(declared) in (typing.Union, types.UnionType)
        else (declared,)
    )
    for annotation in candidates:
        candidate = typing.get_origin(annotation) or annotation
        if isinstance(candidate, type) and any(
            base.__module__ == "numpy" and base.__qualname__ == "ndarray"
            for base in candidate.__mro__
        ):
            return candidate
    return None


class _Checker:
    """Apply the output policy recursively using the active logical-type converter."""

    def __init__(self, converter: TypeConverterProtocol) -> None:
        """Use the active converter for user-class compatibility."""
        self.converter = converter

    def near(
        self, case: NearMiss, value: object, declared: object
    ) -> Nonconformance | None:
        """Apply the single decision table and provide an explicit remedy."""
        if OUTPUT_CONFORMANCE[case] is Treatment.ACCEPT:
            return None
        if case in (NearMiss.INT_FOR_FLOAT, NearMiss.BOOL_FOR_FLOAT):
            # A fixed example avoids disclosing the returned numeric value.
            hint = (
                "Return a float instead: 0.0 or float(value)"
                if declared is float
                else f"Return {type_text(declared)}(value)"
            )
        elif case is NearMiss.NONE_FOR_NON_OPTIONAL:
            hint = f"Return a {type_text(declared)}, or declare the output {type_text(declared)} | None"
        elif case is NearMiss.ITERABLE_FOR_LIST:
            hint = "Return a list instead: list(value)"
            if isinstance(value, (set, frozenset)):
                hint = "Return a list in a fixed order instead, e.g. sorted(value)"
        else:
            hint = f"Return {type_text(declared)}(value) instead"
        return Nonconformance("", declared, value, hint)

    def check(self, value: object, declared: object) -> Nonconformance | None:
        """Return the first mismatch; unknown logical types are checked by conversion."""
        origin = typing.get_origin(declared)
        args = typing.get_args(declared)
        if origin in (typing.Union, types.UnionType):
            non_none = [a for a in args if a is not type(None)]
            if value is None and len(non_none) < len(args):
                return None
            if len(non_none) == 1:
                return self.check(value, non_none[0])
            return None  # unsupported complex unions are rejected by the converter
        if origin is typing.Literal:
            # A Literal names exact values: a member matches by type and value, so
            # True is not 1 and no numeric exception applies.
            if any(type(value) is type(member) and value == member for member in args):
                return None
            return Nonconformance(
                "", declared, value, "Return one of the declared literal values"
            )
        if value is None:
            return self.near(NearMiss.NONE_FOR_NON_OPTIONAL, value, declared)
        array_type = _declared_ndarray_type(declared)
        if array_type is not None:
            return (
                None
                if isinstance(value, array_type)
                else Nonconformance(
                    "",
                    declared,
                    value,
                    "Return an array, for example with np.asarray(...)",
                )
            )
        numpy_type = _numpy_type(declared)
        kind = (
            numpy_type[0]
            if numpy_type
            else "b"
            if declared is bool
            else "i"
            if declared is int
            else "f"
            if declared is float
            else None
        )
        if kind in ("b", "i", "u", "f"):
            return self.check_number(value, declared, kind, numpy_type)
        if declared in (str, bytes, dt.date, dt.datetime, dt.time, dt.timedelta):
            return (
                None
                if isinstance(declared, type) and isinstance(value, declared)
                else Nonconformance("", declared, value)
            )
        if origin is list:
            if not isinstance(value, list):
                return self.near(NearMiss.ITERABLE_FOR_LIST, value, declared)
            for index, item in enumerate(value):
                mismatch = self.check(item, args[0] if args else object)
                if mismatch:
                    return _at(mismatch, f"[{index}]")
            return None
        if origin is dict:
            if not isinstance(value, dict):
                return Nonconformance(
                    "", declared, value, "Return a dict instead: dict(value)"
                )
            key_type, value_type = args if len(args) == 2 else (object, object)
            for key, item in value.items():
                mismatch = _at(self.check(key, key_type), ".keys()") or _at(
                    self.check(item, value_type), "[*]"
                )
                if mismatch:
                    return mismatch
            return None
        logical_type = self.converter.get_logical_type_for_python_type(declared)
        if isinstance(logical_type, UserClassLogicalType):
            if not logical_type.accepts_instance(value, self.converter):
                return Nonconformance(
                    "", declared, value, logical_type.instance_requirement
                )
            cls = logical_type.python_type
            hints = user_class_identity.type_hints(cls)
            for name in user_class_identity.stored_field_names(cls):
                mismatch = self.check(getattr(value, name), hints[name])
                if mismatch:
                    return _at(mismatch, f".{name}")
        return None

    def check_number(
        self,
        value: object,
        declared: object,
        kind: str,
        numpy_type: tuple[str, int] | None,
    ) -> Nonconformance | None:
        """Require the same numeric kind and prevent overflow or floating narrowing."""
        if isinstance(declared, type) and isinstance(value, declared):
            return None  # Python subclassing is the primary acceptance rule.
        scalar = _numpy_scalar(value)
        actual_kind = (
            scalar[0]
            if scalar
            else (
                "b"
                if isinstance(value, bool)
                else "i"
                if isinstance(value, int)
                else "f"
                if isinstance(value, float)
                else None
            )
        )
        if kind in ("i", "u"):
            if actual_kind not in ("i", "u"):
                return Nonconformance(
                    "",
                    declared,
                    value,
                    "Return an int instead: round(value), or int(value) to truncate",
                )
            if numpy_type is not None:
                import numpy as np

                limits = np.iinfo(typing.cast(type, declared))
                number = int(typing.cast(typing.SupportsInt, value))
                if not limits.min <= number <= limits.max:
                    return Nonconformance(
                        "",
                        declared,
                        value,
                        f"Return a value within the range of {type_text(declared)}",
                    )
        elif kind == "b":
            if actual_kind != "b":
                return Nonconformance(
                    "", declared, value, "Return bool(value) explicitly"
                )
        elif kind == "f":
            if actual_kind in ("i", "u", "b"):
                case = (
                    NearMiss.BOOL_FOR_FLOAT
                    if actual_kind == "b"
                    else NearMiss.INT_FOR_FLOAT
                )
                return self.near(case, value, declared)
            if actual_kind != "f":
                return Nonconformance("", declared, value)
            declared_width = numpy_type[1] if numpy_type else 8
            actual_width = scalar[1] if scalar else 8
            if actual_width > declared_width:
                return self.near(NearMiss.FLOAT_NARROWING, value, declared)
        if type(value) is declared or (numpy_type is None and scalar is None):
            return None
        return self.near(NearMiss.NUMPY_SCALAR_SAME_KIND, value, declared)


def check_conformance(
    value: object, declared: object, converter: TypeConverterProtocol
) -> Nonconformance | None:
    """Check one returned value using its declared type and active logical-type rules."""
    return _Checker(converter).check(value, declared)


def normalize_output_numbers(value: object, declared: object) -> object:
    """Normalize already-validated numeric values without rebuilding user-class objects.

    This performs no compatibility decisions: check_conformance and declared-schema
    storage validation must succeed first. Containers are copied, never mutated.
    """
    origin = typing.get_origin(declared)
    args = typing.get_args(declared)
    if value is None:
        return None
    if origin in (typing.Union, types.UnionType):
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return normalize_output_numbers(value, non_none[0])
    numpy_type = _numpy_type(declared)
    kind = numpy_type[0] if numpy_type else None
    if isinstance(declared, type) and isinstance(value, declared):
        return value  # Preserve subclass objects; the declared converter owns storage.
    if declared is bool or kind == "b":
        return bool(value)
    if declared is int or kind in ("i", "u"):
        return int(typing.cast(typing.SupportsInt, value))
    if declared is float or kind == "f":
        return float(typing.cast(typing.SupportsFloat, value))
    if origin is list and isinstance(value, list):
        return [normalize_output_numbers(item, args[0]) for item in value]
    if origin is dict and isinstance(value, dict):
        return {
            normalize_output_numbers(key, args[0]): normalize_output_numbers(
                item, args[1]
            )
            for key, item in value.items()
        }
    return value
