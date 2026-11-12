"""Structural classes: rebuilding from stored metadata, equality and conversion (ITL-972).

A structural class is made by ``orcapod.dataclass``. This module holds what such a class
needs beyond its identity (``user_class_identity``): turning a stored identity record
back into a class when none is defined, the ``__eq__`` that compares across classes of
one type, and ``from_instance``, the explicit conversion.
"""

from __future__ import annotations

import ast
import dataclasses
import functools
import operator
import re
import types
import typing
from typing import TYPE_CHECKING, Any, Literal

import orcapod.logical_types.user_class_identity as uci

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol

_UNIONS = (typing.Union, types.UnionType)

# Structural classes built from stored metadata, by identity string.
_synthesized: dict[str, type] = {}

_SCALARS: dict[str, Any] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "bytes": bytes,
    "None": type(None),
}
_GENERIC_ORIGINS: dict[str, Any] = {
    "list": list,
    "set": set,
    "frozenset": frozenset,
    "tuple": tuple,
    "dict": dict,
}


# ---------------------------------------------------------------------------
# Rebuilding annotations and structural classes from tokens
# ---------------------------------------------------------------------------


def annotation_from_token(token: Any, converter: TypeConverterProtocol) -> Any:
    """Return the Python annotation a type token describes.

    Args:
        token: A type token, as ``type_token`` builds it.
        converter: The type converter of the reading context.

    Returns:
        The annotation.

    Raises:
        ImportError: If a class named by the token cannot be imported, or a logical
            type named by it is not registered in the reading context.
        ValueError: If the token is malformed, or an exact class it names is defined
            with other fields than the token records.
    """
    from orcapod.logical_types.type_utils import _walk_fqcn

    if isinstance(token, str):
        if token in _SCALARS:
            return _SCALARS[token]
        if token == uci.ELLIPSIS_TOKEN:
            return Ellipsis
        return _walk_fqcn(token)
    if not isinstance(token, dict):
        raise ValueError(f"Malformed type token: {token!r}.")
    if "logical" in token:
        name = token["logical"].split(";", 1)[0]
        lt = converter.get_logical_type_by_arrow_extension_name(name)
        if lt is None:
            raise ImportError(
                f"The logical type {name!r} is not registered in this data context."
            )
        from orcapod.logical_types.identifier import logical_type_identifier

        if logical_type_identifier(lt).text != token["logical"]:
            raise ValueError(
                f"The logical type {name!r} registered in this data context has the "
                f"identifier {logical_type_identifier(lt).text!r}, not the stored "
                f"{token['logical']!r}."
            )
        return lt.python_type
    if "generic" in token:
        origin = _GENERIC_ORIGINS.get(token["generic"]) or _walk_fqcn(token["generic"])
        args = tuple(annotation_from_token(arg, converter) for arg in token["args"])
        return origin[args] if args else origin
    if "union" in token:
        members = [
            annotation_from_token(member, converter) for member in token["union"]
        ]
        return functools.reduce(operator.or_, members)
    if "literal" in token:
        return Literal[tuple(ast.literal_eval(text) for text in token["literal"])]  # pyright: ignore[reportInvalidTypeForm]  # built at run time from stored values
    if "class" in token:
        return class_for_record(token["class"], converter)
    raise ValueError(f"Malformed type token: {token!r}.")


def class_for_record(record: dict[str, Any], converter: TypeConverterProtocol) -> type:
    """Return the class a stored identity record describes.

    A structural record gives the most recently defined structural class with that
    record, or a class synthesized from the record when none is defined. An exact
    record gives the class its name refers to, which must have the stored record.

    Args:
        record: A stored identity record.
        converter: The type converter of the reading context.

    Returns:
        The class.

    Raises:
        ImportError: If an exact class cannot be found by its name.
        ValueError: If the record is malformed, or the exact class found is described
            by another record.
    """
    kind, name = record.get("kind"), record.get("name")
    if not isinstance(name, str) or kind not in (*uci.EXACT_KINDS, uci.KIND_STRUCTURAL):
        raise ValueError(f"Malformed identity record: {record!r}.")
    if kind == uci.KIND_STRUCTURAL:
        if not isinstance(record.get("fields"), list):
            raise ValueError(f"Malformed identity record: {record!r}.")
        for cls in uci.structural_classes(name):
            try:
                if uci.class_record(cls, converter) == record:
                    return cls
            except (TypeError, ValueError):
                continue
        return synthesize_structural_class(record, converter)
    cls = uci.find_exact_class(name)
    if uci.class_kind(cls) != kind:
        raise ImportError(
            f"Cannot find the {kind} {name!r} to read stored values of type "
            f"{uci.describe_record(record)}: no such class is defined at that module path."
        )
    current = uci.class_record(cls, converter)
    if current != record:
        raise ValueError(
            f"Stored values of {uci.describe_record(record)} do not match the class "
            f"defined now: {uci.describe_record(current)}."
        )
    return cls


def synthesize_structural_class(
    record: dict[str, Any], converter: TypeConverterProtocol
) -> type:
    """Build a structural class from a stored record when no class is defined for it.

    The class is a dataclass with the stored fields, named after the last component of
    the stored name, in the module ``orcapod.synthesized``. It is built once per
    identity in a process.

    Args:
        record: A stored structural identity record.
        converter: The type converter of the reading context.

    Returns:
        A structural class whose record equals ``record``.

    Raises:
        ValueError: If the record cannot be turned into a class with the same record.
    """
    identity = uci.identity_string(record)
    cached = _synthesized.get(identity)
    if cached is not None:
        return cached
    name = record["name"]
    python_name = re.sub(r"\W", "_", name.rsplit(".", 1)[-1])
    if not python_name.isidentifier():
        python_name = f"_{python_name}"
    fields = [
        (field, annotation_from_token(token, converter))
        for field, token in record["fields"]
    ]
    # Hashable (by field values, as ``==`` compares): the class may have to rebuild a
    # ``set`` of its instances, and the stored record does not say whether the class
    # that wrote them was frozen.
    cls = dataclasses.make_dataclass(python_name, fields, unsafe_hash=True)
    cls.__module__ = uci.SYNTHESIZED_MODULE
    cls.__doc__ = f"Structural class {name!r}, built from stored metadata."
    finish_structural_class(cls, name)
    if uci.class_record(cls, converter) != record:
        raise ValueError(
            f"Cannot rebuild the structural class {uci.describe_record(record)} from its "
            f"stored record."
        )
    _synthesized[identity] = cls
    return cls


def finish_structural_class(
    cls: type, name: str, *, structural_equality: bool = True
) -> None:
    """Turn a dataclass that was created for this purpose into a structural class.

    Marks the class with its name and adds ``from_instance`` and, unless the class
    opted out of generated equality, the ``__eq__`` that compares across classes of
    one type. Used by ``orcapod.dataclass`` and for classes built from stored metadata.

    Args:
        cls: The dataclass just created.
        name: The validated name of the type.
        structural_equality: Whether to install ``structural_eq`` as ``__eq__``.
    """
    uci.make_structural(cls, name)
    if structural_equality:
        setattr(cls, "__eq__", structural_eq)
    setattr(cls, "from_instance", classmethod(_from_instance))


# ---------------------------------------------------------------------------
# Equality across classes of one type
# ---------------------------------------------------------------------------

_MISSING = object()


def structural_eq(self: Any, other: Any) -> Any:
    """Compare two instances of structural classes of the same type by field values.

    Installed as ``__eq__`` by ``orcapod.dataclass``. Instances of the same class
    and instances of two structural classes with the same name and fields (a class
    defined again, a class built from stored metadata) are equal when all their stored
    fields are equal. Anything else is not comparable.

    Args:
        self: An instance of a structural class.
        other: Any object.

    Returns:
        ``True`` or ``False`` for an instance of a class of the same type, else
        ``NotImplemented``.
    """
    mine, theirs = self.__class__, other.__class__
    if theirs is not mine and not (
        uci.is_structural(theirs) and uci.same_user_type(mine, theirs)
    ):
        return NotImplemented
    # Every stored field, and only those: ``orcapod.dataclass`` refuses a per-field
    # ``compare=`` or ``hash=`` choice, so every class of the type compares alike.
    names = uci.stored_field_names(mine)
    return tuple(getattr(self, name) for name in names) == tuple(
        getattr(other, name, _MISSING) for name in names
    )


# ---------------------------------------------------------------------------
# Conversion: an instance of the class from an instance of another dataclass
# ---------------------------------------------------------------------------


def _from_instance(cls: type, other: Any) -> Any:
    """Build an instance of this class from an instance of another dataclass.

    This is the only conversion between classes, and Orcapod never calls it for you.

    Args:
        other: An instance of an ordinary dataclass or an ``orcapod.dataclass`` with
            exactly the same field names and the same declared field types.

    Returns:
        A new instance of this class with ``other``'s field values. Fields typed by
        an ``orcapod.dataclass`` are converted the same way.

    Raises:
        TypeError: If ``other`` is not a dataclass instance, or its fields differ
            from this class's fields.
    """
    return cls(**converted_fields(cls, other))


def converted_fields(cls: type, other: Any) -> dict[str, Any]:
    """Return the keyword arguments that build a ``cls`` from ``other``.

    This is the acceptance rule of ``from_instance``: ``other`` is a dataclass instance whose stored fields have exactly the names of
    ``cls``'s fields, and whose declared field types agree with ``cls``'s.

    Args:
        cls: A structural class.
        other: The instance to convert.

    Returns:
        Field name to value, values of structural field types converted.

    Raises:
        TypeError: If ``other`` is not a dataclass instance, a field is missing or
            extra, or a field's declared type differs.
    """
    source = type(other)
    if not dataclasses.is_dataclass(other) or isinstance(other, type):
        raise TypeError(
            f"{cls.__qualname__} can be built from an instance of a dataclass, not "
            f"from {other!r}."
        )
    wanted = uci.stored_field_names(cls)
    offered = uci.stored_field_names(source)
    if set(wanted) != set(offered):
        missing = sorted(set(wanted) - set(offered))
        extra = sorted(set(offered) - set(wanted))
        raise TypeError(
            f"Cannot build {cls.__qualname__} from an instance of "
            f"{source.__qualname__}: the fields differ (missing: {missing}, not in "
            f"{cls.__qualname__}: {extra}). The two classes must have the same fields."
        )
    wanted_hints = uci.type_hints(cls)
    offered_hints = uci.type_hints(source)
    values: dict[str, Any] = {}
    for field in wanted:
        target = wanted_hints.get(field, Any)
        if not _convertible(offered_hints.get(field, Any), target):
            raise TypeError(
                f"Cannot build {cls.__qualname__} from an instance of "
                f"{source.__qualname__}: field {field!r} is declared "
                f"{offered_hints.get(field, Any)!r} there and {target!r} here."
            )
        values[field] = _converted_value(getattr(other, field), target)
    return values


def _convertible(offered: Any, wanted: Any) -> bool:
    """Return True if a field declared ``offered`` can fill a field declared ``wanted``.

    The declarations are equal, or differ only where ``wanted`` names a structural
    class and ``offered`` a dataclass (converted by ``from_instance``, which checks
    its fields in turn). Generics are compared by origin and arguments, union members
    in any order.
    """
    if offered == wanted:
        return True
    if isinstance(offered, type) and isinstance(wanted, type):
        return uci.is_structural(wanted) and dataclasses.is_dataclass(offered)
    origin_offered, origin_wanted = (
        typing.get_origin(offered),
        typing.get_origin(wanted),
    )
    if origin_offered is None or origin_wanted is None:
        return False
    args_offered, args_wanted = typing.get_args(offered), typing.get_args(wanted)
    if origin_offered in _UNIONS and origin_wanted in _UNIONS:
        return all(
            any(_convertible(x, y) for y in args_wanted) for x in args_offered
        ) and all(any(_convertible(x, y) for x in args_offered) for y in args_wanted)
    if origin_offered != origin_wanted or len(args_offered) != len(args_wanted):
        return False
    return all(_convertible(x, y) for x, y in zip(args_offered, args_wanted))


def _converted_union_value(value: Any, members: tuple[Any, ...]) -> Any:
    """Return a field value converted to the union member it belongs to.

    A value that already is an instance of a member class is kept. Otherwise a
    dataclass instance goes to the first structural member it can be converted to
    (the same field names with the same declared types), and a container to the first
    member with the container's origin whose conversion succeeds. A value no member
    claims (an ``int`` in ``int | P``) is returned as it is.

    Args:
        value: The field value, not ``None``.
        members: The members of the union annotation.

    Returns:
        The value to store in the new instance.

    Raises:
        TypeError: If members claim the value (structural members for a dataclass
            instance, container members of the value's kind) and it can be converted
            to none of them.
    """
    cls = type(value)
    classes = [
        m for m in members if isinstance(m, type) and typing.get_origin(m) is None
    ]
    if cls in classes:
        return value
    if dataclasses.is_dataclass(value):
        structural = [m for m in classes if uci.is_structural(m)]
        for member in structural:
            if _fields_agree(cls, member):
                return _converted_value(value, member)
        if structural:
            names = ", ".join(m.__qualname__ for m in structural)
            raise TypeError(
                f"An instance of {cls.__qualname__} cannot be converted to any member "
                f"of the union ({names}): no member has the same fields with the "
                f"same types."
            )
        return value
    failure: TypeError | None = None
    for member in members:
        if typing.get_origin(member) is cls:
            try:
                return _converted_value(value, member)
            except TypeError as exc:
                failure = exc
    if failure is not None:
        raise TypeError(
            f"A {cls.__name__} value cannot be converted to any member of the union "
            f"that is a {cls.__name__}: {failure}"
        ) from failure
    return value


def _fields_agree(source: type, target: type) -> bool:
    """Return True if an instance of ``source`` can be converted to ``target``.

    The two dataclasses have the same stored field names, and each field's declared
    types agree (``_convertible``): the acceptance rule of ``from_instance``.
    """
    wanted = uci.stored_field_names(target)
    if set(wanted) != set(uci.stored_field_names(source)):
        return False
    wanted_hints = uci.type_hints(target)
    offered_hints = uci.type_hints(source)
    return all(
        _convertible(offered_hints.get(field, Any), wanted_hints.get(field, Any))
        for field in wanted
    )


def _mentions_structural(annotation: Any) -> bool:
    """Return True if an annotation names a structural class at any depth."""
    if isinstance(annotation, type) and typing.get_origin(annotation) is None:
        return uci.is_structural(annotation)
    return any(_mentions_structural(arg) for arg in typing.get_args(annotation))


def _converted_value(value: Any, annotation: Any) -> Any:
    """Return a field value converted to the structural classes its annotation names.

    Used by ``from_instance`` for the fields of the instance it builds. Where the
    annotation names a structural class and the value is an instance of another
    dataclass, the value is replaced by ``from_instance`` of the annotated class.
    Lists, sets, tuples and dict values are rebuilt only when their annotation names a
    structural class; every other value is returned as it is.

    Args:
        value: The field value of the instance being converted.
        annotation: The annotation of the field receiving it.

    Returns:
        The value to store in the new instance.

    Raises:
        TypeError: If a value cannot be converted to the annotated structural class.
    """
    if value is None or not _mentions_structural(annotation):
        return value
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is None:
        if type(value) is annotation:
            return value
        return annotation.from_instance(value)
    if origin in _UNIONS:
        return _converted_union_value(value, args)
    if origin in (list, set, frozenset) and len(args) == 1:
        return origin(_converted_value(item, args[0]) for item in value)
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_converted_value(item, args[0]) for item in value)
        if len(args) == len(value):
            return tuple(_converted_value(item, arg) for item, arg in zip(value, args))
        return value
    if origin is dict and len(args) == 2:
        return {key: _converted_value(item, args[1]) for key, item in value.items()}
    return value
