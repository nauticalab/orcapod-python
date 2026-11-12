"""Identity of user classes (dataclasses and pydantic models).

Orcapod knows three kinds of user class (ITL-972):

- ``"dataclass"``: an ordinary dataclass. An **exact** class: two classes are one type
  only when they are the same class object. Named by module path and qualified name.
- ``"pydantic"``: a ``pydantic.BaseModel`` subclass. Exact as well.
- ``"structural"``: a class made by ``orcapod.dataclass``. Named by its declared name
  (default: the class name); two structural classes with the same name and the same
  fields in the same order are one type, whatever class object they are.

A class is described by an *identity record*, a JSON object with ``kind``, ``name`` and
``fields`` (``[name, type token]`` pairs in declared order; order is part of the
identity). A type token is JSON as well and describes a field type completely: a class
typed by another user class embeds that class's whole record, so a structural class can
be rebuilt from its record with no class importable.

The *identity string* ``f"{name}@{sha256 of the record}"`` is the class's logical type
name and Arrow extension name; ``identity_string`` is the only place that defines its
format. Arrow's and Polars' extension registries are keyed by name, so the digest keeps
two shapes of one name apart within a process.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import sys
import typing
import unicodedata
import weakref
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal

from orcapod.errors import RecursiveTypeError
from orcapod.utils.canonical_type_names import (
    canonical_module_name,
    canonical_type_name,
)

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol

KIND_DATACLASS = "dataclass"
KIND_PYDANTIC = "pydantic"
KIND_STRUCTURAL = "structural"

#: Kinds identified by the class object: two classes are one type only if they are
#: the same object.
EXACT_KINDS: frozenset[str] = frozenset({KIND_DATACLASS, KIND_PYDANTIC})

#: Modules whose classes are "defined in a notebook or script". ``__mp_main__`` is the
#: name a ``multiprocessing`` spawn child gives the main module.
MAIN_MODULES: tuple[str, ...] = ("__main__", "__mp_main__")

#: Prefix reserved for Orcapod's own types (``orcapod.path``, ``orcapod.uuid``, ...).
RESERVED_PREFIX = "orcapod."

#: Number of hex digits of the SHA-256 fingerprint in an identity string (the full digest).
FINGERPRINT_HEX_DIGITS = 64

#: Type token of ``...`` in ``tuple[T, ...]``.
ELLIPSIS_TOKEN = "..."

#: Class attribute that holds the declared name of a structural class. It is looked up
#: in the class's own namespace, so a subclass is structural only if it is decorated too.
STRUCTURAL_NAME_ATTRIBUTE = "__orcapod_structural_name__"

#: Module name given to structural classes built from stored metadata.
SYNTHESIZED_MODULE = "orcapod.synthesized"

_NAME_FORBIDDEN = re.compile(r"[@;\s]")

# The identity record each class object was last described with.
_record_of: weakref.WeakKeyDictionary[type, dict[str, Any]] = (
    weakref.WeakKeyDictionary()
)
# Structural classes defined in this process under each name, most recent last.
_structural_classes: dict[str, list[weakref.ref[type]]] = {}
# Resolved type hints per class.
_hints_of: weakref.WeakKeyDictionary[type, dict[str, Any]] = weakref.WeakKeyDictionary()


# ---------------------------------------------------------------------------
# Kinds and names
# ---------------------------------------------------------------------------


def structural_name(cls: Any) -> str | None:
    """Return the declared name of a structural class, or ``None`` for any other object.

    Args:
        cls: Any object.

    Returns:
        The name given by ``orcapod.dataclass`` to this very class.
    """
    if not isinstance(cls, type):
        return None
    return vars(cls).get(STRUCTURAL_NAME_ATTRIBUTE)


def class_kind(cls: Any) -> str | None:
    """Return the kind of a user class, or ``None`` if it is not one.

    Args:
        cls: Any object.

    Returns:
        ``"structural"`` for a class made by ``orcapod.dataclass``, ``"dataclass"`` for
        any other dataclass type (including pydantic dataclasses), ``"pydantic"`` for a
        ``pydantic.BaseModel`` subclass, else ``None``.
    """
    if not isinstance(cls, type):
        return None
    if dataclasses.is_dataclass(cls):
        if structural_name(cls) is not None:
            return KIND_STRUCTURAL
        return KIND_DATACLASS
    from pydantic import BaseModel

    if issubclass(cls, BaseModel):
        return KIND_PYDANTIC
    return None


def is_structural(cls: Any) -> bool:
    """Return True if ``cls`` is a class made by ``orcapod.dataclass``."""
    return class_kind(cls) == KIND_STRUCTURAL


def stored_field_names(cls: type) -> list[str]:
    """Return the names of the fields stored for a user class, in declared order.

    Args:
        cls: A dataclass or pydantic model class.

    Returns:
        The ``init=True`` fields of a dataclass, or the ``model_fields`` of a pydantic
        model. Computed fields and private attributes are not stored.

    Raises:
        TypeError: If ``cls`` is not a dataclass or pydantic model class.
    """
    kind = class_kind(cls)
    if kind in (KIND_DATACLASS, KIND_STRUCTURAL):
        return [f.name for f in dataclasses.fields(cls) if f.init]
    if kind == KIND_PYDANTIC:
        return list(cls.model_fields)  # pyright: ignore[reportAttributeAccessIssue]  # class_kind guarantees a BaseModel subclass
    raise TypeError(f"{cls!r} is not a dataclass or pydantic model class.")


def type_hints(cls: type) -> dict[str, Any]:
    """Return the resolved type hints of a class.

    Args:
        cls: A class.

    Returns:
        Field name to annotation, with ``Annotated`` extras stripped.

    Raises:
        ValueError: If the hints cannot be resolved.
    """
    cached = _hints_of.get(cls)
    if cached is not None:
        return cached
    try:
        hints = typing.get_type_hints(cls)
    except Exception as exc:
        raise ValueError(f"Cannot get type hints for {cls!r}: {exc}") from exc
    _hints_of[cls] = hints
    return hints


def validate_name(name: str) -> str:
    """Check a declared name of a structural class and return its normal form.

    Args:
        name: The proposed name.

    Returns:
        The name, NFC-normalized, so one name typed in two Unicode forms is one name.

    Raises:
        TypeError: If ``name`` is not a string.
        ValueError: If ``name`` is empty, contains ``@``, ``;`` or whitespace, or starts
            with the reserved ``orcapod.`` prefix.
    """
    if not isinstance(name, str):
        raise TypeError(f"A type name must be a string, got {name!r}.")
    if not name:
        raise ValueError("A type name must not be empty.")
    if _NAME_FORBIDDEN.search(name):
        raise ValueError(
            f"Invalid type name {name!r}: a name must not contain '@', ';' or "
            f"whitespace."
        )
    if name.startswith(RESERVED_PREFIX):
        raise ValueError(
            f"Invalid type name {name!r}: the prefix {RESERVED_PREFIX!r} is reserved "
            f"for Orcapod's own types."
        )
    return unicodedata.normalize("NFC", name)


def exact_class_name(cls: type) -> str:
    """Return the name of an exact class: its module path and qualified name.

    A class defined in a notebook or script is named ``__main__.<qualname>``; a
    ``multiprocessing`` spawn child calls that module ``__mp_main__``, which is written
    ``__main__`` so that the child finds the same type.

    Args:
        cls: An ordinary dataclass or a pydantic model class.

    Returns:
        ``f"{module}.{qualname}"``.
    """
    if cls.__module__ in MAIN_MODULES:
        return f"{MAIN_MODULES[0]}.{cls.__qualname__}"
    module = canonical_module_name(cls) or cls.__module__
    return f"{module}.{cls.__qualname__}"


def class_name(cls: type) -> str:
    """Return the name component of a user class's identity.

    Args:
        cls: A user class.

    Returns:
        The declared name of a structural class, else ``exact_class_name(cls)``.
    """
    declared = structural_name(cls)
    if declared is not None:
        return declared
    return exact_class_name(cls)


def class_path(cls: type) -> str:
    """Return ``module.qualname`` of a class as Python reports it, for messages."""
    return f"{cls.__module__}.{cls.__qualname__}"


def reject_local_class(cls: type) -> None:
    """Raise if an exact class is defined inside a function.

    Args:
        cls: A class.

    Raises:
        ValueError: If ``cls.__qualname__`` contains ``<locals>``.
    """
    if "<locals>" in cls.__qualname__:
        raise ValueError(
            f"Cannot store local class {cls!r}: a class defined inside a function "
            f"cannot be found again by its module path when stored values are read. "
            f"Define the class at module level, or make it an orcapod.dataclass, "
            f"which is identified by its name and fields."
        )


# ---------------------------------------------------------------------------
# Structural classes known in this process
# ---------------------------------------------------------------------------


def remember_structural(name: str, cls: type) -> None:
    """Record ``cls`` as a structural class defined under ``name`` in this process.

    Args:
        name: The declared name.
        cls: The class object.
    """
    refs = _structural_classes.setdefault(name, [])
    refs[:] = [r for r in refs if r() is not None and r() is not cls]
    refs.append(weakref.ref(cls))


def structural_classes(name: str) -> list[type]:
    """Return the live structural classes defined under ``name``, most recent first."""
    classes = [r() for r in _structural_classes.get(name, [])]
    return [c for c in reversed(classes) if c is not None]


# ---------------------------------------------------------------------------
# Identity record and string
# ---------------------------------------------------------------------------


def identity_record(
    kind: str, name: str, fields: Iterable[tuple[str, Any]]
) -> dict[str, Any]:
    """Build the identity record of a user class.

    Args:
        kind: ``"dataclass"``, ``"pydantic"`` or ``"structural"``.
        name: The name component.
        fields: ``(stored field name, type token)`` pairs in declared order.

    Returns:
        ``{"kind": kind, "name": name, "fields": [[field, token], ...]}``. The fields
        keep the given order, which is part of the identity.
    """
    return {
        "kind": kind,
        "name": name,
        "fields": [[field, token] for field, token in fields],
    }


def canonical_json(obj: Any) -> str:
    """Serialize ``obj`` as canonical JSON (sorted object keys, no whitespace).

    Arrays keep their order, so the ``fields`` of an identity record stay in declared
    order.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def identity_string(record: dict[str, Any]) -> str:
    """Return the identity string of an identity record.

    The format is ``f"{name}@{fingerprint}"``, where the fingerprint is the first
    ``FINGERPRINT_HEX_DIGITS`` hex digits of the SHA-256 of ``canonical_json(record)``.
    This is the only definition of the format.

    Args:
        record: An identity record from ``identity_record``.

    Returns:
        The identity string, e.g. ``"Point@76d961e4..."``.
    """
    digest = hashlib.sha256(canonical_json(record).encode("utf-8")).hexdigest()
    return f"{record['name']}@{digest[:FINGERPRINT_HEX_DIGITS]}"


def describe_token(token: Any) -> str:
    """Render a type token for messages, e.g. ``list[int]`` or ``int | None``."""
    if isinstance(token, str):
        return token
    if "logical" in token:
        return token["logical"].split(";", 1)[0]
    if "generic" in token:
        inner = ", ".join(describe_token(arg) for arg in token["args"])
        return f"{token['generic']}[{inner}]"
    if "union" in token:
        return " | ".join(describe_token(member) for member in token["union"])
    if "literal" in token:
        return f"Literal[{', '.join(token['literal'])}]"
    if "class" in token:
        return str(token["class"].get("name"))
    return canonical_json(token)


def describe_record(record: dict[str, Any]) -> str:
    """Render an identity record for messages, e.g. ``Point (dataclass) {x: int}``."""
    text = f"{record.get('name')} ({record.get('kind')})"
    fields = record.get("fields")
    if fields is None:
        return text
    inner = ", ".join(f"{name}: {describe_token(token)}" for name, token in fields)
    return f"{text} {{{inner}}}"


# ---------------------------------------------------------------------------
# Describing a class: record and type tokens
# ---------------------------------------------------------------------------


def class_record(
    cls: type, converter: TypeConverterProtocol, _path: tuple[type, ...] = ()
) -> dict[str, Any]:
    """Return the identity record of a user class.

    The record depends only on the class definition and on the logical types the
    converter knows for its field types; no user class is registered.

    Args:
        cls: A dataclass or pydantic model class.
        converter: The type converter that resolves the logical types of field types
            that are not user classes.

    Returns:
        The identity record.

    Raises:
        TypeError: If ``cls`` is not a user class, a field type has no type token, or
            a structural class has a field typed by an exact class.
        RecursiveTypeError: If the class refers back to itself.
        ValueError: If an exact class is local or type hints cannot be resolved.
    """
    kind = class_kind(cls)
    if kind is None:
        raise TypeError(f"{cls!r} is not a dataclass or pydantic model class.")
    if cls in _path:
        raise RecursiveTypeError(
            " -> ".join(c.__qualname__ for c in (*_path[_path.index(cls) :], cls))
        )
    if kind in EXACT_KINDS:
        reject_local_class(cls)
    hints = type_hints(cls)
    path = (*_path, cls)
    fields: list[tuple[str, Any]] = []
    for name in stored_field_names(cls):
        token = _type_token(hints.get(name, Any), converter, path)
        if kind == KIND_STRUCTURAL:
            exact = _first_exact_class(token)
            if exact is not None:
                raise TypeError(
                    f"Field {name!r} of the orcapod.dataclass {class_name(cls)!r} is "
                    f"typed by {describe_record(exact)}, which is identified by its "
                    f"exact class. Every class inside an orcapod.dataclass must be an "
                    f"orcapod.dataclass, so that it can be rebuilt from stored "
                    f"metadata alone."
                )
        fields.append((name, token))
    record = identity_record(kind, class_name(cls), fields)
    _record_of[cls] = record
    return record


def known_record(cls: type) -> dict[str, Any] | None:
    """Return the record ``cls`` was last described with in this process, or ``None``."""
    return _record_of.get(cls)


def _first_exact_class(token: Any) -> dict[str, Any] | None:
    """Return the record of the first exact class a token mentions, or ``None``."""
    if isinstance(token, str):
        return None
    if "class" in token:
        record = token["class"]
        if record["kind"] in EXACT_KINDS:
            return record
        for _, inner in record.get("fields", []):
            found = _first_exact_class(inner)
            if found is not None:
                return found
        return None
    for members in (token.get("args"), token.get("union")):
        for member in members or []:
            found = _first_exact_class(member)
            if found is not None:
                return found
    return None


def type_token(annotation: Any, converter: TypeConverterProtocol) -> Any:
    """Return the type token of a field annotation.

    A token is JSON. Builtin scalars and classes without a logical type are strings
    (``"int"``, ``"datetime.datetime"``); a type with a logical type is
    ``{"logical": <identifier text>}``, the Arrow extension name and metadata; a
    generic is ``{"generic": <origin>, "args": [...]}``; a union is
    ``{"union": [...]}`` with members sorted and ``"None"`` last; a ``Literal`` is
    ``{"literal": [...]}``; another user class is ``{"class": <its record>}``. The
    ``...`` of ``tuple[T, ...]`` is the string ``"..."``.

    Args:
        annotation: A field annotation (``Annotated`` extras already stripped).
        converter: The type converter that resolves logical types.

    Returns:
        The type token.

    Raises:
        TypeError: If the annotation is not a type the token rules cover.
    """
    return _type_token(annotation, converter, ())


def _type_token(
    annotation: Any, converter: TypeConverterProtocol, path: tuple[type, ...]
) -> Any:
    """Return a type token; ``path`` holds the user classes being described."""
    import types as _types

    from orcapod.logical_types.identifier import logical_type_identifier

    if annotation is None or annotation is type(None):
        return "None"
    if annotation is Ellipsis:
        # ``tuple[T, ...]``: the marker of a tuple of any length.
        return ELLIPSIS_TOKEN
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Union or origin is _types.UnionType:
        members = [_type_token(a, converter, path) for a in args]
        non_none = sorted((m for m in members if m != "None"), key=canonical_json)
        return {"union": non_none + (["None"] if "None" in members else [])}
    if origin is Literal:
        return {"literal": sorted(repr(a) for a in args)}
    if isinstance(annotation, type) and origin is None:
        if class_kind(annotation) is not None:
            return {"class": class_record(annotation, converter, path)}
        _ = converter.register_python_class(annotation)  # raises for unsupported types
        lt = converter.get_logical_type_for_python_type(annotation)
        if lt is not None:
            return {"logical": logical_type_identifier(lt).text}
        return _class_token(annotation)
    if origin is not None:
        return {
            "generic": _class_token(origin),
            "args": [_type_token(a, converter, path) for a in args],
        }
    raise TypeError(f"Cannot build a type token for annotation {annotation!r}.")


def _class_token(cls: Any) -> str:
    """Return the type token of a class: its canonical name, builtins written bare."""
    name = canonical_type_name(cls)
    return name.removeprefix("builtins.")


def make_structural(cls: type, name: str) -> None:
    """Mark a dataclass as the structural class ``name``.

    Called through ``structural_class.finish_structural_class`` on a class that
    ``orcapod.dataclass`` has just created or that was built from stored metadata.

    Args:
        cls: A dataclass created for this purpose.
        name: The validated declared name.
    """
    setattr(cls, STRUCTURAL_NAME_ATTRIBUTE, name)
    remember_structural(name, cls)


def find_exact_class(name: str) -> Any:
    """Return the object an exact class's name refers to now, or ``None``.

    Args:
        name: ``module.qualname``; ``__main__.<qualname>`` is looked up in the running
            main module (``__main__`` or ``__mp_main__``).

    Returns:
        The object found by attribute walk or import, or ``None`` if there is none.
    """
    from orcapod.logical_types.type_utils import _walk_fqcn

    main_prefix = f"{MAIN_MODULES[0]}."
    if name.startswith(main_prefix):
        for module_name in MAIN_MODULES:
            module = sys.modules.get(module_name)
            obj = module
            for part in name[len(main_prefix) :].split("."):
                obj = getattr(obj, part, None)
                if obj is None:
                    break
            if obj is not None:
                return obj
        return None
    try:
        return _walk_fqcn(name)
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Same type?
# ---------------------------------------------------------------------------


def same_user_type(a: type, b: type) -> bool:
    """Return True if two user classes are one type.

    Exact classes are one type only when they are the same object. Structural classes
    are one type when their identity records are equal.

    Args:
        a: A class.
        b: A class.

    Returns:
        Whether Orcapod treats the two classes as the same type.
    """
    if a is b:
        return True
    if not (is_structural(a) and is_structural(b)):
        return False
    record_a, record_b = _record_of.get(a), _record_of.get(b)
    if record_a is None or record_b is None:
        from orcapod.contexts import get_default_context

        # The context declares its converter with the semantic-types protocol; it is
        # the same converter object the logical types use.
        converter: Any = get_default_context().type_converter
        record_a = record_a or class_record(a, converter)
        record_b = record_b or class_record(b, converter)
    return record_a == record_b


def equivalent_hints(a: Any, b: Any) -> bool:
    """Return True if two type hints denote the same types.

    Hints are equal, or differ only in structural classes that are one type
    (``same_user_type``). Generic hints are compared by origin and arguments; union
    members in any order.

    Args:
        a: A type hint.
        b: A type hint.

    Returns:
        True if the hints denote the same types.
    """
    import types as _types

    if a == b:
        return True
    if isinstance(a, type) and isinstance(b, type):
        return same_user_type(a, b)
    origin_a, origin_b = typing.get_origin(a), typing.get_origin(b)
    if origin_a is None or origin_b is None:
        return False
    args_a, args_b = typing.get_args(a), typing.get_args(b)
    unions = (typing.Union, _types.UnionType)
    if origin_a in unions and origin_b in unions:
        return all(any(equivalent_hints(x, y) for y in args_b) for x in args_a) and all(
            any(equivalent_hints(x, y) for x in args_a) for y in args_b
        )
    if origin_a != origin_b or len(args_a) != len(args_b):
        return False
    return all(equivalent_hints(x, y) for x, y in zip(args_a, args_b))
