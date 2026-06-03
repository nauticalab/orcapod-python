# src/orcapod/semantic_types/dataclass_encoding.py
"""
Dataclass <-> Arrow struct encoding for Orcapod.

Encodes Python dataclasses as Arrow structs with a ``__type`` sentinel field
carrying the fully-qualified class name. Decoding uses a three-tier fallback:
import -> registry -> synthesize.
"""

from __future__ import annotations

import dataclasses
import importlib
import logging
import re
import sys
import typing
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)

DATACLASS_TYPE_FIELD = "__type"
DATACLASS_TYPE_PREFIX = "dataclass:"

# Validates fully-qualified class names like "my_module.sub.MyClass".
# Also accepts qualnames containing "<locals>" segments produced by local
# class definitions (e.g. "mod.func.<locals>.MyClass").  Each dot-separated
# segment may be a normal identifier or the literal token "<locals>".
_FQCN_RE = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*|\.<locals>)+$")

# Matches all identifier tokens within a stringified annotation.
# Used by _get_type_hints_safe to handle compound forms like
# "Optional[_Inner]", "list[_Inner]", or "_Inner | None".
_IDENT_RE = re.compile(r"[A-Za-z_]\w*")

# Process-global registry for tier-2 reconstruction.
# Populated via register_dataclass(); persists for the process lifetime.
_DATACLASS_REGISTRY: dict[str, type] = {}


def register_dataclass(cls: type) -> type:
    """Register a dataclass for tier-2 reconstruction by fully-qualified name.

    Can be used as a class decorator or called directly. Returns ``cls``
    unchanged so it works transparently as a decorator.

    Args:
        cls: A Python dataclass type to register.

    Returns:
        The same ``cls`` that was passed in.

    Raises:
        TypeError: If ``cls`` is not a dataclass type.
    """
    if not dataclasses.is_dataclass(cls) or not isinstance(cls, type):
        raise TypeError(f"{cls!r} is not a dataclass type")
    key = f"{cls.__module__}.{cls.__qualname__}"
    _DATACLASS_REGISTRY[key] = cls
    return cls


def has_dataclass_type_sentinel(arrow_type: pa.DataType) -> bool:
    """Return ``True`` if ``arrow_type`` is a struct with a ``__type`` string field.

    Accepts both ``pa.large_string()`` and ``pa.string()`` for compatibility
    with data written by older Arrow versions.

    Args:
        arrow_type: Any PyArrow data type.

    Returns:
        True if ``arrow_type`` is a struct containing a ``__type: (large_)string``
        field.
    """
    if not pa.types.is_struct(arrow_type):
        return False
    for i in range(arrow_type.num_fields):
        field = arrow_type.field(i)
        if field.name == DATACLASS_TYPE_FIELD:
            return pa.types.is_large_string(field.type) or pa.types.is_string(field.type)
    return False


def _get_type_hints_safe(cls: type) -> dict[str, Any]:
    """Return type hints for a dataclass, tolerating unresolvable local annotations.

    Calls ``typing.get_type_hints(cls)`` first. If that raises ``NameError``
    (which happens for classes with annotations that reference locally-scoped
    types when ``from __future__ import annotations`` is in effect), falls
    back to searching call-stack frames for the identifier tokens referenced
    in the annotations, then to module globals, and finally returns raw string
    annotations as a last resort.

    The token scan (via ``_IDENT_RE``) extracts *all* identifiers from each
    string annotation, so compound forms like ``"Optional[_Inner]"``,
    ``"list[_Inner]"``, and ``"_Inner | None"`` are handled correctly — only
    matching the whole annotation string would miss them.

    Frame traversal uses ``sys._getframe()``/``f_back`` rather than
    ``inspect.stack()`` to avoid the overhead and strong-reference pitfalls
    introduced by ``inspect.stack()``'s ``FrameInfo`` wrapper objects.

    Args:
        cls: A Python dataclass type.

    Returns:
        A dict mapping field names to resolved type hints. Values may be string
        annotations for names that could not be resolved.
    """
    try:
        return typing.get_type_hints(cls)
    except NameError:
        pass

    localns: dict[str, Any] = {}

    # 1. Module globals for the class's module (cheap, no frame traversal needed).
    module = sys.modules.get(cls.__module__)
    if module is not None:
        for name, obj in vars(module).items():
            if isinstance(obj, type):
                localns[name] = obj

    # 2. Collect *all* identifier tokens from string annotations so that compound
    #    forms like "Optional[_Inner]" or "_Inner | None" are handled correctly.
    raw_annotations = cls.__annotations__
    token_names: set[str] = set()
    for v in raw_annotations.values():
        if isinstance(v, str):
            token_names.update(_IDENT_RE.findall(v))

    # 3. Walk the live frame chain via f_back — no FrameInfo objects, no extra
    #    strong references to frames.
    if token_names:
        frame = sys._getframe(0)
        while frame is not None:
            remaining = token_names - set(localns)
            if not remaining:
                break
            for name in remaining:
                obj = frame.f_locals.get(name)
                if obj is not None and isinstance(obj, type):
                    localns[name] = obj
            frame = frame.f_back

    try:
        return typing.get_type_hints(cls, localns=localns)
    except NameError:
        pass

    # Last resort: return raw annotations (may contain strings for local types).
    return dict(raw_annotations)


def dataclass_to_arrow_struct_type(
    cls: type,
    converter: Any,
) -> pa.StructType:
    """Derive the Arrow struct type for a dataclass class.

    The resulting struct has ``__type: large_string`` as its first field,
    followed by one field per dataclass field. Field types are resolved via
    ``converter`` (a ``UniversalTypeConverter``), so nested dataclasses
    produce nested structs automatically once the converter has the dataclass
    branch wired in.

    Args:
        cls: A Python dataclass type.
        converter: A ``UniversalTypeConverter`` instance used for field type
            resolution.

    Returns:
        A ``pa.StructType`` with ``__type`` as the first field.

    Raises:
        TypeError: If `cls` is not a dataclass type.
    """
    if not dataclasses.is_dataclass(cls) or not isinstance(cls, type):
        raise TypeError(f"{cls!r} is not a dataclass type")

    hints = _get_type_hints_safe(cls)
    fields: list[pa.Field] = [pa.field(DATACLASS_TYPE_FIELD, pa.large_string())]
    for f in dataclasses.fields(cls):
        arrow_type = converter.python_type_to_arrow_type(hints[f.name])
        fields.append(pa.field(f.name, arrow_type))
    return pa.struct(fields)


def dataclass_to_struct_dict(
    obj: Any,
    field_converters: dict[str, Any],
) -> dict[str, Any]:
    """Encode a dataclass instance to an Arrow-compatible struct dict.

    Args:
        obj: A dataclass instance to encode.
        field_converters: Pre-built per-field converter callables keyed by
            field name. Build these once per type at converter-creation time
            and reuse per row to avoid repeated type dispatch.

    Returns:
        A dict with ``__type`` as the first key followed by encoded field values.

    Raises:
        TypeError: If ``obj`` is not a dataclass instance (e.g. a class itself
            or a non-dataclass value).
    """
    # dataclasses.is_dataclass() returns True for both classes and instances;
    # isinstance(obj, type) distinguishes: True for classes, False for instances.
    if not dataclasses.is_dataclass(obj) or isinstance(obj, type):
        raise TypeError(f"{obj!r} is not a dataclass instance")

    cls = type(obj)
    type_str = f"{DATACLASS_TYPE_PREFIX}{cls.__module__}.{cls.__qualname__}"
    result: dict[str, Any] = {DATACLASS_TYPE_FIELD: type_str}
    for f in dataclasses.fields(cls):
        value = getattr(obj, f.name)
        converter_fn = field_converters.get(f.name, lambda v: v)
        result[f.name] = converter_fn(value)
    return result


def struct_dict_to_dataclass(
    struct_dict: dict[str, Any],
    field_converters: dict[str, Any],
    lookup_cache: dict[str, type],
) -> Any:
    """Decode an Arrow struct dict to a Python dataclass instance.

    Uses a three-tier fallback:

    1. **Import** — ``importlib``-import the class from its fully-qualified name.
    2. **Registry** — look up the FQCN in the process-global ``_DATACLASS_REGISTRY``.
    3. **Synthesize** — create a throwaway dataclass with ``dataclasses.make_dataclass``
       matching the struct's field names (all fields typed as ``Any``).

    Tier 3 never raises. A ``lookup_cache`` (keyed by FQCN) amortises repeated
    resolution across rows in the same read operation.

    Args:
        struct_dict: Arrow struct row dict as produced by ``pa.Table.to_pylist()``.
        field_converters: Per-field Arrow->Python converter callables (keyed by
            field name, excluding ``__type``).
        lookup_cache: Mutable dict used as a per-read cache. Pass the same dict
            for all rows in a read operation; clear between operations if needed.

    Returns:
        A dataclass instance (real or synthesized) with field values set.
    """
    type_str = struct_dict.get(DATACLASS_TYPE_FIELD)

    fqcn: str | None = None
    class_name = "SynthesizedDataclass"

    if type_str and isinstance(type_str, str) and type_str.startswith(DATACLASS_TYPE_PREFIX):
        candidate = type_str[len(DATACLASS_TYPE_PREFIX):]
        if _FQCN_RE.match(candidate):
            fqcn = candidate
            class_name = fqcn.rsplit(".", 1)[-1]
        else:
            logger.warning(
                "struct_dict_to_dataclass: invalid __type value %r — falling back to tier 3",
                type_str,
            )

    cls: type | None = None

    if fqcn is not None:
        # Check lookup cache first (amortises tiers 1-3 across rows)
        if fqcn in lookup_cache:
            cls = lookup_cache[fqcn]
        else:
            # Tier 1: import
            module_path, _, class_attr = fqcn.rpartition(".")
            try:
                module = importlib.import_module(module_path)
                resolved = getattr(module, class_attr)
                if not dataclasses.is_dataclass(resolved) or not isinstance(resolved, type):
                    raise AttributeError(
                        f"{class_attr!r} in {module_path!r} is not a dataclass type"
                    )
                cls = resolved
                lookup_cache[fqcn] = cls
            except (ImportError, AttributeError) as exc:
                logger.debug(
                    "struct_dict_to_dataclass: tier 1 import failed for %r: %s",
                    fqcn, exc,
                )

            # Tier 2: registry
            if cls is None:
                cls = _DATACLASS_REGISTRY.get(fqcn)
                if cls is not None:
                    lookup_cache[fqcn] = cls

            # Tier 3: synthesize (fqcn valid but unresolvable)
            if cls is None:
                field_names = [k for k in struct_dict if k != DATACLASS_TYPE_FIELD]
                cls = dataclasses.make_dataclass(
                    class_name, [(name, typing.Any) for name in field_names]
                )
                lookup_cache[fqcn] = cls
    else:
        # No valid fqcn — tier 3 with no caching (no stable key)
        field_names = [k for k in struct_dict if k != DATACLASS_TYPE_FIELD]
        cls = dataclasses.make_dataclass(
            class_name, [(name, typing.Any) for name in field_names]
        )

    # Instantiate: apply field converters, skip the __type sentinel
    data_kwargs: dict[str, Any] = {}
    for key, value in struct_dict.items():
        if key == DATACLASS_TYPE_FIELD:
            continue
        converter_fn = field_converters.get(key, lambda v: v)
        data_kwargs[key] = converter_fn(value) if value is not None else None

    return cls(**data_kwargs)
