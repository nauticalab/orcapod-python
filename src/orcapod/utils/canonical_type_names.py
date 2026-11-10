"""Python-version-independent names for types that appear in identity strings.

A type's ``__module__`` names the module that *defines* it, which is an
implementation detail of the standard library and moves between Python
versions. Python 3.13 turned ``pathlib`` into a package and moved its classes
into the private ``pathlib._local`` module, so ``pathlib.Path.__module__`` is
``"pathlib"`` on 3.11/3.12 and ``"pathlib._local"`` on 3.13. Any hash input
built from ``__module__`` (or from ``repr``/``str``/``inspect.formatannotation``
of a type, which print it) therefore differed by interpreter version.

The helpers here resolve a standard-library type defined in a private
submodule to the public module that exports it. User-defined and third-party
types are never touched.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Iterator
from typing import cast

# Standard-library private submodules that were already the defining module of
# publicly exported classes on Python 3.11 and/or 3.12 (found by walking every
# stdlib module on both versions). Names derived from them are baked into
# existing caches, so they are frozen as-is and never resolved to the public
# module. Do not add modules introduced after 3.12 here: those must resolve.
_LEGACY_PRIVATE_STDLIB_MODULES: frozenset[str] = frozenset(
    {
        "concurrent.futures._base",
        "ctypes._endian",
        "importlib.metadata._collections",
        "importlib.metadata._meta",
        "xml.sax._exceptions",
        "zipfile._path",
        "zoneinfo._common",
        "zoneinfo._tzpath",
    }
)


def canonical_module_name(obj: object) -> str | None:
    """Return the module name to use for ``obj`` in identity strings.

    This is ``obj.__module__`` unless ``obj`` is a standard-library object
    defined in a private submodule (``pathlib._local``) and re-exported, as the
    very same object, by the public module above it (``pathlib``). In that case
    the public module name is returned, which is what earlier Python versions
    reported.

    Top-level private modules (``_io``, ``_thread``) are returned unchanged:
    they are stable across versions and have no public ancestor.

    Args:
        obj: A class, function, or any object with ``__module__``.

    Returns:
        The canonical module name, or ``None`` if ``obj`` has no string
        ``__module__``.
    """
    module = getattr(obj, "__module__", None)
    if not isinstance(module, str):
        return None

    parts = module.split(".")
    if parts[0] not in sys.stdlib_module_names:
        return module
    private_at = next(
        (i for i, part in enumerate(parts) if i > 0 and part.startswith("_")), None
    )
    if private_at is None or module in _LEGACY_PRIVATE_STDLIB_MODULES:
        return module

    qualname = getattr(obj, "__qualname__", None)
    if not isinstance(qualname, str) or "<locals>" in qualname:
        return module

    public_module = ".".join(parts[:private_at])
    # The parent package of an imported submodule is always in sys.modules.
    exported: object = sys.modules.get(public_module)
    for attr in qualname.split("."):
        exported = getattr(exported, attr, None)
    return public_module if exported is obj else module


def canonical_type_name(tp: type) -> str:
    """Return ``"<module>.<qualname>"`` for ``tp`` using its canonical module.

    Args:
        tp: A class.

    Returns:
        The qualified name, e.g. ``"pathlib.Path"`` on every Python version.
    """
    return f"{canonical_module_name(tp) or '<unknown>'}.{tp.__qualname__}"


def _iter_types(annotation: object, _seen: set[int] | None = None) -> Iterator[type]:
    """Yield every class reachable from ``annotation``.

    Walks generic aliases and unions through ``__origin__`` and ``__args__``,
    and the parameter lists nested inside them.
    """
    seen = _seen if _seen is not None else set[int]()
    if id(annotation) in seen:
        return
    seen.add(id(annotation))

    if isinstance(annotation, type):
        yield annotation
    if isinstance(annotation, (list, tuple)):
        for item in cast("list[object] | tuple[object, ...]", annotation):
            yield from _iter_types(item, seen)
        return
    origin: object = getattr(annotation, "__origin__", None)
    if origin is not None:
        yield from _iter_types(origin, seen)
    args: object = getattr(annotation, "__args__", None)
    if isinstance(args, tuple):
        for arg in cast("tuple[object, ...]", args):
            yield from _iter_types(arg, seen)


def stabilize_type_names(text: str, *annotations: object) -> str:
    """Rewrite version-specific type names inside an already rendered string.

    ``repr``, ``str`` and ``inspect.formatannotation`` all print a class as
    ``<__module__>.<__qualname__>``. For every class reachable from
    ``annotations`` whose canonical module differs from ``__module__``, that
    spelling is replaced in ``text`` by the canonical one. When no such class
    is present ``text`` is returned unchanged, so output on Python versions
    where nothing has moved is byte-identical to what it always was.

    A spelling is replaced only where it stands as a whole dotted name, in a
    single pass with longer spellings tried first. A shorter spelling therefore
    never rewrites the start of a longer name (``json._impl.Foo`` inside
    ``json._impl.FooBar`` or ``x.json._impl.Foo``), and a nested class
    (``Outer.Inner``) is rewritten as one name.

    Args:
        text: A rendered annotation, parameter or signature string.
        *annotations: The annotation objects that ``text`` was rendered from.

    Returns:
        ``text`` with relocated standard-library type names made canonical.
    """
    replacements: dict[str, str] = {}
    for tp in _iter_types(annotations):
        module = tp.__module__
        canonical = canonical_module_name(tp)
        if canonical is not None and canonical != module:
            replacements[f"{module}.{tp.__qualname__}"] = (
                f"{canonical}.{tp.__qualname__}"
            )
    if not replacements:
        return text
    alternatives = "|".join(
        re.escape(name) for name in sorted(replacements, key=len, reverse=True)
    )
    pattern = re.compile(rf"(?<![\w.])(?:{alternatives})(?!\w)")
    return pattern.sub(lambda match: replacements[match.group(0)], text)
