"""Instrument every pyarrow construction call made from an orcapod module.

Each orcapod module refers to pyarrow through a module-level ``pa`` (or ``_pa``)
name, either the real module or a ``LazyModule``. ``install()`` swaps that name in
every loaded ``orcapod.*`` module for a proxy whose ``array``, ``scalar`` and
``Table.from_pylist`` / ``RecordBatch.from_pylist`` record a "hit" whenever a
Python value that is not exactly ``int`` is handed to an integer-typed Arrow field
(the condition under which pyarrow falls back to ``__int__`` and truncates).

A hit prints the Arrow call and the orcapod frames (file:line) that led to it.
Nothing in orcapod is changed; the proxy forwards every call unchanged.

Usage:
    import _tracer; _tracer.install()   # after importing orcapod
"""

from __future__ import annotations

import os
import sys
import traceback
import types
from typing import Any

import pyarrow as _real_pa

HITS: list[dict[str, Any]] = []
_ROOT = os.path.join("src", "orcapod")


def _bad_values(values: Any, arrow_type: Any) -> list[Any]:
    """Return values that are not exactly ``int`` destined for an integer type."""
    out = []
    if arrow_type is None:
        return out
    if _real_pa.types.is_integer(arrow_type):
        for v in values:
            if v is not None and type(v) is not int:
                out.append(v)
    elif _real_pa.types.is_struct(arrow_type):
        for v in values:
            if isinstance(v, dict):
                for f in arrow_type:
                    out += _bad_values([v.get(f.name)], f.type)
    elif _real_pa.types.is_list(arrow_type) or _real_pa.types.is_large_list(arrow_type):
        for v in values:
            if isinstance(v, (list, tuple)):
                out += _bad_values(v, arrow_type.value_type)
    return out


def _orcapod_frames() -> list[str]:
    frames = []
    for fs in traceback.extract_stack()[:-3]:
        if _ROOT in fs.filename:
            rel = fs.filename[fs.filename.index(_ROOT) :]
            frames.append(f"{rel}:{fs.lineno} in {fs.name}")
    return frames


def _record(call: str, bad: list[Any], target: str) -> None:
    frames = _orcapod_frames()
    HITS.append({"call": call, "bad": bad, "target": target, "frames": frames})
    print(f"  >>> HIT {call}: {bad!r} -> {target}")
    for f in frames[-8:]:
        print(f"        {f}")


class _ClassProxyMeta(type):
    _real: Any = None

    def __getattr__(cls, name: str) -> Any:
        return getattr(cls._real, name)

    def __instancecheck__(cls, obj: Any) -> bool:
        return isinstance(obj, cls._real)


def _make_from_pylist(real_cls: Any, label: str):
    def from_pylist(mapping, schema=None, metadata=None):
        if schema is not None:
            for field in schema:
                bad = _bad_values([row.get(field.name) for row in mapping], field.type)
                if bad:
                    _record(
                        f"{label}.from_pylist(rows, schema=...)",
                        bad,
                        f"{field.name}: {field.type}",
                    )
        return real_cls.from_pylist(mapping, schema=schema, metadata=metadata)

    return staticmethod(from_pylist)


class _TableProxy(metaclass=_ClassProxyMeta):
    _real = _real_pa.Table
    from_pylist = _make_from_pylist(_real_pa.Table, "pa.Table")


class _RecordBatchProxy(metaclass=_ClassProxyMeta):
    _real = _real_pa.RecordBatch
    from_pylist = _make_from_pylist(_real_pa.RecordBatch, "pa.RecordBatch")


class _PyArrowProxy(types.ModuleType):
    Table = _TableProxy
    RecordBatch = _RecordBatchProxy

    def __getattr__(self, name: str) -> Any:
        return getattr(_real_pa, name)

    @staticmethod
    def array(obj, type=None, *args, **kwargs):  # noqa: A002
        if type is not None and not isinstance(
            obj, (_real_pa.Array, _real_pa.ChunkedArray)
        ):
            try:
                values = list(obj)
            except TypeError:
                values = []
            bad = _bad_values(values, type)
            if bad:
                _record("pa.array(values, type=...)", bad, str(type))
        return _real_pa.array(obj, type, *args, **kwargs)  # pyright: ignore[reportCallIssue,reportArgumentType]  # transparent forwarding proxy for pyarrow

    @staticmethod
    def scalar(value, type=None, *args, **kwargs):  # noqa: A002
        bad = _bad_values([value], type)
        if bad:
            _record("pa.scalar(value, type=...)", bad, str(type))
        return _real_pa.scalar(value, type, *args, **kwargs)  # pyright: ignore[reportCallIssue,reportArgumentType]  # transparent forwarding proxy for pyarrow


PROXY = _PyArrowProxy("pyarrow")


def install() -> None:
    """Swap ``pa`` / ``_pa`` in every loaded orcapod module for the proxy."""
    import importlib
    import pkgutil

    import orcapod

    for mod in pkgutil.walk_packages(orcapod.__path__, "orcapod."):
        try:
            importlib.import_module(mod.name)
        except Exception:  # optional backends (e.g. spiraldb) may be absent
            pass
    for name, module in list(sys.modules.items()):
        if not name.startswith("orcapod"):
            continue
        for attr in ("pa", "_pa"):
            current = getattr(module, attr, None)
            if current is None:
                continue
            if current is _real_pa or type(current).__name__ == "LazyModule":
                setattr(module, attr, PROXY)


def summary() -> list[str]:
    """Return the distinct innermost orcapod frame of every hit."""
    seen = []
    for hit in HITS:
        key = f"{hit['call']} at {hit['frames'][-1] if hit['frames'] else '?'}"
        if key not in seen:
            seen.append(key)
    return seen
