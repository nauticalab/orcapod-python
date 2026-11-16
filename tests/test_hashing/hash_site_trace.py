"""Run-time trace of every hash Orcapod produces and every place it is consumed (ITL-1154).

A pytest plugin. Load it with ``-p tests.test_hashing.hash_site_trace`` and name the output
file in ``ORCAPOD_HASH_TRACE_OUT``::

    ORCAPOD_HASH_TRACE_OUT=trace.json uv run python -m pytest \\
        -p tests.test_hashing.hash_site_trace tests/test_hashing

It records, with the library call site of each event:

* production: every ``ContentHash`` built (directly, or parsed back with ``from_string`` /
  ``from_prefixed_digest``), and every ``hashlib`` constructor, ``uuid.uuid5`` and starfix
  ``ArrowDigester`` call made from library code;
* consumption: every read of a ``ContentHash``'s digest in any form (``.digest``,
  ``to_hex``, ``to_string``, ``to_prefixed_digest``, ``to_int``, ``to_uuid``,
  ``to_base64``, ``str()``, ``display_name``) and every comparison or ``hash()`` of one,
  linked to the site that produced that digest.

A call site is ``<path under src/orcapod>:<function>``; the ``origin`` of an event is the
first frame outside the hashing machinery (``hashing/``, ``types.py``, the content-identity
base classes), which says what the hash is for. Events with no library frame on the stack
(a test reading a hash directly) are recorded under ``<test>``.
"""

from __future__ import annotations

import re
import hashlib
import json
import os
import sys
import threading
import uuid
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Any

import pytest

_SRC = str(Path(__file__).resolve().parents[2] / "src" / "orcapod") + os.sep
_THIS = str(Path(__file__).resolve())
_MACHINERY = tuple(
    _SRC + part
    for part in (
        "hashing" + os.sep,
        "types.py",
        "core" + os.sep + "base.py",
    )
)
_TESTS = str(Path(__file__).resolve().parents[1]) + os.sep

_state = threading.local()

produced: Counter[tuple[str, str, str, str]] = Counter()
"""(kind, label, site, origin) -> count."""
consumed: Counter[tuple[str, str, str, str, str, str]] = Counter()
"""(accessor, label, site, origin, producer site, producer origin) -> count."""
_producer_of: dict[tuple[str, bytes], tuple[str, str]] = {}
"""(label, digest) -> the first producer: the fallback for a hash parsed from text."""
_producer_by_object: dict[int, tuple[Any, tuple[str, str]]] = {}
"""id(hash) -> (the hash, its producer); the hash is kept so its id is never reused."""


def _depth() -> int:
    return getattr(_state, "depth", 0)


_INLINED_COMPREHENSION = re.compile(r"\.<locals>\.<(?:list|set|dict)comp>(\.|$)")
"""A frame Python 3.11 gives a list, set or dict comprehension; 3.12 inlines it (PEP 709)."""


def _rel(frame: FrameType) -> str:
    """Return ``<path under src>:<qualname>`` of a frame, the same on every Python.

    A comprehension's own frame (Python 3.11 only) is named by the function it is
    written in, as Python 3.12 and later name it.
    """
    path = frame.f_code.co_filename
    qualname = frame.f_code.co_qualname
    # ``f.<locals>.<listcomp>`` -> ``f``; ``f.<locals>.<listcomp>.<lambda>`` ->
    # ``f.<locals>.<lambda>``, as Python 3.12 names them; nested ones one at a time.
    previous = None
    while previous != qualname:
        previous = qualname
        qualname = _INLINED_COMPREHENSION.sub(
            lambda m: ".<locals>." if m.group(1) else "", qualname, count=1
        )
    return f"{path[len(_SRC) :]}:{qualname}"


def _sites(frame: FrameType | None) -> tuple[str, str]:
    """Return (site, origin) for the stack above ``frame``."""
    site = ""
    origin = ""
    test_file = ""
    while frame is not None:
        path = frame.f_code.co_filename
        if path == _THIS:
            frame = frame.f_back
            continue
        if path.startswith(_SRC):
            if not site:
                site = _rel(frame)
            if not path.startswith(_MACHINERY):
                origin = _rel(frame)
                break
        elif path.startswith(_TESTS) and not test_file:
            test_file = "<test>" + path[len(_TESTS) :]
            if not site:
                break
        frame = frame.f_back
    if not site:
        return test_file or "<other>", test_file or "<other>"
    return site, origin or site


def _from_types_parser(frame: FrameType | None) -> bool:
    """Whether the construction is ``ContentHash.from_string`` / ``from_prefixed_digest``."""
    while frame is not None and frame.f_code.co_filename == _THIS:
        frame = frame.f_back
    return frame is not None and frame.f_code.co_name in (
        "from_string",
        "from_prefixed_digest",
    )


def _install_content_hash() -> None:
    from orcapod.types import ContentHash

    digest_slot = ContentHash.__dict__["digest"]
    original_init = ContentHash.__init__

    def traced_init(self: Any, method: str, digest: bytes) -> None:
        original_init(self, method, digest)
        frame = sys._getframe(1)  # pyright: ignore[reportPrivateUsage]
        kind = "parse" if _from_types_parser(frame) else "construct"
        site, origin = _sites(frame)
        produced[(kind, method, site, origin)] += 1
        if kind == "construct":
            _producer_by_object[id(self)] = (self, (site, origin))
            _producer_of.setdefault((method, bytes(digest)), (site, origin))

    def record(self: Any, accessor: str) -> None:
        if _depth():
            return
        frame = sys._getframe(2)  # pyright: ignore[reportPrivateUsage]
        site, origin = _sites(frame)
        method = object.__getattribute__(self, "method")
        raw = digest_slot.__get__(self, type(self))
        # The object's own producer; equal hashes made at two sites stay apart. A
        # parsed hash has none, so it falls back to the first producer of its digest.
        own = _producer_by_object.get(id(self))
        if own is not None and own[0] is self:
            p_site, p_origin = own[1]
        else:
            p_site, p_origin = _producer_of.get(
                (method, raw), ("<unknown>", "<unknown>")
            )
        consumed[(accessor, method, site, origin, p_site, p_origin)] += 1

    def digest_get(self: Any) -> bytes:
        record(self, "digest")
        return digest_slot.__get__(self, type(self))

    def digest_set(self: Any, value: bytes) -> None:
        digest_slot.__set__(self, value)

    def wrap(name: str) -> None:
        original: Callable[..., Any] = getattr(ContentHash, name)

        def traced(self: Any, *args: Any, **kwargs: Any) -> Any:
            record(self, name)
            # A comparison reads both operands' digests.
            if name == "__eq__" and args and isinstance(args[0], ContentHash):
                record(args[0], name)
            _state.depth = _depth() + 1
            try:
                return original(self, *args, **kwargs)
            finally:
                _state.depth = _depth() - 1

        traced.__name__ = name
        setattr(ContentHash, name, traced)

    ContentHash.__init__ = traced_init  # pyright: ignore[reportAttributeAccessIssue]
    ContentHash.digest = property(digest_get, digest_set)  # pyright: ignore[reportAttributeAccessIssue]
    for name in (
        "to_hex",
        "to_string",
        "to_prefixed_digest",
        "to_int",
        "to_uuid",
        "to_base64",
        "__str__",
        "display_name",
        "__eq__",
        "__hash__",
    ):
        wrap(name)


def _library_caller(frame: FrameType | None) -> bool:
    return frame is not None and frame.f_code.co_filename.startswith(_SRC)


def _install_producers() -> None:
    from starfix import ArrowDigester

    def wrap_function(owner: Any, name: str, kind: str) -> None:
        original: Callable[..., Any] = getattr(owner, name)

        def traced(*args: Any, **kwargs: Any) -> Any:
            frame = sys._getframe(1)  # pyright: ignore[reportPrivateUsage]
            if _library_caller(frame):
                label = kind
                if name == "new" and args:
                    label = f"hashlib.{args[0]}"
                site, origin = _sites(frame)
                produced[(label, "", site, origin)] += 1
            return original(*args, **kwargs)

        setattr(owner, name, traced)

    for name in ("sha256", "sha1", "md5", "sha3_256", "blake2b", "new"):
        wrap_function(hashlib, name, f"hashlib.{name}")
    wrap_function(uuid, "uuid5", "uuid5")
    for name in ("hash_table", "hash_schema", "hash_record_batch", "hash_array"):
        wrap_function(ArrowDigester, name, f"starfix.{name}")


_installed = False


def install() -> None:
    """Install the trace in this process (idempotent)."""
    global _installed
    if not _installed:
        _install_content_hash()
        _install_producers()
        _installed = True


def library_sites() -> set[str]:
    """Every library call site the trace saw produce or consume a digest."""
    sites = {site for _, _, site, _ in produced} | {
        site for _, _, site, _, _, _ in consumed
    }
    return {site for site in sites if not site.startswith("<")}


def run_scenario() -> None:
    """Compute every golden value, plus the hash sites no golden case reaches."""
    import tempfile
    import warnings
    from pathlib import Path as _Path

    warnings.filterwarnings("ignore")
    import orcapod as op
    from orcapod.contexts import get_default_context
    from orcapod.core.sources.list_source import ListSource
    from tests.test_hashing.golden import cases

    _ = cases.compute_all()
    hasher = get_default_context().semantic_hasher
    with tempfile.TemporaryDirectory() as tmp:
        root = _Path(tmp)
        (root / "a.txt").write_text("alpha")
        _ = hasher.hash_object([op.Directory(root), op.File(root / "a.txt")])
    _ = ListSource(
        name="item",
        data=[1, 2],
        tag_function=_scenario_tag,
        expected_tag_keys=["index"],
        tag_function_hash_mode="content",
    ).content_hash()


def _scenario_tag(element: int, index: int) -> dict[str, int]:
    """Tag function of the scenario's ``ListSource``."""
    del element
    return {"index": index}


def pytest_configure(config: pytest.Config) -> None:
    """Install the trace before any test module imports the library."""
    del config
    install()


def pytest_unconfigure(config: pytest.Config) -> None:
    """Write the trace to ``ORCAPOD_HASH_TRACE_OUT``."""
    del config
    out = os.environ.get("ORCAPOD_HASH_TRACE_OUT")
    if not out:
        return
    payload = {
        "produced": [
            {"kind": k, "label": label, "site": s, "origin": o, "count": n}
            for (k, label, s, o), n in sorted(produced.items())
        ],
        "consumed": [
            {
                "accessor": a,
                "label": label,
                "site": s,
                "origin": o,
                "producer_site": ps,
                "producer_origin": po,
                "count": n,
            }
            for (a, label, s, o, ps, po), n in sorted(consumed.items())
        ],
    }
    Path(out).write_text(json.dumps(payload, indent=1))


if __name__ == "__main__":
    install()
    run_scenario()
    print(json.dumps(sorted(library_sites())))
