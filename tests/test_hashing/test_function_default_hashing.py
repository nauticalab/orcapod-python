"""A function's default values enter its hashes through the semantic hasher (ITL-953).

Finding L2 of the hash-site audit (Appendix A.9 of the ITL-938 design): a default value
used to enter the function's hashed description as ``repr(value)``, so a ``frozenset``
default hashed differently under every hash seed and a default such as ``object()``
embedded a memory address. Two hashes take a function's signature:

* the hash of a function used as a value (``semantic_hasher.hash_object(fn)``), and
* the stored variation value ``function_signature_hash`` of a data function (the
  ``__pf_var_function_signature_hash`` column).

Every test drives both through the public interface.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import os
import subprocess
import sys
import textwrap
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import pytest

from orcapod import FunctionPod, function_pod
from orcapod.contexts import resolve_context
from orcapod.core.data_function import PythonDataFunction
from orcapod.sources import DictSource


def _value_hash(fn: Callable[..., Any]) -> str:
    """Hash of ``fn`` used as a value, by the default data context's semantic hasher."""
    return resolve_context(None).semantic_hasher.hash_object(fn).to_string()


def _stored_signature_hash(fn: Callable[..., Any]) -> bytes:
    """The ``function_signature_hash`` a data function stores with every result row."""
    data_function = PythonDataFunction(fn, output_keys="out")
    return data_function.get_function_variation_data()["function_signature_hash"]


HASH_SITES = pytest.mark.parametrize(
    "site", [_value_hash, _stored_signature_hash], ids=["value", "stored"]
)


def _with_default(default: Any) -> Callable[..., Any]:
    """A function ``f(x: int, option=<default>) -> int``; only the default varies."""

    def f(x: int, option: Any = default) -> int:
        return x

    return f


def _without_default() -> Callable[..., Any]:
    """The same function as ``_with_default`` builds, with no default on ``option``."""

    def f(x: int, option: Any) -> int:
        return x

    return f


class _Unregistered:
    """A class the semantic hasher has no handler for."""


# ---------------------------------------------------------------------------
# Equal defaults hash equally
# ---------------------------------------------------------------------------


@HASH_SITES
class TestEqualDefaultsHashEqually:
    """Defaults that are equal as values give one hash, however they were built."""

    def test_dict_default_ignores_insertion_order(self, site: Callable[..., Any]):
        """``{"a": 1, "b": 2}`` and ``{"b": 2, "a": 1}`` are one default."""
        assert site(_with_default({"a": 1, "b": 2})) == site(
            _with_default({"b": 2, "a": 1})
        )

    def test_set_default_ignores_construction_order(self, site: Callable[..., Any]):
        """Two equal frozensets built in different orders are one default."""
        forward = frozenset(["w", "x", "y", "z", "alpha", "beta"])
        backward = frozenset(["beta", "alpha", "z", "y", "x", "w"])
        assert site(_with_default(forward)) == site(_with_default(backward))

    def test_same_default_twice(self, site: Callable[..., Any]):
        """Two functions that differ in nothing hash equally."""
        assert site(_with_default((1, "a", None))) == site(
            _with_default((1, "a", None))
        )


_SEED_SCRIPT = """
    from typing import Any

    from orcapod.contexts import resolve_context
    from orcapod.core.data_function import PythonDataFunction

    def f(x: int, option: Any = frozenset({"w", "x", "y", "z", "alpha", "beta"}),
          *, table: Any = {"b": {1, 2, 3}, "a": ("p", "q")}) -> int:
        return x

    value = resolve_context(None).semantic_hasher.hash_object(f).to_string()
    stored = PythonDataFunction(f, output_keys="out").get_function_variation_data()[
        "function_signature_hash"
    ]
    print(value, stored.hex())
"""


def _run_with_seed(script: str, seed: str) -> str:
    """Run ``script`` in a fresh interpreter under ``PYTHONHASHSEED=seed``; return stdout."""
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = seed
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        env=env,
        check=False,  # the assertion below reports stderr on failure
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


class TestAcrossProcessesAndHashSeeds:
    """Both hashes of a function are the same in every process and hash seed."""

    def test_set_and_dict_defaults_hash_equally_under_every_seed(self):
        """Both hashes of one function are the same in every process and hash seed."""
        outputs = {seed: _run_with_seed(_SEED_SCRIPT, seed) for seed in "01234567"}
        assert len(set(outputs.values())) == 1, outputs


# ---------------------------------------------------------------------------
# Different defaults hash differently
# ---------------------------------------------------------------------------


@HASH_SITES
class TestDifferentDefaultsHashDifferently:
    """Defaults that differ as values never share a hash."""

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            (1, 2),
            (1, "1"),
            (1, 1.5),
            (None, "None"),
            (None, 0),
            (False, 0),
            ((1, 2), [1, 2]),
            ((1, 2), (2, 1)),
            (frozenset({1, 2}), (1, 2)),
            ({"a": 1}, {"a": 2}),
            ("a", b"a"),
        ],
    )
    def test_distinct_values(self, site: Callable[..., Any], left: Any, right: Any):
        """Two functions that differ only in the default's value hash differently."""
        assert site(_with_default(left)) != site(_with_default(right))

    def test_default_none_differs_from_no_default(self, site: Callable[..., Any]):
        """``option=None`` and a required ``option`` are different signatures."""
        assert site(_with_default(None)) != site(_without_default())

    def test_default_cannot_spell_the_hash_of_another_default(
        self, site: Callable[..., Any]
    ):
        """A string default equal to another default's hash text is still its own value."""
        hasher = resolve_context(None).semantic_hasher
        hash_of_one = hasher.hash_object(1)
        for spelling in (hash_of_one.to_string(), hash_of_one.to_hex()):
            assert site(_with_default(spelling)) != site(_with_default(1))

    def test_default_cannot_spell_the_hashers_encoding(self, site: Callable[..., Any]):
        """A dict default that spells a tagged encoding is not the value it spells."""
        forged = {"__type__": "tuple", "items": [1, 2]}
        assert site(_with_default(forged)) != site(_with_default((1, 2)))

    def test_the_parameter_a_default_belongs_to_matters(self, site: Callable[..., Any]):
        """``a=1, b=2`` and ``a=2, b=1`` are different signatures."""

        def swapped_ab(x: int, a: int = 1, b: int = 2) -> int:
            return x

        first = swapped_ab

        def swapped_ab(x: int, a: int = 2, b: int = 1) -> int:  # noqa: F811  (same name on purpose)
            return x

        assert site(first) != site(swapped_ab)

    def test_same_repr_different_value(self, site: Callable[..., Any]):
        """Two defaults that print alike are still told apart by their value."""

        class Text(str):
            """A string whose ``repr()`` is that of the integer 1."""

            def __repr__(self) -> str:
                return "1"

        assert repr(Text("one")) == repr(1)
        assert site(_with_default(Text("one"))) != site(_with_default(1))


# ---------------------------------------------------------------------------
# A default the hasher cannot hash raises
# ---------------------------------------------------------------------------


class TestUnhashableDefaultRaises:
    """A default the semantic hasher cannot hash is an error, never a fallback."""

    def test_function_value_names_parameter_and_function(self):
        """Hashing the function names the parameter, the function and the type."""
        fn = _with_default(_Unregistered())
        with pytest.raises(TypeError) as excinfo:
            _value_hash(fn)
        message = str(excinfo.value)
        assert "'option'" in message
        assert fn.__qualname__ in message
        assert "_Unregistered" in message

    def test_data_function_fails_at_construction(self):
        """A data function with such a default cannot be built."""
        fn = _with_default(object())
        with pytest.raises(TypeError) as excinfo:
            PythonDataFunction(fn, output_keys="out")
        message = str(excinfo.value)
        assert "'option'" in message
        assert fn.__qualname__ in message

    def test_function_pod_decorator_fails_at_decoration(self):
        """``@function_pod`` raises when it is applied, naming the parameter."""
        with pytest.raises(TypeError, match="'marker'"):

            @function_pod(output_keys="out")
            def uses_marker(x: int, marker: Any = _Unregistered()) -> int:
                return x

    def test_unhashable_member_of_a_container_default_raises(self):
        """No fallback to ``repr()`` for a value nested inside a hashable container."""
        with pytest.raises(TypeError, match="'option'"):
            _value_hash(_with_default((1, _Unregistered())))

    def test_error_keeps_the_hashers_error_as_its_cause(self):
        """The hasher's own ``TypeError`` is chained as the cause."""
        with pytest.raises(TypeError) as excinfo:
            _value_hash(_with_default(_Unregistered()))
        assert isinstance(excinfo.value.__cause__, TypeError)

    def test_error_states_the_remedy(self):
        """The message says the default is the problem and how to register a handler."""
        with pytest.raises(TypeError) as excinfo:
            _value_hash(_with_default(_Unregistered()))
        message = str(excinfo.value)
        assert "PythonTypeHandlerProtocol" in message or "handler" in message
        assert "default" in message


# ---------------------------------------------------------------------------
# Defaults of common non-primitive types (decision D3: raise, or a value handler)
# ---------------------------------------------------------------------------


class _Colour(enum.Enum):
    """A plain enum, which the semantic hasher has no handler for."""

    RED = 1
    BLUE = 2


@dataclasses.dataclass(frozen=True)
class _Window:
    """A dataclass whose instances the semantic hasher has no handler for."""

    start: int
    stop: int


def _hash_or_error(site: Callable[..., Any], default: Any) -> Any:
    """The site's hash for a function with ``default``, or ``TypeError`` if it refuses."""
    try:
        return site(_with_default(default))
    except TypeError:
        return TypeError


_COMMON_TYPE_PAIRS = [
    pytest.param(lambda: _Colour.RED, lambda: _Colour.BLUE, id="enum"),
    pytest.param(lambda: Path("/data/a"), lambda: Path("/data/b"), id="path"),
    pytest.param(lambda: _Window(0, 10), lambda: _Window(0, 20), id="dataclass"),
    pytest.param(
        lambda: datetime.datetime(2026, 1, 1),
        lambda: datetime.datetime(2026, 1, 2),
        id="datetime",
    ),
    pytest.param(
        lambda: datetime.date(2026, 1, 1), lambda: datetime.date(2026, 1, 2), id="date"
    ),
    pytest.param(lambda: Decimal("1.5"), lambda: Decimal("1.50"), id="decimal"),
]


@HASH_SITES
@pytest.mark.parametrize(("make_left", "make_right"), _COMMON_TYPE_PAIRS)
class TestDefaultsOfCommonTypes:
    """Hold whichever way D3 is decided: such a default is refused or hashed by value."""

    def test_two_values_never_share_a_hash(
        self,
        site: Callable[..., Any],
        make_left: Callable[[], Any],
        make_right: Callable[[], Any],
    ):
        """Two different values are refused or get different hashes."""
        left, right = (
            _hash_or_error(site, make_left()),
            _hash_or_error(site, make_right()),
        )
        assert left is TypeError or right is TypeError or left != right

    def test_equal_values_agree(
        self,
        site: Callable[..., Any],
        make_left: Callable[[], Any],
        make_right: Callable[[], Any],
    ):
        """The same value built twice is refused both times or hashed alike."""
        assert _hash_or_error(site, make_left()) == _hash_or_error(site, make_left())


# ---------------------------------------------------------------------------
# Parameter kind (out-of-scope finding X; in scope only if D1 is decided as (b))
# ---------------------------------------------------------------------------


@HASH_SITES
class TestParameterKind:
    """``f(a, *, b)``, ``f(a, /, b)`` and ``f(a, b)`` are three signatures."""

    def test_keyword_only_marker(self, site: Callable[..., Any]):
        """``f(a, *, b)`` differs from ``f(a, b)``."""

        def kinds(a: int, b: int) -> int:
            return a

        plain = kinds

        def kinds(a: int, *, b: int) -> int:  # noqa: F811  (same name on purpose)
            return a

        assert site(plain) != site(kinds)

    def test_positional_only_marker(self, site: Callable[..., Any]):
        """``f(a, /, b)`` differs from ``f(a, b)``."""

        def kinds(a: int, b: int) -> int:
            return a

        plain = kinds

        def kinds(a: int, /, b: int) -> int:  # noqa: F811  (same name on purpose)
            return a

        assert site(plain) != site(kinds)


# ---------------------------------------------------------------------------
# Finding Y: an annotation containing "=" survives when defaults are left out
# ---------------------------------------------------------------------------


class TestSignatureTextWithoutDefaults:
    """``get_function_signature(include_defaults=False)`` keeps the whole annotation."""

    def test_annotation_containing_equals_is_kept(self):
        # get_function_signature is the public helper the stored hash is built from.
        """``Literal["a=b", "c"]`` survives and both defaults are left out."""
        from orcapod.hashing.hash_utils import get_function_signature

        def fn(mode: Literal["a=b", "c"] = "c", n: int = 3) -> int:
            return n

        text = get_function_signature(fn, include_defaults=False)
        assert "Literal['a=b', 'c']" in text
        assert "n: int" in text
        assert "= 3" not in text and "= 'c'" not in text


# ---------------------------------------------------------------------------
# A function pod's identity does not use the signature hash
# ---------------------------------------------------------------------------


class TestPodIdentityIgnoresDefaults:
    """Changing a default moves the signature hash and nothing a path is built from."""

    @staticmethod
    def _pod_output(factor: float) -> tuple[FunctionPod, Any]:
        def scale(x: int, factor: float = factor) -> float:
            return x * factor

        pod = FunctionPod.from_fn(scale, output_keys="scaled")
        source = DictSource(
            data=[{"k": "a", "x": 1}, {"k": "b", "x": 2}],
            tag_columns=["k"],
            source_id="defaults_source",
            tag_only_columns=True,
        )
        return pod, pod(source)

    def test_uri_pipeline_hash_and_content_hash_are_unchanged(self):
        """Two pods that differ only in a default share uri and both hashes."""
        pod_two, out_two = self._pod_output(2.0)
        pod_three, out_three = self._pod_output(3.0)

        assert pod_two.uri == pod_three.uri
        assert out_two.pipeline_hash() == out_three.pipeline_hash()
        assert out_two.content_hash() == out_three.content_hash()

    def test_stored_signature_hash_differs(self):
        """The same two pods store different signature hashes."""
        pod_two, _ = self._pod_output(2.0)
        pod_three, _ = self._pod_output(3.0)

        def signature_hash(pod: FunctionPod) -> bytes:
            variation = pod.data_function.get_function_variation_data()
            return variation["function_signature_hash"]

        assert signature_hash(pod_two) != signature_hash(pod_three)
