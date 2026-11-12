"""A function pod's returned values must match the declared output types (ITL-895).

pyarrow converts a Python ``float`` to an integer field by calling ``__int__``, so a pod
declared ``-> int`` that returned ``1.5`` stored ``1`` without any error. These tests
drive pods through the public interface (standalone streams, cached pods, ``PipelineJob``
on both orchestrators and both store backends) and check the developer's decisions
(2026-11-11, D1-D7 and ITL-901):

* a value that does not conform to its declared type is rejected with
  ``OutputTypeError`` naming the pod, the output, the declared type and the value's type,
  and saying what to return instead;
* the rejection is a row failure in a pipeline (the run continues, the observer sees it);
* only value-preserving numeric exceptions are accepted silently;
* compatible subclass objects remain intact in memory and store as their base;
* conforming outputs and same-kind NumPy scalars are accepted silently and hash as before.
"""

from __future__ import annotations

import collections
import datetime as dt
import enum
import types
import warnings
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pytest

from pydantic import BaseModel

from orcapod import PipelineJob, errors, function_pod
from orcapod.core.datagrams import Data
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.sources import DictSource


@dataclass
class Reading:
    """A struct-valued output with an integer field."""

    count: int


def make_pod(
    declared: Any, returns: Any, name: str = "f", *, async_function: bool = False
) -> Any:
    """Build a function pod named ``name``, declared ``-> declared``, returning ``returns``."""

    def fn(x: int) -> Any:
        return returns

    if async_function:

        async def async_fn(x: int) -> Any:
            return returns

        fn = async_fn

    fn.__annotations__["return"] = declared
    fn.__name__ = fn.__qualname__ = name
    return function_pod(output_keys="y")(fn)


def make_source(xs: tuple[int, ...] = (1,)) -> DictSource:
    """A source with one row per ``x`` (tag ``k``, data ``x: int``)."""
    return DictSource(
        data=[{"k": f"r{x}", "x": x} for x in xs], tag_columns=["k"], source_id="src"
    )


# (declared type, returned value, value type name expected in the message)
LOSSY = [
    pytest.param(int, 1.5, "float", id="float-for-int"),
    pytest.param(int, -1.5, "float", id="negative-float-for-int"),
    pytest.param(int, 2.0, "float", id="integral-float-for-int"),
    pytest.param(int, np.float64(1.5), "float64", id="np-float64-for-int"),
    pytest.param(int, Decimal("1.5"), "Decimal", id="decimal-for-int"),
    pytest.param(int, Fraction(3, 2), "Fraction", id="fraction-for-int"),
    pytest.param(np.int8, 1.5, "float", id="float-for-np-int8"),
    pytest.param(int | None, 1.5, "float", id="float-for-optional-int"),
    pytest.param(list[int], [1, 1.5], "float", id="float-in-list-of-int"),
    pytest.param(dict[str, int], {"a": 1.5}, "float", id="float-in-dict-of-int"),
    pytest.param(Reading, Reading(count=1.5), "float", id="float-in-dataclass-field"),  # pyright: ignore[reportArgumentType]  # the wrong field type is the point of the test
    pytest.param(dt.date, 1.5, "float", id="number-for-date"),
    pytest.param(dt.datetime, 1.5, "float", id="number-for-datetime"),
    pytest.param(float, 0, "int", id="D1-int-for-float"),
    pytest.param(float, np.int64(1), "int64", id="D1-np-int64-for-float"),
    pytest.param(np.float32, 1, "int", id="D1-int-for-np-float32"),
    pytest.param(float, True, "bool", id="D3-bool-for-float"),
    pytest.param(float, np.bool_(True), "bool", id="D3-np-bool-for-float"),
    pytest.param(list[int], {1, 2}, "set", id="ITL901-set-for-list"),
    pytest.param(
        list[int], frozenset({1}), "frozenset", id="ITL901-frozenset-for-list"
    ),
    pytest.param(list[int], iter([1, 2]), "iterator", id="Q2-iterator-for-list"),
    pytest.param(
        list[int], (i for i in range(2)), "generator", id="Q2-generator-for-list"
    ),
    pytest.param(
        list[int],
        np.array([1.5, 2.0]),
        "ndarray",
        id="Q2-float-ndarray-for-list-of-int",
    ),
    pytest.param(
        list[int], collections.deque([1, 1.5]), "deque", id="Q2-float-in-deque"
    ),
    pytest.param(list[int], {"a": 1}, "dict", id="mapping-for-list"),
    pytest.param(str, b"x", "bytes", id="bytes-for-str"),
    pytest.param(bytes, "x", "str", id="str-for-bytes"),
    pytest.param(int, None, "NoneType", id="none-for-int"),
    pytest.param(float, None, "NoneType", id="none-for-float"),
    pytest.param(list[int], None, "NoneType", id="none-for-list"),
]


class TestStandalonePodRejects:
    """Rejected values raise ``OutputTypeError`` from a pod applied to a stream."""

    @pytest.mark.parametrize(("declared", "returned", "type_name"), LOSSY)
    def test_iter_data_raises_naming_pod_output_and_types(
        self, declared: Any, returned: Any, type_name: str
    ) -> None:
        """The message names the function, the output and the returned type."""
        pod = make_pod(declared, returned, name="lossy_fn")
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        message = str(info.value)
        assert "'lossy_fn'" in message
        assert "'y'" in message
        assert type_name in message

    def test_error_records_what_was_returned(self) -> None:
        """The error keeps the function, output, declared type and value."""
        pod = make_pod(int, 1.5, name="half")
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        assert info.value.function_name == "half"
        assert info.value.output_key == "y"
        assert info.value.declared_type is int
        assert info.value.value == 1.5

    @pytest.mark.parametrize(
        ("declared", "returned", "secret"),
        [
            pytest.param(bytes, "secret-token", "secret-token", id="top-level"),
            pytest.param(
                list[bytes], [b"ok", "secret-token"], "secret-token", id="nested"
            ),
        ],
    )
    def test_message_never_shows_the_value(
        self, declared: Any, returned: Any, secret: str
    ) -> None:
        """Pipelines log the error, so the message names types only; the value stays on
        the error's ``value`` attribute."""
        pod = make_pod(declared, returned)
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        assert secret not in str(info.value)
        assert info.value.value == returned

    def test_long_dict_key_is_omitted_from_the_path(self) -> None:
        """Mapping paths contain structure only, never returned key content."""
        key = "k" * 100
        pod = make_pod(dict[str, int], {key: 1.5})
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        message = str(info.value)
        assert key not in message
        assert "kkkk" not in message
        assert "[*]" in message

    def test_error_is_a_type_error(self) -> None:
        """Existing ``except TypeError`` handlers catch the rejection."""
        pod = make_pod(int, 1.5)
        with pytest.raises(TypeError):
            list(pod.pod(make_source()).iter_data())

    @pytest.mark.parametrize(
        ("declared", "returned", "hint"),
        [
            pytest.param(
                float, 0, "Return a float instead: 0.0 or float(value)", id="D1"
            ),
            pytest.param(
                float, True, "Return a float instead: 0.0 or float(value)", id="D3"
            ),
            pytest.param(
                int, 1.5, "Return an int instead: round(value)", id="float-for-int"
            ),
            pytest.param(list[int], {2, 1}, "sorted(value)", id="ITL901"),
            pytest.param(list[int], iter([1]), "list(value)", id="iterator"),
            pytest.param(int, None, "int | None", id="D4"),
        ],
    )
    def test_message_says_what_to_return_instead(
        self, declared: Any, returned: Any, hint: str
    ) -> None:
        """Each decided rejection carries a hint naming the fix."""
        pod = make_pod(declared, returned)
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        assert hint in str(info.value)

    def test_as_table_names_the_output_instead_of_a_union_error(self) -> None:
        """Mixed int / float results used to fail in as_table with a "Complex unions" error."""

        @function_pod(output_keys="y")
        def sometimes_half(x: int) -> int:
            return x if x == 1 else x + 0.5  # pyright: ignore[reportReturnType]  # the wrong return type is the point of the test

        with pytest.raises(errors.OutputTypeError, match="'y'"):
            sometimes_half.pod(make_source((1, 2))).as_table()

    def test_async_function_rejects(self) -> None:
        """An async function's returned value is checked the same way."""

        @function_pod(output_keys="y")
        async def async_half(x: int) -> int:
            return x + 0.5  # pyright: ignore[reportReturnType]  # the wrong return type is the point of the test

        with pytest.raises(errors.OutputTypeError, match="async_half"):
            list(async_half.pod(make_source()).iter_data())


class TestCachedPodRejects:
    """A pod with a ``result_database`` stores nothing for a rejected value."""

    def test_result_database_stores_nothing(self) -> None:
        """A rejected value is not stored, so the next pod calls the function again."""
        db = InMemoryArrowDatabase()

        @function_pod(output_keys="y", result_database=db)
        def half(x: int) -> int:
            return x + 0.5  # pyright: ignore[reportReturnType]  # the wrong return type is the point of the test

        with pytest.raises(errors.OutputTypeError):
            half.pod(make_source()).as_table()
        # Nothing was stored: a second pod over the same database still calls the function
        # and still raises (a stored truncated row would be served instead).
        with pytest.raises(errors.OutputTypeError):
            half.pod(make_source()).as_table()


class _CrashRecorder(NoOpObserver):
    """An observer that records the errors of failed rows."""

    def __init__(self) -> None:
        """Start with no recorded errors."""
        self.crashes: list[BaseException] = []

    def contextualize(self, *identity_path: str) -> _CrashRecorder:
        """Use the same recorder for every node."""
        return self

    def on_data_crash(self, node_label: Any, tag: Any, data: Any, error: Any) -> None:
        """Record the error of a failed row."""
        self.crashes.append(error)


def _half_on_even(x: int) -> int:
    """Return an int for odd ``x`` and a float (wrong) for even ``x``."""
    return x + 0.5 if x % 2 == 0 else x  # pyright: ignore[reportReturnType]  # the wrong return type is the point of the test


def _store(kind: str, tmp_path: Path) -> Any:
    """A Delta store under ``tmp_path`` for ``"delta"``, else an in-memory store."""
    return (
        DeltaTableDatabase(tmp_path / "db")
        if kind == "delta"
        else InMemoryArrowDatabase()
    )


ORCHESTRATORS = [
    pytest.param(SyncPipelineOrchestrator, id="sync"),
    pytest.param(AsyncPipelineOrchestrator, id="async"),
]
STORES = [pytest.param("memory", id="memory"), pytest.param("delta", id="delta")]


class TestPipelineJobRowFailure:
    """In a ``PipelineJob`` a rejected value is a row failure."""

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    def test_bad_row_fails_and_good_rows_are_stored(
        self, orchestrator_cls: Any, store_kind: str, tmp_path: Path
    ) -> None:
        """Only the rejected row fails; the observer gets the ``OutputTypeError``."""
        half = function_pod(output_keys="y")(_half_on_even)
        observer = _CrashRecorder()
        job = PipelineJob(name="itl895", store=_store(store_kind, tmp_path))
        with job:
            half.pod(make_source((1, 2, 3)), label="half")
        job.run(orchestrator=orchestrator_cls(), observer=observer)

        table = job.nodes["half"].as_table()
        assert sorted(zip(table["k"].to_pylist(), table["y"].to_pylist())) == [
            ("r1", 1),
            ("r3", 3),
        ]
        assert len(observer.crashes) == 1
        assert isinstance(observer.crashes[0], errors.OutputTypeError)

    def test_fail_fast_raises_the_output_type_error(self) -> None:
        """Under ``fail_fast`` the sync orchestrator raises the rejection."""
        half = function_pod(output_keys="y")(_half_on_even)
        job = PipelineJob(name="itl895", store=InMemoryArrowDatabase())
        with job:
            half.pod(make_source((1, 2, 3)), label="half")
        with pytest.raises(errors.OutputTypeError):
            job.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    def test_none_for_non_optional_output_is_a_row_failure(
        self, orchestrator_cls: Any
    ) -> None:
        """A NULL used to be stored in a not-null field, leaving the node unreadable."""
        pod = make_pod(int, None, name="nothing")
        job = PipelineJob(name="itl895", store=InMemoryArrowDatabase())
        with job:
            pod.pod(make_source((1,)), label="nothing")
        job.run(orchestrator=orchestrator_cls())
        assert job.nodes["nothing"].as_table().num_rows == 0


# Developer decision, November 12: former warning conversions now fail.
REJECTED_CONVERSIONS = [
    pytest.param(list[int], (1, 2), "list(value)", id="tuple"),
    pytest.param(list[int], np.array([1, 2]), "list(value)", id="array"),
    pytest.param(list[int], range(2), "list(value)", id="range"),
    pytest.param(list[int], collections.deque([1, 2]), "list(value)", id="deque"),
    pytest.param(list[int], {"a": 1}.values(), "list(value)", id="dict-view"),
    pytest.param(np.float32, 0.1, "numpy.float32(value)", id="python-float-inexact"),
    pytest.param(np.float32, 0.5, "numpy.float32(value)", id="python-float-exact"),
    pytest.param(np.float32, np.float64(0.1), "numpy.float32(value)", id="float64"),
    pytest.param(np.int8, 128, "numpy.int8", id="int8-overflow"),
    pytest.param(np.int8, -129, "numpy.int8", id="int8-underflow"),
    pytest.param(np.int8, np.int64(128), "numpy.int8", id="numpy-int8-overflow"),
    pytest.param(bool, 1, "bool", id="int-for-bool"),
    pytest.param(
        float,
        np.longdouble(1.5),
        "float(value)",
        id="longdouble",
        marks=pytest.mark.skipif(
            np.dtype(np.longdouble).itemsize <= 8,
            reason="longdouble is not wider than float64 on this platform",
        ),
    ),
]


class TestRejectedConversions:
    """Former warning conversions fail at the same boundary on every path."""

    @pytest.mark.parametrize("async_function", [False, True])
    @pytest.mark.parametrize("declared,returned,hint", REJECTED_CONVERSIONS)
    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    def test_rejects_on_all_paths(
        self,
        declared: Any,
        returned: Any,
        hint: str,
        orchestrator_cls: Any,
        store_kind: str,
        tmp_path: Path,
        async_function: bool,
    ) -> None:
        """No rejected row is stored; observers receive the standalone diagnostic."""
        pod = make_pod(
            declared,
            returned,
            name="requires_explicit_conversion",
            async_function=async_function,
        )
        with pytest.raises(errors.OutputTypeError) as caught:
            list(pod.pod(make_source()).iter_data())
        assert hint in str(caught.value)
        with pytest.raises(errors.OutputTypeError) as materialized:
            pod.pod(make_source()).as_table()
        assert str(materialized.value) == str(caught.value)
        recorder = _CrashRecorder()
        job = PipelineJob(name="policy", store=_store(store_kind, tmp_path))
        with job:
            pod.pod(make_source(), label="bad")
        job.run(orchestrator=orchestrator_cls(), observer=recorder)
        assert len(recorder.crashes) == 1
        assert type(recorder.crashes[0]) is errors.OutputTypeError
        assert str(recorder.crashes[0]) == str(caught.value)
        assert job.nodes["bad"].as_table().num_rows == 0


class Count(enum.IntEnum):
    """A value-preserving integer subclass."""

    ONE = 1


class ScientificFloat(float):
    """A float subclass with unchanged numeric value."""


ACCEPTED_SILENTLY = [
    pytest.param(int, Count.ONE, Count.ONE, id="int-enum"),
    pytest.param(
        float, ScientificFloat(1.5), ScientificFloat(1.5), id="float-subclass"
    ),
    pytest.param(int, np.int64(3), 3, id="D2-np-int64-for-int"),
    pytest.param(bool, np.bool_(True), True, id="D2-np-bool-for-bool"),
    pytest.param(float, np.float64(1.5), np.float64(1.5), id="D2-np-float64-for-float"),
    pytest.param(float, np.float32(0.5), 0.5, id="D2-np-float32-for-float"),
    pytest.param(np.int8, np.int64(3), 3, id="D2-np-int64-for-np-int8"),
    pytest.param(
        np.float32, np.float32(0.5), np.float32(0.5), id="np-float32-for-np-float32"
    ),
    pytest.param(np.float32, np.float16(0.5), 0.5, id="D2-np-float16-for-np-float32"),
    pytest.param(int, 3, 3, id="int-for-int"),
    pytest.param(list[int], [1, 2], [1, 2], id="list-for-list"),
    pytest.param(dt.date, dt.date(2020, 1, 1), dt.date(2020, 1, 1), id="date-for-date"),
]


class TestAcceptedSilently:
    """Same-kind NumPy scalars and conforming values: no warning, value stored."""

    @pytest.mark.parametrize(("declared", "returned", "stored"), ACCEPTED_SILENTLY)
    def test_no_warning_and_value_stored(
        self, declared: Any, returned: Any, stored: Any
    ) -> None:
        """No warning and the value is stored as returned."""
        pod = make_pod(declared, returned)
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            table = pod.pod(make_source()).as_table()
        assert not record
        assert table["y"].to_pylist() == [stored]


# (declared type, returned value, value a downstream pod receives)
DOWNSTREAM = ACCEPTED_SILENTLY + [
    pytest.param(np.int8, 127, 127, id="int8-upper-bound"),
    pytest.param(np.int8, -128, -128, id="int8-lower-bound"),
    pytest.param(
        float, np.float32(0.1), float(np.float32(0.1)), id="exact-float32-widening"
    ),
]


class TestDownstreamSeesStoredValue:
    """The output Data must hold what is stored: no as_dict / as_table divergence."""

    def _chain(
        self, declared: Any, returned: Any, received: list[Any]
    ) -> tuple[Any, Any]:
        """Build ``up`` (returns ``returned`` for ``declared``) feeding ``down``, which records its input."""
        up = make_pod(declared, returned, name="up")

        def down(y: Any) -> int:
            received.append(y)
            return 1

        down.__annotations__["y"] = declared
        return up, function_pod(output_keys="z")(down)

    @staticmethod
    def _assert_received(received: list[Any], expected: Any) -> None:
        """``down`` received exactly ``expected``, with its type."""
        assert received == [expected]
        assert type(received[0]) is type(expected)

    @pytest.mark.parametrize(("declared", "returned", "expected"), DOWNSTREAM)
    def test_standalone(self, declared: Any, returned: Any, expected: Any) -> None:
        """A downstream pod applied in memory receives the converted value."""
        received: list[Any] = []
        up, down = self._chain(declared, returned, received)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            list(down.pod(up.pod(make_source())).iter_data())
        self._assert_received(received, expected)

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    @pytest.mark.parametrize(("declared", "returned", "expected"), DOWNSTREAM)
    def test_pipeline_job(
        self,
        declared: Any,
        returned: Any,
        expected: Any,
        orchestrator_cls: Any,
        store_kind: str,
        tmp_path: Path,
    ) -> None:
        """On both orchestrators and stores, downstream receives the stored, converted value."""
        received: list[Any] = []
        up, down = self._chain(declared, returned, received)
        job = PipelineJob(name="itl895", store=_store(store_kind, tmp_path))
        with job:
            down.pod(up.pod(make_source(), label="up"), label="down")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            job.run(orchestrator=orchestrator_cls())
        # Stored rows decode numeric subclasses using their declared storage type.
        assert received == [expected]
        stored = job.nodes["up"].as_table()["y"].to_pylist()
        assert stored == [expected]

    @pytest.mark.parametrize(("declared", "returned", "expected"), DOWNSTREAM)
    def test_output_as_dict_matches_as_table(
        self, declared: Any, returned: Any, expected: Any
    ) -> None:
        """The output ``Data`` gives the same converted value as a dict and as a table."""
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            [(_tag, data)] = list(
                make_pod(declared, returned).pod(make_source()).iter_data()
            )
        assert data.as_dict() == data.as_table().to_pylist()[0]
        assert data.as_dict()["y"] == expected


class TestRejectionInPipelineJob:
    """Every decided rejection is a row failure on both orchestrators and both stores."""

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    @pytest.mark.parametrize(
        ("declared", "returned"),
        [
            pytest.param(float, 0, id="D1"),
            pytest.param(float, True, id="D3"),
            pytest.param(list[int], {1, 2}, id="ITL901"),
        ],
    )
    def test_row_failure_and_nothing_stored(
        self,
        declared: Any,
        returned: Any,
        orchestrator_cls: Any,
        store_kind: str,
        tmp_path: Path,
    ) -> None:
        """D1, D3 and ITL-901 rejections fail the row and store nothing."""
        pod = make_pod(declared, returned, name="rejected")
        observer = _CrashRecorder()
        job = PipelineJob(name="itl895", store=_store(store_kind, tmp_path))
        with job:
            pod.pod(make_source((1,)), label="rejected")
        job.run(orchestrator=orchestrator_cls(), observer=observer)
        assert job.nodes["rejected"].as_table().num_rows == 0
        assert len(observer.crashes) == 1
        assert isinstance(observer.crashes[0], errors.OutputTypeError)


class TestConformingOutputsUnchanged:
    """Conforming outputs are stored and hashed as before ITL-895."""

    def test_content_hash_matches_a_plain_data(self) -> None:
        """A conforming output hashes like ``Data`` built from the same value."""
        pod = make_pod(int, 1)
        [(_tag, data)] = list(pod.pod(make_source()).iter_data())
        expected = Data({"y": 1}, python_schema={"y": int}).content_hash()
        assert data.content_hash() == expected

    @pytest.mark.parametrize(
        ("declared", "returned"),
        [
            (int, 3),
            (float, 1.5),
            (bool, True),
            (str, "x"),
            (bytes, b"x"),
            (list[int], [1, 2]),
            (dict[str, int], {"a": 1}),
            (int | None, None),
            (dt.date, dt.date(2020, 1, 1)),
            (Reading, Reading(count=2)),
        ],
    )
    def test_conforming_value_round_trips(self, declared: Any, returned: Any) -> None:
        """A conforming value comes back unchanged from ``as_dict``."""
        pod = make_pod(declared, returned)
        [(_tag, data)] = list(pod.pod(make_source()).iter_data())
        assert data.as_dict()["y"] == returned


class TestConversionFailuresNameTheOutput:
    """A value of the right type that cannot be stored is reported the same way."""

    @pytest.mark.parametrize(
        ("declared", "returned"),
        [
            pytest.param(np.int8, 300, id="out-of-range-for-int8"),
            pytest.param(dt.datetime, dt.datetime(2020, 1, 1), id="naive-datetime"),
        ],
    )
    def test_raises_output_type_error_that_is_a_value_error(
        self, declared: Any, returned: Any
    ) -> None:
        """A conversion failure names the output and is still a ``ValueError``."""
        pod = make_pod(declared, returned, name="unstorable")
        with pytest.raises(errors.OutputTypeError, match="'unstorable'") as info:
            list(pod.pod(make_source()).iter_data())
        assert isinstance(info.value, ValueError)
        assert info.value.output_key == "y"

    def test_error_survives_pickling(self) -> None:
        """The error pickles with its arguments (executors send it across processes)."""
        import pickle

        error = errors.OutputTypeError("f", "y", int, 1.5)
        restored = pickle.loads(pickle.dumps(error))
        assert str(restored) == str(error)
        assert restored.declared_type is int


@dataclass
class ExtendedReading(Reading):
    """A subclass adds a field that is omitted when stored as its base."""

    extra: int = 2


class TestUnifiedOutputBoundary:
    """Every execution path reports the same function-output mismatch."""

    @pytest.mark.parametrize(
        ("declared", "returned"),
        [(int, 1.5), (Reading, object()), (np.ndarray, np.float64(2))],
    )
    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    def test_errors_match_standalone_and_stored(
        self,
        declared: Any,
        returned: Any,
        orchestrator_cls: Any,
        store_kind: str,
        tmp_path: Path,
    ) -> None:
        """Invalid outputs never reach storage and carry one identical diagnostic."""
        pod = make_pod(declared, returned, name="invalid_output")
        with pytest.raises(errors.OutputTypeError) as standalone:
            list(pod.pod(make_source()).iter_data())
        with pytest.raises(errors.OutputTypeError) as table:
            pod.pod(make_source()).as_table()
        observer = _CrashRecorder()
        job = PipelineJob(name="unified", store=_store(store_kind, tmp_path))
        with job:
            pod.pod(make_source(), label="invalid")
        job.run(orchestrator=orchestrator_cls(), observer=observer)
        assert len(observer.crashes) == 1
        assert type(observer.crashes[0]) is errors.OutputTypeError
        assert str(observer.crashes[0]) == str(standalone.value) == str(table.value)
        assert job.nodes["invalid"].as_table().num_rows == 0
        assert "Cannot store" not in str(standalone.value)

    def test_structural_equivalence_still_checks_fields(self) -> None:
        """Equivalent structural classes share the same recursive output checks."""
        import orcapod as op

        @op.dataclass(name="OutputReading")
        class First:
            count: int

        @op.dataclass(name="OutputReading")
        class Second:
            count: int

        pod = make_pod(First, Second(1.5))  # pyright: ignore[reportArgumentType]  # deliberately invalid field
        with pytest.raises(errors.OutputTypeError, match="count.*float"):
            list(pod.pod(make_source()).iter_data())
        good = make_pod(First, Second(2))
        assert list(good.pod(make_source()).iter_data())[0][1].as_dict()["y"].count == 2

    def test_mapping_keys_are_not_disclosed(self) -> None:
        """A mapping key is returned data, not diagnostic context."""
        pod = make_pod(dict[str, int], {"secret-token": 1.5})
        with pytest.raises(errors.OutputTypeError) as caught:
            list(pod.pod(make_source()).iter_data())
        assert "secret-token" not in str(caught.value)
        assert "[*]" in str(caught.value)

    def test_converter_error_traceback_does_not_disclose_value(self) -> None:
        """Converter diagnostics cannot leak a returned datetime through chaining."""
        import traceback

        pod = make_pod(dt.datetime, dt.datetime(2047, 8, 19, 3, 4, 5))
        with pytest.raises(errors.OutputTypeError) as caught:
            list(pod.pod(make_source()).iter_data())
        diagnostic = "".join(traceback.format_exception(caught.value))
        assert "2047" not in diagnostic
        assert caught.value.__suppress_context__


def test_eager_validation_preserves_user_object_identity() -> None:
    """Storage validation never rebuilds or mutates a compatible user object."""
    original = Reading(np.int64(3))  # pyright: ignore[reportArgumentType]  # exercise the accepted same-kind numeric exception
    result = make_pod(Reading, original).pod(make_source())
    [(_, data)] = list(result.iter_data())
    assert data.as_dict()["y"] is original
    assert type(original.count) is np.int64
    result.as_table()
    assert data.as_dict()["y"] is original


class TestSubclassOutputs:
    """Subclass objects survive in memory while storage uses the declared schema."""

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    @pytest.mark.parametrize("async_function", [False, True])
    def test_subclass_fields_and_hash(
        self,
        orchestrator_cls: Any,
        store_kind: str,
        async_function: bool,
        tmp_path: Path,
    ) -> None:
        """Only declared base fields enter stored data and its content hash."""
        original = ExtendedReading(1, 20)
        first = make_pod(Reading, original, async_function=async_function)
        second = make_pod(
            Reading, ExtendedReading(1, 99), async_function=async_function
        )
        [(_, a)] = list(first.pod(make_source()).iter_data())
        [(_, b)] = list(second.pod(make_source()).iter_data())
        assert a.as_dict()["y"] is original
        assert a.content_hash() == b.content_hash()
        received: list[Any] = []

        @function_pod(output_keys="count")
        def inspect_reading(y: Reading) -> int:
            received.append(y)
            return y.count

        list(inspect_reading.pod(first.pod(make_source())).iter_data())
        assert received[0] is original
        job = PipelineJob(name="subclass", store=_store(store_kind, tmp_path))
        with job:
            first.pod(make_source(), label="base")
        job.run(orchestrator=orchestrator_cls())
        table = job.nodes["base"].get_all_records()
        assert table is not None
        assert table.schema.field("y").type.storage_type.names == ["count"]
        stored = Data(table.select(["y"]))
        assert type(stored.as_dict()["y"]) is Reading
        decoded = stored.as_dict()["y"]
        assert isinstance(decoded, Reading)
        assert decoded.count == 1

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    @pytest.mark.parametrize("store_kind", STORES)
    @pytest.mark.parametrize(
        "declared,returned,stored",
        [
            (int, True, 1),
            (dt.date, dt.datetime(2020, 1, 1, 12), dt.date(2020, 1, 1)),
        ],
    )
    def test_builtin_subclasses(
        self,
        declared: Any,
        returned: Any,
        stored: Any,
        orchestrator_cls: Any,
        store_kind: str,
        tmp_path: Path,
    ) -> None:
        """Python subclassing takes precedence over numeric-kind special cases."""
        pod = make_pod(declared, returned)
        [(_, data)] = list(pod.pod(make_source()).iter_data())
        assert data.as_dict()["y"] is returned
        assert pod.pod(make_source()).as_table()["y"].to_pylist() == [stored]
        job = PipelineJob(name="builtin_subclass", store=_store(store_kind, tmp_path))
        with job:
            pod.pod(make_source(), label="result")
        job.run(orchestrator=orchestrator_cls())
        assert job.nodes["result"].as_table()["y"].to_pylist() == [stored]


@dataclass
class NestedReading:
    """A declared dataclass field containing another declared dataclass."""

    inner: Reading


class ModelReading(BaseModel):
    """A pydantic base output."""

    count: int


class ExtendedModelReading(ModelReading):
    """A pydantic subclass with an extra field."""

    extra: int


@pytest.mark.parametrize("async_function", [False, True])
@pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("shape", ["list", "optional", "nested", "pydantic"])
def test_declared_saver_for_nested_subclasses(
    async_function: bool,
    orchestrator_cls: Any,
    store_kind: str,
    shape: str,
    tmp_path: Path,
) -> None:
    """Declared field schemas select the base saver recursively on every path."""
    cases: dict[str, tuple[Any, Any]] = {
        "list": (list[Reading], [ExtendedReading(3, 9)]),
        "optional": (Reading | None, ExtendedReading(3, 9)),
        "nested": (NestedReading, NestedReading(ExtendedReading(3, 9))),
        "pydantic": (ModelReading, ExtendedModelReading(count=3, extra=9)),
    }
    declared, original = cases[shape]
    pod = make_pod(declared, original, async_function=async_function)
    [(_, data)] = list(pod.pod(make_source()).iter_data())
    if shape == "list":
        assert data.as_dict()["y"][0] is original[0]
    else:
        assert data.as_dict()["y"] is original
    job = PipelineJob(name="nested_subclass", store=_store(store_kind, tmp_path))
    with job:
        pod.pod(make_source(), label="base")
    job.run(orchestrator=orchestrator_cls())
    table = job.nodes["base"].get_all_records()
    assert table is not None
    arrow_type = table.schema.field("y").type.storage_type
    decoded = cast(Any, Data(table.select(["y"])).as_dict()["y"])
    if shape == "list":
        arrow_type, decoded = arrow_type.value_type, decoded[0]
    elif shape == "nested":
        arrow_type, decoded = arrow_type.field("inner").type, decoded.inner
    assert arrow_type.names == ["count"]
    assert type(decoded) is (ModelReading if shape == "pydantic" else Reading)
    assert decoded.count == 3


@pytest.mark.parametrize("async_function", [False, True])
def test_subclass_overridden_field_is_rejected(async_function: bool) -> None:
    """A subclass cannot override the declared base field's value contract."""

    @dataclass
    class WrongReading(Reading):
        count: str = "wrong"  # pyright: ignore[reportIncompatibleVariableOverride]  # deliberately incompatible subclass field

    pod = make_pod(Reading, WrongReading(), async_function=async_function)
    with pytest.raises(errors.OutputTypeError, match="count.*str") as memory:
        list(pod.pod(make_source()).iter_data())
    with pytest.raises(errors.OutputTypeError) as stored:
        pod.pod(make_source()).as_table()
    assert str(memory.value) == str(stored.value)


def test_inferred_source_uses_value_class() -> None:
    """Without a declared base schema, the source retains the subclass fields."""
    source = DictSource(
        data=[{"y": cast(Any, ExtendedReading(3, 9))}],
        tag_columns=[],
        source_id="subclass_source",
    )
    table = source.as_table()
    assert table.schema.field("y").type.storage_type.names == ["count", "extra"]
    assert type(Data(table.select(["y"])).as_dict()["y"]) is ExtendedReading


class TestDeclaredDict:
    """A declared ``dict`` takes a ``dict`` instance; other mappings are unrelated types."""

    @pytest.mark.parametrize(
        ("returned", "type_name"),
        [
            pytest.param(types.MappingProxyType({"a": 1}), "mappingproxy", id="proxy"),
            pytest.param(collections.UserDict(a=1), "UserDict", id="user-dict"),
        ],
    )
    def test_non_dict_mapping_is_rejected(self, returned: Any, type_name: str) -> None:
        """A mapping that is not a ``dict`` needs an explicit ``dict(value)``."""
        pod = make_pod(dict[str, int], returned)
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        assert type_name in str(info.value)

    def test_dict_subclass_is_accepted(self) -> None:
        """A ``dict`` subclass stands in for ``dict``."""
        pod = make_pod(dict[str, int], collections.OrderedDict(a=1))
        [(_, data)] = list(pod.pod(make_source()).iter_data())
        assert data.as_dict()["y"] == {"a": 1}


class TestDeclaredLiteral:
    """A declared ``Literal`` takes exactly its members: same type and same value."""

    @pytest.mark.parametrize(
        ("declared", "returned", "type_name"),
        [
            pytest.param(Literal[1], 1.5, "float", id="float-for-int-member"),
            pytest.param(Literal[1], 2, "int", id="other-int"),
            pytest.param(Literal["yes"], "no", "str", id="other-str"),
            pytest.param(Literal[1], True, "bool", id="bool-equal-to-member"),
            pytest.param(Literal[1], np.int64(1), "int64", id="numpy-equal-to-member"),
            pytest.param(Literal["yes"], 3, "int", id="other-type"),
            pytest.param(Literal["yes"], None, "NoneType", id="none-not-a-member"),
            pytest.param(list[Literal["yes"]], ["yes", "no"], "str", id="nested"),
        ],
    )
    def test_non_member_is_rejected(
        self, declared: Any, returned: Any, type_name: str
    ) -> None:
        """A value outside the members raises, naming the returned type."""
        pod = make_pod(declared, returned, name="literal_fn")
        with pytest.raises(errors.OutputTypeError) as info:
            list(pod.pod(make_source()).iter_data())
        message = str(info.value)
        assert "'literal_fn'" in message
        assert type_name in message
        assert "ArrowTypeError" not in message

    @pytest.mark.parametrize(
        ("declared", "returned"),
        [
            pytest.param(Literal["yes"], "yes", id="str-member"),
            pytest.param(Literal[1, 2], 2, id="int-member"),
            pytest.param(Literal["yes", None], None, id="none-member"),
            pytest.param(Literal["yes", None], "yes", id="member-beside-none"),
        ],
    )
    def test_member_is_accepted(self, declared: Any, returned: Any) -> None:
        """A member is passed on unchanged."""
        pod = make_pod(declared, returned)
        [(_, data)] = list(pod.pod(make_source()).iter_data())
        assert data.as_dict()["y"] == returned
        assert type(data.as_dict()["y"]) is type(returned)

    @pytest.mark.parametrize("orchestrator_cls", ORCHESTRATORS)
    def test_non_member_is_never_stored(self, orchestrator_cls: Any) -> None:
        """``1.5`` for ``Literal[1]`` is a row failure, not a stored ``1``."""
        job = PipelineJob(name="literal", store=InMemoryArrowDatabase())
        with job:
            make_pod(Literal[1], 1.5).pod(make_source(), label="node")
        job.run(orchestrator=orchestrator_cls())
        table = job.nodes["node"].get_all_records()
        assert table is None or table.num_rows == 0


@function_pod(output_keys="y")
def make_extended_reading(x: int) -> Reading:
    """Declared ``Reading``; returns a subclass whose extra field varies with the input."""
    return ExtendedReading(1, x)


@function_pod(output_keys="z")
def read_extra_field(y: Reading) -> int:
    """Reads a field its declared parameter type does not have."""
    return getattr(y, "extra", -1)


class TestSubclassOnlyFieldsDownstream:
    """Baseline for ITL-978 (design issue ITL-895-FO3): today's behaviour when a downstream
    function reads a subclass-only field. The row hash covers the declared base fields only, so the
    result depends on the execution path and is reused across values. These tests pin what
    happens now; they do not state what should happen."""

    @staticmethod
    def _run(store: Any, xs: tuple[int, ...], orchestrator_cls: Any) -> dict[str, int]:
        job = PipelineJob(name="extra", store=store)
        with job:
            made = make_extended_reading.pod(make_source(xs), label="make")
            read_extra_field.pod(made, label="down")
        job.run(orchestrator=orchestrator_cls())
        table = job.nodes["down"].get_all_records()
        assert table is not None
        return {row["k"]: row["z"] for row in table.select(["k", "z"]).to_pylist()}

    def test_standalone_passes_the_subclass_object(self) -> None:
        """In memory the downstream function receives each subclass object."""
        stream = read_extra_field.pod(make_extended_reading.pod(make_source((20, 99))))
        rows = {
            tag.as_dict()["k"]: data.as_dict()["z"] for tag, data in stream.iter_data()
        }
        assert rows == {"r20": 20, "r99": 99}

    def test_sync_orchestrator_passes_the_stored_base(self) -> None:
        """The sync orchestrator hands on the stored ``Reading``: no extra field."""
        rows = self._run(InMemoryArrowDatabase(), (20, 99), SyncPipelineOrchestrator)
        assert rows == {"r20": -1, "r99": -1}

    def test_async_orchestrator_result_depends_on_timing(self) -> None:
        """The async orchestrator hands on the in-memory subclass object, and the two
        rows share one row hash: each gets its own extra field, or the result the other
        row stored first. Which one is a matter of timing, so only the set is pinned."""
        rows = self._run(InMemoryArrowDatabase(), (20, 99), AsyncPipelineOrchestrator)
        assert set(rows) == {"r20", "r99"}
        assert set(rows.values()) <= {20, 99}

    @pytest.mark.parametrize("second_cls", ORCHESTRATORS)
    def test_second_run_reuses_the_result_of_another_value(
        self, second_cls: Any
    ) -> None:
        """A later row whose base fields match is served the first row's result."""
        store = InMemoryArrowDatabase()
        assert self._run(store, (20,), AsyncPipelineOrchestrator) == {"r20": 20}
        assert self._run(store, (20, 99), second_cls) == {"r20": 20, "r99": 20}
