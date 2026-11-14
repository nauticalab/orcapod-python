"""Library value types warn that their hashes are provisional (ITL-1068).

The value hashes and stored identities of the library value types (numpy, torch,
pandas, polars) are not yet stable across releases; they are audited in v0.4. Until
then each of them issues ``ProvisionalHashWarning`` once per type per process, on
whichever path first uses it. Which types are provisional is declared once, in the
``provisional`` list of ``contexts/logical_types.json``.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import warnings
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
import torch

import orcapod.contexts as contexts_package
from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.contexts.core import DataContext
from orcapod.contexts.registry import JSONDataContextRegistry
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import ProvisionalHashWarning

# The test hook that forgets which types have warned; not a public entry point.
from orcapod.logical_types.provisional import reset_provisional_warnings
from orcapod.sources import DictSource

REGISTRATION = Path(contexts_package.__file__).parent / "logical_types.json"


def _value(name: str) -> Any:
    """A fresh value of the type whose extension name is ``name``."""
    if name == "orcapod.numpy.ndarray":
        return np.arange(3.0)
    if name == "orcapod.torch.tensor":
        return torch.arange(3.0)
    if name == "orcapod.pandas.dataframe":
        return pd.DataFrame({"a": [1, 2]})
    if name == "orcapod.pandas.series":
        return pd.Series([1, 2], name="a")
    if name == "orcapod.polars.dataframe":
        return pl.DataFrame({"a": [1, 2]})
    assert name == "orcapod.polars.series"
    return pl.Series("a", [1, 2])


PYTHON_TYPES: dict[str, type] = {
    "orcapod.numpy.ndarray": np.ndarray,
    "orcapod.torch.tensor": torch.Tensor,
    "orcapod.pandas.dataframe": pd.DataFrame,
    "orcapod.pandas.series": pd.Series,
    "orcapod.polars.dataframe": pl.DataFrame,
    "orcapod.polars.series": pl.Series,
}
NAMES = sorted(PYTHON_TYPES)


@pytest.fixture(autouse=True)
def _fresh_bookkeeping() -> Iterator[None]:
    """Every test starts as a new process would: no type has warned yet."""
    reset_provisional_warnings()
    yield
    reset_provisional_warnings()


@contextmanager
def recorded() -> Iterator[list[warnings.WarningMessage]]:
    """Record the ``ProvisionalHashWarning`` issued inside the block."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ProvisionalHashWarning)
        found: list[warnings.WarningMessage] = []
        yield found
    found.extend(w for w in caught if issubclass(w.category, ProvisionalHashWarning))


def _warned_names(caught: list[warnings.WarningMessage]) -> list[str]:
    return sorted(
        name for w in caught for name in NAMES if f"{name} " in str(w.message)
    )


def _registration() -> dict[str, Any]:
    return json.loads(REGISTRATION.read_text())


def _context_with(tmp_path: Path, provisional: list[str] | None) -> DataContext:
    """A data context whose registration declares ``provisional`` (None: no key)."""
    registration = _registration()
    if provisional is None:
        del registration["provisional"]
    else:
        registration["provisional"] = provisional
    custom = tmp_path / "logical_types.json"
    custom.write_text(json.dumps(registration))
    return JSONDataContextRegistry(logical_types_file=custom).get_context()


def _quiet(action: Callable[[], Any]) -> Any:
    """Run ``action`` without recording its warnings, then forget them."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        result = action()
    reset_provisional_warnings()
    return result


# ---------------------------------------------------------------------------
# The class and the declaration
# ---------------------------------------------------------------------------


def test_the_warning_is_a_public_user_warning() -> None:
    assert issubclass(ProvisionalHashWarning, UserWarning)
    assert ProvisionalHashWarning.__module__ == "orcapod.errors"


def test_the_six_library_value_types_are_declared_provisional() -> None:
    assert sorted(_registration()["provisional"]) == NAMES


# ---------------------------------------------------------------------------
# Each entry path warns once per type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", NAMES)
def test_hashing_a_bare_value_warns_once(name: str) -> None:
    hasher = get_default_context().semantic_hasher
    with recorded() as caught:
        hasher.hash_object(_value(name))
    assert _warned_names(caught) == [name]
    (warning,) = caught
    # The warning points at the code that used the type, not at Orcapod.
    assert warning.filename == __file__
    message = str(warning.message)
    assert "v0.4" in message
    assert "recomputed" in message
    assert "ProvisionalHashWarning" in message
    assert "docs/concepts/arrays.md" in message

    with recorded() as again:
        hasher.hash_object(_value(name))
        hasher.hash_object(_value(name))
    assert again == []


@pytest.mark.parametrize("name", NAMES)
def test_a_source_column_of_the_type_warns_once(name: str) -> None:
    with recorded() as caught:
        source = DictSource(
            data=[{"k": i, "x": _value(name)} for i in range(3)],
            tag_columns=["k"],
            source_id="values",
        )
        source.as_table()
        list(source.iter_data())
    assert _warned_names(caught) == [name]

    with recorded() as again:
        DictSource(
            data=[{"k": 9, "x": _value(name)}], tag_columns=["k"], source_id="more"
        ).as_table()
    assert again == []


def _producer(name: str) -> Any:
    """A function pod ``make(n: int)`` whose output ``y`` is of the type ``name``."""

    def make(n: int) -> Any:
        return _value(name)

    make.__annotations__["return"] = PYTHON_TYPES[name]
    return function_pod(output_keys="y")(make)


def _numbers() -> DictSource:
    return DictSource(
        data=[{"k": i, "n": i} for i in range(3)], tag_columns=["k"], source_id="n"
    )


@pytest.mark.parametrize("name", NAMES)
def test_a_function_pod_output_of_the_type_warns_once(name: str) -> None:
    source = _quiet(_numbers)
    with recorded() as caught:
        pod = _producer(name)
        pod.pod(source).as_table()
    assert _warned_names(caught) == [name]
    # Defining the pod is the use: the warning points at that line of user code.
    assert caught[0].filename == __file__


@pytest.mark.parametrize("name", NAMES)
def test_a_function_pod_input_of_the_type_warns_once(name: str) -> None:
    def size(x: Any) -> int:
        return len(x)

    size.__annotations__["x"] = PYTHON_TYPES[name]
    with recorded() as caught:
        function_pod(output_keys="n")(size)
    assert _warned_names(caught) == [name]


@pytest.mark.parametrize("name", NAMES)
def test_reading_stored_results_of_the_type_warns_once(
    name: str, tmp_path: Path
) -> None:
    """A loaded job, whose pods are not saved, reads a stored column of the type."""

    def run_and_save() -> None:
        job = PipelineJob(
            name="values", database=DeltaTableDatabase(tmp_path / "store")
        )
        with job:
            _producer(name).pod(_numbers(), label="make")
        job.run()
        job.save(tmp_path / "job.json")

    _quiet(run_and_save)
    with recorded() as caught:
        loaded = PipelineJob.load(tmp_path / "job.json")
        # A loaded node is empty until it runs (ITL-1095): read the stored view.
        table = loaded.nodes["make"].as_source().as_table()
    assert table.num_rows == 3
    assert _warned_names(caught) == [name]


def test_each_type_warns_once_in_one_pipeline() -> None:
    """Six types used together: six warnings, one per type, however many rows."""
    with recorded() as caught:
        source = DictSource(
            data=[{"k": i, **{n: _value(n) for n in NAMES}} for i in range(4)],
            tag_columns=["k"],
            source_id="everything",
        )
        source.as_table()
    assert _warned_names(caught) == NAMES


# ---------------------------------------------------------------------------
# Behaviour of the warning
# ---------------------------------------------------------------------------


def test_an_error_filter_raises_on_every_use() -> None:
    hasher = get_default_context().semantic_hasher
    with warnings.catch_warnings():
        warnings.simplefilter("error", ProvisionalHashWarning)
        for _ in range(2):
            with pytest.raises(ProvisionalHashWarning):
                hasher.hash_object(np.arange(2))


def test_an_ignore_filter_silences_it() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warnings.filterwarnings("ignore", category=ProvisionalHashWarning)
        get_default_context().semantic_hasher.hash_object(np.arange(2))
    assert not [w for w in caught if issubclass(w.category, ProvisionalHashWarning)]


def test_concurrent_first_uses_warn_once() -> None:
    hasher = get_default_context().semantic_hasher
    barrier = threading.Barrier(8)

    def use() -> None:
        barrier.wait()
        hasher.hash_object(np.arange(3))

    with recorded() as caught:
        threads = [threading.Thread(target=use) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    assert _warned_names(caught) == ["orcapod.numpy.ndarray"]


# ---------------------------------------------------------------------------
# What does not warn
# ---------------------------------------------------------------------------


def test_importing_orcapod_does_not_warn() -> None:
    code = (
        "import warnings\n"
        "with warnings.catch_warnings(record=True) as caught:\n"
        "    warnings.simplefilter('always')\n"
        "    import orcapod\n"
        "    from orcapod.contexts import get_default_context\n"
        "    get_default_context()\n"
        "print(sorted({w.category.__name__ for w in caught}))\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert "ProvisionalHashWarning" not in proc.stdout, proc.stdout


def test_a_pipeline_without_these_types_does_not_warn(tmp_path: Path) -> None:
    @function_pod(output_keys="y")
    def scale(x: int, p: Path) -> float:
        return x * 1.5

    with recorded() as caught:
        source = DictSource(
            data=[{"k": i, "x": i, "p": tmp_path} for i in range(3)],
            tag_columns=["k"],
            source_id="plain",
        )
        scale.pod(source).as_table()
        get_default_context().semantic_hasher.hash_object([1, "a", 2.5])
    assert caught == []


# ---------------------------------------------------------------------------
# The declaration is one line per type and changes no identity
# ---------------------------------------------------------------------------


def _identity(context: DataContext, name: str) -> tuple[str, bytes, str]:
    arrow_type = context.type_converter.python_type_to_arrow_type(PYTHON_TYPES[name])
    assert isinstance(arrow_type, pa.ExtensionType)
    return (
        arrow_type.extension_name,
        arrow_type.__arrow_ext_serialize__(),
        context.semantic_hasher.hash_object(_value(name)).to_string(),
    )


def test_removing_a_name_silences_that_type_only(tmp_path: Path) -> None:
    kept = [n for n in NAMES if n != "orcapod.polars.series"]
    context = _context_with(tmp_path, kept)
    with recorded() as caught:
        for name in NAMES:
            context.semantic_hasher.hash_object(_value(name))
    assert _warned_names(caught) == kept


def test_the_declaration_changes_no_identifier_or_hash(tmp_path: Path) -> None:
    undeclared = _quiet(lambda: _context_with(tmp_path, None))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        for name in NAMES:
            assert _identity(undeclared, name) == _identity(get_default_context(), name)


def test_an_undeclared_type_never_warns(tmp_path: Path) -> None:
    context = _context_with(tmp_path, [])
    with recorded() as caught:
        for name in NAMES:
            context.semantic_hasher.hash_object(_value(name))
            context.type_converter.python_type_to_arrow_type(PYTHON_TYPES[name])
    assert caught == []
