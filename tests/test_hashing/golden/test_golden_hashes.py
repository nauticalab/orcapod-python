"""Golden hash regression tests (ITL-626).

Every hash Orcapod stores, keys data on, or embeds in a column name or a database table path
is pinned in ``data/<group>.json``. These tests recompute each value and compare it with the
pinned one, one test per entry, so a failure names exactly what moved.

A failing entry is a BREAKING change: stored data keyed on the old value is stranded. Do not
regenerate the golden files to make a failure go away; read the header of
``generate_golden_hashes.py`` first.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from orcapod.contexts import get_default_context
from orcapod.types import Schema
from tests.test_hashing.golden import cases
from tests.test_hashing.golden.generate_golden_hashes import golden_path

GENERATOR = Path(__file__).with_name("generate_golden_hashes.py")


def _load_golden() -> dict[str, dict[str, Any]]:
    golden: dict[str, dict[str, Any]] = {}
    for group in cases.GROUPS:
        path = golden_path(group)
        golden[group] = (
            json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        )
    return golden


GOLDEN = _load_golden()
GOLDEN_ENTRIES = [(group, entry) for group in cases.GROUPS for entry in GOLDEN[group]]


def _moved_message(group: str, entry: str, expected: Any, actual: Any) -> str:
    return (
        f"Golden hash entry {group}:{entry} moved.\n"
        f"  pinned: {expected!r}\n"
        f"  live:   {actual!r}\n"
        "A changed hash is a BREAKING change for stored data. Unless the change is "
        "intentional, assessed for migration and approved, the code change is the bug. "
        "See tests/test_hashing/golden/generate_golden_hashes.py."
    )


@pytest.fixture(scope="module")
def live() -> dict[str, dict[str, Any]]:
    """Every golden group, computed in this process."""
    return cases.compute_all()


@pytest.mark.parametrize("group", cases.GROUPS)
def test_golden_file_present(group: str) -> None:
    assert golden_path(group).exists(), (
        f"missing golden file {golden_path(group)}; run generate_golden_hashes.py"
    )


@pytest.mark.parametrize(
    ("group", "entry"), GOLDEN_ENTRIES, ids=[f"{g}:{e}" for g, e in GOLDEN_ENTRIES]
)
def test_golden_entry(live: dict[str, dict[str, Any]], group: str, entry: str) -> None:
    actual = live[group].get(entry, "<entry no longer produced>")
    expected = GOLDEN[group][entry]
    assert actual == expected, _moved_message(group, entry, expected, actual)


@pytest.mark.parametrize("group", cases.GROUPS)
def test_no_unpinned_entries(live: dict[str, dict[str, Any]], group: str) -> None:
    unpinned = sorted(set(live[group]) - set(GOLDEN[group]))
    assert not unpinned, (
        f"{len(unpinned)} {group} entries are computed but not pinned (new cases need "
        f"the generator run; check the diff adds entries only): {unpinned[:10]}"
    )


GENERATOR_TIMEOUT_S = 600


def _run_generator(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run the golden generator in a fresh process and capture its JSON output.

    Args:
        env: Environment for the child process, e.g. with ``PYTHONHASHSEED`` set.

    Returns:
        The completed process, with ``stdout`` and ``stderr`` captured as text.
    """
    try:
        return subprocess.run(
            [sys.executable, str(GENERATOR), "--stdout"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=GENERATOR_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        seed = env.get("PYTHONHASHSEED")
        pytest.fail(
            f"golden generator timed out after {GENERATOR_TIMEOUT_S}s (PYTHONHASHSEED={seed})"
        )


@pytest.mark.parametrize("seed", ["0", "4242"])
def test_values_independent_of_process_and_hash_seed(seed: str) -> None:
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = seed
    result = _run_generator(env)
    assert result.returncode == 0, result.stderr
    computed = json.loads(result.stdout)
    moved = [
        _moved_message(group, entry, value, computed[group].get(entry))
        for group in cases.GROUPS
        for entry, value in GOLDEN[group].items()
        if computed[group].get(entry) != value
    ]
    assert not moved, f"PYTHONHASHSEED={seed}:\n" + "\n".join(moved[:5])


def test_generator_timeout_fails_with_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """A generator that exceeds the timeout fails the test with a clear message."""

    # Exercises the private helper because the test is about its timeout handling.
    def hang(
        cmd: list[str], *, timeout: float, **_: object
    ) -> subprocess.CompletedProcess[str]:
        """Stand in for ``subprocess.run`` and time out immediately."""
        raise subprocess.TimeoutExpired(cmd, timeout)

    monkeypatch.setattr(subprocess, "run", hang)
    with pytest.raises(pytest.fail.Exception, match="golden generator timed out"):
        _ = _run_generator(os.environ.copy())


# ---------------------------------------------------------------------------
# Invariants the pinned values must keep
# ---------------------------------------------------------------------------


def test_large_and_non_large_layouts_hash_equal(
    live: dict[str, dict[str, Any]],
) -> None:
    arrow = live["arrow"]
    for kind in ("table", "schema"):
        for small, large in [
            ("string", "large_string"),
            ("binary", "large_binary"),
            ("list", "large_list"),
        ]:
            assert arrow[f"{kind}.layout.{small}"] == arrow[f"{kind}.layout.{large}"], (
                f"{kind} hash of {small} differs from {large}"
            )
    datagram = live["datagram"]
    for field in ("content_hash", "pipeline_hash"):
        assert (
            datagram[f"arrow_source.string.{field}"]
            == datagram[f"arrow_source.large_string.{field}"]
        )


def test_integer_widths_hash_differently(live: dict[str, dict[str, Any]]) -> None:
    arrow = live["arrow"]
    widths = ("int8", "int16", "int32", "int64", "uint8")
    for kind in ("table", "schema"):
        values = [arrow[f"{kind}.width.{w}"] for w in widths]
        assert len(set(values)) == len(widths), f"{kind} hashes collide across widths"
    # A source's own content_hash is (class, Python schema, source_id) and the Python schema
    # maps every width to ``int``, so widths are told apart by the per-row data hashes.
    datagram = live["datagram"]
    row = 'row[{"k":"r1"}].data_hash'
    row_hashes = {
        datagram[f"arrow_source.{w}.{row}"] for w in ("int8", "int32", "int64")
    }
    assert len(row_hashes) == 3


def test_nullability_changes_schema_hash(live: dict[str, dict[str, Any]]) -> None:
    arrow = live["arrow"]
    assert (
        arrow["schema.nullability.nullable"] != arrow["schema.nullability.non_nullable"]
    )


def test_column_order_does_not_change_arrow_hashes(
    live: dict[str, dict[str, Any]],
) -> None:
    # StarfixArrowHasher canonicalizes column order (see test_cross_process_stability).
    arrow = live["arrow"]
    for kind in ("table", "schema"):
        assert arrow[f"{kind}.order.ab"] == arrow[f"{kind}.order.ba"]


def test_row_order_changes_table_hash(live: dict[str, dict[str, Any]]) -> None:
    arrow = live["arrow"]
    assert arrow["table.order.ab"] != arrow["table.order.rows_reversed"]


def test_dict_hash_independent_of_insertion_order(
    live: dict[str, dict[str, Any]],
) -> None:
    # Set iteration order varies with PYTHONHASHSEED; the subprocess test covers sets.
    semantic = live["semantic"]
    assert semantic["value.dict.ab"] == semantic["value.dict.ba"]


@pytest.mark.parametrize(
    "python_type",
    [
        int,
        float,
        str,
        bool,
        bytes,
        int | None,
        list[int],
        list[list[int]],
        dict[str, int],
        Path,
        uuid.UUID,
        np.ndarray,
        pd.DataFrame,
        list[Path],
        set[uuid.UUID],
        cases.GoldenPoint,
        cases.GoldenModel,
    ],
    ids=str,
)
def test_schema_round_trips_through_arrow_with_equal_hash(python_type: Any) -> None:
    ctx = get_default_context()
    cases.register_logical_classes()
    schema = Schema({"c": python_type})
    round_tripped = ctx.type_converter.arrow_schema_to_python_schema(
        ctx.type_converter.python_schema_to_arrow_schema(schema)
    )
    assert round_tripped == schema
    assert ctx.semantic_hasher.hash_object(
        round_tripped
    ) == ctx.semantic_hasher.hash_object(schema)
