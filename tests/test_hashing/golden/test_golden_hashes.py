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


def _needs(group: str) -> Any:
    """A skip mark for a group whose optional dependency is not installed (ITL-871)."""
    module = cases.OPTIONAL_GROUPS.get(group)
    return pytest.mark.skipif(
        not cases.group_available(group),
        reason=f"golden group {group!r} needs the optional package {module!r}",
    )


GOLDEN_ENTRIES = [
    pytest.param(group, entry, id=f"{group}:{entry}", marks=_needs(group))
    for group in cases.GROUPS
    for entry in GOLDEN[group]
]
GROUP_PARAMS = [pytest.param(group, marks=_needs(group)) for group in cases.GROUPS]


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


@pytest.mark.parametrize(("group", "entry"), GOLDEN_ENTRIES)
def test_golden_entry(live: dict[str, dict[str, Any]], group: str, entry: str) -> None:
    if entry in cases.unavailable_entries(group):
        pytest.skip(f"{group}:{entry} needs a newer version of an optional dependency")
    actual = live[group].get(entry, "<entry no longer produced>")
    expected = GOLDEN[group][entry]
    assert actual == expected, _moved_message(group, entry, expected, actual)


@pytest.mark.parametrize("group", GROUP_PARAMS)
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
        for group in cases.available_groups()
        for entry, value in GOLDEN[group].items()
        if entry not in cases.unavailable_entries(group)
        and computed[group].get(entry) != value
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


def _reference_array_digest(tag: str, shape: tuple[int, ...], data: bytes) -> str:
    """The version-1 array digest from its specification (``hashlib`` + ``struct`` only)."""
    import hashlib
    import struct

    t = tag.encode("utf-8")
    preimage = struct.pack("<Q", len(t)) + t + struct.pack("<Q", len(shape))
    preimage += b"".join(struct.pack("<q", d) for d in shape)
    preimage += struct.pack("<Q", len(data)) + data
    return hashlib.sha256(preimage).hexdigest()


def _reference_tensor_digest(
    element_digest: str,
    flags: tuple[int, int, int],
    strides: tuple[int, ...],
    class_name: str,
) -> str:
    """The tensor value hash ``D`` from its specification (``hashlib`` + ``struct`` only).

    ``D = SHA-256(lp("orcapod.torch.tensor") || E || u8(requires_grad) || u8(conj_bit)
    || u8(neg_bit) || u64le(ndim) || i64le(each stride) || lp(class name))``.
    """
    import hashlib
    import struct

    def lp(text: str) -> bytes:
        """Return ``text`` as UTF-8 behind its length (unsigned 64-bit little-endian)."""
        raw = text.encode("utf-8")
        return struct.pack("<Q", len(raw)) + raw

    preimage = lp("orcapod.torch.tensor") + bytes.fromhex(element_digest)
    preimage += bytes(flags) + struct.pack("<Q", len(strides))
    preimage += b"".join(struct.pack("<q", s) for s in strides) + lp(class_name)
    return hashlib.sha256(preimage).hexdigest()


_TENSOR = "torch.Tensor"
_ARANGE_2X3 = np.arange(6, dtype="<f4").reshape(2, 3)

# name: (dtype tag, shape, element bytes in C order,
#        (requires_grad, conj_bit, neg_bit), strides, class name)
TORCH_PINNED: dict[
    str,
    tuple[str, tuple[int, ...], bytes, tuple[int, int, int], tuple[int, ...], str],
] = {
    "bool": ("bool", (3,), b"\x01\x00\x01", (0, 0, 0), (1,), _TENSOR),
    # bfloat16 bits of 1.5, -2.25, 3.0, little-endian.
    "bfloat16": (
        "bfloat16",
        (3,),
        bytes.fromhex("c03f10c04040"),
        (0, 0, 0),
        (1,),
        _TENSOR,
    ),
    "zero_dim": ("float32", (), np.array(2.5, "<f4").tobytes(), (0, 0, 0), (), _TENSOR),
    "empty": ("float32", (0,), b"", (0, 0, 0), (1,), _TENSOR),
    # The transposed (2, 3) arange: shape (3, 2), elements in C order, strides (1, 3).
    "non_contiguous": (
        "float32",
        (3, 2),
        _ARANGE_2X3.T.copy().tobytes(),
        (0, 0, 0),
        (1, 3),
        _TENSOR,
    ),
    "requires_grad": (
        "float32",
        (2, 3),
        _ARANGE_2X3.tobytes(),
        (1, 0, 0),
        (3, 1),
        _TENSOR,
    ),
    "parameter": (
        "float32",
        (2, 3),
        _ARANGE_2X3.tobytes(),
        (0, 0, 0),
        (3, 1),
        "torch.nn.parameter.Parameter",
    ),
    # The logical values of the conjugate view: 1-2j, 3+4j.
    "conj": (
        "complex64",
        (2,),
        np.array([1 - 2j, 3 + 4j], "<c8").tobytes(),
        (0, 1, 0),
        (1,),
        _TENSOR,
    ),
    # Every second column of a (2, 6) arange, stored as its clone: strides (3, 1).
    "step_slice": (
        "float32",
        (2, 3),
        np.arange(12, dtype="<f4").reshape(2, 6)[:, ::2].copy().tobytes(),
        (0, 0, 0),
        (3, 1),
        _TENSOR,
    ),
}


@pytest.mark.parametrize("name", sorted(TORCH_PINNED))
def test_pinned_tensor_hashes_match_an_independent_reference(name: str) -> None:
    # Runs without torch: the pinned torch_tensor_v1 values are D computed by hand.
    """Pinned tensor hashes equal the value hash computed from the specification."""
    tag, shape, data, flags, strides, class_name = TORCH_PINNED[name]
    element = _reference_array_digest(tag, shape, data)
    assert GOLDEN["torch"][f"value.{name}"] == (
        "torch_tensor_v1:"
        + _reference_tensor_digest(element, flags, strides, class_name)
    )


@pytest.mark.parametrize("name", ["int32", "int64", "float64", "empty"])
def test_tensor_and_ndarray_pins_differ(name: str) -> None:
    """Pinned tensor and ndarray hashes of equal elements have different digests."""
    tensor_label, tensor_body = GOLDEN["torch"][f"value.{name}"].split(":")
    array_label, array_body = GOLDEN["semantic"][f"value.ndarray.{name}"].split(":")
    assert (tensor_label, array_label) == ("torch_tensor_v1", "ndarray_v1")
    assert tensor_body != array_body


OLDER_TORCH_GOLDEN = """
import json
import pathlib
import shutil
import sys

from tests.test_hashing.golden import cases
from tests.test_hashing.golden.generate_golden_hashes import golden_path, main

# Stand in for a supported PyTorch that predates float8_e8m0fnu (added in torch
# 2.7). The attribute itself cannot be removed: torch.save refers to it.
_installed = cases.installed_torch_dtype
cases.installed_torch_dtype = (
    lambda name: None if name == "float8_e8m0fnu" else _installed(name)
)

live = cases.compute_group("torch")
print("produced:", "value.float8_e8m0fnu" in live)
print("unavailable:", sorted(cases.unavailable_entries("torch")))
out = pathlib.Path(sys.argv[1])
shutil.copy(golden_path("torch"), out / "torch.json")
main(["--group", "torch", "--output-dir", str(out)])
kept = json.loads((out / "torch.json").read_text())
pinned = json.loads(golden_path("torch").read_text())
print("pins kept:", kept == pinned)
empty = out / "empty"
main(["--group", "torch", "--output-dir", str(empty)])
fresh = json.loads((empty / "torch.json").read_text())
print("pins kept in an empty output dir:", fresh == pinned)
"""


@pytest.mark.skipif(not cases.group_available("torch"), reason="needs torch")
def test_torch_group_runs_on_a_torch_without_a_newer_dtype(tmp_path: Path) -> None:
    """With a torch lacking a pinned dtype, the group computes, skips that entry, keeps its pin."""
    repo_root = Path(__file__).resolve().parents[3]
    proc = subprocess.run(
        [sys.executable, "-c", OLDER_TORCH_GOLDEN, str(tmp_path)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(repo_root)},
        cwd=repo_root,
        timeout=GENERATOR_TIMEOUT_S,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    lines = [line for line in proc.stdout.splitlines() if ": " in line]
    assert "produced: False" in lines
    assert "unavailable: ['value.float8_e8m0fnu']" in lines
    assert "pins kept: True" in lines
    assert "pins kept in an empty output dir: True" in lines
