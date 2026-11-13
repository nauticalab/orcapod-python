"""PyTorch stays optional: ``import orcapod`` never imports torch (ITL-871).

The development environment installs torch (the dev group pulls ``orcapod[torch]``), so
the behaviour without torch is checked in a subprocess whose import system refuses
``torch`` through a ``sys.meta_path`` finder.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

BLOCK_TORCH = textwrap.dedent(
    """
    import importlib.abc
    import sys

    class _BlockTorch(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if name == "torch" or name.startswith("torch."):
                raise ModuleNotFoundError(f"No module named {name!r}", name=name)
            return None

    sys.meta_path.insert(0, _BlockTorch())
    """
)
"""Prelude that makes ``torch`` unimportable, as in an environment without it."""


def _run(script: str) -> subprocess.CompletedProcess[str]:
    """Run ``script`` in a fresh interpreter from the repository root."""
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
        cwd=REPO_ROOT,
        timeout=300,
    )


SCRIPT = textwrap.dedent(
    """
    import sys

    import numpy as np

    import orcapod
    from orcapod import function_pod
    from orcapod.contexts import get_default_context, resolve_context
    from orcapod.sources import DictSource

    # Building the v0.3 data context names the tensor type without importing torch.
    assert resolve_context("std:v1:default").context_key == "std:v1:default"
    assert "torch" not in sys.modules

    @function_pod(output_keys="y")
    def double(x: np.ndarray) -> np.ndarray:
        return x * 2

    source = DictSource(data=[{"k": "a", "x": np.arange(3.0)}], tag_columns=["k"])
    for _, data in double.pod(source).iter_data():
        data.content_hash()
    get_default_context().semantic_hasher.hash_object([1, "a", np.arange(2)])
    print("torch" in sys.modules)
    """
)


def test_orcapod_never_imports_torch() -> None:
    """Importing Orcapod and running a pipeline leaves torch unimported."""
    proc = _run(SCRIPT)
    assert proc.returncode == 0, proc.stderr[-3000:]
    assert proc.stdout.strip().splitlines()[-1] == "False"


def test_orcapod_works_without_torch() -> None:
    """The same pipeline runs when torch cannot be imported at all."""
    proc = _run(BLOCK_TORCH + SCRIPT)
    assert proc.returncode == 0, proc.stderr[-3000:]
    assert proc.stdout.strip().splitlines()[-1] == "False"


READ_TENSOR_COLUMN = textwrap.dedent(
    """
    import pyarrow as pa

    from orcapod.errors import MissingOptionalDependencyError
    from orcapod.sources import ArrowTableSource

    class StoredTensorType(pa.ExtensionType):
        # Stands in for a column written by Orcapod with PyTorch installed.
        def __init__(self):
            super().__init__(pa.large_binary(), "orcapod.torch.tensor")

        def __arrow_ext_serialize__(self):
            return b'{"ignore":[],"version":1}'

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return cls()

    storage = pa.array([b"a torch.save archive"], type=pa.large_binary())
    table = pa.table(
        {
            "k": pa.array(["r"], pa.large_string()),
            "x": pa.ExtensionArray.from_storage(StoredTensorType(), storage),
        }
    )
    try:
        source = ArrowTableSource(table, tag_columns=["k"])
        for _, data in source.iter_data():
            data.as_dict()
    except MissingOptionalDependencyError as exc:
        print("raised:", exc)
    else:
        print("no error")
    """
)


def test_reading_a_tensor_column_without_torch_names_the_missing_dependency() -> None:
    """Reading a tensor column without torch raises naming the type and the package."""
    proc = _run(BLOCK_TORCH + READ_TENSOR_COLUMN)
    assert proc.returncode == 0, proc.stderr[-3000:]
    last = proc.stdout.strip().splitlines()[-1]
    assert last.startswith("raised:"), proc.stdout
    assert "'orcapod.torch.tensor'" in last
    assert "'torch'" in last
