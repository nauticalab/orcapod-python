"""Regenerate ``v02_array_store.tar.gz`` with Orcapod v0.2 (ITL-888).

The database is kept as an archive because Delta table paths contain ``:``, which
some file systems refuse.

Run from the repository root with a v0.2 source tree first on the path:

    rm -rf /tmp/orcapod_v02 && mkdir -p /tmp/orcapod_v02
    git archive v0.2.0 src/orcapod | tar -x -C /tmp/orcapod_v02
    PYTHONPATH=/tmp/orcapod_v02/src:. uv run python tests/test_arrays/data/make_v02_array_store.py

It refuses to run with an Orcapod that already has the v0.3 array format.
"""

import os
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path

import orcapod
from orcapod.logical_types.numpy_type import LogicalNumpyArray

if LogicalNumpyArray.logical_type_name != "numpy.ndarray":
    sys.exit(f"not a v0.2 Orcapod: {orcapod.__file__}")

from tests.test_arrays.v02_store_pods import build_job  # noqa: E402

archive = Path(__file__).parent / "v02_array_store.tar.gz"
with tempfile.TemporaryDirectory() as tmp:
    store = Path(tmp) / "store"
    build_job(store).run()
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(store / "v02", arcname="v02")
    shutil.rmtree(store)
print(f"wrote {archive}")
sys.stdout.flush()
os._exit(0)  # deltalake can leave the interpreter hanging at exit
