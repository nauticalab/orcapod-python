"""Regenerate ``v02_full_store.tar.gz`` with Orcapod v0.2.0 (ITL-954).

The store is kept as an archive because Delta table paths contain ``:``, which
some file systems refuse.

Run from the repository root in an environment that has Orcapod v0.2.0 installed
from the tag (with the dependencies v0.2.0 pins), not this checkout:

    uv run --no-project --python 3.12 \
        --with "orcapod @ git+file://$(git rev-parse --path-format=absolute --git-common-dir)@v0.2.0" \
        python tests/test_arrays/data/make_v02_full_store.py

It refuses to run with any other version of Orcapod.
"""

import importlib.util
import os
import shutil
import sys
import tarfile
import tempfile
from importlib.metadata import version
from pathlib import Path

import orcapod

if version("orcapod") != "0.2.0":
    sys.exit(f"not Orcapod v0.2.0: {version('orcapod')} at {orcapod.__file__}")

here = Path(__file__).resolve().parent
# Loaded under its real module name without importing the ``tests`` package, so the
# pods get the identity they have in the test run.
name = "tests.test_arrays.v02_full_store_pods"
spec = importlib.util.spec_from_file_location(
    name, here.parent / "v02_full_store_pods.py"
)
assert spec is not None and spec.loader is not None
pods = importlib.util.module_from_spec(spec)
sys.modules[name] = pods
spec.loader.exec_module(pods)

archive = here / "v02_full_store.tar.gz"
with tempfile.TemporaryDirectory() as tmp:
    store = Path(tmp) / "store"
    pods.write_store(store)
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(store / pods.JOB_NAME, arcname=pods.JOB_NAME)
    shutil.rmtree(store)
print(f"wrote {archive}")
sys.stdout.flush()
os._exit(0)  # deltalake can leave the interpreter hanging at exit
