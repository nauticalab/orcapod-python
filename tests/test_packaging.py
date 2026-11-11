"""Packaging guards for ``pyproject.toml`` and the built distributions."""

from __future__ import annotations

import ast
import re
import shutil
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from importlib.metadata import version
from pathlib import Path
from typing import cast

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
COPYRIGHT_LINE = "Copyright (c) 2025-2026 NauticaLab"
MIT_LICENSE_TEXT = f"""MIT License

{COPYRIGHT_LINE}

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


def _pyproject_value(*keys: str) -> object:
    """Return the ``pyproject.toml`` value found by following ``keys``."""
    node: object = tomllib.loads(PYPROJECT.read_text())
    for key in keys:
        assert isinstance(node, dict)
        node = cast("dict[str, object]", node)[key]
    return node


def _pyproject_strings(*keys: str) -> list[str]:
    """Return the ``pyproject.toml`` list of strings found by following ``keys``."""
    value = _pyproject_value(*keys)
    assert isinstance(value, list)
    return [str(item) for item in cast("list[object]", value)]


def test_pygraphviz_is_not_a_dependency():
    """``pygraphviz`` needs Graphviz C headers to build and nothing imports it (ITL-134)."""
    project = tomllib.loads(PYPROJECT.read_text())["project"]
    declared = list(project["dependencies"])
    for extra in project.get("optional-dependencies", {}).values():
        declared.extend(extra)
    assert not [d for d in declared if d.lower().startswith("pygraphviz")]


def test_license_is_mit() -> None:
    """Orcapod is MIT licensed from v0.2.0 on (ITL-875, reversing ITL-827)."""
    # PEP 639 SPDX expression; it is written to the metadata as License-Expression.
    assert _pyproject_value("project", "license") == "MIT"
    # PEP 639 forbids license classifiers next to a license expression.
    classifiers = _pyproject_strings("project", "classifiers")
    assert not [c for c in classifiers if c.startswith("License ::")]
    assert (REPO_ROOT / "LICENSE").read_text() == MIT_LICENSE_TEXT


def test_build_backend_supports_license_expressions() -> None:
    """Hatchling 1.27.0 is the first to emit ``License-Expression`` in metadata 2.4.

    Older releases write the field under ``Metadata-Version: 2.1``, which does not define it.
    """
    requires = _pyproject_strings("build-system", "requires")
    hatchling = [r for r in requires if re.match(r"hatchling\b", r)]
    assert hatchling == ["hatchling>=1.27.0"]


def test_basedpyright_is_a_dev_dependency_only() -> None:
    """A type checker is a development tool, not something users install (ITL-827)."""
    runtime = [d.lower() for d in _pyproject_strings("project", "dependencies")]
    dev = [d.lower() for d in _pyproject_strings("dependency-groups", "dev")]
    assert not [d for d in runtime if d.startswith("basedpyright")]
    assert [d for d in dev if d.startswith("basedpyright")]


def _names(requirements: list[str]) -> set[str]:
    """Return the PEP 503 normalized names of PEP 508 requirement strings."""
    names = (re.split(r"[\s<>=!~;\[]", r, maxsplit=1)[0] for r in requirements)
    return {re.sub(r"[-_.]+", "-", name).lower() for name in names}


@pytest.mark.parametrize("name", ["pymongo", "pyyaml"])
def test_unused_package_is_not_a_dependency(name: str) -> None:
    """Nothing in the repository imports these, so users must not install them."""
    assert name not in _names(_pyproject_strings("project", "dependencies"))
    assert name not in _names(_pyproject_strings("dependency-groups", "dev"))


def test_typing_extensions_is_a_dev_dependency_only() -> None:
    """Only a test imports ``typing_extensions``; ``src/`` does not (ITL-827)."""
    runtime = _names(_pyproject_strings("project", "dependencies"))
    dev = _names(_pyproject_strings("dependency-groups", "dev"))
    assert "typing-extensions" not in runtime
    assert "typing-extensions" in dev


@pytest.fixture(scope="module")
def built_dists(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """Build the sdist, then the wheel from it, exactly as the release workflow does.

    Returns:
        The paths of the sdist and the wheel.
    """
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is required to build the distributions")
    out_dir = tmp_path_factory.mktemp("dist")
    _ = subprocess.run(
        [uv, "build", "--out-dir", str(out_dir), str(REPO_ROOT)],
        check=True,
        capture_output=True,
    )
    return next(out_dir.glob("*.tar.gz")), next(out_dir.glob("*.whl"))


def _sdist_members(sdist: Path) -> list[str]:
    """Return sdist member paths with the ``orcapod-<version>/`` prefix removed."""
    with tarfile.open(sdist) as tar:
        return [name.partition("/")[2] for name in tar.getnames() if "/" in name]


def test_sdist_contains_only_build_inputs(built_dists: tuple[Path, Path]) -> None:
    """The sdist ships the package source, build metadata, README and LICENSE (ITL-827)."""
    members = _sdist_members(built_dists[0])
    top_level = {m.split("/")[0] for m in members}
    # hatchling always adds .gitignore to an sdist; it cannot be excluded.
    build_inputs = {".gitignore", "PKG-INFO", "pyproject.toml", "src"}
    assert top_level == build_inputs | {"LICENSE", "README.md"}
    assert {m.split("/")[1] for m in members if m.startswith("src/")} == {"orcapod"}


def test_wheel_contains_only_the_package(built_dists: tuple[Path, Path]) -> None:
    """The wheel holds the ``orcapod`` package and its dist-info, nothing else."""
    wheel = built_dists[1]
    with zipfile.ZipFile(wheel) as zf:
        top_level = {name.split("/")[0] for name in zf.namelist()}
    wheel_version = wheel.name.split("-")[1]
    assert top_level == {"orcapod", f"orcapod-{wheel_version}.dist-info"}


def test_license_is_shipped_in_sdist_and_wheel(built_dists: tuple[Path, Path]) -> None:
    """Both distributions carry the repository LICENSE verbatim (ITL-827)."""
    sdist, wheel = built_dists
    expected = (REPO_ROOT / "LICENSE").read_bytes()
    with tarfile.open(sdist) as tar:
        member = tar.extractfile(f"{sdist.name.removesuffix('.tar.gz')}/LICENSE")
        assert member is not None
        assert member.read() == expected
    with zipfile.ZipFile(wheel) as zf:
        suffix = ".dist-info/licenses/LICENSE"
        licenses = [n for n in zf.namelist() if n.endswith(suffix)]
        assert len(licenses) == 1
        assert zf.read(licenses[0]) == expected


def _metadata_fields(metadata: str) -> list[tuple[str, str]]:
    """Return the header fields of a core metadata file, in order."""
    header = metadata.partition("\n\n")[0]
    fields = (line.partition(": ") for line in header.splitlines() if ": " in line)
    return [(name, value) for name, _, value in fields]


def test_built_metadata_declares_mit(built_dists: tuple[Path, Path]) -> None:
    """Wheel METADATA and sdist PKG-INFO both declare the MIT license (ITL-875)."""
    sdist, wheel = built_dists
    with zipfile.ZipFile(wheel) as zf:
        (name,) = [n for n in zf.namelist() if n.endswith(".dist-info/METADATA")]
        wheel_metadata = zf.read(name).decode()
    with tarfile.open(sdist) as tar:
        member = tar.extractfile(f"{sdist.name.removesuffix('.tar.gz')}/PKG-INFO")
        assert member is not None
        sdist_metadata = member.read().decode()
    for metadata in (wheel_metadata, sdist_metadata):
        fields = _metadata_fields(metadata)
        # License-Expression and License-File are defined from metadata 2.4 (PEP 639) on.
        (metadata_version,) = [v for k, v in fields if k == "Metadata-Version"]
        major, minor = (int(part) for part in metadata_version.split("."))
        assert (major, minor) >= (2, 4)
        assert ("License-Expression", "MIT") in fields
        assert ("License-File", "LICENSE") in fields
        assert not [v for k, v in fields if k == "License"]
        assert not [
            v for k, v in fields if k == "Classifier" and v.startswith("License ::")
        ]


def _declared_version(version_module: str) -> str:
    """Return the string assigned to ``__version__`` in a generated ``_version.py``."""
    for node in ast.parse(version_module).body:
        if not isinstance(node, ast.Assign):
            continue
        targets = {t.id for t in node.targets if isinstance(t, ast.Name)}
        if "__version__" in targets and isinstance(node.value, ast.Constant):
            assert isinstance(node.value.value, str)
            return node.value.value
    raise AssertionError("no __version__ assignment in _version.py")


def test_wheel_version_file_matches_its_metadata(
    built_dists: tuple[Path, Path],
) -> None:
    """The wheel's ``_version.py`` declares the wheel's own metadata version.

    Together with ``test_version_comes_from_generated_version_file`` this shows that
    ``orcapod.__version__`` in an installed wheel equals
    ``importlib.metadata.version("orcapod")``, without installing the wheel and its
    dependencies into a fresh environment.
    """
    wheel = built_dists[1]
    with zipfile.ZipFile(wheel) as zf:
        version_file = zf.read("orcapod/_version.py").decode()
        (metadata_name,) = [
            n for n in zf.namelist() if n.endswith(".dist-info/METADATA")
        ]
        metadata = zf.read(metadata_name).decode()
    match = re.search(r"^Version: (\S+)$", metadata, re.MULTILINE)
    assert match is not None
    assert _declared_version(version_file) == match.group(1)


_BLOCK_GENERATED_VERSION = """
import importlib.abc
import sys


class _BlockGeneratedVersion(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "orcapod._version":
            raise ImportError("simulated source tree without _version.py")
        return None


sys.meta_path.insert(0, _BlockGeneratedVersion())
"""

_HIDE_DISTRIBUTION_METADATA = """
import importlib.metadata


def _not_installed(name):
    raise importlib.metadata.PackageNotFoundError(name)


importlib.metadata.version = _not_installed
"""


def _run_python(script: str) -> str:
    """Run ``script`` in a fresh interpreter and return its stripped stdout.

    A subprocess keeps simulated import failures out of the test process.
    """
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _orcapod_version(prelude: str) -> str:
    """Return ``orcapod.__version__`` as seen after running ``prelude``."""
    return _run_python(f"{prelude}\nimport orcapod\nprint(orcapod.__version__)\n")


def test_version_comes_from_generated_version_file() -> None:
    """``orcapod.__version__`` is the hatch-vcs generated version (ITL-827)."""
    script = (
        "import orcapod\n"
        "from orcapod import _version\n"
        "print(orcapod.__version__, _version.__version__)\n"
    )
    reported, generated = _run_python(script).split()
    assert reported == generated


def test_version_falls_back_to_distribution_metadata() -> None:
    """Without ``_version.py`` the installed distribution metadata is used."""
    assert _orcapod_version(_BLOCK_GENERATED_VERSION) == version("orcapod")


def test_version_has_placeholder_when_nothing_is_available() -> None:
    """With neither ``_version.py`` nor metadata, import still succeeds."""
    prelude = _BLOCK_GENERATED_VERSION + _HIDE_DISTRIBUTION_METADATA
    assert _orcapod_version(prelude) == "0.0.0+unknown"
