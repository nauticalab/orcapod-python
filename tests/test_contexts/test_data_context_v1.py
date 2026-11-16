"""The data context of v0.3 is ``std:v1:default``, and released manifests are frozen (ITL-944).

A data context version names one generation of hashing. ``released_manifests.json`` pins,
for each version, the sha256 of its manifest without the ``golden`` section (``null`` while
the manifest is open) and, under ``releases``, the digest of each library release's
revision in the manifest's golden history (ITL-946). The rules of the golden history and
its folder are stated in ``tests/test_hashing/golden/store.py``.
"""

from __future__ import annotations

import json
from importlib import resources
from pathlib import Path
from typing import Any

import pytest

import orcapod
from orcapod import contexts
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    PythonTypeHandlerRegistry,
)
from orcapod.semantic_types.universal_converter import UniversalTypeConverter
from tests.test_hashing.golden import store

CONTEXTS_DIR = Path(str(resources.files("orcapod.contexts")))
MANIFEST_DIR = CONTEXTS_DIR / "data"


def _pins() -> dict[str, dict[str, Any]]:
    """The pin of each manifest, by version: ``{"manifest": ..., "releases": {...}}``."""
    return json.loads((CONTEXTS_DIR / "released_manifests.json").read_text())


def _manifest_files() -> dict[str, Path]:
    """The manifest files shipped in the package, by version."""
    return {path.stem: path for path in sorted(MANIFEST_DIR.glob("*.json"))}


def _manifest(version: str) -> dict[str, Any]:
    """A shipped manifest as parsed JSON."""
    return json.loads(_manifest_files()[version].read_text())


def _history(version: str) -> list[dict[str, Any]]:
    """The golden history of a shipped manifest."""
    return _manifest(version)["golden"]["history"]


def test_default_data_context_is_v1() -> None:
    """The default data context has key ``std:v1:default`` and version ``v1``."""
    assert contexts.get_default_context_key() == "std:v1:default"
    assert contexts.get_default_context().version == "v1"


def test_v1_resolves_by_version_and_by_key() -> None:
    """Both ``v1`` and ``std:v1:default`` resolve to the default context object."""
    default = contexts.get_default_context()
    assert contexts.resolve_context("v1") is default
    assert contexts.resolve_context("std:v1:default") is default


def test_every_manifest_is_listed() -> None:
    """The pinned versions are exactly the versions that have a manifest file."""
    assert set(_pins()) == set(_manifest_files())


@pytest.mark.parametrize("version", sorted(_pins()))
def test_a_pinned_manifest_is_unchanged(version: str) -> None:
    """A pinned manifest without its golden section still has the pinned sha256."""
    pinned = _pins()[version]["manifest"]
    if pinned is None:
        return
    assert store.fixed_manifest_digest(_manifest(version)) == pinned, (
        f"The released data context manifest {version}.json was edited. A released "
        "manifest is frozen: a change to hashing needs a new data context version with "
        "its own manifest file."
    )


def test_only_the_default_manifest_is_open() -> None:
    """At most one manifest has no pinned sha256, and it is the default version's."""
    open_versions = [v for v, pin in _pins().items() if pin["manifest"] is None]
    assert open_versions in ([], [contexts.get_default_context().version])


def test_a_final_release_has_its_releases_line_and_a_pinned_manifest() -> None:
    """A final release (not dev, rc or local) is recorded in the pins of its context."""
    pin = _pins()[contexts.get_default_context().version]
    assert store.final_release_problems(pin, orcapod.__version__) == []


# ---------------------------------------------------------------------------
# The golden history (ITL-946)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("version", sorted(_pins()))
def test_the_golden_history_chain_recomputes(version: str) -> None:
    """Each revision's digest recomputes and names the revision before it."""
    assert store.history_problems(_history(version)) == []


@pytest.mark.parametrize("version", sorted(_pins()))
def test_every_golden_file_is_listed_once_with_its_hash(version: str) -> None:
    """Every listed file exists with its sha256; every file is listed by one revision."""
    manifest = _manifest(version)
    folder = store.golden_folder(store.PROJECT_ROOT, manifest)
    assert store.folder_problems(manifest["golden"]["history"], folder) == []


@pytest.mark.parametrize("version", sorted(_pins()))
def test_the_pins_agree_with_the_golden_history(version: str) -> None:
    """Each pinned release is a release revision with that version and digest, and the
    reverse; ``released_in`` is set exactly when the manifest is pinned.
    """
    manifest = _manifest(version)
    problems = store.pin_problems(
        manifest, _pins()[version], manifest["golden"]["history"]
    )
    assert problems == []


# ---------------------------------------------------------------------------
# The contents of the manifest (ITL-938 design document, section 7.4)
# ---------------------------------------------------------------------------


def _v1() -> dict[str, object]:
    """The v1 manifest as parsed JSON."""
    return json.loads((MANIFEST_DIR / "v1.json").read_text())


def test_v1_follows_the_pre_v1_data_context() -> None:
    """The v1 manifest names ``std:v0.1:default`` as its previous context."""
    assert _v1()["previous"] == "std:v0.1:default"


def test_v1_names_its_definitions_libraries_and_changes() -> None:
    """The v1 manifest has named definitions, the hashing libraries, and changes that
    each refer to a definition.
    """
    manifest = _v1()
    definitions = manifest["definitions"]
    assert isinstance(definitions, dict) and definitions
    assert all(isinstance(name, str) and name for name in definitions.values())
    libraries = manifest["libraries"]
    assert isinstance(libraries, dict)
    assert {"starfix", "pyarrow", "polars"} <= set(libraries)
    changes = manifest["changes"]
    assert isinstance(changes, list) and changes
    for change in changes:
        assert change["definition"] in definitions or change["definition"] in (
            "arrow_hasher",
            "semantic_hasher",
        )


def test_v1_lists_no_logical_types() -> None:
    """Logical types are outside the data context version: registering one edits no manifest."""
    text = (MANIFEST_DIR / "v1.json").read_text()
    assert "orcapod.logical_types" not in text
    for name in ("numpy", "pandas", "uuid.UUID"):
        assert name not in text


def test_logical_type_registration_is_unchanged_by_the_move() -> None:
    """The default context registers exactly what the single file registered before.

    The Polars value types were added afterwards (ITL-578).
    """
    context = contexts.create_registry().get_context()
    handlers = context.semantic_hasher.type_handler_registry
    assert isinstance(handlers, PythonTypeHandlerRegistry)
    assert sorted(
        f"{t.__module__}.{t.__qualname__}={type(handlers.get_handler_for_type(t)).__name__}"
        for t in handlers.registered_types()
    ) == [
        "builtins.builtin_function_or_method=FunctionHandler",
        "builtins.bytearray=BytesHandler",
        "builtins.bytes=BytesHandler",
        "builtins.function=FunctionHandler",
        "builtins.method=FunctionHandler",
        "builtins.type=TypeObjectHandler",
        "numpy.ndarray=NumpyArrayHandler",
        "orcapod.logical_types.directory_type.Directory=DirectoryHandler",
        "orcapod.logical_types.file_type.File=FileHandler",
        "orcapod.types.Schema=SchemaHandler",
        "pandas.core.frame.DataFrame=PandasDataFrameHandler",
        "pandas.core.series.Series=PandasSeriesHandler",
        "polars.dataframe.frame.DataFrame=PolarsDataFrameHandler",
        "polars.expr.expr.Expr=PolarsExprHandler",
        "polars.series.series.Series=PolarsSeriesHandler",
        "pyarrow.lib.RecordBatch=ArrowTableHandler",
        "pyarrow.lib.Table=ArrowTableHandler",
        "types.GenericAlias=GenericAliasHandler",
        "types.UnionType=UnionTypeHandler",
        "typing._GenericAlias=GenericAliasHandler",
        "typing._SpecialForm=SpecialFormHandler",
        "uuid.UUID=UUIDHandler",
    ]
    # The logical type registry has no public listing; this test is about what it holds.
    converter = context.type_converter
    assert isinstance(converter, UniversalTypeConverter)
    registry = converter._logical_type_registry
    assert registry is not None
    assert sorted(registry._by_logical_name) == [
        "orcapod.directory",
        "orcapod.file",
        "orcapod.numpy.ndarray",
        "orcapod.pandas.dataframe",
        "orcapod.pandas.series",
        "orcapod.path",
        "orcapod.polars.dataframe",
        "orcapod.polars.series",
        "orcapod.upath",
        "orcapod.uuid",
    ]
    assert sorted(registry._category_factories) == [
        "list",
        "orcapod.dataclass",
        "orcapod.pydantic",
        "orcapod.structural",
        "set",
    ]
    assert sorted(
        f"{t.__module__}.{t.__qualname__}" for t in registry._python_class_factories
    ) == ["builtins.object", "pydantic.main.BaseModel"]


# ---------------------------------------------------------------------------
# The data context of Orcapod before v0.3 is not shipped
# ---------------------------------------------------------------------------


def test_only_the_v1_manifest_is_shipped() -> None:
    """The only manifest file and the only available context are ``v1``."""
    assert sorted(_manifest_files()) == ["v1"]
    assert contexts.get_available_contexts() == ["v1"]


@pytest.mark.parametrize("name", ["std:v0.1:default", "v0.1"])
def test_the_pre_v1_data_context_does_not_resolve(name: str) -> None:
    """Resolving the pre-v1 context raises an error naming it, v0.3 and ``orcapod-
    sanctuary``.
    """
    with pytest.raises(contexts.ContextResolutionError) as info:
        _ = contexts.resolve_context(name)
    message = str(info.value)
    assert "std:v0.1:default" in message
    assert "before v0.3" in message
    assert "orcapod-sanctuary" in message


@pytest.mark.parametrize("name", ["custom:v1:production", "std:v1:fast"])
def test_a_full_key_that_is_not_the_manifests_key_does_not_resolve(name: str) -> None:
    """Only the version part of a key selects the manifest; the whole key must match it."""
    with pytest.raises(contexts.ContextResolutionError) as info:
        _ = contexts.resolve_context(name)
    assert name in str(info.value)
    assert "std:v1:default" in str(info.value)
