"""Workshop scenarios W1–W12 for user class identity (ITL-881).

Each scenario runs notebook cells (IPython ``InteractiveShell``, as a Jupyter kernel runs
them) or scripts in fresh processes, against one ``DeltaTableDatabase``, and drives the
pipeline only through the public API (``DictSource``, ``function_pod``, ``PipelineJob``,
``ExtensionAwareDatabase``). Scenario numbers follow the findings in PR #288.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.test_user_types.child_harness import WSJOB, run_child, write_files

pytestmark = pytest.mark.integration

KERNEL = r"""
import warnings
from IPython.core.interactiveshell import InteractiveShell
from child_harness import error_text, report

shell = InteractiveShell.instance()
CELLS = {
    "setup": "import wsjob\nfrom dataclasses import dataclass\nimport orcapod as op\n",
    "define": "@dataclass\nclass Point:\n    x: int\n    y: int\n",
    "define_reordered": "@dataclass\nclass Point:\n    y: int\n    x: int\n",
    "define_added": "@dataclass\nclass Point:\n    x: int\n    y: int\n    z: int = 0\n",
    "define_model_reordered": "import pydantic\nclass Point(pydantic.BaseModel):\n    y: int\n    x: int\n",
    "define_s": "@op.dataclass\nclass Point:\n    x: int\n    y: int\n",
    "define_s_float": "@op.dataclass\nclass Point:\n    x: int\n    y: float\n",
    "define_float": "@dataclass\nclass Point:\n    x: int\n    y: float\n",
    "define_model": "import pydantic\nclass Point(pydantic.BaseModel):\n    x: int\n    y: int\n",
    "import_mod": "from wsmodels import Point\n",
    "reload_mod": "import importlib, wsmodels\nimportlib.reload(wsmodels)\nfrom wsmodels import Point\n",
    "pod": "shift = wsjob.make_pod(Point)\n",
    "run": "_result = wsjob.run_job(Point)\n",
    "run_pod": "_result = wsjob.run_job(Point, shift)\n",
}
log = []
for step in SCENARIO:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        res = shell.run_cell(CELLS[step], store_history=False, silent=True)
    entry = {
        "cell": step,
        "warnings": [str(w.message) for w in caught if issubclass(w.category, UserWarning)],
    }
    err = res.error_in_exec or res.error_before_exec
    if err is not None:
        entry["error"] = error_text(err)
    elif step.startswith("run"):
        entry.update(shell.user_ns["_result"])
    log.append(entry)
report(log)
"""

WSMODELS = """
from dataclasses import dataclass


@dataclass
class Point:
    '''The scenario's ``Point`` class.'''
    x: int
    y: int
"""


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    """Return a working directory with the job modules and an empty ``db`` directory."""
    write_files(tmp_path, {"wsjob.py": WSJOB, "wsmodels.py": WSMODELS})
    (tmp_path / "db").mkdir()
    return tmp_path


def _kernel(workdir: Path, cells: list[str]) -> list[dict[str, Any]]:
    """Run ``cells`` in a fresh kernel process and return one log entry per cell."""
    code = f"SCENARIO = {cells!r}\n" + KERNEL
    return run_child(["-c", code], cwd=workdir, env={"WS_DB": str(workdir / "db")})


def _runs(log: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Assert that no cell failed and return the log entries of the run cells."""
    errors = [e for e in log if "error" in e]
    assert not errors, errors
    return [e for e in log if e["cell"].startswith("run")]


def _script(workdir: Path, code: str, **env: str) -> Any:
    """Run ``code`` in a fresh process against the working store; return its report."""
    return run_child(
        ["-c", code], cwd=workdir, env={"WS_DB": str(workdir / "db"), **env}
    )


# ---------------------------------------------------------------------------
# Subclasses, whatever the registration order (audit ITL-942, finding R)
# ---------------------------------------------------------------------------

WSFAMILY = """
from dataclasses import dataclass

import pydantic


@dataclass
class Base:
    '''The scenario's ``Base`` class.'''
    x: int


@dataclass
class Child(Base):
    '''The scenario's ``Child`` class.'''
    z: int = 0


class BaseModel(pydantic.BaseModel):
    '''The scenario's ``BaseModel`` class.'''
    x: int


class ChildModel(BaseModel):
    '''The scenario's ``ChildModel`` class.'''
    z: int = 0
"""

FAMILY_SCRIPT = """
import os

import wsfamily
import wsjob
from child_harness import report
from orcapod.sources import DictSource

base = getattr(wsfamily, os.environ["WS_BASE"])
child = getattr(wsfamily, os.environ["WS_CHILD"])


def use_base():
    '''Return the Arrow extension name the base class is stored under.'''
    source = DictSource(data=[{"k": "a", "p": base(x=1)}], tag_columns=["k"])
    return source.as_table().schema.field("p").type.extension_name


def use_child():
    '''Run the job on the child class and return its result.'''
    return wsjob.run_job(child, rows=[{"x": 1, "z": 10}, {"x": 2, "z": 20}])


out = {}
if os.environ["WS_ORDER"] == "base_first":
    out["base_name"] = use_base()
    out["child"] = use_child()
else:
    out["child"] = use_child()
    out["base_name"] = use_base()
report(out)
"""


@pytest.mark.parametrize(
    ("base", "child"),
    [("Base", "Child"), ("BaseModel", "ChildModel")],
    ids=["dataclass", "pydantic"],
)
def test_subclass_identity_and_fields_do_not_depend_on_registration_order(
    workdir: Path, base: str, child: str
) -> None:
    """A subclass keeps its own identity and all its fields in both orders.

    Each order runs in a fresh process against a fresh database. On ``main`` the subclass
    was stored as its base when the base was registered first: its extra field was
    dropped, and the pod's hash and stored path depended on the order.
    """
    write_files(workdir, {"wsfamily.py": WSFAMILY})
    out: dict[str, Any] = {}
    for order in ("base_first", "sub_first"):
        (workdir / order).mkdir()
        out[order] = run_child(
            ["-c", FAMILY_SCRIPT],
            cwd=workdir,
            env={
                "WS_DB": str(workdir / order),
                "WS_ORDER": order,
                "WS_BASE": base,
                "WS_CHILD": child,
            },
        )

    for order, result in out.items():
        run = result["child"]
        assert (run["calls"], run["rows"]) == (2, 2), order
        assert run["q_class_is_point"], order
        assert run["extension_name"].split("@")[0] == f"wsfamily.{child}", order
        assert run["extension_name"] != result["base_name"], order
        assert run["stored_fields"] == [["x", "z"]], order
        assert run["values"] == [f"{child}(x=1, z=10)", f"{child}(x=2, z=20)"], order

    first, second = out["base_first"], out["sub_first"]
    assert first["base_name"] == second["base_name"]
    for key in (
        "extension_name",
        "pod_pipeline_hash",
        "pod_uri",
        "node_identity_path",
        "table_paths",
        "stored_fields",
        "values",
    ):
        assert first["child"][key] == second["child"][key], key


# ---------------------------------------------------------------------------
# Notebook cell re-runs in one kernel
# ---------------------------------------------------------------------------


def test_w1_rerun_define_cell_same_fields_rebinds(workdir: Path) -> None:
    """Re-running the class cell unchanged reuses the stored results."""
    log = _kernel(workdir, ["setup", "define", "run", "define", "run"])
    first, second = _runs(log)
    assert (first["calls"], first["rows"]) == (2, 2)
    assert (second["calls"], second["rows"]) == (0, 2)
    assert second["q_class_is_point"]
    assert first["pod_pipeline_hash"] == second["pod_pipeline_hash"]
    assert first["values"] == ["Point(x=10, y=11)", "Point(x=20, y=21)"]
    assert second["values"] == first["values"]


def test_w1_old_pod_cell_refuses_a_redefined_ordinary_class(workdir: Path) -> None:
    """After re-running only the class cell, the earlier pod refuses the new class."""
    log = _kernel(workdir, ["setup", "define", "pod", "run_pod", "define", "run_pod"])
    assert [e["cell"] for e in log if "error" in e] == ["run_pod"]
    assert "error" not in log[3]
    assert log[-1]["error"].startswith("ValueError")
    assert "column 'p'" in log[-1]["error"]


def test_w1_old_pod_cell_with_redefined_structural_class(workdir: Path) -> None:
    """Re-running only an ``op.dataclass`` cell keeps the earlier pod cell usable."""
    log = _kernel(
        workdir, ["setup", "define_s", "pod", "run_pod", "define_s", "run_pod"]
    )
    first, second = _runs(log)
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)
    assert first["extension_name"].split("@")[0] == "Point"


def test_w2_structural_rerun_with_changed_fields_is_a_new_type(workdir: Path) -> None:
    """An ``op.dataclass`` re-run with a changed field type is a new type."""
    log = _kernel(workdir, ["setup", "define_s", "run", "define_s_float", "run"])
    first, second = _runs(log)
    assert (second["calls"], second["rows"]) == (2, 2)
    assert first["extension_name"] != second["extension_name"]
    assert set(first["table_paths"]) < set(second["table_paths"])


def test_w1_run_all_twice(workdir: Path) -> None:
    """Running all cells a second time reuses the stored results."""
    log = _kernel(workdir, ["setup", "define", "pod", "run_pod"] * 2)
    first, second = _runs(log)
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)


@pytest.mark.parametrize(
    ("define", "reordered"),
    [("define", "define_reordered"), ("define_model", "define_model_reordered")],
    ids=["dataclass", "pydantic"],
)
def test_reordered_fields_are_a_new_type(
    workdir: Path, define: str, reordered: str
) -> None:
    """Declared field order is identity: the reordered class computes into new tables."""
    log = _kernel(workdir, ["setup", define, "run", reordered, "run", define, "run"])
    first, second, third = _runs(log)
    assert (second["calls"], second["rows"]) == (2, 2)
    assert second["q_class_is_point"]
    assert first["extension_name"] != second["extension_name"]
    assert first["pod_pipeline_hash"] != second["pod_pipeline_hash"]
    assert first["pod_uri"] != second["pod_uri"]
    assert set(first["table_paths"]) < set(second["table_paths"])
    assert first["stored_fields"] == [["x", "y"]]
    assert second["stored_fields"] == [["x", "y"], ["y", "x"]]
    assert first["values"] == ["Point(x=10, y=11)", "Point(x=20, y=21)"]
    assert second["values"] == ["Point(y=10, x=11)", "Point(y=20, x=21)"]
    assert third["values"] == first["values"]
    # The first order's results are untouched and found again.
    assert (third["calls"], third["rows"]) == (0, 2)
    assert third["pod_pipeline_hash"] == first["pod_pipeline_hash"]


def test_class_redefined_with_an_added_field_is_a_new_type(workdir: Path) -> None:
    """An added field (even with a default) makes a new type with its own tables."""
    log = _kernel(
        workdir, ["setup", "define", "run", "define_added", "run", "define", "run"]
    )
    first, second, third = _runs(log)
    assert (second["calls"], second["rows"]) == (2, 2)
    assert second["q_class_is_point"]
    assert first["extension_name"] != second["extension_name"]
    assert first["pod_pipeline_hash"] != second["pod_pipeline_hash"]
    assert set(first["table_paths"]) < set(second["table_paths"])
    assert second["stored_fields"] == [["x", "y"], ["x", "y", "z"]]
    assert second["values"] == ["Point(x=10, y=11, z=12)", "Point(x=20, y=21, z=22)"]
    # The two-field results are untouched and found again.
    assert (third["calls"], third["rows"]) == (0, 2)
    assert third["values"] == first["values"]


def test_w2_rerun_with_changed_fields_is_a_new_type(workdir: Path) -> None:
    """Re-running with a changed field type gives a new type with its own tables."""
    log = _kernel(workdir, ["setup", "define", "run", "define_float", "run"])
    first, second = _runs(log)
    assert (second["calls"], second["rows"]) == (2, 2)
    assert second["q_class_is_point"]
    assert first["pod_pipeline_hash"] != second["pod_pipeline_hash"]
    assert first["extension_name"].split("@")[0] == "__main__.Point"
    assert second["extension_name"].split("@")[0] == "__main__.Point"
    assert first["extension_name"] != second["extension_name"]
    assert set(first["table_paths"]) < set(second["table_paths"])


def test_w3_pydantic_rerun_rebinds(workdir: Path) -> None:
    """Re-running a pydantic class cell unchanged reuses the stored results."""
    log = _kernel(workdir, ["setup", "define_model", "run", "define_model", "run"])
    first, second = _runs(log)
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)


def test_dataclass_and_pydantic_with_same_name_and_fields_are_distinct(
    workdir: Path,
) -> None:
    """A dataclass and a pydantic model with one name and fields are distinct types."""
    log = _kernel(workdir, ["setup", "define", "run", "define_model", "run"])
    first, second = _runs(log)
    assert second["calls"] == 2
    assert first["pod_pipeline_hash"] != second["pod_pipeline_hash"]
    assert first["extension_name"] != second["extension_name"]


def test_w4_module_reload_rebinds(workdir: Path) -> None:
    """Reloading the class's module reuses the stored results."""
    log = _kernel(workdir, ["setup", "import_mod", "run", "reload_mod", "run"])
    first, second = _runs(log)
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)
    assert first["extension_name"].startswith("wsmodels.Point@")


# ---------------------------------------------------------------------------
# Across processes
# ---------------------------------------------------------------------------


def test_w5_restart_same_class_hits_cache(workdir: Path) -> None:
    """The same class in a restarted kernel reuses the stored results."""
    first = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    second = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)


def test_w6_restart_with_changed_fields_recomputes(workdir: Path) -> None:
    """Changed fields after a restart compute into new tables instead of 0 rows."""
    first = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    changed = _runs(_kernel(workdir, ["setup", "define_float", "run"]))[0]
    back = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    assert (changed["calls"], changed["rows"]) == (2, 2)
    assert first["node_identity_path"] != changed["node_identity_path"]
    assert (back["calls"], back["rows"]) == (0, 2)


def test_w7_script_as_main_twice(workdir: Path) -> None:
    """A script run as a file and then with ``-m`` reuses the stored results."""
    write_files(
        workdir,
        {
            "pkg/__init__.py": "",
            "pkg/pipeline.py": """
                from dataclasses import dataclass
                import wsjob
                from child_harness import report

                @dataclass
                class Point:
                    '''The scenario's ``Point`` class.'''
                    x: int
                    y: int

                def main():
                    '''Run the job on ``Point`` and report its result.'''
                    report(wsjob.run_job(Point))

                if __name__ == "__main__":
                    main()
            """,
        },
    )
    env = {"WS_DB": str(workdir / "db")}
    first = run_child(["pkg/pipeline.py"], cwd=workdir, env=env)
    second = run_child(["-m", "pkg.pipeline"], cwd=workdir, env=env)
    assert (first["calls"], second["calls"]) == (2, 0)


def test_w8_script_then_import_hits_with_a_structural_class(workdir: Path) -> None:
    """An ``op.dataclass`` in a module run as main and then imported shares results."""
    write_files(
        workdir,
        {
            "pkg/__init__.py": "",
            "pkg/pipeline.py": """
                import orcapod as op
                import wsjob
                from child_harness import report

                @op.dataclass
                class Point:
                    '''The scenario's ``Point`` class.'''
                    x: int
                    y: int

                def main():
                    '''Run the job on ``Point`` and report its result.'''
                    report(wsjob.run_job(Point))

                if __name__ == "__main__":
                    main()
            """,
        },
    )
    env = {"WS_DB": str(workdir / "db")}
    first = run_child(["-m", "pkg.pipeline"], cwd=workdir, env=env)
    second = run_child(
        ["-c", "import pkg.pipeline; pkg.pipeline.main()"], cwd=workdir, env=env
    )
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)
    assert first["node_identity_path"] == second["node_identity_path"]


def test_w11_notebook_to_module_with_a_structural_class(workdir: Path) -> None:
    """An ``op.dataclass`` moved from a notebook into a module keeps its results."""
    write_files(
        workdir,
        {
            "pkg/__init__.py": "",
            "pkg/models.py": """
                import orcapod as op

                @op.dataclass
                class Point:
                    '''The scenario's ``Point`` class.'''
                    x: int
                    y: int
            """,
        },
    )
    notebook = _runs(_kernel(workdir, ["setup", "define_s", "run"]))[0]
    module = _script(
        workdir,
        "import wsjob\nfrom child_harness import report\nfrom pkg.models import Point\n"
        "report(wsjob.run_job(Point))\n",
    )
    assert (module["calls"], module["rows"]) == (0, 2)
    assert module["pod_pipeline_hash"] == notebook["pod_pipeline_hash"]
    assert module["node_identity_path"] == notebook["node_identity_path"]
    assert module["table_paths"] == notebook["table_paths"]
    assert module["extension_name"] == notebook["extension_name"]
    assert module["values"] == ["Point(x=10, y=11)", "Point(x=20, y=21)"]
    assert module["values"] == notebook["values"]


def test_w11_ordinary_module_class_is_a_different_type(workdir: Path) -> None:
    """An ordinary class in a module is another type than the notebook's class."""
    notebook = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    module = _runs(_kernel(workdir, ["setup", "import_mod", "run"]))[0]
    assert module["calls"] == 2
    assert module["pod_pipeline_hash"] != notebook["pod_pipeline_hash"]


# ---------------------------------------------------------------------------
# Reading stored values
# ---------------------------------------------------------------------------

READER = """
import os
from pathlib import Path
from child_harness import error_text, report
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, ExtensionAwareDatabase
import wsjob

{define}

root = Path(os.environ["WS_DB"])
path = tuple(next(p for p in wsjob.table_paths(root) if "/_result/" in p).split("/"))
converter = get_default_context().type_converter
try:
    table = ExtensionAwareDatabase(DeltaTableDatabase(root), converter=converter).get_all_records(path)
    rows = converter.arrow_table_to_python_dicts(table.select(["q"]))
    report({{"values": sorted(repr(r["q"]) for r in rows), "classes": sorted({{type(r["q"]).__module__ + "." + type(r["q"]).__qualname__ for r in rows}})}})
except Exception as exc:
    report({{"error": error_text(exc), "message": str(exc)}})
"""


def _store_from_notebook(workdir: Path, define: str = "define") -> None:
    """Store results for the notebook ``Point`` class in the working store."""
    _runs(_kernel(workdir, ["setup", define, "run"]))


def test_w12_read_notebook_results_from_script_with_same_class(workdir: Path) -> None:
    """A script defining the same class reads the notebook's stored values."""
    _store_from_notebook(workdir)
    define = "from dataclasses import dataclass\n@dataclass\nclass Point:\n    x: int\n    y: int\n"
    out = _script(workdir, READER.format(define=define))
    assert out.get("error") is None, out
    assert out["values"] == ["Point(x=10, y=11)", "Point(x=20, y=21)"]
    assert out["classes"] == ["__main__.Point"]


def test_read_with_a_structural_class_from_a_module(workdir: Path) -> None:
    """A structural class of the stored name and fields in a module reads the values."""
    _store_from_notebook(workdir, "define_s")
    write_files(
        workdir,
        {
            "geo.py": """
                import orcapod as op

                @op.dataclass
                class Point:
                    '''The scenario's ``Point`` class.'''
                    x: int
                    y: int
            """
        },
    )
    out = _script(workdir, READER.format(define="import geo"))
    assert out.get("error") is None, out
    assert out["classes"] == ["geo.Point"]


def test_read_without_an_ordinary_class_names_it(workdir: Path) -> None:
    """A read without the ordinary class raises, naming the column, class and fields."""
    _store_from_notebook(workdir)
    out = _script(workdir, READER.format(define=""))
    assert out["error"].startswith("ImportError: Column 'q'")
    assert "'__main__.Point'" in out["message"]
    assert "{x: int, y: int}" in out["message"]


def test_read_without_a_structural_class_builds_it(workdir: Path) -> None:
    """A read without the ``op.dataclass`` gives values of a class built from metadata."""
    _store_from_notebook(workdir, "define_s")
    out = _script(workdir, READER.format(define=""))
    assert out.get("error") is None, out
    assert out["values"] == ["Point(x=10, y=11)", "Point(x=20, y=21)"]
    assert out["classes"] == ["orcapod.synthesized.Point"]


def test_ordinary_module_class_does_not_read_notebook_results(workdir: Path) -> None:
    """An ordinary class with the same fields at another module path is not the type."""
    _store_from_notebook(workdir)
    write_files(
        workdir,
        {
            "geo.py": """
                from dataclasses import dataclass

                @dataclass
                class Point:
                    '''The scenario's ``Point`` class.'''
                    x: int
                    y: int
            """
        },
    )
    out = _script(workdir, READER.format(define="import geo"))
    assert out["error"].startswith("ImportError: Column 'q'"), out


def test_read_with_different_fields_raises_naming_both_shapes(workdir: Path) -> None:
    """A read with a class of other fields raises, naming both shapes."""
    _store_from_notebook(workdir)
    define = "from dataclasses import dataclass\n@dataclass\nclass Point:\n    x: int\n    y: float\n"
    out = _script(workdir, READER.format(define=define))
    assert out["error"].startswith("TypeIdentityError: Column 'q'"), out
    assert "{x: int, y: int}" in out["message"]
    assert "{x: int, y: float}" in out["message"]
