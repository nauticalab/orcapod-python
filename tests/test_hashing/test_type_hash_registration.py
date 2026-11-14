"""A type's hash does not depend on whether its class was registered (ITL-1137).

A user class (an ordinary dataclass, an ``orcapod.dataclass``, a pydantic model) gets its
logical type when something registers it: a function pod naming it, a source built from its
values. Before ITL-1137 the type hasher only looked the class up, so a schema hashed before
registration hashed the class by module path and qualified name, and the same schema hashed
after registration by its logical type identifier: two hashes for one pipeline node, two stored
tables for one node. Hashing a type now resolves its logical type, registering it when a factory
covers the class, so the hash before and after is the registered one, the value the golden suite
pins.

Each test imports its classes from a module written for it alone, so no other test has
registered them; a class defined inside a function cannot be stored.
"""

import importlib
import itertools
import json
import subprocess
import sys
import textwrap
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from orcapod import SourceNode, function_pod
from orcapod.contexts import get_default_context
from orcapod.sources import DictSource
from orcapod.types import Schema

_module_numbers = itertools.count()


@pytest.fixture
def fresh_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Return a function that writes Python source to a new module and imports it."""
    monkeypatch.syspath_prepend(str(tmp_path))

    def load(source: str) -> ModuleType:
        name = f"itl1137_classes_{next(_module_numbers)}"
        _ = (tmp_path / f"{name}.py").write_text(textwrap.dedent(source))
        return importlib.import_module(name)

    return load


def _slot(data_type: Any) -> SourceNode:
    """A slot with one tag column and one data column of ``data_type``."""
    return SourceNode(
        "slot", tag_schema=Schema({"id": int}), data_schema=Schema({"p": data_type})
    )


def _slot_hashes(data_type: Any) -> tuple[str, str]:
    """The pipeline hash and content hash of a slot holding ``data_type``."""
    node = _slot(data_type)
    return node.pipeline_hash().to_string(), node.content_hash().to_string()


def _schema_hash(data_type: Any) -> str:
    """The hash of a schema holding ``data_type``, hashed on its own."""
    hasher = get_default_context().semantic_hasher
    return hasher.hash_object(Schema({"p": data_type})).to_string()


def _register_by_building_a_source(data_type: Any, value: Any) -> DictSource:
    """Build a source of ``data_type``: the source registers the class it holds."""
    return DictSource(
        [{"id": 1, "p": value}],
        tag_columns=["id"],
        data_schema={"id": int, "p": data_type},
        source_id="s",
    )


# Each module defines ``TYPE``, the column type, and ``VALUE``, a value of it.
_KINDS = {
    "exact dataclass": """
        import dataclasses

        @dataclasses.dataclass
        class Point:
            x: int
            label: str

        TYPE, VALUE = Point, Point(1, "a")
    """,
    "structural class": """
        import orcapod

        @orcapod.dataclass
        class Point:
            x: int
            label: str

        TYPE, VALUE = Point, Point(1, "a")
    """,
    "pydantic model": """
        import pydantic

        class Point(pydantic.BaseModel):
            x: int
            label: str

        TYPE, VALUE = Point, Point(x=1, label="a")
    """,
    "nested exact": """
        import dataclasses

        @dataclasses.dataclass
        class Inner:
            x: int

        @dataclasses.dataclass
        class Outer:
            inner: Inner
            label: str

        TYPE, VALUE = Outer, Outer(Inner(1), "a")
    """,
    "nested structural": """
        import orcapod

        @orcapod.dataclass
        class Inner:
            x: int

        @orcapod.dataclass
        class Outer:
            inner: Inner
            label: str

        TYPE, VALUE = Outer, Outer(Inner(1), "a")
    """,
    "list of a dataclass": """
        import dataclasses

        @dataclasses.dataclass
        class Point:
            x: int

        TYPE, VALUE = list[Point], [Point(1), Point(2)]
    """,
    "optional dataclass": """
        import dataclasses

        @dataclasses.dataclass
        class Point:
            x: int

        TYPE, VALUE = Point | None, Point(1)
    """,
}


@pytest.mark.parametrize("source", _KINDS.values(), ids=list(_KINDS))
def test_a_slot_hashes_the_same_before_and_after_registration(
    fresh_module: Any, source: str
) -> None:
    """A slot's pipeline hash and content hash do not move when a source registers."""
    module = fresh_module(source)
    before = _slot_hashes(module.TYPE)
    built = _register_by_building_a_source(module.TYPE, module.VALUE)
    assert _slot_hashes(module.TYPE) == before
    # The value is the registered one: the slot's pipeline hash is the source's.
    assert before[0] == built.pipeline_hash().to_string()


@pytest.mark.parametrize("source", _KINDS.values(), ids=list(_KINDS))
def test_a_schema_hashes_the_same_before_and_after_registration(
    fresh_module: Any, source: str
) -> None:
    """A schema hashed on its own gives the registered value, also before registration."""
    module = fresh_module(source)
    before = _schema_hash(module.TYPE)
    _register_by_building_a_source(module.TYPE, module.VALUE)
    assert _schema_hash(module.TYPE) == before


def test_a_function_pod_defined_after_the_slot_sees_the_same_slot_hash(
    fresh_module: Any,
) -> None:
    """A slot hashed before the pod that names its class keeps its hash afterwards."""
    module = fresh_module(
        """
        import dataclasses

        @dataclasses.dataclass
        class Point:
            x: int
        """
    )
    early_slot = _slot(module.Point)
    before = early_slot.pipeline_hash()
    pods = fresh_module(
        f"""
        from orcapod import function_pod
        from {module.__name__} import Point

        @function_pod(output_keys="y")
        def read_x(p: Point) -> int:
            return p.x
        """
    )
    late_slot = _slot(module.Point)
    assert late_slot.pipeline_hash() == before
    early, late = pods.read_x.pod(early_slot), pods.read_x.pod(late_slot)
    assert early.pipeline_hash() == late.pipeline_hash()


def test_two_classes_with_one_qualified_name_and_different_fields_hash_apart(
    fresh_module: Any,
) -> None:
    """The class's identity enters the hash, not only its module path and name."""
    source = """
        import dataclasses
        import orcapod

        @dataclasses.dataclass
        class Point:
            x: int
            label: str

        FIRST = Point

        @dataclasses.dataclass
        class Point:
            x: float

        SECOND = Point

        @orcapod.dataclass
        class Point:
            x: int
            label: str

        THIRD = Point
    """
    module = fresh_module(source)
    classes = (module.FIRST, module.SECOND, module.THIRD)
    assert len({_slot(cls).pipeline_hash() for cls in classes}) == 3


def test_a_class_no_factory_covers_hashes_by_its_name_stably(
    fresh_module: Any,
) -> None:
    """A class Orcapod has no logical type for hashes as ``type:<module>.<qualname>``."""
    module = fresh_module(
        """
        class Opaque:
            pass
        """
    )
    opaque = module.Opaque
    hasher = get_default_context().semantic_hasher
    expected = hasher.hash_object(f"type:{module.__name__}.Opaque")
    assert hasher.hash_object(opaque) == expected
    slot_hash = _slot(opaque).pipeline_hash()
    assert hasher.hash_object(opaque) == expected
    assert _slot(opaque).pipeline_hash() == slot_hash
    assert get_default_context().type_converter.get_logical_type(opaque) is None


def test_hashing_a_type_has_no_effect_beyond_registering_it(fresh_module: Any) -> None:
    """The first hash registers the class; hashing again registers nothing more."""
    module = fresh_module(
        """
        import dataclasses

        @dataclasses.dataclass
        class Point:
            x: int
        """
    )
    converter = get_default_context().type_converter
    hasher = get_default_context().semantic_hasher
    # The registry has no public size; the test is about what hashing adds to it.
    registry = converter._logical_type_registry  # pyright: ignore[reportAttributeAccessIssue]

    def registry_size() -> tuple[int, int, int]:
        return (
            len(registry._by_python_type),
            len(registry._by_arrow_name),
            len(registry._by_logical_name),
        )

    first = hasher.hash_object(module.Point)
    logical_type = converter.get_logical_type(module.Point)
    assert logical_type is not None
    size = registry_size()
    assert hasher.hash_object(module.Point) == first
    _ = hasher.hash_object(Schema({"p": module.Point}))
    _ = _slot(module.Point).pipeline_hash()
    assert registry_size() == size
    assert converter.get_logical_type(module.Point) is logical_type


def test_a_function_annotated_with_a_class_hashes_the_same_before_and_after(
    fresh_module: Any,
) -> None:
    """A parameter annotation resolves its class as a type hashed as a value does."""
    module = fresh_module(
        """
        import dataclasses

        @dataclasses.dataclass
        class Point:
            x: int

        def first_x(p: Point, rest: list[Point] | None = None) -> int:
            return p.x
        """
    )
    hasher = get_default_context().semantic_hasher
    before = hasher.hash_object(module.first_x)
    _ = _register_by_building_a_source(module.Point, module.Point(1))
    assert hasher.hash_object(module.first_x) == before


def test_a_data_function_stores_one_signature_hash_whatever_was_registered_first(
    fresh_module: Any,
) -> None:
    """The signature hash stored with every result row does not depend on the order."""
    module = fresh_module(
        """
        import dataclasses

        @dataclasses.dataclass
        class Point:
            x: int

        def read_x(p: Point) -> int:
            return p.x
        """
    )

    def stored_signature_hash() -> bytes:
        pod = function_pod(output_keys="y")(module.read_x)
        return pod.pod.data_function.get_function_variation_data()[
            "function_signature_hash"
        ]

    # The first pod is built before anything has registered Point.
    before = stored_signature_hash()
    assert stored_signature_hash() == before


# ---------------------------------------------------------------------------
# One store, two processes: a pipeline built before the source, then after it
# ---------------------------------------------------------------------------

_CLASS_MODULE = textwrap.dedent(
    """\
    import dataclasses


    @dataclasses.dataclass
    class Reading:
        value: int
        unit: str
    """
)

_RUN_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys
    import warnings

    warnings.simplefilter("ignore")
    sys.path.insert(0, sys.argv[1])
    store, order = sys.argv[2], sys.argv[3]

    from readings import Reading

    from orcapod import Pipeline, PipelineJob, SourceNode, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import MapData
    from orcapod.sources import DictSource
    from orcapod.types import CacheMode, Schema


    @function_pod(output_keys="doubled")
    def double(n: int) -> int:
        return n * 2


    def source():
        return DictSource(
            [{"id": 1, "r": Reading(3, "mV"), "n": 3}],
            tag_columns=["id"],
            data_schema={"id": int, "r": Reading, "n": int},
            source_id="readings",
        )


    if order == "source_first":
        bound = source()
    pipeline = Pipeline("demo")
    with pipeline:
        slot = SourceNode(
            "slot",
            tag_schema=Schema({"id": int}),
            data_schema=Schema({"r": Reading, "n": int}),
        )
        MapData({"r": "reading"})(slot, label="rename")
        double.pod(slot, label="double")
    database = DeltaTableDatabase(store)
    job = PipelineJob.from_pipeline(pipeline, database=database.at("job"))
    job.nodes["rename"].attach_databases(
        pipeline_database=database.at("operators"),
        cache_mode=CacheMode.LOG,
    )
    # Look at the job before any source exists, as a user printing its nodes would.
    for node in job.nodes.values():
        node.pipeline_hash()
    if order != "source_first":
        bound = source()
    job.bind(sources={"slot": bound})
    job.run()
    summary = job.last_run.nodes["double"]
    print(json.dumps({"computed": summary.computed, "from_store": summary.reused}))
    """
)


def _run(tmp_path: Path, order: str) -> dict[str, int]:
    """Run the job in a fresh interpreter, building the pipeline or the source first."""
    result = subprocess.run(
        [
            sys.executable,
            str(tmp_path / "run_job.py"),
            str(tmp_path),
            str(tmp_path / "store"),
            order,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_one_stored_table_per_node_whatever_is_built_first(tmp_path: Path) -> None:
    """A job whose pods never name the class stores each node at one path."""
    from orcapod.databases import DeltaTableDatabase

    _ = (tmp_path / "readings.py").write_text(_CLASS_MODULE)
    _ = (tmp_path / "run_job.py").write_text(_RUN_SCRIPT)
    first = _run(tmp_path, "pipeline_first")
    second = _run(tmp_path, "source_first")
    third = _run(tmp_path, "pipeline_first")
    assert first == {"computed": 1, "from_store": 0}
    assert second == third == {"computed": 0, "from_store": 1}

    job_tables = DeltaTableDatabase(tmp_path / "store" / "job").list_table_paths()
    pipeline_tables = [p for p in job_tables if p[-1] == "pdb_v2"]
    assert len(pipeline_tables) == 1, pipeline_tables
    operator_tables = DeltaTableDatabase(
        tmp_path / "store" / "operators"
    ).list_table_paths()
    assert len(operator_tables) == 1, operator_tables


_SAVE_LOAD_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys
    import warnings

    warnings.simplefilter("ignore")
    sys.path.insert(0, sys.argv[1])
    work, mode = sys.argv[2], sys.argv[3]

    from readings import Reading

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import MapData
    from orcapod.sources import DictSource

    # Building the source registers Reading, in both processes before the job exists.
    source = DictSource(
        [{"id": 1, "r": Reading(3, "mV"), "n": 3}, {"id": 2, "r": Reading(4, "V"), "n": 4}],
        tag_columns=["id"],
        data_schema={"id": int, "r": Reading, "n": int},
        source_id="readings",
    )
    if mode == "save":

        @function_pod(output_keys="unit")
        def unit_of(r: Reading) -> str:
            return r.unit

        job = PipelineJob(name="demo", database=DeltaTableDatabase(work + "/store"))
        with job:
            renamed = MapData({"n": "count"})(source, label="renamed")
            unit_of.pod(renamed, label="unit_of")
        job.run()
        job.save(work + "/job.json")
        reused = None
    else:
        job = PipelineJob.load(work + "/job.json")
        job.bind(sources={"readings": source})
        job.run(load_only=True)
        reused = {label: node.reused for label, node in job.last_run.nodes.items()}
    hashes = {
        label: [node.pipeline_hash().to_string(), node.content_hash().to_string()]
        for label, node in sorted(job.nodes.items())
    }
    print(json.dumps({"hashes": hashes, "reused": reused}))
    """
)


def test_a_saved_job_loads_in_a_fresh_process_with_the_saved_hashes(
    tmp_path: Path,
) -> None:
    """Every node of a job over a user class loads with its saved hashes and results.

    The class is registered before the load here (the source is built first); a job
    loaded before anything registers its class is ITL-1138.
    """
    _ = (tmp_path / "readings.py").write_text(_CLASS_MODULE)
    _ = (tmp_path / "save_load.py").write_text(_SAVE_LOAD_SCRIPT)

    def run(mode: str) -> dict[str, Any]:
        result = subprocess.run(
            [
                sys.executable,
                str(tmp_path / "save_load.py"),
                str(tmp_path),
                str(tmp_path),
                mode,
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr[-3000:]
        return json.loads(result.stdout.strip().splitlines()[-1])

    saved = run("save")
    loaded = run("load")
    assert loaded["hashes"] == saved["hashes"]
    assert loaded["reused"] == {"unit_of": 2}
