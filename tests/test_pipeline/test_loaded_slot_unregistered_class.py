"""A job loaded before its user class is registered reads like the job that saved it (ITL-1138).

A job over a column of a user class is saved; a fresh process loads it before anything has
registered the class (no source of it built, no function pod naming it defined). The saved type
of such a column cannot be restored to a class at load, so the loaded source slot keeps it as
pending text. The slot keeps its saved pipeline hash and content hash, and restores the type when
a source is bound, by which time the source has registered the class. Every node then has the
saving process's hashes, table schema (with its system-tag column names), rows and stored rows.
A slot whose class is still not registered at bind refuses the source with an error that names
the column and the saved class.

Each process runs in a fresh interpreter: what a class's registration changes is per process.
"""

import json
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

_CLASSES = textwrap.dedent(
    """\
    import dataclasses

    import orcapod
    import pydantic


    @dataclasses.dataclass
    class Exact:
        u: int
        v: str


    @dataclasses.dataclass
    class OtherExact:
        u: int
        v: str


    @orcapod.dataclass
    class Structural:
        u: int
        v: str


    class Model(pydantic.BaseModel):
        u: int
        v: str
    """
)

_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys
    import warnings

    warnings.simplefilter("ignore")
    sys.path.insert(0, sys.argv[1])
    mode, work, class_name = sys.argv[2], sys.argv[3], sys.argv[4]

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import Join, MapData
    from orcapod.sources import DictSource


    def sources(cls):
        s1 = DictSource(
            [{"id": 1, "p": cls(u=1, v="a")}, {"id": 2, "p": cls(u=2, v="b")}],
            tag_columns=["id"],
            data_schema={"id": int, "p": cls},
            source_id="s1",
        )
        s2 = DictSource([{"id": 1, "r": 5}, {"id": 2, "r": 6}], tag_columns=["id"], source_id="s2")
        return s1, s2


    def rows(table):
        # The table's rows as sorted JSON text, so row order does not matter.
        return sorted(json.dumps(row, sort_keys=True, default=str) for row in table.to_pylist())


    def describe(job):
        out = {}
        for label, node in sorted(job.nodes.items()):
            # The schema with every column group: the system-tag names are compared.
            schema = node.as_table(all_info=True).schema
            # A source node stores nothing; a function or operator node has a stored view.
            stored_view = getattr(node, "as_source", None)
            out[label] = {
                "pipeline_hash": node.pipeline_hash().to_string(),
                "content_hash": node.content_hash().to_string(),
                "schema": [
                    f"{field.name}: {field.type} nullable={field.nullable}" for field in schema
                ],
                # Values of the user columns: a load-only run's status and execution
                # columns differ from the saving run's by design.
                "rows": rows(node.as_table()),
                "stored_rows": None if stored_view is None else rows(stored_view().as_table()),
            }
        return out


    if mode == "save":
        import classes

        cls = getattr(classes, class_name)
        s1, s2 = sources(cls)

        @function_pod(output_keys="q")
        def add(p: cls, r: int) -> int:
            return p.u + r

        @function_pod(output_keys="w")
        def read_u(pp: cls) -> int:
            return pp.u

        job = PipelineJob(name="demo", database=DeltaTableDatabase(work + "/store"))
        with job:
            joined = Join()(s1, s2, label="joined")
            add.pod(joined, label="add")
            renamed = MapData({"p": "pp"})(s1, label="renamed")
            read_u.pod(renamed, label="read_u")
        job.run()
        job.save(work + "/job.json")
        print(json.dumps(describe(job)))
    else:
        if mode == "load_old_file":
            # A file as an earlier Orcapod wrote it: the format before orcapod_version,
            # and no saved hashes on the source slots.
            with open(work + "/job.json") as f:
                data = json.load(f)
            data["orcapod_pipeline_job_version"] = "0.1.0"
            data.pop("orcapod_version", None)
            for descriptor in data["pipeline"]["nodes"].values():
                if descriptor["node_type"] == "source":
                    descriptor.pop("pipeline_hash", None)
                    descriptor.pop("content_hash", None)
            with open(work + "/job.json", "w") as f:
                json.dump(data, f)
        # Nothing registers the class before the load.
        job = PipelineJob.load(work + "/job.json")
        # The source slots' hashes while unbound, before the class is imported.
        from orcapod.core.nodes.source_node import SourceJobNode

        unbound_slots = {
            label: [node.pipeline_hash().to_string(), node.content_hash().to_string()]
            for label, node in job.nodes.items()
            if isinstance(node, SourceJobNode)
        }
        import classes

        bound_class = sys.argv[5] if len(sys.argv) > 5 else class_name
        s1, s2 = sources(getattr(classes, bound_class))  # registers the class now
        try:
            job.bind(sources={"s1": s1, "s2": s2})
        except Exception as exc:
            print(json.dumps({"bind_error": f"{type(exc).__name__}: {exc}"}))
            sys.exit(0)
        job.run(load_only=True)
        out = describe(job)
        out["_reused"] = {label: n.reused for label, n in job.last_run.nodes.items()}
        out["_unbound_slots"] = unbound_slots
        print(json.dumps(out))
    """
)


def _run(tmp_path: Path, *args: str) -> dict[str, Any]:
    """Run the script in a fresh interpreter and return the JSON it prints last."""
    result = subprocess.run(
        [sys.executable, str(tmp_path / "script.py"), str(tmp_path), *args],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.fixture
def saved_job(tmp_path: Path):
    """Return a function that saves the job over a class and returns the saving view."""
    _ = (tmp_path / "classes.py").write_text(_CLASSES)
    _ = (tmp_path / "script.py").write_text(_SCRIPT)

    def save(class_name: str) -> dict[str, Any]:
        return _run(tmp_path, "save", str(tmp_path / "work"), class_name)

    return save


@pytest.mark.parametrize("class_name", ["Exact", "Structural", "Model"])
def test_a_job_loaded_before_its_class_is_registered_reads_like_the_saved_job(
    tmp_path: Path, saved_job, class_name: str
) -> None:
    """Hashes, schemas with system-tag names, rows and stored rows equal the saving process."""
    saved = saved_job(class_name)
    loaded = _run(tmp_path, "load", str(tmp_path / "work"), class_name)
    assert "bind_error" not in loaded, loaded.get("bind_error")
    assert loaded.pop("_reused") == {"add": 2, "read_u": 2}
    # Each source slot has its saved hashes as soon as it is loaded, before any bind:
    # the saving run's pipeline hash, and the content hash the job file records for
    # the slot (a bound slot's content hash is its source's, compared below).
    job_file = json.loads((tmp_path / "work" / "job.json").read_text())
    file_content_hashes = {
        descriptor["label"]: descriptor["content_hash"]
        for descriptor in job_file["pipeline"]["nodes"].values()
        if descriptor["node_type"] == "source"
    }
    assert loaded.pop("_unbound_slots") == {
        label: [saved[label]["pipeline_hash"], file_content_hashes[label]]
        for label in ("s1", "s2")
    }
    assert loaded == saved


def test_a_slot_whose_class_is_never_registered_refuses_the_source_clearly(
    tmp_path: Path, saved_job
) -> None:
    """Binding a source of a look-alike class names the column and the saved class."""
    _ = saved_job("Exact")
    loaded = _run(tmp_path, "load", str(tmp_path / "work"), "Exact", "OtherExact")
    error = loaded["bind_error"]
    assert error.startswith("SourceSpecMismatchError: ")
    assert "'p'" in error
    assert "classes.Exact" in error
    assert "not registered" in error


def test_a_file_without_the_slots_saved_hashes_loads_and_binds(
    tmp_path: Path, saved_job
) -> None:
    """With no saved hashes, the slot restores its type at bind and hashes as saved."""
    saved = saved_job("Exact")
    loaded = _run(tmp_path, "load_old_file", str(tmp_path / "work"), "Exact")
    assert "bind_error" not in loaded, loaded.get("bind_error")
    assert loaded.pop("_reused") == {"add": 2, "read_u": 2}
    _ = loaded.pop("_unbound_slots")  # no saved hashes: recomputed once bound
    assert loaded == saved


# ---------------------------------------------------------------------------
# A loaded slot's pending column: saving the job again, and copies of the slot
# ---------------------------------------------------------------------------

_NULLABLE_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys
    import warnings

    warnings.simplefilter("ignore")
    sys.path.insert(0, sys.argv[1])
    mode, work = sys.argv[2], sys.argv[3]

    from orcapod import PipelineJob, function_pod
    from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource


    def source(cls):
        return DictSource(
            [{"id": 1, "p": cls(u=1, v="a")}, {"id": 2, "p": None}],
            tag_columns=["id"],
            data_schema={"id": int, "p": cls | None},
            source_id="s",
        )


    def slot_view(job):
        node = job.nodes["s"]
        return {
            "data_schema": {k: str(v) for k, v in node.output_schema()[1].items()},
            "pipeline_hash": node.pipeline_hash().to_string(),
        }


    if mode == "save":
        from classes import Exact

        @function_pod(output_keys="q")
        def u_or_zero(p: Exact | None) -> int:
            return 0 if p is None else p.u

        job = PipelineJob(name="demo", database=DeltaTableDatabase(work + "/store"))
        with job:
            u_or_zero.pod(source(Exact), label="u_or_zero")
        job.run()
        job.save(work + "/saved.json")
        print(json.dumps(slot_view(job)))
    elif mode in ("load_bind_save", "load_save"):
        # Nothing registers the class before the load; the job is saved again.
        job = PipelineJob.load(work + "/saved.json")
        if mode == "load_bind_save":
            from classes import Exact

            job.bind(sources={"s": source(Exact)})
        job.save(work + "/resaved.json")
        print(json.dumps({}))
    elif mode == "reload":
        from classes import Exact

        _ = source(Exact)  # registers the class before the load
        job = PipelineJob.load(work + "/" + sys.argv[4])
        print(json.dumps(slot_view(job)))
    elif mode == "legacy_validate":
        # A file without the slots' saved hashes: the slot hashes the type text until
        # the type is restored, here by validate() without a bind.
        with open(work + "/saved.json") as f:
            data = json.load(f)
        for descriptor in data["pipeline"]["nodes"].values():
            if descriptor["node_type"] == "source":
                descriptor.pop("pipeline_hash", None)
                descriptor.pop("content_hash", None)
        with open(work + "/legacy.json", "w") as f:
            json.dump(data, f)
        job = PipelineJob.load(work + "/legacy.json")
        slot = job.nodes["s"]
        _ = slot.pipeline_hash()  # computed, and kept, from the type text
        from classes import Exact

        slot.validate(source(Exact))
        print(json.dumps({"pipeline_hash": slot.pipeline_hash().to_string()}))
    else:  # copies of a loaded slot whose class is not registered
        job = PipelineJob.load(work + "/saved.json")
        slot = job.nodes["s"]
        renamed = SourceJobNode.from_stream(slot, name="renamed")
        same_name = SourceJobNode.from_stream(slot)
        wrapped = SourceNode.from_stream(slot)
        from classes import Exact

        exact = source(Exact)  # registers the class now
        bound = SourceJobNode.from_stream(wrapped)
        bound.bound_source = exact
        print(json.dumps({
            "renamed_keeps_content_hash": renamed.content_hash() == slot.content_hash(),
            "same_name_keeps_content_hash": same_name.content_hash() == slot.content_hash(),
            "renamed_keeps_pipeline_hash": renamed.pipeline_hash() == slot.pipeline_hash(),
            "wrapped_keeps_pipeline_hash": wrapped.pipeline_hash() == slot.pipeline_hash(),
            "wrapped_then_bound_schema": str(bound.output_schema()[1]["p"]),
        }))
    """
)


@pytest.fixture
def nullable_job(tmp_path: Path):
    """Save a job over ``p: Exact | None`` and return how to run the other modes."""
    _ = (tmp_path / "classes.py").write_text(_CLASSES)
    _ = (tmp_path / "nullable.py").write_text(_NULLABLE_SCRIPT)
    work = tmp_path / "work"

    def run(*args: str) -> dict[str, Any]:
        result = subprocess.run(
            [
                sys.executable,
                str(tmp_path / "nullable.py"),
                str(tmp_path),
                args[0],
                str(work),
                *args[1:],
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr[-3000:]
        return json.loads(result.stdout.strip().splitlines()[-1])

    saved = run("save")
    return saved, run


@pytest.mark.parametrize("mode", ["load_bind_save", "load_save"])
def test_saving_a_loaded_job_again_keeps_a_pending_columns_nullability(
    nullable_job, mode: str
) -> None:
    """A job loaded before its class is registered and saved again reloads as saved."""
    saved, run = nullable_job
    _ = run(mode)
    assert run("reload", "resaved.json") == run("reload", "saved.json") == saved


def test_copies_of_a_loaded_slot_keep_its_pending_types_and_name_bound_hashes(
    nullable_job,
) -> None:
    """A copy keeps pending types; only a copy of the same name keeps its content hash."""
    _, run = nullable_job
    copies = run("copies")
    assert copies == {
        "renamed_keeps_content_hash": False,
        "same_name_keeps_content_hash": True,
        "renamed_keeps_pipeline_hash": True,
        "wrapped_keeps_pipeline_hash": True,
        "wrapped_then_bound_schema": "classes.Exact | None",
    }


def test_restoring_a_type_by_validation_rehashes_a_slot_without_saved_hashes(
    nullable_job,
) -> None:
    """A slot that hashed its type text hashes the restored class after validate()."""
    saved, run = nullable_job
    assert run("legacy_validate") == {"pipeline_hash": saved["pipeline_hash"]}
