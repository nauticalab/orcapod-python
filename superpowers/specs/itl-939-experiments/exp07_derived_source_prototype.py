"""ITL-939 experiment 07: prototype of the DerivedSource form (out of tree).

``ProtoDerivedSource`` stands in for the proposed extension of ``DerivedSource``: a source
that derives from another stream and exposes chosen tag columns also as data columns under
given names. It reads the origin's rows, appends the copies, and runs the standard source
enrichment (``SourceStreamBuilder``), so it is a new root with its own provenance. It is
defined here, not in ``src/``; the real class would build its rows lazily.

This prototype records the proposal as it was first sent. Its identity (the origin's content
hash plus the copies) and its default ``source_id`` (``derived:<origin source_id>``) are not
the final rules: the implemented ``DerivedSource`` uses the root-source identity (class name,
output schema, ``source_id``) and a default ``source_id`` of ``tags_as_data:<label>:<hex>``
(design document section 8.10). The captured output is kept as it was.

Shows: keys and schema, types and nullability of the copy, provenance columns, the
elapsed-time pod and the ``Join`` with ``session``, ``Join`` with the original, hashes
compared with the origin and with the hand-written recipe (W2), a ``PipelineJob`` run with
both orchestrators, and save/load over a file-backed DB table source (SQLite stands in for
SpiralDB: both are ``DBTableSource`` subclasses whose primary keys become tags).

Run:

    uv run python superpowers/specs/itl-939-experiments/exp07_derived_source_prototype.py
"""

from __future__ import annotations

import contextlib
import importlib
import io
import sqlite3
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent))
with contextlib.redirect_stdout(io.StringIO()):  # reuse fixtures, not their output
    fx = importlib.import_module("exp02_workarounds_today")

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import Join
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.serialization import (
    register_source,
    resolve_source_from_config,
)
from orcapod.sources import ArrowTableSource, SQLiteTableSource


class ProtoDerivedSource(RootSource):
    """A source derived from another stream, with chosen tags also exposed as data.

    Args:
        origin: The stream to derive from.
        tags_as_data: Maps a tag column of the origin to the name of the data column
            that receives a copy of it.
        source_id: Canonical source name. Defaults to ``derived:<origin source_id>``.
    """

    def __init__(
        self,
        origin: Any,
        tags_as_data: Mapping[str, str],
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        if source_id is None:
            source_id = f"derived:{origin.source_id}"
        super().__init__(
            source_id=source_id, data_context=origin.data_context_key, **kwargs
        )
        self._origin = origin
        self._tags_as_data = dict(tags_as_data)
        tag_keys, data_keys = origin.keys()
        for tag, name in self._tags_as_data.items():
            if tag not in tag_keys:
                raise ValueError(f"{tag!r} is not a tag column of the origin")
            if name in tag_keys or name in data_keys or name.startswith("_"):
                raise ValueError(f"{name!r} is already a column or is reserved")
        table = origin.as_table()
        for tag, name in self._tags_as_data.items():
            field = table.schema.field(tag)
            table = table.append_column(
                pa.field(name, field.type, nullable=field.nullable), table[tag]
            )
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        self._stream = builder.build(
            table, tag_columns=tag_keys, source_id=source_id
        ).stream

    def identity_structure(self) -> Any:
        """Return the origin's content hash plus the copies."""
        return (
            self._origin.content_hash(),
            ("tags_as_data", tuple(sorted(self._tags_as_data.items()))),
        )

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Serialize the origin's config and the copies."""
        return {
            "source_type": "proto_derived",
            "origin": self._origin.to_config(db_registry=db_registry),
            "tags_as_data": self._tags_as_data,
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(
        cls, config: dict[str, Any], db_registry: Any = None
    ) -> ProtoDerivedSource:
        """Rebuild the origin from its config, then the derived source."""
        origin = resolve_source_from_config(config["origin"], db_registry=db_registry)
        return cls(origin, config["tags_as_data"], source_id=config["source_id"])


register_source("proto_derived", ProtoDerivedSource)

COPY = {"session_start_datetime": "session_start"}
COLS = ["session_start", "experiment_start_timestamp"]


def h(x: Any) -> str:
    """Return the first 16 hex characters of a ``ContentHash``."""
    return x.to_hex(16)


def prov(table: pa.Table) -> list[str]:
    """Return shortened system-tag and source-info column names."""
    return [c[:34] for c in table.column_names if c.startswith(("_tag_", "_source_"))]


experiment = fx.src(fx.experiment_table(), fx.KEYS_EXPERIMENT, "experiment")
derived = ProtoDerivedSource(experiment, COPY)
# The recipe (W2) names its source "experiment"; this one compares like with like.
derived_same_id = ProtoDerivedSource(experiment, COPY, source_id="experiment")

print("=== 1. columns")
print("origin  keys():", experiment.keys())
print("derived keys():", derived.keys())
print("derived output_schema():", derived.output_schema())
o_t, d_t = experiment.as_table(all_info=True), derived.as_table(all_info=True)
print("tag field :", o_t.schema.field("session_start_datetime"))
print("copy field:", d_t.schema.field("session_start"))
print("copy equals tag:", d_t["session_start"].equals(d_t["session_start_datetime"]))

print("\n=== 2. provenance: a new source")
print("origin :", prov(o_t))
print("derived:", prov(d_t))
print("source_id:", experiment.source_id, "->", derived.source_id)
print("_source_session_start[0]:", d_t["_source_session_start"][0].as_py())
print(
    "_source_task_id[0] origin/derived:",
    o_t["_source_task_id"][0].as_py(),
    "/",
    d_t["_source_task_id"][0].as_py(),
)
rid_o = next(c for c in o_t.column_names if c.startswith("_tag_record_id"))
rid_d = next(c for c in d_t.column_names if c.startswith("_tag_record_id"))
print("record id values equal the origin's:", o_t[rid_o].equals(d_t[rid_d]))
s_t = derived_same_id.as_table(all_info=True)
print(
    "... with source_id='experiment' passed explicitly:",
    o_t[rid_o].equals(s_t[rid_d]),
)
print("upstreams:", derived.upstreams)

print("\n=== 3. nullable tag -> nullable copy")
t = fx.experiment_table()
nullable = ArrowTableSource(t, tag_columns=fx.KEYS_EXPERIMENT, source_id="experiment")
print(
    "copy field:",
    ProtoDerivedSource(nullable, COPY).as_table().schema.field("session_start"),
)
print(
    "python type:",
    ProtoDerivedSource(nullable, COPY).output_schema()[1]["session_start"],
)

print("\n=== 4. the elapsed-time pod and the Join with session")
out = fx.elapsed_s2.pod(derived.select_data_columns(COLS))
joined = out.join(fx.session)
print("rows:", joined.as_table().num_rows, "| keys():", joined.keys())

print("\n=== 5. Join(derived, original)")
try:
    derived.join(experiment).as_table()
except Exception as exc:  # noqa: BLE001 - the experiment records any failure
    print(f"{type(exc).__name__}: {str(exc)[:160]}")
only_copy = derived.select_data_columns(["session_start"]).join(experiment)
print("Join(derived[copy only], original) rows:", only_copy.as_table().num_rows)

print("\n=== 6. hashes")
print(
    f"origin : content {h(experiment.content_hash())} pipeline {h(experiment.pipeline_hash())}"
)
print(
    f"derived: content {h(derived.content_hash())} pipeline {h(derived.pipeline_hash())}"
)
print(
    f"recipe : content {h(fx.w2_src.content_hash())} pipeline {h(fx.w2_src.pipeline_hash())}"
)
r_t = fx.w2_src.as_table(all_info=True)
print("with source_id='experiment', as the recipe passes by hand:")
print(
    "  system-tag and source-info columns equal the recipe's:", prov(r_t) == prov(s_t)
)
print(
    "  all columns and values equal the recipe's:",
    r_t.select(s_t.column_names).equals(s_t),
)
w2_out = fx.elapsed_s2.pod(fx.w2_src.select_data_columns(COLS))
print(
    "pod output pipeline_hash equal the recipe's:",
    h(w2_out.pipeline_hash()) == h(out.pipeline_hash()),
)
print(
    "origin hashes after deriving:",
    h(experiment.content_hash()),
    h(experiment.pipeline_hash()),
)


@function_pod(output_keys="n")
def name_length(name_copy: str) -> int:
    """Return the length of the copied key."""
    return len(name_copy)


print(
    "\n=== 7. PipelineJob over a file-backed DB table source, both orchestrators, save/load"
)
with tempfile.TemporaryDirectory() as tmp:
    db_path = str(Path(tmp) / "edi.db")
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.execute(
            "CREATE TABLE probe (probe_id TEXT NOT NULL, name TEXT NOT NULL, "
            "channels INTEGER, PRIMARY KEY (probe_id, name))"
        )
        conn.executemany(
            "INSERT INTO probe VALUES (?, ?, ?)",
            [("p1", "alpha", 32), ("p2", "beta", 64), ("p3", "gamma", None)],
        )
        conn.commit()

    def run_job(
        label: str, orch: AsyncPipelineOrchestrator | None
    ) -> tuple[PipelineJob, ProtoDerivedSource]:
        """Run the copy-then-pod job with the given orchestrator and print its rows."""
        job = PipelineJob(name=f"probe_{label}", store=InMemoryArrowDatabase())
        with job:
            probe = SQLiteTableSource(db_path, "probe")
            src = ProtoDerivedSource(probe, {"name": "name_copy"})
            name_length.pod(src.select_data_columns(["name_copy"]), label="len")
        job.run(orchestrator=orch)
        res = job.nodes["len"].as_table()
        print(label, "keys:", job.nodes["len"].keys(), "| n:", res["n"].to_pylist())
        return job, src

    run_job("sync", None)
    job, src = run_job("async", AsyncPipelineOrchestrator())

    path = Path(tmp) / "job.json"
    job.save(path)
    loaded = PipelineJob.load(path, store=InMemoryArrowDatabase())
    print("loaded unbound sources:", loaded.unbound_sources)
    print("loaded sources:", {k: type(v).__name__ for k, v in loaded.sources.items()})
    reloaded = loaded.sources["derived:probe"]
    print("reloaded source keys():", reloaded.keys())
    print("reloaded copy:", reloaded.as_table()["name_copy"].to_pylist())
    print(
        "reloaded content_hash equal:",
        h(reloaded.content_hash()) == h(src.content_hash()),
    )
    print(
        "node pipeline_hash equal after reload:",
        h(loaded.nodes["len"].pipeline_hash()) == h(job.nodes["len"].pipeline_hash()),
    )

    print("\n=== 7b. origin and derived source used in one job")
    job_b = PipelineJob(name="both", store=InMemoryArrowDatabase())
    try:
        with job_b:
            probe = SQLiteTableSource(db_path, "probe")
            src_b = ProtoDerivedSource(probe, {"name": "name_copy"})
            Join()(
                name_length.pod(src_b.select_data_columns(["name_copy"]), label="len"),
                probe,
                label="back",
            )
        job_b.run()
        print(
            "sources:",
            list(job_b.sources),
            "| rows:",
            job_b.nodes["back"].as_table().num_rows,
        )
    except Exception as exc:  # noqa: BLE001 - the experiment records any failure
        print(f"{type(exc).__name__}: {str(exc)[:300]}")
    try:
        with PipelineJob(name="clash", store=InMemoryArrowDatabase()):
            probe = SQLiteTableSource(db_path, "probe")
            same = ProtoDerivedSource(probe, {"name": "name_copy"}, source_id="probe")
            Join()(
                name_length.pod(same.select_data_columns(["name_copy"]), label="len"),
                probe,
                label="back",
            )
    except Exception as exc:  # noqa: BLE001 - the experiment records any failure
        print(f"with the origin's source_id: {type(exc).__name__}: {str(exc)[:120]}")

    print("\n=== 8. the hand-written recipe saved and loaded (for contrast)")
    job_r = PipelineJob(name="recipe", store=InMemoryArrowDatabase())
    with job_r:
        raw = SQLiteTableSource(db_path, "probe").as_table()
        naive = raw.append_column("name_copy", raw["name"])
        print("recipe, copy by name   :", naive.schema.field("name_copy"))
        raw = raw.append_column(
            raw.schema.field("name").with_name("name_copy"), raw["name"]
        )
        print("recipe, copy with field:", raw.schema.field("name_copy"))
        rec = ArrowTableSource(raw, tag_columns=["probe_id", "name"], source_id="probe")
        name_length.pod(rec.select_data_columns(["name_copy"]), label="len")
    job_r.run()
    job_r.save(path)
    loaded_r = PipelineJob.load(path, store=InMemoryArrowDatabase())
    print("recipe loaded unbound sources:", loaded_r.unbound_sources)
