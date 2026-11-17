"""ITL-1043 Q2: where do the stored rows go when a loaded REPLAY operator node reads 0?

Non-null tags, in-memory store. Reads the raw operator-cache table at the loaded node's
path and compares its ``__node_content_hash`` values with the loaded node's content hash.

Run: uv run python superpowers/specs/itl-1043/q2_where_rows_go.py
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import PipelineJob
from orcapod.system_constants import constants
from orcapod.types import CacheMode

NAME = "j"


def src(col: str, sid: str) -> ArrowTableSource:
    # Declared non-null: a bare pa.table({...}) makes every field nullable.
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field(col, pa.int64(), nullable=False),
        ]
    )
    return ArrowTableSource(
        pa.table({"id": [0, 1], col: [1, 2]}, schema=schema),
        tag_columns=["id"],
        source_id=sid,
    )


store = InMemoryArrowDatabase()
job = PipelineJob(name=NAME, database=store)
with job:
    Join()(src("a", "a"), src("b", "b"), label="joined")
node = job.nodes["joined"]
node.attach_databases(pipeline_database=store.at(NAME), cache_mode=CacheMode.LOG)
job.run()
with tempfile.TemporaryDirectory() as d:
    job.save(Path(d) / "job.json")
    lnode = PipelineJob.load(str(Path(d) / "job.json"), database=store).nodes["joined"]
lnode.attach_databases(pipeline_database=store.at(NAME), cache_mode=CacheMode.REPLAY)

print("same table path:", lnode._stored_table_path == node._stored_table_path)
raw = store.at(NAME).get_all_records(
    lnode._stored_table_path, record_id_column=node.HASH_COLUMN_NAME
)
assert raw is not None, "the run stored no table at the loaded path"
stored = set(pc.unique(raw.column(constants.NODE_CONTENT_HASH_COL)).to_pylist())
print("stored rows:", raw.num_rows)
print("stored __node_content_hash:", stored)
print("run node content hash:     ", node.content_hash().to_prefixed_digest())
print("loaded node content hash:  ", lnode.content_hash().to_prefixed_digest())
print("loaded inputs:", [(type(s).__name__, s._bound_source) for s in lnode.upstreams])
print("loaded REPLAY rows:", lnode.as_table().num_rows)
