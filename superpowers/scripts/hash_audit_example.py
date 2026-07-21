#!/usr/bin/env python
"""hash_audit_example.py — concrete hash values for docs/reference/hashing.md.

Constructs a deterministic example pipeline (two DictSources joined on "student",
fed into a FunctionPod + FunctionJobNode + SideEffectPod + PipelineJob) and prints
all 14 hash site values.  Use explicit source_id values so every hash is
reproducible across runs.

Run with:  uv run python superpowers/scripts/hash_audit_example.py
"""
from __future__ import annotations

import hashlib

from orcapod.config import DEFAULT_CONFIG
from orcapod.contexts import resolve_context
from orcapod.core.function_pod import function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.operators.join import Join
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline.job import PipelineJob
from orcapod.side_effects import InvocationContext, SideEffectPod
from orcapod.utils.schema_utils import compute_schema_hash


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def section(title: str) -> None:
    print(f"\n{'=' * 64}")
    print(f"  {title}")
    print("=" * 64)


def sub(title: str) -> None:
    print(f"\n  --- {title} ---")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    ctx = resolve_context()
    semantic_hasher = ctx.semantic_hasher
    schema_n_char = DEFAULT_CONFIG.hashing.schema_n_char

    # ------------------------------------------------------------------
    # Build the example pipeline (deterministic source_id values)
    # ------------------------------------------------------------------

    scores = DictSource(
        [
            {"student": "alice", "math": 90},
            {"student": "bob",   "math": 75},
        ],
        tag_columns=["student"],
        source_id="scores_v1",
    )
    attendance = DictSource(
        [
            {"student": "alice", "days": 180},
            {"student": "bob",   "days": 160},
        ],
        tag_columns=["student"],
        source_id="attendance_v1",
    )

    join_op = Join()
    joined = join_op(scores, attendance)

    @function_pod(output_keys="grade")
    def grade(math: int, days: int) -> float:
        return math * 0.7 + days / 180.0 * 30.0

    pod = grade.pod

    # ==================================================================
    # [1]  FRAMEWORK OBJECT IDENTITY
    # ==================================================================
    section("[1] FRAMEWORK OBJECT IDENTITY")

    sub("Sources (content_hash vs pipeline_hash)")
    print(f"    scores.content_hash()       = {scores.content_hash()}")
    print(f"    scores.pipeline_hash()      = {scores.pipeline_hash()}")
    print(f"    attendance.content_hash()   = {attendance.content_hash()}")
    print(f"    attendance.pipeline_hash()  = {attendance.pipeline_hash()}")

    sub("Joined stream")
    print(f"    joined.content_hash()       = {joined.content_hash()}")
    print(f"    joined.pipeline_hash()      = {joined.pipeline_hash()}")

    sub("FunctionPod")
    print(f"    pod.content_hash()          = {pod.content_hash()}")
    print(f"    pod.pipeline_hash()         = {pod.pipeline_hash()}")
    print(f"    pod.ctx_arg_name            = {pod.ctx_arg_name!r}  (None → content == pipeline)")

    sub("DataFunction URI components")
    data_fn = pod._data_function
    print(f"    data_function.uri           = {data_fn.uri}")

    # ==================================================================
    # [2]  SOURCE PROVENANCE & SYSTEM TAGS
    # ==================================================================
    section("[2] SOURCE PROVENANCE & SYSTEM TAGS")

    sub("Site 3 — Schema hash embedded in system-tag column names")
    tag_schema, data_schema = scores.output_schema()
    schema_hash = compute_schema_hash(tag_schema, data_schema, semantic_hasher, schema_n_char)
    print(f"    (tag_schema, data_schema)   = ({tag_schema!r}, {data_schema!r})")
    print(f"    schema_hash (char_count={schema_n_char!r}) = {schema_hash!r}")

    sub("Site 4 — Default source_id (explicit in this example → no hash computed)")
    print(f"    scores.source_id            = {scores.source_id!r}  (explicit; skips Arrow hash)")

    sub("Sites 3 + 5 — System tag column names and per-row record_id values")
    scores_rows = list(scores.iter_data())
    alice_tag_src, _ = scores_rows[0]
    bob_tag_src,   _ = scores_rows[1]

    alice_sys = alice_tag_src.as_table(columns={"system_tags": True})
    bob_sys   = bob_tag_src.as_table(columns={"system_tags": True})

    print(f"    system tag columns          = {alice_sys.schema.names!r}")

    record_id_col = next(n for n in alice_sys.schema.names if "record_id" in n)
    source_id_col = next(n for n in alice_sys.schema.names if "source_id" in n)

    print(f"    alice {source_id_col!r}  = {alice_sys[source_id_col][0].as_py()!r}")
    print(f"    alice {record_id_col!r}  = {alice_sys[record_id_col][0].as_py().hex()!r}  (UUID v5 bytes)")
    print(f"    bob   {record_id_col!r}  = {bob_sys[record_id_col][0].as_py().hex()!r}  (UUID v5 bytes)")

    sub("Site 6 — Post-join system tag column names (topology suffix appended)")
    joined_rows = list(joined.iter_data())
    alice_tag_j, alice_data_j = joined_rows[0]
    bob_tag_j,   bob_data_j   = joined_rows[1]

    alice_joined_sys = alice_tag_j.as_table(columns={"system_tags": True})
    print(f"    post-join system tag cols   = {alice_joined_sys.schema.names!r}")
    print("    (each original col name → col name + '::{pipeline_hash}:{idx}')")

    # ==================================================================
    # [3]  PIPELINE DB ENTRY KEYS  (sites 7 + 8)
    # ==================================================================
    section("[3] PIPELINE DB ENTRY KEYS (FunctionJobNode)")

    db_for_node = InMemoryArrowDatabase()
    node = FunctionJobNode(
        function_pod=pod,
        input_stream=joined,
        pipeline_database=db_for_node,
    )

    sub("Site 7 — compute_base_entry_id() — recomputation-index-free")
    alice_base = node.compute_base_entry_id(alice_tag_j, alice_data_j)
    bob_base   = node.compute_base_entry_id(bob_tag_j, bob_data_j)
    print(f"    alice base_entry_id (hex)   = {alice_base.hex()!r}")
    print(f"    bob   base_entry_id (hex)   = {bob_base.hex()!r}")

    sub("Site 8 — compute_pipeline_entry_id() — includes recomputation_index=0")
    alice_pipe = node.compute_pipeline_entry_id(alice_tag_j, alice_data_j, recomputation_index=0)
    bob_pipe   = node.compute_pipeline_entry_id(bob_tag_j, bob_data_j,   recomputation_index=0)
    print(f"    alice pipeline_entry_id (hex) = {alice_pipe.hex()!r}")
    print(f"    bob   pipeline_entry_id (hex) = {bob_pipe.hex()!r}")

    sub("FunctionJobNode hashes")
    print(f"    node.content_hash()         = {node.content_hash()}")
    print(f"    node.pipeline_hash()        = {node.pipeline_hash()}")

    # ==================================================================
    # [4]  SIDE-EFFECT RECORD ID & INVOCATION HASH  (sites 9 + 10)
    # ==================================================================
    section("[4] SIDE-EFFECT RECORD ID & INVOCATION HASH")

    captured_ctx: list[InvocationContext] = []

    def _log_fn(data, ctx: InvocationContext) -> None:  # noqa: ANN001
        captured_ctx.append(ctx)

    side_pod = SideEffectPod(_log_fn, name="audit_logger")
    side_stream = side_pod.process(joined)
    for _ in side_stream.iter_data():  # trigger execution to populate captured_ctx
        pass

    for i, ctx in enumerate(captured_ctx):
        row_label = "alice" if i == 0 else "bob"
        sub(f"Site 10 — invocation_hash for {row_label}")
        print(f"    invocation_hash             = {ctx.invocation_hash!r}")
