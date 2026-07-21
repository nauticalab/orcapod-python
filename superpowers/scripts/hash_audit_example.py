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
