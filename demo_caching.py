"""
End-to-end demo: all three pod caching strategies at work.

Demonstrates:
1. PersistentSource — always-on cache scoped to content_hash()
   - DeltaTableSource with canonical source_id (defaults to dir name)
   - Named sources: same name + same schema = same identity (data-independent)
   - Unnamed sources: identity determined by table hash (data-dependent)
2. PersistentFunctionNode — pipeline_hash()-scoped cache, cross-source sharing
   - Two pipelines with different source identities but same schema share one cache table
3. PersistentOperatorNode — content_hash()-scoped with CacheMode (OFF/LOG/REPLAY)
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from orcapod.core.function_pod import FunctionPod, PersistentFunctionNode
from orcapod.core.operator_node import PersistentOperatorNode
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource, DeltaTableSource, PersistentSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode

# Shared databases
source_db = InMemoryArrowDatabase()
pipeline_db = InMemoryArrowDatabase()
result_db = InMemoryArrowDatabase()
operator_db = InMemoryArrowDatabase()

# ============================================================
# STEP 1: PersistentSource with DeltaTableSource
# ============================================================
print("=" * 70)
print("STEP 1: PersistentSource (source pod caching)")
print("=" * 70)

with tempfile.TemporaryDirectory() as tmpdir:
    # --- Create Delta tables on disk ---
    patients_path = Path(tmpdir) / "patients"
    labs_path = Path(tmpdir) / "labs"

    patients_arrow = pa.table(
        {
            "patient_id": pa.array(["p1", "p2", "p3"], type=pa.large_string()),
            "age": pa.array([30, 45, 60], type=pa.int64()),
        }
    )
    labs_arrow = pa.table(
        {
            "patient_id": pa.array(["p1", "p2", "p3"], type=pa.large_string()),
            "cholesterol": pa.array([180, 220, 260], type=pa.int64()),
        }
    )
    write_deltalake(str(patients_path), patients_arrow)
    write_deltalake(str(labs_path), labs_arrow)

    # --- DeltaTableSource: source_id defaults to directory name ---
    patients_src = DeltaTableSource(patients_path, tag_columns=["patient_id"])
    labs_src = DeltaTableSource(labs_path, tag_columns=["patient_id"])

    print(f"\n  patients_src.source_id: {patients_src.source_id!r}")
    print(f"  labs_src.source_id:     {labs_src.source_id!r}")
    print("  (defaults to Delta table directory name)")

    patients = PersistentSource(patients_src, cache_database=source_db)
    labs = PersistentSource(labs_src, cache_database=source_db)

    patients.run()
    labs.run()

    print(f"\n  Patients cache_path: {patients.cache_path}")
    print(f"  Labs cache_path:     {labs.cache_path}")
    print(
        f"  Different tables (different source_id): {patients.cache_path != labs.cache_path}"
    )

    patients_records = patients.get_all_records()
    labs_records = labs.get_all_records()
    print(f"\n  Patients cached rows: {patients_records.num_rows}")
    print(f"  Labs cached rows:     {labs_records.num_rows}")

    # --- Named source identity: same name + same schema = same identity ---
    print("\n  --- Named source identity ---")
    # Rebuild from same Delta dir (same name, same schema) → same content_hash
    patients_src_2 = DeltaTableSource(patients_path, tag_columns=["patient_id"])
    patients_2 = PersistentSource(patients_src_2, cache_database=source_db)
    print(
        f"  Same dir, same name → same content_hash: "
        f"{patients.content_hash() == patients_2.content_hash()}"
    )

    # Now update the Delta table with new data — same dir name → same identity
    patients_arrow_v2 = pa.table(
        {
            "patient_id": pa.array(["p1", "p2", "p3", "p4"], type=pa.large_string()),
            "age": pa.array([30, 45, 60, 25], type=pa.int64()),
        }
    )
    write_deltalake(str(patients_path), patients_arrow_v2, mode="overwrite")
    patients_src_updated = DeltaTableSource(patients_path, tag_columns=["patient_id"])
    patients_updated = PersistentSource(patients_src_updated, cache_database=source_db)

    print(
        f"  Updated data, same dir name → same source_id: "
        f"{patients_src.source_id == patients_src_updated.source_id}"
    )
    print(
        f"  Updated data, same dir name → same content_hash: "
        f"{patients.content_hash() == patients_updated.content_hash()}"
    )
    print("  (Named sources: identity = name + schema, not data content)")

    # Cumulative caching: new rows accumulate in the same cache table
    patients_updated.run()
    updated_records = patients_updated.get_all_records()
    print(
        f"  After update + re-run, cached rows: {updated_records.num_rows} "
        f"(3 original + 1 new, deduped)"
    )

    # --- Unnamed source: identity determined by table hash ---
    print("\n  --- Unnamed source identity (no source_id) ---")
    t1 = pa.table(
        {
            "k": pa.array(["a"], type=pa.large_string()),
            "v": pa.array([1], type=pa.int64()),
        }
    )
    t2 = pa.table(
        {
            "k": pa.array(["b"], type=pa.large_string()),
            "v": pa.array([2], type=pa.int64()),
        }
    )
    unnamed_1 = ArrowTableSource(t1, tag_columns=["k"])
    unnamed_2 = ArrowTableSource(t2, tag_columns=["k"])
    print(f"  unnamed_1.source_id: {unnamed_1.source_id!r}")
    print(f"  unnamed_2.source_id: {unnamed_2.source_id!r}")
    print(
        f"  Different data → different source_id (table hash): "
        f"{unnamed_1.source_id != unnamed_2.source_id}"
    )
    print(
        f"  Different data → different content_hash: "
        f"{unnamed_1.content_hash() != unnamed_2.content_hash()}"
    )

    # ============================================================
    # STEP 2: PersistentFunctionNode — cross-source sharing
    # ============================================================
    print("\n" + "=" * 70)
    print("STEP 2: PersistentFunctionNode (function pod caching)")
    print("=" * 70)

    def risk_score(age: int, cholesterol: int) -> float:
        """Simple risk = age * 0.5 + cholesterol * 0.3"""
        return age * 0.5 + cholesterol * 0.3

    pf = PythonPacketFunction(risk_score, output_keys="risk")
    pod = FunctionPod(packet_function=pf)

    # Pipeline 1: original patients + labs
    joined_1 = Join()(patients, labs)
    fn_node_1 = PersistentFunctionNode(
        function_pod=pod,
        input_stream=joined_1,
        pipeline_database=pipeline_db,
        result_database=result_db,
    )
    fn_node_1.run()

    print(
        f"\n  Pipeline 1 source_ids: {patients_src.source_id!r}, {labs_src.source_id!r}"
    )
    print(f"  Pipeline 1 pipeline_path: {fn_node_1.pipeline_path}")
    fn_records_1 = fn_node_1.get_all_records()
    print(f"  Pipeline 1 stored records: {fn_records_1.num_rows}")

    print(f"\n  Pipeline 1 output:")
    print(fn_node_1.as_table().to_pandas().to_string(index=False))

    # Pipeline 2: DIFFERENT sources, SAME schema
    # Create completely independent sources with different names
    patients_path_b = Path(tmpdir) / "clinic_b_patients"
    labs_path_b = Path(tmpdir) / "clinic_b_labs"
    write_deltalake(
        str(patients_path_b),
        pa.table(
            {
                "patient_id": pa.array(["x1", "x2"], type=pa.large_string()),
                "age": pa.array([28, 72], type=pa.int64()),
            }
        ),
    )
    write_deltalake(
        str(labs_path_b),
        pa.table(
            {
                "patient_id": pa.array(["x1", "x2"], type=pa.large_string()),
                "cholesterol": pa.array([160, 290], type=pa.int64()),
            }
        ),
    )
    patients_b = PersistentSource(
        DeltaTableSource(patients_path_b, tag_columns=["patient_id"]),
        cache_database=source_db,
    )
    labs_b = PersistentSource(
        DeltaTableSource(labs_path_b, tag_columns=["patient_id"]),
        cache_database=source_db,
    )

    joined_2 = Join()(patients_b, labs_b)
    fn_node_2 = PersistentFunctionNode(
        function_pod=pod,
        input_stream=joined_2,
        pipeline_database=pipeline_db,
        result_database=result_db,
    )

    print(f"\n  Pipeline 2 source_ids: {patients_b.source_id!r}, {labs_b.source_id!r}")
    print(f"  Pipeline 2 pipeline_path: {fn_node_2.pipeline_path}")
    print(
        f"  Same pipeline_path (cross-source sharing): "
        f"{fn_node_1.pipeline_path == fn_node_2.pipeline_path}"
    )
    print("  (pipeline_hash ignores source identity — only schema + topology matter)")

    fn_node_2.run()
    fn_records_2 = fn_node_2.get_all_records()
    print(f"\n  After pipeline 2 run, shared DB records: {fn_records_2.num_rows}")
    print(f"  (pipeline 1's 3 records + pipeline 2's 2 new records = 5 total)")

    print(f"\n  Pipeline 2 output:")
    print(fn_node_2.as_table().to_pandas().to_string(index=False))

    # ============================================================
    # STEP 3: PersistentOperatorNode — CacheMode
    # ============================================================
    print("\n" + "=" * 70)
    print("STEP 3: PersistentOperatorNode (operator pod caching)")
    print("=" * 70)

    join_op = Join()

    # --- CacheMode.OFF (default): compute, no DB writes ---
    print("\n  --- CacheMode.OFF ---")
    op_node_off = PersistentOperatorNode(
        operator=join_op,
        input_streams=[patients, labs],
        pipeline_database=operator_db,
        cache_mode=CacheMode.OFF,
    )
    op_node_off.run()
    off_records = operator_db.get_all_records(op_node_off.pipeline_path)
    print(f"  Computed rows: {op_node_off.as_table().num_rows}")
    print(
        f"  DB records after OFF: "
        f"{off_records.num_rows if off_records is not None else 'None (no writes)'}"
    )

    # --- CacheMode.LOG: compute AND write to DB ---
    print("\n  --- CacheMode.LOG ---")
    op_node_log = PersistentOperatorNode(
        operator=join_op,
        input_streams=[patients, labs],
        pipeline_database=operator_db,
        cache_mode=CacheMode.LOG,
    )
    op_node_log.run()
    log_records = operator_db.get_all_records(op_node_log.pipeline_path)
    print(f"  Computed rows: {op_node_log.as_table().num_rows}")
    print(
        f"  DB records after LOG: "
        f"{log_records.num_rows if log_records is not None else 'None'}"
    )
    print(f"  Pipeline path: {op_node_log.pipeline_path}")
    print("  (scoped to content_hash — each source combination gets its own table)")

    # Show content_hash scoping: different sources → different paths
    op_node_b = PersistentOperatorNode(
        operator=join_op,
        input_streams=[patients_b, labs_b],
        pipeline_database=operator_db,
        cache_mode=CacheMode.LOG,
    )
    print(f"\n  Operator v1 pipeline_path: {op_node_log.pipeline_path}")
    print(f"  Operator v2 pipeline_path: {op_node_b.pipeline_path}")
    print(
        f"  Different paths (content_hash scoping): "
        f"{op_node_log.pipeline_path != op_node_b.pipeline_path}"
    )

    # --- CacheMode.REPLAY: skip computation, load from DB ---
    print("\n  --- CacheMode.REPLAY ---")
    op_node_replay = PersistentOperatorNode(
        operator=join_op,
        input_streams=[patients, labs],
        pipeline_database=operator_db,
        cache_mode=CacheMode.REPLAY,
    )
    op_node_replay.run()
    print(
        f"  Replayed rows (from cache, no computation): "
        f"{op_node_replay.as_table().num_rows}"
    )

    # --- REPLAY with no prior cache → empty stream ---
    print("\n  --- CacheMode.REPLAY with no prior cache ---")
    op_node_empty = PersistentOperatorNode(
        operator=join_op,
        input_streams=[patients, labs],
        pipeline_database=InMemoryArrowDatabase(),
        cache_mode=CacheMode.REPLAY,
    )
    op_node_empty.run()
    empty_table = op_node_empty.as_table()
    print(f"  Empty cache → empty stream: {empty_table.num_rows} rows")
    print(f"  Schema preserved: {empty_table.column_names}")

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print("""
  Source identity:
    - Named sources (DeltaTable, CSV): source_id = canonical name (dir/file path)
      → identity = (class, schema, name) — data-independent
    - Unnamed sources (ArrowTableSource): source_id = table data hash
      → identity = (class, schema, hash) — data-dependent

  Source pod (PersistentSource):
    - Always-on caching, scoped to content_hash()
    - Named sources: same name + same schema → same cache table
      (data updates accumulate cumulatively, deduped by row hash)
    - Transparent StreamProtocol — downstream is unaware of caching

  Function pod (PersistentFunctionNode):
    - Cache scoped to pipeline_hash() (schema + topology only)
    - Cross-source sharing: different source identities, same schema
      → same pipeline_path → same cache table
    - Rows distinguished by system tags (source_id + record_id)

  Operator pod (PersistentOperatorNode):
    - Cache scoped to content_hash() (includes source identity)
    - Different source combinations → different cache tables
    - CacheMode.OFF: compute only (default)
    - CacheMode.LOG: compute + persist
    - CacheMode.REPLAY: load from cache, skip computation
""")
