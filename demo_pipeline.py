"""
Pipeline demo: automatic persistent wrapping of all pipeline nodes.

Demonstrates:
1. Building a pipeline with sources, operators (via convenience methods),
   and function pods
2. Auto-compile on context exit — all nodes become persistent
3. Running the pipeline — data cached in the database
4. Accessing results by label
5. Persistence with DeltaTableDatabase — data survives across runs
6. Re-running a pipeline — only new data is computed
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import PersistentFunctionNode, PersistentOperatorNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.core.nodes import SourceNode
from orcapod.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Helper functions used in the pipeline
# ---------------------------------------------------------------------------


def risk_score(age: int, cholesterol: int) -> float:
    """Simple risk = age * 0.5 + cholesterol * 0.3."""
    return age * 0.5 + cholesterol * 0.3


def categorize(risk: float) -> str:
    if risk < 80:
        return "low"
    elif risk < 120:
        return "medium"
    else:
        return "high"


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

patients_table = pa.table(
    {
        "patient_id": pa.array(["p1", "p2", "p3"], type=pa.large_string()),
        "age": pa.array([30, 45, 60], type=pa.int64()),
    }
)

labs_table = pa.table(
    {
        "patient_id": pa.array(["p1", "p2", "p3"], type=pa.large_string()),
        "cholesterol": pa.array([180, 220, 260], type=pa.int64()),
    }
)

patients = ArrowTableSource(patients_table, tag_columns=["patient_id"])
labs = ArrowTableSource(labs_table, tag_columns=["patient_id"])


# ============================================================
# PART 1: Pipeline with InMemoryArrowDatabase
# ============================================================
print("=" * 70)
print("PART 1: Pipeline with InMemoryArrowDatabase")
print("=" * 70)

db = InMemoryArrowDatabase()

risk_fn = PythonPacketFunction(risk_score, output_keys="risk")
risk_pod = FunctionPod(packet_function=risk_fn)

cat_fn = PythonPacketFunction(categorize, output_keys="category")
cat_pod = FunctionPod(packet_function=cat_fn)

# --- Build the pipeline using convenience methods ---
pipeline = Pipeline(name="risk_pipeline", pipeline_database=db)

with pipeline:
    # .join() is a convenience method on any stream/source
    joined = patients.join(labs, label="join_data")
    risk_stream = risk_pod(joined, label="compute_risk")
    cat_pod(risk_stream, label="categorize")

# --- Inspect compiled nodes ---
print("\n  Compiled nodes:")
for name, node in pipeline.compiled_nodes.items():
    print(f"    {name}: {type(node).__name__}")

print("\n  Source nodes (SourceNode):")
for n in pipeline._node_graph.nodes():
    if isinstance(n, SourceNode):
        print(f"    label: {n.label}, stream: {type(n.stream).__name__}")

# --- Access nodes by label ---
print(f"\n  pipeline.join_data       -> {type(pipeline.join_data).__name__}")
print(f"  pipeline.compute_risk    -> {type(pipeline.compute_risk).__name__}")
print(f"  pipeline.categorize      -> {type(pipeline.categorize).__name__}")

# --- Node types ---
assert isinstance(pipeline.join_data, PersistentOperatorNode)
assert isinstance(pipeline.compute_risk, PersistentFunctionNode)
assert isinstance(pipeline.categorize, PersistentFunctionNode)
print("\n  All node types verified.")

# --- Run the pipeline ---
print("\n  Running pipeline...")
pipeline.run()
print("  Done.")

# --- Inspect results ---
risk_table = pipeline.compute_risk.as_table()
print(f"\n  Risk scores:")
print(f"    {risk_table.to_pandas()[['patient_id', 'risk']].to_string(index=False)}")

cat_table = pipeline.categorize.as_table()
print(f"\n  Categories:")
print(f"    {cat_table.to_pandas()[['patient_id', 'category']].to_string(index=False)}")

# --- Show what's in the database ---
fn_records = pipeline.compute_risk.get_all_records()
print(f"  Function records (compute_risk): {fn_records.num_rows} rows")

cat_records = pipeline.categorize.get_all_records()
print(f"  Function records (categorize):   {cat_records.num_rows} rows")


# ============================================================
# PART 2: Persistence with DeltaTableDatabase
# ============================================================
print("\n" + "=" * 70)
print("PART 2: Pipeline with DeltaTableDatabase (persistent storage)")
print("=" * 70)

with tempfile.TemporaryDirectory() as tmpdir:
    db_path = Path(tmpdir) / "pipeline_db"

    # --- First run: compute everything ---
    print("\n  --- First run ---")
    delta_db = DeltaTableDatabase(base_path=db_path)

    pipe1 = Pipeline(name="persistent_demo", pipeline_database=delta_db)
    with pipe1:
        joined = patients.join(labs, label="joiner")
        risk_pod(joined, label="scorer")

    pipe1.run()

    result = pipe1.scorer.as_table()
    print(f"  Computed {result.num_rows} risk scores:")
    print(f"    {result.to_pandas()[['patient_id', 'risk']].to_string(index=False)}")

    # Show files on disk
    delta_tables = list(db_path.rglob("*.parquet"))
    print(f"\n  Parquet files on disk: {len(delta_tables)}")
    for f in sorted(delta_tables):
        print(f"    {f.relative_to(db_path)}")

    # --- Second run: data already cached ---
    print("\n  --- Second run (same data -> reads from cache) ---")
    delta_db_2 = DeltaTableDatabase(base_path=db_path)

    pipe2 = Pipeline(name="persistent_demo", pipeline_database=delta_db_2)
    with pipe2:
        joined = patients.join(labs, label="joiner")
        risk_pod(joined, label="scorer")

    pipe2.run()

    result2 = pipe2.scorer.as_table()
    print(f"  Retrieved {result2.num_rows} risk scores (from cache):")
    print(f"    {result2.to_pandas()[['patient_id', 'risk']].to_string(index=False)}")

    # --- Third run: add new data -> only new rows computed ---
    print("\n  --- Third run (new patient added -> incremental computation) ---")
    patients_v2 = ArrowTableSource(
        pa.table(
            {
                "patient_id": pa.array(
                    ["p1", "p2", "p3", "p4"], type=pa.large_string()
                ),
                "age": pa.array([30, 45, 60, 25], type=pa.int64()),
            }
        ),
        tag_columns=["patient_id"],
    )
    labs_v2 = ArrowTableSource(
        pa.table(
            {
                "patient_id": pa.array(
                    ["p1", "p2", "p3", "p4"], type=pa.large_string()
                ),
                "cholesterol": pa.array([180, 220, 260, 150], type=pa.int64()),
            }
        ),
        tag_columns=["patient_id"],
    )

    delta_db_3 = DeltaTableDatabase(base_path=db_path)

    pipe3 = Pipeline(name="persistent_demo", pipeline_database=delta_db_3)
    with pipe3:
        joined = patients_v2.join(labs_v2, label="joiner")
        risk_pod(joined, label="scorer")

    pipe3.run()

    result3 = pipe3.scorer.as_table()
    print(f"  Total risk scores after incremental run: {result3.num_rows}")
    print(f"    {result3.to_pandas()[['patient_id', 'risk']].to_string(index=False)}")
    print("  (p4 was computed fresh; p1-p3 were already in the cache)")


# ============================================================
# PART 3: Convenience methods in pipelines
# ============================================================
print("\n" + "=" * 70)
print("PART 3: Convenience methods (.join, .select_packet_columns, .map_packets)")
print("=" * 70)

db3 = InMemoryArrowDatabase()

pipe = Pipeline(name="convenience_demo", pipeline_database=db3)

with pipe:
    # .join() on a source
    joined = patients.join(labs, label="join_data")
    # .select_packet_columns() to keep only "age"
    ages_only = joined.select_packet_columns(["age"], label="select_age")
    # .map_packets() to rename a column
    renamed = ages_only.map_packets({"age": "patient_age"}, label="rename_col")
    # function pod on the renamed stream
    # (categorize expects "risk" but let's just show the chain works)

print("\n  Compiled nodes from chained convenience methods:")
for name, node in pipe.compiled_nodes.items():
    print(f"    {name}: {type(node).__name__}")

pipe.run()

renamed_table = pipe.rename_col.as_table()
print(f"\n  After select + rename:")
print(f"    columns: {renamed_table.column_names}")
print(f"    {renamed_table.to_pandas().to_string(index=False)}")


# ============================================================
# PART 4: Separate function database
# ============================================================
print("\n" + "=" * 70)
print("PART 4: Separate function_database for result isolation")
print("=" * 70)

pipeline_db = InMemoryArrowDatabase()
function_db = InMemoryArrowDatabase()

pipe = Pipeline(
    name="isolated",
    pipeline_database=pipeline_db,
    function_database=function_db,
)

with pipe:
    joined = patients.join(labs, label="joiner")
    risk_pod(joined, label="scorer")

pipe.run()

# Function results are stored in function_db, not pipeline_db
fn_node = pipe.scorer
print(f"\n  pipeline_database and function_database are separate objects:")
print(
    f"    function result DB is function_db: "
    f"{fn_node._packet_function._result_database is function_db}"
)

# Show the record_path prefix includes the pipeline name
record_path = fn_node._packet_function.record_path
print(f"\n  Function result record_path: {record_path}")

# When function_database is None, results go under pipeline_name/_results
pipe_shared = Pipeline(
    name="shared",
    pipeline_database=pipeline_db,
    function_database=None,  # explicit None
)

with pipe_shared:
    joined = patients.join(labs, label="joiner")
    risk_pod(joined, label="scorer")

shared_record_path = pipe_shared.scorer._packet_function.record_path
print(f"  Shared DB record_path:       {shared_record_path}")
print(
    f"  Starts with ('shared', '_results'): "
    f"{shared_record_path[:2] == ('shared', '_results')}"
)


# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print("""
  Pipeline wraps ALL nodes as persistent variants automatically:
    - Leaf streams       -> SourceNode  (graph vertex wrapper, no caching)
    - Operator calls     -> PersistentOperatorNode (DB-backed cache)
    - Function pod calls -> PersistentFunctionNode (DB-backed cache)

  Building a pipeline (using stream convenience methods):
    pipeline = Pipeline(name="my_pipe", pipeline_database=db)
    with pipeline:
        joined = src_a.join(src_b, label="my_join")
        selected = joined.select_packet_columns(["col_a"], label="select")
        pod(selected, label="my_func")
    pipeline.run()  # executes in topological order

  Available convenience methods on any stream/source:
    stream.join(other)               # Join
    stream.semi_join(other)          # SemiJoin
    stream.map_tags({"old": "new"})  # MapTags
    stream.map_packets({"a": "b"})   # MapPackets
    stream.select_tag_columns([..])  # SelectTagColumns
    stream.select_packet_columns(..) # SelectPacketColumns
    stream.drop_tag_columns([..])    # DropTagColumns
    stream.drop_packet_columns([..]) # DropPacketColumns
    stream.batch(batch_size=N)       # Batch
    stream.polars_filter(col="val")  # PolarsFilter

  Accessing results:
    pipeline.my_join          # -> PersistentOperatorNode
    pipeline.my_func          # -> PersistentFunctionNode
    pipeline.my_func.as_table()    # -> PyArrow Table with results

  Persistence:
    - InMemoryArrowDatabase: fast, data lost when process exits
    - DeltaTableDatabase:    data persists to disk as Delta Lake tables
    - Re-running with DeltaTableDatabase reads from cache;
      new rows are computed incrementally

  Function database:
    - function_database=None -> results stored under pipeline_name/_results/
    - function_database=db   -> results stored in separate database
""")
