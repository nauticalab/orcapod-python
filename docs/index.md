# orcapod

**Intuitive and powerful library for highly reproducible scientific data pipelines.**

orcapod is a Python framework for building data pipelines with built-in provenance tracking,
content-addressable caching, and deterministic computation. Every value produced by an orcapod
pipeline is traceable back to its original source, every computation is memoizable, and every
result is verifiable.

---

## Key Features

- **Full Provenance Tracking** — Every value carries metadata tracing it back to its originating source and record. Operator topology is captured in system tags, forming a complete lineage chain.

- **Content-Addressable Caching** — Computations are identified by their content hash. Identical computations are never repeated, and results are automatically shared across compatible pipeline runs.

- **Immutable Data Flow** — Streams are immutable sequences of (Tag, Packet) pairs backed by Apache Arrow tables. Data flows forward through the pipeline without side effects.

- **Strict Operator / Function Pod Boundary** — Operators transform structure (joins, filters, renames) without inspecting packet data. Function pods transform data without inspecting tags. This separation keeps provenance clean and reasoning simple.

- **Schema as a First-Class Citizen** — Every stream is self-describing. Schemas are predicted at construction time, not discovered at runtime, enabling early validation and deterministic system tag naming.

- **Incremental Computation** — Database-backed nodes compute only what's missing. Add new data to a source and re-run: only the new rows are processed.

- **Pluggable Execution** — Run pipelines synchronously for debugging or with async push-based channels for production. Swap in a Ray executor for distributed computation. Results are identical regardless of execution strategy.

---

## How It Works

```
Source → Stream → [Operator / FunctionPod] → Stream → ...
```

1. **Sources** load data from external systems (CSV, Delta Lake, DataFrames, dicts) and annotate each row with provenance metadata.

2. **Streams** carry the data as immutable (Tag, Packet) pairs over a shared schema.

3. **Operators** reshape streams — join, filter, batch, select, rename — without creating new values.

4. **Function Pods** apply packet functions that transform individual packets, producing new computed values with tracked provenance.

5. **Pipelines** orchestrate the full graph, automatically persisting results and enabling incremental re-computation.

---

## Quick Example

```python
import pyarrow as pa
from orcapod import ArrowTableSource, FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.operators import Join

# Create sources
patients = ArrowTableSource(
    pa.table({
        "patient_id": ["p1", "p2", "p3"],
        "age": [30, 45, 60],
    }),
    tag_columns=["patient_id"],
)

labs = ArrowTableSource(
    pa.table({
        "patient_id": ["p1", "p2", "p3"],
        "cholesterol": [180, 220, 260],
    }),
    tag_columns=["patient_id"],
)

# Join sources on shared tag columns
joined = Join()(patients, labs)

# Define and apply a packet function
def risk_score(age: int, cholesterol: int) -> float:
    return age * 0.5 + cholesterol * 0.3

risk_fn = PythonPacketFunction(risk_score, output_keys="risk")
risk_pod = FunctionPod(packet_function=risk_fn)
result = risk_pod(joined)

# Iterate over results
for tag, packet in result.iter_packets():
    print(f"{tag.as_dict()} → risk={packet.as_dict()['risk']}")
```

---

## Next Steps

<div class="grid cards" markdown>

-   :material-download: **[Installation](getting-started/installation.md)**

    Install orcapod and its dependencies

-   :material-rocket-launch: **[Quickstart](getting-started/quickstart.md)**

    Learn the basics in 5 minutes

-   :material-book-open-variant: **[Concepts](concepts/architecture.md)**

    Understand the architecture and design principles

-   :material-api: **[API Reference](api/index.md)**

    Browse the full API documentation

</div>
