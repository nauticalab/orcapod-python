# orcapod

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

**Intuitive and powerful library for highly reproducible scientific data pipelines.**

orcapod is a Python framework for building data pipelines with built-in provenance tracking, content-addressable caching, and deterministic computation. Every value produced by an orcapod pipeline is traceable back to its original source, every computation is memoizable, and every result is verifiable.

## Key Features

- **Full Provenance Tracking** — Every value carries metadata tracing it back to its originating source and record.
- **Content-Addressable Caching** — Identical computations are never repeated. Results are automatically shared across compatible pipeline runs.
- **Immutable Data Flow** — Streams are immutable (Tag, Packet) sequences backed by Apache Arrow tables.
- **Strict Operator / Function Pod Boundary** — Operators transform structure without inspecting data. Function pods transform data without inspecting tags.
- **Schema as a First-Class Citizen** — Every stream is self-describing with schemas predicted at construction time.
- **Incremental Computation** — Database-backed nodes compute only what's missing.
- **Pluggable Execution** — Synchronous, async channels, or distributed via Ray — results are identical.

## Quick Example

```python
import pyarrow as pa
from orcapod import ArrowTableSource, FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.operators import Join

# Create sources with tag (join key) and packet (data) columns
patients = ArrowTableSource(
    pa.table({"patient_id": ["p1", "p2", "p3"], "age": [30, 45, 60]}),
    tag_columns=["patient_id"],
)
labs = ArrowTableSource(
    pa.table({"patient_id": ["p1", "p2", "p3"], "cholesterol": [180, 220, 260]}),
    tag_columns=["patient_id"],
)

# Join on shared tag columns
joined = Join()(patients, labs)

# Apply a computation to each packet
def risk_score(age: int, cholesterol: int) -> float:
    return age * 0.5 + cholesterol * 0.3

risk_fn = PythonPacketFunction(risk_score, output_keys="risk")
result = FunctionPod(packet_function=risk_fn)(joined)

# Iterate results
for tag, packet in result.iter_packets():
    print(f"{tag.as_dict()} → {packet.as_dict()}")
```

## Installation

```bash
# From source with uv (recommended)
git clone https://github.com/walkerlab/orcapod-python.git
cd orcapod-python
uv sync

# Or with pip
pip install -e .
```

### Optional Dependencies

```bash
pip install orcapod[ray]    # Distributed execution via Ray
pip install orcapod[redis]  # Redis-backed caching
pip install orcapod[all]    # Everything
```

## Documentation

Full documentation is available at the [orcapod docs site](https://walkerlab.github.io/orcapod-python/).

- [Getting Started](https://walkerlab.github.io/orcapod-python/getting-started/installation/) — Installation and quickstart
- [Concepts](https://walkerlab.github.io/orcapod-python/concepts/architecture/) — Architecture and design principles
- [User Guide](https://walkerlab.github.io/orcapod-python/user-guide/sources/) — Detailed guides for each component
- [API Reference](https://walkerlab.github.io/orcapod-python/api/) — Auto-generated API documentation

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Run tests
uv run pytest tests/

# Run tests with coverage
uv run pytest tests/ --cov=src --cov-report=term-missing

# Build documentation locally
uv sync --group docs
uv run mkdocs serve
```

## Architecture at a Glance

```
Source → Stream → [Operator / FunctionPod] → Stream → ...
```

| Abstraction | Role |
|-------------|------|
| **Source** | Load external data, establish provenance |
| **Stream** | Immutable (Tag, Packet) sequence over shared schema |
| **Operator** | Structural transformation (join, filter, select, rename) |
| **Function Pod** | Data transformation (compute new values) |
| **Pipeline** | Orchestrate, persist, and incrementally recompute |

## License

MIT License — see [LICENSE](LICENSE) for details.
