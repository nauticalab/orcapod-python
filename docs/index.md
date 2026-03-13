# Orcapod

Orcapod is an intuitive and powerful Python library for building highly reproducible
scientific data pipelines. It provides a structured way to load, transform, and track data
through typed, immutable streams -- ensuring that every step of your analysis is traceable,
content-addressable, and reproducible by design.

## Installation

Orcapod is not yet published on PyPI. Install from source using [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/walkerlab/orcapod-python.git
cd orcapod-python
uv sync
```

## Quick example

Create a data source, apply a transformation, and inspect the results -- all in a few lines:

```python
from orcapod import function_pod
from orcapod.sources import DictSource

# 1. Load data into a source
source = DictSource(
    data=[
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ],
    tag_columns=["name"],
    source_id="people",
)

# 2. Define a transform with the function_pod decorator
@function_pod(output_keys="birth_year")
def compute_birth_year(age: int) -> int:
    return 2026 - age

# 3. Apply the function pod and inspect the output
result = compute_birth_year.pod.process(source)
for tag, packet in result.iter_packets():
    print(f"{tag.as_dict()} -> {packet.as_dict()}")
# {'name': 'Alice'} -> {'birth_year': 1996}
# {'name': 'Bob'} -> {'birth_year': 2001}
# {'name': 'Charlie'} -> {'birth_year': 1991}
```

## Next steps

- [Getting Started](getting-started.md) -- a hands-on walkthrough of sources, streams,
  function pods, and operators.
- [Concepts](concepts/sources.md) -- deeper explanations of Orcapod's core abstractions.
- [API Reference](api/index.md) -- complete reference for all public classes and functions.
