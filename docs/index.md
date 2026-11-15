# Orcapod

Orcapod is an intuitive and powerful Python library for building highly reproducible
scientific data pipelines. It provides a structured way to load, transform, and track data
through typed, immutable streams -- ensuring that every step of your analysis is traceable,
content-addressable, and reproducible by design.

## Installation

Orcapod supports Python 3.11, 3.12 and 3.13 and is published on PyPI as `orcapod`, under the
MIT License:

```bash
pip install orcapod
# or, in a uv project
uv add orcapod
```

`0.2.0` is not on PyPI, where the latest release is `0.1.4`; `0.2.0` is the tag `v0.2.0` of
the repository.

Rendering pipeline graphs additionally requires the [Graphviz](https://graphviz.org/download/)
`dot` binary on your `PATH`. It is optional: everything else works without it, and no
Graphviz development headers are needed.

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
result = compute_birth_year.pod(source)
for tag, data in result.iter_data():
    print(f"{tag.as_dict()} -> {data.as_dict()}")
# {'name': 'Alice'} -> {'birth_year': 1996}
# {'name': 'Bob'} -> {'birth_year': 2001}
# {'name': 'Charlie'} -> {'birth_year': 1991}
```

## Next steps

- [Getting Started](getting-started.md) -- a hands-on walkthrough of sources, streams,
  function pods, and operators.
- [Tutorial](tutorial/index.md) -- a self-paced course in eleven short parts, from a first
  stream to a stored, re-runnable pipeline over files, with exercises and solutions.
- [Concepts](concepts/sources.md) -- deeper explanations of Orcapod's core abstractions.
- [Troubleshooting](troubleshooting.md) -- common errors, their causes and fixes.
- [API Reference](api/index.md) -- complete reference for all public classes and functions.
