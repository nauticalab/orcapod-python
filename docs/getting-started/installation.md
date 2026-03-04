# Installation

## Requirements

- Python 3.11 or later
- [uv](https://docs.astral.sh/uv/) (recommended package manager)

## Install from Source

Clone the repository and install with `uv`:

<!--pytest-codeblocks:skip-->
```bash
git clone https://github.com/walkerlab/orcapod-python.git
cd orcapod-python
uv sync
```

Or install with pip:

<!--pytest-codeblocks:skip-->
```bash
pip install -e .
```

## Optional Dependencies

orcapod has optional dependency groups for extended functionality:

=== "Redis"

    ```bash
    pip install orcapod[redis]
    ```

    Enables Redis-backed caching.

=== "Ray"

    ```bash
    pip install orcapod[ray]
    ```

    Enables distributed execution via Ray.

=== "All"

    ```bash
    pip install orcapod[all]
    ```

    Installs all optional dependencies.

## Development Setup

For contributing to orcapod:

<!--pytest-codeblocks:skip-->
```bash
git clone https://github.com/walkerlab/orcapod-python.git
cd orcapod-python
uv sync --group dev
```

Verify your installation:

<!--pytest-codeblocks:skip-->
```bash
uv run pytest tests/ -x -q
```

## Core Dependencies

orcapod builds on several key libraries:

| Library | Purpose |
|---------|---------|
| [PyArrow](https://arrow.apache.org/docs/python/) | Columnar data representation and Arrow table backing |
| [Polars](https://pola.rs/) | DataFrame filtering (used by `PolarsFilter` operator) |
| [Delta Lake](https://delta.io/) | Persistent database storage via Delta tables |
| [xxhash](https://github.com/Cyan4973/xxHash) | Fast content hashing |
| [NetworkX](https://networkx.org/) | Pipeline graph compilation and topological sorting |
| [Graphviz](https://graphviz.org/) | Pipeline visualization |
