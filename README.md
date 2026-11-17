# Orcapod

Orcapod is a Python library for building reproducible scientific data pipelines. Data enters
a pipeline through **sources** and flows through typed, immutable streams, in which every row
is a tag (the keys that identify the row) and its data. **Function pods** compute new values
from each row's data with plain Python functions; **operators** join, filter, group and
reshape streams without computing new values. A **job** runs these steps against a database:
every function result is stored, and a later run computes only what is not stored yet. Every
row records which source and which source row it came from, and every value carries a
provenance token.

## Installation

Orcapod supports Python 3.11, 3.12 and 3.13 and is published on PyPI as `orcapod`:

```bash
pip install orcapod
# or, in a uv project
uv add orcapod
```

0.3.0 is the first release on PyPI since 0.1.4; 0.2.0 was never published to PyPI. Tables
written by 0.2.0 or earlier are not read by 0.3.0: see
[Upgrading from 0.2.0](https://github.com/nauticalab/orcapod-python/blob/main/CHANGELOG.md#upgrading-from-020)
(it also covers 0.1.4).

Optional features are installed as extras:

| Extra | Adds |
|-------|------|
| `orcapod[ray]` | Distributed execution with `RayExecutor` |
| `orcapod[redis]` | `RedisCacher`, a Redis-backed hash cache |
| `orcapod[postgresql]` | The PostgreSQL connector, table source and file hash cache |
| `orcapod[spiraldb]` | The SpiralDB connector and table source |
| `orcapod[torch]` | PyTorch, for `torch.Tensor` values (any installed `torch` works) |
| `orcapod[all]` | All of the above |

Rendering pipeline graphs needs the [Graphviz](https://graphviz.org/download/) `dot` binary on
your `PATH`. It is optional; everything else works without it.

## A quick example

A source of two rows, one function pod, and a job that stores its results in a database:

```python
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys="temp_fahrenheit")
def to_fahrenheit(temperature: float) -> float:
    return temperature * 9.0 / 5.0 + 32.0


rows = [
    {"experiment": "exp_001", "temperature": 20.5},
    {"experiment": "exp_002", "temperature": 22.3},
]
database = InMemoryArrowDatabase()


def run_job(rows):
    source = DictSource(data=rows, tag_columns=["experiment"], source_id="lab_results")
    job = PipelineJob(name="lab", database=database)
    with job:  # record the steps; nothing runs yet
        to_fahrenheit.pod(source, label="fahrenheit")
    print(job.run())
    return job


job = run_job(rows)
# Run of job 'lab': all rows succeeded.
#   fahrenheit: 2 rows: 2 computed, 0 from store, 0 failed
print(job.nodes["fahrenheit"].as_table().sort_by("experiment").to_pylist())
# [{'experiment': 'exp_001', 'temp_fahrenheit': 68.9}, {'experiment': 'exp_002', 'temp_fahrenheit': 72.14}]
```

Add a row and run a new job over the same database. The two stored results are found again
by their input values, and only the new row is computed:

```python
rows.append({"experiment": "exp_003", "temperature": 19.8})
job = run_job(rows)
# Run of job 'lab': all rows succeeded.
#   fahrenheit: 3 rows: 1 computed, 2 from store, 0 failed
```

`InMemoryArrowDatabase` keeps results for the life of the Python process;
`DeltaTableDatabase("some/directory")` writes them to disk, so they are found again in a new
session.

## Documentation

- [Getting Started](https://github.com/nauticalab/orcapod-python/blob/main/docs/getting-started.md):
  sources, streams and function pods, step by step.
- [Tutorial](https://github.com/nauticalab/orcapod-python/blob/main/docs/tutorial/index.md):
  a self-paced course in eleven short parts, from a first stream to a stored, re-runnable
  pipeline over files, with exercises.
- [Concepts](https://github.com/nauticalab/orcapod-python/tree/main/docs/concepts): sources,
  streams, function pods, operators, pipelines and jobs, identity and hashing, file types,
  arrays and your own data types.
- Reference: [Hashing](https://github.com/nauticalab/orcapod-python/blob/main/docs/reference/hashing.md)
  and [Golden hashes](https://github.com/nauticalab/orcapod-python/blob/main/docs/reference/golden-hashes.md).
- [Troubleshooting](https://github.com/nauticalab/orcapod-python/blob/main/docs/troubleshooting.md):
  common errors, their causes and fixes.
- [CHANGELOG](https://github.com/nauticalab/orcapod-python/blob/main/CHANGELOG.md): what
  changed in each release, with upgrade notes.

## Status

Orcapod is before version 1.0. In 0.3.0:

- **Hash values are frozen for data context v1.** Hashes name stored tables and find stored
  results again, so a hash value released in 0.3.0 does not change in a later release of the
  same data context; CI checks this. See
  [Golden hashes](https://github.com/nauticalab/orcapod-python/blob/main/docs/reference/golden-hashes.md).
- **Library value types are provisional.** The hashes of `numpy.ndarray`, `torch.Tensor`, and
  pandas and polars `DataFrame` and `Series` values may change in 0.4, and results computed
  from them may then be computed again. Using one issues `ProvisionalHashWarning` once per
  type per process.
- **Stores from earlier versions are not read as current.** Tables written by 0.1.x or 0.2.0
  are neither read as results nor modified, and results stored by a development build of 0.3
  are not found again: 0.3.0 computes them again. Read
  [Upgrading from 0.2.0](https://github.com/nauticalab/orcapod-python/blob/main/CHANGELOG.md#upgrading-from-020)
  before running a job over an old store, in particular one with side-effect nodes.
- **Connector databases cannot hold function nodes yet.** A job over a SQLite, PostgreSQL or
  SpiralDB database refuses every function node when it is built. Use `InMemoryArrowDatabase`
  or `DeltaTableDatabase` for jobs.

## Related packages

SpikeInterface support lives in a separate package,
[`orcapod-extension-spikeinterface`](https://github.com/nauticalab/orcapod-extension-spikeinterface).
It is not published on PyPI; install it from its repository.

## Development

The project uses [uv](https://docs.astral.sh/uv/):

```bash
uv sync --all-extras --dev
```

Every pull request must pass these checks (pytest on Python 3.11, 3.12 and 3.13):

```bash
uv run --with ruff ruff check .
uv run --with ruff ruff format --check .
uv run --with basedpyright basedpyright
uv run pytest tests/
```

The PostgreSQL integration tests are skipped unless asked for
(`uv run pytest -m postgres --postgres`) and need a running PostgreSQL server, reached
through the standard `PG*` environment variables; see
[the workflow that runs them](https://github.com/nauticalab/orcapod-python/blob/main/.github/workflows/run-postgres-tests.yml).
Releases are cut by the GitHub Actions Release workflow; see
[RELEASING.md](https://github.com/nauticalab/orcapod-python/blob/main/RELEASING.md).

## License

Orcapod is released under the
[MIT License](https://github.com/nauticalab/orcapod-python/blob/main/LICENSE).
