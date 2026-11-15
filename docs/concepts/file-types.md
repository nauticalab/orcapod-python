# File and Path Types

Pipelines over scientific data pass references to files between steps. Orcapod has types
for this, and the choice of type decides **what a result's validity depends on**: the
content of the file, or only its location.

For a guided introduction see the tutorial, [Part 7](../tutorial/07-files.md).

## The types

| Type | What it refers to | Identified by |
|---|---|---|
| `op.File` | One existing file | The file's **content** |
| `op.Directory` | One existing directory and everything in it | The **names and content** of the files in it |
| `pathlib.Path` (also available as `op.Path`) | A location | The **path string** |

"Identified by" is what Orcapod hashes when it decides whether a function's stored result
can be reused (see [Identity & Hashing](identity.md)).

- With `op.File` and `op.Directory`, editing the file makes it a new input and results
  computed from it are recomputed. Moving or renaming the file or the directory itself
  without changing its content does not. Renaming a file inside an `op.Directory` does,
  because the names of its files are part of the directory's identity. The edit is seen when the source is built again, for example by a new job in a new
  Python session. A job that already ran over a `DictSource` holds its run and does not see
  the edit: build a new source and bind it (see
  [Pipelines and Jobs](pipeline-job.md#what-a-re-run-computes)).
- With `Path`, it is the other way round: moving the file makes it a new input, and editing
  it in place goes unnoticed.

Use `op.File` or `op.Directory` for every input whose content your function reads.

`op.UPath` is the path class of [universal-pathlib](https://github.com/fsspec/universal_pathlib),
which `op.File` and `op.Directory` are built on and which also addresses remote storage such
as S3. A local `op.UPath` value in a source is treated as a `Path`.

## `op.File`

```python
import tempfile
from pathlib import Path

import orcapod as op
from orcapod import function_pod
from orcapod.sources import DictSource

workdir = Path(tempfile.mkdtemp())  # a fresh directory, so no file of yours is overwritten
recordings = workdir / "recordings"
recordings.mkdir()
(recordings / "a.txt").write_text("1\n2\n3\n")
(recordings / "b.txt").write_text("4\n5\n")

recording = op.File(recordings / "a.txt")
print(recording.name, recording.suffix, str(recording))
# a.txt .txt .../recordings/a.txt
print(recording.read_text().split())
# ['1', '2', '3']
```

An `op.File`:

- **Validates on creation.** The path must exist and be a regular file. A missing path
  raises `FileNotFoundError`, a directory raises `IsADirectoryError`. Mistakes surface when
  the source is built, not in the middle of a run.
- **Behaves like a path.** It supports the `pathlib` interface: `.name`, `.stem`, `.suffix`,
  `.parent`, `.open()`, `.read_text()`, `.read_bytes()`, and `str(f)` for libraries that
  want a string.
- **Follows symbolic links** by default; `op.File(path, follow_symlinks=False)` rejects
  them.
- Is not a subclass of `pathlib.Path`. Test with `isinstance(x, op.File)`.

```python
try:
    op.File(recordings / "missing.txt")
except FileNotFoundError as err:
    print(type(err).__name__)
# FileNotFoundError
```

Put `op.File` values in a source to get a `File` column, and annotate the function
parameter as `op.File`:

```python
files = DictSource(
    data=[
        {"name": "a", "recording": op.File(recordings / "a.txt")},
        {"name": "b", "recording": op.File(recordings / "b.txt")},
    ],
    tag_columns=["name"],
    source_id="recordings",
)
print(files.output_schema()[1])
# Schema({'recording': File})


@function_pod(output_keys="n_lines")
def count_lines(recording: op.File) -> int:
    return len(recording.read_text().splitlines())


print(count_lines.pod(files).as_table().to_pylist())
# [{'name': 'a', 'n_lines': 3}, {'name': 'b', 'n_lines': 2}]
```

## `op.Directory`

`op.Directory(path)` refers to a directory as a single value. It validates that the path is
an existing directory, and its identity is computed from the files inside it, recursively.

```python
folder = op.Directory(recordings, ignore=["*.log"])
print(sorted(p.name for p in folder.iterdir()))
# ['a.txt', 'b.txt']
```

`ignore` excludes entries from the directory's identity. It takes a list of glob patterns,
matched against each entry's path relative to the directory (`"*.log"` matches a `.log` file
at any depth; excluding a directory excludes everything below it), or a function that
receives the relative path and returns `True` to exclude it. An ignored file can change, or
appear and disappear, without the directory counting as a different input. The patterns only
affect identity: an ignored file is still there when your function lists the directory.

## Functions that write files

A function that produces a file writes it and returns where it is. Annotate the return type
as `Path`:

```python
summaries = workdir / "summaries"
summaries.mkdir()


@function_pod(output_keys="summary")
def summarize(recording: op.File) -> Path:
    out_path = summaries / f"{recording.name}_summary.txt"
    out_path.write_text(f"{len(recording.read_text().splitlines())} lines\n")
    return out_path


result = summarize.pod(files)
print(result.output_schema()[1])
# Schema({'summary': Path})
for tag, data in result.iter_data():
    print(tag.as_dict(), data.as_dict()["summary"].read_text().strip())
# {'name': 'a'} 3 lines
# {'name': 'b'} 2 lines
```

Rows whose `op.File` inputs have the same content share one stored result, so they get the
same output path; since the output was computed from that content, it is the right file
for each of them. If an input file changes, though, its new result overwrites the old output
file under the same name, while the stored result of the old content still points there.
To keep both, put a digest of the input's content into the output name. A digest cannot
give same-content rows separate files; if each row needs its own, pass the value that
should name it (a row id, say) to the function as an input.

When such a function runs inside a [`PipelineJob`](pipeline-job.md):

- **The database holds the path, not the file.** A re-run finds the stored path and does
  not call the function again, whether or not the file is still there. Keep or delete output
  files together with the database that refers to them.
- **Derive the output name from the inputs**, so that rows with different inputs write
  different files. Rows whose inputs have the same content share one result and one output
  path (see above).
- **Prefer absolute output paths** for results you keep. A relative path is stored as
  written and only resolves from the directory the job was run in.

A function may also return an `op.File`. The file must then exist when the function returns,
and a result read back from the database raises `FileNotFoundError` if the file has since been
deleted.

## Lists of files

After [`GroupBy` or `Batch`](operators.md), a `File` column becomes a list of files. Annotate
the consuming function's parameter as `list[op.File]`:

```python
@function_pod(output_keys="total_lines")
def total_lines(recording: list[op.File]) -> int:
    return sum(len(f.read_text().splitlines()) for f in recording)


everything = files.batch()
for _, data in total_lines.pod(everything).iter_data():
    print(data.as_dict())
# {'total_lines': 5}
```

## Large files

Hashing a file means reading it. For large files Orcapod can cache the hash, keyed by the
file's path, size and modification time, so that an unchanged file is not read again:

```python
op.enable_file_hash_caching()
```

See [File Hash Caching](file-hash-caching.md) for how the cache works and its options, and
the `orcapod warm-cache` command for filling it ahead of a run.

## How it connects to other concepts

- File values travel in the data columns of [Streams](streams.md) and enter through
  [Sources](sources.md).
- [Function Pods](function-pods.md) receive and return them like any other typed value.
- Their content hashes feed the [identity chain](identity.md) that decides what a
  [`PipelineJob`](pipeline-job.md) recomputes.
- How each type is stored and named in a database is listed under
  [Logical Type Identifiers](logical-type-identifiers.md).
