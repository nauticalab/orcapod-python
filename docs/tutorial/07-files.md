# Part 7: Working with files

Recordings do not live in CSV cells. They are files: binary dumps, spike tables, images,
one folder per session. A pipeline over such data passes *references to files* from step to
step, and the functions open them. Orcapod supports this with a file type that it identifies
by **content**, so that a result is recomputed when the file it was computed from changes,
and not otherwise.

You will:

- build a stream whose data column holds files
- write function pods that read a file and that write one
- see what makes a file-based result recompute
- treat a whole folder as one input

```python
from pathlib import Path

import orcapod as op
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource

data_dir = make_dataset()
print(sorted(p.name for p in (data_dir / "sessions" / "mouse_01_s1").iterdir()))
# ['notes.txt', 'spikes_imec0.csv', 'spikes_imec1.csv']
```

Each session folder holds one spike file per probe (columns `time_s` and `unit`, one row per
spike) and a text file of notes.

## A stream of files

`op.File(path)` marks a value as "this file". Build one row per spike file. There is no
table listing them, so walk the folders and use `DictSource`:

```python
def spike_file_rows():
    rows = []
    for session_dir in sorted((data_dir / "sessions").iterdir()):
        subject_id, session = session_dir.name.rsplit("_", 1)
        for path in sorted(session_dir.glob("spikes_*.csv")):
            rows.append(
                {
                    "subject_id": subject_id,
                    "session": session,
                    "probe": path.stem.removeprefix("spikes_"),
                    "spikes": op.File(path),
                }
            )
    return rows


spike_files = DictSource(
    data=spike_file_rows(),
    tag_columns=["subject_id", "session", "probe"],
    source_id="spike_files",
)
print(spike_files.output_schema())
# (Schema({'subject_id': str, 'session': str, 'probe': str}), Schema({'spikes': File}))
print(spike_files.as_table().num_rows)
# 16
```

The data column has type `File`. Two things about `op.File`:

- It checks that the file exists **when you create it**. `op.File("no_such_file.csv")`
  raises `FileNotFoundError` on the spot, not later in the middle of a run.
- It behaves like a `pathlib.Path`: `.name`, `.stem`, `.parent`, `.read_text()`, `.open()`
  all work, and `str(f)` is the path.

## A function that reads a file

Annotate the parameter as `op.File` and open it however you like:

```python
import csv

calls = []


@function_pod(output_keys="n_spikes")
def count_spikes(spikes: op.File) -> int:
    calls.append(spikes.name)
    with spikes.open(newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


database = InMemoryArrowDatabase()


def run_counts():
    job = PipelineJob(name="files", database=database)
    with job:
        files = DictSource(
            data=spike_file_rows(),
            tag_columns=["subject_id", "session", "probe"],
            source_id="spike_files",
        )
        count_spikes.pod(files, label="counts")
    job.run()
    counts = job.nodes["counts"].as_pandas_df()
    return counts.sort_values(["subject_id", "session", "probe"], ignore_index=True)


counts = run_counts()
print(counts.head(3))
#   subject_id session  probe  n_spikes
# 0   mouse_01      s1  imec0      1736
# 1   mouse_01      s1  imec1      1160
# 2   mouse_01      s2  imec0      3151
print(len(calls))
# 16
```

Sixteen files, sixteen calls. Run again and nothing is computed, as in Part 5:

```python
run_counts()
print(len(calls))
# 16
```

## What makes a file result recompute

Change the content of one file. Here, one spike is appended to the first file:

```python
first = data_dir / "sessions" / "mouse_01_s1" / "spikes_imec0.csv"
first.write_text(first.read_text() + "59.9999,0\n")

counts = run_counts()
print(len(calls))
# 17
print(counts.head(1))
#   subject_id session  probe  n_spikes
# 0   mouse_01      s1  imec0      1737
```

One call, for the file that changed. Nobody told Orcapod which file was edited. A `File` is
identified by a hash of its content, so a changed file is a changed input.

The reverse also holds. The same content under a different name or location is the *same*
input:

```python
import shutil

moved = data_dir / "sessions" / "mouse_01_s1" / "spikes_imec0_copy.csv"
shutil.copy(first, moved)

job = PipelineJob(name="files", database=database)
with job:
    one_file = DictSource(
        data=[
            {
                "subject_id": "mouse_01",
                "session": "s1",
                "probe": "imec0",
                "spikes": op.File(moved),
            }
        ],
        tag_columns=["subject_id", "session", "probe"],
        source_id="spike_files",
    )
    count_spikes.pod(one_file, label="counts")
job.run()
print(len(calls))
# 17
moved.unlink()
```

No call: the stored result for that content was reused. You can reorganise a data directory
without recomputing anything.

!!! warning "`pathlib.Path` columns are identified by the path, not the content"
    You may also put plain `pathlib.Path` values in a source and annotate a parameter as
    `Path`. Orcapod then identifies the value by the path *string*. If the file's content
    changes and its path does not, **nothing is recomputed**. Use `op.File` for every input
    whose content matters, which is nearly always.

## A function that writes a file

A function may produce a file. Write it, and return its path. Annotate the return type as
`Path`:

```python
out_dir = Path("spike_summaries")
out_dir.mkdir(exist_ok=True)


@function_pod(output_keys="summary")
def write_summary(spikes: op.File) -> Path:
    with spikes.open(newline="") as f:
        times = [float(row["time_s"]) for row in csv.DictReader(f)]
    out_path = out_dir / f"{spikes.parent.name}_{spikes.stem}.txt"
    out_path.write_text(f"n_spikes: {len(times)}\nlast_spike_s: {max(times)}\n")
    return out_path


job = PipelineJob(name="files", database=database)
with job:
    files = DictSource(
        data=spike_file_rows(),
        tag_columns=["subject_id", "session", "probe"],
        source_id="spike_files",
    )
    write_summary.pod(files, label="summaries")
job.run()

summaries = job.nodes["summaries"]
print(summaries.output_schema()[1])
# Schema({'summary': Path})
wanted = {"subject_id": "mouse_01", "session": "s1", "probe": "imec0"}
data = next(data for tag, data in summaries.iter_data() if tag.as_dict() == wanted)
print(data.as_dict()["summary"].read_text())
# n_spikes: 1737
# last_spike_s: 59.9999
print(len(list(out_dir.iterdir())))
# 16
```

Three things to keep in mind when a function writes files:

- **Choose the output name from the inputs**, as above, so that two rows never write to the
  same file. Two rows whose files have the same content share one stored result, and so
  one output path, which holds the right summary for both. A changed file's new result,
  though, overwrites the old output under the same name, while the stored result of the old
  content still points there; to keep both, put a digest of the file's content into the
  output name.
- **The database remembers the path, not the file.** If you delete `spike_summaries/` and
  run again, Orcapod finds the stored results, calls nothing, and hands you paths to files
  that no longer exist. Treat output files as part of the database: delete both or neither.
- **Paths are stored as you returned them.** A relative path, as here, only means something
  from the directory the script was run in. For work you intend to keep, build output paths
  from an absolute directory.

## A folder as one input

Some inputs are a folder, not a file: a session directory, an image stack. `op.Directory`
identifies a folder by the names and content of everything in it:

```python
@function_pod(output_keys="n_spike_files")
def count_spike_files(folder: op.Directory) -> int:
    calls.append(folder.name)
    return len(list(folder.glob("spikes_*.csv")))


def run_folders():
    job = PipelineJob(name="folders", database=database)
    with job:
        folders = DictSource(
            data=[
                {
                    "subject_id": d.name.rsplit("_", 1)[0],
                    "session": d.name.rsplit("_", 1)[1],
                    "folder": op.Directory(d, ignore=["notes.txt"]),
                }
                for d in sorted((data_dir / "sessions").iterdir())
            ],
            tag_columns=["subject_id", "session"],
            source_id="session_folders",
        )
        count_spike_files.pod(folders, label="n_files")
    job.run()
    return job


before = len(calls)
run_folders()
print(len(calls) - before)
# 8
```

`ignore` lists file name patterns that do not count towards the folder's identity. Here the
notes are ignored, so editing them does not cause recomputation:

```python
(data_dir / "sessions" / "mouse_01_s1" / "notes.txt").write_text("re-checked on Friday\n")

before = len(calls)
run_folders()
print(len(calls) - before)
# 0
```

Without `ignore`, any change to any file in the folder makes it a new input.

## Large files

Identifying a file by content means reading it. For files of many gigabytes that takes
longer than you want to wait on every run. Orcapod can remember a file's hash and reuse it
as long as the file's size and modification time are unchanged. Turn this on once, at the
start of your script:

```python
op.enable_file_hash_caching()
```

The details and options are on the [File Hash Caching](../concepts/file-hash-caching.md)
page. With the small files of this tutorial it makes no noticeable difference.

## What to remember

- `op.File(path)` in a source, `op.File` as a parameter type. Files are identified by
  content: edit a file and its results are recomputed; move it and they are not.
- Plain `Path` values are identified by path only.
- A function that writes a file returns its `Path`. The database keeps the path; keep the file.
- `op.Directory(path, ignore=[...])` treats a folder as one input.

More detail: [File and path types](../concepts/file-types.md).

## Exercises

**1.** The table `units.csv` claims a spike count per unit; the spike files contain the
spikes. Write a pod that takes a spike file and returns the number of spikes of unit 0 in
it. Check your result for `mouse_02, s1, imec1` against `units.csv`.

??? success "Solution"
    ```python
    @function_pod(output_keys="n_spikes_unit0")
    def count_unit0(spikes: op.File) -> int:
        with spikes.open(newline="") as f:
            return sum(1 for row in csv.DictReader(f) if row["unit"] == "0")


    result = count_unit0.pod(spike_files.polars_filter(subject_id="mouse_02", session="s1"))
    print(result.as_pandas_df())
    #   subject_id session  probe  n_spikes_unit0
    # 0   mouse_02      s1  imec0             321
    # 1   mouse_02      s1  imec1             237

    with (data_dir / "units.csv").open(newline="") as f:
        for row in csv.DictReader(f):
            if (row["subject_id"], row["session"], row["probe"], row["unit"]) == (
                "mouse_02",
                "s1",
                "imec1",
                "0",
            ):
                print(row["n_spikes"])
    # 237
    ```

    This pod was applied directly, without a job, so nothing was stored. That is fine for
    a quick check.

**2.** Write one output file **per session**: a text file `<subject>_<session>_total.txt`
containing the total number of spikes over both probes. (Hint: Part 4. After
`group_by`, a `File` column becomes `list[op.File]`.)

??? success "Solution"
    Group the spike files by session, then hand the list of files to one function call.
    The function has no access to the tags, so it takes the session's name from the folder
    the files are in.

    ```python
    totals_dir = Path("session_totals")
    totals_dir.mkdir(exist_ok=True)


    @function_pod(output_keys="total_file")
    def write_session_total(spikes: list[op.File]) -> Path:
        total = 0
        for spike_file in spikes:
            with spike_file.open(newline="") as f:
                total += sum(1 for _ in csv.DictReader(f))
        out_path = totals_dir / f"{spikes[0].parent.name}_total.txt"
        out_path.write_text(f"{total}\n")
        return out_path


    per_session = spike_files.group_by(["subject_id", "session"])
    print(per_session.output_schema()[1])
    # Schema({'spikes': list[orcapod.logical_types.file_type.File]})

    totals = write_session_total.pod(per_session)
    for tag, data in totals.iter_data():
        path = data.as_dict()["total_file"]
        print(path.name, path.read_text().strip())
    # mouse_01_s1_total.txt 2897
    # mouse_01_s2_total.txt 6522
    # mouse_02_s1_total.txt 4209
    # mouse_02_s2_total.txt 7206
    # mouse_03_s1_total.txt 3533
    # mouse_03_s2_total.txt 4857
    # mouse_04_s1_total.txt 4931
    # mouse_04_s2_total.txt 2462
    ```

**3.** In `run_folders`, remove `ignore=["notes.txt"]` and run it twice, editing a
`notes.txt` in between. How many calls does each run make, and why?

??? success "Solution"
    The first run makes 8 calls: without `ignore`, each folder's identity now includes its
    notes file, so all eight are inputs that have not been seen before. After the edit, the
    second run makes 1 call, for the one folder whose content changed.

    ```python
    def run_folders_strict():
        job = PipelineJob(name="folders", database=database)
        with job:
            folders = DictSource(
                data=[
                    {
                        "subject_id": d.name.rsplit("_", 1)[0],
                        "session": d.name.rsplit("_", 1)[1],
                        "folder": op.Directory(d),
                    }
                    for d in sorted((data_dir / "sessions").iterdir())
                ],
                tag_columns=["subject_id", "session"],
                source_id="session_folders",
            )
            count_spike_files.pod(folders, label="n_files")
        job.run()


    before = len(calls)
    run_folders_strict()
    print(len(calls) - before)
    # 8

    (data_dir / "sessions" / "mouse_03_s2" / "notes.txt").write_text("probe re-inserted\n")
    before = len(calls)
    run_folders_strict()
    print(len(calls) - before)
    # 1
    ```

Next: [Part 8: Inspecting and tracing](08-inspecting-and-tracing.md).
