# ITL-892: process does not exit after reading a Delta table

November 10, 2026. **Outcome:** the coordinator approved option (c) plus the (d) note. It is
implemented in the same PR (`storage_utils.delta_to_pyarrow_dataset`, two new test modules,
`docs/troubleshooting.md`). The investigation itself changed no `src/` file or `uv.lock`. Scripts, raw results
and native stacks are in this directory; the tables are produced by `summarize.py`.

## 1. Summary

- **Reproduced on macOS and Linux, Python 3.11, 3.12 and 3.13, with and without Orcapod.**
  A process that scans a Delta table with deltalake's default pyarrow reader can fail at
  interpreter exit: it hangs forever, or on Linux with Python 3.11/3.12 it may instead abort
  with SIGABRT ("terminate called without an active exception"). The script's work and output
  are complete in every failing run.
- **Rate**: one write plus one scan, then exit: about 57% of runs on macOS arm64, 5–38% on
  Linux x86_64 (GitHub runners), depending on the Python version and the read call. The ITL-864 script (scan,
  four merges with a scan after each) fails 12% on macOS. Through Orcapod, a fresh
  `DeltaTableDatabase.get_all_records` fails 53–77% on macOS and 8–23% on Linux, and
  `DeltaTableSource` 4–10%. A `PipelineJob` run fails 0–4%.
- **Cause (upstream, not Orcapod)**: `DeltaTable.to_pyarrow_dataset()` (and `to_pyarrow_table()`,
  which calls it) reads through `pyarrow.fs.PyFileSystem(DeltaStorageHandler)`, a filesystem
  implemented in Python. Arrow's async scan finishes tearing itself down on a thread-pool worker
  after `to_table()` returns. When that teardown releases the last reference to a Python-backed
  buffer (`PyBuffer`) while the interpreter is finalizing, the worker calls `PyGILState_Ensure`
  and CPython stops the thread. It hangs it on 3.13 and exits it on 3.11/3.12, where glibc's
  forced unwind then aborts the process. Then `ThreadPool::~ThreadPool`, run from
  `__cxa_finalize`, waits forever for a worker that will never finish. This is
  [apache/arrow#50482](https://github.com/apache/arrow/issues/50482) and
  [apache/arrow#54536](https://github.com/apache/arrow/issues/54536), both open. The fix PR
  [apache/arrow#54467](https://github.com/apache/arrow/pull/54467) is open and not merged. No
  matching delta-rs issue exists.
- **No dependency version avoids it**: every pyarrow 20.0.0–26.0.1 × deltalake 1.0.2/1.6.6
  combination fails, at 36–68% on macOS.
- **Recommended mitigation, option (c)**: Orcapod scans local Delta tables through a native
  pyarrow filesystem (`to_pyarrow_dataset(filesystem=SubTreeFileSystem(table_path,
  LocalFileSystem()))`). This takes the failure rate from up to 77% to 0/100 in every Orcapod mode
  tested, on macOS and on Linux 3.11/3.12/3.13. The result is schema- and value-identical, including on partitioned
  tables with escaped partition values. The full suite passes with it (6834 passed, 96 skipped,
  16 xfailed). No API, hash, stored-schema or dependency change. Cloud stores (`s3://` etc.)
  keep deltalake's default reader and stay exposed until Arrow ships a fix. Document that case
  (option d).

## 2. Mechanism and evidence

Native stacks of hung processes (`samples/`, captured with macOS `sample` and with `gdb` on
Linux):

- Main thread, every platform:
  `exit → __cxa_finalize_ranges → std::shared_ptr<arrow::internal::ThreadPool>::~shared_ptr →
  ThreadPool::~ThreadPool → ThreadPool::Shutdown(bool) → condition_variable::wait`.
- Python 3.13, macOS (`samples/dataset_scan_py3.13_pa23.0.1_dl1.0.2.txt`) and Linux
  (`linux/round1-py3.13/samples/linux_op=to_table_py3.13_locked.txt`): one pool worker is in
  `… MergedGenerator::State::~State → parquet FileReaderImpl::~FileReaderImpl →
  SerializedFile::~SerializedFile → ReadRangeCache::~ReadRangeCache → Future<Buffer> result →
  arrow::py::PyBuffer::~PyBuffer → PyGILState_Ensure → _PyThreadState_Attach → take_gil →
  PyThread_hang_thread`. The `ReadRangeCache` exists because deltalake sets
  `ParquetFragmentScanOptions(pre_buffer=True)`. The cached buffers are Python `bytes` returned
  by `DeltaStorageHandler`.
- Python 3.11/3.12: the worker is gone. CPython exits a thread that asks for the GIL during
  finalization, so it never deregisters from the pool. On macOS the process hangs: the
  `samples/orcapod_plain_read_hang_manual.txt` sample has no Arrow worker left and the main
  thread still waits in `Shutdown`. On Linux the thread exit is a forced unwind through a
  `noexcept` destructor, so `std::terminate` aborts the process (rc −6) in some runs, and others
  hang.
- `faulthandler` does not help: the hang is below Python.

Consistent with this:

| condition (macOS, py3.12, locked versions, 100 runs) | failed |
|---|---:|
| `write_deltalake` only, or `merge` only, or `to_pyarrow_dataset()` without scanning | 0% |
| pyarrow parquet write + `pyarrow.dataset` scan with deltalake imported | 0% |
| `to_pyarrow_table()` / `to_pyarrow_dataset().to_table()` | 57% / 57% |
| same scan, native filesystem (`filesystem=SubTreeFileSystem(...)`) | 0% |
| same scan, `QueryBuilder` (DataFusion), `pq.read_table(dt.file_uris())`, `pl.read_delta` | 0% each |
| same scan in a `fork` child (exits with `os._exit`, no finalization) | 0% |
| same scan in a `spawn` child (child finalizes normally) | 90% (the parent then waits forever in `join`) |

## 3. Characterisation

Full tables: §10. Headline cells, 100 runs each.

**Operation.** Only scanning (`to_table`, `to_pyarrow_table`) triggers it. Writing and merging
do not. More scans earlier in the process do not raise the rate (ITL-864 script with 4, 12 or
24 merge rounds: 12%, 0%, 0%). What matters is a scan shortly before exit. Orcapod's
`DeltaTableDatabase` scans in `get_all_records`, in `_read_delta_table` (used by
`get_record_by_id`, `get_records_by_ids`, `get_records_with_column_value`), and in
`_refresh_existing_ids_cache` (writes with `skip_duplicates`). `DeltaTableSource.__init__` also
scans. `observability_reader` uses `pl.read_delta`, which is safe (0%).

**Thread pools and environment** (macOS, one-scan cell, baseline 57%): `pa.set_cpu_count(1)`
33%; `pa.set_io_thread_count(1)` 53%; `OMP_NUM_THREADS=1` 45%;
`RAYON_NUM_THREADS=1 TOKIO_WORKER_THREADS=1` 49%. `to_table(use_threads=False)` gives 0% on
macOS but 8–20% on Linux, so it is not a fix. No thread setting removes it.

**Two Arrow runtimes.** deltalake's Rust arrow alone (writes, merges, `QueryBuilder`) never
triggers it, and pyarrow alone (`pyarrow_only`) never does. Only pyarrow's C++ pool combined
with the Python-implemented filesystem does.

**Polars as a third runtime.** Importing polars after pyarrow gives 48%, importing it first 27%.
Polars changes timing, not the outcome.

**Python version** (macOS): one-scan cell 3.11 57%, 3.12 58%, 3.13 65%; Orcapod plain-read 3.11 77%, 3.12 53%, 3.13 76%. The 3.11/3.13 Orcapod cells ran in an isolated environment with a 30 s deadline and 4 parallel runs; the 3.12 cell ran in the project environment with 6 s and 10. Orcapod `PipelineJob` sync and async: 0% on all three. Linux: see §10. On 3.13 failures are
always hangs. On 3.11/3.12 Linux they are hangs or SIGABRT.

**Load.** Running 1, 4 or 10 children in parallel gives 3%, 5%, 10% on the ITL-864 script. Load
raises the rate a little; it is not the cause.

**Platforms.** macOS arm64: measured here. Linux x86_64: measured on GitHub `ubuntu-latest`
through a temporary workflow on this branch (`.github/workflows/itl-892-exit-hang-probe.yml`,
not a required check, since deleted; GitHub Actions runs 42969989607 and 42972298798). Docker is not installed on
this machine.

## 4. Versions and upstream

`uv run --no-project --with pyarrow==X --with deltalake==Y` (uv.lock untouched), macOS,
py3.12, one-scan cell:

| pyarrow \ deltalake | 1.0.2 (locked) | 1.6.6 (latest) |
|---|---:|---:|
| 20.0.0 (project floor) | 67% | 59% |
| 22.0.0 | 55% | 55% |
| 23.0.1 (locked) | 50–58% | 61% |
| 24.0.0 | 53% | 58% |
| 25.0.1 | 36% | 39% |
| 26.0.1 (latest) | 56% | 68% |

Linux, py3.11/3.12/3.13: pyarrow 22.0.0 + deltalake 1.0.2 fails 6–14%; 26.0.1 + 1.6.6 fails
2–5%. Within what the project allows (`pyarrow>=20`, `deltalake>=1.0.2`) **there is no
combination that does not fail**, so there is no "newest good" version. The oldest failing
combination is the project floor. Upstream reports it on pyarrow 22–26. deltalake 1.6.6 still
defaults to `PyFileSystem(DeltaStorageHandler)` with `pre_buffer=True`.

Upstream:

- [apache/arrow#50482](https://github.com/apache/arrow/issues/50482) (open, July 2026):
  interpreter hangs at exit after `pq.read_table(python_file_object)`, a thread-pool teardown
  deadlock. Reported on pyarrow 22–25, Linux and macOS.
- [apache/arrow#54536](https://github.com/apache/arrow/issues/54536) (open, October 2026): abort
  at exit, `~PyReadableFile` takes the GIL on a pool worker during finalization; Linux,
  pyarrow 25–26, SIGABRT with the same message we see.
- [apache/arrow#54467](https://github.com/apache/arrow/pull/54467) (open PR, not merged): an
  `atexit` callback that releases the GIL and waits for the CPU pool to go idle.
- delta-rs: no matching issue (searched "hang", "hangs", "deadlock", "exit",
  "interpreter shutdown", "PyFileSystem"). Proposed: a delta-rs issue pointing at the Arrow
  issues, suggesting a native object-store filesystem for local paths. Not filed: this run
  does not post to GitHub.

## 5. Options

Failure rates are over 100 runs. "Orcapod" cells use `repro_orcapod.py`, which prototypes each
option by wrapping deltalake (`--patch`), so `src/` is unchanged.

| option | macOS | Linux 3.11 / 3.12 / 3.13 | cost |
|---|---|---|---|
| none (locked) | Orcapod plain-read 53%, source-read 8%, pipeline 0% | two rounds: plain-read 9–12 / 8–19 / 15–23%, source-read 4–8 / 5–10 / 8–10%, pipeline-sync 0 / 0–4 / 0–1% | — |
| (a) pin or bump pyarrow/deltalake | 36–68% for every combination | 2–14% for every tried combination | none possible today. A future pyarrow with #54467 would fix it: bump `uv.lock` then and re-run golden hashes and the suite |
| (b1) `atexit`: `gc.collect()` | Orcapod plain-read 27% | plain-read 21 / 12 / 17%, source-read 5 / 6 / 6%, pipeline-sync 1 / 1 / 1% | trivial, but does not fix it |
| (b2) drop references + `gc.collect()` before exit | 2% (pure) | 0 / 0 / 0% (pure) | needs user code; not a fix on macOS |
| (b3) `atexit`: release the GIL for 50 ms (`time.sleep(0.05)`) | pure 0%; Orcapod 0% in all four modes | pure 0 / 0 / 0% (50 ms and 500 ms); Orcapod 0% in all three modes on each version | +50 ms on every exit of a process that imported Orcapod; timing-based (a busier machine may need longer); a global side effect of importing a library |
| (b4) `os._exit` after flushing | 0% | — | skips `atexit`, buffered I/O and other libraries' cleanup; only acceptable inside our own experiment harnesses, not in the library |
| **(c) native filesystem for local tables** | **0% in all four Orcapod modes; pure 0% on py3.11/3.12/3.13** | **pure 0 / 0 / 0%; Orcapod 0% in all three modes on each version** | ~20 lines in `delta_lake_databases.py` (one helper used by the 4 scan sites) and `delta_table_source.py`. Byte-identical results (`check_native_fs_equivalence.py`, partition edge cases). Full suite passes under the patch. Cloud URIs keep the default path |
| (c') `to_table(use_threads=False)` | 0% | 8–20% (pure) | slower scans; not a fix on Linux |
| (c'') read via `QueryBuilder` / polars / `pq.read_table(file_uris())` | 0% (pure) | — | larger change, and a different reader with its own type mapping (large types, nullability) to verify against the type-strictness rules; no benefit over (c) |
| (d) documentation only | unchanged | unchanged | a troubleshooting entry: "if a script that used a Delta store does not exit, upgrade pyarrow when #54467 ships; meanwhile end the script with `os._exit(0)` after flushing". There is no troubleshooting page on `main` yet |

Effect on the locked dependencies, golden hashes and the suite:

- (a) Nothing to lock. A future bump would change `uv.lock` only.
- (b) and (c) change no dependency and no stored value. Option (c) changes the read path only.
  Hashes are computed from the tables, which are identical. The suite run with the (c) patch
  (`native_fs_pytest_plugin.py`) gave 6834 passed, 96 skipped, 16 xfailed, no failures. The
  golden hash tests are part of that run.

## 6. Recommendation

Ship **(c)** for v0.3, plus a short **(d)** note for cloud stores:

1. In `DeltaTableDatabase`, route the four scan sites (`get_all_records`, `_read_delta_table`,
   `_refresh_existing_ids_cache`, and through them every read method) through one helper that
   passes `filesystem=SubTreeFileSystem(<table path>, LocalFileSystem())` when the store is
   local (`not self._is_cloud`). Do the same in `DeltaTableSource`, which already requires a
   local path.
2. Keep deltalake's default reader for cloud URIs. A native `S3FileSystem` has its own exit
   deadlock ([apache/arrow#50188](https://github.com/apache/arrow/issues/50188)), and the
   storage-option mapping is not one-to-one. Document the remaining exposure.
3. Revisit when pyarrow ships #54467. Bump the floor then, and decide whether the helper can
   go.

Why (c) over (b3). (b3) also measured 0% everywhere, and it is the only option that also
covers cloud stores. But it does not remove the cause. It only gives the pool time to finish
before finalization starts. The 50 ms is a guess that held on these machines; a loaded machine
or a large scan could outlast it, and the failure would come back silently. It would also run
in every process that imported Orcapod.

(c) removes the Python objects from the scan, so there is nothing for a worker to release
late. Its results are byte-identical by check. A possible follow-up, not proposed for v0.3
unless the developer wants cloud covered now: register the (b3) hook lazily, only the first
time a cloud-store scan runs, so local users never pay for it.

## 7. Regression check

`regression_check_proto.py` (prototype, not in `tests/` yet): for each of two modes
(`DeltaTableDatabase` write, flush and fresh-handle read; `DeltaTableSource` read), it starts 40
fresh interpreters, 4 at a time. It fails if any run does not exit within 60 s or exits
non-zero, which covers both the hang and the Linux abort. `ITL892_PATCH` lets the same check
validate a prototype.

| | macOS | Linux 3.11 / 3.12 / 3.13 |
|---|---|---|
| unpatched (today's `main`) | fails both cases (wall time 497 s, bounded by the 60 s deadlines) | fails both cases on each version (162 / 198 / 511 s) |
| native-fs patch | passes, 10 s | passes on each version, 39 / 50 / 50 s |

Detection. Measured per-run failure rates without a fix: macOS 53–77% (plain-read) and 8%
(source-read); Linux 8–23% and 4–10%. With 40 runs per case, both cases miss a regression with
probability below 1e-12 on macOS and below 1% per Linux Python job, which is about 1e-6 across
CI's three Python jobs.

False failures. A clean run takes 1–2 s, so the 60 s deadline leaves a wide margin. Every
patched run passed (one macOS and three Linux runs of the check, plus thousands of patched
harness runs).

Recommendation: add it to `tests/` as an ordinary test that runs in CI, for example
`tests/test_databases/test_delta_exit.py`, with the child script inline and only the public API.
When it passes it costs about 40–50 s per Python job on GitHub runners (10 s on this Mac) and
needs no infrastructure. If that time is unwelcome, halve `RUNS` to 20: a miss is then about 8%
likely per job and 0.05% across the three jobs. It is reliable enough for CI. An opt-in marker
would be justified only by its run time, not by flakiness.

## 8. Not verified

- Linux arm64 and Windows.
- Cloud stores (S3/MinIO): not run. The analysis says they stay exposed with (c), since they
  keep the Python filesystem.
- Jupyter kernels: not affected in the same way, since the kernel does not exit.
- The mechanism on 3.11/3.12 macOS is inferred: the worker had already vanished in every
  sample, so its last frame was not captured. The 3.13 stacks show it directly.

## 8a. Out-of-scope findings

- `delta_lake_databases.py:868`: `return dataset.to_table()(filters=filters)` calls the
  returned `Table`, which raises `TypeError` whenever `_read_delta_table` gets a filter
  operator other than `=` or `in`.

## 9. Files

| file | purpose |
|---|---|
| `harness.py` | runs a command N times with a deadline; counts hangs and exit codes; samples the first hung child's native stack |
| `repro_pure.py` | pyarrow + deltalake only; `--op`, `--read-via`, thread, polars, mp and mitigation switches |
| `repro_orcapod.py` | the same through Orcapod's public API; `--patch` prototypes options (b) and (c) |
| `run_matrix.sh` | the macOS matrix groups |
| `check_native_fs_equivalence.py` | option (c) returns identical schemas and values |
| `native_fs_pytest_plugin.py` | runs the suite with option (c) applied |
| `regression_check_proto.py` | proposed regression check |
| `summarize.py` | renders the tables below |
| `results.jsonl` | macOS raw results (one line per cell) |
| `samples/` | representative macOS stacks: py3.12 (`op=dataset_scan_py3.12_locked.txt`), py3.13 with the worker in `PyGILState_Ensure` (`dataset_scan_py3.13_pa23.0.1_dl1.0.2.txt`), Orcapod with no worker left (`orcapod_plain_read_hang_manual.txt`). The other stacks named in `results.jsonl` are not committed (`*.txt` is gitignored; about 4 MB) |
| `linux/round*/` | Linux raw results from the temporary CI probe, with two py3.13 `gdb` stacks. On 3.11/3.12, `gdb` could not attach because the child aborted first |

## 10. All results

Each cell is one `harness.py` run (`results.jsonl`, `linux/*/results.jsonl`). "hung" means the run missed its deadline. In almost every cell it had already printed `done`; the exception is the `spawn` cell, where the parent waits on a hung child. Early exploratory cells (`baseline`, `load`, `amplify`, `itl864 ...`) used the ITL-864 script, which fails less often than the one-scan cell (`dataset_scan`) used afterwards.

#### macOS arm64 (this machine)

| cell | runs | hung | aborted (rc<0) | other rc!=0 | failed |
|---|---:|---:|---:|---:|---:|
| baseline itl864 py3.12 locked | 40 | 2 | 0 | 0 | **5%** |
| load j=1 itl864 py3.12 locked | 60 | 2 | 0 | 0 | **3%** |
| load j=4 itl864 py3.12 locked | 60 | 3 | 0 | 0 | **5%** |
| amplify rounds=12 itl864 py3.12 locked | 40 | 0 | 0 | 0 | **0%** |
| amplify rounds=24 itl864 py3.12 locked | 40 | 0 | 0 | 0 | **0%** |
| load j=10 itl864 py3.12 locked | 60 | 6 | 0 | 0 | **10%** |
| op=pyarrow_only py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| op=write py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| op=merge py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| op=to_dataset py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| op=to_table py3.12 locked | 100 | 57 | 0 | 0 | **57%** |
| op=dataset_scan py3.12 locked | 100 | 57 | 0 | 0 | **57%** |
| op=itl864 py3.12 locked | 100 | 12 | 0 | 0 | **12%** |
| itl864 read-via=native_fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| itl864 read-via=no_threads py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| itl864 read-via=querybuilder py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| itl864 read-via=files py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| itl864 read-via=polars py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| itl864 cpu_threads=1 | 100 | 6 | 0 | 0 | **6%** |
| dataset_scan read-via=native_fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan read-via=no_threads py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan read-via=querybuilder py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan read-via=files py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan read-via=polars py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan cpu_threads=1 | 100 | 33 | 0 | 0 | **33%** |
| dataset_scan io_threads=1 | 100 | 53 | 0 | 0 | **53%** |
| dataset_scan OMP_NUM_THREADS=1 | 100 | 45 | 0 | 0 | **45%** |
| dataset_scan RAYON/TOKIO threads=1 | 100 | 49 | 0 | 0 | **49%** |
| dataset_scan import polars after pyarrow | 100 | 48 | 0 | 0 | **48%** |
| dataset_scan import polars first | 100 | 27 | 0 | 0 | **27%** |
| dataset_scan in fork child | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan in spawn child | 100 | 90 | 0 | 0 | **90%** |
| dataset_scan mitigation=del | 100 | 2 | 0 | 0 | **2%** |
| dataset_scan mitigation=atexit_del | 100 | 1 | 0 | 0 | **1%** |
| dataset_scan mitigation=os_exit | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan mitigation=atexit_sleep | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan mitigation=atexit_sleep_long | 100 | 0 | 0 | 0 | **0%** |
| orcapod plain-read py3.12 locked | 100 | 53 | 0 | 0 | **53%** |
| orcapod source-read py3.12 locked | 100 | 8 | 0 | 0 | **8%** |
| orcapod pipeline-sync py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-async py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod rerun py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan py3.11 pa23.0.1 dl1.0.2 | 100 | 57 | 0 | 0 | **57%** |
| dataset_scan native_fs py3.11 pa23.0.1 dl1.0.2 | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan py3.12 pa23.0.1 dl1.0.2 | 100 | 50 | 0 | 0 | **50%** |
| dataset_scan native_fs py3.12 pa23.0.1 dl1.0.2 | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan py3.13 pa23.0.1 dl1.0.2 | 100 | 65 | 0 | 0 | **65%** |
| dataset_scan native_fs py3.13 pa23.0.1 dl1.0.2 | 100 | 0 | 0 | 0 | **0%** |
| dataset_scan py3.12 pa20.0.0 dl1.6.6 | 100 | 59 | 0 | 0 | **59%** |
| dataset_scan py3.12 pa22.0.0 dl1.0.2 | 100 | 55 | 0 | 0 | **55%** |
| dataset_scan py3.12 pa22.0.0 dl1.6.6 | 100 | 55 | 0 | 0 | **55%** |
| dataset_scan py3.12 pa23.0.1 dl1.6.6 | 100 | 61 | 0 | 0 | **61%** |
| dataset_scan py3.12 pa24.0.0 dl1.0.2 | 100 | 53 | 0 | 0 | **53%** |
| dataset_scan py3.12 pa24.0.0 dl1.6.6 | 100 | 58 | 0 | 0 | **58%** |
| dataset_scan py3.12 pa25.0.1 dl1.0.2 | 100 | 36 | 0 | 0 | **36%** |
| dataset_scan py3.12 pa25.0.1 dl1.6.6 | 100 | 39 | 0 | 0 | **39%** |
| dataset_scan py3.12 pa26.0.1 dl1.0.2 | 100 | 56 | 0 | 0 | **56%** |
| dataset_scan py3.12 pa26.0.1 dl1.6.6 | 100 | 68 | 0 | 0 | **68%** |
| dataset_scan py3.12 pa20.0.0 dl1.0.2 | 100 | 67 | 0 | 0 | **67%** |
| orcapod plain-read patch=native-fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod source-read patch=native-fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-sync patch=native-fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-async patch=native-fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod plain-read patch=no-threads py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod source-read patch=no-threads py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-sync patch=no-threads py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-async patch=no-threads py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod plain-read patch=atexit-gc py3.12 locked | 100 | 27 | 0 | 0 | **27%** |
| orcapod source-read patch=atexit-gc py3.12 locked | 100 | 5 | 0 | 0 | **5%** |
| orcapod pipeline-sync patch=atexit-gc py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-async patch=atexit-gc py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod plain-read patch=atexit-sleep py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod source-read patch=atexit-sleep py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-sync patch=atexit-sleep py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-async patch=atexit-sleep py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod plain-read py3.11 locked | 100 | 77 | 0 | 0 | **77%** |
| orcapod pipeline-sync py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-async py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod plain-read py3.13 locked | 100 | 76 | 0 | 0 | **76%** |
| orcapod pipeline-sync py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| orcapod pipeline-async py3.13 locked | 100 | 0 | 0 | 0 | **0%** |

#### Linux x86_64, GitHub ubuntu-latest (round1-py3.11)

| cell | runs | hung | aborted (rc<0) | other rc!=0 | failed |
|---|---:|---:|---:|---:|---:|
| linux op=dataset_scan py3.11 locked | 100 | 8 | 13 | 0 | **21%** |
| linux op=to_table py3.11 locked | 100 | 4 | 19 | 0 | **23%** |
| linux op=itl864 py3.11 locked | 100 | 8 | 0 | 0 | **8%** |
| linux dataset_scan native_fs py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux dataset_scan no_threads py3.11 locked | 100 | 11 | 0 | 0 | **11%** |
| linux dataset_scan io_threads=1 py3.11 locked | 100 | 18 | 0 | 0 | **18%** |
| linux dataset_scan mitigation=del py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read py3.11 locked | 100 | 12 | 0 | 0 | **12%** |
| linux orcapod source-read py3.11 locked | 100 | 4 | 0 | 0 | **4%** |
| linux orcapod pipeline-sync py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod pipeline-async py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux op=dataset_scan py3.11 pa22.0.0 dl1.0.2 | 100 | 6 | 0 | 0 | **6%** |
| linux op=dataset_scan py3.11 pa26.0.1 dl1.6.6 | 100 | 2 | 2 | 0 | **4%** |

#### Linux x86_64, GitHub ubuntu-latest (round1-py3.12)

| cell | runs | hung | aborted (rc<0) | other rc!=0 | failed |
|---|---:|---:|---:|---:|---:|
| linux op=dataset_scan py3.12 locked | 100 | 10 | 8 | 0 | **18%** |
| linux op=to_table py3.12 locked | 100 | 3 | 17 | 0 | **20%** |
| linux op=itl864 py3.12 locked | 100 | 8 | 0 | 0 | **8%** |
| linux dataset_scan native_fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux dataset_scan no_threads py3.12 locked | 100 | 20 | 1 | 0 | **21%** |
| linux dataset_scan io_threads=1 py3.12 locked | 100 | 18 | 0 | 0 | **18%** |
| linux dataset_scan mitigation=del py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read py3.12 locked | 100 | 8 | 0 | 0 | **8%** |
| linux orcapod source-read py3.12 locked | 100 | 5 | 0 | 0 | **5%** |
| linux orcapod pipeline-sync py3.12 locked | 100 | 4 | 0 | 0 | **4%** |
| linux orcapod pipeline-async py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux op=dataset_scan py3.12 pa22.0.0 dl1.0.2 | 100 | 13 | 0 | 0 | **13%** |
| linux op=dataset_scan py3.12 pa26.0.1 dl1.6.6 | 100 | 2 | 1 | 0 | **3%** |

#### Linux x86_64, GitHub ubuntu-latest (round1-py3.13)

| cell | runs | hung | aborted (rc<0) | other rc!=0 | failed |
|---|---:|---:|---:|---:|---:|
| linux op=dataset_scan py3.13 locked | 100 | 5 | 0 | 0 | **5%** |
| linux op=to_table py3.13 locked | 100 | 38 | 0 | 0 | **38%** |
| linux op=itl864 py3.13 locked | 100 | 10 | 0 | 0 | **10%** |
| linux dataset_scan native_fs py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux dataset_scan no_threads py3.13 locked | 100 | 8 | 0 | 0 | **8%** |
| linux dataset_scan io_threads=1 py3.13 locked | 100 | 32 | 0 | 0 | **32%** |
| linux dataset_scan mitigation=del py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read py3.13 locked | 100 | 15 | 0 | 0 | **15%** |
| linux orcapod source-read py3.13 locked | 100 | 10 | 0 | 0 | **10%** |
| linux orcapod pipeline-sync py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod pipeline-async py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux op=dataset_scan py3.13 pa22.0.0 dl1.0.2 | 100 | 14 | 0 | 0 | **14%** |
| linux op=dataset_scan py3.13 pa26.0.1 dl1.6.6 | 100 | 5 | 0 | 0 | **5%** |

#### Linux x86_64, GitHub ubuntu-latest (round2-py3.11)

| cell | runs | hung | aborted (rc<0) | other rc!=0 | failed |
|---|---:|---:|---:|---:|---:|
| linux dataset_scan mitigation=atexit_sleep py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux dataset_scan mitigation=atexit_sleep_long py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=none py3.11 locked | 100 | 9 | 0 | 0 | **9%** |
| linux orcapod source-read patch=none py3.11 locked | 100 | 8 | 0 | 0 | **8%** |
| linux orcapod pipeline-sync patch=none py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=native-fs py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod source-read patch=native-fs py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod pipeline-sync patch=native-fs py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=atexit-sleep py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod source-read patch=atexit-sleep py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod pipeline-sync patch=atexit-sleep py3.11 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=atexit-gc py3.11 locked | 100 | 8 | 13 | 0 | **21%** |
| linux orcapod source-read patch=atexit-gc py3.11 locked | 100 | 4 | 1 | 0 | **5%** |
| linux orcapod pipeline-sync patch=atexit-gc py3.11 locked | 100 | 1 | 0 | 0 | **1%** |

#### Linux x86_64, GitHub ubuntu-latest (round2-py3.12)

| cell | runs | hung | aborted (rc<0) | other rc!=0 | failed |
|---|---:|---:|---:|---:|---:|
| linux dataset_scan mitigation=atexit_sleep py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux dataset_scan mitigation=atexit_sleep_long py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=none py3.12 locked | 100 | 8 | 11 | 0 | **19%** |
| linux orcapod source-read patch=none py3.12 locked | 100 | 8 | 2 | 0 | **10%** |
| linux orcapod pipeline-sync patch=none py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=native-fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod source-read patch=native-fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod pipeline-sync patch=native-fs py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=atexit-sleep py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod source-read patch=atexit-sleep py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod pipeline-sync patch=atexit-sleep py3.12 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=atexit-gc py3.12 locked | 100 | 11 | 1 | 0 | **12%** |
| linux orcapod source-read patch=atexit-gc py3.12 locked | 100 | 6 | 0 | 0 | **6%** |
| linux orcapod pipeline-sync patch=atexit-gc py3.12 locked | 100 | 1 | 0 | 0 | **1%** |

#### Linux x86_64, GitHub ubuntu-latest (round2-py3.13)

| cell | runs | hung | aborted (rc<0) | other rc!=0 | failed |
|---|---:|---:|---:|---:|---:|
| linux dataset_scan mitigation=atexit_sleep py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux dataset_scan mitigation=atexit_sleep_long py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=none py3.13 locked | 100 | 23 | 0 | 0 | **23%** |
| linux orcapod source-read patch=none py3.13 locked | 100 | 8 | 0 | 0 | **8%** |
| linux orcapod pipeline-sync patch=none py3.13 locked | 100 | 1 | 0 | 0 | **1%** |
| linux orcapod plain-read patch=native-fs py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod source-read patch=native-fs py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod pipeline-sync patch=native-fs py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=atexit-sleep py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod source-read patch=atexit-sleep py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod pipeline-sync patch=atexit-sleep py3.13 locked | 100 | 0 | 0 | 0 | **0%** |
| linux orcapod plain-read patch=atexit-gc py3.13 locked | 100 | 17 | 0 | 0 | **17%** |
| linux orcapod source-read patch=atexit-gc py3.13 locked | 100 | 6 | 0 | 0 | **6%** |
| linux orcapod pipeline-sync patch=atexit-gc py3.13 locked | 100 | 1 | 0 | 0 | **1%** |


## 11. Proposed delta-rs issue (not filed; for the developer)

**Title:** `to_pyarrow_dataset()`'s default `PyFileSystem(DeltaStorageHandler)` makes processes hang or abort at interpreter exit

**Body:**

> A process that scans a Delta table with `DeltaTable.to_pyarrow_dataset().to_table()` or
> `to_pyarrow_table()` and then exits normally often fails during interpreter shutdown. It
> hangs forever (macOS, any Python; Linux, Python 3.13), or on Linux with Python 3.11/3.12 it
> aborts with `terminate called without an active exception`. All of its work and output are
> done by then.
>
> ```python
> import tempfile, deltalake, pyarrow as pa
> p = tempfile.mkdtemp()
> deltalake.write_deltalake(p, pa.table({"k": [1, 2, 3]}))
> deltalake.DeltaTable(p).to_pyarrow_dataset().to_table()
> print("done")  # prints, then the process may never exit
> ```
>
> Rate over 100 fresh processes: about 57% on macOS arm64 and 5–38% on Linux x86_64. It
> happens on every combination we tried: pyarrow 20.0.0–26.0.1 × deltalake 1.0.2 and 1.6.6,
> Python 3.11–3.13. Writes and merges alone never trigger it.
>
> Native stacks (Python 3.13): the main thread is in `exit → __cxa_finalize →
> arrow::internal::ThreadPool::~ThreadPool → Shutdown`. A pool worker is in
> `parquet::arrow::FileReaderImpl::~FileReaderImpl → ReadRangeCache::~ReadRangeCache →
> arrow::py::PyBuffer::~PyBuffer → PyGILState_Ensure → PyThread_hang_thread`. The cached
> buffers come from `DeltaStorageHandler` (Python `bytes`; `pre_buffer=True`), and Arrow
> releases them on a worker after `to_table()` returns. If that happens during finalization,
> the worker cannot take the GIL. The root bug is in Arrow: apache/arrow#50482, #54536, fix PR
> #54467.
>
> Passing a native filesystem avoids it completely (0/100 on macOS and Linux, identical
> results, partitioned tables included). `table_uri` is percent-encoded (a bare path on 1.0.2
> and 1.2.1, a `file://` URI on 1.6.6), so it has to be decoded first:
>
> ```python
> uri = dt.table_uri if "://" in dt.table_uri else "file://" + dt.table_uri
> fs, root = pyarrow.fs.FileSystem.from_uri(uri)
> dt.to_pyarrow_dataset(filesystem=pyarrow.fs.SubTreeFileSystem(root, fs))
> ```
>
> Suggestion: default to a native pyarrow filesystem for local tables (and possibly the native
> object-store filesystems), or document the issue and the workaround in `to_pyarrow_dataset`
> until Arrow's fix is released.
