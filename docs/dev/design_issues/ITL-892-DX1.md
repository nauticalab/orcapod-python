## `src/orcapod/databases/delta_lake_databases.py`, `src/orcapod/core/sources/delta_table_source.py` — exit after a Delta read

### DX1 — A process that read a Delta table can hang or abort at interpreter exit
**Status:** resolved (local stores); cloud stores remain exposed upstream
**Severity:** high
**Issue:** ITL-892

`DeltaTable.to_pyarrow_dataset()` (and `to_pyarrow_table()`) reads through deltalake's
`PyFileSystem(DeltaStorageHandler)`, a filesystem implemented in Python. After a scan returns,
Arrow finishes tearing down the scan on a thread-pool worker. If that worker drops the last
reference to a Python-backed buffer during interpreter finalization, CPython stops the thread,
and `ThreadPool::~ThreadPool` in `__cxa_finalize` waits for it forever. On Linux with Python
3.11/3.12 the process may abort with SIGABRT instead. This is upstream:
apache/arrow#50482 and #54536, with an open fix PR, #54467. The failure follows a scan, not a
write.

Measured over 100 runs each, locked versions: one write plus one scan fails 57% of runs on
macOS arm64 and 5–38% on Linux x86_64. A fresh `DeltaTableDatabase.get_all_records` fails 53%
on macOS and 8–23% on Linux. `DeltaTableSource` fails 4–10%. A `PipelineJob` run fails 0–4%.
Every pyarrow 20.0.0–26.0.1 × deltalake 1.0.2/1.6.6 combination fails, so no version pin helps.
Scanning local tables through a native pyarrow filesystem (`SubTreeFileSystem(table_path,
LocalFileSystem())`) gives 0/100 in every case. Its results are byte-identical. Cloud stores
keep the Python filesystem and stay exposed until pyarrow ships the fix. Findings, scripts and
raw results: `docs/dev/specs/itl-892-exit-hang/FINDINGS.md`.

**Fix:** `storage_utils.delta_to_pyarrow_dataset` opens local Delta tables on a native pyarrow
filesystem (`SubTreeFileSystem` over `LocalFileSystem`). Every scan in `DeltaTableDatabase` and
`DeltaTableSource` goes through it. Cloud URIs keep deltalake's reader. The results are
schema- and value-identical: `tests/test_databases/test_delta_native_fs_read.py` compares them
with deltalake's reader for logical types (Path, dataclass, numpy, pandas), partitioned tables
and escaped partition values, and for table roots whose names need percent-encoding
(`table_uri` is percent-encoded, so the helper decodes it with `pyarrow.fs.FileSystem.from_uri`;
a root that is neither a `file://` URI nor an absolute POSIX path, such as a Windows drive path,
is untested and keeps deltalake's reader). `tests/test_databases/test_delta_exit.py` runs fresh
interpreters and fails if one does not exit cleanly; it failed before the fix. The remaining
cloud-store exposure is documented in `docs/troubleshooting.md`.

**Considered alternative, not implemented:** an `atexit` hook that releases the GIL briefly
(`time.sleep(0.05)`) before finalization, the idea of apache/arrow#54467. It measured 0/100 on
macOS and Linux and also covers cloud stores. But it is timing-based, so a loaded machine or a
large scan can outlast it. It would also run in every process that imports Orcapod. If cloud
stores need covering before pyarrow ships the fix, register it lazily on the first cloud-store
scan. When pyarrow ships #54467, bump the floor and reconsider whether the helper is still
needed.
