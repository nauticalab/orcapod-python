# Troubleshooting

## A script finishes its work but the process never exits

**Symptom.** A Python script that used a Delta database prints all of its output and then hangs,
and has to be killed. On Linux with Python 3.11 or 3.12 the process may instead end with
`terminate called without an active exception` and exit code 134 (SIGABRT). Notebooks are not
affected in the same way, because the kernel keeps running.

**Cause.** This is a bug in pyarrow, not in your pipeline
([apache/arrow#50482](https://github.com/apache/arrow/issues/50482),
[apache/arrow#54536](https://github.com/apache/arrow/issues/54536)). deltalake's default reader
goes through a filesystem written in Python. After a read returns, Arrow finishes cleaning it up
on a worker thread. If that cleanup runs while the interpreter is shutting down, the worker
cannot take the GIL and never finishes, and Arrow's thread pool waits for it forever. Your data
is not affected: the hang happens after every write has completed.

**What Orcapod does.** `DeltaTableDatabase` and `DeltaTableSource` read **local** Delta tables
through a native pyarrow filesystem, so they are not affected. Tables on cloud storage
(`s3://`, `gs://`, `az://` and similar) still use deltalake's reader and can still hang at exit.
Reading a Delta table yourself with `deltalake.DeltaTable(...).to_pyarrow_table()` or
`to_pyarrow_dataset()` is affected too, whatever the storage.

**Workarounds.**

- Upgrade pyarrow once a release includes the fix
  ([apache/arrow#54467](https://github.com/apache/arrow/pull/54467)).
- In a script that reads a cloud Delta database, end it by flushing your outputs and calling
  `os._exit(0)`. That skips interpreter shutdown, where the hang happens. It also skips
  `atexit` handlers and other cleanup, so close files and flush stores first.
- When reading local Delta tables directly with deltalake, pass a native filesystem. Resolve
  `dt.table_uri` first: it is percent-encoded, and newer deltalake versions return a
  `file://` URI.

  ```python
  uri = dt.table_uri if "://" in dt.table_uri else "file://" + dt.table_uri
  fs, root = pyarrow.fs.FileSystem.from_uri(uri)
  dataset = dt.to_pyarrow_dataset(filesystem=pyarrow.fs.SubTreeFileSystem(root, fs))
  ```
