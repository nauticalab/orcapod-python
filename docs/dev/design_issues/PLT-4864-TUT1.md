## Workshop tutorial (PLT-4864)

### TUT1 — Newcomer-facing defects found while writing the tutorial
**Status:** open
**Severity:** medium
**Issue:** PLT-4864 (related: ITL-821, ITL-817)

Writing `docs/tutorial/` against the library exercised paths the existing docs did not. The
tutorial and `docs/troubleshooting.md` describe each of these as it behaves today; none is
fixed here (this work does not touch `src/`). Re-checked against `main` on November 14, 2026;
items found then to be fixed were removed.

1. **`CSVSource` rejects a `pathlib.Path`.** `CSVSource(Path("f.csv"), tag_columns=["a"])`
   raises `TypeError: 'PosixPath' object is not iterable`; the `file_path` parameter is typed
   `str` and the path is used as the default `source_id`. `CSVSource(str(path), ...)` works.
2. **`list(job.nodes)` order depends on `PYTHONHASHSEED`** for source nodes: a job over two
   sources `units` and `sessions` lists `['units', 'sessions', 'Join', 'fn']` under some
   seeds and `['sessions', 'units', 'Join', 'fn']` under others. Cosmetic, but it makes
   printed listings unreproducible; the docs print `sorted(job.nodes)`.
3. **A `sink_pod` / `tap_pod` / `side_effect_pod` function without the context parameter
   fails at run time, not at decoration time.** Decorating `def show(n: int) -> None` with
   `@sink_pod` succeeds; `job.run()` then raises `TypeError: show() got an unexpected
   keyword argument 'ctx'`.
4. **`PipelineJob` has no `show_graph`.** `job.show_graph()` raises `AttributeError:
   'PipelineJob' has no attribute 'show_graph'. Available node labels: [...]`; the method is
   `job.pipeline.show_graph()`.

The re-run recomputation caused by set-ordered tag columns (ST1) was also found
independently here; `tests/test_docs/test_rerun_computes_nothing.py` pins the fixed
behaviour under five hash seeds.
