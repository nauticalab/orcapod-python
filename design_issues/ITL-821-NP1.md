## Newcomer papercuts found by the docs audit

### NP1 — Missing export, misleading errors, inconsistent defaults, broken example, write noise
**Status:** in progress
**Severity:** medium
**Issue:** ITL-821

The docs audit (PR #271) ran every sample against `main` and found behavior that trips a
newcomer. Re-checked on `b197544e` (v0.2.0 + #291):

1. `orcapod.enable_file_hash_caching` was not exported. **Fix:** re-exported from `orcapod`.
2. `DictSource(data=[], tag_columns=[...])` raised `tag_columns not found in table: {...}.
   Available columns: []`. **Fix:** `SourceStreamBuilder.build` raises `Table is empty` for
   a table with no columns; a zero-row source with `data_schema=` still builds.
3. A pod applied to a stream with an extra column printed `Key 'age' not found in parameter
   types.` (a `logger.warning` reaching logging's last-resort stderr handler) before raising
   `Incoming data data type ...`. **Fix:** `schema_utils.schema_incompatibility` returns the
   reason instead of logging; `FunctionPod` and `FunctionNode` raise `Incoming data schema
   ... : column 'age' is not a parameter of the function`.
4. Defaults that disagree: `@function_pod` `version="v0.0"` vs `FunctionPod.from_fn` /
   `side_effect_function_pod` `"v1.0"` (the major version is in the data function's `uri`, so
   it moves identity hashes and the result-store path); `MapData`/`MapTags`
   `drop_unmapped=False` vs `stream.map_data`/`map_tags` `True` (in the operator identity).
   **Open:** awaiting the developer's choice of default.
5. `examples/save_and_load_pipelines.py` passed a database to `Pipeline()` and failed.
   **Fix:** rewritten around `PipelineJob`; `tests/test_examples/test_examples.py` runs every
   example in a subprocess so an example cannot break silently again.
6. Noise: the protobuf gencode `UserWarning`s on import no longer reproduce (every
   `orcapod` module imported with and without all extras, `-W always`; protobuf is never
   loaded), so that half is closed. deltalake's `WARN deltalake_core::writer::stats Skipping
   column ... because it's a binary field` lines on every Delta write still appear.
   **Open:** awaiting the developer's choice of how to silence them.
