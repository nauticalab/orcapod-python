# ITL-1026: stamp orcapod.__version__ into saved job and pipeline files

Plan (contained change; no hash, golden, schema or format-version change):

1. Tests first (fail): `orcapod_version == orcapod.__version__` in a saved pipeline file
   and in a saved job file (top level and embedded pipeline); a file with the key deleted
   loads exactly as before; a file with a different/odd `orcapod_version` loads too
   (informational only); format versions stay `0.1.0`.
2. `graph.py` writer and `job.py` writer add `"orcapod_version": orcapod.__version__`
   beside the format version. Loaders never read it.
3. One docs line in docs/concepts/function-pods.md "Saving and loading a job".
4. changelog.d/ITL-1026.md (### Added).
5. Verify `__version__` in editable install (and a built wheel: hatch-vcs writes `_version.py`).
6. Full suite 3.11/3.12/3.13, ruff, basedpyright, docs tests. One push, then CodeRabbit.

Notes for a successor: branch eywalker/itl-1026-stamp-the-orcapod-version-into-saved-job.

## Follow-up (developer decision, 2026-11-12): bump both formats to 0.2.0

- `PIPELINE_FORMAT_VERSION` / `PIPELINE_JOB_FORMAT_VERSION` = "0.2.0"; the `SUPPORTED_*` sets hold
  "0.1.0" and "0.2.0" (one place each, in `pipeline/serialization.py`; loaders test membership).
- Tests: 0.2.0 round trip, committed 0.1.0 fixture (`tests/test_pipeline/fixtures/`, generated
  from origin/main cdb4f160 by `make_job_format_0_1_0.py`), unknown version rejected.
- Changelog fragment, docs line, PR body/title, CodeRabbit reply, merge origin/main.
- Later (do not act): ITL-1043 adds to the same 0.2.0; referential-tags uses 0.3.0.
