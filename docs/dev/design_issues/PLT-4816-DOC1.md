## `docs/` (user-facing documentation)

### DOC1 — Documentation samples and behavioural claims had drifted from the code
**Status:** resolved
**Severity:** high
**Issue:** PLT-4816 (related: ITL-807)

Nothing executed the code samples in the user-facing docs, so they drifted as the library
changed. Against `main` at `61d0f18e`, 24 of 45 samples failed, and several prose claims
described behaviour the code does not have: that a function pod picks the columns it needs
out of a wider stream (it rejects extra data columns — ITL-807), that editing a function's
body invalidates its cached results (only its name, output schema and major version do), a
database-backed `FunctionNode(..., pipeline_database=..., result_database=...)` API that no
longer exists, `_tag::` as the system tag column prefix (the columns are `_tag_source_id::` and
`_tag_record_id::`), and `op.enable_file_hash_caching` (the function is in `orcapod.contexts`).
`mkdocs build --strict` also failed, on stale docstring parameters.

**Fix:** Added `tests/test_docs/`, which runs every fenced `python` block in `README.md`,
`docs/index.md`, `docs/getting-started.md`, `docs/concepts/` and `docs/reference/` (except the
two PostgreSQL samples in `docs/concepts/file-hash-caching.md`, marked
`<!-- docs-test: skip -->`) and checks the printed output against the output shown in the
sample, and which builds the site with `mkdocs build --strict`. Fixed every failing sample and the wrong prose to match the code;
library behaviour was not changed. What the docs still do not cover is listed in
`docs/dev/plans/2026-11-09-docs-gap-report.md`.
