# ITL-1074 plan: inventory of skipped, xfailed and failing tests

## Method (no full local suite)
1. Static grep of every skip / skipif / xfail / importorskip / filterwarnings in `tests/`, `test-objective/`, conftests, `pytest.ini`, `.github/workflows`.
2. CI facts: full job logs of `Run Tests` on main (`gh api --allow-escape-sequences repos/.../actions/jobs/<id>/logs`; `gh run view --log` truncates at 78%), Postgres and objective workflows.
3. Local: `--collect-only`, and the files holding xfails with `--runxfail` (Python 3.12, one process).
4. This PR's CI prints reasons and durations (`-rsxX --durations=20` added to the workflow invocation).

## Actions
- Commit `superpowers/specs/2026-11-13-itl-1074-test-inventory.md`.
- Workflow: add `-rsxX --durations=20` to the `Run Tests` pytest invocation only (not `pytest.ini`, PR #364 edits pytest config).
- Marks: all xfails are already `strict=True`. Marks that cite a wrong or closed issue are re-pointed once the coordinator gives new Linear ids.
- Everything needing a library change, CI service or dependency is listed, not done.

## Notes for a successor
- Facts are in the spec. Open: S3/MinIO skip reason (read from this PR's CI `-rs` output).
