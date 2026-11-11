# ITL-810 — refresh PR #269 for merged fixes and the public release route

Coordinator task `task_d429e47e6104`, 2026-11-10. Docs-only change on
`eywalker/itl-810-cut-orcapod-python-v02-by-november-1` (PR #269). Refs ITL-875.

## Starting state (checked before any change)

- Branch head `9fe6725c`, equal to `origin/eywalker/itl-810-cut-orcapod-python-v02-by-november-1`.
- Only untracked file: `superpowers/plans/2026-11-09-pr-269-coderabbit-review.md` (the
  CodeRabbit log, deliberately never committed).
- Merge base with `origin/main` is `ab544982` (#278), and #279 is already in it. `main` is 3
  commits ahead: #281 (ITL-849, `62a182d7`), #282 (ITL-845, `f18339e3`), #280 (ITL-837 with
  ITL-854, ITL-857 and ITL-859, `c438d4d1`).

## Constraints

- No source, `LICENSE`, `pyproject.toml`, `tests/test_packaging.py` or `release.yml` edits.
  The license change is ITL-875's own PR.
- Merge `origin/main` (no rebase, no force-push). The tag is the coordinator's job.
- Every changelog claim is checked against merged code (a snippet I run or a test I cite).

## Steps

1. **Merge `origin/main`.** Main's three new entries land under `[0.1.1]` in this branch,
   because this branch renamed `[Unreleased]` to `[0.1.1]`. Move each into `[0.2.0]` so
   it appears once:
   - ITL-849 → *Fixed* (it adds a capability over already aggregated streams).
   - ITL-845 → *Fixed*, with main's wording kept verbatim (developer's wording on stored
     rows: not repaired, drop pipeline tables, keep result tables, ITL-853).
   - ITL-837 (+ ITL-859) and ITL-854/ITL-857 → *Breaking changes and migration*, because
     each changes behaviour or hashes on a documented path.
   - Add the matching one-line *Changed*/*Fixed* index lines with PR numbers.
2. **Known limitations.** Update:
   - ITL-835: `Join` now applies the disagreeing-types rule too (verify which operators).
   - ITL-841: re-check which operators emit large variants after #280.
   - Add: ITL-863 (migration crashes on a real v0.1.4 store; workaround
     `ignore_schema=("v0",)` and recompute), ITL-853 (stored source-info tokens written before
     ITL-845 stay wrong until pipeline tables are dropped), ITL-867 (meta columns have no
     written rule), ITL-865 and ITL-856 (async path with zero-row inputs). ITL-851 is
     already listed; keep it, sharpen the wording.
   - Remove nothing that is still true.
3. **Public release route (developer decision 2026-11-10).** Rewrite every internal-registry,
   not-on-PyPI and proprietary sentence in `CHANGELOG.md`, `README.md`, `docs/index.md`,
   `RELEASING.md` and the release checklist:
   - Install: `pip install orcapod` / `uv add orcapod`.
   - License: MIT from 0.2.0 (applied by the ITL-875 PR; a cut precondition).
   - Release: manual dispatch of `release.yml`, which runs `validate-branch`, tests on
     3.11–3.13 and the license check, `build` (creates and pushes the tag, builds wheel and
     sdist), `publish-testpypi` then `publish-pypi` through OIDC trusted publishing (`uv
     publish`, environments `testpypi`/`pypi`), creates the GitHub Release, and runs the
     Linear jobs. Describe exactly that; drop the "(superseded)" markers.
   - Open preconditions in the checklist: trusted publisher on PyPI and TestPyPI pointing at
     `metamorphic-labs/orcapod-python` (only the PyPI project owner can do it); the license
     PR merged; v0.2.0 is published only if the route is rehearsed in time, otherwise the tag
     only marks the commit and v0.3.0 is the first public release from this repository.
   - Point out the interaction between a hand-made tag and the workflow's duplicate-tag
     guard (a hand-made `v0.2.0` tag makes a later `release.yml` run for `0.2.0` abort).
4. **Checklist refresh.** Rows 10–12 merged with merge commits; precondition boxes ticked
   with evidence; blockers table; freeze list; Part 3 status; *Findings* 1 updated (MIT).
5. **Verify.** Run snippets for each new changelog claim (Join type error, Join meta drop,
   Datagram.copy schema, per-row source info, GroupBy after MergeJoin, Index/Pick types,
   nested nullability) on the merged tree, or cite the merged test. Then the four CI
   commands and `tests/test_docs`.
6. **PR.** Push (no force), update the PR body: keep `Part of ITL-810`, add `Refs ITL-875`,
   new status and test plan. Request a CodeRabbit round, address it, wait for CI green.
   `orca linear attach`, `orca worktree set --pr 269`, then `worker_done`.

## Out of scope

- Fixing any of the listed known issues.
- `pyproject.toml` URLs already changed earlier in this PR stay as they are.
