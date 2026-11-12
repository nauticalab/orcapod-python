# ITL-905: refresh PR #301 against main (November 12, 2026)

Goal: merge `origin/main` (`b46549fd`, the v0.3 hash step) into the branch so the PR can merge
before the v0.3 tag. No design change: both points were decided by the developer on
November 12, 2026 and stay as implemented.

## Steps

1. Merge `origin/main` (merge commit, no rebase). Done: `c0f40fb9`.
   Hand-resolved: `src/orcapod/errors.py` (both `CanonicalOrderError` and
   `UnsupportedValueError`), `src/orcapod/semantic_types/universal_converter.py` (both imports;
   one `except` clause per error type in the per-column wrapper),
   `tests/test_hashing/golden/cases.py` (both sides' cases), `CHANGELOG.md` (main's),
   `DESIGN_ISSUES.md` (deleted, as on main), golden JSON (main's, regenerated in step 4).
2. `CHANGELOG.md` text -> `changelog.d/ITL-905.md`; `DESIGN_ISSUES.md` entry ->
   `design_issues/ITL-905-DO1.md`. Both checks pass.
3. v0.2.0 store tests (`TestV020Store`): v0.3 reads no v0.2 table, so the tests now show that
   every input is computed, nothing stored in insertion order is served, and the v0.2 tables
   keep their rows. Claims in the changelog fragment, design entry, `docs/reference/hashing.md`
   (section renamed 1e; main added its own 1d) and the PR body follow.
4. Golden: regenerate once; report every changed line against main by cause. Expected: two
   moved entries (`pipeline:index_pick.index.row[...]` data hashes, dict not in key order) and
   added `dict_order` entries. Anything else: stop and ask.
5. Suite on 3.12, 3.11, 3.13; ruff check, ruff format, basedpyright. One push. PR body.
6. CodeRabbit round on the new head; answer every thread and review-body finding.

## Decisions from the coordinator (November 12)

- Ordering layers: the semantic hasher's order (canonical JSON text of the hash input, ITL-942)
  and `canonical_order.py` (Arrow storage order) are two layers; stated in the PR body and
  `docs/reference/hashing.md`. Not unified.
- `ListLogicalType.python_to_storage` (`set[<logical type>]`) keeps its own `sorted()` /
  `repr` fallback (option A). Reported in `worker_done` with measurements; not changed here.

## Decided November 12, 2026 (unchanged, one decision point each)

- (a) `Datagram._canonical_dict_values` (`core/datagrams/datagram.py`): key-ordered copies.
- (b) `CanonicalOrderError` raised in `semantic_types/canonical_order.py`; column named by the
  callers (`Datagram._canonical_dict_values`, the converter's per-column wrapper).

## Codex continuation

- Existing issue, branch, plan and approved design are retained; no Linear changes.
- Fetch confirms main remains `b46549fd`; no second merge is needed.
- Reviewed regenerated goldens against main: exactly two existing pipeline row data hashes
  change; four Arrow keys and 27 Datagram keys are added for `dict_order`.
- Finish documentation of the dated decisions, measure logical sets without changing them,
  run suites sequentially on 3.12/3.11/3.13, then lint/format/type and fragment checks.
- Update PR body to current v0.3 claims, commit and push once, then check all review threads,
  review bodies and checks on the pushed head; route non-trivial findings to coordinator.

### Approved review continuation (November 12)

- Dataclass instances remain untouched: add a public characterization test and document
  canonical stored values/hashes versus insertion-ordered fresh nested dicts.
- Fix confirmed plain-set updates after Arrow materialization (coordinator approved):
  first add failing `Datagram.update` and `with_columns` tests; preserve exact Arrow schema
  and list order, then route set inputs at the plain-list conversion boundary through
  `sort_set_elements`. Check dict updates at the same boundary. No golden movement expected.
- Initial Python 3.12 suite: 9437 passed, 101 skipped, 19 xfailed, one failure because the
  untracked deleted `migrations/` directory retained `__pycache__` and imported as a namespace
  package. Preserved that directory under `/tmp/itl905-stale-migrations-20261112`; the
  failing old-layout test now passes. Full suite will rerun after the set-update fix.

### Validation checkpoint

- Code/docs committed as `a32387ce`; no push yet.
- Three public set-write regressions failed before the boundary fix and now pass.
- Focused dict/set/golden suite: 1145 passed; no additional golden changes.
- Full Python 3.12.15 suite: 9445 passed, 101 skipped, 19 xfailed, exit 0.
- Ruff check/format, changelog/design checks pass; basedpyright exits 0 (0 errors).
- Logical-set measurement: ten processes (seeds 0,1,2,42,random, twice each) agree.
  UUID bytes and Path strings use natural sorting and match `sort_set_elements`;
  frozen dataclass str/int fields use repr-sorted dict storage, rejected by the helper.
- Python 3.11 first run: 9440 passed with five packaging fixture errors from sandbox DNS;
  full rerun uses ordinary `uv run --python 3.11` with network permission.
- Scratch reports/logs are in `.untracked/`; the preserved stale migrations bytecode directory
  was moved there too. Developer withdrew the temporary custom-cache/environment instruction;
  subsequent runs use ordinary uv and the worktree's `.venv`.
- Coordinator approved replies: qualify nested-canonical unchanged-hash wording; document and
  defer user-class reconstruction; fix Arrow-backed set updates. Twelve older threads already
  have signed answers; two latest threads and two review-body set-update findings await replies.
- Full ordinary-uv Python 3.11.18 rerun: 9445 passed, 101 skipped, 19 xfailed,
  exit 0 (270.52 seconds); packaging passed. Python 3.13 full suite now running.

- Full ordinary-uv Python 3.13.18 suite: 9445 passed, 101 skipped, 19 xfailed,
  exit 0 (282.25 seconds). All three interpreter suites are green.
