# ITL-578 follow-up: merge main, move Polars pins into the golden suite

**Goal:** bring PR #286 up to date with `main` (golden hash suite, ITL-626, merged in c5ab1e05),
prove the PR moves no pinned hash, and pin this PR's Polars hashes in the shared golden suite.

## Steps

1. Merge `origin/main` (no rebase). Resolve `DESIGN_ISSUES.md`: keep HS1 (main) and PL1 (this
   PR), each once, PL1 last.
2. Run `tests/test_hashing/golden` unchanged: every pinned entry must pass, and rerunning the
   generator must leave `data/` untouched. On any failure, stop and ask the coordinator.
3. Add Polars cases to `tests/test_hashing/golden/cases.py`, each a case of its own so that
   running the generator against an older release (`--allow-errors`) loses only the Polars
   entries:
   - `semantic:polars_value.<name>` — semantic hash of the five values pinned in
     `test_polars_dataframe_hashing._GOLDEN` (fixtures moved verbatim).
   - `datagram:polars_data_hash.<name>` — content hash of the `Data` holding the value in
     column `v` (the `_GOLDEN["data"]` construction, verbatim).
   - `semantic:polars_type.{dataframe,series}`, `schema:polars_single_field.*` (stored Arrow type
     and `Schema` hash, which name system-tag columns), and `datagram:polars_dataframe` /
     `datagram:polars_series` stream summaries (same shape as main's pandas `dataframe` case).
   TDD: `test_no_unpinned_entries` fails before the generator runs.
4. Run the generator. `git diff tests/test_hashing/golden/data` must show added lines only, and
   the `polars_value` / `polars_data_hash` hex digests must equal the `_GOLDEN` values.
5. Remove `_GOLDEN`, `_golden_values` and `test_golden_hashes` from
   `tests/test_hashing/test_polars_dataframe_hashing.py`; keep the property tests. Point the
   docs and PL1 at the golden suite.
6. Full suite on 3.11/3.12/3.13, ruff check/format, basedpyright. Push, update the PR
   description, address CI and CodeRabbit.
