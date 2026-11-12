# ITL-926 — Re-verify SpiralDB table as a source (workshop, November 17, 2026)

Linear: ITL-926 (related: ITL-554 CI project, ITL-40 string asymmetry, ITL-170 pyspiral pin,
ITL-471 metadata, done). Branch `eywalker/itl-926-re-verify-spiraldb-table-as-a-source-for`
from `origin/main` (ebf2511b). PR body: "Part of ITL-926" until the live run is done.

The coordinator's brief is the spec. Phase 1 needs no SpiralDB credentials.

## Phase 1 — audit, fake, end-to-end, pyspiral, live-run kit (no code changes to `src/`)

1. **Audit (A).** Read `spiraldb_table_source.py`, `db_table_source.py`, `spiraldb_connector.py`,
   `connector_arrow_database.py`, `db_connector_protocol.py`, `pipeline/serialization.py`
   against CLAUDE.md "Arrow types". Run the mocked unit tests
   (`tests/test_core/sources/test_spiraldb_table_source.py`,
   `tests/test_databases/test_spiraldb_connector.py`, `test_spiraldb_metadata_helpers.py`).
   Compare with SQLite/PostgreSQL `DBTableSource` subclasses. ITL-40 status.
2. **Fake pyspiral (B).** The existing mocks are `MagicMock`s (the source tests replace the
   whole connector), so they never run the connector against Arrow data. Build
   `tests/test_databases/fake_pyspiral.py`: `Spiral`, `Project`, `Table`, `Scan`, `Schema`,
   `TableResource` with the methods the connector calls, backed by real Arrow tables on disk
   (so a second process sees the same tables), and modelling the storage behaviours known from
   live runs: string/binary come back non-large, timezone dropped, Arrow metadata dropped
   (KV store round-trips), rows returned sorted by key, upsert by key. Each modelled behaviour
   is a named switch so it is clear which ones are verified live and which are assumptions.
3. **End-to-end (B).** Drive through the public interface:
   `SpiralDBTableSource` -> `PipelineJob` (function pod + `Join` with a second source) ->
   `DeltaTableDatabase` -> re-run (no recomputation) -> `save` -> `load` in a fresh process ->
   read results. Awkward column types: non-large/large string+binary, int8, float32, nullable
   and non-null, nested list/struct, dictionary, tz timestamps, decimal, extension type,
   composite PK, zero rows, no key schema. Record each failure with a minimal repro.
4. **pyspiral (C).** Scratch venvs (outside the repo lock) with pyspiral 0.16.x on 3.11, 3.12,
   3.13; inspect every symbol and signature the connector uses; read the changelog.
   Recommend a version; the pin change goes through `ask`.
5. **Live kit (D).** List what a live run needs; gap list of the integration tests vs. the
   workshop path; write `superpowers/specs/itl-926-live/live_verify.py` (not run).
6. **One `ask`** to the coordinator: findings by severity, fixes (mechanical vs decision),
   pyspiral recommendation, what the developer must provide.

Contained bug fixes (failing test first) may proceed before the answer; anything touching
public API, hashes, stored schemas or dependencies waits.

## Phase 2 — after the answer

Fixes with failing tests first, docs (`docs/concepts/sources.md` Spiral example with
`spiral login`, doc-test skip marker since it needs a live project), CHANGELOG `[0.3.0]`,
DESIGN_ISSUES entry, full suite one Python version at a time, PR.

## Notes / progress log

(updated as work proceeds so a successor can continue)

- 2026-11-11 Phase 1 done: fake (`tests/test_databases/fake_pyspiral.py`), live kit
  (`superpowers/specs/itl-926-live/live_verify.py` read-only + write modes,
  `inspect_tables.py`), findings in `superpowers/specs/itl-926-live/FINDINGS.md`.
  Developer decisions so far: workshop runs read-only on `edi-sample-v1-855632`; writes only
  to a separate test project (being provisioned); stay on a working pyspiral; CI restore
  nice-to-have. Live read-only probing from this terminal was blocked by the permission
  classifier after one successful call. Sent the Phase 1 `ask`; waiting for decisions.
