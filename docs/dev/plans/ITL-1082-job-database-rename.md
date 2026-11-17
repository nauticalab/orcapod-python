# ITL-1082: PipelineJob `store` -> `database`

Mechanical rename of the PipelineJob parameter, property and private attribute
(`__init__`, `from_pipeline`, `bind`, `load`, `job.store`). No alias.
Out of scope: `set_ephemeral_store`, error classes, prose.
Saved-file key `bindings.store` -> `bindings.database` (decided Nov 13, 2026; old file raises a clear ValueError, ITL-1048 migration note). Notes: callers found by grep for `store=` and `.store`.

## CodeRabbit round 1 (head e836a023)
- Inline Major (keep `bindings.store`): declined, decided otherwise (see above). 0.2.0 was introduced in unreleased v0.3 and never shipped; released files carry 0.1.0.
- Outside diff: `superpowers/specs/itl-936-live/live_nullability.py:83` and `superpowers/scripts/itl_897/claim_d_migration.py:82` passed `store=`: fixed to `database=`.
- Whole-repo grep: no other runnable `store=` / `job.store`. Left: historical prose in superpowers/{plans,specs}/*.md, docs/metamorphic/plans/*.md, design_issues/ITL-904-PJLOAD1.md, notebooks/old_tutorials (legacy orcabridge `data_store=`).

## Follow-up (separate PR, developer decision Nov 14, 2026; NOT in #368)
Rename the remaining store names. Define each class name by a single-token replace so `DatabaseError` can still change.
- A. `set_ephemeral_store` -> `set_ephemeral_database` (Pipeline, function/operator nodes, protocols; parameter `store` -> `database`; `_ephemeral_store*` follow). `StoreError`/`StoreWriteError`/`StoreReadError` -> `DatabaseError`/`DatabaseWriteError`/`DatabaseReadError`; `SideEffectLogDataContextError` derives from `DatabaseError`.
- B. yes: `StoreContentWarning` -> `DatabaseContentWarning`; `contexts/stored.py` `describe_store_table` -> `describe_database_table`, `store_path` -> `database_path`, private protocols `_DescribedStore`/`_RecordStore`; `utils/store_errors.py` -> `database_errors.py`, `keep_store_error` -> `keep_database_error`, `store_error` -> `database_error`; prose naming the ArrowDatabase as "store" -> "database". Keep: `ResultCache.store` (verb), test file names and local variable names.
- Must not reach saved-file keys, column names, layout constants or hashes (the legacy `bindings.store` check in `load()` stays).
