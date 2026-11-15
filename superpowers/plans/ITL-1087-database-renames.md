# ITL-1087 plan: rename remaining public "store" names to "database"

Mechanical rename, no behaviour change (skip-brainstorming: names decided by developer Nov 13-14; coordinator brief).

Renames: set_ephemeral_store->set_ephemeral_database (param store->database; ephemeral_result_store attr and
_ephemeral_store_in_use follow); StoreError->OrcapodDatabaseError; StoreWriteError->OrcapodDatabaseWriteError;
StoreReadError->OrcapodDatabaseReadError; StoreContentWarning->OrcapodDatabaseContentWarning;
describe_store_table->describe_database_table; store_path->database_path; utils/store_errors.py->utils/database_errors.py
(store_error_of, keep_store_error, note_callback_failure param store_error follow).

Steps: 1 git mv + perl rename over src/tests/docs/README/CLAUDE.md/.zed/rules/design_issues (not superpowers, CHANGELOG.md,
old changelog.d). 2 re-grep, list kept 'store' identifiers. 3 changelog.d/ITL-1087.md. 4 targeted tests, ruff, basedpyright,
golden. 5 merge origin/main, push, draft PR, CI, one signed @coderabbitai review.

Notes: (progress notes below)
Progress: rename applied (scripted), wording pass done; next tests, changelog, PR
