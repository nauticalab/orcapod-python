# ITL-1147: PipelineJob.set_ephemeral_database with side-effect nodes

Bug: `PipelineBase.set_ephemeral_database` calls the setter on every non-source node;
`SideEffectJobNode` (live and loaded) has none -> AttributeError.

Fix (smallest): pipeline setter skips `source` and `side_effect` node types; no ephemeral
storage for side-effect nodes. Function and operator nodes still receive the database.

Tests (public interface, tests/test_pipeline/test_ephemeral_database_side_effects.py):
live job and loaded job with a side-effect node, set to a db and to None; function and
operator nodes receive it (read `ephemeral_result_database` where public).

Also: `### Fixed` bullet in changelog.d/ITL-1087.md; `Fixes ITL-1147` in PR body.
