# ITL-1078 — remove `resolve_field` (developer approved via coordinator, Nov 13, 2026)

Decision: delete `SourceProtocol.resolve_field`, `RootSource.resolve_field`, `SourceProxy.resolve_field`,
`FieldNotResolvableError`, and every test/doc that asserts or promises it. No alias, no shim.
History: added 2026-02-27 (10d4c9b2); implementation dropped 2026-03-13 (b4c73c5c); never re-implemented; no caller; no replacement.

Steps: edit src (protocol, base, source_proxy, source_registry docstring, errors); delete tests
(objective class, ~15 tests in tests/test_core/sources); CLAUDE.md + .zed/rules layout line; TESTING_PLAN.md lines;
changelog.d/ITL-1078.md; run golden + sources tests; PR.
