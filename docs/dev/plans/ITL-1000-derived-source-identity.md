# ITL-1000 plan: DerivedSource tags_as_data mapping enters content hash

Decision (developer, 2026-11-12): the mapping is part of identity when used; fix before the tag.

1. Failing test (public API) in tests/test_core/sources/test_derived_source_tags_as_data.py:
   two DerivedSources, same origin, same explicit source_id, mappings of two same-typed tags to
   one target name -> content hashes differ; record ids, source ids, pipeline hashes equal.
   Without the option: identity_structure unchanged.
2. Fix: DerivedSource.identity_structure appends ("tags_as_data", sorted items) when given
   (as SpiralDBTableSource, PR #336). Pipeline hash stays schema-only. Plain form untouched.
3. Measure which hashes move (explicit id vs default id). Run golden suite; additions only.
4. Check to_config/from_config and saved-job node identities.
5. changelog.d/ITL-1000.md (Breaking changes and migration), design_issues/ITL-1000-*.md.
6. Suite 3.12/3.11/3.13, ruff, format, basedpyright. One push, PR, readiness.
