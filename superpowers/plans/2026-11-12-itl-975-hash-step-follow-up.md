# ITL-975 Hash Step Follow-up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the seven small findings left after PR #324's review (ITL-975), each with a
regression test that fails first, without moving any golden value, stored path or stored schema.

**Architecture:** Three two-to-five-line source fixes (a readable-metadata check in the
Arrow-to-Python type mapping, a flag reset in `FunctionJobNode.set_ephemeral_store`, a data
context check at the top of `CachedDataFunction.call` / `async_call`), three test-only
hardenings, and one sentence in an existing changelog fragment.

**Tech Stack:** Python, pyarrow, pytest, `uv run`.

**Source of the fixes:** the evaluation report's revision plan
(`.../scratchpad/itl967_base_findings/report.md`; its items 24, 5, 22, 23, 3, 19, 20 are
items 1-7 of the issue).

**Out of scope (do not touch):** per-row async checks, operator `forward`, cache-hit context
(ITL-951); read-only operator node (ITL-904).

---

### Task 1 (issue item 1): Arrow type to Python type checks the identifier

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py` (`_convert_arrow_to_python`, about line 1345)
- Test: `tests/test_logical_types/test_logical_type_identifiers.py`

- [ ] Failing test, after `test_values_of_an_unreadable_identifier_are_not_decoded`:

```python
def test_python_type_of_an_unreadable_identifier_is_refused() -> None:
    """Mapping an Arrow type to a Python type checks the identifier, also when nested."""
    strict = _converter(_fresh_context(_thing_logical_type(b'{"v":2}')))
    table = _typed_thing_column(b'{"v":1}')
    column_type = table.schema.field("c").type

    with pytest.raises(LogicalTypeIdentifierError):
        strict.arrow_type_to_python_type(column_type)
    with pytest.raises(LogicalTypeIdentifierError):
        strict.arrow_schema_to_python_schema(table.schema)
    with pytest.raises(LogicalTypeIdentifierError):
        strict.arrow_type_to_python_type(pa.struct([pa.field("t", column_type)]))
    with pytest.raises(LogicalTypeIdentifierError):
        strict.arrow_type_to_python_type(pa.large_list(column_type))  # if Arrow allows it


def test_python_type_of_a_readable_older_identifier_is_the_current_class() -> None:
    lenient = _converter(_fresh_context(_thing_logical_type(b'{"v":2}', also_reads=(b'{"v":1}',))))
    column_type = _typed_thing_column(b'{"v":1}').schema.field("c").type
    assert lenient.arrow_type_to_python_type(column_type) is Thing
```

- [ ] Run: `uv run pytest tests/test_logical_types/test_logical_type_identifiers.py -k python_type_of -q` — first test FAILS (returns the class).
- [ ] Fix: before `return lt.python_type`:

```python
            if lt is not None:
                # The schema names the registered class only for a column whose
                # values that class reads (never by extension name alone).
                from orcapod.logical_types.identifier import require_readable

                require_readable(lt, arrow_type.__arrow_ext_serialize__())
                return lt.python_type
```

  Check whether a result cache keyed by Arrow type sits in front of this method; if so the
  check must run before the cache is consulted or the cache must not hold a refused type.
- [ ] Run the file; PASS. Commit `fix(converter): ...`.

### Task 2 (item 2): `set_ephemeral_store` resets the stale-table flags

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` (`FunctionJobNode.set_ephemeral_store`, about line 1268)
- Test: `tests/test_pipeline/test_stored_data_context.py`, class `TestSideEffectFunctionNodes`

- [ ] Failing test `test_a_late_attached_ephemeral_store_is_checked_again`: build the `notify`
  node on a fresh store; `get_all_records()` (the node checks its clean stores); build an
  ephemeral store holding the result rows (`rdb_v2`) of a written store relabelled `OTHER`
  with other keys; attach it through the public `set_ephemeral_store` and
  `NodeConfig(is_result_ephemeral=True)` (through `PipelineJob.set_ephemeral_store` /
  `apply_node_config` if the paths allow, else on the node); `run()` must raise
  `SideEffectLogDataContextError` and `delivered == []`.
- [ ] Run; FAILS (node runs, delivers `[2, 4]`).
- [ ] Fix, at the top of `FunctionJobNode.set_ephemeral_store`:

```python
        # Another store has its own tables of other generations to account for.
        self._stale_tables_checked = False
        self._current_tables_checked = False
        self._unlisted_store_reported = False
```

- [ ] Run class; PASS. Commit.

### Task 3 (item 3): data context check before the cache bypass

**Files:**
- Modify: `src/orcapod/core/data_function.py` (`CachedDataFunction.call`, `async_call`)
- Test: `tests/test_pipeline/test_stored_data_context.py`, class `TestInputOfAnotherDataContext`

- [ ] Failing test, parametrized over sync/async and over
  `{}`, `{"skip_cache_lookup": True}`, `{"skip_cache_lookup": True, "skip_cache_insert": True}`:
  `CachedDataFunction(double.pod.data_function, result_database=store)`; call with
  `Data({"x": 1}, data_context=other_context)`; expect `InputValidationError` naming both
  contexts, `calls == []`, no table written.
- [ ] Run; the two bypass variants FAIL (function called).
- [ ] Fix: first statement of both methods:

```python
        # Checked before anything else: with ``skip_cache_lookup`` the lookup, which
        # also checks it, does not run, and the function must not be called.
        require_same_data_context(data.data_context_key, self.data_context_key)
```

  (import from `orcapod.contexts.stored`; check for an import cycle.)
- [ ] Run; PASS. Commit.

### Task 4 (item 4): env var in `test_list_table_paths.py`

- [ ] Show failure: `FSSPEC_S3_ENDPOINT_URL=https://example.test uv run pytest tests/test_databases/test_list_table_paths.py -k http_endpoint -q` FAILS.
- [ ] Add `monkeypatch.delenv("FSSPEC_S3_ENDPOINT_URL", raising=False)` in
  `test_an_http_endpoint_from_the_environment_follows_the_same_rule` (and check the sibling
  tests of the class under the same variable; clear it wherever it changes the outcome).
- [ ] Same command PASSES.

### Task 5 (item 5): `changelog.d/ITL-944.md` Migration paragraph

- [ ] "side-effect nodes fail on such a store" gains "unless the store cannot list its
  tables (then the node warns and runs)". `uv run python scripts/assemble_changelog.py --check`.

### Task 6 (item 6): label literals in `test_serialization_helpers.py`

- [ ] Replace the eight `"semantic_v1:..."` literals with strings built from
  `SEMANTIC_HASH_LABEL` (`orcapod.hashing.semantic_hashing.semantic_hasher`), e.g.
  `f"{SEMANTIC_HASH_LABEL}:abc123"`. Run the file.

### Task 7 (item 7): key-list length in `test_wrong_hash_findings.py`

- [ ] `sync_keys = stored_keys(...)`, `async_keys = stored_keys(...)`;
  `assert len(sync_keys) == len(async_keys) == 2` then equality. Run the test.

### Task 8: changelog fragment, verification, PR

- [ ] `changelog.d/ITL-975.md`: `### Fixed` with two bullets (items 1 and 3; item 2 too).
- [ ] `uv run ruff format .`, `uv run ruff check .`, `uv run --with basedpyright basedpyright` (exit 0),
  `uv run pytest tests/` on 3.12, golden regeneration with empty diff,
  `scripts/assemble_changelog.py --check`, `scripts/design_issues.py --check`.
- [ ] Merge origin/main if moved, push, PR against main (`Fixes ITL-975`), link in Orca,
  CodeRabbit rounds, `worker_done` with the reviewed sha.

## Notes for a successor

- 2026-11-12: Tasks 1-8 implemented and pushed (head 988a8361). PR #326 opened against main.
  Full suite on 3.12: 8913 passed, 101 skipped, 18 xfailed. Golden regeneration: empty diff.
  Item 1 also names the column in `arrow_schema_to_python_schema` (caller wraps with
  `for_column`). Item 4 clears both endpoint variables in the `bucket` fixture.
  Remaining: CI green, CodeRabbit round(s), `worker_done` with the reviewed sha.
- 2026-11-12, CodeRabbit round 1 (head c982e96b): one finding, reproduced. The cache in
  `arrow_type_to_python_type` is keyed by the Arrow type, and Arrow compares two instances
  of one extension class without their metadata, so a struct remembered for a readable
  identifier answered for an unreadable one. Coordinator approved option A: walk the type
  before the cache lookup (`_require_readable_members`), keep the check in
  `_convert_arrow_to_python`. Stop rule after this push: repeats, base findings and style
  notes are answered without a further push.
- 2026-11-12, CodeRabbit round 2 (head ce97810a, after merging origin/main): three threads.
  (1) the walk skipped unions: it now visits every nested type (`num_fields` / `field(i)`,
  extension storage, dictionary values). (2) base finding, included on the coordinator's
  decision: `get_arrow_to_python_converter` returned a cached value converter before the
  readable check; the walk now runs before that cache too. (3) unquoted annotation.
  Sweep of caches keyed by an Arrow type in `universal_converter.py`:
  `_arrow_to_python_types` (walk before lookup), `_arrow_to_python_converters` (walk before
  lookup), `_struct_signature_to_typeddict` (reached only from `_convert_arrow_to_python`,
  behind `arrow_type_to_python_type`'s walk). The other caches are keyed by Python types.
  After this push the stop rule applies.
