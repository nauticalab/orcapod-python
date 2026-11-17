# ITL-1068: warn that library value types are provisional

Branch `eywalker/itl-1068-warn-that-library-value-types-numpy-torch`. Refs ITL-1069 (v0.4
audit), ITL-1054. PR #339 (pandas handlers become `IgnoreListHandler`, pandas joins
`hash_ignore`) must be merged into this branch before the pandas part is finished.

Status (2026-11-13): approved with notes (test "one line removes a type"; practical docs
guidance; ITL-1069 only in design_issues; API reference page for warnings: none exists).
Implemented and pushed: class, helper, declaration, both registries, tests, docs, changelog,
rules, design issue. Revision of (b): the declared list is held per data context (logical type
registry `_config` ref + `PythonTypeHandlerRegistry.declare_provisional`), not process-global,
so a context without a name does not warn for it; only the issued set is process-global.
Stored-read test reads through `PipelineJob.load` + node `as_table` (a raw Delta read never
resolves the logical type). Remaining: merge main once #339 lands, then add
`EXTENSION_NAME = LogicalPandasDataFrame.EXTENSION_NAME` / `LogicalPandasSeries...` to the
two pandas handlers in builtin_handlers.py and a one-line pointer in hashing.md section 10;
verified on a trial merge (41/41 new tests, hashing/logical_types/contexts suites green).

## (a) Warning class

`ProvisionalHashWarning(UserWarning)` in `orcapod/errors.py`, beside
`ValueConversionWarning`, `RowFailuresWarning`, `StoreContentWarning` (all `UserWarning`
subclasses; none re-exported at top level). Public as `orcapod.errors.ProvisionalHashWarning`.
`UserWarning`, not `FutureWarning`: `FutureWarning` means "an API you call is deprecated /
will change behaviour"; this is a stability notice about stored data, and the three existing
Orcapod warnings are `UserWarning` subclasses. Both show by default; the class is what a user
filters on.

## (b) The one declaration

Top-level key `"provisional"` in `contexts/logical_types.json` (not frozen), beside
`hash_ignore`: a list of extension names, one per line. Removing a type = deleting its line.

Not a per-entry field: value handler entries are `[type, handler]` pairs with no slot, and a
`_config` key would be passed to the constructors, i.e. next to what builds the identifier.

`contexts/registry._create_context_from_spec` validates it (list of strings) and calls
`logical_types/provisional.declare_provisional(names)`: a process-global set (the
once-per-process bookkeeping is process-global anyway). It is passed to no constructor, so it
cannot reach extension metadata, an identifier, a hash or a path. Proof: golden suite and
`test_hash_label_rule` unchanged; plus a test that the identifiers and value hashes of all six
types are equal with and without the declaration.

## (c) Emit points

Helper module `orcapod/logical_types/provisional.py`:

- `declare_provisional(names)`, `warn_if_provisional(extension_name)`,
  `reset_provisional_warnings()` (test hook: clears the issued set).
- `threading.RLock` around check / warn / record. The name is recorded only after
  `warnings.warn` returns, so an error filter raises on every use (as `ValueConversionWarning`
  does).
- stacklevel: walk `sys._getframe` to the first frame outside the `orcapod` package; fall
  back to stacklevel 2 when none (worker threads).

Call sites (every use resolves the type through one of these):

1. `LogicalTypeRegistry.get_by_python_type` and `get_by_arrow_extension_name`: the only ways
   the type converter (17 call sites in `universal_converter.py`) and the database hooks (2)
   resolve a logical type. Covers source columns (Python values or Arrow extension columns),
   function pod input/output annotations (resolved when the pod is built), Arrow to Python
   conversion, reading stored columns. `get_by_logical_name` and registration do not warn.
2. `IgnoreListHandler.handle` and `hash_ignoring` (base of all six value handlers once #339
   is merged): a bare value hashed by the semantic hasher, which never touches the logical type
   registry. Each handler gets `EXTENSION_NAME` taken from its logical type class's constant
   (no new literal).

## (d) Message

"<ext name> values are provisional in Orcapod 0.3: their value hash and stored identity may
change in v0.4, and results cached now may then be recomputed. See 'Provisional value hashes'
in docs/concepts/arrays.md. To silence: warnings.filterwarnings('ignore',
category=orcapod.errors.ProvisionalHashWarning)."

## (e) What must not warn; CI noise

- `import orcapod`: no lookups at import (subprocess test).
- Pipelines of primitive / Arrow / Path / user-class columns: never resolve these names (test).
- `pytest.ini`: `filterwarnings = ignore::orcapod.errors.ProvisionalHashWarning` (one class).
  New tests: autouse fixture `reset_provisional_warnings()` and
  `catch_warnings(record=True)` + `simplefilter("always")` / `pytest.warns`.
- Existing tests with strict filters over these types are order-dependent once the warning
  exists (first use per process warns): `test_output_conformance.py` 488/511/524,
  `test_array_value_format.py` 263, `test_tensor_conversion_warning.py`,
  `test_job_run_summary.py` 151, `test_workshop_scenarios.py` 44. Each gets an explicit
  `filterwarnings("ignore", category=ProvisionalHashWarning)` inside its block; no loosening.
- Docs samples run in a subprocess and compare stdout only; warnings go to stderr.

## (f) Docs and changelog

- `docs/concepts/arrays.md`: section "Provisional value hashes" (anchor named in the message).
- `docs/reference/hashing.md` section 8 (array, torch) and 1e (polars), pandas section: a note.
- `changelog.d/ITL-1068.md` under `### Known limitations`.
- `CLAUDE.md` + `.zed/rules`: one rule.

## Steps (after approval)

1. Failing tests `tests/test_logical_types/test_provisional_warning.py`.
2. Warning class, helper, declaration, loader, two emit sites.
3. Adjust the strict-filter tests listed above; pytest.ini filter.
4. Docs, changelog, CLAUDE.md, .zed/rules.
5. Merge main after #339; pandas handlers.
6. Targeted tests (3.12), push, PR, Copilot review.
