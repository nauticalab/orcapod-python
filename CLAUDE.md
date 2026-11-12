# Claude Code instructions for orcapod-python

## Running commands

Always run Python commands via `uv run`, e.g.:

```
uv run pytest tests/
uv run python -c "..."
```

Never use `python`, `pytest`, or `python3` directly.

## Updating agent instructions

When adding or changing any instruction, update BOTH:
- `CLAUDE.md` (for Claude Code)
- `.zed/rules` (for Zed AI)

## Design issues log

`design_issues/` at the project root is the canonical log of known design problems, bugs, and
code quality issues: one file per entry (developer, November 11, 2026; ITL-968). There is no
`DESIGN_ISSUES.md`; never bring it back, and never put two entries in one file.

- An entry is the file `design_issues/<LINEAR-ISSUE-ID>-<ENTRY-ID>.md` (e.g.
  `design_issues/ITL-955-GB1.md`). Its first line is the `## <area heading>` it belongs
  under, then a blank line, `### <ENTRY-ID> — <title>`, the `**Status:**` (`open`,
  `in progress` or `resolved`), `**Severity:**` and `**Issue:**` lines, a blank line and the
  body.
- Read the log with `uv run python scripts/design_issues.py` (index by area; `--status open`
  filters; `--full` prints every entry). Find one entry by id: `ls design_issues/*-GB1.md`.
- Check the files with `uv run python scripts/design_issues.py --check`
  (`tests/test_design_issues.py` runs it in CI).
- `design_issues/README.md` is the full statement of the convention, with the severity
  guide.

When fixing a bug or addressing a design problem:
1. Check `design_issues/` first — if a matching entry exists, edit its file: update its
   status to `in progress` while working and `resolved` once done, adding a brief **Fix:**
   note.
2. If no matching entry exists, ask the user whether it should be added before proceeding.
   If yes, add it as a new file (status `open` or `in progress` as appropriate).

When discovering a new issue that won't be fixed immediately, ask the user whether it should be
logged in `design_issues/` before adding it.

## Superpowers artifacts

Place all superpowers-related artifacts (design specs, plans, etc.) in the `superpowers/`
directory at the project root — **not** under `docs/`. The `docs/` directory is reserved for
actual library documentation.

- Specs go in `superpowers/specs/`

## Backward compatibility

This is a greenfield project pre-v0.1.0. Do **not** add backward-compatibility shims,
re-exports, aliases, or deprecation wrappers when making design or implementation changes.
Just change the code and update all references directly.

## No `sys.modules` hacks

Never manipulate `sys.modules` directly (e.g. `sys.modules.setdefault`). If a subpackage
import path doesn't work, create a proper re-export package with an `__init__.py` instead.

## No message-only parameters

Never pass a value into a function only so that the function can put it in an error or log
message (developer, November 11, 2026). If the function does not use the value to do its job,
it must not receive it: the function raises what it knows, and the caller, which has the
context, catches and wraps or annotates the error (path, row, column, node label). A helper
whose job is to report or record (log a suppressed failure, attach a note) legitimately takes
the label and context it records.

## Docstrings

Use [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
Python docstrings everywhere.

**Never mix in ReST markup.** Do not use `:meth:`, `:class:`, `:exc:`, `:attr:`,
`:func:`, or any other ReST cross-reference roles. Use plain ``backtick`` notation
to refer to names (e.g. ``PollingSource``, ``iter_data``, ``CursorInvalidatedError``).

## Documentation samples

Every fenced `python` block in `README.md`, `docs/index.md`, `docs/getting-started.md`,
`docs/concepts/` and `docs/reference/` is executed by `tests/test_docs/test_doc_samples.py`,
and `tests/test_docs/test_mkdocs_build.py` runs `mkdocs build --strict`. When writing docs:

- Blocks on one page share a namespace and run top to bottom.
- Every line a block prints must appear as a `# ...` comment in that block; `...` inside the
  comment is a wildcard for hashes and other run-dependent values.
- Put `<!-- docs-test: skip -->` on the line above a fence only when the sample needs
  infrastructure the test run lacks (e.g. a Postgres server) — never to hide a wrong sample.
- If a sample fails because the library changed, fix the page to match the code.

## Linear issue tracking

All work must be linked to a Linear issue. Before starting any feature, bug fix, or
refactor:

1. **Check for an existing issue** — search Linear for a corresponding issue.
2. **If none exists** — ask the developer whether to create one. Do not proceed without
   either a linked issue or explicit approval to skip.
3. **When starting work on an issue** — update its Linear status to **In Progress**.
4. **When a new issue is discovered** during development (bug, design problem, deferred
   work), create a corresponding Linear issue using the template below.

When creating Linear issues, always use this template for the description:

```
## Overview
What is this project about? Describe the problem space and the high-level approach.

## Goals & Success Criteria
* Specific, measurable outcomes.

## Scope & Boundaries
(Optional — remove if not needed.)
In scope:
* ...
Out of scope:
* ...

## Dependencies & Risks
(Optional — remove if none.)
* ...

## Resources & References
(Optional — remove if none.)
* ...

## Milestones
(Optional — only for projects longer than ~4 weeks. Remove for shorter projects.)
* ...
```

Remove any optional sections that don't apply rather than leaving them empty.

### Branches and PRs

When working on a feature, create and checkout a git branch using the `gitBranchName`
returned by the primary Linear issue (e.g. `eywalker/plt-911-add-documentation-for-orcapod-python`).

**Feature branch PRs always target `main`.** Create a feature branch from `main` and open PRs against `main`.

If a feature branch / PR corresponds to multiple Linear issues, list all of them in the
PR description body so that Linear's GitHub integration auto-tracks the PR against each
issue. Use the format `Fixes PLT-123` or `Closes PLT-123` (GitHub magic words) for issues
that the PR fully resolves, and simply mention `PLT-456` for issues that are related but
not fully resolved by the PR.

## Responding to PR reviews

When asked to respond to PR reviewer comments:

1. **Read all comments carefully** — fetch every review comment on the PR before forming any opinion.
2. **Evaluate each comment** — decide whether to accept, partially accept, or decline, and why.
3. **Present a revision plan** — show the full plan (table: comment summary, verdict, proposed action)
   and **wait for user approval before touching any code or posting any replies**.
4. **Fix, then reply** — once approved, make all fixes in a single commit, then post replies to each
   reviewer comment explaining what was done (or why it was declined).

Never implement changes or reply to reviewers before the plan has been approved.

## Coordinating issue-specific agents with Orca

When the developer asks you to coordinate, supervise, or dispatch issue work across agents,
follow `AGENT_COORDINATION.md` at the project root. In short:

- The coordinator owns scope, task specs, and reporting. It never edits issue code and never
  substitutes generic subagents for Orca workers.
- Each Linear issue gets its own Orca worktree (from `origin/main`, on the issue's exact
  `gitBranchName`) and one dedicated worker, started with `orca orchestration worker-start`.
- Workers route every question through the orchestration `ask` command. Wherever this file
  says to ask the user or wait for approval, a dispatched worker asks its coordinator.
- Workers push and open PRs against `main`. No agent merges a PR, cuts a release, or marks
  an issue Done.
- The coordinator approves obvious plans and specs itself and raises only critical decisions
  (public API, hash or stored-schema changes, new dependencies, scope growth, release) to
  the developer.
- Every wave opens with a discussion of its key decision points, and the developer selects
  what is dispatched. Issues the developer did not file are suggestions to triage, not
  decisions.
- A PR reaches the developer only after its CodeRabbit rounds are addressed and CI is green
  (the runbook's PR readiness pipeline). Every GitHub comment, reply, and review an agent
  posts ends with the agent signature the runbook gives.

Load the version-matched guides (`orca skills get orca-cli`, `orca skills get orchestration`)
before running Orca commands.

## Git commits

Always use [Conventional Commits](https://www.conventionalcommits.org/) style:

```
<type>(<optional scope>): <short description>
```

Common types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`, `ci`.

Examples:
- `feat(schema): add optional_fields to Schema`
- `fix(data_function): reject variadic parameters at construction`
- `test(function_pod): add schema validation tests`
- `refactor(schema_utils): use Schema.optional_fields directly`

## Changelog

A pull request never edits `CHANGELOG.md` (developer, November 11, 2026; ITL-961). It adds
exactly one file, `changelog.d/<ISSUE-ID>.md` (e.g. `changelog.d/ITL-955.md`; a PR that
resolves several issues uses its primary issue's id).

- The file holds only the text that would have gone under the release heading in
  `CHANGELOG.md`: the same category headings `CHANGELOG.md` uses
  (`### Breaking changes and migration`, `### Added`, `### Changed`, `### Fixed`,
  `### Known limitations`), each followed by its text: bullet lines, or `####` sub-headings
  with paragraphs, as `CHANGELOG.md` itself has. No version heading, no date, no other
  `#`, `##` or `###` heading, and no empty category.
- If your branch already edits `CHANGELOG.md`, restore `CHANGELOG.md` to its state on the
  branch's base and move that text, unchanged, into the fragment.
- Check a fragment with `uv run python scripts/assemble_changelog.py --check`
  (`tests/test_changelog_fragments.py` runs it in CI).
- Only the release PR changes `CHANGELOG.md`: it folds the fragments in with
  `uv run python scripts/assemble_changelog.py --version X.Y.Z`, which also deletes them
  (see `RELEASING.md`). Entries already in `CHANGELOG.md` on `main` stay there.
- `changelog.d/README.md` is the full statement of the convention.

## Lint, format and type checks

Every pull request must pass the metamorphic-labs required CI, which runs exactly these
commands (pytest on Python 3.11, 3.12 and 3.13; the others on 3.11):

```
uv run --with ruff ruff check .
uv run --with ruff ruff format --check .
uv run --with basedpyright basedpyright
uv run pytest tests/
```

- New and changed code must be ruff-clean and ruff-formatted, and must add no
  basedpyright errors. Warnings are reported but do not fail the check
  (`failOnWarnings = false`); still do not add them needlessly. Run
  `uv run ruff format .` and `uv run ruff check .` before committing;
  `uv run pre-commit install` once per clone runs both on every commit.
- Check types with `uv run --with basedpyright basedpyright` and read its **exit code**:
  non-zero means a new error and fails CI.
- The tool versions are the ones in `uv.lock`: `--with ruff` and `--with basedpyright` are
  satisfied by the locked dev dependencies, so a version bump in `uv.lock` can change what
  CI reports.
- Existing type errors and warnings are recorded in `.basedpyright/baseline.json`, a debt
  list to pay down. Never regenerate the baseline (`basedpyright --writebaseline`) to
  hide a new error; regenerate it only to **remove** entries you fixed. Fix the
  new diagnostic, or, when it is a genuine false positive, suppress that one line with a
  targeted `# pyright: ignore[<rule>]` and a reason.
- Do not lower the basedpyright strictness mode, and do not add repo-wide or per-directory
  ruff ignores or excludes to make a check pass. A targeted `# noqa: <rule>` on one line
  needs a reason.
- The Graphviz `dot` binary is optional. The tests that render a graph to an image skip
  when `dot` is not on `PATH` and run when it is.

## Logical type identifiers

A logical type's identifier is its Arrow extension name plus its Arrow extension metadata
(developer, November 11, 2026, ITL-935/ITL-938); see `docs/concepts/logical-type-identifiers.md`.

- The identifier is declared in one place, the extension type returned by
  `get_arrow_extension_type()`, and read through `logical_types/identifier.py`
  (`logical_type_identifier`, `LogicalTypeIdentifier.text`). Never put a logical type's bare
  name into a hash input: use the identifier text.
- Change a type's identifier when, and only when, an existing value would be stored or hashed
  differently. It is not a package version. Built-in types use `version_metadata(N)`
  (`{"version":N}`); a new built-in type must write one, never empty metadata.
- A changed identifier is a schema change: it moves stored paths and golden values of that
  type. Treat it like any other hash change (developer approval, migration assessment).
- The native containers `list[T]` / `set[T]` of a logical type (`ListLogicalType`) have no
  version of their own, by design: never add a `"version"` key to their metadata. Their
  identifier is the container kind plus the element's identifier; the type hash treats them
  like any generic alias (origin and arguments). A change to how they are stored or hashed is
  a data context (global hash version) change, not a logical type change.
- On read, stored metadata must be one of the type's `readable_extension_metadata()`;
  otherwise raise `LogicalTypeIdentifierError`. Never wrap a stored column by name alone and
  never fall back to the storage type. List an older value in `also_reads` only when the
  current code decodes that layout.

## User classes: exact and structural

A class a user defines is one of two kinds (developer, November 12, 2026, ITL-972; see
`docs/concepts/your-own-data-types.md`).

- An ordinary `@dataclass` or a pydantic model is an **exact** class: named by module path
  and qualified name, and one type only with itself. Never make two different exact classes
  equivalent (no declared names, no equivalence by description). Python subclassing still
  applies at a pod input: a subclass column is accepted at a base-class parameter, and the
  subclass keeps its own identity and fields.
- A class made with `orcapod.dataclass` is **structural**: identified by its name (default
  the class name) and its fields in declared order. Two structural classes with equal
  identity records are compatible in the schema check and their instances compare equal.
  Equality and hashing of a structural class cover exactly its stored fields; never let a
  per-field `compare=` or `hash=` choice through, because a class rebuilt from metadata
  cannot know it. Pydantic has no structural kind (ITL-973).
- **No automatic conversion.** Orcapod checks compatibility and passes a value as it is. A
  function annotated with a structural class may receive another class of the same
  identity; a structural class promises its fields, not its methods. `from_instance` is the
  only conversion, and Orcapod never calls it.
- A declared class stores its own instances and subclasses (`UserClassLogicalType.accepts_instance`);
  only the declared class fields enter storage. Unrelated look-alike classes are refused.
- The identity record (`logical_types/user_class_identity.py`) is `{kind, name, fields}`
  with JSON type tokens. The Arrow extension name is `name@<SHA-256 of the record>` and the
  record is the extension metadata; together they are the logical type identifier. A nested
  class embeds its whole record and a logical-type field the whole identifier text, so a
  structural class can be rebuilt from metadata alone. A change to the record or token
  format moves every user-class identifier: treat it as a hash change.
- Arrow-backed data carries the identifier, not a class object: the registry binds an
  identifier to the newest class object with that identity, and data is read as that class.

## Golden hash values

Hash values must stay identical across releases (developer, November 10, 2026). Every hash
Orcapod stores or embeds in a name is pinned in `tests/test_hashing/golden/data/*.json` and
checked by `tests/test_hashing/golden/test_golden_hashes.py`.

- A failing golden entry means a hash moved. Treat it as a bug in the change, not a stale
  file. Never regenerate the golden files to make a test pass.
- Regenerate (`uv run python tests/test_hashing/golden/generate_golden_hashes.py`) only to add
  entries (check the diff adds lines and changes none), or for a hash change the developer has
  approved together with a migration assessment.
- Never move or rename the fixtures in `tests/test_hashing/golden/cases.py`: function pods,
  dataclasses and pydantic models hash with their module path.

## Hash labels

One rule holds for every hash label (developer, November 11, 2026; ITL-945; stated with
examples in `docs/reference/hashing.md`, "The hash label rule").

- A label names the algorithm that produced a digest (the whole procedure: canonical form,
  preparation, encoding, digest function) and nothing else. Format `<algorithm>_v<N>`,
  integer `N`: `semantic_v1`, `arrow_v1`, `directory_v1`, `ndarray_v1`, `polars_expr_v1`. The
  file content hash keeps `sha256`, the only exception. A logical type's implementer picks
  its label.
- Change a label only when its algorithm would give an existing input a different digest.
- Define each label in exactly one place, as a constant beside its algorithm. Never write a
  label literal anywhere else in code, and never in a data context file.
- Never feed a label into a hash computation. Feed a nested hash's digest only: never put
  `to_string()`, `str(hash)` or `to_prefixed_digest()` into an identity structure, a preimage
  table or a digest input. Inside a structure, return the `ContentHash` object and let the
  structure hasher embed it.
- A hash inside a name (path component, column name, `uri`) is the bare hex digest:
  `to_hex()`. Exception (developer, November 11, 2026): a source id is the source's name. A
  default source id built from a hash keeps a source-kind prefix and the hash's label
  (`derived:<label>:<hex>`, via `to_string()`, never a label literal) and is used as it is in
  the `source_id` system tag and in source-info values.
- A hash stored as a value in its own column or field carries its label: `to_string()` as
  text, `to_prefixed_digest()` in a binary column.
- A record id is an identifier built by a recipe, not a hash, and carries no label.
- `tests/test_hashing/test_hash_label_rule.py` recomputes every golden value under renamed
  labels; a value that moves there means a label reached a hash input.

## Testing conventions

Tests use the **public interface** wherever possible (developer, November 10, 2026): drive an
operator by calling it, `op(stream)`, and read the result through the stream's public methods.
Use a non-public entry point (`unary_static_process`, `static_process`,
`binary_static_process`, `_private` helpers) only when the test is specifically about that
entry point. The standing exception is verifying a schema prediction, which must compare
against `static_process(...).output_schema(...)`; see "Important implementation details".

---

## Project layout

```
src/orcapod/
├── types.py                    # Schema, ColumnConfig, ContentHash
├── system_constants.py         # Column prefixes and separators
├── errors.py                   # InputValidationError, DuplicateTagError, FieldNotResolvableError
├── config.py                   # Config dataclass
├── contexts/                   # DataContext (semantic_hasher, arrow_hasher, type_converter)
├── protocols/
│   ├── hashing_protocols.py    # PipelineElementProtocol, ContentIdentifiableProtocol
│   └── core_protocols/         # StreamProtocol, PodProtocol, SourceProtocol,
│                               # DataFunctionProtocol, DatagramProtocol, TagProtocol,
│                               # DataProtocol, TrackerProtocol
├── core/
│   ├── base.py                 # ContentIdentifiableBase, PipelineElementBase, TraceableBase
│   ├── static_output_pod.py    # StaticOutputPod (operator base), DynamicPodStream
│   ├── function_pod.py         # FunctionPod, FunctionPodStream, FunctionNode
│   ├── data_function.py      # DataFunctionBase, PythonDataFunction, CachedDataFunction
│   ├── operator_node.py        # OperatorNode (DB-backed operator execution)
│   ├── tracker.py              # Invocation tracking
│   ├── datagrams/
│   │   ├── datagram.py         # Datagram (unified dict/Arrow backing, lazy conversion)
│   │   └── tag_data.py       # Tag (+ system tags), Data (+ source info)
│   ├── sources/
│   │   ├── base.py             # RootSource (abstract, no upstream)
│   │   ├── arrow_table_source.py  # Core source — all other sources delegate to it
│   │   ├── derived_source.py   # DerivedSource (backed by FunctionNode/OperatorNode DB)
│   │   ├── csv_source.py, dict_source.py, list_source.py,
│   │   │   data_frame_source.py, delta_table_source.py  # Delegating wrappers
│   │   └── source_registry.py  # SourceRegistry for provenance resolution
│   ├── streams/
│   │   ├── base.py             # StreamBase (abstract)
│   │   └── arrow_table_stream.py  # ArrowTableStream (concrete, immutable)
│   └── operators/
│       ├── base.py             # UnaryOperator, BinaryOperator, NonZeroInputOperator
│       ├── join.py             # Join (N-ary inner join, commutative)
│       ├── merge_join.py       # MergeJoin (binary, colliding cols → sorted list[T])
│       ├── semijoin.py         # SemiJoin (binary, non-commutative)
│       ├── batch.py            # Batch (group rows by count, types become list[T])
│       ├── group_by.py         # GroupBy (many→one reduction keyed on tag values)
│       ├── column_selection.py # Select/Drop Tag/Data columns
│       ├── mappers.py          # MapTags, MapData (rename columns)
│       ├── filters.py          # PolarsFilter
│       └── drop_nulls.py       # DropNulls (drop rows with a null in named or all columns, T | None → T)
├── hashing/
│   └── semantic_hashing/       # BaseSemanticHasher, type handlers
├── semantic_types/             # Type conversion (Python ↔ Arrow)
├── databases/                  # ArrowDatabaseProtocol implementations (Delta Lake, in-memory)
└── utils/
    ├── arrow_data_utils.py     # System tag manipulation, source info, column helpers
    ├── arrow_utils.py          # Arrow table utilities
    ├── schema_utils.py         # Schema extraction, union, intersection, compatibility
    └── lazy_module.py          # LazyModule for deferred heavy imports

tests/
├── test_core/
│   ├── datagrams/              # Lazy conversion, dict/Arrow round-trip
│   ├── sources/                # Source construction, protocol conformance, DerivedSource
│   ├── streams/                # ArrowTableStream behavior
│   ├── function_pod/           # FunctionPod, FunctionNode, pipeline hash integration
│   ├── operators/              # All operators, OperatorNode, MergeJoin
│   └── data_function/        # DataFunction, CachedDataFunction
├── test_hashing/               # Semantic hasher, hash stability
├── test_databases/             # Delta Lake, in-memory, no-op databases
└── test_semantic_types/        # Type converter tests
```

---

## Architecture overview

See `orcapod-design.md` at the project root for the full design specification.

### Core data flow

```
RootSource → ArrowTableStream → [Operator / FunctionPod] → ArrowTableStream → ...
```

Every stream is an immutable sequence of (Tag, Data) pairs backed by a PyArrow Table.
Tag columns are join keys and metadata; data columns are the data payload.

### Core abstractions

**Datagram** (`core/datagrams/datagram.py`) — immutable data container with lazy dict ↔ Arrow
conversion. Two specializations:
- **Tag** — metadata columns + hidden system tag columns for provenance tracking
- **Data** — data columns + per-column source info provenance tokens

**Stream** (`core/streams/arrow_table_stream.py`) — immutable (Tag, Data) sequence.
Key methods: `output_schema()`, `keys()`, `iter_data()`, `as_table()`.

**Source** (`core/sources/`) — produces a stream from external data. `ArrowTableSource` is the
core implementation; CSV/Delta/DataFrame/Dict/List sources all delegate to it internally. Each
source adds source-info columns and a system tag column. `DerivedSource` wraps a
FunctionNode/OperatorNode's DB records as a new source.

**Function Pod** (`core/function_pod.py`) — wraps a `DataFunction` that transforms individual
data. Never inspects tags. Two execution models:
- `FunctionPod` → `FunctionPodStream`: lazy, in-memory
- `FunctionNode`: DB-backed, two-phase (yield cached results first, then compute missing)

**Calling a function pod** (developer, November 12, 2026, ITL-807). `pod(a, b)` is the
convenient call and `pod.process(a, b)` the exact primitive:

- `pod(a, b)` joins the inputs as `process` does and, when the joined stream carries data
  columns that are not parameters of the function, places one automatic `SelectDataColumns`
  after the automatic join and before the pod (`_FunctionPodBase.__call__`, the one place
  that decides it). The select is an ordinary operator step: graph, hashes and stored tables
  must equal those of a pipeline with that select written by hand (the stream's
  `select_data_columns` method builds the same operator, and `a.join(b)` the same `Join`).
  Nothing to drop means no step. One info log names the dropped columns; it is not a warning.
- `pod(a, b, strict=True)` and `pod.process(a, b)` reject an extra column. `strict` is part of
  no identity. `process` never takes a `strict` argument and never drops a column.
- A missing required column is an error in every form. Tags are never dropped. A data column
  shared by two inputs fails at the join, used or not: no special handling.
- No select is added when the data function's `accepts_arbitrary_keywords` is true (no kind
  today; `PythonDataFunction` rejects `**kwargs`).
- Function pods only. An operator's and a `SideEffectPod`'s `__call__` stay aliases of
  `process` with no `strict` argument. There is no global setting.
- A test of the exact input contract uses `process` or `strict=True`.

**Operator** (`core/operators/`) — structural pod transforming streams without synthesizing new
data values. All subclass `StaticOutputPod`:
- `UnaryOperator` — 1 input (Batch, Select/Drop columns, Map, Filter)
- `BinaryOperator` — 2 inputs (MergeJoin, SemiJoin)
- `NonZeroInputOperator` — 1+ inputs (Join)

**OperatorNode** (`core/operator_node.py`) — DB-backed operator execution, analogous to
FunctionNode.

### Strict operator / function pod boundary

| | Operator | Function Pod |
|---|---|---|
| Inspects data content | Never, except filters (see below) | Yes |
| Inspects / uses tags | Yes | No |
| Can rename columns | Yes | No |
| Synthesizes new values | No | Yes |
| Stream arity | Configurable | Single in, single out |

**No data element comes from a tag value** (developer, November 11, 2026, ITL-955). An operator
may *use* tags (to join, filter, group or order), but tag content never becomes data: every
element of an output data column came from an input data column, and an operator never turns a
tag column into a data column. When adding or changing an operator, check `keys()` of the output:
no input tag name may appear among the data columns.

Two operators reduce row count many→one. `Batch` groups consecutive rows by count; `GroupBy`
groups by tag values: it collapses the N rows sharing a tag tuple into one row with
list-valued members. Neither synthesizes new data values — every emitted element came from an
input row — and `GroupBy` keys only on tags, never on data. Its non-key tags stay tags,
list-valued, as `Batch` keeps them.

Filters are the operators that read data columns (ITL-943): `PolarsFilter` evaluates its
predicate on values, and `DropNulls` reads only whether a value is present (its validity),
never the value. A filter decides which rows exist and never synthesizes a value; no other
operator reads data content.

**Operator or derived source?** (developer, November 11, 2026) The test is: does any data
element come from a tag value, or from anywhere other than the input's data elements? If yes,
the step is not an operator: it is a derived source. So a tag's value is offered as data only
by a source, `DerivedSource(origin, tags_as_data={tag: data_name})` (ITL-939), never by an
operator. That form is a new root: fresh provenance through `SourceStreamBuilder`, schema-only
`pipeline_hash`, and a default `source_id` of the form
`tags_as_data:<algorithm label>:<hex>` (the kind names the operation; the config type key stays
`derived`), the labelled content hash an operator node doing the
derivation would have (`DerivedSource._default_source_id`, the one place that defines it;
the string is used as-is in system-tag values and source-info tokens). `DerivedSource` without `tags_as_data` (a job node's stored records)
keeps its old identity; do not change it (ITL-956).

### Two identity chains

Every pipeline element has two parallel hashes:

1. **`content_hash()`** — the pipeline bound to its named sources: schema, topology and source
   identity. For a source it is a stable *address*, not a digest of its rows (developer,
   November 11, 2026: "sources content hash is meant to be a stable address and is not
   necessarily going to reflect the very data content"). A root source hashes
   `(class name, output schema, source_id)`; given a `source_id`, changed, added or reordered
   rows leave it unchanged. Row content enters only where a source puts it into its own
   identity: the default `source_id` of the in-memory sources
   (`<source kind>:<algorithm label>:<hex>` of the table hash, used when none is given) and the elements of a `ListSource`. A downstream stream or node hashes its
   pod's identity plus its upstreams' content hashes; a function pod's identity is its uri,
   not its body. Only a datagram (one row's tags or data) has a `content_hash()` that is a
   digest of its values; that row hash, not the node's content hash, is what results are
   memoized by. The node's content hash tells apart, inside a shared table, the rows of
   pipelines bound to different sources (`__node_content_hash`).
2. **`pipeline_hash()`** — schema + topology only. Ignores data content. Used for DB path
   scoping so that different sources with identical schemas share database tables.

Base case: `RootSource.pipeline_identity_structure()` returns `(tag_schema, data_schema)`.
Each downstream node's pipeline hash commits to its own identity plus the pipeline hashes of
its upstreams, forming a Merkle chain.

The pipeline hash uses a **resolver pattern** — `PipelineElementProtocol` objects route through
`pipeline_hash()`, other `ContentIdentifiable` objects route through `content_hash()`.

### Column naming conventions

| Prefix | Meaning | Example | Controlled by |
|--------|---------|---------|---------------|
| `__` | System metadata | `__data_id`, `__pod_version` | `ColumnConfig(meta=True)` |
| `_source_` | Source info provenance | `_source_age` | `ColumnConfig(source=True)` |
| `_tag::` | System tag | `_tag::source:abc123` | `ColumnConfig(system_tags=True)` |
| `_context_key` | Data context | `_context_key` | `ColumnConfig(context=True)` |

Prefixes are computed from `SystemConstant` in `system_constants.py`. The `constants` singleton
(with no global prefix) is used throughout.

### Stored tables: data context and layout versions

Every row Orcapod stores carries `__data_context` (`constants.DATA_CONTEXT_COL`,
`large_string`): the key of the data context that wrote it (`std:v1:default` since v0.3;
developer, November 11, 2026, ITL-944). The key names one generation of hashing; rows of the
current data context are the current entries.

| Table | Layout (last path component) | Constant |
|---|---|---|
| Function pipeline | `pdb_v2` | `PIPELINE_DB_SCHEMA_VERSION` |
| Function result | `rdb_v2` | `RESULT_DB_SCHEMA_VERSION` |
| Side-effect log | `tdb_v2` | `TRACKING_DB_SCHEMA_VERSION` |
| Operator cache | `odb_v1` | `OPERATOR_DB_SCHEMA_VERSION` |
| Cached source | `sdb_v1` | `SOURCE_DB_SCHEMA_VERSION` |

- A change to the bookkeeping columns of a table kind moves its layout version, so the table
  gets a new path. Never add, rename or retype a stored column without it.
- Every new stored table kind writes `__data_context` (`contexts/stored.with_data_context`),
  and every read or lookup keeps rows of the current data context only
  (`keep_rows_of_data_context`, or a lookup constraint on the column).
- A table at an older layout is never read as current and never modified. What a node does
  about such tables, and about rows of another data context, is decided in one place,
  `contexts/stored.account_for_stale_tables`: count and report, or, for a node that delivers
  side effects (a `SideEffectPod` node, or a function node whose pod takes the invocation
  context, `ctx_arg`), raise `SideEffectLogDataContextError` before any delivery. A new kind
  of node goes through that function; never decide it locally.
- A table written before v0.3 is found by NAME, never by a computed hash (ITL-954): the
  hashes in a path differ between versions. `find_stale_tables(..., name_path=...)` lists the
  store under the hash-free beginning of the node's path (`ArrowDatabaseProtocol.list_table_paths`;
  `<function>`, `side_effect_function/<function>`, `side_effects/<pod>`, relative to the
  node's database) and every table there whose last component is not in
  `contexts/stored.CURRENT_LAYOUTS` is pre-v0.3. A new layout constant must be added to
  `CURRENT_LAYOUTS`, and a superseded one to the `OLD_*_SCHEMA_VERSIONS` tuples. A store that
  cannot list raises `TableListingNotSupportedError`: side-effecting nodes then warn and
  proceed (developer, November 11, 2026). Operator caches and cached sources have no stable
  name; `PipelineJob.run()` reports one total for them. Every database class implements
  `list_table_paths`.
- A released data context manifest (`contexts/data/<version>.json`) is frozen: its sha256 is
  pinned in `contexts/released_manifests.json`. A change that moves an existing golden hash
  value for a reason other than a logical type's own change needs a new data context version
  with its own manifest file, never an edit of a released one.
- In memory the data context column is still `_context_key` (`ColumnConfig(context=True)`).
- A writer refuses an input of another data context than its own
  (`contexts/stored.require_same_data_context`, `InputValidationError` naming both keys)
  before anything is written; a whole-input condition is checked outside the row error
  policy. The eventual behaviour is ITL-951.
- `std:v0.1:default` is not shipped; saved files that name it load through
  `contexts.resolve_saved_context`. There is no `SchemaVersionError`, `ignore_schema` or
  `orcapod.migrations`: do not reintroduce a per-layout migration path without a decision.
- Logical types and the value handlers that hash their Python values are registered in
  `contexts/logical_types.json`, never in a data context manifest: a logical type is
  versioned by its own identifier, and that file is not frozen.

### System tag evolution rules

1. **Name-preserving** — single-stream ops (filter, select, map). Column name and value pass
   through unchanged.
2. **Name-extending** — multi-input ops (join, merge join). Each input's system tag column
   name gets `::{pipeline_hash}:{canonical_position}` appended. Commutative operators
   canonically order inputs by `pipeline_hash` and sort system tag values per row.
3. **Reducing** — many→one ops (`Batch`, `GroupBy`). User tag and data columns become
   `list[T]` and stay in their own group (a tag stays a tag; only the scalar keys of a
   `GroupBy` are not list-wrapped), and source-info columns become `list[str]` with one element
   per member. System
   tag columns must stay **scalar**, because `_build_record_id_preimage`
   (`core/nodes/function_node.py`) hashes them directly to derive record identity. They fold
   to a deterministic digest via `arrow_utils.fold_system_tag_values` and their column name
   gains `::{pipeline_hash}` — a blend of rules 2 and 3.

   The fold is SHA-based (`uuid5` for `record_id`, SHA-256 of the canonical JSON member list
   for `source_id`) and so is stable across processes. Never use `hash()` or a set-based construction there: the digest
   becomes a cache key, so a per-process value would look correct in a single-process test and
   miss the cache on every new driver run.

### Schema types and ColumnConfig

`Schema` (`types.py`) — immutable `Mapping[str, DataType]` with `optional_fields` support.
`output_schema()` always returns `(tag_schema, data_schema)` as a tuple of Schemas.

`ColumnConfig` (`types.py`) — frozen dataclass controlling which column groups are included.
Fields: `meta`, `context`, `source`, `system_tags`, `content_hash`, `sort_by_tags`.
Normalize via `ColumnConfig.handle_config(columns, all_info)` at the top of `output_schema()`
and `as_table()` methods. `all_info=True` sets everything to True.

### Key patterns

- **`LazyModule("pyarrow")`** — deferred import for heavy deps (pyarrow, polars). Used in
  `if TYPE_CHECKING:` / `else:` blocks at module level.
- **Argument symmetry** — each operator declares `argument_symmetry(streams)` returning
  `frozenset` (commutative) or `tuple` (ordered). Determines how upstream hashes combine.
- **`StaticOutputPod.process()` → `DynamicPodStream`** — wraps `static_process()` output
  with timestamp-based staleness detection and automatic recomputation.
- **Source delegation** — CSVSource, DictSource, etc. all create an internal
  `ArrowTableSource` and delegate every method to it.

### Arrow types

Governing principle (developer, November 9, 2026): minimize convenient inference and hold to
complete predictability, even when that is more tedious for the user; explicit type conversion
is always preferred over implicit coercion. So an operator never widens, promotes or picks a
side when its inputs' types disagree; a result's type never depends on data values, argument
order or the path the data took; and inferring a type from Python values is a last resort for
when no type is known.

Respect the Arrow types the inputs already have, normalized to their large variants (developer,
November 10, 2026: large and non-large variants hash identically, so always normalize to large
for consistency). Infer an Arrow type from Python values only when no Arrow type is known, and
then through the data context's type converter — never call `pa.array(values)` /
`pa.Table.from_pylist(rows)` without a type for a column whose input type is known, because
inference widens `int8` to `int64`, `float32` to `double`, and takes decimal precision from the
data. The rules:

- **Type strictness** (developer, November 10, 2026): Orcapod is type strict. It never
  expands or converts a type implicitly; the only exception is normalization to large variants.
  Inputs that disagree on a column's type are an error naming the column and both types, on
  every execution path (sync and async).
  Function-output rule (developer, November 12, 2026): the returned value must be an instance of the declared type, or an exact numeric equivalent; a subclass stands in for its base and is stored as the base.
  Structural classes additionally accept equivalent structural identities. Numeric
  narrowing and unrelated container types require explicit conversion. Base storage
  omits subclass-only fields, and those fields do not enter the stored row hash.

- Every output column takes the large-normalized form of its input type
  (`arrow_utils.normalize_to_large_types`: `string` to `large_string`, `binary` to `large_binary`, `list`
  to `large_list`, recursively). Width, nullability and logical (extension) types are never
  changed; `int8` stays `int8`, and `null` stays `null` (operators and the type converter pass
  `keep_null=True`; only `Data` maps `null` to `large_string`, the function's default). An
  operator that round-trips through Polars casts its columns back
  to the normalized input types (`arrow_utils.restore_column_types`, or
  `restore_schema_nullability` when nullability is restored too).
- List columns built by `Batch`, `GroupBy` and `MergeJoin` are typed by
  `arrow_utils.aggregated_list_type` (directly or through `build_aggregated_table`):
  `large_list<normalized T>`, or for a logical `T` the logical list type from the type converter
  (it raises, naming the column, if the converter has none; never strip the element to its
  storage type). Source-info list columns are `large_list` of the incoming source-info type:
  `large_list<large_string>` over scalar tokens, one list level deeper for each further
  aggregation (`Batch` or `GroupBy` after `MergeJoin`, `Batch` or `GroupBy` gives
  `large_list<large_list<large_string>>`, ITL-849). An already list-valued data column
  likewise becomes a list of its incoming list type; never flatten it. Zero-row and
  non-empty results have the same types.
- When two inputs give the same column different Arrow types, normalize both: equal proceeds,
  anything else raises `InputValidationError` naming the column and both types — no widening,
  promotion or picking a side. The rule is `arrow_utils.shared_arrow_type`, which takes only the
  two types and returns the shared large type or `None`; the error is raised by
  `core/operators/base.require_shared_arrow_type`, called by `MergeJoin` in
  `binary_static_process` and by `Join` (shared tag keys, sync and async) before the join.
  Keep error wording in the caller, not in the type rule. `output_schema()` exposes only Python
  types, so such disagreements are invisible at graph-build time.
- An operator with its own `async_execute` must yield exactly the rows of its sync path (types,
  nullability, column order, system tags, content hashes). Build output rows from the incoming
  datagrams' Arrow tables (`as_table()`), never from `as_dict()` values, and take the data
  context from those datagrams (`data_context_key`). Add new async operators to
  `tests/test_core/operators/test_async_sync_equivalence.py`.
- The type converter keys synthesized struct classes by the large-normalized struct, so
  `struct<b: string>` and `struct<b: large_string>` are one Python type (a workaround,
  `TODO(ITL-841)`).
- `normalize_to_large_types` changes layouts only: every nested field (list items, struct
  children, map keys and items) keeps its name, nullability and metadata, so it never changes
  a schema or value hash. Never rebuild a nested type as `pa.large_list(element_type)`, which
  drops the item's non-null flag.

### Important implementation details

- `ArrowTableSource.__init__` raises `ValueError` if any `tag_columns` are not in the table.
- A source id is the source's name (developer, November 11, 2026; ITL-957). A default id
  built from a hash is `<source kind>:<algorithm label>:<hex>` and is built only by
  `core/sources/source_ids.hash_source_id`: never format one by hand, and never write a label
  as a literal. The kind is the class's `SOURCE_KIND` (its config type key: `arrow_table`,
  `dict`, `list`, `data_frame`, `derived`), or names the operation that derives the source
  (`tags_as_data`). `node.as_source()` gives no id of its own; it takes the `DerivedSource`
  default. The origin's source id is never written into a default id, and a `source_id` given
  by the user is never changed. `SourceStreamBuilder.build` has no bare-hash default: it
  needs a `source_id` or a `source_kind`.
- `ArrowTableStream` requires at least one data column; raises `ValueError` otherwise.
- `FunctionNode.iter_data()` Phase 1 returns ALL records in the shared `pipeline_path`
  DB table (not filtered to current inputs). Phase 2 skips inputs whose hash is already
  in the DB.
- Empty data → `ArrowTableSource` raises `ValueError("Table is empty")`.
- `DerivedSource` over a job node before `run()` → presents zero rows with the origin's columns
  (no computed records; no error), in the plain form and with `tags_as_data`.
- Join requires non-overlapping data columns; raises `InputValidationError` on collision.
- MergeJoin requires colliding data columns to have identical Python types; merges into sorted
  `large_list<T>` with source columns reordered to match, where `T` is the large-normalized input
  Arrow type; inputs whose Arrow types still differ after normalization raise
  `InputValidationError`. Non-empty struct columns cannot be merged yet (ITL-839).
- Operators predict their output schema (including system tag column names) without
  performing the actual computation. Verify a prediction against
  `unary_static_process(stream).output_schema(...)`, **not** against `process(...)` (which
  delegates straight back to the pod, making the check circular) and **not** against
  `as_table()` (which legitimately differs: `ArrowTableStream.output_schema` ignores
  `columns.source`, so `_source_*`, `_content_hash`, and `_context_key` appear in the table
  but never in the schema).
- `DropNulls` keeps the rows where every named column (tag or data) is non-null and clears
  the nullable flag of each named field, so `T | None` becomes `T`. Only top-level validity is
  read (not list items, struct children or NaN). Its identity is the sorted tuple of unique
  names. Both paths apply one rule, `DropNulls._narrow_table`: an Arrow validity mask and
  the same arrays re-wrapped under the new field flags, with no Polars round-trip and no cast
  other than large normalization. A named column that is already non-nullable is a no-op.
- `DropNulls()` with no argument (`columns=None`) means every user tag and data column of
  the input (developer, November 12, 2026). The names are resolved from each input
  (`DropNulls._target_columns`: `stream.keys()`, or the row's tag and data keys on the
  async path) and never stored, so its identity is the fixed `("DropNulls", None)`,
  different from any list of names. System-tag, source-info, context and meta columns are
  never included. An explicitly empty collection is not "no argument": it raises `ValueError`.
- `GroupBy` requires every column in `by` to be a scalar tag column; it raises
  `InputValidationError` for unknown, data, or list-valued columns (the last usually means the
  stream came from `Batch` — group before batching, not after). Members are sorted by non-key
  tag values with the system `record_id` as tiebreaker, so the hashed lists are stable across
  runs. Groups themselves are emitted in key order, so a reordered input yields a
  byte-identical table.
- `GroupBy`'s output tag columns are its input's tag columns, in input order: keys scalar,
  every other tag `list[T]` (ITL-955). A grouped stream can be grouped again on a coarser key;
  its list-valued tags then nest (`list[list[T]]`) and still order the members, so the member
  sort key (`_null_last_key`) is recursive and sorts nulls last at every depth.
- `Data` source-info values may be `str`, `None`, or `list[...]`, nested to any depth; the
  Arrow and Python types are derived from the value, with `None` mapping to `large_string`.
  That rule has one definition, `arrow_utils.source_info_arrow_type`, and applies only to
  tokens supplied as Python values: `ArrowTableStream.iter_data` hands each row's source info
  to `Data` as Arrow columns, so a type the stream already has is never re-inferred. A
  many→one operator must emit a list for **every** row of a list-valued column — mixing a bare
  `None` into some rows makes per-row `as_table()` schemas diverge and `pa.concat_tables` fail
  on the barrier path.
- Aggregating operators (`Batch`, `GroupBy`) must build list-valued columns via
  `arrow_utils.build_aggregated_table`, never `pa.list_(field.type)`. The helper types every list
  column with `aggregated_list_type`: `large_list` of the large-normalized element, or, because
  Arrow cannot embed an extension type inside a list value field, the logical list type
  (`list[<element>]` from `ListLogicalType`) for a logical element such as a pod annotated
  `-> Path`; it builds that list over the element's storage and wraps it.
- Never build an identity or hash input from a type's raw `__module__`, or from
  `repr`/`str`/`inspect.formatannotation` of a type, without going through
  `utils/canonical_type_names.py` (`canonical_module_name`, `canonical_type_name`,
  `stabilize_type_names`). The standard library moves classes between modules across Python
  versions (`pathlib.Path.__module__` is `pathlib._local` on 3.13), so the raw value makes
  hashes differ by interpreter. Private stdlib submodules that already existed on 3.11/3.12
  are frozen in `_LEGACY_PRIVATE_STDLIB_MODULES`; never add a newer one there.
