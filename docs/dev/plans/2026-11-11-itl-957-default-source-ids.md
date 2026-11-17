# ITL-957 Default Source Ids Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking. A successor worker
> continues from the first unchecked step; "Progress notes" at the end is kept current.

**Goal:** Every default source id built from a hash has the form
`<source kind>:<algorithm label>:<hex>`; `node.as_source()` uses the `DerivedSource`
default `derived:<label>:<hex>`; a source id given by the user is never changed.

**Architecture:** One function defines the format (`hash_source_id` in
`core/sources/source_ids.py`). Each in-memory source class declares its kind once
(`SOURCE_KIND`, the config type key) and uses it in `to_config`, in the source registry and
as the kind it passes to `SourceStreamBuilder.build`. The builder no longer has a bare-hash
default: with no `source_id` it needs a `source_kind`. `as_source()` stops building an id.

**Tech Stack:** Python, pyarrow, pytest, uv, ruff, basedpyright.

Issue: https://linear.app/metamorphic/issue/ITL-957
Issue branch (Step 2, not yet created): `eywalker/itl-957-default-source-ids-built-from-a-hash-carry`,
from `origin/eywalker/itl-967-v03-hash-step-combine-the-hash-changing` once the coordinator
says the six merges are in. Step 1 work (this plan and the failing tests) is committed on the
local scratch branch `itl-957-step1-wip` (at `c1949b24` + 1) and is cherry-picked onto the
issue branch.

---

## Findings (Step 1, measured on `origin/…itl-967…` at `c1949b24`: #309, #313, #318 merged)

### Today's defaults

| Source | Default `source_id` today | Where |
|---|---|---|
| `ArrowTableSource`, `DictSource`, `ListSource`, `DataFrameSource` | bare hex of the Arrow table hash, `table_hash.to_hex(char_count=config.hashing.path_n_char)`. `path_n_char` defaults to `None`, so the full 70 hex characters (`000002…`, starfix format prefix + SHA-256). No kind, no label | `stream_builder.py:249`, the only reader of `path_n_char` in `src/` |
| plain `DerivedSource(node)` | `derived:<label>:<hex>` of the node's content hash | `derived_source.py:51` |
| `FunctionJobNode.as_source()`, `OperatorJobNode.as_source()` | `<node path joined by "/">:<label>:<hex>` | `function_node.py:2181`, `operator_node.py:1269` |
| `DerivedSource(origin, tags_as_data=…)` (PR #317, not on the combine branch) | `tags_as_data:<label>:<hex>` | consistent already; not touched |
| `PollingSource` | `str(impl.identity())`, the Python `repr` of whatever the implementation returns, for example `"('MyDBSource', 'postgresql://…')"` | `polling_source.py:274` |
| CSV, Delta, DB table, SpiralDB, cached | a path, table name or the wrapped source's id: not a hash, out of scope | — |

Only the four in-memory classes reach the builder's default: every other caller of
`SourceStreamBuilder.build` passes a `source_id`.

### Truncation

`path_n_char` truncates today's default. New form with the setting: the kind and the label
are never truncated; only the hex is (`hash.to_string(hexdigits=path_n_char)`), for example
`dict:arrow_v0.2:000002e12eaf`. **Recommended:** keep honouring it this way. It is the
setting's only use, the default (`None`) gives the full hex, and dropping it would leave a
public config field with no effect. The alternative (never truncate, as the derived ids,
and remove the field) is a public config change; raised with the coordinator.

### PollingSource

Not built from a hash, so outside rule 5a as the issue scopes it ("sources whose default id
is not a hash"). Recommended: no change in this PR. Reported as out-of-scope findings:
the `repr` can contain `::` (an IPv6 URL), can put a DSN with credentials into every stored
system tag and source-info value, and is not a stable format across Python versions for
every value type.

### Separator safety

Source-info values are `<source_id>::<row token>::<column>` and record ids hash
`<source_id>::<row token>`. A default id has exactly two single `:`; `hash_source_id`
raises `ValueError` if the kind or the label is empty or contains `:`, and the hex cannot.
So a default id never contains `::`. Nothing in `src/` parses a source id or a source-info
value back (the `::` splitting in `join.py` and `arrow_utils.py` is on column names).

### What moves in stored values

In-memory source with **no** `source_id` given:

| Value | Moves |
|---|---|
| `source_id`; the job slot name of the source (`SourceNode` name, saved job file) | yes |
| `_tag_source_id::…` values | yes |
| record ids (`uuid5(<source_id>::<row token>)`) | yes |
| `_source_<col>` values | yes |
| source `content_hash` | yes for `ArrowTableSource`, `DictSource`, `DataFrameSource`; **no for `ListSource`** (its identity is name, elements and tag function, not the source id) |
| downstream node content hashes, `__node_content_hash`, pipeline entry ids (record-id preimages) | yes |
| `pipeline_hash`, schema hash, system-tag column names, data hashes, table hash | no |
| function pipeline / result table paths (pipeline-hash scoped) | no. Result rows are keyed by input data hash, so nothing is recomputed |
| paths that hold a content hash: `instance:<content digest>` (`table_scope="content_hash"`) and the default cached-source path `source/node:<content digest>` | yes, indirectly through the source's content hash. No path uses the source id itself |

`as_source()`: only the name moves (`.source_id`, the job slot name). The plain derived
source's rows are the node's stored records and its content hash is the node's, so no
system tag, record id, source-info value, hash or path moves.

A source with a given `source_id`: nothing moves.

### Places that pin a default id (prototype run: 8329 tests, 13 fail because of the change)

- Golden `schema.json`, case `source_stream_builder.default_source_id`: 8 entries move
  (`.source_id`, `.content_hash`, three `_tag_source_id` values, three `_tag_record_id`
  values). `test_values_independent_of_process_and_hash_seed[0]` and `[4242]` follow.
- `tests/test_core/operators/test_materialize_source_info.py::TestIdentityPinned::test_operator_node_identity_and_row_zero_record_hash`
  (node content hash and record hash over a default-id source).
- `tests/test_core/operators/test_group_by.py::TestGroupByOrdering::test_record_id_breaks_ties_between_duplicate_tags`
  (member order follows record ids; `test_falls_back_to_record_id_…` in the same class
  pins an order too and happened not to move).
- `tests/test_core/sources/test_derived_source.py::TestDerivedSourceId::test_source_id_contains_pipeline_path`.
- After #314 merges: `tests/test_hashing/test_hash_label_rule.py::test_default_derived_source_ids_keep_the_hash_label`
  pins `<node path>:<label>:<hex>`.
- Direct `builder.build(...)` calls with no `source_id` in tests and in
  `tests/test_hashing/golden/cases.py` need a `source_kind`.
- Docs: `docs/reference/hashing.md` (site 4 rows, the `RootSource` row, and on #314 the
  "bare digest in a name" and "labelled hash inside a source id" rows),
  `docs/concepts/sources.md:246`, `CLAUDE.md` / `.zed/rules` ("Two identity chains"),
  docstrings in `stream_builder.py`, `arrow_table_source.py`, `derived_source.py`.
  No executed doc sample prints a default id.

### Pre-existing failures on the combine branch at `c1949b24` (not caused by this change)

- `tests/test_core/sources/test_stream_builder.py::TestUnhashableColumnTypes` (3 tests):
  the hasher now raises `ValueError`, the builder catches `NotImplementedError`.
- `tests/test_core/sources/test_spiraldb_table_source_fake.py::TestSourceId::test_unrecorded_table_hashes_are_unchanged`:
  pins an `arrow_v0.1` value.

### Merge points

- **#317 (ITL-939)** is not one of the six merges. It adds `DerivedSource.SOURCE_KIND = "derived"`
  (same name and value as here), `TAGS_AS_DATA_KIND`, and `as_source(tags_as_data=…)` that
  still builds `<node path>:…` for the plain form. Whichever merges second: keep one
  `SOURCE_KIND`, keep `source_id=None` in `as_source()`, and `_default_source_id` may call
  `hash_source_id`.
- A later label rename moves default ids (accepted, rule 5a).

---

## File structure

| File | Change |
|---|---|
| `src/orcapod/core/sources/source_ids.py` (new) | `hash_source_id(kind, content_hash, hexdigits=None)`: the single definition of the format |
| `src/orcapod/core/sources/stream_builder.py` | `build(..., source_kind=None)`; default id through `hash_source_id`; `ValueError` when neither id nor kind is given |
| `src/orcapod/core/sources/{arrow_table,dict,list,data_frame}_source.py` | `SOURCE_KIND: ClassVar[str]`; `to_config` uses it; pass `source_kind=self.SOURCE_KIND` |
| `src/orcapod/core/sources/derived_source.py` | `SOURCE_KIND = "derived"`; default through `hash_source_id` |
| `src/orcapod/core/nodes/function_node.py`, `operator_node.py` | `as_source()` passes no `source_id` |
| `src/orcapod/pipeline/serialization.py` | registry keys of the four classes come from `SOURCE_KIND` |
| `tests/test_core/sources/test_default_source_ids.py` (new, written) | the behaviour, public interface |
| `tests/test_core/sources/test_source_ids.py` (new) | `hash_source_id` unit tests |
| pinned tests, golden case, docs, `changelog.d/ITL-957.md`, `design_issues/ITL-957-SRC1.md` | as listed above |

---

## Tasks

### Task 0: Step 1 (done except the reply)

- [x] Read ITL-957, design 5.6 rule 5a / 7.2 row 6b / Part 8 item 15, PR #317, commit `40a71022`.
- [x] Prototype run to find the pins (prototype reverted).
- [x] Write `tests/test_core/sources/test_default_source_ids.py`; on the unmodified combine
      branch 23 of its tests fail (no `SOURCE_KIND`, bare hex default, node path in the
      `as_source()` id) and the given-id, determinism, separator and system-tag tests pass.
- [x] Send the plan through `ask`; wait. Do not create the issue branch before the
      coordinator says the six merges are in.

### Task 1: Branch

- [x] `git fetch origin && git checkout -b eywalker/itl-957-default-source-ids-built-from-a-hash-carry origin/eywalker/itl-967-v03-hash-step-combine-the-hash-changing`
- [x] `git cherry-pick itl-957-step1-wip` (plan + failing tests); push.
- [x] `uv run pytest tests/ -q` on the base; record pre-existing failures by name.
- [x] `uv run python tests/test_hashing/golden/generate_golden_hashes.py` must leave
      `git status` clean on the base. If not, stop and `ask`.

### Task 2: `hash_source_id`

**Files:** create `src/orcapod/core/sources/source_ids.py`, `tests/test_core/sources/test_source_ids.py`.

- [x] Failing test:

```python
"""``hash_source_id``: the one definition of a hash-based default source id (ITL-957)."""

import pytest

from orcapod.core.sources.source_ids import hash_source_id
from orcapod.types import ContentHash

HASH = ContentHash("algo_v1", bytes.fromhex("00ab" * 16))


def test_format_is_kind_label_hex():
    assert hash_source_id("dict", HASH) == f"dict:{HASH.to_string()}"
    assert hash_source_id("dict", HASH).split(":") == ["dict", "algo_v1", "00ab" * 16]


def test_hexdigits_truncates_the_hex_only():
    assert hash_source_id("dict", HASH, hexdigits=6) == "dict:algo_v1:00ab00"


@pytest.mark.parametrize("kind", ["", "a:b"])
def test_kind_must_be_non_empty_without_a_colon(kind):
    with pytest.raises(ValueError, match="kind"):
        hash_source_id(kind, HASH)


@pytest.mark.parametrize("label", ["", "a:b"])
def test_label_must_be_non_empty_without_a_colon(label):
    with pytest.raises(ValueError, match="label"):
        hash_source_id("dict", ContentHash(label, b"\x00" * 32))
```

- [x] Run `uv run pytest tests/test_core/sources/test_source_ids.py -q`: fails, module missing.
- [x] Implement:

```python
"""The form of a default source id that is built from a hash."""

from __future__ import annotations

from orcapod.types import ContentHash


def hash_source_id(
    kind: str, content_hash: ContentHash, hexdigits: int | None = None
) -> str:
    """Return ``<kind>:<algorithm label>:<hex>``, a default source id built from a hash.

    A source id is the source's name. The kind says what sort of source it is, and
    the label and hex are the hash in its labelled form (``ContentHash.to_string``).
    The parts are joined by a single ``:`` and none contains one, so the id never
    contains the ``::`` that separates the parts of a source-info value.

    Args:
        kind: The source kind, for example ``dict`` or ``derived``.
        content_hash: The hash the id is built from.
        hexdigits: If given, keep only this many hex characters of the digest.

    Returns:
        The source id.

    Raises:
        ValueError: If the kind or the hash's label is empty or contains ``:``.
    """
    if not kind or ":" in kind:
        raise ValueError(f"source kind must be non-empty without ':', got {kind!r}")
    if not content_hash.method or ":" in content_hash.method:
        raise ValueError(
            f"hash label must be non-empty without ':', got {content_hash.method!r}"
        )
    return f"{kind}:{content_hash.to_string(hexdigits=hexdigits)}"
```

- [x] Tests pass; `git commit -m "feat(sources): hash_source_id, the form of a hash-based default source id (ITL-957)"`.

### Task 3: In-memory sources and the builder

**Files:** `stream_builder.py`, the four source modules, `serialization.py`, `tests/test_core/sources/test_stream_builder.py`.

- [x] Add to `test_stream_builder.py` (the builder is the unit under test there):

```python
def test_build_without_source_id_or_kind_raises(builder, simple_table):
    with pytest.raises(ValueError, match="source_id or a source_kind"):
        builder.build(simple_table, tag_columns=["id"])
```

      (use that file's existing builder and table fixtures; rename to match them.)
- [x] `stream_builder.py`: add the keyword `source_kind: str | None = None` after
      `source_id`; replace step 6 with

```python
        # 6. Default source_id: the source kind and the labelled table hash.
        if source_id is None:
            if source_kind is None:
                raise ValueError(
                    "build() needs a source_id or a source_kind to make a default "
                    "source id from."
                )
            source_id = hash_source_id(
                source_kind, table_hash, hexdigits=self._config.hashing.path_n_char
            )
```

      and the docstring: `source_id: Canonical source name. When not given, the default is
      ``<source_kind>:<algorithm label>:<hex>`` of the table hash, which changes when rows
      are reordered; …` / `source_kind: Kind prefix of the default source id. Required
      when ``source_id`` is not given.`
- [x] Each of the four classes: `SOURCE_KIND: ClassVar[str] = "arrow_table"` (`"dict"`,
      `"list"`, `"data_frame"`) with the comment `# The config type key; also the kind
      prefix of the default source id.`; `to_config` returns `"source_type": self.SOURCE_KIND`;
      the `builder.build(...)` call gains `source_kind=self.SOURCE_KIND`.
- [x] `serialization._build_source_registry`: the four entries become
      `DictSource.SOURCE_KIND: DictSource`, and so on.
- [x] Give `source_kind="arrow_table"` to the other direct `builder.build` calls that pass
      no `source_id` (`tests/test_core/conftest.py`, `test_streams.py`,
      `test_lazy_conversion.py`, `test_source_builder_integration.py`, `test_stream_builder.py`,
      `test_polling_source.py`: check each with `grep -n "\.build(" `).
- [x] `uv run pytest tests/test_core/sources -q`: the in-memory classes of
      `test_default_source_ids.py` pass. Commit
      `feat(sources)!: in-memory default source ids are <kind>:<label>:<hex> (ITL-957)`.

### Task 4: `derived` for `as_source()`

**Files:** `derived_source.py`, `function_node.py`, `operator_node.py`.

- [x] `DerivedSource`: add `SOURCE_KIND: ClassVar[str] = "derived"`; default becomes
      `source_id = hash_source_id(self.SOURCE_KIND, origin.content_hash())`.
- [x] Both `as_source()` methods: delete the `path_str` / `source_id` lines and the
      `source_id=source_id` argument.
- [x] `test_derived_source.py`: replace `test_source_id_contains_pipeline_path` with
      `test_source_id_has_no_pipeline_path` (`assert "/" not in src.source_id`).
      After #314: update `test_default_derived_source_ids_keep_the_hash_label` so
      `from_node.source_id == f"derived:{labelled}"`.
- [x] `uv run pytest tests/test_core/sources tests/test_core/operators tests/test_hashing/test_hash_label_rule.py -q`.
      Commit `feat(nodes)!: as_source() uses the derived:<label>:<hex> default id (ITL-957)`.

### Task 5: Pinned tests and golden files

- [x] `test_materialize_source_info.py::TestIdentityPinned` and
      `test_group_by.py::test_record_id_breaks_ties_between_duplicate_tags`: re-record the
      values that move, with a line in each docstring naming ITL-957 as the cause.
- [x] `tests/test_hashing/golden/cases.py`: `"default_source_id": {"source_kind": "arrow_table"}`.
      Add a case `schema` / `default_source_ids` that records `source_id` and
      `content_hash().to_string()` of an `ArrowTableSource`, `DictSource`, `ListSource` and
      `DataFrameSource` built with no `source_id`, and the `source_id` of
      `FunctionJobNode.as_source()` and `OperatorJobNode.as_source()` (new entries).
- [x] Regenerate: `uv run python tests/test_hashing/golden/generate_golden_hashes.py`.
      `git diff --stat tests/test_hashing/golden/data` must show only: the 8 changed
      entries of `source_stream_builder.default_source_id`, plus added lines of the new
      case. Anything else: stop and `ask`. Write the list into "Progress notes".
- [x] Commit `test(golden): default source ids (ITL-957)`.

### Task 6: Docs, changelog, design issue

- [x] `docs/reference/hashing.md`: site 4 (summary row and section) gives the format, the
      label and the truncation rule; the `RootSource` row; on #314's rows, move "the default
      source id built from a table hash" from "bare digest in a name" to "labelled hash
      inside a source id" and change `<node path>:<label>:<hex>` to `derived:<label>:<hex>`.
- [x] `docs/concepts/sources.md` near line 246: one paragraph on the default id with an
      example (`dict:arrow_…:…`), no executed sample printing a hash.
- [x] `CLAUDE.md` and `.zed/rules`, "Two identity chains": "the default `source_id` of the
      in-memory sources (`<source kind>:<algorithm label>:<hex>` of the table hash, …)", and a
      bullet under "Important implementation details": a hash-based default id is built only
      by `hash_source_id`; a new source class declares `SOURCE_KIND`.
- [x] Docstrings: `ArrowTableSource`, `DerivedSource`, `as_source()`.
- [x] `changelog.d/ITL-957.md`: `### Breaking changes and migration` (default ids of the four
      in-memory sources and of `as_source()`; what moves; pass an explicit `source_id` to keep
      a name) and `### Changed` (`SOURCE_KIND`).
- [x] `design_issues/ITL-957-SRC1.md`: `## Sources`, `### SRC1 — Default source ids built
      from a hash had no kind or label`, **Status:** resolved, **Fix:** note.
- [x] `uv run pytest tests/test_docs tests/test_changelog_fragments.py -q`; commit `docs: …`.

### Task 7: Verify and deliver

- [x] `uv run ruff format . && uv run --with ruff ruff check . && uv run --with basedpyright basedpyright` (exit code 0) `&& uv run pytest tests/`. (basedpyright: 2 errors, both in the combine branch's `tests/test_pipeline/test_hash_step_combined.py`, none from this change.)
- [x] sensei:mid-development retrospective; sensei:verification-before-completion.
- [x] Merge current `origin/…itl-967…` (and `origin/main` if it moved), rerun the suite.
- [ ] Push; open the PR against `main` with sensei:create-pr: "Stacked on the ITL-967 pull
      request"; `Fixes ITL-957`; the list of golden entries moved and added.
      `orca linear attach`, `orca worktree set --worktree current --pr <n>`.
- [ ] Readiness: CI green; CodeRabbit review covers the latest head (inline threads and
      review bodies), one push per round.
- [ ] `worker_done` with the PR URL and the out-of-scope findings (PollingSource default).

---

## Progress notes

- 2026-11-11: Step 1 done up to the `ask`. Heartbeat loop runs in the background.
- 2026-11-12: Coordinator approved A1 (keep `path_n_char`, hex only), B1 (PollingSource
  unchanged), C (new golden case), and the design. Issue branch created from the combine
  branch at `6ba6cc33`; combine branch merged again at `b30b6fa3` and at `8c2066ac` (its
  golden regeneration).
- Deviations from the tasks above: the new golden case is `pipeline` / `default_source_ids`
  (not `schema`); the design-issue file is `design_issues/ITL-957-S2.md`; `test_group_by`'s
  moved test is `test_falls_back_to_record_id_when_key_covers_all_tags` (under the `_v1`
  labels the other one keeps its order).
- Found while implementing: #314's
  `TestRenamingALabelChangesNoDigest::test_every_pinned_value_keeps_its_digest` flagged the
  default-id golden entries (content hash and record ids of a source with no given id
  depend on the label, through the name). That is rule 5a's accepted consequence; the test
  now allows exactly those entries (`DEFAULT_ID_CASES`) and nothing else.
- Golden regeneration on top of `8c2066ac`: 8 entries changed (schema
  `source_stream_builder.default_source_id`: `.source_id`, `.content_hash`, three
  `_tag_source_id` values, three `_tag_record_id` values), 10 added (pipeline
  `default_source_ids.*`). Nothing else.
- Full suite at that point: 8583 passed, 101 skipped, 18 xfailed, 0 failed.
- Remaining: PR, CI, CodeRabbit rounds, `worker_done`.
