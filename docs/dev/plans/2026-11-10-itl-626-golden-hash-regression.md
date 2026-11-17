# ITL-626 Golden Hash Regression Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pin every hash value Orcapod persists or embeds in names (semantic hashes, schema
hashes, system-tag column names, Arrow table/schema hashes, per-row data hashes, record ids,
stream/node content and pipeline hashes, database table paths) in committed JSON golden files,
so any unintended change fails CI with a message naming the entry that moved.

**Architecture:** One case catalogue module (`tests/test_hashing/golden/cases.py`) computes
every pinned value as a flat `{entry_name: json_value}` map per group. One generator script
(`tests/test_hashing/golden/generate_golden_hashes.py`) writes those maps to
`tests/test_hashing/golden/data/<group>.json`. One test module
(`tests/test_hashing/golden/test_golden_hashes.py`) recomputes the maps and compares entry by
entry (one parametrized test per entry), checks the entry sets match, checks the invariants
the golden values encode (large == non-large, int8 != int64, nullable != non-nullable,
round-trip equality), and recomputes everything in subprocesses with different
`PYTHONHASHSEED` values. The same generator can be pointed at an older checkout
(`--output-dir`, `--allow-errors`) to produce the release-note diff against v0.1.4 and the
v0.2.0 candidate.

**Tech Stack:** pytest, pyarrow, numpy, pandas, pydantic, orcapod public API
(`orcapod.sources`, `orcapod.operators`, `function_pod`, `PipelineJob`,
`orcapod.databases.InMemoryArrowDatabase`, `orcapod.contexts.get_default_context`).

---

## Decisions

- **Module path is part of identity.** Function pods, dataclasses and pydantic models hash
  with their defining module. All of them live in `tests.test_hashing.golden.cases`; the
  generator puts the project root on `sys.path` and imports that same dotted name, so pytest
  and the script produce identical values. Never move these definitions.
- **Flat entries.** Each JSON file is a flat, sorted `{name: value}` object; names are dotted
  (`pipeline.join_pod.pod.pipeline_hash`). Values are strings, or lists of strings for
  column-name lists. Bytes are hex. A failure names the dotted entry.
- **Volatile by design, not pinned:** result-database record ids and the per-row token inside
  function-pod output source info are UUIDv7 (time based); `__pod_ts`, git hash, Python
  version columns. Pinned instead: the result DB's `__input_data_hash` (its lookup key) and the
  pipeline DB's record ids, which are deterministic.
- **Known hasher gaps (ITL-848) are excluded**, not pinned as errors: `hash_schema` on a schema
  with an extension type, `null`-typed columns, `arrow.uuid`, map and fixed-size-list values.
  The generator header lists them so they are added when ITL-848 lands.
- **Lazy imports inside case builders** for anything absent from older releases (e.g.
  `GroupBy`, `orcapod.logical_types`), so the generator can run against v0.1.4 with
  `--allow-errors` and record `"ERROR: <type>: <msg>"` for cases the old API cannot express.
- No change under `src/`.

## File Structure

- Create `tests/test_hashing/golden/__init__.py` — package marker.
- Create `tests/test_hashing/golden/cases.py` — case registry plus all cases, grouped:
  - `semantic`: `BaseSemanticHasher` (default context) on scalars, collections, `Schema`,
    type objects, UUID, `numpy.ndarray`, `pandas.DataFrame`.
  - `schema`: `Schema` hashes over scalar / nested / optional / logical types (Path, UUID,
    ndarray, DataFrame, dataclass, pydantic, `list[Path]`, `set[UUID]`); Arrow type string of
    each Python type via the type converter; `compute_schema_hash` (full and truncated),
    `compute_source_schema_hash`, `system_tag_column_names`; `SourceStreamBuilder.build()`
    `schema_hash` / `system_tag_columns` / `table_hash` / record ids.
  - `arrow`: `StarfixArrowHasher` `hash_table` / `hash_schema` over plain numeric, string, bool,
    null-containing, nested list/struct, large vs non-large layouts, int8/16/32/64, nullable
    vs non-nullable, column order, row order, `orcapod.path` / `orcapod.uuid` extension
    columns, and an extension type with no registered handler (passthrough).
  - `datagram`: public `DictSource` streams carrying each logical type — stream
    `content_hash` / `pipeline_hash`, system-tag column names, per-row record ids and data
    content hashes.
  - `pipeline`: the end-to-end pipelines listed in the task, plus one `PipelineJob` on
    `InMemoryArrowDatabase`.
- Create `tests/test_hashing/golden/generate_golden_hashes.py` — header documents when
  regeneration is legitimate; CLI `--output-dir`, `--allow-errors`, `--group`, `--stdout`.
- Create `tests/test_hashing/golden/data/{semantic,schema,arrow,datagram,pipeline}.json`.
- Create `tests/test_hashing/golden/test_golden_hashes.py`.
- Modify `DESIGN_ISSUES.md` — append one ITL-626 entry.

## Core code

`cases.py` registry:

```python
CaseFn = Callable[[], dict[str, Any]]
_CASES: dict[str, list[tuple[str, CaseFn]]] = {}

def case(group: str, name: str) -> Callable[[CaseFn], CaseFn]:
    def register(fn: CaseFn) -> CaseFn:
        _CASES.setdefault(group, []).append((name, fn))
        return fn
    return register

def compute_group(group: str, *, allow_errors: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, fn in _CASES[group]:
        try:
            values = fn()
        except Exception as exc:
            if not allow_errors:
                raise
            out[name] = f"ERROR: {type(exc).__name__}: {exc}"
            continue
        for key, value in values.items():
            full = f"{name}.{key}" if key else name
            assert full not in out, f"duplicate golden entry {full}"
            out[full] = value
    return dict(sorted(out.items()))
```

Stream summary helper used by `datagram` and `pipeline` cases (public interface only):

```python
def stream_summary(stream) -> dict[str, Any]:
    tag_keys, _ = stream.keys()
    sys_cols = [k for k in stream.keys(system_tags=True)[0] if k not in tag_keys]
    out = {"content_hash": stream.content_hash().to_string(),
           "pipeline_hash": stream.pipeline_hash().to_string(),
           "system_tag_columns": sys_cols}
    for tag, data in stream.iter_data():
        row = _row_key(tag.as_dict())
        tag_all = tag.as_dict(all_info=True)
        for col in sys_cols:
            out[f"row[{row}].{col}"] = _jsonable(tag_all[col])
        out[f"row[{row}].data_hash"] = data.content_hash().to_string()
    return out
```

Test module core:

```python
@pytest.mark.parametrize(("group", "entry"), ALL_ENTRIES, ids=...)
def test_golden_entry(group, entry, live):
    expected = GOLDEN[group][entry]
    actual = live[group].get(entry, "<missing from live output>")
    assert actual == expected, (
        f"Golden hash entry {group}:{entry} moved.\n  expected: {expected}\n"
        f"  actual:   {actual}\nThis is a BREAKING change ... see generate_golden_hashes.py")
```

## Tasks

### Task 1: Scaffolding and the failing test
- [ ] Create package, registry, generator, test module with one trivial `semantic` case and
      no data file. Run `uv run pytest tests/test_hashing/golden -q`: FAILS (missing golden
      file). Generate; rerun: PASS. Commit `test(hashing): golden hash regression scaffolding (ITL-626)`.

### Task 2: semantic + schema + arrow groups
- [ ] Add the cases listed above, generate, run the suite and the invariant tests. Verify
      `PYTHONHASHSEED=0` and `=12345` subprocess test passes. Commit.

### Task 3: datagram + pipeline groups
- [ ] Add the logical-type streams and the pipelines: source→pod; source→Join→pod; MergeJoin;
      SemiJoin; Batch; GroupBy; Index/Pick; MapData; Select/Drop columns; PolarsFilter with
      keyword constraints; PipelineJob on `InMemoryArrowDatabase` (node hashes, `list_sources()`
      table paths, pipeline-DB record ids, result-DB `__input_data_hash`). Generate; run twice
      in separate processes and diff. Commit.

### Task 4: Cross-interpreter check
- [ ] Run the generator with `--stdout` under Python 3.11, 3.12, 3.13 (separate
      `UV_PROJECT_ENVIRONMENT`s) and diff against the committed files. Any difference → `ask`
      the coordinator; do not pin per-version values.

### Task 5: Release comparisons (report only, nothing committed)
- [ ] Scratch worktrees at PR #280 head (`e0f7fdb3`) and `v0.1.4`; run this generator with
      `--allow-errors --output-dir <scratch>` against each; diff with the golden files; explain
      each difference from the git history.

### Task 6: Docs, DESIGN_ISSUES, full suite, PR
- [ ] Append the ITL-626 entry to `DESIGN_ISSUES.md`. Ruff, format, basedpyright, full
      `uv run pytest tests/`. Merge origin/main, push, open PR (`Fixes ITL-626`), link, report.
