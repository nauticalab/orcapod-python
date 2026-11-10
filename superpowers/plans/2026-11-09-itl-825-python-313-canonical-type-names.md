# ITL-825 Python 3.13 Canonical Type Names Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Canonical type names and every hash derived from them are identical on Python 3.11, 3.12 and 3.13, unchanged on 3.11/3.12, and the repo officially supports 3.13.

**Architecture:** One dependency-free helper module (`orcapod/utils/canonical_type_names.py`) resolves a stdlib type's *defining* module to the *public* module it is exported from, when the defining module is a private submodule (`pathlib._local` → `pathlib`). Every identity-string site that prints a type routes through it. A frozen set of stdlib private submodules that were already the defining module on 3.11/3.12 is exempt, so no string changes on those versions.

**Tech Stack:** Python 3.11–3.13, uv, pytest, GitHub Actions.

---

## Evidence (established before planning, on main @ 07e5a2ea)

Stdlib survey (all importable stdlib modules, 3.11 / 3.12 / 3.13, plus 3.14 for a forward look):

| Type | 3.11 | 3.12 | 3.13 |
|---|---|---|---|
| `pathlib.{Path,PosixPath,WindowsPath,PurePath,PurePosixPath,PureWindowsPath}` | `pathlib.X` | `pathlib.X` | `pathlib._local.X` |
| `zipfile.Path`, `zipfile.CompleteDirs` | `zipfile.X` | `zipfile._path.X` | `zipfile._path.X` |
| `re.error` (qualname rename) | `re.error` | `re.error` | `re.PatternError` |
| `signal.ItimerError` (qualname rename) | `signal.itimer_error` | `signal.itimer_error` | `signal.ItimerError` |

Everything else users plausibly annotate with (`datetime.*`, `decimal.Decimal`, `uuid.UUID`,
`fractions.Fraction`, `ipaddress.*`, `re.Pattern`/`re.Match`, `enum.*`, `collections.abc.*`,
typing constructs) has the same module, qualname and `inspect.formatannotation` output on all three.

Stdlib private submodules whose classes a public ancestor re-exports:

- 3.11 ∪ 3.12 (frozen exemption list — strings must not change): `concurrent.futures._base`,
  `ctypes._endian`, `importlib.metadata._collections`, `importlib.metadata._meta`,
  `xml.sax._exceptions`, `zipfile._path`, `zoneinfo._common`, `zoneinfo._tzpath`.
- New in 3.13: `pathlib._local`, `pathlib._abc` (only `UnsupportedOperation`, new in 3.13).
- New in 3.14: `pathlib._os`, `concurrent.interpreters._queues`, `compression.zstd._zstdfile`
  (all new types; the rule gives them their public names).

Affected Orcapod code paths (probe on a default-configured context):

- `PythonDataFunction._function_signature_hash` — `hash_utils.get_function_signature` uses
  `str(param)`, `canonical_annotation_str(annotation)` **without** a type converter, and
  `f"-> {ret}"`. Differs on 3.13 for any `Path` annotation.
- `PythonDataFunction._function_content_hash` — `hash_utils.get_function_components` emits
  `annotations:{k}:{v}` using `str(type)`. Differs on 3.13.
  Both are stored as function-variation columns of the result cache.
- `TypeObjectHandler` (`type:<module>.<qualname>`) and `canonical_annotation_str` fallback —
  differ on 3.13 for *unregistered* pathlib types (`PurePath` family) and when no type
  converter is supplied. `pathlib.Path` itself resolves to `orcapod.path` first.
- NOT affected: `uri`, `content_hash`, `pipeline_hash`, schema hashes, record ids of a pod
  annotated with `Path` (they go through the registered logical type).

Out of scope, reported only: `zipfile.Path` already differs between 3.11 and 3.12 (changing
either needs developer approval); the two qualname renames above are exception classes.

## File structure

- Create `src/orcapod/utils/canonical_type_names.py` — the resolver.
- Modify `src/orcapod/hashing/hash_utils.py` — `canonical_annotation_str`, `get_function_signature`, `get_function_components`.
- Modify `src/orcapod/utils/function_info.py` — `annotations:` component.
- Modify `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` — `TypeObjectHandler`.
- Modify `src/orcapod/hashing/semantic_hashing/semantic_hasher.py` — `_handle_unknown`.
- Create `tests/test_hashing/test_canonical_type_names.py` — pinned literals + golden hashes.
- Modify `.github/workflows/{run-tests,run-objective-tests,run-postgres-tests,release}.yml`, `pyproject.toml`, `DESIGN_ISSUES.md` (one appended entry), `CLAUDE.md` + `.zed/rules` (one implementation note).

### Task 1: Failing tests

- [ ] Create `tests/test_hashing/test_canonical_type_names.py` with:
  - `EXPECTED_CANONICAL` — literal `canonical_annotation_str` strings (no converter) taken from
    3.12 for: the six pathlib classes, `Path | str`, `Optional[Path]`, `list[Path]`,
    `dict[str, Path]`, `tuple[Path, ...]`, `datetime.{datetime,date,time,timedelta}`,
    `decimal.Decimal`, `uuid.UUID`, `fractions.Fraction`, `ipaddress.IPv4Address`,
    `re.Pattern`, `enum.Enum`, `collections.abc.Sequence[int]`, `collections.abc.Mapping[str, int]`,
    `typing.Any`, `typing.Literal["a", "b"]`, `concurrent.futures.Future` (exempt, stays
    `concurrent.futures._base.Future`), `io.StringIO` (stays `_io.StringIO`).
  - user-defined and third-party types keep `__module__` exactly.
  - `TypeObjectHandler` output and `semantic_hasher.hash_object(PurePath)` literal.
  - golden: `get_function_signature`, `get_function_components` annotations line, and
    `PythonDataFunction` `function_signature_hash` / `function_content_hash` literals for a
    module-level function annotated with `Path`, `Path | str`, `list[Path]`,
    `dict[str, Path]`, `datetime.datetime` (literals generated on 3.12 from main).
- [ ] Run on 3.13: `uv run --python 3.13 pytest tests/test_hashing/test_canonical_type_names.py tests/test_hashing/test_hash_utils.py tests/test_hashing/test_function_info_extractors.py -q` → FAIL with `pathlib._local`.
- [ ] Run on 3.12 → PASS except helper-import tests (helper does not exist yet).

### Task 2: Resolver

- [ ] Create `src/orcapod/utils/canonical_type_names.py`:

```python
def canonical_module_name(obj: object) -> str | None: ...
def canonical_type_name(tp: type) -> str: ...            # "<module>.<qualname>"
def stabilize_type_names(text: str, *annotations: object) -> str: ...
```

  Rule in `canonical_module_name`: return `__module__` unchanged unless (a) its top-level
  package is in `sys.stdlib_module_names`, (b) a non-leading component starts with `_`,
  (c) the module is not in `_LEGACY_PRIVATE_STDLIB_MODULES`, and (d) walking `__qualname__`
  from the public ancestor (`sys.modules`) reaches the very same object. Then return the
  public ancestor.

  `stabilize_type_names` walks an annotation (`__origin__`, `__args__`, nested lists) and
  replaces `"<__module__>.<qualname>"` with the canonical name in `text` for every class
  whose module was resolved. It is a no-op when nothing was resolved, which is what keeps
  3.11/3.12 output byte-identical.

### Task 3: Route identity-string sites through it

- [ ] `canonical_annotation_str` fallback: `stabilize_type_names(inspect.formatannotation(annotation), annotation)`.
- [ ] `get_function_signature`: stabilise each `str(param)` against its annotation and the `-> {ret}` text against the return annotation.
- [ ] `get_function_components` (both copies): stabilise each `{k}:{v}` against `v`.
- [ ] `TypeObjectHandler`: `canonical_module_name(obj) or "<unknown>"`.
- [ ] `_handle_unknown`: `canonical_module_name(type(obj))`.
- [ ] Run Task 1 tests on 3.11, 3.12, 3.13 → PASS. Commit.

### Task 4: Prove no change on 3.11/3.12

- [ ] Re-run the probe (`scratchpad/probe/probe.py`, 438 identity values) on the branch for all
  three versions; branch-3.11 == main-3.11, branch-3.12 == main-3.12, and branch-3.13 ==
  branch-3.12 except time-based record ids and the pre-existing `zipfile.Path` /
  `typing.NamedTuple` rows.

### Task 5: Official 3.13 support

- [ ] Add `"3.13"` to the matrices in the four workflows; add classifiers
  `Programming Language :: Python :: 3.11|3.12|3.13` to `pyproject.toml` (no lockfile change:
  classifiers are not locked; verify `uv lock --check`).
- [ ] Full suite on 3.11, 3.12, 3.13: `uv run --python X pytest tests/ -q`; also `test-objective/`.
- [ ] Fix contained 3.13-only failures; report the rest.

### Task 6: Docs, hygiene, PR

- [ ] Append `DESIGN_ISSUES.md` entry (resolved, ITL-825); add the implementation note to `CLAUDE.md` and `.zed/rules`.
- [ ] `ruff check` / `ruff format --check` on new files, `basedpyright` on changed files vs main.
- [ ] Rebase on origin/main, push, open PR (`Fixes ITL-825`), attach to Linear, report CI.
