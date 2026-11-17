# ITL-826 Org Required CI Compliance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `main` pass the four checks of the metamorphic-labs `Python — Required CI` workflow (ruff lint, ruff format, basedpyright, pytest without Graphviz) without changing runtime behaviour.

**Architecture:** Five reviewable commits in a fixed order: skip-guard for the `dot`-dependent tests, a formatter-only commit with the ruff config, hand-reviewed lint fixes, a basedpyright config plus committed baseline, then contributor docs and pre-commit hooks. No dependency or `uv.lock` change.

**Tech Stack:** uv, ruff 0.15.8 and basedpyright 1.38.4 (both resolved from `uv.lock`), pytest.

---

## Measured state on `origin/main` (07e5a2ea)

All commands run through `uv run --python 3.11`.

| Check | Result |
| -- | -- |
| `uv run --with ruff ruff check .` | 271 errors: F401 147, E402 40, E721 21, E702 19, F541 11, F841 10, E731 7, F811 4, E722 3, E741 3, F821 3, invalid-syntax 2, E712 1 |
| `uv run --with ruff ruff format --check .` | 281 of 450 files would be reformatted |
| `uv run --with basedpyright basedpyright` | 2080 errors, 31200 warnings (tests 1238, src 678, test-objective 84, notebooks 76, examples 3, superpowers 1) |
| `uv run pytest tests/` (3.11 and 3.12, no `dot`) | 3 failed, 4821 passed, 93 skipped, 2 xfailed |
| `uv run pytest test-objective/` (3.11 and 3.12) | 567 passed, 7 xfailed |

**Tool versions.** `uv run --with ruff` does not install the latest ruff here: ruff and
basedpyright are already in the project environment (dev group / dependencies), so the
`--with` requirement is satisfied by the locked versions, ruff 0.15.8 and basedpyright
1.38.4. The CI log of run 42724396559 confirms it: it reports exactly 271 lint errors, the
0.15.8 number. The latest ruff (0.17.4 via `uvx`) reports 1670 errors with a much larger
default rule set. The lock file is therefore the version pin; this PR does not change it.

**Line length.** Formatter diff by line length (ruff 0.15.8, target py311):

| line-length | files reformatted | changed lines |
| -- | -- | -- |
| 79 | 356 | 18029 |
| 88 | 281 | 8636 |
| 100 | 321 | 9002 |
| 120 | 331 | 10944 |

88 is the smallest on both measures. It is also the ruff default the existing pre-commit
hook has been applying.

**Notebooks.** `notebooks/` holds 8 `.ipynb` files. One
(`old_tutorials/02_orcapod_basic_usage copy.ipynb`) does not parse; two would be
reformatted with JSON churn; six import `orcabridge`, a package that no longer exists, and
account for 76 basedpyright errors. `notebooks/` is excluded from ruff and basedpyright by
config. No file is deleted.

**The three failing tests** (all in `tests/test_pipeline/test_graph_rendering.py`):
`TestRenderGraphFullPath::test_render_with_output_path`,
`TestRenderGraphFullPath::test_render_with_show_mocked`,
`TestPipelineShowGraphFullPath::test_show_graph_with_show_mocked`.

## Rules for every task

- Never run `uv lock` or `uv add`. Check `git status` shows no `uv.lock` change after each step.
- Do not change behaviour in `src/orcapod/hashing` (ITL-825). Lint findings there are fixed only when the fix is provably behaviour-neutral, else suppressed with a reasoned `# noqa`.
- Do not touch `CHANGELOG.md`.
- A lint fix that would change runtime behaviour, a file deletion, or an exclusion other than `notebooks/` and build artefacts goes to the coordinator through `ask` first.
- End every commit message with `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>`.

---

### Task 0: Snapshot the public surface

**Files:** none in the repo (scratchpad script only).

- [ ] **Step 1:** Write `surface.py` in the scratchpad. It walks `orcapod` with `pkgutil.walk_packages`, imports every module (recording import failures by exception type), and writes `{module_name: sorted(dir(module))}` as JSON.
- [ ] **Step 2:** Run `uv run --python 3.11 python surface.py before.json` on the untouched tree.

### Task 1: Skip the `dot`-dependent tests when Graphviz is absent

**Files:** Modify `tests/test_pipeline/test_graph_rendering.py`.

- [ ] **Step 1:** The failing state already exists: the three tests fail with `graphviz.backend.execute.ExecutableNotFound` when `dot` is not on `PATH`.
- [ ] **Step 2:** Add after the imports:

```python
import shutil

requires_dot = pytest.mark.skipif(
    shutil.which("dot") is None,
    reason="Graphviz `dot` executable not found on PATH; install Graphviz to run "
    "the tests that render a graph to an image",
)
```

- [ ] **Step 3:** Decorate exactly the three tests above with `@requires_dot`. Leave the test bodies unchanged: where `dot` exists the real rendering still runs.
- [ ] **Step 4:** `uv run pytest tests/test_pipeline/test_graph_rendering.py -rs`. Expected without `dot`: 3 skipped with the reason above, 0 failed. Then run with a stub `dot` first on `PATH` to confirm the marker is evaluated from `PATH` and the three tests are selected to run rather than skipped.
- [ ] **Step 5:** Commit `test(graph_rendering): skip dot-dependent tests when Graphviz is not installed`.

The repository's own `Run Tests` workflow installs Graphviz, so on the PR it must show these three tests as passed, not skipped. Check its log.

### Task 2: ruff config and formatter run

**Files:** Modify `pyproject.toml`; formatter output across the repo.

- [ ] **Step 1:** Add to `pyproject.toml`:

```toml
[tool.ruff]
target-version = "py311"
line-length = 88
# Tutorial notebooks are not linted or formatted: reformatting rewrites their JSON, and
# the ones under old_tutorials/ target a package that no longer exists.
extend-exclude = ["notebooks"]
```

- [ ] **Step 2:** `uv run --with ruff ruff format .`
- [ ] **Step 3:** `uv run --with ruff ruff format --check .` reports every file formatted. `git diff --stat` touches only `.py` files and `pyproject.toml`.
- [ ] **Step 4:** `uv run pytest tests/ -q` and `uv run pytest test-objective/ -q` on 3.11: same counts as Task 1.
- [ ] **Step 5:** Commit `style: apply ruff format across the repository` (config plus formatter output, nothing else).

### Task 3: Lint fixes

**Files:** as reported by `ruff check`; no new files.

- [ ] **Step 1:** `uv run --with ruff ruff check . --fix` (safe fixes only). Review the diff of every `__init__.py` and every file under `src/`.
- [ ] **Step 2:** Re-exports. `src/orcapod/execution_engines/__init__.py`, `nodes/__init__.py`, `operators/__init__.py`, `sources/__init__.py`, `streams/__init__.py`: keep the import and make it explicit (`from x import y as y`, or an `__all__` entry). Never remove a name from a package `__init__`.
- [ ] **Step 3:** F821 (3, all `Path` used inside a string annotation without an import, in `contexts/__init__.py` and `pipeline/job.py`): add `from pathlib import Path` under `if TYPE_CHECKING:`. No runtime effect.
- [ ] **Step 4:** F811 (4). Two are a repeated local import (`pa`, `asyncio`): remove the repeat. Two are duplicate test class names (`TestOperatorNodeAsyncExecute` in `tests/test_channels/test_node_async_execute.py`, `TestSideEffectFunctionPodDecorator` in `tests/test_core/side_effect_function/test_side_effect_function_pod.py`): the second definition shadows the first, so 4 + 2 tests are never collected. Rename the second class (`TestOperatorNodeAsyncExecuteCacheModes`, `TestSideEffectFunctionPodDecoratorForms`). Run the six newly collected tests. If any fails, stop and `ask`.
- [ ] **Step 5:** E402. Restructure where the late import has no reason (move `logger = ...` below the imports; drop the duplicated `_JSONSCHEMA_AVAILABLE` line in `contexts/registry.py` only if the two assignments are identical). Where a test must set `sys.path` or environment before importing, use `# noqa: E402` with the reason.
- [ ] **Step 6:** Remaining rules by hand: E721 (`type(x) == T` to `is`, only where the operands are types; behaviour-identical for classes), E702 (split statements), E731 (lambda to `def`), E741 (rename `l`), E712, F841 (drop the unused binding, keep the call), F541. E722 bare `except`: `except:` and `except Exception:` differ for `KeyboardInterrupt`/`SystemExit`, so a bare except is kept and suppressed with a reasoned `# noqa: E722` rather than changed.
- [ ] **Step 7:** `uv run --with ruff ruff check .` reports `All checks passed!`; `ruff format --check .` still passes.
- [ ] **Step 8:** Run `surface.py after.json` and diff against `before.json`. Expected: no name removed from any module. Record the comparison for the PR.
- [ ] **Step 9:** Full suites on 3.11 and 3.12. Expected: 4821 + 6 passed (the unshadowed tests), 96 skipped, 2 xfailed, 0 failed.
- [ ] **Step 10:** Commit `fix(lint): make ruff check pass`.

### Task 4: basedpyright config and baseline

**Files:** Modify `pyproject.toml`; create `.basedpyright/baseline.json`.

- [ ] **Step 1:** Add to `pyproject.toml` (no `typeCheckingMode`, so the default `recommended` mode stays):

```toml
[tool.basedpyright]
# Setting `exclude` replaces the built-in defaults, so they are repeated here.
exclude = [
    "**/node_modules",
    "**/__pycache__",
    "**/.*",
    "build",
    "dist",
    "site",
    "notebooks",
]
```

- [ ] **Step 2:** Compare the error set under `--pythonplatform Linux` and `Darwin`. If they differ, pin `pythonPlatform = "Linux"` and `pythonVersion = "3.11"` so the baseline written on macOS matches what CI sees; if identical, leave them unset.
- [ ] **Step 3:** Fix cheap real defects first. Known: the three undefined `Path` names (fixed in Task 3). `orcapod.hashing.legacy_core` does not exist, but the import sits in a `try/except ImportError` that defines public names as `None`; changing it alters the public surface of `orcapod.hashing`, so it is baselined and reported, not fixed. `cloudpickle` and `spiral` are optional packages that are not installed: baselined.
- [ ] **Step 4:** `uv run --python 3.11 --with basedpyright basedpyright --writebaseline`.
- [ ] **Step 5:** `uv run --with basedpyright basedpyright` exits 0 with `0 errors`. Record the baselined error count by top-level directory.
- [ ] **Step 6:** Add `x: int = "not an int"` to a source file; the same command must exit 1 with exactly 1 error. Revert the line.
- [ ] **Step 7:** If the baseline exceeds 500 kB, exempt it from the `check-added-large-files` pre-commit hook in Task 5.
- [ ] **Step 8:** Commit `chore(types): add basedpyright config and baseline of existing errors`.

### Task 5: Contributor docs and pre-commit

**Files:** Modify `CLAUDE.md`, `.zed/rules`, `.pre-commit-config.yaml`, `DESIGN_ISSUES.md`.

- [ ] **Step 1:** Add a "Lint, format and type checks" section with identical text to `CLAUDE.md` and `.zed/rules`: the four commands; new and changed code must be ruff-clean, ruff-formatted and add no basedpyright errors; the baseline is regenerated only to remove fixed entries, never to hide a new error; the `dot` binary is optional and the tests that need it skip without it; tool versions come from `uv.lock`.
- [ ] **Step 2:** `.pre-commit-config.yaml`: set the `ruff-pre-commit` rev to `v0.15.8` (the locked version) and add the `ruff-check` hook ahead of `ruff-format`.
- [ ] **Step 3:** Append one entry to the end of `DESIGN_ISSUES.md` for ITL-826: the baseline as type debt, with the counts by directory.
- [ ] **Step 4:** Commit `docs: document lint, format and type-check requirements; align pre-commit with CI`.

### Task 6: Verify and open the PR

- [ ] **Step 1:** Rebase onto `origin/main` if it moved; rerun the suites.
- [ ] **Step 2:** Full suites on 3.11 and 3.12, `test-objective/`, and the four org commands on 3.11.
- [ ] **Step 3:** Push; open the PR against `main` with `Fixes ITL-826` in the body; `orca linear attach`; `orca worktree set --worktree current --pr <n>`.
- [ ] **Step 4:** Wait for CI. Required green: ruff, typecheck, license-check, pytest 3.11, pytest 3.12. pytest 3.13 may fail only the three ITL-825 hashing tests; read its log and confirm.
- [ ] **Step 5:** Send `worker_done` with the acceptance list.
