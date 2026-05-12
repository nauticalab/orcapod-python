# Rename `packets` → `data` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename every `packet`/`Packet`/`PACKET_*` identifier to `data`/`Data`/`DATA_*` across orcapod-python and land it as one atomic commit.

**Architecture:** A Python script applies an ordered substitution table to all text files in the repo; then `git mv` renames the affected files and directory. The existing test suite verifies the rename. A single atomic commit captures all changes including a new `CHANGELOG.md`.

**Tech Stack:** Python 3.x (rename script), Git (file renames + commit), pytest via `uv run`

**Spec:** `superpowers/specs/2026-05-12-packet-to-data-rename-design.md`

---

### Task 1: Check out the feature branch

**Files:** (git operation only)

- [ ] **Step 1: Check out the feature branch**

```bash
git checkout -b eywalker/eng-454-rename-packets-data-across-orcapod-python-hard-break
```

Expected:
```
Switched to a new branch 'eywalker/eng-454-rename-packets-data-across-orcapod-python-hard-break'
```

- [ ] **Step 2: Verify you are on the correct branch**

```bash
git branch --show-current
```

Expected:
```
eywalker/eng-454-rename-packets-data-across-orcapod-python-hard-break
```

---

### Task 2: Create `scripts/rename_packet_to_data.py`

**Files:**
- Create: `scripts/rename_packet_to_data.py`

This script is a one-shot tool that rewrites file contents. File/directory renames are done separately via `git mv` in Task 4. Delete the script before committing (Task 8).

- [ ] **Step 1: Create the `scripts/` directory and write the script**

Create `scripts/rename_packet_to_data.py` with the following content:

```python
#!/usr/bin/env python3
"""One-shot rename script: packet → data across orcapod-python.

Run from the repo root:
    python scripts/rename_packet_to_data.py

After running, use git mv to rename files (Task 4 of the implementation plan).
Delete this script before committing.
"""

from pathlib import Path

# ── Substitutions in longest/most-specific first order ──────────────────────
# Plain str.replace — no regex. Order is significant: longer patterns must
# precede shorter ones that are substrings of them (e.g. `packet_function`
# must come before `packet`, `output_packet_schema_hash` before
# `output_packet_schema`).
SUBSTITUTIONS: list[tuple[str, str]] = [
    # SCREAMING_SNAKE_CASE system constants
    # (all-caps PACKET is not matched by the PascalCase or lowercase rules below)
    ("INPUT_PACKET_HASH_COL", "INPUT_DATA_HASH_COL"),
    ("PACKET_RECORD_ID", "DATA_RECORD_ID"),

    # PascalCase class names — longest first
    ("PacketFunctionExecutorProtocol", "DataFunctionExecutorProtocol"),
    ("PacketExecutionLoggerProtocol", "DataExecutionLoggerProtocol"),
    ("PacketFunctionWrapper", "DataFunctionWrapper"),
    ("CachedPacketFunction", "CachedDataFunction"),
    ("PythonPacketFunction", "PythonDataFunction"),
    ("PacketFunctionProtocol", "DataFunctionProtocol"),
    ("PacketFunctionProxy", "DataFunctionProxy"),
    ("PacketFunctionBase", "DataFunctionBase"),
    ("PacketFunction", "DataFunction"),
    ("SelectPacketColumns", "SelectDataColumns"),
    ("DropPacketColumns", "DropDataColumns"),
    ("MapPackets", "MapData"),
    ("PacketLogger", "DataLogger"),
    ("PacketProtocol", "DataProtocol"),
    ("Packet", "Data"),

    # snake_case identifiers — longest first
    ("packet_function_type_id", "data_function_type_id"),
    ("input_packet_schema", "input_data_schema"),
    ("output_packet_schema_hash", "output_data_schema_hash"),
    ("output_packet_schema", "output_data_schema"),
    ("_async_process_packet_internal", "_async_process_data_internal"),
    ("_process_packet_internal", "_process_data_internal"),
    ("_iter_packets_sequential", "_iter_data_sequential"),
    ("_iter_packets_concurrent", "_iter_data_concurrent"),
    ("_async_execute_one_packet", "_async_execute_one_data"),
    ("_build_output_packet", "_build_output_data"),
    ("get_cached_output_for_packet", "get_cached_output_for_data"),
    ("record_packet", "record_data"),
    ("async_process_packet", "async_process_data"),
    ("process_packet", "process_data"),
    ("execute_packet", "execute_data"),
    ("async_iter_packets", "async_iter_data"),
    ("iter_packets", "iter_data"),
    ("map_packets", "map_data"),
    ("select_packet_columns", "select_data_columns"),
    ("drop_packet_columns", "drop_data_columns"),
    ("packet_function", "data_function"),   # after packet_function_type_id
    ("on_packet_start", "on_data_start"),
    ("on_packet_end", "on_data_end"),
    ("on_packet_crash", "on_data_crash"),
    ("create_packet_logger", "create_data_logger"),
    ("verify_packet_schema", "verify_data_schema"),
    ("_build_packet_function_registry", "_build_data_function_registry"),
    ("resolve_packet_function_from_config", "resolve_data_function_from_config"),
    ("register_packet_function", "register_data_function"),
    ("packet_schema", "data_schema"),
    ("input_packet", "input_data"),
    ("output_packet", "output_data"),
    ("packets", "data"),    # plural before singular
    ("packet", "data"),     # catch-all — must be last
]

# File extensions to process
EXTENSIONS = frozenset({
    ".py", ".md", ".rst", ".ipynb", ".toml",
    ".yaml", ".yml", ".ini", ".cfg", ".txt",
})

# Known extensionless files to also process (relative to repo root)
EXTRA_FILES = [
    ".zed/rules",
]

# Directories whose entire subtree is skipped
SKIP_DIRS = frozenset({
    ".git", ".venv", "venv", "__pycache__", ".mypy_cache",
    "node_modules", ".tox", "dist", "build",
    "superpowers",  # design docs — preserve old names as historical reference
})


def apply_substitutions(content: str) -> str:
    for old, new in SUBSTITUTIONS:
        content = content.replace(old, new)
    return content


def process_file(path: Path) -> bool:
    """Apply substitutions to one file. Returns True if the file changed."""
    try:
        original = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, PermissionError) as exc:
        print(f"  SKIP {path}: {exc}")
        return False
    updated = apply_substitutions(original)
    if updated != original:
        path.write_text(updated, encoding="utf-8")
        return True
    return False


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    changed: list[Path] = []

    # Walk all files with matching extensions
    for path in sorted(repo_root.rglob("*")):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.resolve() == Path(__file__).resolve():
            continue
        if path.is_file() and path.suffix in EXTENSIONS:
            if process_file(path):
                changed.append(path.relative_to(repo_root))

    # Process known extensionless files
    for rel in EXTRA_FILES:
        path = repo_root / rel
        if path.exists() and path.is_file():
            if process_file(path):
                changed.append(path.relative_to(repo_root))

    print(f"\nModified {len(changed)} file(s):")
    for p in sorted(changed):
        print(f"  {p}")


if __name__ == "__main__":
    main()
```

---

### Task 3: Run the rename script (content substitution)

**Files:** ~186 files modified in-place by the script

- [ ] **Step 1: Run the script from the repo root**

```bash
python scripts/rename_packet_to_data.py
```

Expected output (abbreviated — exact count may vary):
```
Modified 186 file(s):
  CLAUDE.md
  DESIGN_ISSUES.md
  README.md
  docs/...
  examples/...
  notebooks/...
  src/orcapod/core/datagrams/tag_packet.py
  src/orcapod/core/data_function.py    ← content already updated, name still old
  ...
  tests/test_core/packet_function/test_cached_packet_function.py
  ...
```

Note: file names in the output still show the old names — that is correct at this stage.
File renames happen via `git mv` in Task 4.

- [ ] **Step 2: Spot-check a few critical files to confirm substitutions applied**

```bash
grep -n "DataFunctionBase\|DataProtocol\|iter_data\|data_function" \
    src/orcapod/core/datagrams/tag_packet.py \
    src/orcapod/core/packet_function.py \
    src/orcapod/protocols/core_protocols/packet_function.py | head -30
```

Expected: lines containing the NEW names (`DataFunctionBase`, `iter_data`, etc.), not the old ones.

- [ ] **Step 3: Confirm no bare `packet` / `Packet` remain in source or docs**

```bash
# Python source and tests
grep -rn --include="*.py" "packet\|Packet" src/ tests/ | head -20

# Markdown docs and examples
grep -rn --include="*.md" "packet\|Packet" docs/ examples/ notebooks/ README.md \
    DESIGN_ISSUES.md CLAUDE.md 2>/dev/null | head -20
```

Expected: zero matches in both commands. If any remain, check whether they are genuine
misses or false positives (e.g. a comment that uses "packet" in a non-identifier context).
Fix genuine misses by adding them to `SUBSTITUTIONS` and re-running the script.

---

### Task 4: Find all files with `packet` in their path name, then rename them

**Files:** (git mv operations)

- [ ] **Step 1: List all git-tracked files with `packet` in their path**

```bash
git ls-files | grep -i packet
```

Expected output (these are the files that need `git mv`):
```
src/orcapod/core/datagrams/tag_packet.py
src/orcapod/core/packet_function.py
src/orcapod/core/packet_function_proxy.py
src/orcapod/protocols/core_protocols/packet_function.py
tests/test_core/packet_function/__init__.py
tests/test_core/packet_function/test_cached_packet_function.py
tests/test_core/packet_function/test_executor.py
tests/test_core/packet_function/test_packet_function.py
tests/test_core/packet_function/test_packet_function_config.py
tests/test_core/packet_function/test_packet_function_proxy.py
```

If there are more files than expected, rename them with the same `packet` → `data` pattern
before proceeding.

- [ ] **Step 2: Rename individual test files first (while still in the old directory)**

```bash
git mv tests/test_core/packet_function/test_cached_packet_function.py \
       tests/test_core/packet_function/test_cached_data_function.py

git mv tests/test_core/packet_function/test_packet_function.py \
       tests/test_core/packet_function/test_data_function.py

git mv tests/test_core/packet_function/test_packet_function_config.py \
       tests/test_core/packet_function/test_data_function_config.py

git mv tests/test_core/packet_function/test_packet_function_proxy.py \
       tests/test_core/packet_function/test_data_function_proxy.py
```

(`test_executor.py` has no `packet` in its name — no rename needed.)

- [ ] **Step 3: Rename the test directory**

```bash
git mv tests/test_core/packet_function tests/test_core/data_function
```

- [ ] **Step 4: Rename the source files**

```bash
git mv src/orcapod/core/datagrams/tag_packet.py \
       src/orcapod/core/datagrams/tag_data.py

git mv src/orcapod/core/packet_function.py \
       src/orcapod/core/data_function.py

git mv src/orcapod/core/packet_function_proxy.py \
       src/orcapod/core/data_function_proxy.py

git mv src/orcapod/protocols/core_protocols/packet_function.py \
       src/orcapod/protocols/core_protocols/data_function.py
```

- [ ] **Step 5: Verify no `packet` remains in any tracked path name**

```bash
git ls-files | grep -i packet
```

Expected: no output (zero matches).

- [ ] **Step 6: Confirm the renamed paths exist and are staged**

```bash
git status --short | grep -E "^R|^A|^M" | grep -E "data_function|tag_data" | head -20
```

Expected: each renamed file appears as `R  old/path -> new/path`.

---

### Task 5: Review and fix same-scope `data` collisions

**Files:** Any source files where the rename introduces a same-scope name clash

- [ ] **Step 1: Find any function/method bodies that now have two `data` identifiers in the same scope**

```bash
grep -n "\bdata\b" src/orcapod/core/data_function.py \
                   src/orcapod/core/cached_function_pod.py \
                   src/orcapod/core/function_pod.py \
                   src/orcapod/pipeline/observer.py \
                   src/orcapod/pipeline/logging_observer.py | \
    grep -v "data_function\|data_schema\|input_data\|output_data\|data_context\|iter_data\|process_data\|data_logger\|DataFunction\|DataProtocol\|DataLogger\|variation_data\|exec_data" | head -30
```

For any true same-scope clash (e.g. a local variable named `data` in a function that also
takes a `data` parameter), rename the pre-existing variable to something more specific.

Example — if you find a method like:
```python
def process_data(self, data: DataProtocol) -> dict:
    data = self._fetch_variation_data()  # clash: shadowing parameter
    return data
```

Fix it to:
```python
def process_data(self, data: DataProtocol) -> dict:
    variation_data = self._fetch_variation_data()
    return variation_data
```

- [ ] **Step 2: Confirm no regressions from collision fixes by running a quick import check**

```bash
uv run python -c "import orcapod; print('import OK')"
```

Expected:
```
import OK
```

If this raises an `ImportError` or `ModuleNotFoundError`, the most likely cause is a
`__init__.py` still importing from the old module path (e.g. `from .packet_function import
...`). Fix by checking which `__init__.py` still references the old name:

```bash
grep -rn "packet_function\|tag_packet" src/orcapod/
```

Any matches are `__init__.py` lines that the rename script missed. Update them manually.

---

### Task 6: Run the full test suite

**Files:** (read-only — verify only)

- [ ] **Step 1: Run all tests**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected:
```
... passed, ... warnings in ...s
```

- [ ] **Step 2: If any tests fail, diagnose and fix**

The most common failure modes after a rename:

a) **ImportError** — a file still imports from the old name. Fix:
   ```bash
   grep -rn "from.*packet\|import.*packet" src/ tests/
   ```
   Update each remaining reference manually.

b) **AttributeError on `iter_packets` / `process_packet`** — a test or source file uses
   the old method name. Fix by searching and replacing the specific occurrence.

c) **FileNotFoundError on a module path** — a string-based module reference (e.g. in
   `serialization.py`) still uses the old name. Fix by searching for string literals:
   ```bash
   grep -rn '"packet_function"\|"tag_packet"' src/ tests/
   ```

Repeat `uv run pytest tests/ -x -q` after each fix until all tests pass.

- [ ] **Step 3: Run the full suite without `-x` to see the complete picture**

```bash
uv run pytest tests/ -q 2>&1 | tail -5
```

Expected: `N passed` with no failures or errors.

---

### Task 7: Create `CHANGELOG.md`

**Files:**
- Create: `CHANGELOG.md` (repo root)

- [ ] **Step 1: Create `CHANGELOG.md` at the repo root**

```markdown
# Changelog

## [Unreleased]

### Breaking Changes

#### `packets` → `data` rename (hard break)

All identifiers containing `packet`/`packets`/`Packet` have been renamed to
`data`/`Data`. No deprecation aliases. Pre-v0.1 artifacts will not load.

| Old name | New name |
|---|---|
| `Packet` | `Data` |
| `PacketProtocol` | `DataProtocol` |
| `PacketFunction` | `DataFunction` |
| `PacketFunctionBase` | `DataFunctionBase` |
| `PacketFunctionProtocol` | `DataFunctionProtocol` |
| `PacketFunctionProxy` | `DataFunctionProxy` |
| `PythonPacketFunction` | `PythonDataFunction` |
| `CachedPacketFunction` | `CachedDataFunction` |
| `PacketFunctionWrapper` | `DataFunctionWrapper` |
| `PacketFunctionExecutorProtocol` | `DataFunctionExecutorProtocol` |
| `PacketExecutionLoggerProtocol` | `DataExecutionLoggerProtocol` |
| `PacketLogger` | `DataLogger` |
| `SelectPacketColumns` | `SelectDataColumns` |
| `DropPacketColumns` | `DropDataColumns` |
| `MapPackets` | `MapData` |
| `iter_packets()` | `iter_data()` |
| `process_packet()` | `process_data()` |
| `async_process_packet()` | `async_process_data()` |
| `execute_packet()` | `execute_data()` |
| `map_packets()` | `map_data()` |
| `select_packet_columns()` | `select_data_columns()` |
| `drop_packet_columns()` | `drop_data_columns()` |
| `on_packet_start()` | `on_data_start()` |
| `on_packet_end()` | `on_data_end()` |
| `on_packet_crash()` | `on_data_crash()` |
| `INPUT_PACKET_HASH_COL` | `INPUT_DATA_HASH_COL` |
| `PACKET_RECORD_ID` | `DATA_RECORD_ID` |
```

---

### Task 8: Delete the rename script and create the atomic commit

**Files:**
- Delete: `scripts/rename_packet_to_data.py`
- Commit: all staged changes

- [ ] **Step 1: Confirm tests still pass (final check before committing)**

```bash
uv run pytest tests/ -q 2>&1 | tail -5
```

Expected: all passing, zero failures.

- [ ] **Step 2: Delete the rename script**

```bash
rm scripts/rename_packet_to_data.py
```

Remove the `scripts/` directory too if it is now empty:

```bash
rmdir scripts/ 2>/dev/null || true
```

- [ ] **Step 3: Stage all changes**

```bash
git add -A
```

- [ ] **Step 4: Verify the staged file count looks right**

```bash
git diff --cached --stat | tail -5
```

Expected: ~190+ files changed (186+ content edits + 9 file renames + `CHANGELOG.md`).
The rename script itself should NOT appear (it was deleted and never committed).

- [ ] **Step 5: Create the single atomic commit**

```bash
git commit -m "$(cat <<'EOF'
refactor: rename packet → data across orcapod-python (ENG-454)

Hard break — no deprecation aliases, no migration shims.
All public and private identifiers, docstrings, comments,
module names, and file names containing packet/Packet renamed
to data/Data. Adds CHANGELOG.md with full mapping table.

Fixes ENG-454
EOF
)"
```

- [ ] **Step 6: Verify the commit looks correct**

```bash
git show --stat HEAD | head -20
```

Expected: the commit message shows `refactor: rename packet → data...` and the stat lists
the renamed files (shown as `old_name => new_name`).

- [ ] **Step 7: Push the branch**

```bash
git push -u origin eywalker/eng-454-rename-packets-data-across-orcapod-python-hard-break
```

---

### Task 9: Open the pull request

- [ ] **Step 1: Create the PR targeting `dev`**

```bash
gh pr create \
  --base dev \
  --title "refactor: rename packet → data across orcapod-python" \
  --body "$(cat <<'EOF'
## Summary

Hard break rename of all `packet`/`Packet` identifiers to `data`/`Data` throughout
orcapod-python, landing before v0.1 stable.

- All public + private symbols, module names, file names, docstrings, and comments updated
- No deprecation shims, no backwards-compat aliases
- `CHANGELOG.md` created with full name-mapping table
- Test suite passing

Fixes ENG-454

## Files renamed

| Old | New |
|---|---|
| `src/orcapod/core/datagrams/tag_packet.py` | `tag_data.py` |
| `src/orcapod/core/packet_function.py` | `data_function.py` |
| `src/orcapod/core/packet_function_proxy.py` | `data_function_proxy.py` |
| `src/orcapod/protocols/core_protocols/packet_function.py` | `data_function.py` |
| `tests/test_core/packet_function/` | `tests/test_core/data_function/` |

## Test plan
- [ ] `uv run pytest tests/ -q` passes with zero failures
- [ ] `grep -rn "packet\|Packet" src/ tests/` returns zero matches
- [ ] `git ls-files | grep -i packet` returns zero matches
EOF
)"
```

- [ ] **Step 2: Record the PR URL**

Copy the URL printed by `gh pr create` and post it to the Linear issue comment or PR
description so reviewers can find it.
