# Rename `tag` → `key` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename every `tag`/`Tag`/`TAG` identifier to `key`/`Key`/`KEY` across orcapod-python and land it as one atomic commit.

**Architecture:** A Python script applies an ordered substitution table to all text files in the repo — using plain `str.replace` for compound identifiers and regex word-boundary replacement for bare `tag`/`tags` (required because `_staggered_join` contains "tag" as a substring and must not be touched). Two files are renamed via `git mv`. The existing test suite verifies correctness. One atomic commit captures all changes.

**Tech Stack:** Python 3.x (rename script), Git (file renames + commit), pytest via `uv run`

**Spec:** `superpowers/specs/2026-05-12-tag-to-key-rename-design.md`

---

### Task 1: Verify the feature branch

**Files:** (git operation only)

- [ ] **Step 1: Confirm you are on the correct branch**

```bash
git branch --show-current
```

Expected:
```
eywalker/eng-455-rename-tags-keys-across-orcapod-python-hard-break
```

If you are not on this branch:
```bash
git checkout eywalker/eng-455-rename-tags-keys-across-orcapod-python-hard-break
```

---

### Task 2: Create `scripts/rename_tag_to_key.py`

**Files:**
- Create: `scripts/rename_tag_to_key.py`

This script rewrites file **contents** only. File renames are done separately via `git mv`
in Task 4. The script must be deleted before committing (Task 8).

**IMPORTANT — why regex is required:** The method `_staggered_join` in
`src/orcapod/core/operators/join.py` contains the substring "tag" inside "stagger". A bare
`str.replace("tag", "key")` would corrupt it to `_skeygered_join`. The script therefore uses:
- Plain `str.replace` for all **compound** identifiers (these are specific enough to be safe)
- `re.sub(r'\b<word>\b', ...)` for bare `tag`/`tags` only (word boundaries prevent the corruption)

- [ ] **Step 1: Create `scripts/` directory and write the script**

```bash
mkdir -p scripts
```

Create `scripts/rename_tag_to_key.py` with this exact content:

```python
#!/usr/bin/env python3
"""One-shot rename script: tag → key across orcapod-python.

Run from the repo root:
    uv run python scripts/rename_tag_to_key.py

After running, use git mv to rename files (Task 4 of the implementation plan).
Delete this script before committing.

DESIGN NOTE
-----------
Plain str.replace is used for all compound identifiers (safe because they are
specific enough that no unintended substrings match).

Bare "tag"/"tags" use regex word-boundary (\b) replacement.  This is required
because `_staggered_join` contains "tag" as a substring — a plain replace would
silently corrupt it to `_skeygered_join`.
"""

import re
from pathlib import Path

# ── Phase 1: Specific compound identifier substitutions ────────────────────
# Plain str.replace — no regex. Order is significant: longer/more-specific
# patterns must precede shorter ones that are substrings of them.
SPECIFIC_SUBSTITUTIONS: list[tuple[str, str]] = [
    # Column prefix string literals — most specific first
    ("_tag_source_id", "_key_source_id"),
    ("_tag_record_id", "_key_record_id"),
    ("_tag::", "_key::"),
    ("_tag_", "_key_"),

    # SCREAMING_SNAKE_CASE constants — most specific first
    ("SYSTEM_TAG_SOURCE_ID_PREFIX", "SYSTEM_KEY_SOURCE_ID_PREFIX"),
    ("SYSTEM_TAG_RECORD_ID_PREFIX", "SYSTEM_KEY_RECORD_ID_PREFIX"),
    ("SYSTEM_TAG_SOURCE_ID_FIELD", "SYSTEM_KEY_SOURCE_ID_FIELD"),
    ("SYSTEM_TAG_RECORD_ID_FIELD", "SYSTEM_KEY_RECORD_ID_FIELD"),
    ("SYSTEM_TAG_PREFIX_NAME", "SYSTEM_KEY_PREFIX_NAME"),
    ("SYSTEM_TAG_PREFIX", "SYSTEM_KEY_PREFIX"),

    # PascalCase class / protocol names — most specific first
    ("TagProtocol", "KeyProtocol"),
    ("DuplicateTagError", "DuplicateKeyError"),
    ("SelectTagColumns", "SelectKeyColumns"),
    ("DropTagColumns", "DropKeyColumns"),
    ("MapTags", "MapKeys"),
    ("TagValue", "KeyValue"),
    ("Tag", "Key"),  # catch-all PascalCase — safe, no English PascalCase word has "Tag" unintentionally

    # Named snake_case methods / private functions — most specific first
    ("_ensure_system_tags_table", "_ensure_system_keys_table"),
    ("_system_tags_python_schema", "_system_keys_python_schema"),
    ("_system_tags_table", "_system_keys_table"),
    ("_system_tags", "_system_keys"),
    ("_predict_system_tag_schema", "_predict_system_key_schema"),
    ("_compute_system_tag_suffixes", "_compute_system_key_suffixes"),
    ("_sort_merged_system_tags", "_sort_merged_system_keys"),
    ("_rename_sys_tags", "_rename_sys_keys"),
    ("_parse_system_tag_column", "_parse_system_key_column"),
    ("add_system_tag_columns", "add_system_key_columns"),
    ("add_system_tag_column", "add_system_key_column"),
    ("append_to_system_tags", "append_to_system_keys"),
    ("sort_system_tag_values", "sort_system_key_values"),
    ("select_tag_columns", "select_key_columns"),
    ("drop_tag_columns", "drop_key_columns"),
    ("include_system_tags", "include_system_keys"),
    ("system_tags", "system_keys"),
    ("sort_by_tags", "sort_by_keys"),
    ("map_tags", "map_keys"),

    # Common compound variable / parameter patterns — most specific first
    # (each entry also catches variants with a prefix, e.g. "all_tag_schema"
    #  contains "tag_schema" and is handled automatically)
    ("tag_schema", "key_schema"),
    ("tag_columns", "key_columns"),
    ("tag_data", "key_data"),    # file / module name references
    ("test_tag", "test_key"),    # test file name references

    # Catch-all snake_case patterns
    # Safety proof: "stagger"/"_staggered" do NOT contain "tags_", "tag_", or
    # "_tag" as substrings (verified: after "tag" in "stagger" comes "g", not
    # "_" or "s_"; and "_staggered" starts with "_s", not "_t").
    ("tags_", "keys_"),   # e.g. tags_to_drop → keys_to_drop
    ("tag_", "key_"),     # e.g. tag_cols → key_cols, tag_tables → key_tables
    ("_tag", "_key"),     # e.g. sys_tags → sys_keys, left_tag → left_key
]

# ── Phase 2: Word-boundary regex for bare tag / tags ───────────────────────
# Handles standalone `tag`/`tags` identifiers and natural-language uses in
# docstrings, comments, and prose.  \b ensures "stagger" is never touched.
# Order: most-specific case variant first.
WORD_BOUNDARY_SUBSTITUTIONS: list[tuple[str, str]] = [
    ("TAGS", "KEYS"),
    ("TAG", "KEY"),
    ("Tags", "Keys"),
    ("Tag", "Key"),
    ("tags", "keys"),
    ("tag", "key"),
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
    "superpowers",  # design docs — preserve old names as historical record
})


def apply_substitutions(content: str) -> str:
    # Phase 1: specific compound identifiers (plain replace)
    for old, new in SPECIFIC_SUBSTITUTIONS:
        content = content.replace(old, new)
    # Phase 2: bare word-boundary replacements for residual tag/tags
    for old, new in WORD_BOUNDARY_SUBSTITUTIONS:
        content = re.sub(r"\b" + re.escape(old) + r"\b", new, content)
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

**Files:** ~50+ files modified in-place by the script

- [ ] **Step 1: Run the script from the repo root**

```bash
uv run python scripts/rename_tag_to_key.py
```

Expected output (abbreviated — exact count will vary):
```
Modified N file(s):
  .zed/rules
  CLAUDE.md
  DESIGN_ISSUES.md
  README.md
  orcapod-design.md
  src/orcapod/core/datagrams/tag_data.py
  src/orcapod/core/operators/column_selection.py
  src/orcapod/core/operators/join.py
  src/orcapod/core/operators/mappers.py
  src/orcapod/core/operators/merge_join.py
  src/orcapod/core/streams/arrow_table_stream.py
  src/orcapod/core/streams/base.py
  src/orcapod/errors.py
  src/orcapod/protocols/core_protocols/datagrams.py
  src/orcapod/system_constants.py
  src/orcapod/types.py
  src/orcapod/utils/arrow_data_utils.py
  src/orcapod/utils/arrow_utils.py
  src/orcapod/utils/polars_data_utils.py
  ...
  test-objective/unit/test_tag.py
  tests/...
```

Note: file *names* still show the old names at this stage — that is correct.
File renames happen via `git mv` in Task 4.

- [ ] **Step 2: Verify `_staggered_join` was NOT corrupted**

```bash
grep -n "_staggered" src/orcapod/core/operators/join.py
```

Expected: lines containing `_staggered_join` (NOT `_skeygered_join`). If you see
`_skeygered_join`, the word-boundary guard failed — stop and diagnose before continuing.

- [ ] **Step 3: Spot-check critical renamed identifiers**

```bash
grep -n "KeyProtocol\|DuplicateKeyError\|SelectKeyColumns\|MapKeys\|system_keys\|SYSTEM_KEY_PREFIX" \
    src/orcapod/protocols/core_protocols/datagrams.py \
    src/orcapod/errors.py \
    src/orcapod/core/operators/column_selection.py \
    src/orcapod/system_constants.py | head -30
```

Expected: lines containing the NEW names.

- [ ] **Step 4: Verify `system_constants.py` string value was updated**

```bash
grep -n "SYSTEM_KEY_PREFIX_NAME\|PREFIX_NAME" src/orcapod/system_constants.py
```

Expected output contains:
```
SYSTEM_KEY_PREFIX_NAME = "key"
```

(Both the identifier AND the string value `"tag"` → `"key"` must be updated.)

- [ ] **Step 5: Confirm no bare `tag`/`Tag` remain in Python source**

```bash
grep -rn --include="*.py" "\btag\b\|\bTag\b\|\bTAG\b" src/ tests/ test-objective/ | \
    grep -v "_staggered" | head -30
```

Expected: zero matches (or only false-positives that you can review and confirm are intentional).
If genuine misses appear, add them to `SPECIFIC_SUBSTITUTIONS` and re-run the script.

---

### Task 4: Rename the two files with `tag` in their path

**Files:** (git mv operations)

- [ ] **Step 1: List all git-tracked files with `tag` in their path**

```bash
git ls-files | grep -i tag | grep -v superpowers
```

Expected output:
```
src/orcapod/core/datagrams/tag_data.py
test-objective/unit/test_tag.py
```

If additional files appear, rename them with the same `tag` → `key` pattern before proceeding.

- [ ] **Step 2: Rename the source file**

```bash
git mv src/orcapod/core/datagrams/tag_data.py \
       src/orcapod/core/datagrams/key_data.py
```

- [ ] **Step 3: Rename the test-objective file**

```bash
git mv test-objective/unit/test_tag.py \
       test-objective/unit/test_key.py
```

- [ ] **Step 4: Verify no `tag` remains in any tracked path name (excluding superpowers)**

```bash
git ls-files | grep -i tag | grep -v superpowers
```

Expected: no output (zero matches).

- [ ] **Step 5: Verify the renamed files exist and are staged**

```bash
git status --short | grep -E "^R" | grep -E "key_data|test_key"
```

Expected: each renamed file appears as `R  old/path -> new/path`.

- [ ] **Step 6: Quick import check**

```bash
uv run python -c "import orcapod; print('import OK')"
```

Expected:
```
import OK
```

If this raises `ImportError` or `ModuleNotFoundError`, a `__init__.py` is still importing
from the old module path. Find it:

```bash
grep -rn "tag_data\|test_tag" src/orcapod/
```

Update any matches manually (e.g. change `from .tag_data import ...` to
`from .key_data import ...`).

---

### Task 5: Update `CHANGELOG.md`

**Files:**
- Modify: `CHANGELOG.md` (repo root)

- [ ] **Step 1: Add the `tag → key` rename section**

Open `CHANGELOG.md` and add the following block immediately after the
`## [Unreleased]` heading (before the existing `packets → data` entry):

```markdown
#### `tag` → `key` rename (hard break)

All identifiers containing `tag`/`tags`/`Tag` have been renamed to
`key`/`keys`/`Key`. No deprecation aliases. Pre-v0.1 artifacts will not load.

| Old name | New name |
|---|---|
| `Tag` | `Key` |
| `TagProtocol` | `KeyProtocol` |
| `TagValue` | `KeyValue` |
| `DuplicateTagError` | `DuplicateKeyError` |
| `SelectTagColumns` | `SelectKeyColumns` |
| `DropTagColumns` | `DropKeyColumns` |
| `MapTags` | `MapKeys` |
| `system_tags()` | `system_keys()` |
| `map_tags()` | `map_keys()` |
| `select_tag_columns()` | `select_key_columns()` |
| `drop_tag_columns()` | `drop_key_columns()` |
| `sort_by_tags` | `sort_by_keys` |
| `SYSTEM_TAG_PREFIX` | `SYSTEM_KEY_PREFIX` |
| `SYSTEM_TAG_PREFIX_NAME` (`"tag"`) | `SYSTEM_KEY_PREFIX_NAME` (`"key"`) |
| `SYSTEM_TAG_SOURCE_ID_PREFIX` | `SYSTEM_KEY_SOURCE_ID_PREFIX` |
| `SYSTEM_TAG_RECORD_ID_PREFIX` | `SYSTEM_KEY_RECORD_ID_PREFIX` |
| `SYSTEM_TAG_SOURCE_ID_FIELD` | `SYSTEM_KEY_SOURCE_ID_FIELD` |
| `SYSTEM_TAG_RECORD_ID_FIELD` | `SYSTEM_KEY_RECORD_ID_FIELD` |
| `ColumnConfig(system_tags=...)` | `ColumnConfig(system_keys=...)` |
| Column prefix `_tag_` | `_key_` (e.g. `_tag_source_id` → `_key_source_id`) |
| Column prefix `_tag::` | `_key::` (e.g. `_tag::source:abc` → `_key::source:abc`) |
| `src/orcapod/core/datagrams/tag_data.py` | `key_data.py` |
| `test-objective/unit/test_tag.py` | `test_key.py` |
```

---

### Task 6: Run the full test suite

**Files:** (read-only — verify only)

- [ ] **Step 1: Run all tests with fail-fast**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected:
```
... passed, ... warnings in ...s
```

- [ ] **Step 2: If any tests fail, diagnose and fix**

The most common failure modes after this rename:

**a) ImportError — a file still imports from the old name**
```bash
grep -rn "from.*tag_data\|import.*tag_data\|from.*TagProtocol\|import.*Tag[^V]" src/ tests/
```
Update each remaining reference manually.

**b) AttributeError on old method name (e.g. `system_tags`, `map_tags`)**
```bash
grep -rn "\.system_tags\b\|\.map_tags\b\|\.select_tag_columns\b\|\.drop_tag_columns\b" src/ tests/
```
Replace each occurrence with the new name.

**c) `KeyError` / `ValueError` on column prefix string**

If a test asserts an exact column name like `"_tag_source_id"`:
```bash
grep -rn '"_tag_\|_tag::' src/ tests/
```
Any match is a string literal the script missed. Update manually.

**d) `NameError` on a renamed constant**
```bash
grep -rn "SYSTEM_TAG_" src/ tests/
```
Replace each with the `SYSTEM_KEY_` equivalent.

Repeat `uv run pytest tests/ -x -q` after each fix until all tests pass.

- [ ] **Step 3: Run the full suite without `-x` for a complete picture**

```bash
uv run pytest tests/ -q 2>&1 | tail -5
```

Expected: `N passed` with no failures or errors.

---

### Task 7: Final verification

**Files:** (read-only — verify only)

- [ ] **Step 1: Confirm no `tag`/`Tag`/`TAG` remain in Python source (excluding `_staggered`)**

```bash
grep -rn --include="*.py" "\btag\b\|\bTag\b\|\bTAG\b" src/ tests/ test-objective/ | \
    grep -v "_staggered"
```

Expected: zero matches. Any match is a genuine miss — fix it manually.

- [ ] **Step 2: Confirm no `tag`/`Tag` remain in Markdown docs**

```bash
grep -rn --include="*.md" "\btag\b\|\bTag\b" \
    docs/ examples/ notebooks/ README.md CLAUDE.md orcapod-design.md 2>/dev/null | \
    grep -iv "git tag\|github.*tag\|release.*tag\|version.*tag" | head -20
```

Review any remaining matches. Legitimate exceptions (e.g. "git tag" in RELEASING.md)
are fine; content references to the old API are not.

- [ ] **Step 3: Confirm `.zed/rules` was updated**

```bash
grep -n "tag\|Tag" .zed/rules | grep -iv "git tag" | head -10
```

Expected: zero matches (or only contextual historical mentions). The script processes
`.zed/rules` as an `EXTRA_FILE`.

- [ ] **Step 4: Run the full test suite one final time**

```bash
uv run pytest tests/ -q 2>&1 | tail -5
```

Expected: `N passed` with no failures or errors. Do not proceed to Task 8 until this is clean.

---

### Task 8: Delete the rename script, stage all changes, atomic commit

**Files:**
- Delete: `scripts/rename_tag_to_key.py`
- Commit: all staged and unstaged changes

- [ ] **Step 1: Delete the rename script**

```bash
rm scripts/rename_tag_to_key.py
rmdir scripts/ 2>/dev/null || true
```

- [ ] **Step 2: Stage all changes**

```bash
git add -A
```

- [ ] **Step 3: Verify the staged file count looks right**

```bash
git diff --cached --stat | tail -5
```

Expected: 50+ files changed (content edits + 2 file renames + `CHANGELOG.md`).
The rename script itself must NOT appear (it was deleted, never committed).

- [ ] **Step 4: Create the single atomic commit**

```bash
git commit -m "$(cat <<'EOF'
refactor: rename tag → key across orcapod-python (ENG-455)

Hard break — no deprecation aliases, no migration shims.
All public and private identifiers, docstrings, comments,
module names, file names, and serialized column prefixes
containing tag/Tag renamed to key/Key. Updates CHANGELOG.md
with full name-mapping table.

Fixes ENG-455
EOF
)"
```

- [ ] **Step 5: Verify the commit looks correct**

```bash
git show --stat HEAD | head -20
```

Expected: the commit message shows `refactor: rename tag → key...` and the stat
lists the two renamed files as `old_name => new_name`.

---

### Task 9: Push branch and open the pull request

- [ ] **Step 1: Push the branch**

```bash
git push -u origin eywalker/eng-455-rename-tags-keys-across-orcapod-python-hard-break
```

- [ ] **Step 2: Create the PR targeting `dev`**

```bash
gh pr create \
  --base dev \
  --title "refactor: rename tag → key across orcapod-python" \
  --body "$(cat <<'EOF'
## Summary

Hard break rename of all `tag`/`Tag`/`TAG` identifiers to `key`/`Key`/`KEY`
throughout orcapod-python, landing before v0.1 stable.

- All public + private symbols, module names, file names, docstrings, and comments updated
- Serialized column prefixes `_tag_` and `_tag::` renamed to `_key_` and `_key::` in-place
- `SYSTEM_TAG_PREFIX_NAME = "tag"` changed to `SYSTEM_KEY_PREFIX_NAME = "key"` (drives all generated column headers)
- No deprecation shims, no backwards-compat aliases
- `CHANGELOG.md` updated with full name-mapping table
- Test suite passing

Fixes ENG-455

## Files renamed

| Old | New |
|---|---|
| `src/orcapod/core/datagrams/tag_data.py` | `key_data.py` |
| `test-objective/unit/test_tag.py` | `test_key.py` |

## Test plan
- [ ] `uv run pytest tests/ -q` passes with zero failures
- [ ] `grep -rn '\btag\b\|\bTag\b' src/ tests/ | grep -v _staggered` returns zero matches
- [ ] `git ls-files | grep -i tag | grep -v superpowers` returns zero matches
EOF
)"
```

- [ ] **Step 3: Record the PR URL**

Copy the URL printed by `gh pr create` and note it for the Linear issue.
