# PLT-1663: Merge extension-type-system → main

## Overview

The `extension-type-system` integration branch contains all work from PLT-1652 through
PLT-1660, PLT-1668, and PLT-1672. This plan covers the final steps to bring the branch
up-to-date with `main` and create the merge PR.

## Situation

- `extension-type-system` is 205 commits ahead of `main`
- It is 5 commits **behind** `main` (all PLT-1773: pyspiral `0.11.7 → 0.14.9` upgrade)
- The missing commits cause `spiral-integration` CI to fail (external service issue,
  not a code bug — fixed by the pyspiral version bump on main)
- All other CI checks pass (unit tests 3.11/3.12, license check)
- Code audit: all old naming patterns removed from production code
  - `ExtensionTypeConverter` — gone ✅
  - `ExtensionTypeRegistry` — gone ✅
  - `SemanticTypeRegistry` — only in v0.1.json changelog comment ✅
  - `BaseSemanticHasher` — only in v0.1.json changelog comment ✅
  - Shape-based code — only in explanatory comments ✅

## Steps

1. **Rebase** `extension-type-system` onto `origin/main`
   - Brings in 5 PLT-1773 commits (pyspiral fix + lock file updates)
   - No conflicts expected (verified via dry-run)
   - Will fix the `spiral-integration` CI failure

2. **Force-push** `extension-type-system` to origin
   - Required after rebase; targets feature branch only (not main)

3. **Create PR** `extension-type-system` → `main`
   - Comprehensive description listing all sub-issues resolved
   - References PLT-1663 and all related issues (PLT-1652 through PLT-1660, PLT-1668, PLT-1672)

## Success Criteria

- CI passes on the updated `extension-type-system` branch
- PR is open and ready for review
- PR description references PLT-1663 and all sub-issues
