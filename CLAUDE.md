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

`DESIGN_ISSUES.md` at the project root is the canonical log of known design problems, bugs, and
code quality issues.

When fixing a bug or addressing a design problem:
1. Check `DESIGN_ISSUES.md` first — if a matching issue exists, update its status to
   `in progress` while working and `resolved` once done, adding a brief **Fix:** note.
2. If no matching issue exists, ask the user whether it should be added before proceeding.
   If yes, add it (status `open` or `in progress` as appropriate).

When discovering a new issue that won't be fixed immediately, ask the user whether it should be
logged in `DESIGN_ISSUES.md` before adding it.

## Git commits

Always use [Conventional Commits](https://www.conventionalcommits.org/) style:

```
<type>(<optional scope>): <short description>
```

Common types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`, `ci`.

Examples:
- `feat(schema): add optional_fields to Schema`
- `fix(packet_function): reject variadic parameters at construction`
- `test(function_pod): add schema validation tests`
- `refactor(schema_utils): use Schema.optional_fields directly`
