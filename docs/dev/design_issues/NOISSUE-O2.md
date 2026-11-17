## `src/orcapod/core/operators/` — Async execution

### O2 — Operators silently discard a non-default `_context_key`
**Status:** open
**Severity:** medium

Every operator reads its input with
`stream.as_table(columns={"source": True, "system_tags": True})` — `batch.py:48`,
`merge_join.py:168`, `semijoin.py:60`, `column_selection.py:524`. That column set excludes
`_context_key`, so no operator ever sees the input's data context. `ArrowTableStream.__init__`
then finds no context column on the result and substitutes
`contexts.get_default_context_key()`.

Consequence: a stream carrying a non-default context key reverts to the default at the first
operator it crosses, with no warning. Function pods are unaffected — only the operator layer.

This is pre-existing and layer-wide rather than specific to any one operator. It may also be
intended (operators produce output in the ambient context rather than inheriting an input's), in
which case the fix is to document the rule rather than change behavior. Logged while working
NPIPE-204; deliberately not addressed there.

Fix: decide whether operator output should inherit the input context. If yes, request
`context: True` and propagate it, erroring when inputs disagree. If no, document the reset
explicitly on `StaticOutputPod`.
