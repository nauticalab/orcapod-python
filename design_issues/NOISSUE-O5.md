## `src/orcapod/core/operators/` — Async execution

### O5 — A reduction persists a result computed from an incomplete member set
**Status:** open
**Severity:** high

When an upstream pod's output changes, the first `job.run()` afterwards emits only the rows
that were recomputed in that pass. For a per-row pod that emission is *complete* — each row is
independent, so processing just the changed one is correct. For a many→one operator it is
*partial*: the group is reduced over only the members present in that pass, the reducing pod
executes, and its result is persisted with nothing marking it incomplete. The next run emits
the full set and the group is recomputed correctly.

Measured with two upstream pod stages (`source → sync_like → stringify → [GroupBy] → pod`),
one member of one group changed, same Delta store, fresh objects per run:

```
CONTROL (per-row, no GroupBy)
  changed run1   sync_like(99), stringify(sync_99), pod(sync_99.parquet)   <- converged
  changed run2   []

GROUPED
  changed run1   sync_like(99), stringify(sync_99), GROUP ['sync_99']      <- ONE member
  changed run2   GROUP ['sync_99', 'sync_1']                              <- correct
  changed run3   []
```

The control converges in a single run; only the grouped path needs a second one, and it
executes on an incomplete set first.

Consequence: a reducing pod with a side effect writes a complete-looking artifact from partial
input. For the motivating consumer (`common_clock_op`) an intermediate run can produce an
`alignment.json` built from one of a session's probes. It is replaced on the next run, so a
driver that runs to convergence never observes it — but a consumer reading between runs gets a
wrong answer with no signal.

`Batch` behaves identically, so this is pre-existing pipeline semantics surfaced by reduction
rather than something `GroupBy` introduced. It is more consequential for `GroupBy`, because
`GroupBy` exists specifically so a pod can reason over a *complete* group, whereas nobody
derives a result from `Batch` membership.

Not fixable inside the operator: an operator sees whatever its upstream emits and has no way to
know whether a group is complete. A fix needs a completeness signal at the node level — either
the upstream emitting its full cached set on every pass, or a reducing node deferring execution
until its inputs are known settled.

History: first reported as a cache-corruption bug (wrong group recomputing, tag/data
misalignment), then retracted as a propagation lag "identical with and without `GroupBy`". Both
framings are wrong. There is no tag/data misalignment and no cache defect — but the control
above shows the behaviour is *not* identical, because a partial emission is harmless per-row
and harmful for a reduction.
