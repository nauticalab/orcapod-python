# ITL-895 Developer Decisions (D1-D7, ITL-901) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Follows `2026-11-11-itl-895-float-to-int.md` (the draft PR #299). That plan built the
mechanism; this one applies the developer's decisions of 2026-11-11.

## Status log (for a successor)

- 2026-11-11: Plan written. The warning mechanism (section "Design") and the open question
  Q2 went to the coordinator through `ask`.
- 2026-11-11: Coordinator: Q1 approved as designed (the name `OutputConversionWarning` is a
  new public name it confirms with the developer; keep a rename one line). Q2: (a),
  provisionally, structured so (b) is a small switch (`iterable_for_list` in the table). PR #300
  (tensors) will import `orcapod.errors.OutputConversionWarning` too; whichever PR merges
  second reconciles. Finding: `tuple[...]` is not a supported output annotation at all
  ("Unsupported annotation"), so the reverse-of-D5 case cannot occur; the checker's
  `tuple[...]` branch is dead code and is removed.

- 2026-11-11: Developer, final: the category is `orcapod.errors.ValueConversionWarning`
  (plain `UserWarning` subclass, docstring verbatim from the coordinator, shared with PR #300;
  the caller builds the message), deduplicated explicitly per function instance and (output,
  location, near miss), marked only after `warnings.warn` returns; Q2 option (a) final.
  Done: tests (red, then green), rule table, warning, explicit dedup, docs table, CHANGELOG,
  DESIGN_ISSUES (FO1 resolved, FO2 inputs open/deferred), merge of origin/main (ITL-878,
  ITL-821), no-debug-only-parameters refactor of the checker (containers annotate results).
  Dedup evidence: `superpowers/specs/itl-895-float-to-int/09_warning_dedup.py`. Finding:
  `PipelineJob` runs grow superlinearly with rows on main too (1,000 rows: minutes), so the
  pipeline dedup scenarios use 1,000 rows. **Next:** full suite on the final head, PR body,
  mark ready, CI, CodeRabbit (review bodies too), worker_done.

**Goal:** `OUTPUT_CONFORMANCE` applies the developer's decisions: each near-miss value is
either accepted silently, accepted and converted with an `OutputConversionWarning`, or rejected
with an `OutputTypeError` that says what to return instead.

**Architecture:** The rule table's fields become a three-valued `Treatment` (`ACCEPT`,
`CONVERT_WITH_WARNING`, `REJECT`). The checker returns the first mismatch and the conversions
it found. `PythonDataFunction._build_output_data` raises for a mismatch, and otherwise warns
once per conversion from a single `warnings.warn` call before the eager conversion that
already exists. The stored value and the value downstream pods receive stay the same object
path (the eager Arrow conversion), so they cannot diverge.

**Tech Stack:** Python 3.11-3.13, pyarrow, numpy, pytest, `uv run`.

---

## Decisions (developer, 2026-11-11)

| Case | Declared | Returned | Treatment |
|---|---|---|---|
| D1 | `float` (any float type) | `int` / NumPy integer | reject; message says `return 0.0` / `float(x)` |
| D2 | `int` / `bool` / `float` (or NumPy type of that kind) | NumPy scalar of the same kind, value-exact | accept |
| D3 | `float` | `bool` / `numpy.bool_` | reject |
| D4 | not optional | `None` | reject (unchanged) |
| D5 | `list[T]` | `tuple` | accept, converted to `list`, warning |
| D6 | `date` | `datetime` | accept, time dropped, warning (reverses the draft) |
| D7 | `numpy.float32` | Python `float` / wider NumPy float | accept, rounded to 32 bits, warning |
| ITL-901 | `list[T]` | `set` / `frozenset` | reject (order differs between processes) |

Everything else in the draft stands. Inputs (sources, `Datagram`) are **deferred** by the
developer: no change; DESIGN_ISSUES records it as open/deferred.

Finding while planning: under the draft, other iterables for `list[T]` pass **unchecked**, and
pyarrow truncates inside them: `numpy.array([1.5, 2.0])` returned for `list[int]` is stored as
`[1, 2]`. See Q2.

## Design (sent to the coordinator)

- `orcapod.errors.OutputConversionWarning(UserWarning)`, next to `OutputTypeError`. It is
  built from `function_name`, `output_key`, `location`, `declared_type`, `returned_type`,
  `conversion` and `hint` and keeps them as attributes. Message, with **no per-row value**:
  - D5: `Function 'f' returned tuple for output 'y', which is declared list[int]; Orcapod
    converted it to a list. Return a list (list(value)) to avoid this warning.`
  - D6: `Function 'f' returned datetime.datetime for output 'd', which is declared
    datetime.date; Orcapod dropped the time of day. Return value.date() to avoid this warning.`
  - D7: `Function 'f' returned float for output 'y', which is declared numpy.float32; Orcapod
    rounded it to 32 bits. Return numpy.float32(value) to avoid this warning.`
  - Nested values name a structural location, never an index or key that varies by row:
    `for output 'y' at [*]` (list element, dict value), `.field` (dataclass field), `[0]`
    (position in a fixed `tuple[...]`).
- Raised from one line (`warnings.warn(warning, stacklevel=1)` in a helper of
  `data_function.py`), so the default `"default"` action shows it once per distinct message
  per process: one warning per function, output, location and conversion kind, however many
  rows. `-W error::orcapod.errors.OutputConversionWarning` turns it into an exception, which is
  then a row failure like `OutputTypeError` (a strict mode for free). `-W always` shows every
  row.
- The checker is pure: it returns `ConformanceReport(mismatch, conversions)`, and the caller
  (`_build_output_data`, which knows the function and output) builds the error / warning. No
  helper receives a value only for a message.
- Cached rows are not recomputed, so they do not warn again.
- Async orchestrator: the function runs in an executor thread, but `_build_output_data` runs on
  the event loop thread after it returns; the module registry is shared, so dedup holds.
- Tests: `pytest.warns(errors.OutputConversionWarning, match=...)`; dedup tested with
  `warnings.catch_warnings(record=True)` + `simplefilter("default")` over many rows (pytest
  resets the registry per test). A 10,000-row run, sync and async, is verified by a script and
  reported.
- Assumption: a `list` returned for a declared `tuple[...]` is the reverse of D5 and gets the
  same treatment (accept, converted, warning).

**Q2** (asked): other iterables for `list[T]` (NumPy array, `deque`, `range`, dict views,
generators). Recommended (a): re-iterable ordered containers are accepted with the D5 warning,
their elements checked (an ndarray by its dtype, not per element); one-shot iterators
(generators) are rejected with `return list(...)`; `set` / `frozenset` rejected (ITL-901).
(b) strict: only `list`, plus `tuple` with a warning. (c) draft: unchecked.

## Files

- Modify: `src/orcapod/semantic_types/output_conformance.py` (Treatment, table, report, hints).
- Modify: `src/orcapod/errors.py` (`OutputConversionWarning`; `OutputTypeError` takes a hint).
- Modify: `src/orcapod/core/data_function.py` (`_build_output_data` warns).
- Modify: `tests/test_core/function_pod/test_output_conformance.py`.
- Modify: `docs/concepts/function-pods.md`, `CHANGELOG.md`, `DESIGN_ISSUES.md`, PR body.

## Task 1: Tests first (after the coordinator's reply)

- [ ] Replace `TestUndecidedCasesKeepTodaysBehaviour` with:
  - `REJECTED_BY_DECISION`: `float<-0` (D1; message contains `0.0` and `float(`),
    `float<-np.int64(1)` (D1), `np.float32<-1` (D1), `float<-True`, `float<-np.bool_(True)`
    (D3), `list[int]<-{1, 2}`, `list[int]<-frozenset({1})` (ITL-901), plus Q2 cases.
  - `ACCEPTED_SILENTLY` (assert no `OutputConversionWarning` via
    `warnings.catch_warnings(record=True)` + `simplefilter("always")`): `int<-np.int64(3)`,
    `bool<-np.bool_(True)`, `float<-np.float64(1.5)`, `float<-np.float32(0.5)`,
    `np.float32<-np.float32(0.1)`, every conforming value.
  - `CONVERTED_WITH_WARNING`: `list[int]<-(1, 2)` -> `[1, 2]`, `dt.date<-datetime(2020,1,1,12)`
    -> `date(2020,1,1)`, `np.float32<-0.1` -> `float(np.float32(0.1))`,
    `tuple[int, int]<-[1, 2]`, nested `dict[str, list[int]]<-{"a": (1,)}` (location `[*]`).
    Each: `pytest.warns(..., match=<function>, <output>, declared, returned type)`, stored value.
  - Downstream sees the converted value, standalone and `PipelineJob` sync/async x
    memory/delta: tuple -> `list` seen by `down`, datetime -> `date` (not `datetime`),
    `0.1` -> `float(np.float32(0.1))`.
  - Dedup: 1,000 rows under `simplefilter("default")` -> exactly one warning per kind.
  - Rejections in `PipelineJob` sync/async x memory/delta: a row failure with nothing stored.
- [ ] Run, show the failures, commit `test(function_pod): developer decisions D1-D7 (ITL-895)`.

## Task 2: Rule table and checker

- [ ] `Treatment` enum; `ConformanceRule` fields `int_for_float`, `numpy_scalar_same_kind`,
  `bool_for_float`, `none_for_non_optional`, `tuple_for_list`, `datetime_for_date`,
  `float_for_float32`, `set_for_list`, `iterable_for_list` (Q2); the table per the decisions.
- [ ] `check_conformance(value, declared, rule) -> ConformanceReport` (replaces
  `find_nonconformance`); `Nonconformance` gains `hint`; `Conversion(location, expected,
  returned_type, conversion, hint)`; conversions deduplicated within one value.
- [ ] Run the tests; commit `feat(output_conformance): apply ITL-895 decisions`.

## Task 3: Warning class and `_build_output_data`

- [ ] `OutputConversionWarning` in `errors.py`; `OutputTypeError` appends the hint.
- [ ] `_build_output_data`: raise on mismatch; `_warn_conversions` issues each warning from one
  line; then the existing eager conversion. Run tests; commit.

## Task 4: Docs, CHANGELOG, DESIGN_ISSUES

- [ ] `docs/concepts/function-pods.md`: the full table (accepted / converted with a warning /
  rejected), a runnable warning sample, the dedup and `-W error` notes.
- [ ] CHANGELOG `[0.3.0] — unreleased`: update the ITL-895 entry, mention ITL-901.
- [ ] DESIGN_ISSUES FO1: resolved for outputs; inputs open/deferred with the developer's reason.

## Task 5: Verify and finish

- [ ] 10,000-row script, sync and async, default filters: count warnings; record in this file.
- [ ] No-debug-only-parameters review of the whole diff.
- [ ] ruff check, ruff format --check, basedpyright (exit 0), `uv run pytest tests/`,
  `uv run pytest test-objective`, golden tests unchanged.
- [ ] Merge origin/main (PR #287 landed), rerun, push, PR body (Fixes ITL-895, Fixes ITL-901),
  mark ready, CI green, CodeRabbit rounds (review bodies too), worker_done.
