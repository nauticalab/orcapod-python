# ITL-895 Function Output Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

## Status log (for a successor)

- 2026-11-11: Step 1 (root cause) is done. See `superpowers/specs/itl-895-float-to-int/FINDINGS.md`;
  commit `07037243`.
- 2026-11-11: Coordinator reply. The developer decides D1-D7, the option and the input-side
  scope; the coordinator recommends (c). For now:
  - Build the mechanism of (c) for **function outputs only**, with the conformance rule as one
    table so that each of D1-D7 is a one-line switch.
  - Every **undecided** D keeps today's behaviour.
  - Decided cases: D4 (`None` for a non-optional output: reject). The coordinator also listed
    datetime -> date among the lossy pairs to fail, so D6 is reject.
  - Do not change `DictSource` / `ListSource` / `Datagram` input behaviour.
  - Keep the PR a draft.
  - Report the out-of-scope items in worker_done.
- 2026-11-11: Tasks 1-6 done. Draft PR #299 is open, and CI is green (`run-tests`, objective
  tests, ruff, typecheck). `test-objective/unit/test_data_function.py::test_call_returns_none_propagates`
  encoded the old `None` behaviour; its function now declares `-> int | None`, and a sibling
  test checks the rejection. CodeRabbit skipped the draft.
  **Next:** when the developer decides D1-D7, flip the entries in `OUTPUT_CONFORMANCE`
  (`semantic_types/output_conformance.py`) and the matching tests in
  `TestUndecidedCasesKeepTodaysBehaviour`. Extend the check to inputs if they are in scope.
  Merge main after PR #287 / PR #289 land; PR #289's ndarray check in `_build_output_data` can
  then raise `OutputTypeError`. Mark the PR ready, then run the CodeRabbit rounds.

**Goal:** A function pod rejects a returned value that does not conform to its declared output
type, before anything is stored. The rejection names the pod, the output, the declared type
and the value's type. It is a row failure under `error_policy`. The output `Data` holds exactly
what is stored and hashed.

**Architecture:** A new module, `semantic_types/output_conformance.py`, holds the rule. It has
one frozen `ConformanceRule` table with a field per D-decision, plus `find_nonconformance(value,
declared, rule)`, which walks orcapod's type grammar (primitives, numpy scalar types, `list`,
`tuple`, `dict`, `Optional`, dataclass fields, date/time) and returns the first mismatch.
Logical types and anything else stay with their own converters.

`PythonDataFunction._build_output_data` runs the check, then converts the outputs to Arrow
**eagerly** through the data context's type converter, and builds the `Data` from that table.
So `as_dict()`, `as_table()`, the store and the hash all see the same values. Any failure,
from the check or from the conversion, raises the new `orcapod.errors.OutputTypeError`.
`OutputTypeError` subclasses both `TypeError` and `ValueError`, so existing handlers and tests
for the conversion's `ValueError`s still apply. It is raised inside the data function's `call`,
so every orchestrator already treats it as a function failure (row failure under
`error_policy`, `on_data_crash`).

**Tech Stack:** Python 3.11-3.13, pyarrow, pytest, `uv run`.

## Files

- Create: `src/orcapod/semantic_types/output_conformance.py`. Rule table and checker.
- Modify: `src/orcapod/errors.py`. Add `OutputTypeError`.
- Modify: `src/orcapod/core/data_function.py`. In `_build_output_data` (line ~569), add the
  check and the eager conversion.
- Create: `tests/test_core/function_pod/test_output_conformance.py`. Public-interface
  regressions.
- Modify: `docs/concepts/function-pods.md`, `CHANGELOG.md` (new `[0.3.0] — unreleased`
  section), `DESIGN_ISSUES.md` (one appended entry).

## Task 1: Failing regression tests (public interface)

**Files:** Create `tests/test_core/function_pod/test_output_conformance.py`

- [ ] Write the tests (full code in the file). They cover:
  - Standalone pod: each lossy pair raises `OutputTypeError` from `iter_data()`, and the
    message names the pod, the output, the declared type and the value's type. The pairs are
    int<-1.5, int<-np.float64(1.5), int<-Decimal('1.5'), int<-Fraction(3, 2), np.int8<-1.5,
    `int | None`<-1.5, list[int]<-[1, 1.5], dict[str, int]<-{'a': 1.5}, tuple[int, int]<-(1, 1.5),
    date<-1.5, datetime<-1.5, date<-datetime, str<-b'x', bytes<-'x', int<-None, float<-None,
    list[int]<-None, and a dataclass field int<-1.5.
  - The standalone `as_table()` over mixed results raises `OutputTypeError`, not the
    "Complex unions" `ValueError`.
  - `function_pod(result_database=...)`: raises, and nothing is stored.
  - `PipelineJob` (sync and async orchestrators x in-memory and Delta stores): the run
    completes, the failed row is absent, the conforming rows are stored, and an observer's
    `on_data_crash` receives the `OutputTypeError`. With `error_policy="fail_fast"`, the sync
    orchestrator raises `OutputTypeError`.
  - Divergence: in the chain `up` (`-> float`, returns the int `1`, accepted under D1 until
    decided) into `down(y: float)`, `down` receives the stored value (`1.0`, a `float`)
    standalone and on both orchestrators. `as_dict()` equals `as_table()`.
  - Undecided cases stay accepted with today's stored value: np.int64 for int, 1 for float,
    True for float, (1, 2) for list[int], 0.1 for np.float32, np.bool_ for bool.
  - Conforming outputs are unchanged: the content hash of a pod output equals that of
    `Data({'y': 1}, python_schema={'y': int})`.
- [ ] Run `uv run pytest tests/test_core/function_pod/test_output_conformance.py -q`. Expected:
  the rejection tests FAIL (no error raised, value truncated); the accepted-case tests pass.
- [ ] Commit `test(function_pod): failing regressions for output type conformance (ITL-895)`.

## Task 2: `OutputTypeError`

**Files:** Modify `src/orcapod/errors.py`

```python
class OutputTypeError(TypeError, ValueError):
    """A function returned a value that does not match its declared output type (ITL-895).

    Orcapod never converts a returned value implicitly: a ``float`` for an output declared
    ``int`` would otherwise be truncated by pyarrow. Raised by the data function before
    anything is stored or hashed, so a pipeline treats it like any other function failure
    (a row failure under ``error_policy``).

    Subclasses both ``TypeError`` and ``ValueError``: a type mismatch is a ``TypeError``,
    and conversion failures the error also wraps (an integer out of range, a naive
    datetime under the strict timezone policy) were ``ValueError`` before.

    Attributes:
        function_name: Name of the function pod's function.
        output_key: Name of the output column.
        declared_type: The declared output type.
        value: The value the function returned for that output.
    """
```

The constructor takes `function_name`, `output_key`, `declared_type`, `value` and `detail`, and
builds the message: `Function 'f' returned float (1.5) for output 'y', which is declared int:
<detail>. Orcapod does not convert types implicitly; convert the value in the function.`

## Task 3: Conformance rule

**Files:** Create `src/orcapod/semantic_types/output_conformance.py`

- `ConformanceRule` is a frozen dataclass with these fields: `int_for_float` (D1),
  `numpy_scalar_same_kind` (D2), `bool_for_float` (D3), `none_for_non_optional` (D4),
  `sequence_for_list` (D5), `datetime_for_date` (D6) and `float_for_float32` (D7).
- `OUTPUT_CONFORMANCE` is the one table, one line per decision. D1, D2, D3, D5 and D7 are True
  (today's behaviour, undecided). D4 and D6 are False (decided).
- `find_nonconformance(value, declared, rule) -> Nonconformance | None`, where
  `Nonconformance` has `path`, `expected` and `value`.
  - `Optional` / `X | None`: `None` passes; otherwise check `X`.
  - `None` for a non-optional type: rejected unless D4.
  - `bool`: exact `bool`, or a numpy bool (D2).
  - `int`: an `int` that is not a `bool` (subclasses such as `IntEnum` keep their value), or a
    numpy integer (D2). Any float, Decimal, Fraction or numpy float is rejected.
  - numpy integer dtypes (`np.int8`, ...): Python `int`, the declared numpy type, or another
    numpy integer (D2). The range is checked by the conversion.
  - `float` / `np.float64`: a `float` (a numpy float64 is a `float` subclass); a numpy float of
    another width (D2); an `int` (D1); a numpy integer (D1 and D2); a `bool` (D3).
  - `np.float32`: an `np.float32`; a Python `float` or another numpy float (D7); an `int`
    (D1); a `bool` (D3).
  - `str`: an `str`. `bytes`: a `bytes`.
  - `date`: a `date` that is not a `datetime` (D6). `datetime`: a `datetime`.
    `time` / `timedelta`: by `isinstance`.
  - `list[T]`: a `list`, or a `tuple` (D5), each element checked against `T`. Any other
    non-str iterable is passed through unchecked under D5 (today) and rejected otherwise.
  - `tuple[T1, ..., Tn]`: a `tuple`, or a `list` (D5), of length n, each element checked.
  - `dict[K, V]`: a `Mapping`, each key and value checked.
  - A dataclass declared type, when the value is an instance: each field checked against
    `get_type_hints`.
  - Anything else (logical types, pydantic, `Any`, `Literal`, complex unions): `None`. Those
    are left to their own converter.

## Task 4: Check and eager conversion in `_build_output_data`

**Files:** Modify `src/orcapod/core/data_function.py`

```python
output_data = parse_function_outputs(self._output_keys, values)
schema = self.output_data_schema
for key, value in output_data.items():
    problem = find_nonconformance(value, schema[key])
    if problem is not None:
        raise OutputTypeError(self.canonical_function_name, key, schema[key], value, problem.describe())
...
if not output_data:
    return Data(output_data, ...)  # unchanged: no columns to convert
try:
    table = self.data_context.type_converter.python_dicts_to_arrow_table([output_data], python_schema=schema)
except Exception as exc:
    key = <first key whose single-column conversion fails, else all keys>
    raise OutputTypeError(..., detail=f"{type(exc).__name__}: {exc}") from exc
return Data(table, source_info=source_info, record_uuid=new_record_uuid, data_context=self.data_context)
```

- [ ] Run the Task 1 tests. Expected: all pass.
- [ ] Run `uv run pytest tests/test_core tests/test_pipeline -q -x`, then the golden suite
  `uv run pytest tests/test_hashing/golden -q`. Expected: green, golden unchanged (644).
- [ ] Commit `fix(data_function): reject function outputs that do not match the declared type (ITL-895)`.

## Task 5: Docs, CHANGELOG, DESIGN_ISSUES

- [ ] `docs/concepts/function-pods.md`: a new section, "Return values must match the declared
  type". It gives a runnable sample, `# ...` output comments, and the list of what is accepted
  while D1-D7 are open.
- [ ] `CHANGELOG.md`: a `## [0.3.0] — unreleased` section with `### Fixed` above `[0.2.0]`.
- [ ] `DESIGN_ISSUES.md`: one appended entry, `FO1`, status `in progress` (it becomes
  `resolved` when D1-D7 and the input-side scope are decided).
- [ ] `uv run pytest tests/test_docs -q`. Commit `docs: ...`.

## Task 6: Verification and PR

- [ ] ruff check, ruff format --check and basedpyright (exit 0). Full `uv run pytest tests/`
  on 3.11, 3.12 and 3.13.
- [ ] Merge origin/main, rerun, push, and open a **draft** PR ("Fixes ITL-895"). Then run
  `orca linear attach` and `orca worktree set --pr`.

## Self-review

- Spec coverage: lossy pairs (Task 1); None (D4); divergence; row failure on sync and async,
  in-memory and Delta; pod-level error naming; undecided defaults; no hash change (golden);
  inputs untouched (no converter change). Covered.
- Known gap, by design until the developer decides: logical types (Path, UUID, ndarray,
  pydantic) keep their own conversion rules. PR #289 adds the ndarray check.
