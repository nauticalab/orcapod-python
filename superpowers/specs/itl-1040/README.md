# ITL-1040 measurement: appending to a pipeline table an async run wrote before the fix

Evidence for the ITL-1040 decision. These scripts are not tests, and the test run does not
collect them.

`measure.py` runs each step as a subcommand (see its docstring). The job is source (`k`; `x`,
`s`) → `times_ten(x)`; the pod's automatic select drops `s`. An in-memory store is carried
from one build to another by dumping its tables to Arrow files and reloading them.

- `phase1 <dir>` (run on `main` before the fix): async runs write rows `r0..r3` to Delta
  stores and to a dumped in-memory store. It also measures the baseline on `main`: a sync run
  appending rows `r4, r5` to an async-written table.
- `phase1b` (run on `main` before the fix): the reverse baseline, an async run appending to
  a sync-written table.
- `phase2 <dir>` (run on the fixed build): async and sync runs append `r4, r5` to the
  tables phase 1 wrote.
- `operator_node`: a cached operator node downstream of an async select. Are its stored
  records the same after a sync and an async run?

Results (November 12, 2026):

| Store | Who appends | Result |
|---|---|---|
| Delta | fixed build, async or sync | succeeds; the old `_source_s` column stays, old rows keep their tokens, new rows get NULL |
| in memory | fixed build, async or sync | `StoreWriteError` (schema mismatch); cannot happen across processes, since an in-memory store dies with its process |
| Delta | `main`, sync after async | succeeds; new rows get NULL in `_source_s` (the same state as the fixed build) |
| Delta | `main`, async after sync | succeeds; the OLD rows get NULL in `_source_s` |
| in memory | `main`, sync after async, or async after sync | fails (`StoreWriteError` / `ExceptionGroup`) |
