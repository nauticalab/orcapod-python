## `src/orcapod/core/data_function.py`

### P3 — `DataFunctionWrapper.__init__` passes no `version` to `DataFunctionBase`
**Status:** open
**Severity:** medium
`DataFunctionBase.__init__` requires a `version` string and parses it into
`_major_version`/`_minor_version`. `DataFunctionWrapper` calls `super().__init__(**kwargs)`
without a `version`, so it either crashes (no version in kwargs) or silently defaults to `"v0.0"`.
Those parsed fields are then shadowed by the delegating properties, making them dead state.
Options: pass the inner function's version through, or avoid calling the base version-parsing
logic entirely.
