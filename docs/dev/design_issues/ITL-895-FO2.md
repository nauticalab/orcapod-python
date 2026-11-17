## `src/orcapod/core/data_function.py` — returned values not checked against the declared type

### FO2 — Inputs built from Python values are not checked against their declared types
**Status:** open
**Severity:** medium
**Issue:** ITL-895

Inputs built from Python values (`DictSource` and `ListSource` with a declared schema,
`Datagram`, `Data` and `Tag` from dicts, `Datagram.update` / `with_columns`) still go through
the unchecked pyarrow conversion and truncate the same way (`1.5` for an `int` column is
stored as `1`). Evidence: `docs/dev/specs/itl-895-float-to-int/04_input_sites.py`.
Deferred by the developer (2026-11-11): "generally a big implementation and given the nature
of Python, not sure if it's something I'd care to put explicit check".
