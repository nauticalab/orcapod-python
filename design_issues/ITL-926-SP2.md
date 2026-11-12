## `src/orcapod/core/sources/spiraldb_table_source.py` — Spiral tables as a workshop source (ITL-926)

### SP2 — A Spiral table source was unverified on v0.3, and is limited for large or unusual tables
**Status:** in progress
**Severity:** high
**Issue:** ITL-926 (follow-ups: ITL-186 streaming source)

Re-verified against the EDI sample project (`edi-sample-v1-855632`, read-only) and a
disk-backed pyspiral fake (`tests/test_databases/fake_pyspiral.py`). The workshop path works:
a `SpiralDBTableSource` with a composite key, a `Join` of two Spiral sources, a function pod,
a Delta store, a re-run with no function calls, save and load, and the same hashes in a fresh
process. Findings, with repro scripts, are in `superpowers/specs/itl-926-live/FINDINGS.md`.

**Fixed:**

* Nested string, binary and list types are normalized to large variants on both read paths
  (ITL-40).
* Errors name the project and dataset.
* pyspiral is raised to 0.16.33.
* The default `source_id` is `<project id>/<dataset>/<table>` (approved hash change).
* Source building no longer calls `to_pylist()` on the whole table (byte-identical).
* Non-nullable columns stay non-nullable end to end: true only on the original pyspiral
  fake. SpiralDB keeps no non-null flag except on list items; fixed by the nullability record
  (SP3, ITL-936).

**Open (developer decisions pending):**

* Every column of the EDI sample tables is nullable, so a function pod over them must
  annotate `T | None`; recorded columns arrive as `T` (SP3, ITL-936).
* `fixed_size_list` columns cannot be hashed: not supported until Orcapod adopts starfix
  0.5.0 (ITL-910; starfix support is ITL-927). `map` columns fail the same way in starfix
  0.4.0 (none in the EDI tables).
* The whole table is read at construction, which rules out tables such as `spike_event`
  (8.1M rows); the streaming design is in
  `superpowers/specs/2026-11-11-itl-186-spiral-streaming-source-design.md`.
* Record IDs are positional; key-based IDs are approved for the streaming source work
  (ITL-186).
* `unit` (100 rows) takes about 35 s to build live, and the Spiral scan itself is slow
  (14.5 s for 3 rows); Orcapod's share is about 0.4 s. `scan_cost_probe.py` measures it.
* `PostgreSQLTableSource` and `SQLiteTableSource` still default `source_id` to the bare table
  name (to be filed separately).
