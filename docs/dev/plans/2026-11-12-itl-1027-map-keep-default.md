# ITL-1027 — map_tags / map_data keep unmapped columns by default

Findings (origin/main 003d2e82): `MapTags`/`MapData` classes already default `drop_unmapped=False`;
only `StreamBase.map_tags/map_data` and the protocol default `True`. `drop_unmapped` is in
`identity_structure` (and `to_config`). So: change the fluent methods + protocol to `False`.
A call that relied on the old fluent default and has hash-bearing pins gets `drop_unmapped=True`.

Steps: failing tests -> flip defaults (base.py, protocol, docstrings) -> golden case
`cases.py` map_data job fixture gets explicit True -> docs (operators.md note, sources.md) ->
changelog.d/ITL-1027.md -> design_issues/ITL-821-NP1.md item 4 fix note -> suite, lint, PR.
Notes: duplicate dead `MapTags` class in column_selection.py (out of scope, report).
