# ITL-1067: all_info inside a ColumnConfig is honoured

Bug: `ColumnConfig.handle_config(columns, all_info)` expands only the keyword; `columns={"all_info": True}`
and `ColumnConfig(all_info=True)` are silently ignored (reproduced on origin/main 5c7a058f).

Plan
1. Tests first (tests/test_types_column_config_all_info.py), public interface: source, function pod stream,
   operator stream, job function node, job operator node; as_table / output_schema / head with both embedded
   spellings equal the keyword; `{"all_info": True, "meta": False}` equals all (docstring: overrides others);
   keyword False + embedded True: embedded wins.
2. Fix in `ColumnConfig.handle_config` only: normalize, then `if column_config.all_info: return cls.all()`.
   Resolve the TODO.
3. Audit readers of `.all_info` and builders of ColumnConfig (list in PR body). No hash/stored change.
4. changelog.d/ITL-1067.md, design_issues entry, docs check.

Notes
