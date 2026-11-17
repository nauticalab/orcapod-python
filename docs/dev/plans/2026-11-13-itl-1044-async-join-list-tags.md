# ITL-1044: async Join with list/struct-valued shared tags

## Defect
`Join._binary_streaming_join` indexes rows by `tuple(tag.as_dict()[k])`: a list value is unhashable (TypeError).
Measured side effects of the same code: top-level null primitive keys match each other (sync: never), NaN != NaN (sync: equal).

## Measured sync (Polars inner join) equality, per key column
- list order matters; `[]` != null list; `[None]` == `[None]`; `[1,None]` == `[1,None]`
- NaN == NaN (also inside lists); 0.0 == -0.0
- struct: field-wise, null struct != struct of nulls; null struct == null struct
- top-level null of a primitive: never matches. Top-level null of a list/struct: matches another null (Polars quirk, measured).

## Rule
Key = frozen values read from the row's Arrow column (storage of a logical column), not as_dict:
dict -> tuple of (name, frozen), list -> tuple, NaN -> shared sentinel, nested top-level null -> NULL sentinel,
primitive top-level null -> row has no key (never matches). Types are checked equal first, so frozen values of different types never meet.
No repr/str. Shared helper module `operators/match_keys.py`, used by Join and SemiJoin (SemiJoin's _freeze moves there).

## Other operators
SemiJoin: already freezes; verify equivalence on null list. MergeJoin/GroupBy/Batch: barrier async over tables, check.

## Tests
Equivalence cases in test_async_sync_equivalence.py, unordered inputs. Regression first.
