# ITL-897: columns whose restored Arrow type is null (review round 6)

Approved by the coordinator (2026-11-13) after a Copilot finding on `pandas_value_hash.py`.

**Defect (measured):** an all-null or empty `pd.ArrowDtype(decimal128)` column, and an
all-null or empty `object` column, raise `ValueError: Hashing data of type null` from the
hasher, though storage accepts them. Other all-null or empty kinds (Arrow string, timestamp,
int64, list, struct; Int64, string, categorical) hash and round-trip fine.

**Rule:** storage cannot restore the decimal type of an all-null column (it reads back as
`object [None]`), so a column whose restored Arrow type is null is digested as an all-null
`large_string` column of the same length (`_restored_form`, for values and read-backs alike).
Dtype descriptors: both decimal-null and object-null are `{"numpy": "object"}`, so they hash
EQUAL. Nothing that hashed before moves.

**Steps:** failing tests (strict round trip, Series and DataFrame column, equality of the two
shapes, matrix entries) -> fix -> golden additions (new entries only) -> docs section 10 +
stale `arrow_v1` DataFrame line (574) -> merge origin/main -> targeted tests (py3.12) -> one
push -> `gh pr edit 339 --add-reviewer @copilot` -> signed replies.
