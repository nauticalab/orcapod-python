## Hash-site audit — wrong hashes fixed before v0.3 (ITL-942)

### WH2 — A value-hashed logical type lost its identity in a column with no value
**Status:** resolved
**Severity:** high
**Issue:** ITL-967 (follow-up to ITL-942, finding I; found by CodeRabbit on PR #323)

ITL-942's fix for finding I made the Arrow hasher derive the hashed type from the declared
type alone. For a logical type whose values are hashed by value (UUID, `File`, `Directory`,
array, `DataFrame`, `Series`, and `list` / `set` of them) the hashed type was a bare
`large_binary`, and the logical type's identifier was only in each cell token. A column with
no value, zero rows or nulls only, has no token, so it hashed like a plain `large_binary`
column and like every other value-hashed logical type. Measured on the combined v0.3 branch:
the table hashes of a zero-row UUID, `File`, array and plain binary column were all equal,
where `main` and the branch before ITL-942 gave four different hashes.

**Decision** (developer through the coordinator, November 12, 2026): two different logical
types must not share a hash, with or without rows.

**Fix:** `SemanticHashingVisitor.hashed_field` replaces the field by a `large_binary` field
that keeps the logical type's whole identifier as its `ARROW:extension:name` and
`ARROW:extension:metadata` field metadata. `StarfixArrowHasher` already hashes those keys
(starfix `include_metadata`) at every nesting level: top level, inside lists, structs and
fixed-size lists (checked against starfix 0.5.0). The hashed field still depends on the
declared field only, so finding I's rule holds. The cell token is unchanged.

**What moved:** the Arrow hash of every table with a value-hashed logical-type column, with
data too. Path columns are not value-hashed and do not move.
