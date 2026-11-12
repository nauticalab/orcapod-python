## `src/orcapod/core/operators/filters.py` — `PolarsFilter` identity and nullability (ITL-828)

### PF1 — An expression predicate could not be hashed, and a filter broke a join inside a job
**Status:** resolved

**PF1a.** `stream.polars_filter(pl.col("x") > 0).pipeline_hash()` raised `TypeError` (no
semantic handler for `pl.Expr`), so an expression filter failed upstream of a join and anywhere
inside a `PipelineJob`.

**Fix (PF1a):** `PolarsExprHandler` hashes a `pl.Expr` as `polars_expr_v1`: a strict parser
turns `Expr.meta.serialize(format="json")` into a tree in an Orcapod-owned grammar
(`hashing/semantic_hashing/polars_expr.py`), hashed as sha256 of its canonical JSON. Polars'
serialization is not hashed directly: it changed layout between 1.20, 1.30 and 1.36. Unknown
nodes and unsupported expressions (Python functions, multi-column selectors, aggregations,
`alias`, `when/then`, non-finite literals, `pl.Series` literals) raise `ValueError` naming the
expression, so a Polars layout change fails loudly instead of moving a hash. `predicates` is
normalized to a tuple. The developer confirmed the identity design (November 11, 2026).

Also fixed: inside a `PipelineJob` any `PolarsFilter` (keyword constraints too) upstream of a
`Join` raised `InputValidationError` (`str` vs `str | None`), because Polars returns every field
nullable and the filter restored types but not nullability.

**PF1b.** Data columns of a filtered stream also came back nullable, which broke the rule that
operators never change nullability, and gave a filtered row a different data hash from its
source row.

**Fix (PF1b):** the filter restores every column's input nullability (developer, November 11,
2026). A filtered row's data hash now equals its source row's. The pinned
`pipeline:polars_filter` row data hashes moved (approved); they are the cache keys of any
function pod downstream of a filter, so a 0.2 store recomputes those results once.
