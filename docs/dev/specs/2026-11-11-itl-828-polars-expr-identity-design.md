# ITL-828: identity of a Polars expression predicate (design proposal)

Status: **approved** — the coordinator approved Option C provisionally and the developer
confirmed it (November 11, 2026).

## Defect (reproduced on main b197544e)

```python
a = DictSource(data=[{"id": "a", "x": 1}], tag_columns=["id"])
a.polars_filter(pl.col("x") > 0).pipeline_hash()
# TypeError: SemanticAwarePythonHasher (strict): no implementation of
#   PythonTypeHandlerProtocol registered for type 'polars.expr.expr.Expr' ...
```

`PolarsFilter.identity_structure()` is `(class name, predicates, constraints)`; the
predicates tuple reaches the semantic hasher, which has no handler for `pl.Expr`.

## Experiments

Scripts: `superpowers/specs/itl-828-experiments/exp1.py`, `exp2.py`. Each was run with
`uv run --no-project --with polars==X` for X in 1.20.0, 1.30.0, 1.36.1 (project minimum is
`>=1.36.0`), 1.38.1, 1.41.2 (locked), 1.44.2 (latest on PyPI), each under
`PYTHONHASHSEED=0` and `123`.

### Candidate 1: `expr.meta.serialize(format="binary")` (msgpack in 1.36+)

* Deterministic across processes for every non-UDF expression tested.
* Identical bytes across 1.36.1, 1.38.1, 1.41.2 and 1.44.2 for every non-UDF expression.
* **Changed** between 1.20 -> 1.30 -> 1.36 for almost every expression (`Literal{Int}` became
  `Literal{Dyn{Int}}`; scalar literal layout changed twice; function `options` blocks
  dropped; `Columns` became `Selector{ByName}`; `Wildcard` moved under `Selector`). Polars
  documents serialization as not stable across versions.
* Equal expressions written differently serialize identically: `pl.col("x") > 0`,
  `pl.col("x").gt(0)`, `pl.col("x") > pl.lit(0)`, `0 < pl.col("x")` (Python's reflected
  operator).
* Distinguishes `> 0` (dyn int), `> 0.0` (dyn float), `> pl.lit(0, dtype=pl.Int8)` (typed),
  column names, dates (days), datetimes (value, unit, tz).
* UDFs (`map_elements`, `map_batches`): the bytes embed a cloudpickle of the lambda and
  **differ between processes** (and fail outright when cloudpickle is not installed).
* Verdict: the hash would silently move on a future Polars layout change. Not acceptable as
  a permanent public hash on its own.

### Candidate 2: `str(expr)`

* Deterministic, but **lossy**: `pl.lit(0.1, dtype=Float32)` and `pl.lit(0.1, dtype=Float64)`
  both print `(0.1)`; datetime literals lose their unit; `pl.lit(0, dtype=Int8)` prints `(0)`.
  Long list literals are truncated by the display code.
* **Changed** across versions (`cols(["x","y"])` -> `cs.by_name(...)`, `is_in([Series])` ->
  `is_in([["a","b"]])`, `0.5.strict_cast(Float32)` -> `0.5`).
* Verdict: rejected (collisions and drift).

### Candidate 3: `expr.meta.serialize(format="json")` hashed as-is

* Same stability profile as binary (identical within 1.36–1.44, changed before).
* Additionally lossy for non-finite floats: NaN, inf and -inf all serialize as `null`.
* List literals (`is_in([...])`) are embedded as Arrow IPC bytes.
* Verdict: rejected as the hash input itself, but it is the only public, structured view of
  the expression tree, so it is the right **input to a parser** (Candidate 5).

### Candidate 4: a tree walk via `expr.meta` methods

`meta.root_names`, `meta.pop`, `meta.output_name`, `meta.is_column` etc. expose inputs and
names but not the operator or function of a node, so a lossless walk is impossible.
`meta.tree_format` is display text (Candidate 2's problems). Rejected.

### Candidate 5 (recommended): Orcapod's own canonical encoding of a supported subset

Parse the JSON serialization with a **strict** parser into a grammar Orcapod owns, and hash
that. A Polars release that changes the JSON layout makes the parser raise a clear error
(never a silent hash move); the fix is to teach the parser the new layout, mapping it onto
the same grammar, so the hash is unchanged. Anything outside the subset (UDFs included) is
rejected with a `ValueError` naming the expression.

## Proposed definition: label `polars_expr_v1`

```text
ContentHash("polars_expr_v1", sha256(canonical_json(node)))
canonical_json = json.dumps(node, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
```

Grammar (v1):

```text
node    := {"col": <name>}
         | {"lit": <literal>}
         | {"binary": [<op>, <node>, <node>]}
         | {"fn": [<fname>, <options>, [<node>, ...]]}
         | {"cast": [<dtype>, <"strict"|"non_strict">, <node>]}
op      := "eq" | "ne" | "eq_missing" | "ne_missing" | "lt" | "le" | "gt" | "ge"
         | "and" | "or" | "xor" | "add" | "sub" | "mul" | "truediv" | "floordiv" | "mod"
fname   := "not" | "is_null" | "is_not_null" | "is_nan" | "is_not_nan"
         | "is_in" {"nulls_equal": bool} | "is_between" {"closed": "both"|"left"|"right"|"none"}
         | "all_horizontal" | "any_horizontal" | "negate"
         | "str.contains" {"literal": bool, "strict": bool}
         | "str.starts_with" | "str.ends_with"
literal := {"dyn_int": "<decimal string>"}        Python int literal (Polars adapts its type)
         | {"dyn_float": "<float.hex()>"}         finite Python float literal
         | {"null": null} | {"bool": true|false} | {"string": <str>}
         | {"int8"|...|"uint64": "<decimal string>"}
         | {"float32"|"float64": "<float.hex()>"}  float32 rounded to f32 first
         | {"decimal": ["<unscaled int>", <precision>, <scale>]}
         | {"binary": "<hex>"}
         | {"date": <days>} | {"time": <ns>}
         | {"datetime": [<value>, <"ns"|"us"|"ms">, <tz or null>]}
         | {"duration": [<value>, <"ns"|"us"|"ms">]}
         | {"list": [<dtype>, [<element literal or null>, ...]]}   is_in values (decoded from IPC)
dtype   := the PR #286 dtype descriptor grammar ("int64", {"datetime": [unit, tz]}, ...)
```

Integers are decimal strings so i128 literals (`2**70`) survive JSON. Floats use
`float.hex()` so `-0.0` and `0.0` differ and precision is exact.

Rejected in v1, each with `ValueError` naming `str(expr)` and the unsupported part:
UDFs (`map_elements`, `map_batches`, any `AnonymousFunction`/python_udf node), multi-column
selectors (`pl.col("x", "y")`, `pl.all()`, regex columns, `pl.exclude`, `cs.*`, dtype
selection: their meaning depends on the schema), aggregations and windows (`mean`, `over`),
`alias`, `when/then/otherwise`, non-finite float literals (the JSON form cannot tell NaN from
inf), and any node, key or value shape the parser does not know.

Extending the subset later is additive (no label change): previously supported expressions
keep their bytes, previously rejected ones never had a hash. Only a change to the encoding of
an already supported construct needs `polars_expr_v2`.

### Equalities this gives

Equal: `pl.col("x") > 0`, `pl.col("x").gt(0)`, `pl.col("x") > pl.lit(0)`, `0 < pl.col("x")`.
Different: `> 0` vs `> 0.0` vs `> pl.lit(0, dtype=pl.Int8)`; `a & b` vs `b & a` (the
expression as written; no commutativity normalization, consistent with type-strict
predictability).

## PolarsFilter pipeline identity

`identity_structure()` keeps its shape: `("PolarsFilter", predicates, constraints)`. Each
`pl.Expr` predicate becomes the leaf `"polars_expr_v1:<hex>"` (registered handler, same
mechanism as `ndarray`/`polars_dataframe_v1`). Keyword constraints are untouched, so the
golden `pipeline/polars_filter` entries (keyword-only) do not move. Predicate order is kept
(`filter(a, b)` and `filter(b, a)` hash differently).

Decided: `predicates` is normalized to a `tuple` in `__init__`, so
`PolarsFilter(predicates=[e])` and `stream.polars_filter(e)` hash the same (a list and a
tuple expand differently). This moves no golden value (default is `()`).
