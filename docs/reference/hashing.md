# Hashing Reference

This document is a developer-facing reference for every hash computation site in the
orcapod-python framework. For conceptual background on the two identity chains
(`content_hash` and `pipeline_hash`), see [Identity & Hashing](../concepts/identity.md).

---

## Hash Site Index

The table below summarises all 15 hash computation sites across the six usage groups
documented in the sections that follow. (Site 6b is numbered alongside site 6 rather than
appended, because it is a variant of the same system tag mechanism.)

| # | Site | Algorithm | Output format | One-line guarantee |
|---|------|-----------|---------------|--------------------|
| 1 | `content_hash()` | `SemanticAwarePythonHasher` → JSON + SHA-256 | `ContentHash` | Unique per semantic content of the object; data-inclusive |
| 2 | `pipeline_hash()` | `SemanticAwarePythonHasher` → JSON + SHA-256 | `ContentHash` | Unique per pipeline topology + schema; data-exclusive |
| 3 | Schema hash (system tag column naming) | `SemanticAwarePythonHasher.hash_object((tag_schema, data_schema)).to_hex(n)` | Truncated hex `str` | Unique per `(tag_schema, data_schema)` pair; embedded in system tag column names |
| 4 | Default `source_id` | `StarfixArrowHasher.hash_table(table).to_hex(n)` | Truncated hex `str` | Unique per raw table content; used as source identifier when none is provided |
| 5 | Per-row `record_id` (system tag value) | `uuid.uuid5(NAMESPACE, f"{source_id}::{provenance_token}")` | `bytes` (16, UUID v5) | Deterministic per `(source_id, row_identity)`; stable across re-runs of the same source |
| 6 | Join system tag suffix | `stream.pipeline_hash().to_hex(n)` + `:{idx}` appended to column name | Column name suffix | Unique per `(input topology, canonical join position)`; encodes full join lineage in column name |
| 6b | Aggregating operator system tag fold | `uuid.uuid5(NAMESPACE, joined_member_hex)` for `record_id`; `combine_hashes(*member_values)` otherwise | `bytes` (16) / hex `str`, plus a column name suffix | Deterministic per ordered member set; keeps system tags scalar through a many-to-one reduction |
| 7 | `compute_base_entry_id()` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content)` across all recomputation attempts |
| 8 | `compute_pipeline_entry_id()` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL + recomputation_index)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content, recomputation attempt)` |
| 9 | Side-effect `record_id` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL + recomputation_index=0)` | `bytes` | Unique per `(tag lineage, input_data content)`; pod-version scoped via table path (`uri` + `pipeline_hash()`) |
| 10 | `invocation_hash` | `f"{serialize(pipeline_hash_ch)}::{serialize(record_id_hash_ch)}"` | `str` | Unique per `(pod topology, tag lineage, input_data content)`; pod-version scoped via table path |
| 11 | Output schema hash | `SemanticAwarePythonHasher.hash_object(output_data_schema).to_string()` | `str` (ContentHash `.to_string()`) | Unique per output schema definition; embedded in function URI |
| 12 | `run_id` | `uuid.uuid4().hex[:16]` | 16-char hex `str` | Non-deterministic; unique per pipeline execution |
| 13 | `snapshot_hash` | `hashlib.sha256(sorted_leaf_content_hash_strings joined by newline).hexdigest()[:16]` | 16-char hex `str` | Unique per `(DAG leaf topology + data state)` at run time |
| 14 | `datagram_uuid` | `uuid7()` normalised to `stdlib uuid.UUID` | `uuid.UUID` | Unique per datagram instance; time-ordered; not a content hash |

---

## 7. Worked Example

The pipeline below exercises hash sites 1--14; it contains no aggregating operator, so site 6b
is not covered. `source_id` values are set explicitly so
every content hash is reproducible across runs. Run the script to regenerate the values
in this section:

```
uv run python superpowers/scripts/hash_audit_example.py
```

### Example pipeline

```python
from orcapod.core.function_pod import function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.operators.join import Join
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.side_effects import SideEffectPod, InvocationContext

scores = DictSource(
    [{"student": "alice", "math": 90}, {"student": "bob", "math": 75}],
    tag_columns=["student"],
    source_id="scores_v1",
)
attendance = DictSource(
    [{"student": "alice", "days": 180}, {"student": "bob", "days": 160}],
    tag_columns=["student"],
    source_id="attendance_v1",
)

joined = Join()(scores, attendance)      # sites 3–6

@function_pod(output_keys="grade")
def grade(math: int, days: int) -> float:
    return math * 0.7 + days / 180.0 * 30.0

pod  = grade.pod
node = FunctionJobNode(pod, joined, pipeline_database=InMemoryArrowDatabase())

captured: list[InvocationContext] = []

def _log_fn(ctx: InvocationContext, **kwargs) -> None:
    captured.append(ctx)

side_pod = SideEffectPod(_log_fn, ctx_arg_name="ctx")
```

### Concrete hash values

#### §1 — Framework Object Identity

| Object | `content_hash()` | `pipeline_hash()` |
|--------|-----------------|-------------------|
| `scores` | `semantic_v0.1:d8e191c5df8ff50457dd90406e3ac864c529bc5fca56df732b431a7ad782662c` | `semantic_v0.1:43733ff397db1413d574830fb6b2e45891b31a49f5fa11237836713a8b24eebb` |
| `attendance` | `semantic_v0.1:0fec51648c526f6e2c3a4adf6fac82a0d7994a3bb044f6d55bd02a0c9bbccda3` | `semantic_v0.1:9e1bfae16816bc68bdc5f1ab79a3f94a1604f314ace3feadbeb4894c17b0e7cd` |
| `joined` | `semantic_v0.1:c2ac84fed5108af40500e7581c4ec596627d7ce767630a3dead48144c29bc16d` | `semantic_v0.1:beaeba6e85f92932b21beebd6389ddacd804e7e43ced0af704c2b007208cf4cc` |
| `pod` | `semantic_v0.1:02e1c1becdbcca36e978a7edcc052c85858dd6e3c4bfbb21805bc5858e88f0f4` | `semantic_v0.1:02e1c1becdbcca36e978a7edcc052c85858dd6e3c4bfbb21805bc5858e88f0f4` |
| `node` | `semantic_v0.1:d8ca22587cb8bf09ca1a73db6ad24f96f4f21c01d4dc834f6f133262eae7746d` | `semantic_v0.1:a8cf2c871e0a6524c7d6b29a2a74ebfd6ef3e7e0255042b99bdb36166431a1b6` |

> `pod.content_hash() == pod.pipeline_hash()` because `grade` has no `ctx_arg_name`, so
> `identity_structure()` and `pipeline_identity_structure()` return the same value.
>
> `scores.pipeline_hash() ≠ attendance.pipeline_hash()` even though both sources have
> `tag_columns=["student"]` — their data schemas differ (`{math: int}` vs `{days: int}`).

#### §2 — Source Provenance & System Tags

| Site | Item | Value |
|------|------|-------|
| 3 | Schema hash (scores) | `43733ff397db1413d574830fb6b2e45891b31a49f5fa11237836713a8b24eebb` |
| 4 | `scores.source_id` | `"scores_v1"` (explicit; no Arrow hash computed) |
| 5 | `alice` `record_id` bytes (hex) | `e2d15e48e9f05f0f81e25d8e9f120b0a` |
| 5 | `bob` `record_id` bytes (hex) | `0d565e02049f5913bf45a5711e71ff52` |

System tag column names on `scores` (sites 3 + 5):

```
_tag_source_id::43733ff397db1413d574830fb6b2e45891b31a49f5fa11237836713a8b24eebb
_tag_record_id::43733ff397db1413d574830fb6b2e45891b31a49f5fa11237836713a8b24eebb
```

Post-join system tag column names (site 6):

```
_tag_source_id::43733ff397db1413d574830fb6b2e45891b31a49f5fa11237836713a8b24eebb::43733ff397db1413d574830fb6b2e45891b31a49f5fa11237836713a8b24eebb:0
_tag_record_id::43733ff397db1413d574830fb6b2e45891b31a49f5fa11237836713a8b24eebb::43733ff397db1413d574830fb6b2e45891b31a49f5fa11237836713a8b24eebb:0
_tag_source_id::9e1bfae16816bc68bdc5f1ab79a3f94a1604f314ace3feadbeb4894c17b0e7cd::9e1bfae16816bc68bdc5f1ab79a3f94a1604f314ace3feadbeb4894c17b0e7cd:1
_tag_record_id::9e1bfae16816bc68bdc5f1ab79a3f94a1604f314ace3feadbeb4894c17b0e7cd::9e1bfae16816bc68bdc5f1ab79a3f94a1604f314ace3feadbeb4894c17b0e7cd:1
```

The suffix `::43733ff...:0` on `scores` columns encodes the canonical join position (`0`); the
suffix `::9e1bfae...:1` on `attendance` columns encodes position `1`. The pipeline hash in
each suffix is the stream's `pipeline_hash()` (sites 1b and 6).

#### §3 — Pipeline DB Entry Keys

| Site | Row | Value (hex) |
|------|-----|-------------|
| 7 | `alice` `base_entry_id` | `6172726f775f76302e313a00000113f967f1187ac2982e65b6afbf324d264d50a6efe3357a35f09b8a2b32454d61` |
| 7 | `bob` `base_entry_id` | `6172726f775f76302e313a000001b6162a6faa76b5e065e89849acc09f41edfb5bb1a7c7ea626fd2920de6046586` |
| 8 | `alice` `pipeline_entry_id` (idx=0) | `6172726f775f76302e313a0000016d7853d1b6b3dc62063d3982911c3f18b8a719e63cfd41429fff970137c52d9c` |
| 8 | `bob` `pipeline_entry_id` (idx=0) | `6172726f775f76302e313a000001244dc0d59c23ec5d4a19b972a2a98f410054da09f839115841cefdf7f4851f63` |

> The hex prefix `6172726f775f76302e31` decodes to the ASCII string `arrow_v0.1` — the
> `method` portion of the `b"{method}:{digest}"` format described in §3.

#### §4 — Side-Effect Record ID & Invocation Hash

| Site | Row | Value |
|------|-----|-------|
| 10 | `alice` `invocation_hash` | `semantic_v0.1:1f71386f47538f7b5ca97554104469650b8ec5103a824cbe811a7b776913da91::arrow_v0.1:0000016d7853d1b6b3dc62063d3982911c3f18b8a719e63cfd41429fff970137c52d9c` |
| 10 | `bob` `invocation_hash` | `semantic_v0.1:1f71386f47538f7b5ca97554104469650b8ec5103a824cbe811a7b776913da91::arrow_v0.1:000001244dc0d59c23ec5d4a19b972a2a98f410054da09f839115841cefdf7f4851f63` |

> Both rows share the same `semantic_v0.1:1f71386f...` component — this is the
> `SideEffectPodStream.pipeline_hash()`, which depends only on the pod and its upstream
> topology, not on row content. The `arrow_v0.1:...` component is the per-row `record_id`
> from site 9, which differs per row.

#### §5 — Data Function URI Hash

| Site | Item | Value |
|------|------|-------|
| 11 | `data_function.uri` | `('grade', 'semantic_v0.1:e44414c648e8997ded6bface49d8ea3ac790182641cc78c034f113097fec751f', 'v0', 'python.function.v0')` |
| 11 | `output_schema_hash` (uri[1]) | `semantic_v0.1:e44414c648e8997ded6bface49d8ea3ac790182641cc78c034f113097fec751f` |

#### §6 — Pipeline Run Identity

| Site | Item | Value |
|------|------|-------|
| 12 | `run_id` | *(non-deterministic — changes each run)* |
| 13 | `snapshot_hash` | `14782ba90632f2df` |
| 14 | `datagram_uuid` | *(time-ordered UUID v7 — changes each run)* |

> `snapshot_hash = "14782ba90632f2df"` is deterministic: the pipeline has one leaf node whose
> `content_hash().to_string()` is
> `"semantic_v0.1:ecaf448623d79be4ac3186d7fcc19cf52d66223ceb8d77884c3036462a5a127f"`.
> `hashlib.sha256(leaf_hash.encode()).hexdigest()[:16]` = `"14782ba90632f2df"`.

---

## 1. Framework Object Identity

Every class that inherits from `TraceableBase` carries both a `content_hash()` and a
`pipeline_hash()`. Both use `SemanticAwarePythonHasher`, which recursively expands the
object's identity structure into a JSON-serialisable representation and takes its
SHA-256 digest.

### Algorithm — `SemanticAwarePythonHasher`

1. Call `identity_structure()` (for content hash) or `pipeline_identity_structure()` (for pipeline hash) on the object.
2. Recursively expand the structure: `ContentIdentifiable` objects are replaced by their hash via a **resolver callback** (see below); containers (list, dict, tuple, set) are serialised to nested JSON.
3. JSON-serialise the expanded structure and compute `hashlib.sha256(json_bytes).digest()` → `ContentHash`.

**Resolver pattern:** The resolver callback is threaded through the entire recursive hash computation to ensure one consistent context per call:

- **`content_resolver`** (used by `content_hash()`): routes every `ContentIdentifiable` leaf through `leaf.content_hash(hasher)`.
- **`pipeline_resolver`** (used by `pipeline_hash()`): routes `PipelineElementProtocol` objects through `leaf.pipeline_hash(hasher)`; routes all other `ContentIdentifiable` objects (e.g. raw data values) through `leaf.content_hash(hasher)`.

Both hashes are **cached by `hasher_id`** on each object, so repeated calls are free.

---

### 1a. `content_hash()` — data-inclusive identity

The table below shows what each class returns from `identity_structure()`.

| Class | `identity_structure()` return value | Notes |
|-------|--------------------------------------|-------|
| `Datagram` | `self._ensure_data_table()` — the raw Arrow table | Dispatched to `ArrowTableHandler` → `StarfixArrowHasher` |
| `Tag` | User tag columns only (raw Arrow table) | System tag columns (`_tag::*`) excluded — they are provenance metadata, not tag content |
| `Data` | Data columns only (raw Arrow table) | Source info columns (`_source_*`) excluded — they are provenance metadata, not data content |
| `EmptyData` | Raises `EmptyDataAccessError` | `content_hash()` is overridden to return a stored `cached_content_hash` directly |
| `DataFunctionBase` | `self.uri` — `(canonical_function_name, output_schema_hash, major_version, data_function_type_id)` | `output_schema_hash` is site 11; see §5 |
| `FunctionPod` | `self.data_function` (bare object) when no ctx arg; `(self.data_function, self._ctx_arg_name)` when ctx arg present | `ctx_arg_name` is included when present so a ctx-aware pod has a distinct `content_hash` from a plain pod using the same data function |
| `ArrowTableStream` | `(producer, argument_symmetry(upstreams))` | Falls back to `(class_name, as_table(all_info=True), tag_columns)` when no producer or producer is not a `PipelineElementProtocol` |
| `RootSource` (`ArrowTableSource`) | `(class_name, (tag_schema, data_schema), source_id)` | `source_id` is the explicit user-supplied value; when not provided it defaults to the Arrow hash of the raw table (site 4). Data-inclusive base case of the Merkle chain. |
| `DerivedSource` | Origin node's `content_hash` | Inherits its generating node's identity |
| `DerivedSource` with `tags_as_data` | `(class_name, output_schema, source_id)`; the default `source_id` is `tags_as_data:<label>:<hex>` of the hash of `(("DerivedSource", sorted tags_as_data items), (origin,))`, the origin entering through its `content_hash`; a `record_id_column`, when set, is appended to the parameters as `("record_id_column", name)` | The rule of every root source; the origin and the mapping enter through the default `source_id`, which is the content hash an operator node doing the derivation would have |
| Operators (unary) | `(operator_class_name, upstream_stream)` | Stream reference resolved via `content_resolver` |
| Operators (binary/N-ary) | `(operator_class_name, argument_symmetry(streams))` | `frozenset` for commutative (Join, MergeJoin); `tuple` for ordered (SemiJoin) |

**Known exclusions from `identity_structure()`:**

| Class | Excluded | Reason |
|-------|----------|--------|
| `Tag` | System tag columns (`_tag::*`) | Provenance metadata, not content |
| `Data` | Source info columns (`_source_*`) | Provenance metadata, not content |
| `EmptyData` | All data | No payload; `content_hash()` overridden to return stored hash |

---

### 1b. `pipeline_hash()` — schema and topology only

The pipeline hash uses the same `SemanticAwarePythonHasher` with the **`pipeline_resolver`**. The key difference is what each class returns from `pipeline_identity_structure()`.

| Class | `pipeline_identity_structure()` return value | Notes |
|-------|----------------------------------------------|-------|
| `RootSource` | `(tag_schema, data_schema)` | Base case — schemas only, no data content |
| `DerivedSource` | `(tag_schema, data_schema)` | Acts as a new root in the pipeline Merkle chain |
| `DataFunctionBase` | `self.uri` (same as `identity_structure()`) | Function identity is already schema-only |
| `FunctionPod` | `self.data_function` **only** (excludes `ctx_arg_name`) | A ctx-aware pod and a plain pod over the same data function share the same `pipeline_hash` value, but have different `uri` values (`ctx_arg_name` is set → `"side_effect_function"` is prepended to the URI) and therefore write to **different** DB table paths |
| `ArrowTableStream` | `(producer, argument_symmetry(upstreams pipeline hashes))` | `pipeline_resolver` routes upstreams through `pipeline_hash()`. Falls back to `(tag_schema, data_schema)` when producer is absent or not a `PipelineElementProtocol`. |
| Operators | `(operator_class_name, argument_symmetry(upstream pipeline hashes))` | Same structure as content identity but using pipeline hashes of upstreams |
| `SideEffectPodStream` | `(pod, argument_symmetry(upstreams))` | Same as `identity_structure()` |

> **Note:** `FunctionPod.ctx_arg_name` IS included in `content_hash()` (via `identity_structure()`). It is excluded only from `pipeline_identity_structure()`. A ctx-aware pod receives a system-injected `InvocationContext` argument — a deliberate deviation from the pure data-in / data-out contract — so it is kept distinct from a plain pod at every level: different `content_hash`, different `uri` (prefix `"side_effect_function"`), and different DB table path. The shared `pipeline_hash` value has no practical effect on scoping once the `uri` differs.

---

### 1c. Polars expression predicates

`PolarsFilter.identity_structure()` is `("PolarsFilter", predicates, constraints)`, with
`predicates` always a tuple. Keyword constraints hash as plain values. A `pl.Expr` predicate
hashes through `PolarsExprHandler` to the leaf `polars_expr_v1:<hex>`.

Polars' own serialization of an expression (`Expr.meta.serialize`) changed layout between
1.20, 1.30 and 1.36 and is documented as unstable across versions, and `str(expr)` is lossy
(`pl.lit(0.1, dtype=pl.Float32)` and `pl.lit(0.1, dtype=pl.Float64)` print alike), so neither
is hashed. Orcapod parses the JSON serialization with a strict parser into a tree in its own
grammar and hashes that:

```text
ContentHash("polars_expr_v1", sha256(canonical JSON of the tree))
canonical JSON = keys sorted, separators (",", ":"), UTF-8

node    := {"col": <name>} | {"lit": <literal>}
         | {"binary": [<op>, <node>, <node>]}
         | {"fn": [<fname>, <options>, [<node>, ...]]}
         | {"cast": [<dtype>, "strict" | "non_strict", <node>]}
op      := eq ne eq_missing ne_missing lt le gt ge and or xor add sub mul truediv
           floordiv mod
fname   := not is_null is_not_null is_nan is_not_nan negate all_horizontal
           any_horizontal str.starts_with str.ends_with     (options {})
         | is_in {"nulls_equal": bool} | is_between {"closed": both|left|right|none}
         | str.contains {"literal": bool, "strict": bool}
literal := {"dyn_int": "<decimal>"} | {"dyn_float": "<float.hex()>"}
         | {"null": null} | {"boolean": bool} | {"string": str}
         | {"int8" ... "uint64": "<decimal>"} | {"float32" | "float64": "<float.hex()>"}
         | {"decimal": ["<unscaled>", precision, scale]} | {"binary": "<hex>"}
         | {"date": days} | {"time": ns}
         | {"datetime": [value, unit, time zone or null]} | {"duration": [value, unit]}
         | {"list": [<dtype>, [<value or null>, ...]]}   (no lists of decimals or bytes)
dtype   := the dtype names of the Polars value descriptor ("int64", "string",
           {"datetime": [unit, tz]}, {"list": <dtype>}, ...)
```

The module docstring of `orcapod.hashing.semantic_hashing.polars_expr` is the full
definition. Consequences:

* `pl.col("x") > 0`, `pl.col("x").gt(0)`, `pl.col("x") > pl.lit(0)` and `0 < pl.col("x")`
  hash equal. `> 0` (a Python int, typed by Polars against the column), `> 0.0` and
  `> pl.lit(0, dtype=pl.Int8)` hash differently. The expression is hashed as written:
  `a & b` and `b & a` differ, and so does the order of the predicates of one filter.
* Unsupported expressions raise `ValueError` naming the expression: Python functions
  (`map_elements`, `map_batches`, whose serialized form differs between processes),
  selectors over several columns (`pl.col("x", "y")`, `pl.all()`, regex columns,
  `pl.exclude`, `cs.*`, dtype columns), aggregations and windows, `alias`,
  `when/then/otherwise`, NaN and infinite literals (the JSON form cannot tell them apart),
  `pl.Series` literals, and any node or layout the parser does not know. A Polars release
  that changes its serialization therefore fails loudly rather than moving a hash; the parser
  is then taught the new layout and maps it onto the same tree.
* Supporting a new construct adds to the grammar without changing any existing hash. Only a
  change to the encoding of a supported construct needs a new label (`polars_expr_v2`).

The canonical trees of a corpus of expressions are pinned in
`tests/test_hashing/test_polars_expr_hashing.py`, and their hashes in the golden suite.

---

## 2. Source Provenance & System Tags

When a source is built by `SourceStreamBuilder`, four hash-related operations happen in
sequence before the stream is returned:

1. **Site 3 — Schema hash:** computed from `(tag_schema, data_schema)`; embedded in system tag column names.
2. **Site 4 — Default `source_id`:** derived from the raw table hash if no explicit `source_id` is given.
3. **Site 5 — Per-row `record_id`:** a UUID v5 computed from `(source_id, row_provenance_token)` and stored per row.
4. Two system tag columns, `_tag_source_id::{schema_hash}` and `_tag_record_id::{schema_hash}`, are appended to every row.

At Join time (site 6), all existing system tag columns are **renamed** by appending a
topology suffix, encoding the full join lineage into each column name.

---

### Site 3 — Schema hash

| Field | Value |
|---|---|
| **Inputs** | `(tag_schema, data_schema)` — Python `Schema` objects mapping column names to Python types |
| **Algorithm** | `SemanticAwarePythonHasher.hash_object((tag_schema, data_schema)).to_hex(schema_n_char)` where `schema_n_char = OrcapodConfig.hashing.schema_n_char` |
| **Output format** | Truncated hex `str` (length = `schema_n_char`; `None` = full digest) |
| **Uniqueness guarantee** | Unique per `(tag_schema, data_schema)` pair; two sources with identical schemas produce identically-named system tag columns, enabling consistent system tag lookup across sources of the same schema |
| **Known exclusions** | No data content; no source identity; purely structural |

---

### Site 4 — Default `source_id`

| Field | Value |
|---|---|
| **Inputs** | Full raw Arrow table (all columns, before any system tag injection) |
| **Algorithm** | `StarfixArrowHasher.hash_table(table).to_hex(path_n_char)` — versioned SHA-256 via the `starfix` Rust crate |
| **Output format** | Truncated hex `str` (length = `path_n_char`) |
| **Uniqueness guarantee** | Unique per raw table content; changes if any cell value changes |
| **Known exclusions** | Used **only** as a fallback when no explicit `source_id` is provided. A user-supplied `source_id` bypasses this computation entirely. |

---

### Site 5 — Per-row `record_id` (system tag value)

| Field | Value |
|---|---|
| **Inputs** | `source_id` string + provenance token: `"{col}={value}"` when `record_id_column` is specified, otherwise `"row_{index}"` |
| **Algorithm** | `uuid.uuid5(_SOURCE_RECORD_ID_NAMESPACE, f"{source_id}::{provenance_token}")` where `_SOURCE_RECORD_ID_NAMESPACE = uuid.uuid5(NAMESPACE_URL, "orcapod::record_id")` is a fixed constant |
| **Output format** | `bytes` (16 bytes, UUID v5 bit pattern), stored in a `pa.binary(16)` Arrow column |
| **Uniqueness guarantee** | Deterministic per `(source_id, row_identity)`; stable across identical re-runs of the same source |
| **Known exclusions** | When `record_id_column` is not specified, the row index is used as the provenance token; `record_id` changes if rows are **reordered** within the source table |

---

### Site 6 — Join system tag suffix

| Field | Value |
|---|---|
| **Inputs** | `stream.pipeline_hash()` of each canonically-ordered input stream + its 0-based canonical position index `idx` |
| **Algorithm** | Streams are first sorted by `stream.pipeline_hash().to_string()` for determinism. For each input at canonical position `idx`, every existing system tag column name has `{BLOCK_SEPARATOR}{stream.pipeline_hash().to_hex(system_tag_n_char)}:{idx}` appended via `arrow_utils.append_to_system_tags()`. (`system_tag_n_char` from `OrcapodConfig.hashing`; default `None` = full digest.) |
| **Output format** | Column name suffix; no separate value is stored |
| **Uniqueness guarantee** | Each post-join system tag column name uniquely identifies `(original schema, input topology, canonical join position)`; no collision even when joining streams with identical schemas |
| **Known exclusions** | `SemiJoin` passes system tags through unchanged. Aggregating operators (`Batch`, `GroupBy`) use the fold described in site 6b instead. |

---

### Site 6b — Aggregating operator system tag fold

| Field | Value |
|---|---|
| **Inputs** | The ordered list of a group's member values for one system tag column, plus that column's name (which selects the fold) |
| **Algorithm** | `arrow_utils.fold_system_tag_values()`. For a `record_id` column: `uuid.uuid5(_AGGREGATED_RECORD_ID_NAMESPACE, BLOCK_SEPARATOR.join(v.hex() for v in values))`, where `_AGGREGATED_RECORD_ID_NAMESPACE = uuid.uuid5(NAMESPACE_URL, "https://orcapod.org/namespaces/aggregated-record-id")` is a fixed constant. For every other system tag column: `combine_hashes(*[str(v) for v in values], order=False)` — SHA-256 over the members concatenated in order. `None` members contribute the empty string. The folded column name then gains `{BLOCK_SEPARATOR}{stream.pipeline_hash().to_hex(system_tag_n_char)}` via `arrow_utils.append_to_system_tags()`. |
| **Output format** | `bytes` (16, UUID v5 bit pattern) for a `record_id` column; 64-character hex `str` otherwise — in both cases **scalar**, matching the input column's type |
| **Uniqueness guarantee** | Deterministic per ordered member set, and stable across processes (both primitives are SHA-based). `Batch` and `GroupBy` both order their members deterministically before folding, so an unchanged member set always folds to the same digest |
| **Known exclusions** | Member order is significant; a permuted member set folds differently. This is why `GroupBy` sorts members by their non-key tag values with `record_id` as a tiebreaker rather than relying on upstream emission order |

> **Why fold rather than list-wrap.** A many-to-one operator turns user tag and data columns
> into `list[T]`, but system tag columns must stay scalar: `_build_record_id_preimage`
> (`core/nodes/function_node.py`) hashes those columns directly to derive record identity.
> Folding rather than dropping them preserves the Merkle link, so an upstream recompute that
> yields identical data still invalidates downstream records. Because the digest becomes a
> cache key, the fold must never use `hash()` or a set-based construction — a per-process value
> would look correct in a single-process test and miss the cache on every new driver run.

> **Key insight for entry IDs:** Because `compute_base_entry_id()` (§3) calls
> `tag.as_table(columns={"system_tags": True})`, its preimage captures the full set of
> chained system tag column names. After a two-way join, the preimage contains four system
> tag columns — two per input — each with the join topology embedded in its name. The entry
> ID therefore implicitly commits to the complete join provenance graph without any
> join-awareness in the entry ID computation itself.

---

## 3. Pipeline DB Entry Keys

`FunctionJobNode` uses two related Arrow-based hashes as database primary keys. Both are
computed by `StarfixArrowHasher` over a single-row preimage table.

The **base entry ID** (site 7) is stable across all recomputation attempts for the same
logical input — it is used as an in-memory cache key and for Phase 1 DB lookups. The
**pipeline entry ID** (site 8) adds a `recomputation_index` column and is the actual
primary key stored in the pipeline DB.

The preimage for both sites is built by `_build_record_id_preimage(tag, input_data)`:

```
preimage = tag.as_table(columns={"system_tags": True})
         .append_column(INPUT_DATA_HASH_COL,
                        pa.array([input_data.content_hash().to_prefixed_digest()],
                                 type=pa.large_binary()))
```

Because `tag.as_table(columns={"system_tags": True})` includes all chained system tag
columns (see §2, site 6), the preimage implicitly captures the full join provenance of
the row.

---

### Site 7 — `compute_base_entry_id()`

| Field | Value |
|---|---|
| **Inputs** | All system tag columns from `tag.as_table(columns={"system_tags": True})` (including join-chained columns) + `INPUT_DATA_HASH_COL` (`input_data.content_hash().to_prefixed_digest()` as `pa.large_binary()`) |
| **Algorithm** | `StarfixArrowHasher.hash_table(preimage).to_prefixed_digest()` — single-row Arrow table hash |
| **Output format** | `bytes` in `b"{method}:{digest}"` format |
| **Uniqueness guarantee** | Unique per `(node, tag lineage, input_data content)` across all recomputation attempts; used as the in-memory cache key and Phase 1 DB filter |
| **Known exclusions** | `NODE_CONTENT_HASH_COL` excluded — the node's content hash is fully determined by the DB table path (scoped by `uri` + `pipeline_hash()`). Recomputation index excluded by design — use site 8 for a versioned key. |

---

### Site 8 — `compute_pipeline_entry_id()`

| Field | Value |
|---|---|
| **Inputs** | Same preimage as site 7 + `_PIPELINE_RECOMPUTATION_INDEX_COL` (value: `recomputation_index`, type `pa.int32()`; default `0`) |
| **Algorithm** | `StarfixArrowHasher.hash_table(preimage).to_prefixed_digest()` |
| **Output format** | `bytes` in `b"{method}:{digest}"` format |
| **Uniqueness guarantee** | Unique per `(node, tag lineage, input_data content, recomputation attempt)` — the primary key for all rows in the pipeline DB |
| **Known exclusions** | At `recomputation_index=0` this produces a hash that differs from the pre-ITL-508 implementation (the index column is now part of the preimage); existing pipeline DB records were intentionally invalidated when ITL-508 landed |

---

## 4. Side-Effect Record ID & Invocation Hash

Side-effect pods (and `FunctionPod` instances with a `ctx_arg_name`) use a parallel but
distinct record key scheme. The row-level preimage structure mirrors §3; pod-version
scoping is handled at the delivery log's table-path level via `pipeline_hash()` and `uri`,
not at the preimage level — consistent with how `FunctionJobNode` scopes function cache
entries in §3.

The `invocation_hash` string is composed from two `ContentHash` components and exposed to
the pod function as an idempotency key via `InvocationContext.invocation_hash`.

---

### Site 9 — Side-effect `record_id`

| Field | Value |
|---|---|
| **Inputs** | System tags + `INPUT_DATA_HASH_COL` (`input_data.content_hash().to_prefixed_digest()` as `pa.large_binary()`) + `_SIDE_EFFECT_RECOMPUTATION_INDEX_COL` (fixed `0`, `pa.int32()`) |
| **Algorithm** | `StarfixArrowHasher.hash_table(preimage)` |
| **Output format** | `ContentHash`; `.to_prefixed_digest()` → `bytes` when stored in the delivery log |
| **Uniqueness guarantee** | Unique per `(tag lineage, input_data content)` at `recomputation_index=0`. Pod-version scoping is provided by the delivery log's table path (derived from `pipeline_hash()` and `uri`), not by the preimage. |
| **Known exclusions** | `NODE_CONTENT_HASH_COL` is not included — pod-version identity is captured at the table-path level, consistent with how sites 7–8 scope function cache entries. Recomputation index is always `0`; side-effect delivery records are not versioned across recomputation attempts. |

---

### Site 10 — `invocation_hash`

| Field | Value |
|---|---|
| **Inputs** | `pipeline_hash_ch` — the pod's own `pipeline_hash()` as `ContentHash` + `record_id_hash_ch` — the `ContentHash` from site 9 |
| **Algorithm** | `f"{serialize(pipeline_hash_ch)}::{serialize(record_id_hash_ch)}"` where each component is serialised as `f"{method}:{hex_or_base64_digest}"` via `InvocationHashConfig` (default: hex, full digest) |
| **Output format** | `str` of the form `"{method}:{digest}::{method}:{digest}"` |
| **Uniqueness guarantee** | Unique per `(pod topology, tag lineage, input_data content)`; exposed to pod functions as an idempotency key. Pod-version scoping is provided by the `pipeline_hash()`-derived table path, not by the preimage. |
| **Known exclusions** | When `track_completion=True` (default): `run_id` is **excluded** — hash is run-independent for idempotency. When `track_completion=False` **and** `pipeline_run_id` is set: `run_id` is appended as a third `::` component so that each run produces a distinct hash. |

---

## 5. Data Function URI Hash

`DataFunctionBase.uri` is a tuple used as the canonical identity of a data function:

```
uri = (canonical_function_name, output_schema_hash, major_version, data_function_type_id)
```

The `output_schema_hash` component (site 11) is the only hash in the URI; the other
components are plain strings.

---

### Site 11 — Output schema hash

| Field | Value |
|---|---|
| **Inputs** | `output_data_schema` — a `Schema` mapping output column names to Python types |
| **Algorithm** | `SemanticAwarePythonHasher.hash_object(output_data_schema).to_string()` |
| **Output format** | `str` (ContentHash string representation including method prefix, e.g. `"object_v0.1:abcd1234..."`) |
| **Uniqueness guarantee** | Unique per output schema definition; changing any output column name or type changes this hash and therefore the function's entire URI, `content_hash`, and `pipeline_hash` |
| **Known exclusions** | Input schema not included (changing input schema alone does not change the URI); function code not included (tracked separately via `major_version`); `data_function_type_id` not included (plain string component) |

---

## 6. Pipeline Run Identity

`PipelineJob.run()` generates three identifiers at execution time. None of these are used
in any data-level preimage; they serve logging, observability, and result inspection.

---

### Site 12 — `run_id`

| Field | Value |
|---|---|
| **Inputs** | None (random) |
| **Algorithm** | `uuid.uuid4().hex[:16]` |
| **Output format** | 16-char hex `str` |
| **Uniqueness guarantee** | Non-deterministic; unique per execution with overwhelming probability |
| **Known exclusions** | Does not reflect pipeline structure, data content, or any input. Two runs with identical pipelines and data produce different `run_id` values. |

---

### Site 13 — `snapshot_hash`

| Field | Value |
|---|---|
| **Inputs** | Sorted `content_hash().to_string()` values of all DAG **leaf** nodes (nodes with no downstream successors in the execution DAG) |
| **Algorithm** | `hashlib.sha256("\n".join(sorted_leaf_hashes).encode()).hexdigest()[:16]` |
| **Output format** | 16-char hex `str`; embedded in `pipeline_uri` as `{pipeline_name}@{snapshot_hash}` |
| **Uniqueness guarantee** | Unique per `(leaf node topology + data state)` at run time; changes if any leaf node's schema, function code, or source data changes |
| **Known exclusions** | Covers only leaf (sink) nodes — intermediate nodes not included. Truncated to 16 chars (collision-resistant in practice but not cryptographically guaranteed at this length). |

---

### Site 14 — `datagram_uuid`

| Field | Value |
|---|---|
| **Inputs** | Current wall-clock time (monotonic within a process) |
| **Algorithm** | `uuid_utils.uuid7()` normalised to `stdlib uuid.UUID` via `uuid.UUID(bytes=uuid7().bytes)` |
| **Output format** | `uuid.UUID` |
| **Uniqueness guarantee** | Unique per datagram instance; time-ordered (monotonically increasing within a process) |
| **Known exclusions** | **Not a content hash.** Two datagrams with identical content have different UUIDs. Not used in any hash preimage. Serves as an object identity token, not a content fingerprint. |

---

## 8. Array Value Digest

**Value hash labels.** A type whose values are hashed by a dedicated handler labels its hash
`<type>_v<N>`, for example `ndarray_v1` (and later `tensor_v1`). The label is the
`ContentHash.method` and appears in every token built from the hash. The bytes produced under
an existing label never change. A new definition gets a new label, so a hash always says
which definition produced it.

A `numpy.ndarray` value is hashed by value (since v0.3, ITL-888). The semantic hasher's
`NumpyArrayHandler` returns `ContentHash(method="ndarray_v1", digest=D)`, where `D` is the
version-1 array digest of the array's stored row. Inside a table hash, an array cell becomes
the token `b"orcapod:ndarray::ndarray_v1:" + D`: the extension name, then the label and the
digest. The stored row, the dtype tags and the canonical form are described in
[NumPy Arrays](../concepts/arrays.md).

For a stored row with dtype tag `T`, shape `(d0, ..., dn-1)` and data bytes `B`:

```
P = u64le(len(utf8(T))) || utf8(T)
 || u64le(n) || i64le(d0) || ... || i64le(dn-1)
 || u64le(len(B)) || B
D = SHA-256(P)
```

`u64le` and `i64le` are 8-byte little-endian unsigned and signed integers. Every field is
length-prefixed, so two different rows never share a preimage. `D` depends only on the stored
row, so it can be recomputed from a stored table by any SHA-256 implementation. The label
`ndarray_v1` names this definition; a different definition gets a different label. A
`torch.Tensor` type is to share the digest under its own label.

| Variation | Same `D`? |
|---|---|
| C or Fortran order, strides, views | yes |
| Little- or big-endian byte order | yes |
| Structured offsets, alignment padding | yes |
| `np.matrix`, `np.recarray`, `np.memmap` against the plain array | yes |
| Dtype (including width), shape, any element bit, structured field names or field order | no |

The reference implementation below uses only the standard library, and the vectors are pinned:

```python
import hashlib
import struct

def array_digest_v1(tag: str, shape: tuple[int, ...], data: bytes) -> str:
    t = tag.encode("utf-8")
    p = struct.pack("<Q", len(t)) + t + struct.pack("<Q", len(shape))
    p += b"".join(struct.pack("<q", d) for d in shape)
    p += struct.pack("<Q", len(data)) + data
    return hashlib.sha256(p).hexdigest()

print(array_digest_v1("float32", (2, 3), struct.pack("<6f", 0, 1, 2, 3, 4, 5)))
# 318f6e503e8064e89ce6039453fce709f8e01f60c501af2b63bbbe9cf8fbf90e
print(array_digest_v1("float64", (), struct.pack("<d", 2.5)))
# 47f182d862f5dd597d88836deddaed609ed94490827302e75e8364e877633cae
print(array_digest_v1("int8", (0, 3), b""))
# 54165f92526b48bf9bae265168aa7020b135a9973d372d9ceaa9ae92f611068a
```

The same values through Orcapod:

```python
import numpy as np

from orcapod.contexts import get_default_context

hasher = get_default_context().semantic_hasher
print(hasher.hash_object(np.arange(6, dtype=np.float32).reshape(2, 3)).to_string())
# ndarray_v1:318f6e503e8064e89ce6039453fce709f8e01f60c501af2b63bbbe9cf8fbf90e
print(hasher.hash_object(np.array(2.5)).to_string())
# ndarray_v1:47f182d862f5dd597d88836deddaed609ed94490827302e75e8364e877633cae
```
