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
| 4 | Default `source_id` of an in-memory source | `"<source kind>:" + StarfixArrowHasher.hash_table(table).to_string()` | `str`, `<source kind>:<algorithm label>:<hex>` | Unique per source kind and raw table content; used as the source's name when none is given |
| 5 | Per-row `record_id` (system tag value) | `uuid.uuid5(NAMESPACE, f"{source_id}::{provenance_token}")` | `bytes` (16, UUID v5) | Deterministic per `(source_id, row_identity)`; stable across re-runs of the same source |
| 6 | Join system tag suffix | `stream.pipeline_hash().to_hex(n)` + `:{idx}` appended to column name | Column name suffix | Unique per `(input topology, canonical join position)`; encodes full join lineage in column name |
| 6b | Aggregating operator system tag fold | `uuid.uuid5(NAMESPACE, joined_member_hex)` for `record_id`; SHA-256 of the canonical JSON list of member values otherwise | `bytes` (16) / hex `str`, plus a column name suffix | Deterministic per ordered member set; keeps system tags scalar through a many-to-one reduction |
| 7 | `compute_base_entry_id()` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content)` across all recomputation attempts |
| 8 | `compute_pipeline_entry_id()` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL + recomputation_index)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content, recomputation attempt)` |
| 9 | Side-effect `record_id` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL + recomputation_index=0)` | `bytes` | Unique per `(tag lineage, input_data content)`; pod-version scoped via table path (`uri` + `pipeline_hash()`) |
| 10 | `invocation_hash` | `f"{serialize(pipeline_hash_ch)}::{serialize(record_id_hash_ch)}"` | `str` | Unique per `(pod topology, tag lineage, input_data content)`; pod-version scoped via table path |
| 11 | Output schema hash | `SemanticAwarePythonHasher.hash_object(output_data_schema).to_hex()` | `str` (bare hex digest) | Unique per output schema definition; embedded in function URI |
| 12 | `run_id` | `uuid.uuid4().hex[:16]` | 16-char hex `str` | Non-deterministic; unique per pipeline execution |
| 13 | `snapshot_hash` | `hashlib.sha256(sorted_leaf_content_digests joined by newline).hexdigest()[:16]` | 16-char hex `str` | Unique per `(DAG leaf topology + data state)` at run time |
| 14 | `datagram_uuid` | `uuid7()` normalised to `stdlib uuid.UUID` | `uuid.UUID` | Unique per datagram instance; time-ordered; not a content hash |

---

## The hash label rule

A `ContentHash` pairs a **label** (`method`) with a digest. The rule below holds for every
hash Orcapod computes (decided November 11, 2026; ITL-945).

| # | Rule |
|---|------|
| 1 | A label names the algorithm that produced the digest and nothing else. "Algorithm" is the whole hashing procedure: the canonical form, any preparation, the encoding and the digest function. |
| 2 | The format is `<algorithm>_v<N>` with an integer `N`. In v0.3 every label is `_v1`. The file content hash keeps `sha256`, the name of the digest function applied to the file's bytes: the one exception. |
| 3 | A label changes only when its algorithm would give an existing input a different digest. A change that leaves existing digests alone keeps the label. |
| 4 | A hash stored as a value in its own column or field carries its label: `"<label>:<hex>"` as text, `b"<label>:" + digest` in a binary column. |
| 5 | A hash inside a name is the bare hex digest, without a label: path components, column names, the function `uri`, operator identity and the function `uri` inside a source-info token. One stated exception: a source id is the source's name, and a default source id built from a hash keeps a source-kind prefix and the hash's label (`derived:semantic_v1:<hex>`); that string is used as it is in the `source_id` system tag and in source-info values. |
| 6 | A label never enters a hash computation. A nested hash contributes its digest only, so renaming a label changes no digest. |
| 7 | A record id is an identifier built by a recipe (`uuid5`, `uuid7`), not a hash, and carries no label. |
| 8 | The implementer of a logical type chooses the label of its value hash, in the same format. |
| 9 | Each label is defined in exactly one place, in code beside its algorithm. The data context file names the hasher class and does not repeat the label. |

The labels of v0.3:

| Label | Names | Defined in |
|---|---|---|
| `semantic_v1` | the structure hash: a Python structure expanded to tagged JSON, SHA-256 of its canonical text | `SEMANTIC_HASH_LABEL`, `hashing/semantic_hashing/semantic_hasher.py` |
| `arrow_v1` | the Arrow table and schema hash (§9) | `ARROW_HASH_LABEL`, `hashing/arrow_hashers.py` |
| `directory_v1` | the directory hash: a SHA-256 Merkle tree over the entries | `DIRECTORY_HASH_LABEL`, `hashing/directory_hashers.py` |
| `ndarray_v1` | the array value hash (§8) | `NumpyArrayHandler.HASH_LABEL`, `hashing/semantic_hashing/builtin_handlers.py` |
| `pandas_dataframe_v1` | the `pandas.DataFrame` value hash (§10) | `PANDAS_DATAFRAME_HASH_LABEL`, `hashing/pandas_value_hash.py` |
| `pandas_series_v1` | the `pandas.Series` value hash (§10) | `PANDAS_SERIES_HASH_LABEL`, `hashing/pandas_value_hash.py` |
| `torch_tensor_v1` | the torch tensor value hash (§8): the element digest, then `requires_grad`, the lazy conjugate and negative bits, the strides and the class | `TORCH_TENSOR_HASH_LABEL`, `logical_types/torch_type.py` |
| `polars_expr_v1` | the Polars expression hash (§1c) | `POLARS_EXPR_HASH_LABEL`, `hashing/semantic_hashing/polars_expr.py` |
| `polars_dataframe_v1` | the Polars DataFrame value hash (§1e) | `PolarsDataFrameHandler.HASH_LABEL`, `hashing/semantic_hashing/builtin_handlers.py` |
| `polars_series_v1` | the Polars Series value hash (§1e) | `PolarsSeriesHandler.HASH_LABEL`, `hashing/semantic_hashing/builtin_handlers.py` |
| `sha256` | the file content hash: SHA-256 of the file's bytes | the file hasher's `algorithm` |

Where a hash appears, and in which form:

| Form | Where |
|---|---|
| Labelled value | `__input_data_hash`, `__output_data_hash`, `__node_content_hash`, `__pipeline_base_entry_id`, the pipeline table's record id, the side-effect log's `record_id` and `record_id_hash`, the record id (`_record_hash`) of operator-node and cached-source tables, the `_content_hash` column, the hashes in a saved job file, `InvocationContext.invocation_hash` |
| Bare digest in a name | the function `uri` (its second component), the path components `schema:<pipeline digest>`, `instance:<content digest>` and `node:<content digest>`, system-tag column names, the operator `uri`, the function `uri` at the start of a function output's source-info token |
| Labelled hash inside a source id | every default source id built from a hash, `<source kind>:<label>:<hex>`: an in-memory source (`arrow_table:`, `dict:`, `list:`, `data_frame:`, then the labelled table hash), and a `DerivedSource`, which is also what `node.as_source()` returns (`derived:`, then the node's labelled content hash). `hash_source_id` in `core/sources/source_ids.py` is the single definition of that form. A `DerivedSource` built with `tags_as_data` has its own kind, `tags_as_data:<label>:<hex>` of its derivation hash (`DerivedSource._default_source_id`) |
| Digest only, inside a hash input | a nested hash in a structure: `{"__type__": "hash", "digest": "<hex>"}`; an extension-typed cell in a table: `b"<extension name>;<hex of the extension metadata>::" + digest` (the logical type's whole identifier, which the hashed field also keeps as its extension name and metadata, so the type is part of the hash of a column with no value too); the input data hash in the entry-id preimage (§3); the node content hash in the operator-node record hash; the leaf digests of the run snapshot hash (site 13) |

A function node's table in v0.3, with the values it stores:

```python
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource
from orcapod.types import ContentHash


@function_pod(output_keys="doubled")
def double(x: int) -> int:
    return 2 * x


rule_store = InMemoryArrowDatabase()
rule_job = PipelineJob(name="my_pipeline", database=rule_store)
with rule_job:
    numbers = DictSource(
        data=[{"k": "a", "x": 1}], tag_columns=["k"], source_id="numbers"
    )
    _ = double.pod(numbers, label="double")
rule_job.run()

# A hash kept as a value carries its label.
rule_node = rule_job.nodes["double"]
print(rule_node.pipeline_hash())
# semantic_v1:256f7afa17669a49782015df03b38e32089009439b76c07a4a0b59bfda764c44

# A hash inside a name is the bare digest: the function uri and the table path.
print(double.pod.uri)
# ('double', '25139951ac9aa97bd5a4f4014aefd5d63fbc313b3c26e20ca2195cc4de9b6792', 'v0', 'python.function.v0')
pipeline_table = next(p for p in rule_store.list_sources() if "_result" not in p)
print("/".join(pipeline_table))
# my_pipeline/double/25139951ac9aa97bd5a4f4014aefd5d63fbc313b3c26e20ca2195cc4de9b6792/v0/python.function.v0/schema:256f7afa17669a49782015df03b38e32089009439b76c07a4a0b59bfda764c44/pdb_v2

# Stored columns hold labelled hashes; the system-tag column name holds a bare digest.
stored = rule_store.get_all_records(pipeline_table)
print(ContentHash.from_prefixed_digest(stored["__input_data_hash"][0].as_py()))
# arrow_v1:000002fcbe7eec1821a57679d34084a583d8282798563aa706ecb9c0c24382580de4df
print(ContentHash.from_prefixed_digest(stored["__node_content_hash"][0].as_py()))
# semantic_v1:0c8224446312bf4932b11683b91c54c2d2da582b7504cc1914e1f380fb48c89e
print([name for name in stored.column_names if name.startswith("_tag_source_id")])
# ['_tag_source_id::b74ae0abcd5bd99c8a339e22482ddb5cea19b4639f99fe84f668ec5b7769b559']
```

The path is `<job>/<function name>/<output-schema digest>/v<major>/<wrapper kind>/schema:<pipeline digest>/<table layout>`.
Renaming a label changes no digest and therefore no path, column name or key.

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
| `scores` | `semantic_v1:123db4cf199c8591080423df8e9ee36c20435732195a706b442da2a9088476c2` | `semantic_v1:577916c072bcdd784ac1da886acfa33881c38281475a82508f9ecdc8de4799a8` |
| `attendance` | `semantic_v1:ec9cb32280025c3307e627cfd46af211739b13e63fc23da9be3ab7ec83733837` | `semantic_v1:96fb3b2d45b82b78d224aaae23a1cb086e1a2ea98285183706f7f68944d0bc24` |
| `joined` | `semantic_v1:f7c478aa0d37888e2305e70de95c81a01ee858c2c5cfb165cfc9034281ed81eb` | `semantic_v1:c28a220e44441f7a079709aca3640376610c6a2f4d09c413bf26bc31743daee6` |
| `pod` | `semantic_v1:f994371fc3dd550b5d9e1da52545c735f225ab15c160f5fd1cd8f3e1d4ec2783` | `semantic_v1:f994371fc3dd550b5d9e1da52545c735f225ab15c160f5fd1cd8f3e1d4ec2783` |
| `node` | `semantic_v1:76d16bec67883d98ca039188ee359d75845ef1d028a3cc083e3bca2dc1f4f479` | `semantic_v1:6db83c3c1367eed2ef98d362ba2abb680952dd67c1b7fdd73f52b91518f2b53f` |

> `pod.content_hash() == pod.pipeline_hash()` because `grade` has no `ctx_arg_name`, so
> `identity_structure()` and `pipeline_identity_structure()` return the same value.
>
> `scores.pipeline_hash() ≠ attendance.pipeline_hash()` even though both sources have
> `tag_columns=["student"]` — their data schemas differ (`{math: int}` vs `{days: int}`).

#### §2 — Source Provenance & System Tags

| Site | Item | Value |
|------|------|-------|
| 3 | Schema hash (scores) | `577916c072bcdd784ac1da886acfa33881c38281475a82508f9ecdc8de4799a8` |
| 4 | `scores.source_id` | `"scores_v1"` (explicit; no Arrow hash computed) |
| 5 | `alice` `record_id` bytes (hex) | `e2d15e48e9f05f0f81e25d8e9f120b0a` |
| 5 | `bob` `record_id` bytes (hex) | `0d565e02049f5913bf45a5711e71ff52` |

System tag column names on `scores` (sites 3 + 5):

```
_tag_source_id::577916c072bcdd784ac1da886acfa33881c38281475a82508f9ecdc8de4799a8
_tag_record_id::577916c072bcdd784ac1da886acfa33881c38281475a82508f9ecdc8de4799a8
```

Post-join system tag column names (site 6):

```
_tag_source_id::577916c072bcdd784ac1da886acfa33881c38281475a82508f9ecdc8de4799a8::577916c072bcdd784ac1da886acfa33881c38281475a82508f9ecdc8de4799a8:0
_tag_record_id::577916c072bcdd784ac1da886acfa33881c38281475a82508f9ecdc8de4799a8::577916c072bcdd784ac1da886acfa33881c38281475a82508f9ecdc8de4799a8:0
_tag_source_id::96fb3b2d45b82b78d224aaae23a1cb086e1a2ea98285183706f7f68944d0bc24::96fb3b2d45b82b78d224aaae23a1cb086e1a2ea98285183706f7f68944d0bc24:1
_tag_record_id::96fb3b2d45b82b78d224aaae23a1cb086e1a2ea98285183706f7f68944d0bc24::96fb3b2d45b82b78d224aaae23a1cb086e1a2ea98285183706f7f68944d0bc24:1
```

The suffix `::577916c...:0` on `scores` columns encodes the canonical join position (`0`); the
suffix `::96fb3b2...:1` on `attendance` columns encodes position `1`. The pipeline hash in
each suffix is the stream's `pipeline_hash()` (sites 1b and 6).

#### §3 — Pipeline DB Entry Keys

| Site | Row | Value (hex) |
|------|-----|-------------|
| 7 | `alice` `base_entry_id` | `6172726f775f76313a000002262241cf2035116e7220c5d770435408291e5cac9f8600c47854634b96d6b673` |
| 7 | `bob` `base_entry_id` | `6172726f775f76313a0000027e2b05c90a2b99931595d9c46fe86171d85ade44b8829c5f465a7ececed55040` |
| 8 | `alice` `pipeline_entry_id` (idx=0) | `6172726f775f76313a000002b771b90e363d0f0623fdcb318bd7d433e9e232e45322c00a9b3475506257b057` |
| 8 | `bob` `pipeline_entry_id` (idx=0) | `6172726f775f76313a000002966301b9abf3a8071162423baad504c9b7c71b2bec8c052ee64265550f0f6d0d` |

> The hex prefix `6172726f775f7631` decodes to the ASCII string `arrow_v1` — the
> `method` portion of the `b"{method}:{digest}"` format described in §3.

#### §4 — Side-Effect Record ID & Invocation Hash

| Site | Row | Value |
|------|-----|-------|
| 10 | `alice` `invocation_hash` | `semantic_v1:14d51d1929777dcbbfeb6ffbdfc32876739cde2f2ce03d26cac781042aae0214::arrow_v1:000002b771b90e363d0f0623fdcb318bd7d433e9e232e45322c00a9b3475506257b057` |
| 10 | `bob` `invocation_hash` | `semantic_v1:14d51d1929777dcbbfeb6ffbdfc32876739cde2f2ce03d26cac781042aae0214::arrow_v1:000002966301b9abf3a8071162423baad504c9b7c71b2bec8c052ee64265550f0f6d0d` |

> Both rows share the same `semantic_v1:14d51d19...` component — this is the
> `SideEffectPodStream.pipeline_hash()`, which depends only on the pod and its upstream
> topology, not on row content. The `arrow_v1:...` component is the per-row `record_id`
> from site 9, which differs per row.

#### §5 — Data Function URI Hash

| Site | Item | Value |
|------|------|-------|
| 11 | `data_function.uri` | `('grade', '37ff1cf52cd878dc29b78dc38d0d0f2d67b9aa5795c0febe8ef50dd2f0459c91', 'v0', 'python.function.v0')` |
| 11 | `output_schema_hash` (uri[1]) | `37ff1cf52cd878dc29b78dc38d0d0f2d67b9aa5795c0febe8ef50dd2f0459c91` (bare digest: it is part of a name) |

#### §6 — Pipeline Run Identity

| Site | Item | Value |
|------|------|-------|
| 12 | `run_id` | *(non-deterministic — changes each run)* |
| 13 | `snapshot_hash` | `9b744dabab258289` |
| 14 | `datagram_uuid` | *(time-ordered UUID v7 — changes each run)* |

> `snapshot_hash = "9b744dabab258289"` is deterministic: the pipeline has one leaf node whose
> `content_hash().to_hex()` is
> `"eef8b1a89639de271a3b1cbd66d4a009f5a63c21933b69ba2668965e8daad762"`.
> `hashlib.sha256(leaf_digest.encode()).hexdigest()[:16]` = `"9b744dabab258289"`. The digest
> enters without its label.

---

## 1. Framework Object Identity

Every class that inherits from `TraceableBase` carries both a `content_hash()` and a
`pipeline_hash()`. Both use `SemanticAwarePythonHasher`, which recursively expands the
object's identity structure into a JSON-serialisable representation and takes its
SHA-256 digest.

### Algorithm — `SemanticAwarePythonHasher`

1. Call `identity_structure()` (for content hash) or `pipeline_identity_structure()` (for pipeline hash) on the object.
2. Recursively expand the structure: `ContentIdentifiable` objects are replaced by their hash via a **resolver callback** (see below); containers (list, dict, tuple, set) are serialised to nested JSON. A nested hash (of a type, a schema, a pod, a stream, a value with its own hasher) enters as `{"__type__": "hash", "digest": "<hex digest>"}`: the digest only, never the label (see [The hash label rule](#the-hash-label-rule)).
3. JSON-serialise the expanded structure and compute `hashlib.sha256(json_bytes).digest()` → `ContentHash`.

How containers expand (canonical JSON: sorted object keys, no whitespace, UTF-8):

| Container | Expanded form | What counts |
|---|---|---|
| `list` | JSON array | order |
| `tuple` | `{"__type__": "tuple", "items": [...]}` | order |
| `set`, `frozenset` | `{"__type__": "set", "items": [...]}`, items sorted by their canonical JSON text | membership only; never iteration order or `PYTHONHASHSEED` (ITL-942: before v0.3, items were sorted by `str()`, so `{1, "1"}` hashed differently per process) |
| `dict` with only `str` keys, none of them `"__type__"` | JSON object | keys and values, not insertion order |
| any other `dict` | `{"__type__": "dict", "items": [[key, value], ...]}`, pairs sorted by canonical JSON | key types: `{1: "x"}` and `{"1": "x"}` differ (ITL-942: before v0.3, keys went through `str()` and could collapse) |
| namedtuple | `{"__type__": "namedtuple", "name": ..., "fields": [[name, value], ...]}` | class name, field names and values, in declared order (ITL-942: before v0.3 fields were sorted by name, so `Point(x=1, y=2)` and a `Point` declared `(y, x)` holding `y=2, x=1` shared a hash) |
| a type handler's `Encoded(tag, fields)` | `{"__type__": tag, **fields}` | the handler's tag and fields |

**Only the hasher writes `"__type__"`.** A JSON object with a `"__type__"` key comes either
from the hasher's own encodings (tags `tuple`, `set`, `dict`, `namedtuple`, `hash`) or from a
type handler that returned an `Encoded` value (`generic_alias`, `union`). A mapping that has a
`"__type__"` key is hashed in the pair-list form, so a user value such as
`{"__type__": "tuple", "items": [1, 2]}` never shares the hash of `(1, 2)` (ITL-942). For
authors of a `PythonTypeHandlerProtocol`: return
`orcapod.hashing.semantic_hashing.Encoded(tag, fields)` to produce a tagged object; a plain
dict with a `"__type__"` key is hashed as an ordinary mapping. `Encoded` rejects the hasher's
own tags and a `"__type__"` field, and is trusted only as a handler's return value: an
`Encoded` inside a user value has no handler and is refused by the strict hasher.

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
| `RootSource` (`ArrowTableSource`) | `(class_name, (tag_schema, data_schema), source_id)` | `source_id` is the explicit user-supplied value; when not provided it defaults to `<source kind>:<algorithm label>:<hex>` of the Arrow hash of the raw table (site 4). Data-inclusive base case of the Merkle chain. |
| `DerivedSource` | Origin node's `content_hash` | Inherits its generating node's identity |
| `DerivedSource` with `tags_as_data` | `(class_name, output_schema, source_id, ("tags_as_data", sorted items))`; the default `source_id` is `tags_as_data:<label>:<hex>` of the hash of `(("DerivedSource", sorted tags_as_data items), (origin,))`, the origin entering through its `content_hash`; a `record_id_column`, when set, is appended to the parameters as `("record_id_column", name)` | The rule of every root source plus the mapping, which enters the `content_hash` directly (two mappings can give one schema) and so also with an explicit `source_id`; the origin enters through the default `source_id` only, which is the content hash an operator node doing the derivation would have |
| Operators (unary) | `(operator_class_name, upstream_stream)` | Stream reference resolved via `content_resolver` |
| Operators (binary/N-ary) | `(operator_class_name, argument_symmetry(streams))` | `frozenset` for commutative (Join, MergeJoin); `tuple` for ordered (SemiJoin) |
| `SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns` | `(operator_class_name, sorted unique column names as a list, strict)` | The operators act on a set of names: a list, a tuple, a set, a single string, a repeated or a reordered name list give one hash (ITL-942) |

**Inputs of a commutative operator are a set (kept by decision, ITL-942).** A pipeline hash
covers the structure of the pipeline, not only its schemas. `MergeJoin()(m1, m1)`, a stream
joined with itself, is a different pipeline from `MergeJoin()(m1, m2)` and has a different
pipeline hash even when both inputs share one schema. Inputs are an unordered set compared by
identity, so input order never matters, and two inputs with the same source identity (same
class, schemas and `source_id`) count as one input: `MergeJoin()(m1, m1b)` hashes like
`MergeJoin()(m1, m1)` when `m1b` has the `source_id` of `m1`.

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

### 1d. Function values and parameter defaults

A function used as a value (inside an identity structure, or passed to
`semantic_hasher.hash_object`) is hashed from one structured description of its signature:

```text
{"module": <module>, "name": <name>,
 "parameters": [{"name": ..., "kind": ..., "annotation": ..., "default": ...}, ...],
 "returns": <return annotation>}
```

| Member of a parameter entry | Value | Absent when |
|---|---|---|
| `name` | the parameter name | never |
| `kind` | `POSITIONAL_ONLY`, `POSITIONAL_OR_KEYWORD`, `VAR_POSITIONAL`, `KEYWORD_ONLY` or `VAR_KEYWORD` | never |
| `annotation` | the canonical text of the annotation (union members sorted, logical types by their identifier) | the parameter has no annotation |
| `default` | the semantic hasher's hash of the default value, as a nested hash | the parameter has no default |

Parameters are listed in declared order. The function body is not part of this hash.

**A default value is hashed like every other hashed value** (ITL-953): by the semantic
hasher, never by its `repr()`. Equal defaults give equal hashes in every process and under
every `PYTHONHASHSEED`; a default of `None` differs from having no default; and because a
default enters as a nested hash, no literal default can spell out another default's entry.
Before v0.3 the default entered as `repr(value)`, so a `frozenset` default hashed differently
per process and an `object()` default embedded a memory address. Before v0.3 the parameter
kind was also not hashed: `f(a, *, b)`, `f(a, /, b)` and `f(a, b)` shared a hash.

**A default the hasher cannot hash is an error.** There is no fallback to `repr()` or to the
type name, either of which would let two different defaults share a hash. Hashing the
function raises `TypeError` naming the parameter and the function. Since a data function
computes its signature hash when it is built, **a function pod whose function has such a
default fails at construction** (`@function_pod`, `FunctionPod.from_fn`,
`PythonDataFunction`).

| Default value | Hashed as |
|---|---|
| `None`, `bool`, `int`, `float`, `str` | the JSON primitive (a subclass such as an `IntEnum` member or `numpy.float64` hashes as the number it is) |
| `list`, `tuple`, `set`, `frozenset`, `dict`, namedtuple | the container forms of [the algorithm above](#algorithm-semanticawarepythonhasher) |
| `bytes`, `uuid.UUID`, a function, a type, `numpy.ndarray`, `pandas` objects, `File`, `Directory`, any object with `identity_structure()` | its registered handler |
| a plain `enum.Enum` member, a dataclass or pydantic instance, `pathlib.Path`, `datetime`, `date`, `Decimal`, `complex`, a numpy scalar other than `numpy.float64`, any other object | **not hashable: `TypeError`** |

Remedies: use a default the hasher can hash (for example a `str` or `None`, building the
object inside the function); remove the default and supply the value as an input column;
or register a `PythonTypeHandlerProtocol` for the type in the data context's handler
registry. Raising is the v0.3 behaviour (developer decision, November 11, 2026); hashing a
pod parameter's default as the column value it would be, which would cover `Path`,
`datetime`, `date` and numpy scalars, is the follow-up ITL-971.

**Where the hash is used.** As the hash of a function that appears as a value, and as the
`function_signature_hash` stored with every result row (the
`__pf_var_function_signature_hash` column), which is this same hash of the pod's function. A
function pod's identity does **not** use it: the pod's `uri`, its `pipeline_hash()`, its
`content_hash()` and its table paths depend on the function's name, version and output
schema (see [Data Function URI Hash](#5-data-function-uri-hash)), so changing a default
moves no path.

### 1e. Polars DataFrame and Series values

This hash is provisional in v0.3 (see [section 8](#8-array-value-hash)).

A `polars.DataFrame` or `polars.Series` is a value-hashed logical type
(`PolarsDataFrameHandler`, `PolarsSeriesHandler`; the algorithm is defined in
`orcapod.logical_types.polars_type`). Inside a table its cell token is, like every
value-hashed type's (see [the hash label rule](#the-hash-label-rule)),
`b"orcapod.polars.dataframe;<hex of the extension metadata>::" + digest`: the logical type's
identifier, then the digest, without the label. The extension metadata is
`{"ignore":[],"version":1}`: the version and the list of properties the hash ignores, empty
by default.

Storage and hashing are separate. The value is stored with Polars' own `write_ipc`; the
stored bytes are never hashed (they depend on the Polars version that wrote them). The hash
is computed from the value's content:

```text
sha256(descriptor + b"\x00" + starfix.ArrowDigester.hash_table(
    values, ordered_columns=True, ordered_struct_fields=True))
```

under the label `polars_dataframe_v1` for a frame and `polars_series_v1` for a Series (a
Series is hashed as its one-column frame, with its own descriptor).

`values` is the frame exported to Arrow with `compat_level=oldest`, with top-level `Null` columns left
out, `Array` cast to `large_list`, nested `Null` cast to an all-null `bool` and dictionaries
decoded, for hashing only. starfix runs in its ordered mode, so column order and struct field
order at every depth are part of the digest, and it tells a null struct from a struct whose
fields are all null. The descriptor carries what the Arrow values cannot (Enum categories, a
`Categorical`'s `Categories`, `Array` size, `Null` columns, the row count, a Series' name),
and states names, order and dtypes again in Orcapod's own terms. `descriptor` is canonical JSON (keys
sorted, compact separators, UTF-8) in a grammar Orcapod owns, never Polars' `repr`:

```text
frame:  {"kind": "orcapod.polars.dataframe", "height": <rows>,
         "columns": [[<name>, <dtype>], ...]}        in column order
series: {"kind": "orcapod.polars.series", "name": <name>, "height": <rows>,
         "dtype": <dtype>}

<dtype> := "int8" | "int16" | "int32" | "int64" | "uint8" | "uint16" | "uint32" | "uint64"
         | "float16" | "float32" | "float64" | "boolean" | "string" | "binary"
         | "date" | "time" | "null"
         | {"categorical": [<name>, <namespace>, <physical>]}   its Categories
         | {"enum": [<category>, ...]}               every declared category, in order
         | {"decimal": [<precision>, <scale>]}
         | {"datetime": [<unit>, <time zone or null>]}
         | {"duration": <unit>}
         | {"list": <dtype>}
         | {"array": [<dtype>, <size>]}              a multi-dimensional array nests
         | {"struct": [[<field>, <dtype>], ...]}     in field order
```

The rule: everything a pod can observe through the public Polars API as the frame's logical
content is identity. So two frames hash equal exactly when they have the same row count, the
same column names in the same order, the same dtype for each column (struct fields and their
order at every depth, Enum categories and their order, a `Categorical`'s `Categories` name,
namespace and physical width, Decimal precision and scale, Datetime unit and time zone,
Duration unit and Array shape included) and the same values (a null struct differs from a
struct whose fields are all null; a null differs from NaN, and `0.0` from `-0.0`). A pod that
returns `df.columns[0]` therefore never receives another frame's cached result. An Enum's full enumeration of choices is part of its dtype: all its declared categories in
declared order, including categories no row holds, so `Enum(["a", "b"])`, `Enum(["b", "a"])`
and `Enum(["a", "b", "z"])` hash differently over the same values.

Physical properties are not identity: chunking (`n_chunks()`) and rechunking, slice offsets,
the bytes under null slots, sortedness flags (`Series.flags`, which storage does not keep: a
frame read back has them cleared), physical `Categorical` codes (`to_physical()`) and the
contents of the process-wide category registry, which depend on what the process saw before,
the Arrow layout Polars exports (string views, large or small offsets) and memory size.
Supporting a new dtype adds to the grammar without changing any existing hash; only a change
that would give an existing value a different digest needs new labels (`polars_dataframe_v2`,
`polars_series_v2`). A dtype outside the grammar raises `ValueError` naming the column.

One kind of value cannot be hashed: starfix addresses a column or struct field by the path of
its names joined with `/`, so a value in which two names form the same path (a column `"a/b"`
beside a struct column `a` with a field `b`, or the same between a struct's fields) raises
`ValueError` naming the path. A name containing `/` is fine as long as no other name forms
the same path. The Arrow table hash (§9) has the same limit.

The hash is strict by default. An ignore list, part of the logical type's identifier, can
leave out `column_order` and `struct_field_order` for a frame, and `series_name` and
`struct_field_order` for a Series. A data context declares its list once, under `hash_ignore`
in `contexts/logical_types.json` (`"orcapod.polars.dataframe": []` by default); two lists are
two logical types, so their columns never share a hash, and a stored column whose list
differs from the data context's is refused on read. Ignoring column order lists the
descriptor's columns sorted by name and runs starfix with `ordered_columns=False`; ignoring
struct field order does the same for every struct's fields with
`ordered_struct_fields=False`; ignoring a Series' name writes `""` for `"name"` in the
descriptor and hashes its values as a column named `""`.

A `pandas.DataFrame` is hashed differently: with the `pandas_dataframe_v1` hash (§10), which
counts column order. A Polars frame and the equal pandas frame never share a hash.

---

### 1e. `dict` and `set` values: canonical order

A `dict` and a `set` have no order, so equal values must hash equally whatever order they
were built in. Arrow stores both as lists: `dict[K, V]` as `large_list<struct<key, value>>`
and a plain `set[T]` as `large_list<T>`. Orcapod puts them in canonical order at the point
where a Python value becomes Arrow, so the stored list, and therefore the row hash, does not
depend on insertion order (dicts) or on `PYTHONHASHSEED` (sets of `str` or `bytes`).

```python
from orcapod.sources import DictSource


def dict_row_hash(value):
    source = DictSource(
        data=[{"k": "r", "m": value}],
        tag_columns=["k"],
        data_schema={"k": str, "m": dict[str, int]},
    )
    _, data = next(iter(source.iter_data()))
    return data.content_hash()


print(dict_row_hash({"a": 1, "b": 2}) == dict_row_hash({"b": 2, "a": 1}))
# True
```

**Where the order is applied.** In the type converter (`get_python_to_arrow_converter`),
which every Python-to-Arrow path uses: sources built from Python values, datagrams, function
pod outputs (sync and async), and dicts nested in lists, dicts or dataclass and pydantic
fields. A datagram built from Python values also holds its dict values (including dicts
inside lists, dicts and `Optional`) as key-ordered copies, including values set later with
`update` or `with_columns`, so a newly computed pod output and the same output read back
from the cache iterate in the same order (decided November 12, 2026). The caller's dict
is never modified; dicts inside dataclass or pydantic objects
keep their own order in memory. Their stored dict values and hashes are canonical, but
a fresh user-class instance can iterate a nested dict in insertion order while a read-back
instance iterates it in key order. An order-sensitive pod can therefore observe different
orders for equal hashed inputs; reconstructing user-class instances is outside this fix.

**The order** is defined on the converted (storage) key or element, in
`semantic_types/canonical_order.py`:

| Key or element type | Order |
|---|---|
| `None` (for `K \| None`) | first |
| `bool` | `False` before `True` |
| `int`, `float`, `Decimal` | numeric |
| `str` | Unicode code point, never locale |
| `bytes` | unsigned bytewise |
| `date`, `datetime`, `time`, `timedelta` | natural order; datetimes by instant (they are timezone aware after conversion) |

A NaN key or element (a Python `float`, a NumPy floating scalar or a `Decimal`) has no order
and raises `CanonicalOrderError` (a `ValueError`) naming the column, as do keys or elements
that cannot be compared and two keys or elements that convert to the same stored value
(decided November 12, 2026). A
dict or set whose nested unordered containers are all already in this order is stored
byte-for-byte as before this rule existed,
so its hash did not change.

**Signed zero.** A float is hashed by its stored bytes, and `0.0` and `-0.0` have different
bytes, so they hash differently wherever they appear: in a plain `float` column and as a
dict key or set element alike, although Python compares them equal (`{0.0: 1} == {-0.0: 1}`).
Orcapod does not rewrite `-0.0` to `0.0`: pod code can tell them apart (`math.copysign`,
`repr`, division), so the finer identity can cost a recompute but never serves a wrong
result. Within one dict or set the two compare equal, so they cannot both be present.

**Arrow tables are hashed exactly as stored.** `StarfixArrowHasher` never sorts list
contents. A `list<struct<key, value>>` column that arrives already in Arrow form (an
`ArrowTableSource`, a Delta table, rows read back from a store) is read by Python as a
`dict`, but at the Arrow level it is an ordered list and its stored order is part of its
hash. A `list[T]` value is never sorted: its order is part of its identity. (Tables written
by `0.2.0`, which stored dicts in insertion order, are not read by v0.3 at all.)

**Two orderings, two layers.** This section is about the order a dict or set is *stored* in,
which is also the order it is read back in. The semantic hasher (section 1a) separately
orders the *hash input* it builds for a Python `dict` or `set` object, by each item's
canonical JSON text (ITL-942); that order is never stored and never seen. The two rules are
different on purpose (JSON text puts `10` before `2`; storage order is numeric) and neither
is defined in terms of the other: changing the hasher's order would move semantic hashes
of affected inputs, and changing storage order would move row hashes and what a pod reads back.
`set[T]` of a logical type (`set[Path]`, `set[UUID]`) is ordered by `ListLogicalType`, not
by this rule.

---

## 2. Source Provenance & System Tags

When a source is built by `SourceStreamBuilder`, four hash-related operations happen in
sequence before the stream is returned:

1. **Site 3 — Schema hash:** computed from `(tag_schema, data_schema)`; embedded in system tag column names.
2. **Site 4 — Default `source_id`:** `<source kind>:<algorithm label>:<hex>` of the raw table hash if no explicit `source_id` is given.
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
| **Algorithm** | `hash_source_id(<source kind>, StarfixArrowHasher.hash_table(table), hexdigits=path_n_char)` — the table hash is starfix hash format 2: the 3-byte format number `00 00 02` followed by a SHA-256 digest (`starfix` 0.5, byte-identical to the Rust crate) |
| **Output format** | `str`: `<source kind>:<algorithm label>:<hex>`, for example `dict:arrow_v1:000002fe…`. The source kind is the class's config type key (`SOURCE_KIND`: `arrow_table`, `dict`, `list`, `data_frame`); the label is the one the hash carries. The kind and the label are never shortened; `hashing.path_n_char`, when set, shortens the hex only. The parts hold no `:`, so the id never contains the `::` that separates the parts of a source-info value |
| **Uniqueness guarantee** | Unique per source kind and raw table content; changes if any cell value changes or the rows are reordered |
| **Known exclusions** | Used **only** as a fallback when no explicit `source_id` is provided. A user-supplied `source_id` bypasses this computation entirely. |

---

### Site 5 — Per-row `record_id` (system tag value)

| Field | Value |
|---|---|
| **Inputs** | `source_id` string + provenance token: `"{col}={value}"` when `record_id_column` is specified, otherwise `"row_{index}"` |
| **Algorithm** | `uuid.uuid5(_SOURCE_RECORD_ID_NAMESPACE, f"{source_id}::{provenance_token}")` where `_SOURCE_RECORD_ID_NAMESPACE = uuid.uuid5(NAMESPACE_URL, "orcapod::record_id")` is a fixed constant |
| **Output format** | `bytes` (16 bytes, UUID v5 bit pattern), stored in a `large_binary` Arrow column; the column keeps that type through tags, operators and stored tables (ITL-942: before v0.3 a source gave it `fixed_size_binary[16]`, so the operator cache key of one row differed between a direct run and the async orchestrator) |
| **Uniqueness guarantee** | Deterministic per `(source_id, row_identity)`; stable across identical re-runs of the same source |
| **Known exclusions** | When `record_id_column` is not specified, the row index is used as the provenance token; `record_id` changes if rows are **reordered** within the source table |

---

### Site 6 — Join system tag suffix

| Field | Value |
|---|---|
| **Inputs** | `stream.pipeline_hash()` of each canonically-ordered input stream + its 0-based canonical position index `idx` |
| **Algorithm** | Streams are first sorted by `stream.pipeline_hash().to_hex()` for determinism. For each input at canonical position `idx`, every existing system tag column name has `{BLOCK_SEPARATOR}{stream.pipeline_hash().to_hex(system_tag_n_char)}:{idx}` appended via `arrow_utils.append_to_system_tags()`. (`system_tag_n_char` from `OrcapodConfig.hashing`; default `None` = full digest.) |
| **Output format** | Column name suffix; no separate value is stored |
| **Uniqueness guarantee** | Each post-join system tag column name uniquely identifies `(original schema, input topology, canonical join position)`; no collision even when joining streams with identical schemas |
| **Known exclusions** | `SemiJoin` passes system tags through unchanged. Aggregating operators (`Batch`, `GroupBy`) use the fold described in site 6b instead. |

---

### Site 6b — Aggregating operator system tag fold

| Field | Value |
|---|---|
| **Inputs** | The ordered list of a group's member values for one system tag column, plus that column's name (which selects the fold) |
| **Algorithm** | `arrow_utils.fold_system_tag_values()`. For a `record_id` column: `uuid.uuid5(_AGGREGATED_RECORD_ID_NAMESPACE, BLOCK_SEPARATOR.join(v.hex() for v in values))`, where `_AGGREGATED_RECORD_ID_NAMESPACE = uuid.uuid5(NAMESPACE_URL, "https://orcapod.org/namespaces/aggregated-record-id")` is a fixed constant. A `None` member contributes `null`. For every other system tag column: SHA-256 of the canonical JSON array of the members in order (`json.dumps([None if v is None else str(v) for v in values], separators=(",", ":"), ensure_ascii=False)`, UTF-8), as 64 hex characters. The array marks where each member ends, so `["ab", "c"]` and `["a", "bc"]` fold differently, and a `None` member (`null`) differs from `""` (ITL-942; before v0.3 the members were concatenated with no separator). The folded column name then gains `{BLOCK_SEPARATOR}{stream.pipeline_hash().to_hex(system_tag_n_char)}` via `arrow_utils.append_to_system_tags()`. |
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
                        pa.array([input_data.content_hash().digest],
                                 type=pa.large_binary()))
```

The input data hash enters the preimage as its raw digest, without its label, so the entry
ids do not depend on the name of the Arrow label. The stored `__input_data_hash` column keeps
the labelled form `b"<label>:<digest>"`.

Because `tag.as_table(columns={"system_tags": True})` includes all chained system tag
columns (see §2, site 6), the preimage implicitly captures the full join provenance of
the row.

---

### Site 7 — `compute_base_entry_id()`

| Field | Value |
|---|---|
| **Inputs** | All system tag columns from `tag.as_table(columns={"system_tags": True})` (including join-chained columns) + `INPUT_DATA_HASH_COL` (`input_data.content_hash().digest` as `pa.large_binary()`) |
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
| **Inputs** | System tags + `INPUT_DATA_HASH_COL` (`input_data.content_hash().digest` as `pa.large_binary()`) + `_SIDE_EFFECT_RECOMPUTATION_INDEX_COL` (fixed `0`, `pa.int32()`) |
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
| **Output format** | `str` of the form `"{method}:{digest}::{method}:{digest}"`. This string keeps its labels in v0.3; the library feeds it into no hash computation |
| **Uniqueness guarantee** | Unique per `(pod topology, tag lineage, input_data content)`; exposed to pod functions as an idempotency key. Pod-version scoping is provided by the `pipeline_hash()`-derived table path, not by the preimage. |
| **Known exclusions** | When `track_completion=True` (default): `run_id` is **excluded** — hash is run-independent for idempotency. When `track_completion=False` **and** `pipeline_run_id` is set: `run_id` is appended as a third `::` component so that each run produces a distinct hash. |

---

## 5. Data Function URI Hash

`DataFunctionBase.uri` is a tuple used as the canonical identity of a data function:

```
uri = (canonical_function_name, output_schema_hash, major_version, data_function_type_id)
```

The `output_schema_hash` component (site 11) is the only hash in the URI; the other
components are plain strings. `major_version` comes from the `version` argument, which
defaults to `"v0.0"` (major `v0`) for every entry point: `@function_pod`,
`FunctionPod.from_fn`, `side_effect_function_pod` and `PythonDataFunction` (ITL-942: before
v0.3, `from_fn` and `side_effect_function_pod` defaulted to `"v1.0"`).

---

### Site 11 — Output schema hash

| Field | Value |
|---|---|
| **Inputs** | `output_data_schema` — a `Schema` mapping output column names to Python types |
| **Algorithm** | `SemanticAwarePythonHasher.hash_object(output_data_schema).to_hex()` |
| **Output format** | `str`: the bare hex digest, 64 characters, no label (the hash is part of a name: the function `uri`, table paths and source-info tokens) |
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
| **Inputs** | Sorted `content_hash().to_hex()` digests of all DAG **leaf** nodes (nodes with no downstream successors in the execution DAG) |
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

## 8. Array Value Hash

**Provisional.** The value hashes of the library value types, `numpy.ndarray` (this section),
`torch.Tensor` ([below](#torch-tensor-value-hash)), polars `DataFrame` and `Series`
([1e](#1e-polars-dataframe-and-series-values)) and pandas `DataFrame` and `Series`, and their
logical type identifiers, are provisional in v0.3 and may change in v0.4. Using one issues
`ProvisionalHashWarning` once per type per process (see
[Provisional value hashes](../concepts/arrays.md#provisional-value-hashes)). The types are
declared in the `provisional` list of `orcapod/contexts/logical_types.json`, by extension name;
that list enters no hash, identifier or stored path.

**Value hash labels.** A type whose values are hashed by a dedicated handler labels its hash
`<type>_v<N>`, for example `ndarray_v1` or `torch_tensor_v1`. The label is the `ContentHash.method`; the type's
implementer chooses it. The bytes produced under an existing label never change. A new
definition gets a new label, so a hash always says which definition produced it. The label
is not part of any hash input (see [The hash label rule](#the-hash-label-rule)).

A `numpy.ndarray` is stored as a `.npy` file and hashed by value (ITL-981). The hash is
defined on the array, never on the stored bytes. The semantic hasher's `NumpyArrayHandler`
returns `ContentHash(method="ndarray_v1", digest=D)`. What is stored and what reads back is
described in [NumPy Arrays](../concepts/arrays.md).

`D` is computed in two steps. Notation: `u8` is one byte, `u64le` and `i64le` are 8-byte
little-endian unsigned and signed integers, and `lp(x) = u64le(len(utf8(x))) || utf8(x)`.

**Step 1, the element digest `E`.** Take the array in canonical form: C order,
little-endian, structured fields packed in declared order. With dtype tag `T` (below), shape
`(d0, ..., dn-1)` and the canonical element bytes `B`:

```
E = SHA-256( lp(T) || u64le(n) || i64le(d0) || ... || i64le(dn-1) || u64le(len(B)) || B )
```

`E` covers the dtype, the shape and every element bit, and nothing else. It is shared with
other array types: a `torch.Tensor` computes the same `E`, see
[Torch tensor value hash](#torch-tensor-value-hash) below.

**Step 2, the value hash `D`.**

```
D = SHA-256( lp("orcapod.numpy.ndarray") || E || u8(memory_order)
             || lp(byte_order) || lp(struct_layout) )
```

| Field | Value | Canonical value (written when the property is ignored) |
|---|---|---|
| leading text | the type's extension name, `orcapod.numpy.ndarray` | |
| `memory_order` | `1` when the array is Fortran-contiguous and not C-contiguous, else `0` | `0` |
| `byte_order` | one character per scalar field of the dtype, depth first in declared field order, a subarray field counting once: `<` little-endian, `>` big-endian, `\|` for a type without byte order (`bool`, 8-bit integers, `S`, `V`). NumPy's native marker `=` is resolved to the machine's order. `float32` gives `<` | the same text with every `>` replaced by `<` |
| `struct_layout` | JSON without whitespace of `L(dtype)`: `null` for an unstructured dtype, otherwise the list `[itemsize, [offset_1, L(field_1)], [offset_2, L(field_2)], ...]` in declared field order; a subarray field uses its base dtype. `float32` gives `null` | `L` of the packed dtype |

Every field is always present and length-prefixed or of fixed width, so two different
inputs never share a preimage.

**The ignore list.** The three properties `memory_order`, `byte_order` and `struct_layout`
count by default. A data context can list any of them as ignored for the type. An ignored
property's field is still written, with its canonical value, so arrays that differ only in
ignored properties have equal `D`, and with all three ignored `D` equals the default `D` of
the canonical array. The list itself is not in the preimage. It is in the type's extension
metadata, `{"ignore":[...],"version":1}` (names sorted, keys sorted, no whitespace), and so
in the logical type identifier: two lists are two logical types. A bare array is hashed with
the data context's list; a cell is hashed with the list of its own column.

**Cells.** Inside a table hash, an array cell becomes the token
`b"orcapod.numpy.ndarray;7b2269676e6f7265223a5b5d2c2276657273696f6e223a317d::" + D`: the
logical type's identifier (the extension name, `;`, the extension metadata
`{"ignore":[],"version":1}` in hex), then the digest, without the label. The cell is decoded
to an array first; the stored `.npy` bytes are never hashed.

| Variation | Same `D` by default? |
|---|---|
| Dtype (including width, units, string width), shape, any element bit, structured field names or field order | no |
| C against Fortran memory order | no (`memory_order`) |
| Little- against big-endian byte order | no (`byte_order`) |
| Structured field offsets or item size (aligned against packed) | no (`struct_layout`) |
| Step slices, reversed or broadcast views against their C-contiguous copy | yes |
| `np.matrix`, `np.recarray`, `np.memmap` against the plain array | yes |
| The `writeable` flag, dtype `metadata`, the `align` flag, padding bytes | yes |

The last three rows are what the `.npy` storage does not keep: a value read back from a
store must hash like the value that was written.

### Dtype tags

The tag names the element type without a byte order. For a structured dtype, the tag is a
JSON list with one `[name, tag]` entry per field, or `[name, tag, [dims]]` for a subarray
field, in declared field order. It is written with no whitespace and with non-ASCII
characters escaped (`json.dumps(..., separators=(",", ":"), ensure_ascii=True)`).

| NumPy | Tag | Canonical element bytes |
|---|---|---|
| `bool` | `bool` | one byte, 0 or 1 |
| `int8` ... `int64`, `uint8` ... `uint64` | `int8` ... `uint64` | two's complement or unsigned, little-endian |
| `float16`, `float32`, `float64` | same names | IEEE 754, little-endian |
| `complex64`, `complex128` | same names | real part, then imaginary part |
| `datetime64[ns]`, `datetime64[10ms]`, ... | `datetime64[ns]`, `datetime64[10ms]`, ... | `int64`, little-endian (NaT is the minimum) |
| `timedelta64[s]`, ... | `timedelta64[s]`, ... | `int64`, little-endian |
| `U5` | `ucs4[5]` | five UCS-4 code units, each `uint32` little-endian, NUL-padded |
| `S3` | `bytes[3]` | three raw bytes, NUL-padded |
| `V4` | `void[4]` | four raw bytes |
| `[("b", "f8"), ("a", "i4", (2,))]` | `[["b","float64"],["a","int32",[2]]]` | fields packed in tag order |

### Reference implementation

The implementation below uses only the standard library, and the vectors are pinned:

```python
import hashlib
import struct

def lp(text: str) -> bytes:
    data = text.encode("utf-8")
    return struct.pack("<Q", len(data)) + data

def element_digest(tag: str, shape: tuple[int, ...], data: bytes) -> bytes:
    p = lp(tag) + struct.pack("<Q", len(shape))
    p += b"".join(struct.pack("<q", d) for d in shape)
    p += struct.pack("<Q", len(data)) + data
    return hashlib.sha256(p).digest()

def ndarray_hash(
    tag: str,
    shape: tuple[int, ...],
    data: bytes,
    memory_order: int = 0,
    byte_order: str = "<",
    struct_layout: str = "null",
) -> str:
    p = lp("orcapod.numpy.ndarray") + element_digest(tag, shape, data)
    p += bytes([memory_order]) + lp(byte_order) + lp(struct_layout)
    return hashlib.sha256(p).hexdigest()

six = struct.pack("<6f", 0, 1, 2, 3, 4, 5)
print(element_digest("float32", (2, 3), six).hex())
# 318f6e503e8064e89ce6039453fce709f8e01f60c501af2b63bbbe9cf8fbf90e
print(ndarray_hash("float32", (2, 3), six))
# bef92ae814ed78857164d1aea02e5590ff1ab7f00179f4c75d17aec7cae27317
print(ndarray_hash("float32", (2, 3), six, memory_order=1))
# bb8d317f7bd44a4832eeb7468867b2137621fb539b084de988426dd5928af130
print(ndarray_hash("float32", (2, 3), six, byte_order=">"))
# 1d5b6c2eb0ceaafd3758356256d651c5edf9ca0efa4deecf12c76c4e15bdb637
print(ndarray_hash("float64", (), struct.pack("<d", 2.5)))
# 642ad052de6aa53ffd059e5cd9a2eeeb5519caa3d3eb12efcf9e0c3fdbeab17b
print(
    ndarray_hash(
        '[["a","int8"],["b","float64"]]',
        (1,),
        struct.pack("<bd", 1, 0.5),
        byte_order="|<",
        struct_layout="[16,[0,null],[8,null]]",
    )
)
# a47c34b71f1d8c812f52d3a3b78ef6dbafb3b9adb320123b885cbacf72fa50b8
```

The same values through Orcapod:

```python
import numpy as np

from orcapod.contexts import get_default_context

hasher = get_default_context().semantic_hasher
a = np.arange(6, dtype=np.float32).reshape(2, 3)
print(hasher.hash_object(a).to_string())
# ndarray_v1:bef92ae814ed78857164d1aea02e5590ff1ab7f00179f4c75d17aec7cae27317
print(hasher.hash_object(np.asfortranarray(a)).to_string())
# ndarray_v1:bb8d317f7bd44a4832eeb7468867b2137621fb539b084de988426dd5928af130
print(hasher.hash_object(a.astype(">f4")).to_string())
# ndarray_v1:1d5b6c2eb0ceaafd3758356256d651c5edf9ca0efa4deecf12c76c4e15bdb637
print(hasher.hash_object(np.array(2.5)).to_string())
# ndarray_v1:642ad052de6aa53ffd059e5cd9a2eeeb5519caa3d3eb12efcf9e0c3fdbeab17b
aligned = np.dtype([("a", "i1"), ("b", "<f8")], align=True)
print(hasher.hash_object(np.array([(1, 0.5)], dtype=aligned)).to_string())
# ndarray_v1:a47c34b71f1d8c812f52d3a3b78ef6dbafb3b9adb320123b885cbacf72fa50b8
```

### Torch tensor value hash

A `torch.Tensor` value (since v0.3, ITL-871) is hashed by `TorchTensorHandler`, which returns
`ContentHash(method="torch_tensor_v1", digest=D)`. The hash is defined on the tensor, never on
its stored bytes (a cell is a `torch.save` archive, whose bytes PyTorch does not promise to
keep across versions). It has two steps.

Step 1 is the element digest `E` of the array value hash above, over `(T, shape, B)`,
where `T` is the PyTorch dtype name (`float32`, `bfloat16`), and `B` holds the logical
elements (lazy conjugate and negative bits applied) in C order, little-endian. The dtypes
NumPy does not have give their raw bits: `bfloat16` the 16-bit patterns, `complex32` the real
then the imaginary `float16` of each element, and the `float8` tags (`float8_e4m3fn`,
`float8_e5m2`, `float8_e4m3fnuz`, `float8_e5m2fnuz`, `float8_e8m0fnu`) one byte per element.

Step 2, the value hash:

```
lp(x) = u64le(len(utf8(x))) || utf8(x)
P = lp("orcapod.torch.tensor")
 || E                                  (32 bytes)
 || u8(requires_grad) || u8(conj_bit) || u8(neg_bit)
 || u64le(n) || i64le(s0) || ... || i64le(sn-1)
 || lp(class name)
D = SHA-256(P)
```

`s0 ... sn-1` are the strides, in elements, and the class name is `torch.Tensor` or
`torch.nn.parameter.Parameter`. The leading text is the type's extension name; it keeps a
tensor apart from every other array type, so a tensor and an ndarray with equal elements
never share a hash, also inside an untyped Python structure.

The five properties of step 2 are read from the tensor **as it is stored**
([Arrays and Tensors](../concepts/arrays.md#how-a-tensor-is-stored)): a tensor that does not
cover its buffer is stored as `clone(memory_format=torch.preserve_format)`, so its strides are
those of the clone and its lazy bits are 0. This is what makes
`hash(read(write(v))) == hash(v)`.

Each of the five properties has an option name: `requires_grad`, `memory_layout`, `conj_bit`,
`neg_bit`, `subclass`. A name in the type's ignore list (the `ignore` array of the extension
metadata, `{"ignore":[...],"version":1}`) writes that property's slot with its canonical
value instead of the real one: `0`, the strides of a C-contiguous tensor of that shape (the
last is 1, each earlier one is the next stride times the next dimension's size, a size of 0
counting as 1), `0`, `0`, `torch.Tensor`. The slot is always present and the list itself is
not in `P`. So values that differ only in ignored properties hash the same, and with all five
ignored `D` equals the strict `D` of the plain contiguous tensor. A bare value is hashed with
the list of the data context; a stored cell with the list of its own column. The cell token is
`<identifier text> || "::" || D`, where the identifier text (the extension name, `;`, the
metadata in hex) holds the list: for the default,
`b"orcapod.torch.tensor;7b2269676e6f7265223a5b5d2c2276657273696f6e223a317d::" + D`.

| Variation | Same `D` by default? |
|---|---|
| Dtype, shape, any element bit | no |
| `requires_grad` | no |
| Strides of a tensor that covers its buffer (transposed, `channels_last`) | no |
| Lazy conjugate or negative bit of a tensor that covers its buffer | no |
| `torch.Tensor` against `torch.nn.Parameter` | no |
| Device (CPU, CUDA, MPS) | yes |
| Autograd history (`grad_fn`, `.grad`) | yes |
| A view of a larger buffer against its `clone(memory_format=torch.preserve_format)` | yes |

A reference implementation with the standard library only, with `lp` and `element_digest`
from the array reference above:

```python
def torch_tensor_hash(
    element: bytes,
    requires_grad: bool,
    conj_bit: bool,
    neg_bit: bool,
    strides: tuple[int, ...],
    class_name: str,
) -> str:
    p = lp("orcapod.torch.tensor") + element
    p += bytes([requires_grad, conj_bit, neg_bit]) + struct.pack("<Q", len(strides))
    p += b"".join(struct.pack("<q", s) for s in strides) + lp(class_name)
    return hashlib.sha256(p).hexdigest()

# torch.arange(6, dtype=torch.float32).reshape(2, 3)
elements = element_digest("float32", (2, 3), struct.pack("<6f", 0, 1, 2, 3, 4, 5))
print(torch_tensor_hash(elements, False, False, False, (3, 1), "torch.Tensor"))
# 118005088af13c5566b5bc6e798fd5906520e34da424811f6ba128ea2fbd8fcb
```

The same value through Orcapod (PyTorch is optional, so this sample needs it installed):

```python
import torch

print(hasher.hash_object(torch.arange(6, dtype=torch.float32).reshape(2, 3)).to_string())
# torch_tensor_v1:118005088af13c5566b5bc6e798fd5906520e34da424811f6ba128ea2fbd8fcb
```

### The Arrow schema hash

`StarfixArrowHasher.hash_schema` hashes each field under the same rule as `hash_table`. A
logical type that has a value handler (`uuid.UUID`, `File`, `Directory`, `numpy.ndarray`,
the pandas types, and `list` or `set` of them) is hashed as a `large_binary` field that
carries the type's identifier; its storage type is not part of the hash. A logical type
without a value handler (`Path`, dataclasses and other user classes) is hashed as its
storage type with the identifier, because its stored values are what a table hash covers.
Orcapod's own schema hash (site 3), type hash and pipeline hash do not use this method:
they hash Python types, where a logical type contributes its identifier text only.

## 9. Arrow Table Hash (`arrow_v1`)

Every Arrow-derived hash in the sites above (row data hashes, `record_id`s built from a
default `source_id`, base and pipeline entry ids, operator and cached-source record hashes,
`pa.Table` values) comes from `StarfixArrowHasher.hash_table`, which returns
`ContentHash(method="arrow_v1", digest=D)`. `D` is 35 bytes: the starfix hash format
number `00 00 02` followed by a SHA-256 digest, computed by `starfix` 0.5 (hash format 2) and
byte-identical to the Rust `starfix` crate. The byte layout is specified in starfix's
`docs/byte-layout-spec.md`.

**Label.** `arrow_v1` names this procedure: the replacement of extension-typed cells by
their value digests, then the starfix digest. It moves to `arrow_v2` only when an existing
table would get a different digest. Its number is not kept equal to the starfix format number.

| Label | starfix | Format prefix | Orcapod |
|---|---|---|---|
| `arrow_v0.1` | 0.4 | `00 00 01` | up to v0.2 |
| `arrow_v1` | 0.5 | `00 00 02` | v0.3 |

**What is part of the hash.** Column names, Arrow types (with large and non-large layouts of
strings, binaries and lists hashing alike), nullability at every depth (top-level columns,
struct fields and list items, whether or not the column holds data), every value, row order, list element
order, list lengths at every depth, null structs (distinct from structs whose fields are all
null) and the row count. Column order and struct field order are not part of the hash:
this is the hash of Orcapod's own tables (streams, datagrams, rows), which ignore order on
purpose. A value whose own equality depends on order is hashed with
`StarfixArrowHasher.hash_table_ordered` instead, which is the same procedure with starfix's
`ordered_columns` and `ordered_struct_fields` options on, so that column order and struct
field order at every depth are part of the digest; pandas values use it (§10). How a
table is split into chunks or slices is not part of it either. A `fixed_size_list<T>[n]`
column hashes differently from a `large_list<T>` column with the same values. Extension
columns are hashed as their storage type plus their extension name and metadata, including
pyarrow's canonical extension types (`arrow.uuid`, `arrow.fixed_shape_tensor`); only
`ARROW:extension:*` metadata is hashed, all other metadata is ignored. List item names
(`item`, `element`) are not part of the hash.

**The hashed type is the declared type.** Before starfix sees a table, Orcapod replaces the
values of logical types that have a value hasher (`Path`, `File`, `Directory`, `UUID`, ...)
by their content-hash tokens, typed `large_binary`. The type each column is hashed under is
derived from its declared type alone (`SemanticHashingVisitor.hashed_type`): every struct and
list field keeps its name, nullability and metadata. A column therefore hashes under one type
whether it has data, only nulls or no rows (ITL-942: before v0.3 the type was rebuilt from the
first non-null value, which dropped nested nullability for a column with data).

**What cannot be hashed.** starfix raises `ValueError`, naming the column path and type, for:
data of `null`, `map`, interval, `string_view` and `binary_view` columns (whatever the row
count; their schemas hash); union and run-end-encoded types; two schema nodes sharing a path
(duplicate column names, or a name containing `/` that aliases a struct child, such as a
column `"a/b"` next to a struct column `a` with a child `b`); and a non-nullable field holding
a null. A source whose table has such a column fails when it is constructed.

#### The data context of a stored row

A label names the algorithm that produced one hash. It does not say which generation of
Orcapod's hashing a stored row belongs to. That is the **data context**: one key that changes
whenever any existing hash value can change (the hashers, the identity definitions, the
record-id recipes, the libraries behind them). Orcapod v0.3 writes `std:v1:default`;
everything stored before v0.3 is the earlier generation, `std:v0.1:default`. The manifest of
the current data context is `orcapod/contexts/data/v1.json`. It is frozen at the release of
the library version that contains it and never edited afterwards. It names the hashers, the
definitions that live in code, the libraries the hashes depend on and what changed from the
previous data context. Logical types are not part of it: they are registered in
`orcapod/contexts/logical_types.json` and versioned by their own identifier.

Every row Orcapod stores names its data context in the column `__data_context`:

| Table | Layout (last path component) |
|---|---|
| Function-node pipeline table | `pdb_v2` |
| Function result table | `rdb_v2` |
| Operator-node cache table | `odb_v1` |
| Cached-source table | `sdb_v1` |
| Side-effect invocation log | `tdb_v2` |

The layout component is always the last component of a table's path, also when a
`CachedSource` is given an explicit `cache_path`.

**Rows of the current data context are the current entries.** Key lookups match rows of the
current data context only, and every read of a whole table (a node's `as_table()`,
`iter_data()`, `get_all_records()`, `as_source()`, a cached function's
`get_all_cached_outputs()`, a stored operator node, a `CachedSource`) leaves rows of another
data context out. Those rows are never changed or deleted, and a read that leaves rows out
logs one warning per table with their number (logger `orcapod.contexts.stored`).

**Tables written before v0.3** have no `__data_context` column and sit at an older layout
(`pdb_v1`, `rdb_v1`, `tdb_v1`; operator-node and cached-source tables, and the function
tables of Orcapod v0.1, had no layout component). Orcapod does not read them as results and never modifies them. When it finds
one it logs one warning with the number of rows:

```text
Found 3 rows in stored table '...' written by an earlier version of Orcapod (older table
layout, data context std:v0.1:default). They are not used and are left unchanged; they can
be migrated later.
```

**How they are found.** The path of a table holds hashes (of the function's output schema,
of the pipeline), and a later version of Orcapod can compute other hashes, so an older table
is not only looked for at the path the current version computes. The beginning of a path
holds no hash: the job, then the name of the function (`<job>/<function>/...` and
`<job>/_result/<function>/...`; `side_effect_function/<function>` for a function pod that
takes the invocation context; `side_effects/<pod>` for a side-effect pod). A node lists the
tables stored under that beginning (`list_table_paths` of the database) and tells old from
current by the last path component, the layout: `pdb_v2`, `rdb_v2`, `tdb_v2`, `odb_v1` and
`sdb_v1` are current, every other table under the name was written before v0.3. Tables
found this way are not read, so they are reported without a number of rows, once per node:

```text
Found 2 tables written by an earlier version of Orcapod under 'my_job/double' (older table
layout, data context std:v0.1:default): 'my_job/double/.../pdb_v1', ... They are not used
and are left unchanged; they can be migrated later. Tables of another function or node with
the same name are included.
```

Operator caches and cached sources have no name in their path that survives a change of
hashes. A job run lists the tables under the job's name once and reports one total of the
older tables that no function or side-effect node of the job reports itself: operator
caches, cached sources stored under the job, and tables of nodes the job no longer has.

```text
Store 'my_job' holds 2 more tables written by an earlier version of Orcapod (operator
caches, cached sources, or nodes not in this job): '...'. They are not used and are left
unchanged; they can be migrated later.
```

Two consequences of finding tables by name: a table of another function or pod with the
same name in the same job, and a table of another node of the same function, count as well.
A store that cannot list its tables (`TableListingNotSupportedError`) is not searched this
way; tables at the path the current version computes are still found.

Nodes that perform side effects are the exception. A completion is looked up by its key, so
completions stored under another data context would not be found and every side effect would
be performed again. There are two kinds: a side-effect pod (`@side_effect_pod`, `@sink_pod`,
`@tap_pod`), whose completions are in its invocation log, and a function pod that takes the
invocation context (`@function_pod(ctx_arg=...)`), whose completions are in its pipeline and
result tables. Either raises `SideEffectLogDataContextError`, naming the tables, before
doing anything when it finds a table at an older layout anywhere under its name, or
completions of another data context in its current tables. Because every older table under
the name counts, an unrelated pod of the same name in the same job raises the error too;
the error names every table so that you can tell. When the store cannot list its tables,
the node logs one warning saying that it could not look for older completions, and
proceeds.

A node stores rows for inputs of its own data context only: an input of another data context
raises `InputValidationError` naming both keys before anything is written. This holds for
nodes, caches and cached sources used directly; inside a `PipelineJob` every input is wrapped
in a source node of the default data context, so the refusal is not reached there. (What
should eventually happen instead is tracked in ITL-951.) `std:v0.1:default` itself is not shipped
with v0.3 and does not resolve; a pipeline or job file saved by v0.2 loads under the current
data context with a warning when its schema types are supported. Saved files with name-only
logical extension types are rejected: current saved schema text requires the full logical
type identifier. See [Saving and loading a job](../concepts/function-pods.md#saving-and-loading-a-job).

`stored_data_contexts(database, record_path)` reads a stored table and counts its rows by
data context. A table without the column reports all its rows under `std:v0.1:default`.

```python
from orcapod import PipelineJob, function_pod
from orcapod.contexts import stored_data_contexts
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys="doubled")
def double_for_contexts(x: int) -> int:
    return 2 * x


context_store = InMemoryArrowDatabase()
context_job = PipelineJob(name="contexts_demo", database=context_store)
with context_job:
    rows = DictSource(data=[{"k": "a", "x": 1}, {"k": "b", "x": 2}], tag_columns=["k"])
    _ = double_for_contexts.pod(rows, label="dbl")
context_job.run()

for path in sorted(context_store.list_sources()):
    print(path[-1], stored_data_contexts(context_store, path))
# rdb_v2 {'std:v1:default': 2}
# pdb_v2 {'std:v1:default': 2}
```

```python
import pyarrow as pa
from orcapod.contexts import get_default_context

hasher = get_default_context().arrow_hasher
digest = hasher.hash_table(pa.table({"x": pa.array([1, 2, 3], pa.int64())}))
print(digest.method, digest.digest[:3].hex())
# arrow_v1 000002
```

## 10. pandas Value Hash (`pandas_dataframe_v1`, `pandas_series_v1`)

This hash is provisional in v0.3 (see [section 8](#8-array-value-hash)).

A `pandas.DataFrame` or `pandas.Series` value (a pod input or output, a value in a source
column, or any object given to the semantic hasher) is hashed by
`pandas_frame_content_hash` / `pandas_series_content_hash` in
`orcapod/hashing/pandas_value_hash.py`.

The pandas dependency is capped below 3 (`pandas>=2.2.3,<3`) for v0.3.0: the hash and its
round trip are verified on pandas 2 only. ITL-1054 lifts the cap.

Storage and hashing are separate (developer, November 12, 2026). Storage is the library's
own serialization: the Arrow IPC stream of `pa.Table.from_pandas(value,
preserve_index=True)`, read back with `to_pandas()`. The hash is defined by Orcapod and is
about the content of the value; it is never a hash of the stored bytes.

```text
ContentHash("pandas_dataframe_v1" | "pandas_series_v1",
            sha256(canonical_json(descriptor) + b"\x00" + D))
```

- `D` is the 35-byte digest of `StarfixArrowHasher.hash_table_ordered` (§9) over the
  **values table**: `pa.Table.from_pandas(value, preserve_index=True)` with its fields
  renamed by position, `c0 .. cN-1` for the columns in frame order, then `i0 .. iM-1` for
  the index levels in order. A Series is digested as its one-column frame. `D` covers the
  values, the Arrow types, row order, the row count, **column order, index level order and
  struct field order**.
- `descriptor` is canonical JSON (sorted keys, `,` and `:` separators, UTF-8) in a grammar
  Orcapod owns, built from the pandas objects themselves. It carries what the Arrow values
  do not show: `attrs`, typed labels, `columns.names`, the dtype of the column axis, index
  level names and the pandas dtypes.
- The label never enters the computation; only the bytes of `D` do.

```text
frame:  {"kind": "orcapod.pandas.dataframe", "height": <rows>, "attrs": <attrs>,
         "columns": [[<label>, <dtype>], ...],      in column order
         "columns_names": [<label>, ...],           columns.names, one per column level
         "columns_dtypes": [<dtype>, ...],          column axis, one per column level; a
                                                    pd.ArrowDtype level is
                                                    {"arrow": <Arrow type text>}
         "index": [[<name>, <dtype>], ...]}         index levels in order: name, dtype
series: {"kind": "orcapod.pandas.series", "height": <rows>, "attrs": <attrs>,
         "name": <label>, "dtype": <dtype>,
         "index": [[<name>, <dtype>], ...]}

<attrs> := attrs as the JSON object storage keeps ({1: "x"} is {"1": "x"}; a tuple is a
           list); attrs with an entry JSON cannot write are refused
<name>  := <str> | null                             an index level name as storage restores
                                                    it: its text
<label> := <str> | null | {"bool": b} | {"int": n} | {"float": <float.hex()>}
         | {"tuple": [<label>, ...]}
         | {"timestamp": [<count since the epoch, in unit>, <unit>, <tz or null>]}
         | {"date": <iso date>}
         | {"timedelta": [<count, in unit>, <unit>]}
         | {"period": [<ordinal>, <period alias>]}
         | {"nat": "datetime" | "timedelta"}
<dtype> := {"numpy": <numpy dtype name>}
         | {"masked": "Int8" | ... | "UInt64" | "Float32" | "Float64" | "boolean"}
         | "string"                                 pd.StringDtype, any storage
         | {"datetimetz": [<unit>, <tz>]}            <tz>: the text storage writes:
                                                    "UTC", "Europe/Berlin", "+01:00"
         | {"categorical": {"categories": [<dtype>, [<label>, ...]], "ordered": <bool>}}
         | {"period": <period alias>} | {"interval": [<subtype dtype>, <closed>]}
         | {"arrow": <Arrow type text>}             pd.ArrowDtype of a non-nested Arrow type:
                                                    str(type), "large_binary", "null",
                                                    "timestamp[ns, tz=UTC]"
         | "arrow"                                  pd.ArrowDtype of a nested, dictionary or
                                                    run-end type: the type is in D only
```

`kind` is the name of the value's logical type (its Arrow extension name).

**The hash is strict.** Everything about a value that can be changed is part of its
identity (developer, November 11 and 12, 2026), with one exception: **a property storage
does not restore never counts**, so that a value read back from a store hashes exactly like
the value that was written. `PANDAS_IDENTITY_PROPERTIES` in
`orcapod/hashing/pandas_value_hash.py` is the one list in code of every property below.

| Property | Part of the hash | Carried by |
|---|---|---|
| Column order | yes | `D` (ordered) |
| MultiIndex level order (frame and Series) | yes | `D` (ordered) |
| Struct field order in a `pd.ArrowDtype` struct column, at every depth | yes | `D` (ordered) |
| Values, row order, row count, Arrow type (int width, datetime unit and timezone) | yes | `D` |
| Index values, index level names (as the text storage writes), index dtype (as storage restores it) | yes | `D`, descriptor |
| `columns.name` / `columns.names` | yes | descriptor |
| Dtype of the column axis, per level (an `int64` versus an `object` Index of the same labels) | yes | descriptor |
| Column label type where the labels have one type (`0` versus `"0"`), MultiIndex columns versus flat labels | yes | descriptor |
| Series name, typed (`0` versus `"0"`); a frame versus its one-column Series | yes | descriptor |
| pandas dtype where the Arrow values coincide: `Int64` versus `int64`, `boolean` versus `bool`, `string` versus `object` | yes | descriptor |
| Categorical versus string; the category list, including unused categories; category order; `ordered` | yes | descriptor |
| `attrs` (as the JSON object storage keeps: `{1: "x"}` is `{"1": "x"}`) | yes, although pandas' equality ignores it | descriptor |
| `-0.0` versus `0.0` | yes, although pandas calls them equal: the values are hashed as bytes | `D` |
| `flags` | never: storage does not restore it | |
| Type of an index level name (`1` versus `"1"`) | never: storage writes the name as text | |
| Types of column labels of mixed type (`0` beside `"b"` versus `"0"` beside `"b"`) | never: storage writes such labels as text | |
| A masked, string or Arrow dtype of an index level (`Int64` versus `int64`) | never: storage restores an index level as NumPy | |
| `RangeIndex` versus an `int64` index with the same values | never: storage restores an `Index` | |
| `freq` of a `DatetimeIndex` or `TimedeltaIndex` | never: storage does not restore it | |
| Unused levels of a MultiIndex | never: storage does not restore them | |
| Storage of a string dtype (`string[python]` versus `string[pyarrow]`) | never: storage restores `string[python]` | |
| `object` dtype of a column that reads back as another dtype | never: see below | |
| Key order of dicts in an `object` column | never: a `dict` is unordered | |

**`object` columns are hashed in the form storage restores.** An `object` column or index
level whose values are not strings does not read back as `object`: `[1, 2]` reads back
`int64`, `[1, None]` reads back `float64`, `datetime` objects read back `datetime64[us]`.
Such a column is described and digested in the form it reads back in, so it hashes like the
typed column it becomes. An `object` column of lists or dicts stays `object`, but its
elements can change (a list `[1, None]` reads back `[1.0, nan]`), so the Arrow form of what
is read back is digested. Any other `object` column that reads back as `object` (strings,
bytes, decimals, dates) is hashed as it is.

**The axes are described as storage restores them.** Storage writes the name of an index
level as text. The index names `1` and `"1"` are therefore one name once read back, and
**they hash equal**: a hash that told them apart would change when the value is read from a
store. `columns.names` and a Series' name are restored with their types, so they stay typed
(`0` versus `"0"` differ). The dtype of each level of the column axis is restored, so it is
part of the hash. A timestamp or duration label is the integer count in its own unit, with
the unit.

More consequences of the same rule:

- **Column labels of mixed type hash as their text.** Storage restores labels of one type
  with that type (`0, 1` stay integers), but writes labels of mixed type as text: the
  columns `0, "b"` read back `"0", "b"`. So a frame with the columns `0, "b"` and one with
  `"0", "b"` hash equal.
- **An index level hashes as the NumPy level it reads back as.** Storage restores a column
  with its masked, string or Arrow dtype, but an index level as NumPy: an `Int64` index
  reads back `int64` (`float64` when it holds a missing value), a `string` index reads
  back `object`. So an `Int64` index and the `int64` index with the same values hash
  equal. Categorical, timezone-aware, period and interval index levels are restored and
  keep their dtype.
- **The categories of a categorical are described with the dtype storage restores.** The
  categories are read back from the Arrow dictionary: `object` categories `[1, 2]` read
  back `int64`, and `string` categories read back `object`. So a categorical with `object`
  categories `[1, 2]` and one with `int64` categories `[1, 2]` hash equal. The category
  list, its order, unused categories and `ordered` are restored and count.
- **A column is described with the dtype its Arrow schema restores.** A
  `pd.ArrowDtype(pa.string())` column reads back as `string`, so the two hash equal. An
  Arrow decimal column reads back as `object` holding `Decimal` values and is hashed in
  that form. Other `pd.ArrowDtype` columns are restored as they are, with their exact Arrow type.
- **A column that reads back as Arrow `null` is digested as an all-null `large_string`
  column.** Starfix refuses a `null` column, and storage cannot restore the type of a column
  with no value: an all-null or empty `pd.ArrowDtype` decimal column reads back as `object`
  holding `None`, exactly like a plain `object` column of `None`. Every column whose
  restored Arrow type is `null` is therefore digested as an all-null `large_string` column
  of the same length (a cast of a null array holds no value, so nothing is inferred), for a
  value and for its read-back alike. The dtype descriptor of both the all-null or empty
  decimal column and the all-null or empty `object` column is `{"numpy": "object"}`, so they
  **hash equal**; the height still counts, and a `string`-dtype column of `None` (descriptor
  `"string"`) stays different. Other all-null or empty columns (Arrow string, timestamp,
  integer, list or struct, `Int64`, `string`, categorical) keep their own types and are
  unchanged.
  A `pd.ArrowDtype` of a non-nested Arrow type is described by its Arrow type text, so
  `pd.ArrowDtype(pa.null())` (`{"arrow": "null"}`) never hashes like a
  `pd.ArrowDtype(pa.large_string())` column of `None`, and `binary` and `large_binary`
  columns (which storage restores as themselves, although D unifies large and non-large
  variants) hash differently. A decimal column is the exception that shows the rule: storage
  restores it as `object`, so two precisions hash equal. A nested `pd.ArrowDtype` (list,
  struct, map, union), a dictionary or a run-end type keeps the bare term `"arrow"`; none
  reads back today, and ITL-1054 decides what identifies one (so a `list` and a `large_list`
  column hash equal until then).
  A `pd.ArrowDtype` of a nested type with a `null` child (`list<null>`, `struct<a: null>`)
  is still refused by the hash, as before.
- **A timezone is the text storage writes for it** (`"UTC"`, `"Europe/Berlin"`,
  `"+01:00"`), whatever class holds it: `datetime.timezone(timedelta(hours=1))` is read
  back as another class of the same offset, and both hash equal.

The descriptor holds the column labels, `columns.names`, a Series' name and the index names
as read from the frame storage builds for the value's Arrow schema. The value's own labels
and names must still be in the grammar: a label of another type raises `ValueError`.

**`attrs` must be JSON.** Storage writes `attrs` as JSON. A key that is not text is written
as text (`{1: "x"}` reads back `{"1": "x"}`, and hashes like it), and a tuple reads back as
a list. `attrs` with an entry JSON cannot write (a `set`, an arbitrary object, a tuple as a
key) are refused: the hash and `python_to_storage` raise `ValueError` naming the key, on
every supported pyarrow version (pyarrow itself would drop the whole `attrs` with a
warning, or raise `TypeError`, depending on its version). With `attrs` in the ignore list
the hash does not read them (it never copies them either, so `attrs` that cannot be copied
do not matter then).

Storage refuses a value it would corrupt. Column labels and index names that collide
once written as text (the index name `1` with a column `"1"`; integer names of MultiIndex
levels) would be written as two fields of one name, and the value read back would have lost
its index. Distinct labels that read back equal (the labels `True` and `False` both read
back `True`) would come back as duplicates. `python_to_storage` raises `ValueError` in both
cases.

A label or dtype outside the grammar raises `ValueError` naming it (a dtype error names its
column). A value that cannot be converted to Arrow raises `ValueError`.

**What a data context can make the hash ignore.** Nothing is ignored by default. The
registration file (`orcapod/contexts/logical_types.json`, `hash_ignore`) can name, per type,
properties the hash ignores. The list is saved in the logical type's extension metadata, so
two lists are two logical types, and a column stored under another list is refused on read.
The preimage keeps its shape: an ignored property is hashed with a fixed value.

| Option | Type | When ignored |
|---|---|---|
| `column_order` | DataFrame | the columns are hashed sorted by the canonical JSON of their typed labels |
| `series_name` | Series | `name` is hashed as `null` |
| `struct_field_order` | both | starfix's `ordered_struct_fields` option is off for `D` |
| `attrs` | both | `attrs` is hashed as `{}` |

Every other property always counts, and the properties storage does not restore never do;
neither kind is an option.

**Names.** The logical types are `orcapod.pandas.dataframe` and `orcapod.pandas.series`
with metadata `{"ignore":[],"version":1}` (the version and the ignore list). Before v0.3 they were stored as `pandas.dataframe` /
`pandas.series`; those names are not registered any more, and a column stored under them is
refused on read. Before v0.3 a pandas value was also hashed as an Arrow table (§9, order
ignored), so a frame and the same frame with its columns reordered shared a hash, and a
cached pod could be served the result computed for the other one (ITL-897, ITL-887).

```python
import pandas as pd
from orcapod.contexts import get_default_context

semantic_hasher = get_default_context().semantic_hasher
frame = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
print(semantic_hasher.hash_object(frame).method)
# pandas_dataframe_v1
print(semantic_hasher.hash_object(frame) == semantic_hasher.hash_object(frame.copy()))
# True
print(semantic_hasher.hash_object(frame) == semantic_hasher.hash_object(frame[["b", "a"]]))
# False
print(semantic_hasher.hash_object(frame["a"]).method)
# pandas_series_v1
```
