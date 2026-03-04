# Identity & Hashing

orcapod maintains two parallel identity chains implemented as recursive Merkle-like hash
trees. These hashes are central to caching, deduplication, and database scoping.

## Two Identity Chains

### Content Hash (`content_hash()`)

Data-inclusive identity capturing the precise semantic content of an object. Changes when
data changes. Used for deduplication and memoization.

| Component | What Gets Hashed |
|-----------|-----------------|
| RootSource | Class name + tag columns + table content hash |
| PacketFunction | URI (canonical name + output schema hash + version + type ID) |
| FunctionPodStream | Function pod + argument symmetry of inputs |
| Operator | Operator class + identity structure |
| ArrowTableStream | Producer + upstreams (or table content if no producer) |
| Datagram | Arrow table content |
| DerivedSource | Origin node's content hash |

### Pipeline Hash (`pipeline_hash()`)

Schema-and-topology-only identity. Excludes data content so that different sources with
identical schemas share database tables. Used for database path scoping.

| Component | What Gets Hashed |
|-----------|-----------------|
| RootSource | `(tag_schema, packet_schema)` — base case |
| PacketFunction | Raw packet function object (via content hash) |
| FunctionPodStream | Function pod + input stream pipeline hashes |
| Operator | Operator class + argument symmetry (pipeline hashes of inputs) |
| ArrowTableStream | Producer + upstreams pipeline hashes (or schema if no producer) |
| DerivedSource | Inherited from RootSource: `(tag_schema, packet_schema)` |

### Why Two Hashes?

Consider a medical pipeline that processes patient data from different clinics. Both clinics
produce tables with schema `{patient_id: str, age: int, cholesterol: int}` but different
data.

- **Content hash** differs because the data differs — each clinic's results are cached
  separately.
- **Pipeline hash** is identical because the schema and topology match — both clinics' data
  can share the same database table, distinguished by system tags.

## The ContentHash Type

All hashes are represented as `ContentHash` — a frozen dataclass pairing a method identifier
with raw digest bytes:

<!--pytest-codeblocks:skip-->
```python
from orcapod.types import ContentHash

hash_val = source.content_hash()

# Various representations
print(hash_val.to_hex())    # Hexadecimal string
print(hash_val.to_int())    # Integer
print(hash_val.to_uuid())   # UUID
print(hash_val.to_base64()) # Base64 string
print(hash_val.to_string()) # "{method}:{hex_digest}"
```

The method name (e.g., `"object_v0.1"`, `"arrow_v2.1"`) enables detecting version mismatches
across hash configurations.

## Semantic Hashing

Content hashes use a `BaseSemanticHasher` that:

1. Recursively expands structures (dicts, lists, tuples).
2. Dispatches to type-specific handlers for each leaf value.
3. Terminates at `ContentHash` leaves (preventing hash-of-hash inflation).

This ensures that structurally identical objects produce identical hashes regardless of how
they were constructed.

## The Resolver Pattern

Pipeline hash uses a **resolver pattern** — a callback that routes objects to the correct
hash method:

- `PipelineElementProtocol` objects → `pipeline_hash()`
- Other `ContentIdentifiable` objects → `content_hash()`

This ensures the correct identity chain is used for nested objects within a single hash
computation.

## Argument Symmetry

Each pod declares how upstream hashes are combined:

- **Commutative** (`frozenset`) — upstream hashes sorted before combining. Used when input
  order is semantically irrelevant (Join, MergeJoin).
- **Non-commutative** (`tuple`) — upstream hashes combined in declared order. Used when
  input position is significant (SemiJoin).
- **Partial symmetry** — nesting expresses mixed constraints.

<!--pytest-codeblocks:skip-->
```python
# Commutative: Join(A, B) == Join(B, A)
join.argument_symmetry(streams)  # returns frozenset

# Non-commutative: SemiJoin(A, B) != SemiJoin(B, A)
semi_join.argument_symmetry(streams)  # returns tuple
```

## Packet Function URI

Every packet function has a unique signature:

```
(canonical_function_name, output_schema_hash, major_version, packet_function_type_id)
```

For Python functions, the identity structure additionally includes the function's bytecode
hash, input parameter signature, and Git version information.
