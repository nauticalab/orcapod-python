# Identity & Hashing

Every pipeline element in Orcapod -- [sources](sources.md), [streams](streams.md),
[operators](operators.md), and [function pods](function-pods.md) -- carries two parallel
identity hashes. These hashes enable Orcapod to deduplicate computations, scope database
storage, and detect when a pipeline's structure or data has changed.

## Two identity chains

### `content_hash()` -- data-inclusive identity

The content hash includes **everything**: schema, topology, and actual data values. It changes
whenever any data in the pipeline changes. Two pipeline elements have the same content hash
only if they are completely identical -- same structure, same function, same input data, same
output.

**Used for:** deduplication and memoization. When a `FunctionNode` processes a packet, it
checks the packet's content hash against its database. If the hash already exists, the cached
result is returned without recomputation.

### `pipeline_hash()` -- schema and topology only

The pipeline hash captures the pipeline's **structure** -- schemas, function identities, and
how elements are connected -- but deliberately ignores data content. Two pipeline elements
with identical schemas and the same computational graph have the same pipeline hash, even if
they process completely different data.

**Used for:** database scoping. Pipeline hash determines the database table path where results
are stored. This means that two `FunctionNode` instances with the same function and the same
input schema share the same database table -- even if they process different source data. This
is a powerful feature: it means that running the same function on new data automatically
benefits from results already cached for previous data with the same schema.

## How it works in practice

Consider two sources with the same schema but different data:

```
source_a = DictSource(data=[{"x": 1, "y": 2}], tag_columns=["x"])
source_b = DictSource(data=[{"x": 10, "y": 20}], tag_columns=["x"])
```

- `source_a.content_hash() != source_b.content_hash()` -- different data
- `source_a.pipeline_hash() == source_b.pipeline_hash()` -- same schema and structure

If both sources feed into the same function via `FunctionNode`, the nodes share a database
table (same pipeline hash), but each packet is stored and retrieved by its content hash.

## The Merkle chain

Pipeline hashes form a **Merkle chain** -- each element's pipeline hash commits to its own
identity plus the pipeline hashes of all its upstream elements.

### Base case: sources

A `RootSource`'s pipeline identity is simply its `(tag_schema, packet_schema)`. Sources with
the same column names and types have the same pipeline hash, regardless of their data.

### Recursive case: downstream elements

Each downstream element (operator, function pod, or node) computes its pipeline hash from:

1. Its own identity (e.g., the function's name, version, and output schema for a function pod;
   the operator class name for an operator)
2. The pipeline hashes of all its upstream streams

This creates a chain: changing any element's structure (renaming a column, modifying a
function, adding an operator) changes the pipeline hash of that element and all downstream
elements, while leaving upstream hashes unchanged.

## The resolver pattern

Orcapod uses a resolver pattern to determine which hash method to call on different objects:

- Objects implementing `PipelineElementProtocol` (sources, operators, function pods, streams)
  route through `pipeline_hash()`
- Other `ContentIdentifiable` objects (like raw data values) route through `content_hash()`

This distinction matters when computing pipeline identity structures: a function pod's
pipeline hash depends on its upstream stream's pipeline hash (structural), not its content
hash (data-inclusive).

## Identity in practice

### Memoization

When a `FunctionNode` iterates over its input stream, it follows a two-phase process:

1. **Phase 1 (cached):** Read all existing records from the shared pipeline database table
   and yield them immediately.
2. **Phase 2 (compute):** For each input packet whose content hash is not already in the
   database, run the function, store the result, and yield it.

This means that adding new data to a source only triggers computation for the new rows --
previously computed results are served from the cache.

### DB path scoping

The pipeline hash determines the database table path. Two `FunctionNode` instances that
apply the same function to sources with the same schema will write to and read from the same
table. This is intentional: it maximizes cache reuse across pipeline runs.

### Change detection

If you modify a function (change its code, rename its output, bump its version), its identity
changes, which changes the pipeline hash of its node and all downstream nodes. This
automatically creates new database table paths, preventing stale cached results from being
returned for the modified pipeline.

## How it connects to other concepts

- [Sources](sources.md) form the base case of the Merkle chain with schema-only identity
- [Streams](streams.md) carry both hashes and propagate them through the pipeline
- [Operators](operators.md) contribute their own identity to the chain
- [Function Pods](function-pods.md) and `FunctionNode` use content hashes for memoization
  and pipeline hashes for DB scoping
