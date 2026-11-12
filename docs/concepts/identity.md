# Identity & Hashing

Every pipeline element in Orcapod -- [sources](sources.md), [streams](streams.md),
[operators](operators.md), and [function pods](function-pods.md) -- carries two parallel
identity hashes. These hashes enable Orcapod to deduplicate computations, scope database
storage, and detect when a pipeline's structure or data has changed.

## Two identity chains

### `content_hash()` -- recursive source-inclusive identity

The content hash captures schema, topology, and the **identity of the sources** feeding the
pipeline. It is computed recursively: each element's content hash depends on its own identity
plus the content hashes of all its upstream elements, all the way back to the sources.

A source's identity is its class, its schema and its `source_id`. What the `source_id` is
depends on how the source was created:

- If you do not pass `source_id`, an in-memory source such as `DictSource` or
  `ArrowTableSource` derives it from the data, so two sources with different rows have
  different content hashes, and two sources with identical rows have the same one.
- File- and table-backed sources default to a name derived from their location (the file
  path for `CSVSource`, the table directory name for `DeltaTableSource`), not from the rows.
- If you pass `source_id` yourself, you are naming the source: two sources with the same
  class, schema and `source_id` have the same content hash even when their rows differ.

Rows are still identified individually. Each row carries its own content hash, and that
per-row hash is what caching uses.

**Used for:** deduplication and memoization. When a function runs inside a
[`PipelineJob`](function-pods.md#pipelinejob-database-backed-cached-execution), each input
row's content hash is checked against the database. If the hash already exists, the cached
result is returned without recomputation.

### `pipeline_hash()` -- schema and topology only

The pipeline hash captures the pipeline's **structure** -- schemas, function identities, and
how elements are connected -- but deliberately ignores source identity. Two pipeline elements
with identical schemas and the same computational graph have the same pipeline hash, even if
they are fed by completely different sources.

**Used for:** database scoping. Pipeline hash determines the database table path where results
are stored. This means that two pipeline nodes with the same function, the same input schema
and the same upstream steps share the same database table -- regardless of which source
produced the data.
This is a powerful feature: it means that running the same function on new data automatically
benefits from results already cached for previous data with the same schema.

## How it works in practice

Consider two sources with the same schema but different data:

```python
from orcapod.sources import DictSource

source_a = DictSource(data=[{"x": 1, "y": 2}], tag_columns=["x"])
source_b = DictSource(data=[{"x": 10, "y": 20}], tag_columns=["x"])

print(source_a.content_hash() == source_b.content_hash())
# False
print(source_a.pipeline_hash() == source_b.pipeline_hash())
# True
```

- The content hashes differ -- different source identity
- The pipeline hashes are equal -- same schema and structure

If both sources feed into the same function in a `PipelineJob`, the nodes share a database
table (same pipeline hash), but each row is stored and retrieved by its content hash.

## The Merkle chain

Pipeline hashes form a **Merkle chain** -- each element's pipeline hash commits to its own
identity plus the pipeline hashes of all its upstream elements.

### Base case: sources

A `RootSource`'s pipeline identity is simply its `(tag_schema, data_schema)`. Sources with
the same column names and types have the same pipeline hash, regardless of their data.

### Recursive case: downstream elements

Each downstream element (operator, function pod, or node) computes its pipeline hash from:

1. Its own identity (e.g., the function's name, major version, and output schema for a
   function pod; the operator class name for an operator)
2. The pipeline hashes of all its upstream streams

This creates a chain: changing any element's structure (renaming a column, renaming a
function or its output, adding an operator) changes the pipeline hash of that element and all downstream
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

When a function node runs inside a `PipelineJob`, each input row is handled in one of two
ways:

1. **Cached:** if a result for that input's content hash is already in the shared database
   table, it is read back and the function is not called.
2. **Computed:** otherwise the function runs, and the result is stored and returned.

This means that adding new data to a source only triggers computation for the new rows --
previously computed results are served from the cache. The node's own stream
(`iter_data()`, `as_table()`) contains the results for its current inputs only;
`get_all_records()` also returns rows stored by earlier runs of the same node -- same job
name, same source -- over inputs that are no longer present. It does not return rows that
other nodes wrote to the same shared table, such as the same function run over a source with
a different `source_id`.

### DB path scoping

The pipeline hash determines the database table path. Two nodes that apply the same function
to sources with the same schema, through the same upstream steps, will write to and read from
the same table. This is intentional: it maximizes cache reuse across pipeline runs. A
schema-preserving step added upstream (a filter, say) changes the pipeline hash and so the
table. This is the default `table_scope="pipeline_hash"` of a node; a node built with
`table_scope="content_hash"` also adds its content hash to the path, giving each source its
own table.

### Change detection

A function's identity is its **name**, its **output schema** and its **major version**
(`version="v1.0"` has major version 1; the default is `"v0.0"`). Changing any of these
changes the pipeline hash of its node and of all downstream nodes, which creates new database
table paths, so results stored under the old identity are not returned for the new one.

The function's source code is **not** part of that identity. Editing the body of a function,
or bumping only the minor version (`"v0.0"` to `"v0.1"`), leaves the pipeline hash unchanged,
and previously stored results keep being returned:

```python
from orcapod import function_pod

source = DictSource(data=[{"subject_id": "mouse_01", "age": 12}], tag_columns=["subject_id"])

@function_pod(output_keys="next_age")
def step(age: int) -> int:
    return age + 1

original = step.pod(source).pipeline_hash()

@function_pod(output_keys="next_age")
def step(age: int) -> int:
    return age + 2  # body edited, same name and version

print(step.pod(source).pipeline_hash() == original)
# True

@function_pod(output_keys="next_age", version="v1.0")
def step(age: int) -> int:
    return age + 2

print(step.pod(source).pipeline_hash() == original)
# False
```

When you change what a function computes, bump its major version (or rename it) so that
stale results are not reused.

## Your own classes in the identity

A class enters both chains through its **identity**: its kind, its name and the names and
types of its fields, in declared order. The identity is part of every schema hash, so it
reaches the pipeline hash of each source, pod and node that uses the class, and with it
the database table paths. Each stored value is hashed together with it, so it also
reaches the content hash.

The name depends on the kind of class:

- A class made with `@op.dataclass` is named by its class name (or the `name=` you give).
  Two such classes with the same name and fields share results, wherever each is
  defined.
- An ordinary `@dataclass` or a pydantic model is named by its module path and class
  name. Moving it to another module makes a new type.

In both kinds a class whose fields change or are reordered is a new type: its pods get
new pipeline hashes and new tables, and the results stored with the old definition stay
where they are.

[Your Own Data Types](your-own-data-types.md) describes the two kinds and when to use
each.

## How it connects to other concepts

- [Sources](sources.md) form the base case of the Merkle chain (schema-only for pipeline hash,
  source identity for content hash)
- [Streams](streams.md) carry both hashes and propagate them through the pipeline
- [Operators](operators.md) contribute their own identity to the chain
- [Function Pods](function-pods.md) run inside a `PipelineJob` use content hashes for
  memoization and pipeline hashes for DB scoping
- [Your Own Data Types](your-own-data-types.md) explains the identity of dataclasses and
  pydantic models
