# Datagrams, Tags & Packets

Datagrams are orcapod's universal immutable data containers. They hold named columns with
explicit type information and support lazy conversion between Python dict and Apache Arrow
representations.

## Datagram

A `Datagram` is the base container. It can be constructed from either a Python dict or an
Arrow table/record batch:

```python
from orcapod.core.datagrams import Datagram

# From a dict
dg = Datagram({"name": "Alice", "age": 30})

# Access as dict (always available)
print(dg.as_dict())  # {'name': 'Alice', 'age': 30}

# Access as Arrow table (lazily computed and cached)
table = dg.as_table()

# Schema introspection
print(dg.schema())  # Schema({'name': str, 'age': int})
print(dg.keys())   # ('name', 'age')
```

### Lazy Conversion

Datagrams convert between dict and Arrow representations lazily:

- If created from a dict, the Arrow table is computed on first `.as_table()` call and cached.
- If created from an Arrow table, the dict is computed on first `.as_dict()` call and cached.
- Content hashing always uses the Arrow representation for determinism.
- Value access always uses the Python dict for convenience.

### Immutability

Datagrams are immutable. Operations like `select()`, `drop()`, `rename()`, and `update()`
return new datagrams:

```python
from orcapod.core.datagrams import Datagram

dg = Datagram({"a": 1, "b": 2, "c": 3})

selected = dg.select("a", "b")       # Datagram({'a': 1, 'b': 2})
dropped = dg.drop("c")              # Datagram({'a': 1, 'b': 2})
renamed = dg.rename({"a": "alpha"}) # Datagram({'alpha': 1, 'b': 2, 'c': 3})
```

## Tag

A **Tag** is a datagram specialization for metadata columns. Tags are used for routing,
filtering, joining, and annotation. They carry additional **system tags** — framework-managed
hidden provenance columns.

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.datagrams import Tag

tag = Tag({"patient_id": "p1", "visit": "v1"})

# Regular keys (user-visible)
print(tag.keys())  # ('patient_id', 'visit')

# System tags (hidden provenance columns)
print(tag.system_tags())  # {...}
```

### Key Properties of Tags

- **Non-authoritative** — never used for cache lookup or pod identity computation.
- **Auto-propagated** — tags flow forward through the pipeline automatically.
- **Join keys** — operators join streams by matching tag columns.

### Tag Merging in Joins

When streams are joined:

- **Shared tag keys** act as the join predicate — values must match.
- **Non-shared tag keys** propagate freely into the joined output.

## Packet

A **Packet** is a datagram specialization for data payload columns. Packets carry additional
**source info** — per-column provenance tokens tracing each value back to its originating
source and record.

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.datagrams import Packet

packet = Packet({"age": 30, "cholesterol": 180})

# Source info (provenance pointers)
print(packet.source_info())
# {'age': 'source_abc::row_0::age', 'cholesterol': 'source_abc::row_0::cholesterol'}
```

### Source Info Format

Each packet column carries a source info string:

```
{source_id}::{record_id}::{column_name}
```

- `source_id` — canonical identifier of the originating source
- `record_id` — row identifier (positional like `row_0` or column-based like `user_id=abc123`)
- `column_name` — the original column name

Source info is **immutable through the pipeline** — set once when a source creates the data
and preserved through all downstream transformations.

## Column Naming Conventions

orcapod uses column name prefixes to distinguish metadata from user data:

| Prefix | Meaning | Example |
|--------|---------|---------|
| `__` | System metadata | `__packet_id`, `__pod_version` |
| `_source_` | Source info provenance | `_source_age` |
| `_tag::` | System tag | `_tag::source_id::abc123` |
| `_context_key` | Data context | `_context_key` |

These prefixes are controlled by `ColumnConfig` and excluded from standard output by default.
See [Schema & Column Configuration](schema.md) for details.
