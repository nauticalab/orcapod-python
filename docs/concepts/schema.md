# Schema & Column Configuration

Every stream in orcapod is self-describing. Schemas are embedded explicitly at every level
rather than resolved against a central registry.

## Schema

A `Schema` is an immutable mapping from field names to Python types, with support for
optional fields:

```python
from orcapod.types import Schema

schema = Schema({"name": str, "age": int, "email": str}, optional_fields={"email"})

print(schema)                  # Schema({'name': str, 'age': int, 'email': str})
print(schema.optional_fields)  # frozenset({'email'})
print(schema.required_fields)  # frozenset({'name', 'age'})
```

### Schema Operations

```python
from orcapod.types import Schema

a = Schema({"x": int, "y": str})
b = Schema({"y": str, "z": float})

# Merge (union) — raises on type conflicts
merged = a.merge(b)  # Schema({'x': int, 'y': str, 'z': float})

# Select specific fields
selected = a.select("x")  # Schema({'x': int})

# Drop specific fields
dropped = a.drop("y")  # Schema({'x': int})

# Compatibility check
a.is_compatible_with(b)  # True if shared keys have compatible types
```

### Output Schema

Every stream and pod exposes `output_schema()` returning a tuple:

<!--pytest-codeblocks:skip-->
```python
tag_schema, packet_schema = stream.output_schema()
```

- `tag_schema` — the schema of tag (metadata) columns.
- `packet_schema` — the schema of packet (data) columns.

## ColumnConfig

`ColumnConfig` controls which column groups are included in schema and data output. By
default, metadata columns are excluded for clean output.

```python
from orcapod.types import ColumnConfig

# Default: only user data columns
config = ColumnConfig()

# Include specific metadata
config = ColumnConfig(source=True)       # Include source-info columns
config = ColumnConfig(system_tags=True)  # Include system tag columns
config = ColumnConfig(meta=True)         # Include system metadata (__packet_id, etc.)

# Include everything
config = ColumnConfig.all()

# Explicitly data-only
config = ColumnConfig.data_only()
```

### ColumnConfig Fields

| Field | Default | Controls |
|-------|---------|----------|
| `meta` | `False` | System metadata columns (`__` prefix) |
| `context` | `False` | Data context column (`_context_key`) |
| `source` | `False` | Source-info provenance columns (`_source_` prefix) |
| `system_tags` | `False` | System tag columns (`_tag::` prefix) |
| `content_hash` | `False` | Per-row content hash column |
| `sort_by_tags` | `False` | Whether to sort output by tag columns |

### Using ColumnConfig

Pass `ColumnConfig` to `output_schema()` and `as_table()`:

<!--pytest-codeblocks:skip-->
```python
# Schema with source info columns
tag_schema, packet_schema = stream.output_schema(
    columns=ColumnConfig(source=True)
)

# Materialized table with everything
table = stream.as_table(columns=ColumnConfig.all())
```

## Column Naming Conventions

orcapod uses prefixes to distinguish column types:

| Prefix | Category | Example | ColumnConfig Field |
|--------|----------|---------|--------------------|
| `__` | System metadata | `__packet_id` | `meta` |
| `_source_` | Source info | `_source_age` | `source` |
| `_tag::` | System tag | `_tag::source_id::abc` | `system_tags` |
| `_context_key` | Data context | `_context_key` | `context` |
| *(no prefix)* | User data | `age`, `name` | Always included |

These prefixes are defined in `SystemConstant` (`system_constants.py`) and computed from
a shared `constants` singleton.
