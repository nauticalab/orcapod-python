# Types

Core type definitions used throughout orcapod.

## Schema

::: orcapod.types.Schema
    options:
      members:
        - merge
        - with_values
        - select
        - drop
        - is_compatible_with
        - empty
        - optional_fields
        - required_fields

## ColumnConfig

::: orcapod.types.ColumnConfig
    options:
      members:
        - all
        - data_only
        - handle_config

## ContentHash

::: orcapod.types.ContentHash
    options:
      members:
        - to_hex
        - to_int
        - to_uuid
        - to_base64
        - to_string
        - from_string
        - display_name

## CacheMode

::: orcapod.types.CacheMode

## ExecutorType

::: orcapod.types.ExecutorType

## NodeConfig

::: orcapod.types.NodeConfig

## PipelineConfig

::: orcapod.types.PipelineConfig

## Type Aliases

The following type aliases are used throughout the API:

| Alias | Definition | Description |
|-------|-----------|-------------|
| `DataType` | `type \| UnionType` | A Python type or union of types |
| `TagValue` | `int \| str \| None \| Collection` | Valid tag column values |
| `DataValue` | Scalar, path, or nested collections | Valid packet column values |
| `PacketLike` | `dict[str, DataValue]` | Dict mapping field names to values |
| `SchemaLike` | `dict[str, DataType]` | Dict mapping field names to types |
