# API Reference

This section provides auto-generated API documentation from the orcapod source code. Browse
the reference by module:

## Core Types

- **[Types](types.md)** — `Schema`, `ColumnConfig`, `ContentHash`, `DataType`, `CacheMode`,
  `ExecutorType`, `NodeConfig`, `PipelineConfig`
- **[Errors](errors.md)** — `InputValidationError`, `DuplicateTagError`, `FieldNotResolvableError`
- **[Configuration](configuration.md)** — `Config` dataclass

## Data Containers

- **[Datagrams](datagrams.md)** — `Datagram`, `Tag`, `Packet`
- **[Streams](streams.md)** — `ArrowTableStream`

## Data Sources

- **[Sources](sources.md)** — `ArrowTableSource`, `DictSource`, `ListSource`,
  `DataFrameSource`, `DeltaTableSource`, `CSVSource`, `DerivedSource`

## Computation

- **[Packet Functions](packet-functions.md)** — `PythonPacketFunction`,
  `PacketFunctionBase`, `CachedPacketFunction`
- **[Function Pods](function-pods.md)** — `FunctionPod`, `FunctionPodStream`,
  `function_pod` decorator
- **[Operators](operators.md)** — `Join`, `MergeJoin`, `SemiJoin`, `Batch`,
  `SelectTagColumns`, `SelectPacketColumns`, `DropTagColumns`, `DropPacketColumns`,
  `MapTags`, `MapPackets`, `PolarsFilter`

## Execution

- **[Operator & Function Nodes](nodes.md)** — `FunctionNode`, `PersistentFunctionNode`,
  `OperatorNode`, `PersistentOperatorNode`
- **[Pipeline](pipeline.md)** — `Pipeline`, `PersistentSourceNode`
- **[Databases](databases.md)** — `InMemoryArrowDatabase`, `DeltaTableDatabase`
