# API Reference

This section contains the full API reference for Orcapod, auto-generated from
source code docstrings.

## Package structure

The top-level `orcapod` namespace exposes the most commonly used entry points
directly:

| Symbol | Description |
|--------|-------------|
| [`FunctionPod`](function-pods.md) | Wraps a Python function to transform data in a stream |
| [`function_pod`](function-pods.md) | Decorator that attaches a `FunctionPod` to a callable |
| [`Pipeline`](pipeline.md) | A pipeline blueprint: records which pods are applied to which streams |
| [`PipelineJob`](pipeline.md) | A pipeline bound to sources and a database; the object you run |
| [`side_effect_pod`, `sink_pod`, `tap_pod`](function-pods.md) | Decorators for functions run for their side effects |

Everything else lives in subpackages:

| Subpackage | Description |
|------------|-------------|
| [`orcapod.sources`](sources.md) | Source classes for ingesting external data into pipelines |
| [`orcapod.operators`](operators.md) | Structural stream transformations (join, filter, select, batch, etc.) |
| [`orcapod.databases`](databases.md) | Persistent storage backends for computation results |
| [`orcapod.nodes`](nodes.md) | Pipeline graph elements, including the DB-backed job nodes |
| [`orcapod.streams`](streams.md) | Immutable (Tag, Data) sequences backed by PyArrow tables |
| [`orcapod.types`](types.md) | Core type definitions: `Schema`, `ColumnConfig`, `ContentHash` |
