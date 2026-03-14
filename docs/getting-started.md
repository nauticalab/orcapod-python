# Getting Started

This guide walks you through the core workflow in Orcapod: creating a data source,
inspecting its stream, applying a transformation with a function pod, and examining
the results.

## Creating a source

A **source** is the entry point for data in an Orcapod pipeline. The simplest way to
get started is with `DictSource`, which accepts a list of dictionaries:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[
        {"experiment": "exp_001", "temperature": 20.5, "pressure": 1.01},
        {"experiment": "exp_002", "temperature": 22.3, "pressure": 0.98},
        {"experiment": "exp_003", "temperature": 19.8, "pressure": 1.05},
    ],
    tag_columns=["experiment"],
    source_id="lab_results",
)
```

There are two important concepts here:

- **Tag columns** (`tag_columns`) are the keys that identify each row -- like primary keys
  in a database or independent variables in an experiment. Here, `experiment` uniquely
  identifies each measurement.
- **Packet columns** are everything else -- the actual data payload. In this example,
  `temperature` and `pressure` are the packet columns.

The `source_id` is a human-readable name used for provenance tracking.

!!! note
    Orcapod provides several source types beyond `DictSource`: `ListSource`, `CSVSource`,
    `ArrowTableSource`, `DataFrameSource`, and `DeltaTableSource`. They all produce the
    same kind of immutable stream. See [Sources](concepts/sources.md) for details.

## Inspecting the stream

In Orcapod, a source *is* a stream. You can inspect it immediately without any extra
conversion step.

### Schema

Use `output_schema()` to see the tag and packet column types:

```python
tag_schema, packet_schema = source.output_schema()
print(tag_schema)
# Schema({'experiment': <class 'str'>})
print(packet_schema)
# Schema({'temperature': <class 'float'>, 'pressure': <class 'float'>})
```

### Column names

Use `keys()` to get just the column names:

```python
tag_keys, packet_keys = source.keys()
print(tag_keys)
# ('experiment',)
print(packet_keys)
# ('temperature', 'pressure')
```

### Iterating over rows

Use `iter_packets()` to walk through each (Tag, Packet) pair:

```python
for tag, packet in source.iter_packets():
    print(f"Tag: {tag.as_dict()}, Packet: {packet.as_dict()}")
# Tag: {'experiment': 'exp_001'}, Packet: {'temperature': 20.5, 'pressure': 1.01}
# Tag: {'experiment': 'exp_002'}, Packet: {'temperature': 22.3, 'pressure': 0.98}
# Tag: {'experiment': 'exp_003'}, Packet: {'temperature': 19.8, 'pressure': 1.05}
```

### Getting the full table

Use `as_table()` to get a PyArrow Table, which you can convert to pandas:

```python
table = source.as_table()
print(table.to_pandas())
#   experiment  temperature  pressure
# 0    exp_001         20.5      1.01
# 1    exp_002         22.3      0.98
# 2    exp_003         19.8      1.05
```

## Applying a function pod

A **function pod** transforms packet data row by row. Use the `@function_pod` decorator
to turn a plain Python function into a reusable, trackable transformation:

```python
from orcapod import function_pod

@function_pod(output_keys=["temp_fahrenheit", "is_high_pressure"])
def analyze_conditions(temperature: float, pressure: float) -> tuple[float, bool]:
    temp_f = temperature * 9.0 / 5.0 + 32.0
    is_high = pressure > 1.0
    return temp_f, is_high
```

A few things to note about function pods:

- **Parameter names match packet column names.** The function's parameter names
  (`temperature`, `pressure`) must match the packet column names from the input stream.
  Orcapod uses type annotations to validate compatibility at process time.
- **`output_keys` names the output columns.** Since the function returns a tuple of two
  values, `output_keys` must be a list of two names. For a function returning a single
  value, pass a single string (e.g., `output_keys="result"`).
- **The decorator creates a `.pod` attribute.** You call `.pod.process()` to apply
  the function to a stream.

Now apply it:

```python
result = analyze_conditions.pod(source)
```

!!! tip
    All standard pods support `__call__` as a shorthand for `.process()`, so
    `pod(stream)` is equivalent to `pod.process(stream)`.

The `result` is a new stream. Tags are preserved from the input; the packet columns
are replaced with the function's outputs.

## Inspecting the result

The result stream supports the same inspection methods as the source:

```python
tag_schema, packet_schema = result.output_schema()
print(tag_schema)
# Schema({'experiment': <class 'str'>})
print(packet_schema)
# Schema({'temp_fahrenheit': <class 'float'>, 'is_high_pressure': <class 'bool'>})
```

The tag schema is unchanged -- function pods never modify tags. The packet schema
now reflects the function's output types.

Iterate over the results:

```python
for tag, packet in result.iter_packets():
    print(f"Tag: {tag.as_dict()}, Packet: {packet.as_dict()}")
# Tag: {'experiment': 'exp_001'}, Packet: {'temp_fahrenheit': 68.9, 'is_high_pressure': True}
# Tag: {'experiment': 'exp_002'}, Packet: {'temp_fahrenheit': 72.14, 'is_high_pressure': False}
# Tag: {'experiment': 'exp_003'}, Packet: {'temp_fahrenheit': 67.64, 'is_high_pressure': True}
```

Or view it as a table:

```python
print(result.as_table().to_pandas())
#   experiment  temp_fahrenheit  is_high_pressure
# 0    exp_001            68.90              True
# 1    exp_002            72.14             False
# 2    exp_003            67.64              True
```

## Putting it all together

Here is the complete example in one block:

```python
from orcapod import function_pod
from orcapod.sources import DictSource

# Create a source from raw data
source = DictSource(
    data=[
        {"experiment": "exp_001", "temperature": 20.5, "pressure": 1.01},
        {"experiment": "exp_002", "temperature": 22.3, "pressure": 0.98},
        {"experiment": "exp_003", "temperature": 19.8, "pressure": 1.05},
    ],
    tag_columns=["experiment"],
    source_id="lab_results",
)

# Define a transformation
@function_pod(output_keys=["temp_fahrenheit", "is_high_pressure"])
def analyze_conditions(temperature: float, pressure: float) -> tuple[float, bool]:
    temp_f = temperature * 9.0 / 5.0 + 32.0
    is_high = pressure > 1.0
    return temp_f, is_high

# Apply and view results
result = analyze_conditions.pod(source)
print(result.as_table().to_pandas())
#   experiment  temp_fahrenheit  is_high_pressure
# 0    exp_001            68.90              True
# 1    exp_002            72.14             False
# 2    exp_003            67.64              True
```

## Next steps

Now that you have the basics, explore these topics:

- [Sources](concepts/sources.md) -- learn about the different source types and how
  provenance tracking works.
- [Streams](concepts/streams.md) -- understand the immutable (Tag, Packet) stream model.
- [Function Pods](concepts/function-pods.md) -- advanced function pod usage, including
  caching with databases.
- [Operators](concepts/operators.md) -- structural transforms like Join, Batch, and Filter
  that work on tags and stream structure without inspecting packet content.
- [Identity & Hashing](concepts/identity.md) -- how Orcapod tracks content identity and
  pipeline structure for reproducibility.
