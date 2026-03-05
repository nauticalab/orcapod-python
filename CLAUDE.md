# Claude Code instructions for orcapod-python

## Naming convention

Always write "orcapod" with a **lowercase p** — never "OrcaPod" or "Orcapod". This applies
everywhere: documentation, docstrings, comments, commit messages, and code comments.

## Running commands

Always run Python commands via `uv run`, e.g.:

```
uv run pytest tests/
uv run python -c "..."
```

Never use `python`, `pytest`, or `python3` directly.

## Branch hygiene

Periodically check the target branch (typically `dev`) for updates and incorporate them into
your working branch. Before pushing, fetch and rebase onto the latest target branch to avoid
divergence and merge conflicts. If cherry-picking is needed due to unrelated commit history,
prefer cherry-picking your commits onto a fresh branch from the target rather than resolving
massive rebase conflicts.

## Updating agent instructions

When adding or changing any instruction, update BOTH:
- `CLAUDE.md` (for Claude Code)
- `.zed/rules` (for Zed AI)

## Design issues log

`DESIGN_ISSUES.md` at the project root is the canonical log of known design problems, bugs, and
code quality issues.

When fixing a bug or addressing a design problem:
1. Check `DESIGN_ISSUES.md` first — if a matching issue exists, update its status to
   `in progress` while working and `resolved` once done, adding a brief **Fix:** note.
2. If no matching issue exists, ask the user whether it should be added before proceeding.
   If yes, add it (status `open` or `in progress` as appropriate).

When discovering a new issue that won't be fixed immediately, ask the user whether it should be
logged in `DESIGN_ISSUES.md` before adding it.

## Backward compatibility

This is a greenfield project pre-v0.1.0. Do **not** add backward-compatibility shims,
re-exports, aliases, or deprecation wrappers when making design or implementation changes.
Just change the code and update all references directly.

## Docstrings

Use [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
Python docstrings everywhere.

## Git commits

Always use [Conventional Commits](https://www.conventionalcommits.org/) style:

```
<type>(<optional scope>): <short description>
```

Common types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`, `ci`.

Examples:
- `feat(schema): add optional_fields to Schema`
- `fix(packet_function): reject variadic parameters at construction`
- `test(function_pod): add schema validation tests`
- `refactor(schema_utils): use Schema.optional_fields directly`

---

## Project layout

```
src/orcapod/
├── types.py                    # Schema, ColumnConfig, ContentHash, PipelineConfig,
│                               # NodeConfig, ExecutorType, CacheMode
├── system_constants.py         # Column prefixes and separators
├── errors.py                   # InputValidationError, DuplicateTagError, FieldNotResolvableError
├── config.py                   # Config dataclass
├── channels.py                 # Async channel primitives (Channel, BroadcastChannel,
│                               # ReadableChannel, WritableChannel, ChannelClosed)
├── contexts/                   # DataContext (semantic_hasher, arrow_hasher, type_converter)
├── protocols/
│   ├── hashing_protocols.py    # PipelineElementProtocol, ContentIdentifiableProtocol
│   ├── database_protocols.py   # ArrowDatabaseProtocol
│   ├── pipeline_protocols.py   # Pipeline-level protocols
│   ├── semantic_types_protocols.py  # TypeConverterProtocol
│   └── core_protocols/         # StreamProtocol, PodProtocol, SourceProtocol,
│                               # PacketFunctionProtocol, DatagramProtocol, TagProtocol,
│                               # PacketProtocol, TrackerProtocol, AsyncExecutableProtocol,
│                               # PacketFunctionExecutorProtocol, OperatorPodProtocol,
│                               # LabelableProtocol, TemporalProtocol, TraceableProtocol
├── core/
│   ├── base.py                 # LabelableMixin, DataContextMixin, TraceableBase
│   ├── function_pod.py         # FunctionPod, FunctionPodStream, @function_pod decorator
│   ├── packet_function.py      # PacketFunctionBase, PythonPacketFunction, CachedPacketFunction
│   ├── tracker.py              # BasicTrackerManager, GraphTracker
│   ├── datagrams/
│   │   ├── datagram.py         # Datagram (unified dict/Arrow backing, lazy conversion)
│   │   └── tag_packet.py       # Tag (+ system tags), Packet (+ source info)
│   ├── sources/
│   │   ├── base.py             # RootSource (abstract, no upstream)
│   │   ├── arrow_table_source.py  # Core source — all other sources delegate to it
│   │   ├── persistent_source.py   # PersistentSource (DB-backed caching wrapper)
│   │   ├── derived_source.py   # DerivedSource (backed by node DB records)
│   │   ├── csv_source.py, dict_source.py, list_source.py,
│   │   │   data_frame_source.py, delta_table_source.py  # Delegating wrappers
│   │   └── source_registry.py  # SourceRegistry for provenance resolution
│   ├── streams/
│   │   ├── base.py             # StreamBase (abstract)
│   │   └── arrow_table_stream.py  # ArrowTableStream (concrete, immutable)
│   ├── nodes/
│   │   ├── function_node.py    # FunctionNode, PersistentFunctionNode
│   │   ├── operator_node.py    # OperatorNode, PersistentOperatorNode
│   │   └── source_node.py      # SourceNode (leaf stream in graph)
│   ├── operators/
│   │   ├── static_output_pod.py  # StaticOutputOperatorPod, DynamicPodStream
│   │   ├── base.py             # UnaryOperator, BinaryOperator, NonZeroInputOperator
│   │   ├── join.py             # Join (N-ary inner join, commutative)
│   │   ├── merge_join.py       # MergeJoin (binary, colliding cols → sorted list[T])
│   │   ├── semijoin.py         # SemiJoin (binary, non-commutative)
│   │   ├── batch.py            # Batch (group rows, types become list[T])
│   │   ├── column_selection.py # Select/Drop Tag/Packet columns
│   │   ├── mappers.py          # MapTags, MapPackets (rename columns)
│   │   └── filters.py          # PolarsFilter
│   └── executors/
│       ├── base.py             # PacketFunctionExecutorBase (ABC)
│       ├── local.py            # LocalExecutor (default in-process)
│       └── ray.py              # RayExecutor (dispatch to Ray cluster)
├── pipeline/
│   ├── graph.py                # Pipeline (extends GraphTracker, compiles to persistent nodes)
│   ├── nodes.py                # PersistentSourceNode (DB-backed leaf wrapper)
│   └── orchestrator.py         # AsyncPipelineOrchestrator (channel-based concurrent execution)
├── hashing/
│   ├── file_hashers.py         # BasicFileHasher, CachedFileHasher
│   ├── arrow_hashers.py        # Arrow-specific hashing
│   ├── arrow_serialization.py  # Arrow serialization utilities
│   ├── arrow_utils.py          # Arrow manipulation for hashing
│   ├── defaults.py             # Factory functions for default hashers
│   ├── hash_utils.py           # hash_file(), get_function_components()
│   ├── string_cachers.py       # String caching strategies
│   ├── versioned_hashers.py    # Versioned hasher support
│   ├── visitors.py             # Visitor pattern for hashing
│   └── semantic_hashing/       # BaseSemanticHasher, type handlers, TypeHandlerRegistry
├── semantic_types/             # Type conversion (Python ↔ Arrow), UniversalTypeConverter,
│                               # SemanticTypeRegistry, type inference
├── databases/                  # ArrowDatabaseProtocol implementations
│   ├── delta_lake_databases.py # DeltaTableDatabase
│   ├── in_memory_databases.py  # InMemoryArrowDatabase
│   ├── noop_database.py        # NoOpArrowDatabase
│   └── file_utils.py           # File utilities for database operations
├── execution_engines/
│   └── ray_execution_engine.py # RayEngine (execution on Ray clusters)
└── utils/
    ├── arrow_data_utils.py     # System tag manipulation, source info, column helpers
    ├── arrow_utils.py          # Arrow table utilities
    ├── schema_utils.py         # Schema extraction, union, intersection, compatibility
    ├── lazy_module.py          # LazyModule for deferred heavy imports
    ├── function_info.py        # Function introspection utilities
    ├── git_utils.py            # Git metadata extraction
    ├── name.py                 # Name utilities
    ├── object_spec.py          # Object specification/serialization
    └── polars_data_utils.py    # Polars-specific utilities

tests/
├── test_core/
│   ├── datagrams/              # Lazy conversion, dict/Arrow round-trip
│   ├── sources/                # Source construction, protocol conformance, DerivedSource,
│   │                           # PersistentSource
│   ├── streams/                # ArrowTableStream behavior, convenience methods
│   ├── function_pod/           # FunctionPod, FunctionNode, pipeline hash integration,
│   │                           # @function_pod decorator
│   ├── operators/              # All operators, OperatorNode, MergeJoin
│   └── packet_function/        # PacketFunction, CachedPacketFunction, executor
├── test_channels/              # Async channels, async_execute for operators/nodes/pods,
│                               # native async operators, pipeline integration
├── test_pipeline/              # Pipeline compilation, AsyncPipelineOrchestrator
├── test_hashing/               # Semantic hasher, hash stability, file hashers, string cachers
├── test_databases/             # Delta Lake, in-memory, no-op databases
└── test_semantic_types/        # Type converter, semantic registry, struct converters
```

---

## Architecture overview

See `orcapod-design.md` at the project root for the full design specification.

### Core data flow

**Pull-based (synchronous):**
```
RootSource → ArrowTableStream → [Operator / FunctionPod] → ArrowTableStream → ...
```

**Push-based (async pipeline):**
```
Pipeline.compile() → AsyncPipelineOrchestrator.run() → channels → persistent nodes → DB
```

Every stream is an immutable sequence of (Tag, Packet) pairs backed by a PyArrow Table.
Tag columns are join keys and metadata; packet columns are the data payload.

### Core abstractions

**Datagram** (`core/datagrams/datagram.py`) — immutable data container with lazy dict ↔ Arrow
conversion. Two specializations:
- **Tag** — metadata columns + hidden system tag columns for provenance tracking
- **Packet** — data columns + per-column source info provenance tokens

**Stream** (`core/streams/arrow_table_stream.py`) — immutable (Tag, Packet) sequence.
Key methods: `output_schema()`, `keys()`, `iter_packets()`, `as_table()`.

**Source** (`core/sources/`) — produces a stream from external data. `ArrowTableSource` is the
core implementation; CSV/Delta/DataFrame/Dict/List sources all delegate to it internally. Each
source adds source-info columns and a system tag column. `DerivedSource` wraps a node's DB
records as a new source. `PersistentSource` wraps any `RootSource` with DB-backed caching
(deduped by per-row content hash).

**Function Pod** (`core/function_pod.py`) — wraps a `PacketFunction` that transforms individual
packets. Never inspects tags. Supports async functions via `PythonPacketFunction`. The
`@function_pod` decorator creates `FunctionPod` instances directly from Python functions.

**Node** (`core/nodes/`) — graph-aware wrappers that participate in the computation DAG:
- `SourceNode` — leaf stream in the graph (wraps a `StreamProtocol`)
- `FunctionNode` / `PersistentFunctionNode` — packet function invocations (persistent variant
  is DB-backed with two-phase execution: yield cached, then compute missing)
- `OperatorNode` / `PersistentOperatorNode` — operator invocations (persistent variant
  is DB-backed with deduplication)

**Operator** (`core/operators/`) — structural pod transforming streams without synthesizing new
packet values. All subclass `StaticOutputOperatorPod`. Each operator also implements
`AsyncExecutableProtocol` for push-based channel execution:
- `UnaryOperator` — 1 input (Batch, Select/Drop columns, Map, Filter)
- `BinaryOperator` — 2 inputs (MergeJoin, SemiJoin)
- `NonZeroInputOperator` — 1+ inputs (Join)

**Executor** (`core/executors/`) — pluggable execution backends for packet functions:
- `LocalExecutor` — default in-process execution
- `RayExecutor` — dispatches to a Ray cluster

**Channel** (`channels.py`) — async primitives for push-based pipeline execution:
- `Channel[T]` — bounded async channel with backpressure and close signaling
- `BroadcastChannel[T]` — fan-out channel for multiple consumers
- `ReadableChannel[T]` / `WritableChannel[T]` — consumer/producer protocols

**Pipeline** (`pipeline/`) — persistent, async-capable pipeline infrastructure:
- `Pipeline` — extends `GraphTracker`; records operator/function pod invocations during a
  `with` block, then `compile()` replaces every node with its persistent variant
  (leaf streams → `PersistentSourceNode`, function nodes → `PersistentFunctionNode`,
  operator nodes → `PersistentOperatorNode`)
- `AsyncPipelineOrchestrator` — executes a compiled pipeline using channels; walks the
  persistent node graph in topological order, creates bounded channels between nodes,
  launches all nodes concurrently via `asyncio.TaskGroup`

### Async execution model

All pipeline nodes implement `AsyncExecutableProtocol`:
```python
async def async_execute(
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
) -> None
```

The orchestrator wires channels between nodes and launches tasks without knowing node types.
`PipelineConfig` controls buffer sizes (`channel_buffer_size`) and concurrency limits
(`default_max_concurrency`). Per-node overrides are set via `NodeConfig`.

### Strict operator / function pod boundary

| | Operator | Function Pod |
|---|---|---|
| Inspects packet content | Never | Yes |
| Inspects / uses tags | Yes | No |
| Can rename columns | Yes | No |
| Synthesizes new values | No | Yes |
| Stream arity | Configurable | Single in, single out |

### Two identity chains

Every pipeline element has two parallel hashes:

1. **`content_hash()`** — data-inclusive. Changes when data changes. Used for deduplication
   and memoization.
2. **`pipeline_hash()`** — schema + topology only. Ignores data content. Used for DB path
   scoping so that different sources with identical schemas share database tables.

Base case: `RootSource.pipeline_identity_structure()` returns `(tag_schema, packet_schema)`.
Each downstream node's pipeline hash commits to its own identity plus the pipeline hashes of
its upstreams, forming a Merkle chain.

The pipeline hash uses a **resolver pattern** — `PipelineElementProtocol` objects route through
`pipeline_hash()`, other `ContentIdentifiable` objects route through `content_hash()`.

### Column naming conventions

| Prefix | Meaning | Example | Controlled by |
|--------|---------|---------|---------------|
| `__` | System metadata | `__packet_id`, `__pod_version` | `ColumnConfig(meta=True)` |
| `_source_` | Source info provenance | `_source_age` | `ColumnConfig(source=True)` |
| `_tag::` | System tag | `_tag::source:abc123` | `ColumnConfig(system_tags=True)` |
| `_context_key` | Data context | `_context_key` | `ColumnConfig(context=True)` |

Prefixes are computed from `SystemConstant` in `system_constants.py`. The `constants` singleton
(with no global prefix) is used throughout.

### System tag evolution rules

1. **Name-preserving** — single-stream ops (filter, select, map). Column name and value pass
   through unchanged.
2. **Name-extending** — multi-input ops (join, merge join). Each input's system tag column
   name gets `::{pipeline_hash}:{canonical_position}` appended. Commutative operators
   canonically order inputs by `pipeline_hash` and sort system tag values per row.
3. **Type-evolving** — aggregation ops (batch). Column type changes from `str` to `list[str]`.

### Schema types and ColumnConfig

`Schema` (`types.py`) — immutable `Mapping[str, DataType]` with `optional_fields` support.
`output_schema()` always returns `(tag_schema, packet_schema)` as a tuple of Schemas.

`ColumnConfig` (`types.py`) — frozen dataclass controlling which column groups are included.
Fields: `meta`, `context`, `source`, `system_tags`, `content_hash`, `sort_by_tags`.
Normalize via `ColumnConfig.handle_config(columns, all_info)` at the top of `output_schema()`
and `as_table()` methods. `all_info=True` sets everything to True.

### Key patterns

- **`LazyModule("pyarrow")`** — deferred import for heavy deps (pyarrow, polars). Used in
  `if TYPE_CHECKING:` / `else:` blocks at module level.
- **Argument symmetry** — each operator declares `argument_symmetry(streams)` returning
  `frozenset` (commutative) or `tuple` (ordered). Determines how upstream hashes combine.
- **`StaticOutputOperatorPod.process()` → `DynamicPodStream`** — wraps `static_process()`
  output with timestamp-based staleness detection and automatic recomputation.
- **Source delegation** — CSVSource, DictSource, etc. all create an internal
  `ArrowTableSource` and delegate every method to it.
- **`Pipeline` context manager** — records non-persistent nodes during `with` block, then
  `compile()` promotes them to persistent variants with DB backing.
- **`AsyncExecutableProtocol`** — unified interface for all pipeline nodes. The orchestrator
  wires channels and launches tasks without knowing node types.
- **`GraphTracker`** — tracks operator/function pod invocations in a NetworkX DAG; `Pipeline`
  extends it to add compilation and persistence.

### Important implementation details

- `ArrowTableSource.__init__` raises `ValueError` if any `tag_columns` are not in the table.
- `ArrowTableStream` requires at least one packet column; raises `ValueError` otherwise.
- `PersistentFunctionNode.iter_packets()` Phase 1 returns ALL records in the shared
  `pipeline_path` DB table (not filtered to current inputs). Phase 2 skips inputs whose hash
  is already in the DB.
- Empty data → `ArrowTableSource` raises `ValueError("Table is empty")`.
- `DerivedSource` before `run()` → raises `ValueError` (no computed records).
- Join requires non-overlapping packet columns; raises `InputValidationError` on collision.
- MergeJoin requires colliding packet columns to have identical types; merges into sorted
  `list[T]` with source columns reordered to match.
- Operators predict their output schema (including system tag column names) without
  performing the actual computation.
- `CachedFileHasher` uses mtime+size cache busting to detect file changes without re-hashing.
- `PersistentSource` cache is always on; returns the union of all cached data across runs.
- `AsyncPipelineOrchestrator` uses `BroadcastChannel` for fan-out (one node feeding multiple
  downstream consumers).
