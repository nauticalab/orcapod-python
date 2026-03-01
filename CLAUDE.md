# Claude Code instructions for orcapod-python

## Running commands

Always run Python commands via `uv run`, e.g.:

```
uv run pytest tests/
uv run python -c "..."
```

Never use `python`, `pytest`, or `python3` directly.

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
├── types.py                    # Schema, ColumnConfig, ContentHash
├── system_constants.py         # Column prefixes and separators
├── errors.py                   # InputValidationError, DuplicateTagError, FieldNotResolvableError
├── config.py                   # Config dataclass
├── contexts/                   # DataContext (semantic_hasher, arrow_hasher, type_converter)
├── protocols/
│   ├── hashing_protocols.py    # PipelineElementProtocol, ContentIdentifiableProtocol
│   └── core_protocols/         # StreamProtocol, PodProtocol, SourceProtocol,
│                               # PacketFunctionProtocol, DatagramProtocol, TagProtocol,
│                               # PacketProtocol, TrackerProtocol
├── core/
│   ├── base.py                 # ContentIdentifiableBase, PipelineElementBase, TraceableBase
│   ├── static_output_pod.py    # StaticOutputPod (operator base), DynamicPodStream
│   ├── function_pod.py         # FunctionPod, FunctionPodStream, FunctionNode
│   ├── packet_function.py      # PacketFunctionBase, PythonPacketFunction, CachedPacketFunction
│   ├── operator_node.py        # OperatorNode (DB-backed operator execution)
│   ├── tracker.py              # Invocation tracking
│   ├── datagrams/
│   │   ├── datagram.py         # Datagram (unified dict/Arrow backing, lazy conversion)
│   │   └── tag_packet.py       # Tag (+ system tags), Packet (+ source info)
│   ├── sources/
│   │   ├── base.py             # RootSource (abstract, no upstream)
│   │   ├── arrow_table_source.py  # Core source — all other sources delegate to it
│   │   ├── derived_source.py   # DerivedSource (backed by FunctionNode/OperatorNode DB)
│   │   ├── csv_source.py, dict_source.py, list_source.py,
│   │   │   data_frame_source.py, delta_table_source.py  # Delegating wrappers
│   │   └── source_registry.py  # SourceRegistry for provenance resolution
│   ├── streams/
│   │   ├── base.py             # StreamBase (abstract)
│   │   └── arrow_table_stream.py  # ArrowTableStream (concrete, immutable)
│   └── operators/
│       ├── base.py             # UnaryOperator, BinaryOperator, NonZeroInputOperator
│       ├── join.py             # Join (N-ary inner join, commutative)
│       ├── merge_join.py       # MergeJoin (binary, colliding cols → sorted list[T])
│       ├── semijoin.py         # SemiJoin (binary, non-commutative)
│       ├── batch.py            # Batch (group rows, types become list[T])
│       ├── column_selection.py # Select/Drop Tag/Packet columns
│       ├── mappers.py          # MapTags, MapPackets (rename columns)
│       └── filters.py          # PolarsFilter
├── hashing/
│   └── semantic_hashing/       # BaseSemanticHasher, type handlers
├── semantic_types/             # Type conversion (Python ↔ Arrow)
├── databases/                  # ArrowDatabaseProtocol implementations (Delta Lake, in-memory)
└── utils/
    ├── arrow_data_utils.py     # System tag manipulation, source info, column helpers
    ├── arrow_utils.py          # Arrow table utilities
    ├── schema_utils.py         # Schema extraction, union, intersection, compatibility
    └── lazy_module.py          # LazyModule for deferred heavy imports

tests/
├── test_core/
│   ├── datagrams/              # Lazy conversion, dict/Arrow round-trip
│   ├── sources/                # Source construction, protocol conformance, DerivedSource
│   ├── streams/                # ArrowTableStream behavior
│   ├── function_pod/           # FunctionPod, FunctionNode, pipeline hash integration
│   ├── operators/              # All operators, OperatorNode, MergeJoin
│   └── packet_function/        # PacketFunction, CachedPacketFunction
├── test_hashing/               # Semantic hasher, hash stability
├── test_databases/             # Delta Lake, in-memory, no-op databases
└── test_semantic_types/        # Type converter tests
```

---

## Architecture overview

See `orcapod-design.md` at the project root for the full design specification.

### Core data flow

```
RootSource → ArrowTableStream → [Operator / FunctionPod] → ArrowTableStream → ...
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
source adds source-info columns and a system tag column. `DerivedSource` wraps a
FunctionNode/OperatorNode's DB records as a new source.

**Function Pod** (`core/function_pod.py`) — wraps a `PacketFunction` that transforms individual
packets. Never inspects tags. Two execution models:
- `FunctionPod` → `FunctionPodStream`: lazy, in-memory
- `FunctionNode`: DB-backed, two-phase (yield cached results first, then compute missing)

**Operator** (`core/operators/`) — structural pod transforming streams without synthesizing new
packet values. All subclass `StaticOutputPod`:
- `UnaryOperator` — 1 input (Batch, Select/Drop columns, Map, Filter)
- `BinaryOperator` — 2 inputs (MergeJoin, SemiJoin)
- `NonZeroInputOperator` — 1+ inputs (Join)

**OperatorNode** (`core/operator_node.py`) — DB-backed operator execution, analogous to
FunctionNode.

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
- **`StaticOutputPod.process()` → `DynamicPodStream`** — wraps `static_process()` output
  with timestamp-based staleness detection and automatic recomputation.
- **Source delegation** — CSVSource, DictSource, etc. all create an internal
  `ArrowTableSource` and delegate every method to it.

### Important implementation details

- `ArrowTableSource.__init__` raises `ValueError` if any `tag_columns` are not in the table.
- `ArrowTableStream` requires at least one packet column; raises `ValueError` otherwise.
- `FunctionNode.iter_packets()` Phase 1 returns ALL records in the shared `pipeline_path`
  DB table (not filtered to current inputs). Phase 2 skips inputs whose hash is already
  in the DB.
- Empty data → `ArrowTableSource` raises `ValueError("Table is empty")`.
- `DerivedSource` before `run()` → raises `ValueError` (no computed records).
- Join requires non-overlapping packet columns; raises `InputValidationError` on collision.
- MergeJoin requires colliding packet columns to have identical types; merges into sorted
  `list[T]` with source columns reordered to match.
- Operators predict their output schema (including system tag column names) without
  performing the actual computation.
