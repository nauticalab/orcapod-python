# Orcapod Python
Orcapod's Python library for developing reproducbile scientific pipelines.

## Continuous Integration

This project uses GitHub Actions for continuous integration:

- **Run Tests**: Runs the full unit test suite on Ubuntu with Python 3.11 and 3.12. PostgreSQL integration tests are excluded.
- **Run PostgreSQL Tests**: Runs only the `@pytest.mark.postgres` integration tests against a PostgreSQL 16 service container.

### Running Tests Locally

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
# Install all dependencies
uv sync --all-extras --dev

# Run the standard test suite (no PostgreSQL required)
uv run pytest -m "not postgres"

# Run with coverage
uv run pytest -m "not postgres" --cov=src --cov-report=term-missing
```

### Running PostgreSQL Integration Tests Locally

The PostgreSQL integration tests require a running PostgreSQL instance. The easiest way is Docker:

```bash
docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
```

Then in another terminal:

```bash
PGPASSWORD=postgres uv run pytest -m postgres -v
```

The tests connect via standard `PG*` environment variables — override any of these defaults as needed:

| Variable     | Default     |
|--------------|-------------|
| `PGHOST`     | `localhost`  |
| `PGPORT`     | `5432`       |
| `PGDATABASE` | `testdb`     |
| `PGUSER`     | `postgres`   |
| `PGPASSWORD` | `postgres`   |

## Lower-level details on pipeline workflow (tentative)

While the following is subject to change based on future development, it represents a nice high-level overview of some of the lower-level details of how Orcapod pipelines work and how the various objects interact with one another.

### Pipeline setup

  Instantiate:

  - OBSERVER with a log database
    - Two kinds: NoOpObserver, LoggingObserver
  - ORCHESTRATOR with this observer
    - Two kinds: SyncPipelineOrchestrator, AsyncPipelineOrchestrator
  - EXECUTOR
    - Two kinds: LocalExecutor, RayExecutor
    - Executors have a slim base protocol (PacketFunctionExecutorProtocol — identity, compatibility, lifecycle) and a Python-specific subtype (PythonFunctionExecutorProtocol — adds
   execute_callable / async_execute_callable). This leaves room for non-Python executor types in the future.
  - PIPELINE (name, DB)

### Construct PIPELINE using with pipeline

  "Compile" PIPELINE
  - auto_compile=False is default
  - Walk the DAG, wrap each element in a node that owns execution logic, fixates input stream, attaches caching
  - Nodes (function, operator) are associated with storage/DB
  - If a function node has no executor, Pipeline.compile() assigns LocalExecutor() by default — so capture/logging works out of the box without explicit executor setup

  Run PIPELINE by passing several objects to pipeline.run():
  - The EXECUTOR (optional) — node.executor = ray_executor.with_options(**opts) called on every function node, replacing the compile-time default
    - Per-node override also possible: pipeline.transform_a.executor = RayExecutor(...) after compile
  - The ORCHESTRATOR
    - Calls obs.on_run_start(run_id)
    - Walks nodes in topological order (or launches all concurrently for async)
    - Wires nodes together (edges in DAG) with channels (async) or buffers (sync)
    - For each node, calls node.execute(input_stream, observer=obs) or node.async_execute(input_channel, output_channel, observer=obs)
    - on_run_end(run_id) is in a try/finally — always fires, even on failure
  - Engine opts dict — forwarded to executor via with_options(**opts)

### Function node processing

  Capture happens at the bottom of the chain (inside the executor). A logger is injected DOWN the call chain — FunctionNode creates it from the observer and passes it through to
  the executor. The executor captures I/O, calls logger.record(**captured_fields), and re-raises on failure. No CapturedLogs appears in any return type or protocol — it's an
  internal convenience struct inside the executor.

  The observer is contextualized per-node: FunctionNode.execute() calls observer.contextualize(node_hash, node_label) to get a lightweight wrapper stamped with node identity. If no
   observer is provided, a NoOpObserver is used as default, eliminating all is not None checks.

  The logger protocol is generic: PacketExecutionLoggerProtocol.record(**kwargs) accepts arbitrary keyword arguments, so different executor types can log different fields without
  the protocol being tied to any specific data structure.

  If no executor is set and a logger is passed, a UserWarning is emitted: "A logger was passed but no executor is set — capture will not occur."

  Down the chain (FunctionNode → user function)

  FunctionNode.execute(input_stream, observer=obs)
      │
      │  obs = observer or NoOpObserver()
      │  ctx_observer = obs.contextualize(node_hash, node_label)
      │  ctx_observer.on_node_start(node_label, node_hash)
      │
      │  for each non-cached (tag, packet):
      │
      │  ctx_observer.on_packet_start(node_label, tag, packet)
      │
      ├─► pkt_logger = ctx_observer.create_packet_logger(tag, packet, pipeline_path=...)
      │       │
      │       └─► _ContextualizedLoggingObserver creates a PacketLogger bound to
      │           (run_id, node_label, node_hash, tag_data, log_path)
      │
      ├─► FunctionNode._process_packet_internal(tag, packet, logger=pkt_logger)
      │       │
      │       ├─► CachedFunctionPod.process_packet(tag, packet, logger=pkt_logger)
      │       │       │
      │       │       │  checks pod-level cache (ResultCache.lookup)
      │       │       │  cache hit? → return (tag, cached_packet)
      │       │       │  cache miss ↓
      │       │       │
      │       │       ├─► _FunctionPodBase.process_packet(tag, packet, logger=pkt_logger)
      │       │       │       │
      │       │       │       ├─► PythonPacketFunction.call(packet, logger=pkt_logger)
      │       │       │       │       │
      │       │       │       │       │  executor is set? (LocalExecutor from compile,
      │       │       │       │       │  or RayExecutor from pipeline.run)
      │       │       │       │       │
      │       │       │       │       ├─► executor.execute_callable(fn, kwargs, logger=pkt_logger)
      │       │       │       │       │       │
      │       │       │       │       │       │  ── LocalExecutor path ──
      │       │       │       │       │       │  ctx = LocalCaptureContext()
      │       │       │       │       │       │  with ctx:
      │       │       │       │       │       │    fn(**kwargs)               ← USER FUNCTION RUNS HERE
      │       │       │       │       │       │  on success: logger.record(**captured.as_dict())
      │       │       │       │       │       │  on exception: logger.record(**captured.as_dict())
      │       │       │       │       │       │                then RE-RAISE the original exception
      │       │       │       │       │       │
      │       │       │       │       │       │  ── RayExecutor path ──
      │       │       │       │       │       │  dispatches _make_capture_wrapper() to Ray
      │       │       │       │       │       │  Ray worker runs the wrapper which:
      │       │       │       │       │       │    - redirects fd 1/2 to temp files
      │       │       │       │       │       │    - installs a logging handler
      │       │       │       │       │       │    - calls fn(**kwargs)       ← USER FUNCTION RUNS HERE
      │       │       │       │       │       │    - on success: returns (raw, stdout, stderr, python_logs)
      │       │       │       │       │       │    - on failure: RAISES _CapturedTaskError(cause, stdout, ...)
      │       │       │       │       │       │      → Ray propagates via RayTaskError (retries etc. work)
      │       │       │       │       │       │  driver side:
      │       │       │       │       │       │    success → logger.record(stdout=..., stderr=..., success=True, ...)
      │       │       │       │       │       │    failure → logger.record(stdout=..., success=False, ...)
      │       │       │       │       │       │              then RE-RAISE the original user exception
      │       │       │       │       │       │
      │       │       │       │       │       └─► returns raw_result (or raises)
      │       │       │       │       │
      │       │       │       │       │  no executor? warns if logger was passed, calls direct_call()
      │       │       │       │       │
      │       │       │       │       │  builds output Packet from raw_result
      │       │       │       │       │
      │       │       │       │       └─► returns Packet | None (or raises)
      │       │       │       │
      │       │       │       └─► returns (tag, Packet | None)
      │       │       │
      │       │       │  stores result in pod-level cache (on success)
      │       │       │
      │       │       └─► returns (tag, Packet | None)
      │       │
      │       │  writes pipeline provenance record (on success)
      │       │  caches result internally
      │       │
      │       └─► returns (tag, Packet | None)
      │
      │  ← back in FunctionNode.execute() with (tag_out, result)
      │
      │  (logger.record already called inside the executor — nothing to do here)
      │
      ├─► try/except around _process_packet_internal:
      │   on success:
      │       ctx_observer.on_packet_end(node_label, tag, packet, result, cached=False)
      │       emit (tag_out, result) downstream
      │   on exception:
      │       ctx_observer.on_packet_crash(node_label, tag, packet, exc)
      │       if error_policy == "fail_fast": raise
      │       otherwise: skip this packet, continue
      │
      ctx_observer.on_node_end(node_label, node_hash)

  PacketLogger.record(**kwargs)

  PacketLogger.record(**kwargs)
      │
      └─► builds a pyarrow table row:
          Context columns (always present, prefixed with "__"):
            `__log_id`, `__run_id`, `__node_label`, `__node_hash`, `__timestamp`
          Execution output columns (from **kwargs, prefixed with "`__`"):
            __stdout, __stderr, __python_logs, __traceback, __success
            (or any other fields the executor passes — protocol is generic)
          Tag columns (unprefixed, from tag_data baked in at creation):
            e.g. "idx" → "0", "key" → "a"

Writes the row to the database at the mirrored log path.

### Key design points

  - CapturedLogs never appears in any return type or protocol — it's an internal convenience struct inside executors. The protocol uses record(**kwargs) so different executor types
   can log different fields.
  - Exceptions propagate — direct_call() and executors re-raise user exceptions after recording logs. Error handling happens at the FunctionNode.execute() boundary via try/except.
  - Ray exceptions propagate through Ray — the worker raises _CapturedTaskError (carrying captured I/O), Ray wraps it in RayTaskError (retries etc. work normally), the driver
  extracts the captured data, records to logger, and re-raises the original user exception type.
  - Observer is contextualized per-node — contextualize(node_hash, node_label) returns a lightweight wrapper. No GraphNode reference crosses the protocol boundary — only strings.
  NoOpObserver is used as default when no observer is provided, eliminating null checks.
  - pipeline_path is a storage detail — passed to create_packet_logger() for routing log table location, but NOT stored as a column in log rows (only node_hash + node_label
  identify the node).
  - No auto-executor in the class — PacketFunctionBase.__init__ does not assign a default executor. Pipeline.compile() assigns LocalExecutor to function nodes that have none. Users
   can override per-node (pipeline.node.executor = ...) or globally via pipeline.run(execution_engine=...).
  - Log columns use __ prefix — fixed columns (__log_id, __stdout, __success, etc.) are prefixed to avoid collision with user-defined tag column names.