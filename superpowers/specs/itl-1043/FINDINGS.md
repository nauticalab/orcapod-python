# ITL-1043 investigation findings (origin/main 4127e6bb, November 12, 2026)

Scripts in this directory, each run with `uv run python superpowers/specs/itl-1043/<name>`:
`q1_repro.py`, `q2_where_rows_go.py`, `q3_schemas.py`, `q4_blast_radius.py` (output in
`q4_output.txt`), `q4b_from_pipeline.py`, `q5_nullability_hash.py` (output in
`q5_output.txt`).

## Q1: reproduction and supported usage

Both behaviours reproduce on the in-memory and Delta stores (`q1_repro.py`):

| tags | (a) no re-attach | (b) re-attached, REPLAY |
|---|---|---|
| non-null | `RuntimeError: ... no stored results available for this read` | 0 rows (2 stored) |
| nullable | same | `RuntimeError: ... the cache table is absent` at `schema:4c4a...` (run wrote `schema:659c...`) |

Supported? Partly documented as not yet supported:

- `docs/concepts/function-pods.md:688-693`: "A loaded operator without a readable cache raises
  ... Saved descriptors do not yet preserve all runtime identity and cache configuration
  needed for operator readback without reconstruction (follow-up ITL-977)."
- ITL-977 (Backlog, after v0.3) records that the saved operator descriptor has no
  `pipeline_path`, no run-time `content_hash` and no `cache_mode`, and that the developer chose
  not to change the saved file format for v0.3. `tests/test_pipeline/test_hash_step_combined.py`
  injects the missing fields into the file to read an operator node back.
- `tests/test_pipeline/loaded_job/test_loaded_job_readback.py:758`
  (`test_loaded_operator_node_raises_instead_of_returning_empty`) pins behaviour (a).
- The intended way to read cached operator output today: rebuild the job from code and attach
  the operator node in `CacheMode.REPLAY`
  (`tests/test_databases/test_delta_arrow_type_roundtrip.py:276-302`). That works in every case
  tested here (`q4_blast_radius.py`, "rebuilt").

## Cause of (a)

The operator's `cache_mode` is not saved: `Pipeline.save` writes no `cache_mode`
(`src/orcapod/pipeline/graph.py:205-212`), `OperatorJobNode.from_descriptor` defaults to
`"off"` (`src/orcapod/core/nodes/operator_node.py:783`), and `_distribute_databases` keeps OFF
(`src/orcapod/pipeline/job.py:500`). Documented (ITL-977).

## Q2: the 0-row case

The table is found at the right path and its rows are filtered out by content hash
(`q2_where_rows_go.py`):

- Rows are written with the run's node content hash in `__node_content_hash`
  (`operator_node.py:963`); `0fd8...` here.
- The loaded node is rebuilt with its operator (full mode, `operator_node.py:789`) over
  `SourceJobNode` stubs whose in-memory sources were not saved (unbound), so its content hash is
  the schema-only one (`source_node.py:85-87`): `1cb6...`.
- `_filter_by_content_hash` (`operator_node.py:922`) keeps rows equal to the loaded hash: none.
- Function nodes avoid this because `PipelineJob.save` writes their run content hash to
  `run.node_job_content_hashes` (`job.py:1339-1351`) and `load` passes it to the stub
  (`job.py:1564`). Operator nodes get nothing (`job.py:1585`). Known in ITL-977.

## Q3: the hash question (nullable tags)

`q3_schemas.py`. Running node and blueprint: source `a` tag `{id: int | None}`, pipeline hash
`9d1f...`; `joined` pipeline hash `659c...`. Saved descriptor of `a`:
`output_schema {'tag': {'id': 'int64'}, ...}` and `source_config.tag_schema {'id': 'int64'}`.
Loaded: source `a` tag `{id: int}`, pipeline hash `42d7...`; `joined` pipeline hash `4c4a...`,
equal to a run with non-null tags. `optional_fields` is empty in every case (nullability is
carried by the `T | None` type, not by `optional_fields`).

Where it diverges: on SAVE. `serialize_schema` writes
`type_converter.python_type_to_arrow_type(T | None)`, an Arrow `DataType`, which has no
field nullability (`src/orcapod/pipeline/serialization.py:651-656`): `int | None` and `int`
both become `"int64"`. On load `Pipeline.load` rebuilds the `SourceNode` with `int`
(`graph.py:327-350`), `SourceNodeBase.pipeline_identity_structure` hashes that schema
(`source_node.py:95`), and the operator node, rebuilt with its operator, recomputes its path
from it (`operator_node.py:306`). Nested nullability is kept in the type string; only the
top-level flag is lost.

Which hash is intended: the RUNNING node's (`659c...` here; `ea6e...` in the report).

- `docs/reference/hashing.md:1204-1206`: nullability at every depth is part of the hash.
- Golden: `tests/test_hashing/golden/data/datagram.json:18` vs `:33` (ArrowTableSource, data
  column `int64` non-null vs nullable: pipeline hash `979b...` vs `fcfb...`);
  `schema.json:54` vs `:56` (`str | None` vs `str`).
- CLAUDE.md "Arrow types": nullability is never changed.

No golden entry pins a loaded node or a save/load round trip (no such case in
`tests/test_hashing/golden/cases.py`). The loaded value is an artifact of information lost in
the saved file.

## Q4: blast radius

`q4_blast_radius.py` (tag and data nullability varied; function node `inc`, operator `joined`):

- `PipelineJob.load`, loaded function node: pipeline hash, content hash and path EQUAL the run's
  (the stub takes them from the descriptor, `function_node.py:1366-1394`); rows read back. Its
  `output_schema()` reports `int` for `int | None`.
- Loaded source nodes: pipeline hash wrong whenever a tag or data column is nullable; content
  hash always differs (in-memory sources unbound: expected); `output_schema()` loses `| None`.
- Loaded operator node: content hash always differs (Q2); pipeline hash and path wrong whenever
  any input column (tag or data) is nullable. Data column order of a commutative `Join` can
  differ too, because inputs are ordered by the (wrong) pipeline hash.
- Re-run from code with the same definitions: every node equal, function node
  `computed=0 reused=2`, operator REPLAY reads its rows. No miss.
- A loaded `PipelineJob` cannot run (by design). `Pipeline.load` + `PipelineJob.from_pipeline`
  fails for any pipeline with a function node (`to_invocations() cannot serialise FunctionNode
  ... _function_pod is None`).
- `q4b_from_pipeline.py`: an operator-only pipeline through `Pipeline.load` +
  `from_pipeline(sources=<live sources>)`: content hash is right (bound sources), but with a
  nullable tag the path is wrong (`605d...` instead of `68a6...`): REPLAY reads 0 rows, and a
  LOG run would write a second table under the wrong path (a silent cache miss). Non-null: works.
- A bare `pa.table({...})` or `pa.Table.from_pandas(...)` makes every field nullable, so the
  nullable case is the common one for `ArrowTableSource` and `DataFrameSource`.

## Q5: is nullability part of the schema hash?

Yes, as the declared type, not the presence of nulls (`q5_nullability_hash.py`): Arrow
non-nullable `5a41...`; nullable without nulls and nullable with a null both `3a86...`;
`DictSource` declared `int | None` `3a86...`; `DictSource` inferring from values gives `5a41...`
without a `None` and `3a86...` with one (inference only when no type is declared).

## Related issues

Not the same as ITL-1029, ITL-1039, ITL-1040, ITL-940 or ITL-1042. Causes (a) and the 0-row
case are ITL-977's deferred defect. The lost nullability in the saved schema is not recorded
anywhere found (Linear ITL-977, `design_issues/`).
