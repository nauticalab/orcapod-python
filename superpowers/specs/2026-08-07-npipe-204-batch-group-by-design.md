# NPIPE-204 — Tag-based grouping for `Batch` (many→one reduction)

**Linear:** [NPIPE-204](https://linear.app/metamorphic/issue/NPIPE-204/orcapod-add-tag-based-grouping-to-batch-manyone-reduction-fix-batch)
**Branch:** `arnoldb/npipe-204-orcapod-add-tag-based-grouping-to-batch-manyone-reduction`
**Base rev:** `966d759a`

## Overview

Every existing orcapod pipeline fans *out*. Nothing reduces. The consumer that motivates this
change — `common_clock_op` in orcapod-sync-and-qc — produces one `AlignmentResult` per recording
session, computed from *all* of that session's spikeglx-sync result parquets at once. Expressing
that requires a many→one operator keyed on tag values, which orcapod does not have.

`Batch` is the only aggregating operator, and it has two defects:

1. **It groups by row count, not by tag.** `batch_size` only; no way to say "one packet per
   `(subject, date)`".
2. **It is broken inside `job.run()`.** It list-wraps *every* column, including the `_source_*`
   provenance columns, and `Data._ensure_source_info_table` hard-codes `pa.large_string()` for
   those fields.

Both were verified against `966d759a`. Reproduction of (2):

```
pyarrow.lib.ArrowTypeError: Expected bytes, got a 'list' object
  core/nodes/operator_node.py:946        execute
  core/operators/static_output_pod.py:214  _materialize_to_stream
  core/datagrams/tag_data.py:433           as_table
  core/datagrams/tag_data.py:342           _ensure_source_info_table
```

### The bug is not Batch-specific

`MergeJoin` merges colliding data columns into `list[T]` and carries their `_source_*` columns
along as parallel lists (`merge_join.py:262`). It therefore fails with the identical
`ArrowTypeError` inside `job.run()`, verified independently of `Batch`. The root cause is in the
`Data` datagram, not in either operator: `Data` cannot represent a non-scalar source-info value.

This is the already-logged `DESIGN_ISSUES.md` **U1 — Source-info column type hard-coded to
`large_string`** (severity: critical), whose recorded location is the sibling call site
`arrow_utils.add_source_info_to_table()`. This change fixes the `tag_data.py` half and leaves the
`arrow_utils.py` half open, keeping the upstream PR scoped.

## Goals & Success Criteria

* `Batch(group_by=["subject", "date"])` emits one packet per distinct tag tuple, with the group
  keys as scalar tag columns and the members as list-valued data columns.
* `Batch` and `MergeJoin` both survive `job.run()` end to end.
* Memoization over a grouped stream holds across two identical runs and invalidates when a single
  member's data changes.
* Group member order is deterministic across runs, so an unchanged member set never produces a
  different list hash.
* No pipeline-DB schema version bump.

## Scope & Boundaries

In scope:
* `core/datagrams/tag_data.py` — type-aware source info on `Data`.
* `core/operators/batch.py` — `group_by`, provenance handling, output schema.
* Operator-level and job-level tests, including a `MergeJoin` job-level regression test.

Out of scope:
* `arrow_utils.add_source_info_to_table()` (the other half of U1).
* Incremental/streaming emission for `group_by` — see *Async execution* below.
* The downstream rev bump in orcapod-sync-and-qc and orcapod-spikesorting.

---

## Part 1 — Type-aware source info in `Data`

`Data` stores per-data-column provenance tokens in `self._source_info`, surfaced as `_source_*`
columns. Two places assume those values are always scalar strings:

* `_ensure_source_info_table()` (`tag_data.py:330`) builds the Arrow schema as
  `pa.field(k, pa.large_string())` for every key.
* `Data.schema()` (`tag_data.py:384`) sets `schema[f"{SOURCE_PREFIX}{key}"] = str` for every key.

Both become derived from the stored value:

| Stored value | Arrow type | Python type |
|---|---|---|
| `str` | `large_string` | `str` |
| `None` | `large_string` | `str` |
| `list[str]` | `large_list(large_string)` | `list[str]` |
| nested list | `large_list(<elem>)`, recursive | `list[...]` |
| `[]` | `large_list(large_string)` | `list[str]` |

`None` keeps mapping to `large_string` so unknown-provenance columns behave exactly as today.

`self._source_info`'s annotation widens from `dict[str, str | None]` to a recursive
`SourceInfoValue = str | None | list["SourceInfoValue"]`, with `source_info()`,
`with_source_info()`, `rename()`, and `with_columns()` following. The dict and table
construction paths in `__init__` already pass values through untouched — the table path recovers
lists correctly via `to_pylist()`.

**No schema version bump.** A node's `_source_*` column type is fixed by that node's own output
schema. A `FunctionNode` downstream of a `Batch` writes list-typed source columns from its first
record; every pre-existing node keeps `large_string`. Nothing re-reads an old table under a new
type.

This part alone fixes `MergeJoin` inside `job.run()`.

---

## Part 2 — `Batch` rewrite

### Constructor

```python
def __init__(self, batch_size=0, drop_partial_batch=False, group_by=None, **kwargs):
```

* `batch_size < 0` → `ValueError` (unchanged).
* `batch_size` and `group_by` both truthy → `ValueError`, mutually exclusive.
* `self.group_by = tuple(group_by) if group_by else None`.

`validate_unary_input` raises `InputValidationError` if any `group_by` name is not a tag column of
the input stream.

### Partitioning

* **`group_by` mode** — key on the tuple of group-key tag values, accumulated into a plain dict so
  first-seen group order is preserved. `drop_partial_batch` is inapplicable and ignored.
* **`batch_size` mode** — positional chunks of `batch_size` rows, `drop_partial_batch` honored.
  Unchanged from today.

### Member ordering

Within a `group_by` group, members are sorted by the tuple of their **non-group-key tag values**
before emission, falling back to the system `record_id` when a stream has no non-key tags. Tags are
unique within a stream, so this is a total order, and it does not depend on which data column
happens to hold a path.

This matters because orcapod hashes the emitted list to build the cache key. Upstream emission
order is not stable across runs (Ray executor scheduling, DB fetch order), so an unsorted list
would make an identical member set hash differently and trigger a spurious recompute.

`batch_size` batches are inherently positional — batch membership itself depends on arrival order,
so sorting within a batch would not make it deterministic. That path is left unsorted.

### Column treatment

| Column class | `group_by` mode | `batch_size` mode |
|---|---|---|
| group-key tags | **scalar**, remain tag columns | — |
| other user tags | list-valued **data** columns | list-valued, remain **tag** columns |
| data columns | list-valued | list-valued |
| `_source_*` | list-valued | list-valued |
| `_tag_source_id` / `_tag_record_id` | **scalar digest**, name extended | **scalar digest**, name extended |
| `_context_key` | scalar, shared by all members | scalar |

Non-key tags become list-valued *data* rather than being dropped, so a consumer can tell which
member each list element came from. Those promoted columns have no provenance token, so
`Data.source_info()` reports `None` for them — its existing behavior for unknown keys.

`batch_size` mode keeps its list-valued tag columns rather than promoting them to data. This is the
status quo for that path and nothing in-repo depends on the alternative; only the provenance
columns change there.

Nullability: list-wrapped columns are `nullable=False` (a group always has at least one member, so
the list itself is never null). Scalar group-key columns inherit the input column's nullable flag.

### System tag folding

`_build_record_id_preimage` (`core/nodes/function_node.py:82`) computes a record's identity from the
system-tag columns plus a hash of the input data. System tags must therefore be scalar. A many→one
operator has to define how N members' system tags collapse into one record's provenance.

**Rule:** compute one deterministic digest over the group's ordered sequence of
`(source_id, record_id)` pairs, then project it back into each column's declared type —
`large_string` for `_tag_source_id`, `binary(16)` for `_tag_record_id`. Column names are extended
with `{BLOCK_SEPARATOR}{pipeline_hash}` via the existing `arrow_utils.append_to_system_tags`,
mirroring the name-extending rule already used by joins.

Why this rule:

* **Invalidation is correct.** Any member whose record identity changes changes the digest, so the
  downstream record_id changes and the cache misses. This is strictly stronger than relying on the
  input-data hash alone, which would miss an upstream recompute that produced identical data.
* **Nothing else has to change.** `function_node.py` and the pdb schema are untouched.
* **Member identities remain recoverable** from the list-valued `_source_*` columns, which Part 1
  now preserves per member.

The extended name `_tag_source_id::<schema_hash>::<pipeline_hash>` has no trailing `:position`, so
`_parse_system_tag_column` returns `None` for it and `sort_system_tag_values` skips it. That is
correct — `Batch` is unary, so there is no cross-input commutativity to normalize.

Two alternatives were considered and rejected. Having `Batch` mint a fresh source-like identity
severs the Merkle link to the member records. Keeping system tags list-valued and teaching
`_build_record_id_preimage` to hash lists is the most information-preserving option but changes the
record-identity machinery and the pdb column types, requiring a v1→v2 migration on top of an
already cross-repo change.

### Output schema

`unary_output_schema` must mirror the table exactly: scalar group keys in the tag schema, promoted
non-key tags and list-wrapped data columns in the data schema, list-typed `_source_*` entries when
`columns={"source": True}`, and renamed scalar system-tag entries when
`columns={"system_tags": True}`. The operator predicts this without performing the computation,
consistent with every other operator.

### Serialization and identity

`to_config()` and `identity_structure()` both gain `group_by`. `from_config` needs no change — it
already forwards `config["config"]` as kwargs.

### Async execution

`async_execute` falls back to barrier mode whenever `group_by` is set: no group can be emitted
before the input channel closes, because any row not yet seen could belong to a group already
started.

Under `AsyncPipelineOrchestrator` this stalls one node, not the pipeline. Upstream nodes still run
concurrently and stream into the Batch; downstream resumes full concurrency once the barrier
releases, fanning the N groups out to N concurrent invocations. The unavoidable cost is that the
last straggler member in *any* group delays the first downstream invocation for *every* group.

This is not a regression: `batch_size=0` already takes the barrier path (`batch.py:118`), and
`SyncPipelineOrchestrator` is node-at-a-time regardless. The `batch_size=N>0` streaming path is
untouched.

Emitting groups early would require a guarantee that input arrives clustered by group key. orcapod
streams carry no ordering guarantee, so that would need an unverifiable `assume_grouped=True`
opt-in. Deliberately not built.

---

## Part 3 — Tests

### Operator-level (`tests/test_core/operators/`)

* `group_by` produces one row per distinct tag tuple; group keys scalar, members list-valued.
* Non-key tags are promoted to list-valued data columns, not dropped.
* `batch_size` and `group_by` together raise `ValueError`.
* `group_by` naming a non-tag column raises `InputValidationError`.
* Members are sorted by non-key tags — same input in two different row orders yields byte-identical
  output tables.
* `unary_output_schema` matches `as_table().schema` for every `ColumnConfig` combination.
* System tag columns are scalar and renamed; `_source_*` columns are list-valued.
* Existing `TestBatchBehavior` cases continue to pass unchanged.

### Job-level (new)

The gap that let defect (2) ship: `Batch`'s existing tests are all `op.process(stream)` followed by
`as_table()`. None run through `job.run()`.

Backed by a `DeltaTableDatabase` store:

* `Batch(group_by=...)` completes end to end inside `job.run()` feeding a `@function_pod` that takes
  a `list[str]` parameter.
* `Batch(batch_size=N)` likewise — the provenance fix is independent of grouping.
* **Memoization holds:** two identical `job.run()` calls; the pod body executes only on the first.
* **Memoization invalidates:** change one file in one group's list; only that group's pod
  invocation re-runs.
* **`MergeJoin` regression:** a `MergeJoin` with colliding data columns completes inside
  `job.run()`.

---

## Dependencies & Risks

* `orcapod-python` is upstream (`walkerlab/orcapod-python`); orcapod-sync-and-qc pins
  `nauticalab/orcapod-python @ 966d759a`. This needs an upstream PR followed by a coordinated rev
  bump in **both** orcapod-sync-and-qc and orcapod-spikesorting, which are deliberately kept on the
  same rev because they target the same Ray cluster.
* The schedule risk is the upstream PR and two-repo rev bump, not the code — Parts 1 and 2 are on
  the order of 100 lines together.

## Resources & References

* `DESIGN_ISSUES.md` §U1 — source-info type hard-coding (this change fixes the `tag_data.py` half).
* `CLAUDE.md` §"System tag evolution rules" — needs a wording update: rule 3 currently describes
  `Batch` as purely type-evolving (`str` → `list[str]`), which is now only true of user tag and data
  columns. System tags fold to a scalar and extend their name.
