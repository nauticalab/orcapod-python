# ITL-939 design spike: a value that is both a tag and data

**Status:** the developer has decided the form (November 11, 2026). Design C, the operator
`CopyTagsToData`, is **rejected**: "if we have CopyTagToData that cannot be treated as an
operator as that is actively introducing new data into data." The chosen direction is the
**DerivedSource form**: "CopyTagsToData should be taken as what I call 'DerivedSource' -- a
data source that in itself derives from other pods." Section 8 is the design addendum for that
form; section 8.10 records the developer's answers to its decisions. Sections 1 to 7 are the original spike, kept for its
analysis; where they recommend the operator, section 8 replaces them.
**Issue:** ITL-939. **Related:** ITL-929 (record ids from primary keys), ITL-938 (versioning).
**Date:** November 11, 2026.
**Experiments:** `superpowers/specs/itl-939-experiments/` (exp01 today's rules, exp02 today's
workarounds, exp03 copy-operator prototype, exp04 foreign keys, exp05 hashes and divergence,
exp06 today's `DerivedSource`, exp07 derived-source prototype, exp08 `GroupBy` and non-key tags; each `expNN_*.py` has its captured
output next to it as `expNN_*.out`; rerun with `uv run python <script>`).

## 1. Summary

* **Today** a name cannot be in both groups, but nothing checks for it when the graph is
  built. `Join`, `MergeJoin` and a function pod whose output key equals a tag name all
  predict a stream with the name in both groups. That stream then fails when it is
  materialised, with a raw `KeyError` or `ValueError`, or (for the pod) yields diverging
  values through `iter_data()`.
* **The need is real but narrow.** In the 27 EDI tables, the key that a pod wants per row as
  data is essentially one column: `session_start_datetime` (a key in 20 tables). It is needed
  for session-level time arithmetic: session duration, experiment onset, age at session,
  days since implant. Index and `timestamp` keys mostly reach pods after a `GroupBy`, which
  already turns non-key tags into list-valued data. Hash keys are never inputs.
* **Workarounds.** Of today's options only one is correct: copy the column under another name
  before building the source (one line). Choosing `tag_columns` so the column stays data, or
  reading the table twice and joining, gives silently wrong joins: 9 rows instead of 5 on the
  sample. For a `SpiralDBTableSource` the correct recipe means leaving the DB source for an
  in-memory `ArrowTableSource`, which loses save and reload.
* **Recommendation (superseded, see the status above and section 8):** the smallest safe feature is design C. It is an operator,
  `CopyTagsToData`, that copies named tags into the data group under new names. The tags stay
  where they are. There is no new column model, no change to any existing hash, path or
  stored schema, and it fits the operator boundary: operators may read tags, and `GroupBy`
  already moves tags into data (**not a precedent**: the developer has ruled that behaviour of
  `GroupBy` incorrect, to be fixed in v0.3 under ITL-955; see 8.5). It is about a day of work with tests. If the developer would
  rather wait, the one-line recipe (section 6.2) is enough for the workshop on in-memory
  sources.
* **Not recommended:** a column that belongs to both groups (A), or two same-named columns
  (B). Both touch every operator and every stored table, and B turns today's crash on
  `Join(penetration, probe)` into a silently wrong cross product. Also not recommended: pods
  that read tags (D). The result cache keys on the input data hash only, so such a pod
  would get wrong cache hits.
* **Related defects found** (filed as ITL-940, see section 7): `Join`/`MergeJoin` miss a
  tag-vs-data name collision across inputs. A pod output key may equal a tag name. Both
  surface as late, unclear errors. The first one hits a natural EDI join (`penetration` with
  `probe` on `probe_id`).

## 2. What happens today (experiments 01, 02 and 04)

Fixtures: EDI-shaped `session` (keys `subject_id`, `session_start_datetime`) and `experiment`
(keys `subject_id`, `session_start_datetime`, `experiment_index`; data
`experiment_start_timestamp` as u64 microseconds, `task_id`). Subject `m1` has two sessions,
each with `experiment_index` 0 and 1, so `experiment_index` alone does not identify an
experiment, as in the real tables. All timestamps are UTC: a naive `datetime` returned by a pod
is rejected by the strict timezone policy.

### 2.1 Where the one-name-one-group rule is enforced (exp01)

| Probe | Result | Where |
|---|---|---|
| Arrow table with the field name twice, the name in `tag_columns` | `KeyError: Field "session_start_datetime" exists 2 times` | `SourceStreamBuilder.build` (`stream_builder.py:185`) |
| `MapData` renames a data column onto a tag name | `InputValidationError`, clear message | `MapData.validate_unary_input` |
| `MapTags` renames a tag onto a data name | `InputValidationError`, clear message | `MapTags.validate_unary_input` |
| Pod output key equals a tag name: `keys()` | **accepted**: `(('subject_id','session_start_datetime'), ('session_start_datetime',))` | none |
| ... `as_table()` | `ValueError: Duplicate column name` | `arrow_utils.hstack_tables` |
| ... `iter_data()` | **works, values differ**: tag is the start time, data is the end time | none |
| `Join(A[tag x], B[data x])`: `keys()`, `output_schema()` | **accepted**, `x` in both groups | none (`Join` checks only data-vs-data) |
| ... `as_table()` and `iter_data()` | `KeyError: Field "x" exists 2 times` | `arrow_utils.split_by_column_groups` |
| `MergeJoin(A, B)` same shapes | same as `Join` | same |
| `SemiJoin(A, B)` | works (B's data never reaches the output) | |
| A pod parameter named after a tag | `ValueError: required parameter ... has no matching column` | pod input validation |

So the rule exists only physically: the Arrow table cannot hold the name twice. The `Datagram`
pair (`Tag`, `Data`) can already hold it, as the pod probe shows. No validator stops it at
graph-build time.

### 2.2 Every way to get a tag value into a pod today (exp02)

The task: elapsed seconds from session start to experiment start, then `Join` with `session` on
the shared keys. The correct answer has 5 rows.

| | Recipe | Extra code | Join with `session` | Hash and provenance consequences |
|---|---|---|---|---|
| W1 | `tag_columns` without `session_start_datetime`, so it stays data | 1 changed argument; for a DB source the user lists the other primary keys by hand | **9 rows: silently wrong.** The pod output lost the session identity; the join pairs every `m1` experiment with both sessions | new schema hash, so new `pipeline_hash` and system-tag column names (`_tag_source_id::a7dc…` instead of `::2f50…`) |
| W2 | copy the column before the source: `t.append_column("session_start", t["session_start_datetime"])` | 1 line, plus pod parameter `session_start` | **5 rows: correct** | new schema hash (extra data column); the copy gets a normal source-info token; record ids unchanged |
| W2' | same, mid-pipeline: `as_table()`, copy, new `ArrowTableSource` | 3 lines | correct | **lineage cut**: `upstreams == ()`; new root, so the downstream `pipeline_hash` no longer chains to the real sources |
| W2 on a DB source | `SpiralDBTableSource(...).as_table()`, copy, `ArrowTableSource(..., source_id="experiment")` | 3 lines | correct | the pipeline cannot be reloaded: `ArrowTableSource.from_config` raises `NotImplementedError`. The `source_id` must be passed by hand, or it defaults to the table hash and record ids move with the data |
| W3 | read the table twice (once with the column as data), `MapData` to a new name, `Join` | 3 lines | **9 rows: silently wrong.** The two reads share only `subject_id`, `experiment_index` as tags | two sources, two schema hashes |
| W4 | an operator that copies a tag into data | none exists: `MapTags`/`MapData` rename within a group; Select/Drop only remove; `Pick`/`Index` read data; `InvocationContext` (`ctx_arg`) carries hashes, not tag values | | |

Post-run hooks do receive the tag (read-only, `hooks.py:82`), but they run after the
computation and are not a way to feed it.

### 2.3 The inverse direction: a data column wanted as a tag (exp04)

Nine EDI tables carry another table's key as data: `experiment.task_id`/`task_version`,
`penetration.probe_id`/`chamber_id`, `lfp_location.chamber_id`, `sample_unit.atlas_label_region_short`,
and others. Joining on such a foreign key needs it as a tag on both sides.

* `Join(penetration, probe)` with primary keys as tags: `probe_id` is a tag in `probe` and
  data in `penetration`. `keys()` is accepted. Materialising raises
  `KeyError: Field "probe_id" exists 2 times`. This is the defect in 2.1, met on a natural
  workshop join.
* Today's recipe works at the source: `tag_columns=[*primary_keys, "probe_id"]` gives the
  correct 2 rows.
* After a pod, no operator can turn a data column into a tag. Only re-rooting can (W2').

## 3. How common is the need?

From `edi_sample_v1` (27 tables, mean key width 4.4, widest 9):

| Key column | Tables keyed on it | Plausibly a per-row computation input? |
|---|---|---|
| `subject_id` | 22 | no (categorical label) |
| `session_start_datetime` | 20 | **yes**: time arithmetic against other timestamps and dates |
| `recording_index`, `probe_index`, `experiment_index`, `unit_id` | 11, 10, 8, 4 | rarely (ordinals; used for ordering after grouping) |
| `*_hash` (preprocessing, extraction, localization) | 18 total | never |
| `timestamp` | 6 (event and time-series tables) | yes, but normally consumed per group (spike trains, gaze traces), where `GroupBy` already hands it to the pod as list data |
| `trial_index`, `frame_index` | 1 each | as for `timestamp` |
| `atlas_id`, `atlas_label_region_short`, `chamber_id`, `probe_id`, `name`, `task_*` | 1 to 5 | as labels, rarely as inputs |

Workshop-style computations that need a key as data:

1. **Session duration**: `session_end_datetime - session_start_datetime` on `session`. Needs the
   key per row.
2. **Experiment onset within the session**: `experiment_start_timestamp - session_start_datetime`
   on `experiment`. Likewise `recording_start_timestamp` on `recording`. Needs the key per row.
3. **Age at session**: `session_start_datetime - date_of_birth` after `Join(session, subject)`.
   Needs the key per row.
4. **Days since chamber implant**: `session_start_datetime - chamber_implant_date`. This needs
   the key as data, and also `chamber_id` promoted from data to tag in `penetration` to join
   `chamber`. Both directions in one exercise.
5. **Spike-train statistics per unit** (rate, ISI, PSTH): `spike_event.timestamp` is a key, but
   `GroupBy(by=[... "unit_id"])` already passes the timestamps to the pod as a `list[int]` data
   column (exp03 section 6 shows the mechanism). **No feature needed.**

Plainly: the need comes down to one column, `session_start_datetime` (and any similar date
key). It shows up in perhaps 2 to 4 workshop exercises, all of them session-level time
arithmetic. It is not a general pattern across keys. The inverse need, a foreign key as a tag
for a join, is more common (9 tables), and the source-level `tag_columns` recipe already
covers it, except after a pod.

## 4. The four designs

### 4.1 Comparison

| | A: one column in both groups | B: two columns, one user-visible name | C: copy operator (no new column model) | D: pod reads declared tags |
|---|---|---|---|---|
| Arrow representation | one physical column, listed in both groups | data twin under a reserved prefix (e.g. `_data_::x`), tag under `x` | ordinary data column under a new name `x2` | unchanged |
| `keys()`, `output_schema()` | `x` in both tuples and schemas | `x` in both | `x` in tags, `x2` in data | unchanged |
| `as_table()`, `as_df()`, `as_polars_df()` | one column `x`; a reader cannot tell it is also data | must rename (leaks the prefix) or fail: names must be unique | ordinary | unchanged |
| `iter_data()` | `x` in `Tag` and in `Data` | `x` in both (the datagrams already allow this, exp01 #10) | `x` in `Tag`, `x2` in `Data` | unchanged |
| Pod binding | parameter `x` | parameter `x` | parameter `x2` | parameter `x` plus a declared `tag_inputs=["x"]` |
| Values equal? | by construction, until an operator must split them | no: independent once created | equal at copy time, independent after (an ordinary data column) | n/a |
| Operators | not closed: `MapData`/`MapTags` rename, `GroupBy` (key stays a scalar tag, data must become a list) and Drop of one side all force a split into B | every name lookup must say which group; `PolarsFilter` (one namespace) becomes ambiguous; `GroupBy` turns a non-key tag `x` into data, colliding with the data twin `x`; `Join` data overlap rule must handle twins | unchanged; the copy is an ordinary data column | unchanged |
| `Join(penetration[data probe_id], probe[tag probe_id])` | still a collision | **legal and silently a cross product** (no shared tag) | unchanged (should be a clear error, section 7) | unchanged |
| Source info, record ids | membership must be stored | twin needs its own `_source_` | copy's `_source_` is null, as `GroupBy` does for tags it moves into data; record ids unchanged | unchanged |
| `content_hash`, `pipeline_hash` | schema hash must encode membership | schema hash sees `x` in both schemas | only for streams that use the operator (a new node) | pod identity must include `tag_inputs`; **the result cache keys on the input data hash only** (`result_cache.py:186`), so the key must gain the tag values, or rows with equal data and different tags share a cached result (exp03 #4) |
| Existing hashes, golden values | unchanged if membership is absent by default | unchanged if the prefix appears only with twins | unchanged | unchanged |
| Stored tables, save and load, `DerivedSource` | stored schema must record membership | a new reserved physical prefix in every stored table: a stored-schema convention for ITL-938 | a data column like any other | result-cache layout changes for such pods |
| Sync and async | every operator, both paths | every operator, both paths | one operator, both paths | function pod and executors |
| Tag `T` vs data `T \| None` | one column, one nullability | twins may diverge after a pod | copy keeps the tag's exact Arrow type and nullability (exp03 #5) | pod sees the tag's type |
| Tag/data principle | blurred: one value is both label and input | kept internally, blurred for the user | **kept**: the label stays a label; a copy is input | **broken**: a deliberate exception |
| Cost | weeks; touches every operator, datagrams, stores | weeks; same, plus name resolution everywhere | about a day with tests and docs | 2 to 4 days, plus the cache-key change |

### 4.2 Design A: one column in both groups

`ArrowTableStream` computes data columns as "every column that is not a tag"
(`arrow_table_stream.py:107`), so A needs explicit membership. Display is pleasant: one
column. It falls apart under operators, because they treat the two groups differently:

* `MapData({"x": "y"})` renames the data side only, so the column must split into a tag `x`
  and a data `y`. `MapTags`, `DropTagColumns` and `DropDataColumns` behave the same way.
* `GroupBy(by=["x"])` keeps `x` as a scalar tag, but every data column becomes a list, so the
  data side must split.
* `Join` keys on `x`. Both inputs also carry `x` as data, which the non-overlap rule rejects,
  so it needs a special case ("equal because it is a key").

So A is B plus a storage optimisation that holds only until the first operator. Not recommended.

### 4.3 Design B: two columns, one visible name

This matches the developer's "more simply" option, and per-row datagrams already allow it
(exp01 #10). The costs are in the table world:

* Physical names must be unique, so one side needs a reserved prefix. That prefix then
  appears in `as_table()` and the dataframes, or those methods must refuse the stream.
* Every operator that addresses a column by name (`Join` keys and data overlap, `MergeJoin`,
  `PolarsFilter` expressions, `GroupBy`'s tag-to-data step, `Batch`, Select, Drop and Map)
  needs a rule for which group it means. `PolarsFilter` sees one namespace, so
  `pl.col("x")` becomes ambiguous.
* `GroupBy` collides with itself: grouping by other keys turns the non-key tag `x` into a
  data `x`, and a data twin `x` already exists.
* The worst problem is that B legalises the state that today crashes on
  `Join(penetration, probe)`. Under B, `probe_id` is a tag in one input and data in the other,
  they share no tag, and the join becomes a silent cross product. Today's accidental error
  would become a silent wrong answer.
* Every stored table (function-node results, operator nodes, `DerivedSource`) gains a reserved
  physical prefix. That is a stored-schema convention, and it belongs in ITL-938's versioning
  system.
* Nothing ties the twins together. A pod that outputs `x` replaces the data twin, and the
  same name then means two different values (exp01 #10 shows this happening today).

Not recommended. The benefit (the pod parameter can keep the key's name) does not justify a
cross-cutting change before the public v0.3.

### 4.4 Design C: a copy operator (rejected by the developer, November 11, 2026)

> **Rejected as an operator.** The developer: "if we have CopyTagToData that cannot be treated
> as an operator as that is actively introducing new data into data." Operators never put new
> content into the data group. The same copy is to be offered by a source instead: see
> section 8. The analysis below is kept because its findings on types, hashes and divergence
> carry over.

`CopyTagsToData({"session_start_datetime": "session_start"})` appends one data column per entry:

* It copies the tag's Arrow column unchanged: same type (large-normalized like every operator
  output), same nullability.
* The tags, the system tags (name-preserving rule) and the existing data columns are unchanged.
* (`GroupBy` is not a precedent: its promotion of non-key tags into data is being fixed,
  ITL-955.) The copy's `_source_` value is null, exactly as `GroupBy` already does for a non-key tag it
  turns into data (`group_by.py` docstring). An operator cannot know which source a tag value
  came from; after a `Join` it came from several.
* The operator reads tags, which operators may do, and never reads data. It synthesizes no
  value; it copies one.

The prototype in exp03 (about 50 lines, defined in the script, not in `src/`) shows:

* `keys()`, `output_schema()`, `as_table()` and `iter_data()` show the copy as ordinary data,
  and the schema prediction matches `static_process(...).output_schema()`.
* The pod reads `session_start`, and the `Join` with `session` on the shared keys gives the
  correct 5 rows.
* The per-row input data hashes equal the W2 recipe's exactly, so result caches agree with
  the hand-made recipe. Source info does not enter the data hash.
* Two rows with equal data and different tags get different data hashes once the tag is
  copied. That is what makes caching correct, where D would fail.
* A nullable tag gives a nullable copy (`datetime | None`): all EDI keys are nullable in the
  DB schema.
* After the copy, `GroupBy` turns the copy into a list like any data column. `MapData` on the
  copy leaves the tag alone. Equality is a snapshot at copy time; after that the copy is just
  data.
* The `pipeline_hash` of the input stream is unchanged. Only the new node has a new hash.

The cost to the user is one call, and the pod parameter carries the copy's name
(`session_start`), not the key's. That can be read as a feature: the code shows which value is
the label and which is the input.

**Variant C-source:** an option on sources (`data_copies=`) could give the copy a real
source-info token (`experiment::row_0::session_start_datetime`). It would touch every source
class and `to_config`, and the operator already works on any stream, mid-pipeline included.
Not recommended now.

**Inverse (C'), `CopyDataToTags`:** promote a data column into the tags while keeping it as
data. It would cover the foreign-key join after a pod. It is a bigger step for the principle:
the data values would then drive `Join` matching, which is what operators deliberately avoid.
The source-level recipe (`tag_columns=[*pks, "probe_id"]`) covers the EDI tables for the
workshop. Defer.

### 4.5 Design D: pods read declared tags

`@function_pod(..., tag_inputs=["session_start_datetime"])` would pass named tag values as
read-only arguments. No column model changes. But:

* It is the exception the principle forbids: "function pods never inspect tags".
* Caching breaks (below).
* After `Batch`, tags are lists, so the parameter type depends on the upstream.

**Why the cache problem makes D unsafe.** A pod's computed result is cached by its input:
`ResultCache.lookup` matches on `input_data.content_hash()` and nothing else
(`result_cache.py:186`). That is correct today only because a pod's output depends on its data
alone. Under D the output also depends on the tag values the pod reads. Those values are not
in the data hash, so two rows with equal data and different tags look like the same input, and
the second row is served the first row's result. Nothing signals the mistake. The `FunctionNode`
pipeline record would still be per row (its preimage includes the tags), but the value it
records would be the cached one. exp03 section 4 builds that case: two `experiment` rows with
the same `experiment_start_timestamp` and different `session_start_datetime` tags.

```text
=== 4. two rows, same data, different tags -> same input data hash
data hashes: ['00000138a40c4de2', '00000138a40c4de2']
-> a pod that read session_start_datetime from the TAG would get one cached result
   for both rows (ResultCache keys on the input data hash only).
with the copy, data hashes: ['00000162e219798e', '000001ed3ee152c9']
```

The elapsed time differs between the two rows (different session starts), yet a D pod with a
result database would return one elapsed time for both. Making D safe means adding the declared
tag values to the cache key and the input identity. That is design C carried out inside the pod,
plus a new cache-key layout to version under ITL-938. With C the value is in the data, so the
cache key already covers it, as the last line shows.

Not recommended.

## 5. Identity, storage and interactions for the recommended design

* **Hashes:** no existing `content_hash`, `pipeline_hash`, schema hash, system-tag column name
  or golden value changes. A pipeline that uses the operator gets a new node, with
  `identity_structure = ("CopyTagsToData", sorted(name_map.items()))`, and downstream nodes
  chain from it as usual.
* **Record ids:** unchanged. The record-id preimage is system tags plus the input data hash
  (`function_node.py:94`). The copy enters the data hash, which is the intended behaviour: the
  value is now an input.
* **Stored schemas:** none change. A downstream function node stores the copy like any data
  column. No new prefix, path component or version label, so nothing for ITL-938.
* **ITL-929:** independent. The copy happens after the source, and record identity from
  primary keys is a source matter. If ITL-929 lands, nothing here changes.
* **Sync and async:** `async_execute` copies per row from `tag.as_table()`, giving the same
  Arrow type, nullability and column order as the sync path. Add the operator to
  `test_async_sync_equivalence.py`.
* **Save and load:** `to_config()` / `from_config()` carry `name_map`. The DB source stays a
  DB source, unlike the W2 recipe on a DB source.
* **Validation (at graph-build time):** every key must be a user tag column of the input, not
  a system tag. Target names must be distinct from each other, unused in both groups, and must
  not start with `_` (`constants.DATAGRAM_PREFIX`, which covers `_tag_`, `_source_`, `__` and
  `_context_key`), since Orcapod reads such a column as a system column (sources drop it). Otherwise raise
  `InputValidationError` naming the column. Copying a list-valued tag (after `Batch`) gives a
  list-valued data column.

### 5.1 Hashes compared with the W2 recipe (exp05)

Both routes run the same elapsed-time pod on the same rows. Route W2 copies the column before
the source; route C applies `CopyTagsToData` to the unchanged source.

| Hash | W2 vs C | Why |
|---|---|---|
| Per-row input data `content_hash` (the pod's result-cache key) | **equal** | the data columns, their values and their Arrow types are the same; source info does not enter the data hash |
| Per-row output data `content_hash` | **equal** | same inputs, same pod |
| Stream `content_hash` (pod input and output streams) | differ (`139b…` vs `a343…`; `0ada…` vs `1f9c…`) | W2's chain starts at a source with an extra column; C's chain is the original source plus an operator node |
| `pipeline_hash` (pod input and output) | differ (`5a1e…` vs `df45…`; `9708…` vs `a473…`) | W2's source has another schema hash; C adds an operator node |
| Record-id preimage values: `source_id`, `record_id`, input data hash | **equal** (`experiment`, `3a75a398…`, `…4862a684…`) | both routes read the same source rows under the same `source_id` |
| Record-id preimage column names | differ: `_tag_source_id::8b20…` vs `::2f50…` | the system-tag column name carries the source's schema hash, and W2's schema has an extra column |
| `FunctionNode` base entry id (arrow hash of the preimage) | differ (`000001a5ca3e…` vs `000001996248…`) | the preimage is hashed with its column names |

So the two routes present the pod with the same input data hash, which is the result-cache
key. When the pod has a result database (`function_pod(result_database=...)`) and both routes
use the same one, a result computed on one route is reused on the other. Without a result
database there is no cache to share, and each run computes. Either way they are different
pipelines: different node tables (paths come from `pipeline_hash`) and different pipeline
record ids. That is expected, because two different graphs produce the rows. A user with a
shared result database who switches from the recipe to the operator recomputes nothing, but
gets new pipeline records once.
For the source itself, C changes nothing: the source keeps its schema hash, its system-tag
column names and its stored tables. W2 changes all three.

### 5.2 Can the copied value diverge from the tag?

Yes, and that is acceptable. At copy time the two values are equal. After that the copy is an
ordinary data column. A pod may output a new value under the same name, `MapData` may rename
it, `Drop` may remove it, `GroupBy` and `Batch` turn it into a list. exp05 shows a pod that
truncates the copy to the day:

```text
tag: 2026-01-05 09:00:00+00:00 | data session_start: 2026-01-05 00:00:00+00:00
after MapData the tag is unchanged: (('subject_id', 'session_start_datetime', 'experiment_index'), ('experiment_start_timestamp', 'task_id', 't0'))
```

What it means: the copy has no tie to the tag after creation. Nothing re-checks equality, and
nothing should. It is acceptable because the two values play different roles. The tag
identifies the row, and no pod can change it, since pods never write tags. The copy is an
input to computation, and computation may transform it like any other value. Row identity at
the source (source record ids) and `Join` matching follow the tags only, so a diverged copy can
never mis-identify or mis-join a row. A downstream pod's record id does depend on the copy: its
preimage includes the input data hash (`function_node.py:94`). If an earlier step changed the
copy, the pod's input data and therefore its record id change too. That is correct, since the
pod received a different input. The only risk is a reader assuming the two are still equal. The names make the
difference visible (`session_start_datetime` is the key, `session_start` the input), and the
docs should call the copy a snapshot. Design B carries the same divergence (exp01 #10) under a
single name, which is where it does become confusing.

## 6. Recommendation (original spike; the operator in 6.1 is rejected, see section 8)

### 6.1 For the workshop: `CopyTagsToData`

API (stream method plus operator, like the other operators):

```python
from orcapod.operators import CopyTagsToData

stream.copy_tags_to_data({"session_start_datetime": "session_start"})
CopyTagsToData({"session_start_datetime": "session_start"})(stream)
```

The same elapsed-time computation, before and after. Shared by both:

```python
from datetime import datetime

from orcapod import function_pod
from orcapod.sources import SpiralDBTableSource

PROJECT, DATASET = "...", "edi_sample_v1"
EXPERIMENT_KEYS = [
    "experiment_preprocessing_hash", "subject_id", "session_start_datetime", "experiment_index",
]

session = SpiralDBTableSource(PROJECT, "session", dataset=DATASET)

# EDI keys and columns are nullable in the DB schema, so the parameters are Optional.
@function_pod(output_keys="onset_s")
# Illustrative only: in the EDI tutorial data experiment_start_timestamp is a relative clock,
# not microseconds since the epoch, so this formula does not compute an onset on that table.
def onset_s(session_start: datetime | None, experiment_start_timestamp: int | None) -> float | None:
    if session_start is None or experiment_start_timestamp is None:
        return None
    return experiment_start_timestamp / 1e6 - session_start.timestamp()
```

**Before** (today: the W2 recipe, see 6.2 for its drawbacks):

```python
from orcapod.sources import ArrowTableSource

raw = SpiralDBTableSource(PROJECT, "experiment", dataset=DATASET).as_table()
raw = raw.append_column("session_start", raw["session_start_datetime"])
experiment = ArrowTableSource(raw, tag_columns=EXPERIMENT_KEYS, source_id="experiment")

onsets = onset_s.pod(experiment.select_data_columns(["session_start", "experiment_start_timestamp"]))
result = onsets.join(session)  # keyed on subject_id, session_start_datetime
```

**After** (`CopyTagsToData`):

```python
experiment = SpiralDBTableSource(PROJECT, "experiment", dataset=DATASET)

onsets = onset_s.pod(
    experiment.copy_tags_to_data({"session_start_datetime": "session_start"})
    .select_data_columns(["session_start", "experiment_start_timestamp"])
)
result = onsets.join(session)  # keyed on subject_id, session_start_datetime
```

The pod is the same in both. The "after" version keeps the DB source (it reloads from its
config, and `source_id`, nullability and record ids come from the source as usual) and works at
any point in a pipeline, not only at the source. The unit of `experiment_start_timestamp`
(microseconds since the epoch here) is an assumption to confirm against the EDI data.

Scope: one operator file, a stream method, `__all__`, sync and async paths, `to_config`,
tests (public interface; schema prediction against `static_process`; async/sync equivalence;
`Join` downstream; `GroupBy` after the copy; validation errors), a short docs section, and a
CHANGELOG entry under `[0.3.0] — unreleased`. No hash, golden value or stored schema changes.

### 6.2 If the developer prefers to wait: the recipe

For in-memory sources (`DictSource`, `ArrowTableSource`, CSV), copy the column before building
the source. It is one line, and it is what exp02 W2 shows is correct:

```python
t = t.append_column("session_start", t["session_start_datetime"])
```

For `SpiralDBTableSource`, the recipe in full (the shared block in 6.1 defines `PROJECT`,
`DATASET`, `EXPERIMENT_KEYS`, `session` and `onset_s`):

```python
from orcapod.sources import ArrowTableSource, SpiralDBTableSource

# 1. Load the DB table and take its Arrow table (user columns; types and nullability kept).
raw = SpiralDBTableSource(PROJECT, "experiment", dataset=DATASET).as_table()
# 2. Copy the key under a second name, which becomes a data column.
#    (Pass the field, not just a name, when the key is NOT NULL: a bare name makes the
#    copy nullable, and a pod with a non-optional parameter then rejects it. exp07 #8.)
raw = raw.append_column(
    raw.schema.field("session_start_datetime").with_name("session_start"),
    raw["session_start_datetime"],
)
# 3. Re-root as an in-memory source with the same keys as tags and an explicit source_id.
experiment = ArrowTableSource(raw, tag_columns=EXPERIMENT_KEYS, source_id="experiment")

onsets = onset_s.pod(experiment.select_data_columns(["session_start", "experiment_start_timestamp"]))
result = onsets.join(session)
```

It gives correct results (exp02 W2: 5 rows, as expected). It has two drawbacks:

1. **The pipeline cannot be reloaded.** The source is now an `ArrowTableSource`, whose
   `to_config()` records only metadata. `ArrowTableSource.from_config` raises
   `NotImplementedError` ("the in-memory Arrow table is not serializable"). A saved pipeline
   no longer knows the data came from SpiralDB `experiment`. This matters if the workshop saves
   and reloads pipelines; in a notebook run top to bottom it does not.
2. **`source_id` must be passed by hand.** Without it, the source id defaults to the table's
   content hash. Record ids (`source_id` plus row position) then change whenever any value in
   the table changes, and stored pipeline records no longer match on re-run. The DB source
   defaults `source_id` to the table name. The recipe must repeat that, under the same name,
   in every notebook.

**Never** recommend W1 or W3. Both give wrong joins without any error.

### 6.3 What waits

The inverse operator (C'), the source-level variant, and designs A, B and D.

### 6.4 Risks to the tag/data principle

Low. C keeps the label in the tags and passes an explicit, named copy to the pod. The pod
still never sees a tag. The risk is user confusion: two columns hold the same value at first
and can diverge later. The docs should say the copy is a snapshot.

### 6.5 Open decisions for the developer

1. **Ship `CopyTagsToData` before the workshop, or use the recipe and ship it after?**
   Recommendation: ship it now. It is additive, about a day, and changes no hash.
2. **Name and shape of the API.** `CopyTagsToData(name_map)` plus
   `stream.copy_tags_to_data(name_map)`, with an explicit map and no default naming.
   Recommendation: yes. A default suffix would invent names the user never wrote.
3. **Source info of the copy.** Null, as `GroupBy` does for tags it moves into data, or a
   token derived from the system tags. Recommendation: null.
4. **The inverse direction (data to tag, keeping the data).** Recommendation: defer. Use
   `tag_columns=[*primary_keys, fk]` at the source for the workshop. File an issue only if a
   post-pod foreign-key join turns up.
5. **Reject designs A, B and D** and record why (sections 4.2, 4.3, 4.5). Recommendation: reject.
6. **Fix the `Join`/`MergeJoin` tag-vs-data collision** (filed as ITL-940) so it raises `InputValidationError` at
   graph-build time, naming the column and which input has it as a tag and which as data,
   instead of a raw `KeyError` at materialisation. Recommendation: yes, as a separate issue
   before the workshop, because `Join(penetration, probe)` hits it.
7. **Reject a pod whose output key equals a tag name** (filed as ITL-940) when the pod is applied (today
   `keys()` accepts it, `as_table()` fails and `iter_data()` yields diverging values).
   Recommendation: yes, a separate issue, after the workshop.

## 7. Related defects found (filed as ITL-940, not fixed here)

* **J1.** `Join` and `MergeJoin` check data-vs-data overlap but not a name that is a tag in one
  input and data in another (`join.py:103-118`). `keys()`/`output_schema()` predict the name
  in both groups. `as_table()` and `iter_data()` then raise
  `KeyError: Field "x" exists 2 times in schema` (exp01 #6, #7, #10b; exp04 F1).
* **P1.** A function pod whose output key equals an input tag name is accepted. `as_table()`
  raises `Duplicate column name`, and `iter_data()` yields a tag and a data value with the same
  name and different values (exp01 #5, #10).

## 8. Addendum: the DerivedSource form (November 11, 2026)

The developer's decisions, binding:

1. The copy is **not an operator**. Operators never put new content into the data group, and
   function pods never see tags.
2. The copy belongs to a **source that derives from other pods**. A source defines its own tag
   and data columns, so it may expose a tag's value as a data column.
3. The need is narrow (`session_start_datetime`), and the feature is wanted for the workshop
   (November 17, 2026) or shortly after.

Evidence: exp06 (today's `DerivedSource`) and exp07 (an out-of-tree prototype of the proposal).
No library code has changed.

### 8.1 Summary of the proposal

`DerivedSource` gains one argument:

```python
DerivedSource(experiment, tags_as_data={"session_start_datetime": "session_start"})
```

The result is a new root source. Its tags are the origin's tags. Its data columns are the
origin's data columns plus one copy per entry. It gets source provenance of its own, exactly as
if the rows had been handed to `ArrowTableSource`. It stores nothing, changes no existing hash,
and can be saved and reloaded whenever its origin can.

In effect it is the hand-written recipe (section 6.2) made into a class: with the same
`source_id`, its table equals the recipe's column for column and value for value, and
downstream nodes get the same `pipeline_hash` (exp07 #6).

### 8.2 (a) Today's `DerivedSource` and what the developer means

The developer's meaning is a concept: a source whose rows come from other pods. Today's class
is one narrow case of it: a re-read of the stored records of one node of a `PipelineJob`.
Measured in exp06:

| Question | Today's `DerivedSource` |
|---|---|
| What can be its origin? | Only a database-backed job node (`FunctionJobNode`, `OperatorNode`), through `node.as_source()`. A source has no `as_source` (`AttributeError`). An in-memory pod stream has none either. `DerivedSource(origin=<a source>)` fails on first read: the source has no `get_all_records`. |
| Does it need a database and a completed run? | Yes. `as_source()` raises `RuntimeError` without a database. Before the origin has run it shows zero rows. |
| Can its tag/data assignment differ from the origin's? | No. `keys()` and `output_schema()` delegate to the origin, and the constructor takes no column arguments (`TypeError` on `tag_columns=`). |
| Provenance | It carries **no system-tag columns** (the origin has two; `get_all_records` drops them), and its `_source_*` values are null. Its `source_id` is set but appears in no row. So it neither starts provenance afresh nor carries the origin's. |
| Identity | `content_hash` from the origin's content hash. `pipeline_hash` from the schema only, so it differs from the origin's: `as_source()` cuts the chain on purpose (`tests/test_pipeline/test_pipeline.py`, "detaching via .as_source() breaks chain"). |
| In a second `PipelineJob` | Runs with both orchestrators (5 rows in, 5 out, 5 stored records). |
| Save and load | It has no `to_config`. A saved job loads with this source unbound. |

So today's class cannot serve the workshop case, which is a `SpiralDBTableSource` whose keys
are tags: it accepts no source as origin, cannot add a column, and cannot be reloaded.

Two more things seen, reported and not changed: a `DerivedSource` created before the origin
ran keeps showing zero rows after the run (it caches the empty table; a new `as_source()` call
shows the rows), and the missing system tags above.

### 8.3 (b) The smallest change

**API.** One new keyword on the existing class, and the same keyword on the existing
`node.as_source()`:

```python
from orcapod.sources import DerivedSource

DerivedSource(origin, tags_as_data={"<tag column>": "<new data column>"}, source_id=None,
              record_id_column=None)
node.as_source(tags_as_data={...})
```

The map is explicit; no default naming. The origin may be any source (including a
`CachedSource`) or a job node. Arbitrary in-memory streams are left out for now (decision 3).

Validation when the source is built: every key must be a user tag column of the origin; every
new name must be unused in both groups, distinct from the others, and must not start with `_`.
Otherwise `InputValidationError` naming the column.

**Before and after, on a Spiral source.** Shared by both (as in section 6.1):

```python
session = SpiralDBTableSource(PROJECT, "session", dataset=DATASET)

@function_pod(output_keys="onset_s")
# Illustrative only: in the EDI tutorial data experiment_start_timestamp is a relative clock,
# not microseconds since the epoch, so this formula does not compute an onset on that table.
def onset_s(session_start: datetime | None, experiment_start_timestamp: int | None) -> float | None:
    if session_start is None or experiment_start_timestamp is None:
        return None
    return experiment_start_timestamp / 1e6 - session_start.timestamp()
```

Before (the recipe; cannot be reloaded, `source_id` and nullability handled by hand):

```python
raw = SpiralDBTableSource(PROJECT, "experiment", dataset=DATASET).as_table()
raw = raw.append_column(
    raw.schema.field("session_start_datetime").with_name("session_start"),
    raw["session_start_datetime"],
)
experiment = ArrowTableSource(raw, tag_columns=EXPERIMENT_KEYS, source_id="experiment")
```

After:

```python
experiment = DerivedSource(
    SpiralDBTableSource(PROJECT, "experiment", dataset=DATASET),
    tags_as_data={"session_start_datetime": "session_start"},
)
```

Then, the same in both:

```python
onsets = onset_s.pod(experiment.select_data_columns(["session_start", "experiment_start_timestamp"]))
result = onsets.join(session)  # keyed on subject_id, session_start_datetime
```

On the EDI-shaped in-memory tables the "after" line is the same with an `ArrowTableSource` as
origin. exp07 #4 runs it: 5 rows, the correct answer.

**What the derived source is** (exp07 #1 to #3, #6):

| Aspect | Proposal |
|---|---|
| Tag columns | the origin's, unchanged |
| Data columns | the origin's, then the copies in map order |
| Copy's Arrow type | the tag's field exactly: same type, same nullability, logical types kept. Not null stays not null (`timestamp[us, tz=UTC] not null`); a nullable tag gives `datetime \| None`. The column is the tag's Arrow column, never rebuilt from Python values. |
| System tags | its own pair, named with its own schema hash (`_tag_source_id::8b20…`, where the origin has `::2f50…`). It starts provenance afresh, as every source does. |
| Record ids | from its `source_id` and a per-row token, by the standard source rule: `record_id_column` if given, otherwise the row position. A positional token names a position, not a row: if the origin returns the same rows in another order, a moved row gets another record id and is recorded again downstream. This is the limitation every source has (see `stream_builder._make_provenance_token`; ITL-929 addresses it). It cannot pair a result with the wrong row, because every downstream identity includes the row's data hash and a pod's result depends only on its input data; with `tags_as_data` the copied key is part of that data. Pass `record_id_column` when the origin's row order is not stable. |
| Source-info | one token per data column, pointing at the derived source: `derived:experiment::row_0::session_start` for the copy, `derived:experiment::row_0::task_id` for an original column. The origin's own tokens are not carried. |
| Default `source_id` | as first proposed: `derived:<origin source_id>` (decision 5). **Superseded by 8.10:** `tags_as_data:<algorithm label>:<hex>`, built from the origin's content hash, so it is defined for job-node origins too, which have no `source_id` of their own. |
| `upstreams`, `producer` | `()` and `None`: it is a root |
| Data context | the origin's |

**Identity** (as first proposed; the developer's answers in 8.10 replace the `content_hash` and
default `source_id` rules given here).

* `content_hash`: the origin's content hash, the sorted copies and the `source_id`. A
  `DerivedSource` made without the new argument keeps today's identity, `(origin content
  hash,)`, so no existing hash moves.
* `pipeline_hash`: schema only, as for every root source and as `DerivedSource` already does.
  **This is a decision (decision 6).** "Derives from other pods" could be read as "the
  upstream's pipeline hash should enter". If it did, the derived source would stop being a
  root: it would be a link in the origin's chain, which is what an operator node is. Schema
  only keeps it a source: two derived sources with the same schema share downstream tables,
  like any two sources with the same schema, and rows stay apart by their record ids. The
  origin is still recorded, in the saved config.

**Rows and storage.** It needs no store. Its schema and keys come from the origin's schema
without reading rows. Rows are built on first use: read the origin's table, append the copies
(Arrow shares the buffers; nothing is copied), run the standard source enrichment. Nothing is
written anywhere.

**Before the origin has run.** A source origin always has rows. A job-node origin behaves as
today: zero rows with the right schema until the node has run.

**Orchestrators and `PipelineJob`.** exp07 #7: a job over a file-backed DB table source runs
with the sync and the async orchestrator and gives the same rows. Only the derived source is a
node of the job; its origin sits inside it, as the inner source of a `CachedSource` does. The
origin and the derived source can both be used in one job (exp07 #7b), because their default
`source_id`s differ.

**Save and load.** `to_config()` holds `source_type: "derived"`, the origin's own config, the
map, the `record_id_column` and the `source_id` (so a reload keeps the same record ids). `from_config()` rebuilds the origin first. exp07 #7: the saved job
loads with no unbound source, the reloaded source has the same `content_hash`, and the
downstream node has the same `pipeline_hash`. The recipe, saved the same way, loads with its
source unbound (exp07 #8). A derived source over a job node stays not reloadable, as today
(decision 8).

**Large tables and ITL-186.** The copy is a per-row step, so it can stream: read the origin's
batches, append the column to each, number the rows as they pass. The default `source_id` does
not depend on a table hash, so nothing needs the whole table. Until sources stream, it reads
the origin's table once, as every source does today.

**Not run here:** a live SpiralDB table. `pyspiral` and its credentials are not on this
machine. `SQLiteTableSource` stood in: like `SpiralDBTableSource` it is a `DBTableSource`
whose primary keys become tags and which reloads from its config.

### 8.4 (c) How this differs from the rejected operator

* **The rule.** An operator's data elements all come from its input's data elements. A step
  where a data element comes from a tag value, or from anywhere else, is a derived source
  (the developer's criterion, quoted in 8.5). The copy fails that test as an operator and
  passes as a source.
* **Where the boundary is crossed.** Only when a source is defined. A source decides which of
  its columns are tags and which are data; that is its job. No operator and no pod is
  involved, so "operators never add data" and "pods never see tags" both hold without
  exception.
* **What downstream sees.** An ordinary source. Nothing downstream can tell that
  `session_start` began as a copy of a key. There is no operator node in the graph and no new
  node type.
* **Identity.** The operator was a link in the origin's chain. The derived source is a new
  root with its own system tags, tokens and schema-only `pipeline_hash`.
* **`Join` with the original.** Both have the same tags, so they match row for row. Joined
  whole, they raise `InputValidationError`, because both carry the origin's data columns.
  After selecting only the copy, `Join(derived[copy], original)` gives the 5 rows (exp07 #5).
  The normal use is to take the derived source instead of the original, not beside it.
* **Can the copy later differ from the tag?** Yes, as section 5.2 found for the operator, and
  for the same reason it is acceptable. The values are equal when the source is read. After
  that the copy is plain data: a pod may replace it, `MapData` may rename it. Row identity and
  `Join` matching follow the tags only, so a changed copy cannot mis-identify or mis-join a
  row.

### 8.5 (d) `GroupBy` checked against the developer's criterion

**The criterion** (developer, November 11, 2026), the rule that separates an operator from a
derived source: *does any data element come from a tag value, or from anywhere other than the
input data elements?* If yes, the step is a derived source. If no, it is an operator. In the
developer's words: "On GroupBy -- that is still just an operator. The key is whether any data
element has itself come from the tag or elsewhere OR not. In the case of GroupBy, grouping
makes use of tag but tag info never directly enters into the resultant data. This makes it a
proper operator. Whereas tag to data directly creates new data element from tag value, making
it derived source."

So `GroupBy` is not an open question. **But the facts do not match the description**, and the
developer should see them. Today's `GroupBy` does put tag values into the data group. exp08,
on the `experiment` table (tags `subject_id`, `session_start_datetime`, `experiment_index`):

```text
=== GroupBy(by=['subject_id'])
grouped keys(): (('subject_id',), ('session_start_datetime', 'experiment_index', 'experiment_start_timestamp', 'task_id'))
data schema: {'session_start_datetime': list[datetime.datetime], 'experiment_index': list[int], 'experiment_start_timestamp': list[int], 'task_id': list[str]}
experiment_index: [[0, 1, 0, 1], [0]]
_source_experiment_index: [None, None]
a pod reads the former tag as data: [('m1', 1), ('m2', 0)]
```

The group key `subject_id` stays a tag. The two tags that are **not** group keys,
`session_start_datetime` and `experiment_index`, leave the tag group and become list-valued
**data** columns. Their elements are the members' tag values, and a pod can read them
(`last_index` above takes `experiment_index: list[int]`). Their `_source_*` values are null,
because a tag never had a provenance token. This is documented behaviour (the `GroupBy`
docstring: "`probe` becomes a list-valued **data** column"). `Batch`, for contrast, keeps every
tag a tag, as a list.

By the criterion, then, the grouping itself is a proper operator, and the promotion of non-key
tags into data is the part that is not: those data elements come from tag values.

**Outcome (developer, November 11, 2026):** this behaviour of `GroupBy` is incorrect and will be
fixed in v0.3. It is filed as ITL-955 and handled separately; nothing is changed here. It
follows that `GroupBy` is **not a precedent** for moving tag values into data, and the places
in sections 1 to 6 that cite it as one no longer hold. A pipeline that needs a non-key tag's
values in a pod after grouping takes them through a derived source first.

### 8.6 (e) The inverse: a data column also as a tag

**Outcome (developer, November 11, 2026):** "data as tags is the cheaper and trivial operator.
We never care to track how tag has been added." By the criterion in 8.5 this direction is a
plain **operator**, not a derived source: no data element is created, and the data column
stays as it was. A tag gains a value that came from data, and tags carry no provenance to
protect.

It is out of scope for this change. A small follow-up operator (copy a data column into the
tags, keeping the data) would cover the foreign-key join after a pod (section 2.3). Until
then `tag_columns=[*primary_keys, "probe_id"]` at the source covers the workshop tables.

### 8.7 (f) Hashes and stored schemas

* **New:** the identity of a `DerivedSource` built with `tags_as_data`, and the config key
  `source_type: "derived"` in a saved job. Both exist only for pipelines that use the feature.
* **Moves:** nothing. A `DerivedSource` without the argument keeps its `content_hash`,
  `pipeline_hash` and `source_id`. No source, operator, pod or node changes. The golden hash
  tests must pass unchanged.
* **Stored schemas:** none. The derived source writes no table. Downstream nodes store the
  copy as any data column.
* **Versioning rules (ITL-938):** no path changes, because no stored schema changes. No
  algorithm is added, so there is no new label: the system-tag names, record ids and schema
  hash come from the existing source rule. The data context is the origin's. Nothing here
  writes a table, so the `__data_context` column of ITL-944 is not involved.

### 8.8 (g) Size and risk

About one day: the argument on `DerivedSource` (roughly 120 lines), `to_config`/`from_config`
and the registry entry, tests through the public interface (columns, exact types and
nullability, provenance, `Join`, both orchestrators, save and load, validation errors, the
unchanged identity of the plain form), a docs section with an executed example, CHANGELOG.
It can ship before November 17.

Risk is low: the change is additive and one class wide. The main risk is the choice of
defaults (decisions 4 to 6), which become stored record ids once people run pipelines.

**Fallback if it does not ship:** the recipe in section 6.2, now corrected to copy the field
rather than only the name. exp07 #8 found that `append_column("name", column)` makes the copy
nullable even when the key is not null, and a pod with a non-optional parameter then rejects
the stream. The recipe still cannot be reloaded.

### 8.9 (h) Decisions for the developer

1. **One class or two?** Extend `DerivedSource` with `tags_as_data`, or add a separate class.
   Recommendation: extend. It is the class the developer named, and the plain form is
   untouched.
2. **API surface.** Constructor `DerivedSource(origin, tags_as_data={tag: data_name})` plus
   the same keyword on `node.as_source()`; no new method on sources or streams.
   Recommendation: yes. A method can be added once the feature has proved itself.
3. **Origins.** Sources and job nodes now; arbitrary in-memory streams later.
   Recommendation: yes. A stream has no config to reload from, and inside a job its pods would
   run once as nodes and once more for the derived source.
4. **Provenance.** Fresh, by the standard source rule (own system tags, own tokens, record ids
   from `source_id` and row token), or carry the origin's system tags and tokens.
   Recommendation: fresh. It is what "a new source" means, it adds no new rule, and it equals
   the recipe exactly.
5. **Default `source_id`.** `derived:<origin source_id>`, or the origin's own.
   Recommendation: `derived:<origin source_id>`. With the origin's id, the record ids equal
   the origin's, but a job using both raises `InconsistentSourceError` (exp07 #7b), and the
   copy's token `experiment::row_0::session_start` would name a column the source called
   `experiment` does not have.
6. **`pipeline_hash`.** Schema only, or include the origin's pipeline hash.
   Recommendation: schema only (see 8.3). Including it would make the derived source an
   operator node in all but name.
7. **`content_hash`.** Origin's content hash, copies and `source_id`; the plain form unchanged.
   Recommendation: yes.
8. **Today's plain `node.as_source()`** has no system tags, null tokens, no `to_config`, and
   caches an empty result. Leave it alone in this change, or bring it under the same source
   rule? Recommendation: leave it alone now, because changing it moves the record ids of
   existing two-stage pipelines; file an issue to decide it separately.
9. **`GroupBy` promotes non-key tags into data** (8.5). The developer has ruled `GroupBy` a
   proper operator on the understanding that tag information never enters the data. It does,
   for tags that are not group keys (exp08). Keep them as list-valued tags like `Batch`, drop
   them, or accept the promotion as a stated exception? Recommendation: decide it in its own
   issue after the workshop, since the first two change `GroupBy`'s output schema and stored
   hashes; no change in this work.
10. **The inverse (`data_as_tags`)**. Recommendation: out of scope (8.6).
11. **Ship before the workshop?** Recommendation: yes, about a day, with a live check against
    a SpiralDB table before November 17, since none was possible here.

### 8.10 The developer's answers (November 11, 2026)

| # | Decision | Answer |
|---|---|---|
| 1 | One class or two | Extend `DerivedSource`. |
| 2 | API | `tags_as_data` on the constructor and on `node.as_source()`. |
| 3 | Origins | Sources and job nodes now. |
| 4 | Provenance | Fresh. "It should also try to save the result if associated with a database just like any other source pod." |
| 5 | Default `source_id` | Not `derived:<origin source_id>`. "It should be identified with combination of input stream/source id (origin source id) AND hash of the controlling criteria for the derived source operation (e.g. tag that's mapped) -- effectively it should be the content hash of the node I think." |
| 6 | `pipeline_hash` | Schema only. "Pipeline hash should stay true to what it is -- hash without propagating beyond source. ... the actual provenance above is captured by how the source ID evolves/extends with the content hash of the node to serve as signature of the fact this is a derived source." |
| 7 | `content_hash` | "Content hash should behave just like any other source -- it should include info about the source id. Just note that source id of a derived table IS what would have been an ordinary content hash invoked ON the derived source node, treating it as a downstream operator in a pipeline." |
| 8 | Plain `node.as_source()` gaps | Leave; filed as ITL-956. |
| 9 | `GroupBy` promoting non-key tags | Incorrect; to be fixed in v0.3 (ITL-955). Not a precedent. |
| 10 | Inverse | A plain operator, out of scope here (8.6). |
| 11 | Ship before November 17 | Yes. |

What answers 5 to 7 change in 8.3:

* **Default `source_id`** is `tags_as_data:<algorithm label>:<hex>`. Label and digest are the
  content hash that an operator node doing this derivation over the origin would have: the
  hash of `(("DerivedSource", sorted tags_as_data items), (origin,))`, with the origin entering
  through its own content hash (a `record_id_column`, when set, is one more parameter). It
  changes when the origin's identity or a parameter changes, and with nothing else. An
  explicit `source_id=` replaces it. Later decisions by the developer that shaped the format:
  a source id is the source's name and should identify the source easily, so a hash used as a
  default id carries its algorithm label and a kind prefix; the one string is used as-is in
  system-tag values and source-info tokens; the kind names the operation (`tags_as_data`),
  while a plain node-backed derived source keeps `derived:`; the origin's source id is not
  part of it, because a derived source may later combine several sources.
* **`content_hash`** follows the rule of every root source: class name, output schema and
  `source_id`. The origin and the mapping enter through the default `source_id`.
  **With an explicit `source_id`** the origin, the mapping and the `record_id_column` do not
  enter the `content_hash`: a source's content hash is an address, not a digest of its rows,
  and an explicit `source_id` is a name the user vouches for. This is the rule of every
  source (two `ArrowTableSource`s given the same `source_id` and schema have the same content
  hash). It matters for `derived.cached(database)`, whose table is keyed by the content hash:
  changing the origin, the mapping or the `record_id_column` under the same explicit
  `source_id` would reuse the rows cached before, so such a change needs a new `source_id`.
  The default `source_id` does this by itself, since it is built from all three.
* **`pipeline_hash`** stays schema only.
* The plain form, `node.as_source()` without `tags_as_data`, keeps today's identity and
  `source_id` (ITL-956).

On answer 4: no source saves its rows to a job's store today. A `PipelineJob` with a store
writes tables for its function and operator nodes only. A source is saved when the user wraps
it, `source.cached(database)`, which writes `source/node:<content hash>`. A derived source is
a root source, so `derived.cached(database)` works the same way. Whether more is wanted is
with the developer.

Implementation: branch `eywalker/itl-939-derived-source-tags-as-data`, pull request #317. A
read-only check against the live SpiralDB tutorial project passed there. It also showed that
`experiment_start_timestamp` in that dataset is a relative clock, not microseconds since the
epoch, so the elapsed-time formula in sections 6.1 and 8.3 is an illustration of the
mechanism, not a correct computation on the EDI tables.
This document and its experiments stay in the design pull request (#310).
