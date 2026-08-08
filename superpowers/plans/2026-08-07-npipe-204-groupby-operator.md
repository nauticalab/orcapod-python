# NPIPE-204 — `GroupBy` Operator + Type-Aware Source Info Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `GroupBy` operator that reduces N rows sharing a tag tuple into one packet with list-valued members, and fix the `Data` datagram so list-valued provenance no longer crashes `job.run()`.

**Architecture:** Three independent layers. (1) `Data` derives `_source_*` Arrow/Python types from the stored value instead of hard-coding `large_string`/`str` — this alone repairs the already-shipped `MergeJoin`. (2) A shared `arrow_utils` helper folds N members' system-tag values into one scalar using SHA-based primitives already in the codebase. (3) `GroupBy` partitions by tag values, sorts members deterministically, list-wraps everything except the group keys, and folds the system tags. `Batch` is semantically untouched — it only picks up the provenance fix.

**Tech Stack:** Python 3.11+, PyArrow, Polars, pytest, `uv run` for all commands, Delta Lake (`DeltaTableDatabase`) for job-level tests.

**Spec:** `superpowers/specs/2026-08-07-npipe-204-batch-group-by-design.md`

**Linear:** NPIPE-204 (status already *In Progress*). Branch `arnoldb/npipe-204-orcapod-add-tag-based-grouping-to-batch-manyone-reduction` is already checked out.

---

## Background an implementer needs

**The bug, minimally.** A `Data` datagram whose source info holds a list crashes on `as_table`:

```python
data.source_info()                        # {'probe': None, 'path': ['s0', 's1']}
data.schema(columns={"source": True})     # '_source_path': str      <- WRONG
data.as_table(columns={"source": True})   # ArrowTypeError: Expected bytes, got a 'list' object
```

Two operators already produce this state: `MergeJoin` (carries source columns as parallel lists when merging colliding data columns, `merge_join.py:262`) and `Batch` (list-wraps every column). Neither is caught by existing tests because all operator tests stop at `op.process(stream)` + `as_table()`; the crash happens in `StaticOutputOperatorPod._materialize_to_stream`, only reached via `job.run()`.

**Column vocabulary.** For a stream with tag `subject` and data `path`, `stream.as_table(columns={"source": True, "system_tags": True})` yields exactly:

```
subject                                      # user tag
path                                         # data
_tag_source_id::<schema_hash>                # system tag, large_string
_tag_record_id::<schema_hash>                # system tag, binary(16)
_source_path                                 # provenance, large_string
```

Note `_context_key` is **absent** — no operator sees it. Do not add it.

**`ArrowTableStream` fills in missing source columns.** If the output table has a data column with no matching `_source_<col>`, the stream creates one with value `None` and type `large_string`. Verified. So `GroupBy` does not need to synthesize source columns for the tag columns it promotes to data — but `unary_output_schema` must still *predict* them.

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `src/orcapod/core/datagrams/tag_data.py` | `Data` source-info type derivation | Modify |
| `src/orcapod/utils/polars_data_utils.py` | Delete dead `add_source_info` | Modify |
| `src/orcapod/utils/arrow_utils.py` | Shared system-tag fold helper | Modify |
| `src/orcapod/core/operators/batch.py` | Provenance fix only | Modify |
| `src/orcapod/core/operators/group_by.py` | The new operator | **Create** |
| `src/orcapod/core/operators/__init__.py` | Export | Modify |
| `src/orcapod/pipeline/serialization.py` | Operator registry | Modify |
| `src/orcapod/core/streams/base.py` | `.group_by()` fluent method | Modify |
| `tests/test_core/datagrams/test_data_source_info_types.py` | Part 1 unit tests | **Create** |
| `tests/test_utils/test_arrow_utils.py` | Fold helper + cross-process stability | Modify |
| `tests/test_core/operators/test_group_by.py` | Operator-level tests | **Create** |
| `tests/test_pipeline/test_aggregation_job.py` | Job-level tests | **Create** |
| `CLAUDE.md` + `.zed/rules` | Doc updates | Modify |
| `DESIGN_ISSUES.md` | U1 resolution note | Modify |

---

## Task 1: Type-aware source info in `Data`

**Files:**
- Modify: `src/orcapod/core/datagrams/tag_data.py`
- Test: `tests/test_core/datagrams/test_data_source_info_types.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_core/datagrams/test_data_source_info_types.py`:

```python
"""Source-info values may be lists, not just scalar strings.

Many->one operators (GroupBy, MergeJoin) produce one provenance token per
member.  `Data` must represent those without collapsing or crashing.
"""

from __future__ import annotations

import pyarrow as pa

from orcapod.core.datagrams import Data


def _data_with_mixed_source_info() -> Data:
    """Data with one list-valued and one scalar-null source token."""
    return Data(
        {"probe": [0, 1], "path": ["a", "b"]},
        source_info={"probe": None, "path": ["s0", "s1"]},
    )


class TestListValuedSourceInfo:
    def test_schema_reports_list_type_for_list_valued_token(self):
        data = _data_with_mixed_source_info()
        schema = data.schema(columns={"source": True})
        assert schema["_source_path"] == list[str]

    def test_schema_reports_str_for_none_token(self):
        data = _data_with_mixed_source_info()
        schema = data.schema(columns={"source": True})
        assert schema["_source_probe"] is str

    def test_as_table_round_trips_list_valued_token(self):
        data = _data_with_mixed_source_info()
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_list(
            pa.large_string()
        )
        assert table.column("_source_path").to_pylist() == [["s0", "s1"]]

    def test_as_table_keeps_none_token_as_large_string(self):
        data = _data_with_mixed_source_info()
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_probe").type == pa.large_string()
        assert table.column("_source_probe").to_pylist() == [None]

    def test_empty_list_token_defaults_to_list_of_string(self):
        data = Data({"path": ["a"]}, source_info={"path": []})
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_list(
            pa.large_string()
        )

    def test_scalar_token_unchanged(self):
        """Existing scalar behavior must not regress."""
        data = Data({"path": "a"}, source_info={"path": "src::row_0::path"})
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_string()
        assert data.schema(columns={"source": True})["_source_path"] is str

    def test_arrow_table_construction_recovers_list_token(self):
        """Data built from an Arrow table keeps list-valued source info."""
        table = pa.table({
            "path": pa.array([["a", "b"]], pa.list_(pa.large_string())),
            "_source_path": pa.array([["s0", "s1"]], pa.list_(pa.large_string())),
        })
        data = Data(table)
        assert data.source_info()["path"] == ["s0", "s1"]
        assert data.as_table(columns={"source": True}).column(
            "_source_path"
        ).to_pylist() == [["s0", "s1"]]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_core/datagrams/test_data_source_info_types.py -v
```

Expected: `test_schema_reports_list_type_for_list_valued_token` FAILS (`assert str == list[str]`), and the four `as_table` tests FAIL with `ArrowTypeError: Expected bytes, got a 'list' object`. `test_schema_reports_str_for_none_token` and `test_scalar_token_unchanged` should already PASS.

- [ ] **Step 3: Add the type-derivation helpers**

In `src/orcapod/core/datagrams/tag_data.py`, immediately above `class Data(Datagram):` (currently line 240), add:

```python
# A provenance token is a string, an unknown (None), or — for many->one
# operators such as GroupBy and MergeJoin — a list of tokens, one per member.
SourceInfoValue = str | None | list["SourceInfoValue"]


def _source_info_arrow_type(value: "SourceInfoValue") -> "pa.DataType":
    """Derive the Arrow type for a single source-info value.

    Scalars and unknowns map to ``large_string``; lists map to ``large_list``
    of their element type, recursively.  An empty list defaults to
    ``large_list(large_string)``.

    Args:
        value: The stored provenance token.

    Returns:
        The Arrow type to declare for this value.
    """
    import pyarrow as _pa

    if isinstance(value, (list, tuple)):
        if not value:
            return _pa.large_list(_pa.large_string())
        return _pa.large_list(_source_info_arrow_type(value[0]))
    return _pa.large_string()


def _source_info_python_type(value: "SourceInfoValue") -> type:
    """Derive the Python type for a single source-info value.

    Mirrors ``_source_info_arrow_type`` for the ``Schema`` representation.

    Args:
        value: The stored provenance token.

    Returns:
        ``str`` for scalars and unknowns, ``list[...]`` for lists.
    """
    if isinstance(value, (list, tuple)):
        if not value:
            return list[str]
        return list[_source_info_python_type(value[0])]  # type: ignore[misc]
    return str
```

- [ ] **Step 4: Use the helpers in `_ensure_source_info_table`**

Replace the body of `_ensure_source_info_table` (currently `tag_data.py:330-347`):

```python
    def _ensure_source_info_table(self) -> "pa.Table":
        if self._source_info_table is None:
            import pyarrow as _pa

            if self._source_info:
                prefixed = {
                    f"{constants.SOURCE_PREFIX}{k}": v
                    for k, v in self._source_info.items()
                }
                schema = _pa.schema(
                    [
                        _pa.field(k, _source_info_arrow_type(v))
                        for k, v in prefixed.items()
                    ]
                )
                self._source_info_table = _pa.Table.from_pylist(
                    [prefixed], schema=schema
                )
            else:
                self._source_info_table = _pa.table({})
        return self._source_info_table
```

- [ ] **Step 5: Use the helper in `Data.schema`**

In `Data.schema` (currently `tag_data.py:384-395`), replace the `if column_config.source:` block:

```python
        if column_config.source:
            for key in super().keys():
                schema[f"{constants.SOURCE_PREFIX}{key}"] = _source_info_python_type(
                    self._source_info.get(key)
                )
```

- [ ] **Step 6: Widen the type annotations**

Four annotation sites in `tag_data.py`, all mechanical — no logic change:

1. `Data.__init__` signature (line ~257):
   `source_info: "Mapping[str, str | None] | None" = None`
   → `source_info: "Mapping[str, SourceInfoValue] | None" = None`
2. Arrow-path assignment (line ~296):
   `self._source_info: dict[str, str | None] = {`
   → `self._source_info: dict[str, SourceInfoValue] = {`
3. Dict-path local (line ~309):
   `contained_source_info: dict[str, str | None] = {`
   → `contained_source_info: dict[str, SourceInfoValue] = {`
4. `source_info()` return (line ~353) and `with_source_info()` kwargs (line ~357):
   `def source_info(self) -> "dict[str, str | None]":`
   → `def source_info(self) -> "dict[str, SourceInfoValue]":`
   `def with_source_info(self, **source_info: "str | None") -> Self:`
   → `def with_source_info(self, **source_info: "SourceInfoValue") -> Self:`

- [ ] **Step 7: Run the test to verify it passes**

```bash
uv run pytest tests/test_core/datagrams/test_data_source_info_types.py -v
```

Expected: all 7 PASS.

- [ ] **Step 8: Verify the MergeJoin crash is gone**

This is the payoff — an already-shipped operator that was broken.

```bash
uv run pytest tests/test_core/ tests/test_utils/ -x -q
```

Expected: no failures, no new errors.

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/core/datagrams/tag_data.py tests/test_core/datagrams/test_data_source_info_types.py
git commit -m "fix(datagrams): derive source-info column types from value (NPIPE-204)

_ensure_source_info_table and Data.schema hard-coded large_string / str for
every _source_* field, so any operator producing a list-valued provenance
token crashed with ArrowTypeError inside job.run(). MergeJoin already does
this (merge_join.py:262) and was silently broken.

Types are now derived from the stored value. None still maps to
large_string, so unknown-provenance columns are unchanged. No pipeline-DB
schema bump: a node's source-column type is fixed by its own output schema.

Refs DESIGN_ISSUES U1.
NPIPE-204"
```

---

## Task 2: Delete the dead `polars_data_utils.add_source_info`

**Files:**
- Modify: `src/orcapod/utils/polars_data_utils.py`

Third site with the same hard-coded assumption (`dtype=pl.String()`, line 119). It has **zero callers** in `src/` and no test coverage — the tests that import `add_source_info` import it from `arrow_utils`. It also has a latent shadowing bug: `source_column` is rebound to a `pl.Series` inside the per-column loop, so from the second column onward it formats `f"{<Series repr>}::{col}"`.

- [ ] **Step 1: Confirm it is unreferenced**

```bash
grep -rn "polars_data_utils" --include=*.py src/ tests/
grep -rn "add_source_info" --include=*.py src/ tests/
```

Expected: the only `polars_data_utils` references are `drop_system_columns` (from `data_frame_source.py:57` and `polling_source.py:454`). Every `add_source_info` reference resolves to `arrow_utils`. If either expectation fails, **stop** and report — do not delete.

- [ ] **Step 2: Delete the function**

Remove the entire `def add_source_info(...)` definition from `src/orcapod/utils/polars_data_utils.py` (starts line 93, ends at the `return df` around line 125). Leave every other function in the module untouched.

- [ ] **Step 3: Verify nothing broke**

```bash
uv run pytest tests/ -q -x
```

Expected: full suite passes.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/utils/polars_data_utils.py
git commit -m "chore(utils): delete dead polars_data_utils.add_source_info (NPIPE-204)

Zero callers in src/, no test coverage, and a latent shadowing bug where
source_column is rebound to a pl.Series inside the per-column loop. It was
also a third site hard-coding a scalar string type for source info.

Refs DESIGN_ISSUES U1.
NPIPE-204"
```

---

## Task 3: Shared system-tag fold helper

**Files:**
- Modify: `src/orcapod/utils/arrow_utils.py`
- Test: `tests/test_utils/test_arrow_utils.py`

`_build_record_id_preimage` (`core/nodes/function_node.py:82`) hashes system-tag columns directly, so they must be scalar. A many→one operator must fold N members' values into one, preserving each column's Arrow type.

**Both primitives are reused from the codebase and are SHA-based, therefore stable across processes.** Never use `hash()` or set iteration order here — a fold using either would pass every same-process test and then miss the cache on every new driver run.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_utils/test_arrow_utils.py`:

```python
# ---------------------------------------------------------------------------
# fold_system_tag_values
# ---------------------------------------------------------------------------


class TestFoldSystemTagValues:
    """Folding N members' system-tag values into one scalar (NPIPE-204).

    The expected digests below are hard-coded on purpose.  A fold that used
    hash() or set-iteration order would still be self-consistent within one
    process; pinning the values is what catches it.
    """

    SOURCE_COL = "_tag_source_id::abc123"
    RECORD_COL = "_tag_record_id::abc123"

    RIDS = [
        bytes.fromhex("0102030405060708090a0b0c0d0e0f10"),
        bytes.fromhex("1112131415161718191a1b1c1d1e1f20"),
    ]
    EXPECTED_RID = bytes.fromhex("853be16a3f38565f8ced039f84fdbea6")
    EXPECTED_SID = (
        "7916442d59841140bedf6c1f5dcc1304ae9fce0ba885765c06e511086b85da2e"
    )

    def test_record_id_folds_to_16_bytes(self):
        from orcapod.utils.arrow_utils import fold_system_tag_values

        result = fold_system_tag_values(self.RECORD_COL, self.RIDS)
        assert isinstance(result, bytes)
        assert len(result) == 16

    def test_record_id_digest_is_pinned(self):
        from orcapod.utils.arrow_utils import fold_system_tag_values

        assert fold_system_tag_values(self.RECORD_COL, self.RIDS) == self.EXPECTED_RID

    def test_source_id_digest_is_pinned(self):
        from orcapod.utils.arrow_utils import fold_system_tag_values

        result = fold_system_tag_values(self.SOURCE_COL, ["src_a", "src_b"])
        assert result == self.EXPECTED_SID

    def test_order_matters(self):
        """Member order is part of the identity, matching the data lists."""
        from orcapod.utils.arrow_utils import fold_system_tag_values

        forward = fold_system_tag_values(self.RECORD_COL, self.RIDS)
        reverse = fold_system_tag_values(self.RECORD_COL, list(reversed(self.RIDS)))
        assert forward != reverse

    def test_single_member_is_still_folded(self):
        """A one-member group folds rather than passing the value through."""
        from orcapod.utils.arrow_utils import fold_system_tag_values

        result = fold_system_tag_values(self.RECORD_COL, self.RIDS[:1])
        assert isinstance(result, bytes) and len(result) == 16
        assert result != self.RIDS[0]

    def test_none_members_are_tolerated(self):
        from orcapod.utils.arrow_utils import fold_system_tag_values

        assert isinstance(
            fold_system_tag_values(self.RECORD_COL, [None, self.RIDS[0]]), bytes
        )
        assert isinstance(
            fold_system_tag_values(self.SOURCE_COL, [None, "src_a"]), str
        )

    def test_digest_is_stable_across_processes(self):
        """Fresh interpreter, therefore fresh PYTHONHASHSEED.

        This is the test that catches a fold built on hash() or set order:
        such a fold is self-consistent within one process and only diverges
        on a new driver run.
        """
        import subprocess
        import sys

        script = (
            "from orcapod.utils.arrow_utils import fold_system_tag_values\n"
            "rids = [bytes.fromhex('0102030405060708090a0b0c0d0e0f10'),\n"
            "        bytes.fromhex('1112131415161718191a1b1c1d1e1f20')]\n"
            "print(fold_system_tag_values('_tag_record_id::abc123', rids).hex())\n"
            "print(fold_system_tag_values('_tag_source_id::abc123', ['src_a','src_b']))\n"
        )
        out = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
        assert out[0] == self.EXPECTED_RID.hex()
        assert out[1] == self.EXPECTED_SID
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_utils/test_arrow_utils.py::TestFoldSystemTagValues -v
```

Expected: all FAIL with `ImportError: cannot import name 'fold_system_tag_values'`.

- [ ] **Step 3: Implement the helper**

In `src/orcapod/utils/arrow_utils.py`, add immediately after `append_to_system_tags` (currently ends line 1157):

```python
# Fixed namespace for aggregated record IDs produced by many->one operators.
# Mirrors _SOURCE_RECORD_ID_NAMESPACE in core/sources/stream_builder.py.
# Computed value: uuid.UUID('96411bfc-d3ba-5395-ba6f-5bb5726f18ad')
_AGGREGATED_RECORD_ID_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL,
    "https://orcapod.org/namespaces/aggregated-record-id",
)


def fold_system_tag_values(
    column_name: str, values: "Sequence[Any]"
) -> "str | bytes":
    """Fold a group's system-tag values into one scalar of the same type.

    Many->one operators must emit scalar system tags, because
    ``_build_record_id_preimage`` (``core/nodes/function_node.py``) hashes
    those columns directly to derive a record's identity.  Each column folds
    independently over its own ordered member values.

    Both digests are SHA-based and therefore stable across processes.  Never
    substitute ``hash()`` or a set-based construction: orcapod uses the result
    as a cache key, so a per-process digest would miss the cache on every new
    driver run while looking correct in a single-process test.

    Member order is significant — it matches the order of the list-valued data
    columns the folded tag accompanies.

    Args:
        column_name: The system-tag column name, used to select the fold. Names
            starting with ``constants.SYSTEM_TAG_RECORD_ID_PREFIX`` fold to
            ``binary(16)``; everything else folds to a hex string.
        values: The group's member values, in emission order.

    Returns:
        16 raw bytes for a record_id column, a 64-character hex string
        otherwise.
    """
    if column_name.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX):
        name = constants.BLOCK_SEPARATOR.join(
            "" if v is None else v.hex() for v in values
        )
        return uuid.uuid5(_AGGREGATED_RECORD_ID_NAMESPACE, name).bytes
    return combine_hashes(
        *["" if v is None else str(v) for v in values], order=False
    )
```

Add the imports at the top of `arrow_utils.py` if not already present:

```python
import uuid

from orcapod.hashing.hash_utils import combine_hashes
```

If importing `combine_hashes` at module scope creates a circular import, move it inside the function body instead and note why.

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_utils/test_arrow_utils.py::TestFoldSystemTagValues -v
```

Expected: all 7 PASS, including `test_digest_is_stable_across_processes`.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/utils/arrow_utils.py tests/test_utils/test_arrow_utils.py
git commit -m "feat(arrow_utils): add fold_system_tag_values for many-to-one operators (NPIPE-204)

System tags must stay scalar because _build_record_id_preimage hashes them
directly. Each column folds independently over its ordered member values:
record_id via uuid5 (matching stream_builder._make_record_id), source_id via
combine_hashes. Both SHA-based, so the digest is stable across processes --
pinned by a subprocess test, since a hash()-based fold would look correct
within one process and miss the cache on every new driver run.

NPIPE-204"
```

---

## Task 4: `Batch` provenance fix

**Files:**
- Modify: `src/orcapod/core/operators/batch.py:43-102`
- Test: `tests/test_core/operators/test_operators.py`

`Batch`'s partitioning, its list-valued tag columns, and its streaming `async_execute` all stay exactly as they are. The only change: system-tag columns fold to scalars instead of being list-wrapped, and `_source_*` columns keep their list form (now representable thanks to Task 1).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_core/operators/test_operators.py`, inside `class TestBatchBehavior`:

```python
    def test_batch_system_tags_are_scalar(self):
        """System tags must stay scalar -- record identity hashes them directly."""
        from orcapod.core.sources import ArrowTableSource
        from orcapod.system_constants import constants

        table = pa.table({
            "animal": ["cat", "dog"],
            "weight": [4.0, 12.0],
        })
        source = ArrowTableSource(table, tag_columns=["animal"], infer_nullable=True)
        out = Batch(batch_size=0).process(source)
        result = out.as_table(columns={"source": True, "system_tags": True})

        sys_cols = [
            c for c in result.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert sys_cols, "expected system tag columns on the batched output"
        for col in sys_cols:
            assert not pa.types.is_list(result.schema.field(col).type)
            assert not pa.types.is_large_list(result.schema.field(col).type)

    def test_batch_source_columns_are_lists(self):
        """Provenance stays per-member rather than collapsing."""
        from orcapod.core.sources import ArrowTableSource
        from orcapod.system_constants import constants

        table = pa.table({
            "animal": ["cat", "dog"],
            "weight": [4.0, 12.0],
        })
        source = ArrowTableSource(table, tag_columns=["animal"], infer_nullable=True)
        out = Batch(batch_size=0).process(source)
        result = out.as_table(columns={"source": True, "system_tags": True})

        src_col = f"{constants.SOURCE_PREFIX}weight"
        assert src_col in result.column_names
        assert len(result.column(src_col).to_pylist()[0]) == 2
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_core/operators/test_operators.py::TestBatchBehavior -v
```

Expected: `test_batch_system_tags_are_scalar` FAILS (system tag columns are `list<...>`). `test_batch_source_columns_are_lists` should already PASS.

- [ ] **Step 3: Rewrite `unary_static_process`**

Replace `Batch.unary_static_process` (`batch.py:43-82`) with:

```python
    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Group rows into fixed-size batches, list-wrapping their values.

        Tag and data columns become list-valued.  Source-info columns become
        list-valued too, one element per batch member.  System-tag columns are
        folded to a scalar instead — record identity hashes them directly, so
        they must not become lists.

        Args:
            stream: The upstream stream.

        Returns:
            A stream with one row per batch.
        """
        table = stream.as_table(columns={"source": True, "system_tags": True})

        tag_columns, _ = stream.keys()

        system_tag_columns = tuple(
            c
            for c in table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        member_columns = tuple(
            c for c in table.column_names if c not in system_tag_columns
        )

        data_list = table.to_pylist()

        batches: list[list[dict[str, Any]]] = []
        next_batch: list[dict[str, Any]] = []

        for entry in data_list:
            next_batch.append(entry)
            if self.batch_size > 0 and len(next_batch) >= self.batch_size:
                batches.append(next_batch)
                next_batch = []

        if next_batch and not self.drop_partial_batch:
            batches.append(next_batch)

        batched_data = [
            {
                **{c: [m[c] for m in members] for c in member_columns},
                **{
                    c: arrow_utils.fold_system_tag_values(c, [m[c] for m in members])
                    for c in system_tag_columns
                },
            }
            for members in batches
        ]

        input_fields = {f.name: f for f in table.schema}
        batched_schema = pa.schema(
            [
                pa.field(c, pa.list_(input_fields[c].type), nullable=False)
                if c in member_columns
                else input_fields[c]
                for c in table.column_names
            ]
        )
        batched_table = pa.Table.from_pylist(batched_data, schema=batched_schema)

        n_char = self.orcapod_config.hashing.system_tag_n_char
        batched_table = arrow_utils.append_to_system_tags(
            batched_table, stream.pipeline_hash().to_hex(n_char)
        )

        return ArrowTableStream(
            batched_table,
            tag_columns=tag_columns,
            data_context=stream.data_context,
        )
```

Add these imports to the top of `batch.py`:

```python
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils
```

- [ ] **Step 4: Update `unary_output_schema` to mirror it**

Replace `Batch.unary_output_schema` (`batch.py:84-102`):

```python
    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Predict the batched output schemas without batching.

        Every user tag, data, and source column becomes ``list[T]``.  System
        tag columns keep their scalar type and gain a ``::{pipeline_hash}``
        name suffix.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config.
            all_info: Include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.
        """
        tag_types, data_types = stream.output_schema(columns=columns, all_info=all_info)
        n_char = self.orcapod_config.hashing.system_tag_n_char
        suffix = stream.pipeline_hash().to_hex(n_char)

        batched_tag_types: dict[str, Any] = {}
        for name, col_type in tag_types.items():
            if name.startswith(constants.SYSTEM_TAG_PREFIX):
                batched_tag_types[
                    f"{name}{constants.BLOCK_SEPARATOR}{suffix}"
                ] = col_type
            else:
                batched_tag_types[name] = list[col_type]

        batched_data_types = {k: list[v] for k, v in data_types.items()}

        return Schema(batched_tag_types), Schema(batched_data_types)
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
uv run pytest tests/test_core/operators/ -v
```

Expected: all PASS, including the pre-existing `TestBatchBehavior` cases (`test_batch_groups_rows`, `test_batch_drop_partial`, `test_batch_output_lineage`, `test_batch_size_zero_returns_single_batch`, `test_negative_batch_size_raises`).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/operators/batch.py tests/test_core/operators/test_operators.py
git commit -m "fix(batch): fold system tags to scalar instead of list-wrapping (NPIPE-204)

Batch list-wrapped every column including the system tags, but record
identity hashes those columns directly, so they must stay scalar. They now
fold via fold_system_tag_values and gain a ::{pipeline_hash} name suffix,
mirroring the name-extending rule joins already use.

Partitioning, list-valued tag columns, and the streaming async_execute path
are unchanged.

NPIPE-204"
```

---

## Task 5: The `GroupBy` operator

**Files:**
- Create: `src/orcapod/core/operators/group_by.py`
- Test: `tests/test_core/operators/test_group_by.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_core/operators/test_group_by.py`:

```python
"""Tests for the GroupBy operator — many->one reduction keyed on tag values."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import GroupBy
from orcapod.core.sources import ArrowTableSource
from orcapod.errors import InputValidationError
from orcapod.system_constants import constants


@pytest.fixture
def session_table() -> pa.Table:
    """Two sessions x two probes: the common-clock shape from NPIPE-204."""
    return pa.table({
        "subject": ["G", "G", "G", "G"],
        "date": ["d1", "d1", "d2", "d2"],
        "probe": [1, 0, 1, 0],
        "path": ["b", "a", "d", "c"],
    })


@pytest.fixture
def session_source(session_table) -> ArrowTableSource:
    return ArrowTableSource(
        session_table,
        tag_columns=["subject", "date", "probe"],
        infer_nullable=True,
    )


class TestGroupByShape:
    def test_one_row_per_distinct_key(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        assert len(out.as_table()) == 2

    def test_group_keys_are_scalar_tags(self, session_source):
        op = GroupBy(by=["subject", "date"])
        out = op.process(session_source)
        tag_cols, _ = out.keys()
        assert set(tag_cols) == {"subject", "date"}
        assert out.as_table().column("subject").to_pylist() == ["G", "G"]

    def test_non_key_tags_promoted_to_list_data(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        _, data_cols = out.keys()
        assert "probe" in data_cols
        assert out.as_table().column("probe").to_pylist() == [[0, 1], [0, 1]]

    def test_data_columns_are_lists(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        assert out.as_table().column("path").to_pylist() == [["a", "b"], ["c", "d"]]

    def test_source_columns_are_lists(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        table = out.as_table(columns={"source": True})
        assert len(table.column(f"{constants.SOURCE_PREFIX}path").to_pylist()[0]) == 2

    def test_system_tags_are_scalar_and_renamed(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        table = out.as_table(columns={"system_tags": True})
        sys_cols = [
            c for c in table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert sys_cols
        for col in sys_cols:
            field_type = table.schema.field(col).type
            assert not pa.types.is_list(field_type)
            assert not pa.types.is_large_list(field_type)
            # name-extended: original "::<schema_hash>" plus "::<pipeline_hash>"
            assert col.count(constants.BLOCK_SEPARATOR) >= 2


class TestGroupByOrdering:
    def test_members_sorted_by_non_key_tags(self, session_source):
        """probe=[1,0] on input must emit as [0,1]."""
        out = GroupBy(by=["subject", "date"]).process(session_source)
        assert out.as_table().column("probe").to_pylist()[0] == [0, 1]

    def test_row_order_does_not_affect_output(self, session_table):
        """Same rows, shuffled, must produce a byte-identical table."""
        shuffled = session_table.take([3, 1, 2, 0])

        def run(tbl):
            src = ArrowTableSource(
                tbl, tag_columns=["subject", "date", "probe"], infer_nullable=True
            )
            return GroupBy(by=["subject", "date"]).process(src).as_table()

        assert run(session_table).equals(run(shuffled))

    def test_falls_back_to_record_id_when_key_covers_all_tags(self):
        """by covering every tag leaves no non-key tag to sort on."""
        table = pa.table({"subject": ["G", "G"], "path": ["b", "a"]})
        src = ArrowTableSource(table, tag_columns=["subject"], infer_nullable=True)
        out = GroupBy(by=["subject"]).process(src)
        assert len(out.as_table()) == 1
        assert sorted(out.as_table().column("path").to_pylist()[0]) == ["a", "b"]


class TestGroupByValidation:
    def test_empty_by_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            GroupBy(by=[])

    def test_unknown_column_raises(self, session_source):
        op = GroupBy(by=["subject", "nonexistent"])
        with pytest.raises(InputValidationError, match="nonexistent"):
            op.process(session_source)

    def test_data_column_as_key_raises(self, session_source):
        """Grouping on a data column is not allowed -- keys must be tags."""
        op = GroupBy(by=["path"])
        with pytest.raises(InputValidationError, match="path"):
            op.process(session_source)


class TestGroupByEmptyInput:
    def test_empty_input_yields_zero_groups(self):
        table = pa.table({
            "subject": pa.array([], pa.large_string()),
            "path": pa.array([], pa.large_string()),
        })
        from orcapod.core.streams import ArrowTableStream

        stream = ArrowTableStream(table, tag_columns=["subject"])
        out = GroupBy(by=["subject"]).process(stream)
        assert len(out.as_table()) == 0


class TestGroupByIdentity:
    def test_identity_structure_includes_by(self):
        assert GroupBy(by=["a"]).identity_structure() != GroupBy(
            by=["b"]
        ).identity_structure()

    def test_to_config_round_trip(self):
        op = GroupBy(by=["subject", "date"])
        config = op.to_config()
        # A list, not a tuple, so the config stays JSON-serializable.
        assert config["config"]["by"] == ["subject", "date"]
        rebuilt = GroupBy.from_config(config)
        assert rebuilt.identity_structure() == op.identity_structure()

    def test_to_config_is_json_serializable(self):
        import json

        json.dumps(GroupBy(by=["subject", "date"]).to_config()["config"])
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_core/operators/test_group_by.py -v
```

Expected: all FAIL at collection with `ImportError: cannot import name 'GroupBy'`.

- [ ] **Step 3: Create the operator**

**Do not write an `async_execute` override.** `UnaryOperator.async_execute`
(`core/operators/base.py:71`) already collects the full input before calling
`static_process`, which is exactly the barrier `GroupBy` needs — no group can be emitted
before the input channel closes, because any row not yet seen could belong to a group
already started. Task 7 adds a test asserting the override is absent.

Create `src/orcapod/core/operators/group_by.py`:

```python
"""GroupBy operator — many->one reduction keyed on tag values."""

from __future__ import annotations

import logging
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class GroupBy(UnaryOperator):
    """Reduce rows sharing a tag tuple into one packet with list-valued members.

    This is the only many->one operator.  Every other operator preserves one
    row per tag; ``GroupBy`` collapses N rows into one, which is what lets a
    downstream pod receive a whole group at once (for example, all of a
    recording session's per-probe result parquets).

    Given tags ``(subject, date, probe)`` and data ``(path)``, grouping by
    ``["subject", "date"]`` emits one row per distinct ``(subject, date)``:

    * ``subject`` and ``date`` stay scalar and remain the output's tag columns
    * ``probe`` becomes a list-valued **data** column, so a consumer can tell
      which member each list element came from
    * ``path`` becomes list-valued
    * ``_source_*`` columns become list-valued, one element per member
    * system-tag columns fold to a scalar digest and gain a
      ``::{pipeline_hash}`` name suffix

    Members are sorted by their non-group-key tag values, so the emitted lists
    are stable across runs.  This matters because orcapod hashes those lists to
    build the cache key — an unsorted list would make an identical member set
    hash differently and trigger a spurious recompute.

    Contrast with ``Batch``, which partitions by row count for throughput and
    keeps its tag columns as list-valued tags.

    Args:
        by: Tag column names to group on.  Must be non-empty and must all be
            tag columns of the input stream.
    """

    def __init__(self, by: Collection[str], **kwargs: Any) -> None:
        by_tuple = tuple(by)
        if not by_tuple:
            raise ValueError("GroupBy requires at least one column in `by`.")
        self.by = by_tuple
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        return (self.__class__.__name__, self.by)

    def to_config(self) -> dict[str, Any]:
        """Serialize this GroupBy operator to a config dict.

        ``by`` is emitted as a list rather than a tuple so the config stays
        JSON-serializable; ``__init__`` normalizes it back to a tuple.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``by``.
        """
        config = super().to_config()
        config["config"] = {"by": list(self.by)}
        return config

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Verify every grouping column is a tag column of the input.

        Args:
            stream: The upstream stream to validate.

        Raises:
            InputValidationError: If any name in ``by`` is not a tag column.
        """
        tag_columns, data_columns = stream.keys()
        missing = [c for c in self.by if c not in tag_columns]
        if missing:
            raise InputValidationError(
                f"GroupBy: {missing} are not tag columns of the input stream. "
                f"Available tag columns: {list(tag_columns)}. "
                f"(Data columns cannot be grouping keys: {list(data_columns)})"
            )

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Partition rows by group key and emit one row per group.

        Args:
            stream: The upstream stream.

        Returns:
            A stream with one row per distinct group-key tuple.
        """
        table = stream.as_table(columns={"source": True, "system_tags": True})
        tag_columns, _ = stream.keys()

        system_tag_columns = tuple(
            c
            for c in table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        member_columns = tuple(
            c
            for c in table.column_names
            if c not in self.by and c not in system_tag_columns
        )
        # Non-key user tags give a total order within a group: tags are unique
        # within a stream.  When `by` covers every tag, fall back to record_id.
        sort_columns = tuple(c for c in tag_columns if c not in self.by)
        record_id_column = next(
            (
                c
                for c in system_tag_columns
                if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
            ),
            None,
        )

        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in table.to_pylist():
            groups.setdefault(tuple(row[c] for c in self.by), []).append(row)

        grouped_rows: list[dict[str, Any]] = []
        for key, members in groups.items():
            if sort_columns:
                members.sort(
                    key=lambda r: tuple(
                        (r[c] is None, r[c]) for c in sort_columns
                    )
                )
            elif record_id_column is not None:
                members.sort(key=lambda r: r[record_id_column] or b"")

            grouped_rows.append({
                **dict(zip(self.by, key)),
                **{c: [m[c] for m in members] for c in member_columns},
                **{
                    c: arrow_utils.fold_system_tag_values(
                        c, [m[c] for m in members]
                    )
                    for c in system_tag_columns
                },
            })

        input_fields = {f.name: f for f in table.schema}
        grouped_schema = pa.schema([
            pa.field(c, pa.list_(input_fields[c].type), nullable=False)
            if c in member_columns
            else input_fields[c]
            for c in table.column_names
        ])
        grouped_table = pa.Table.from_pylist(grouped_rows, schema=grouped_schema)

        n_char = self.orcapod_config.hashing.system_tag_n_char
        grouped_table = arrow_utils.append_to_system_tags(
            grouped_table, stream.pipeline_hash().to_hex(n_char)
        )

        return ArrowTableStream(
            grouped_table,
            tag_columns=self.by,
            data_context=stream.data_context,
        )

    # ------------------------------------------------------------------
    # Schema prediction
    # ------------------------------------------------------------------

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Predict the grouped output schemas without grouping.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config.
            all_info: Include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.  Group keys stay scalar in
            the tag schema; promoted non-key tags and list-wrapped data columns
            land in the data schema.
        """
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        tag_types, data_types = stream.output_schema(
            columns=columns, all_info=all_info
        )
        n_char = self.orcapod_config.hashing.system_tag_n_char
        suffix = stream.pipeline_hash().to_hex(n_char)

        out_tag_types: dict[str, Any] = {}
        out_data_types: dict[str, Any] = {}

        for name, col_type in tag_types.items():
            if name.startswith(constants.SYSTEM_TAG_PREFIX):
                out_tag_types[
                    f"{name}{constants.BLOCK_SEPARATOR}{suffix}"
                ] = col_type
            elif name in self.by:
                out_tag_types[name] = col_type
            else:
                # Promoted to a list-valued data column.
                out_data_types[name] = list[col_type]
                if column_config.source:
                    # Promoted columns carry no provenance token; the stream
                    # fills in a scalar null.
                    out_data_types[f"{constants.SOURCE_PREFIX}{name}"] = str

        for name, col_type in data_types.items():
            out_data_types[name] = list[col_type]

        return Schema(out_tag_types), Schema(out_data_types)
```

- [ ] **Step 4: Export it**

In `src/orcapod/core/operators/__init__.py`, add the import alphabetically after `from .filters import PolarsFilter`:

```python
from .group_by import GroupBy
```

and add `"GroupBy",` to `__all__` after `"Batch",`.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
uv run pytest tests/test_core/operators/test_group_by.py -v
```

Expected: all PASS. If `test_row_order_does_not_affect_output` fails, the sort key is wrong — check that `sort_columns` excludes the `by` columns and that `members.sort` runs before the dict comprehension.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/operators/group_by.py src/orcapod/core/operators/__init__.py tests/test_core/operators/test_group_by.py
git commit -m "feat(operators): add GroupBy for many-to-one tag-keyed reduction (NPIPE-204)

Every other operator preserves one row per tag; GroupBy collapses N rows
sharing a tag tuple into one packet with list-valued members, which is what
lets a downstream pod receive a whole recording session at once.

Group keys stay scalar tags; non-key tags are promoted to list-valued data
columns rather than dropped, so consumers can tell which member each element
came from. Members sort by non-key tag values so the hashed lists are stable
across runs.

NPIPE-204"
```

---

## Task 6: Schema-mirroring test

**Files:**
- Test: `tests/test_core/operators/test_group_by.py`

`unary_output_schema` predicting something the table does not match is the classic operator bug — it surfaces far downstream, as a DB write failure rather than a schema error.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/operators/test_group_by.py`:

```python
class TestGroupBySchemaMirror:
    """unary_output_schema must match what the produced stream reports."""

    @pytest.mark.parametrize(
        "config",
        [
            {},
            {"source": True},
            {"system_tags": True},
            {"source": True, "system_tags": True},
        ],
        ids=["bare", "source", "system_tags", "source+system_tags"],
    )
    def test_predicted_schema_matches_produced_stream(self, session_source, config):
        op = GroupBy(by=["subject", "date"])

        predicted_tags, predicted_data = op.unary_output_schema(
            session_source, columns=config
        )
        produced = op.unary_static_process(session_source)
        actual_tags, actual_data = produced.output_schema(columns=config)

        assert dict(predicted_tags) == dict(actual_tags)
        assert dict(predicted_data) == dict(actual_data)
```

**Two comparisons that look right and are not.** Both were tried during review and both silently pass:

- **Against `op.process(...)`** — `DynamicPodStream.output_schema` (`static_output_pod.py:321-332`) delegates straight back to the pod, so this compares the prediction against itself and can never fail.
- **Against `as_table(columns=config).column_names`** — `output_schema` and `as_table` legitimately disagree. `ArrowTableStream.output_schema` (`arrow_table_stream.py:183-204`) returns `self._data_schema` unconditionally, so `columns.source` is a documented no-op there (`source_node.py:178-180` states this). `_source_*`, `_content_hash`, and `_context_key` appear in the table but never in the schema. The same gap exists on a plain un-batched `ArrowTableSource`, so it is a property of the schema layer, not of any operator.

Compare the prediction against the materialized `ArrowTableStream` from `unary_static_process`. That is the contract that actually binds.

- [ ] **Step 2: Run the test**

```bash
uv run pytest tests/test_core/operators/test_group_by.py::TestGroupBySchemaMirror -v
```

Expected: PASS if Task 5 was implemented correctly. If a case fails, the assertion message names the exact mismatched columns — fix `unary_output_schema` to match the table, not the other way round.

- [ ] **Step 3: Commit**

```bash
git add tests/test_core/operators/test_group_by.py
git commit -m "test(group_by): assert output_schema mirrors as_table (NPIPE-204)

NPIPE-204"
```

---

## Task 7: Registration — serialization and fluent API

**Files:**
- Modify: `src/orcapod/pipeline/serialization.py:160-192`
- Modify: `src/orcapod/core/streams/base.py:135-149`
- Test: `tests/test_core/operators/test_group_by.py`

Without the registry entry, a pipeline containing a `GroupBy` cannot be deserialized.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_core/operators/test_group_by.py`:

```python
class TestGroupByRegistration:
    def test_in_operator_registry(self):
        from orcapod.pipeline.serialization import _build_operator_registry

        assert _build_operator_registry()["GroupBy"] is GroupBy

    def test_stream_fluent_method(self, session_source):
        out = session_source.group_by(["subject", "date"])
        assert len(out.as_table()) == 2


class TestGroupByAsyncIsBarrier:
    """GroupBy must NOT override async_execute.

    ``UnaryOperator.async_execute`` (``core/operators/base.py:71``) already
    collects the full input before calling ``static_process``, which is exactly
    the barrier GroupBy needs: no group can be emitted before the input channel
    closes, because any row not yet seen could belong to a group already
    started.  Adding an override would duplicate that logic and risk drifting
    from it.
    """

    def test_does_not_override_async_execute(self):
        from orcapod.core.operators.base import UnaryOperator

        assert "async_execute" not in GroupBy.__dict__
        assert GroupBy.async_execute is UnaryOperator.async_execute
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_core/operators/test_group_by.py::TestGroupByRegistration -v
```

Expected: `test_in_operator_registry` FAILS with `KeyError: 'GroupBy'`; `test_stream_fluent_method` FAILS with `AttributeError`.

- [ ] **Step 3: Add the registry entry**

In `src/orcapod/pipeline/serialization.py`, inside `_build_operator_registry`, add `GroupBy` to the import list (alphabetically, after `DropTagColumns`):

```python
    from orcapod.core.operators import (
        Batch,
        DropDataColumns,
        DropTagColumns,
        GroupBy,
        Join,
        MapData,
        MapTags,
        MergeJoin,
        PolarsFilter,
        SelectDataColumns,
        SelectTagColumns,
        SemiJoin,
    )
```

and add the entry to the returned dict, after `"Batch": Batch,`:

```python
        "GroupBy": GroupBy,
```

- [ ] **Step 4: Add the fluent method**

In `src/orcapod/core/streams/base.py`, immediately after the existing `batch` method (ends line 149), add:

```python
    def group_by(
        self,
        by: Collection[str],
        label: str | None = None,
    ) -> StreamBase:
        """Reduce rows sharing a tag tuple into one packet per group.

        Group-key columns stay scalar tags; every other column becomes
        list-valued.  See ``orcapod.core.operators.GroupBy``.

        Args:
            by: Tag column names to group on.
            label: Optional node label for the pipeline graph.

        Returns:
            A stream with one row per distinct group-key tuple.
        """
        from orcapod.core.operators import GroupBy

        return GroupBy(by=by)(self, label=label)
```

`Collection` is already imported at the top of `base.py`. Verify with `grep -n "from collections.abc" src/orcapod/core/streams/base.py` and add it to that import if absent.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
uv run pytest tests/test_core/operators/test_group_by.py -v
uv run pytest tests/test_pipeline/test_serialization.py -q
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/serialization.py src/orcapod/core/streams/base.py tests/test_core/operators/test_group_by.py
git commit -m "feat(operators): register GroupBy in serialization and stream API (NPIPE-204)

Without the registry entry a pipeline containing a GroupBy cannot be
deserialized.

NPIPE-204"
```

---

## Task 8: Job-level tests

**Files:**
- Create: `tests/test_pipeline/test_aggregation_job.py`

This is the gap that let the provenance crash ship. Every existing `Batch` test stops at `op.process(stream)` + `as_table()`; the crash only happens inside `job.run()`, in `StaticOutputOperatorPod._materialize_to_stream`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_pipeline/test_aggregation_job.py`:

```python
"""Job-level tests for aggregating operators (NPIPE-204).

Operator-level tests (`op.process(stream)` then `as_table()`) never reach
`StaticOutputOperatorPod._materialize_to_stream`, which is where list-valued
provenance used to crash.  These run the full `job.run()` path against a real
Delta Lake store.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Batch, GroupBy, MergeJoin
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import PipelineJob


@pytest.fixture
def store(tmp_path: Path) -> DeltaTableDatabase:
    return DeltaTableDatabase(str(tmp_path / "store"))


@pytest.fixture
def session_source_factory():
    """Build a 2-group source; `paths` lets a test mutate one group's data."""

    def _make(paths: list[str] | None = None) -> ArrowTableSource:
        table = pa.table({
            "subject": ["G", "G", "G", "G"],
            "date": ["d1", "d1", "d2", "d2"],
            "probe": [0, 1, 0, 1],
            "path": paths or ["a", "b", "c", "d"],
        })
        return ArrowTableSource(
            table,
            tag_columns=["subject", "date", "probe"],
            infer_nullable=True,
        )

    return _make


class _CountingFunction:
    """Records every invocation so memoization can be asserted on.

    `PythonDataFunction` binds data columns to parameter names via
    `inspect.signature`, which resolves a callable instance to `__call__`.
    The parameter must therefore be named after the data column — hence the
    separate `_CountingV` below for MergeJoin's merged `v` column.
    """

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, path: list[str]) -> int:
        self.calls.append(list(path))
        return len(path)


class _CountingV:
    """Same as `_CountingFunction`, bound to a column named `v`."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, v: list[str]) -> int:
        self.calls.append(list(v))
        return len(v)


def _run(store, source, operator, fn, name):
    pod = FunctionPod(PythonDataFunction(fn, output_keys="n", function_name="count"))
    job = PipelineJob(name=name, store=store)
    with job:
        pod(operator(source, label="agg"), label="counter")
    return job.run()


class TestGroupByInJob:
    def test_group_by_completes(self, store, session_source_factory):
        fn = _CountingFunction()
        _run(store, session_source_factory(), GroupBy(by=["subject", "date"]), fn, "gb")
        assert len(fn.calls) == 2
        assert sorted(fn.calls) == [["a", "b"], ["c", "d"]]

    def test_batch_completes(self, store, session_source_factory):
        """The provenance fix is independent of grouping."""
        fn = _CountingFunction()
        _run(store, session_source_factory(), Batch(batch_size=2), fn, "b")
        assert len(fn.calls) == 2


class TestGroupByMemoization:
    def test_identical_runs_hit_cache(self, store, session_source_factory):
        fn = _CountingFunction()
        _run(store, session_source_factory(), GroupBy(by=["subject", "date"]), fn, "m")
        assert len(fn.calls) == 2

        fn2 = _CountingFunction()
        _run(store, session_source_factory(), GroupBy(by=["subject", "date"]), fn2, "m")
        assert fn2.calls == [], "second identical run must not recompute"

    def test_changed_member_invalidates_only_its_group(
        self, store, session_source_factory
    ):
        """Two groups; change one member of the first only.

        With a single group this assertion would be vacuous -- it must show
        that the untouched group stays cached.
        """
        fn = _CountingFunction()
        _run(store, session_source_factory(), GroupBy(by=["subject", "date"]), fn, "i")
        assert len(fn.calls) == 2

        fn2 = _CountingFunction()
        changed = session_source_factory(["a", "B_CHANGED", "c", "d"])
        _run(store, changed, GroupBy(by=["subject", "date"]), fn2, "i")

        assert fn2.calls == [["a", "B_CHANGED"]], (
            "only the changed group should recompute; "
            f"got {fn2.calls}"
        )


class TestMergeJoinRegression:
    def test_merge_join_completes_in_job(self, store):
        """MergeJoin carries source columns as parallel lists (merge_join.py:262).

        It crashed with the same ArrowTypeError before the Data fix.
        """
        left = ArrowTableSource(
            pa.table({"id": ["a", "b"], "v": ["l1", "l2"]}),
            tag_columns=["id"],
            infer_nullable=True,
        )
        right = ArrowTableSource(
            pa.table({"id": ["a", "b"], "v": ["r1", "r2"]}),
            tag_columns=["id"],
            infer_nullable=True,
        )

        fn = _CountingV()
        pod = FunctionPod(
            PythonDataFunction(fn, output_keys="n", function_name="count_v")
        )
        job = PipelineJob(name="mj", store=store)
        with job:
            pod(MergeJoin()(left, right, label="mj"), label="counter")
        job.run()

        assert len(fn.calls) == 2
        # MergeJoin merges colliding `v` columns into a sorted 2-element list.
        assert sorted(fn.calls) == [["l1", "r1"], ["l2", "r2"]]
```

- [ ] **Step 2: Run test to verify it fails, and how**

```bash
uv run pytest tests/test_pipeline/test_aggregation_job.py -v
```

Expected before Tasks 1–7: `ArrowTypeError: Expected bytes, got a 'list' object`. After Tasks 1–7: these should pass. If a memoization test fails, that is a real finding — report the actual `fn2.calls` value rather than relaxing the assertion.

- [ ] **Step 3: Fix any failures found**

If `test_changed_member_invalidates_only_its_group` shows *both* groups recomputing, the likely cause is that `fold_system_tag_values` is being fed members in a different order between runs — verify the sort in `unary_static_process` runs before the fold. If *neither* recomputes, the input data hash is not reaching the record preimage; inspect `_build_record_id_preimage` output for both runs.

- [ ] **Step 4: Run the full suite**

```bash
uv run pytest tests/ -q
```

Expected: no failures.

- [ ] **Step 5: Commit**

```bash
git add tests/test_pipeline/test_aggregation_job.py
git commit -m "test(pipeline): job-level coverage for GroupBy, Batch, MergeJoin (NPIPE-204)

Every existing aggregating-operator test stopped at op.process() + as_table(),
which never reaches _materialize_to_stream -- the reason the list-valued
provenance crash shipped. These run the full job.run() path against a Delta
Lake store and assert memoization holds across identical runs and invalidates
only the group whose member changed.

NPIPE-204"
```

---

## Task 9: Documentation

**Files:**
- Modify: `CLAUDE.md`
- Modify: `.zed/rules`
- Modify: `DESIGN_ISSUES.md`

Per `CLAUDE.md`, agent instructions must be updated in **both** `CLAUDE.md` and `.zed/rules`.

- [ ] **Step 1: Update the project layout tree**

In `CLAUDE.md`, in the `src/orcapod/core/operators/` block, add after the `batch.py` line:

```
│       ├── group_by.py         # GroupBy (many→one reduction keyed on tag values)
```

- [ ] **Step 2: Update the operator / function pod boundary table**

`GroupBy` synthesizes no new values but does change row count. Add a note under that table in `CLAUDE.md`:

```markdown
`GroupBy` is the only operator that changes row count in a many→one direction: it
reduces N rows sharing a tag tuple to one row with list-valued members. It still
synthesizes no new data values — every emitted element came from an input row.
```

- [ ] **Step 3: Update the system tag evolution rules**

In `CLAUDE.md`, the "System tag evolution rules" section currently has three rules, and rule 3 describes `Batch` as purely type-evolving (`str` → `list[str]`). That is now wrong. Replace rule 3 with:

```markdown
3. **Reducing** — many→one ops (`Batch`, `GroupBy`). User tag and data columns become
   `list[T]`; source-info columns become `list[str]`, one element per member. System tag
   columns must stay **scalar** (record identity hashes them directly), so they fold to a
   deterministic digest via `arrow_utils.fold_system_tag_values` and their column name gains
   `::{pipeline_hash}`. The fold is SHA-based and stable across processes — never use
   `hash()` there, since the digest becomes a cache key.
```

- [ ] **Step 4: Add the `Important implementation details` entries**

In `CLAUDE.md`, append to that list:

```markdown
- `GroupBy` requires every column in `by` to be a tag column; raises `InputValidationError`
  otherwise. Members are sorted by non-group-key tag values (falling back to system
  `record_id`) so the hashed lists are stable across runs.
- `Data` source-info values may be `str`, `None`, or `list[...]`. Types are derived from the
  value; `None` maps to `large_string`.
```

- [ ] **Step 5: Mirror every change into `.zed/rules`**

`.zed/rules` carries the same instructions for Zed AI and must stay in sync — this is
required by `CLAUDE.md` itself. Apply Steps 1–4 verbatim to `.zed/rules`, then confirm the
two files agree on this content:

```bash
diff CLAUDE.md .zed/rules
```

Expected: no differences in any section touched by Steps 1–4. If the files already diverge
elsewhere for unrelated reasons, leave those differences alone — only verify that every
line you added appears in both.

- [ ] **Step 6: Resolve the `tag_data.py` half of U1**

In `DESIGN_ISSUES.md`, update the U1 status line:

```markdown
**Status:** resolved (`tag_data.py` half), open (`arrow_utils.py` half)
```

and append to the `**Fix (NPIPE-204):**` paragraph:

```markdown
**Fix:** landed in NPIPE-204. `_source_info_arrow_type` / `_source_info_python_type` in
`core/datagrams/tag_data.py` derive the Arrow and Python types from the stored value.
The dead `polars_data_utils.add_source_info` was deleted. `add_source_info_to_table()`
in `arrow_utils.py` is untouched and remains open.
```

- [ ] **Step 7: Verify docs are consistent**

```bash
uv run pytest tests/ -q
grep -n "group_by.py" CLAUDE.md .zed/rules
grep -n "GroupBy" CLAUDE.md .zed/rules
```

Expected: suite passes; both files mention `group_by.py` and `GroupBy`.

- [ ] **Step 8: Commit**

```bash
git add CLAUDE.md .zed/rules DESIGN_ISSUES.md
git commit -m "docs: document GroupBy and the reducing system-tag rule (NPIPE-204)

Rule 3 described Batch as purely type-evolving, which was only ever true of
user tag and data columns -- system tags fold to a scalar and extend their
name. Renames the rule to 'reducing' and covers both operators.

Resolves the tag_data.py half of DESIGN_ISSUES U1.
NPIPE-204"
```

---

## Task 10: Final verification and PR

**Files:** none

- [ ] **Step 1: Full suite**

```bash
uv run pytest tests/ -q
```

Expected: all pass. Record the exact pass/fail counts — do not claim success without reading the output.

- [ ] **Step 2: Confirm both original crashes are gone**

```bash
uv run pytest tests/test_pipeline/test_aggregation_job.py -v
```

Expected: `TestGroupByInJob`, `TestGroupByMemoization`, and `TestMergeJoinRegression` all pass.

- [ ] **Step 3: Review the whole diff**

```bash
git diff main...HEAD --stat
git diff main...HEAD
```

Check: no debug prints, no `sys.modules` manipulation, no backward-compat shims (all forbidden by `CLAUDE.md`), Google-style docstrings with no ReST roles.

- [ ] **Step 4: Push and open the PR**

```bash
git push -u origin arnoldb/npipe-204-orcapod-add-tag-based-grouping-to-batch-manyone-reduction
```

PR body must include `Fixes NPIPE-204` so Linear's GitHub integration links it, and must call out the API deviation:

```markdown
## Summary

Adds a `GroupBy` operator for many→one reduction keyed on tag values, and fixes the
`Data` datagram so list-valued provenance no longer crashes `job.run()`.

**API note:** NPIPE-204 specifies `Batch(group_by=[...])`. This PR adds a separate
`GroupBy(by=[...])` instead, so each operator keeps one output contract and `Batch`'s
streaming `async_execute` path stays untouched. See
`superpowers/specs/2026-08-07-npipe-204-batch-group-by-design.md`.

The provenance fix also repairs `MergeJoin`, which produced list-valued `_source_*`
columns (`merge_join.py:262`) and was silently broken inside `job.run()`.

Fixes NPIPE-204

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

- [ ] **Step 5: Update the Linear issue**

NPIPE-204's "What to build" section and the `common_clock_op` wiring example both still say `Batch(group_by=...)`. Update them to `GroupBy(by=...)` so the downstream orcapod-sync-and-qc change is written against the real API.

**Confirm with the user before editing the Linear issue** — it is an outward-facing change to a shared record.

---

## Downstream (not part of this plan)

The rev bump must land in **both** orcapod-sync-and-qc and orcapod-spikesorting together — they are deliberately kept on the same orcapod rev because they target the same Ray cluster. `common_clock_op` wiring becomes:

```python
grouped = op.operators.GroupBy(by=["subject", "date"]).process(sync_out)
common_clock_op.pod(grouped)
```
