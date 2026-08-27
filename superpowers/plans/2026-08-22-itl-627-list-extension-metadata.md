# ITL-627: List-Extension Metadata — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three defects where `ListLogicalType`-backed extension columns are mishandled by Join, the semantic hashing visitor, and MergeJoin.

**Architecture:** Three independent fixes in three files (`list_logical_type_factory.py`, `visitors.py`, `merge_join.py`) plus test coverage in a new `tests/test_logical_types/` directory and additions to existing test files. Each task is independently committable.

**Tech Stack:** PyArrow extension types, Polars C data interface, Python `typing` module generics, `get_default_context()` for type converter access.

**Spec:** `superpowers/specs/2026-08-22-itl-627-list-extension-metadata.md`

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `tests/test_logical_types/__init__.py` | Create | Make new package importable |
| `tests/test_logical_types/test_list_logical_type.py` | Create | Unit regression tests for Fix 1 |
| `src/orcapod/logical_types/list_logical_type_factory.py` | Modify line 151 | Fix 1: pass `metadata=` |
| `tests/test_core/operators/test_operators.py` | Modify | Integration test for Fix 1 via Join |
| `tests/test_core/operators/test_merge_join.py` | Modify | Integration test for Fix 1 via MergeJoin + Fix 3 tests |
| `src/orcapod/hashing/visitors.py` | Modify `visit_extension` | Fix 2: delegate list-backed extension to `_visit_list_elements` |
| `tests/test_hashing/test_extension_type_hashing.py` | Modify | Fix 2 regression + symmetry tests |
| `src/orcapod/core/operators/merge_join.py` | Modify `binary_static_process` | Fix 3: produce extension-typed merged arrays |

---

## Task 1: Fix 1 — pass metadata to `make_polars_extension_type`

**Files:**
- Create: `tests/test_logical_types/__init__.py`
- Create: `tests/test_logical_types/test_list_logical_type.py`
- Modify: `src/orcapod/logical_types/list_logical_type_factory.py:149-155`

- [ ] **Step 1.1: Create the new test package**

```bash
touch tests/test_logical_types/__init__.py
```

- [ ] **Step 1.2: Write the failing unit tests**

Create `tests/test_logical_types/test_list_logical_type.py`:

```python
"""Regression tests for ListLogicalType Polars extension type metadata.

Defect 1 (ITL-627): get_polars_extension_type() was called without passing
metadata= to make_polars_extension_type, so ext_metadata() returned None.
Polars exported b'' on to_arrow(), which _deserialize rejected with ValueError.
"""

from __future__ import annotations

import json

import pytest

from orcapod.logical_types.builtin_logical_types import LogicalPath
from orcapod.logical_types.list_logical_type_factory import ListLogicalType


class TestListLogicalTypePolarsMetadata:
    def test_list_polars_ext_carries_metadata(self):
        """get_polars_extension_type() for list[Path] must carry JSON metadata.

        With the buggy code (metadata= not passed), ext_metadata() returns None.
        Polars then exports b'' on to_arrow(), causing _deserialize to raise
        ValueError because b'' != the expected JSON bytes.
        """
        lt = ListLogicalType(LogicalPath(), is_set=False)
        polars_ext = lt.get_polars_extension_type()

        meta = polars_ext.ext_metadata()
        assert meta is not None, (
            "ext_metadata() returned None — metadata= was not passed to "
            "make_polars_extension_type. This causes the Polars→Arrow round-trip to fail."
        )
        parsed = json.loads(meta)
        assert parsed["category"] == "list"
        assert parsed["element_ext_name"] == "orcapod.path"

    def test_set_polars_ext_carries_metadata(self):
        """Same one-line fix covers set[T] (identical code path, is_set=True)."""
        lt = ListLogicalType(LogicalPath(), is_set=True)
        polars_ext = lt.get_polars_extension_type()

        meta = polars_ext.ext_metadata()
        assert meta is not None
        parsed = json.loads(meta)
        assert parsed["category"] == "set"
        assert parsed["element_ext_name"] == "orcapod.path"

    def test_polars_to_arrow_round_trip_preserves_extension_type(self):
        """Full Polars round-trip must not raise and must preserve the extension type.

        pl.DataFrame(table).to_arrow() calls _deserialize; without the fix it
        receives b'' and raises ValueError.
        """
        import pyarrow as pa
        import polars as pl

        lt = ListLogicalType(LogicalPath(), is_set=False)
        ext_type = lt.get_arrow_extension_type()

        storage = pa.array([["/a.txt", "/b.txt"]], type=pa.large_list(pa.large_string()))
        ext_array = pa.ExtensionArray.from_storage(ext_type, storage)
        table = pa.table({"paths": ext_array})

        # Must not raise ValueError from _deserialize
        result = pl.DataFrame(table).to_arrow()

        assert isinstance(result.schema.field("paths").type, pa.ExtensionType)
        assert result.schema.field("paths").type.extension_name == "list[orcapod.path]"
```

- [ ] **Step 1.3: Run to verify tests fail**

```bash
uv run pytest tests/test_logical_types/test_list_logical_type.py -v
```

Expected: 3 FAILED — `ext_metadata()` returns `None`, round-trip raises `ValueError`.

- [ ] **Step 1.4: Apply Fix 1**

In `src/orcapod/logical_types/list_logical_type_factory.py`, modify `get_polars_extension_type` (lines 149–155):

```python
    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for this list/set logical type.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under the logical
            type name.
        """
        if self._polars_ext is None:
            polars_ext_class = make_polars_extension_type(
                self._logical_type_name,
                self._storage_type,
                metadata=self._metadata_bytes.decode("utf-8"),
            )
            self._polars_ext = polars_ext_class()
        return self._polars_ext
```

The only change is adding `metadata=self._metadata_bytes.decode("utf-8")` on the line after `self._storage_type,`.

- [ ] **Step 1.5: Run to verify tests pass**

```bash
uv run pytest tests/test_logical_types/test_list_logical_type.py -v
```

Expected: 3 PASSED.

- [ ] **Step 1.6: Commit**

```bash
git add tests/test_logical_types/__init__.py \
        tests/test_logical_types/test_list_logical_type.py \
        src/orcapod/logical_types/list_logical_type_factory.py
git commit -m "fix(logical_types): pass metadata to make_polars_extension_type in ListLogicalType

ListLogicalType.get_polars_extension_type() was not passing the JSON metadata
bytes to make_polars_extension_type, so ext_metadata() returned None. Polars
exported b'' on to_arrow(), which _deserialize rejected with ValueError during
the Join/MergeJoin Polars round-trip. One-line fix covers both list[T] and set[T].

Fixes ITL-627 (Defect 1)."
```

---

## Task 2: Defect 1 integration tests — Join and MergeJoin preserve list extension column

**Files:**
- Modify: `tests/test_core/operators/test_operators.py`
- Modify: `tests/test_core/operators/test_merge_join.py`

- [ ] **Step 2.1: Add the Join integration test**

In `tests/test_core/operators/test_operators.py`, add a new test class after `TestJoinBehavior`:

```python
class TestJoinWithListExtensionColumn:
    """Regression tests for ITL-627 Defect 1: Join Polars round-trip with list extension columns."""

    def test_join_preserves_list_extension_column(self):
        """Join must not raise and must preserve extension<list[orcapod.path]>.

        Before Fix 1, df.to_arrow() inside static_process called _deserialize
        with b'' (no metadata), raising ValueError.
        """
        import pyarrow as pa
        from orcapod.logical_types.builtin_logical_types import LogicalPath
        from orcapod.logical_types.list_logical_type_factory import ListLogicalType

        lt = ListLogicalType(LogicalPath(), is_set=False)
        ext_type = lt.get_arrow_extension_type()

        storage = pa.array(
            [["/a.txt", "/b.txt"], ["/c.txt"]],
            type=pa.large_list(pa.large_string()),
        )
        ext_array = pa.ExtensionArray.from_storage(ext_type, storage)
        left_table = pa.table({
            "animal": pa.array(["cat", "dog"], type=pa.large_string()),
            "paths": ext_array,
        })
        left_stream = ArrowTableStream(left_table, tag_columns=["animal"])

        right_table = pa.table({
            "animal": pa.array(["cat", "dog"], type=pa.large_string()),
            "speed": pa.array([30.0, 45.0], type=pa.float64()),
        })
        right_stream = ArrowTableStream(right_table, tag_columns=["animal"])

        op = Join()
        result = op.static_process(left_stream, right_stream)  # must not raise
        out_table = result.as_table()

        paths_type = out_table.schema.field("paths").type
        assert isinstance(paths_type, pa.ExtensionType), (
            f"'paths' column must remain an extension type, got {paths_type}"
        )
        assert paths_type.extension_name == "list[orcapod.path]"
```

- [ ] **Step 2.2: Add the MergeJoin round-trip test (non-colliding list column)**

In `tests/test_core/operators/test_merge_join.py`, add a new test class:

```python
class TestMergeJoinWithListExtensionColumn:
    """Regression for ITL-627 Defect 1: MergeJoin Polars round-trip with list extension columns."""

    def test_merge_join_preserves_non_colliding_list_extension_column(self):
        """MergeJoin must not raise and must preserve extension<list[orcapod.path]>
        for a non-colliding list[Path] data column.

        MergeJoin also does a Polars round-trip; without Fix 1 it raises ValueError.
        """
        import pyarrow as pa
        from orcapod.logical_types.builtin_logical_types import LogicalPath
        from orcapod.logical_types.list_logical_type_factory import ListLogicalType

        lt = ListLogicalType(LogicalPath(), is_set=False)
        ext_type = lt.get_arrow_extension_type()

        storage = pa.array(
            [["/a.txt", "/b.txt"], ["/c.txt"]],
            type=pa.large_list(pa.large_string()),
        )
        ext_array = pa.ExtensionArray.from_storage(ext_type, storage)

        # Left stream: list[Path] data column (non-colliding) + shared tag
        left_table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "paths": ext_array,
        })
        left_stream = ArrowTableStream(left_table, tag_columns=["id"])

        # Right stream: different non-colliding data column + same tag
        right_table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "score": pa.array([10.0, 20.0], type=pa.float64()),
        })
        right_stream = ArrowTableStream(right_table, tag_columns=["id"])

        result = MergeJoin().static_process(left_stream, right_stream)  # must not raise
        out_table = result.as_table()

        paths_type = out_table.schema.field("paths").type
        assert isinstance(paths_type, pa.ExtensionType), (
            f"'paths' column must remain an extension type, got {paths_type}"
        )
        assert paths_type.extension_name == "list[orcapod.path]"
```

- [ ] **Step 2.3: Run both new tests to verify they pass (Fix 1 already applied)**

```bash
uv run pytest tests/test_core/operators/test_operators.py::TestJoinWithListExtensionColumn \
             tests/test_core/operators/test_merge_join.py::TestMergeJoinWithListExtensionColumn \
             -v
```

Expected: 2 PASSED (Fix 1 from Task 1 already resolves these).

- [ ] **Step 2.4: Commit**

```bash
git add tests/test_core/operators/test_operators.py \
        tests/test_core/operators/test_merge_join.py
git commit -m "test(operators): add regression tests for list extension column round-trip

Exercises the full Join and MergeJoin Polars round-trip with list[Path] extension
columns, confirming Fix 1 (ITL-627 Defect 1) at the operator integration level."
```

---

## Task 3: Fix 2 — semantic hashing for list-backed extension columns

**Files:**
- Modify: `src/orcapod/hashing/visitors.py` — `SemanticHashingVisitor.visit_extension`
- Modify: `tests/test_hashing/test_extension_type_hashing.py`

- [ ] **Step 3.1: Write the failing hashing tests**

Add a new test class `TestListExtensionHashing` to `tests/test_hashing/test_extension_type_hashing.py`:

```python
class TestListExtensionHashing:
    """Regression and contract tests for ITL-627 Defect 2.

    Before Fix 2, visit_extension short-circuited on `not isinstance(python_type, type)`
    for list[File] (a GenericAlias), returning the extension type unchanged — raw JSON
    path strings were hashed instead of file contents.
    """

    def _make_list_file_ext_type(self, ctx):
        """Return the extension<list[orcapod.file]> Arrow type via the type converter."""
        from orcapod.logical_types.file_type import File
        ctx.type_converter.register_python_class(list[File])
        return ctx.type_converter.python_type_to_arrow_type(list[File])

    def _make_scalar_file_ext_type(self, ctx):
        """Return the extension<orcapod.file> Arrow type."""
        from orcapod.logical_types.file_type import File
        return ctx.type_converter.register_python_class(File)

    def _file_storage(self, ctx, path):
        """Return the large_string storage value for a File."""
        from orcapod.logical_types.file_type import File
        return ctx.type_converter.python_to_storage(File(path), File)

    def test_list_file_extension_hashed_to_list_of_large_binary(self, ctx, tmp_path):
        """visit_extension for extension<list[orcapod.file]> must return
        (large_list(large_binary), [bytes, bytes]) — not the extension type unchanged.

        With the buggy code the isinstance guard exits immediately, returning
        (extension_type, storage_value). This assertion on new_type would fail.
        """
        f0 = tmp_path / "f0.txt"; f0.write_text("alpha")
        f1 = tmp_path / "f1.txt"; f1.write_text("beta")

        list_ext_type = self._make_list_file_ext_type(ctx)
        s0 = self._file_storage(ctx, f0)
        s1 = self._file_storage(ctx, f1)
        storage_value = [s0, s1]

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(list_ext_type, storage_value)

        import pyarrow as pa
        assert new_type == pa.large_list(pa.large_binary()), (
            f"Expected large_list(large_binary), got {new_type}. "
            "Buggy code returns the extension type unchanged."
        )
        assert isinstance(new_data, list)
        assert len(new_data) == 2
        assert all(isinstance(b, bytes) for b in new_data)

    def test_list_file_extension_is_hash_of_file_content_hashes(self, ctx, tmp_path):
        """Each element of the list result equals the scalar visit result for the same file.

        Contract: visit(list_ext, [s0, s1])[1][i] == visit(scalar_ext, si)[1]
        This is the symmetry invariant — list[File] and scalar File hash identically
        per element. Starfix then sees an ordered list of content-hash tokens.
        """
        f0 = tmp_path / "contract0.txt"; f0.write_text("content zero")
        f1 = tmp_path / "contract1.txt"; f1.write_text("content one")

        scalar_ext_type = self._make_scalar_file_ext_type(ctx)
        list_ext_type = self._make_list_file_ext_type(ctx)
        s0 = self._file_storage(ctx, f0)
        s1 = self._file_storage(ctx, f1)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)

        # Scalar hashes
        _, h0_bytes = visitor.visit(scalar_ext_type, s0)
        _, h1_bytes = visitor.visit(scalar_ext_type, s1)

        # List hash
        _, list_result = visitor.visit(list_ext_type, [s0, s1])

        assert list_result[0] == h0_bytes, (
            "Element 0 of list result must equal the scalar hash of file 0"
        )
        assert list_result[1] == h1_bytes, (
            "Element 1 of list result must equal the scalar hash of file 1"
        )

    def test_list_file_extension_content_change_changes_hash(self, ctx, tmp_path):
        """Changing file content changes the per-element hash."""
        f = tmp_path / "mutable.txt"
        f.write_text("v1")

        list_ext_type = self._make_list_file_ext_type(ctx)
        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)

        s_v1 = self._file_storage(ctx, f)
        _, result_v1 = visitor.visit(list_ext_type, [s_v1])
        r0_v1 = result_v1[0]

        f.write_text("v2")
        from orcapod.logical_types.file_type import File
        s_v2 = ctx.type_converter.python_to_storage(File(f), File)
        _, result_v2 = visitor.visit(list_ext_type, [s_v2])
        r0_v2 = result_v2[0]

        assert r0_v1 != r0_v2, "Content change must change the per-element hash"

    def test_list_file_extension_same_content_same_hash(self, ctx, tmp_path):
        """Two files with identical content produce identical per-element hashes."""
        fa = tmp_path / "a.txt"; fa.write_text("identical")
        fb = tmp_path / "b.txt"; fb.write_text("identical")

        list_ext_type = self._make_list_file_ext_type(ctx)
        sa = self._file_storage(ctx, fa)
        sb = self._file_storage(ctx, fb)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, result_a = visitor.visit(list_ext_type, [sa])
        _, result_b = visitor.visit(list_ext_type, [sb])

        assert result_a[0] == result_b[0], (
            "Same content at different paths must produce the same hash"
        )

    def test_list_file_extension_passthrough_when_no_handler(self, ctx, tmp_path):
        """When the registry has no FileHandler, visit_extension must passthrough."""
        from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
        from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher

        empty_registry = PythonTypeHandlerRegistry()
        stripped_hasher = SemanticAwarePythonHasher(
            hasher_id="test_v0",
            type_handler_registry=empty_registry,
        )

        f = tmp_path / "f.txt"; f.write_text("test")
        list_ext_type = self._make_list_file_ext_type(ctx)
        storage_value = [self._file_storage(ctx, f)]

        visitor = SemanticHashingVisitor(ctx.type_converter, stripped_hasher)
        new_type, new_data = visitor.visit(list_ext_type, storage_value)

        assert new_type == list_ext_type, "Must passthrough extension type when no handler"
        assert new_data == storage_value, "Must passthrough storage value when no handler"

    def test_list_path_extension_passthrough(self, ctx, tmp_path):
        """extension<list[orcapod.path]> must passthrough — Path has no content handler."""
        from orcapod.logical_types.builtin_logical_types import LogicalPath
        from orcapod.logical_types.list_logical_type_factory import ListLogicalType
        import pyarrow as pa
        from pathlib import Path

        lt = ListLogicalType(LogicalPath(), is_set=False)
        list_ext_type = lt.get_arrow_extension_type()
        storage_value = ["/a.txt", "/b.txt"]

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(list_ext_type, storage_value)

        assert new_type == list_ext_type, "Path has no handler — must passthrough"
        assert new_data == storage_value

    def test_set_file_extension_hashed_to_list_of_large_binary(self, ctx, tmp_path):
        """set[File] (extension<set[orcapod.file]>) also hashes per element.

        get_origin(set[File]) is `set`, covered by `in (list, set)` in Fix 2.
        """
        from orcapod.logical_types.file_type import File
        from orcapod.logical_types.list_logical_type_factory import ListLogicalType
        from orcapod.logical_types.file_type import LogicalFile
        import pyarrow as pa

        f0 = tmp_path / "s0.txt"; f0.write_text("set alpha")
        f1 = tmp_path / "s1.txt"; f1.write_text("set beta")

        lt = ListLogicalType(LogicalFile(), is_set=True)
        set_ext_type = lt.get_arrow_extension_type()
        s0 = self._file_storage(ctx, f0)
        s1 = self._file_storage(ctx, f1)
        storage_value = [s0, s1]

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(set_ext_type, storage_value)

        assert new_type == pa.large_list(pa.large_binary())
        assert isinstance(new_data, list)
        assert len(new_data) == 2
        assert all(isinstance(b, bytes) for b in new_data)

    def test_list_list_file_extension_hashed_recursively(self, ctx, tmp_path):
        """extension<list[list[orcapod.file]]> recurses: each inner list becomes large_list(large_binary).

        Fix 2 recurses naturally: outer visit_extension delegates to _visit_list_elements
        with virtual_type=large_list(extension<list[orcapod.file]>), which calls
        visit(extension<list[orcapod.file]>, inner_list) for each element, which
        recurses back into visit_extension.
        """
        from orcapod.logical_types.file_type import File, LogicalFile
        from orcapod.logical_types.list_logical_type_factory import ListLogicalType
        import pyarrow as pa

        f0 = tmp_path / "n0.txt"; f0.write_text("nested zero")
        f1 = tmp_path / "n1.txt"; f1.write_text("nested one")
        f2 = tmp_path / "n2.txt"; f2.write_text("nested two")

        inner_lt = ListLogicalType(LogicalFile(), is_set=False)
        outer_lt = ListLogicalType(inner_lt, is_set=False)
        outer_ext_type = outer_lt.get_arrow_extension_type()

        s0 = self._file_storage(ctx, f0)
        s1 = self._file_storage(ctx, f1)
        s2 = self._file_storage(ctx, f2)
        # One row: [[s0, s1], [s2]]
        storage_value = [[s0, s1], [s2]]

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(outer_ext_type, storage_value)

        assert new_type == pa.large_list(pa.large_list(pa.large_binary())), (
            f"Expected large_list(large_list(large_binary)), got {new_type}"
        )
        assert len(new_data) == 2
        assert len(new_data[0]) == 2  # two files in first inner list
        assert len(new_data[1]) == 1  # one file in second inner list
        assert all(isinstance(b, bytes) for b in new_data[0])
        assert all(isinstance(b, bytes) for b in new_data[1])

    def test_dataclass_with_list_file_field_hashed(self, ctx, tmp_path):
        """A Dataclass stored as a struct whose field is list[File] must hash per element.

        visit_struct recurses into fields via visit(field_type, field_data).
        The list[File] field has type extension<list[orcapod.file]>, so visit
        dispatches to visit_extension, which Fix 2 handles correctly.
        """
        from dataclasses import dataclass
        from orcapod.logical_types.file_type import File
        import pyarrow as pa

        @dataclass
        class FileBundle:
            name: str
            files: list[File]

        # Register the dataclass with the type converter
        ctx.type_converter.register_python_class(FileBundle)
        arrow_type = ctx.type_converter.python_type_to_arrow_type(FileBundle)

        f0 = tmp_path / "dc0.txt"; f0.write_text("dc alpha")
        f1 = tmp_path / "dc1.txt"; f1.write_text("dc beta")

        s0 = ctx.type_converter.python_to_storage(File(f0), File)
        s1 = ctx.type_converter.python_to_storage(File(f1), File)
        storage_value = {"name": "bundle", "files": [s0, s1]}

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(arrow_type, storage_value)

        # The `files` field should be hashed to large_list(large_binary)
        files_field_type = new_type.field("files").type
        assert files_field_type == pa.large_list(pa.large_binary()), (
            f"files field must be large_list(large_binary), got {files_field_type}"
        )
        files_hashes = new_data["files"]
        assert len(files_hashes) == 2
        assert all(isinstance(b, bytes) for b in files_hashes)
```

- [ ] **Step 3.2: Run to verify all new tests fail**

```bash
uv run pytest tests/test_hashing/test_extension_type_hashing.py::TestListExtensionHashing -v
```

Expected: All FAILED — the `isinstance(python_type, type)` guard short-circuits for `list[File]`.

- [ ] **Step 3.3: Apply Fix 2**

In `src/orcapod/hashing/visitors.py`, replace the full `visit_extension` method of `SemanticHashingVisitor` (lines 196–231):

```python
    def visit_extension(
        self,
        extension_type: "pa.ExtensionType",
        storage_value: Any,
    ) -> tuple["pa.DataType", Any]:
        """Hash an extension type value to pa.large_binary(), or passthrough.

        For list-backed extension types (e.g. ``extension<list[orcapod.file]>``),
        delegates to ``_visit_list_elements`` with a virtual
        ``large_list(elem_ext_type)`` so that each element is hashed identically
        to the scalar ``visit_extension`` path. This covers ``list[T]``,
        ``set[T]``, and arbitrary nesting depth via recursion.
        """
        if storage_value is None:
            return extension_type, None

        # Resolve extension type → Python type.
        python_type = self._type_converter.arrow_type_to_python_type(extension_type)

        # Detect list-backed extension types: extension<list[orcapod.file]>,
        # extension<set[orcapod.file]>, etc.  list[File] is a types.GenericAlias
        # (not isinstance(..., type)), so the guard below would incorrectly skip it.
        # We intercept here and delegate to _visit_list_elements with a virtual
        # large_list(elem_ext_type) so each element goes through visit_extension.
        if (
            typing.get_origin(python_type) in (list, set)
            and pa.types.is_large_list(extension_type.storage_type)
        ):
            args = typing.get_args(python_type)
            if args:
                elem_python_type = args[0]
                if (
                    isinstance(elem_python_type, type)
                    and self._python_hasher.type_handler_registry.has_handler(
                        elem_python_type
                    )
                ):
                    elem_arrow_type = self._type_converter.python_type_to_arrow_type(
                        elem_python_type
                    )
                    virtual_list_type = pa.large_list(elem_arrow_type)
                    return self._visit_list_elements(virtual_list_type, storage_value)

        # If the converter couldn't resolve to a concrete class, passthrough.
        if python_type is typing.Any or not isinstance(python_type, type):
            return extension_type, storage_value

        # Only hash if a semantic hasher is registered for this Python type.
        if not self._python_hasher.type_handler_registry.has_handler(python_type):
            return extension_type, storage_value

        # Convert storage value → Python object and hash it.
        python_obj = self._type_converter.storage_to_python(storage_value, python_type)
        content_hash = self._python_hasher.hash_object(python_obj)

        # Encode as binary: "<type_name>::<method>:<digest>"
        # Dots in the extension name → colons (e.g. "orcapod.path" → "orcapod:path").
        # The "::" separator is unambiguous because to_prefixed_digest() uses only ":".
        type_name = extension_type.extension_name.replace(".", ":")
        hash_bytes = (
            type_name.encode("utf-8")
            + b"::"
            + content_hash.to_prefixed_digest()
        )
        return pa.large_binary(), hash_bytes
```

- [ ] **Step 3.4: Run to verify new tests pass**

```bash
uv run pytest tests/test_hashing/test_extension_type_hashing.py -v
```

Expected: All PASSED (new tests + pre-existing tests).

- [ ] **Step 3.5: Commit**

```bash
git add src/orcapod/hashing/visitors.py \
        tests/test_hashing/test_extension_type_hashing.py
git commit -m "fix(hashing): hash list-backed extension columns element-by-element

SemanticHashingVisitor.visit_extension short-circuited on
\`not isinstance(python_type, type)\` for list[File] (a GenericAlias), returning
the extension type unchanged. Raw JSON path strings were hashed instead of file
contents. Fix detects list/set-backed extension types and delegates to
_visit_list_elements with a virtual large_list(elem_ext_type), producing
per-element content hashes identical to the scalar visit path. Recurses
naturally for nested list[list[T]].

Fixes ITL-627 (Defect 2)."
```

---

## Task 4: Fix 3 — MergeJoin produces extension-typed merged arrays

**Files:**
- Modify: `src/orcapod/core/operators/merge_join.py` — `binary_static_process`
- Modify: `tests/test_core/operators/test_merge_join.py`

- [ ] **Step 4.1: Write the failing MergeJoin tests**

Add `TestMergeJoinLogicalTypeColumns` to `tests/test_core/operators/test_merge_join.py`:

```python
class TestMergeJoinLogicalTypeColumns:
    """Regression tests for ITL-627 Defect 3.

    Before Fix 3, pa.array(merged_vals) inferred the array type from raw storage
    values, producing plain large_list(storage_type) — the extension wrapper was lost.
    """

    def test_merge_join_scalar_logical_type_column_yields_list_extension(self, tmp_path):
        """Merging a File column must produce extension<list[orcapod.file]>, not large_list.

        Before Fix 3: pa.array([[json1, json2]]) inferred large_list(large_string).
        After Fix 3: pa.ExtensionArray.from_storage(list_file_ext, ...) gives the correct type.
        """
        import pyarrow as pa
        from orcapod.logical_types.file_type import File, LogicalFile
        from orcapod.contexts import get_default_context

        f1 = tmp_path / "mj1.txt"; f1.write_text("merge left")
        f2 = tmp_path / "mj2.txt"; f2.write_text("merge right")

        ctx = get_default_context()
        scalar_lt = LogicalFile()
        ext_type = scalar_lt.get_arrow_extension_type()

        s1 = ctx.type_converter.python_to_storage(File(f1), File)
        s2 = ctx.type_converter.python_to_storage(File(f2), File)

        left_table = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "file": pa.ExtensionArray.from_storage(
                ext_type, pa.array([s1], type=pa.large_string())
            ),
        })
        right_table = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "file": pa.ExtensionArray.from_storage(
                ext_type, pa.array([s2], type=pa.large_string())
            ),
        })
        left_stream = ArrowTableStream(left_table, tag_columns=["id"])
        right_stream = ArrowTableStream(right_table, tag_columns=["id"])

        result = MergeJoin().static_process(left_stream, right_stream)
        out_table = result.as_table()

        file_type = out_table.schema.field("file").type
        assert isinstance(file_type, pa.ExtensionType), (
            f"'file' column must be extension<list[orcapod.file]>, got {file_type}. "
            "Buggy code produces plain large_list(large_string)."
        )
        assert file_type.extension_name == "list[orcapod.file]"

        # Values must be a list of two storage values
        file_values = out_table.column("file").to_pylist()
        assert len(file_values) == 1  # one row
        assert len(file_values[0]) == 2  # two merged elements

    def test_merge_join_list_backed_column_yields_nested_list_extension(self, tmp_path):
        """Merging a list[File] column must produce extension<list[list[orcapod.file]]>.

        Fix 3 handles this naturally: elem_python_type = list[File],
        get_logical_type_for_python_type(list[list[File]]) = ListLogicalType(ListLogicalType(LogicalFile())).
        """
        import pyarrow as pa
        from orcapod.logical_types.file_type import File, LogicalFile
        from orcapod.logical_types.list_logical_type_factory import ListLogicalType
        from orcapod.contexts import get_default_context

        f1 = tmp_path / "nl1.txt"; f1.write_text("nested left")
        f2 = tmp_path / "nl2.txt"; f2.write_text("nested right")

        ctx = get_default_context()
        inner_lt = ListLogicalType(LogicalFile(), is_set=False)
        inner_ext_type = inner_lt.get_arrow_extension_type()

        s1 = ctx.type_converter.python_to_storage(File(f1), File)
        s2 = ctx.type_converter.python_to_storage(File(f2), File)

        # Each row's "files" value is a list of one file-storage-value
        left_storage = pa.array([[s1]], type=pa.large_list(pa.large_string()))
        right_storage = pa.array([[s2]], type=pa.large_list(pa.large_string()))

        left_table = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "files": pa.ExtensionArray.from_storage(inner_ext_type, left_storage),
        })
        right_table = pa.table({
            "id": pa.array([1], type=pa.int64()),
            "files": pa.ExtensionArray.from_storage(inner_ext_type, right_storage),
        })
        left_stream = ArrowTableStream(left_table, tag_columns=["id"])
        right_stream = ArrowTableStream(right_table, tag_columns=["id"])

        result = MergeJoin().static_process(left_stream, right_stream)
        out_table = result.as_table()

        files_type = out_table.schema.field("files").type
        assert isinstance(files_type, pa.ExtensionType), (
            f"'files' column must be extension<list[list[orcapod.file]]>, got {files_type}"
        )
        assert files_type.extension_name == "list[list[orcapod.file]]"

        # One row with two inner lists
        files_values = out_table.column("files").to_pylist()
        assert len(files_values) == 1
        assert len(files_values[0]) == 2
```

- [ ] **Step 4.2: Run to verify tests fail**

```bash
uv run pytest tests/test_core/operators/test_merge_join.py::TestMergeJoinLogicalTypeColumns -v
```

Expected: 2 FAILED — column type is plain `large_list(large_string)`, not the extension type.

- [ ] **Step 4.3: Apply Fix 3**

In `src/orcapod/core/operators/merge_join.py`, make two changes to `binary_static_process`:

**Change A** — add the colliding column type snapshot just before the Polars join (after the `output_nullable` block, around line 206). Insert before the `COMMON_JOIN_KEY` block:

```python
        # Snapshot Arrow types of colliding columns BEFORE the Polars round-trip.
        # The round-trip may strip or alter extension metadata; we need the original
        # element type to reconstruct the correct list extension type after merging.
        colliding_col_types: dict[str, "pa.DataType"] = {
            col: left_table.schema.field(col).type
            for col in colliding_keys
            if col in left_table.schema.names
        }
```

**Change B** — replace the `merged_array = pa.array(merged_vals)` line (around line 276) with:

```python
            elem_arrow_type = colliding_col_types.get(col)
            if elem_arrow_type is not None and isinstance(elem_arrow_type, pa.ExtensionType):
                from orcapod.contexts import get_default_context
                tc = get_default_context().type_converter
                elem_python_type = tc.arrow_type_to_python_type(elem_arrow_type)
                list_lt = tc.get_logical_type_for_python_type(list[elem_python_type])
                if list_lt is not None:
                    list_ext_type = list_lt.get_arrow_extension_type()
                    storage_array = pa.array(merged_vals, type=list_ext_type.storage_type)
                    merged_array = pa.ExtensionArray.from_storage(list_ext_type, storage_array)
                else:
                    merged_array = pa.array(merged_vals)
            else:
                merged_array = pa.array(merged_vals)
```

- [ ] **Step 4.4: Run to verify new tests pass**

```bash
uv run pytest tests/test_core/operators/test_merge_join.py -v
```

Expected: All PASSED.

- [ ] **Step 4.5: Run the full test suite to check for regressions**

```bash
uv run pytest tests/ -v --tb=short
```

Expected: All PASSED.

- [ ] **Step 4.6: Commit**

```bash
git add src/orcapod/core/operators/merge_join.py \
        tests/test_core/operators/test_merge_join.py
git commit -m "fix(operators): MergeJoin produces extension-typed list when merging logical-type columns

binary_static_process used pa.array(merged_vals) which inferred array type from
raw storage values, producing plain large_list(storage_type) and losing the
extension wrapper. Fix snapshots the element Arrow type before the Polars round-trip,
then builds the merged array as pa.ExtensionArray.from_storage(ListLogicalType, ...)
when the element is an extension type. Handles nested list[list[T]] naturally.

Fixes ITL-627 (Defect 3)."
```

---

## Self-Review

**Spec coverage:**
- Defect 1 fix ✓ (Task 1) + unit test ✓ + integration tests ✓ (Task 2)
- Defect 1 `test_list_logical_type_polars_ext_carries_metadata` ✓
- Defect 2 fix ✓ (Task 3) + all 8 hashing tests ✓
- Defect 3 fix ✓ (Task 4) + scalar and nested MergeJoin tests ✓
- `set[File]` test ✓
- `list[list[File]]` hashing test ✓
- Dataclass with `list[File]` field test ✓
- `list[File] × list[File]` MergeJoin test ✓

**No placeholders:** All test code and implementation code is complete.

**Type consistency:** `list_lt`, `list_ext_type`, `elem_arrow_type` are consistent across Fix 3 Part A and Part B.
