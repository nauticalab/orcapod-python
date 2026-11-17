# Pick and Index Operators Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Pick` and `Index` operators that let users project into dict/struct and list columns on a stream, with static type resolution and per-packet miss handling.

**Architecture:** Two `UnaryOperator` subclasses (one per operator) backed by a shared `BaseLogicalType` class that adds `pick_field`/`index_element` to the extension type hierarchy. Both operators use Python-level column access via `data[col]` for both streaming (`async_execute`) and barrier (`unary_static_process`) paths — the stream's type converter handles Arrow ↔ Python round-tripping transparently.

**Tech Stack:** Python 3.11+, PyArrow, orcapod `UnaryOperator`, `Data.update()` / `Data.with_columns()` / `Data.with_source_info()`, `_materialize_to_stream()`.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/orcapod/extension_types/base_logical_type.py` | `BaseLogicalType` with default `NotImplementedError` impls |
| Modify | `src/orcapod/extension_types/protocols.py` | Add `pick_field`/`index_element` to `LogicalTypeProtocol` |
| Modify | `src/orcapod/extension_types/builtin_logical_types.py` | Inherit `BaseLogicalType` on 3 classes |
| Modify | `src/orcapod/extension_types/dataclass_logical_type_factory.py` | Inherit `BaseLogicalType` on `DataclassLogicalType` |
| Modify | `src/orcapod/extension_types/pydantic_logical_type_factory.py` | Inherit `BaseLogicalType` on `PydanticLogicalType` |
| Create | `src/orcapod/core/operators/pick.py` | `Pick` `UnaryOperator` |
| Create | `src/orcapod/core/operators/index.py` | `Index` `UnaryOperator` |
| Modify | `src/orcapod/core/operators/__init__.py` | Export `Pick`, `Index` |
| Modify | `src/orcapod/operators/__init__.py` | Re-export `Pick`, `Index` |
| Modify | `src/orcapod/core/streams/base.py` | Add `.pick()` / `.index()` convenience methods |
| Create | `tests/test_core/operators/test_pick_index.py` | All unit + integration tests |

---

## Task 1: `BaseLogicalType` + `LogicalTypeProtocol` update

**Files:**
- Create: `src/orcapod/extension_types/base_logical_type.py`
- Modify: `src/orcapod/extension_types/protocols.py`
- Modify: `src/orcapod/extension_types/builtin_logical_types.py`
- Modify: `src/orcapod/extension_types/dataclass_logical_type_factory.py`
- Modify: `src/orcapod/extension_types/pydantic_logical_type_factory.py`

- [ ] **Step 1: Write the failing tests for `BaseLogicalType`**

Create `tests/test_core/operators/test_pick_index.py` with this initial content:

```python
"""Tests for Pick and Index operators."""
from __future__ import annotations

import pytest

from orcapod.extension_types.base_logical_type import BaseLogicalType


class ConcreteLogicalType(BaseLogicalType):
    """Minimal concrete subclass for testing defaults."""
    pass


def test_base_logical_type_pick_field_raises():
    lt = ConcreteLogicalType()
    with pytest.raises(NotImplementedError, match="does not yet support pick"):
        lt.pick_field("some_key")


def test_base_logical_type_index_element_raises():
    lt = ConcreteLogicalType()
    with pytest.raises(NotImplementedError, match="does not yet support index"):
        lt.index_element()
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py -v
```

Expected: `ModuleNotFoundError: No module named 'orcapod.extension_types.base_logical_type'`

- [ ] **Step 3: Create `BaseLogicalType`**

Create `src/orcapod/extension_types/base_logical_type.py`:

```python
"""Base class for all orcapod logical types."""
from __future__ import annotations


class BaseLogicalType:
    """Shared base for all logical types.

    Provides default ``NotImplementedError`` implementations for structural
    projection methods. Logical types that support ``pick`` or ``index``
    override these methods.
    """

    def pick_field(self, key: str) -> type:
        """Return the Python type of field ``key``.

        Args:
            key: Name of the field to project into.

        Returns:
            The Python type of the requested field.

        Raises:
            NotImplementedError: Until implemented for this logical type.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not yet support pick (keyed field access). "
            "Support for this extension type is planned for a future issue."
        )

    def index_element(self) -> type:
        """Return the Python element type for positional list access.

        Returns:
            The Python type of elements in this list-like logical type.

        Raises:
            NotImplementedError: Until implemented for this logical type.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not yet support index (positional access). "
            "Support for this extension type is planned for a future issue."
        )
```

- [ ] **Step 4: Run the tests — expect them to pass**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py::test_base_logical_type_pick_field_raises tests/test_core/operators/test_pick_index.py::test_base_logical_type_index_element_raises -v
```

Expected: PASS

- [ ] **Step 5: Add `pick_field` / `index_element` signatures to `LogicalTypeProtocol`**

In `src/orcapod/extension_types/protocols.py`, find the `LogicalTypeProtocol` class (around line 93) and add these two methods after the existing method signatures:

```python
    def pick_field(self, key: str) -> type:
        """Return the Python type of field ``key`` in this structured logical type.

        Args:
            key: Name of the field to project into.

        Returns:
            The Python type of the requested field.

        Raises:
            InputValidationError: If the field does not exist in the type's schema.
            NotImplementedError: If this logical type does not support keyed access.
        """
        ...

    def index_element(self) -> type:
        """Return the Python element type for positional list access.

        Returns:
            The Python type of elements in this list-like logical type.

        Raises:
            NotImplementedError: If this logical type does not support positional access.
        """
        ...
```

- [ ] **Step 6: Wire `BaseLogicalType` into all five existing implementers**

In `src/orcapod/extension_types/builtin_logical_types.py`, change three class declarations:

```python
# Before:
class LogicalPath:
# After:
class LogicalPath(BaseLogicalType):

# Before:
class LogicalUPath:
# After:
class LogicalUPath(BaseLogicalType):

# Before:
class LogicalUUID:
# After:
class LogicalUUID(BaseLogicalType):
```

Add the import at the top of the file:
```python
from orcapod.extension_types.base_logical_type import BaseLogicalType
```

In `src/orcapod/extension_types/dataclass_logical_type_factory.py`, change:
```python
# Before (line 44):
class DataclassLogicalType:
# After:
class DataclassLogicalType(BaseLogicalType):
```

Add the import:
```python
from orcapod.extension_types.base_logical_type import BaseLogicalType
```

In `src/orcapod/extension_types/pydantic_logical_type_factory.py`, change:
```python
# Before (line 44):
class PydanticLogicalType:
# After:
class PydanticLogicalType(BaseLogicalType):
```

Add the import:
```python
from orcapod.extension_types.base_logical_type import BaseLogicalType
```

- [ ] **Step 7: Verify the existing test suite still passes**

```bash
uv run pytest tests/ -x -q
```

Expected: all existing tests pass (no regressions).

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/extension_types/base_logical_type.py \
        src/orcapod/extension_types/protocols.py \
        src/orcapod/extension_types/builtin_logical_types.py \
        src/orcapod/extension_types/dataclass_logical_type_factory.py \
        src/orcapod/extension_types/pydantic_logical_type_factory.py \
        tests/test_core/operators/test_pick_index.py
git commit -m "feat(extension_types): add BaseLogicalType with pick_field/index_element stubs (ITL-140)"
```

---

## Task 2: `Pick` operator

**Files:**
- Create: `src/orcapod/core/operators/pick.py`
- Test: `tests/test_core/operators/test_pick_index.py`

### Background — key internal APIs

**Type detection** (in `validate_unary_input`):
```python
import typing
col_type = data_schema[self.column]          # Python type from Schema
origin = typing.get_origin(col_type)         # e.g. dict, list, or None
args   = typing.get_args(col_type)           # e.g. (str, int) for dict[str, int]
```

`dict[K, V]` columns are stored in Arrow as `pa.large_list(pa.struct([pa.field("key", K), pa.field("value", V)]))`. The Python type converter handles the dict ↔ Arrow round-trip transparently, so `data[col]` always returns a plain Python `dict`.

**Updating a Data object** (in `async_execute` and `unary_static_process`):
```python
# Replace value in an EXISTING column (out=None):
new_data = (
    data.update(**{self.column: extracted})
        .with_source_info(**{self.column: new_src})
)

# ADD a new column (out='new_name'):
new_data = (
    data.with_columns(
        column_types={self.out: self._output_type},
        **{self.out: extracted},
    ).with_source_info(**{self.out: new_src})
)
```

**Source token convention:**
```python
src_token = data.source_info().get(self.column) or ""
new_src   = f"{src_token}[{self.key!r}]" if src_token else None
```

**Rebuilding a stream in `unary_static_process`:**
```python
rows = []
for tag, data in stream.iter_data():
    ...
    rows.append((tag, new_data))
return self._materialize_to_stream(rows)   # static method on StaticOutputOperatorPod
```

- [ ] **Step 1: Write failing tests for `Pick` build-time validation**

Append to `tests/test_core/operators/test_pick_index.py`:

```python
import pyarrow as pa
import pytest

from orcapod.core.operators import Pick
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def dict_stream() -> ArrowTableStream:
    """Stream with 1 tag and 1 dict[str, int] data column."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"animal": "cat",  "scores": {"speed": 8,  "stealth": 9}},
            {"animal": "dog",  "scores": {"speed": 7,  "strength": 6}},
            {"animal": "bird", "scores": {"speed": 10, "stealth": 7}},
        ],
        python_schema={"animal": str, "scores": dict[str, int]},
    )
    return ArrowTableStream(table, tag_columns=["animal"])


# ── build-time validation tests ───────────────────────────────────────────────

def test_pick_missing_column_raises(dict_stream):
    with pytest.raises(InputValidationError, match="not found in data schema"):
        Pick("nonexistent", "speed")(dict_stream)


def test_pick_out_collision_raises(dict_stream):
    with pytest.raises(InputValidationError, match="already exists"):
        Pick("scores", "speed", out="scores")(dict_stream)
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py::test_pick_missing_column_raises tests/test_core/operators/test_pick_index.py::test_pick_out_collision_raises -v
```

Expected: `ImportError` (Pick not yet defined).

- [ ] **Step 3: Implement `Pick`**

Create `src/orcapod/core/operators/pick.py`:

```python
"""Pick operator — keyed projection into dict/struct-typed columns."""
from __future__ import annotations

import logging
import typing
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_MISSING = object()  # sentinel for unresolved output type


class Pick(UnaryOperator):
    """Extract a value from a struct- or dict-typed data column by key.

    For ``dict[K, V]`` columns the lookup is per-packet; if the key is absent
    the packet is skipped (or an error is raised when ``fail_on_miss=True``).

    For extension-type struct columns (Pydantic, dataclass) the field
    existence is validated at build time and guaranteed at runtime.

    Args:
        column: Name of the data column to project into.
        key: Dict key or struct field name to extract.
        out: Output column name.  ``None`` (default) replaces ``column``
            in-place; a string adds a new column alongside the original.
        fail_on_miss: If ``True``, raise ``RuntimeError`` when the key is
            absent in a packet instead of skipping.  Excluded from
            ``identity_structure`` — miss-handling does not affect
            functional output semantics.  See ITL-439.
    """

    def __init__(
        self,
        column: str,
        key: str,
        out: str | None = None,
        fail_on_miss: bool = False,
        **kwargs: Any,
    ) -> None:
        self.column = column
        self.key = key
        self.out = out
        self.fail_on_miss = fail_on_miss
        self._output_type: type = _MISSING  # type: ignore[assignment]
        self._mode: str = ""
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        # fail_on_miss intentionally excluded — it controls error behaviour,
        # not the functional transformation.
        return (self.__class__.__name__, self.column, self.key, self.out)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Validate input schema and resolve output type.

        Args:
            stream: The upstream stream to validate.

        Raises:
            InputValidationError: If ``column`` is missing, ``out`` collides
                with an existing column, or the column type does not support
                pick.
            NotImplementedError: If the column holds an extension type whose
                ``pick_field`` implementation is not yet available.
        """
        _, data_schema = stream.output_schema()
        data_columns = list(data_schema.keys())

        if self.column not in data_columns:
            raise InputValidationError(
                f"Pick: column {self.column!r} not found in data schema. "
                f"Available data columns: {data_columns}"
            )

        out_name = self.out if self.out is not None else self.column
        if self.out is not None and self.out in data_columns:
            raise InputValidationError(
                f"Pick: out column {self.out!r} already exists in data schema. "
                f"Choose a different name to avoid clobbering existing data."
            )

        col_type = data_schema[self.column]
        origin = typing.get_origin(col_type)

        if origin is dict:
            # dict[K, V] — dynamic key lookup at runtime
            args = typing.get_args(col_type)
            self._output_type = args[1] if args else Any  # type: ignore[assignment]
            self._mode = "map"
        else:
            # Extension type — delegate to logical type's pick_field
            from orcapod.contexts import get_default_type_converter
            converter = get_default_type_converter()
            # Access registry via the concrete converter (private but stable)
            registry = getattr(converter, "_logical_type_registry", None)
            if registry is None:
                raise InputValidationError(
                    f"Pick: cannot resolve logical type for column {self.column!r} "
                    f"(no LogicalTypeRegistry configured)."
                )
            lt = registry.get_by_python_type(col_type)
            if lt is None:
                raise InputValidationError(
                    f"Pick: column {self.column!r} has type {col_type!r} which is "
                    f"not a supported pick target (not dict[K,V] and no registered "
                    f"logical type)."
                )
            # May raise NotImplementedError for types not yet implemented
            self._output_type = lt.pick_field(self.key)
            self._mode = "struct"

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
        """Return the (tag, data) schemas for the output stream.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config passed through to ``output_schema``.
            all_info: Whether to include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.
        """
        tag_schema, data_schema = stream.output_schema(columns=columns, all_info=all_info)
        data_dict = dict(data_schema)

        if self.out is None:
            # Replace existing column type in-place
            data_dict[self.column] = self._output_type
        else:
            # Add new column; source column type unchanged
            data_dict[self.out] = self._output_type

        return tag_schema, Schema(data_dict)

    # ------------------------------------------------------------------
    # Barrier-mode execution
    # ------------------------------------------------------------------

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Process the full stream in barrier mode.

        Args:
            stream: The fully materialised input stream.

        Returns:
            A new stream with the projection applied.

        Raises:
            RuntimeError: If ``fail_on_miss=True`` and any packet's dict
                lacks ``self.key``.
        """
        out_rows: list[tuple[Any, Any]] = []
        skipped_count = 0

        for tag, data in stream.iter_data():
            col_val = data[self.column]

            if self._mode == "struct":
                extracted = col_val[self.key]
            else:  # map
                if self.key not in col_val:
                    skipped_count += 1
                    continue
                extracted = col_val[self.key]

            src_token = data.source_info().get(self.column) or ""
            new_src = f"{src_token}[{self.key!r}]" if src_token else None

            if self.out is None:
                new_data = (
                    data.update(**{self.column: extracted})
                        .with_source_info(**{self.column: new_src})
                )
            else:
                new_data = (
                    data.with_columns(
                        column_types={self.out: self._output_type},
                        **{self.out: extracted},
                    ).with_source_info(**{self.out: new_src})
                )

            out_rows.append((tag, new_data))

        if skipped_count:
            if self.fail_on_miss:
                raise RuntimeError(
                    f"Pick: {skipped_count} packet(s) missing key {self.key!r} in "
                    f"column {self.column!r} (fail_on_miss=True). See ITL-439."
                )
            logger.warning(
                "Pick: %d packet(s) skipped — key %r not found in column %r.",
                skipped_count,
                self.key,
                self.column,
            )

        if not out_rows:
            raise ValueError(
                f"Pick operator produced an empty stream: all packets were skipped "
                f"(key {self.key!r} absent in every packet of column {self.column!r})."
            )

        return self._materialize_to_stream(out_rows)

    # ------------------------------------------------------------------
    # Streaming execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash] | None = None,
    ) -> None:
        """Process packets one at a time as they arrive.

        Args:
            inputs: Single-element sequence of readable channels.
            output: Channel to send transformed packets to.
            input_pipeline_hashes: Ignored; present for protocol compliance.
        """
        try:
            async for tag, data in inputs[0]:
                col_val = data[self.column]

                if self._mode == "struct":
                    extracted = col_val[self.key]
                else:  # map
                    if self.key not in col_val:
                        if self.fail_on_miss:
                            raise RuntimeError(
                                f"Pick: key {self.key!r} not found in column "
                                f"{self.column!r} (fail_on_miss=True). See ITL-439."
                            )
                        logger.warning(
                            "Pick: skipping packet — key %r not found in column %r.",
                            self.key,
                            self.column,
                        )
                        continue
                    extracted = col_val[self.key]

                src_token = data.source_info().get(self.column) or ""
                new_src = f"{src_token}[{self.key!r}]" if src_token else None

                if self.out is None:
                    new_data = (
                        data.update(**{self.column: extracted})
                            .with_source_info(**{self.column: new_src})
                    )
                else:
                    new_data = (
                        data.with_columns(
                            column_types={self.out: self._output_type},
                            **{self.out: extracted},
                        ).with_source_info(**{self.out: new_src})
                    )

                await output.send((tag, new_data))
        finally:
            await output.close()
```

- [ ] **Step 4: Run the build-time validation tests**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py::test_pick_missing_column_raises tests/test_core/operators/test_pick_index.py::test_pick_out_collision_raises -v
```

Expected: PASS

- [ ] **Step 5: Write functional tests for `Pick`**

Append to `tests/test_core/operators/test_pick_index.py`:

```python
# ── functional tests: Pick ────────────────────────────────────────────────────

def test_pick_dict_default_out(dict_stream):
    """pick with out=None replaces the column in-place, source token updated."""
    result = Pick("scores", "speed")(dict_stream)
    tag_schema, data_schema = result.output_schema()

    assert "scores" in data_schema
    assert data_schema["scores"] == int

    rows = list(result.iter_data())
    assert len(rows) == 3
    values = [data["scores"] for _, data in rows]
    assert values == [8, 7, 10]

    # source token for 'scores' should now end with "['speed']"
    for _, data in rows:
        src = data.source_info().get("scores")
        assert src is not None and src.endswith("['speed']"), f"unexpected source: {src}"


def test_pick_dict_explicit_out(dict_stream):
    """pick with out='speed_score' adds new column, original unchanged."""
    result = Pick("scores", "speed", out="speed_score")(dict_stream)
    tag_schema, data_schema = result.output_schema()

    assert "scores" in data_schema       # original preserved
    assert "speed_score" in data_schema
    assert data_schema["speed_score"] == int

    rows = list(result.iter_data())
    assert [data["speed_score"] for _, data in rows] == [8, 7, 10]

    for _, data in rows:
        src = data.source_info().get("speed_score")
        assert src is not None and src.endswith("['speed']")


def test_pick_dict_all_keys_present(dict_stream):
    """When all packets contain the key, all pass through with no warning."""
    import logging
    with pytest.warns(None) as warning_list:
        result = Pick("scores", "speed")(dict_stream)
        rows = list(result.iter_data())
    assert len(rows) == 3


def test_pick_dict_missing_key_skip():
    """Packets missing the key are skipped; others pass through."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "data": {"x": 10}},
            {"id": 2, "data": {"y": 20}},   # missing "x"
            {"id": 3, "data": {"x": 30}},
        ],
        python_schema={"id": int, "data": dict[str, int]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    result = Pick("data", "x", fail_on_miss=False)(stream)
    rows = list(result.iter_data())
    assert len(rows) == 2
    ids = [tag["id"] for tag, _ in rows]
    assert ids == [1, 3]


def test_pick_dict_missing_key_fail():
    """fail_on_miss=True raises RuntimeError when key is absent."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "data": {"x": 10}},
            {"id": 2, "data": {"y": 20}},
        ],
        python_schema={"id": int, "data": dict[str, int]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    with pytest.raises(RuntimeError, match="fail_on_miss=True"):
        list(Pick("data", "x", fail_on_miss=True)(stream).iter_data())


def test_pick_extension_type_not_implemented():
    """pick on a Pydantic/dataclass column raises NotImplementedError."""
    import dataclasses

    @dataclasses.dataclass
    class MyModel:
        value: int

    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [{"id": 1, "rec": MyModel(value=42)}],
        python_schema={"id": int, "rec": MyModel},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    with pytest.raises(NotImplementedError, match="does not yet support pick"):
        Pick("rec", "value")(stream)
```

- [ ] **Step 6: Run the functional tests**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py -k "pick" -v
```

Expected: all pick tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/operators/pick.py tests/test_core/operators/test_pick_index.py
git commit -m "feat(operators): add Pick operator with dict/struct projection (ITL-140)"
```

---

## Task 3: `Index` operator

**Files:**
- Create: `src/orcapod/core/operators/index.py`
- Test: `tests/test_core/operators/test_pick_index.py`

### Background

`list[T]` columns are stored as `pa.large_list(T_arrow)`. `data[col]` returns a Python list. Negative indices follow Python semantics; bounds check:

```python
length = len(col_val)
effective_i = self.i if self.i >= 0 else length + self.i
if effective_i < 0 or effective_i >= length:
    # out of bounds
```

Source token: `f"{src_token}[{self.i}]"` (e.g., `"src::rec::col[3]"`).

- [ ] **Step 1: Write failing tests for `Index`**

Append to `tests/test_core/operators/test_pick_index.py`:

```python
# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def list_stream() -> ArrowTableStream:
    """Stream with 1 tag and 1 list[int] data column."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"animal": "cat",  "scores": [8, 5, 9]},
            {"animal": "dog",  "scores": [7, 6, 4]},
            {"animal": "bird", "scores": [10, 7, 3]},
        ],
        python_schema={"animal": str, "scores": list[int]},
    )
    return ArrowTableStream(table, tag_columns=["animal"])


# ── build-time validation ─────────────────────────────────────────────────────

def test_index_missing_column_raises(list_stream):
    with pytest.raises(InputValidationError, match="not found in data schema"):
        from orcapod.core.operators.index import Index
        Index("nonexistent", 0)(list_stream)


def test_index_out_collision_raises(list_stream):
    from orcapod.core.operators.index import Index
    with pytest.raises(InputValidationError, match="already exists"):
        Index("scores", 0, out="scores")(list_stream)
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py::test_index_missing_column_raises tests/test_core/operators/test_pick_index.py::test_index_out_collision_raises -v
```

Expected: `ImportError` (Index not yet defined).

- [ ] **Step 3: Implement `Index`**

Create `src/orcapod/core/operators/index.py`:

```python
"""Index operator — positional projection into list-typed columns."""
from __future__ import annotations

import logging
import typing
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema

logger = logging.getLogger(__name__)

_MISSING = object()


class Index(UnaryOperator):
    """Extract an element from a list-typed data column by position.

    The list length is per-packet data, so bounds are not checked at build
    time.  Out-of-bounds access causes the packet to be skipped (or an error
    when ``fail_on_miss=True``).  Negative indices follow Python semantics
    (``-1`` is the last element).

    Args:
        column: Name of the data column to project into.
        i: Position to extract.  Negative indices follow Python semantics.
        out: Output column name.  ``None`` (default) replaces ``column``
            in-place; a string adds a new column alongside the original.
        fail_on_miss: If ``True``, raise ``RuntimeError`` on out-of-bounds
            instead of skipping.  Excluded from ``identity_structure``.
            See ITL-439.
    """

    def __init__(
        self,
        column: str,
        i: int,
        out: str | None = None,
        fail_on_miss: bool = False,
        **kwargs: Any,
    ) -> None:
        self.column = column
        self.i = i
        self.out = out
        self.fail_on_miss = fail_on_miss
        self._output_type: type = _MISSING  # type: ignore[assignment]
        self._mode: str = ""
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        return (self.__class__.__name__, self.column, self.i, self.out)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Validate input schema and resolve output element type.

        Args:
            stream: The upstream stream to validate.

        Raises:
            InputValidationError: If ``column`` is missing, ``out`` collides,
                or the column type is not a supported index target.
            NotImplementedError: If the column holds an extension type whose
                ``index_element`` is not yet implemented.
        """
        _, data_schema = stream.output_schema()
        data_columns = list(data_schema.keys())

        if self.column not in data_columns:
            raise InputValidationError(
                f"Index: column {self.column!r} not found in data schema. "
                f"Available data columns: {data_columns}"
            )

        if self.out is not None and self.out in data_columns:
            raise InputValidationError(
                f"Index: out column {self.out!r} already exists in data schema. "
                f"Choose a different name to avoid clobbering existing data."
            )

        col_type = data_schema[self.column]
        origin = typing.get_origin(col_type)

        if origin is list:
            args = typing.get_args(col_type)
            self._output_type = args[0] if args else Any  # type: ignore[assignment]
            self._mode = "list"
        else:
            from orcapod.contexts import get_default_type_converter
            converter = get_default_type_converter()
            registry = getattr(converter, "_logical_type_registry", None)
            if registry is None:
                raise InputValidationError(
                    f"Index: cannot resolve logical type for column {self.column!r}."
                )
            lt = registry.get_by_python_type(col_type)
            if lt is None:
                raise InputValidationError(
                    f"Index: column {self.column!r} has type {col_type!r} which is "
                    f"not a supported index target (not list[T] and no registered "
                    f"logical type)."
                )
            self._output_type = lt.index_element()
            self._mode = "extension"

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
        """Return the (tag, data) schemas for the output stream.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config.
            all_info: Include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.
        """
        tag_schema, data_schema = stream.output_schema(columns=columns, all_info=all_info)
        data_dict = dict(data_schema)

        if self.out is None:
            data_dict[self.column] = self._output_type
        else:
            data_dict[self.out] = self._output_type

        return tag_schema, Schema(data_dict)

    # ------------------------------------------------------------------
    # Barrier-mode execution
    # ------------------------------------------------------------------

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Process the full stream in barrier mode.

        Args:
            stream: The fully materialised input stream.

        Returns:
            A new stream with the projection applied.

        Raises:
            RuntimeError: If ``fail_on_miss=True`` and any packet is
                out-of-bounds.
        """
        out_rows: list[tuple[Any, Any]] = []
        skipped_count = 0

        for tag, data in stream.iter_data():
            col_val = data[self.column]
            length = len(col_val)
            effective_i = self.i if self.i >= 0 else length + self.i

            if effective_i < 0 or effective_i >= length:
                skipped_count += 1
                continue

            extracted = col_val[self.i]

            src_token = data.source_info().get(self.column) or ""
            new_src = f"{src_token}[{self.i}]" if src_token else None

            if self.out is None:
                new_data = (
                    data.update(**{self.column: extracted})
                        .with_source_info(**{self.column: new_src})
                )
            else:
                new_data = (
                    data.with_columns(
                        column_types={self.out: self._output_type},
                        **{self.out: extracted},
                    ).with_source_info(**{self.out: new_src})
                )

            out_rows.append((tag, new_data))

        if skipped_count:
            if self.fail_on_miss:
                raise RuntimeError(
                    f"Index: {skipped_count} packet(s) out-of-bounds at index "
                    f"{self.i} in column {self.column!r} (fail_on_miss=True). "
                    f"See ITL-439."
                )
            logger.warning(
                "Index: %d packet(s) skipped — index %d out of bounds in column %r.",
                skipped_count,
                self.i,
                self.column,
            )

        if not out_rows:
            raise ValueError(
                f"Index operator produced an empty stream: all packets were skipped "
                f"(index {self.i} out of bounds for every packet in column "
                f"{self.column!r})."
            )

        return self._materialize_to_stream(out_rows)

    # ------------------------------------------------------------------
    # Streaming execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash] | None = None,
    ) -> None:
        """Process packets one at a time as they arrive.

        Args:
            inputs: Single-element sequence of readable channels.
            output: Channel to send transformed packets to.
            input_pipeline_hashes: Ignored; present for protocol compliance.
        """
        try:
            async for tag, data in inputs[0]:
                col_val = data[self.column]
                length = len(col_val)
                effective_i = self.i if self.i >= 0 else length + self.i

                if effective_i < 0 or effective_i >= length:
                    if self.fail_on_miss:
                        raise RuntimeError(
                            f"Index: index {self.i} out of bounds for column "
                            f"{self.column!r} (length {length}, "
                            f"fail_on_miss=True). See ITL-439."
                        )
                    logger.warning(
                        "Index: skipping packet — index %d out of bounds for "
                        "column %r (length %d).",
                        self.i,
                        self.column,
                        length,
                    )
                    continue

                extracted = col_val[self.i]

                src_token = data.source_info().get(self.column) or ""
                new_src = f"{src_token}[{self.i}]" if src_token else None

                if self.out is None:
                    new_data = (
                        data.update(**{self.column: extracted})
                            .with_source_info(**{self.column: new_src})
                    )
                else:
                    new_data = (
                        data.with_columns(
                            column_types={self.out: self._output_type},
                            **{self.out: extracted},
                        ).with_source_info(**{self.out: new_src})
                    )

                await output.send((tag, new_data))
        finally:
            await output.close()
```

- [ ] **Step 4: Run the build-time validation tests**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py::test_index_missing_column_raises tests/test_core/operators/test_pick_index.py::test_index_out_collision_raises -v
```

Expected: PASS

- [ ] **Step 5: Write functional tests for `Index`**

Append to `tests/test_core/operators/test_pick_index.py`:

```python
# ── functional tests: Index ───────────────────────────────────────────────────

def test_index_list_default_out(list_stream):
    """index with out=None replaces the column, source token updated."""
    from orcapod.core.operators.index import Index
    result = Index("scores", 0)(list_stream)
    tag_schema, data_schema = result.output_schema()

    assert "scores" in data_schema
    assert data_schema["scores"] == int

    rows = list(result.iter_data())
    assert [data["scores"] for _, data in rows] == [8, 7, 10]

    for _, data in rows:
        src = data.source_info().get("scores")
        assert src is not None and src.endswith("[0]"), f"unexpected source: {src}"


def test_index_list_explicit_out(list_stream):
    """index with out='first' adds new column, original unchanged."""
    from orcapod.core.operators.index import Index
    result = Index("scores", 0, out="first")(list_stream)
    tag_schema, data_schema = result.output_schema()

    assert "scores" in data_schema       # original preserved
    assert "first" in data_schema
    assert data_schema["first"] == int

    rows = list(result.iter_data())
    assert [data["first"] for _, data in rows] == [8, 7, 10]

    for _, data in rows:
        src = data.source_info().get("first")
        assert src is not None and src.endswith("[0]")


def test_index_in_bounds_negative(list_stream):
    """i=-1 returns the last element of each list."""
    from orcapod.core.operators.index import Index
    result = Index("scores", -1)(list_stream)
    rows = list(result.iter_data())
    assert [data["scores"] for _, data in rows] == [9, 4, 3]


def test_index_oob_positive_skip():
    """Positive OOB index skips the packet."""
    from orcapod.contexts import get_default_type_converter
    from orcapod.core.operators.index import Index
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "vals": [10, 20, 30]},
            {"id": 2, "vals": [40]},           # OOB at index 2
            {"id": 3, "vals": [50, 60, 70]},
        ],
        python_schema={"id": int, "vals": list[int]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    result = Index("vals", 2, fail_on_miss=False)(stream)
    rows = list(result.iter_data())
    assert len(rows) == 2
    assert [tag["id"] for tag, _ in rows] == [1, 3]


def test_index_oob_positive_fail():
    """fail_on_miss=True raises RuntimeError on positive OOB."""
    from orcapod.contexts import get_default_type_converter
    from orcapod.core.operators.index import Index
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [{"id": 1, "vals": [10]}, {"id": 2, "vals": []}],
        python_schema={"id": int, "vals": list[int]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    with pytest.raises(RuntimeError, match="fail_on_miss=True"):
        list(Index("vals", 0, fail_on_miss=True)(stream).iter_data())


def test_index_oob_negative_skip():
    """Negative OOB index (e.g. -5 on length-3 list) skips the packet."""
    from orcapod.contexts import get_default_type_converter
    from orcapod.core.operators.index import Index
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "vals": [10, 20, 30]},
            {"id": 2, "vals": [40, 50, 60]},
        ],
        python_schema={"id": int, "vals": list[int]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    result = Index("vals", -5, fail_on_miss=False)(stream)
    rows = list(result.iter_data())
    assert len(rows) == 0  # all skipped, operator raises ValueError for empty stream
    # Note: this test should catch ValueError from empty stream or len==0


def test_index_oob_negative_fail():
    """fail_on_miss=True raises RuntimeError on negative OOB."""
    from orcapod.contexts import get_default_type_converter
    from orcapod.core.operators.index import Index
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [{"id": 1, "vals": [10, 20, 30]}],
        python_schema={"id": int, "vals": list[int]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    with pytest.raises(RuntimeError, match="fail_on_miss=True"):
        list(Index("vals", -5, fail_on_miss=True)(stream).iter_data())
```

- [ ] **Step 6: Run all Index tests**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py -k "index" -v
```

Expected: all index tests PASS. Note: `test_index_oob_negative_skip` expects a `ValueError` from the empty-stream guard in `unary_static_process` — adjust the assertion if needed.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/operators/index.py tests/test_core/operators/test_pick_index.py
git commit -m "feat(operators): add Index operator with list positional projection (ITL-140)"
```

---

## Task 4: Wire exports and stream convenience methods

**Files:**
- Modify: `src/orcapod/core/operators/__init__.py`
- Modify: `src/orcapod/operators/__init__.py`
- Modify: `src/orcapod/core/streams/base.py`
- Test: `tests/test_core/operators/test_pick_index.py`

- [ ] **Step 1: Write failing import tests**

Append to `tests/test_core/operators/test_pick_index.py`:

```python
# ── export / API tests ────────────────────────────────────────────────────────

def test_pick_importable_from_public_api():
    from orcapod.operators import Pick  # noqa: F401


def test_index_importable_from_public_api():
    from orcapod.operators import Index  # noqa: F401


def test_stream_has_pick_method(dict_stream):
    assert hasattr(dict_stream, "pick")
    result = dict_stream.pick("scores", "speed")
    tag_schema, data_schema = result.output_schema()
    assert data_schema["scores"] == int


def test_stream_has_index_method(list_stream):
    assert hasattr(list_stream, "index")
    result = list_stream.index("scores", 1)
    tag_schema, data_schema = result.output_schema()
    assert data_schema["scores"] == int
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py -k "importable or stream_has" -v
```

Expected: `ImportError` for Pick/Index from public API, `AttributeError` for stream methods.

- [ ] **Step 3: Update `src/orcapod/core/operators/__init__.py`**

```python
from .batch import Batch
from .column_selection import (
    DropDataColumns,
    DropTagColumns,
    SelectDataColumns,
    SelectTagColumns,
)
from .filters import PolarsFilter
from .index import Index
from .join import Join
from .mappers import MapData, MapTags
from .merge_join import MergeJoin
from .pick import Pick
from .semijoin import SemiJoin

__all__ = [
    "Join",
    "MergeJoin",
    "SemiJoin",
    "MapTags",
    "MapData",
    "Batch",
    "SelectTagColumns",
    "SelectDataColumns",
    "DropTagColumns",
    "DropDataColumns",
    "PolarsFilter",
    "Pick",
    "Index",
]
```

- [ ] **Step 4: Update `src/orcapod/operators/__init__.py`**

Add `Pick` and `Index` to whatever re-exports already exist in that file. Open it, read the existing imports, and add:

```python
from orcapod.core.operators import Pick, Index
```

and include them in `__all__` if one exists.

- [ ] **Step 5: Add `.pick()` and `.index()` to `StreamBase`**

In `src/orcapod/core/streams/base.py`, add after the existing convenience methods (e.g., after `batch`):

```python
    def pick(
        self,
        column: str,
        key: str,
        out: str | None = None,
        fail_on_miss: bool = False,
        label: str | None = None,
    ) -> "StreamBase":
        """Extract a value from a struct- or dict-typed column by key.

        Args:
            column: Data column to project into.
            key: Field name (struct) or dict key to extract.
            out: Output column name.  ``None`` replaces ``column`` in-place;
                a string adds a new column alongside the original.
            fail_on_miss: If ``True``, raise on missing key instead of
                skipping.
            label: Optional pipeline label for this node.

        Returns:
            A new stream with the projection applied.
        """
        from orcapod.core.operators import Pick
        return Pick(column, key, out=out, fail_on_miss=fail_on_miss)(self, label=label)

    def index(
        self,
        column: str,
        i: int,
        out: str | None = None,
        fail_on_miss: bool = False,
        label: str | None = None,
    ) -> "StreamBase":
        """Extract an element from a list-typed column by position.

        Args:
            column: Data column to project into.
            i: Position to extract.  Negative indices follow Python semantics
                (``-1`` is the last element).
            out: Output column name.  ``None`` replaces ``column`` in-place;
                a string adds a new column alongside the original.
            fail_on_miss: If ``True``, raise on out-of-bounds instead of
                skipping.
            label: Optional pipeline label for this node.

        Returns:
            A new stream with the projection applied.
        """
        from orcapod.core.operators import Index
        return Index(column, i, out=out, fail_on_miss=fail_on_miss)(self, label=label)
```

- [ ] **Step 6: Run the export/API tests**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py -k "importable or stream_has" -v
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/operators/__init__.py \
        src/orcapod/operators/__init__.py \
        src/orcapod/core/streams/base.py \
        tests/test_core/operators/test_pick_index.py
git commit -m "feat(operators): export Pick/Index and add .pick()/.index() stream methods (ITL-140)"
```

---

## Task 5: Integration tests

**Files:**
- Test: `tests/test_core/operators/test_pick_index.py`

- [ ] **Step 1: Write integration tests**

Append to `tests/test_core/operators/test_pick_index.py`:

```python
# ── integration tests ─────────────────────────────────────────────────────────

def test_chained_pick_then_index():
    """stream.pick('col', 'key').index('col', 1) chains correctly end-to-end."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "data": {"scores": [10, 20, 30]}},
            {"id": 2, "data": {"scores": [40, 50, 60]}},
        ],
        python_schema={"id": int, "data": dict[str, list[int]]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    result = stream.pick("data", "scores").index("data", 1)
    tag_schema, data_schema = result.output_schema()

    assert "data" in data_schema
    assert data_schema["data"] == int

    rows = list(result.iter_data())
    assert [data["data"] for _, data in rows] == [20, 50]

    # Source token should encode both projections
    for _, data in rows:
        src = data.source_info().get("data")
        assert src is not None
        assert "['scores']" in src
        assert "[1]" in src


def test_composition_with_join():
    """pick used in a pipeline that also includes join."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()

    left_table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "meta": {"label": "alpha"}},
            {"id": 2, "meta": {"label": "beta"}},
        ],
        python_schema={"id": int, "meta": dict[str, str]},
    )
    right_table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
        ],
        python_schema={"id": int, "value": int},
    )
    left = ArrowTableStream(left_table, tag_columns=["id"])
    right = ArrowTableStream(right_table, tag_columns=["id"])

    result = left.pick("meta", "label").join(right)
    tag_schema, data_schema = result.output_schema()

    assert "meta" in data_schema    # now holds str, not dict
    assert "value" in data_schema
    assert data_schema["meta"] == str

    rows = list(result.iter_data())
    assert len(rows) == 2
```

- [ ] **Step 2: Run the integration tests**

```bash
uv run pytest tests/test_core/operators/test_pick_index.py -k "chained or composition" -v
```

Expected: PASS

- [ ] **Step 3: Run the full test suite to verify no regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_core/operators/test_pick_index.py
git commit -m "test(operators): add integration tests for Pick/Index chaining and join (ITL-140)"
```

---

## Task 6: File follow-up Linear issues and create PR

- [ ] **Step 1: File two follow-up Linear issues**

File these two issues in the "Tools" team, "Orcapod Python v0.2 Stabilization Push" project, referencing ITL-140:

**Issue A — dataclass support:**
- Title: `Add pick_field / index_element to DataclassLogicalType (ITL-140 follow-up)`
- Description (use the project CLAUDE.md template):
  - Overview: Implement `pick_field(key)` and `index_element()` on `DataclassLogicalType` so that `Pick` and `Index` operators can project into dataclass-typed columns with static field-name validation and runtime Python-object access.
  - Success criteria: `pick` on a `dataclass` column resolves field type at build time; runtime accesses the Python object's attribute; missing field raises `InputValidationError` at build time.
  - Reference: ITL-140.

**Issue B — Pydantic support:**
- Title: `Add pick_field / index_element to PydanticLogicalType (ITL-140 follow-up)`
- Same structure as Issue A but for `PydanticLogicalType`.

- [ ] **Step 2: Push the branch**

```bash
git push -u origin eywalker/itl-140-add-pick-and-index-operators-for-structural-projection-into
```

- [ ] **Step 3: Create the PR** using `sensei:create-pr`

Target branch: `main`. Include `Fixes ITL-140` in the description body.
