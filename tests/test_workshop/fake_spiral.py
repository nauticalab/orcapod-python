"""An in-memory fake of the pyspiral surface ``edi_tutorial_dataset.py`` uses.

Modelled on pyspiral 0.16.33 and on what was seen live (ITL-926, ITL-933):

* ``Spiral.project``, ``list_projects``, ``create_project(id_prefix=, description=,
  volume_id=)``, ``scan(table_or_select, where=)``.
* ``Project.list_tables`` (``.dataset``, ``.table``), ``table`` (``ValueError`` when
  missing), ``create_table(identifier, key_schema=)``, ``drop_table``.
* ``Table[...]`` and ``Table.select(...)`` build expressions; comparisons and ``&``
  combine them. A comparison literal must carry the column's exact Arrow type, as live
  SpiralDB refuses ``u64`` against ``i64`` ("Cannot compare different DTypes").
* ``Table.key_schema.to_arrow()``, ``Table.schema().to_arrow()`` (the key schema until
  data is written), ``get_metadata`` / ``set_metadata`` (upsert), ``txn()`` with
  ``write``, ``commit``, ``abort`` and the context-manager protocol.
* Writes upsert by key and keep rows sorted by key. The first write fixes the table's
  schema; a later write whose schema differs beyond nullability raises. A struct-level
  null raises, as SpiralDB refuses one.
* Nullability as the server keeps it (live probe on edi-tutorial-v1-841226, November 11,
  2026): every top-level column, keys included, and every struct field is stored
  nullable; only list and fixed-size-list items keep a non-null flag. A top-level null
  under a non-null declaration is refused (Arrow validates the incoming batch); a null
  in a struct field declared non-null is accepted.

Every mutating call is appended to ``Spiral.log`` as ``(project_id, operation, name)``.
"""

from __future__ import annotations

import dataclasses
import itertools
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc


class Expr:
    """A filter expression over one table's columns."""

    def evaluate(self, table: pa.Table) -> Any:  # pragma: no cover
        """Evaluate the expression against ``table``."""
        raise NotImplementedError

    def _compare(self, other: Any, op: str) -> Expr:
        """Return a comparison of this expression with ``other`` using ``op``."""
        return Compare(self, other, op)

    def __ge__(self, other: Any) -> Expr:
        """Build ``self >= other``."""
        return self._compare(other, "greater_equal")

    def __gt__(self, other: Any) -> Expr:
        """Build ``self > other``."""
        return self._compare(other, "greater")

    def __le__(self, other: Any) -> Expr:
        """Build ``self <= other``."""
        return self._compare(other, "less_equal")

    def __lt__(self, other: Any) -> Expr:
        """Build ``self < other``."""
        return self._compare(other, "less")

    def __and__(self, other: Expr) -> Expr:
        """Combine two expressions with a logical and."""
        return And(self, other)


@dataclasses.dataclass(eq=False)
class Column(Expr):
    """A column reference, as ``Table[name]`` returns it."""

    name: str

    def evaluate(self, table: pa.Table) -> Any:
        """Return the named column of ``table``."""
        return table.column(self.name)


@dataclasses.dataclass(eq=False)
class Compare(Expr):
    """A comparison of an expression with a literal.

    The literal must be a ``pa.Scalar`` of the column's exact Arrow type, as live
    SpiralDB refuses ``u64`` against ``i64`` ("Cannot compare different DTypes").
    """

    left: Expr
    right: Any
    op: str

    def evaluate(self, table: pa.Table) -> Any:
        """Return the comparison's boolean mask over ``table``.

        Raises:
            ValueError: if the literal's Arrow type differs from the column's.
        """
        col = self.left.evaluate(table)
        if not isinstance(self.right, pa.Scalar) or self.right.type != col.type:
            got = (
                self.right.type
                if isinstance(self.right, pa.Scalar)
                else type(self.right)
            )
            raise ValueError(f"Cannot compare different DTypes {col.type} and {got}")
        return getattr(pc, self.op)(col, self.right)


@dataclasses.dataclass(eq=False)
class And(Expr):
    """Two expressions combined with ``&``."""

    left: Expr
    right: Expr

    def evaluate(self, table: pa.Table) -> Any:
        """Return the logical and of both sides' masks over ``table``."""
        return pc.and_(self.left.evaluate(table), self.right.evaluate(table))


@dataclasses.dataclass(eq=False)
class Select:
    """A column projection of a table, as ``Table.select(...)`` returns it."""

    table: FakeTable
    columns: tuple[str, ...]


class _Schema:
    """A Spiral schema; ``to_arrow()`` gives the Arrow schema."""

    def __init__(self, schema: pa.Schema) -> None:
        """Wrap ``schema``."""
        self._schema = schema

    def to_arrow(self) -> pa.Schema:
        """Return the Arrow schema."""
        return self._schema

    @property
    def names(self) -> list[str]:
        """Return the column names."""
        return self._schema.names


@dataclasses.dataclass(frozen=True)
class TableResource:
    """An entry of ``Project.list_tables`` (``.dataset``, ``.table``)."""

    project_id: str
    dataset: str
    table: str


@dataclasses.dataclass
class _Stored:
    """A stored table: key schema, rows (``None`` until written) and metadata."""

    key_schema: pa.Schema
    data: pa.Table | None = None
    metadata: dict[str, bytes] = dataclasses.field(default_factory=dict)


def stored_type(t: pa.DataType) -> pa.DataType:
    """Return ``t`` as the server stores it: struct fields nullable, list items kept."""
    if pa.types.is_struct(t):
        return pa.struct([pa.field(f.name, stored_type(f.type), True) for f in t])
    if pa.types.is_fixed_size_list(t):
        item = t.value_field
        return pa.list_(
            pa.field(item.name, stored_type(item.type), item.nullable), t.list_size
        )
    if pa.types.is_list(t):
        item = t.value_field
        return pa.list_(pa.field(item.name, stored_type(item.type), item.nullable))
    if pa.types.is_large_list(t):
        item = t.value_field
        return pa.large_list(pa.field(item.name, stored_type(item.type), item.nullable))
    return t


def stored_schema(schema: pa.Schema) -> pa.Schema:
    """Return ``schema`` as the server stores it: every top-level column nullable."""
    return pa.schema([pa.field(f.name, stored_type(f.type), True) for f in schema])


class FakeTxn:
    """A write transaction, as ``Table.txn()`` returns it.

    Batches are checked on ``write`` and applied on ``commit``; as a context manager it
    commits on a clean exit and aborts on an exception.
    """

    def __init__(self, table: FakeTable) -> None:
        """Open a transaction on ``table``."""
        self._table = table
        self._batches: list[pa.RecordBatch] = []
        self._done = False

    def __enter__(self) -> FakeTxn:
        """Return the transaction."""
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Commit on a clean exit; abort on an exception."""
        if exc_type is None:
            self.commit()
        else:
            self.abort()

    def write(self, batch: pa.RecordBatch | pa.Table) -> None:
        """Check ``batch`` against the table and stage it.

        Raises:
            ValueError: if the batch breaks the table's key or stored schema, holds a
                struct-level null, or a null under a non-null top-level declaration.
        """
        self._table._check_write(batch)
        if isinstance(batch, pa.Table):
            self._batches.extend(batch.to_batches())
        else:
            self._batches.append(batch)

    def commit(self) -> None:
        """Apply the staged batches once; later calls do nothing."""
        if not self._done:
            self._done = True
            if self._batches:
                self._table._commit(pa.Table.from_batches(self._batches))

    def abort(self) -> None:
        """Discard the staged batches."""
        self._done = True
        self._batches = []


class FakeTable:
    """A handle on a stored table, as ``Project.table`` returns it."""

    def __init__(self, spiral: FakeSpiral, project_id: str, ident: str) -> None:
        """Refer to table ``ident`` of project ``project_id``."""
        self._spiral = spiral
        self._project_id = project_id
        self._ident = ident

    @property
    def _stored(self) -> _Stored:
        """Return the table's stored state."""
        return self._spiral._projects[self._project_id][self._ident]

    def __getitem__(self, name: str) -> Column:
        """Return a column expression for ``name``."""
        return Column(name)

    def select(self, *columns: str) -> Select:
        """Return a projection of ``columns`` for ``Spiral.scan``."""
        return Select(self, columns)

    @property
    def key_schema(self) -> _Schema:
        """Return the key schema, as the server stores it (every column nullable)."""
        return _Schema(self._stored.key_schema)

    def schema(self) -> _Schema:
        """Return the stored schema: the key schema until data is written."""
        data = self._stored.data
        return _Schema(self._stored.key_schema if data is None else data.schema)

    def get_metadata(self) -> dict[str, bytes]:
        """Return a copy of the table's metadata."""
        return dict(self._stored.metadata)

    def set_metadata(self, metadata: dict[str, bytes]) -> None:
        """Upsert ``metadata`` into the table's metadata (logged)."""
        self._spiral.log.append((self._project_id, "set_metadata", self._ident))
        self._stored.metadata.update(metadata)

    def txn(self) -> FakeTxn:
        """Open a write transaction."""
        return FakeTxn(self)

    def _check_write(self, batch: pa.RecordBatch | pa.Table) -> None:
        """Log the write and check ``batch`` as the server would.

        Raises:
            ValueError: if a key column is missing or differs from the key schema, the
                schema differs from the stored one beyond nullability, a struct column
                holds a struct-level null, or a non-nullable top-level column holds
                nulls.
        """
        self._spiral.log.append((self._project_id, "write", self._ident))
        self._spiral.written_schemas.setdefault(
            (self._project_id, self._ident), []
        ).append(batch.schema)
        keys = self._stored.key_schema
        for k in keys:
            if k.name not in batch.schema.names:
                raise ValueError(f"write is missing key column {k.name!r}")
            if stored_schema(pa.schema([batch.schema.field(k.name)])).field(0) != k:
                raise ValueError(
                    f"key column {batch.schema.field(k.name)} differs from key schema {k}"
                )
        data = self._stored.data
        if data is not None and stored_schema(batch.schema) != data.schema:
            raise ValueError(f"write schema {batch.schema} differs from {data.schema}")
        for i, field in enumerate(batch.schema):
            col = batch.column(i)
            if pa.types.is_struct(field.type) and col.null_count:
                raise ValueError("SpiralDB does not allow struct-level nulls")
            if not field.nullable and col.null_count:
                raise ValueError(
                    f"Column '{field.name}' is declared as non-nullable but contains "
                    "null values"
                )

    def _commit(self, incoming: pa.Table) -> None:
        """Upsert ``incoming`` by key, stored nullable, keeping rows sorted by key."""
        stored = self._stored
        keys = stored.key_schema.names
        incoming = pa.Table.from_arrays(
            incoming.columns, schema=stored_schema(incoming.schema)
        )
        if stored.data is not None:
            new_keys = set(
                zip(*(incoming.column(k).to_pylist() for k in keys), strict=True)
            )
            keep = pa.array(
                [
                    k not in new_keys
                    for k in zip(
                        *(stored.data.column(k).to_pylist() for k in keys), strict=True
                    )
                ],
                pa.bool_(),
            )
            incoming = pa.concat_tables([stored.data.filter(keep), incoming])
        stored.data = incoming.sort_by(
            [(k, "ascending") for k in keys]
        ).combine_chunks()


class FakeScan:
    """A scan result, as ``Spiral.scan`` returns it."""

    def __init__(self, table: pa.Table, batch_rows: int) -> None:
        """Hold ``table``, read back in batches of ``batch_rows`` rows."""
        self._table = table
        self._batch_rows = batch_rows

    def to_record_batches(self) -> pa.RecordBatchReader:
        """Return a reader over the rows in batches of ``batch_rows`` rows."""
        batches = self._table.to_batches(max_chunksize=self._batch_rows)
        return pa.RecordBatchReader.from_batches(self._table.schema, batches)

    def to_table(self) -> pa.Table:
        """Return every row as one table."""
        return self._table


@dataclasses.dataclass
class ProjectInfo:
    """An entry of ``Spiral.list_projects``, as ``create_project`` also returns."""

    id: str
    description: str = ""


class FakeProject:
    """A project, as ``Spiral.project`` returns it."""

    def __init__(self, spiral: FakeSpiral, project_id: str) -> None:
        """Refer to project ``project_id``."""
        self._spiral = spiral
        self.id = project_id

    @property
    def _tables(self) -> dict[str, _Stored]:
        """Return the project's stored tables by identifier."""
        return self._spiral._projects.setdefault(self.id, {})

    def list_tables(self) -> list[TableResource]:
        """Return the project's tables, sorted by identifier."""
        return [
            TableResource(self.id, *ident.split(".", 1))
            for ident in sorted(self._tables)
        ]

    def table(self, identifier: str) -> FakeTable:
        """Return the table ``identifier``.

        Raises:
            ValueError: if the table does not exist.
        """
        if identifier not in self._tables:
            raise ValueError(f"Table not found: {self.id}.{identifier}")
        return FakeTable(self._spiral, self.id, identifier)

    def create_table(self, identifier: str, *, key_schema: pa.Schema) -> FakeTable:
        """Create an empty table keyed by ``key_schema`` (stored nullable; logged).

        Raises:
            ValueError: if the table already exists.
        """
        self._spiral.log.append((self.id, "create_table", identifier))
        if identifier in self._tables:
            raise ValueError(f"Table already exists: {identifier}")
        self._tables[identifier] = _Stored(stored_schema(key_schema))
        return FakeTable(self._spiral, self.id, identifier)

    def drop_table(self, identifier: str) -> None:
        """Drop the table ``identifier`` (logged)."""
        self._spiral.log.append((self.id, "drop_table", identifier))
        del self._tables[identifier]


class FakeSpiral:
    """The client. ``batch_rows`` sets how many rows each scanned batch holds."""

    def __init__(self, *, batch_rows: int = 3) -> None:
        """Start with no projects; scans return batches of ``batch_rows`` rows."""
        self._projects: dict[str, dict[str, _Stored]] = {}
        self._descriptions: dict[str, str] = {}
        self._suffix = itertools.count(100001)
        self.batch_rows = batch_rows
        self.log: list[tuple[str, str, str]] = []
        self.written_schemas: dict[tuple[str, str], list[pa.Schema]] = {}

    # -- setup helpers for tests (not part of pyspiral) --------------------------
    def add_table(
        self, project_id: str, identifier: str, table: pa.Table, keys: list[str]
    ) -> None:
        """Store ``table`` as-is (setup only; not logged)."""
        projects = self._projects.setdefault(project_id, {})
        key_schema = pa.schema([table.schema.field(k) for k in keys])
        projects[identifier] = _Stored(
            key_schema, table.sort_by([(k, "ascending") for k in keys])
        )
        self._descriptions.setdefault(project_id, "")

    def rows(self, project_id: str, identifier: str) -> pa.Table:
        """Return the stored rows of a table (test helper, not part of pyspiral)."""
        data = self._projects[project_id][identifier].data
        assert data is not None
        return data

    # -- pyspiral surface ---------------------------------------------------------
    def project(self, project_id: str) -> FakeProject:
        """Return the project ``project_id``."""
        return FakeProject(self, project_id)

    def list_projects(self) -> list[ProjectInfo]:
        """Return every project with its description, sorted by ID."""
        return [
            ProjectInfo(p, self._descriptions.get(p, ""))
            for p in sorted(self._projects)
        ]

    def create_project(
        self, *, id_prefix: str, description: str, volume_id: str | None = None
    ) -> ProjectInfo:
        """Create a project named ``id_prefix`` plus a numeric suffix (logged)."""
        pid = f"{id_prefix}-{next(self._suffix)}"
        self.log.append((pid, "create_project", volume_id or ""))
        self._projects[pid] = {}
        self._descriptions[pid] = description
        return ProjectInfo(pid, description)

    def scan(
        self, projection: FakeTable | Select, *, where: Expr | None = None
    ) -> FakeScan:
        """Scan a table or a projection, optionally filtered by ``where``.

        Raises:
            ValueError: if the projection selects key columns only, which SpiralDB does
                not support, or a comparison literal's type differs from its column's.
        """
        table = projection.table if isinstance(projection, Select) else projection
        data = table._stored.data
        if data is None:
            data = table._stored.key_schema.empty_table()
        if isinstance(projection, Select):
            keys = set(table._stored.key_schema.names)
            if set(projection.columns) <= keys:
                raise ValueError(
                    "Reading key columns without including at least one column from "
                    "any column group is currently not supported"
                )
            data = data.select(list(projection.columns))
        if where is not None:
            data = data.filter(where.evaluate(data))
        return FakeScan(data, self.batch_rows)


def iter_rows(table: pa.Table) -> Iterator[dict[str, Any]]:
    """Yield the rows of ``table`` as dictionaries."""
    yield from table.to_pylist()
