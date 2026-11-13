"""A disk-backed fake of the pyspiral surface that ``SpiralDBConnector`` uses.

The unit tests for ``SpiralDBConnector`` and ``SpiralDBTableSource`` replace pyspiral (or the
whole connector) with ``MagicMock`` objects, so they never push a real Arrow table through the
connector's read path. This fake does: it stores real Arrow tables and returns them the way
SpiralDB does, so a source built on it goes through the same code as one built on a live
project.

Only the calls the connector makes are implemented:

* ``Spiral(overrides=...)``, ``Spiral.project(id)``,
  ``Spiral.scan(expr, where=..., limit=..., asof=...)``, ``Spiral.core.manifest_stats``
  (each call is recorded in ``scan_log``; batches are read on demand)
* ``Project.list_tables()`` (``TableResource`` with ``.dataset`` / ``.table``),
  ``Project.table(id)``, ``Project.create_table(id, key_schema=..., exist_ok=...)``,
  ``Project.drop_table(id)``
* ``Table.key_schema``, ``Table.table_id``, ``Table.last_modified_at()``, ``Table[name] == v``,
  ``Table.core.get_snapshot()``, ``Table.schema().to_arrow()``, ``Table.select()``, ``Table.write(t)``,
  ``Table.get_metadata()``, ``Table.set_metadata(kv)``, ``Table.drop_metadata(keys)``
* ``Scan.to_record_batches()``, ``Scan.to_table()``

Tables live under a directory (``FAKE_SPIRAL_ROOT`` or the ``root`` given to ``install``), one
Arrow IPC file per table, so a second Python process sees the tables the first one wrote.

Storage behaviour. Each way SpiralDB changes data on the way in or out is a switch in
``StorageBehaviour``; ``observed`` says whether it was seen on a live project (ITL-40, the
connector's docstrings, pyspiral's own documentation) or is an assumption to confirm live:

* ``string_as_utf8`` (observed, ITL-40): strings come back as ``string`` even when written as
  ``large_string``. Applied at every nesting level (assumed for nested).
* ``binary_as``: ``"binary"`` (observed on pyspiral 0.11-0.14, ITL-40) or ``"large_binary"``
  (what pyspiral 0.14.9's ``Scan.to_record_batches(disable_view_types=True)`` documents).
* ``drop_timezone`` (observed): ``timestamp[us, tz=UTC]`` comes back as ``timestamp[us]``.
* ``drop_arrow_metadata`` (observed, ITL-471): schema and field metadata are dropped; the
  table's key-value store (``set_metadata``) round-trips.
* ``decode_dictionaries`` (assumed): dictionary columns come back as their value type.
* ``server_nullability`` (observed, pyspiral 0.16.33, ITL-936): SpiralDB reports every
  top-level column nullable, key columns included, and every struct field nullable, whatever
  was written; only list and fixed-size-list items keep a non-null flag. A null in a
  top-level column declared non-null is refused on write (Arrow validates the outgoing batch
  on the client). A null in a struct field declared non-null is accepted and stored as the
  type's zero value (a null written to an ``int32`` field read back as ``0``). Switch it off
  only to model a store that keeps nullability.
* ``reject_struct_nulls`` (documented by pyspiral ``Table.write``): a null struct value is
  refused unless written with ``push_down_nulls=True``, which turns it into a struct of null
  fields (observed live, ITL-936; top-level struct columns only in the fake).
* Rows come back sorted by key and writes upsert by key (SpiralDB is a key-sorted store).
"""

from __future__ import annotations

import json
import os
import types
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc

_ROOT_ENV = "FAKE_SPIRAL_ROOT"
_BEHAVIOUR_FILE = "_behaviour.json"
# pyspiral 0.16.33 ``Table.set_metadata`` limits.
_MAX_PAIR_BYTES = 8192
_MAX_KEY_CHARS = 255
_MAX_ENTRIES = 1024


@dataclass
class ScanRecord:
    """One ``Spiral.scan(...)`` call: the table, its keyword arguments, batches handed out."""

    identifier: str
    kwargs: dict[str, Any]
    batches_read: int = 0


# Every scan opened in this process, oldest first. Tests clear it and read it.
scan_log: list[ScanRecord] = []
# Rows per batch a scan hands out (SpiralDB's batches follow its stored fragments).
scan_batch_rows = 2


@dataclass(frozen=True)
class StorageBehaviour:
    """How the fake transforms data, mirroring SpiralDB's storage behaviour."""

    string_as_utf8: bool = True
    binary_as: str = "binary"
    drop_timezone: bool = True
    drop_arrow_metadata: bool = True
    decode_dictionaries: bool = True
    server_nullability: bool = True
    reject_struct_nulls: bool = True


def _stored_type(t: pa.DataType, b: StorageBehaviour) -> pa.DataType:
    """Return the Arrow type SpiralDB hands back for a column written as ``t``.

    Args:
        t: The type that was written.
        b: The storage behaviour to apply.

    Returns:
        The stored type, with the same nesting and field names. Struct fields become
        nullable under ``server_nullability``; list items keep their nullability.
    """
    if isinstance(t, pa.ExtensionType):
        # Vortex stores the storage type; the extension name lives in field metadata,
        # which is dropped.
        return _stored_type(t.storage_type, b)
    if pa.types.is_dictionary(t) and b.decode_dictionaries:
        return _stored_type(t.value_type, b)
    if (pa.types.is_string(t) or pa.types.is_large_string(t)) and b.string_as_utf8:
        return pa.string()
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return pa.binary() if b.binary_as == "binary" else pa.large_binary()
    if pa.types.is_timestamp(t) and t.tz is not None and b.drop_timezone:
        return pa.timestamp(t.unit)
    if pa.types.is_struct(t):
        return pa.struct(
            [
                _stored_field(t.field(i), b, nullable=b.server_nullability or None)
                for i in range(t.num_fields)
            ]
        )
    if pa.types.is_map(t):
        # pyarrow accepts key/item fields here; its stubs list only types.
        return pa.map_(  # pyright: ignore[reportCallIssue]
            _stored_field(t.key_field, b),
            _stored_field(t.item_field, b),
            keys_sorted=t.keys_sorted,
        )
    if pa.types.is_large_list(t):
        return pa.large_list(_stored_field(t.value_field, b))
    if pa.types.is_list(t):
        return pa.list_(_stored_field(t.value_field, b))
    if pa.types.is_fixed_size_list(t):
        return pa.list_(_stored_field(t.value_field, b), t.list_size)
    return t


def _stored_field(
    f: pa.Field[Any], b: StorageBehaviour, nullable: bool | None = None
) -> pa.Field[Any]:
    """Return the field SpiralDB hands back for a field written as ``f``.

    Args:
        f: The field that was written.
        b: The storage behaviour to apply.
        nullable: ``True`` to report the field nullable whatever was written; ``None``
            keeps the written flag.
    """
    metadata = None if b.drop_arrow_metadata else f.metadata
    flag = f.nullable if nullable is None else nullable
    return pa.field(f.name, _stored_type(f.type, b), flag, metadata)


def _zero_struct_field_nulls(
    col: pa.ChunkedArray[Any], written: pa.DataType
) -> pa.ChunkedArray[Any]:
    """Store a null in a struct field declared non-null as the type's zero (seen live).

    Only top-level struct columns with primitive fields are modelled; other nulls are kept.
    """
    if not pa.types.is_struct(written):
        return col
    chunks = []
    for chunk in col.chunks:
        struct = cast("pa.StructArray", chunk)
        children = []
        for i in range(written.num_fields):
            child = struct.field(i)
            field = written.field(i)
            if not field.nullable and child.null_count:
                try:
                    child = pc.fill_null(child, pa.scalar(0).cast(field.type))
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    pass
            children.append(child)
        chunks.append(
            pa.StructArray.from_arrays(
                children,
                fields=[
                    pa.field(written.field(i).name, children[i].type, True)
                    for i in range(written.num_fields)
                ],
                mask=struct.is_null(),
            )
        )
    return pa.chunked_array(chunks)


def _push_down_struct_nulls(col: pa.ChunkedArray[Any]) -> pa.ChunkedArray[Any]:
    """Turn each null struct of a top-level struct column into a struct of null fields.

    What ``push_down_nulls=True`` was seen to do live (pyspiral 0.16.33, ITL-936): the
    struct-level null and any values under it are lost.
    """
    if not pa.types.is_struct(col.type) or not col.null_count:
        return col
    chunks = []
    for chunk in col.chunks:
        struct = cast("pa.StructArray", chunk)
        valid = struct.is_valid()
        children = [
            pc.if_else(
                valid, struct.field(i), pa.scalar(None, struct.type.field(i).type)
            )
            for i in range(struct.type.num_fields)
        ]
        chunks.append(
            pa.StructArray.from_arrays(
                children,
                fields=[
                    struct.type.field(i).with_nullable(True)
                    for i in range(struct.type.num_fields)
                ],
            )
        )
    return pa.chunked_array(chunks)


def _has_struct_nulls(arr: pa.ChunkedArray[Any]) -> bool:
    """Return whether ``arr`` is a struct column with a null struct value."""
    return pa.types.is_struct(arr.type) and arr.null_count > 0


class _Store:
    """Directory-backed table store shared by every fake object."""

    def __init__(self, root: Path) -> None:
        """Open the store under ``root`` and load its storage behaviour."""
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        bfile = self.root / _BEHAVIOUR_FILE
        self.behaviour = (
            StorageBehaviour(**json.loads(bfile.read_text()))
            if bfile.exists()
            else StorageBehaviour()
        )

    def _path(self, project_id: str, identifier: str, suffix: str) -> Path:
        # identifiers contain a dot (``dataset.table``), so never use ``with_suffix``
        """Return the file holding ``identifier``'s part named by ``suffix``."""
        return self.root / project_id / f"{identifier}{suffix}"

    def exists(self, project_id: str, identifier: str) -> bool:
        """Return whether table ``identifier`` exists in ``project_id``."""
        return self._path(project_id, identifier, ".keys.json").exists()

    def identifiers(self, project_id: str) -> list[str]:
        """Return every table identifier of ``project_id``, sorted."""
        pdir = self.root / project_id
        if not pdir.exists():
            return []
        return sorted(p.name[: -len(".keys.json")] for p in pdir.glob("*.keys.json"))

    def create(self, project_id: str, identifier: str, key_schema: pa.Schema) -> None:
        """Create table ``identifier`` with ``key_schema`` and empty metadata."""
        (self.root / project_id).mkdir(parents=True, exist_ok=True)
        sink = pa.BufferOutputStream()
        pa.ipc.new_stream(sink, key_schema).close()
        self._path(project_id, identifier, ".keys.arrow").write_bytes(
            sink.getvalue().to_pybytes()
        )
        self._path(project_id, identifier, ".keys.json").write_text(
            json.dumps(key_schema.names)
        )
        self._path(project_id, identifier, ".kv.json").write_text("{}")
        # SpiralDB gives every table its own id; a recreated table gets a new one.
        self._path(project_id, identifier, ".id.txt").write_text(
            f"table_{uuid.uuid4().hex[:12]}"
        )
        self._path(project_id, identifier, ".version.txt").write_text("0")

    def drop(self, project_id: str, identifier: str) -> None:
        """Remove every file of table ``identifier``."""
        for suffix in (
            ".keys.json",
            ".keys.arrow",
            ".kv.json",
            ".data.arrow",
            ".id.txt",
            ".version.txt",
        ):
            p = self._path(project_id, identifier, suffix)
            if p.exists():
                p.unlink()

    def key_schema(self, project_id: str, identifier: str) -> pa.Schema:
        """Return table ``identifier``'s key schema."""
        raw = self._path(project_id, identifier, ".keys.arrow").read_bytes()
        return pa.ipc.open_stream(raw).schema

    def read(self, project_id: str, identifier: str) -> pa.Table | None:
        """Return table ``identifier``'s rows, or ``None`` before the first write."""
        p = self._path(project_id, identifier, ".data.arrow")
        if not p.exists():
            return None
        with pa.ipc.open_file(pa.memory_map(str(p))) as r:
            return r.read_all()

    def write(self, project_id: str, identifier: str, table: pa.Table) -> None:
        """Replace table ``identifier``'s rows with ``table`` and move its version on."""
        p = self._path(project_id, identifier, ".data.arrow")
        with pa.ipc.new_file(str(p), table.schema) as w:
            w.write_table(table)
        self._path(project_id, identifier, ".version.txt").write_text(
            str(self.version(project_id, identifier) + 1)
        )

    def table_id(self, project_id: str, identifier: str) -> str:
        """Return table ``identifier``'s own id."""
        return self._path(project_id, identifier, ".id.txt").read_text()

    def version(self, project_id: str, identifier: str) -> int:
        """Return table ``identifier``'s version: ``0`` before the first write."""
        return int(self._path(project_id, identifier, ".version.txt").read_text())

    def kv(self, project_id: str, identifier: str) -> dict[str, bytes]:
        """Return table ``identifier``'s key-value metadata."""
        raw = json.loads(self._path(project_id, identifier, ".kv.json").read_text())
        return {k: bytes.fromhex(v) for k, v in raw.items()}

    def set_kv(self, project_id: str, identifier: str, kv: dict[str, bytes]) -> None:
        """Upsert ``kv`` into table ``identifier``'s key-value metadata."""
        current = self.kv(project_id, identifier)
        current.update(kv)
        self._path(project_id, identifier, ".kv.json").write_text(
            json.dumps({k: v.hex() for k, v in current.items()})
        )

    def drop_kv(self, project_id: str, identifier: str, keys: list[str]) -> None:
        """Remove ``keys`` from table ``identifier``'s key-value metadata."""
        current = self.kv(project_id, identifier)
        for key in keys:
            current.pop(key, None)
        self._path(project_id, identifier, ".kv.json").write_text(
            json.dumps({k: v.hex() for k, v in current.items()})
        )


@dataclass(frozen=True)
class TableResource:
    """What ``Project.list_tables()`` returns per table."""

    id: str
    project_id: str
    dataset: str
    table: str


class FakeSchema:
    """What ``Table.schema()`` returns: a wrapper whose ``to_arrow()`` gives the schema."""

    def __init__(self, schema: pa.Schema) -> None:
        """Wrap ``schema``."""
        self._schema = schema

    def to_arrow(self) -> pa.Schema:
        """Return the stored Arrow schema."""
        return self._schema


class FakeExpr:
    """The result of ``Table.select()``: names the table to scan."""

    def __init__(self, table: FakeTable) -> None:
        """Name ``table`` as the table to scan."""
        self.table = table


class FakePredicate:
    """A ``where`` expression: every named column equals its value."""

    def __init__(self, equalities: list[tuple[str, Any]]) -> None:
        """Hold the ``(column, value)`` pairs a row must all match."""
        self.equalities = equalities

    def __and__(self, other: FakePredicate) -> FakePredicate:
        """Return a predicate requiring this one's equalities and ``other``'s."""
        return FakePredicate(self.equalities + other.equalities)

    def __repr__(self) -> str:
        """Return the equalities as ``name == value`` terms joined by ``&``."""
        return " & ".join(f"{name} == {value!r}" for name, value in self.equalities)


class FakeColumn:
    """What ``table[name]`` returns: compare it with ``==`` to get a predicate."""

    def __init__(self, name: str) -> None:
        """Name the column ``name``."""
        self._name = name

    def __eq__(self, value: object) -> FakePredicate:  # pyright: ignore[reportIncompatibleMethodOverride]  # as pyspiral's Expr does
        """Return the predicate that this column equals ``value``."""
        return FakePredicate([(self._name, value)])

    __hash__ = None  # pyright: ignore[reportAssignmentType]  # an expression, not a value


class FakeScan:
    """What ``Spiral.scan(table.select(), where=..., limit=...)`` returns."""

    def __init__(
        self,
        table: FakeTable,
        record: ScanRecord,
        where: FakePredicate | None,
        limit: int | None,
    ) -> None:
        """Prepare a scan of ``table``; no row is read until rows are asked for.

        Args:
            table: The table to scan.
            record: The ``scan_log`` entry that counts the batches handed out.
            where: The equalities a row must match, or ``None`` for every row.
            limit: The most rows to return, or ``None`` for no limit.
        """
        self._table = table
        self._record = record
        self._where = where
        self._limit = limit

    def _rows(self) -> pa.Table:
        """Return the rows matching ``where``, cut to ``limit``, in key order."""
        rows = self._table._read_all()
        for name, value in self._where.equalities if self._where else []:
            rows = rows.filter(
                pc.equal(
                    rows.column(name), pa.scalar(value, rows.schema.field(name).type)
                )
            )
        return rows if self._limit is None else rows.slice(0, self._limit)

    def to_table(self) -> pa.Table:
        """Return the scanned rows, in key order."""
        return self._rows()

    def to_record_batches(self, **kwargs: Any) -> pa.RecordBatchReader:
        """Return the rows in key order, ``scan_batch_rows`` per batch, read on demand."""
        rows = self._rows()

        def batches() -> Iterator[pa.RecordBatch]:
            # SpiralDB yields more than one batch for larger tables; split to exercise that.
            """Yield ``rows`` in batches, counting each in the scan record."""
            for batch in rows.to_batches(max_chunksize=scan_batch_rows):
                self._record.batches_read += 1
                yield batch

        return pa.RecordBatchReader.from_batches(rows.schema, batches())


class FakeTable:
    """A stored table: key schema, Arrow schema, key-value metadata and upserting writes.

    Args:
        store: The directory-backed store.
        project_id: The project the table belongs to.
        identifier: ``"<dataset>.<table>"``.

    Raises:
        ValueError: If the table does not exist.
    """

    def __init__(self, store: _Store, project_id: str, identifier: str) -> None:
        """Open table ``identifier`` of ``project_id`` in ``store``."""
        if not store.exists(project_id, identifier):
            raise ValueError(f"Table not found: {project_id}/{identifier}")
        self._store = store
        self._project_id = project_id
        self._identifier = identifier

    @property
    def key_schema(self) -> pa.Schema:
        """The key columns, in key order."""
        return self._store.key_schema(self._project_id, self._identifier)

    @property
    def table_id(self) -> str:
        """The table's own id: kept on rename, new when the table is recreated."""
        return self._store.table_id(self._project_id, self._identifier)

    @property
    def core(self) -> Any:
        """The table's low-level handle; only ``get_snapshot`` is modelled."""
        return types.SimpleNamespace(get_snapshot=lambda asof=None: self)

    def last_modified_at(self) -> int:
        """Return the table's version: ``0`` before the first write."""
        return self._store.version(self._project_id, self._identifier)

    def __getitem__(self, name: str) -> FakeColumn:
        """Return the column expression ``table[name]``."""
        return FakeColumn(name)

    def schema(self) -> FakeSchema:
        """Return the stored schema (only the key columns before the first write)."""
        stored = self._store.read(self._project_id, self._identifier)
        keys = self.key_schema
        if stored is None:
            return FakeSchema(keys)
        return FakeSchema(stored.schema)

    def select(self, *paths: str, exclude: list[str] | None = None) -> FakeExpr:
        """Return an expression selecting the whole table (projections are not modelled)."""
        if paths or exclude:
            raise NotImplementedError("fake Table.select supports only select()")
        return FakeExpr(self)

    def get_metadata(self) -> dict[str, bytes]:
        """Return the table's key-value metadata."""
        return self._store.kv(self._project_id, self._identifier)

    def set_metadata(self, metadata: dict[str, bytes]) -> None:
        """Upsert ``metadata`` into the table's key-value metadata.

        Enforces the limits pyspiral 0.16.33 documents for ``Table.set_metadata``: str keys
        and bytes values, a key + value pair of at most 8 KiB, keys of at most 255
        characters, and at most 1024 entries per table.

        Raises:
            TypeError: If a key is not ``str`` or a value not ``bytes``.
            ValueError: If a limit is exceeded.
        """
        for k, v in metadata.items():
            if not isinstance(k, str) or not isinstance(v, bytes):
                raise TypeError("metadata keys must be str and values bytes")
            if len(k) > _MAX_KEY_CHARS:
                raise ValueError(f"metadata key {k[:40]!r}... exceeds 255 characters")
            if len(k.encode()) + len(v) > _MAX_PAIR_BYTES:
                raise ValueError(f"metadata entry {k!r} exceeds 8 KiB")
        current = self._store.kv(self._project_id, self._identifier)
        if len({*current, *metadata}) > _MAX_ENTRIES:
            raise ValueError("a table can have at most 1024 metadata entries")
        self._store.set_kv(self._project_id, self._identifier, metadata)

    def drop_metadata(self, keys: list[str]) -> None:
        """Remove ``keys`` from the table's key-value metadata; absent keys are ignored."""
        self._store.drop_kv(self._project_id, self._identifier, keys)

    def write(
        self, table: pa.Table, push_down_nulls: bool = False, **kwargs: Any
    ) -> None:
        """Store ``table`` the way SpiralDB does: transform types, then upsert by key.

        Args:
            table: The rows to write; every key column must be present.
            push_down_nulls: Accept null struct values (SpiralDB's own flag).
            **kwargs: Accepted for signature compatibility; unused.

        Raises:
            ValueError: If a key column is missing, a struct value is null without
                ``push_down_nulls``, or the column set differs from the stored table.
        """
        b = self._store.behaviour
        keys = self.key_schema
        missing = [k for k in keys.names if k not in table.column_names]
        if missing:
            raise ValueError(f"write is missing key columns {missing}")
        if b.reject_struct_nulls and not push_down_nulls:
            for name in table.column_names:
                if _has_struct_nulls(table.column(name)):
                    raise ValueError(
                        f"SpiralDB does not allow struct-level nulls (column {name!r}); "
                        "write with push_down_nulls=True"
                    )
        fields = []
        arrays = []
        for f in table.schema:
            col = table.column(f.name)
            if push_down_nulls:
                col = _push_down_struct_nulls(col)
            if b.server_nullability:
                # Arrow validates the outgoing batch on the client (live, ITL-936).
                if not f.nullable and col.null_count:
                    raise ValueError(
                        f"Column {f.name!r} is declared as non-nullable but contains "
                        "null values"
                    )
                col = _zero_struct_field_nulls(col, f.type)
            sf = _stored_field(f, b, nullable=b.server_nullability or None)
            if isinstance(f.type, pa.ExtensionType):
                col = pa.chunked_array(
                    [cast("pa.ExtensionArray[Any]", c).storage for c in col.chunks]
                )
            if pa.types.is_dictionary(col.type) and b.decode_dictionaries:
                col = col.cast(col.type.value_type)
            fields.append(sf)
            arrays.append(_cast_storage(col, sf.type))
        incoming = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        existing = self._store.read(self._project_id, self._identifier)
        if existing is not None:
            if existing.schema.names != incoming.schema.names:
                raise ValueError(
                    "fake SpiralDB: column set differs from existing table"
                )
            incoming = incoming.cast(existing.schema)
            if not keys.names:
                incoming = pa.concat_tables([existing, incoming])
            else:
                incoming = _upsert(existing, incoming, keys.names)
        if keys.names:
            incoming = incoming.sort_by([(k, "ascending") for k in keys.names])
        self._store.write(self._project_id, self._identifier, incoming.combine_chunks())

    def _read_all(self) -> pa.Table:
        """Return every stored row (an empty key-only table before the first write)."""
        stored = self._store.read(self._project_id, self._identifier)
        if stored is None:
            keys = self.key_schema
            return keys.empty_table()
        return stored


def _upsert(existing: pa.Table, incoming: pa.Table, key_names: list[str]) -> pa.Table:
    """Replace existing rows whose key appears in ``incoming`` and append the rest."""
    new_keys = set(zip(*(incoming.column(k).to_pylist() for k in key_names)))
    keep = pa.array(
        [
            k not in new_keys
            for k in zip(*(existing.column(k).to_pylist() for k in key_names))
        ],
        type=pa.bool_(),
    )
    return pa.concat_tables([existing.filter(keep), incoming])


def _cast_storage(
    col: pa.ChunkedArray[Any], target: pa.DataType
) -> pa.ChunkedArray[Any]:
    """Cast ``col`` to the stored type ``target``, keeping the UTC instant of a timestamp."""
    if col.type == target:
        return col
    if pa.types.is_timestamp(col.type) and pa.types.is_timestamp(target):
        # dropping the tz keeps the UTC instant (what SpiralDB was seen to do)
        return pc.cast(col, pa.timestamp(col.type.unit, tz="UTC")).cast(target)
    return col.cast(target)


class FakeProject:
    """What ``Spiral.project(id)`` returns: list, open, create and drop tables."""

    def __init__(self, store: _Store, project_id: str) -> None:
        """Bind project ``project_id`` in ``store``."""
        self._store = store
        self._project_id = project_id

    def list_tables(self) -> list[TableResource]:
        """Return every table of the project, in every dataset."""
        out = []
        for ident in self._store.identifiers(self._project_id):
            dataset, _, table = ident.partition(".")
            table_id = self._store.table_id(self._project_id, ident)
            out.append(TableResource(table_id, self._project_id, dataset, table))
        return out

    def table(self, identifier: str) -> FakeTable:
        """Open the table ``"<dataset>.<table>"``."""
        return FakeTable(self._store, self._project_id, identifier)

    def create_table(
        self,
        identifier: str,
        *,
        key_schema: Any,
        root_uri: Any = None,
        exist_ok: bool = False,
    ) -> FakeTable:
        """Create the table ``"<dataset>.<table>"`` with ``key_schema`` and open it.

        Key columns are stored the way SpiralDB stores them, nullable under
        ``server_nullability`` however they were declared.

        Args:
            identifier: ``"<dataset>.<table>"``.
            key_schema: An Arrow schema, or ``(name, type)`` pairs as pyspiral accepts.
            root_uri: Accepted for signature compatibility; unused.
            exist_ok: Open an existing table instead of raising.

        Returns:
            The new table, or with ``exist_ok`` the existing one.

        Raises:
            ValueError: If the table exists and ``exist_ok`` is false.
        """
        if self._store.exists(self._project_id, identifier):
            if not exist_ok:
                raise ValueError(f"Table already exists: {identifier}")
            return self.table(identifier)
        if isinstance(key_schema, pa.Schema):
            schema = key_schema
        else:
            schema = pa.schema([pa.field(n, t, True) for n, t in key_schema])
        b = self._store.behaviour
        # SpiralDB reports key columns nullable however they were declared (ITL-936).
        schema = pa.schema(
            [
                pa.field(
                    f.name,
                    _stored_type(f.type, b),
                    True if b.server_nullability else f.nullable,
                )
                for f in schema
            ]
        )
        self._store.create(self._project_id, identifier, schema)
        return self.table(identifier)

    def drop_table(self, identifier: str) -> None:
        """Drop the table ``"<dataset>.<table>"`` if it exists."""
        self._store.drop(self._project_id, identifier)


class FakeSpiral:
    """Stands in for ``spiral.Spiral``; tables live under ``FAKE_SPIRAL_ROOT``.

    Args:
        config: Accepted for signature compatibility; unused.
        overrides: Recorded; unused.

    Raises:
        RuntimeError: If ``FAKE_SPIRAL_ROOT`` is not set (use ``install``).
    """

    def __init__(
        self, *, config: Any = None, overrides: dict[str, str] | None = None
    ) -> None:
        """Open the store under ``FAKE_SPIRAL_ROOT`` and record ``overrides``."""
        root = os.environ.get(_ROOT_ENV)
        if not root:
            raise RuntimeError(f"{_ROOT_ENV} is not set; call fake_pyspiral.install()")
        self._store = _Store(Path(root))
        self.overrides = overrides

    def project(self, project_id: str) -> FakeProject:
        """Return project ``project_id`` (created on first use)."""
        return FakeProject(self._store, project_id)

    def scan(self, *projections: Any, **kwargs: Any) -> FakeScan:
        """Return a scan of one ``table.select()``, restricted by ``where`` and ``limit``.

        Every call is appended to ``scan_log``. ``asof`` is accepted and recorded; the
        fake keeps one version of a table. Other projections and ``key_ranges`` are
        not modelled.
        """
        if len(projections) != 1 or not isinstance(projections[0], FakeExpr):
            raise NotImplementedError(
                "fake Spiral.scan supports only scan(table.select())"
            )
        unsupported = {k for k, v in kwargs.items() if v is not None} - {
            "where",
            "limit",
            "asof",
        }
        if unsupported:
            raise NotImplementedError(f"fake Spiral.scan: {sorted(unsupported)}")
        table = projections[0].table
        record = ScanRecord(table._identifier, dict(kwargs))
        scan_log.append(record)
        return FakeScan(table, record, kwargs.get("where"), kwargs.get("limit"))

    @property
    def core(self) -> Any:
        """The client's low-level handle; only ``manifest_stats`` is modelled."""

        def manifest_stats(snapshot: FakeTable) -> Any:
            """Return stats whose ``key_space_stats.total_rows`` counts rows."""
            rows = snapshot._read_all().num_rows
            return types.SimpleNamespace(
                key_space_stats=types.SimpleNamespace(total_rows=rows)
            )

        return types.SimpleNamespace(manifest_stats=manifest_stats)


fake_spiral_module = types.SimpleNamespace(Spiral=FakeSpiral)


def write_behaviour(root: Path, behaviour: StorageBehaviour) -> None:
    """Persist ``behaviour`` so every process using ``root`` applies it."""
    root.mkdir(parents=True, exist_ok=True)
    (root / _BEHAVIOUR_FILE).write_text(json.dumps(asdict(behaviour)))


@contextmanager
def install(root: Path | str):
    """Route ``SpiralDBConnector`` to the fake, with tables stored under ``root``."""
    with (
        patch.dict(os.environ, {_ROOT_ENV: str(root)}),
        patch("orcapod.databases.spiraldb_connector.sp", fake_spiral_module),
    ):
        yield
