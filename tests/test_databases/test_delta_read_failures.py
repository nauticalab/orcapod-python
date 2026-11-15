"""Regression tests for ITL-884: a Delta table that cannot be read is never "no rows".

Covers:
- A table that exists but cannot be read raises ``OrcapodDatabaseReadError`` on every read
  path, naming the table; a table that does not exist still reads as ``None``.
- A table whose log cannot be loaded is never overwritten by a flush.
- ``flush`` raises ``OrcapodDatabaseReadError`` and the append refusal's
  ``InputValidationError`` themselves, not wrapped in ``RuntimeError``, and
  notes every other batch that failed in the same flush.
- An append whose column types differ from the stored table is refused before
  anything is written, naming the column and both types; legitimate appends
  (same types in another layout, new or missing columns, unchanged extension
  storage) still succeed.
- A readable table whose stored user-class shape differs from the registered class
  raises ``TypeIdentityError``, not ``OrcapodDatabaseReadError`` (ITL-881).
"""

from __future__ import annotations

import dataclasses
import datetime
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
import pytest
from deltalake.exceptions import DeltaProtocolError

from orcapod.databases import DeltaTableDatabase
from orcapod.errors import (
    InputValidationError,
    OrcapodDatabaseReadError,
    TypeIdentityError,
)
from orcapod.sources import DictSource

PATH = ("results", "t")


def rows(ids: list[bytes], x: pa.Array[Any], **extra: pa.Array[Any]) -> pa.Table:
    """Build a table keyed by ``rid`` with an ``x`` column and optional extras."""
    return pa.table({"rid": pa.array(ids, pa.large_binary()), "x": x, **extra})


def table_dir(base: Path) -> Path:
    """Return the directory of the table at ``PATH`` under ``base``."""
    return base.joinpath(*PATH)


def snapshot(directory: Path) -> dict[str, bytes]:
    """Every file under ``directory`` with its bytes, keyed by relative path."""
    return {
        str(p.relative_to(directory)): p.read_bytes()
        for p in sorted(directory.rglob("*"))
        if p.is_file()
    }


class _Ext(pa.ExtensionType):
    """A minimal extension type standing in for a logical type's storage."""

    def __init__(self, storage: pa.DataType) -> None:
        """Wrap ``storage`` in the test extension type."""
        super().__init__(storage, "orcapod.test.itl884")

    def __arrow_ext_serialize__(self) -> bytes:
        """Return the (empty) serialized parameters of the type."""
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "_Ext":
        """Rebuild the type over ``storage_type``."""
        return cls(storage_type)


def ext_array(values: list[Any], storage: pa.DataType) -> pa.ExtensionArray[Any]:
    """Build an extension array of ``values`` over the given storage type."""
    return pa.ExtensionArray.from_storage(_Ext(storage), pa.array(values, storage))


@pytest.fixture
def base(tmp_path: Path) -> Path:
    """Return the database root, inside the test's temporary directory."""
    return tmp_path / "store"


@pytest.fixture
def stored(base: Path) -> DeltaTableDatabase:
    """A store holding two rows (``a``, ``b``) of ``x: int64`` at ``PATH``."""
    db = DeltaTableDatabase(base_path=base)
    db.add_records(
        PATH,
        rows([b"a", b"b"], pa.array([1, 2], pa.int64())),
        record_id_column="rid",
        flush=True,
    )
    return db


@pytest.fixture
def unreadable(base: Path, stored: DeltaTableDatabase) -> DeltaTableDatabase:
    """A fresh view of a table whose log is intact but whose data files are gone."""
    data_files = list(table_dir(base).glob("*.parquet"))
    assert data_files
    for f in data_files:
        f.unlink()
    return DeltaTableDatabase(base_path=base)


def unsupported_reader_feature(base: Path) -> None:
    """Make the table's log require a reader feature deltalake does not know."""
    log = table_dir(base) / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "protocol" in action:
            action["protocol"] = {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["orcapodMadeUpFeature"],
                "writerFeatures": ["orcapodMadeUpFeature"],
            }
    log.write_text("\n".join(json.dumps(a) for a in actions))


def assert_names_table(exc: OrcapodDatabaseReadError, base: Path) -> None:
    """Assert that ``exc`` names the table at ``PATH`` and chains its cause."""
    assert Path(exc.table) == table_dir(base)
    assert exc.table in str(exc)
    assert exc.__cause__ is not None


# ---------------------------------------------------------------------------
# Reads: unreadable table raises, absent table is None
# ---------------------------------------------------------------------------

READS: dict[str, Callable[[DeltaTableDatabase], object]] = {
    "get_all_records": lambda db: db.get_all_records(PATH),
    "get_records_by_ids": lambda db: db.get_records_by_ids(PATH, [b"a"]),
    "get_record_by_id": lambda db: db.get_record_by_id(PATH, b"a"),
    "get_records_with_column_value": lambda db: db.get_records_with_column_value(
        PATH, {"x": 1}
    ),
}


class TestUnreadableTable:
    """A table that exists but cannot be read raises on every read path."""

    @pytest.mark.parametrize("read", READS.values(), ids=list(READS))
    def test_read_raises_store_read_error(self, base, unreadable, read):
        """Each read of the unreadable table raises ``OrcapodDatabaseReadError`` naming it."""
        with pytest.raises(OrcapodDatabaseReadError) as info:
            read(unreadable)
        assert_names_table(info.value, base)

    def test_skip_duplicates_append_raises_store_read_error(self, base, unreadable):
        """The existing-ids read for ``skip_duplicates`` raises, never assumes none."""
        # skip_duplicates reads the stored record ids before buffering the rows.
        with pytest.raises(OrcapodDatabaseReadError) as info:
            unreadable.add_records(
                PATH,
                rows([b"c"], pa.array([3], pa.int64())),
                record_id_column="rid",
                skip_duplicates=True,
            )
        assert_names_table(info.value, base)

    def test_table_still_exists(self, unreadable):
        """The unreadable table is still reported as existing."""
        assert unreadable.table_exists(PATH)


def test_bad_filter_is_not_reported_as_unreadable_table(stored):
    """A bad filter column is the caller's error, not a ``OrcapodDatabaseReadError``."""
    # The table reads fine; the filter names a column it does not have.
    with pytest.raises(pa.ArrowInvalid):
        stored.get_records_with_column_value(PATH, {"no_such_column": 1})


class TestAbsentTable:
    """A table that does not exist still reads as ``None``."""

    @pytest.mark.parametrize("read", READS.values(), ids=list(READS))
    def test_read_returns_none(self, base, read):
        """Each read of a table that was never written returns ``None``."""
        db = DeltaTableDatabase(base_path=base)
        assert read(db) is None
        assert not db.table_exists(PATH)


# ---------------------------------------------------------------------------
# Flush never overwrites a table it could not load
# ---------------------------------------------------------------------------


class TestUnloadableTableIsNeverOverwritten:
    """A table whose log cannot be loaded is never replaced by a flush."""

    def test_unsupported_reader_feature(self, base, stored):
        """An unloadable log makes reads and the flush raise; nothing is written."""
        unsupported_reader_feature(base)
        before = snapshot(table_dir(base))
        db = DeltaTableDatabase(base_path=base)

        with pytest.raises(OrcapodDatabaseReadError):
            db.get_all_records(PATH)
        with pytest.raises(OrcapodDatabaseReadError) as info:
            db.add_records(
                PATH,
                rows([b"c"], pa.array([3], pa.int64())),
                record_id_column="rid",
                flush=True,
            )
        assert_names_table(info.value, base)
        assert snapshot(table_dir(base)) == before

    def test_protocol_error_on_load_is_not_treated_as_absent(
        self, base, stored, monkeypatch
    ):
        """A ``DeltaProtocolError`` on load makes the flush raise, not overwrite."""
        # deltalake 1.0.2 does not raise DeltaProtocolError for a local table we can
        # build here, so the load is replaced by one that raises it for this table.
        # Before ITL-884 the flush treated this error as "no table" and wrote the
        # batch with mode="overwrite", replacing the stored rows.
        real = deltalake.DeltaTable
        target = table_dir(base)

        def failing_load(table_uri, *args, **kwargs):
            """Raise ``DeltaProtocolError`` for the target table only."""
            if Path(str(table_uri)) == target:
                raise DeltaProtocolError("simulated")
            return real(table_uri, *args, **kwargs)

        before = snapshot(target)
        db = DeltaTableDatabase(base_path=base)
        monkeypatch.setattr(deltalake, "DeltaTable", failing_load)
        with pytest.raises(OrcapodDatabaseReadError) as info:
            db.add_records(
                PATH,
                rows([b"c"], pa.array([3], pa.int64())),
                record_id_column="rid",
                flush=True,
            )
        monkeypatch.undo()

        assert isinstance(info.value.__cause__, DeltaProtocolError)
        assert snapshot(target) == before
        stored_rows = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert stored_rows is not None
        assert stored_rows.num_rows == 2


class TestFlushRaisesStoreErrorsThemselves:
    """``flush`` raises database errors as themselves, with the other failures noted."""

    def test_store_read_error_is_raised_with_a_note_per_other_failed_batch(
        self, base, stored
    ):
        """The ``OrcapodDatabaseReadError`` is raised and the refused batch is noted on it."""
        other = ("results", "u")
        stored.add_records(
            other,
            rows([b"a"], pa.array([1], pa.int64())),
            record_id_column="rid",
            flush=True,
        )
        unsupported_reader_feature(base)
        db = DeltaTableDatabase(base_path=base)
        # Queued first: refused at flush, because x is stored as int64.
        db.add_records(other, rows([b"b"], pa.array([2.0])), record_id_column="rid")
        # Queued second: its table's log cannot be loaded.
        db.add_records(
            PATH, rows([b"c"], pa.array([3], pa.int64())), record_id_column="rid"
        )

        with pytest.raises(OrcapodDatabaseReadError) as info:
            db.flush()

        # The OrcapodDatabaseReadError wins over the earlier refusal, because it is the one
        # that fails a run whatever the error policy; the refusal is noted on it.
        assert_names_table(info.value, base)
        notes = getattr(info.value, "__notes__", [])
        assert len(notes) == 1
        assert "results/u" in notes[0]
        assert "InputValidationError" in notes[0]
        # Both failed batches stay queued, so the next flush fails the same way.
        with pytest.raises(OrcapodDatabaseReadError):
            db.flush()

    def test_other_flush_failures_still_raise_runtime_error(self, base, monkeypatch):
        """A flush that fails for another reason still raises ``RuntimeError``."""

        def failing_write(*args, **kwargs):
            """Raise ``OSError`` for every write."""
            raise OSError("simulated disk failure")

        db = DeltaTableDatabase(base_path=base)
        db.add_records(
            PATH, rows([b"a"], pa.array([1], pa.int64())), record_id_column="rid"
        )
        monkeypatch.setattr(deltalake, "write_deltalake", failing_write)

        with pytest.raises(RuntimeError, match="Failed to flush 1 batch") as info:
            db.flush()
        assert isinstance(info.value.__cause__, OSError)


# ---------------------------------------------------------------------------
# Append refusal: a column's type differs from the stored table
# ---------------------------------------------------------------------------


def append(db: DeltaTableDatabase, table: pa.Table) -> None:
    """Add ``table`` at ``PATH`` and flush it."""
    db.add_records(PATH, table, record_id_column="rid", flush=True)


class TestChangedColumnTypeIsRefused:
    """An append whose column type differs from the stored table is refused."""

    @pytest.mark.parametrize(
        ("new_type", "value", "named"),
        [
            (pa.float64(), 2.0, "double"),
            (pa.int32(), 2, "int32"),
            (pa.large_string(), "2", "string"),
            (pa.timestamp("ns"), datetime.datetime(2020, 1, 2), "timestamp"),
        ],
        ids=[
            "int64->float64",
            "int64->int32",
            "int64->large_string",
            "int64->timestamp[ns]",
        ],
    )
    def test_plain_type_change(self, base, stored, new_type, value, named):
        """A changed column type is refused, naming the column and both types."""
        with pytest.raises(InputValidationError) as info:
            append(stored, rows([b"c"], pa.array([value], new_type)))
        message = str(info.value)
        assert "'x'" in message
        assert "int64" in message
        assert named in message
        assert str(table_dir(base)) in message

        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        assert back.num_rows == 2
        assert back.schema.field("x").type == pa.int64()

    def test_extension_storage_change(self, base):
        """A changed extension storage type is refused and the table stays readable."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], ext_array([b"x"], pa.large_binary())))

        with pytest.raises(InputValidationError) as info:
            append(
                db,
                rows([b"b"], ext_array([[1.0]], pa.large_list(pa.float32()))),
            )
        message = str(info.value)
        assert "'x'" in message
        assert "binary" in message
        assert "list" in message

        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        assert back.num_rows == 1

    def test_nested_field_type_change(self, base: Path) -> None:
        """A changed type of an existing struct field is refused."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([{"a": 1}], pa.struct([("a", pa.int64())]))))

        with pytest.raises(InputValidationError) as info:
            append(
                db,
                rows([b"b"], pa.array([{"a": 2.0}], pa.struct([("a", pa.float64())]))),
            )
        message = str(info.value)
        assert "'x'" in message
        assert "struct<a: int64>" in message
        assert "struct<a: double>" in message

        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        assert back.num_rows == 1
        assert back.schema.field("x").type == pa.struct([("a", pa.int64())])


class TestLegitimateAppendsStillPass:
    """Appends whose types match the stored table, as Delta stores them, succeed."""

    def read_back(self, base: Path) -> pa.Table:
        """Read every row at ``PATH`` with a fresh database view."""
        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        return back

    def test_identical_schema(self, base, stored):
        """An append with the stored schema succeeds."""
        append(stored, rows([b"c"], pa.array([3], pa.int64())))
        assert self.read_back(base).num_rows == 3

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            (pa.array(["a"], pa.string()), pa.array(["b"], pa.large_string())),
            (pa.array(["a"], pa.large_string()), pa.array(["b"], pa.string())),
            (
                pa.array([[1]], pa.list_(pa.int64())),
                pa.array([[2]], pa.large_list(pa.int64())),
            ),
            (pa.array([b"a"], pa.binary()), pa.array([b"b"], pa.large_binary())),
        ],
        ids=["string->large", "large->string", "list->large", "binary->large"],
    )
    def test_large_and_non_large_layouts(self, base, first, second):
        """Large and non-large layouts of one type count as the same type."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], first))
        append(db, rows([b"b"], second))
        assert self.read_back(base).num_rows == 2

    def test_new_column(self, base, stored):
        """An append that adds a column succeeds."""
        append(
            stored,
            rows([b"c"], pa.array([3], pa.int64()), y=pa.array(["new"])),
        )
        back = self.read_back(base)
        assert back.num_rows == 3
        assert "y" in back.column_names

    def test_new_struct_field(self, base: Path) -> None:
        """An append that adds a field to a stored struct column succeeds."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([{"a": 1}], pa.struct([("a", pa.int64())]))))
        evolved = pa.struct([("a", pa.int64()), ("b", pa.int64())])
        append(db, rows([b"b"], pa.array([{"a": 2, "b": 3}], evolved)))

        back = DeltaTableDatabase(base_path=base).get_all_records(
            PATH, record_id_column="rid"
        )
        assert back is not None
        back = back.sort_by("rid")
        assert back.schema.field("x").type == evolved
        assert back.column("x").to_pylist() == [
            {"a": 1, "b": None},
            {"a": 2, "b": 3},
        ]

    def test_new_struct_field_inside_a_list(self, base: Path) -> None:
        """An append that adds a field to a struct inside a list column succeeds."""
        db = DeltaTableDatabase(base_path=base)
        stored_type = pa.large_list(pa.struct([("a", pa.int64())]))
        append(db, rows([b"a"], pa.array([[{"a": 1}]], stored_type)))
        evolved = pa.large_list(pa.struct([("a", pa.int64()), ("b", pa.int64())]))
        append(db, rows([b"b"], pa.array([[{"a": 2, "b": 3}]], evolved)))

        back = DeltaTableDatabase(base_path=base).get_all_records(
            PATH, record_id_column="rid"
        )
        assert back is not None
        back = back.sort_by("rid")
        assert back.schema.field("x").type == evolved
        assert back.column("x").to_pylist() == [
            [{"a": 1, "b": None}],
            [{"a": 2, "b": 3}],
        ]

    def test_missing_column(self, base):
        """An append that omits a stored column succeeds."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([1], pa.int64()), y=pa.array(["old"])))
        append(db, rows([b"b"], pa.array([2], pa.int64())))
        assert self.read_back(base).num_rows == 2

    def test_extension_type_with_unchanged_storage(self, base):
        """An extension type with unchanged storage appends."""
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], ext_array([b"x"], pa.large_binary())))
        append(db, rows([b"b"], ext_array([b"y"], pa.large_binary())))
        assert self.read_back(base).num_rows == 2

    def test_nullability_only_difference_is_accepted(self, base):
        """A nullability-only difference is accepted (pinned, ITL-842)."""
        # Pins today's behaviour: the append check ignores nullability (ITL-884,
        # decision D3). Whether a nullability change should be refused belongs to
        # the nullability audit, ITL-842. The read-back assertion records what
        # Delta does on merge: the stored non-null child becomes nullable, so the
        # rows written first now read back with a different (looser) type.
        strict = pa.struct([pa.field("a", pa.int64(), nullable=False)])
        loose = pa.struct([pa.field("a", pa.int64(), nullable=True)])
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([{"a": 1}], strict)))
        append(db, rows([b"b"], pa.array([{"a": 2}], loose)))

        back = self.read_back(base)
        assert back.num_rows == 2
        assert back.schema.field("x").type == loose


# ---------------------------------------------------------------------------
# A readable table whose class differs is not an unreadable table (ITL-881)
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _StoredShape:
    """A registered user class; the table below stores another shape under its name."""

    x: int
    y: int


@pytest.fixture
def other_shape(base: Path) -> DeltaTableDatabase:
    """A readable table storing ``_StoredShape``'s type name over another struct."""
    value: Any = _StoredShape(1, 2)
    source = DictSource(data=[{"k": "a", "p": value}], tag_columns=["k"])
    ext = source.as_table().schema.field("p").type
    wrong = pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.int64())])
    field = pa.field("x", wrong).with_metadata(
        {
            b"ARROW:extension:name": ext.extension_name.encode(),
            b"ARROW:extension:metadata": ext.__arrow_ext_serialize__(),
        }
    )
    table = pa.table(
        {
            "__record_id": pa.array([b"a"], pa.large_binary()),
            "x": pa.array([{"x": 1.5, "y": 2}], type=wrong),
        },
        schema=pa.schema([pa.field("__record_id", pa.large_binary()), field]),
    )
    deltalake.write_deltalake(str(table_dir(base)), table)
    return DeltaTableDatabase(base_path=base)


READS_WITHOUT_VALUE_FILTER = {
    name: read
    for name, read in READS.items()
    if name != "get_records_with_column_value"
}


@pytest.mark.parametrize(
    "read",
    READS_WITHOUT_VALUE_FILTER.values(),
    ids=list(READS_WITHOUT_VALUE_FILTER),
)
def test_class_mismatch_is_type_identity_error_not_store_read_error(other_shape, read):
    """A stored shape that differs from the registered class raises ``TypeIdentityError``.

    ``OrcapodDatabaseReadError`` means the table cannot be read. Here the table is intact and the
    class differs, so the identity error is raised as is, never wrapped.
    """
    with pytest.raises(TypeIdentityError) as info:
        read(other_shape)
    assert not isinstance(info.value, OrcapodDatabaseReadError)
