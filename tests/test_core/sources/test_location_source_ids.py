"""Every source's default source id names its kind; a given id is used as it is (ITL-1154).

Developer, November 15, 2026: "every source ought to have some default naming convention
for the source id that reflects the category, but we should make it totally overridable."
A default is ``<kind>:<rest>``, the kind being the class's ``SOURCE_KIND`` (its config type
key): ``<kind>:<label>:<hex>`` from a hash, ``<kind>:<location>`` from where the data lives.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from orcapod.contexts import get_default_context
from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DerivedSource,
    DictSource,
    ListSource,
    PollingSource,
    SQLiteTableSource,
)
from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.core.sources.postgresql_table_source import PostgreSQLTableSource
from orcapod.core.sources.source_ids import hash_source_id, location_source_id
from orcapod.core.sources.spiraldb_table_source import SpiralDBTableSource
from orcapod.pipeline.serialization import SOURCE_REGISTRY
from tests.test_channels.test_polling_source import FakeDynamicSource

GIVEN = "a given id: kept as it is, with : and spaces"


# ---------------------------------------------------------------------------
# The one place that builds a location default
# ---------------------------------------------------------------------------


def test_location_source_id_is_kind_then_location() -> None:
    """``location_source_id`` joins the kind and the location with one ``:``."""
    assert location_source_id("csv", "data/x.csv") == "csv:data/x.csv"
    # A ':' in the location is kept: only '::' is refused.
    assert location_source_id("csv", "C:/x.csv") == "csv:C:/x.csv"


@pytest.mark.parametrize(
    ("kind", "location"),
    [("csv", "a::b.csv"), ("csv", ":b.csv"), ("csv", ""), ("", "x"), ("c:sv", "x")],
    ids=[
        "double-colon",
        "leading-colon",
        "empty-location",
        "empty-kind",
        "colon-in-kind",
    ],
)
def test_location_source_id_refuses(kind: str, location: str) -> None:
    """A location with ``::``, an empty location and a bad kind have no default."""
    with pytest.raises(ValueError):
        location_source_id(kind, location)


# ---------------------------------------------------------------------------
# One vocabulary: the kind is the config type key
# ---------------------------------------------------------------------------


ROOT_SOURCES: list[type[Any]] = [
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DerivedSource,
    DictSource,
    ListSource,
    PollingSource,
    SQLiteTableSource,
    PostgreSQLTableSource,
    SpiralDBTableSource,
    DBTableSource,
]


@pytest.mark.parametrize("cls", ROOT_SOURCES, ids=lambda c: c.__name__)
def test_every_root_source_has_a_kind(cls: type[Any]) -> None:
    """Every root source class declares a ``SOURCE_KIND`` with no ``:``."""
    kind = cls.__dict__.get("SOURCE_KIND")
    assert isinstance(kind, str) and kind and ":" not in kind


def test_registry_keys_are_the_kinds() -> None:
    """The source config registry is keyed by each class's ``SOURCE_KIND``."""
    for key, cls in SOURCE_REGISTRY.items():
        assert key == cls.SOURCE_KIND, (key, cls)


# ---------------------------------------------------------------------------
# Location defaults, and a given id kept as it is
# ---------------------------------------------------------------------------


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "rows.csv"
    path.write_text("k,v\na,1\nb,2\n")
    return path


@pytest.fixture
def delta_path(tmp_path: Path) -> Path:
    path = tmp_path / "events"
    write_deltalake(str(path), pa.table({"k": ["a", "b"], "v": [1, 2]}))
    return path


@pytest.fixture
def sqlite_path(tmp_path: Path) -> Path:
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE trials (k TEXT PRIMARY KEY, v INTEGER)")
        conn.execute("INSERT INTO trials VALUES ('a', 1), ('b', 2)")
    return path


def test_csv_default_and_given(csv_path: Path) -> None:
    """``csv:<path as passed>``; a given id is used as it is."""
    assert (
        CSVSource(str(csv_path), tag_columns="k", tag_only_columns=True).source_id
        == f"csv:{csv_path}"
    )
    assert (
        CSVSource(
            str(csv_path), tag_columns="k", source_id=GIVEN, tag_only_columns=True
        ).source_id
        == GIVEN
    )


def test_csv_path_with_double_colon_needs_a_source_id(tmp_path: Path) -> None:
    """A path holding ``::`` cannot be a default id: the source asks for a source_id."""
    path = tmp_path / "a::b.csv"
    path.write_text("k,v\na,1\n")
    with pytest.raises(ValueError, match="source_id"):
        CSVSource(str(path), tag_columns="k", tag_only_columns=True)
    assert (
        CSVSource(
            str(path), tag_columns="k", source_id="ab", tag_only_columns=True
        ).source_id
        == "ab"
    )


def test_delta_default_and_given(delta_path: Path) -> None:
    """``delta_table:<path as passed>``; a given id is used as it is."""
    assert DeltaTableSource(
        delta_path, tag_columns="k", tag_only_columns=True
    ).source_id == (f"delta_table:{delta_path}")
    given = DeltaTableSource(
        delta_path, tag_columns="k", source_id=GIVEN, tag_only_columns=True
    )
    assert given.source_id == GIVEN


def test_sqlite_default_and_given(sqlite_path: Path) -> None:
    """``sqlite_table:<db path as passed>:main.<table>``; a given id is used as is."""
    assert SQLiteTableSource(
        sqlite_path, "trials", tag_only_columns=True
    ).source_id == (f"sqlite_table:{sqlite_path}:main.trials")
    given = SQLiteTableSource(
        sqlite_path, "trials", source_id=GIVEN, tag_only_columns=True
    )
    assert given.source_id == GIVEN


def test_polling_default_is_the_hash_of_the_impl_identity() -> None:
    """``polling_source:<label>:<hex>`` of ``impl.identity()``, never its repr."""
    source = PollingSource(
        FakeDynamicSource(batches=[]), tag_columns="id", record_id_columns="id"
    )
    identity_hash = get_default_context().semantic_hasher.hash_object(
        "FakeDynamicSource"
    )
    assert source.source_id == hash_source_id("polling_source", identity_hash)
    given = PollingSource(
        FakeDynamicSource(batches=[]),
        tag_columns="id",
        record_id_columns="id",
        source_id=GIVEN,
    )
    assert given.source_id == GIVEN


@pytest.mark.parametrize(
    "make",
    [
        lambda: DictSource(
            data=[{"k": "a", "v": 1}],
            tag_columns=["k"],
            source_id=GIVEN,
            tag_only_columns=True,
        ),
        lambda: ArrowTableSource(
            pa.table({"k": ["a"], "v": [1]}),
            tag_columns=["k"],
            source_id=GIVEN,
            tag_only_columns=True,
        ),
        lambda: ListSource(name="item", data=[1, 2], source_id=GIVEN),
    ],
    ids=["dict", "arrow_table", "list"],
)
def test_in_memory_sources_keep_a_given_id(make: Any) -> None:
    """A given id is used as it is by the in-memory sources too."""
    assert make().source_id == GIVEN


def test_a_given_source_id_with_the_separator_is_refused() -> None:
    """A given source id holding ``::`` is refused (ITL-1154)."""
    with pytest.raises(ValueError, match="'::'"):
        DictSource(
            data=[{"k": "a", "v": 1}],
            tag_columns=["k"],
            source_id="a::b",
            tag_only_columns=True,
        )


def test_an_empty_source_id_is_refused() -> None:
    """An empty source id names nothing; its tokens would start with ``::`` (ITL-1154)."""
    with pytest.raises(ValueError, match="empty"):
        DictSource(
            data=[{"k": "a", "v": 1}],
            tag_columns=["k"],
            source_id="",
            tag_only_columns=True,
        )


def test_a_column_name_with_the_separator_is_refused() -> None:
    """A column name holding ``::`` is refused (ITL-1154)."""
    from orcapod.errors import InputValidationError

    with pytest.raises(InputValidationError, match="'::'"):
        DictSource(
            data=[{"k": "a", "x::y": 1}],
            tag_columns=["k"],
            source_id="s",
            tag_only_columns=True,
        )


def test_a_polling_source_needs_key_columns() -> None:
    """A polled row is identified by its key: no key columns, no source (ITL-1158)."""
    with pytest.raises(ValueError, match="record_id_columns"):
        PollingSource(FakeDynamicSource(batches=[]), tag_columns="id")
    with pytest.raises(ValueError, match="record_id_columns"):
        PollingSource(
            FakeDynamicSource(batches=[]), tag_columns="id", record_id_columns=()
        )


def test_polled_batches_give_each_row_its_own_record_id() -> None:
    """Rows of different batches no longer share a record id (ITL-1158)."""
    from tests.test_channels.test_polling_source import _batch

    fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
    source = PollingSource(fake, tag_columns="id", record_id_columns="id")
    list(source.iter_data())
    source.check_for_new_data()
    table = source.as_table(columns={"system_tags": True, "source": True})
    (record_id,) = [n for n in table.column_names if n.startswith("_tag_record_id")]
    assert sorted(str(v) for v in table.column(record_id).to_pylist()) == [
        '{"id":1}',
        '{"id":2}',
    ]


def test_a_bad_polled_key_stops_the_source_at_once() -> None:
    """A batch whose keys repeat is refused, not retried (ITL-1154)."""
    import asyncio

    from orcapod.errors import RowIdentifierError
    from tests.test_channels.test_polling_source import _batch

    duplicate = {
        "id": pa.array([1, 1], type=pa.int64()),
        "val": pa.array([10, 20], type=pa.int64()),
    }
    fake = FakeDynamicSource(batches=[duplicate, _batch(2, 20)])
    source = PollingSource(fake, tag_columns="id", record_id_columns="id")

    async def collect() -> list[Any]:
        return [pair async for pair in source.async_iter_data()]

    with pytest.raises(RowIdentifierError, match="appears in rows 0 and 1"):
        asyncio.run(collect())
    assert len(fake.fetch_cursors) == 1


def test_a_sqlite_table_without_a_primary_key_names_rowid(tmp_path: Path) -> None:
    """The refusal of a rowid-only table names ``record_id_columns="rowid"``."""
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE logs (msg TEXT)")
        conn.execute("INSERT INTO logs VALUES ('a')")
    with pytest.raises(ValueError, match='record_id_columns="rowid"'):
        SQLiteTableSource(path, "logs", tag_only_columns=True)
    source = SQLiteTableSource(
        path, "logs", record_id_columns="rowid", tag_only_columns=True
    )
    assert source.keys()[0] == ("rowid",)


def test_a_key_polled_again_is_refused_and_the_batch_not_committed() -> None:
    """A key repeated in a later batch is refused; earlier rows stay (ITL-1154).

    The refused batch is not committed and the cursor does not move, so a later
    read polls the same batch again and is refused again.
    """
    from orcapod.errors import RowIdentifierError
    from tests.test_channels.test_polling_source import _batch

    fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(1, 11)])
    source = PollingSource(fake, tag_columns="id", record_id_columns="id")
    assert [data["val"] for _, data in source.iter_data()] == [10]
    with pytest.raises(RowIdentifierError, match='repeats the key {"id":1}'):
        source.check_for_new_data()
    assert source.head().as_table().column("val").to_pylist() == [10]
    with pytest.raises(RowIdentifierError, match="repeats the key"):
        list(source.iter_data())


def test_a_key_polled_again_stops_the_async_source_at_once() -> None:
    """On the async path the repeated key is not retried (ITL-1154)."""
    import asyncio

    from orcapod.errors import RowIdentifierError
    from tests.test_channels.test_polling_source import _batch

    fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(1, 11), _batch(2, 20)])
    source = PollingSource(fake, tag_columns="id", record_id_columns="id")
    seen: list[Any] = []

    async def collect() -> None:
        async for _, data in source.async_iter_data():
            seen.append(data["val"])

    with pytest.raises(RowIdentifierError, match="repeats the key"):
        asyncio.run(collect())
    assert seen == [10]
    assert len(fake.fetch_cursors) == 2


def test_a_rowid_key_is_read_when_it_is_not_a_tag(tmp_path: Path) -> None:
    """``record_id_columns="rowid"`` reads the rowid even when the tags differ."""
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE logs (category TEXT, msg TEXT)")
        conn.execute("INSERT INTO logs VALUES ('a', 'x'), ('b', 'y')")
    conn.close()
    source = SQLiteTableSource(
        path,
        "logs",
        tag_columns="category",
        record_id_columns="rowid",
        tag_only_columns=True,
    )
    table = source.as_table(columns={"system_tags": True})
    (name,) = [n for n in table.column_names if n.startswith("_tag_record_id")]
    assert sorted(str(v) for v in table.column(name).to_pylist()) == [
        '{"rowid":1}',
        '{"rowid":2}',
    ]


def test_a_colon_at_a_token_boundary_is_refused() -> None:
    """A column starting with ':' or a source id ending with ':' is refused.

    Either would make a source-info token read two ways: ``s::row_0:::x`` is column
    ``:x`` of record ``row_0`` and column ``x`` of record ``row_0:`` (ITL-1154).
    """
    from orcapod.core.sources.source_ids import location_source_id
    from orcapod.errors import InputValidationError

    with pytest.raises(InputValidationError, match="starts with ':'"):
        DictSource(
            data=[{"k": "a", ":x": 1}],
            tag_columns=["k"],
            source_id="s",
            tag_only_columns=True,
        )
    with pytest.raises(ValueError, match="ends with ':'"):
        DictSource(
            data=[{"k": "a", "x": 1}],
            tag_columns=["k"],
            source_id="s:",
            tag_only_columns=True,
        )
    with pytest.raises(ValueError, match="end with ':'"):
        location_source_id("csv", "data:")
    assert location_source_id("csv", "a:b") == "csv:a:b"


def test_a_location_whose_parts_cannot_be_told_apart_has_no_default_id(
    tmp_path: Path,
) -> None:
    """A part holding its separator would give two locations one default id."""
    from orcapod.core.sources.source_ids import joined_location
    from orcapod.core.sources.spiraldb_table_source import spiraldb_table_address

    assert joined_location(".", "db", "public", "t") == "db.public.t"
    with pytest.raises(ValueError, match="give the source a source_id"):
        joined_location(".", "a.b", "c", "t")
    with pytest.raises(ValueError, match="give the source a source_id"):
        spiraldb_table_address("proj", "data.set", "t", "id1")
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE "extra:main.logs" (k TEXT PRIMARY KEY, v INT)')
        conn.execute("INSERT INTO \"extra:main.logs\" VALUES ('a', 1)")
    conn.close()
    with pytest.raises(ValueError, match="give the source a source_id"):
        SQLiteTableSource(path, "extra:main.logs", tag_only_columns=True)
    named = SQLiteTableSource(
        path, "extra:main.logs", source_id="logs", tag_only_columns=True
    )
    assert named.source_id == "logs"


def test_a_rowid_key_beside_an_integer_primary_key_keeps_its_name(
    tmp_path: Path,
) -> None:
    """The injected rowid is aliased, so SQLite does not name it after the key."""
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.execute("INSERT INTO t VALUES (7, 'x')")
    conn.close()
    source = SQLiteTableSource(
        path, "t", record_id_columns="rowid", tag_only_columns=True
    )
    table = source.as_table(columns={"system_tags": True})
    (name,) = [n for n in table.column_names if n.startswith("_tag_record_id")]
    assert table.column(name).to_pylist() == ['{"rowid":7}']


def test_a_sqlite_path_may_hold_a_colon(tmp_path: Path) -> None:
    """Only the table name is checked: a path keeps its colons (``C:\\data``)."""
    folder = tmp_path / "run:1"
    folder.mkdir()
    path = folder / "db.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE trials (k TEXT PRIMARY KEY, v INT)")
        conn.execute("INSERT INTO trials VALUES ('a', 1)")
    conn.close()
    source = SQLiteTableSource(path, "trials", tag_only_columns=True)
    assert source.source_id == f"sqlite_table:{path}:main.trials"
