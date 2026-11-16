"""SQLiteTableSource — a read-only RootSource backed by a SQLite table.

Wraps a SQLite table as an OrcaPod Source. Primary-key columns are used
as tag columns and as the row identifier (the record id) by default. For a
table with no explicit primary key (a ROWID-only table) the implicit ``rowid``
integer column becomes the tag, but the rows have no default identifier: pass
``record_id_columns="rowid"`` (or ``()`` for the position), since SQLite may
renumber the rowids of such a table (``VACUUM``).

Example::

    # File-backed (supports from_config round-trip)
    source = SQLiteTableSource("/path/to/my.db", "measurements")

    # In-memory (for tests / throwaway pipelines; cannot round-trip)
    source = SQLiteTableSource(":memory:", "events", tag_columns=["session_id"])

Note:
    ``:memory:`` sources cannot be reconstructed via ``from_config`` because
    each new ``SQLiteConnector(":memory:")`` opens a fresh empty database.
    File-backed sources (including ROWID-only tables) round-trip correctly.
"""

from __future__ import annotations

import os
from collections.abc import Collection, Iterator
from contextlib import contextmanager
from typing import Any, ClassVar, TYPE_CHECKING

from typing_extensions import override

from orcapod.core.referential_tags import TagOnlyColumns, check_tag_only_columns
from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.core.sources.source_ids import joined_location
from orcapod.core.sources.stream_builder import normalize_record_id_columns
from orcapod.databases.sqlite_connector import SQLiteConnector
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.utils.schema_utils import _normalize_column_list

if TYPE_CHECKING:
    from orcapod import contexts
    from orcapod.config import OrcapodConfig


def _access_path(db_path: str | os.PathLike[str]) -> str | os.PathLike[str]:
    """The path a later read opens: a file path made absolute, a special name as given.

    ``":memory:"`` and ``file:`` URIs are not file system paths.
    """
    text = os.fspath(db_path)
    if text == ":memory:" or text.startswith("file:"):
        return db_path
    return os.path.abspath(text)


class SQLiteTableSource(DBTableSource):
    """A read-only Source backed by a table in a SQLite database.

    At construction time the source:
    1. Opens a ``SQLiteConnector`` for *db_path*.
    2. Validates the table exists.
    3. Resolves tag columns:
       - If *tag_columns* is provided, uses them as-is.
       - Otherwise uses the table's primary-key columns.
       - If the table has no explicit PK (ROWID-only), falls back to the
         implicit ``rowid`` integer column; such a table also needs
         ``record_id_columns`` (``"rowid"`` or ``()``), or it is refused.
    4. Determines the fetch query: injects ``SELECT rowid, *`` when
       ``"rowid"`` is a resolved tag column and not a normal table column
       (handles both auto-detection and ``from_config`` reconstruction).
    5. Delegates to ``DBTableSource.__init__`` for fetching and stream building.
    6. Closes the connector — all data is eagerly loaded into memory, so the
       connection is released immediately to avoid holding file locks.

    Args:
        db_path: Path to the SQLite database file, or ``":memory:"`` for an
            in-process in-memory database.
        table_name: Name of the table to expose as a source.
        tag_columns: Columns to use as tag columns. A bare string is
            accepted as a single column name. If ``None`` (default),
            the table's primary-key columns are used; ROWID-only tables fall
            back to ``["rowid"]``.
        system_tag_columns: Additional system-level tag columns.
        record_id_columns: The key columns that identify a row; ``None``: the
            primary key (a table without one is refused); ``()``: the position.
        source_id: Canonical source name. Defaults to
            ``sqlite_table:<db path as passed>:main.<table>``.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: OrcaPod configuration.
        tag_only_columns: Tag columns kept as tags only, not data, exactly as in
            v0.3; ``True`` for all of them. Keyword-only. ``True`` is the default and
            the only value v0.3 accepts: any other value is refused, because
            referential tags arrive in v0.4 (tags then become referential by default
            and ``tag_only_columns`` keeps a tag plain).

    Raises:
        ValueError: If the table is not found or is empty.
        sqlite3.OperationalError: If *db_path* cannot be opened.
    """

    SOURCE_KIND: ClassVar[str] = "sqlite_table"
    """Config type key and default source-id kind of this source (one vocabulary)."""

    def __init__(
        self,
        db_path: str | os.PathLike,
        table_name: str,
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_columns: str | Collection[str] | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: "OrcapodConfig | None" = None,
        *,
        tag_only_columns: TagOnlyColumns = True,
    ) -> None:
        check_tag_only_columns(tag_only_columns)
        self._db_path = db_path
        # A check (ITL-1102) reopens the database found now, whatever the working
        # directory is then; an in-memory database or a URI is kept as given.
        self._access_path: str | os.PathLike[str] = _access_path(db_path)
        connector = SQLiteConnector(db_path)

        try:
            # A missing table is reported as missing, not as a table without a key.
            if table_name not in connector.get_table_names():
                raise ValueError(f"Table {table_name!r} not found in database.")
            # Step 3: Resolve tag columns.
            pk_cols = connector.get_pk_columns(table_name)
            if record_id_columns is None and not pk_cols:
                raise ValueError(
                    f"Table {table_name!r} has no primary key, so its rows have no "
                    'identifier: pass record_id_columns="rowid" (SQLite may renumber '
                    "rowids, for example on VACUUM), name other key columns, or pass "
                    "record_id_columns=() to identify rows by position."
                )
            if tag_columns is None:
                resolved_tags: list[str] = pk_cols if pk_cols else ["rowid"]
            else:
                resolved_tags = _normalize_column_list(tag_columns)

            # Step 4: Determine the fetch query.
            # If "rowid" is a tag or a key column but not a real column, we need
            # SELECT rowid, * to include it.  This also handles from_config
            # reconstruction where tag_columns=["rowid"] is passed explicitly.
            normal_cols = {ci.name for ci in connector.get_column_info(table_name)}
            needs_rowid = "rowid" in resolved_tags or "rowid" in (
                normalize_record_id_columns(record_id_columns) or ()
            )
            if needs_rowid and "rowid" not in normal_cols:
                # Aliased: unaliased, SQLite names it after an INTEGER PRIMARY KEY.
                _query: str | None = f'SELECT rowid AS rowid, * FROM "{table_name}"'
            else:
                _query = None

            super().__init__(
                connector,
                table_name,
                tag_columns=resolved_tags,
                system_tag_columns=system_tag_columns,
                record_id_columns=record_id_columns,
                source_id=source_id,
                label=label,
                data_context=data_context,
                config=config,
                tag_only_columns=tag_only_columns,
                _query=_query,
            )
        finally:
            try:
                connector.close()
            except Exception:
                # Suppress connector close errors to avoid masking __init__ failures.
                pass

    @override
    def _table_address(self, connector: DBConnectorProtocol, table_name: str) -> str:
        """Return ``<db path as passed>:main.<table>``.

        SQLite names a table ``<schema>.<table>``, the schema of the main database
        file being ``main``; the file itself is the database's address, so it comes
        first, as passed (a relative path stays relative; ITL-1154).

        Args:
            connector: The connector (unused: the path is the source's own).
            table_name: The table's name.

        Returns:
            The table's address.

        Raises:
            ValueError: If the table name contains ``:``, which would make the
                boundary after the path ambiguous; give the source a ``source_id``.
        """
        del connector
        # The path is read up to the last ``:main.``, so only the table name must not
        # hold a ``:``; the path is kept as passed, colons included (``C:\\data``).
        table = joined_location(":", f"main.{table_name}")
        return f"{os.fspath(self._db_path)}:{table}"

    @contextmanager
    @override
    def _open_connector(self) -> Iterator[SQLiteConnector]:
        """Open a new connector for a check and close it afterwards.

        The connector the source was built with was closed once the table was read.
        """
        connector = SQLiteConnector(self._access_path)
        try:
            yield connector
        finally:
            connector.close()

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        base = super().to_config()
        base.pop("connector", None)
        return {**base, "source_type": self.SOURCE_KIND, "db_path": str(self._db_path)}

    @classmethod
    def from_config(
        cls, config: dict[str, Any], db_registry=None
    ) -> "SQLiteTableSource":
        """Reconstruct a SQLiteTableSource from a config dict.

        Args:
            config: Dict as produced by ``to_config()``.

        Returns:
            A new ``SQLiteTableSource`` instance.

        Note:
            ``:memory:`` sources cannot be reconstructed — the new in-memory
            database will be empty and ``ValueError`` will be raised.
        """
        return cls(
            db_path=config["db_path"],
            table_name=config["table_name"],
            tag_columns=config.get("tag_columns"),
            tag_only_columns=config.get("tag_only_columns", True),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_columns=config.get("record_id_columns"),
            source_id=config.get("source_id"),
            label=config.get("label"),
            data_context=config.get("data_context"),
        )
