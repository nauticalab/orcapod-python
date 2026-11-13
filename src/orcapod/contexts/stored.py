"""The data context recorded on stored rows (ITL-944).

Every row Orcapod stores carries the key of the data context that wrote it, in the
column ``__data_context`` (``constants.DATA_CONTEXT_COL``): function pipeline and
result tables, operator-cache tables, cached-source tables and side-effect invocation
logs. The key names one generation of hashing (``std:v1:default`` since Orcapod v0.3),
so two rows with the same key were hashed with exactly the same definitions.

Rows written under the current data context are the current entries: every read and
lookup keeps those rows only. Rows of another data context stay in the table,
unchanged, until they are migrated.

Tables written before v0.3 have no ``__data_context`` column and live at an older
table layout (another last path component). They are the generation
``std:v0.1:default``; Orcapod counts and reports them and does not read them as
results.

The path of a table holds hashes, and those can differ between versions of Orcapod, so
a table written before v0.3 is not looked for at a computed path only. The beginning of
the path, up to the name of the function or pod, holds no hash: the tables under it
are listed (``list_table_paths``), and a table whose last path component is not one of
the current layouts (``CURRENT_LAYOUTS``) was written before v0.3 (ITL-954).

Example:
    To list the data contexts a stored table holds, call
    ``stored_data_contexts(store, record_path)`` (from ``orcapod.contexts``); it
    returns, for example, ``{"std:v1:default": 3}``.
"""

from __future__ import annotations

import logging
import warnings
from collections.abc import Collection
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol, cast

from orcapod.errors import (
    InputValidationError,
    SideEffectLogDataContextError,
    StoreContentWarning,
    TableListingNotSupportedError,
)
from orcapod.system_constants import (
    OLD_PIPELINE_DB_SCHEMA_VERSIONS,
    OLD_RESULT_DB_SCHEMA_VERSIONS,
    OLD_TRACKING_DB_SCHEMA_VERSIONS,
    OPERATOR_DB_SCHEMA_VERSION,
    PIPELINE_DB_SCHEMA_VERSION,
    PRE_V1_DATA_CONTEXT_KEY,
    RESULT_DB_SCHEMA_VERSION,
    SOURCE_DB_SCHEMA_VERSION,
    TRACKING_DB_SCHEMA_VERSION,
    constants,
)
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")

logger = logging.getLogger(__name__)

# The last path component of every table this version of Orcapod writes.
CURRENT_LAYOUTS: frozenset[str] = frozenset(
    {
        PIPELINE_DB_SCHEMA_VERSION,
        RESULT_DB_SCHEMA_VERSION,
        TRACKING_DB_SCHEMA_VERSION,
        OPERATOR_DB_SCHEMA_VERSION,
        SOURCE_DB_SCHEMA_VERSION,
    }
)

# Last path components that mark a table as written before v0.3 wherever it is found:
# the older layout names, and the beginnings of the hash components that ended a path
# when a table had no layout component (also as a connector store spells them).
_OLD_LAYOUTS: frozenset[str] = frozenset(
    OLD_PIPELINE_DB_SCHEMA_VERSIONS
    + OLD_RESULT_DB_SCHEMA_VERSIONS
    + OLD_TRACKING_DB_SCHEMA_VERSIONS
)
_BARE_PATH_ENDINGS: tuple[str, ...] = (
    "schema:",
    "instance:",
    "node:",
    "schema_",
    "instance_",
    "node_",
)

# How many table paths a report spells out before it only counts the rest.
_PATHS_SPELLED_OUT = 5


def with_data_context(table: "pa.Table", context_key: str) -> "pa.Table":
    """Return ``table`` with the ``__data_context`` column appended.

    Args:
        table: Rows about to be stored.
        context_key: Key of the data context that produced the rows.

    Returns:
        A new table whose last column is ``__data_context``, a non-null
        ``large_string`` holding ``context_key`` on every row.
    """
    return table.append_column(
        pa.field(constants.DATA_CONTEXT_COL, pa.large_string(), nullable=False),
        pa.array([context_key] * table.num_rows, type=pa.large_string()),
    )


def require_same_data_context(input_context_key: str, own_context_key: str) -> None:
    """Refuse an input that was hashed under another data context than the writer's.

    A stored row names one data context, the one whose hashers produced its keys. An
    input of another data context would give a row whose keys mix two generations of
    hashing, so the writer refuses it before anything is written. What should
    eventually happen instead (the node's data context is meant to win) is tracked
    in ITL-951.

    Args:
        input_context_key: Data context key of the input (a row or a stream).
        own_context_key: Data context key of the node, cache or source that writes.

    Raises:
        InputValidationError: If the two keys differ.
    """
    if input_context_key != own_context_key:
        raise InputValidationError(
            f"The input was produced under the data context {input_context_key!r}, "
            f"but this node stores its rows under {own_context_key!r}. A stored row "
            "carries one data context; convert the input to the node's data context "
            "first, or run the node under the input's data context."
        )


def keep_rows_of_data_context(table: "pa.Table", context_key: str) -> "pa.Table":
    """Drop the rows of ``table`` written under another data context.

    Every read of a whole stored table goes through this function, so rows of another
    data context are never returned as results.

    Args:
        table: Rows read from a stored table at the current layout.
        context_key: Key of the current data context.

    Returns:
        ``table`` itself when no row is dropped, otherwise ``table`` without the rows
        whose ``__data_context`` is not ``context_key``.

    Raises:
        KeyError: If ``table`` has no ``__data_context`` column.
    """
    mine = cast(
        "pa.BooleanArray",
        pc.equal(
            table.column(constants.DATA_CONTEXT_COL),
            pa.scalar(context_key, pa.large_string()),
        ).fill_null(False),
    )
    if pc.all(mine).as_py() is not False:
        return table
    # The single place where stored rows of another data context are left out of a read.
    return table.filter(mine)


def report_rows_of_other_data_contexts(
    record_path: tuple[str, ...], skipped: int
) -> None:
    """Warn that a read left out stored rows written under another data context.

    Args:
        record_path: Path of the stored table from the root of the store.
        skipped: Number of rows of the table left out of the read.
    """
    logger.warning(
        "Skipped %d %s of stored table %r: written under another data context than "
        "the current one. They are not used and are left unchanged; they can be "
        "migrated later.",
        skipped,
        "row" if skipped == 1 else "rows",
        "/".join(record_path),
    )


class _DescribedStore(Protocol):
    """The part of ``ArrowDatabaseProtocol`` that names a store (no import cycle)."""

    @property
    def base_path(self) -> tuple[str, ...]:
        """Path of this store's view from the root of the underlying store."""
        ...

    def to_config(self) -> dict[str, Any]:
        """Return the store's configuration, with its location when it has one."""
        ...


def describe_store_table(
    database: _DescribedStore, record_path: tuple[str, ...]
) -> str:
    """Name a stored table for a message: the store's location and the table's path.

    Args:
        database: The store, or a view of it.
        record_path: Path of the table relative to ``database``.

    Returns:
        For example ``"store at '/data/store' (table 'job/node/...')"``.
    """
    store_config = database.to_config()
    location = store_config.get("root_uri")
    store = (
        f"store at {location!r}"
        if location
        else f"{store_config.get('type', 'attached')} store"
    )
    table = "/".join(database.base_path + record_path)
    return f"{store} (table {table!r})"


def report_unexpected_row_count(
    node: str, location: str, expected: int, found: int
) -> None:
    """Warn that a loaded node's store holds another number of rows than its run's.

    The store is the source of truth: the read returns what it holds. The warning says
    that it differs from what the job file recorded (ITL-1043).

    Args:
        node: The node, as messages name it (``"FunctionJobNode 'dbl'"``).
        location: The store and table, from ``describe_store_table``.
        expected: The rows the job file records for the node's run.
        found: The rows the store holds under the node's run key.
    """
    warnings.warn(
        StoreContentWarning(
            f"{node}: the job file records {expected} "
            f"{'row' if expected == 1 else 'rows'} for its run, but the {location} "
            f"holds {found} under the run's key. The read returns what the store "
            "holds; the store may not be the one the run wrote, or it may have "
            "changed since."
        ),
        stacklevel=3,
    )


def report_old_layout_rows(record_path: tuple[str, ...], rows: int) -> None:
    """Warn that a table written at an older table layout was found and is not used.

    Args:
        record_path: Path of the old-layout table from the root of the store.
        rows: Number of rows the table holds.
    """
    logger.warning(
        "Found %d %s in stored table %r written by an earlier version of Orcapod "
        "(older table layout, data context %s). They are not used and are left "
        "unchanged; they can be migrated later.",
        rows,
        "row" if rows == 1 else "rows",
        "/".join(record_path),
        PRE_V1_DATA_CONTEXT_KEY,
    )


def report_tables_under_name(
    name_path: tuple[str, ...], record_paths: Collection[tuple[str, ...]]
) -> None:
    """Warn that tables written before v0.3 were found under a function's name.

    Args:
        name_path: Path, from the root of the store, up to the function's name.
        record_paths: Paths of the tables found, from the root of the store.
    """
    logger.warning(
        "Found %d %s written by an earlier version of Orcapod under %r (older table "
        "layout, data context %s): %s. They are not used and are left unchanged; "
        "they can be migrated later. Tables of another function or node with the "
        "same name are included.",
        len(record_paths),
        "table" if len(record_paths) == 1 else "tables",
        "/".join(name_path),
        PRE_V1_DATA_CONTEXT_KEY,
        _spelled_out(record_paths),
    )


def report_unclaimed_tables(
    store_path: tuple[str, ...], record_paths: Collection[tuple[str, ...]]
) -> None:
    """Warn, once for a job, about the pre-v0.3 tables none of its nodes reports.

    Args:
        store_path: Path of the job's tables from the root of the store.
        record_paths: Paths of the tables found, from the root of the store.
    """
    logger.warning(
        "Store %r holds %d more %s written by an earlier version of Orcapod "
        "(operator caches, cached sources, or nodes not in this job): %s. They are "
        "not used and are left unchanged; they can be migrated later.",
        "/".join(store_path),
        len(record_paths),
        "table" if len(record_paths) == 1 else "tables",
        _spelled_out(record_paths),
    )


def report_unlisted_side_effect_tables(
    name_paths: Collection[tuple[str, ...]], reason: str
) -> None:
    """Warn that a side-effecting node could not look for completions logged before v0.3.

    Args:
        name_paths: Paths, from the root of the store, up to the node's function or
            pod name: where the tables would have been looked for.
        reason: Why the store could not list its tables.
    """
    logger.warning(
        "The store cannot list its tables (%s), so the node that performs side "
        "effects could not check for completions logged before Orcapod v0.3 under "
        "%s. If the store holds such tables, every side effect is delivered again.",
        reason,
        " and ".join(repr("/".join(path)) for path in name_paths),
    )


def _spelled_out(record_paths: Collection[tuple[str, ...]]) -> str:
    """Return the first few of ``record_paths`` as text, and how many more there are."""
    ordered = sorted(record_paths)
    shown = ", ".join(repr("/".join(path)) for path in ordered[:_PATHS_SPELLED_OUT])
    more = len(ordered) - _PATHS_SPELLED_OUT
    return f"{shown} (and {more} more)" if more > 0 else shown


class _RecordStore(Protocol):
    """The part of ``ArrowDatabaseProtocol`` this module needs (no import cycle)."""

    @property
    def base_path(self) -> tuple[str, ...]:
        """Path of this store's view from the root of the underlying store."""
        ...

    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return whether a table is stored at ``record_path``.

        Args:
            record_path: Path of the table, relative to ``base_path``.

        Returns:
            True if the store holds a table at that path.
        """
        ...

    def get_all_records(
        self, record_path: tuple[str, ...], record_id_column: str | None = None
    ) -> "pa.Table | None":
        """Return every row of the table at ``record_path``.

        Args:
            record_path: Path of the table, relative to ``base_path``.
            record_id_column: Name of a column to add holding each row's record
                id; no such column when ``None``.

        Returns:
            The rows as an Arrow table, or ``None`` if there are none.
        """
        ...


class StaleTable(NamedTuple):
    """Stored rows a node will not use: an old-layout table, or rows of another data context.

    Attributes:
        record_path: Path of the table from the root of the store.
        data_context: Key of the data context that wrote the rows.
        rows: Number of such rows in the table; ``None`` for a table found by
            listing the store, which is not read.
        old_layout: True for a table at an older layout, False for rows of another
            data context in a table at the current layout.
        found_under: For a table found by listing the store, the path (from the
            root of the store) under which the tables were listed; ``None`` for a
            table found at a computed path.
    """

    record_path: tuple[str, ...]
    data_context: str
    rows: int | None
    old_layout: bool
    found_under: tuple[str, ...] | None = None


class UnlistedTables(NamedTuple):
    """A place where tables written before v0.3 could not be looked for.

    Attributes:
        name_path: Path, from the root of the store, under which the tables would
            have been listed.
        reason: Why the store could not list its tables.
    """

    name_path: tuple[str, ...]
    reason: str


@dataclass
class StaleTables:
    """What a search for stored rows of other generations found.

    Attributes:
        tables: The tables found, each once.
        unlisted: The places that could not be searched because the store cannot
            list its tables.
    """

    tables: list[StaleTable] = field(default_factory=list)
    unlisted: list[UnlistedTables] = field(default_factory=list)

    def include(self, other: "StaleTables") -> None:
        """Add what another search found, leaving out tables already known.

        Args:
            other: The result of a search for another table of the same node.
        """
        known = {(table.record_path, table.old_layout) for table in self.tables}
        self.tables.extend(
            table
            for table in other.tables
            if (table.record_path, table.old_layout) not in known
        )
        self.unlisted.extend(other.unlisted)


def tables_written_before_v03(
    database: object, name_path: tuple[str, ...]
) -> list[tuple[str, ...]]:
    """List the tables under ``name_path`` that were written before Orcapod v0.3.

    Old and current are told apart by the layout, the last path component, never
    by a hash: every table this version writes ends in one of ``CURRENT_LAYOUTS``.
    ``name_path`` is expected to end with the name of one function or pod, so that
    every table under it belongs to a node of that name.

    Args:
        database: The store to list.
        name_path: Leading path components, relative to the database's ``base_path``.

    Returns:
        The paths, relative to the database's ``base_path``, of the tables under
        ``name_path`` whose last component is not a current layout, sorted.

    Raises:
        TableListingNotSupportedError: If the store cannot list its tables.
    """
    return [
        path
        for path in _list_table_paths(database, name_path)
        if path[-1] not in CURRENT_LAYOUTS
    ]


def marked_tables_written_before_v03(database: object) -> list[tuple[str, ...]]:
    """List the tables of a whole database view that carry a mark of an older version.

    Unlike under the name of one function, a store holds other tables beside node
    tables (observer logs, tables of the user), so here a table counts only when its
    last path component is an older layout (``pdb_v1``, ``rdb_v1``, ``tdb_v1``) or
    the hash component that ended a path before tables had a layout component
    (``schema:...``, ``instance:...``, ``node:...``).

    Args:
        database: The store to list, positioned at the tables of one job.

    Returns:
        The paths of those tables, relative to the database's ``base_path``, sorted.

    Raises:
        TableListingNotSupportedError: If the store cannot list its tables.
    """
    return [
        path
        for path in _list_table_paths(database, ())
        if path[-1] in _OLD_LAYOUTS or path[-1].startswith(_BARE_PATH_ENDINGS)
    ]


def unclaimed_tables_written_before_v03(
    database: object, claimed: Collection[tuple[str, ...]]
) -> list[tuple[str, ...]]:
    """List the marked pre-v0.3 tables of a job that none of its nodes accounts for.

    A function or side-effect node accounts for every table under its own name.
    What is left are operator caches and cached sources, whose paths hold no name
    that survives a change of hashes, and tables of nodes the job no longer has.

    Args:
        database: The store, positioned at the tables of one job.
        claimed: The name paths, relative to ``database``, that nodes of the job
            account for themselves.

    Returns:
        The paths, relative to the database's ``base_path``, of the tables that
        ``marked_tables_written_before_v03`` finds outside every claimed name path.

    Raises:
        TableListingNotSupportedError: If the store cannot list its tables.
    """
    return [
        path
        for path in marked_tables_written_before_v03(database)
        if not any(path[: len(name_path)] == name_path for name_path in claimed)
    ]


def _list_table_paths(
    database: object, prefix: tuple[str, ...]
) -> list[tuple[str, ...]]:
    """Call ``list_table_paths`` of a store that may not have the method.

    Raises:
        TableListingNotSupportedError: If the store has no such method or cannot
            list its tables.
    """
    list_table_paths = getattr(database, "list_table_paths", None)
    if list_table_paths is None:
        raise TableListingNotSupportedError(
            f"{type(database).__name__} has no list_table_paths method"
        )
    return cast("list[tuple[str, ...]]", list_table_paths(prefix))


def find_stale_tables(
    database: _RecordStore,
    old_paths: tuple[tuple[str, ...], ...],
    current: tuple[tuple[str, ...], str] | None = None,
    name_path: tuple[str, ...] | None = None,
) -> StaleTables:
    """Find the stored rows of other generations that belong to one table of a node.

    Three searches, none of which writes anything:

    - at ``old_paths``, where an earlier version stored the table if no hash in the
      path changed since (the rows are counted, which reads the table);
    - under ``name_path``, by listing the store, for tables of an earlier version
      wherever their hashes put them (ITL-954; not read, so not counted);
    - in the current table, for rows of another data context.

    Args:
        database: The store the paths belong to.
        old_paths: Paths at which an earlier version of Orcapod stored this table.
        current: The table's current path and the current data context key, to also
            look for rows of another data context in the current table (this reads
            the whole table). ``None`` skips that search.
        name_path: The beginning of the table's path up to the name of its function
            or pod, the part that holds no hash. ``None`` skips the listing.

    Returns:
        The old-layout tables found (those at ``old_paths`` first), then one entry
        per other data context found in the current table (in key order); and
        ``name_path`` under ``unlisted`` if the store could not list its tables.
    """
    found = StaleTables()
    for path in old_paths:
        if not database.table_exists(path):
            continue
        records = database.get_all_records(path)
        rows = 0 if records is None else records.num_rows
        found.tables.append(
            StaleTable(database.base_path + path, PRE_V1_DATA_CONTEXT_KEY, rows, True)
        )
    if name_path is not None:
        under = database.base_path + name_path
        try:
            listed = tables_written_before_v03(database, name_path)
        except TableListingNotSupportedError as exc:
            found.unlisted.append(UnlistedTables(under, str(exc)))
        else:
            found.include(
                StaleTables(
                    [
                        StaleTable(
                            database.base_path + path,
                            PRE_V1_DATA_CONTEXT_KEY,
                            None,
                            True,
                            under,
                        )
                        for path in listed
                    ]
                )
            )
    if current is not None:
        current_path, current_context = current
        for key, rows in sorted(stored_data_contexts(database, current_path).items()):
            if key != current_context:
                found.tables.append(
                    StaleTable(database.base_path + current_path, key, rows, False)
                )
    return found


def account_for_stale_tables(
    found: StaleTables, *, delivers_side_effects: bool, current_context: str
) -> None:
    """Decide what a node does about stored rows of other generations it will not use.

    This is the one place where that is decided, for every kind of node:

    - A node whose results can be computed again (function, operator and source
      nodes) reports each table with its number of rows and moves on.
    - A node whose run is an external action (a side-effect pod, or a function pod
      that takes the invocation context) must not act again for inputs it already
      handled, and those completions cannot be matched under the current data
      context. It fails before doing anything. Every old-layout table under the
      node's name counts, so a table of another function or pod with the same name
      in the job, or of another node of the same function, fails it too: the error
      names the tables. Where the store cannot list its tables, the node warns and
      proceeds (developer, November 11, 2026).

    Args:
        found: What ``find_stale_tables`` found for the node's tables.
        delivers_side_effects: Whether running the node is an external action.
        current_context: Key of the node's data context.

    Raises:
        SideEffectLogDataContextError: If ``delivers_side_effects`` and a table was
            found. The caller adds the node label.
    """
    if delivers_side_effects:
        if found.tables:
            raise SideEffectLogDataContextError(
                tuple(table.record_path for table in found.tables),
                found.tables[0].data_context,
                current_context,
            )
        if found.unlisted:
            report_unlisted_side_effect_tables(
                [unlisted.name_path for unlisted in found.unlisted],
                found.unlisted[0].reason,
            )
        return
    listed: dict[tuple[str, ...], list[tuple[str, ...]]] = {}
    for table in found.tables:
        if table.found_under is not None:
            listed.setdefault(table.found_under, []).append(table.record_path)
        elif table.rows is None:
            continue
        elif table.old_layout:
            report_old_layout_rows(table.record_path, table.rows)
        else:
            report_rows_of_other_data_contexts(table.record_path, table.rows)
    for name_path, record_paths in listed.items():
        report_tables_under_name(name_path, record_paths)


def stored_data_contexts(
    database: _RecordStore, record_path: tuple[str, ...]
) -> dict[str, int]:
    """Count the rows of a stored table by the data context that wrote them.

    Reads the whole table. A table without a ``__data_context`` column was written
    before Orcapod v0.3; all its rows are counted under ``std:v0.1:default``.

    Args:
        database: The store, positioned so that ``record_path`` names the table.
        record_path: Path of the stored table, including its layout component.

    Returns:
        A mapping from data context key to number of rows, for example
        ``{"std:v1:default": 3}``. A missing or empty table gives ``{}``.
    """
    table = database.get_all_records(record_path)
    if table is None or table.num_rows == 0:
        return {}
    if constants.DATA_CONTEXT_COL not in table.column_names:
        return {PRE_V1_DATA_CONTEXT_KEY: table.num_rows}
    per_value = pc.value_counts(table.column(constants.DATA_CONTEXT_COL))
    keys = cast("list[str]", per_value.field(0).to_pylist())
    counts = cast("list[int]", per_value.field(1).to_pylist())
    return dict(zip(keys, counts))
