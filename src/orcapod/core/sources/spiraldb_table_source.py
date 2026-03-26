"""SpiralDBTableSource — a read-only RootSource backed by a SpiralDB table.

Wraps a SpiralDB table as an OrcaPod Source. The table's primary-key
(key-schema) columns are used as tag columns by default.

Requires the ``spiraldb`` optional extra: ``pip install orcapod[spiraldb]``.
Authentication is handled externally — run ``spiral login`` once to store
credentials in ``~/.config/pyspiral/auth.json``.

Example::

    # Default dataset, PK columns become tag columns automatically
    source = SpiralDBTableSource("my-project-123456", "spike_data")

    # Explicit dataset
    source = SpiralDBTableSource(
        "my-project-123456", "spike_data", dataset="prod"
    )

    # Override tag columns
    source = SpiralDBTableSource(
        "my-project-123456", "spike_data", tag_columns=["session_id"]
    )

Note:
    SpiralDB enforces non-null values in key-schema columns at storage time,
    so NULL values in PK (tag) columns are not expected in practice.  If a
    table is written with NULL key columns via an external tool, those NULLs
    will be propagated into the tag columns as-is.

    Tables with no key schema and no explicit ``tag_columns`` will raise
    ``ValueError`` at construction time.  Either define a key schema on the
    SpiralDB table or supply explicit ``tag_columns``.
"""
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.databases.spiraldb_connector import SpiralDBConnector

if TYPE_CHECKING:
    from orcapod import contexts
    from orcapod.config import Config


class SpiralDBTableSource(DBTableSource):
    """A read-only Source backed by a table in a SpiralDB dataset.

    At construction time the source:

    1. Opens a ``SpiralDBConnector`` for *project_id* and *dataset*.
    2. Validates the table exists.
    3. Resolves tag columns:

       - If *tag_columns* is provided, uses them as-is.
       - Otherwise uses the table's primary-key (key-schema) columns.
       - Raises ``ValueError`` if the table has no key schema and no
         explicit *tag_columns* are given.

    4. Delegates to ``DBTableSource.__init__`` for fetching and stream
       building (source-info provenance, schema hash, system tags).
    5. Closes the connector — all data is eagerly loaded into memory, so
       the connection is released immediately.

    Args:
        project_id: SpiralDB project identifier (e.g. ``"my-project-123456"``).
        table_name: Name of the SpiralDB table to expose as a source.
        dataset: Dataset within the project. Defaults to ``"default"``.
        tag_columns: Columns to use as tag columns. If ``None`` (default),
            the table's primary-key (key-schema) columns are used. Raises
            ``ValueError`` if the table has no key schema.
        system_tag_columns: Additional system-level tag columns.
        record_id_column: Column for stable per-row record IDs in provenance.
        source_id: Canonical source name for the registry and provenance
            tokens. Defaults to *table_name*.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: OrcaPod configuration.
        overrides: Optional pyspiral client config overrides passed through
            to ``SpiralDBConnector``, e.g.
            ``{"server.url": "http://api.spiraldb.dev"}``.

    Raises:
        ValueError: If the table is not found, has no primary-key columns
            and no explicit *tag_columns* are given, or the table is empty.
    """

    def __init__(
        self,
        project_id: str,
        table_name: str,
        dataset: str = "default",
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: "str | contexts.DataContext | None" = None,
        config: "Config | None" = None,
        overrides: dict[str, str] | None = None,
    ) -> None:
        self._project_id = project_id
        self._dataset = dataset
        self._overrides = overrides

        connector = SpiralDBConnector(
            project_id=project_id,
            dataset=dataset,
            overrides=overrides,
        )

        try:
            resolved_tags: list[str] | None = (
                list(tag_columns) if tag_columns is not None else None
            )

            # DBTableSource handles PK resolution and raises ValueError when
            # the table has no key schema and no explicit tag_columns are given.
            super().__init__(
                connector,
                table_name,
                tag_columns=resolved_tags,
                system_tag_columns=system_tag_columns,
                record_id_column=record_id_column,
                source_id=source_id,
                label=label,
                data_context=data_context,
                config=config,
            )
        finally:
            try:
                connector.close()
            except Exception:
                # Suppress connector close errors to avoid masking __init__
                # failures.
                pass

    def to_config(self) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict.

        Note:
            Does **not** call ``super().to_config()`` (i.e. ``DBTableSource.to_config()``)
            because that would invoke ``self._connector.to_config()``, which raises
            ``RuntimeError`` once the connector has been closed. The connector is always
            closed during ``__init__`` after the eager data load, so the full dict is
            built from the already-captured instance attributes instead.
        """
        return {
            "source_type": "spiraldb_table",
            "project_id": self._project_id,
            "dataset": self._dataset,
            "overrides": self._overrides,
            "table_name": self._table_name,
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_column": self._record_id_column,
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "SpiralDBTableSource":
        """Reconstruct a SpiralDBTableSource from a config dict.

        Args:
            config: Dict as produced by ``to_config()``.

        Returns:
            A new ``SpiralDBTableSource`` instance.
        """
        return cls(
            project_id=config["project_id"],
            table_name=config["table_name"],
            dataset=config.get("dataset", "default"),
            tag_columns=config.get("tag_columns"),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
            label=config.get("label"),
            data_context=config.get("data_context"),
            overrides=config.get("overrides"),
        )
