"""
Tag and Data — datagram subclasses with system-tags and source-info support.

``Tag``
    Extends ``Datagram`` with *system tags*: metadata fields whose names start with
    ``constants.SYSTEM_TAG_PREFIX``.  System tags travel alongside the primary data
    but are excluded from content hashing and structural operations unless explicitly
    requested via ``ColumnConfig(system_tags=True)``.

``Data``
    Extends ``Datagram`` with *source information*: provenance tokens (strings,
    None, or lists of tokens) keyed by data-column name.  Source-info keys are
    stored without the ``constants.SOURCE_PREFIX`` internally and added back when
    serialising via ``as_dict()`` / ``as_table()``.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Self

from orcapod import contexts
from orcapod.core.datagrams.datagram import Datagram, context_key_field
from orcapod.semantic_types import infer_python_schema_from_pylist_data
from orcapod.system_constants import constants
from orcapod.types import (
    ColumnConfig,
    ContentHash,
    DataType,
    DataValue,
    Schema,
    SchemaLike,
    SourceInfoValue,
)
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tag
# ---------------------------------------------------------------------------


def _source_info_field(name: str, token: SourceInfoValue) -> pa.Field[Any]:
    """The field of source-info column ``name`` holding ``token``, as a row's table has it."""
    import pyarrow as _pa

    return _pa.field(name, arrow_utils.source_info_arrow_type(token))


class Tag(Datagram):
    """
    Datagram with system-tags support.

    System tags are metadata fields whose names begin with
    ``constants.SYSTEM_TAG_PREFIX``.  They are excluded from the primary data
    representation (and therefore from content hashing) unless the caller requests
    them via ``ColumnConfig(system_tags=True)``.

    Accepts the same inputs as ``Datagram`` (dict or Arrow table/batch).
    System-tag fields found in the input are automatically extracted.
    """

    def __init__(
        self,
        data: "Mapping[str, DataValue] | pa.Table | pa.RecordBatch",
        system_tags: "Mapping[str, DataValue] | None" = None,
        meta_info: "Mapping[str, DataValue] | None" = None,
        python_schema: "SchemaLike | None" = None,
        data_context: "str | contexts.DataContext | None" = None,
        record_uuid: "uuid.UUID | None" = None,
        **kwargs,
    ) -> None:
        import pyarrow as _pa

        if isinstance(data, _pa.RecordBatch):
            data = _pa.Table.from_batches([data])

        extracted_sys_tags: dict[str, DataValue]

        if isinstance(data, _pa.Table):
            # Arrow path: call super() first, then extract system-tag columns from
            # self._data_table (same pattern as the legacy ArrowTag).
            super().__init__(
                data,
                meta_info=meta_info,
                data_context=data_context,
                record_uuid=record_uuid,
                **kwargs,
            )
            sys_tag_cols = [
                c
                for c in self._data_table.column_names  # type: ignore[union-attr]
                if c.startswith(constants.SYSTEM_TAG_PREFIX)
            ]
            if sys_tag_cols:
                extracted_sys_tags = (
                    self._data_context.type_converter.arrow_table_to_python_dicts(
                        self._data_table.select(sys_tag_cols)  # type: ignore[union-attr]
                    )[0]
                )
                self._data_table = self._data_table.drop_columns(sys_tag_cols)  # type: ignore[union-attr]
                # Invalidate derived caches
                self._data_arrow_schema = None
            else:
                extracted_sys_tags = {}
        else:
            # Dict path: extract system-tag keys before calling super()
            data_only = {
                k: v
                for k, v in data.items()
                if not k.startswith(constants.SYSTEM_TAG_PREFIX)
            }
            extracted_sys_tags = {
                k: v
                for k, v in data.items()
                if k.startswith(constants.SYSTEM_TAG_PREFIX)
            }
            super().__init__(
                data_only,
                python_schema=python_schema,
                meta_info=meta_info,
                data_context=data_context,
                record_uuid=record_uuid,
                **kwargs,
            )

        self._system_tags: dict[str, DataValue] = {
            **extracted_sys_tags,
            **(system_tags or {}),
        }
        self._system_tags_python_schema: Schema = infer_python_schema_from_pylist_data(
            [self._system_tags], default_type=str
        )
        self._system_tags_table: "pa.Table | None" = None

    # ------------------------------------------------------------------
    # Internal helper
    # ------------------------------------------------------------------

    def _ensure_system_tags_table(self) -> "pa.Table":
        if self._system_tags_table is None:
            self._system_tags_table = (
                self._data_context.type_converter.python_dicts_to_arrow_table(
                    [self._system_tags],
                    python_schema=self._system_tags_python_schema,
                )
            )
        return self._system_tags_table

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def keys(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        keys = super().keys(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            keys += tuple(self._system_tags.keys())
        return keys

    def schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> Schema:
        schema = super().schema(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            return Schema({**schema, **self._system_tags_python_schema})
        return schema

    def arrow_schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Schema":
        schema = super().arrow_schema(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags and self._system_tags:
            return arrow_utils.join_arrow_schemas(
                schema, self._ensure_system_tags_table().schema
            )
        return schema

    def as_dict(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "dict[str, DataValue]":
        result = super().as_dict(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags:
            result.update(self._system_tags)
        return result

    def as_table(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Table":
        table = super().as_table(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.system_tags and self._system_tags:
            table = arrow_utils.hstack_tables(table, self._ensure_system_tags_table())
        return table

    def system_tags(self) -> "dict[str, DataValue]":
        """Return a copy of the system-tags dict."""
        return dict(self._system_tags)

    def as_datagram(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> Datagram:
        data = self.as_dict(columns=columns, all_info=all_info)
        python_schema = self.schema(columns=columns, all_info=all_info)
        return Datagram(
            data, python_schema=python_schema, data_context=self._data_context
        )

    def copy(self, include_cache: bool = True, preserve_id: bool = False) -> Self:
        new_tag = super().copy(include_cache=include_cache, preserve_id=preserve_id)
        new_tag._system_tags = dict(self._system_tags)
        new_tag._system_tags_python_schema = self._system_tags_python_schema
        new_tag._system_tags_table = self._system_tags_table if include_cache else None
        return new_tag


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


def _source_info_python_type(value: "SourceInfoValue") -> DataType:
    """Derive the Python type for a single source-info value.

    Read off ``arrow_utils.source_info_arrow_type``, the one source-info type
    rule, so the ``Schema`` and the Arrow table always agree on a token's depth.

    Args:
        value: The stored provenance token.

    Returns:
        ``str`` for scalars and unknowns, ``list[...]`` nested to the token's
        depth for lists.
    """
    import pyarrow as _pa

    arrow_type = arrow_utils.source_info_arrow_type(value)
    depth = 0
    while _pa.types.is_large_list(arrow_type):
        arrow_type = arrow_type.value_type
        depth += 1
    python_type: DataType = str
    for _ in range(depth):
        python_type = list[python_type]  # type: ignore[valid-type]
    return python_type


class Data(Datagram):
    """
    Datagram with source-information tracking.

    Source info maps each data-column name to a provenance token
    (``SourceInfoValue``: a string, ``None``, or -- for many->one operators -- a
    list of tokens, one per aggregated member).
    Keys in ``_source_info`` are stored **without** the ``SOURCE_PREFIX``; the
    prefix is added transparently when serialising to dict or Arrow table.

    Accepts the same inputs as ``Datagram`` (dict or Arrow table/batch).
    Source-info fields (columns beginning with ``SOURCE_PREFIX``) found in the
    input are automatically extracted.
    """

    def __init__(
        self,
        data: "Mapping[str, DataValue] | pa.Table | pa.RecordBatch",
        meta_info: "Mapping[str, DataValue] | None" = None,
        source_info: "Mapping[str, SourceInfoValue] | None" = None,
        python_schema: "SchemaLike | None" = None,
        data_context: "str | contexts.DataContext | None" = None,
        record_uuid: "uuid.UUID | None" = None,
        **kwargs,
    ) -> None:
        import pyarrow as _pa

        if isinstance(data, _pa.RecordBatch):
            data = _pa.Table.from_batches([data])

        if isinstance(data, _pa.Table):
            # Arrow path: use prepare_prefixed_columns to split source-info from data
            if source_info is None:
                source_info = {}
            else:
                # Normalise: remove existing prefix from provided keys
                source_info = {
                    k.removeprefix(constants.SOURCE_PREFIX)
                    if k.startswith(constants.SOURCE_PREFIX)
                    else k: v
                    for k, v in source_info.items()
                }

            data_table, prefixed_tables = arrow_utils.prepare_prefixed_columns(
                data,
                {constants.SOURCE_PREFIX: source_info},
                exclude_columns=[constants.CONTEXT_KEY],
                exclude_prefixes=[constants.META_PREFIX],
            )
            super().__init__(
                data_table,
                meta_info=meta_info,
                data_context=data_context,
                record_uuid=record_uuid,
                **kwargs,
            )
            si_table = prefixed_tables[constants.SOURCE_PREFIX]
            if si_table.num_columns > 0 and si_table.num_rows > 0:
                self._source_info: dict[str, SourceInfoValue] = {
                    k.removeprefix(constants.SOURCE_PREFIX): v
                    for k, v in si_table.to_pylist()[0].items()
                }
            else:
                self._source_info = {}
        else:
            # Dict path: extract source-info keys before calling super()
            data_only = {
                k: v
                for k, v in data.items()
                if not k.startswith(constants.SOURCE_PREFIX)
            }
            contained_source_info: dict[str, SourceInfoValue] = {
                k.removeprefix(constants.SOURCE_PREFIX): v  # type: ignore[misc]
                for k, v in data.items()
                if k.startswith(constants.SOURCE_PREFIX)
            }
            super().__init__(
                data_only,
                python_schema=python_schema,
                meta_info=meta_info,
                data_context=data_context,
                record_uuid=record_uuid,
                **kwargs,
            )
            self._source_info = {**contained_source_info, **(source_info or {})}

        self._source_info_table: "pa.Table | None" = None

    # ------------------------------------------------------------------
    # Internal helper
    # ------------------------------------------------------------------

    def _ensure_source_info_table(self) -> "pa.Table":
        if self._source_info_table is None:
            import pyarrow as _pa

            # Only the source info of the data columns this Data holds, in the order of
            # those columns, as every other view gives it (ITL-1040).
            current = {
                k: self._source_info[k]
                for k in super().keys()
                if k in self._source_info
            }
            if current:
                prefixed = {
                    f"{constants.SOURCE_PREFIX}{k}": v for k, v in current.items()
                }
                schema = _pa.schema(
                    [_source_info_field(k, v) for k, v in prefixed.items()]
                )
                self._source_info_table = _pa.Table.from_pylist(
                    [prefixed], schema=schema
                )
            else:
                self._source_info_table = _pa.table({})
        return self._source_info_table

    @staticmethod
    def function_output_zero_row_table(
        python_schema: Schema,
        data_context: contexts.DataContext,
        meta_python_schema: Schema | None = None,
    ) -> pa.Table:
        """The table a function's output rows give, with no row.

        The zero-row form of ``Data.as_table(columns={"context": True, "source":
        True})`` for the ``Data`` a data function returns: built with
        ``python_schema=`` its declared output schema and a string source-info
        token for every column. Each part comes from the code that types a
        computed row: the data fields from the converter's
        ``python_schema_to_arrow_schema`` (``Datagram._ensure_arrow_schema``), the
        context field from ``context_key_field``, the meta fields from the same
        conversion of their Python types (``Datagram._ensure_meta_table``) and the
        source-info fields from ``_source_info_field`` with a string token
        (ITL-912). The columns are in the order ``Data.arrow_schema`` gives them.

        Args:
            python_schema: The function's declared output schema.
            data_context: The data context the rows are typed in.
            meta_python_schema: The meta columns every output row carries (a
                cached pod's ``__computed`` flag), if any.

        Returns:
            A zero-row table: data columns, ``_context_key``, meta columns,
            source-info columns.
        """
        import pyarrow as _pa

        fields = list(
            data_context.type_converter.python_schema_to_arrow_schema(python_schema)
        )
        fields.append(context_key_field())
        if meta_python_schema:
            fields.extend(
                data_context.type_converter.python_schema_to_arrow_schema(
                    meta_python_schema
                )
            )
        # A data function's source-info token is always a string (see
        # ``PythonDataFunction``): the type is the one a string token gives.
        fields.extend(
            _source_info_field(f"{constants.SOURCE_PREFIX}{name}", "")
            for name in python_schema
        )
        return _pa.schema(fields).empty_table()

    # ------------------------------------------------------------------
    # Source-info API
    # ------------------------------------------------------------------

    def source_info(self) -> "dict[str, SourceInfoValue]":
        """Return source info for all data-column keys (None for unknown)."""
        return {k: self._source_info.get(k) for k in self.keys()}

    def with_source_info(self, **source_info: "SourceInfoValue") -> Self:
        """Create a copy with updated source-information entries."""
        current = dict(self._source_info)
        for key, value in source_info.items():
            if key.startswith(constants.SOURCE_PREFIX):
                key = key.removeprefix(constants.SOURCE_PREFIX)
            current[key] = value
        new_p = self.copy(include_cache=False)
        new_p._source_info = current
        return new_p

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def keys(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        keys = super().keys(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            keys += tuple(f"{constants.SOURCE_PREFIX}{k}" for k in super().keys())
        return keys

    def schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> Schema:
        schema = dict(super().schema(columns=columns, all_info=all_info))
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            for key in super().keys():
                schema[f"{constants.SOURCE_PREFIX}{key}"] = _source_info_python_type(
                    self._source_info.get(key)
                )
        return Schema(schema)

    def arrow_schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Schema":
        schema = super().arrow_schema(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            si_table = self._ensure_source_info_table()
            if si_table.num_columns > 0:
                return arrow_utils.join_arrow_schemas(schema, si_table.schema)
        return schema

    def as_dict(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "dict[str, DataValue]":
        result = super().as_dict(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            for key, value in self.source_info().items():
                result[f"{constants.SOURCE_PREFIX}{key}"] = value
        return result

    def as_table(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Table":
        table = super().as_table(columns=columns, all_info=all_info)
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        if column_config.source:
            si_table = self._ensure_source_info_table()
            if si_table.num_columns > 0 and si_table.num_rows > 0:
                table = arrow_utils.hstack_tables(table, si_table)
        return table

    def select(self, *column_names: str) -> Self:
        """Return a copy holding only the given data columns and their source info.

        The source info is in the order of the columns the copy holds.
        """
        new_p = super().select(*column_names)
        # Datagram.keys: the data columns, also for an EmptyData, whose keys() raises.
        new_p._source_info = {
            k: self._source_info[k]
            for k in Datagram.keys(new_p)
            if k in self._source_info
        }
        new_p._source_info_table = None
        return new_p

    def drop(self, *column_names: str, ignore_missing: bool = False) -> Self:
        """Return a copy without the given data columns and their source info."""
        new_p = super().drop(*column_names, ignore_missing=ignore_missing)
        new_p._source_info = {
            k: v for k, v in self._source_info.items() if k not in column_names
        }
        new_p._source_info_table = None
        return new_p

    def rename(self, column_mapping: "Mapping[str, str]") -> Self:
        new_p = super().rename(column_mapping)
        new_p._source_info = {
            column_mapping.get(k, k): v for k, v in self._source_info.items()
        }
        new_p._source_info_table = None
        return new_p

    def with_columns(
        self,
        column_types: Mapping[str, DataType | pa.DataType | pa.Field[Any]]  # pyright: ignore[reportExplicitAny]  # Field is invariant; Any admits every Arrow type
        | None = None,
        **updates: DataValue,
    ) -> Self:
        """Return a copy with new data columns, each with empty source info.

        Args:
            column_types: Optional types for some or all new columns; see
                ``Datagram.with_columns``.
            **updates: New column names mapped to their values.

        Returns:
            A new ``Data`` with the added columns and their source info set
            to ``None``.
        """
        new_p = super().with_columns(column_types=column_types, **updates)
        new_source_info = dict(self._source_info)
        for col in updates:
            new_source_info[col] = None  # new columns get empty source info
        new_p._source_info = new_source_info
        new_p._source_info_table = None
        return new_p

    def as_datagram(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> Datagram:
        data = self.as_dict(columns=columns, all_info=all_info)
        python_schema = self.schema(columns=columns, all_info=all_info)
        return Datagram(
            data=data, python_schema=python_schema, data_context=self._data_context
        )

    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        new_p = super().copy(include_cache=include_cache, preserve_id=preserve_id)
        new_p._source_info = dict(self._source_info)
        new_p._source_info_table = self._source_info_table if include_cache else None
        return new_p


# ---------------------------------------------------------------------------
# EmptyData
# ---------------------------------------------------------------------------


class EmptyData(Data):
    """A ``Data``-shaped token representing missing data.

    ``EmptyData`` is produced when an upstream pod's ephemeral result has
    expired or been pruned. It carries all normal datagram metadata (data
    context, record UUID) but has no data payload. Every payload-access
    method raises ``EmptyDataAccessError`` — callers must
    ``isinstance(data, EmptyData)`` before touching columns.

    ``content_hash()`` is overridden to return ``cached_content_hash`` (the
    hash of the **upstream output** — i.e., the data payload this token stands
    in for). Because the downstream node's result cache is keyed by its own
    input (= the upstream output), returning this hash lets
    ``ResultCache.lookup(empty_data)`` work transparently without any changes
    to the cache infrastructure. If ``cached_content_hash`` is ``None`` (a
    pipeline DB row lacking the stored hash column), ``content_hash()`` raises
    ``EmptyDataHashMissingError`` loudly.

    ``empty_source_info`` is an optional provenance field for the future
    tag-row reconstruction follow-up. This release defines the field; the write
    logic is deferred.

    Args:
        cached_content_hash: The content hash of the missing data payload this
            token represents — typically the upstream node's output hash, which
            equals the downstream node's input hash. ``None`` for old-format
            rows lacking the stored hash column.
        empty_source_info: Optional provenance dict for tag-row reconstruction
            (follow-up). Keys match tag-row source columns; ``record_id`` may
            be ``None``.
        python_schema: Optional schema hint (passed to parent).
        data_context: Data context key or instance (passed to parent).
        record_uuid: Optional explicit UUID for this token.
    """

    def __init__(
        self,
        cached_content_hash: ContentHash | None = None,
        empty_source_info: dict[str, str | None] | None = None,
        python_schema: SchemaLike | None = None,
        data_context: str | contexts.DataContext | None = None,
        record_uuid: uuid.UUID | None = None,
    ) -> None:
        # Initialise the parent with an empty dict — no payload columns.
        super().__init__(
            {},
            python_schema=python_schema,
            data_context=data_context,
            record_uuid=record_uuid,
        )
        self._cached_content_hash: ContentHash | None = cached_content_hash
        self._empty_source_info: dict[str, str | None] | None = empty_source_info

    # ------------------------------------------------------------------
    # Content identity
    # ------------------------------------------------------------------

    def content_hash(self, hasher: Any = None) -> ContentHash:
        """Return the cached content hash or raise ``EmptyDataHashMissingError``.

        The returned hash is the hash of the **missing payload** this token
        stands in for — typically the upstream node's output hash (stored as
        ``OUTPUT_DATA_HASH_COL`` in the pipeline DB). This equals the
        downstream node's input hash, so ``ResultCache.lookup(empty_data)``
        finds the correct cached result transparently without touching the
        missing data.

        Args:
            hasher: Ignored — ``EmptyData`` uses the stored hash directly.

        Returns:
            The ``cached_content_hash`` set at construction.

        Raises:
            EmptyDataHashMissingError: If ``cached_content_hash`` is ``None``.
        """
        from orcapod.errors import EmptyDataHashMissingError

        if self._cached_content_hash is None:
            raise EmptyDataHashMissingError(self)
        return self._cached_content_hash

    def identity_structure(self) -> Any:
        """Always raises ``EmptyDataAccessError`` — no payload to hash."""
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "identity_structure")

    # ------------------------------------------------------------------
    # Payload-access overrides — all raise EmptyDataAccessError
    # ------------------------------------------------------------------

    def as_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "as_dict")

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "as_table")

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "keys")

    def schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> Schema:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "schema")

    def arrow_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Schema:
        from orcapod.errors import EmptyDataAccessError

        raise EmptyDataAccessError(self, "arrow_schema")

    # ------------------------------------------------------------------
    # Copy — preserves EmptyData-specific fields
    # ------------------------------------------------------------------

    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        """Return a shallow copy of this token, preserving EmptyData-specific fields.

        Overrides ``Datagram.copy()`` so that ``_cached_content_hash`` and
        ``_empty_source_info`` are carried across (the base implementation uses
        ``object.__new__`` and only copies ``Datagram`` fields, which would
        silently drop our extra attributes).

        Args:
            include_cache: Forwarded to ``super().copy()``.
            preserve_id: Forwarded to ``super().copy()``.

        Returns:
            A new ``EmptyData`` instance with all fields copied.
        """
        new_p = super().copy(include_cache=include_cache, preserve_id=preserve_id)
        new_p._cached_content_hash = self._cached_content_hash
        new_p._empty_source_info = self._empty_source_info
        return new_p

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------

    @property
    def cached_content_hash(self) -> ContentHash | None:
        """The stored content hash, or ``None`` if absent (old-format row)."""
        return self._cached_content_hash

    @property
    def empty_source_info(self) -> dict[str, str | None] | None:
        """Optional provenance dict for future tag-row reconstruction."""
        return self._empty_source_info
