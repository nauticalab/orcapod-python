"""
Unified datagram implementation.

A single ``Datagram`` class that internally holds either an Arrow table or a Python
dict — whichever was provided at construction — and lazily converts to the other
representation only when required.

Principles
----------
- **Minimal conversion**: structural operations (select, drop, rename) stay Arrow-native
  when the Arrow representation is already loaded.
- **Dict for value access**: ``__getitem__``, ``get``, ``as_dict()`` always operate through
  the Python dict (loaded lazily from Arrow when needed).
- **Arrow for hashing**: ``content_hash()`` always uses the Arrow table (loaded lazily from
  dict when needed) via the data context's ``ArrowTableHandler``.
- **Meta is always dict**: meta columns are stored as a Python dict regardless of how the
  primary data was provided; the Arrow meta table is built lazily.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Self, cast

from uuid_utils import uuid7

from orcapod import contexts
from orcapod.config import Config
from orcapod.core.base import ContentIdentifiableBase
from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol
from orcapod.semantic_types import infer_python_schema_from_pylist_data
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, DataValue, Schema, SchemaLike
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class Datagram(ContentIdentifiableBase):
    """
    Immutable datagram backed by either an Arrow table or a Python dict.

    Accepts either a ``Mapping[str, DataValue]`` (dict-path) or a
    ``pa.Table | pa.RecordBatch`` (Arrow-path) as primary data.  The alternative
    representation is computed lazily and cached.

    Column conventions (same as the legacy implementations):
    - Keys starting with ``constants.META_PREFIX`` (``__``) → meta columns
    - The special key ``constants.CONTEXT_KEY`` → data-context column (extracted, not stored)
    - Everything else → primary data columns
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        data: Mapping[str, DataValue] | pa.Table | pa.RecordBatch,
        python_schema: SchemaLike | None = None,
        meta_info: Mapping[str, DataValue] | None = None,
        record_id: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None:
        if isinstance(data, pa.RecordBatch):
            data = pa.Table.from_batches([data])

        if isinstance(data, pa.Table):
            self._init_from_table(data, meta_info, data_context, record_id)
        else:
            self._init_from_dict(
                data, python_schema, meta_info, data_context, record_id
            )

    def _init_from_dict(
        self,
        data: Mapping[str, DataValue],
        python_schema: SchemaLike | None,
        meta_info: Mapping[str, DataValue] | None,
        data_context: str | contexts.DataContext | None,
        record_id: str | None,
    ) -> None:
        data_columns: dict[str, DataValue] = {}
        meta_columns: dict[str, DataValue] = {}
        extracted_context = None

        for k, v in data.items():
            if k == constants.CONTEXT_KEY:
                if data_context is None:
                    extracted_context = cast(str, v)
            elif k.startswith(constants.META_PREFIX):
                meta_columns[k] = v
            else:
                data_columns[k] = v

        super().__init__(data_context=data_context or extracted_context)
        self._datagram_id = record_id

        self._data_dict: dict[str, DataValue] | None = data_columns
        self._data_table: pa.Table | None = None

        inferred = infer_python_schema_from_pylist_data(
            [data_columns], default_type=str
        )
        inferred = infer_python_schema_from_pylist_data(
            [data_columns], default_type=str
        )
        self._data_python_schema: Schema | None = (
            Schema({k: python_schema.get(k, v) for k, v in inferred.items()})
            if python_schema
            else inferred
        )
        self._data_arrow_schema: pa.Schema | None = None

        if meta_info is not None:
            meta_columns.update(meta_info)
        self._meta: dict[str, DataValue] = meta_columns
        self._meta_python_schema: Schema = infer_python_schema_from_pylist_data(
            [meta_columns], default_type=str
        )
        self._meta_table: pa.Table | None = None
        self._context_table: pa.Table | None = None

    def _init_from_table(
        self,
        table: pa.Table,
        meta_info: Mapping[str, DataValue] | None,
        data_context: str | contexts.DataContext | None,
        record_id: str | None,
    ) -> None:
        if len(table) != 1:
            raise ValueError(
                "Table must contain exactly one row to be a valid datagram."
            )

        table = arrow_utils.normalize_table_to_large_types(table)

        # Extract context from table if not provided externally
        if constants.CONTEXT_KEY in table.column_names and data_context is None:
            data_context = table[constants.CONTEXT_KEY].to_pylist()[0]

        context_cols = [c for c in table.column_names if c == constants.CONTEXT_KEY]

        super().__init__(data_context=data_context)
        self._datagram_id = record_id

        meta_col_names = [
            c for c in table.column_names if c.startswith(constants.META_PREFIX)
        ]
        self._data_table = table.drop_columns(context_cols + meta_col_names)
        self._data_dict = None
        self._data_python_schema = None  # computed lazily
        self._data_arrow_schema = None  # computed lazily

        if len(self._data_table.column_names) == 0:
            raise ValueError("Data table must contain at least one data column.")

        # Build meta table
        meta_table: "pa.Table | None" = (
            table.select(meta_col_names) if meta_col_names else None
        )
        if meta_info is not None:
            normalized_meta = {
                k
                if k.startswith(constants.META_PREFIX)
                else f"{constants.META_PREFIX}{k}": v
                for k, v in meta_info.items()
            }
            new_meta = self.converter.python_dicts_to_arrow_table([normalized_meta])
            if meta_table is None:
                meta_table = new_meta
            else:
                keep = [
                    c for c in meta_table.column_names if c not in new_meta.column_names
                ]
                meta_table = arrow_utils.hstack_tables(
                    meta_table.select(keep), new_meta
                )

        # Store meta as dict (always); Arrow table is lazy.
        # Derive schema via infer_python_schema_from_pylist_data (same as DictDatagram)
        # to avoid typing.Any values that arrow_schema_to_python_schema may emit.
        if meta_table is not None and meta_table.num_columns > 0:
            self._meta = meta_table.to_pylist()[0]
            self._meta_python_schema = infer_python_schema_from_pylist_data(
                [self._meta], default_type=str
            )
        else:
            self._meta = {}
            self._meta_python_schema = Schema.empty()

        self._meta_table = None  # built lazily
        self._context_table = None

    # ------------------------------------------------------------------
    # Internal helpers (lazy loading)
    # ------------------------------------------------------------------

    def _ensure_data_dict(self) -> dict[str, DataValue]:
        """
        Ensure that dictionary representation is materialized and then returned
        """
        if self._data_dict is None:
            assert self._data_table is not None
            self._data_dict = self.converter.arrow_table_to_python_dicts(
                self._data_table
            )[0]
        return self._data_dict

    def _ensure_data_table(self) -> pa.Table:
        """
        Ensure that Arrow table representation is materialized and then returned
        """
        if self._data_table is None:
            assert self._data_dict is not None
            self._data_table = self.converter.python_dicts_to_arrow_table(
                [self._data_dict],
                self._data_python_schema,
            )
        return self._data_table

    def _ensure_python_schema(self) -> Schema:
        """
        Ensure that Python schema is materialized and then returned.

        Arrow nullable flags are always respected: nullable=True → T | None,
        nullable=False → T. The nullable-stripping concern belongs exclusively
        at the data ingestion boundary (SourceStreamBuilder.build).
        """
        if self._data_python_schema is None:
            assert self._data_table is not None
            raw_schema = self._data_table.schema
            self._data_python_schema = self.converter.arrow_schema_to_python_schema(
                raw_schema
            )
        return self._data_python_schema

    def _ensure_arrow_schema(self) -> pa.Schema:
        """
        Ensure that Arrow schema is materialized and then returned
        """
        if self._data_arrow_schema is None:
            if self._data_table is not None:
                self._data_arrow_schema = self._data_table.schema
            else:
                self._data_arrow_schema = self.converter.python_schema_to_arrow_schema(
                    self._ensure_python_schema()
                )
        return self._data_arrow_schema

    def _ensure_context_table(self) -> pa.Table:
        """
        Ensure context table is materialized and then returned (relevant for Arrow representation)
        """
        if self._context_table is None:
            import pyarrow as _pa

            schema = _pa.schema(
                [_pa.field(constants.CONTEXT_KEY, _pa.large_string(), nullable=False)]
            )
            self._context_table = _pa.Table.from_pylist(
                [{constants.CONTEXT_KEY: self._data_context.context_key}],
                schema=schema,
            )
        return self._context_table

    def _ensure_meta_table(self) -> pa.Table | None:
        """
        Ensure meta table is materialized and returned (relevant for Arrow representation)
        """
        if not self._meta:
            return None
        if self._meta_table is None:
            self._meta_table = self.converter.python_dicts_to_arrow_table(
                [self._meta], python_schema=self._meta_python_schema
            )
        return self._meta_table

    # ------------------------------------------------------------------
    # 1. Core Properties
    # ------------------------------------------------------------------

    @property
    def meta_columns(self) -> tuple[str, ...]:
        return tuple(self._meta.keys())

    # ------------------------------------------------------------------
    # 2. Dict-like Interface
    # ------------------------------------------------------------------

    def __getitem__(self, key: str) -> DataValue:
        return self._ensure_data_dict()[key]

    def __contains__(self, key: str) -> bool:
        if self._data_table is not None:
            return key in self._data_table.column_names
        assert self._data_dict is not None
        return key in self._data_dict

    def __iter__(self) -> Iterator[str]:
        if self._data_table is not None:
            return iter(self._data_table.column_names)
        assert self._data_dict is not None
        return iter(self._data_dict)

    def get(self, key: str, default: DataValue = None) -> DataValue:
        if key not in self:
            return default
        return self._ensure_data_dict()[key]

    # ------------------------------------------------------------------
    # 3. Structural Information
    # ------------------------------------------------------------------

    def keys(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> tuple[str, ...]:
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)

        if self._data_table is not None:
            data_keys: list[str] = list(self._data_table.column_names)
        else:
            assert self._data_dict is not None
            data_keys = list(self._data_dict.keys())

        if column_config.context:
            data_keys.append(constants.CONTEXT_KEY)

        if column_config.meta:
            include_meta = column_config.meta
            if include_meta is True:
                data_keys.extend(self.meta_columns)
            elif isinstance(include_meta, Collection):
                data_keys.extend(
                    c
                    for c in self.meta_columns
                    if any(c.startswith(p) for p in include_meta)
                )

        return tuple(data_keys)

    def schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> Schema:
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        result = dict(self._ensure_python_schema())

        if column_config.context:
            result[constants.CONTEXT_KEY] = str

        if column_config.meta and self._meta:
            include_meta = column_config.meta
            if include_meta is True:
                result.update(self._meta_python_schema)
            elif isinstance(include_meta, Collection):
                result.update(
                    {
                        k: v
                        for k, v in self._meta_python_schema.items()
                        if any(k.startswith(p) for p in include_meta)
                    }
                )

        return Schema(result)

    def arrow_schema(
        self,
        *,
        columns: "ColumnConfig | dict[str, Any] | None" = None,
        all_info: bool = False,
    ) -> "pa.Schema":
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        all_schemas = [self._ensure_arrow_schema()]

        if column_config.context:
            all_schemas.append(self._ensure_context_table().schema)

        if column_config.meta and self._meta:
            meta_table = self._ensure_meta_table()
            if meta_table is not None:
                include_meta = column_config.meta
                if include_meta is True:
                    all_schemas.append(meta_table.schema)
                elif isinstance(include_meta, Collection):
                    import pyarrow as _pa

                    matched = [
                        f
                        for f in meta_table.schema
                        if any(f.name.startswith(p) for p in include_meta)
                    ]
                    if matched:
                        all_schemas.append(_pa.schema(matched))

        return arrow_utils.join_arrow_schemas(*all_schemas)

    def identity_structure(self) -> Any:
        """Return the primary data table as this datagram's identity.

        The semantic hasher dispatches ``pa.Table`` to ``ArrowTableHandler``,
        which delegates to the data context's ``arrow_hasher``.  This means
        ``content_hash()`` (inherited from ``ContentIdentifiableBase``) produces
        a stable, content-addressed hash of the data columns without any
        special-casing in ``Datagram`` itself.
        """
        return self._ensure_data_table()

    @property
    def datagram_id(self) -> str:
        """Return (or lazily generate) the datagram's unique ID."""
        if self._datagram_id is None:
            self._datagram_id = str(uuid7())
        return self._datagram_id

    @property
    def converter(self) -> TypeConverterProtocol:
        """Semantic type converter for this datagram's data context."""
        return self.data_context.type_converter

    def with_context_key(self, new_context_key: str) -> Self:
        """Create a new datagram with a different data-context key."""
        new_datagram = self.copy(include_cache=False)
        new_datagram._data_context = contexts.resolve_context(new_context_key)
        return new_datagram

    # ------------------------------------------------------------------
    # 4. Format Conversions
    # ------------------------------------------------------------------

    def as_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        result = dict(self._ensure_data_dict())

        if column_config.context:
            result[constants.CONTEXT_KEY] = self._data_context.context_key

        if column_config.meta and self._meta:
            include_meta = column_config.meta
            if include_meta is True:
                result.update(self._meta)
            elif isinstance(include_meta, Collection):
                result.update(
                    {
                        k: v
                        for k, v in self._meta.items()
                        if any(k.startswith(p) for p in include_meta)
                    }
                )

        return result

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        column_config = ColumnConfig.handle_config(columns, all_info=all_info)
        all_tables = [self._ensure_data_table()]

        if column_config.context:
            all_tables.append(self._ensure_context_table())

        if column_config.meta and self._meta:
            meta_table = self._ensure_meta_table()
            if meta_table is not None:
                include_meta = column_config.meta
                if include_meta is True:
                    all_tables.append(meta_table)
                elif isinstance(include_meta, Collection):
                    # Normalize: ensure all given prefixes start with META_PREFIX
                    prefixes = [
                        p
                        if p.startswith(constants.META_PREFIX)
                        else f"{constants.META_PREFIX}{p}"
                        for p in include_meta
                    ]
                    matched_cols = [
                        c
                        for c in meta_table.column_names
                        if any(c.startswith(p) for p in prefixes)
                    ]
                    if matched_cols:
                        all_tables.append(meta_table.select(matched_cols))

        return arrow_utils.hstack_tables(*all_tables)

    def as_arrow_compatible_dict(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> dict[str, DataValue]:
        return self.as_table(columns=columns, all_info=all_info).to_pylist()[0]

    # ------------------------------------------------------------------
    # 5. Meta Column Operations
    # ------------------------------------------------------------------

    def get_meta_value(self, key: str, default: DataValue = None) -> DataValue:
        if not key.startswith(constants.META_PREFIX):
            key = constants.META_PREFIX + key
        return self._meta.get(key, default)

    def get_meta_info(self) -> dict[str, DataValue]:
        return dict(self._meta)

    def with_meta_columns(self, **meta_updates: DataValue) -> Self:
        prefixed = {
            k
            if k.startswith(constants.META_PREFIX)
            else f"{constants.META_PREFIX}{k}": v
            for k, v in meta_updates.items()
        }
        new_d = self.copy(include_cache=False)
        new_d._meta = {**self._meta, **prefixed}
        new_d._meta_python_schema = infer_python_schema_from_pylist_data(
            [new_d._meta], default_type=str
        )
        return new_d

    def drop_meta_columns(self, *keys: str, ignore_missing: bool = False) -> Self:
        prefixed = {
            k if k.startswith(constants.META_PREFIX) else f"{constants.META_PREFIX}{k}"
            for k in keys
        }
        missing = prefixed - set(self._meta.keys())
        if missing and not ignore_missing:
            raise KeyError(
                f"Following meta columns do not exist and cannot be dropped: {sorted(missing)}"
            )
        new_d = self.copy(include_cache=False)
        new_d._meta = {k: v for k, v in self._meta.items() if k not in prefixed}
        new_d._meta_python_schema = infer_python_schema_from_pylist_data(
            [new_d._meta], default_type=str
        )
        return new_d

    # ------------------------------------------------------------------
    # 6. Data Column Operations (prefer Arrow when loaded)
    # ------------------------------------------------------------------

    def select(self, *column_names: str) -> Self:
        if self._data_table is not None:
            missing = set(column_names) - set(self._data_table.column_names)
            if missing:
                raise ValueError(f"Columns not found: {missing}")
            new_d = self.copy(include_cache=False)
            new_d._data_table = self._data_table.select(list(column_names))
            new_d._data_dict = None
            new_d._data_python_schema = None
            new_d._data_arrow_schema = None
            return new_d
        else:
            assert self._data_dict is not None
            missing = set(column_names) - set(self._data_dict.keys())
            if missing:
                raise ValueError(f"Columns not found: {missing}")
            schema = self._ensure_python_schema()
            new_d = self.copy(include_cache=False)
            new_d._data_dict = {
                k: v for k, v in self._data_dict.items() if k in column_names
            }
            new_d._data_python_schema = Schema(
                {k: v for k, v in schema.items() if k in column_names}
            )
            return new_d

    def drop(self, *column_names: str, ignore_missing: bool = False) -> Self:
        if self._data_table is not None:
            missing = set(column_names) - set(self._data_table.column_names)
            if missing and not ignore_missing:
                raise KeyError(
                    f"Following columns do not exist and cannot be dropped: {sorted(missing)}"
                )
            existing = [c for c in column_names if c in self._data_table.column_names]
            new_d = self.copy(include_cache=False)
            if existing:
                new_d._data_table = self._data_table.drop_columns(existing)
            new_d._data_dict = None
            new_d._data_python_schema = None
            new_d._data_arrow_schema = None
            return new_d
        else:
            assert self._data_dict is not None
            missing = set(column_names) - set(self._data_dict.keys())
            if missing and not ignore_missing:
                raise KeyError(
                    f"Following columns do not exist and cannot be dropped: {sorted(missing)}"
                )
            new_data = {
                k: v for k, v in self._data_dict.items() if k not in column_names
            }
            if not new_data:
                raise ValueError("Cannot drop all data columns")
            schema = self._ensure_python_schema()
            new_d = self.copy(include_cache=False)
            new_d._data_dict = new_data
            new_d._data_python_schema = Schema(
                {k: v for k, v in schema.items() if k in new_data}
            )
            return new_d

    def rename(self, column_mapping: Mapping[str, str]) -> Self:
        if not column_mapping:
            return self
        if self._data_table is not None:
            new_names = [
                column_mapping.get(k, k) for k in self._data_table.column_names
            ]
            new_d = self.copy(include_cache=False)
            new_d._data_table = self._data_table.rename_columns(new_names)
            new_d._data_dict = None
            new_d._data_python_schema = None
            new_d._data_arrow_schema = None
            return new_d
        else:
            assert self._data_dict is not None
            schema = self._ensure_python_schema()
            new_d = self.copy(include_cache=False)
            new_d._data_dict = {
                column_mapping.get(k, k): v for k, v in self._data_dict.items()
            }
            new_d._data_python_schema = Schema(
                {column_mapping.get(k, k): v for k, v in schema.items()}
            )
            return new_d

    def update(self, **updates: DataValue) -> Self:
        if not updates:
            return self

        data_keys = (
            set(self._data_table.column_names)
            if self._data_table is not None
            else set(self._data_dict.keys())  # type: ignore[union-attr]
        )
        missing = set(updates.keys()) - data_keys
        if missing:
            raise KeyError(
                f"Only existing columns can be updated. "
                f"Following columns were not found: {sorted(missing)}"
            )

        if self._data_table is not None and self._data_dict is None:
            # Arrow-native update: preserves type precision without loading full dict
            sub_schema = arrow_utils.schema_select(
                self._data_table.schema, list(updates.keys())
            )
            update_table = self.converter.python_dicts_to_arrow_table(
                [updates], arrow_schema=sub_schema
            )
            new_d = self.copy(include_cache=False)
            new_d._data_table = arrow_utils.hstack_tables(
                self._data_table.drop_columns(list(updates.keys())), update_table
            ).select(self._data_table.column_names)
            new_d._data_dict = None
            new_d._data_python_schema = None
            new_d._data_arrow_schema = None
            return new_d
        else:
            assert self._data_dict is not None
            new_d = self.copy(include_cache=False)
            new_d._data_dict = {**self._data_dict, **updates}
            new_d._data_table = None
            return new_d

    def with_columns(
        self,
        column_types: "Mapping[str, type] | None" = None,
        **updates: DataValue,
    ) -> Self:
        if not updates:
            return self

        data_keys = (
            set(self._data_table.column_names)
            if self._data_table is not None
            else set(self._data_dict.keys())  # type: ignore[union-attr]
        )
        existing_overlaps = set(updates.keys()) & data_keys
        if existing_overlaps:
            raise ValueError(
                f"Columns already exist: {sorted(existing_overlaps)}. "
                f"Use update() to modify existing columns."
            )

        if self._data_table is not None and self._data_dict is None:
            new_data_table = self.converter.python_dicts_to_arrow_table(
                [updates],
                python_schema=dict(column_types) if column_types else None,
            )
            new_d = self.copy(include_cache=False)
            new_d._data_table = arrow_utils.hstack_tables(
                self._data_table, new_data_table
            )
            new_d._data_python_schema = None
            new_d._data_arrow_schema = None
            return new_d
        else:
            assert self._data_dict is not None
            new_data = {**self._data_dict, **updates}
            schema = dict(self._ensure_python_schema())
            if column_types:
                schema.update(column_types)
            inferred = infer_python_schema_from_pylist_data([new_data])
            new_schema = Schema(
                {k: schema.get(k, inferred.get(k, str)) for k in new_data}
            )
            new_d = self.copy(include_cache=False)
            new_d._data_dict = new_data
            new_d._data_python_schema = new_schema
            new_d._data_table = None
            return new_d

    # ------------------------------------------------------------------
    # 8. Utility Operations
    # ------------------------------------------------------------------

    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        new_d = object.__new__(self.__class__)

        # Fields from ContentIdentifiableBase / DataContextMixin
        new_d._data_context = self._data_context
        new_d._orcapod_config = self._orcapod_config
        new_d._content_hash_cache = (
            dict(self._content_hash_cache) if include_cache else {}
        )
        new_d._cached_int_hash = None

        # Datagram identity
        new_d._datagram_id = self._datagram_id if preserve_id else None

        # Data representations — Arrow table is immutable so a ref copy is fine
        new_d._data_table = self._data_table
        new_d._data_dict = (
            dict(self._data_dict) if self._data_dict is not None else None
        )
        new_d._data_python_schema = Schema(
            dict(self._data_python_schema)
            if self._data_python_schema is not None
            else None
        )
        new_d._data_arrow_schema = self._data_arrow_schema

        # Meta — always dict
        new_d._meta = dict(self._meta)
        new_d._meta_python_schema = Schema(self._meta_python_schema)

        if include_cache:
            new_d._meta_table = self._meta_table
            new_d._context_table = self._context_table
        else:
            new_d._meta_table = None
            new_d._context_table = None

        return new_d

    # ------------------------------------------------------------------
    # 9. String Representations
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        if self._data_dict is not None:
            return str(self._data_dict)
        return str(self.as_dict())

    def __repr__(self) -> str:
        return self.__str__()
