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
import uuid
from collections.abc import Collection, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Self, cast

from uuid_utils import uuid7

from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.core.base import ContentIdentifiableBase
from orcapod.errors import CanonicalOrderError
from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol
from orcapod.semantic_types import infer_python_schema_from_pylist_data
from orcapod.semantic_types.canonical_order import canonical_python_value
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, DataType, DataValue, Schema, SchemaLike
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
        record_uuid: uuid.UUID | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
    ) -> None:
        if isinstance(data, pa.RecordBatch):
            data = pa.Table.from_batches([data])

        if isinstance(data, pa.Table):
            self._init_from_table(data, meta_info, data_context, record_uuid)
        else:
            self._init_from_dict(
                data, python_schema, meta_info, data_context, record_uuid
            )

    def _init_from_dict(
        self,
        data: Mapping[str, DataValue],
        python_schema: SchemaLike | None,
        meta_info: Mapping[str, DataValue] | None,
        data_context: str | contexts.DataContext | None,
        record_uuid: uuid.UUID | None,
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
        self._datagram_uuid = record_uuid

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
        self._data_dict = self._canonical_dict_values(
            data_columns, self._data_python_schema
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

    def _canonical_dict_values(
        self, columns: dict[str, DataValue], schema: Schema
    ) -> dict[str, DataValue]:
        """Return ``columns`` with every dict value in canonical key order.

        A dict is order independent (ITL-905): its entries are stored and hashed in
        canonical key order, so a value built here holds the same order a stored copy
        reads back with. Dicts are copied, never mutated; dicts inside other objects
        (dataclass or pydantic fields) are left as given.
        """
        out: dict[str, DataValue] = {}
        for name, value in columns.items():
            try:
                out[name] = canonical_python_value(
                    value,
                    schema.get(name),
                    self.converter.get_python_to_arrow_converter,
                )
            except CanonicalOrderError as exc:
                raise CanonicalOrderError(f"Column {name!r}: {exc}") from exc
        return out

    def _init_from_table(
        self,
        table: pa.Table,
        meta_info: Mapping[str, DataValue] | None,
        data_context: str | contexts.DataContext | None,
        record_uuid: uuid.UUID | None,
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
        self._datagram_uuid = record_uuid

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

        # Store meta as dict (always); keep the Arrow table when available.
        # IMPORTANT: use the universal converter (not raw to_pylist()) to
        # convert Arrow → Python so that complex types round-trip correctly.
        # For example, Arrow map columns (used for dict[str, str]) become
        # list[struct] via to_pylist() but proper Python dicts via the
        # converter. Without the converter, downstream schema inference
        # produces list[Any] which cannot be converted back to Arrow.
        #
        # Derive the Python schema from the Arrow schema (not from inference)
        # to preserve precise types even for empty containers (e.g. an empty
        # dict {} would infer as dict[Any, Any] but the Arrow schema knows
        # it's dict[str, str]).
        if meta_table is not None and meta_table.num_columns > 0:
            self._meta = self.converter.arrow_table_to_python_dicts(meta_table)[0]
            self._meta_python_schema = self.converter.arrow_schema_to_python_schema(
                meta_table.schema
            )
            self._meta_table = meta_table
        else:
            self._meta = {}
            self._meta_python_schema = Schema.empty()
            self._meta_table = None
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
            self._data_python_schema = self.converter.arrow_schema_to_python_schema(
                self._data_table.schema
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
            schema = pa.schema(
                [pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False)]
            )
            self._context_table = pa.Table.from_pylist(
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
    def datagram_uuid(self) -> uuid.UUID:
        """Return (or lazily generate) the datagram's unique UUID."""
        if self._datagram_uuid is None:
            # uuid_utils is used here because it provides UUIDv7 (time-ordered,
            # monotonic) generation, which is not available in the stdlib before
            # Python 3.12.  However, uuid_utils.UUID and stdlib uuid.UUID are
            # distinct types that do not compare equal even for identical bit
            # patterns.  We therefore normalise to stdlib uuid.UUID immediately
            # so that the public API always returns a consistent type and
            # equality / hashing work correctly regardless of how a UUID was
            # originally produced.
            self._datagram_uuid = uuid.UUID(bytes=uuid7().bytes)
        return self._datagram_uuid

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
        """Return a copy with meta columns added or overwritten.

        Untouched meta columns keep their Arrow fields verbatim; only the
        columns being set are built from their Python types.

        Args:
            **meta_updates: Meta column values, keyed with or without the
                meta prefix.

        Returns:
            A new datagram with the updated meta columns.
        """
        prefixed = {
            k
            if k.startswith(constants.META_PREFIX)
            else f"{constants.META_PREFIX}{k}": v
            for k, v in meta_updates.items()
        }
        new_d = self.copy(include_cache=False)
        new_d._meta = {**self._meta, **prefixed}
        # Preserve existing schema types for unchanged fields. This avoids
        # re-inference which can lose precise types (e.g. an empty dict {}
        # infers as dict[Any, Any] instead of dict[str, str], and
        # arrow-derived types like timestamps may use Any).
        new_inferred = infer_python_schema_from_pylist_data(
            [prefixed], default_type=str
        )
        merged = dict(self._meta_python_schema)
        merged.update(new_inferred)
        new_d._meta_python_schema = Schema(merged)
        # Carry the Arrow fields of untouched meta columns verbatim; only the
        # columns being set are (re)built from their Python types.
        old_table = self._ensure_meta_table()
        tables: list[pa.Table] = []
        if old_table is not None:
            tables.append(
                old_table.drop_columns(
                    [c for c in prefixed if c in old_table.column_names]
                )
            )
        if prefixed:
            tables.append(
                self.converter.python_dicts_to_arrow_table(
                    [prefixed],
                    python_schema=Schema({k: merged[k] for k in prefixed}),
                )
            )
        meta_names = list(new_d.meta_columns)
        new_d._meta_table = (
            arrow_utils.hstack_tables(*tables).select(meta_names)
            if meta_names
            else None
        )
        return new_d

    def drop_meta_columns(self, *keys: str, ignore_missing: bool = False) -> Self:
        """Return a copy without the given meta columns.

        The remaining meta columns keep their Arrow fields verbatim.

        Args:
            *keys: Meta column names, with or without the meta prefix.
            ignore_missing: If True, names that are not present are skipped.

        Returns:
            A new datagram without the dropped meta columns.

        Raises:
            KeyError: If a column is missing and ``ignore_missing`` is False.
        """
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
        # Preserve existing schema types for remaining fields (see
        # with_meta_columns for rationale).
        new_d._meta_python_schema = Schema(
            {k: v for k, v in self._meta_python_schema.items() if k not in prefixed}
        )
        # Remaining meta columns keep their Arrow fields verbatim.
        old_table = self._ensure_meta_table()
        remaining = list(new_d.meta_columns)
        new_d._meta_table = (
            old_table.select(remaining) if old_table is not None and remaining else None
        )
        return new_d

    # ------------------------------------------------------------------
    # 6. Data Column Operations (prefer Arrow when loaded)
    # ------------------------------------------------------------------

    def select(self, *column_names: str) -> Self:
        """Return a copy holding only the given data columns.

        When the Arrow table is loaded the selected columns keep their Arrow
        fields; the cached Arrow schema is reset in either case.

        Args:
            *column_names: Data columns to keep, in the order given.

        Returns:
            A new datagram with only the selected data columns.

        Raises:
            ValueError: If any named column does not exist.
        """
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
            new_d._data_arrow_schema = None
            return new_d

    def drop(self, *column_names: str, ignore_missing: bool = False) -> Self:
        """Return a copy without the given data columns.

        When the Arrow table is loaded the remaining columns keep their Arrow
        fields; the cached Arrow schema is reset in either case.

        Args:
            *column_names: Data columns to drop.
            ignore_missing: If True, names that are not present are skipped.

        Returns:
            A new datagram without the dropped data columns.

        Raises:
            KeyError: If a column is missing and ``ignore_missing`` is False.
            ValueError: If a dict-backed datagram would be left with no data
                columns.
        """
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
            new_d._data_arrow_schema = None
            return new_d

    def rename(self, column_mapping: Mapping[str, str]) -> Self:
        """Return a copy with data columns renamed.

        When the Arrow table is loaded each renamed column keeps its Arrow
        type; the cached Arrow schema is reset in either case.

        Args:
            column_mapping: Old column names mapped to new ones. Columns not
                in the mapping keep their names.

        Returns:
            A new datagram with the renamed columns, or ``self`` if the
            mapping is empty.
        """
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
            new_d._data_arrow_schema = None
            return new_d

    def update(self, **updates: DataValue) -> Self:
        """Return a copy with values of existing data columns replaced.

        Whenever the Arrow table is loaded, each updated column keeps its
        exact Arrow field and untouched columns are carried through verbatim.

        Args:
            **updates: Existing column names mapped to their new values.

        Returns:
            A new datagram with the updated values, or ``self`` if no updates
            are given.

        Raises:
            KeyError: If any named column does not exist.
        """
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

        if self._data_table is not None:
            # Arrow-native update whenever the Arrow table is known (even if the
            # dict cache is also loaded): each updated column keeps its exact
            # Arrow field, and untouched columns are carried through verbatim.
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
            # Updated dict values are held in canonical key order, as at
            # construction (ITL-905); the caller's dicts are copied, not mutated.
            canonical_updates = self._canonical_dict_values(
                updates, self._ensure_python_schema()
            )
            new_d = self.copy(include_cache=False)
            new_d._data_dict = {**self._data_dict, **canonical_updates}
            new_d._data_table = None
            return new_d

    def _new_columns_table(
        self,
        updates: dict[str, DataValue],
        column_types: Mapping[str, DataType | pa.DataType | pa.Field[Any]],  # pyright: ignore[reportExplicitAny]  # Field is invariant
    ) -> pa.Table:
        """Build a one-row Arrow table holding only the genuinely new columns.

        Each column's Arrow type comes from ``column_types`` when given there:
        a ``pa.Field`` is used verbatim, nullability included (only its name is
        replaced by the column's); a ``pa.DataType`` is used verbatim (nullable
        only when the value is ``None``); a Python type is converted. Columns
        absent from ``column_types`` are inferred from their value.

        Args:
            updates: New column names mapped to their values.
            column_types: Declared types for some or all of the new columns.

        Returns:
            A one-row table whose columns follow the order of ``updates``.

        Raises:
            ValueError: If a column declared by a non-nullable ``pa.Field`` has
                the value ``None``.
        """
        arrow_fields: dict[str, pa.Field[pa.DataType]] = {}
        python_schema: dict[str, DataType] = {}
        # Iterate the declarations, not ``updates``: binding an ``updates`` value
        # to a loop variable makes it partially unknown (``DataValue`` contains
        # the unparametrized ``os.PathLike``). Output order is restored by the
        # final ``select``.
        for name, declared in column_types.items():
            if name not in updates:
                continue
            if isinstance(declared, pa.Field):
                if updates[name] is None and not declared.nullable:
                    raise ValueError(
                        f"Column {name!r} is declared non-nullable"
                        + f" ({declared}) but its value is None"
                    )
                arrow_fields[name] = declared.with_name(name)
            elif isinstance(declared, pa.DataType):
                arrow_fields[name] = pa.field(
                    name, declared, nullable=updates[name] is None
                )
            else:
                python_schema[name] = declared
        inferred_names = [n for n in updates if n not in column_types]
        if inferred_names:
            python_schema.update(
                infer_python_schema_from_pylist_data(
                    [{n: updates[n] for n in inferred_names}]
                )
            )

        tables: list[pa.Table] = []
        if python_schema:
            tables.append(
                self.converter.python_dicts_to_arrow_table(
                    [{n: updates[n] for n in python_schema}],
                    python_schema=python_schema,
                )
            )
        if arrow_fields:
            tables.append(
                self.converter.python_dicts_to_arrow_table(
                    [{n: updates[n] for n in arrow_fields}],
                    arrow_schema=pa.schema(list(arrow_fields.values())),
                )
            )
        return arrow_utils.hstack_tables(*tables).select(list(updates))

    def with_columns(
        self,
        column_types: Mapping[str, DataType | pa.DataType | pa.Field[Any]]  # pyright: ignore[reportExplicitAny]  # Field is invariant; Any admits every Arrow type
        | None = None,
        **updates: DataValue,
    ) -> Self:
        """Return a copy with new data columns added.

        Whenever the Arrow table is loaded, or an Arrow type is declared, the
        Arrow path is taken and only the new columns are typed (declared) or
        inferred (undeclared). If the Arrow table is loaded, existing columns
        keep their exact Arrow fields. On a dict-only datagram with a declared
        Arrow type, no Arrow fields are known yet, so existing columns are
        converted from the Python schema, as ``as_table()`` would convert
        them. A dict-only datagram without declared Arrow types keeps its
        Python-schema behaviour.

        Args:
            column_types: Optional types for some or all new columns. A
                ``pa.Field`` is used verbatim, nullability included (its name
                is replaced by the column's); a ``pa.DataType`` is used
                verbatim (nullable only when the value is ``None``); a Python
                type is converted.
            **updates: New column names mapped to their values.

        Returns:
            A new datagram with the added columns, or ``self`` if no columns
            are given.

        Raises:
            ValueError: If any named column already exists, or a column
                declared by a non-nullable ``pa.Field`` has the value ``None``.
        """
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

        has_arrow_types = column_types is not None and any(
            isinstance(t, (pa.DataType, pa.Field)) for t in column_types.values()
        )
        if self._data_table is not None or has_arrow_types:
            # Arrow-native path whenever an Arrow type is known — the table is
            # loaded (even if the dict cache is too) or the caller declared an
            # Arrow type. Existing columns keep their Arrow fields (converted
            # from the Python schema if the table was not loaded); only the new
            # columns are typed (declared) or inferred (undeclared).
            new_d = self.copy(include_cache=False)
            new_d._data_table = arrow_utils.hstack_tables(
                self._ensure_data_table(),
                self._new_columns_table(updates, column_types or {}),
            )
            new_d._data_dict = None
            new_d._data_python_schema = None
            new_d._data_arrow_schema = None
            return new_d
        else:
            assert self._data_dict is not None
            new_data = {**self._data_dict, **updates}
            schema = dict(self._ensure_python_schema())
            if column_types:
                # No Arrow types reach this branch (they take the Arrow path).
                schema.update(
                    {
                        k: t
                        for k, t in column_types.items()
                        if not isinstance(t, (pa.DataType, pa.Field))
                    }
                )
            inferred = infer_python_schema_from_pylist_data([new_data])
            new_schema = Schema(
                {k: schema.get(k, inferred.get(k, str)) for k in new_data}
            )
            new_d = self.copy(include_cache=False)
            # New dict values are held in canonical key order, as at construction
            # (ITL-905); the caller's dicts are copied, not mutated.
            new_d._data_dict = self._canonical_dict_values(new_data, new_schema)
            new_d._data_python_schema = new_schema
            new_d._data_table = None
            new_d._data_arrow_schema = None
            return new_d

    # ------------------------------------------------------------------
    # 8. Utility Operations
    # ------------------------------------------------------------------

    def copy(self, include_cache: bool = True, preserve_id: bool = True) -> Self:
        """Return a shallow copy of this datagram.

        The data Arrow table and the meta Arrow table are shared (Arrow tables
        are immutable), so a copy keeps every column's exact Arrow type.

        Args:
            include_cache: If True, cached content hashes and the context
                table are carried over.
            preserve_id: If True, the copy keeps this datagram's UUID.

        Returns:
            A new datagram of the same class.
        """
        new_d = object.__new__(self.__class__)

        # Fields from ContentIdentifiableBase / DataContextMixin
        new_d._data_context = self._data_context
        new_d._orcapod_config = self._orcapod_config
        new_d._content_hash_cache = (
            dict(self._content_hash_cache) if include_cache else {}
        )
        new_d._cached_int_hash = None

        # Datagram identity
        new_d._datagram_uuid = self._datagram_uuid if preserve_id else None

        # Data representations — Arrow table is immutable so a ref copy is fine
        new_d._data_table = self._data_table
        new_d._data_dict = (
            dict(self._data_dict) if self._data_dict is not None else None
        )
        # A lazy (None) Python schema stays lazy; wrapping None in Schema would
        # make it an empty schema that is never recomputed.
        new_d._data_python_schema = (
            Schema(dict(self._data_python_schema))
            if self._data_python_schema is not None
            else None
        )
        new_d._data_arrow_schema = self._data_arrow_schema

        # Meta — always dict
        new_d._meta = dict(self._meta)
        new_d._meta_python_schema = Schema(self._meta_python_schema)

        # The meta Arrow table is not just a cache: for an Arrow-born datagram it
        # holds the meta columns' exact Arrow types, which rebuilding from the
        # Python schema would lose (e.g. int8 -> int64). It always matches
        # ``_meta`` here; the meta mutators replace it when they change ``_meta``.
        new_d._meta_table = self._meta_table
        new_d._context_table = self._context_table if include_cache else None

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
