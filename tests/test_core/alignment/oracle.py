"""What every row of a test's sources is, derived from the rows the test wrote (ITL-1034).

The oracle never reads a table Orcapod produced. It takes the Arrow table a test hands to a
source, and recomputes each row's provenance token and record id by the recipe the library
documents (``docs/reference/hashing.md``, site 5): the token is ``"<column>=<value>"`` with a
``record_id_column``, else ``"row_<index>"``; the record id is the UUID v5 of
``"<source_id>::<token>"``. Aggregated system tags fold by site 6b.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

# The namespace of a source row's record id. The code (``core/sources/stream_builder.py``,
# ``_SOURCE_RECORD_ID_NAMESPACE``) is the authority: docs/reference/hashing.md site 5 names
# another string, a documentation error reported with ITL-1034.
SOURCE_RECORD_ID_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://orcapod.org/namespaces/source-record-id"
)
# The namespace of a folded (aggregated) record id: docs/reference/hashing.md site 6b.
AGGREGATED_RECORD_ID_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://orcapod.org/namespaces/aggregated-record-id"
)
BLOCK_SEPARATOR = "::"


def source_record_id(source_id: str, token: str) -> bytes:
    """The record id of the source row ``token`` of ``source_id`` (site 5)."""
    return uuid.uuid5(
        SOURCE_RECORD_ID_NAMESPACE, f"{source_id}{BLOCK_SEPARATOR}{token}"
    ).bytes


def fold_record_ids(record_ids: Sequence[bytes | None]) -> bytes:
    """The folded record id of an ordered member list (site 6b)."""
    name = BLOCK_SEPARATOR.join("null" if v is None else v.hex() for v in record_ids)
    return uuid.uuid5(AGGREGATED_RECORD_ID_NAMESPACE, name).bytes


def fold_values(values: Sequence[Any]) -> str:
    """The folded value of any other system tag of an ordered member list (site 6b)."""
    members = json.dumps(
        [None if v is None else str(v) for v in values],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(members.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Origin:
    """One row of one source, as the test wrote it.

    Attributes:
        source_id: The source's id.
        token: The row's provenance token within the source.
        record_id: The row's record id, recomputed from ``source_id`` and ``token``.
        values: Every column of the row (tags and data), as Python values.
        data_table: The row's data columns as a one-row Arrow table with the source's
            types: what the source's ``Data`` hashes.
    """

    source_id: str
    token: str
    record_id: bytes
    values: Mapping[str, Any]
    data_table: pa.Table = field(compare=False, repr=False)


@dataclass(frozen=True)
class Derivation:
    """A column a function computes from one origin row.

    Attributes:
        source_id: The source whose row is the function's input.
        compute: The value the function returns for that row.
        token: The source-info token the column has for that row, when it can be
            recomputed from the row (an operator's ``Pick`` or ``Index``); ``None`` for
            a function's output, whose token names a datagram made at run time.
    """

    source_id: str
    compute: Callable[[Origin], Any]
    token: Callable[[Origin], str] | None = None


class Oracle:
    """Every origin row of a test, and how each output column relates to them."""

    def __init__(self) -> None:
        """Build an oracle with no sources."""
        self.by_record_id: dict[bytes, Origin] = {}
        self.by_token: dict[tuple[str, str], Origin] = {}
        self.origins: list[Origin] = []
        self.derivations: dict[str, Derivation] = {}
        self.renames: dict[str, str] = {}
        self.schema_hashes: dict[str, str] = {}

    def add_source(
        self,
        source_id: str,
        table: pa.Table,
        *,
        tag_columns: Sequence[str],
        schema_hash: str,
        record_id_column: str | None = None,
        tokens: Sequence[str] | None = None,
    ) -> None:
        """Register the rows of a source from the table the test gave it.

        Args:
            source_id: The id the test gave the source.
            table: The table the test gave the source, in its row order.
            tag_columns: The source's tag columns.
            schema_hash: The schema hash that names the source's system tag columns
                (``_tag_source_id::<schema hash>``). Every system tag column holding
                this source's rows, however extended downstream, starts with it.
            record_id_column: The source's record id column, if any.
            tokens: Each row's provenance token, for a source with its own documented
                token recipe (SpiralDB: the labelled Arrow hash of the row's key),
                computed by the test from its own rows.
        """
        self.schema_hashes[source_id] = schema_hash
        data_columns = [c for c in table.column_names if c not in tag_columns]
        for index, values in enumerate(table.to_pylist()):
            if tokens is not None:
                token = tokens[index]
            elif record_id_column is not None:
                token = f"{record_id_column}={values[record_id_column]}"
            else:
                token = f"row_{index}"
            origin = Origin(
                source_id=source_id,
                token=token,
                record_id=source_record_id(source_id, token),
                values=values,
                data_table=table.slice(index, 1).select(data_columns),
            )
            self.origins.append(origin)
            self.by_record_id[origin.record_id] = origin
            self.by_token[(source_id, token)] = origin

    def derive(
        self,
        column: str,
        source_id: str,
        compute: Callable[[Origin], Any],
        token: Callable[[Origin], str] | None = None,
    ) -> None:
        """Declare that the output column ``column`` is computed from a ``source_id`` row.

        Args:
            column: The output column.
            source_id: The source whose row the value is computed from.
            compute: The value for that row.
            token: The column's source-info token for that row, when recomputable.
        """
        self.derivations[column] = Derivation(source_id, compute, token)

    def rename(self, new: str, old: str) -> None:
        """Declare that the output column ``new`` is the input column ``old`` renamed."""
        self.renames[new] = old

    def base_name(self, column: str) -> str:
        """The input column an output column holds, following the whole rename chain.

        Raises:
            ValueError: If the renames form a cycle.
        """
        seen = {column}
        while column in self.renames:
            column = self.renames[column]
            if column in seen:
                raise ValueError(f"the renames form a cycle at {column!r}")
            seen.add(column)
        return column

    def _derivation(self, column: str) -> Derivation | None:
        """The derivation of ``column``, or of the nearest name in its rename chain.

        Raises:
            ValueError: If the renames form a cycle.
        """
        seen = {column}
        while True:
            derivation = self.derivations.get(column)
            if derivation is not None or column not in self.renames:
                return derivation
            column = self.renames[column]
            if column in seen:
                raise ValueError(f"the renames form a cycle at {column!r}")
            seen.add(column)

    def origins_for_value(self, column: str, value: Any) -> list[Origin]:
        """Every origin whose value of the output column ``column`` is ``value``."""
        derivation = self._derivation(column)
        if derivation is not None:
            return [
                o
                for o in self.origins
                if o.source_id == derivation.source_id
                and derivation.compute(o) == value
            ]
        name = self.base_name(column)
        return [o for o in self.origins if name in o.values and o.values[name] == value]

    def expected_value(self, column: str, origin: Origin) -> Any:
        """The value the output column ``column`` has for a row whose origin is ``origin``."""
        derivation = self._derivation(column)
        if derivation is not None:
            return derivation.compute(origin)
        return origin.values[self.base_name(column)]

    def holds(self, column: str, origin: Origin) -> bool:
        """Whether a row of ``origin``'s source gives the output column ``column``."""
        derivation = self._derivation(column)
        if derivation is not None:
            return derivation.source_id == origin.source_id
        return self.base_name(column) in origin.values
