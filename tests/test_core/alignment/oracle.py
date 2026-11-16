"""What every row of a test's sources is, derived from the rows the test wrote (ITL-1034).

The oracle never reads a table Orcapod produced. It takes the Arrow table a test hands to a
source, and recomputes each row's record id by the recipe the library documents
(``docs/reference/hashing.md``, "Default source ids and row identifiers"; ITL-1154): the
record id is the row identifier itself, the canonical JSON object of the key columns in key
order (``{"x":3}``) with ``record_id_columns``, else ``"row_<index>"``; a source-info token
is ``"<source_id>::<record id>::<column>"``. Aggregated system tags fold by site 6b.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

BLOCK_SEPARATOR = "::"


def key_record_id(values: Mapping[str, Any], key_columns: Sequence[str]) -> str:
    """The record id of a row identified by ``key_columns``: their canonical JSON."""
    return json.dumps(
        {c: values[c] for c in key_columns}, separators=(",", ":"), ensure_ascii=False
    )


def fold_values(values: Sequence[Any], label: str) -> str:
    """The folded value of any other system tag of an ordered member list (site 6b).

    ``"<label>:<hex>"``: the fold's label (``BATCH_FOLD_LABEL`` or
    ``GROUP_BY_FOLD_LABEL``, each defined once beside the fold), then the SHA-256 of the
    canonical JSON member list, computed here on its own.
    """
    members = json.dumps(
        [None if v is None else str(v) for v in values],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return f"{label}:{hashlib.sha256(members.encode('utf-8')).hexdigest()}"


@dataclass(frozen=True)
class Origin:
    """One row of one source, as the test wrote it.

    Attributes:
        source_id: The source's id.
        token: The row's provenance token within the source.
        record_id: The row's record id: its row identifier, the same as ``token``.
        values: Every column of the row (tags and data), as Python values.
        data_table: The row's data columns as a one-row Arrow table with the source's
            types: what the source's ``Data`` hashes.
    """

    source_id: str
    token: str
    record_id: str
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
        # A record id names a row within its source: keyed by (source id, record id).
        self.by_record_id: dict[tuple[str, str], Origin] = {}
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
        record_id_columns: str | Sequence[str] | None = None,
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
            record_id_columns: The source's key columns, if any.
            tokens: Each row's record id, for a source whose key the test computes
                from its own rows (SpiralDB: its primary key).
        """
        self.schema_hashes[source_id] = schema_hash
        key_columns = (
            (record_id_columns,)
            if isinstance(record_id_columns, str)
            else tuple(record_id_columns or ())
        )
        data_columns = [c for c in table.column_names if c not in tag_columns]
        for index, values in enumerate(table.to_pylist()):
            if tokens is not None:
                token = tokens[index]
            elif key_columns:
                token = key_record_id(values, key_columns)
            else:
                token = f"row_{index}"
            origin = Origin(
                source_id=source_id,
                token=token,
                record_id=token,
                values=values,
                data_table=table.slice(index, 1).select(data_columns),
            )
            self.origins.append(origin)
            self.by_record_id[(source_id, origin.record_id)] = origin
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
