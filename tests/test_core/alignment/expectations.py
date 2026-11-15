"""The rows a table must hold, derived from the fixtures by each operator's rule (ITL-1034).

A table whose every row is aligned can still be wrong as a whole: a row dropped and
another repeated in its place, or a join that returns nothing. These functions build, from
the rows the test wrote (the ``Oracle``) and the documented rule of each operation, the
multiset of row identities a table must hold. They never read a table Orcapod produced.

A row identity is a tuple of members, one member unless the row aggregates several input
rows; a member is the sorted ``(source id, provenance token)`` pairs of its origin rows.
Aggregated rows list their members sorted, since their order is the operator's choice.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from .checker import RowIdentity
from .oracle import Oracle, Origin

KEYS = ("g", "n")


def member(*origins: Origin) -> tuple[tuple[str, str], ...]:
    """One member: the sorted (source id, token) pairs of its origin rows."""
    return tuple(sorted((o.source_id, o.token) for o in origins))


def of_source(oracle: Oracle, source_id: str) -> list[Origin]:
    """The rows of ``source_id``, in the order the test wrote them."""
    return [o for o in oracle.origins if o.source_id == source_id]


def rows(
    oracle: Oracle, source_id: str, keep: Callable[[Origin], bool] = lambda o: True
) -> list[RowIdentity]:
    """One row per row of ``source_id`` that ``keep`` keeps (a pass-through or a filter)."""
    return [(member(o),) for o in of_source(oracle, source_id) if keep(o)]


def keys_match(left: Origin, right: Origin, keys: Sequence[str] = KEYS) -> bool:
    """Whether two rows join: every key present, non-null and equal.

    A null key matches nothing, as in an inner join, on the sync and the async path
    (the async path matched null keys until ITL-1050 was fixed).
    """
    return all(
        left.values[k] is not None
        and right.values[k] is not None
        and left.values[k] == right.values[k]
        for k in keys
    )


def pairs(oracle: Oracle, left: str, right: str) -> list[RowIdentity]:
    """One row per pair of rows of ``left`` and ``right`` whose keys match (``Join``)."""
    return [
        (member(a, b),)
        for a in of_source(oracle, left)
        for b in of_source(oracle, right)
        if keys_match(a, b)
    ]


def semi(oracle: Oracle, left: str, right: str) -> list[RowIdentity]:
    """The rows of ``left`` with at least one matching row of ``right`` (``SemiJoin``)."""
    others = of_source(oracle, right)
    return rows(oracle, left, lambda a: any(keys_match(a, b) for b in others))


def groups(oracle: Oracle, source_id: str, by: Sequence[str]) -> list[RowIdentity]:
    """One row per distinct value of ``by`` (null included), holding its rows (``GroupBy``)."""
    grouped: dict[tuple[Any, ...], list[Origin]] = {}
    for o in of_source(oracle, source_id):
        grouped.setdefault(tuple(o.values[k] for k in by), []).append(o)
    return [tuple(sorted(member(o) for o in members)) for members in grouped.values()]


def no_nulls(*columns: str) -> Callable[[Origin], bool]:
    """Keep a row whose ``columns`` are all non-null (``DropNulls``)."""

    def keep(o: Origin) -> bool:
        """Whether every named column of ``o`` is non-null."""
        return all(o.values[c] is not None for c in columns)

    return keep
