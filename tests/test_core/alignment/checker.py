"""Check that every per-row column of a produced table belongs to its own row (ITL-1034).

``assert_rows_aligned`` walks every row and recomputes each derived column from that row
alone, never by comparing two runs (two wrong runs can agree):

* data values: each value must be the value its origin row has (``Oracle``), and every
  column of one row must come from the same origin row of each source;
* ``_source_<c>``: the token names the origin row whose value the row holds (element by
  element for list columns);
* system tags: a record id is the documented UUID v5 of the origin row the data came from,
  and the source id its source's id; a folded (aggregated) pair is the documented fold of
  the row's own members; the pair's column descends from its source's own column (its
  first hash is that source's schema hash), so a pair moved to another input's column
  fails. Limit: two sources of one schema share that hash (a commutative join then orders
  their pairs per row), so a pair moved between their columns cannot be told apart;
* user tags: the origin row's tag values (element by element for the list-valued tags
  of ``Batch``; ``GroupBy`` keeps only its scalar keys);
* ``_content_hash``: the data context's Arrow hash of the row's data columns;
* ``_context_key``: the stream's data context key.

A function's output token embeds the output datagram's uuid, made at run time: it cannot be
recomputed from the row, so it is compared, keyed on the row's record id, with the stored
record's (``function_tokens``). A column no rule covers fails the check, so a new kind of
per-row column cannot slip by unchecked.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from orcapod.system_constants import constants

from .oracle import Oracle, Origin, fold_values

MAX_REPORTED = 12

# A row's origins: one tuple per member (one member unless aggregated), each the sorted
# (source id, provenance token) pairs of the origin rows the member came from.
RowIdentity = tuple[tuple[tuple[str, str], ...], ...]
DEFAULT_CONTENT_HASH_COLUMN = "_content_hash"


@dataclass(frozen=True)
class Layout:
    """How the columns of a produced table relate to its origin rows.

    Attributes:
        tag_keys: The user tag columns.
        data_keys: The data columns.
        members: Columns holding one element per member row (``Batch``, ``GroupBy``): the
            row's members are the elements, the same members in every such column.
        merged: Columns holding one element per input (``MergeJoin``), each element from
            another input of the same row.
        content_hash_column: The content hash column's name when it is not the default.
        null_tokens: Whether a source-info cell may be null beside a value. Only for a
            plain ``DerivedSource`` (``node.as_source()``), whose source info is null in
            every row by the current design (ITL-1059); every other view fails on it.
        no_system_tags: Whether the view may have no system tags although they are
            asked for: the same plain ``DerivedSource`` (ITL-1059), and nothing else.
        fold_label: The label of the fold that produced the folded system tags, for a
            view with members: ``BATCH_FOLD_LABEL`` (``Batch``) or
            ``GROUP_BY_FOLD_LABEL`` (``GroupBy``).
    """

    tag_keys: tuple[str, ...]
    data_keys: tuple[str, ...]
    members: frozenset[str] = frozenset()
    merged: frozenset[str] = frozenset()
    content_hash_column: str = DEFAULT_CONTENT_HASH_COLUMN
    null_tokens: bool = False
    no_system_tags: bool = False
    fold_label: str | None = None


@dataclass
class _Row:
    """The origin rows one produced row was found to come from, per member."""

    members: list[dict[str, Origin]] = field(default_factory=lambda: [{}])


def split_token(token: str) -> tuple[str, str, str]:
    """Split a source-info token into source id (or function uri), row token and column."""
    prefix, column = token.rsplit(constants.BLOCK_SEPARATOR, 1)
    source, row_token = prefix.rsplit(constants.BLOCK_SEPARATOR, 1)
    return source, row_token, column


class _RowChecker:
    """Check one row; collect every failure."""

    def __init__(
        self,
        index: int,
        row: dict[str, Any],
        layout: Layout,
        oracle: Oracle,
        function_tokens: Mapping[tuple[str, str], str] | None,
    ) -> None:
        """Prepare to check row ``index`` (its values ``row``) against ``oracle``."""
        self.index = index
        self.row = row
        self.layout = layout
        self.oracle = oracle
        self.function_tokens = function_tokens
        self.failures: list[str] = []
        self.state = _Row()
        self.checked: set[str] = set()
        # The sources whose system tag pair was valid.
        self.paired: set[str] = set()
        # The origin each resolved element of a merged column came from.
        self.merged_origins: dict[str, list[Origin]] = {}
        # (member, column, value, token) of every data element no single origin
        # explained.
        self.unresolved: list[tuple[int, str, Any, str | None]] = []

    def fail(self, column: str, message: str) -> None:
        """Record a failure of ``column`` in this row."""
        self.failures.append(f"row {self.index}, column {column!r}: {message}")

    # -- origins -----------------------------------------------------------

    def _elements(self, column: str) -> list[Any]:
        """The elements of ``column``: the list for a member or merged column."""
        value = self.row[column]
        if column in self.layout.members or column in self.layout.merged:
            return list(value) if value is not None else []
        return [value]

    def _member_count(self) -> int:
        """How many members the row has: the length of its member columns."""
        lengths = {len(self._elements(c)) for c in self.layout.members}
        if len(lengths) > 1:
            self.fail("members", f"member columns have different lengths {lengths}")
        return lengths.pop() if lengths else 1

    def _assign(self, member: int, column: str, origin: Origin) -> None:
        """Record that ``column`` of ``member`` comes from ``origin``; it must agree."""
        known = self.state.members[member].get(origin.source_id)
        if known is None:
            self.state.members[member][origin.source_id] = origin
        elif known is not origin:
            self.fail(
                column,
                f"comes from row {origin.token!r} of {origin.source_id!r}, but another "
                f"column of the same row comes from row {known.token!r}",
            )

    def _resolve(self, column: str, value: Any, token: str | None) -> Origin | None:
        """The origin of one element of a data column, from its token and its value."""
        from_token: Origin | None = None
        token_name = f"{constants.SOURCE_PREFIX}{column}"
        if token is not None:
            self.checked.add(token_name)
            source, row_token, token_column = split_token(token)
            if column in self.oracle.derivations:
                # A function output's token names the column itself; an operator's
                # derived token (``Pick``, ``Index``) is checked whole, by its recipe.
                if self.oracle.derivations[column].token is None and (
                    token_column != column
                ):
                    self.fail(
                        token_name, f"token {token!r} names column {token_column!r}"
                    )
            else:
                if token_column != self.oracle.base_name(column):
                    self.fail(
                        token_name, f"token {token!r} names column {token_column!r}"
                    )
                from_token = self.oracle.by_token.get((source, row_token))
                if from_token is None:
                    self.fail(token_name, f"token {token!r} names no source row")
        if from_token is not None:
            expected = self.oracle.expected_value(column, from_token)
            if expected != value:
                self.fail(
                    token_name,
                    f"value {value!r} but its token {token!r} names a row whose value "
                    f"is {expected!r}",
                )
            return from_token
        # A null is looked up like any value: a null no origin row has there is wrong,
        # though it leaves the row without an origin to compare the rest with.
        candidates = self.oracle.origins_for_value(column, value)
        if not candidates:
            self.fail(column, f"value {value!r} is the value of no origin row")
            return None
        return candidates[0] if len(candidates) == 1 else None

    def _tokens(self, column: str, count: int) -> list[str | None]:
        """The source-info tokens of ``column``, one per element, or ``None`` each."""
        name = f"{constants.SOURCE_PREFIX}{column}"
        if name not in self.row:
            return [None] * count
        value = self.row[name]
        if column in self.layout.members or column in self.layout.merged:
            tokens = list(value) if value is not None else []
            if len(tokens) != count:
                self.fail(name, f"{len(tokens)} tokens for {count} values")
                return [None] * count
            return tokens
        return [value]

    def _check_present_token(self, column: str, value: Any, token: str | None) -> None:
        """A source-info cell that is present must not be null beside a value."""
        name = f"{constants.SOURCE_PREFIX}{column}"
        if name not in self.row or token is not None:
            return
        if self.layout.null_tokens:
            # ITL-1059: a plain DerivedSource has null source info by design (for now).
            self.checked.add(name)
        elif value is not None:
            self.fail(name, f"null token beside the value {value!r}")

    def resolve_data(self) -> None:
        """Find each data element's origin; all columns of a member must agree."""
        count = self._member_count()
        self.state.members = [{} for _ in range(count)]
        for column in self.layout.data_keys:
            elements = self._elements(column)
            tokens = self._tokens(column, len(elements))
            for position, (value, token) in enumerate(
                zip(elements, tokens, strict=True)
            ):
                self._check_present_token(column, value, token)
                origin = self._resolve(column, value, token)
                member = position if column in self.layout.members else 0
                if origin is None:
                    if column not in self.layout.merged:
                        self.unresolved.append((member, column, value, token))
                    continue
                if column in self.layout.merged:
                    self._assign_merged(column, origin)
                else:
                    self._assign(member, column, origin)
                    self.checked.add(column)
                if token is not None and column in self.oracle.derivations:
                    self._check_function_token(column, origin, token)

    def _member_tags(self, member: int) -> dict[str, Any]:
        """The user tag values of ``member``: its element of a list-valued tag."""
        tags: dict[str, Any] = {}
        for column in self.layout.tag_keys:
            if column not in self.row:
                continue
            if column in self.layout.members:
                elements = self._elements(column)
                if member < len(elements):
                    tags[column] = elements[member]
            else:
                tags[column] = self.row[column]
        return tags

    def resolve_remaining(self) -> None:
        """Explain, by everything the row shows, the elements no single value explained.

        A value several origin rows share (a null of a nullable column) names no origin;
        with no token and no system tag, no other column may name one either, and the
        member's values and tags would then go unchecked. So, source by source, the
        member's unexplained elements that source gives must be the values of one of its
        rows whose tags are the member's tags: one such row is the member's origin of
        that source (and a derived column's token is then checked against it); none
        leaves those elements unexplained, which fails the row; several leave it
        ambiguous, its values and tags those of every candidate.
        """
        for member in sorted({m for m, _, _, _ in self.unresolved}):
            known = self.state.members[member]
            for _, column, _, token in (e for e in self.unresolved if e[0] == member):
                # An element whose column a known origin holds is that origin's (its
                # value is checked by ``check_values``); a derived token is checked
                # here. Several known origins holding it name no single row.
                holders = [o for o in known.values() if self.oracle.holds(column, o)]
                if (
                    len(holders) == 1
                    and token is not None
                    and column in self.oracle.derivations
                ):
                    self._check_function_token(column, holders[0], token)
            entries = [
                (column, value, token)
                for m, column, value, token in self.unresolved
                if m == member
                and not any(self.oracle.holds(column, o) for o in known.values())
            ]
            if not entries:
                continue
            tags = self._member_tags(member)
            explained: set[str] = set()
            for source in sorted({o.source_id for o in self.oracle.origins}):
                if source in known:
                    continue
                rows = [o for o in self.oracle.origins if o.source_id == source]
                given = [e for e in entries if self.oracle.holds(e[0], rows[0])]
                if not given:
                    continue
                candidates = [
                    o
                    for o in rows
                    if all(
                        self.oracle.expected_value(column, o) == value
                        for column, value, _ in given
                    )
                    and all(
                        o.values[self.oracle.base_name(k)] == v
                        for k, v in tags.items()
                        if self.oracle.base_name(k) in o.values
                    )
                ]
                if not candidates:
                    continue
                explained.update(column for column, _, _ in given)
                # Every candidate has these values and tags: the comparison is done
                # even when no single origin can be named.
                self.checked.update(column for column, _, _ in given)
                self.checked.update(
                    k for k in tags if self.oracle.base_name(k) in rows[0].values
                )
                if len(candidates) == 1:
                    (origin,) = candidates
                    self._assign(member, given[0][0], origin)
                    self.checked.update(column for column, _, _ in given)
                for column, _, token in given:
                    if token is not None and column in self.oracle.derivations:
                        if len(candidates) == 1:
                            self._check_function_token(column, candidates[0], token)
                        else:
                            self._check_function_token_of_any(column, candidates, token)
            unexplained = {c: v for c, v, _ in entries if c not in explained}
            if unexplained:
                self.fail(
                    next(iter(unexplained)),
                    f"no origin row has the values {unexplained!r} with the tags "
                    f"{tags!r}",
                )

    def _assign_merged(self, column: str, origin: Origin) -> None:
        """Record the origin of one element of a merged column (one element per input)."""
        self._assign(0, column, origin)
        self.merged_origins.setdefault(column, []).append(origin)
        self.checked.add(column)

    def _token_error(self, column: str, origin: Origin, token: str) -> str | None:
        """Why ``token`` is not the derived column's token of ``origin``; ``None`` if it is.

        The token is recomputed, or the one stored for this row's record.
        """
        recompute = self.oracle.derivations[column].token
        if recompute is not None:
            expected = recompute(origin)
            if token != expected:
                return f"token {token!r}, its row's token is {expected!r}"
            return None
        if self.function_tokens is None:
            _, row_token, _ = split_token(token)
            if len(row_token) != 32:
                return f"token {token!r} does not name an output datagram"
            return None
        expected = self.function_tokens.get((column, origin.record_id))
        if expected != token:
            return (
                f"token {token!r}, but the record of row {origin.token!r} stores "
                f"{expected!r}"
            )
        return None

    def _check_function_token(self, column: str, origin: Origin, token: str) -> None:
        """A derived column's token must be the token of ``origin``."""
        error = self._token_error(column, origin, token)
        if error is not None:
            self.fail(f"{constants.SOURCE_PREFIX}{column}", error)

    def _check_function_token_of_any(
        self, column: str, origins: Sequence[Origin], token: str
    ) -> None:
        """A derived column's token must be the token of at least one of ``origins``."""
        errors = [self._token_error(column, o, token) for o in origins]
        if all(e is not None for e in errors):
            self.fail(
                f"{constants.SOURCE_PREFIX}{column}",
                f"no candidate origin has this token: {errors[0]}",
            )

    # -- system tags ---------------------------------------------------------

    def check_system_tags(self) -> None:
        """Every system tag pair belongs to this row's origin rows (or folds its members)."""
        source_prefix = constants.SYSTEM_TAG_SOURCE_ID_PREFIX
        record_prefix = constants.SYSTEM_TAG_RECORD_ID_PREFIX
        folded = bool(self.layout.members)
        paired = self.paired
        for name in self.row:
            if not name.startswith(source_prefix):
                continue
            suffix = name[len(source_prefix) :]
            record_name = f"{record_prefix}{suffix}"
            if record_name not in self.row:
                self.fail(name, f"no matching record id column {record_name!r}")
                continue
            self.checked.update((name, record_name))
            source_value = self.row[name]
            record_value = self.row[record_name]
            # ``::<schema hash>`` first, then one ``::<pipeline hash>[:<position>]`` per
            # join or fold: the first segment names the schema of the pair's source.
            lineage = suffix.removeprefix(constants.BLOCK_SEPARATOR).split(
                constants.BLOCK_SEPARATOR
            )[0]
            if folded:
                source = self._check_folded(
                    name, record_name, source_value, record_value, lineage
                )
            else:
                source = self._check_scalar(
                    record_name, source_value, record_value, lineage
                )
            if source is not None:
                paired.add(source)

    def check_pairs_complete(self) -> None:
        """A row with system tags has a pair for every source its data came from.

        Run after every origin is resolved: a pair copied over another input's pair
        (two inputs of one schema) leaves one out, also when only the tags name the
        source.
        """
        paired = self.paired
        data_sources = {s for member in self.state.members for s in member}
        if paired and (missing := sorted(data_sources - paired)):
            self.fail(
                "system tags",
                f"no system tag pair for the sources {missing} the row's data came from",
            )

    def _check_lineage(self, name: str, source: str, lineage: str) -> bool:
        """The column holding a pair of ``source`` must descend from that source's column.

        A pair moved, whole, into the column of another input passes every value check;
        only the column name, whose first hash is the schema hash of the source whose
        rows it holds, tells it apart. Two sources of one schema share that hash, so a
        pair moved between their columns is not caught (by design: a commutative join
        orders such pairs per row).
        """
        expected = self.oracle.schema_hashes.get(source)
        if expected != lineage:
            self.fail(
                name,
                f"holds a pair of {source!r}, whose system tags are named by schema "
                f"hash {expected!r}, but the column descends from {lineage!r}",
            )
            return False
        return True

    def _check_scalar(
        self, name: str, source_value: Any, record_value: Any, lineage: str
    ) -> str | None:
        """A scalar record id is the record id of this row's origin row of its source.

        Returns:
            The pair's source when the pair is valid, else ``None``.
        """
        origin = self.oracle.by_record_id.get((source_value, record_value))
        if origin is None:
            self.fail(
                name, f"record id {record_value!r} is no row of source {source_value!r}"
            )
            return None
        if origin.source_id != source_value:
            self.fail(
                name,
                f"record id of a row of {origin.source_id!r} beside source id "
                f"{source_value!r}",
            )
            return None
        if not self._check_lineage(name, origin.source_id, lineage):
            return None
        self._assign(0, name, origin)
        return origin.source_id

    def _check_folded(
        self,
        name: str,
        record_name: str,
        source_value: Any,
        record_value: Any,
        lineage: str,
    ) -> str | None:
        """A folded pair is the documented fold of this row's members of one source.

        Args:
            name: The folded source-id column.
            record_name: The folded record-id column beside it.
            source_value: The row's folded source id.
            record_value: The row's folded record id.
            lineage: The first hash of the columns' names.

        Returns:
            The pair's source when the pair is valid, else ``None``.
        """
        label = self.layout.fold_label
        if label is None:
            self.fail(name, "the layout names no fold label for a folded system tag")
            return None
        sources = {s for member in self.state.members for s in member}
        for source in sorted(sources):
            origins = [member.get(source) for member in self.state.members]
            if fold_values([source] * len(origins), label) != source_value:
                continue
            if not self._check_lineage(name, source, lineage):
                return None
            if any(o is None for o in origins):
                self.fail(name, f"members of {source!r} not all known; cannot fold")
                return None
            expected = fold_values(
                [o.record_id for o in origins if o is not None], label
            )
            if expected != record_value:
                self.fail(
                    record_name,
                    "folded record id is not the fold of the row's own members "
                    f"{[o.token for o in origins if o is not None]!r}",
                )
                return None
            return source
        self.fail(name, f"folded source id {source_value!r} folds no member list")
        return None

    # -- values --------------------------------------------------------------

    def identity(self) -> RowIdentity:
        """The origin rows this row was found to come from: per member, sorted pairs.

        An aggregated row's members are sorted too: their order is the operator's choice.
        """
        members = [
            tuple(sorted((o.source_id, o.token) for o in member.values()))
            for member in self.state.members
        ]
        return tuple(sorted(members)) if self.layout.members else tuple(members)

    def check_values(self) -> None:
        """Every tag and data value is its origin row's value."""
        for column in (*self.layout.tag_keys, *self.layout.data_keys):
            if column in self.layout.merged:
                self._check_merged_values(column)
                continue
            elements = self._elements(column)
            if column in self.layout.members:
                if len(elements) != len(self.state.members):
                    self.fail(column, "length differs from the row's member count")
                    continue
                pairs = list(zip(elements, self.state.members, strict=True))
            else:
                pairs = [(elements[0], m) for m in self.state.members]
            for value, member in pairs:
                for origin in member.values():
                    if not self.oracle.holds(column, origin):
                        continue
                    self.checked.add(column)
                    expected = self.oracle.expected_value(column, origin)
                    if expected != value:
                        self.fail(
                            column,
                            f"value {value!r}, but its row {origin.token!r} of "
                            f"{origin.source_id!r} has {expected!r}",
                        )

    def _check_merged_values(self, column: str) -> None:
        """A merged column holds exactly the values of the row's inputs that have it."""
        expected = [
            self.oracle.expected_value(column, o)
            for o in self.state.members[0].values()
            if self.oracle.holds(column, o)
        ]
        actual = self._elements(column)
        if sorted(map(repr, expected)) != sorted(map(repr, actual)):
            self.fail(column, f"holds {actual!r}, its inputs have {expected!r}")
        resolved = self.merged_origins.get(column, [])
        if len(resolved) == len(actual):
            # One element per contributing input: two elements resolving to one
            # input leave the other's out, though the values may look right.
            got = Counter(o.source_id for o in resolved)
            want = Counter(
                o.source_id
                for o in self.state.members[0].values()
                if self.oracle.holds(column, o)
            )
            if got != want:
                self.fail(
                    column,
                    f"its elements come from the inputs {dict(got)}, not "
                    f"one from each of {dict(want)}",
                )
        self.checked.add(column)


def assert_rows_aligned(
    table: pa.Table,
    layout: Layout,
    oracle: Oracle,
    data_context: Any,
    *,
    function_tokens: Mapping[tuple[str, str], str] | None = None,
    require_rows: bool = True,
    expected_rows: Collection[RowIdentity] | None = None,
) -> set[str]:
    """Assert that every per-row column of every row of ``table`` belongs to that row.

    Args:
        table: The produced table.
        layout: How its columns relate to the origin rows.
        oracle: The origin rows, from the tables the test wrote.
        data_context: The stream's data context: its key and Arrow hasher.
        function_tokens: The stored source-info token of each function output, keyed by
            ``(column, origin record id)``; without it a function token's form is checked.
        require_rows: Fail on a table with no rows (a check of nothing).
        expected_rows: The origin rows the table must hold, as a multiset of row
            identities derived from the fixtures by the operator's documented rule
            (``expectations.py``), never from the library's output. A row that is
            missing, extra or repeated fails, even when each row is aligned.

    Returns:
        The names of the columns at least one rule compared.

    Raises:
        AssertionError: Naming every misaligned row and column (the first few).
    """
    if require_rows:
        assert table.num_rows > 0, (
            "the table has no rows: the check would check nothing"
        )
    hasher = data_context.arrow_hasher
    context_key = data_context.context_key
    hash_column = layout.content_hash_column
    known = set(layout.tag_keys) | set(layout.data_keys)
    unknown = [
        c
        for c in table.column_names
        if c not in known
        and c != hash_column
        and c != constants.CONTEXT_KEY
        # Only source-id / record-id pairs: any other system tag has no rule.
        and not c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
        and not c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
        and not (
            c.startswith(constants.SOURCE_PREFIX)
            and c[len(constants.SOURCE_PREFIX) :] in layout.data_keys
        )
    ]
    assert not unknown, f"no alignment rule covers the columns {unknown!r}"

    failures: list[str] = []
    checked: set[str] = set()
    identities: list[RowIdentity] = []
    rows = table.to_pylist()
    for index, row in enumerate(rows):
        checker = _RowChecker(index, row, layout, oracle, function_tokens)
        checker.resolve_data()
        if layout.members:
            # A folded pair is checked against the members' origins, which a shared
            # null member has only once its tags resolve it. A scalar pair assigns its
            # origin itself, so it goes first.
            checker.resolve_remaining()
            checker.check_system_tags()
        else:
            checker.check_system_tags()
            checker.resolve_remaining()
        checker.check_pairs_complete()
        checker.check_values()
        if hash_column in row:
            data_row = table.slice(index, 1).select(list(layout.data_keys))
            expected = hasher.hash_table(data_row).to_string()
            if row[hash_column] != expected:
                checker.fail(
                    hash_column,
                    f"{row[hash_column]!r} is not the hash of the row's data {expected!r}",
                )
            checker.checked.add(hash_column)
        if constants.CONTEXT_KEY in row:
            if row[constants.CONTEXT_KEY] != context_key:
                checker.fail(constants.CONTEXT_KEY, f"{row[constants.CONTEXT_KEY]!r}")
            checker.checked.add(constants.CONTEXT_KEY)
        failures.extend(checker.failures)
        checked |= checker.checked
        identities.append(checker.identity())
    if expected_rows is not None and not failures:
        got, want = Counter(identities), Counter(expected_rows)
        if got != want:
            missing = list((want - got).elements())[:MAX_REPORTED]
            extra = list((got - want).elements())[:MAX_REPORTED]
            failures.append(
                f"the rows are not the expected rows: missing {missing}, "
                f"extra or repeated {extra}"
            )
    if failures:
        shown = "\n  ".join(failures[:MAX_REPORTED])
        raise AssertionError(
            f"{len(failures)} misaligned cells in {table.num_rows} rows:\n  {shown}"
        )
    # Every column must have been compared in some row: a column no rule reached is a
    # column the check said nothing about.
    if table.num_rows:
        assert_all_columns_checked(table, checked)
    return checked


def assert_requested_columns(
    table: pa.Table,
    config: Mapping[str, Any],
    layout: Layout,
    *,
    require_sorted: bool = True,
) -> None:
    """Assert ``table`` has the per-row columns ``config`` asks for.

    A table that silently left out a requested column would be checked on fewer
    columns than asked: the content hash column and the context key are required
    whenever the configuration (or ``all_info``) asks for them.

    A table asked to be sorted (``sort_by_tags`` or ``all_info``) must be in tag order.
    Source info (one ``_source_<column>`` per data column) and system tags (at least one
    source-id / record-id pair) are required too when asked for; a plain
    ``DerivedSource`` has no system tags by design (``layout.no_system_tags``, ITL-1059).

    Args:
        table: The produced table.
        config: The keyword arguments the table was asked with (``columns``,
            ``all_info``).
        layout: The table's layout: its data columns and content hash column name.
        require_sorted: Whether a sorted order is required when asked for; ``False``
            for ``head``, which shows rows in stream order.
    """
    columns = config.get("columns", {})
    # all_info given at the top or inside the column config (ITL-1067, #363).
    all_info = bool(config.get("all_info")) or bool(columns.get("all_info"))
    names = table.column_names
    if all_info or columns.get("content_hash"):
        assert layout.content_hash_column in names, (
            f"no {layout.content_hash_column!r} column"
        )
    if all_info or columns.get("context"):
        assert constants.CONTEXT_KEY in names, "no context key column"
    if all_info or columns.get("source"):
        missing = [
            f"{constants.SOURCE_PREFIX}{c}"
            for c in layout.data_keys
            if f"{constants.SOURCE_PREFIX}{c}" not in names
        ]
        assert not missing, f"no source-info columns {missing}"
    if require_sorted and (all_info or columns.get("sort_by_tags")):
        # List-valued tags (Batch) too: they are sorted with Polars, nulls last.
        assert_sorted_by_tags(table, layout.tag_keys)
    if (all_info or columns.get("system_tags")) and not layout.no_system_tags:
        pairs = [
            c
            for c in names
            if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
            and f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}"
            f"{c[len(constants.SYSTEM_TAG_SOURCE_ID_PREFIX) :]}"
            in names
        ]
        assert pairs, "no system tag (source id / record id) pair"


def assert_all_columns_checked(table: pa.Table, checked: Collection[str]) -> None:
    """Assert that the checker compared every column of ``table`` at least once."""
    missed = [c for c in table.column_names if c not in checked]
    assert not missed, f"the check compared nothing in the columns {missed!r}"


def rows_by_identity(
    table: pa.Table, ignore: Collection[str] = ()
) -> dict[tuple[Any, ...], dict[str, Any]]:
    """The rows of ``table`` keyed by their record ids (every system tag record id column).

    Raises:
        AssertionError: If the table has no record id column or two rows share a key.
    """
    keys = sorted(
        c
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
    )
    assert keys, "the table has no record id column to key rows by"
    keyed: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in table.to_pylist():
        key = tuple(row[k] for k in keys)
        assert key not in keyed, f"two rows share the record ids {key!r}"
        keyed[key] = {c: v for c, v in row.items() if c not in ignore}
    return keyed


def assert_same_rows_by_identity(
    left: pa.Table, right: pa.Table, ignore: Collection[str] = ()
) -> None:
    """Assert two tables hold the same rows, matched by record ids, never by position.

    Args:
        left: One table.
        right: The other; same columns in the same order.
        ignore: Columns that differ between runs by design (a per-run uuid, a time).
    """
    assert left.column_names == right.column_names, (
        f"columns differ: {left.column_names} vs {right.column_names}"
    )
    # Arrow types and nullability too: the async rows must equal the sync rows in both.
    for name in left.column_names:
        if name in ignore:
            continue
        a, b = left.schema.field(name), right.schema.field(name)
        assert (a.type, a.nullable) == (b.type, b.nullable), (
            f"column {name!r}: {a.type} (nullable={a.nullable}) vs "
            f"{b.type} (nullable={b.nullable})"
        )
    left_rows = rows_by_identity(left, ignore)
    right_rows = rows_by_identity(right, ignore)
    assert left_rows.keys() == right_rows.keys(), "the tables hold different records"
    for key, row in left_rows.items():
        assert row == right_rows[key], f"record {key!r}: {row} != {right_rows[key]}"


def rows_table(pairs: Iterable[tuple[Any, Any]]) -> pa.Table:
    """The ``(tag, data)`` rows an iteration yielded, as one table with every group.

    Each row's own Arrow tables (tag with system tags; data with source info and context)
    side by side, so the rows ``iter_data`` yields are checked like a produced table.
    """
    from orcapod.utils import arrow_utils

    tables = [
        arrow_utils.hstack_tables(
            tag.as_table(columns={"system_tags": True}),
            data.as_table(columns={"source": True, "context": True}),
        )
        for tag, data in pairs
    ]
    return pa.concat_tables(tables, promote_options="permissive")


def _null_last(value: Any) -> tuple[Any, ...]:
    """A sort key: ascending, a null cell after every value.

    A list cell (``Batch``'s tags, sorted with Polars) is ordered element by element, a
    shorter prefix first; inside a list a null element comes before every value, as
    Polars orders it (only the null cell itself goes last).
    """
    if value is None:
        return (1,)
    return (0, _in_list(value) if isinstance(value, list) else value)


def _in_list(value: list[Any]) -> tuple[Any, ...]:
    """The Polars order of a list's elements: a null element before every value."""
    return tuple(
        (0,) if v is None else (1, _in_list(v) if isinstance(v, list) else v)
        for v in value
    )


def assert_sorted_by_tags(table: pa.Table, tag_keys: Sequence[str]) -> None:
    """Assert the rows of ``table`` are in the presentation order ``sort_by_tags`` asks.

    The order of ``as_table(sort_by_tags=True)`` and ``head`` (#354,
    ``arrow_utils.sort_by_tag_columns``): ascending by ``tag_keys``, a null cell last (a
    list-valued tag is ordered with Polars: element by element, a null element first),
    then by the system tag columns the table has, in column order. Rows equal on all of these keep their
    input order, which the table does not show, so they may come in any order.
    """
    system_tags = [
        c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    keys = list(dict.fromkeys([*tag_keys, *system_tags]))
    if not keys or table.num_rows < 2:
        return
    rows = list(zip(*(table.column(k).to_pylist() for k in keys), strict=True))
    order = [tuple(_null_last(v) for v in row) for row in rows]
    out_of_order = [i for i in range(1, len(order)) if order[i - 1] > order[i]]
    assert not out_of_order, (
        f"rows not in tag order (by {keys}, nulls last): row {out_of_order[0]} "
        f"{rows[out_of_order[0]]} comes after {rows[out_of_order[0] - 1]}"
    )


def assert_members_cover(
    table: pa.Table, column: str, expected: Collection[Any]
) -> None:
    """Assert the flattened member values of ``column`` are ``expected``, each once.

    For a many-to-one operator whose groups depend on arrival order (``Batch``): each row
    is aligned by itself, so a table that repeated one whole group and left out another
    would pass every per-row check; its members must still be the input's rows.
    """
    flattened = [x for xs in table.column(column).to_pylist() for x in xs or []]
    assert sorted(flattened) == sorted(expected), (
        f"the members of {column!r} {sorted(flattened)} are not the input rows "
        f"{sorted(expected)}"
    )
