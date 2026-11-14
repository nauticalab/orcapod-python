"""A view of a stream's rows and its notebook preview (ITL-991, ITL-1081).

``head(n)`` on a stream, source or node returns a ``StreamView`` of at most ``n`` rows the
object already holds, has cached from an earlier computation, has stored, or can read from
its table with a bounded read. It never computes: no user function is called, no operator
is run, no side effect is delivered, no source is polled and nothing is written. The
notebook preview of a stream is its ``head()`` rendered by ``StreamView._repr_html_``.

``run()`` on a stream computes it (the work ``as_table()`` does) and returns a
``StreamView`` of all the computed rows (developer, November 13, 2026, ITL-1081). The
view's own methods (``head``, ``tail``, ``sort_by_tags``) choose what is shown; each
returns a new view and never touches the stream.
"""

from __future__ import annotations

import ast
import dataclasses
import html
import importlib
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any, cast

from typing_extensions import override

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")


class ViewStatus(str, Enum):
    """Where the rows of a ``StreamView`` came from, or why it has none.

    Attributes:
        ROWS: The rows are the first rows of the stream's complete content. Zero rows
            means the content is empty.
        PARTIAL: A computation stopped part way; the rows are those computed so far.
        NOT_COMPUTED: Nothing is computed, or what was computed is out of date.
        NOTHING_STORED: A job node whose store holds no rows for it yet.
        UNAVAILABLE: No rows can exist here: a pipeline blueprint, an input slot with no
            source bound, or a source that is not available.
    """

    ROWS = "rows"
    PARTIAL = "partial"
    NOT_COMPUTED = "not_computed"
    NOTHING_STORED = "nothing_stored"
    UNAVAILABLE = "unavailable"


NOT_COMPUTED_NOTE = "not computed; run() computes it"
STALE_NOTE = (
    "not computed: inputs changed since the last computation; run() recomputes it"
)
NOTHING_STORED_NOTE = "nothing stored; run the job"
BLUEPRINT_NOTE = "pipeline blueprint; run it in a PipelineJob"
SIDE_EFFECTS_NOTE = (
    "not run; run() delivers its side effects; displaying never delivers side effects"
)

_DEFAULT_NOTES = {
    ViewStatus.NOT_COMPUTED: NOT_COMPUTED_NOTE,
    ViewStatus.NOTHING_STORED: NOTHING_STORED_NOTE,
    ViewStatus.UNAVAILABLE: "no rows available",
}

SCHEMA_NEEDS_POLL_NOTE = "columns not known until its source is polled"


def default_note(status: ViewStatus) -> str:
    """Return the note shown for a head with ``status`` and no rows of its own."""
    return _DEFAULT_NOTES[status]


# Words naming the rows of a ``ROWS`` head, by where they came from.
_ORIGIN_NOUNS = {
    "held": "rows",
    "read": "rows",
    "computed": "computed rows",
    "stored": "stored rows",
    "delivered": "delivered rows",
}

# Display limits of the preview.
# A view nobody cut to n rows (the view ``run()`` returns, or a sort of it) shows at most
# this many rows: the first and the last half, with Polars' ellipsis row between.
_DISPLAY_ROWS = 10
_MAX_TEXT = 80
_TEXT_START = 56
_TEXT_END = 20
_MAX_LIST_ITEMS = 5


@dataclass(frozen=True)
class ColumnRoleMark:
    """How the display marks the header of a column with one role.

    Only the display is marked: never ``StreamView.table``, ``as_table()``,
    ``as_polars_df()`` or a schema.

    Attributes:
        text_prefix: Put before the column's name in the text display.
        html_style: Inline CSS of the column's header cell in the HTML display.
        text_legend: The legend line of the text display naming this mark, or empty.
        html_legend: The legend line of the HTML display naming this mark, or empty.
    """

    text_prefix: str = ""
    html_style: str = ""
    text_legend: str = ""
    html_legend: str = ""


# The one place that says how the display marks each column role (developer, November 13,
# 2026, ITL-1081). Roles today: "tag" and "data". A new role (e.g. a referential tag) gets
# its own entry here; the renderers need no change. The accent colour has a contrast of
# about 4.3:1 on white and on #111; bold carries the meaning without the colour.
COLUMN_ROLE_MARKS: dict[str, ColumnRoleMark] = {
    "tag": ColumnRoleMark(
        text_prefix="*",
        html_style="font-weight:bold;color:#2a7fb8",
        text_legend="* = tag",
        html_legend="bold = tag",
    ),
    "data": ColumnRoleMark(),
}

# The HTML display draws this rule after the last column of each role, on every row, so
# the table reads "tags | data". It takes the text colour, so it reads on any theme.
ROLE_BOUNDARY_STYLE = "border-right:2px solid currentColor"


@dataclass(frozen=True)
class StreamView:
    """Rows of a stream, and how they were obtained.

    ``head(n)`` on a stream returns a view of at most ``n`` readily available rows;
    ``run()`` returns a view of all the computed rows. The view's ``head``, ``tail`` and
    ``sort_by_tags`` return a new view of its own rows, with a header that says what they
    are; they never read, compute or change the stream. A view holds the columns it was
    made with: for other column groups (system tags, source info, context, content
    hashes) ask the stream, ``stream.as_table(columns=...)`` or
    ``stream.head(columns=...)``. After ``run()`` they read the rows a stream kept (a
    pod's or operator's output stream, a job node, an in-memory source); a stream that
    keeps none does its ``as_table()`` work again (a side-effect stream delivers again, a
    polling source polls again).

    Attributes:
        table: The rows, with the exact Arrow types the stream gives them. When the
            stream has no rows to show, a zero-row table with its columns.
        status: Where the rows came from, or why there are none.
        kind: Class name of the stream, source or node shown.
        label: The name shown for it: its label, or for a source given no label its
            source id (``display_name``). Never used as an identity.
        tag_columns: The columns of ``table`` that are tags.
        total_rows: The stream's row count when it is known without computing, else
            ``None``. For ``PARTIAL``, the number of rows computed so far.
        total_is_estimate: ``True`` when ``total_rows`` is a recorded estimate.
        origin: What the rows are: ``"held"`` (in memory), ``"read"`` (a bounded read
            of a table), ``"computed"`` (cached from an earlier computation),
            ``"stored"`` (a job node's store) or ``"delivered"`` (the rows a side-effect
            stream passed on while delivering).
        note: For a view without rows, the explanation shown instead of the default;
            for a view with rows, a remark shown after the row count.
        description: When given, the whole text after the name in the header, written
            by a stream that words its own rows (a table-backed source naming its
            ``where``); otherwise the header is built from the fields above.
        sorted_by_tags: ``True`` when the rows were asked for in tag order (the notebook
            preview, ``sort_by_tags``): the header then says so, and for a bounded read
            or a partial computation says they are the rows read or computed so far, in
            tag order. A ``description`` words its own sorting.
        start: The position of the first row of ``table`` among the rows it was cut
            from (``tail`` of a view moves it), counted from 0.
        pool: When set, the rows were cut from or sorted within a set of rows that is
            not the stream's whole sequence (a few rows read, a view already cut), and
            this is the header text naming that set; ``pool_rows`` is its size.
        pool_rows: The number of rows of ``pool``.
        display_all_rows: ``True`` when the display shows every row (a view cut to
            ``n`` rows by ``head`` or ``tail``); ``False`` for a view nobody cut (the
            view ``run()`` returns, or a sort of it), whose display shows the first and
            the last rows of a long view, as Polars does. The header gives the count.
    """

    table: pa.Table
    status: ViewStatus
    kind: str
    label: str
    tag_columns: tuple[str, ...] = ()
    total_rows: int | None = None
    total_is_estimate: bool = False
    origin: str = "held"
    note: str | None = None
    description: str | None = None
    sorted_by_tags: bool = False
    start: int = 0
    pool: str | None = None
    pool_rows: int = 0
    display_all_rows: bool = True

    @classmethod
    def of_rows(
        cls,
        stream: Any,
        table: pa.Table,
        *,
        tag_columns: tuple[str, ...],
        total_rows: int | None,
        status: ViewStatus = ViewStatus.ROWS,
        total_is_estimate: bool = False,
        origin: str = "held",
        note: str | None = None,
        description: str | None = None,
        display_all_rows: bool = True,
    ) -> StreamView:
        """Return a view with rows of ``stream``."""
        return cls(
            table=table,
            status=status,
            kind=type(stream).__name__,
            label=display_name(stream),
            tag_columns=tuple(c for c in tag_columns if c in table.column_names),
            total_rows=total_rows,
            total_is_estimate=total_is_estimate,
            origin=origin,
            note=note,
            description=description,
            display_all_rows=display_all_rows,
        )

    @classmethod
    def without_rows(
        cls,
        stream: Any,
        status: ViewStatus,
        table: pa.Table,
        *,
        tag_columns: tuple[str, ...],
        note: str | None = None,
    ) -> StreamView:
        """Return a head of ``stream`` with no rows: ``table`` holds its columns only."""
        return cls(
            table=table.slice(0, 0),
            status=status,
            kind=type(stream).__name__,
            label=display_name(stream),
            tag_columns=tuple(c for c in tag_columns if c in table.column_names),
            note=note,
        )

    def for_stream(
        self, stream: Any, *, origin: str | None = None, note: str | None = None
    ) -> StreamView:
        """Return the same rows shown as ``stream`` (a source over its inner stream, a node
        over its cached output), optionally with another origin or note."""
        return dataclasses.replace(
            self,
            kind=type(stream).__name__,
            label=display_name(stream),
            origin=self.origin if origin is None else origin,
            note=self.note if note is None else note,
        )

    # ------------------------------------------------------------------
    # Data access
    # ------------------------------------------------------------------

    def as_table(self) -> pa.Table:
        """Return the rows as a PyArrow table."""
        return self.table

    def as_polars_df(self) -> pl.DataFrame:
        """Return the rows as a Polars DataFrame."""
        return pl.DataFrame(self.table)

    def __len__(self) -> int:
        """Return the number of rows in ``table``."""
        return self.table.num_rows

    # ------------------------------------------------------------------
    # Choosing what is shown
    # ------------------------------------------------------------------

    def head(self, n: int = 10) -> StreamView:
        """Return a view of the first ``n`` rows of this view.

        Never reads or computes anything: the rows are this view's. The header says
        which rows they are ("first 10 of 1,234 computed rows").

        Args:
            n: The largest number of rows to keep.

        Raises:
            TypeError: If ``n`` is not an ``int``.
            ValueError: If ``n`` is negative.
        """
        _check_row_count(n)
        view = self._cuttable()
        return dataclasses.replace(
            view, table=view.table.slice(0, n), display_all_rows=True
        )

    def tail(self, n: int = 10) -> StreamView:
        """Return a view of the last ``n`` rows of this view.

        Never reads or computes anything: the rows are this view's. The header says
        which rows they are ("last 10 of 1,234 computed rows", "rows 11–20 of 1,234
        computed rows").

        Args:
            n: The largest number of rows to keep.

        Raises:
            TypeError: If ``n`` is not an ``int``.
            ValueError: If ``n`` is negative.
        """
        _check_row_count(n)
        view = self._cuttable()
        dropped = max(view.table.num_rows - n, 0)
        return dataclasses.replace(
            view,
            table=view.table.slice(dropped),
            start=view.start + dropped,
            display_all_rows=True,
        )

    def sort_by_tags(self) -> StreamView:
        """Return a view of this view's rows in tag order.

        The rows are ordered as ``as_table(columns={"sort_by_tags": True})`` orders them
        (``arrow_utils.sort_by_tag_columns``: ascending by the tag columns, nulls last).
        Rows with equal tags are then ordered by the system tag columns the view holds;
        the view ``run()`` returns holds none, so such rows keep their order in the view,
        where ``as_table`` with ``sort_by_tags`` orders them by their system tags. A view
        of all of a stream's rows sorts them all ("all 1,234 computed rows, sorted by
        tags"); a view of some of them sorts those, and its header says so ("first 10 of
        1,234 computed rows, then sorted by tags"). A view with no tag column or no rows
        is returned as it is. Never reads or computes anything.
        """
        if not self.tag_columns or self.table.num_rows == 0:
            return self
        if self._is_window() and self.pool is None and self.sorted_by_tags:
            return self
        # arrow_utils is read when a view is sorted, not imported: it sits above this
        # module in the import graph (types, protocols, side effects), as the array
        # logical type does for the display.
        arrow_utils = importlib.import_module("orcapod.utils.arrow_utils")
        table: pa.Table = arrow_utils.sort_by_tag_columns(self.table, self.tag_columns)
        if self._is_window() and self.pool is None and self._holds_every_row():
            return dataclasses.replace(self, table=table, sorted_by_tags=True)
        view = self._as_pool()
        return dataclasses.replace(
            view, table=table, pool=f"{view.pool}, then sorted by tags"
        )

    def _is_window(self) -> bool:
        """Whether the rows are a run of the stream's own row sequence, so that a cut
        can be named by position: a ``ROWS`` view with no ``description``, unsorted or
        sorted among all of the stream's rows."""
        return (
            self.status is ViewStatus.ROWS
            and self.description is None
            and (not self.sorted_by_tags or self._total_is_exact())
        )

    def _total_is_exact(self) -> bool:
        """Whether the stream's row count is known exactly."""
        return self.total_rows is not None and not self.total_is_estimate

    def _holds_every_row(self) -> bool:
        """Whether ``table`` holds every row of the stream."""
        return (
            self._total_is_exact()
            and self.start == 0
            and self.table.num_rows == self.total_rows
        )

    def _cuttable(self) -> StreamView:
        """Return this view in a form whose rows can be cut by position: itself when its
        rows are a run of the stream's rows (or of a pool), else a pool of its rows."""
        if self.status not in (ViewStatus.ROWS, ViewStatus.PARTIAL):
            return self
        if self.pool is not None or self._is_window():
            return self
        return self._as_pool()

    def _as_pool(self) -> StreamView:
        """Return this view as a pool: its rows, named by its current header text."""
        return dataclasses.replace(
            self,
            pool=self.description or self._describe(),
            pool_rows=self.table.num_rows,
            start=0,
            description=None,
            note=None,
            sorted_by_tags=False,
        )

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def header(self) -> str:
        """Return the one-line description shown above the rows."""
        return f"{self.kind}[{self.label}] — {self.description or self._describe()}"

    def _describe(self) -> str:
        """Return the header text after the name, for this status."""
        shown = self.table.num_rows
        if self.pool is not None:
            return _window_text(self.pool, shown, self.start, self.pool_rows)
        if self.status is ViewStatus.ROWS:
            text = self._describe_rows(shown)
        elif self.status is ViewStatus.PARTIAL:
            total = self.total_rows if self.total_rows is not None else shown
            suffix = ", sorted by tags" if self.sorted_by_tags else ""
            if shown == total:
                text = f"{total:,} {_rows(total)} computed so far{suffix}"
            else:
                text = f"first {shown:,} of {total:,} rows computed so far{suffix}"
            text += " (computation incomplete)"
        else:
            return self.note or _DEFAULT_NOTES[self.status]
        if self.note:
            text += f"; {self.note}"
        return text

    def _describe_rows(self, shown: int) -> str:
        """Return the header text for a ``ROWS`` head showing ``shown`` rows."""
        total = self.total_rows
        noun = _ORIGIN_NOUNS.get(self.origin, "rows")
        if shown == 0 and (total is None or total == 0):
            return "computed: no rows" if self.origin == "computed" else f"no {noun}"
        suffix = ", sorted by tags" if self.sorted_by_tags else ""
        if self.start and shown:
            # A cut past the first row (``tail``): name the rows by position.
            first, last = self.start + 1, self.start + shown
            if total is None:
                return f"rows {first:,}–{last:,}{suffix}"
            if self.total_is_estimate:
                return f"rows {first:,}–{last:,} of about {total:,}"
            if last == total:
                return f"last {shown:,} of {total:,} {noun}{suffix}"
            return f"rows {first:,}–{last:,} of {total:,} {noun}{suffix}"
        if total is None:
            return f"first {shown:,} {_rows(shown)}{suffix}"
        if self.total_is_estimate:
            if self.sorted_by_tags:
                # A bounded read: the rows read, in tag order, not the first in tag order.
                return (
                    f"{shown:,} {_rows(shown)} read, sorted by tags, of about {total:,}"
                )
            return f"first {shown:,} {_rows(shown)} of about {total:,}"
        if shown == total:
            return f"all {total:,} {noun if total != 1 else noun[:-1]}{suffix}"
        return f"first {shown:,} of {total:,} {noun}{suffix}"

    def _display_columns(self) -> list[tuple[str, str]]:
        """Return the shown columns in display order, each with its role: tags first."""
        tags = [c for c in self.tag_columns if c in self.table.column_names]
        return [(c, "tag") for c in tags] + [
            (c, "data") for c in self.table.column_names if c not in self.tag_columns
        ]

    def _legend(self, text: bool) -> str:
        """Return the legend line of the marks the shown columns carry, or empty."""
        roles = dict.fromkeys(role for _, role in self._display_columns())
        legends = [
            COLUMN_ROLE_MARKS[role].text_legend
            if text
            else COLUMN_ROLE_MARKS[role].html_legend
            for role in roles
        ]
        return "; ".join(legend for legend in legends if legend)

    def _display_frame(self, text: bool = False) -> pl.DataFrame:
        """Return a Polars frame of the rows for display: tag columns first, large cells
        summarised; for the text display, each header carries its role's prefix (a name
        that would then clash with another column's stays unmarked). ``table`` itself is
        never changed."""
        columns_shown = self._display_columns()
        table = self.table.select([name for name, _ in columns_shown])
        rows = table.num_rows
        half = _DISPLAY_ROWS // 2
        # Only the rows the display shows are summarised; the others are left null,
        # because Polars shows the first and last ``half`` of a long frame only.
        shown = (
            table
            if self._displays_every_row()
            else pa.concat_tables([table.slice(0, half), table.slice(rows - half)])
        )
        columns = []
        for name in table.column_names:
            summary = _summarise(shown.column(name))
            if summary is None:
                columns.append(table.column(name))
            elif shown is table:
                columns.append(pa.chunked_array([summary], type=pa.large_string()))
            else:
                columns.append(
                    pa.chunked_array(
                        [
                            pa.array(summary[:half], type=pa.large_string()),
                            pa.nulls(rows - 2 * half, type=pa.large_string()),
                            pa.array(summary[half:], type=pa.large_string()),
                        ]
                    )
                )
        names = list(table.column_names)
        if text:
            marked = [
                COLUMN_ROLE_MARKS[role].text_prefix + n for n, role in columns_shown
            ]
            taken = set(names)
            names = [
                m if m == n or m not in taken else n
                for m, n in zip(marked, names, strict=True)
            ]
            if len(set(names)) != len(names):
                names = list(table.column_names)
        return pl.DataFrame(pa.table(columns, names=names))

    def _displays_every_row(self) -> bool:
        """Whether the display shows every row: a view cut to ``n`` rows, or a short one."""
        return self.display_all_rows or self.table.num_rows <= _DISPLAY_ROWS

    def _display_rows(self) -> int:
        """Return Polars' ``tbl_rows`` for this view: all rows, or ``_DISPLAY_ROWS``."""
        return -1 if self._displays_every_row() else _DISPLAY_ROWS

    def _repr_html_(self) -> str:
        """Return the notebook preview: header, legend and the rows (large cells
        summarised), each column's header styled by its role (``COLUMN_ROLE_MARKS``)."""
        parts = [
            f"<div><strong>{html.escape(self.header(), quote=False)}</strong></div>"
        ]
        legend = self._legend(text=False)
        if legend:
            parts.append(
                f"<div><small>{html.escape(legend, quote=False)}</small></div>"
            )
        if self.table.num_columns:
            # Polars shortens strings to its own default length; show up to _MAX_TEXT.
            with pl.Config(fmt_str_lengths=_MAX_TEXT, tbl_rows=self._display_rows()):
                frame_html = self._display_frame()._repr_html_()
            roles = [role for _, role in self._display_columns()]
            parts.append(_mark_columns(frame_html, roles))
        return "\n".join(parts)

    def __str__(self) -> str:
        """Return the preview as text: header, legend and the rows, each column's
        header prefixed by its role's mark (``*`` for a tag)."""
        lines = [self.header()]
        legend = self._legend(text=True)
        if legend:
            lines.append(legend)
        if self.table.num_columns:
            with pl.Config(fmt_str_lengths=_MAX_TEXT, tbl_rows=self._display_rows()):
                lines.append(str(self._display_frame(text=True)))
        return "\n".join(lines)

    def __repr__(self) -> str:
        """Return a short description: kind, label, status and row count."""
        return (
            f"StreamView({self.kind}[{self.label}], status={self.status.value}, "
            f"rows={self.table.num_rows})"
        )


def _mark_columns(frame_html: str, roles: Sequence[str]) -> str:
    """Return Polars' HTML table ``frame_html`` with its columns styled by role.

    The cells are found by position, never by name, so a name that HTML escapes is
    handled as Polars wrote it. The header cell of a column gets its role's
    ``html_style``; every cell of the last column of a role followed by another role gets
    ``ROLE_BOUNDARY_STYLE``. Everything else is passed on unchanged.

    Args:
        frame_html: The HTML of the display frame, one column per entry of ``roles``.
        roles: The role of each column, in display order.
    """
    header = [COLUMN_ROLE_MARKS[role].html_style for role in roles]
    boundary = [
        ROLE_BOUNDARY_STYLE if i + 1 < len(roles) and roles[i + 1] != roles[i] else ""
        for i in range(len(roles))
    ]
    marker = _ColumnMarker(header, boundary)
    marker.feed(frame_html)
    marker.close()
    return "".join(marker.out)


class _ColumnMarker(HTMLParser):
    """Copies Polars' HTML table, adding inline styles to cells by column position."""

    def __init__(self, header: list[str], boundary: list[str]) -> None:
        super().__init__(convert_charrefs=False)
        self.header: list[str] = header
        self.boundary: list[str] = boundary
        self.out: list[str] = []
        self.in_thead: bool = False
        self.rows_in_thead: int = 0
        self.column: int = 0

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        raw = self.get_starttag_text() or f"<{tag}>"
        if tag == "thead":
            self.in_thead = True
        elif tag == "tr":
            self.column = 0
            if self.in_thead:
                self.rows_in_thead += 1
        elif tag in ("th", "td") and not attrs:
            index = self.column
            self.column += 1
            styles: list[str] = []
            if index < len(self.header):
                if self.in_thead and self.rows_in_thead == 1:
                    styles.append(self.header[index])
                styles.append(self.boundary[index])
            style = ";".join(part for part in styles if part)
            if style:
                raw = f'<{tag} style="{html.escape(style)}">'
        elif tag in ("th", "td"):
            self.column += 1
        self.out.append(raw)

    @override
    def handle_endtag(self, tag: str) -> None:
        if tag == "thead":
            self.in_thead = False
        self.out.append(f"</{tag}>")

    @override
    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.out.append(self.get_starttag_text() or f"<{tag}/>")

    @override
    def handle_data(self, data: str) -> None:
        self.out.append(data)

    @override
    def handle_entityref(self, name: str) -> None:
        self.out.append(f"&{name};")

    @override
    def handle_charref(self, name: str) -> None:
        self.out.append(f"&#{name};")

    @override
    def handle_comment(self, data: str) -> None:
        self.out.append(f"<!--{data}-->")

    @override
    def handle_decl(self, decl: str) -> None:
        self.out.append(f"<!{decl}>")


def _check_row_count(n: int) -> None:
    """Check a number of rows given to a view's ``head`` or ``tail``.

    Raises:
        TypeError: If ``n`` is not an ``int``.
        ValueError: If ``n`` is negative.
    """
    if isinstance(n, bool) or not isinstance(n, int):
        raise TypeError(f"the number of rows must be an int, got {type(n).__name__}")
    if n < 0:
        raise ValueError(f"the number of rows must not be negative, got {n}")


def _window_text(pool: str, shown: int, start: int, pool_rows: int) -> str:
    """Return the header text for ``shown`` rows from ``start`` of a pool of
    ``pool_rows`` rows named ``pool``."""
    if shown == pool_rows:
        return pool
    if start == 0:
        return f"first {shown:,} of {pool}"
    if start + shown == pool_rows:
        return f"last {shown:,} of {pool}"
    return f"rows {start + 1:,}–{start + shown:,} of {pool}"


_MAX_NAME = 48


def display_name(stream: Any) -> str:
    """Return the name the header shows for ``stream``: never its label changed.

    A stream that says (``_display_name``) gives its own, e.g. a source with no label
    of its own is shown by its source id; otherwise its label. A long name keeps its
    start and end around an ellipsis.
    """
    name_of = getattr(stream, "_display_name", None)
    name = str(name_of() if callable(name_of) else stream.label)
    if len(name) > _MAX_NAME:
        name = f"{name[:30]}…{name[-12:]}"
    return name


def preview_html(stream: Any) -> str:
    """Return the notebook preview of ``stream``: its head with the rows in tag order.

    The preview asks ``head`` for ``sort_by_tags`` (developer, November 12, 2026), so the
    rows shown do not depend on the order they were computed in. For a bounded read or a
    partial computation they are the rows read or computed so far, in tag order. A stream
    with no tag column shows its rows in stream order. It never computes anything more
    than ``head`` does.
    """
    try:
        head = stream.head(columns={"sort_by_tags": True})
    except Exception as error:  # noqa: BLE001 - a display hook must not raise
        return render_error_html(stream, error)
    if head.tag_columns and head.table.num_rows:
        head = dataclasses.replace(head, sorted_by_tags=True)
    return head._repr_html_()


def head_of(
    head: Any, stream: Any, *, origin: str | None = None, note: str | None = None
) -> StreamView:
    """Return ``head`` (another stream's) shown as ``stream``.

    A ``StreamView`` keeps every field. Any other result of ``head`` (a
    ``StreamViewProtocol``) is adapted from what that protocol gives: its rows, status
    and total.

    Args:
        head: What another stream's ``head`` returned.
        stream: The stream, source or node that shows those rows as its own.
        origin: Another origin for the rows, if given.
        note: Another note, if given.
    """
    if not isinstance(head, StreamView):
        head = StreamView(
            table=head.table,
            status=ViewStatus(head.status),
            kind=type(stream).__name__,
            label=display_name(stream),
            total_rows=head.total_rows,
        )
    return head.for_stream(stream, origin=origin, note=note)


def render_error_html(stream: Any, error: BaseException) -> str:
    """Return the preview of a stream whose ``head`` raised: its name and the error."""
    title = f"{type(stream).__name__}[{display_name(stream)}] — could not read: {error}"
    return f"<div><strong>{html.escape(title, quote=False)}</strong></div>"


def _rows(count: int) -> str:
    """Return ``row`` or ``rows`` for ``count``."""
    return "row" if count == 1 else "rows"


def _size(count: int) -> str:
    """Return a byte count as text: ``300 B``, ``1.2 KB``, ``3.4 MB``."""
    value = float(count)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1000 or unit == "GB":
            return f"{count} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1000
    return f"{count} B"  # pragma: no cover


def _summarise(column: pa.ChunkedArray[Any]) -> list[str | None] | None:
    """Return display strings for a column whose cells are too large to show as they are,
    or ``None`` to show the column as it is."""
    arrow_type = column.type
    if isinstance(arrow_type, pa.ExtensionType):
        return _summarise_extension(column, arrow_type)
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return _byte_summaries([column], "bytes")
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        # Lengths come from Arrow; only the kept parts become Python strings. A long
        # value keeps its start and its end (hash keys differ at the end), with an
        # ellipsis between.
        lengths: list[int | None] = pc.utf8_length(column).to_pylist()
        if all(n is None or n <= _MAX_TEXT for n in lengths):
            return None
        shorts: list[str | None] = pc.utf8_slice_codeunits(
            column, 0, _MAX_TEXT
        ).to_pylist()
        starts: list[str | None] = pc.utf8_slice_codeunits(
            column, 0, _TEXT_START
        ).to_pylist()
        ends = [
            None if n is None or n <= _MAX_TEXT else _text_end(column[i])
            for i, n in enumerate(lengths)
        ]
        return [
            None if n is None else f"{a}…{b}" if n > _MAX_TEXT else short
            for n, short, a, b in zip(lengths, shorts, starts, ends)
        ]
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return _summarise_list(column, arrow_type.value_type)
    return None


def _summarise_extension(
    column: pa.ChunkedArray[Any], arrow_type: pa.ExtensionType
) -> list[str | None] | None:
    """Return display strings for a logical-type column, or ``None`` to let Polars show it.

    An array column is recognised by its logical type's extension name, never by the
    bytes of a cell: it shows as ``ndarray float64 (2, 3)``, read from its stored form
    without decoding the data. Another logical type over binary storage shows its size,
    over a struct its name; any other storage is shown by Polars.
    """
    # The array logical type's module is read when a column is shown, not imported:
    # it sits above this module in the import graph (types, protocols, side effects),
    # and it loads numpy, which only an array column needs.
    numpy_type = importlib.import_module("orcapod.logical_types.numpy_type")
    array_extension_name: str = numpy_type.NDARRAY_EXTENSION_NAME

    storage_type = arrow_type.storage_type
    short_name = arrow_type.extension_name.rsplit(".", 1)[-1]
    storages = [
        cast("pa.ExtensionArray[Any]", array).storage for array in column.chunks
    ]
    is_binary = pa.types.is_binary(storage_type) or pa.types.is_large_binary(
        storage_type
    )
    if arrow_type.extension_name == array_extension_name:
        if pa.types.is_struct(storage_type):
            return [
                None if v is None else f"ndarray {v['dtype']} {_shape_text(v['shape'])}"
                for storage in storages
                for v in _struct_fields(
                    cast(pa.StructArray, storage), ["dtype", "shape"]
                )
            ]
        if is_binary:
            return [
                _npy_summary(storage[i])
                for storage in storages
                for i in range(len(storage))
            ]
    if is_binary:
        return _byte_summaries(storages, short_name)
    if pa.types.is_struct(storage_type):
        return [
            f"<{short_name}>" if valid else None
            for storage in storages
            for valid in storage.is_valid().to_pylist()
        ]
    return None


def _npy_summary(cell: Any) -> str | None:
    """Return ``ndarray <dtype> <shape>`` for one ``.npy`` cell, reading its header only.

    The ``.npy`` header is a magic string, a version, the header length and a Python
    dict literal with ``descr``, ``fortran_order`` and ``shape``. The buffer is sliced
    without copying, and the array data is never read.
    """
    if not cell.is_valid:
        return None
    buffer = cell.as_buffer()
    size = f"ndarray ({_size(buffer.size)})"
    try:
        prefix = buffer[:12].to_pybytes()
        if not prefix.startswith(b"\x93NUMPY"):
            return size
        if prefix[6] == 1:
            start, length = 10, int.from_bytes(prefix[8:10], "little")
        else:
            start, length = 12, int.from_bytes(prefix[8:12], "little")
        text = buffer[start : start + length].to_pybytes().decode("utf-8")
        header = ast.literal_eval(text)
        import numpy as np

        dtype = np.dtype(header["descr"])
        return f"ndarray {dtype.name} {_shape_text(header['shape'])}"
    except (ValueError, SyntaxError, KeyError, TypeError, IndexError):
        return size


def _byte_summaries(arrays: Sequence[Any], name: str) -> list[str | None]:
    """Return ``name (size)`` for each binary cell of ``arrays``, in order.

    Sizes come from Arrow (``binary_length``); the bytes are never read.
    """
    return [
        None if n is None else f"{name} ({_size(n)})"
        for array in arrays
        for n in pc.binary_length(array).to_pylist()
    ]


def _struct_fields(
    array: pa.StructArray, names: list[str]
) -> list[dict[str, Any] | None]:
    """Return each row of ``array`` as a dict of the named fields (``None`` for a null row)."""
    fields = {name: array.field(name).to_pylist() for name in names}
    valid = array.is_valid().to_pylist()
    return [
        {name: fields[name][i] for name in names} if valid[i] else None
        for i in range(len(array))
    ]


def _shape_text(shape: Any) -> str:
    """Return an array shape as Python writes a tuple: ``(40,)``, ``(2, 3)``."""
    dims = list(shape or [])
    if len(dims) == 1:
        return f"({dims[0]},)"
    return "(" + ", ".join(str(d) for d in dims) + ")"


def _text_end(cell: Any) -> str:
    """Return the last ``_TEXT_END`` characters of a string cell, reading only its end.

    A character is at most 4 bytes in UTF-8, so the last ``4 * _TEXT_END`` bytes hold
    them; a character cut at the start of that window is dropped when decoding.
    """
    buffer = cell.as_buffer()
    window = buffer[max(0, buffer.size - 4 * _TEXT_END) :].to_pybytes()
    return window.decode("utf-8", errors="ignore")[-_TEXT_END:]


def _summarise_list(
    column: pa.ChunkedArray[Any], value_type: pa.DataType
) -> list[str | None] | None:
    """Return display strings for a list column: short lists of scalars as written, others by length."""
    simple_items = not (
        isinstance(value_type, pa.ExtensionType)
        or pa.types.is_nested(value_type)
        or pa.types.is_binary(value_type)
        or pa.types.is_large_binary(value_type)
    )
    lengths: list[int | None] = pc.list_value_length(column).to_pylist()
    if simple_items and all(n is None or n <= _MAX_LIST_ITEMS for n in lengths):
        return None
    item = "list" if pa.types.is_nested(value_type) else None
    out: list[str | None] = []
    for index, length in enumerate(lengths):
        if length is None:
            out.append(None)
        elif simple_items and length <= _MAX_LIST_ITEMS:
            # Only a short list is converted to Python, to show its items.
            out.append(repr(column[index].as_py()))
        else:
            out.append(f"list (len {length})" + (f" of {item}" if item else ""))
    return out
