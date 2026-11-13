"""The first rows of a stream that are readily available, and their notebook preview (ITL-991).

``head(n)`` on a stream, source or node returns a ``StreamHead``: at most ``n`` rows the
object already holds, has cached from an earlier computation, has stored, or can read from
its table with a bounded read. It never computes: no user function is called, no operator
is run, no side effect is delivered, no source is polled and nothing is written. The
notebook preview of a stream is its ``head()`` rendered by ``StreamHead._repr_html_``.
"""

from __future__ import annotations

import ast
import dataclasses
import html
import importlib
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")


class HeadStatus(str, Enum):
    """Where the rows of a ``StreamHead`` came from, or why it has none.

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


NOT_COMPUTED_NOTE = "not computed; as_table(), iter_data() or a job run computes it"
STALE_NOTE = "not computed: inputs changed since the last computation"
NOTHING_STORED_NOTE = "nothing stored; run the job"
BLUEPRINT_NOTE = "pipeline blueprint; run it in a PipelineJob"
SIDE_EFFECTS_NOTE = (
    "not run; iterating it or a job run delivers its side effects; "
    "displaying never delivers side effects"
)

_DEFAULT_NOTES = {
    HeadStatus.NOT_COMPUTED: NOT_COMPUTED_NOTE,
    HeadStatus.NOTHING_STORED: NOTHING_STORED_NOTE,
    HeadStatus.UNAVAILABLE: "no rows available",
}

SCHEMA_NEEDS_POLL_NOTE = "columns not known until its source is polled"


def default_note(status: HeadStatus) -> str:
    """Return the note shown for a head with ``status`` and no rows of its own."""
    return _DEFAULT_NOTES[status]


# Words naming the rows of a ``ROWS`` head, by where they came from.
_ORIGIN_NOUNS = {
    "held": "rows",
    "read": "rows",
    "computed": "computed rows",
    "stored": "stored rows",
}

# Display limits of the preview.
_MAX_TEXT = 80
_TEXT_START = 56
_TEXT_END = 20
_MAX_LIST_ITEMS = 5


@dataclass(frozen=True)
class StreamHead:
    """At most ``n`` readily available rows of a stream, and how they were obtained.

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
            of a table), ``"computed"`` (cached from an earlier computation) or
            ``"stored"`` (a job node's store).
        note: For a head without rows, the explanation shown instead of the default;
            for a head with rows, a remark shown after the row count.
        description: When given, the whole text after the name in the header, written
            by a stream that words its own rows (a table-backed source naming its
            ``where``); otherwise the header is built from the fields above.
        sorted_by_tags: ``True`` when the rows were asked for in tag order (the notebook
            preview): the header then says so, and for a bounded read or a partial
            computation says they are the rows read or computed so far, in tag order.
            A ``description`` words its own sorting.
    """

    table: pa.Table
    status: HeadStatus
    kind: str
    label: str
    tag_columns: tuple[str, ...] = ()
    total_rows: int | None = None
    total_is_estimate: bool = False
    origin: str = "held"
    note: str | None = None
    description: str | None = None
    sorted_by_tags: bool = False

    @classmethod
    def of_rows(
        cls,
        stream: Any,
        table: pa.Table,
        *,
        tag_columns: tuple[str, ...],
        total_rows: int | None,
        status: HeadStatus = HeadStatus.ROWS,
        total_is_estimate: bool = False,
        origin: str = "held",
        note: str | None = None,
        description: str | None = None,
    ) -> StreamHead:
        """Return a head with rows of ``stream``."""
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
        )

    @classmethod
    def without_rows(
        cls,
        stream: Any,
        status: HeadStatus,
        table: pa.Table,
        *,
        tag_columns: tuple[str, ...],
        note: str | None = None,
    ) -> StreamHead:
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
    ) -> StreamHead:
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
    # Rendering
    # ------------------------------------------------------------------

    def header(self) -> str:
        """Return the one-line description shown above the rows."""
        return f"{self.kind}[{self.label}] — {self.description or self._describe()}"

    def _describe(self) -> str:
        """Return the header text after the name, for this status."""
        shown = self.table.num_rows
        if self.status is HeadStatus.ROWS:
            text = self._describe_rows(shown)
        elif self.status is HeadStatus.PARTIAL:
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

    def _display_frame(self) -> pl.DataFrame:
        """Return a Polars frame of the rows for display: tag columns first, large cells
        summarised. ``table`` itself is never changed."""
        order = [c for c in self.tag_columns if c in self.table.column_names] + [
            c for c in self.table.column_names if c not in self.tag_columns
        ]
        table = self.table.select(order)
        columns = []
        for name in table.column_names:
            column = table.column(name)
            summary = _summarise(column)
            if summary is None:
                columns.append(column)
            else:
                columns.append(pa.chunked_array([summary], type=pa.large_string()))
        return pl.DataFrame(pa.table(columns, names=table.column_names))

    def _tags_line(self) -> str:
        """Return the line naming the tag columns, or an empty string when there are none."""
        if not self.tag_columns:
            return ""
        return "tags: " + ", ".join(self.tag_columns)

    def _repr_html_(self) -> str:
        """Return the notebook preview: header, tag line and the rows (large cells summarised)."""
        parts = [
            f"<div><strong>{html.escape(self.header(), quote=False)}</strong></div>"
        ]
        tags = self._tags_line()
        if tags:
            parts.append(f"<div><small>{html.escape(tags, quote=False)}</small></div>")
        if self.table.num_columns:
            # Polars shortens strings to its own default length; show up to _MAX_TEXT.
            with pl.Config(fmt_str_lengths=_MAX_TEXT, tbl_rows=-1):
                parts.append(self._display_frame()._repr_html_())
        return "\n".join(parts)

    def __str__(self) -> str:
        """Return the preview as text: header, tag line and the rows."""
        lines = [self.header()]
        tags = self._tags_line()
        if tags:
            lines.append(tags)
        if self.table.num_columns:
            with pl.Config(fmt_str_lengths=_MAX_TEXT, tbl_rows=-1):
                lines.append(str(self._display_frame()))
        return "\n".join(lines)

    def __repr__(self) -> str:
        """Return a short description: kind, label, status and row count."""
        return (
            f"StreamHead({self.kind}[{self.label}], status={self.status.value}, "
            f"rows={self.table.num_rows})"
        )


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
) -> StreamHead:
    """Return ``head`` (another stream's) shown as ``stream``.

    A ``StreamHead`` keeps every field. Any other result of ``head`` (a
    ``StreamHeadProtocol``) is adapted from what that protocol gives: its rows, status
    and total.

    Args:
        head: What another stream's ``head`` returned.
        stream: The stream, source or node that shows those rows as its own.
        origin: Another origin for the rows, if given.
        note: Another note, if given.
    """
    if not isinstance(head, StreamHead):
        head = StreamHead(
            table=head.table,
            status=HeadStatus(head.status),
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
