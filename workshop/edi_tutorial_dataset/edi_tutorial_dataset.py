"""Build the workshop Spiral project: a small subset of the V1 EDI sample.

The workshop reads EDI tables from SpiralDB through Orcapod sources. The V1 EDI sample
project (``edi-sample-v1-855632``) is too large for a laptop, and every column in it,
key columns included, is declared nullable. This tool copies a subset of it (a set of
units and one time window; small tables whole) into a new project, and declares each
column non-nullable where the EDI schema specification (v0.3.0) requires a value.

Two files next to this module drive it:

* ``nullability_map.json``: for every table and column, the specification's type, the
  nullability to declare, the key columns and the foreign keys. Entries whose
  nullability is ``null`` are decisions still pending; ``copy`` refuses to run while
  any remain.
* ``subset.json``: the time window, the unit IDs, and how each table is selected.

Every command needs ``pyspiral>=0.16.33`` and a ``spiral login``. Run from the
repository root, for example::

    uv run --with pyspiral==0.16.33 python \\
        workshop/edi_tutorial_dataset/edi_tutorial_dataset.py plan

Commands (see ``--help`` on each):

* ``plan``: read-only dry run of the whole copy against the source.
* ``create``: create the target project and its marker table.
* ``probe``: check, in the target project, how SpiralDB treats non-nullable columns.
* ``copy``: copy the subset (``--dry-run`` writes nothing).
* ``verify``: check the target against the source and the map.
* ``derive-map`` / ``render-map``: build the map from the specification, print it as
  Markdown.

The tool reads only from ``--source-project``. It writes only to ``--target-project``,
which must be repeated as ``--allow-writes``, must not start with ``edi-sample-v``, and
must be a project that ``create`` made: ``create`` writes a marker table
(``meta.edi_tutorial_build``) into the new project, and every table the tool writes
carries the same marker in its metadata. A target holding any table without the marker
is refused.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import re
import sys
import typing
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

HERE = Path(__file__).resolve().parent
MAP_PATH = HERE / "nullability_map.json"
SUBSET_PATH = HERE / "subset.json"
DECISIONS_PATH = HERE / "decisions.json"

MIN_PYSPIRAL = (0, 16, 33)
DEFAULT_SOURCE = "edi-sample-v1-855632"
MARKER_TABLE = "meta.edi_tutorial_build"
PROBE_TABLE = "meta.nullability_probe"
MARKER_KEY = "edi_tutorial_dataset.marker"
STATUS_KEY = "edi_tutorial_dataset.status"
PROVENANCE_KEY = "edi_tutorial_dataset.provenance"
# SpiralDB limits each table-metadata key and value pair to 8 KiB.
METADATA_LIMIT = 8 * 1024
FORBIDDEN_TARGET_PREFIXES = ("edi-sample-v",)
TOOL_NAME = "orcapod-python workshop/edi_tutorial_dataset"

# Rows per slice when two key-sorted streams are compared value by value.
COMPARE_SLICE_ROWS = 4096


# --------------------------------------------------------------------------- errors


class ToolError(Exception):
    """Base class for every error this tool raises on purpose."""


class TargetRefusedError(ToolError):
    """The target project is not one this tool may write to."""


class PendingDecisionError(ToolError):
    """The nullability map still holds decisions for the developer."""


class SchemaMismatchError(ToolError):
    """A table's columns or Arrow types differ from what the map records."""


class NullInRequiredColumnError(ToolError):
    """A column declared non-nullable holds nulls.

    Attributes:
        path: Column path, outermost first (a struct child is ``(group, field)``).
        null_count: Number of nulls found.
    """

    def __init__(self, path: tuple[str, ...], null_count: int) -> None:
        """Record the column path and null count and build the message."""
        self.path = path
        self.null_count = null_count
        super().__init__(
            f"column {'.'.join(path)!r} is declared non-nullable but holds "
            f"{null_count} null(s); nothing is coerced"
        )

    def within(self, parent: str) -> NullInRequiredColumnError:
        """Return the same error with ``parent`` prepended to the column path."""
        return NullInRequiredColumnError((parent, *self.path), self.null_count)


class StaleTableError(ToolError):
    """A complete target table was built from a different subset or map."""


class VerificationError(ToolError):
    """``verify`` found problems; ``problems`` lists them."""

    def __init__(self, problems: list[str]) -> None:
        """Keep ``problems`` and list them in the message."""
        self.problems = problems
        super().__init__(f"{len(problems)} problem(s):\n  " + "\n  ".join(problems))


# ------------------------------------------------------------------- map and subset


@dataclasses.dataclass(frozen=True)
class ColumnSpec:
    """One column (or one field of a column group) in the nullability map.

    Attributes:
        arrow_type: The source column's Arrow type, as ``str(pa.DataType)``.
        nullable: Nullability to declare; ``None`` while the decision is pending.
        spec_type: The specification's type (``None`` if the spec does not define it).
        key: Whether the column is part of the table's key.
    """

    arrow_type: str
    nullable: bool | None
    spec_type: str | None = None
    key: bool = False


@dataclasses.dataclass(frozen=True)
class GroupSpec:
    """A Spiral column group (a top-level struct column) in the nullability map."""

    arrow_type: str
    nullable: bool | None
    fields: Mapping[str, ColumnSpec]


@dataclasses.dataclass(frozen=True)
class ForeignKey:
    """Child columns that must resolve to the key of rows in ``parent``.

    The child columns carry the same names as the parent's key columns.

    Attributes:
        parent: The referenced table.
        columns: The parent's key columns, present under the same names in the child.
        note: Why the reference is checked, when the specification does not say.
        accepted_unresolved: Key tuples known not to resolve in the source, accepted
            by the developer; they are reported, not counted as problems. Values are
            JSON scalars; a timestamp or date is written in ISO format.
    """

    parent: str
    columns: tuple[str, ...]
    note: str = ""
    accepted_unresolved: frozenset[tuple[Any, ...]] = frozenset()


@dataclasses.dataclass(frozen=True)
class TableSpec:
    """One table of the nullability map."""

    name: str
    in_spec: bool
    keys: tuple[str, ...]
    columns: Mapping[str, ColumnSpec]
    groups: Mapping[str, GroupSpec]
    foreign_keys: tuple[ForeignKey, ...]

    def pending(self) -> list[str]:
        """Return the column paths whose nullability is still undecided."""
        out = [n for n, c in self.columns.items() if c.nullable is None]
        for g, group in self.groups.items():
            if group.nullable is None:
                out.append(g)
            out.extend(
                f"{g}.{n}" for n, c in group.fields.items() if c.nullable is None
            )
        return out


@dataclasses.dataclass(frozen=True)
class NullabilityMap:
    """The whole nullability map: tables by name, plus where it came from."""

    spec_version: str
    source_project: str
    tables: Mapping[str, TableSpec]
    raw: Mapping[str, Any]

    def pending(self) -> dict[str, list[str]]:
        """Return pending decisions by table (tables without any are left out)."""
        return {n: p for n, t in self.tables.items() if (p := t.pending())}


def _column_from_json(d: Mapping[str, Any]) -> ColumnSpec:
    """Build a ``ColumnSpec`` from its JSON form."""
    return ColumnSpec(
        arrow_type=d["arrow_type"],
        nullable=d["nullable"],
        spec_type=d.get("spec_type"),
        key=bool(d.get("key", False)),
    )


def parse_map(raw: Mapping[str, Any]) -> NullabilityMap:
    """Build a ``NullabilityMap`` from its JSON form."""
    tables: dict[str, TableSpec] = {}
    for name, t in raw["tables"].items():
        groups = {
            g: GroupSpec(
                arrow_type=gd["arrow_type"],
                nullable=gd["nullable"],
                fields={f: _column_from_json(fd) for f, fd in gd["fields"].items()},
            )
            for g, gd in t.get("groups", {}).items()
        }
        tables[name] = TableSpec(
            name=name,
            in_spec=bool(t["in_spec"]),
            keys=tuple(t["keys"]),
            columns={c: _column_from_json(cd) for c, cd in t["columns"].items()},
            groups=groups,
            foreign_keys=tuple(
                ForeignKey(
                    fk["parent"],
                    tuple(fk["columns"]),
                    fk.get("note", ""),
                    frozenset(tuple(k) for k in fk.get("accepted_unresolved", [])),
                )
                for fk in t.get("foreign_keys", [])
            ),
        )
    return NullabilityMap(raw["spec_version"], raw["source_project"], tables, raw)


def load_map(path: Path = MAP_PATH) -> NullabilityMap:
    """Read the nullability map from ``path``."""
    return parse_map(json.loads(Path(path).read_text(encoding="utf-8")))


@dataclasses.dataclass(frozen=True)
class Subset:
    """Which rows of the source make up the tutorial dataset.

    Attributes:
        dataset: Spiral dataset of every table, in source and target.
        window: Half-open ``[start, stop)`` in µs relative to ``session_start_datetime``.
        unit_ids: The kept units.
        tables: Selection mode per table: ``whole``, ``window``, ``window_units``,
            ``units`` or ``trials``.
        trial_table: The table selected by ``trials`` mode.
        raw: The JSON form, hashed into each table's status digest.
    """

    dataset: str
    window: tuple[int, int]
    unit_ids: tuple[int, ...]
    tables: Mapping[str, str]
    trial_table: str
    raw: Mapping[str, Any]


SELECTION_MODES = ("whole", "window", "window_units", "units", "trials")


def parse_subset(raw: Mapping[str, Any]) -> Subset:
    """Build a ``Subset`` from its JSON form.

    Raises:
        ToolError: if the window is empty or a selection mode is unknown.
    """
    start, stop = (int(x) for x in raw["window_us"])
    if not 0 <= start < stop:
        raise ToolError(
            f"window_us must satisfy 0 <= start < stop, got {[start, stop]}"
        )
    tables = {n: t["mode"] for n, t in raw["tables"].items()}
    unknown = {n: m for n, m in tables.items() if m not in SELECTION_MODES}
    if unknown:
        raise ToolError(f"unknown selection modes {unknown}; known: {SELECTION_MODES}")
    return Subset(
        dataset=raw.get("dataset", "default"),
        window=(start, stop),
        unit_ids=tuple(sorted(int(u) for u in raw["unit_ids"])),
        tables=tables,
        trial_table=raw.get("trial_table", "experiment_freeviewing_trial"),
        raw=raw,
    )


def load_subset(path: Path = SUBSET_PATH) -> Subset:
    """Read the subset definition from ``path``."""
    return parse_subset(json.loads(Path(path).read_text(encoding="utf-8")))


# The parts of a table's map entry that decide what is written. Foreign keys and
# their accepted exceptions only decide what plan and verify report.
_CONTENT_KEYS = ("in_spec", "keys", "columns", "groups")


def table_digest(nmap: NullabilityMap, subset: Subset, table: str) -> str:
    """Return a digest of everything that decides a table's copied content."""
    entry = nmap.raw["tables"][table]
    payload = {
        "source_project": nmap.source_project,
        "map": {k: entry[k] for k in _CONTENT_KEYS if k in entry},
        "subset": {k: v for k, v in subset.raw.items() if k != "notes"},
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


# ------------------------------------------------------------------ schema handling


def _same_type_up_to_nullability(a: pa.DataType, b: pa.DataType) -> bool:
    """Return whether two types are equal, ignoring struct children's nullability."""
    if pa.types.is_struct(a) and pa.types.is_struct(b):
        if a.num_fields != b.num_fields:
            return False
        return all(
            a.field(i).name == b.field(i).name
            and _same_type_up_to_nullability(a.field(i).type, b.field(i).type)
            for i in range(a.num_fields)
        )
    return a == b


def target_schema(
    source: pa.Schema, spec: TableSpec, *, allow_pending: bool = False
) -> pa.Schema:
    """Return the schema the target table is written with.

    Types are the source's, exactly. Nullability comes from the map: for every
    top-level column, and for a column group and its fields.

    Args:
        source: The source table's schema.
        spec: The table's entry in the nullability map.
        allow_pending: Keep the source's nullability where a decision is pending
            (used by ``plan``); otherwise a pending decision raises.

    Raises:
        SchemaMismatchError: if the source's columns or types differ from the map.
        PendingDecisionError: if a decision is pending and ``allow_pending`` is false.
    """
    expected = list(spec.columns) + list(spec.groups)
    if sorted(source.names) != sorted(expected):
        missing = sorted(set(expected) - set(source.names))
        extra = sorted(set(source.names) - set(expected))
        raise SchemaMismatchError(
            f"source columns differ from the map: missing {missing}, unexpected {extra}"
        )

    def nullability(decided: bool | None, field: pa.Field[Any], path: str) -> bool:
        """Return the decided nullability, or the source's while pending if allowed.

        Raises:
            PendingDecisionError: if ``decided`` is pending and ``allow_pending`` is
                false.
        """
        if decided is not None:
            return decided
        if allow_pending:
            return field.nullable
        raise PendingDecisionError(f"nullability of {path!r} is pending in the map")

    fields: list[pa.Field[Any]] = []
    for field in source:
        if field.name in spec.groups:
            group = spec.groups[field.name]
            if str(field.type) != group.arrow_type:
                raise SchemaMismatchError(
                    f"column group {field.name!r} has type {field.type}, "
                    f"the map records {group.arrow_type}"
                )
            children = []
            for child in field.type:
                cs = group.fields.get(child.name)
                if cs is None:
                    raise SchemaMismatchError(
                        f"field {field.name}.{child.name} is not in the map"
                    )
                children.append(
                    child.with_nullable(
                        nullability(cs.nullable, child, f"{field.name}.{child.name}")
                    )
                )
            fields.append(
                pa.field(
                    field.name,
                    pa.struct(children),
                    nullability(group.nullable, field, field.name),
                )
            )
        else:
            cs = spec.columns[field.name]
            if str(field.type) != cs.arrow_type:
                raise SchemaMismatchError(
                    f"column {field.name!r} has type {field.type}, "
                    f"the map records {cs.arrow_type}"
                )
            fields.append(
                field.with_nullable(nullability(cs.nullable, field, field.name))
            )
    return pa.schema(fields)


def key_schema(schema: pa.Schema, keys: Sequence[str]) -> pa.Schema:
    """Return the key columns of ``schema``, in key order."""
    return pa.schema([schema.field(k) for k in keys])


def _stored_type(t: pa.DataType) -> pa.DataType:
    """Return ``t`` as SpiralDB stores it: struct fields nullable, list items kept."""
    if pa.types.is_struct(t):
        return pa.struct([pa.field(f.name, _stored_type(f.type), True) for f in t])
    if pa.types.is_fixed_size_list(t):
        item = t.value_field
        return pa.list_(item.with_type(_stored_type(item.type)), t.list_size)
    if pa.types.is_list(t):
        return pa.list_(t.value_field.with_type(_stored_type(t.value_field.type)))
    if pa.types.is_large_list(t):
        return pa.large_list(t.value_field.with_type(_stored_type(t.value_field.type)))
    return t


def stored_schema(schema: pa.Schema) -> pa.Schema:
    """Return ``schema`` as SpiralDB stores it.

    SpiralDB (pyspiral 0.16.33, probed live on November 11, 2026) stores every
    top-level column nullable, key columns included, and every struct field nullable;
    only list and fixed-size-list items keep a non-null flag. The nullability the map
    declares is therefore enforced by the copy (a null in a required column stops it)
    and checked by ``verify`` from the data, not from the stored schema.
    """
    return pa.schema([pa.field(f.name, _stored_type(f.type), True) for f in schema])


# The path segment for a list item, as in Orcapod's nullability record (ITL-936).
ITEM = "[]"


def _is_list(t: pa.DataType) -> bool:
    """Return whether ``t`` is a list, large list or fixed-size list type."""
    return (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
    )


def non_null_paths(schema: pa.Schema) -> list[tuple[str, ...]]:
    """Return the paths ``schema`` declares non-nullable, sorted.

    A path starts with a top-level column; each further segment is a struct field
    name, or ``"[]"`` for a list item, as Orcapod's nullability record (ITL-936)
    lists them. Used to show what ``annotate`` records and to check it in ``verify``.
    """
    out: list[tuple[str, ...]] = []

    def walk(field: pa.Field[Any], path: tuple[str, ...]) -> None:
        """Record ``path`` if ``field`` is non-nullable; descend into its children."""
        if not field.nullable:
            out.append(path)
        t = field.type
        if pa.types.is_struct(t):
            for child in t:
                walk(child, (*path, child.name))
        elif _is_list(t):
            walk(t.value_field, (*path, ITEM))

    for f in schema:
        walk(f, (f.name,))
    return sorted(out)


def _conform_array(arr: pa.Array[Any], field: pa.Field[Any]) -> pa.Array[Any]:
    """Return ``arr`` typed as ``field`` declares, refusing nulls it does not allow.

    Raises:
        SchemaMismatchError: if the types differ beyond nullability.
        NullInRequiredColumnError: if ``field`` (or a declared struct child, or a
            list item declared non-nullable) holds nulls.
    """
    if not _same_type_up_to_nullability(arr.type, field.type):
        raise SchemaMismatchError(
            f"column {field.name!r} has type {arr.type}, expected {field.type}"
        )
    if not field.nullable and arr.null_count:
        raise NullInRequiredColumnError((field.name,), arr.null_count)
    if _is_list(field.type):
        # Check the items against the item field, recursively, so a required item
        # of a nested list or a required field of a struct item is checked too.
        try:
            _conform_array(pc.list_flatten(arr), field.type.value_field)
        except NullInRequiredColumnError as err:
            raise NullInRequiredColumnError(
                (field.name, ITEM, *err.path[1:]), err.null_count
            ) from None
    if not pa.types.is_struct(field.type):
        return arr
    struct = arr if isinstance(arr, pa.StructArray) else None
    if struct is None:
        raise SchemaMismatchError(f"column {field.name!r} is not a struct array")
    # Check every child even when the types already match: Arrow builds arrays
    # without enforcing child nullability, so equal types prove nothing about values.
    children = []
    for child_field, child in zip(field.type, struct.flatten(), strict=True):
        try:
            children.append(_conform_array(child, child_field))
        except NullInRequiredColumnError as err:
            raise err.within(field.name) from None
    if arr.type == field.type:
        return arr
    mask = struct.is_null() if struct.null_count else None
    return pa.StructArray.from_arrays(children, fields=list(field.type), mask=mask)


def conform_batch(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """Return ``batch`` with ``schema``'s nullability, after checking it may have it.

    The column names and types must equal the schema's (nullability aside). Every
    non-nullable column, and every non-nullable field of a struct column, must hold
    no nulls. Nothing is cast or coerced.

    Raises:
        SchemaMismatchError: on a column or type difference.
        NullInRequiredColumnError: on a null in a non-nullable column.
    """
    if batch.schema.names != schema.names:
        raise SchemaMismatchError(
            f"batch columns {batch.schema.names} differ from {schema.names}"
        )
    arrays = [
        _conform_array(col, field)
        for col, field in zip(batch.columns, schema, strict=True)
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


# --------------------------------------------------------------------- source reads


def _qualified(subset: Subset, table: str) -> str:
    """Return the identifier of ``table`` in the subset's dataset."""
    return f"{subset.dataset}.{table}"


def _window_filter(table: Any, ts_type: pa.DataType, window: tuple[int, int]) -> Any:
    """Return a Spiral filter for ``window`` on ``table``'s ``timestamp`` column.

    The literals carry the column's exact Arrow type: SpiralDB refuses to compare
    ``u64`` with an ``i64`` literal.
    """
    start, stop = window
    col = table["timestamp"]
    return (col >= pa.scalar(start, ts_type)) & (col < pa.scalar(stop, ts_type))


def _row_mask(
    batch: pa.RecordBatch,
    mode: str,
    window: tuple[int, int],
    unit_ids: pa.Array[Any],
    trials: pa.Array[Any] | None,
) -> pa.Array[Any] | None:
    """Return the client-side row mask for ``mode`` (``None`` keeps every row).

    The window is also applied here, so a row outside it is dropped even if a
    server-side filter let it through.
    """
    masks = []
    if mode in ("window", "window_units"):
        ts = batch.column(batch.schema.get_field_index("timestamp"))
        ts_type = ts.type
        masks.append(
            pc.and_(
                pc.greater_equal(ts, pa.scalar(window[0], ts_type)),
                pc.less(ts, pa.scalar(window[1], ts_type)),
            )
        )
    if mode in ("window_units", "units"):
        col = batch.column(batch.schema.get_field_index("unit_id"))
        masks.append(pc.is_in(col, value_set=unit_ids.cast(col.type)))
    if mode == "trials":
        if trials is None:
            raise ToolError("trial selection needs the referenced trials first")
        col = batch.column(batch.schema.get_field_index("trial_index"))
        masks.append(pc.is_in(col, value_set=trials.cast(col.type)))
    if not masks:
        return None
    mask = masks[0]
    for m in masks[1:]:
        mask = pc.and_(mask, m)
    return mask


def read_selection(
    client: Any,
    project: Any,
    subset: Subset,
    table: str,
    mode: str,
    trials: Sequence[int] | None = None,
) -> Iterator[pa.RecordBatch]:
    """Yield the selected rows of one source table, in key order.

    Args:
        client: A ``spiral.Spiral`` client (or a fake with the same surface).
        project: The source ``Project``.
        subset: The subset definition.
        table: Table name (without dataset).
        mode: The table's selection mode.
        trials: The trial indexes to keep (``trials`` mode only).
    """
    t = project.table(_qualified(subset, table))
    where = None
    if mode in ("window", "window_units"):
        ts_type = t.schema().to_arrow().field("timestamp").type
        where = _window_filter(t, ts_type, subset.window)
    unit_ids = pa.array(subset.unit_ids, pa.int64())
    trial_arr = pa.array(sorted(trials), pa.int64()) if trials is not None else None
    scan = client.scan(t, where=where) if where is not None else client.scan(t)
    for batch in scan.to_record_batches():
        mask = _row_mask(batch, mode, subset.window, unit_ids, trial_arr)
        out = batch if mask is None else batch.filter(mask)
        if out.num_rows:
            yield out


def overlapping_trials(
    client: Any, project: Any, subset: Subset
) -> tuple[set[int], set[int]]:
    """Return the trials that overlap the window, and every trial in the table.

    A trial overlaps ``[start, stop)`` if it starts before ``stop`` and ends at or
    after ``start``; a trial without an end overlaps if it starts inside the window.
    """
    start, stop = subset.window
    keep: set[int] = set()
    every: set[int] = set()
    for batch in client.scan(
        project.table(_qualified(subset, subset.trial_table))
    ).to_record_batches():
        rows = batch.select(
            ["trial_index", "trial_start_timestamp", "trial_end_timestamp"]
        ).to_pylist()
        for r in rows:
            every.add(r["trial_index"])
            begin, end = r["trial_start_timestamp"], r["trial_end_timestamp"]
            if end is None:
                if start <= begin < stop:
                    keep.add(r["trial_index"])
            elif begin < stop and end >= start:
                keep.add(r["trial_index"])
    return keep, every


def kept_trials(client: Any, project: Any, subset: Subset) -> list[int]:
    """Return the trials to keep: those overlapping the window, plus every trial a
    kept row of another table references through ``trial_index``.

    Raises:
        ToolError: if a kept row references a trial the trial table does not hold.
    """
    referenced: set[int] = set()
    for name in table_order(subset):
        mode = subset.tables[name]
        if mode == "trials":
            continue
        schema = project.table(_qualified(subset, name)).schema().to_arrow()
        if "trial_index" not in schema.names:
            continue
        for batch in read_selection(client, project, subset, name, mode):
            referenced.update(
                x
                for x in pc.unique(batch.column("trial_index")).to_pylist()
                if x is not None
            )
    overlap, every = overlapping_trials(client, project, subset)
    unknown = referenced - every
    if unknown:
        raise ToolError(
            f"kept rows reference trials missing from {subset.trial_table}: {sorted(unknown)}"
        )
    return sorted(overlap | referenced)


def table_order(subset: Subset) -> list[str]:
    """Return the copy order: trial table last, so trial references are known."""
    names = sorted(subset.tables)
    trial_tables = [n for n in names if subset.tables[n] == "trials"]
    return [n for n in names if subset.tables[n] != "trials"] + trial_tables


# ------------------------------------------------------------- integrity bookkeeping


@dataclasses.dataclass
class TableResult:
    """What the pipeline produced for one table."""

    name: str
    rows: int = 0
    nbytes: int = 0
    action: str = ""


@dataclasses.dataclass
class Integrity:
    """Distinct key tuples, for checking foreign keys after every table is read."""

    parent_keys: dict[str, set[tuple[Any, ...]]] = dataclasses.field(
        default_factory=dict
    )
    child_refs: dict[tuple[str, ForeignKey], set[tuple[Any, ...]]] = dataclasses.field(
        default_factory=dict
    )

    def observe(self, nmap: NullabilityMap, table: str, batch: pa.RecordBatch) -> None:
        """Record the key tuples of ``batch`` that foreign keys need."""
        spec = nmap.tables[table]
        if any(
            fk.parent == table for t in nmap.tables.values() for fk in t.foreign_keys
        ):
            self.parent_keys.setdefault(table, set()).update(_tuples(batch, spec.keys))
        for fk in spec.foreign_keys:
            if fk.parent in nmap.tables:
                self.child_refs.setdefault((table, fk), set()).update(
                    t for t in _tuples(batch, fk.columns) if None not in t
                )

    def problems(self, present: Iterable[str]) -> list[str]:
        """Return every unresolved reference among the tables in ``present``.

        References a foreign key lists as accepted are left out; ``accepted`` reports
        them.
        """
        return self._unresolved(set(present), accepted=False)

    def accepted(self, present: Iterable[str]) -> list[str]:
        """Return the unresolved references the map accepts, among ``present``."""
        return self._unresolved(set(present), accepted=True)

    def _unresolved(self, present: set[str], *, accepted: bool) -> list[str]:
        """Return the unresolved references among the tables in ``present``.

        Args:
            present: Tables the project holds.
            accepted: Report the key tuples the map accepts as unresolved instead of the
                others (a parent table missing from the project is reported only when
                false).

        Returns:
            One line per foreign key with unresolved key tuples, with a sample of them.
        """
        out = []
        for (child, fk), refs in sorted(
            self.child_refs.items(), key=lambda kv: (kv[0][0], kv[0][1].parent)
        ):
            if child not in present:
                continue
            if fk.parent not in present:
                if not accepted:
                    out.append(
                        f"{child} -> {fk.parent}: parent table is not in the project"
                    )
                continue
            missing = {
                k
                for k in refs - self.parent_keys.get(fk.parent, set())
                if (_json_key(k) in fk.accepted_unresolved) == accepted
            }
            if missing:
                sample = sorted(missing, key=repr)[:3]
                out.append(
                    f"{child} -> {fk.parent} {list(fk.columns)}: {len(missing)} "
                    f"{'accepted ' if accepted else ''}unresolved key(s), e.g. {sample}"
                )
        return out


def _json_key(key: tuple[Any, ...]) -> tuple[Any, ...]:
    """Return ``key`` as the map writes it: timestamps and dates in ISO format."""
    return tuple(v.isoformat() if hasattr(v, "isoformat") else v for v in key)


def _tuples(batch: pa.RecordBatch, columns: Sequence[str]) -> set[tuple[Any, ...]]:
    """Return the distinct tuples of ``columns`` across the rows of ``batch``."""
    if not batch.num_rows:
        return set()
    cols = [batch.column(batch.schema.get_field_index(c)).to_pylist() for c in columns]
    return set(zip(*cols, strict=True))


# ----------------------------------------------------------------------- the sinks


class DryRunSink:
    """Accepts every batch and writes nothing.

    Args:
        replace: Tables a real copy would drop and copy again.
    """

    skipped_rows = 0

    def __init__(self, replace: Iterable[str] = ()) -> None:
        """Remember the tables a real copy would replace."""
        self._replace = frozenset(replace)

    def open_table(
        self, table: str, schema: pa.Schema, keys: pa.Schema, digest: str
    ) -> str:
        """Return the action a real copy would take for ``table``."""
        return "would replace" if table in self._replace else "would write"

    def write(self, batch: pa.RecordBatch) -> None:
        """Discard ``batch``."""

    def commit(self, rows: int) -> None:
        """Do nothing."""

    def abort(self) -> None:
        """Do nothing."""


class SpiralSink:
    """Writes each table to the target project in one transaction.

    Every table it creates carries the build marker in its metadata, set before any
    data is written. When a table's transaction commits, its status metadata records
    the row count and the subset digest, so a re-run can skip it.
    """

    def __init__(
        self,
        project: Any,
        subset: Subset,
        marker: Mapping[str, Any],
        replace: Iterable[str] = (),
    ) -> None:
        """Hold the target project and the subset, and encode the build marker.

        Args:
            project: The target Spiral project.
            subset: The subset being copied.
            marker: The build marker, stored as JSON in each created table's metadata.
            replace: Tables to drop and copy again.
        """
        self._project = project
        self._subset = subset
        self._replace = frozenset(replace)
        self._marker = json.dumps(marker, sort_keys=True).encode()
        self._table: Any = None
        self._txn: Any = None
        self._digest = ""
        self.skipped_rows = 0

    def open_table(
        self, table: str, schema: pa.Schema, keys: pa.Schema, digest: str
    ) -> str:
        """Prepare ``table`` for writing; return ``"skip"`` if it is already complete.

        An existing table that is not complete is dropped and created again, so no
        row of an earlier attempt survives.

        Raises:
            TargetRefusedError: if the table exists without this build's marker.
            StaleTableError: if it is complete for a different digest.
        """
        ident = _qualified(self._subset, table)
        existing = {f"{r.dataset}.{r.table}" for r in self._project.list_tables()}
        if ident in existing and table in self._replace:
            if (
                self._project.table(ident).get_metadata().get(MARKER_KEY)
                != self._marker
            ):
                raise TargetRefusedError(f"{ident} exists without this build's marker")
            self._project.drop_table(ident)
            existing.discard(ident)
            replaced = True
        else:
            replaced = False
        if ident in existing:
            t = self._project.table(ident)
            meta = t.get_metadata()
            if meta.get(MARKER_KEY) != self._marker:
                raise TargetRefusedError(f"{ident} exists without this build's marker")
            status = json.loads(meta[STATUS_KEY]) if STATUS_KEY in meta else None
            if status and status.get("complete"):
                if status.get("digest") != digest:
                    raise StaleTableError(
                        f"{ident} is complete for a different subset or map; "
                        "drop it and copy again"
                    )
                self.skipped_rows = int(status.get("rows", 0))
                return "skip"
            # An incomplete table may hold rows of an earlier attempt (its data
            # committed, its status did not), possibly for another subset: start over.
            self._project.drop_table(ident)
            t = self._project.create_table(ident, key_schema=keys)
            t.set_metadata({MARKER_KEY: self._marker})
            action = "rewrite"
        else:
            t = self._project.create_table(ident, key_schema=keys)
            t.set_metadata({MARKER_KEY: self._marker})
            action = "replace" if replaced else "create"
        self._table = t
        self._digest = digest
        self._txn = t.txn()
        return action

    def write(self, batch: pa.RecordBatch) -> None:
        """Write ``batch`` inside the open transaction."""
        self._txn.write(batch)

    def commit(self, rows: int) -> None:
        """Commit the transaction and record the table as complete."""
        self._txn.commit()
        status = {"complete": True, "rows": rows, "digest": self._digest}
        self._table.set_metadata({STATUS_KEY: json.dumps(status).encode()})
        self._txn = None

    def abort(self) -> None:
        """Abort the open transaction, if any."""
        if self._txn is not None:
            self._txn.abort()
            self._txn = None


# --------------------------------------------------------------------- the pipeline


@dataclasses.dataclass
class Report:
    """The outcome of ``plan``, ``copy`` or ``verify``."""

    tables: list[TableResult]
    kept_trials: list[int]
    pending: dict[str, list[str]]
    problems: list[str]
    accepted: list[str] = dataclasses.field(default_factory=list)
    notes: list[str] = dataclasses.field(default_factory=list)

    def render(self) -> str:
        """Return a human-readable summary."""
        lines = [f"{'table':40s} {'rows':>10s} {'MB':>9s}  action"]
        for t in self.tables:
            lines.append(
                f"{t.name:40s} {t.rows:>10,d} {t.nbytes / 1e6:>9.1f}  {t.action}"
            )
        total = sum(t.nbytes for t in self.tables)
        lines.append(
            f"{'total':40s} {sum(t.rows for t in self.tables):>10,d} {total / 1e6:>9.1f}"
        )
        lines.append(f"kept trials: {self.kept_trials}")
        if self.pending:
            lines.append("pending decisions (source nullability kept in this dry run):")
            lines.extend(f"  {t}: {cols}" for t, cols in self.pending.items())
        if self.accepted:
            lines.append("accepted exceptions (recorded in the map):")
            lines.extend(f"  {a}" for a in self.accepted)
        if self.notes:
            lines.append("notes:")
            lines.extend(f"  {n}" for n in self.notes)
        if self.problems:
            lines.append("problems:")
            lines.extend(f"  {p}" for p in self.problems)
        return "\n".join(lines)


def _run(
    client: Any,
    source: Any,
    nmap: NullabilityMap,
    subset: Subset,
    sink: DryRunSink | SpiralSink,
    *,
    allow_pending: bool,
    on_table: Callable[[TableResult], None] | None = None,
    only: Iterable[str] | None = None,
) -> Report:
    """Read every selected source table, check it, and hand it to ``sink``.

    ``only`` restricts the run to those tables; foreign keys are then left to
    ``verify``, which sees every table.
    """
    missing = sorted(set(subset.tables) - set(nmap.tables))
    if missing:
        raise ToolError(f"tables in the subset but not in the map: {missing}")
    integrity = Integrity()
    trials = (
        kept_trials(client, source, subset)
        if "trials" in subset.tables.values()
        else []
    )
    results = []
    selected = set(subset.tables) if only is None else set(only)
    for name in table_order(subset):
        if name not in selected:
            continue
        mode = subset.tables[name]
        spec = nmap.tables[name]
        src_table = source.table(_qualified(subset, name))
        src_schema = src_table.schema().to_arrow()
        result = TableResult(name)
        try:
            src_keys = list(src_table.key_schema.to_arrow().names)
            if src_keys != list(spec.keys):
                raise SchemaMismatchError(
                    f"source key columns {src_keys} differ from the map's "
                    f"{list(spec.keys)}"
                )
            schema = target_schema(src_schema, spec, allow_pending=allow_pending)
            keys = key_schema(schema, spec.keys)
            result.action = sink.open_table(
                name, schema, keys, table_digest(nmap, subset, name)
            )
            if result.action == "skip":
                result.rows = sink.skipped_rows
                results.append(result)
                continue
            try:
                for batch in read_selection(client, source, subset, name, mode, trials):
                    out = conform_batch(batch, schema)
                    integrity.observe(nmap, name, out)
                    sink.write(out)
                    result.rows += out.num_rows
                    result.nbytes += out.nbytes
                if not result.rows:
                    raise ToolError(
                        "the selection holds no rows; an empty table means the subset "
                        "is wrong"
                    )
            except BaseException:
                sink.abort()
                raise
            sink.commit(result.rows)
        except ToolError as err:
            err.add_note(f"while processing table {name!r}")
            raise
        results.append(result)
        if on_table is not None:
            on_table(result)
    complete = only is None and not any(r.action == "skip" for r in results)
    problems = integrity.problems(subset.tables) if complete else []
    accepted = integrity.accepted(subset.tables) if complete else []
    return Report(results, trials, nmap.pending(), problems, accepted)


def plan(
    client: Any, source_project: str, nmap: NullabilityMap, subset: Subset
) -> Report:
    """Run the whole copy against the source without writing anything.

    Pending nullability decisions keep the source's nullability and are listed in
    the report.
    """
    source = client.project(source_project)
    return _run(client, source, nmap, subset, DryRunSink(), allow_pending=True)


# ------------------------------------------------------------------ target guards


def check_target_name(target: str | None, allow_writes: str | None, source: str) -> str:
    """Return ``target`` if the names alone allow writing to it.

    Raises:
        TargetRefusedError: if no target is given, ``allow_writes`` differs from it,
            it equals the source, or it starts with a forbidden prefix.
    """
    if not target:
        raise TargetRefusedError("--target-project is required")
    if allow_writes != target:
        raise TargetRefusedError(
            "--allow-writes must repeat --target-project exactly "
            f"(got {allow_writes!r} for {target!r})"
        )
    if target == source:
        raise TargetRefusedError("the target project equals the source project")
    if target.startswith(FORBIDDEN_TARGET_PREFIXES):
        raise TargetRefusedError(
            f"target {target!r} starts with one of {FORBIDDEN_TARGET_PREFIXES}"
        )
    return target


def read_marker(project: Any) -> dict[str, Any]:
    """Return the build marker of a project this tool created.

    Raises:
        TargetRefusedError: if the project has no marker table, or holds a table
            without the same marker.
    """
    tables = {f"{r.dataset}.{r.table}" for r in project.list_tables()}
    if MARKER_TABLE not in tables:
        raise TargetRefusedError(
            f"project has no {MARKER_TABLE} table: it was not made by this tool's create step"
        )
    raw = project.table(MARKER_TABLE).get_metadata().get(MARKER_KEY)
    if raw is None:
        raise TargetRefusedError(f"{MARKER_TABLE} carries no {MARKER_KEY} metadata")
    foreign = sorted(
        ident
        for ident in tables - {MARKER_TABLE}
        if project.table(ident).get_metadata().get(MARKER_KEY) != raw
    )
    if foreign:
        raise TargetRefusedError(
            f"project holds tables this build did not write: {foreign}"
        )
    return json.loads(raw)


def open_target(
    client: Any, target: str, allow_writes: str | None, source: str
) -> tuple[Any, dict[str, Any]]:
    """Return the target ``Project`` and its marker after every guard has passed."""
    check_target_name(target, allow_writes, source)
    project = client.project(target)
    return project, read_marker(project)


def create_project(
    client: Any,
    *,
    id_prefix: str,
    allow_create: str | None,
    description: str,
    volume_id: str | None,
    source_project: str,
) -> str:
    """Create the target project and its marker table; return the new project ID.

    Raises:
        TargetRefusedError: if ``allow_create`` differs from ``id_prefix``, the prefix
            is forbidden, or the new project is not empty.
    """
    if allow_create != id_prefix:
        raise TargetRefusedError("--allow-create must repeat --id-prefix exactly")
    if id_prefix.startswith(FORBIDDEN_TARGET_PREFIXES) or id_prefix == source_project:
        raise TargetRefusedError(f"id prefix {id_prefix!r} is forbidden")
    before = {p.id for p in client.list_projects()}
    created = client.create_project(
        id_prefix=id_prefix, description=description, volume_id=volume_id
    )
    if created.id in before:
        raise TargetRefusedError(f"project {created.id} existed before create")
    project = client.project(created.id)
    if list(project.list_tables()):
        raise TargetRefusedError(f"new project {created.id} is not empty")
    marker = {
        "build_id": uuid.uuid4().hex,
        "tool": TOOL_NAME,
        "source_project": source_project,
        "project_id": created.id,
    }
    t = project.create_table(
        MARKER_TABLE,
        key_schema=pa.schema([pa.field("build_id", pa.large_string(), False)]),
    )
    t.set_metadata({MARKER_KEY: json.dumps(marker, sort_keys=True).encode()})
    return created.id


def copy(
    client: Any,
    *,
    source_project: str,
    target_project: str | None,
    allow_writes: str | None,
    nmap: NullabilityMap,
    subset: Subset,
    dry_run: bool = False,
    on_table: Callable[[TableResult], None] | None = None,
    tables: Sequence[str] | None = None,
    replace: bool = False,
) -> Report:
    """Copy the subset from the source into the target project.

    With ``dry_run`` the target guards still run, but nothing is written. ``tables``
    restricts the copy to those tables; with ``replace`` each of them is dropped (it
    must carry this build's marker) and copied again from the source, which picks up
    a change made in the source after the first copy.

    Raises:
        TargetRefusedError: if the target fails a guard.
        PendingDecisionError: if the map holds pending decisions.
        NullInRequiredColumnError: if a selected row has a null where the map
            forbids one (that table's transaction is aborted).
    """
    project, marker = open_target(
        client, target_project or "", allow_writes, source_project
    )
    if marker.get("source_project") != source_project:
        raise TargetRefusedError(
            f"the target was created for source {marker.get('source_project')!r}"
        )
    pending = nmap.pending()
    if pending:
        raise PendingDecisionError(
            f"decide these in the nullability map first: {pending}"
        )
    if replace and not tables:
        raise ToolError("--replace needs --tables naming the tables to copy again")
    unknown = sorted(set(tables or ()) - set(subset.tables))
    if unknown:
        raise ToolError(f"tables not in the subset: {unknown}")
    replacing = tuple(tables or ()) if replace else ()
    source = client.project(source_project)
    sink: DryRunSink | SpiralSink = (
        DryRunSink(replacing)
        if dry_run
        else SpiralSink(project, subset, marker, replacing)
    )
    report = _run(
        client,
        source,
        nmap,
        subset,
        sink,
        allow_pending=False,
        on_table=on_table,
        only=tables,
    )
    record = provenance(client, source_project, nmap, subset, report)
    existing = project.table(MARKER_TABLE).get_metadata().get(PROVENANCE_KEY)
    if tables and existing is not None:
        record["rows"] = {**json.loads(existing).get("rows", {}), **record["rows"]}
    encoded = json.dumps(record, sort_keys=True, default=str).encode()
    if len(PROVENANCE_KEY) + len(encoded) > METADATA_LIMIT:
        raise ToolError(f"provenance is {len(encoded)} bytes, over SpiralDB's 8 KiB")
    marker_table = project.table(MARKER_TABLE)
    if (
        not dry_run
        and not report.problems
        and marker_table.get_metadata().get(PROVENANCE_KEY) != encoded
    ):
        marker_table.set_metadata({PROVENANCE_KEY: encoded})
    return report


def provenance(
    client: Any,
    source_project: str,
    nmap: NullabilityMap,
    subset: Subset,
    report: Report,
) -> dict[str, Any]:
    """Return the provenance record ``copy`` stores on the marker table.

    It names the source project (with its description), the specification version,
    the window, units and trials, the row count of every table, and the source's
    ``sample_info`` row, which is not copied because it describes the whole sample
    (its ``sources_json`` is replaced by a SHA-256 digest to stay under 8 KiB).
    """
    descriptions = {p.id: p.description for p in client.list_projects()}
    record: dict[str, Any] = {
        "source_project": source_project,
        "source_description": descriptions.get(source_project),
        "spec_version": nmap.spec_version,
        "window_us": list(subset.window),
        "unit_ids": list(subset.unit_ids),
        "kept_trials": report.kept_trials,
        "rows": {t.name: t.rows for t in report.tables},
        "decisions": nmap.raw.get("decisions", {}).get("decided_by"),
    }
    source = client.project(source_project)
    names = {r.table for r in source.list_tables() if r.dataset == subset.dataset}
    if "sample_info" in names:
        rows = client.scan(source.table(_qualified(subset, "sample_info"))).to_table()
        info = []
        for row in rows.to_pylist():
            blob = row.pop("sources_json", None)
            if blob is not None:
                row["sources_json_sha256"] = hashlib.sha256(blob.encode()).hexdigest()
            info.append(row)
        record["source_sample_info"] = info
    return record


# --------------------------------------------------------------------------- verify


def _rechunk(batches: Iterable[pa.RecordBatch], rows: int) -> Iterator[pa.Table]:
    """Yield tables of exactly ``rows`` rows (the last may be shorter)."""
    pending: list[pa.RecordBatch] = []
    count = 0
    for b in batches:
        pending.append(b)
        count += b.num_rows
        while count >= rows:
            t = pa.Table.from_batches(pending)
            yield t.slice(0, rows)
            rest = t.slice(rows)
            pending = rest.to_batches()
            count = rest.num_rows
    if count:
        yield pa.Table.from_batches(pending)


def verify(
    client: Any,
    *,
    source_project: str,
    target_project: str,
    nmap: NullabilityMap,
    subset: Subset,
    read_record: Callable[[Mapping[str, bytes]], Any] | None = None,
) -> Report:
    """Check the target project against the source, the map and the subset.

    With ``read_record`` (Orcapod's ``read_nullability_record``, ITL-936), each
    table's nullability record must list exactly the non-nullable paths the map
    declares; a table without a record is listed in the report's notes.

    Checks: the project holds exactly the subset's tables (plus the marker table);
    each table's schema has the source's types exactly and the map's nullability, its
    key columns are the source's, in order, and non-null; required columns hold no
    nulls; every foreign key resolves; and every row equals the source selection.

    Raises:
        VerificationError: listing every problem found.
    """
    target = client.project(target_project)
    read_marker(target)
    stored = target.table(MARKER_TABLE).get_metadata().get(PROVENANCE_KEY)
    source = client.project(source_project)
    problems: list[str] = []
    if stored is None:
        problems.append(f"{MARKER_TABLE} carries no {PROVENANCE_KEY} metadata")
    else:
        record = json.loads(stored)
        if (
            record.get("source_project") != source_project
            or record.get("window_us") != list(subset.window)
            or record.get("unit_ids") != list(subset.unit_ids)
        ):
            problems.append("the provenance record does not match the subset")
    present = {r.table for r in target.list_tables() if r.dataset == subset.dataset}
    if present != set(subset.tables):
        problems.append(
            f"tables differ: missing {sorted(set(subset.tables) - present)}, "
            f"unexpected {sorted(present - set(subset.tables))}"
        )
    trials = (
        kept_trials(client, source, subset)
        if "trials" in subset.tables.values()
        else []
    )
    integrity = Integrity()
    results = []
    for name in table_order(subset):
        if name not in present:
            continue
        spec = nmap.tables[name]
        t = target.table(_qualified(subset, name))
        src_schema = source.table(_qualified(subset, name)).schema().to_arrow()
        want = target_schema(src_schema, spec, allow_pending=True)
        got = t.schema().to_arrow()
        if got != stored_schema(want):
            problems.append(
                f"{name}: schema {got} differs from expected {stored_schema(want)}"
            )
        got_keys = t.key_schema.to_arrow()
        src_keys = source.table(_qualified(subset, name)).key_schema.to_arrow().names
        if got_keys.names != list(spec.keys) or got_keys.names != src_keys:
            problems.append(
                f"{name}: key columns {got_keys.names}; the map's {list(spec.keys)}, "
                f"the source's {src_keys}"
            )
        mode = subset.tables[name]
        expected = (
            conform_batch(b, want)
            for b in read_selection(client, source, subset, name, mode, trials)
        )
        actual_batches: list[int] = [0]

        def observed(
            batches: Iterable[pa.RecordBatch], table: str = name
        ) -> Iterator[pa.RecordBatch]:
            """Yield ``batches`` unchanged, counting rows and recording their keys."""
            for b in batches:
                actual_batches[0] += b.num_rows
                integrity.observe(nmap, table, b)
                yield b

        actual = observed(client.scan(t).to_record_batches())
        result = TableResult(name, action="verified")
        mismatch = _compare_streams(expected, actual, want)
        result.rows = actual_batches[0]
        if mismatch:
            problems.append(f"{name}: {mismatch}")
        results.append(result)
    problems.extend(integrity.problems(present))
    notes: list[str] = []
    if read_record is not None:
        for name in sorted(present & set(subset.tables)):
            ident = _qualified(subset, name)
            src_schema = source.table(ident).schema().to_arrow()
            want = set(non_null_paths(target_schema(src_schema, nmap.tables[name])))
            record = read_record(target.table(ident).get_metadata())
            if record is None:
                notes.append(f"{name}: no nullability record")
            elif set(record) != want:
                problems.append(
                    f"{name}: nullability record differs from the map: "
                    f"missing {sorted(want - set(record))}, "
                    f"extra {sorted(set(record) - want)}"
                )
    report = Report(
        results, trials, nmap.pending(), problems, integrity.accepted(present), notes
    )
    if problems:
        raise VerificationError(problems)
    return report


def load_record_reader() -> Callable[[Mapping[str, bytes]], Any] | None:
    """Return Orcapod's ``read_nullability_record`` (ITL-936), or ``None`` without it."""
    import orcapod.databases

    return getattr(orcapod.databases, "read_nullability_record", None)


_FLOAT_BITS = {16: np.uint16, 32: np.uint32, 64: np.uint64}


def _float_bits(arr: pa.Array[Any]) -> Any:
    """Return the raw bit patterns of a float array's slots, as unsigned integers."""
    dtype = _FLOAT_BITS[arr.type.bit_width]
    data = arr.buffers()[1]
    assert data is not None
    # to_pybytes copies; compared slices hold at most COMPARE_SLICE_ROWS rows
    return np.frombuffer(data.to_pybytes(), dtype=dtype)[
        arr.offset : arr.offset + len(arr)
    ]


def identical(a: pa.Array[Any], b: pa.Array[Any]) -> bool:
    """Return whether two arrays hold the same values, comparing floats bit for bit.

    Arrow's ``equals`` treats NaN as unequal to itself, so a column holding NaN
    (gaze positions during blinks, some unit quality metrics) never equals its own
    copy. Here a float slot equals another when their bit patterns are identical: NaN
    equals NaN with the same payload, and -0.0 differs from 0.0. Bit-exact is the
    right test for a copy, which must not change a single value. Struct fields and
    list items are compared the same way, recursively; other types use ``equals``.
    """
    if a.type != b.type or len(a) != len(b):
        return False
    t = a.type
    if not (pa.types.is_floating(t) or pa.types.is_struct(t) or _is_list(t)):
        # Arrow's equals treats -0.0 as 0.0 and NaN as unequal to itself, so only
        # types that cannot hold a float take it.
        return a.equals(b)
    if a.null_count != b.null_count or not a.is_null().equals(b.is_null()):
        return False
    if pa.types.is_floating(t):
        valid = np.asarray(a.is_valid().to_numpy(zero_copy_only=False), dtype=bool)
        return bool(np.array_equal(_float_bits(a)[valid], _float_bits(b)[valid]))
    if pa.types.is_struct(t):
        sa, sb = a, b
        assert isinstance(sa, pa.StructArray) and isinstance(sb, pa.StructArray)
        return all(
            identical(x, y) for x, y in zip(sa.flatten(), sb.flatten(), strict=True)
        )
    if not pc.list_value_length(a).equals(pc.list_value_length(b)):
        return False
    return identical(pc.list_flatten(a), pc.list_flatten(b))


def _compare_streams(
    expected: Iterable[pa.RecordBatch],
    actual: Iterable[pa.RecordBatch],
    schema: pa.Schema,
) -> str:
    """Return a description of the first difference, or ``""`` if the rows are equal."""
    exp_iter = _rechunk(expected, COMPARE_SLICE_ROWS)
    act_iter = _rechunk(actual, COMPARE_SLICE_ROWS)
    offset = 0
    while True:
        e = next(exp_iter, None)
        a = next(act_iter, None)
        if e is None and a is None:
            return ""
        if e is None or a is None or e.num_rows != a.num_rows:
            e_rows = (
                offset
                + (e.num_rows if e is not None else 0)
                + sum(t.num_rows for t in exp_iter)
            )
            a_rows = (
                offset
                + (a.num_rows if a is not None else 0)
                + sum(t.num_rows for t in act_iter)
            )
            return f"row count {a_rows} differs from expected {e_rows}"
        if a.schema.names != schema.names:
            return f"columns {a.schema.names} differ from {schema.names}"
        for i, name in enumerate(schema.names):
            want_col = e.column(i)
            got_col = a.column(i)
            try:
                for chunk in got_col.chunks:
                    _conform_array(chunk, schema.field(i))
            except NullInRequiredColumnError as err:
                return str(err)
            except SchemaMismatchError as err:
                return str(err)
            if want_col.type != got_col.type:
                want_col = want_col.cast(got_col.type)
            if not identical(got_col.combine_chunks(), want_col.combine_chunks()):
                return f"column {name!r} differs from the source in rows {offset}..{offset + e.num_rows - 1}"
        offset += e.num_rows


# ---------------------------------------------------------------------- annotate


RecordWriter = Callable[[Any, pa.Schema], bool]


def load_record_writer() -> RecordWriter:
    """Return Orcapod's ``write_nullability_record`` (ITL-936).

    Raises:
        ToolError: if the installed Orcapod does not have it.
    """
    import orcapod.databases

    writer = getattr(orcapod.databases, "write_nullability_record", None)
    if writer is None:
        raise ToolError(
            "this Orcapod has no orcapod.databases.write_nullability_record; annotate "
            "needs Orcapod with ITL-936 (the SpiralDB nullability record)"
        )
    return writer


def annotate(
    client: Any,
    *,
    source_project: str,
    target_project: str,
    allow_writes: str | None,
    nmap: NullabilityMap,
    subset: Subset,
    write_record: RecordWriter | None,
    dry_run: bool = False,
) -> list[str]:
    """Give each copied table Orcapod's nullability record (ITL-936).

    The record is written by ``write_record`` (Orcapod's ``write_nullability_record``)
    from the exact schema ``copy`` wrote the table with: the source's types and the
    map's nullability. The record's format is Orcapod's; this tool does not encode it.
    Before recording a table, its data in the target is read and every column the
    schema declares non-nullable is checked for nulls, since the helper reads no data.
    Only table metadata is written, never data; a table whose record is current is
    left alone.

    Args:
        client: The Spiral client.
        source_project: The source project (read for each table's schema).
        target_project: The project to annotate (a project ``create`` made).
        allow_writes: Must repeat ``target_project``.
        nmap: The nullability map.
        subset: The subset definition (which tables, which dataset).
        write_record: Writes the record; returns whether it changed anything. Not
            called (and may be ``None``) with ``dry_run``.
        dry_run: Check the data and list each table's non-nullable paths, but write
            nothing.

    Returns:
        One line per table: ``recorded``, ``unchanged``, or with ``dry_run`` the
        paths that would be recorded.

    Raises:
        TargetRefusedError: if the target fails a guard.
        ToolError: if a subset table is missing from the target.
        NullInRequiredColumnError: if a table holds a null where the map forbids one
            (nothing is recorded for it).
    """
    project, _ = open_target(client, target_project, allow_writes, source_project)
    source = client.project(source_project)
    present = {f"{r.dataset}.{r.table}" for r in project.list_tables()}
    lines = []
    for name in sorted(subset.tables):
        ident = _qualified(subset, name)
        if ident not in present:
            raise ToolError(f"{ident} is not in the target; copy it first")
        src_schema = source.table(ident).schema().to_arrow()
        declared = target_schema(src_schema, nmap.tables[name])
        table = project.table(ident)
        try:
            for batch in client.scan(table).to_record_batches():
                conform_batch(batch.select(declared.names), declared)
        except ToolError as err:
            err.add_note(f"while checking table {name!r} before recording it")
            raise
        if dry_run or write_record is None:
            paths = ["/".join(p) for p in non_null_paths(declared)]
            lines.append(f"{name}: would record {len(paths)} non-null path(s): {paths}")
            continue
        changed = write_record(table, declared)
        lines.append(f"{name}: {'recorded' if changed else 'unchanged'}")
    return lines


# ---------------------------------------------------------------- the Orcapod check


def _declared_nullable(spec: TableSpec, column: str) -> bool | None:
    """Return the nullability ``spec`` declares for ``column`` (``None`` if unknown)."""
    if column in spec.columns:
        return spec.columns[column].nullable
    if column in spec.groups:
        return spec.groups[column].nullable
    return None


def orcapod_check(
    target_project: str,
    nmap: NullabilityMap,
    subset: Subset,
    make_source: Callable[[str, str, str], Any],
) -> tuple[list[str], list[str]]:
    """Load every target table as an Orcapod source and check its column types.

    A column declared non-nullable must have a plain Python type ``T``; a nullable
    one must be ``T | None``. Tables with a ``fixed_size_list`` column are skipped:
    Orcapod cannot hash that type until starfix supports it (ITL-927).

    Args:
        target_project: The project to read.
        nmap: The nullability map.
        subset: The subset definition (which tables, which dataset).
        make_source: Builds a source from ``(project_id, table, dataset)``; the CLI
            passes ``SpiralDBTableSource`` (eager).

    Returns:
        One line per table describing the outcome, and the list of problems.
    """
    lines: list[str] = []
    problems: list[str] = []
    for name in sorted(subset.tables):
        spec = nmap.tables[name]
        types = [c.arrow_type for c in spec.columns.values()]
        if any(t.startswith("fixed_size_list") for t in types):
            lines.append(f"{name}: skipped (fixed_size_list, ITL-927)")
            continue
        source = make_source(target_project, name, subset.dataset)
        tag_schema, data_schema = source.output_schema()
        wrong = []
        for column, py_type in {**tag_schema, **data_schema}.items():
            optional = type(None) in typing.get_args(py_type)
            declared = _declared_nullable(spec, column)
            if declared is not None and optional != declared:
                wrong.append(f"{column}: {py_type}")
        if wrong:
            problems.append(f"{name}: nullability differs from the map: {wrong}")
        lines.append(
            f"{name}: {len(tag_schema)} tag + {len(data_schema)} data columns"
            + (f", WRONG {wrong}" if wrong else ", nullability as the map")
        )
    return lines, problems


# ------------------------------------------------------------------------ the probe


def _probe_batch(
    k: int, v: int | None, x: int | None, *, v_nullable: bool, x_nullable: bool
) -> pa.RecordBatch:
    """Return one probe row: key ``k``, top-level ``v`` and nested ``g.x``."""
    group = pa.struct(
        [pa.field("x", pa.int32(), x_nullable), pa.field("y", pa.int32(), True)]
    )
    items = pa.list_(pa.field("item", pa.float32(), False), 2)
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), False),
            pa.field("v", pa.int32(), v_nullable),
            pa.field("w", pa.int32(), True),
            pa.field("g", group, False),
            pa.field("l", items, False),
        ]
    )
    g = pa.StructArray.from_arrays(
        [pa.array([x], pa.int32()), pa.array([None], pa.int32())],
        fields=list(group),
    )
    return pa.RecordBatch.from_arrays(
        [
            pa.array([k], pa.int64()),
            pa.array([v], pa.int32()),
            pa.array([None], pa.int32()),
            g,
            # pyarrow-stubs types `type` as None; pyarrow accepts a FixedSizeListType
            pa.FixedSizeListArray.from_arrays(
                pa.array([1.0, 2.0], pa.float32()),
                type=items,  # pyright: ignore[reportArgumentType]
            ),
        ],
        schema=schema,
    )


def probe_nullability(
    client: Any, *, target_project: str, allow_writes: str | None, source_project: str
) -> dict[str, Any]:
    """Record how SpiralDB treats non-nullable columns, in a scratch table.

    Writes to ``meta.nullability_probe`` in the target (a project ``create`` made) and
    drops it afterwards. The table has a non-null key ``k``, a non-null top-level
    column ``v``, a nullable ``w``, a struct column ``g`` with a non-null field ``x``,
    and a ``fixed_size_list`` ``l`` with non-null items. Reported: the schema SpiralDB
    keeps (its own repr, where ``?`` marks nullable, and the Arrow form), and what each
    write of a null does, declared non-nullable and declared nullable, at the top
    level and in the struct.
    """
    project, _ = open_target(client, target_project, allow_writes, source_project)
    tables = {f"{r.dataset}.{r.table}" for r in project.list_tables()}
    if PROBE_TABLE in tables:
        project.drop_table(PROBE_TABLE)
    out: dict[str, Any] = {}
    t = project.create_table(
        PROBE_TABLE, key_schema=pa.schema([pa.field("k", pa.int64(), False)])
    )
    # Marked like every table this build writes, so a probe killed before its
    # cleanup leaves a table the next command accepts and the next probe drops.
    t.set_metadata({MARKER_KEY: project.table(MARKER_TABLE).get_metadata()[MARKER_KEY]})

    def state() -> dict[str, Any]:
        """Return the probe table's Spiral and Arrow schemas, scan schema and rows."""
        return {
            "spiral_schema": repr(t.schema()),
            "arrow_schema": str(t.schema().to_arrow()),
            "scan_schema": str(client.scan(t).to_table().schema),
            "rows": client.scan(t).to_table().to_pylist(),
        }

    try:
        out["key_schema_after_create"] = repr(t.key_schema)
        with t.txn() as txn:
            txn.write(_probe_batch(1, 10, 100, v_nullable=False, x_nullable=False))
        out["after_non_null_write"] = state()
        variants = {
            "null_v_declared_non_null": (2, None, 200, False, False),
            "null_v_declared_nullable": (3, None, 300, True, False),
            "null_x_declared_non_null": (4, 40, None, False, False),
            "null_x_declared_nullable": (5, 50, None, False, True),
        }
        for label, (k, v, x, v_nullable, x_nullable) in variants.items():
            try:
                batch = _probe_batch(
                    k, v, x, v_nullable=v_nullable, x_nullable=x_nullable
                )
                with t.txn() as txn:
                    txn.write(batch)
                out[label] = {"raised": None, **state()}
            except Exception as err:  # noqa: BLE001 - the probe records whatever SpiralDB raises
                out[label] = {"raised": f"{type(err).__name__}: {err}"}
    finally:
        project.drop_table(PROBE_TABLE)
    return out


# ---------------------------------------------------------------- deriving the map

_SPEC_PRIMITIVES = {
    "utf8": pa.string(),
    "u8": pa.uint8(),
    "u16": pa.uint16(),
    "u32": pa.uint32(),
    "u64": pa.uint64(),
    "i16": pa.int16(),
    "i32": pa.int32(),
    "i64": pa.int64(),
    "f32": pa.float32(),
    "f64": pa.float64(),
    "bool": pa.bool_(),
    "datetime": pa.timestamp("us"),
    "date": pa.date32(),
}


def spec_arrow_type(spec_type: str) -> tuple[pa.DataType, bool]:
    """Return the Arrow type and nullability a specification type denotes.

    A trailing ``?`` marks a nullable value, at any nesting level.

    Raises:
        ToolError: for a type this tool does not know.
    """
    s = spec_type.strip().strip("`")
    nullable = s.endswith("?")
    if nullable:
        s = s[:-1]
    if s in _SPEC_PRIMITIVES:
        return _SPEC_PRIMITIVES[s], nullable
    if m := re.fullmatch(r"fixed_size_list\((.*)\)\[(\d+)\]", s):
        inner, inner_nullable = spec_arrow_type(m.group(1))
        return pa.list_(
            pa.field("item", inner, inner_nullable), int(m.group(2))
        ), nullable
    if m := re.fullmatch(r"list\((.*)\)", s):
        inner, inner_nullable = spec_arrow_type(m.group(1))
        return pa.list_(pa.field("item", inner, inner_nullable)), nullable
    raise ToolError(f"unknown specification type {spec_type!r}")


def parse_spec_tables(text: str) -> dict[str, list[dict[str, Any]]]:
    """Return the column tables of ``edi_schema_spec.md`` by table name.

    Each column is ``{"column", "type", "pk", "fk", "fk_parents", "group"}``; a
    ``**... (`name` column group)**`` row starts a column group.
    """
    tables: dict[str, list[dict[str, Any]]] = {}
    current: str | None = None
    group: str | None = None
    for line in text.splitlines():
        if m := re.match(r"^####\s+(\w+)\s*$", line):
            current, group = m.group(1), None
            tables[m.group(1)] = []
            continue
        if current is None or not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3 or cells[0] == "Column" or set(cells[0]) <= set("-: "):
            continue
        if m := re.search(r"`(\w+)` column group", cells[0]):
            group = m.group(1)
            continue
        desc = cells[3] if len(cells) > 3 else ""
        tables[current].append(
            {
                "column": cells[0].strip("`"),
                "type": cells[1],
                "pk": "PK" in cells[2],
                "fk": "FK" in cells[2],
                "fk_parents": re.findall(r"FK →\s*`(\w+)", desc),
                "group": group,
            }
        )
    return tables


# References the specification does not declare as foreign keys, checked because the
# subset keeps whole trials and units. Each is ``child: [(parent, note)]``.
EXTRA_REFERENCES: dict[str, list[tuple[str, str]]] = {
    "experiment_freeviewing_event": [
        ("experiment_freeviewing_trial", "trial_index; not an FK in the spec")
    ],
    "experiment_screen_capture_event": [
        ("experiment_freeviewing_trial", "trial_index; not an FK in the spec"),
        (
            "experiment",
            "the spec's parent experiment_screen_capture is not in the sample",
        ),
    ],
    "experiment_screen_capture_frame_270p": [("experiment", "table not in the spec")],
    "sample_unit": [("unit", "table not in the spec")],
}


def apply_decisions(
    raw: Mapping[str, Any], decisions: Mapping[str, Any]
) -> dict[str, Any]:
    """Return a copy of the map's JSON form with the developer's decisions applied.

    ``decisions`` holds:

    * ``tables_not_in_spec``: ``{"keys": "required" | "nullable", "values": ...}``,
      applied to every pending column of a table the specification does not define;
    * ``column_group_structs``: ``"required"`` or ``"nullable"``, for every pending
      column group struct;
    * ``accepted_unresolved``: ``[{"child", "parent", "keys", "evidence"}]``, key
      tuples known not to resolve in the source.

    Raises:
        ToolError: if an accepted reference names a foreign key the map lacks, or a
            value is neither ``"required"`` nor ``"nullable"``.
    """
    out = json.loads(json.dumps(raw))

    def nullable(choice: str) -> bool:
        """Return whether ``choice`` means nullable.

        Raises:
            ToolError: if ``choice`` is neither ``"required"`` nor ``"nullable"``.
        """
        if choice not in ("required", "nullable"):
            raise ToolError(f"expected 'required' or 'nullable', got {choice!r}")
        return choice == "nullable"

    outside = decisions.get("tables_not_in_spec")
    groups = decisions.get("column_group_structs")
    for table in out["tables"].values():
        if outside is not None and not table["in_spec"]:
            for c in table["columns"].values():
                if c["nullable"] is None:
                    c["nullable"] = nullable(outside["keys" if c["key"] else "values"])
        if groups is not None:
            for g in table["groups"].values():
                if g["nullable"] is None:
                    g["nullable"] = nullable(groups)
    for item in decisions.get("accepted_unresolved", []):
        fks = out["tables"][item["child"]]["foreign_keys"]
        match = [fk for fk in fks if fk["parent"] == item["parent"]]
        if not match:
            raise ToolError(f"{item['child']} has no foreign key to {item['parent']}")
        match[0]["accepted_unresolved"] = item["keys"]
    out["decisions"] = dict(decisions)
    return out


def derive_map(
    spec_text: str,
    spec_version: str,
    source_project: str,
    source_schemas: Mapping[str, tuple[pa.Schema, Sequence[str]]],
    decisions: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the JSON form of the nullability map.

    Args:
        spec_text: The text of ``edi_schema_spec.md``.
        spec_version: The specification's version, recorded in the map.
        source_project: The source project's ID, recorded in the map.
        source_schemas: Per table, its Arrow schema and key columns as SpiralDB
            reports them.

    Returns:
        The map. A column the specification defines gets the specification's
        nullability; a column or table it does not define gets ``nullable: null``
        (pending), as does each column group struct (the specification types only
        the group's fields). Specification types that disagree with the source's
        are listed under ``mismatches``. ``decisions``, if given, resolves pending
        entries (see ``apply_decisions``).
    """
    spec = parse_spec_tables(spec_text)
    out: dict[str, Any] = {
        "spec_version": spec_version,
        "source_project": source_project,
        "mismatches": [],
        "absent_from_sample": sorted(t for t in spec if t not in source_schemas),
        "tables": {},
    }
    for table, (schema, keys) in sorted(source_schemas.items()):
        rows = {r["column"]: r for r in spec.get(table, [])}
        in_spec = table in spec
        entry: dict[str, Any] = {
            "in_spec": in_spec,
            "keys": list(keys),
            "columns": {},
            "groups": {},
        }

        def column(
            field: pa.Field[Any], row: dict[str, Any] | None, *, is_key: bool
        ) -> dict[str, Any]:
            """Return the map entry for ``field``, recording any spec difference."""
            if row is None:
                if in_spec:
                    out["mismatches"].append(f"{table}.{field.name}: not in the spec")
                return {
                    "arrow_type": str(field.type),
                    "nullable": None,
                    "spec_type": None,
                    "key": is_key,
                }
            want, nullable = spec_arrow_type(row["type"])
            if want != field.type:
                out["mismatches"].append(
                    f"{table}.{field.name}: spec {row['type']} is {want}, source is {field.type}"
                )
            if row["pk"] != is_key:
                out["mismatches"].append(
                    f"{table}.{field.name}: spec key={row['pk']}, source key={is_key}"
                )
            return {
                "arrow_type": str(field.type),
                "nullable": nullable,
                "spec_type": row["type"],
                "key": is_key,
            }

        for field in schema:
            if pa.types.is_struct(field.type) and field.name not in rows:
                entry["groups"][field.name] = {
                    "arrow_type": str(field.type),
                    "nullable": None,
                    "fields": {
                        child.name: column(child, rows.get(child.name), is_key=False)
                        for child in field.type
                    },
                }
            else:
                entry["columns"][field.name] = column(
                    field, rows.get(field.name), is_key=field.name in keys
                )
        if in_spec:
            spec_keys = [r["column"] for r in spec[table] if r["pk"]]
            if spec_keys != list(keys):
                out["mismatches"].append(
                    f"{table}: spec key {spec_keys}, source key {list(keys)}"
                )
            listed = set(entry["columns"]) | {
                f for g in entry["groups"].values() for f in g["fields"]
            }
            for name in rows:
                if name not in listed:
                    out["mismatches"].append(
                        f"{table}.{name}: in the spec, not in the source"
                    )
        parents: list[tuple[str, str]] = []
        for r in spec.get(table, []):
            parents.extend((p, "") for p in r["fk_parents"] if p != table)
        parents.extend(EXTRA_REFERENCES.get(table, []))
        fks = []
        seen: set[str] = set()
        for parent, note in parents:
            if parent in seen:
                continue
            seen.add(parent)
            if parent not in source_schemas:
                fks.append(
                    {
                        "parent": parent,
                        "columns": [],
                        "note": "parent table not in the sample; not checked",
                    }
                )
                continue
            pkeys = list(source_schemas[parent][1])
            if not set(pkeys) <= set(schema.names):
                out["mismatches"].append(
                    f"{table} -> {parent}: child lacks parent key columns {sorted(set(pkeys) - set(schema.names))}"
                )
                continue
            fks.append({"parent": parent, "columns": pkeys, "note": note})
        entry["foreign_keys"] = [fk for fk in fks if fk["columns"]]
        entry["unchecked_references"] = [fk for fk in fks if not fk["columns"]]
        out["tables"][table] = entry
    return apply_decisions(out, decisions) if decisions is not None else out


def render_map(nmap: NullabilityMap) -> str:
    """Return the map as Markdown tables, one row per column."""
    lines = []
    for name, t in sorted(nmap.tables.items()):
        lines.append(
            f"### `{name}`{'' if t.in_spec else ' (not in the specification)'}\n"
        )
        lines.append("| Column | Arrow type | Spec type | Key | Declared |")
        lines.append("| --- | --- | --- | --- | --- |")

        def row(col: str, c: ColumnSpec) -> str:
            """Return the Markdown table row for column ``col``."""
            declared = (
                "pending"
                if c.nullable is None
                else ("nullable" if c.nullable else "**required**")
            )
            return f"| `{col}` | `{c.arrow_type}` | {f'`{c.spec_type}`' if c.spec_type else '—'} | {'key' if c.key else ''} | {declared} |"

        for col, c in t.columns.items():
            lines.append(row(col, c))
        for g, group in t.groups.items():
            declared = (
                "pending"
                if group.nullable is None
                else ("nullable" if group.nullable else "**required**")
            )
            lines.append(f"| `{g}` (column group) | struct | — | | {declared} |")
            for col, c in group.fields.items():
                lines.append(row(f"{g}.{col}", c))
        if t.foreign_keys:
            lines.append("")
            lines.extend(
                f"References `{fk.parent}` on {', '.join(f'`{c}`' for c in fk.columns)}"
                + (f" ({fk.note})" if fk.note else "")
                + "."
                for fk in t.foreign_keys
            )
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------- CLI


def connect() -> Any:
    """Return a ``spiral.Spiral`` client, after checking the pyspiral version.

    Raises:
        ToolError: if pyspiral is missing or older than 0.16.33.
    """
    try:
        from importlib.metadata import version

        import spiral  # pyright: ignore[reportMissingImports]  # optional; see connect()
    except ImportError as err:
        raise ToolError(
            "pyspiral is not installed; run with `uv run --with pyspiral==0.16.33 ...`"
        ) from err
    found = tuple(int(x) for x in re.findall(r"\d+", version("pyspiral"))[:3])
    if found < MIN_PYSPIRAL:
        raise ToolError(
            f"pyspiral {version('pyspiral')} is too old: 0.16.33 or newer uses the "
            "`spiral login` credentials"
        )
    return spiral.Spiral()


def _source_schemas(
    client: Any, project_id: str, dataset: str
) -> dict[str, tuple[pa.Schema, list[str]]]:
    """Return the schema and key column names of each table in ``dataset``.

    Args:
        client: The Spiral client.
        project_id: The project to read.
        dataset: The dataset whose tables are returned.

    Returns:
        Table name (without the dataset) to ``(schema, key column names)``.
    """
    project = client.project(project_id)
    out = {}
    for r in project.list_tables():
        if r.dataset != dataset:
            continue
        t = project.table(f"{dataset}.{r.table}")
        out[r.table] = (t.schema().to_arrow(), list(t.key_schema.to_arrow().names))
    return out


def _parser() -> argparse.ArgumentParser:
    """Build the command-line parser and its subcommands."""
    p = argparse.ArgumentParser(
        prog="edi_tutorial_dataset.py",
        description="Build the workshop Spiral project from the V1 EDI sample.",
    )
    p.add_argument("--map", type=Path, default=MAP_PATH, help="nullability map JSON")
    p.add_argument("--subset", type=Path, default=SUBSET_PATH, help="subset JSON")
    p.add_argument("--source-project", default=DEFAULT_SOURCE)
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("plan", help="read-only dry run of the copy")

    c = sub.add_parser("create", help="create the target project and its marker table")
    c.add_argument("--id-prefix", required=True)
    c.add_argument("--allow-create", required=True, help="repeat --id-prefix")
    c.add_argument("--description", required=True)
    c.add_argument("--volume", default=None, help="Spiral Volume ID for the new tables")

    for name, helptext in (
        ("probe", "check how SpiralDB treats non-nullable columns (scratch table)"),
        ("copy", "copy the subset into the target"),
    ):
        s = sub.add_parser(name, help=helptext)
        s.add_argument("--target-project", required=True)
        s.add_argument("--allow-writes", required=True, help="repeat --target-project")
        if name == "copy":
            s.add_argument("--dry-run", action="store_true")
            s.add_argument(
                "--tables", default=None, help="comma-separated: copy only these tables"
            )
            s.add_argument(
                "--replace",
                action="store_true",
                help="drop the --tables and copy them again from the source",
            )

    a = sub.add_parser(
        "annotate",
        help="give each table Orcapod's nullability record (ITL-936; metadata only)",
    )
    a.add_argument("--target-project", required=True)
    a.add_argument("--allow-writes", required=True, help="repeat --target-project")
    a.add_argument(
        "--dry-run", action="store_true", help="list the paths, write nothing"
    )

    v = sub.add_parser("verify", help="check the target against the source and the map")
    v.add_argument("--target-project", required=True)

    o = sub.add_parser(
        "orcapod-check", help="load each target table as an Orcapod source (read-only)"
    )
    o.add_argument("--target-project", required=True)

    d = sub.add_parser("derive-map", help="build the nullability map from the spec")
    d.add_argument("--spec", type=Path, required=True, help="edi_schema_spec.md")
    d.add_argument("--spec-version", required=True)
    d.add_argument("--output", type=Path, required=True)
    d.add_argument(
        "--decisions",
        type=Path,
        default=DECISIONS_PATH,
        help="the developer's decisions (default: decisions.json)",
    )

    sub.add_parser("render-map", help="print the map as Markdown")
    return p


def main(
    argv: Sequence[str] | None = None, client_factory: Callable[[], Any] = connect
) -> int:
    """Run the command line; return the process exit code."""
    args = _parser().parse_args(argv)
    try:
        if args.command == "render-map":
            print(render_map(load_map(args.map)))
            return 0
        client = client_factory()
        if args.command == "derive-map":
            subset = load_subset(args.subset)
            raw = derive_map(
                args.spec.read_text(encoding="utf-8"),
                args.spec_version,
                args.source_project,
                _source_schemas(client, args.source_project, subset.dataset),
                json.loads(args.decisions.read_text(encoding="utf-8")),
            )
            args.output.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
            print(f"wrote {args.output}; {len(raw['mismatches'])} mismatch(es)")
            return 0
        if args.command == "create":
            pid = create_project(
                client,
                id_prefix=args.id_prefix,
                allow_create=args.allow_create,
                description=args.description,
                volume_id=args.volume,
                source_project=args.source_project,
            )
            print(f"created project {pid}")
            return 0
        if args.command == "probe":
            result = probe_nullability(
                client,
                target_project=args.target_project,
                allow_writes=args.allow_writes,
                source_project=args.source_project,
            )
            print(json.dumps(result, indent=2, default=str))
            return 0
        nmap, subset = load_map(args.map), load_subset(args.subset)
        if args.command == "annotate":
            print(
                "\n".join(
                    annotate(
                        client,
                        source_project=args.source_project,
                        target_project=args.target_project,
                        allow_writes=args.allow_writes,
                        nmap=nmap,
                        subset=subset,
                        write_record=None if args.dry_run else load_record_writer(),
                        dry_run=args.dry_run,
                    )
                )
            )
            return 0
        if args.command == "orcapod-check":
            from orcapod.sources import SpiralDBTableSource

            lines, problems = orcapod_check(
                args.target_project,
                nmap,
                subset,
                lambda project, table, dataset: SpiralDBTableSource(
                    project, table, dataset=dataset
                ),
            )
            print("\n".join(lines))
            if problems:
                print("problems:\n  " + "\n  ".join(problems))
            return 1 if problems else 0
        if args.command == "plan":
            report = plan(client, args.source_project, nmap, subset)
        elif args.command == "copy":
            report = copy(
                client,
                source_project=args.source_project,
                target_project=args.target_project,
                allow_writes=args.allow_writes,
                nmap=nmap,
                subset=subset,
                dry_run=args.dry_run,
                tables=args.tables.split(",") if args.tables else None,
                replace=args.replace,
                on_table=lambda r: print(
                    f"{r.action:12s} {r.name} ({r.rows:,d} rows)", flush=True
                ),
            )
        else:
            report = verify(
                client,
                source_project=args.source_project,
                target_project=args.target_project,
                nmap=nmap,
                subset=subset,
                read_record=load_record_reader(),
            )
        print(report.render())
        return 1 if report.problems else 0
    except ToolError as err:
        print(f"error: {err}", file=sys.stderr)
        for note in getattr(err, "__notes__", []):
            print(f"  {note}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
