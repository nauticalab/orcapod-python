"""ITL-936 live check of the Spiral nullability record.

Run from the repository root after ``spiral login``, with the ``spiraldb`` extra
(``uv sync --all-extras``). Prints a JSON report; exit status 0 when every check passed.

**Write mode** (scratch tables only; repeat the project id to allow writes):

    uv run python superpowers/specs/itl-936-live/live_nullability.py write \
        --project edi-tutorial-v1-841226 --allow-writes edi-tutorial-v1-841226 \
        [--dataset meta] [--report write.json]   # dataset defaults to meta

Creates tables ``<dataset>.itl936_<run>_*`` and drops every one of them at the end, even on
failure (the ITL-933 tool refuses a target holding a table without its marker, so nothing
may be left behind). Checks:

1. ``fresh``: right after ``create_table`` the schema holds only the key columns (the
   connector's test for "a table with no rows yet", where it writes the record).
2. ``record``: ``SpiralDBConnector.upsert_records`` writes the record; SpiralDB's own schema
   is all nullable; ``get_column_info`` and ``iter_batches`` restore the written nullability
   at every depth; ``SpiralDBTableSource`` gives ``T``; a pod ``x: float`` runs; a ``Join`` of
   two recorded tables runs; the saved job loads in a fresh process, with every row and
   equal source hashes.
3. ``refusals``: a null in a non-null struct field and a write declaring other nullability are
   refused, and the table is unchanged.
4. ``hidden_values``: what SpiralDB stores for values under a null parent and for nullable
   fields: a struct written null with ``push_down_nulls=True`` (child value present
   underneath), a null list with items underneath, null items in a nullable list, a null in a
   nullable struct field. Reports the rows and the child arrays as read back.
   ``struct_null_without_push_down``: a null struct written straight through pyspiral without
   ``push_down_nulls`` (refused, or what is stored); ``struct_null_connector``: the same rows
   through ``SpiralDBConnector`` are refused by Orcapod before anything is written.
5. ``metadata``: several keys in one ``set_metadata`` call; a 400-column record (chunked);
   an entry over 8 KiB (expected refused).

**Read mode** (read-only; for the tutorial tables once they carry the record):

    uv run python superpowers/specs/itl-936-live/live_nullability.py read \
        --project edi-tutorial-v1-841226 --tables session,experiment \
        --float-column <a required float column of the second table> [--report read.json]

For each table: the record, the source's ``output_schema``, and that every recorded
top-level column is ``T``; then a ``Join`` of the first two tables, a pod over
``--float-column`` annotated ``x: float``, run twice (no calls the second time), save, and
reload in a fresh process with equal hashes.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, cast

import pyarrow as pa

_REPO = Path(__file__).resolve().parents[3]


def _schema_text(schema: pa.Schema) -> str:
    """Return ``schema`` as text, without its metadata."""
    return schema.to_string(show_schema_metadata=False)


def _fresh_load(
    project: str, dataset: str, table: str, job_path: Path, store: Path
) -> dict[str, Any]:
    """Load the saved job and build ``table``'s source in a new process.

    Returns:
        The loaded ``double`` node's row count, the loaded job's unbound sources, and the
        source's content and pipeline hash, all computed in the new process.
    """
    script = (
        "import json\n"
        "from orcapod import PipelineJob\n"
        "from orcapod.databases import DeltaTableDatabase\n"
        "from orcapod.sources import SpiralDBTableSource\n"
        f"job = PipelineJob.load({str(job_path)!r}, database=DeltaTableDatabase({str(store)!r}))\n"
        "records = job.nodes['double'].get_all_records()\n"
        f"s = SpiralDBTableSource({project!r}, {table!r}, dataset={dataset!r})\n"
        "print(json.dumps({'rows': None if records is None else records.num_rows,\n"
        "    'unbound': [str(u) for u in job.unbound_sources],\n"
        "    'hashes': [s.content_hash().to_string(), s.pipeline_hash().to_string()]}))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
        cwd=_REPO,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def _workshop_path(
    project: str, dataset: str, first: str, second: str, column: str, store: Path
) -> dict[str, Any]:
    """Join two sources, run a ``x: float`` pod twice, save, reload in a fresh process."""
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.operators import Join, MapData, SelectDataColumns
    from orcapod.sources import SpiralDBTableSource

    calls: list[int] = []

    def build() -> PipelineJob:
        @function_pod(output_keys="doubled")
        def double(x: float) -> float:
            calls.append(1)
            return 2 * x

        job = PipelineJob(name="itl936", database=DeltaTableDatabase(store))
        with job:
            joined = Join()(
                SpiralDBTableSource(project, first, dataset=dataset),
                SpiralDBTableSource(project, second, dataset=dataset),
                label="joined",
            )
            selected = SelectDataColumns([column])(joined, label="selected")
            if column != "x":
                selected = MapData({column: "x"})(selected, label="renamed")
            double.pod(selected, label="double")
        return job

    job = build()
    job.run()
    first_calls = len(calls)
    calls.clear()
    rerun = build()
    rerun.run()
    path = store.parent / "job.json"
    rerun.save(path)
    second_source = SpiralDBTableSource(project, second, dataset=dataset)
    hashes = [
        second_source.content_hash().to_string(),
        second_source.pipeline_hash().to_string(),
    ]
    fresh = _fresh_load(project, dataset, second, path, store)
    return {
        "joined_tag_schema": str(job.nodes["joined"].output_schema()[0]),
        "first_run_calls": first_calls,
        "second_run_calls": len(calls),
        "fresh_process_loaded_rows": fresh["rows"],
        "fresh_process_unbound": fresh["unbound"],
        "fresh_process_hashes_equal": fresh["hashes"] == hashes,
        "ok": first_calls > 0
        and not calls
        and fresh["rows"] == first_calls
        and not fresh["unbound"]
        and fresh["hashes"] == hashes,
    }


# ---------------------------------------------------------------------------- write mode

_G = pa.struct([pa.field("x", pa.float64(), False), pa.field("y", pa.int32(), True)])
_L = pa.list_(pa.field("item", pa.float32(), False))
_SCHEMA = pa.schema(
    [
        pa.field("k", pa.int64(), False),
        pa.field("x", pa.float64(), False),
        pa.field("w", pa.int32(), True),
        pa.field("g", _G, False),
        pa.field("l", _L, False),
    ]
)


def _rows(keys: list[int], schema: pa.Schema = _SCHEMA, **override: Any) -> pa.Table:
    """Return rows of ``schema`` keyed by ``keys`` (``override`` replaces whole columns)."""
    n = len(keys)
    columns: dict[str, Any] = {
        "k": keys,
        "x": [float(k) / 2 for k in keys],
        "w": [None] * n,
        "g": [{"x": float(k), "y": None} for k in keys],
        "l": [[1.0, 2.0]] * n,
        **override,
    }
    return pa.Table.from_pydict(columns, schema=schema)


def write_mode(project: str, dataset: str, report: dict[str, Any]) -> bool:
    """Run the write-mode checks in scratch tables of ``dataset``, dropping them after.

    Returns:
        Whether every check passed.
    """
    import spiral as sp  # pyright: ignore[reportMissingImports]  (spiraldb extra, absent in the type-check job)

    from orcapod.databases import (
        SpiralDBConnector,
        read_nullability_record,
        write_nullability_record,
    )
    from orcapod.errors import (
        NullabilityRecordMismatchError,
        NullInNonNullableColumnError,
        UnsupportedValueError,
    )
    from orcapod.sources import SpiralDBTableSource
    from orcapod.types import ColumnInfo

    run = time.strftime("%Y%m%d%H%M%S")
    client = sp.Spiral()
    proj = client.project(project)
    names = {
        n: f"itl936_{run}_{n}"
        for n in ("a", "b", "hidden", "struct_null", "struct_null_orcapod", "meta")
    }
    created: list[str] = []
    ok = True
    try:
        connector = SpiralDBConnector(project, dataset=dataset)
        # 1. fresh
        key_info = [ColumnInfo("k", pa.int64(), nullable=False)]
        connector.create_table_if_not_exists(names["a"], key_info, pk_column="k")
        created.append(names["a"])
        fresh = proj.table(f"{dataset}.{names['a']}").schema().to_arrow().names
        report["fresh"] = {"schema_names_after_create": fresh, "ok": fresh == ["k"]}
        ok = ok and bool(report["fresh"]["ok"])

        # 2. record
        connector.upsert_records(names["a"], _rows([1, 2, 3]), id_column="k")
        tbl = proj.table(f"{dataset}.{names['a']}")
        spiral_schema = tbl.schema().to_arrow()
        info = connector.get_column_info(names["a"])
        batches = list(connector.iter_batches(f'SELECT * FROM "{names["a"]}"'))
        source = SpiralDBTableSource(project, names["a"], dataset=dataset)
        record = read_nullability_record(tbl.get_metadata())
        record_report: dict[str, Any] = {
            "record": sorted(list(p) for p in record or ()),
            "spiral_schema": _schema_text(spiral_schema),
            "column_info": [[c.name, str(c.arrow_type), c.nullable] for c in info],
            "batch_schema": _schema_text(batches[0].schema),
            "output_schema": [str(s) for s in source.output_schema()],
        }
        report["record"] = record_report
        expected_nonnull = {"k", "x", "g", "l"}
        expected_record = {("k",), ("x",), ("g",), ("g", "x"), ("l",), ("l", "[]")}
        batch_schema = batches[0].schema
        record_report["ok"] = (
            record == expected_record
            and {c.name for c in info if not c.nullable} == expected_nonnull
            and not batch_schema.field("g").type.field("x").nullable
            and batch_schema.field("g").type.field("y").nullable
            and not batch_schema.field("l").type.value_field.nullable
        )
        ok = ok and bool(report["record"]["ok"])

        connector.create_table_if_not_exists(names["b"], key_info, pk_column="k")
        created.append(names["b"])
        b_schema = pa.schema(
            [pa.field("k", pa.int64(), False), pa.field("z", pa.float64(), False)]
        )
        connector.upsert_records(
            names["b"],
            pa.Table.from_pydict(
                {"k": [1, 2, 3], "z": [0.5, 1.5, 2.5]}, schema=b_schema
            ),
            id_column="k",
        )
        with tempfile.TemporaryDirectory() as tmp:
            # pod over b's z, joined with a: keys are int64 (T) on both sides
            report["workshop_path"] = _workshop_path(
                project, dataset, names["a"], names["b"], "z", Path(tmp) / "store"
            )
        ok = ok and bool(report["workshop_path"]["ok"])

        # 3. refusals
        refusals: dict[str, Any] = {}
        bad = _rows([9], g=[{"x": None, "y": 1}])
        try:
            connector.upsert_records(names["a"], bad, id_column="k")
            refusals["null_in_g_x"] = "NOT refused"
        except NullInNonNullableColumnError as exc:
            refusals["null_in_g_x"] = str(exc)
        wider = _SCHEMA.set(1, _SCHEMA.field("x").with_nullable(True))
        try:
            connector.upsert_records(names["a"], _rows([9], wider), id_column="k")
            refusals["wider_write"] = "NOT refused"
        except NullabilityRecordMismatchError as exc:
            refusals["wider_write"] = str(exc)
        rows_after = sum(
            b.num_rows for b in connector.iter_batches(f'SELECT * FROM "{names["a"]}"')
        )
        refusals["rows_after"] = rows_after
        refusals["ok"] = (
            "NOT" not in refusals["null_in_g_x"]
            and "NOT" not in refusals["wider_write"]
            and rows_after == 3
        )
        report["refusals"] = refusals
        ok = ok and bool(refusals["ok"])

        # 4. hidden values (straight through pyspiral, like any other writer)
        hidden = proj.create_table(
            f"{dataset}.{names['hidden']}",
            key_schema=pa.schema([pa.field("k", pa.int64(), False)]),
        )
        created.append(names["hidden"])
        g_nullable = pa.struct(
            [pa.field("x", pa.int32(), True), pa.field("y", pa.int32(), True)]
        )
        g = pa.StructArray.from_arrays(
            [pa.array([7, 8, None], pa.int32()), pa.array([1, None, 3], pa.int32())],
            fields=list(g_nullable),
            mask=pa.array([True, False, False]),  # row 0: null struct over x=7, y=1
        )
        lists = pa.ListArray.from_arrays(
            pa.array([0, 2, 4, 6], pa.int32()),
            pa.array([1.0, 2.0, 3.0, None, 5.0, 6.0], pa.float32()),
            mask=pa.array([False, False, True]),  # row 2: null list over [5, 6]
        )
        written = pa.table({"k": pa.array([1, 2, 3], pa.int64()), "g": g, "l": lists})
        hidden.write(written, push_down_nulls=True)
        back = client.scan(hidden.select()).to_table()
        gb = cast("pa.StructArray", back.column("g").combine_chunks())
        report["hidden_values"] = {
            "written_rows": written.to_pylist(),
            "read_rows": back.to_pylist(),
            "read_g_validity": gb.is_valid().to_pylist(),
            "read_g_x_physical": gb.field("x").to_pylist(),
            "read_l_validity": back.column("l").is_valid().to_pylist(),
            "schema": _schema_text(back.schema),
        }

        # 4b. a null struct WITHOUT push_down_nulls, straight through pyspiral: refused?
        plain = proj.create_table(
            f"{dataset}.{names['struct_null']}",
            key_schema=pa.schema([pa.field("k", pa.int64(), False)]),
        )
        created.append(names["struct_null"])
        struct_null = pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "g": pa.array([{"x": 1, "y": 1}, None], g_nullable),
            }
        )
        try:
            plain.write(struct_null)
            stored = client.scan(plain.select()).to_table()
            report["struct_null_without_push_down"] = {
                "write": "accepted",
                "read_rows": stored.to_pylist(),
                "read_g_validity": stored.column("g").is_valid().to_pylist(),
            }
        except Exception as exc:  # noqa: BLE001 - the probe records whatever SpiralDB raises
            report["struct_null_without_push_down"] = {
                "write": f"refused: {type(exc).__name__}: {exc}"
            }
        # ... and through the connector: refused by Orcapod before anything is written
        connector.create_table_if_not_exists(
            names["struct_null_orcapod"], key_info, pk_column="k"
        )
        created.append(names["struct_null_orcapod"])
        try:
            connector.upsert_records(
                names["struct_null_orcapod"], struct_null, id_column="k"
            )
            orcapod_refusal = "NOT refused"
        except UnsupportedValueError as exc:
            orcapod_refusal = str(exc)
        report["struct_null_connector"] = {
            "error": orcapod_refusal,
            "ok": "NOT" not in orcapod_refusal,
        }
        ok = ok and bool(report["struct_null_connector"]["ok"])

        # 5. metadata
        meta_tbl = proj.create_table(
            f"{dataset}.{names['meta']}",
            key_schema=pa.schema([pa.field("k", pa.int64(), False)]),
        )
        created.append(names["meta"])
        meta_tbl.set_metadata({"itl936_a": b"1", "itl936_b": b"2"})
        wide = pa.schema(
            [pa.field("k", pa.int64(), False)]
            + [
                pa.field(f"column_with_a_long_name_{i:04d}", pa.float64(), False)
                for i in range(400)
            ]
        )
        write_nullability_record(meta_tbl, wide)
        wide_back = read_nullability_record(meta_tbl.get_metadata())
        try:
            meta_tbl.set_metadata({"itl936_big": b"x" * 8200})
            big = "NOT refused"
        except Exception as exc:  # noqa: BLE001 - the probe records whatever SpiralDB raises
            big = f"{type(exc).__name__}: {exc}"
        md = meta_tbl.get_metadata()
        report["metadata"] = {
            "two_keys_one_call": [md.get("itl936_a"), md.get("itl936_b")]
            == [b"1", b"2"],
            "wide_record_keys": sorted(k for k in md if k.startswith("__orcapod")),
            "wide_record_round_trip": wide_back == frozenset((f.name,) for f in wide),
            "over_8_kib": big,
        }
        report["metadata"]["ok"] = (
            report["metadata"]["two_keys_one_call"]
            and report["metadata"]["wide_record_round_trip"]
            and "NOT" not in big
        )
        ok = ok and bool(report["metadata"]["ok"])
    except Exception:  # noqa: BLE001 - reported, then the scratch tables are dropped
        report["error"] = traceback.format_exc()
        ok = False
    finally:
        dropped = []
        for name in created:
            try:
                proj.drop_table(f"{dataset}.{name}")
                dropped.append(name)
            except Exception as exc:  # noqa: BLE001 - reported
                report.setdefault("drop_errors", []).append(f"{name}: {exc}")
                ok = False
        report["dropped"] = dropped
    return bool(ok)


# ---------------------------------------------------------------------------- read mode


def _field_at(schema: pa.Schema, path: tuple[str, ...]) -> pa.Field[Any] | None:
    """Return the field at a recorded ``path`` of ``schema``, or ``None`` if it is absent."""
    if path[0] not in schema.names:
        return None
    field = schema.field(path[0])
    for segment in path[1:]:
        t = field.type
        if segment == "[]" and (
            pa.types.is_list(t)
            or pa.types.is_large_list(t)
            or pa.types.is_fixed_size_list(t)
        ):
            field = t.value_field
        elif pa.types.is_struct(t) and t.get_field_index(segment) >= 0:
            field = t.field(segment)
        else:
            return None
    return field


def read_mode(
    project: str, dataset: str, tables: list[str], column: str, report: dict[str, Any]
) -> bool:
    """Run the read-only checks on ``tables`` of ``dataset``.

    Returns:
        Whether every check passed.
    """
    import spiral as sp  # pyright: ignore[reportMissingImports]  (spiraldb extra, absent in the type-check job)

    from orcapod.databases import read_nullability_record
    from orcapod.sources import SpiralDBTableSource

    proj = sp.Spiral().project(project)
    ok = True
    for name in tables:
        tbl = proj.table(f"{dataset}.{name}")
        record = read_nullability_record(tbl.get_metadata())
        source = SpiralDBTableSource(project, name, dataset=dataset)
        tag, data = source.output_schema()
        merged = {**dict(tag), **dict(data)}
        top = sorted(p[0] for p in record or () if len(p) == 1)
        optional = [c for c in top if "None" in str(merged.get(c))]
        arrow_schema = source.as_table().schema
        for path in sorted(p for p in record or () if len(p) > 1):
            field = _field_at(arrow_schema, path)
            if field is None or field.nullable:
                optional.append(".".join(path))
        report[name] = {
            "recorded_top_level": top,
            "recorded_nested": sorted(".".join(p) for p in record or () if len(p) > 1),
            "output_schema": [str(tag), str(data)],
            "recorded_but_optional": optional,  # top-level by Python type, nested by Arrow
            "ok": record is not None and not optional,
        }
        ok = ok and bool(report[name]["ok"])
    if len(tables) >= 2:
        with tempfile.TemporaryDirectory() as tmp:
            report["workshop_path"] = _workshop_path(
                project, dataset, tables[0], tables[1], column, Path(tmp) / "store"
            )
        ok = ok and bool(report["workshop_path"]["ok"])
    return bool(ok)


def main() -> int:
    """Parse the command line, run a mode and print the JSON report.

    Returns:
        The exit status: 0 when every check passed.
    """
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("mode", choices=["write", "read"])
    parser.add_argument("--project", required=True)
    parser.add_argument("--allow-writes")
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--tables", default="")
    parser.add_argument("--float-column", default="")
    parser.add_argument("--report")
    args = parser.parse_args()
    report: dict[str, Any] = {"mode": args.mode, "project": args.project}
    if args.mode == "write":
        if args.allow_writes != args.project:
            parser.error("write mode needs --allow-writes repeating --project")
        if args.project.startswith("edi-sample-v"):
            parser.error("never write to an edi-sample-v* project")
        ok = write_mode(args.project, args.dataset or "meta", report)
    else:
        tables = [t for t in args.tables.split(",") if t]
        if len(tables) < 2 or not args.float_column:
            parser.error(
                "read mode needs --tables with at least two tables (the first two are "
                "joined) and --float-column"
            )
        ok = read_mode(
            args.project, args.dataset or "default", tables, args.float_column, report
        )
    report["ok"] = ok
    text = json.dumps(report, indent=2, default=str)
    print(text)
    if args.report:
        Path(args.report).write_text(text)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
