"""``orcapod migrate`` sub-commands.

Provides ``orcapod migrate pipeline-db`` and ``orcapod migrate result-db``
for upgrading v0 pipeline/result DB tables to the v1 schema.
"""
from __future__ import annotations

import json

import typer

migrate_app = typer.Typer(
    name="migrate",
    help="Migrate Orcapod pipeline and result DB tables to the current schema version.",
    no_args_is_help=True,
)


@migrate_app.command("pipeline-db")
def migrate_pipeline_db(
    pipeline_db_path: str = typer.Argument(..., help="Path to the pipeline DB (Delta Lake root)."),
    result_db_path: str = typer.Argument(..., help="Path to the result DB (Delta Lake root)."),
    node_paths: list[str] = typer.Argument(..., help="One or more bare v0 node paths (slash-separated, e.g. 'my_node/schema:abc123')."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Count rows to migrate without writing."),
    batch_size: int = typer.Option(500, "--batch-size", help="Rows processed per batch."),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Log progress messages."),
    json_summary: bool = typer.Option(False, "--json-summary", help="Print JSON summary to stdout on completion."),
) -> None:
    """Migrate one or more pipeline DB node paths from v0 to v1 schema."""
    from orcapod.databases.delta_lake_databases import DeltaTableDatabase
    from orcapod.migrations.pipeline_db import migrate_pipeline_v0_to_v1

    pipeline_db = DeltaTableDatabase(base_path=pipeline_db_path)
    result_db = DeltaTableDatabase(base_path=result_db_path)

    for node_path_str in node_paths:
        pipeline_path = tuple(node_path_str.split("/"))
        result_path = pipeline_path  # by convention result DB mirrors pipeline path

        if progress:
            typer.echo(f"Migrating pipeline DB: {pipeline_db_path}")
            typer.echo(f"  node path: {node_path_str}")

        result = migrate_pipeline_v0_to_v1(
            pipeline_db=pipeline_db,
            pipeline_path=pipeline_path,
            result_db=result_db,
            result_path=result_path,
            dry_run=dry_run,
            batch_size=batch_size,
            progress=progress,
        )

        if progress:
            typer.echo(
                f"  migrated: {result.rows_migrated}   "
                f"skipped (already v1): {result.rows_skipped}   "
                f"unresolvable: {result.rows_unresolvable}"
            )
            typer.echo(f"  elapsed: {result.elapsed_s:.1f}s")

        if json_summary:
            summary = {
                "rows_total": result.rows_total,
                "rows_migrated": result.rows_migrated,
                "rows_skipped": result.rows_skipped,
                "rows_unresolvable": result.rows_unresolvable,
                "elapsed_s": result.elapsed_s,
                "dry_run": result.dry_run,
            }
            typer.echo(json.dumps(summary))


@migrate_app.command("result-db")
def migrate_result_db(
    result_db_path: str = typer.Argument(..., help="Path to the result DB (Delta Lake root)."),
    record_paths: list[str] = typer.Argument(..., help="One or more bare v0 record paths (slash-separated)."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Count rows to migrate without writing."),
    batch_size: int = typer.Option(500, "--batch-size", help="Rows processed per batch."),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Log progress messages."),
    json_summary: bool = typer.Option(False, "--json-summary", help="Print JSON summary to stdout on completion."),
) -> None:
    """Migrate one or more result DB record paths from v0 to v1 schema."""
    from orcapod.databases.delta_lake_databases import DeltaTableDatabase
    from orcapod.migrations.result_db import migrate_result_v0_to_v1

    result_db = DeltaTableDatabase(base_path=result_db_path)

    for record_path_str in record_paths:
        result_path = tuple(record_path_str.split("/"))

        if progress:
            typer.echo(f"Migrating result DB: {result_db_path}")
            typer.echo(f"  record path: {record_path_str}")

        result = migrate_result_v0_to_v1(
            result_db=result_db,
            result_path=result_path,
            dry_run=dry_run,
            batch_size=batch_size,
            progress=progress,
        )

        if progress:
            typer.echo(
                f"  migrated: {result.rows_migrated}   "
                f"skipped (already v1): {result.rows_skipped}"
            )
            typer.echo(f"  elapsed: {result.elapsed_s:.1f}s")

        if json_summary:
            summary = {
                "rows_total": result.rows_total,
                "rows_migrated": result.rows_migrated,
                "rows_skipped": result.rows_skipped,
                "rows_unresolvable": result.rows_unresolvable,
                "elapsed_s": result.elapsed_s,
                "dry_run": result.dry_run,
            }
            typer.echo(json.dumps(summary))
