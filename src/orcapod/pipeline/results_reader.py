"""Read-only viewer for pipeline observability results stored in Delta Lake."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar, TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from upath import UPath


class ResultsReader:
    """Auto-discovers and queries pipeline status and log Delta tables.

    Args:
        root: Path to the results output directory. Supports local paths,
            ``pathlib.Path``, and ``UPath`` for cloud storage.

    Raises:
        ValueError: If ``root`` does not exist or contains no Delta tables.
    """

    def __init__(self, root: str | Path | UPath) -> None:
        self._root = root if isinstance(root, Path) else Path(root)
        if not self._root.exists():
            raise ValueError(
                f"Results root does not exist: {self._root}"
            )

        self._status_tables: dict[str, Path] = {}
        self._log_tables: dict[str, Path] = {}
        self._discover_tables()

        if not self._status_tables and not self._log_tables:
            raise ValueError(
                f"No observability Delta tables found under: {self._root}"
            )

        self._status_df: pl.DataFrame | None = None
        self._logs_df: pl.DataFrame | None = None

    def _discover_tables(self) -> None:
        """Find all Delta tables and classify as status or log."""
        for delta_log_dir in self._root.rglob("_delta_log"):
            if not delta_log_dir.is_dir():
                continue
            table_dir = delta_log_dir.parent
            parts = table_dir.relative_to(self._root).parts

            if "status" in parts:
                idx = parts.index("status")
                if idx + 1 < len(parts):
                    node_name = parts[idx + 1]
                    self._status_tables[node_name] = table_dir
            elif "logs" in parts:
                idx = parts.index("logs")
                if idx + 1 < len(parts):
                    node_name = parts[idx + 1]
                    self._log_tables[node_name] = table_dir

    @property
    def nodes(self) -> list[str]:
        """Sorted list of discovered node names."""
        return sorted(self._status_tables.keys())

    @property
    def tag_columns(self) -> list[str]:
        """Inferred user tag column names."""
        status = self._get_status_df()
        return sorted(
            col for col in status.columns
            if not col.startswith("__")
            and not col.startswith("_status_")
            and not col.startswith("_log_")
            and not col.startswith("_tag_")
            and not col.startswith("_tag::")
            and col != "node_label"
        )

    def _get_status_df(self) -> pl.DataFrame:
        """Lazy-load and return the concatenated status DataFrame."""
        if self._status_df is None:
            frames = []
            for node_name, table_dir in self._status_tables.items():
                df = pl.read_delta(str(table_dir))
                frames.append(df)
            if frames:
                self._status_df = pl.concat(frames, how="diagonal_relaxed")
            else:
                self._status_df = pl.DataFrame()
        return self._status_df

    def _get_logs_df(self) -> pl.DataFrame:
        """Lazy-load and return the concatenated logs DataFrame."""
        if self._logs_df is None:
            frames = []
            for node_name, table_dir in self._log_tables.items():
                df = pl.read_delta(str(table_dir))
                frames.append(df)
            if frames:
                self._logs_df = pl.concat(frames, how="diagonal_relaxed")
            else:
                self._logs_df = pl.DataFrame()
        return self._logs_df

    # -- Column rename mappings ------------------------------------------------

    _STATUS_RENAMES: ClassVar[dict[str, str]] = {
        "_status_node_label": "node_label",
        "_status_state": "state",
        "_status_timestamp": "timestamp",
        "_status_error_summary": "error_summary",
    }

    _LOG_RENAMES: ClassVar[dict[str, str]] = {
        "_log_node_label": "node_label",
        "_log_timestamp": "timestamp",
        "_log_success": "success",
        "_log_stdout_log": "stdout_log",
        "_log_stderr_log": "stderr_log",
        "_log_python_logs": "python_logs",
        "_log_traceback": "traceback",
    }

    _DROP_PREFIXES: ClassVar[tuple[str, ...]] = ("__", "_tag_", "_tag::")
    _STATUS_DROP_EXACT: ClassVar[set[str]] = {
        "_status_id", "_status_run_id", "_status_pipeline_uri", "_status_node_hash",
    }
    _LOG_DROP_EXACT: ClassVar[set[str]] = {
        "_log_id", "_log_run_id", "_log_node_hash",
    }

    _LOG_TERSE_COLUMNS: ClassVar[tuple[str, ...]] = (
        "node_label", "traceback", "success", "timestamp",
    )

    # -- Internal helpers ------------------------------------------------------

    def _clean_status_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """Strip system columns and rename status columns."""
        drop_cols = [
            col for col in df.columns
            if any(col.startswith(p) for p in self._DROP_PREFIXES)
            or col in self._STATUS_DROP_EXACT
        ]
        drop_cols.extend(
            col for col in df.columns
            if col.startswith("_status_") and col not in self._STATUS_RENAMES
        )
        df = df.drop([c for c in drop_cols if c in df.columns])
        return df.rename(
            {k: v for k, v in self._STATUS_RENAMES.items() if k in df.columns}
        )

    def _clean_logs_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """Strip system columns and rename log columns."""
        drop_cols = [
            col for col in df.columns
            if any(col.startswith(p) for p in self._DROP_PREFIXES)
            or col in self._LOG_DROP_EXACT
        ]
        drop_cols.extend(
            col for col in df.columns
            if col.startswith("_log_") and col not in self._LOG_RENAMES
        )
        df = df.drop([c for c in drop_cols if c in df.columns])
        return df.rename(
            {k: v for k, v in self._LOG_RENAMES.items() if k in df.columns}
        )

    def _validate_node(self, node: str) -> None:
        """Raise KeyError if node is not a known node."""
        if node not in self._status_tables:
            raise KeyError(
                f"Node {node!r} not found. Available nodes: {self.nodes}"
            )

    def _apply_filters(
        self, df: pl.DataFrame, filters: dict[str, str],
    ) -> pl.DataFrame:
        """Filter DataFrame by tag column values."""
        tag_cols = self.tag_columns
        for key, value in filters.items():
            if key not in tag_cols:
                raise KeyError(
                    f"Filter column {key!r} is not a tag column. "
                    f"Available tag columns: {tag_cols}"
                )
            df = df.filter(pl.col(key) == value)
        return df

    def _deduplicate_status(self, df: pl.DataFrame) -> pl.DataFrame:
        """Keep only the latest status row per (node_label, tag_columns)."""
        group_cols = ["node_label"] + self.tag_columns
        group_cols = [c for c in group_cols if c in df.columns]
        return df.sort("timestamp").unique(subset=group_cols, keep="last")

    # -- Public query methods --------------------------------------------------

    def status(self, group_by: list[str] | None = None) -> pl.DataFrame:
        """Node-level status overview with counts by execution state.

        CACHED results are counted as SUCCESS since they represent
        previously computed successful results.

        Args:
            group_by: Optional tag columns to group by in addition to
                ``node_label``. If ``None``, groups by ``node_label`` only.

        Returns:
            DataFrame with columns: ``node_label``, ``SUCCESS``, ``FAILED``,
            ``RUNNING``, ``last_updated``, and any ``group_by`` columns.
        """
        df = self._get_status_df()
        df = self._clean_status_df(df)
        df = self._deduplicate_status(df)

        group_cols = ["node_label"]
        if group_by:
            group_cols.extend(c for c in group_by if c != "node_label")

        return (
            df.group_by(group_cols)
            .agg(
                pl.col("state").is_in(["SUCCESS", "CACHED"]).sum().alias("SUCCESS"),
                pl.col("state").eq("FAILED").sum().alias("FAILED"),
                pl.col("state").eq("RUNNING").sum().alias("RUNNING"),
                pl.col("timestamp").max().alias("last_updated"),
            )
            .sort(group_cols)
        )

    def details(
        self, node: str | None = None, **filters: str,
    ) -> pl.DataFrame:
        """Per-input rows showing the latest state for each input.

        Args:
            node: Optional node name to filter to.
            **filters: Tag column filters, e.g. ``subject="Goliath"``.

        Returns:
            DataFrame with columns: ``node_label``, tag columns, ``state``,
            ``timestamp``, ``error_summary``.

        Raises:
            KeyError: If ``node`` is not found or a filter key is invalid.
        """
        df = self._get_status_df()
        df = self._clean_status_df(df)
        if node is not None:
            self._validate_node(node)
            df = df.filter(pl.col("node_label") == node)
        df = self._apply_filters(df, filters)
        return self._deduplicate_status(df)

    def failures(
        self, node: str | None = None, **filters: str,
    ) -> pl.DataFrame:
        """All failed rows across nodes.

        Shorthand for ``details()`` filtered to ``state == "FAILED"``.

        Args:
            node: Optional node name to filter to.
            **filters: Tag column filters, e.g. ``subject="Goliath"``.

        Returns:
            DataFrame with same columns as ``details()``, filtered to failures.
        """
        df = self.details(node=node, **filters)
        return df.filter(pl.col("state") == "FAILED")

    def logs(self, node: str, **filters: str) -> pl.DataFrame:
        """Log entries for a node with terse output.

        Returns ``node_label``, tag columns, ``traceback``, ``success``,
        and ``timestamp``. Use ``full_logs()`` for stdout/stderr/python_logs.

        Args:
            node: Node name to query.
            **filters: Tag column filters, e.g. ``subject="Goliath"``.

        Raises:
            KeyError: If ``node`` is not found or a filter key is invalid.
        """
        df = self.full_logs(node, **filters)
        if df.is_empty():
            return df
        keep = list(self._LOG_TERSE_COLUMNS) + self.tag_columns
        return df.select([c for c in keep if c in df.columns])

    def full_logs(self, node: str, **filters: str) -> pl.DataFrame:
        """Full log entries for a node including stdout/stderr/python_logs.

        Args:
            node: Node name to query.
            **filters: Tag column filters, e.g. ``subject="Goliath"``.

        Raises:
            KeyError: If ``node`` is not found or a filter key is invalid.
        """
        self._validate_node(node)
        df = self._get_logs_df()
        if df.is_empty():
            return df
        df = self._clean_logs_df(df)
        df = df.filter(pl.col("node_label") == node)
        return self._apply_filters(df, filters)
