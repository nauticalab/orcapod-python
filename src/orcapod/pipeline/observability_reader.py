"""Read-only viewer for pipeline observability results stored in Delta Lake."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar, TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from upath import UPath


class ObservabilityReader:
    """Auto-discovers and queries pipeline status and log Delta tables.

    Provides two DataFrames: ``status()`` for execution state and
    ``logs()`` for execution logs. Both return clean polars DataFrames
    ready for further analysis with standard polars operations.

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

        self._status_tables: dict[str, list[Path]] = {}
        self._log_tables: dict[str, list[Path]] = {}
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
                    self._status_tables.setdefault(node_name, []).append(
                        table_dir
                    )
            elif "logs" in parts:
                idx = parts.index("logs")
                if idx + 1 < len(parts):
                    node_name = parts[idx + 1]
                    self._log_tables.setdefault(node_name, []).append(
                        table_dir
                    )

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
            for table_dirs in self._status_tables.values():
                for table_dir in table_dirs:
                    frames.append(pl.read_delta(str(table_dir)))
            if frames:
                self._status_df = pl.concat(frames, how="diagonal_relaxed")
            else:
                self._status_df = pl.DataFrame()
        return self._status_df

    def _get_logs_df(self) -> pl.DataFrame:
        """Lazy-load and return the concatenated logs DataFrame."""
        if self._logs_df is None:
            frames = []
            for table_dirs in self._log_tables.values():
                for table_dir in table_dirs:
                    frames.append(pl.read_delta(str(table_dir)))
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

    # -- Public query methods --------------------------------------------------

    def status(self) -> pl.DataFrame:
        """Latest execution status for every (node, input) combination.

        Returns one row per (node, input) with the most recent status.
        CACHED states are mapped to SUCCESS since they represent
        previously computed successful results.

        Returns:
            DataFrame with columns: ``node_label``, tag columns,
            ``state``, ``timestamp``, ``error_summary``.
        """
        df = self._get_status_df()
        if df.is_empty():
            return df
        df = self._clean_status_df(df)

        # Deduplicate to latest status per (node, input)
        group_cols = ["node_label"] + self.tag_columns
        group_cols = [c for c in group_cols if c in df.columns]
        df = df.sort("timestamp").unique(subset=group_cols, keep="last")

        # Map CACHED -> SUCCESS
        df = df.with_columns(
            pl.when(pl.col("state") == "CACHED")
            .then(pl.lit("SUCCESS"))
            .otherwise(pl.col("state"))
            .alias("state")
        )

        return df

    def logs(self, node: str) -> pl.DataFrame:
        """Full log entries for a node.

        Returns all log fields: stdout, stderr, python logs, traceback,
        success status, and timestamp, alongside tag columns.

        Args:
            node: Node name to query. Use ``reader.nodes`` to see
                available names.

        Returns:
            DataFrame with columns: ``node_label``, tag columns,
            ``stdout_log``, ``stderr_log``, ``python_logs``,
            ``traceback``, ``success``, ``timestamp``.

        Raises:
            KeyError: If ``node`` is not found.
        """
        if node not in self._status_tables and node not in self._log_tables:
            raise KeyError(
                f"Node {node!r} not found. Available nodes: {self.nodes}"
            )
        df = self._get_logs_df()
        if df.is_empty():
            return df
        df = self._clean_logs_df(df)
        return df.filter(pl.col("node_label") == node)
