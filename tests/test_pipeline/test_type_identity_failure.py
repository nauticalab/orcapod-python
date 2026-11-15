"""A stored row that cannot be decoded as its class fails the run (ITL-881).

``TypeIdentityError`` means a stored table is intact but holds another shape than the
registered class. It is not a ``OrcapodDatabaseReadError``, yet it must be as fatal: under
``error_policy="continue"`` a row whose stored result cannot be decoded must never be
skipped as an ordinary row failure, or stored results would silently look dropped. Both
orchestrators must raise it.
"""

from __future__ import annotations

import dataclasses
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import OrcapodDatabaseError, TypeIdentityError
from orcapod.pipeline import PipelineJob
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

Orchestrator = SyncPipelineOrchestrator | AsyncPipelineOrchestrator

_CALLS: list[int] = []


@dataclasses.dataclass
class Pair:
    """The value the pod stores."""

    x: int
    y: int


@function_pod(output_keys="q")
def pair(x: int) -> Pair:
    """Record the call and return ``Pair(x, x)``."""
    _CALLS.append(x)
    return Pair(x, x)


ORCHESTRATORS: dict[str, Callable[[], Orchestrator]] = {
    "sync": lambda: SyncPipelineOrchestrator(error_policy="continue"),
    "async": lambda: AsyncPipelineOrchestrator(error_policy="continue"),
}


@pytest.fixture(autouse=True)
def _reset_calls():
    """Start and end every test with no recorded calls."""
    _CALLS.clear()
    yield
    _CALLS.clear()


@pytest.fixture(params=list(ORCHESTRATORS))
def make_orchestrator(request: pytest.FixtureRequest) -> Callable[[], Orchestrator]:
    """Return a factory for the parametrized orchestrator (``continue`` policy)."""
    return ORCHESTRATORS[request.param]


def build_job(database_path: Path) -> PipelineJob:
    """Build a job that stores one ``Pair`` per row of a three-row source."""
    source = ArrowTableSource(
        pa.table({"k": ["a", "b", "c"], "x": [1, 2, 3]}),
        tag_columns=["k"],
        infer_nullable=True,
    )
    job = PipelineJob(
        name="itl881", database=DeltaTableDatabase(base_path=database_path)
    )
    with job:
        _ = pair.pod(source, label="pair")
    return job


def table_dirs(database_path: Path) -> tuple[Path, Path]:
    """The (result table, pipeline table) directories the job wrote."""
    dirs = {p.parent for p in database_path.rglob("*.parquet")}
    result = [d for d in dirs if "_result" in d.parts]
    pipeline = [d for d in dirs if "_result" not in d.parts]
    assert len(result) == 1 and len(pipeline) == 1, sorted(dirs)
    return result[0], pipeline[0]


def store_another_shape(result_table: Path) -> None:
    """Rewrite column ``q`` with another struct under the same extension name.

    Stands in for a table written by other means: the table stays readable, but its
    stored shape no longer matches the registered ``Pair``.
    """
    stored = deltalake.DeltaTable(str(result_table)).to_pyarrow_table()
    index = stored.schema.get_field_index("q")
    ext = stored.schema.field(index).type
    assert isinstance(ext, pa.ExtensionType)
    wrong = pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.int64())])
    values: list[Any] = [{"x": 0.5, "y": 1}] * stored.num_rows
    field = pa.field("q", wrong).with_metadata(
        {
            b"ARROW:extension:name": ext.extension_name.encode(),
            b"ARROW:extension:metadata": ext.__arrow_ext_serialize__(),
        }
    )
    columns = [
        pa.array(values, type=wrong) if i == index else stored.column(i)
        for i in range(stored.num_columns)
    ]
    fields = [field if i == index else f for i, f in enumerate(stored.schema)]
    deltalake.write_deltalake(
        str(result_table),
        pa.Table.from_arrays(columns, schema=pa.schema(fields)),
        mode="overwrite",
        schema_mode="overwrite",
    )


@pytest.fixture
def stored(tmp_path: Path, make_orchestrator: Callable[[], Orchestrator]) -> Path:
    """A store in which the job has computed and stored all three rows."""
    database_path = tmp_path / "store"
    build_job(database_path).run(orchestrator=make_orchestrator())
    assert len(_CALLS) == 3
    _CALLS.clear()
    return database_path


def test_other_stored_shape_fails_the_run(
    stored: Path, make_orchestrator: Callable[[], Orchestrator]
) -> None:
    """A result table holding another shape fails the run before any function call."""
    result_table, _ = table_dirs(stored)
    store_another_shape(result_table)

    with pytest.raises(TypeIdentityError) as info:
        build_job(stored).run(orchestrator=make_orchestrator())

    assert not isinstance(info.value, OrcapodDatabaseError)
    assert _CALLS == []


def test_other_stored_shape_fails_the_run_in_the_per_row_lookup(
    stored: Path, make_orchestrator: Callable[[], Orchestrator]
) -> None:
    """The per-row lookup's identity error fails the run under ``continue``."""
    # With the pipeline table gone, the first read of the result table is the per-row
    # cache lookup, which runs inside the error_policy="continue" handler.
    result_table, pipeline_table = table_dirs(stored)
    store_another_shape(result_table)
    shutil.rmtree(pipeline_table)

    with pytest.raises(TypeIdentityError) as info:
        build_job(stored).run(orchestrator=make_orchestrator())

    assert not isinstance(info.value, OrcapodDatabaseError)
    assert _CALLS == []


class _CrashingObserver(NoOpObserver):
    """An observer whose callbacks raise while the pod's failure propagates."""

    def on_data_crash(
        self, node_label: str, tag: Any, data: Any, error: Exception
    ) -> None:
        """Raise from the row-crash callback."""
        raise RuntimeError("observer failed in on_data_crash")

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Raise from the node-end callback of the pod's node."""
        if node_label == "pair":
            raise RuntimeError("observer failed in on_node_end")

    def on_run_end(self, run_id: str) -> None:
        """Raise from the run-end callback."""
        raise RuntimeError("observer failed in on_run_end")


def test_failing_observer_does_not_replace_the_identity_error(
    stored: Path, make_orchestrator: Callable[[], Orchestrator]
) -> None:
    """Callbacks that raise while the identity error propagates are noted, not raised."""
    result_table, pipeline_table = table_dirs(stored)
    store_another_shape(result_table)
    shutil.rmtree(pipeline_table)

    with pytest.raises(TypeIdentityError) as info:
        build_job(stored).run(
            orchestrator=make_orchestrator(), observer=_CrashingObserver()
        )

    notes = getattr(info.value, "__notes__", [])
    assert any("observer failed in on_data_crash" in note for note in notes), notes
    assert any("observer failed in on_run_end" in note for note in notes), notes
    assert _CALLS == []
