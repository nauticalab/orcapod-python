"""The workshop EDI dataset tool, run against an in-memory fake of pyspiral.

``workshop/edi_tutorial_dataset/edi_tutorial_dataset.py`` builds a Spiral project the
workshop depends on and may write to Spiral, so its guards and its copy are tested like
library code. The source here is a small synthetic project with the EDI sample's shapes:
composite keys, ``u64`` and ``i64`` timestamps, a column group, a ``fixed_size_list``
column, and tables in and outside the specification.
"""

from __future__ import annotations

import copy as copy_module
import datetime
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from tests.test_workshop.fake_spiral import FakeSpiral

REPO_ROOT = Path(__file__).resolve().parents[2]
TOOL_DIR = REPO_ROOT / "workshop" / "edi_tutorial_dataset"
TOOL = TOOL_DIR / "edi_tutorial_dataset.py"

SOURCE = "edi-sample-v1-855632"


def _load_tool() -> ModuleType:
    """Import the tool script as the ``edi_tutorial_dataset`` module."""
    spec = importlib.util.spec_from_file_location("edi_tutorial_dataset", TOOL)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["edi_tutorial_dataset"] = module  # dataclasses resolve their module
    spec.loader.exec_module(module)
    return module


tool = _load_tool()

# A cut-down specification in the format of edi_schema_spec.md.
MINI_SPEC = """
#### session

| Column | Type | Key | Description |
| --- | --- | --- | --- |
| subject_id | utf8 | PK | Subject |
| session_start_datetime | datetime | PK | Start |
| booth | utf8 | | Booth |
| session_notes | utf8? | | Notes |

#### experiment

| Column | Type | Key | Description |
| --- | --- | --- | --- |
| experiment_preprocessing_hash | utf8 | PK, FK | hash |
| subject_id | utf8 | PK, FK | Subject (FK → `session`) |
| session_start_datetime | datetime | PK, FK | Start (FK → `session`) |
| experiment_index | u32 | PK | index |
| task_id | utf8 | FK | Task |

#### experiment_freeviewing_trial

| Column | Type | Key | Description |
| --- | --- | --- | --- |
| experiment_preprocessing_hash | utf8 | PK, FK | (FK → `experiment`) |
| subject_id | utf8 | PK, FK | (FK → `experiment`) |
| session_start_datetime | datetime | PK, FK | (FK → `experiment`) |
| experiment_index | u32 | PK, FK | (FK → `experiment`) |
| trial_index | u32 | PK | trial |
| trial_start_timestamp | u64 | | start |
| trial_end_timestamp | u64? | | end |

#### experiment_freeviewing_event

| Column | Type | Key | Description |
| --- | --- | --- | --- |
| experiment_preprocessing_hash | utf8 | PK, FK | (FK → `experiment`) |
| subject_id | utf8 | PK, FK | (FK → `experiment`) |
| session_start_datetime | datetime | PK, FK | (FK → `experiment`) |
| experiment_index | u32 | PK, FK | (FK → `experiment`) |
| timestamp | u64 | PK | time |
| name | utf8 | PK | name |
| trial_index | u32 | | trial |

#### unit

| Column | Type | Key | Description |
| --- | --- | --- | --- |
| subject_id | utf8 | PK, FK | (FK → `session`) |
| session_start_datetime | datetime | PK, FK | (FK → `session`) |
| unit_id | u32 | PK | unit |
| **Classification labels (`labels` column group)** |  |  |  |
| kilosort_label | bool? | | label |
| bombcell_label | utf8? | | label |

#### spike_event

| Column | Type | Key | Description |
| --- | --- | --- | --- |
| subject_id | utf8 | PK, FK | (FK → `unit`) |
| session_start_datetime | datetime | PK, FK | (FK → `unit`) |
| timestamp | u64 | PK | time |
| unit_id | u32 | PK, FK | (FK → `unit`) |
| spike_amplitude_uv | f32 | | amplitude |

#### lfp_timeseries

| Column | Type | Key | Description |
| --- | --- | --- | --- |
| subject_id | utf8 | PK, FK | (FK → `session`) |
| session_start_datetime | datetime | PK, FK | (FK → `session`) |
| timestamp | u64 | PK | time |
| lfp_voltage_uv | fixed_size_list(f32?)[4] | | voltages |
"""

SST = datetime.datetime(2026, 3, 24, 17, 54, 17)
HASH = "semantic_v0.1:abc"
EXP_KEYS = {
    "experiment_preprocessing_hash": pa.string(),
    "subject_id": pa.string(),
    "session_start_datetime": pa.timestamp("us"),
    "experiment_index": pa.uint32(),
}
EXP_ROW = {
    "experiment_preprocessing_hash": HASH,
    "subject_id": "Goliath",
    "session_start_datetime": SST,
    "experiment_index": 1,
}
SESSION_ROW = {"subject_id": "Goliath", "session_start_datetime": SST}

WINDOW = (100, 200)
# trial 0 ends before the window, 1 lies inside it, 2 straddles its end, 3 starts
# after it but an event inside the window references it, 4 is never referenced.
TRIALS = [(0, 50, 99), (1, 100, 150), (2, 160, 260), (3, 300, 400), (4, 500, None)]
EVENTS = [(90, "a", 0), (120, "b", 1), (170, "c", 2), (199, "d", 3), (250, "e", 2)]
UNITS = [1, 2, 3]
KEPT_UNITS = [1, 3]


def _nullable(types: dict[str, pa.DataType]) -> pa.Schema:
    """The live sample declares every column nullable, keys included."""
    return pa.schema([pa.field(n, t, True) for n, t in types.items()])


def _table(types: dict[str, pa.DataType], rows: list[dict[str, Any]]) -> pa.Table:
    """Return an Arrow table of ``rows`` with every column nullable."""
    return pa.Table.from_pylist(rows, schema=_nullable(types))


def build_source(client: FakeSpiral) -> None:
    """Store the synthetic source project in ``client``."""
    add = client.add_table
    add(
        SOURCE,
        "default.session",
        _table(
            {
                "subject_id": pa.string(),
                "session_start_datetime": pa.timestamp("us"),
                "booth": pa.string(),
                "session_notes": pa.string(),
            },
            [{**SESSION_ROW, "booth": "2", "session_notes": None}],
        ),
        ["subject_id", "session_start_datetime"],
    )
    add(
        SOURCE,
        "default.experiment",
        _table(
            {**EXP_KEYS, "task_id": pa.string()},
            [{**EXP_ROW, "task_id": "FreeViewing"}],
        ),
        list(EXP_KEYS),
    )
    add(
        SOURCE,
        "default.experiment_freeviewing_trial",
        _table(
            {
                **EXP_KEYS,
                "trial_index": pa.uint32(),
                "trial_start_timestamp": pa.uint64(),
                "trial_end_timestamp": pa.uint64(),
            },
            [
                {
                    **EXP_ROW,
                    "trial_index": i,
                    "trial_start_timestamp": s,
                    "trial_end_timestamp": e,
                }
                for i, s, e in TRIALS
            ],
        ),
        [*EXP_KEYS, "trial_index"],
    )
    add(
        SOURCE,
        "default.experiment_freeviewing_event",
        _table(
            {
                **EXP_KEYS,
                "timestamp": pa.uint64(),
                "name": pa.string(),
                "trial_index": pa.uint32(),
            },
            [
                {**EXP_ROW, "timestamp": t, "name": n, "trial_index": i}
                for t, n, i in EVENTS
            ],
        ),
        [*EXP_KEYS, "timestamp", "name"],
    )
    labels = pa.struct(
        [("kilosort_label", pa.bool_()), ("bombcell_label", pa.string())]
    )
    add(
        SOURCE,
        "default.unit",
        _table(
            {
                "subject_id": pa.string(),
                "session_start_datetime": pa.timestamp("us"),
                "unit_id": pa.uint32(),
                "labels": labels,
            },
            [
                {
                    **SESSION_ROW,
                    "unit_id": u,
                    "labels": {"kilosort_label": u == 1, "bombcell_label": None},
                }
                for u in UNITS
            ],
        ),
        ["subject_id", "session_start_datetime", "unit_id"],
    )
    add(
        SOURCE,
        "default.spike_event",
        _table(
            {
                "subject_id": pa.string(),
                "session_start_datetime": pa.timestamp("us"),
                "timestamp": pa.uint64(),
                "unit_id": pa.uint32(),
                "spike_amplitude_uv": pa.float32(),
            },
            [
                {
                    **SESSION_ROW,
                    "timestamp": t,
                    "unit_id": u,
                    "spike_amplitude_uv": float(t + u),
                }
                for t in range(90, 261, 10)
                for u in UNITS
            ],
        ),
        ["subject_id", "session_start_datetime", "timestamp", "unit_id"],
    )
    add(
        SOURCE,
        "default.lfp_timeseries",
        _table(
            {
                "subject_id": pa.string(),
                "session_start_datetime": pa.timestamp("us"),
                "timestamp": pa.uint64(),
                "lfp_voltage_uv": pa.list_(pa.float32(), 4),
            },
            [
                {
                    **SESSION_ROW,
                    "timestamp": t,
                    "lfp_voltage_uv": [1.0, None, float(t), 0.5],
                }
                for t in range(95, 206, 5)
            ],
        ),
        ["subject_id", "session_start_datetime", "timestamp"],
    )
    add(
        SOURCE,
        "default.experiment_screen_capture_frame_270p",
        _table(
            {
                **EXP_KEYS,
                "screen_capture_import_version_id": pa.int64(),
                "timestamp": pa.int64(),
                "frame_index": pa.uint32(),
                "frame_jpeg": pa.large_binary(),
            },
            [
                {
                    **EXP_ROW,
                    "screen_capture_import_version_id": 0,
                    "timestamp": t,
                    "frame_index": i,
                    "frame_jpeg": b"\xff\xd8" + bytes([i]),
                }
                for i, t in enumerate(range(70, 251, 30))
            ],
        ),
        [*EXP_KEYS, "screen_capture_import_version_id", "timestamp", "frame_index"],
    )
    add(
        SOURCE,
        "default.sample_info",
        _table(
            {"region": pa.string(), "n_units": pa.int64()},
            [{"region": "V1", "n_units": 3}],
        ),
        ["region"],
    )


SUBSET_JSON = {
    "dataset": "default",
    "window_us": list(WINDOW),
    "unit_ids": KEPT_UNITS,
    "trial_table": "experiment_freeviewing_trial",
    "tables": {
        "session": {"mode": "whole"},
        "experiment": {"mode": "whole"},
        "experiment_freeviewing_trial": {"mode": "trials"},
        "experiment_freeviewing_event": {"mode": "window"},
        "unit": {"mode": "units"},
        "spike_event": {"mode": "window_units"},
        "lfp_timeseries": {"mode": "window"},
        "experiment_screen_capture_frame_270p": {"mode": "window"},
        "sample_info": {"mode": "whole"},
    },
}


def _source_schemas(client: FakeSpiral) -> dict[str, tuple[pa.Schema, list[str]]]:
    """Return each source table's schema and key columns, for ``derive_map``."""
    project = client.project(SOURCE)
    return {
        r.table: (
            project.table(f"default.{r.table}").schema().to_arrow(),
            project.table(f"default.{r.table}").key_schema.to_arrow().names,
        )
        for r in project.list_tables()
    }


def derived_map(client: FakeSpiral) -> dict[str, Any]:
    """The map ``derive-map`` builds for the synthetic source (decisions pending)."""
    return tool.derive_map(MINI_SPEC, "0.3.0", SOURCE, _source_schemas(client))


def decide(raw: dict[str, Any]) -> dict[str, Any]:
    """Resolve every pending decision the way a developer might."""
    raw = copy_module.deepcopy(raw)
    for t in raw["tables"].values():
        for c in t["columns"].values():
            if c["nullable"] is None:
                c["nullable"] = not c["key"]
        for g in t["groups"].values():
            if g["nullable"] is None:
                g["nullable"] = False
    return raw


@pytest.fixture
def client() -> FakeSpiral:
    """Return a fake Spiral client holding the synthetic source project."""
    c = FakeSpiral()
    build_source(c)
    return c


@pytest.fixture
def nmap(client: FakeSpiral):
    """Return the derived map with every pending decision resolved."""
    return tool.parse_map(decide(derived_map(client)))


@pytest.fixture
def subset():
    """Return the test subset."""
    return tool.parse_subset(SUBSET_JSON)


def make_target(client: FakeSpiral) -> str:
    """Create a marked target project and return its ID."""
    return tool.create_project(
        client,
        id_prefix="edi-sample-tutorial",
        allow_create="edi-sample-tutorial",
        description="edi-tutorial-v1",
        volume_id="vol_test",
        source_project=SOURCE,
    )


def run_copy(client: FakeSpiral, target: str, nmap: Any, subset: Any, **kw: Any):
    """Copy ``subset`` from the source into ``target``, allowing writes to it."""
    return tool.copy(
        client,
        source_project=SOURCE,
        target_project=target,
        allow_writes=target,
        nmap=nmap,
        subset=subset,
        **kw,
    )


def writes_to(client: FakeSpiral, project: str) -> list[tuple[str, str, str]]:
    """Return the logged mutating calls on ``project``."""
    return [entry for entry in client.log if entry[0] == project]


# ------------------------------------------------------------------------ derive-map


class TestDeriveMap:
    """``derive_map``, ``apply_decisions`` and ``render_map``."""

    def test_spec_nullability_and_keys(self, client):
        raw = derived_map(client)
        session = raw["tables"]["session"]["columns"]
        assert (
            session["subject_id"]["nullable"] is False and session["subject_id"]["key"]
        )
        assert session["booth"]["nullable"] is False
        assert session["session_notes"]["nullable"] is True
        assert raw["mismatches"] == []

    def test_tables_outside_the_spec_and_groups_are_pending(self, client):
        nm = tool.parse_map(derived_map(client))
        pending = nm.pending()
        assert set(pending) == {
            "experiment_screen_capture_frame_270p",
            "sample_info",
            "unit",
        }
        assert pending["unit"] == ["labels"]

    def test_type_difference_is_a_mismatch(self, client):
        schemas = _source_schemas(client)
        schema, keys = schemas["spike_event"]
        i = schema.get_field_index("spike_amplitude_uv")
        schemas["spike_event"] = (
            schema.set(i, pa.field("spike_amplitude_uv", pa.float64())),
            keys,
        )
        raw = tool.derive_map(MINI_SPEC, "0.3.0", SOURCE, schemas)
        assert any("spike_event.spike_amplitude_uv" in m for m in raw["mismatches"])

    def test_foreign_keys_use_the_parents_key_columns(self, client):
        raw = derived_map(client)
        fks = {
            fk["parent"]: fk["columns"]
            for fk in raw["tables"]["spike_event"]["foreign_keys"]
        }
        assert fks == {"unit": ["subject_id", "session_start_datetime", "unit_id"]}
        event_fks = {
            fk["parent"]
            for fk in raw["tables"]["experiment_freeviewing_event"]["foreign_keys"]
        }
        assert event_fks == {"experiment", "experiment_freeviewing_trial"}

    def test_decisions_resolve_every_pending_entry(self, client):
        decisions = {
            "decided_by": "test",
            "tables_not_in_spec": {"keys": "required", "values": "nullable"},
            "column_group_structs": "required",
            "accepted_unresolved": [
                {"child": "spike_event", "parent": "unit", "keys": [["x", "y", 1]]}
            ],
        }
        raw = tool.derive_map(
            MINI_SPEC, "0.3.0", SOURCE, _source_schemas(client), decisions
        )
        nm = tool.parse_map(raw)
        assert nm.pending() == {}
        frames = nm.tables["experiment_screen_capture_frame_270p"].columns
        assert frames["timestamp"].nullable is False
        assert frames["frame_jpeg"].nullable is True
        assert nm.tables["unit"].groups["labels"].nullable is False
        assert nm.tables["spike_event"].foreign_keys[0].accepted_unresolved == {
            ("x", "y", 1)
        }

    def test_a_decision_for_a_missing_foreign_key_is_refused(self, client):
        decisions = {
            "accepted_unresolved": [{"child": "session", "parent": "unit", "keys": []}]
        }
        with pytest.raises(tool.ToolError, match="no foreign key"):
            tool.derive_map(
                MINI_SPEC, "0.3.0", SOURCE, _source_schemas(client), decisions
            )

    def test_render_map_lists_every_column(self, client):
        text = tool.render_map(tool.parse_map(derived_map(client)))
        assert "| `spike_amplitude_uv` | `float` | `f32` |  | **required** |" in text
        assert "| `labels` (column group) | struct | — | | pending |" in text


# ---------------------------------------------------------------------------- guards


class TestTargetGuards:
    """The guards that refuse to create or write an unsafe target project."""

    def test_allow_writes_must_repeat_the_target(self, client, nmap, subset):
        target = make_target(client)
        with pytest.raises(tool.TargetRefusedError, match="allow-writes"):
            tool.copy(
                client,
                source_project=SOURCE,
                target_project=target,
                allow_writes=target + "x",
                nmap=nmap,
                subset=subset,
            )

    def test_source_is_refused_as_target(self, client, nmap, subset):
        with pytest.raises(tool.TargetRefusedError):
            run_copy(client, SOURCE, nmap, subset)

    @pytest.mark.parametrize(
        "target", ["edi-sample-v2-303737", "edi-sample-v1-tutorial"]
    )
    def test_edi_sample_v_prefix_is_refused(self, client, nmap, subset, target):
        with pytest.raises(tool.TargetRefusedError, match="edi-sample-v"):
            run_copy(client, target, nmap, subset)

    def test_project_without_marker_is_refused(self, client, nmap, subset):
        client.add_table(
            "edi-sample-46d-924415",
            "default.subject",
            _table({"subject_id": pa.string()}, [{"subject_id": "x"}]),
            ["subject_id"],
        )
        with pytest.raises(tool.TargetRefusedError, match="create step"):
            run_copy(client, "edi-sample-46d-924415", nmap, subset)
        assert writes_to(client, "edi-sample-46d-924415") == []

    def test_project_with_a_foreign_table_is_refused(self, client, nmap, subset):
        target = make_target(client)
        client.add_table(
            target, "default.other", _table({"k": pa.string()}, [{"k": "x"}]), ["k"]
        )
        with pytest.raises(tool.TargetRefusedError, match="did not write"):
            run_copy(client, target, nmap, subset)

    def test_create_refuses_a_forbidden_prefix(self, client):
        with pytest.raises(tool.TargetRefusedError):
            tool.create_project(
                client,
                id_prefix="edi-sample-v1",
                allow_create="edi-sample-v1",
                description="x",
                volume_id=None,
                source_project=SOURCE,
            )

    def test_create_requires_the_prefix_twice(self, client):
        with pytest.raises(tool.TargetRefusedError):
            tool.create_project(
                client,
                id_prefix="edi-sample-tutorial",
                allow_create=None,
                description="x",
                volume_id=None,
                source_project=SOURCE,
            )

    def test_create_makes_a_marked_project_on_the_volume(self, client):
        target = make_target(client)
        assert target.startswith("edi-sample-tutorial-")
        assert (target, "create_project", "vol_test") in client.log
        marker = tool.read_marker(client.project(target))
        assert marker["source_project"] == SOURCE

    def test_pending_decisions_block_the_copy(self, client, subset):
        target = make_target(client)
        pending = tool.parse_map(derived_map(client))
        with pytest.raises(tool.PendingDecisionError):
            run_copy(client, target, pending, subset)
        assert [op for _, op, _ in writes_to(client, target)] == [
            "create_project",
            "create_table",
            "set_metadata",
        ]


# ------------------------------------------------------------------------------ copy


class TestCopy:
    """``copy``: what it writes, how it enforces the map, and how it re-runs."""

    def test_copies_the_subset_and_never_writes_the_source(self, client, nmap, subset):
        target = make_target(client)
        report = run_copy(client, target, nmap, subset)
        assert writes_to(client, SOURCE) == []
        rows = {t.name: t.rows for t in report.tables}
        # spikes at 100..190 for units 1 and 3
        assert rows["spike_event"] == 20
        assert (
            sorted(
                set(
                    client.rows(target, "default.spike_event")
                    .column("unit_id")
                    .to_pylist()
                )
            )
            == KEPT_UNITS
        )
        assert rows["lfp_timeseries"] == 20  # 100, 105, ..., 195
        assert client.rows(
            target, "default.experiment_screen_capture_frame_270p"
        ).column("timestamp").to_pylist() == [100, 130, 160, 190]
        assert rows["experiment_freeviewing_event"] == 3  # 120, 170, 199
        assert report.kept_trials == [1, 2, 3]
        assert rows["unit"] == 2 and rows["session"] == 1 and rows["sample_info"] == 1
        assert report.problems == []

    def test_writes_the_maps_nullability_and_spiral_stores_it_nullable(
        self, client, nmap, subset
    ):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        written = client.written_schemas[(target, "default.spike_event")][0]
        assert all(not f.nullable for f in written)
        session = client.written_schemas[(target, "default.session")][0]
        assert session.field("session_notes").nullable
        assert not session.field("booth").nullable
        # SpiralDB keeps every top-level column nullable, keys included ...
        stored = client.project(target).table("default.spike_event").schema().to_arrow()
        assert all(f.nullable for f in stored)
        assert all(
            f.nullable
            for f in client.project(target)
            .table("default.spike_event")
            .key_schema.to_arrow()
        )
        # ... and the types stay exactly the source's, list item flags included
        lfp = client.project(target).table("default.lfp_timeseries").schema().to_arrow()
        assert lfp.field("lfp_voltage_uv").type == pa.list_(pa.float32(), 4)

    def test_column_group_is_written_required_with_its_fields(
        self, client, nmap, subset
    ):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        labels = client.written_schemas[(target, "default.unit")][0].field("labels")
        assert not labels.nullable
        assert [f.name for f in labels.type] == ["kilosort_label", "bombcell_label"]
        assert all(f.nullable for f in labels.type)

    def test_a_null_in_a_required_column_fails_loudly(self, client, nmap, subset):
        data = client.rows(SOURCE, "default.spike_event")
        i = data.schema.get_field_index("spike_amplitude_uv")
        values = data.column(i).to_pylist()
        values[data.column("timestamp").to_pylist().index(150)] = None
        client.add_table(
            SOURCE,
            "default.spike_event",
            data.set_column(i, data.schema.field(i), pa.array(values, pa.float32())),
            ["subject_id", "session_start_datetime", "timestamp", "unit_id"],
        )
        target = make_target(client)
        with pytest.raises(tool.NullInRequiredColumnError) as info:
            run_copy(client, target, nmap, subset)
        assert info.value.path == ("spike_amplitude_uv",)
        assert any("spike_event" in note for note in info.value.__notes__)
        # the aborted transaction left no rows behind
        assert client.project(target).table(
            "default.spike_event"
        ).schema().to_arrow().names == [
            "subject_id",
            "session_start_datetime",
            "timestamp",
            "unit_id",
        ]

    def test_a_null_in_a_required_group_field_fails_loudly(self, client, subset):
        raw = decide(derived_map(client))
        raw["tables"]["unit"]["groups"]["labels"]["fields"]["bombcell_label"][
            "nullable"
        ] = False
        target = make_target(client)
        with pytest.raises(tool.NullInRequiredColumnError) as info:
            run_copy(client, target, tool.parse_map(raw), subset)
        assert info.value.path == ("labels", "bombcell_label")

    def test_source_type_drift_is_refused(self, client, nmap, subset):
        data = client.rows(SOURCE, "default.session")
        i = data.schema.get_field_index("booth")
        client.add_table(
            SOURCE,
            "default.session",
            data.set_column(
                i,
                pa.field("booth", pa.large_string()),
                data.column(i).cast(pa.large_string()),
            ),
            ["subject_id", "session_start_datetime"],
        )
        target = make_target(client)
        with pytest.raises(tool.SchemaMismatchError, match="booth"):
            run_copy(client, target, nmap, subset)

    def test_records_provenance_on_the_marker_table(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        meta = client.project(target).table(tool.MARKER_TABLE).get_metadata()
        record = json.loads(meta[tool.PROVENANCE_KEY])
        assert record["source_project"] == SOURCE
        assert record["window_us"] == list(WINDOW)
        assert record["unit_ids"] == KEPT_UNITS
        assert record["kept_trials"] == [1, 2, 3]
        assert record["source_sample_info"] == [{"region": "V1", "n_units": 3}]
        assert record["rows"]["spike_event"] == 20

    def test_verify_requires_the_provenance(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        stored = client._projects[target][tool.MARKER_TABLE]
        del stored.metadata[tool.PROVENANCE_KEY]
        with pytest.raises(tool.VerificationError, match="provenance"):
            tool.verify(
                client,
                source_project=SOURCE,
                target_project=target,
                nmap=nmap,
                subset=subset,
            )

    def test_dry_run_writes_nothing(self, client, nmap, subset):
        target = make_target(client)
        before = list(client.log)
        report = run_copy(client, target, nmap, subset, dry_run=True)
        assert client.log == before
        assert {t.action for t in report.tables} == {"would write"}

    def test_plan_reads_only(self, client, subset):
        before = list(client.log)
        report = tool.plan(client, SOURCE, tool.parse_map(derived_map(client)), subset)
        assert client.log == before
        assert set(report.pending) == {
            "experiment_screen_capture_frame_270p",
            "sample_info",
            "unit",
        }
        assert sum(t.rows for t in report.tables) > 0

    def test_rerun_skips_complete_tables(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        before = {
            name: client.rows(target, f"default.{name}")
            for name in SUBSET_JSON["tables"]
        }
        log_size = len(client.log)
        report = run_copy(client, target, nmap, subset)
        assert {t.action for t in report.tables} == {"skip"}
        assert len(client.log) == log_size
        for name, table in before.items():
            assert client.rows(target, f"default.{name}").equals(table)

    def test_rerun_after_a_failure_completes(self, client, nmap, subset):
        target = make_target(client)
        good = client.rows(SOURCE, "default.spike_event")
        i = good.schema.get_field_index("spike_amplitude_uv")
        bad = good.set_column(
            i, good.schema.field(i), pa.array([None] * good.num_rows, pa.float32())
        )
        keys = ["subject_id", "session_start_datetime", "timestamp", "unit_id"]
        client.add_table(SOURCE, "default.spike_event", bad, keys)
        with pytest.raises(tool.NullInRequiredColumnError):
            run_copy(client, target, nmap, subset)
        client.add_table(SOURCE, "default.spike_event", good, keys)
        report = run_copy(client, target, nmap, subset)
        actions = {t.name: t.action for t in report.tables}
        assert actions["spike_event"] == "rewrite"
        assert actions["session"] == "skip"
        tool.verify(
            client,
            source_project=SOURCE,
            target_project=target,
            nmap=nmap,
            subset=subset,
        )

    def test_a_changed_subset_is_refused_for_complete_tables(
        self, client, nmap, subset
    ):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        changed = tool.parse_subset({**SUBSET_JSON, "unit_ids": [1]})
        with pytest.raises(tool.StaleTableError):
            run_copy(client, target, nmap, changed)

    def test_a_foreign_key_annotation_does_not_make_tables_stale(
        self, client, nmap, subset
    ):
        # removing an accepted exception changes no copied value, so a re-run skips
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        raw = copy_module.deepcopy(dict(nmap.raw))
        raw["tables"]["spike_event"]["foreign_keys"][0]["accepted_unresolved"] = [
            ["Goliath", SST.isoformat(), 9]
        ]
        report = run_copy(client, target, tool.parse_map(raw), subset)
        assert {t.action for t in report.tables} == {"skip"}

    def test_a_nullability_change_makes_the_table_stale(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        raw = copy_module.deepcopy(dict(nmap.raw))
        raw["tables"]["session"]["columns"]["session_notes"]["nullable"] = False
        raw["tables"]["session"]["columns"]["session_notes"]["key"] = False
        with pytest.raises(tool.StaleTableError):
            run_copy(client, target, tool.parse_map(raw), subset)

    def test_window_literals_carry_the_column_type(self, client, nmap, subset):
        # the fake, like SpiralDB, refuses a u64 column compared with an i64 literal;
        # the frames table keys time as int64 and the others as uint64
        target = make_target(client)
        report = run_copy(client, target, nmap, subset)
        assert {t.name: t.rows for t in report.tables}[
            "experiment_screen_capture_frame_270p"
        ] == 4


# ---------------------------------------------------------------------------- verify


class TestVerify:
    """``verify``: a clean copy passes; changed data, schemas and references fail."""

    def test_passes_after_copy(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        report = tool.verify(
            client,
            source_project=SOURCE,
            target_project=target,
            nmap=nmap,
            subset=subset,
        )
        assert report.problems == []

    def test_detects_a_changed_value(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        stored = client._projects[target]["default.spike_event"]
        assert stored.data is not None
        i = stored.data.schema.get_field_index("spike_amplitude_uv")
        values = stored.data.column(i).to_pylist()
        values[0] += 1
        stored.data = stored.data.set_column(
            i, stored.data.schema.field(i), pa.array(values, pa.float32())
        )
        with pytest.raises(tool.VerificationError, match="spike_amplitude_uv"):
            tool.verify(
                client,
                source_project=SOURCE,
                target_project=target,
                nmap=nmap,
                subset=subset,
            )

    def test_detects_a_missing_row_and_a_dangling_reference(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        stored = client._projects[target]["default.experiment_freeviewing_trial"]
        assert stored.data is not None
        stored.data = stored.data.filter(
            pc.not_equal(stored.data.column("trial_index"), pa.scalar(3, pa.uint32()))
        )
        with pytest.raises(tool.VerificationError) as info:
            tool.verify(
                client,
                source_project=SOURCE,
                target_project=target,
                nmap=nmap,
                subset=subset,
            )
        text = "\n".join(info.value.problems)
        assert "row count" in text
        assert "experiment_freeviewing_event -> experiment_freeviewing_trial" in text

    def test_an_accepted_unresolved_reference_is_reported_not_failed(
        self, client, subset
    ):
        # the source's unit table loses unit 3, whose spikes are kept: a broken
        # reference in the source itself, like lfp_location -> atlas_label live
        units = client.rows(SOURCE, "default.unit")
        client.add_table(
            SOURCE,
            "default.unit",
            units.filter(
                pc.not_equal(units.column("unit_id"), pa.scalar(3, pa.uint32()))
            ),
            ["subject_id", "session_start_datetime", "unit_id"],
        )
        raw = decide(derived_map(client))
        report = tool.plan(client, SOURCE, tool.parse_map(raw), subset)
        assert any(p.startswith("spike_event -> unit") for p in report.problems)
        fk = raw["tables"]["spike_event"]["foreign_keys"][0]
        fk["accepted_unresolved"] = [["Goliath", SST.isoformat(), 3]]
        report = tool.plan(client, SOURCE, tool.parse_map(raw), subset)
        assert report.problems == []
        assert len(report.accepted) == 1 and "spike_event -> unit" in report.accepted[0]

    def test_an_accepted_reference_that_now_resolves_is_not_reported(
        self, client, nmap, subset
    ):
        # the source was fixed after the exception was recorded: nothing to report,
        # and nothing fails because the accepted key is no longer missing
        raw = decide(derived_map(client))
        raw["tables"]["spike_event"]["foreign_keys"][0]["accepted_unresolved"] = [
            ["Goliath", SST.isoformat(), 3]
        ]
        accepted_map = tool.parse_map(raw)
        report = tool.plan(client, SOURCE, accepted_map, subset)
        assert report.problems == [] and report.accepted == []
        target = make_target(client)
        run_copy(client, target, accepted_map, subset)
        verified = tool.verify(
            client,
            source_project=SOURCE,
            target_project=target,
            nmap=accepted_map,
            subset=subset,
        )
        assert verified.problems == [] and verified.accepted == []

    def test_detects_a_null_in_a_required_column(self, client, nmap, subset):
        # SpiralDB stores the column nullable, so only the data can show the breach
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        stored = client._projects[target]["default.session"]
        assert stored.data is not None
        i = stored.data.schema.get_field_index("booth")
        stored.data = stored.data.set_column(
            i, stored.data.schema.field(i), pa.array([None], pa.string())
        )
        with pytest.raises(
            tool.VerificationError, match="'booth' is declared non-nullable"
        ):
            tool.verify(
                client,
                source_project=SOURCE,
                target_project=target,
                nmap=nmap,
                subset=subset,
            )

    def test_detects_a_schema_that_differs_from_the_source(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        stored = client._projects[target]["default.session"]
        assert stored.data is not None
        stored.data = stored.data.cast(
            pa.schema(
                [
                    f.with_type(pa.large_string()) if f.name == "booth" else f
                    for f in stored.data.schema
                ]
            )
        )
        with pytest.raises(tool.VerificationError, match="session: schema"):
            tool.verify(
                client,
                source_project=SOURCE,
                target_project=target,
                nmap=nmap,
                subset=subset,
            )


class TestCommittedFiles:
    """The committed map and subset, and the command-line entry point."""

    def test_map_and_subset_load_and_agree(self):
        nm = tool.load_map()
        sub = tool.load_subset()
        assert nm.source_project == SOURCE
        assert set(sub.tables) <= set(nm.tables)
        start, stop = sub.window
        assert 0 < stop - start <= 200_000_000  # at most 200 s

    def test_cli_render_map(self, capsys):
        assert tool.main(["render-map"]) == 0
        assert "### `spike_event`" in capsys.readouterr().out

    def test_cli_plan_uses_the_given_client(self, client, capsys, tmp_path):
        map_path = tmp_path / "map.json"
        map_path.write_text(json.dumps(derived_map(client)))
        subset_path = tmp_path / "subset.json"
        subset_path.write_text(json.dumps(SUBSET_JSON))
        code = tool.main(
            ["--map", str(map_path), "--subset", str(subset_path), "plan"],
            client_factory=lambda: client,
        )
        assert code == 0
        assert "pending decisions" in capsys.readouterr().out


class _StubSource:
    """Stands in for an Orcapod source: only ``output_schema`` is used."""

    def __init__(self, tags: dict[str, Any], data: dict[str, Any]) -> None:
        """Hold the tag and data schemas to return."""
        self._schemas = (tags, data)

    def output_schema(self) -> tuple[dict[str, Any], dict[str, Any]]:
        """Return the ``(tag schema, data schema)`` pair."""
        return self._schemas


class TestOrcapodCheck:
    """``orcapod_check``: column types against the map's nullability."""

    def test_plain_types_for_required_and_optional_for_nullable(self, nmap, subset):
        def make(project: str, table: str, dataset: str) -> _StubSource:
            """Return a stub source with plain types for required columns."""
            if table == "session":
                return _StubSource(
                    {"subject_id": str, "session_start_datetime": datetime.datetime},
                    {"booth": str, "session_notes": str | None},
                )
            return _StubSource({}, {})

        lines, problems = tool.orcapod_check("p", nmap, subset, make)
        assert problems == []
        assert "lfp_timeseries: skipped (fixed_size_list, ITL-927)" in lines

    def test_a_required_column_seen_as_optional_is_a_problem(self, nmap, subset):
        def make(project: str, table: str, dataset: str) -> _StubSource:
            """Return a stub source that types a required key column as optional."""
            if table == "session":
                return _StubSource(
                    {
                        "subject_id": str | None,
                        "session_start_datetime": datetime.datetime,
                    },
                    {"booth": str, "session_notes": str | None},
                )
            return _StubSource({}, {})

        _, problems = tool.orcapod_check("p", nmap, subset, make)
        assert problems == [
            "session: nullability differs from the map: ['subject_id: str | None']"
        ]


class TestProbe:
    """``probe_nullability``: writes only a scratch table, on a marked target."""

    def test_probe_writes_only_a_scratch_table_and_drops_it(self, client):
        target = make_target(client)
        result = tool.probe_nullability(
            client, target_project=target, allow_writes=target, source_project=SOURCE
        )
        assert {r.table for r in client.project(target).list_tables()} == {
            "edi_tutorial_build"
        }
        assert {
            e[2] for e in writes_to(client, target) if e[1] != "create_project"
        } <= {
            tool.PROBE_TABLE,
            tool.MARKER_TABLE,
        }
        assert (
            result["null_v_declared_non_null"]["raised"] is None
            or "v" in (result["null_v_declared_non_null"]["raised"])
        )
        assert "after_non_null_write" in result

    def test_probe_refuses_a_target_without_marker(self, client):
        with pytest.raises(tool.TargetRefusedError):
            tool.probe_nullability(
                client,
                target_project="test-orcapod-885785",
                allow_writes="test-orcapod-885785",
                source_project=SOURCE,
            )


UNIT_KEYS = ["subject_id", "session_start_datetime", "unit_id"]


class TestReplace:
    """``copy`` with ``replace``: dropping and copying chosen tables again."""

    def test_a_source_fix_is_picked_up_by_replacing_one_table(self, client, subset):
        # the source's unit table first lacks unit 3 (an accepted exception), then the
        # owner adds it back, as with atlas_label 'White Matter' live
        units = client.rows(SOURCE, "default.unit")
        missing = units.filter(
            pc.not_equal(units.column("unit_id"), pa.scalar(3, pa.uint32()))
        )
        client.add_table(SOURCE, "default.unit", missing, UNIT_KEYS)
        raw = decide(derived_map(client))
        raw["tables"]["spike_event"]["foreign_keys"][0]["accepted_unresolved"] = [
            ["Goliath", SST.isoformat(), 3]
        ]
        nm = tool.parse_map(raw)
        target = make_target(client)
        first = run_copy(client, target, nm, subset)
        assert len(first.accepted) == 1

        client.add_table(SOURCE, "default.unit", units, UNIT_KEYS)
        before = len(client.log)
        report = run_copy(client, target, nm, subset, tables=["unit"], replace=True)
        assert [(r.name, r.action, r.rows) for r in report.tables] == [
            ("unit", "replace", 2)
        ]
        touched = {e[2] for e in client.log[before:] if e[1] != "set_metadata"}
        assert touched == {"default.unit"}
        verified = tool.verify(
            client, source_project=SOURCE, target_project=target, nmap=nm, subset=subset
        )
        assert verified.problems == [] and verified.accepted == []
        record = json.loads(
            client.project(target)
            .table(tool.MARKER_TABLE)
            .get_metadata()[tool.PROVENANCE_KEY]
        )
        assert record["rows"]["unit"] == 2 and record["rows"]["spike_event"] == 20

    def test_replacing_twice_gives_the_same_table(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        run_copy(client, target, nmap, subset, tables=["session"], replace=True)
        once = client.rows(target, "default.session")
        run_copy(client, target, nmap, subset, tables=["session"], replace=True)
        assert client.rows(target, "default.session").equals(once)

    def test_replace_needs_tables(self, client, nmap, subset):
        target = make_target(client)
        with pytest.raises(tool.ToolError, match="--replace needs --tables"):
            run_copy(client, target, nmap, subset, replace=True)

    def test_unknown_tables_are_refused(self, client, nmap, subset):
        target = make_target(client)
        with pytest.raises(tool.ToolError, match="not in the subset"):
            run_copy(client, target, nmap, subset, tables=["atlas"], replace=True)

    def test_dry_run_replace_writes_nothing(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        before = list(client.log)
        report = run_copy(
            client, target, nmap, subset, tables=["session"], replace=True, dry_run=True
        )
        assert client.log == before
        assert [r.action for r in report.tables] == ["would replace"]


class _RecordWriter:
    """Stands in for Orcapod's ``write_nullability_record`` (ITL-936, not on main)."""

    def __init__(self) -> None:
        """Start with no calls and no recorded schemas."""
        self.calls: list[tuple[Any, pa.Schema]] = []
        self.current: dict[int, pa.Schema] = {}

    def __call__(self, table: Any, schema: pa.Schema) -> bool:
        """Record ``schema`` on ``table``; return whether the record changed."""
        self.calls.append((table, schema))
        key = id(table._stored)
        changed = self.current.get(key) != schema
        if changed:
            table.set_metadata({"__record__": str(schema).encode()})
        self.current[key] = schema
        return changed


class TestAnnotate:
    """``annotate``: recording the written nullability in table metadata."""

    def _annotate(self, client, target, nmap, subset, writer):
        """Run ``annotate`` on ``target`` with ``writer`` as the record writer."""
        return tool.annotate(
            client,
            source_project=SOURCE,
            target_project=target,
            allow_writes=target,
            nmap=nmap,
            subset=subset,
            write_record=writer,
        )

    def test_records_the_schema_the_copy_wrote_metadata_only(
        self, client, nmap, subset
    ):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        writer = _RecordWriter()
        before = len(client.log)
        lines = self._annotate(client, target, nmap, subset, writer)
        assert all(line.endswith("recorded") for line in lines)
        assert {e[1] for e in client.log[before:]} == {"set_metadata"}
        declared = {tbl._ident: schema for tbl, schema in writer.calls}
        for name in SUBSET_JSON["tables"]:
            assert (
                declared[f"default.{name}"]
                == client.written_schemas[(target, f"default.{name}")][0]
            )
        again = self._annotate(client, target, nmap, subset, writer)
        assert all(line.endswith("unchanged") for line in again)

    def test_a_null_in_the_target_blocks_the_record(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        stored = client._projects[target]["default.session"]
        assert stored.data is not None
        i = stored.data.schema.get_field_index("booth")
        stored.data = stored.data.set_column(
            i, stored.data.schema.field(i), pa.array([None], pa.string())
        )
        writer = _RecordWriter()
        with pytest.raises(tool.NullInRequiredColumnError):
            self._annotate(client, target, nmap, subset, writer)
        assert all(tbl._ident != "default.session" for tbl, _ in writer.calls)

    def test_orcapod_without_the_record_helper_is_a_clear_error(self, monkeypatch):
        import orcapod.databases

        monkeypatch.delattr(
            orcapod.databases, "write_nullability_record", raising=False
        )
        with pytest.raises(tool.ToolError, match="ITL-936"):
            tool.load_record_writer()


class TestFloatComparison:
    """verify compares floats bit for bit: NaN equals itself, -0.0 differs from 0.0.

    Live, eye_x_deg (gaze) and unit quality metrics hold NaN, and Arrow's
    ``equals`` treats NaN as unequal to itself (ITL-933 verify run, November 11, 2026).
    """

    def _with_nans(self, client: FakeSpiral) -> None:
        """Put NaN (and -0.0) values in the source's float and float-list columns."""
        data = client.rows(SOURCE, "default.spike_event")
        i = data.schema.get_field_index("spike_amplitude_uv")
        values = [
            float("nan") if n % 2 else v
            for n, v in enumerate(data.column(i).to_pylist())
        ]
        client.add_table(
            SOURCE,
            "default.spike_event",
            data.set_column(i, data.schema.field(i), pa.array(values, pa.float32())),
            ["subject_id", "session_start_datetime", "timestamp", "unit_id"],
        )
        lfp = client.rows(SOURCE, "default.lfp_timeseries")
        j = lfp.schema.get_field_index("lfp_voltage_uv")
        rows = [[float("nan"), None, -0.0, 1.0] for _ in range(lfp.num_rows)]
        client.add_table(
            SOURCE,
            "default.lfp_timeseries",
            lfp.set_column(
                j, lfp.schema.field(j), pa.array(rows, lfp.schema.field(j).type)
            ),
            ["subject_id", "session_start_datetime", "timestamp"],
        )

    def _verify(self, client: FakeSpiral, target: str, nmap: Any, subset: Any):
        """Run ``verify`` on ``target``."""
        return tool.verify(
            client,
            source_project=SOURCE,
            target_project=target,
            nmap=nmap,
            subset=subset,
        )

    def test_nan_values_copied_unchanged_verify(self, client, nmap, subset):
        self._with_nans(client)
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        assert self._verify(client, target, nmap, subset).problems == []

    def test_a_different_nan_payload_is_detected(self, client, nmap, subset):
        self._with_nans(client)
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        stored = client._projects[target]["default.spike_event"]
        assert stored.data is not None
        i = stored.data.schema.get_field_index("spike_amplitude_uv")
        col = stored.data.column(i).combine_chunks()
        buffer = col.buffers()[1]
        assert buffer is not None
        raw = np.frombuffer(buffer.to_pybytes(), dtype=np.uint32)[: len(col)].copy()
        raw[np.isnan(col.to_numpy(zero_copy_only=False))] = (
            0x7FC00001  # another quiet NaN
        )
        stored.data = stored.data.set_column(
            i, stored.data.schema.field(i), pa.array(raw.view(np.float32), pa.float32())
        )
        with pytest.raises(tool.VerificationError, match="spike_amplitude_uv"):
            self._verify(client, target, nmap, subset)

    def test_negative_zero_is_not_zero(self, client, nmap, subset):
        self._with_nans(client)
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        stored = client._projects[target]["default.lfp_timeseries"]
        assert stored.data is not None
        j = stored.data.schema.get_field_index("lfp_voltage_uv")
        field = stored.data.schema.field(j)
        rows = stored.data.column(j).to_pylist()
        rows[0] = [rows[0][0], None, 0.0, rows[0][3]]
        stored.data = stored.data.set_column(j, field, pa.array(rows, field.type))
        with pytest.raises(tool.VerificationError, match="lfp_voltage_uv"):
            self._verify(client, target, nmap, subset)


class TestNullabilityRecordChecks:
    """annotate --dry-run, the list-item null check, and verify against the record."""

    def test_a_null_list_item_declared_non_null_is_refused(self):
        item = pa.field("item", pa.float32(), False)
        field = pa.field("l", pa.list_(item, 2), False)
        # pyarrow builds this array without checking the item's non-null flag
        arr = pa.FixedSizeListArray.from_arrays(
            pa.array([1.0, None], pa.float32()),
            type=field.type,  # pyright: ignore[reportArgumentType]
        )
        batch = pa.RecordBatch.from_arrays([arr], names=["l"])
        with pytest.raises(tool.NullInRequiredColumnError) as info:
            tool.conform_batch(batch, pa.schema([field]))
        assert info.value.path == ("l", "[]")

    def test_a_null_nested_list_item_declared_non_null_is_refused(self):
        inner = pa.list_(pa.field("item", pa.int32(), False))
        field = pa.field("l", pa.list_(pa.field("item", inner, False)), False)
        values = pa.ListArray.from_arrays(
            pa.array([0, 2], pa.int32()),
            pa.array([1, None], pa.int32()),
            type=inner,  # pyright: ignore[reportArgumentType]
        )
        arr = pa.ListArray.from_arrays(
            pa.array([0, 1], pa.int32()),
            values,
            type=field.type,  # pyright: ignore[reportArgumentType]
        )
        batch = pa.RecordBatch.from_arrays([arr], names=["l"])
        with pytest.raises(tool.NullInRequiredColumnError) as info:
            tool.conform_batch(batch, pa.schema([field]))
        assert info.value.path == ("l", "[]", "[]")

    def test_a_null_below_a_struct_between_two_lists_is_refused(self):
        xs_type = pa.list_(pa.field("item", pa.int32(), False))
        struct_type = pa.struct([pa.field("xs", xs_type, True)])
        field = pa.field("l", pa.list_(pa.field("item", struct_type, True)), True)
        xs = pa.ListArray.from_arrays(
            pa.array([0, 1], pa.int32()),
            pa.array([None], pa.int32()),
            type=xs_type,  # pyright: ignore[reportArgumentType]
        )
        items = pa.StructArray.from_arrays([xs], fields=list(struct_type))
        arr = pa.ListArray.from_arrays(
            pa.array([0, 1], pa.int32()),
            items,
            type=field.type,  # pyright: ignore[reportArgumentType]
        )
        batch = pa.RecordBatch.from_arrays([arr], names=["l"])
        with pytest.raises(tool.NullInRequiredColumnError) as info:
            tool.conform_batch(batch, pa.schema([field]))
        assert info.value.path == ("l", "[]", "xs", "[]")

    def test_a_null_in_a_nullable_nested_item_is_accepted(self):
        inner = pa.list_(pa.field("item", pa.int32(), True))
        field = pa.field("l", pa.list_(pa.field("item", inner, False)), False)
        arr = pa.array([[[1, None]]], field.type)
        batch = pa.RecordBatch.from_arrays([arr], names=["l"])
        assert tool.conform_batch(batch, pa.schema([field])).num_rows == 1

    def test_non_null_paths_follow_the_record_layout(self):
        schema = pa.schema(
            [
                pa.field("k", pa.int64(), False),
                pa.field("w", pa.int32(), True),
                pa.field(
                    "g",
                    pa.struct(
                        [pa.field("x", pa.int32(), False), pa.field("y", pa.int32())]
                    ),
                    False,
                ),
                pa.field("l", pa.list_(pa.field("item", pa.float32(), False), 2), True),
            ]
        )
        assert tool.non_null_paths(schema) == [("g",), ("g", "x"), ("k",), ("l", "[]")]

    def test_annotate_dry_run_lists_paths_and_writes_nothing(
        self, client, nmap, subset
    ):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        before = list(client.log)
        lines = tool.annotate(
            client,
            source_project=SOURCE,
            target_project=target,
            allow_writes=target,
            nmap=nmap,
            subset=subset,
            write_record=None,
            dry_run=True,
        )
        assert client.log == before
        session = next(line for line in lines if line.startswith("session:"))
        assert "booth" in session and "session_notes" not in session

    def _verify(self, client, target, nmap, subset, reader):
        """Run ``verify`` on ``target``, reading nullability records with ``reader``."""
        return tool.verify(
            client,
            source_project=SOURCE,
            target_project=target,
            nmap=nmap,
            subset=subset,
            read_record=reader,
        )

    def test_verify_checks_each_record_against_the_map(self, client, nmap, subset):
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        written = client.written_schemas

        def no_record(metadata: Any) -> Any:
            """Report that no table carries a nullability record."""
            return None

        report = self._verify(client, target, nmap, subset, no_record)
        assert len(report.notes) == len(SUBSET_JSON["tables"])

        for (pid, ident), schemas in written.items():
            if pid == target:
                paths = sorted(tool.non_null_paths(schemas[0]))
                client._projects[target][ident].metadata["rec"] = json.dumps(
                    paths
                ).encode()

        def reader(metadata: Any) -> Any:
            """Read the record the test stores under ``rec``."""
            raw = metadata.get("rec")
            return None if raw is None else frozenset(map(tuple, json.loads(raw)))

        report = self._verify(client, target, nmap, subset, reader)
        assert report.problems == [] and report.notes == []

        client._projects[target]["default.session"].metadata["rec"] = (
            b'[["subject_id"]]'
        )
        with pytest.raises(tool.VerificationError, match="session: nullability record"):
            self._verify(client, target, nmap, subset, reader)


class TestReviewRound1:
    """Fixes from the first CodeRabbit round on PR #307."""

    def test_an_incomplete_table_is_rebuilt_without_old_rows(
        self, client, nmap, subset
    ):
        # data committed but the status did not; a retry with another subset must not
        # keep the earlier attempt's rows
        target = make_target(client)
        run_copy(client, target, nmap, subset)
        del client._projects[target]["default.spike_event"].metadata[tool.STATUS_KEY]
        narrower = tool.parse_subset({**SUBSET_JSON, "unit_ids": [1]})
        report = run_copy(client, target, nmap, narrower, tables=["spike_event"])
        assert [r.action for r in report.tables] == ["rewrite"]
        units = client.rows(target, "default.spike_event").column("unit_id").to_pylist()
        assert set(units) == {1}

    def test_an_empty_selection_fails_and_is_not_marked_complete(self, client, nmap):
        target = make_target(client)
        empty = tool.parse_subset({**SUBSET_JSON, "unit_ids": [99]})
        with pytest.raises(tool.ToolError, match="no rows"):
            run_copy(client, target, nmap, empty)
        meta = client.project(target).table("default.spike_event").get_metadata()
        assert tool.STATUS_KEY not in meta

    def test_source_key_columns_must_match_the_map(self, client, nmap, subset):
        data = client.rows(SOURCE, "default.session")
        client.add_table(
            SOURCE, "default.session", data, ["session_start_datetime", "subject_id"]
        )
        target = make_target(client)
        with pytest.raises(tool.SchemaMismatchError, match="source key columns"):
            run_copy(client, target, nmap, subset)

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            (pa.array([-0.0], pa.float32()), pa.array([0.0], pa.float32())),
            (
                pa.array([[1.0, -0.0]], pa.list_(pa.float64())),
                pa.array([[1.0, 0.0]], pa.list_(pa.float64())),
            ),
            (
                pa.array([{"x": -0.0}], pa.struct([("x", pa.float32())])),
                pa.array([{"x": 0.0}], pa.struct([("x", pa.float32())])),
            ),
        ],
        ids=["top-level", "list", "struct"],
    )
    def test_negative_zero_differs_without_nan(self, left, right):
        assert left.equals(right)  # Arrow's own equality would accept it
        assert not tool.identical(left, right)
        assert tool.identical(left, left)

    def test_a_leftover_probe_table_is_accepted_and_dropped(self, client):
        # a probe killed before its cleanup leaves its table, marked like the others
        target = make_target(client)
        project = client.project(target)
        marker = project.table(tool.MARKER_TABLE).get_metadata()[tool.MARKER_KEY]
        leftover = project.create_table(
            tool.PROBE_TABLE, key_schema=pa.schema([pa.field("k", pa.int64(), False)])
        )
        leftover.set_metadata({tool.MARKER_KEY: marker})
        tool.probe_nullability(
            client, target_project=target, allow_writes=target, source_project=SOURCE
        )
        assert {f"{r.dataset}.{r.table}" for r in project.list_tables()} == {
            tool.MARKER_TABLE
        }

    def test_the_probe_table_carries_the_marker(self, client):
        target = make_target(client)
        tool.probe_nullability(
            client, target_project=target, allow_writes=target, source_project=SOURCE
        )
        assert (target, "set_metadata", tool.PROBE_TABLE) in client.log
