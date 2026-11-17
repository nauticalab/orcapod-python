"""ITL-939 experiment 03: an out-of-tree prototype of design C, a tag-to-data COPY operator.

Throwaway spike script. ``CopyTagsToData`` is defined here, not in ``src/``; it is the
smallest feature the design document proposes. Everything else uses the public interface.

Run:

    uv run python superpowers/specs/itl-939-experiments/exp03_copy_tag_prototype.py
"""

from __future__ import annotations

import contextlib
import importlib
import io
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.operators import GroupBy, Join
from orcapod.sources import ArrowTableSource
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema

sys.path.insert(0, str(Path(__file__).parent))
with contextlib.redirect_stdout(
    io.StringIO()
):  # reuse exp02's fixtures, not its output
    fx = importlib.import_module("exp02_workarounds_today")


class CopyTagsToData(UnaryOperator):
    """Copy tag columns into the data group under new names; the tags stay as they are.

    Prototype only. The copy takes the tag's Arrow type and nullability unchanged. Its
    source-info value is null: an operator does not know which source a tag value came
    from (after a Join it came from several).

    Args:
        name_map: Tag column name to the name of its data copy.
    """

    def __init__(self, name_map: Mapping[str, str], **kwargs: Any) -> None:
        self.name_map = dict(name_map)
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        """Return the operator's identity: its class name and the sorted name map."""
        return (self.__class__.__name__, tuple(sorted(self.name_map.items())))

    def validate_unary_input(self, stream) -> None:
        """Check the name map against the input stream.

        Raises:
            InputValidationError: If a key is not a user tag column, two keys share a
                target name, a target starts with the reserved ``_`` prefix, or a target
                is already a tag or data column.
        """
        tag_columns, data_columns = stream.keys()
        missing = set(self.name_map) - set(tag_columns)
        if missing:
            raise InputValidationError(f"Not tag columns: {sorted(missing)}")
        targets = list(self.name_map.values())
        duplicated = sorted({t for t in targets if targets.count(t) > 1})
        if duplicated:
            raise InputValidationError(f"Copy names given twice: {duplicated}")
        reserved = sorted(t for t in targets if t.startswith(constants.DATAGRAM_PREFIX))
        if reserved:
            raise InputValidationError(f"Copy names use a reserved prefix: {reserved}")
        taken = set(targets) & (set(tag_columns) | set(data_columns))
        if taken:
            raise InputValidationError(f"Copy names already used: {sorted(taken)}")

    def unary_output_schema(
        self,
        stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Predict the output schemas: the input's, plus one data entry per copy."""
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        base_tags, _ = stream.output_schema()
        copies = {new: base_tags[old] for old, new in self.name_map.items()}
        return tag_schema, Schema({**data_schema, **copies})

    def unary_static_process(self, stream):
        """Append each copy (and a null source-info column for it) to the input table."""
        tag_columns, _ = stream.keys()
        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )
        for old, new in self.name_map.items():
            col = table.column(old)
            field = table.schema.field(old)
            table = table.append_column(pa.field(new, field.type, field.nullable), col)
            table = table.append_column(
                pa.field(f"{constants.SOURCE_PREFIX}{new}", pa.large_string(), True),
                pa.nulls(table.num_rows, pa.large_string()),
            )
        return ArrowTableStream(table, tag_columns=tag_columns)


def section(title: str) -> None:
    """Print a section header."""
    print(f"\n=== {title}")


experiment = fx.baseline  # keys subject_id, session_start_datetime, experiment_index
session = fx.session

# 1. What the user sees.
copied = CopyTagsToData({"session_start_datetime": "session_start"})(experiment)
section("1. CopyTagsToData({'session_start_datetime': 'session_start'})(experiment)")
print("keys():", copied.keys())
print("output_schema():", copied.output_schema())
print("as_table() columns:", copied.as_table().column_names)
print(
    "as_table(all_info) source cols:",
    [
        c
        for c in copied.as_table(all_info=True).column_names
        if c.startswith("_source_")
    ],
)
tag, data = next(iter(copied.iter_data()))
print("first iter_data():", tag.as_dict(), "|", data.as_dict())
# Schema prediction vs the computed stream (non-public static_process, the CLAUDE.md
# standing exception for checking a schema prediction).
op = CopyTagsToData({"session_start_datetime": "session_start"})
print(
    "prediction matches static_process:",
    op.output_schema(experiment) == op.static_process(experiment).output_schema(),
)
src_cols = {"source": True}
print(
    "... also with columns={'source': True} (no stream lists _source_ columns):",
    op.output_schema(experiment, columns=src_cols)
    == op.static_process(experiment).output_schema(columns=src_cols),
)
for bad in (
    {"session_start_datetime": "x", "experiment_index": "x"},
    {"session_start_datetime": "_tag_x"},
    {"session_start_datetime": "task_id"},
    {"task_id": "t"},
):
    try:
        CopyTagsToData(bad)(experiment)
        print("validation", bad, "-> accepted")
    except InputValidationError as exc:
        print("validation", bad, "->", exc)

# 2. The pod reads the copy; the tags still identify the row.
section("2. pod on the copy, then Join with session on the shared keys")
elapsed = fx.elapsed_s2.pod(
    copied.select_data_columns(["session_start", "experiment_start_timestamp"])
)
fx.rows(
    elapsed, ["subject_id", "session_start_datetime", "experiment_index", "elapsed_s"]
)
j = Join()(elapsed, session)
print("Join keys():", j.keys())
print("Join rows:", j.as_table().num_rows, "(correct answer is 5)")

# 3. Hash comparison with the W2 recipe (column duplicated before the source).
section("3. data content hashes: copy operator vs W2 recipe")
w2_in = fx.w2_src.select_data_columns(["session_start", "experiment_start_timestamp"])
c_in = copied.select_data_columns(["session_start", "experiment_start_timestamp"])
w2_hashes = [d.content_hash().to_hex(16) for _, d in w2_in.iter_data()]
c_hashes = [d.content_hash().to_hex(16) for _, d in c_in.iter_data()]
print("W2   :", w2_hashes)
print("copy :", c_hashes)
print("per-row data hashes equal (so pod result caches agree):", w2_hashes == c_hashes)
print(
    "pipeline_hash experiment (unchanged by the feature):",
    experiment.pipeline_hash().to_hex(16),
)
print("pipeline_hash after copy:", copied.pipeline_hash().to_hex(16))
print("pipeline_hash W2 source :", fx.w2_src.pipeline_hash().to_hex(16))

# 4. Why design D (pods read tags directly) is unsafe with today's result cache.
section("4. two rows, same data, different tags -> same input data hash")
t = pa.table(
    {
        "subject_id": pa.array(["m1", "m1"], pa.large_string()),
        "session_start_datetime": pa.array([fx.S1, fx.S2], fx.TS),
        "experiment_start_timestamp": pa.array(
            [fx.us(fx.S2) + 60_000_000] * 2, pa.uint64()
        ),
    }
)
s = ArrowTableSource(
    t,
    tag_columns=["subject_id", "session_start_datetime"],
    source_id="d",
    infer_nullable=True,
)
print("data hashes:", [d.content_hash().to_hex(16) for _, d in s.iter_data()])
print(
    "-> a pod that read session_start_datetime from the TAG would get one cached result"
)
print("   for both rows (ResultCache keys on the input data hash only).")
s_c = CopyTagsToData({"session_start_datetime": "session_start"})(s)
print(
    "with the copy, data hashes:",
    [d.content_hash().to_hex(16) for _, d in s_c.iter_data()],
)

# 5. Nullable keys (every EDI key column is nullable in the DB schema).
section("5. nullable tag -> nullable copy")
tn = pa.Table.from_arrays(
    [t["subject_id"], t["session_start_datetime"], t["experiment_start_timestamp"]],
    schema=pa.schema(
        [
            pa.field("subject_id", pa.large_string(), True),
            pa.field("session_start_datetime", fx.TS, True),
            pa.field("experiment_start_timestamp", pa.uint64(), True),
        ]
    ),
)
sn = ArrowTableSource(
    tn, tag_columns=["subject_id", "session_start_datetime"], source_id="n"
)
cn = CopyTagsToData({"session_start_datetime": "session_start"})(sn)
print("tag schema :", cn.output_schema()[0])
print("data schema:", cn.output_schema()[1])
print("arrow field:", cn.as_table().schema.field("session_start"))

# 6. Copy before GroupBy: grouped key values reach the pod as a list.
section("6. copy, then GroupBy(by=['subject_id', 'session_start_datetime'])")
g = GroupBy(by=["subject_id", "session_start_datetime"])(
    CopyTagsToData({"experiment_index": "experiment_idx"})(experiment)
)
print("keys():", g.keys())
print("output_schema():", g.output_schema())
fx.rows(
    g, ["subject_id", "session_start_datetime", "experiment_index", "experiment_idx"]
)

# 7. Equality is a snapshot: after a later operator the two may diverge.
section("7. the copy is ordinary data afterwards")
renamed = fx.MapData({"session_start": "t0"})(copied)
print("MapData on the copy leaves the tag alone:", renamed.keys())
