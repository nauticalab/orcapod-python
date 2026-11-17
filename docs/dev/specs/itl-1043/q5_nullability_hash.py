"""ITL-1043 Q5: is a tag's nullability part of the schema hash today?

Pipeline hash of sources whose tag column is declared non-nullable, declared nullable
without nulls, declared nullable with a null; and DictSource inferring from values with
and without a None.

Run: uv run python superpowers/specs/itl-1043/q5_nullability_hash.py
"""

import pyarrow as pa

from orcapod.core.sources import ArrowTableSource, DictSource


def arrow(nullable: bool, ids: list[int | None]) -> ArrowTableSource:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=nullable),
            pa.field("v", pa.int64(), nullable=False),
        ]
    )
    return ArrowTableSource(
        pa.table({"id": ids, "v": [1, 2]}, schema=schema),
        tag_columns=["id"],
        source_id="s",
    )


cases = {
    "arrow non-nullable, no nulls": arrow(False, [0, 1]),
    "arrow nullable, no nulls": arrow(True, [0, 1]),
    "arrow nullable, one null": arrow(True, [0, None]),
    "dict inferred, no None": DictSource(
        data=[{"id": 0, "v": 1}, {"id": 1, "v": 2}], tag_columns=["id"], source_id="s"
    ),
    "dict inferred, one None": DictSource(
        data=[{"id": 0, "v": 1}, {"id": None, "v": 2}],
        tag_columns=["id"],
        source_id="s",
    ),
    "dict declared int | None, no None": DictSource(
        data=[{"id": 0, "v": 1}, {"id": 1, "v": 2}],
        tag_columns=["id"],
        source_id="s",
        data_schema={"id": int | None, "v": int},
    ),
}
for name, src in cases.items():
    tag, _ = src.output_schema()
    print(
        f"{name:36s} tag={dict(tag)!s:28s} pipeline_hash={src.pipeline_hash().to_hex()[:12]}"
    )
