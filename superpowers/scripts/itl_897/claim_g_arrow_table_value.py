"""ITL-897 claim (g): what ``pa.Table`` equality depends on vs its value hash today.

For each property, two tables differing only in it; reports ``Table.equals`` (default
``check_metadata=False``), ``Table.equals(check_metadata=True)``, and whether
``semantic_hasher.hash_object`` gives the same hash. The last section checks that a
``RecordBatch`` hashes like the one-batch ``Table`` with the same content.

Run: ``uv run python superpowers/scripts/itl_897/claim_g_arrow_table_value.py``
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from orcapod.contexts import get_default_context

HASHER = get_default_context().semantic_hasher


def _t(**columns: pa.Array[Any]) -> pa.Table:
    return pa.Table.from_arrays(list(columns.values()), names=list(columns))


AB = _t(a=pa.array([1, 2]), b=pa.array(["x", "y"]))
UV = pa.struct([("u", pa.int64()), ("v", pa.string())])
VU = pa.struct([("v", pa.string()), ("u", pa.int64())])

CASES: list[tuple[str, pa.Table, pa.Table]] = [
    ("column order", AB, AB.select(["b", "a"])),
    ("row order", AB, AB.take([1, 0])),
    ("values", AB, _t(a=pa.array([1, 3]), b=pa.array(["x", "y"]))),
    ("column name", AB, AB.rename_columns(["a", "c"])),
    (
        "struct field order in a column",
        _t(s=pa.array([{"u": 1, "v": "x"}], UV)),
        _t(s=pa.array([{"v": "x", "u": 1}], VU)),
    ),
    (
        "struct field order in a list column",
        _t(s=pa.array([[{"u": 1, "v": "x"}]], pa.list_(UV))),
        _t(s=pa.array([[{"v": "x", "u": 1}]], pa.list_(VU))),
    ),
    (
        "int64 vs int32",
        _t(a=pa.array([1], pa.int64())),
        _t(a=pa.array([1], pa.int32())),
    ),
    (
        "string vs large_string",
        _t(a=pa.array(["x"], pa.string())),
        _t(a=pa.array(["x"], pa.large_string())),
    ),
    (
        "nullable vs non-nullable field",
        AB,
        AB.cast(
            pa.schema([pa.field("a", pa.int64(), False), pa.field("b", pa.string())])
        ),
    ),
    ("chunking", AB, pa.concat_tables([AB.slice(0, 1), AB.slice(1, 1)])),
    ("schema metadata", AB, AB.replace_schema_metadata({"k": "v"})),
]


def main() -> None:
    print("| property | equals | equals(check_metadata) | hash equal | verdict |")
    print("|---|---|---|---|---|")
    for name, t1, t2 in CASES:
        eq = t1.equals(t2)
        eq_meta = t1.equals(t2, check_metadata=True)
        same = HASHER.hash_object(t1) == HASHER.hash_object(t2)
        if not eq and same:
            verdict = "MISMATCH: collision"
        elif eq and not same:
            verdict = "MISMATCH: spurious miss"
        else:
            verdict = "consistent"
        print(f"| {name} | {eq} | {eq_meta} | {'yes' if same else 'no'} | {verdict} |")

    batch = AB.to_batches()[0]
    print(
        "\nRecordBatch hashes like the one-batch Table: "
        f"{HASHER.hash_object(batch) == HASHER.hash_object(AB)}"
    )
    print(f"Label of a table value hash today: {HASHER.hash_object(AB).method!r}")


if __name__ == "__main__":
    main()
