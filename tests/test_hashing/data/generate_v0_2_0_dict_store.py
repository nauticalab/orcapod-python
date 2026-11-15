"""Build a Delta result database with the released v0.2.0 code (ITL-905 test fixture).

Run from a checkout of the v0.2.0 tag, then pack the store:

    uv run python generate_v0_2_0_dict_store.py <store_dir>
    tar -czf v0_2_0_dict_store.tar.gz -C <store_dir> .

The process can hang at interpreter exit inside deltalake after printing; the database is
complete once the record counts are printed.

Two pods write to the store:

* ``total`` takes a dict: ``k=unsorted`` holds ``{"b": 2, "a": 1}`` (stored in that order,
  so its v0.2.0 input hash depends on insertion order), ``k=sorted`` holds
  ``{"a": 1, "c": 3}`` (already in key order).
* ``tally`` takes an int and returns ``{"z": n, "a": n}``, stored in that order.
"""

import sys

from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource


def total(m: dict[str, int]) -> int:
    """Return the sum of the dict's values."""
    return sum(m.values())


def tally(n: int) -> dict[str, int]:
    """Return a dict built out of key order."""
    return {"z": n, "a": n}


db = DeltaTableDatabase(sys.argv[1])

total_pod = CachedFunctionPod(
    # The fixture is built with the released v0.2.0 API, unchanged.
    FunctionPod(PythonDataFunction(total, output_keys="total")),  # pyright: ignore[reportArgumentType]
    result_database=db,
)
dict_inputs = DictSource(
    data=[
        {"k": "unsorted", "m": {"b": 2, "a": 1}},
        {"k": "sorted", "m": {"a": 1, "c": 3}},
    ],
    tag_columns=["k"],
    data_schema={"k": str, "m": dict[str, int]},
    source_id="itl905_v020",
)
list(total_pod.process(dict_inputs).iter_data())

tally_pod = CachedFunctionPod(
    # The fixture is built with the released v0.2.0 API, unchanged.
    FunctionPod(PythonDataFunction(tally, output_keys="counts")),  # pyright: ignore[reportArgumentType]
    result_database=db,
)
int_inputs = DictSource(
    data=[{"k": "r", "n": 5}],
    tag_columns=["k"],
    data_schema={"k": str, "n": int},
    source_id="itl905_v020_int",
)
for _, out in tally_pod.process(int_inputs).iter_data():
    print("tally output", out.as_dict(), out.content_hash().to_string())
db.flush()

for pod in (total_pod, tally_pod):
    records = db.get_all_records(pod.record_path)
    print(pod.record_path, records.num_rows if records is not None else 0)
