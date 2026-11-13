"""ITL-897 claim (d): row-level identities that embed a pandas value hash.

Builds the same ``DictSource`` (one pandas column) before and after installing the prototype
pandas hash in this process, and reports which identities change:

* the default ``source_id`` (the table hash, used when no ``source_id`` is given), which is
  the value of the ``_tag_source_id`` system tag and an input to every record ID;
* record IDs (``_tag_record_id``), with and without an explicit ``source_id``;
* the per-row data hash;
* the source's ``content_hash`` and ``pipeline_hash``.

Run: ``uv run python superpowers/scripts/itl_897/claim_d_source_identity.py``
"""

from __future__ import annotations

import importlib
import sys
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from orcapod.sources import DictSource

warnings.simplefilter("ignore")
sys.path.insert(0, str(Path(__file__).parent))
prototype_v1 = importlib.import_module("prototype_v1")


def _rows() -> list[dict[str, Any]]:
    return [
        {"k": "r1", "n": 1, "df": pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})},
        {"k": "r2", "n": 2, "df": pd.DataFrame({"a": [3]})},
    ]


def _summary(source_id: str | None) -> dict[str, Any]:
    source = DictSource(_rows(), tag_columns=["k"], source_id=source_id)
    table = source.as_table(all_info=True)
    system = {
        c: table.column(c).to_pylist()
        for c in table.column_names
        if c.startswith("_tag")
    }
    data_hashes = [d.content_hash().to_string() for _, d in source.iter_data()]
    return {
        "system tags": system,
        "row data hashes": data_hashes,
        "content_hash": source.content_hash().to_string(),
        "pipeline_hash": source.pipeline_hash().to_string(),
    }


def main() -> None:
    before = {sid: _summary(sid) for sid in (None, "explicit")}
    prototype_v1.install()
    after = {sid: _summary(sid) for sid in (None, "explicit")}
    for sid in (None, "explicit"):
        print(f"\n## source_id={sid!r}")
        for key in before[sid]:
            same = before[sid][key] == after[sid][key]
            print(f"  {key:16} {'unchanged' if same else 'CHANGES'}")
            if key == "system tags" and not same:
                for col, values in before[sid][key].items():
                    if values != after[sid][key].get(col):
                        print(
                            f"      {col.split('::')[0]}: {values} -> {after[sid][key].get(col)}"
                        )


if __name__ == "__main__":
    main()
