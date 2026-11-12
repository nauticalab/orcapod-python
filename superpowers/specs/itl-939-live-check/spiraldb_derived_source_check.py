"""Read-only live check of ``DerivedSource(tags_as_data=...)`` on SpiralDB (ITL-939).

Reads the ``session`` and ``experiment`` tables of a SpiralDB project, derives a source from
``experiment`` that also offers ``session_start_datetime`` as data, runs a pod on the copy
in memory and joins the result with ``session``. The pod only converts the copied start time
to epoch seconds: it makes no assumption about the meaning of any other EDI column (a first
version subtracted it from ``experiment_start_timestamp``, which in the tutorial dataset is a
relative clock, not microseconds since the epoch). It writes nothing: no database, no store, no
file.

Needs ``pyspiral`` (the ``spiraldb`` extra) and ``spiral login`` credentials. Usage::

    uv run --extra spiraldb python superpowers/specs/itl-939-live-check/spiraldb_derived_source_check.py \\
        edi-tutorial-v1-841226 [dataset]
"""

from __future__ import annotations

import sys
from datetime import datetime

from orcapod import function_pod
from orcapod.operators import Join
from orcapod.sources import DerivedSource, SpiralDBTableSource

TAG = "session_start_datetime"
COPY = "session_start"
TIMESTAMP = "experiment_start_timestamp"


@function_pod(output_keys="start_epoch_s")
def start_epoch_s(session_start: datetime | None) -> float | None:
    """Return the session start as seconds since the epoch."""
    if session_start is None:
        return None
    return session_start.timestamp()


def main(project: str, dataset: str) -> None:
    """Run the check and print what it finds."""
    session = SpiralDBTableSource(project, "session", dataset=dataset)
    experiment = SpiralDBTableSource(project, "experiment", dataset=dataset)
    derived = DerivedSource(experiment, tags_as_data={TAG: COPY})

    origin_table = experiment.as_table()
    table = derived.as_table(all_info=True)
    print("origin  keys():", experiment.keys())
    print("derived keys():", derived.keys())
    print("rows origin / derived:", origin_table.num_rows, "/", table.num_rows)
    print("tag field :", origin_table.schema.field(TAG))
    print("copy field:", table.schema.field(COPY))
    print("copy equals tag:", table[COPY].equals(table[TAG]))
    print("source_id:", experiment.source_id, "->", derived.source_id)
    print("first token:", table[f"_source_{COPY}"][0].as_py())
    print("timestamp field:", table.schema.field(TIMESTAMP))

    starts = start_epoch_s.pod(derived.select_data_columns([COPY]))
    joined = Join()(starts, session)
    result = joined.as_table()
    print("joined keys():", joined.keys())
    print("joined rows:", result.num_rows)
    print("start_epoch_s sample:", result["start_epoch_s"].to_pylist()[:5])

    rebuilt = DerivedSource.from_config(derived.to_config())
    print(
        "config round trip equal:",
        rebuilt.source_id == derived.source_id
        and rebuilt.as_table(all_info=True).equals(table),
    )


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "default")
