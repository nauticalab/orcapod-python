"""Check the function node's stored records row by row (ITL-1034).

A record of ``scaled`` holds the input row's tags and system tags, the input's source-info
token, the output, and bookkeeping. Each per-row column is recomputed from the record and
its origin row:

* the record id is the origin row's, the input token ``_source_x`` names that row;
* ``y`` is ``gated_scale`` of the origin's ``x``;
* ``__output_data_hash`` is the Arrow hash of the record's own ``y``;
* ``__input_data_hash`` (and its ``_right`` copy) is the Arrow hash of the origin's ``x``,
  built from the table the test wrote;
* ``_source_y`` names the output datagram ``__data_id``, and ``__data_id`` and the whole
  ``_source_y`` token (function uri included) are those of the node's own output for that
  record id;
* every input row is stored once: the record ids are source A's, each exactly once;
* ``__data_context`` and ``_context_key`` are the data context's key.

The remaining columns are not derived from the row: ``__pod_ts`` is the time the row was
computed; ``__computed``, ``__is_ephemeral``, ``__pf_var_*`` and ``__pf_exec_*`` describe the
function and the run, the same in every row of one run. They are listed by name, so a new
per-row column fails the check until a rule covers it.
"""

from __future__ import annotations

from collections import Counter

import pyarrow as pa

from orcapod import PipelineJob
from orcapod.system_constants import constants

from .checker import split_token
from .fixtures import Sources

NOT_ROW_DERIVED_PREFIXES = ("__pf_var_", "__pf_exec_")
NOT_ROW_DERIVED = {"__pod_ts", "__computed", "__is_ephemeral"}


def node_outputs(job: PipelineJob) -> dict[bytes, tuple[bytes, str]]:
    """The uuid and ``y`` token of each in-memory output of ``scaled``, by record id.

    Read from the ``Data`` objects the node holds after its run (``iter_data``), a path
    independent of the stored records: what a record's ``__data_id`` and ``_source_y``
    must match.
    """
    outputs: dict[bytes, tuple[bytes, str]] = {}
    for tag, data in job.nodes["scaled"].iter_data():
        system_tags = tag.as_dict(columns={"system_tags": True})
        (record_id,) = [
            v
            for k, v in system_tags.items()
            if k.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
        ]
        assert isinstance(record_id, bytes)
        token = data.source_info()["y"]
        assert isinstance(token, str)
        outputs[record_id] = (data.datagram_uuid.bytes, token)
    return outputs


def assert_records_aligned(
    job: PipelineJob, sources: Sources, records: pa.Table | None = None
) -> None:
    """Assert every stored record of ``scaled`` belongs, column by column, to its row.

    Args:
        job: The job that ran.
        sources: Its sources and oracle.
        records: The records to check; by default the node's stored records (a test of
            this checker passes a tampered copy).
    """
    node = job.nodes["scaled"]
    if records is None:
        records = node.get_all_records(all_info=True)
    assert records is not None and records.num_rows > 0, "no records stored"
    outputs = node_outputs(job)
    context = node.data_context
    hasher = context.arrow_hasher
    oracle = sources.oracle
    tag_keys = node.keys()[0]
    (source_id_column,) = [
        c
        for c in records.column_names
        if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
    ]
    (record_id_column,) = [
        c
        for c in records.column_names
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
    ]
    covered = {
        *tag_keys,
        source_id_column,
        record_id_column,
        "_source_x",
        "y",
        "_source_y",
        "__data_id",
        "__output_data_hash",
        "__input_data_hash",
        "__input_data_hash_right",
        "__data_context",
        constants.CONTEXT_KEY,
        *NOT_ROW_DERIVED,
    }
    unknown = [
        c
        for c in records.column_names
        if c not in covered and not c.startswith(NOT_ROW_DERIVED_PREFIXES)
    ]
    assert not unknown, f"no alignment rule covers the record columns {unknown!r}"

    # Every row of the node's input stored once: a record missing, or one valid record
    # stored twice in place of another, passes every per-record check below.
    stored: Counter[object] = Counter(records.column(record_id_column).to_pylist())
    wanted: Counter[object] = Counter(
        o.record_id for o in oracle.origins if o.source_id == "A"
    )
    assert stored == wanted, (
        f"stored record ids are not the input rows once each: "
        f"{sum((wanted - stored).values())} missing, "
        f"{sum((stored - wanted).values())} extra or repeated"
    )

    failures: list[str] = []
    for index, row in enumerate(records.to_pylist()):

        def fail(column: str, message: str, index: int = index) -> None:
            """Record a failure of ``column`` in this record."""
            failures.append(f"record {index}, column {column!r}: {message}")

        origin = oracle.by_record_id.get(row[record_id_column])
        if origin is None or origin.source_id != row[source_id_column]:
            fail(record_id_column, "record id is not a row of its source")
            continue
        for tag in tag_keys:
            if row[tag] != origin.values[tag]:
                fail(tag, f"{row[tag]!r}, its row has {origin.values[tag]!r}")
        if row["_source_x"] != f"A::{origin.token}::x":
            fail(
                "_source_x", f"{row['_source_x']!r} does not name row {origin.token!r}"
            )
        if row["y"] != oracle.expected_value("y", origin):
            fail("y", f"{row['y']!r} is not the output for row {origin.token!r}")
        _, datagram, column = split_token(row["_source_y"])
        if datagram != row["__data_id"].hex() or column != "y":
            fail("_source_y", f"{row['_source_y']!r} does not name __data_id")
        # __data_id and the output token are made at run time (uuid7): matched by record
        # id with the node's own output for this row, the token whole (function uri too).
        output_id, output_token = outputs.get(origin.record_id, (None, None))
        if row["__data_id"] != output_id:
            fail("__data_id", f"is not the uuid of row {origin.token!r}'s output")
        if row["_source_y"] != output_token:
            fail(
                "_source_y",
                f"{row['_source_y']!r}, row {origin.token!r}'s output has "
                f"{output_token!r}",
            )
        one_row = records.slice(index, 1)
        output_hash = hasher.hash_table(one_row.select(["y"])).to_prefixed_digest()
        if row["__output_data_hash"] != output_hash:
            fail("__output_data_hash", "is not the hash of the record's own y")
        input_hash = hasher.hash_table(origin.data_table.select(["x"]))
        for column in ("__input_data_hash", "__input_data_hash_right"):
            if row[column] != input_hash.to_prefixed_digest():
                fail(column, f"is not the hash of row {origin.token!r}'s input")
        for column in ("__data_context", constants.CONTEXT_KEY):
            if row[column] != context.context_key:
                fail(column, f"{row[column]!r}")
    assert not failures, "\n".join(failures[:12])
