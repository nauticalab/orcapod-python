"""The row-alignment checker accepts aligned tables and rejects every kind of misalignment.

A checker that passes everything proves nothing. Each test here takes a table the checker
accepts and moves the cells of one column to other rows (a rotation, the shape of ITL-1029),
and the checker must name that column.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.core.operators.batch import BATCH_FOLD_LABEL
from orcapod.core.operators.group_by import GROUP_BY_FOLD_LABEL
from orcapod.core.operators import Batch, Join
from orcapod.system_constants import constants

from .checker import (
    Layout,
    split_token,
    assert_members_cover,
    assert_requested_columns,
    assert_all_columns_checked,
    assert_rows_aligned,
    assert_same_rows_by_identity,
    assert_sorted_by_tags,
)
from .expectations import pairs, rows
from .fixtures import TAG_COLUMNS, make_sources
from .gate import new_gated_scale_pod
from .jobs import derive_y, make_run, stream_function_tokens
from .records import assert_records_aligned
from .oracle import Oracle


def rotated(table: pa.Table, column: str) -> pa.Table:
    """``table`` with the cells of ``column`` each moved one row down (the last to the top)."""
    index = table.column_names.index(column)
    values = table.column(column)
    order = [table.num_rows - 1, *range(table.num_rows - 1)]
    return table.set_column(index, table.field(column), values.take(order))


def source_table() -> tuple[pa.Table, Layout, Oracle, Any]:
    """Source A of the unique scenario with every column group, its layout and oracle."""
    sources = make_sources("unique")
    table = sources.a.as_table(all_info=True)
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=sources.a.keys()[1])
    return table, layout, sources.oracle, sources.a.data_context


def test_an_aligned_source_table_passes_and_every_column_is_compared() -> None:
    """Source A's full table passes, and the checker compares every one of its columns."""
    table, layout, oracle, context = source_table()
    checked = assert_rows_aligned(table, layout, oracle, context)
    assert_all_columns_checked(table, checked)


def _misalignable_columns() -> list[str]:
    """Every column of source A's full table whose rotation changes the table."""
    table, *_ = source_table()
    # A column with one value in every row (the context key, a single source's id) is
    # unchanged by a rotation; it is checked by test_a_wrong_context_key_is_named and the
    # join test instead.
    return [c for c in table.column_names if len(set(table.column(c).to_pylist())) > 1]


@pytest.mark.parametrize("column", _misalignable_columns())
def test_a_rotated_column_is_named(column: str) -> None:
    """Rotating one column's values by a row is caught and names that column."""
    table, layout, oracle, context = source_table()
    with pytest.raises(AssertionError, match=rf"column '{column}'"):
        assert_rows_aligned(rotated(table, column), layout, oracle, context)


def test_a_wrong_context_key_is_named() -> None:
    """A context key that is not the data context's key is caught."""
    table, layout, oracle, context = source_table()
    index = table.column_names.index(constants.CONTEXT_KEY)
    wrong = table.set_column(
        index,
        table.field(constants.CONTEXT_KEY),
        pa.array(["other"] * table.num_rows, type=pa.large_string()),
    )
    with pytest.raises(AssertionError, match=constants.CONTEXT_KEY):
        assert_rows_aligned(wrong, layout, oracle, context)


def test_a_column_no_rule_covers_fails() -> None:
    """A column no alignment rule covers fails the check instead of being skipped."""
    table, layout, oracle, context = source_table()
    extra = table.append_column("__unexplained", pa.array(range(table.num_rows)))
    with pytest.raises(AssertionError, match="__unexplained"):
        assert_rows_aligned(extra, layout, oracle, context)


def test_rotated_join_record_ids_are_named() -> None:
    """Either input's record id rotated in a join's table is caught and named."""
    sources = make_sources("unique")
    joined = Join()(sources.a, sources.b)
    table = joined.as_table(all_info=True)
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=joined.keys()[1])
    assert_rows_aligned(table, layout, sources.oracle, joined.data_context)
    record_ids = [
        c
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
    ]
    assert len(record_ids) == 2
    for column in record_ids:
        with pytest.raises(AssertionError, match=f"column '{column}'"):
            assert_rows_aligned(
                rotated(table, column), layout, sources.oracle, joined.data_context
            )


def test_join_pairs_exchanged_whole_between_inputs_are_named() -> None:
    """A's (source id, record id) pair in B's columns and B's in A's fails.

    Each pair still names a row the joined row comes from; only the column a pair is in,
    which descends from its source's own column, tells the exchange apart.
    """
    sources = make_sources("unique")
    joined = Join()(sources.a, sources.b)
    table = joined.as_table(all_info=True)
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=joined.keys()[1])
    assert_rows_aligned(table, layout, sources.oracle, joined.data_context)
    prefixes = (
        constants.SYSTEM_TAG_SOURCE_ID_PREFIX,
        constants.SYSTEM_TAG_RECORD_ID_PREFIX,
    )
    suffixes = sorted(
        c[len(constants.SYSTEM_TAG_SOURCE_ID_PREFIX) :]
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
    )
    assert len(suffixes) == 2
    first, second = suffixes
    exchanged = table
    for prefix in prefixes:
        one, other = f"{prefix}{first}", f"{prefix}{second}"
        one_values, other_values = table.column(one), table.column(other)
        exchanged = exchanged.set_column(
            exchanged.column_names.index(one), exchanged.field(one), other_values
        )
        exchanged = exchanged.set_column(
            exchanged.column_names.index(other), exchanged.field(other), one_values
        )
    with pytest.raises(AssertionError, match="but the column descends from"):
        assert_rows_aligned(exchanged, layout, sources.oracle, joined.data_context)


def test_a_rotated_folded_record_id_is_named() -> None:
    """A Batch's folded record id moved to another batch is caught."""
    sources = make_sources("unique")
    batched = Batch(2)(sources.a)
    table = batched.as_table(all_info=True)
    tag_keys, data_keys = batched.keys()
    layout = Layout(
        tag_keys=tag_keys,
        data_keys=data_keys,
        members=frozenset(tag_keys + data_keys),
        fold_label=BATCH_FOLD_LABEL,
    )
    checked = assert_rows_aligned(table, layout, sources.oracle, batched.data_context)
    assert_all_columns_checked(table, checked)
    (record_id,) = [
        c
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
    ]
    # Named under the record-id column, not the source-id column beside it.
    with pytest.raises(
        AssertionError, match=re.escape(f"column '{record_id}': folded record id")
    ):
        assert_rows_aligned(
            rotated(table, record_id), layout, sources.oracle, batched.data_context
        )


def test_a_folded_source_id_with_the_other_fold_label_is_named() -> None:
    """A Batch's folded source id carrying ``GroupBy``'s fold label is caught.

    Only the label changes: the digest, the member list and the record ids stay right.
    """
    sources = make_sources("unique")
    batched = Batch(2)(sources.a)
    table = batched.as_table(all_info=True)
    tag_keys, data_keys = batched.keys()
    layout = Layout(
        tag_keys=tag_keys,
        data_keys=data_keys,
        members=frozenset(tag_keys + data_keys),
        fold_label=BATCH_FOLD_LABEL,
    )
    (source_id,) = [
        c
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
    ]
    values = cast("list[str]", table.column(source_id).to_pylist())
    relabelled = [
        GROUP_BY_FOLD_LABEL + value.removeprefix(BATCH_FOLD_LABEL) for value in values
    ]
    changed = table.set_column(
        table.column_names.index(source_id),
        table.field(source_id),
        pa.array(relabelled, type=table.field(source_id).type),
    )
    with pytest.raises(AssertionError, match="folds no member list"):
        assert_rows_aligned(changed, layout, sources.oracle, batched.data_context)


def test_identity_comparison_ignores_row_order_but_not_cell_moves() -> None:
    """Comparison by row identity accepts reordered rows but not a value moved between rows."""
    table, *_ = source_table()
    reversed_rows = table.take(list(reversed(range(table.num_rows))))
    assert_same_rows_by_identity(table, reversed_rows)
    with pytest.raises(AssertionError):
        assert_same_rows_by_identity(table, rotated(table, "x"))


def test_a_null_token_beside_a_value_is_named() -> None:
    """A null source-info token beside a non-null value is caught."""
    table, layout, oracle, context = source_table()
    index = table.column_names.index("_source_x")
    tokens = table.column("_source_x").to_pylist()
    tokens[0] = None
    nulled = table.set_column(
        index, table.field("_source_x"), pa.array(tokens, type=pa.large_string())
    )
    with pytest.raises(AssertionError, match="column '_source_x': null token"):
        assert_rows_aligned(nulled, layout, oracle, context)


def test_a_system_tag_that_is_no_source_id_or_record_id_fails() -> None:
    """A system tag column that is neither a source id nor a record id fails the check."""
    table, layout, oracle, context = source_table()
    extra = table.append_column(
        f"{constants.SYSTEM_TAG_PREFIX}unexplained", pa.array(range(table.num_rows))
    )
    with pytest.raises(AssertionError, match="no alignment rule"):
        assert_rows_aligned(extra, layout, oracle, context)


def test_rotated_function_tokens_are_named_against_the_same_run() -> None:
    """A function output's tokens are matched by record id with the run's own rows."""
    sources = make_sources("unique")
    derive_y(sources)
    stream = new_gated_scale_pod()(sources.a)
    table = stream.as_table(columns={"source": True, "system_tags": True})
    tags, data = stream.keys()
    layout = Layout(tag_keys=tuple(tags), data_keys=tuple(data))
    tokens = stream_function_tokens(stream)
    assert_rows_aligned(
        table, layout, sources.oracle, stream.data_context, function_tokens=tokens
    )
    with pytest.raises(AssertionError, match="column '_source_y': token"):
        assert_rows_aligned(
            rotated(table, "_source_y"),
            layout,
            sources.oracle,
            stream.data_context,
            function_tokens=tokens,
        )


def test_stored_records_with_a_swapped_output_id_are_named(tmp_path: Path) -> None:
    """A record whose (``__data_id``, ``_source_y``) pair is another row's is caught."""
    run = make_run("unique", "shuffled", "sync", None, "in_memory", tmp_path)
    records = run.job.nodes["scaled"].get_all_records(all_info=True)
    assert records is not None
    assert_records_aligned(run.job, run.sources, records)
    swapped = records
    for column in ("__data_id", "_source_y"):
        index = swapped.column_names.index(column)
        values = swapped.column(column).to_pylist()
        values[0], values[1] = values[1], values[0]
        swapped = swapped.set_column(
            index,
            swapped.field(column),
            pa.array(values, type=swapped.field(column).type),
        )
    with pytest.raises(AssertionError, match="column '__data_id'"):
        assert_records_aligned(run.job, run.sources, swapped)


def test_stored_records_with_a_wrong_function_uri_are_named(tmp_path: Path) -> None:
    """A ``_source_y`` token whose uuid is right but whose function uri is not fails."""
    run = make_run("unique", "shuffled", "sync", None, "in_memory", tmp_path)
    records = run.job.nodes["scaled"].get_all_records(all_info=True)
    assert records is not None
    tokens = records.column("_source_y").to_pylist()
    _, datagram, column = split_token(tokens[0])
    sep = constants.BLOCK_SEPARATOR
    tokens[0] = f"wrong-function{sep}{datagram}{sep}{column}"
    index = records.column_names.index("_source_y")
    wrong = records.set_column(
        index, records.field("_source_y"), pa.array(tokens, type=pa.large_string())
    )
    with pytest.raises(AssertionError, match="column '_source_y'"):
        assert_records_aligned(run.job, run.sources, wrong)


def test_stored_records_with_one_record_repeated_are_named(tmp_path: Path) -> None:
    """A valid record stored twice in place of another fails, though each row is aligned."""
    run = make_run("unique", "shuffled", "sync", None, "in_memory", tmp_path)
    records = run.job.nodes["scaled"].get_all_records(all_info=True)
    assert records is not None and records.num_rows > 1
    repeated = pa.concat_tables(
        [records.slice(0, 1), records.slice(0, records.num_rows - 1)]
    )
    with pytest.raises(AssertionError, match="1 missing, 1 extra or repeated"):
        assert_records_aligned(run.job, run.sources, repeated)
    with pytest.raises(AssertionError, match="1 missing, 0 extra"):
        assert_records_aligned(run.job, run.sources, records.slice(1))


def test_a_table_without_the_requested_provenance_fails() -> None:
    """A table missing the source-info or system tag columns its configuration asks for fails."""
    table, layout, *_ = source_table()
    with pytest.raises(AssertionError, match="no source-info columns"):
        assert_requested_columns(
            table.drop_columns(["_source_x"]), {"columns": {"source": True}}, layout
        )
    system_tags = [
        c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    with pytest.raises(AssertionError, match="no system tag"):
        assert_requested_columns(
            table.drop_columns(system_tags), {"columns": {"system_tags": True}}, layout
        )


def test_a_table_asked_sorted_but_not_in_tag_order_fails() -> None:
    """A table whose configuration asks it sorted by tags fails when its rows are out of tag order."""
    table, layout, *_ = source_table()
    ordered = table.sort_by([("g", "ascending")])
    assert_requested_columns(ordered, {"columns": {"sort_by_tags": True}}, layout)
    unsorted = ordered.take(list(reversed(range(ordered.num_rows))))
    with pytest.raises(AssertionError, match="not in tag order"):
        assert_requested_columns(unsorted, {"columns": {"sort_by_tags": True}}, layout)


def test_a_sorted_table_with_nulls_first_or_a_wrong_tiebreak_fails() -> None:
    """The presentation order of #354: nulls last, then ties by the system tags."""
    sources = make_sources("null")
    # all_info sorts by tags, nulls last, and breaks ties by the system tag columns.
    table = sources.a.as_table(all_info=True)
    assert_sorted_by_tags(table, TAG_COLUMNS)
    g = table.column("g").to_pylist()
    nulls = [i for i, v in enumerate(g) if v is None]
    values = [i for i, v in enumerate(g) if v is not None]
    with pytest.raises(AssertionError, match="not in tag order"):
        assert_sorted_by_tags(table.take(nulls + values), TAG_COLUMNS)
    tags = list(zip(g, table.column("n").to_pylist(), strict=True))
    (tie,) = [i for i in range(1, len(tags)) if tags[i] == tags[i - 1]]
    order = list(range(table.num_rows))
    order[tie - 1], order[tie] = order[tie], order[tie - 1]
    with pytest.raises(AssertionError, match="not in tag order"):
        assert_sorted_by_tags(table.take(order), TAG_COLUMNS)


def test_a_null_data_value_no_origin_has_is_named() -> None:
    """A value nulled where no origin row has a null fails, though it leaves no origin."""
    table, _, oracle, context = source_table()
    # Only x: no other column could give the row its origin.
    bare = table.select([*TAG_COLUMNS, "x"])
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=("x",))
    assert_rows_aligned(bare, layout, oracle, context)
    values = bare.column("x").to_pylist()
    values[0] = None
    nulled = bare.set_column(
        bare.column_names.index("x"),
        pa.field("x", bare.field("x").type),
        pa.array(values, type=bare.field("x").type),
    )
    with pytest.raises(
        AssertionError, match="column 'x': value None is the value of no"
    ):
        assert_rows_aligned(nulled, layout, oracle, context)


def test_a_row_only_its_tags_can_resolve_is_still_checked() -> None:
    """A null shared by several rows names no origin; the row's tags must then match one."""
    table, _, oracle, context = source_table()
    # Only the nullable m: a null m is the value of several rows, and nothing else
    # (no token, no system tag) names the row.
    bare = table.select([*TAG_COLUMNS, "m"])
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=("m",))
    assert_rows_aligned(bare, layout, oracle, context)
    m = bare.column("m").to_pylist()
    g = bare.column("g").to_pylist()
    index = m.index(None)
    g[index] = next(v for v in g if v != g[index])
    changed = bare.set_column(
        bare.column_names.index("g"), bare.field("g"), pa.array(g, bare.field("g").type)
    )
    with pytest.raises(
        AssertionError, match="column 'm': no origin row has the values"
    ):
        assert_rows_aligned(changed, layout, oracle, context)


def test_ambiguous_columns_of_two_joined_sources_resolve_source_by_source() -> None:
    """A's null ``m`` and B's shared parity each resolve by the tags, within their source."""
    sources = make_sources("unique")
    oracle = sources.oracle
    oracle.derive("zp", "B", lambda o: o.values["z"] % 2)
    joined = Join()(sources.a, sources.b)
    full = joined.as_table()
    zs = [z for z in full.column("z").to_pylist() if z is not None]
    assert len(zs) == full.num_rows
    # Only A's nullable m and a parity of B's z: neither names one row by itself.
    table = full.select([*TAG_COLUMNS, "m"]).append_column(
        "zp", pa.array([z % 2 for z in zs], pa.int64())
    )
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=("m", "zp"))
    assert_rows_aligned(
        table,
        layout,
        oracle,
        joined.data_context,
        expected_rows=pairs(oracle, "A", "B"),
    )
    zp = [z % 2 for z in zs]
    zp[0] = 1 - zp[0]
    flipped = table.set_column(
        table.column_names.index("zp"), table.field("zp"), pa.array(zp, pa.int64())
    )
    with pytest.raises(AssertionError, match="no origin row has the values"):
        assert_rows_aligned(flipped, layout, oracle, joined.data_context)


def test_exchanged_tokens_of_a_repeated_derived_value_are_named() -> None:
    """Rows with one derived value, told apart by their tags: their tokens are checked."""
    sources = make_sources("unique")
    oracle = sources.oracle
    oracle.derive("p", "A", lambda o: o.values["x"] % 2)
    origins = [o for o in oracle.origins if o.source_id == "A"]
    sep = constants.BLOCK_SEPARATOR
    tokens = {
        o.record_id: f"fn{sep}{hashlib.md5(o.record_id.encode()).hexdigest()}{sep}p"
        for o in origins
    }
    table = pa.table(
        {
            "g": [o.values["g"] for o in origins],
            "n": [o.values["n"] for o in origins],
            "p": [o.values["x"] % 2 for o in origins],
            "_source_p": [tokens[o.record_id] for o in origins],
        }
    )
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=("p",))
    function_tokens = {("p", record_id): t for record_id, t in tokens.items()}
    context = sources.a.data_context
    assert_rows_aligned(table, layout, oracle, context, function_tokens=function_tokens)
    p = table.column("p").to_pylist()
    first, second = [i for i, v in enumerate(p) if v == p[0]][:2]
    swapped = table.column("_source_p").to_pylist()
    swapped[first], swapped[second] = swapped[second], swapped[first]
    exchanged = table.set_column(
        3, table.field("_source_p"), pa.array(swapped, table.field("_source_p").type)
    )
    with pytest.raises(AssertionError, match="column '_source_p': token"):
        assert_rows_aligned(
            exchanged, layout, oracle, context, function_tokens=function_tokens
        )


def test_a_derived_token_is_checked_against_an_origin_known_from_another_column() -> (
    None
):
    """``x`` names each row's origin; the shared ``p``'s exchanged tokens are named."""
    sources = make_sources("unique")
    oracle = sources.oracle
    oracle.derive("p", "A", lambda o: o.values["x"] % 2)
    origins = [o for o in oracle.origins if o.source_id == "A"]
    sep = constants.BLOCK_SEPARATOR
    tokens = {
        o.record_id: f"fn{sep}{hashlib.md5(o.record_id.encode()).hexdigest()}{sep}p"
        for o in origins
    }
    table = pa.table(
        {
            "g": [o.values["g"] for o in origins],
            "n": [o.values["n"] for o in origins],
            "x": [o.values["x"] for o in origins],
            "p": [o.values["x"] % 2 for o in origins],
            "_source_p": [tokens[o.record_id] for o in origins],
        }
    )
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=("x", "p"))
    function_tokens = {("p", record_id): t for record_id, t in tokens.items()}
    context = sources.a.data_context
    assert_rows_aligned(table, layout, oracle, context, function_tokens=function_tokens)
    p = table.column("p").to_pylist()
    first, second = [i for i, v in enumerate(p) if v == p[0]][:2]
    swapped = table.column("_source_p").to_pylist()
    swapped[first], swapped[second] = swapped[second], swapped[first]
    exchanged = table.set_column(
        4, table.field("_source_p"), pa.array(swapped, table.field("_source_p").type)
    )
    with pytest.raises(AssertionError, match="column '_source_p': token"):
        assert_rows_aligned(
            exchanged, layout, oracle, context, function_tokens=function_tokens
        )


def test_all_info_inside_the_column_config_asks_for_every_group() -> None:
    """``{"columns": {"all_info": True}}`` asks what top-level ``all_info`` asks (#363)."""
    table, layout, *_ = source_table()
    embedded = {"columns": {"all_info": True}}
    assert_requested_columns(table.sort_by([("g", "ascending")]), embedded, layout)
    data_only = table.select([*TAG_COLUMNS, *layout.data_keys])
    with pytest.raises(AssertionError):
        assert_requested_columns(data_only, embedded, layout)


def test_a_row_without_a_pair_for_a_source_of_its_data_fails() -> None:
    """A joined row whose B pair is gone, or is A's pair copied over B's, fails."""
    sources = make_sources("unique")
    joined = Join()(sources.a, sources.b)
    table = joined.as_table(all_info=True)
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=joined.keys()[1])
    assert_rows_aligned(table, layout, sources.oracle, joined.data_context)
    sep = constants.BLOCK_SEPARATOR

    def pair_of(source: str) -> list[str]:
        """The source-id and record-id columns holding ``source``'s rows."""
        lineage = f"{sep}{sources.oracle.schema_hashes[source]}{sep}"
        return [
            c
            for prefix in (
                constants.SYSTEM_TAG_SOURCE_ID_PREFIX,
                constants.SYSTEM_TAG_RECORD_ID_PREFIX,
            )
            for c in table.column_names
            if c.startswith(f"{prefix}{lineage}")
        ]

    a_pair, b_pair = pair_of("A"), pair_of("B")
    assert len(a_pair) == len(b_pair) == 2
    missing = re.escape("no system tag pair for the sources ['B']")
    with pytest.raises(AssertionError, match=missing):
        assert_rows_aligned(
            table.drop_columns(b_pair), layout, sources.oracle, joined.data_context
        )
    copied = table
    for a_column, b_column in zip(a_pair, b_pair, strict=True):
        copied = copied.set_column(
            copied.column_names.index(b_column),
            copied.field(b_column),
            table.column(a_column),
        )
    with pytest.raises(AssertionError, match=missing):
        assert_rows_aligned(copied, layout, sources.oracle, joined.data_context)


def test_a_row_replaced_by_a_copy_of_another_fails_the_expected_rows() -> None:
    """One valid row repeated in place of another fails the expected rows, though each row is aligned."""
    table, layout, oracle, context = source_table()
    expected = rows(oracle, "A")
    assert_rows_aligned(table, layout, oracle, context, expected_rows=expected)
    duplicated = table.take([0, *range(table.num_rows - 1)])
    with pytest.raises(AssertionError, match="not the expected rows"):
        assert_rows_aligned(duplicated, layout, oracle, context, expected_rows=expected)


def test_an_empty_join_fails_its_expected_rows() -> None:
    """A join returning no rows fails when the fixtures have matching keys."""
    sources = make_sources("unique")
    joined = Join()(sources.a, sources.b)
    table = joined.as_table(all_info=True)
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=joined.keys()[1])
    expected = pairs(sources.oracle, "A", "B")
    assert_rows_aligned(
        table, layout, sources.oracle, joined.data_context, expected_rows=expected
    )
    with pytest.raises(AssertionError, match="no rows"):
        assert_rows_aligned(
            table.slice(0, 0),
            layout,
            sources.oracle,
            joined.data_context,
            expected_rows=expected,
        )


def test_a_batch_table_repeating_one_batch_instead_of_another_is_named() -> None:
    """Each batch row is aligned by itself; the members must still cover the input."""
    sources = make_sources("unique")
    batched = Batch(2)(sources.a)
    table = batched.as_table(columns={"source": True, "system_tags": True})
    xs = [o.values["x"] for o in sources.oracle.origins if o.source_id == "A"]
    assert_members_cover(table, "x", xs)
    repeated = table.take([0, 0, 2])
    tag_keys, data_keys = batched.keys()
    layout = Layout(
        tag_keys=tag_keys,
        data_keys=data_keys,
        members=frozenset(tag_keys + data_keys),
        fold_label=BATCH_FOLD_LABEL,
    )
    assert_rows_aligned(repeated, layout, sources.oracle, batched.data_context)
    with pytest.raises(AssertionError, match="are not the input rows"):
        assert_members_cover(repeated, "x", xs)


def test_an_empty_batch_table_fails_member_coverage() -> None:
    """A Batch has no expected-rows rule; an empty table must still fail coverage."""
    sources = make_sources("unique")
    table = Batch(2)(sources.a).as_table()
    xs = [o.values["x"] for o in sources.oracle.origins if o.source_id == "A"]
    with pytest.raises(AssertionError, match="are not the input rows"):
        assert_members_cover(table.slice(0, 0), "x", xs)


def test_a_folded_row_whose_members_only_the_tags_resolve_passes() -> None:
    """A null ``m`` several rows share names no origin; the folded pair must still pass.

    The members' origins come from their tags, so they must be resolved before the
    folded system tags are checked against them (CodeRabbit, ITL-1034).
    """
    sources = make_sources("unique")
    batched = Batch(2)(sources.a)
    table = batched.as_table(all_info=True)
    tag_keys, _ = batched.keys()
    pair = [
        c
        for c in table.column_names
        if c.startswith(
            (
                constants.SYSTEM_TAG_SOURCE_ID_PREFIX,
                constants.SYSTEM_TAG_RECORD_ID_PREFIX,
            )
        )
    ]
    bare = table.select([*tag_keys, "m", *pair])
    layout = Layout(
        tag_keys=tag_keys,
        data_keys=("m",),
        members=frozenset((*tag_keys, "m")),
        fold_label=BATCH_FOLD_LABEL,
    )
    assert any(None in (v or []) for v in bare.column("m").to_pylist())
    assert_rows_aligned(bare, layout, sources.oracle, batched.data_context)


def test_tables_differing_only_in_an_arrow_type_or_nullability_are_not_the_same_rows() -> (
    None
):
    """Async must match sync in types and nullability, not only in Python values."""
    table, _, _, _ = source_table()
    assert_same_rows_by_identity(table, table)
    index = table.column_names.index("m")
    field = table.field("m")
    wider = table.set_column(
        index, pa.field("m", pa.int64()), table.column("m").cast(pa.int64())
    )
    if field.type != pa.int64():
        with pytest.raises(AssertionError, match="column 'm'"):
            assert_same_rows_by_identity(table, wider)
    flipped = table.set_column(
        index, field.with_nullable(not field.nullable), table.column("m")
    )
    with pytest.raises(AssertionError, match="column 'm'"):
        assert_same_rows_by_identity(table, flipped)


def test_a_column_every_candidate_origin_matches_counts_as_compared() -> None:
    """Rows whose tags and ``m`` repeat leave several origins; the comparison is still done."""
    sources = make_sources("duplicate")
    table = sources.a.as_table(all_info=True)
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=("m",))
    bare = table.select([*TAG_COLUMNS, "m"])
    pairs_seen = list(zip(*(bare.column(c).to_pylist() for c in (*TAG_COLUMNS, "m"))))
    assert len(set(pairs_seen)) < len(pairs_seen), "the scenario has no repeated row"
    # Only the repeated rows: no row of the table has a single candidate origin.
    from collections import Counter

    counts = Counter(pairs_seen)
    keep = [i for i, p in enumerate(pairs_seen) if counts[p] > 1]
    repeated = bare.take(keep)
    checked = assert_rows_aligned(
        repeated, layout, sources.oracle, sources.a.data_context
    )
    assert "m" in checked


def test_a_chain_of_renames_resolves_to_the_original_column_and_a_cycle_fails() -> None:
    """``x`` renamed to ``y`` and then ``y`` to ``z`` is still the input column ``x``."""
    oracle = make_sources("unique").oracle
    oracle.rename("y", "x")
    oracle.rename("z", "y")
    assert oracle.base_name("z") == "x"
    assert oracle.base_name("y") == "x"
    assert oracle.base_name("x") == "x"
    oracle.rename("x", "z")
    with pytest.raises(ValueError, match="cycle"):
        oracle.base_name("z")


def test_a_bad_token_of_a_derived_value_several_origins_share_is_named() -> None:
    """Two origins with one tag tuple and one derived value leave the token unexplained
    by origin; its form is still checked."""
    sources = make_sources("duplicate")
    oracle = sources.oracle
    oracle.derive("p", "A", lambda o: 0)
    origins = [o for o in oracle.origins if o.source_id == "A"]
    tags = [(o.values["g"], o.values["n"]) for o in origins]
    repeated = [o for o, t in zip(origins, tags) if tags.count(t) > 1]
    assert repeated, "the scenario has no repeated tag tuple"
    sep = constants.BLOCK_SEPARATOR

    def table_with(token: str) -> pa.Table:
        return pa.table(
            {
                "g": [o.values["g"] for o in repeated],
                "n": [o.values["n"] for o in repeated],
                "p": [0 for _ in repeated],
                "_source_p": [token for _ in repeated],
            }
        )

    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=("p",))
    context = sources.a.data_context
    good = f"fn{sep}{'0' * 32}{sep}p"
    assert_rows_aligned(table_with(good), layout, oracle, context)
    with pytest.raises(AssertionError, match="column '_source_p'"):
        assert_rows_aligned(table_with("fake::bad::p"), layout, oracle, context)


def test_a_source_only_the_tags_name_still_needs_a_system_tag_pair() -> None:
    """A's null ``m`` is resolved from the tags; a row with no pair for A must still fail."""
    sources = make_sources("unique")
    joined = Join()(sources.a, sources.b)
    full = joined.as_table(all_info=True)
    sep = constants.BLOCK_SEPARATOR
    lineage = f"{sep}{sources.oracle.schema_hashes['A']}{sep}"
    a_pair = [
        c
        for c in full.column_names
        if c.startswith(
            (
                f"{constants.SYSTEM_TAG_SOURCE_ID_PREFIX}{lineage}",
                f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{lineage}",
            )
        )
    ]
    assert len(a_pair) == 2
    pairs_left = [
        c
        for c in full.column_names
        if c.startswith(
            (
                constants.SYSTEM_TAG_SOURCE_ID_PREFIX,
                constants.SYSTEM_TAG_RECORD_ID_PREFIX,
            )
        )
        and c not in a_pair
    ]
    # B's z names B's row; A's nullable m names nothing but the tags.
    table = full.select([*TAG_COLUMNS, "m", "z", *pairs_left])
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=("m", "z"))
    # Only the null-m rows: no row names A by a value.
    nulls = [i for i, v in enumerate(table.column("m").to_pylist()) if v is None]
    assert nulls
    table = table.take(nulls)
    with pytest.raises(AssertionError, match=re.escape("sources ['A']")):
        assert_rows_aligned(table, layout, sources.oracle, joined.data_context)


def test_a_renamed_derived_column_resolves_through_its_derivation() -> None:
    """``y`` derived from A and renamed to ``z``: every lookup of ``z`` finds it."""
    oracle = make_sources("unique").oracle
    oracle.derive("y", "A", lambda o: 10 * o.values["x"])
    oracle.rename("z", "y")
    origin = next(o for o in oracle.origins if o.source_id == "A")
    assert oracle.holds("z", origin)
    assert oracle.expected_value("z", origin) == 10 * origin.values["x"]
    assert origin in oracle.origins_for_value("z", 10 * origin.values["x"])


def test_a_merged_column_whose_elements_both_come_from_one_input_is_named() -> None:
    """A and C both hold ``x=7``: ``[7, 7]`` with two tokens of A leaves C's out."""
    sources = make_sources("unique")
    oracle = sources.oracle
    a = next(o for o in oracle.origins if o.source_id == "A")
    c = next(o for o in oracle.origins if o.source_id == "C")
    c_values = cast("dict[str, Any]", c.values)
    for name in (*TAG_COLUMNS, "x"):
        c_values[name] = a.values[name]
    sep = constants.BLOCK_SEPARATOR

    def table_with(tokens: list[str]) -> pa.Table:
        return pa.table(
            {
                **{n: [a.values[n]] for n in TAG_COLUMNS},
                "x": [[a.values["x"], a.values["x"]]],
                "_source_x": [tokens],
            }
        )

    token_a = f"A{sep}{a.token}{sep}x"
    token_c = f"C{sep}{c.token}{sep}x"
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=("x",), merged=frozenset({"x"}))
    context = sources.a.data_context
    assert_rows_aligned(table_with([token_a, token_c]), layout, oracle, context)
    with pytest.raises(AssertionError, match="its elements come from the inputs"):
        assert_rows_aligned(table_with([token_a, token_a]), layout, oracle, context)


def test_a_derivation_on_an_intermediate_rename_name_is_found() -> None:
    """``x`` is a rename of ``raw`` and derived; ``y`` renames ``x``: ``y`` is derived."""
    oracle = make_sources("unique").oracle
    oracle.rename("x", "raw")
    oracle.derive("x", "A", lambda o: 2 * o.values["x"])
    oracle.rename("y", "x")
    origin = next(o for o in oracle.origins if o.source_id == "A")
    assert oracle.expected_value("y", origin) == 2 * origin.values["x"]
