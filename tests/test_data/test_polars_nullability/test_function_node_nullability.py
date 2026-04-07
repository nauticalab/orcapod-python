"""
Integration tests validating that FunctionNode and Join preserve nullable column
constraints across the Arrow → Polars → Arrow round-trip that occurs during
joins and cached-record retrieval.
"""
# ruff: noqa: E501

import pyarrow as pa

import orcapod as op
from orcapod.core.nodes.function_node import FunctionNode
from orcapod.databases import InMemoryArrowDatabase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_function_nodes(pipeline: op.Pipeline) -> list[FunctionNode]:
    """Return all FunctionNode instances from compiled pipeline nodes."""
    return [n for n in pipeline.compiled_nodes.values() if isinstance(n, FunctionNode)]


# ---------------------------------------------------------------------------
# FunctionNode.get_all_records nullability
# ---------------------------------------------------------------------------


class TestFunctionNodeGetAllRecordsNullability:
    """FunctionNode.get_all_records must preserve the non-nullable schema of
    output columns whose Python type annotation is non-optional (e.g. ``int``)."""

    def test_non_optional_return_type_yields_non_nullable_output_column(self):
        """Output column from int return type must be non-nullable after get_all_records."""
        database = InMemoryArrowDatabase()
        source = op.sources.DictSource(
            [{"id": 1, "x": 10}, {"id": 2, "x": 20}],
            tag_columns=["id"],
        )

        @op.function_pod(output_keys=["result"])
        def double(x: int) -> int:
            return x * 2

        pipeline = op.Pipeline("test_fn_nullable", database)
        with pipeline:
            double.pod(source)

        pipeline.run()

        fn_nodes = _get_function_nodes(pipeline)
        assert len(fn_nodes) == 1, "Expected exactly one FunctionNode"
        fn_node = fn_nodes[0]

        table = fn_node.get_all_records()
        assert table is not None, "get_all_records() returned None after pipeline.run()"

        # "result" column has Python type int → nullable=False in Arrow schema
        result_field = table.schema.field("result")
        assert result_field.nullable is False, (
            f"Expected 'result' column (int return type) to be non-nullable, "
            f"but got nullable={result_field.nullable}. "
            "Arrow→Polars→Arrow round-trip in get_all_records() dropped nullability."
        )

    def test_input_tag_column_non_nullable_after_get_all_records(self):
        """Input tag columns that are non-nullable must remain so after Polars join."""
        database = InMemoryArrowDatabase()

        # DictSource with integer id — infer_schema_nullable sets nullable=False (no nulls)
        source = op.sources.DictSource(
            [{"id": 1, "x": 5}, {"id": 2, "x": 15}],
            tag_columns=["id"],
        )

        @op.function_pod(output_keys=["result"])
        def triple(x: int) -> int:
            return x * 3

        pipeline = op.Pipeline("test_fn_tag_nullable", database)
        with pipeline:
            triple.pod(source)

        pipeline.run()

        fn_nodes = _get_function_nodes(pipeline)
        fn_node = fn_nodes[0]

        table = fn_node.get_all_records()
        assert table is not None

        # "id" was non-nullable in the source; after the Polars join it must stay so
        id_field = table.schema.field("id")
        assert id_field.nullable is False, (
            f"Expected 'id' tag column to be non-nullable after get_all_records(), "
            f"but got nullable={id_field.nullable}."
        )


# ---------------------------------------------------------------------------
# FunctionNode.iter_packets nullability
# ---------------------------------------------------------------------------


class TestFunctionNodeIterPacketsNullability:
    """FunctionNode.iter_packets must yield packets whose underlying Arrow schema
    preserves non-nullable column constraints. Uses _load_cached_entries to
    simulate the CACHE_ONLY path used after save/load."""

    def test_iter_packets_from_database_preserves_non_nullable_output(self):
        """Packets loaded from DB via iter_packets carry non-nullable output schema."""
        database = InMemoryArrowDatabase()
        source = op.sources.DictSource(
            [{"id": 1, "x": 7}],
            tag_columns=["id"],
        )

        @op.function_pod(output_keys=["result"])
        def add_one(x: int) -> int:
            return x + 1

        pipeline = op.Pipeline("test_iter_packets_nullable", database)
        with pipeline:
            add_one.pod(source)

        pipeline.run()

        fn_nodes = _get_function_nodes(pipeline)
        fn_node = fn_nodes[0]

        # Force a DB-backed iteration by going through _load_cached_entries
        # (simulates the CACHE_ONLY path used after save/load)
        loaded = fn_node._load_cached_entries()
        packets_seen = list(loaded.values())
        assert len(packets_seen) == 1, "Expected one packet from the database"

        _tag, packet = packets_seen[0]
        packet_schema = packet.arrow_schema()

        result_field = packet_schema.field("result")
        assert result_field.nullable is False, (
            f"Packet 'result' field should be non-nullable (int return type), "
            f"but got nullable={result_field.nullable}. "
            "Arrow→Polars→Arrow round-trip in iter_packets dropped nullability."
        )


# ---------------------------------------------------------------------------
# Join operator nullability
# ---------------------------------------------------------------------------


class TestJoinOperatorNullability:
    """Join.op_forward must preserve non-nullable tag column flags through the
    Polars inner join it uses internally."""

    def test_join_preserves_non_nullable_shared_tag_column(self):
        """Shared tag column remains non-nullable after stream join."""
        # DictSource applies infer_schema_nullable → integer 'id' has nullable=False
        source1 = op.sources.DictSource(
            [{"id": 1, "x": 10}, {"id": 2, "x": 20}],
            tag_columns=["id"],
        )
        source2 = op.sources.DictSource(
            [{"id": 1, "y": 100}, {"id": 2, "y": 200}],
            tag_columns=["id"],
        )

        joined_stream = source1.join(source2)
        table = joined_stream.as_table()

        id_field = table.schema.field("id")
        assert id_field.nullable is False, (
            f"Expected 'id' tag column to be non-nullable after Join, "
            f"but got nullable={id_field.nullable}. "
            "Arrow→Polars→Arrow round-trip in Join.op_forward dropped nullability."
        )

    def test_join_preserves_non_nullable_packet_columns(self):
        """Packet columns that are non-nullable remain so after stream join."""
        source1 = op.sources.DictSource(
            [{"id": 1, "x": 10}, {"id": 2, "x": 20}],
            tag_columns=["id"],
        )
        source2 = op.sources.DictSource(
            [{"id": 1, "y": 100}, {"id": 2, "y": 200}],
            tag_columns=["id"],
        )

        joined_stream = source1.join(source2)
        table = joined_stream.as_table()

        # "x" and "y" are packet columns with integer values → non-nullable
        x_field = table.schema.field("x")
        y_field = table.schema.field("y")

        assert x_field.nullable is False, (
            f"Expected 'x' packet column to be non-nullable after Join, "
            f"but got nullable={x_field.nullable}."
        )
        assert y_field.nullable is False, (
            f"Expected 'y' packet column to be non-nullable after Join, "
            f"but got nullable={y_field.nullable}."
        )

    def test_join_preserves_nullable_optional_column_with_no_nulls(self):
        """Optional[int] packet column (nullable=True) must remain nullable=True after
        Join, even when the data contains no actual null values.

        infer_schema_nullable incorrectly marks it nullable=False because it sees no
        nulls in the data. restore_schema_nullability preserves schema intent.
        """
        # Explicitly declare "x" as nullable=True (Optional[int]) via pa.Schema
        schema1 = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=True),  # Optional — no actual nulls
        ])
        source1 = op.sources.DictSource(
            [{"id": 1, "x": 10}, {"id": 2, "x": 20}],
            tag_columns=["id"],
            data_schema=schema1,
        )
        source2 = op.sources.DictSource(
            [{"id": 1, "y": 100}, {"id": 2, "y": 200}],
            tag_columns=["id"],
        )

        joined_stream = source1.join(source2)
        table = joined_stream.as_table()

        x_field = table.schema.field("x")
        assert x_field.nullable is True, (
            f"Expected 'x' (Optional[int], nullable=True in source schema) to remain "
            f"nullable=True after Join, but got nullable={x_field.nullable}. "
            "infer_schema_nullable incorrectly set it to nullable=False (no actual nulls "
            "in data), ignoring schema intent."
        )


# ---------------------------------------------------------------------------
# Join tag-column nullability with mixed nullable/non-nullable tag keys
# ---------------------------------------------------------------------------


class TestJoinTagColumnNullability:
    """Join must preserve the exact nullable flag of every tag column —
    both non-nullable mandatory keys and nullable optional keys — through
    the Polars inner join used internally."""

    def test_shared_tag_columns_mixed_nullability_preserved(self):
        """When two sources share multiple tag columns with mixed nullable flags,
        each flag is preserved correctly after the join.

        Schema intent:
        - "id"    int64  nullable=False  (mandatory join key)
        - "group" utf8   nullable=True   (Optional grouping key, no actual nulls)
        """
        tag_schema = pa.schema([
            pa.field("id",    pa.int64(), nullable=False),
            pa.field("group", pa.utf8(),  nullable=True),
        ])
        schema1 = pa.schema([
            *tag_schema,
            pa.field("x", pa.int64(), nullable=False),
        ])
        schema2 = pa.schema([
            *tag_schema,
            pa.field("y", pa.int64(), nullable=False),
        ])

        source1 = op.sources.DictSource(
            [{"id": 1, "group": "a", "x": 10}, {"id": 2, "group": "b", "x": 20}],
            tag_columns=["id", "group"],
            data_schema=schema1,
        )
        source2 = op.sources.DictSource(
            [{"id": 1, "group": "a", "y": 100}, {"id": 2, "group": "b", "y": 200}],
            tag_columns=["id", "group"],
            data_schema=schema2,
        )

        table = source1.join(source2).as_table()

        id_field    = table.schema.field("id")
        group_field = table.schema.field("group")

        assert id_field.nullable is False, (
            f"'id' (non-nullable tag) must remain nullable=False after Join, "
            f"got nullable={id_field.nullable}."
        )
        assert group_field.nullable is True, (
            f"'group' (Optional tag, nullable=True) must remain nullable=True after Join "
            f"even though data contains no actual nulls, got nullable={group_field.nullable}."
        )

    def test_non_shared_tag_columns_mixed_nullability_preserved(self):
        """Tag columns that are unique to each side of a join (non-shared) also
        preserve their nullable flags in the combined result.

        source1 has tag "id" (non-nullable int).
        source2 has tag "category" (nullable string, Optional, no actual nulls).
        Neither tag is shared, so the join is a full cartesian product.
        Both tag columns appear in the result and must keep their original nullable flags.
        """
        schema1 = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("x",  pa.int64(), nullable=False),
        ])
        schema2 = pa.schema([
            pa.field("category", pa.utf8(), nullable=True),   # Optional tag
            pa.field("y",        pa.int64(), nullable=False),
        ])

        source1 = op.sources.DictSource(
            [{"id": 1, "x": 10}],
            tag_columns=["id"],
            data_schema=schema1,
        )
        source2 = op.sources.DictSource(
            [{"category": "alpha", "y": 100}],
            tag_columns=["category"],
            data_schema=schema2,
        )

        table = source1.join(source2).as_table()

        id_field       = table.schema.field("id")
        category_field = table.schema.field("category")

        assert id_field.nullable is False, (
            f"'id' (non-nullable tag from source1) must remain nullable=False after "
            f"cartesian join, got nullable={id_field.nullable}."
        )
        assert category_field.nullable is True, (
            f"'category' (Optional tag, nullable=True from source2) must remain "
            f"nullable=True after cartesian join even with no actual nulls, "
            f"got nullable={category_field.nullable}."
        )

    def test_three_way_join_tag_nullability_preserved(self):
        """A three-way join (two Polars join iterations) correctly restores nullable
        flags on all tag columns across both iterations.

        shared tag "id"    int64  nullable=False
        shared tag "group" utf8   nullable=True  (Optional, no actual nulls)
        """
        tag_schema = pa.schema([
            pa.field("id",    pa.int64(), nullable=False),
            pa.field("group", pa.utf8(),  nullable=True),
        ])
        schema1 = pa.schema([*tag_schema, pa.field("a", pa.int64(), nullable=False)])
        schema2 = pa.schema([*tag_schema, pa.field("b", pa.int64(), nullable=True)])   # b is Optional
        schema3 = pa.schema([*tag_schema, pa.field("c", pa.int64(), nullable=False)])

        source1 = op.sources.DictSource(
            [{"id": 1, "group": "x", "a": 1}],
            tag_columns=["id", "group"],
            data_schema=schema1,
        )
        source2 = op.sources.DictSource(
            [{"id": 1, "group": "x", "b": 2}],
            tag_columns=["id", "group"],
            data_schema=schema2,
        )
        source3 = op.sources.DictSource(
            [{"id": 1, "group": "x", "c": 3}],
            tag_columns=["id", "group"],
            data_schema=schema3,
        )

        table = source1.join(source2).join(source3).as_table()

        id_field    = table.schema.field("id")
        group_field = table.schema.field("group")
        b_field     = table.schema.field("b")

        assert id_field.nullable is False, (
            f"'id' (non-nullable tag) must remain nullable=False after 3-way join, "
            f"got nullable={id_field.nullable}."
        )
        assert group_field.nullable is True, (
            f"'group' (Optional tag) must remain nullable=True after 3-way join, "
            f"got nullable={group_field.nullable}."
        )
        assert b_field.nullable is True, (
            f"'b' (Optional packet column) must remain nullable=True after 3-way join, "
            f"got nullable={b_field.nullable}."
        )
