"""
Integration tests: FunctionNode and Join preserve non-nullable column constraints
after the Arrow → Polars → Arrow round-trip that occurs during joins.

RED phase: tests should fail before the fix is applied.
"""

import pyarrow as pa
import pytest

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
    preserves non-nullable column constraints."""

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

        # Force a DB-backed iteration by going through _iter_all_from_database
        # (simulates the CACHE_ONLY path used after save/load)
        packets_seen = list(fn_node._iter_all_from_database())
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
