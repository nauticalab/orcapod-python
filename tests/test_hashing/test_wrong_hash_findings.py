"""Regression tests for the wrong-hash findings of the hash-site audit (ITL-942).

A wrong hash is one where two different things share a hash, or one thing hashes
differently depending on data, process or spelling. Each class reproduces one finding
(letters as in Appendix A.9 of the ITL-938 design) through the public interface.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import uuid
from typing import Any, Optional

import pyarrow as pa
import pytest

from orcapod import FunctionPod, function_pod, side_effect_function_pod
from orcapod.contexts import resolve_context
from orcapod.operators import (
    Batch,
    DropDataColumns,
    DropTagColumns,
    GroupBy,
    MergeJoin,
    SelectDataColumns,
    SelectTagColumns,
)
from orcapod.sources import DictSource
from orcapod.streams import ArrowTableStream

SOURCE_ID_COL = "_tag_source_id::abc"
RECORD_ID_COL = "_tag_record_id::abc"


def _semantic_hasher() -> Any:
    """Return the default data context's structure hasher."""
    return resolve_context(None).semantic_hasher


def _arrow_hasher() -> Any:
    """Return the default data context's Arrow hasher."""
    return resolve_context(None).arrow_hasher


def _run_with_seed(script: str, seed: str) -> str:
    """Run ``script`` in a fresh interpreter under ``PYTHONHASHSEED=seed``; return stdout."""
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = seed
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# H. Batch / GroupBy fold of system-tag values
# ---------------------------------------------------------------------------


def _stream_with_source_ids(source_ids: list[str | None]) -> ArrowTableStream:
    """A two-row stream in one group whose rows carry the given source ids."""
    table = pa.table(
        {
            "g": pa.array([1] * len(source_ids), type=pa.int64()),
            "k": pa.array(list(range(len(source_ids))), type=pa.int64()),
            "x": pa.array(list(range(len(source_ids))), type=pa.int64()),
            SOURCE_ID_COL: pa.array(source_ids, type=pa.large_string()),
            RECORD_ID_COL: pa.array(
                [uuid.UUID(int=i + 1).bytes for i in range(len(source_ids))],
                type=pa.large_binary(),
            ),
        }
    )
    return ArrowTableStream(table, tag_columns=["g", "k"])


def _folded_source_id(stream: Any) -> str:
    """Return the single folded source-id system-tag value of a one-row stream."""
    table = stream.as_table(columns={"system_tags": True})
    [col] = [c for c in table.column_names if c.startswith("_tag_source_id")]
    [value] = table.column(col).to_pylist()
    return value


class TestFoldSeparatesMembers:
    """Finding H: the source-id fold of Batch and GroupBy decodes to its member list."""

    @pytest.mark.parametrize(
        "make_op", [lambda: Batch(batch_size=2), lambda: GroupBy(by=["g"])]
    )
    def test_member_boundaries_are_identity(self, make_op: Any) -> None:
        """Members split differently fold differently."""
        left = _folded_source_id(make_op()(_stream_with_source_ids(["ab", "c"])))
        right = _folded_source_id(make_op()(_stream_with_source_ids(["a", "bc"])))
        assert left != right

    def test_null_member_differs_from_empty_member(self) -> None:
        """A null member folds unlike an empty-string member."""
        left = _folded_source_id(
            Batch(batch_size=2)(_stream_with_source_ids([None, "x"]))
        )
        right = _folded_source_id(
            Batch(batch_size=2)(_stream_with_source_ids(["", "x"]))
        )
        assert left != right

    def test_source_info_stays_the_list_of_member_tokens(self) -> None:
        """Only the scalar system-tag fold changed, not source info."""
        # Only the scalar system-tag fold changed: source-info columns still list
        # each member's own provenance token, in member order.
        source = DictSource(
            data=[{"g": 1, "k": 1, "x": 10}, {"g": 1, "k": 2, "x": 20}],
            tag_columns=["g", "k"],
            tag_only_columns=True,
            source_id="members",
        )
        member_tokens = (
            source.as_table(columns={"source": True}).column("_source_x").to_pylist()
        )
        for op in (Batch(batch_size=2), GroupBy(by=["g"])):
            table = op(source).as_table(columns={"source": True})
            assert table.column("_source_x").to_pylist() == [member_tokens]
            assert table.column("x").to_pylist() == [[10, 20]]

    def test_member_order_is_identity(self) -> None:
        """Member order is part of the fold."""
        left = _folded_source_id(
            Batch(batch_size=2)(_stream_with_source_ids(["a", "b"]))
        )
        right = _folded_source_id(
            Batch(batch_size=2)(_stream_with_source_ids(["b", "a"]))
        )
        assert left != right


# ---------------------------------------------------------------------------
# I. Arrow hashing of nested types does not depend on the data
# ---------------------------------------------------------------------------

_NON_NULL_STRUCT = pa.struct([pa.field("a", pa.int64(), nullable=False)])
_NULLABLE_STRUCT = pa.struct([pa.field("a", pa.int64(), nullable=True)])
_NON_NULL_LIST = pa.large_list(pa.field("item", pa.int64(), nullable=False))
_NULLABLE_LIST = pa.large_list(pa.field("item", pa.int64(), nullable=True))


class TestNestedNullabilityIsIdentity:
    """Finding I: the hashed Arrow type depends on the declared type only."""

    @pytest.mark.parametrize(
        ("non_null", "nullable", "value"),
        [
            (_NON_NULL_STRUCT, _NULLABLE_STRUCT, {"a": 1}),
            (_NON_NULL_LIST, _NULLABLE_LIST, [1]),
            (
                pa.large_list(_NON_NULL_STRUCT),
                pa.large_list(_NULLABLE_STRUCT),
                [{"a": 1}],
            ),
        ],
        ids=["struct", "list", "list_of_struct"],
    )
    @pytest.mark.parametrize("rows", [0, 1], ids=["zero_rows", "with_data"])
    def test_nested_nullability_hashes_with_and_without_data(
        self, non_null: pa.DataType, nullable: pa.DataType, value: Any, rows: int
    ) -> None:
        """Nested nullability counts with data and with zero rows."""
        hasher = _arrow_hasher()
        a = pa.table({"c": pa.array([value] * rows, type=non_null)})
        b = pa.table({"c": pa.array([value] * rows, type=nullable)})
        assert hasher.hash_table(a) != hasher.hash_table(b)

    def test_nested_type_hash_does_not_depend_on_first_row(self) -> None:
        """Null or present first rows hash under the same type."""
        # The same column type, once with a null first row: the type that is hashed must
        # not change with the data.
        hasher = _arrow_hasher()
        first = pa.table({"c": pa.array([{"a": 1}], type=_NON_NULL_STRUCT)})
        nulls = pa.table({"c": pa.array([None], type=_NON_NULL_STRUCT)})
        mixed = pa.table({"c": pa.array([None, {"a": 1}], type=_NON_NULL_STRUCT)})
        # Hash of the schema each table is hashed under: compare against tables that only
        # differ in nested nullability; with data or without, the flag must count.
        nulls_nullable = pa.table({"c": pa.array([None], type=_NULLABLE_STRUCT)})
        mixed_nullable = pa.table(
            {"c": pa.array([None, {"a": 1}], type=_NULLABLE_STRUCT)}
        )
        first_nullable = pa.table({"c": pa.array([{"a": 1}], type=_NULLABLE_STRUCT)})
        assert hasher.hash_table(nulls) != hasher.hash_table(nulls_nullable)
        assert hasher.hash_table(mixed) != hasher.hash_table(mixed_nullable)
        assert hasher.hash_table(first) != hasher.hash_table(first_nullable)

    def test_list_item_name_is_not_identity(self) -> None:
        """List item names stay out of the hash, as starfix defines."""
        # starfix ignores list item names (pod code cannot observe them); keep it so.
        hasher = _arrow_hasher()
        item = pa.large_list(pa.field("item", pa.int64()))
        element = pa.large_list(pa.field("element", pa.int64()))
        for rows in (0, 1):
            a = pa.table({"c": pa.array([[1]] * rows, type=item)})
            b = pa.table({"c": pa.array([[1]] * rows, type=element)})
            assert hasher.hash_table(a) == hasher.hash_table(b)


# ---------------------------------------------------------------------------
# L. Structure hasher: set order and function defaults are process independent
# ---------------------------------------------------------------------------

_SEEDS = ("1", "2", "3", "4", "5", "6")


def _value_hashed_types() -> dict[str, Any]:
    """Python types whose column values the Arrow hasher replaces by a value hash."""
    import numpy as np
    import pandas as pd

    import orcapod as op

    return {
        "uuid": uuid.UUID,
        "file": op.File,
        "directory": op.Directory,
        "ndarray": np.ndarray,
        "dataframe": pd.DataFrame,
        "series": pd.Series,
        "list_of_file": list[op.File],
        "set_of_uuid": set[uuid.UUID],
    }


def _null_column(python_type: Any, rows: int) -> pa.Table:
    """A one-column table of ``python_type`` holding ``rows`` nulls."""
    arrow_type = resolve_context(None).type_converter.python_type_to_arrow_type(
        python_type
    )
    assert isinstance(arrow_type, pa.ExtensionType)
    storage = pa.array([None] * rows, arrow_type.storage_type)
    return pa.table({"c": pa.ExtensionArray.from_storage(arrow_type, storage)})


class TestValueHashedLogicalTypeKeepsItsIdentityWithoutValues:
    """Follow-up to finding I: a logical type is part of the hash with or without rows.

    The hasher replaces a value-hashed logical type by ``large_binary`` tokens. The
    logical type's identifier used to live only in each token, so a column with no
    value (zero rows, or nulls only) hashed like a plain ``large_binary`` column and
    like every other value-hashed logical type (ITL-967).
    """

    @pytest.mark.parametrize("rows", [0, 2], ids=["zero_rows", "all_null"])
    def test_columns_of_different_logical_types_hash_differently(
        self, rows: int
    ) -> None:
        """No two value-hashed logical types share a table hash, nor one with plain binary."""
        hasher = _arrow_hasher()
        hashes = {
            name: hasher.hash_table(_null_column(python_type, rows))
            for name, python_type in _value_hashed_types().items()
        }
        hashes["plain_large_binary"] = hasher.hash_table(
            pa.table({"c": pa.array([None] * rows, pa.large_binary())})
        )
        assert len(set(hashes.values())) == len(hashes), hashes

    @pytest.mark.parametrize("rows", [0, 2], ids=["zero_rows", "all_null"])
    def test_a_column_hashes_the_same_every_time(self, rows: int) -> None:
        """Building the same column twice gives the same hash."""
        hasher = _arrow_hasher()
        for name, python_type in _value_hashed_types().items():
            assert hasher.hash_table(
                _null_column(python_type, rows)
            ) == hasher.hash_table(_null_column(python_type, rows)), name

    def test_zero_rows_and_all_null_rows_differ(self) -> None:
        """The number of rows is still part of the hash."""
        hasher = _arrow_hasher()
        assert hasher.hash_table(_null_column(uuid.UUID, 0)) != hasher.hash_table(
            _null_column(uuid.UUID, 2)
        )

    def test_hashed_type_is_the_same_with_and_without_data(self) -> None:
        """Finding I's rule holds: one declared type is hashed under one type."""
        from orcapod.types import Schema

        context = resolve_context(None)
        with_data = context.type_converter.python_dicts_to_arrow_table(
            [{"c": uuid.UUID(int=1)}, {"c": None}],
            python_schema=Schema({"c": Optional[uuid.UUID]}),
        )
        # Non-public helper: the test is about the schema the hasher hashes under.
        hashed = context.arrow_hasher._process_table_columns  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
        assert hashed(with_data).schema == hashed(with_data.slice(0, 0)).schema
        assert hashed(with_data).schema == hashed(with_data.slice(1, 1)).schema


class TestStructureHasherProcessIndependence:
    """Finding L1: set hashes do not depend on the process."""

    def test_set_with_str_equal_items(self) -> None:
        """A set whose items share a str() hashes the same in every process."""
        script = """
            from orcapod.contexts import resolve_context
            print(resolve_context(None).semantic_hasher.hash_object({1, "1", 2, "2"}).to_string())
        """
        assert len({_run_with_seed(script, seed) for seed in _SEEDS}) == 1

    def test_set_with_str_equal_items_differs_from_set_without(self) -> None:
        """Items that share a str() remain distinct members."""
        hasher = _semantic_hasher()
        assert hasher.hash_object({1, "1"}) != hasher.hash_object({1})
        assert hasher.hash_object({1, "1"}) != hasher.hash_object({"1"})


# ---------------------------------------------------------------------------
# Dict keys keep their type
# ---------------------------------------------------------------------------


class TestDictKeysKeepTheirType:
    """Dict keys keep their type, and no dict spells out another value's encoding."""

    def test_int_key_differs_from_str_key(self) -> None:
        """An int key and its string spelling hash differently."""
        hasher = _semantic_hasher()
        assert hasher.hash_object({1: "x"}) != hasher.hash_object({"1": "x"})

    def test_keys_with_equal_str_do_not_collapse(self) -> None:
        """Two keys with one str() both count."""
        hasher = _semantic_hasher()
        both = {1: "a", "1": "b"}
        assert hasher.hash_object(both) != hasher.hash_object({"1": "b"})
        assert hasher.hash_object(both) != hasher.hash_object({1: "a"})

    @pytest.mark.parametrize(
        ("value", "literal"),
        [
            ((1, 2), lambda h: {"__type__": "tuple", "items": [1, 2]}),
            (frozenset({1}), lambda h: {"__type__": "set", "items": [1]}),
            ({1: "x"}, lambda h: {"__type__": "dict", "items": [[1, "x"]]}),
            (
                list[int],
                lambda h: {
                    "__type__": "generic_alias",
                    "origin": h.hash_object(list).to_string(),
                    "args": [h.hash_object(int).to_string()],
                },
            ),
            (
                int | None,
                lambda h: {
                    "__type__": "union",
                    "args": sorted(
                        h.hash_object(a).to_string() for a in (int, type(None))
                    ),
                },
            ),
        ],
        ids=["tuple", "set", "int_key_dict", "generic_alias", "union"],
        # namedtuple and the nested-hash tag are covered by their own tests below
    )
    def test_encoded_values_differ_from_literal_type_dicts(
        self, value: Any, literal: Any
    ) -> None:
        """A user dict spelling out an encoding never shares its hash."""
        # A user dict spelling out the hasher's own encoding of another value must
        # not share that value's hash.
        hasher = _semantic_hasher()
        assert hasher.hash_object(value) != hasher.hash_object(literal(hasher))

    def test_namedtuple_differs_from_literal_type_dict(self) -> None:
        """A dict spelling out a namedtuple's encoding hashes differently."""
        from collections import namedtuple

        hasher = _semantic_hasher()
        point = namedtuple("Point", ["x", "y"])
        literal = {
            "__type__": "namedtuple",
            "name": "Point",
            "fields": {"x": 1, "y": 2},
        }
        assert hasher.hash_object(point(1, 2)) != hasher.hash_object(literal)

    def test_nested_hash_differs_from_literal_hash_dict(self) -> None:
        """A dict spelling out a nested hash's encoding hashes differently."""
        # The nested-hash tag 'hash' is written by ITL-945 (PR #314):
        # {"__type__": "hash", "digest": <hex>}. Before that branch lands a nested
        # hash enters as its string; either way a user dict spelling out the tag
        # must not share the nested hash's hash. Keep this test when the branches
        # meet.
        hasher = _semantic_hasher()
        nested = hasher.hash_object("x")
        literal = {"__type__": "hash", "digest": nested.to_hex()}
        assert hasher.hash_object([nested]) != hasher.hash_object([literal])

    def test_hash_key_differs_from_its_string_spelling(self) -> None:
        """A hash key and its string spelling stay two keys (CodeRabbit, PR #315)."""
        from orcapod.types import ContentHash

        hasher = _semantic_hasher()
        key = ContentHash("x", b"\x00")
        swapped_a = {key: 1, key.to_string(): 2}
        swapped_b = {key: 2, key.to_string(): 1}
        assert hasher.hash_object(swapped_a) != hasher.hash_object(swapped_b)

    def test_dict_with_type_key_differs_from_same_dict_without(self) -> None:
        """A '__type__' key is part of the dict's identity."""
        hasher = _semantic_hasher()
        assert hasher.hash_object({"__type__": "a", "b": 1}) != hasher.hash_object(
            {"b": 1}
        )

    def test_str_keyed_dict_insertion_order_still_ignored(self) -> None:
        """Insertion order never matters, for any key type."""
        hasher = _semantic_hasher()
        assert hasher.hash_object({"a": 1, "b": 2}) == hasher.hash_object(
            {"b": 2, "a": 1}
        )
        assert hasher.hash_object({1: "a", 2: "b"}) == hasher.hash_object(
            {2: "b", 1: "a"}
        )


class TestEncodedValues:
    """Only a type handler can produce a tagged value (ITL-942, mechanism M2)."""

    def test_user_supplied_encoded_is_refused(self) -> None:
        """An Encoded found in data is refused, never trusted."""
        from orcapod.hashing.semantic_hashing import Encoded

        hasher = _semantic_hasher()
        forged = Encoded("tuple_like", {"items": [1, 2]})
        with pytest.raises(TypeError):
            hasher.hash_object(forged)
        with pytest.raises(TypeError):
            hasher.hash_object([forged])

    @pytest.mark.parametrize("tag", ["tuple", "set", "dict", "namedtuple", "hash", ""])
    def test_reserved_and_empty_tags_are_rejected(self, tag: str) -> None:
        """Encoded rejects the hasher's own tags and an empty tag."""
        from orcapod.hashing.semantic_hashing import Encoded

        with pytest.raises(ValueError):
            Encoded(tag, {})

    def test_type_key_cannot_be_a_field(self) -> None:
        """Encoded rejects a '__type__' field."""
        from orcapod.hashing.semantic_hashing import Encoded

        with pytest.raises(ValueError):
            Encoded("custom", {"__type__": "tuple"})

    def test_tags_are_unique_across_hasher_and_builtin_handlers(self) -> None:
        """Built-in handler tags are Encoded and disjoint from the hasher's tags."""
        from orcapod.hashing.semantic_hashing import Encoded
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            GenericAliasHandler,
            UnionTypeHandler,
        )
        from orcapod.hashing.semantic_hashing.encoded import HASHER_TAGS

        hasher = _semantic_hasher()
        handler_results = [
            GenericAliasHandler().handle(list[int], hasher),
            GenericAliasHandler().handle(Optional[int], hasher),
            UnionTypeHandler().handle(int | None, hasher),
        ]
        assert all(isinstance(r, Encoded) for r in handler_results)
        handler_tags = {r.tag for r in handler_results}
        assert handler_tags == {"generic_alias", "union"}
        assert handler_tags.isdisjoint(HASHER_TAGS)

    def test_plain_str_keyed_dicts_are_unchanged(self) -> None:
        """A plain str-keyed dict keeps its pre-ITL-942 hash."""
        # A str-keyed dict without a '__type__' key stays a native JSON object, so
        # its hash equals the sha256 of its canonical JSON (the pre-ITL-942 form).
        import hashlib
        import json

        hasher = _semantic_hasher()
        value = {"b": [1, 2], "a": "x"}
        expected = hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).digest()
        assert hasher.hash_object(value).digest == expected


# ---------------------------------------------------------------------------
# T. A namedtuple's field order is part of its hash
# ---------------------------------------------------------------------------


class TestNamedtupleFieldOrder:
    """Finding T: a namedtuple's declared field order is part of its hash."""

    def test_declared_field_order_is_identity(self) -> None:
        """Unequal namedtuples with the same name-to-value mapping differ."""
        from collections import namedtuple

        hasher = _semantic_hasher()
        xy = namedtuple("Point", ["x", "y"])
        yx = namedtuple("Point", ["y", "x"])
        # Unequal tuples ((1, 2) versus (2, 1)) with the same name-to-value mapping.
        assert xy(x=1, y=2) != yx(y=2, x=1)
        assert hasher.hash_object(xy(x=1, y=2)) != hasher.hash_object(yx(y=2, x=1))

    def test_equal_namedtuples_share_a_hash(self) -> None:
        """Equal namedtuples of identically declared classes share a hash."""
        from collections import namedtuple

        hasher = _semantic_hasher()
        first = namedtuple("Point", ["x", "y"])
        second = namedtuple("Point", ["x", "y"])
        assert hasher.hash_object(first(1, 2)) == hasher.hash_object(second(1, 2))


# ---------------------------------------------------------------------------
# F. Column selection identity is the set of names
# ---------------------------------------------------------------------------


def _xy_source() -> DictSource:
    """Return a source with tag columns k, j and data columns x, y."""
    return DictSource(
        data=[{"k": 1, "j": 2, "x": 1, "y": 2}],
        tag_columns=["k", "j"],
        tag_only_columns=True,
        source_id="s",
    )


class TestColumnSelectionIdentity:
    """Finding F: column selection identity is the set of names."""

    @pytest.mark.parametrize("op_cls", [SelectDataColumns, DropDataColumns])
    def test_data_column_spellings_share_identity(self, op_cls: Any) -> None:
        """Every spelling of one set of data columns gives one identity.

        The source has a third data column, ``z``, so dropping ``x`` and ``y`` leaves
        one: a selection that leaves no data column is refused (ITL-1080).
        """
        source = DictSource(
            data=[{"k": 1, "j": 2, "x": 1, "y": 2, "z": 3}],
            tag_columns=["k", "j"],
            source_id="s",
            tag_only_columns=True,
        )
        spellings = [["x", "y"], ("x", "y"), {"x", "y"}, ["y", "x"], ["x", "y", "x"]]
        streams = [op_cls(columns=c)(source) for c in spellings]
        assert len({s.pipeline_hash() for s in streams}) == 1
        assert len({s.content_hash() for s in streams}) == 1

    @pytest.mark.parametrize("op_cls", [SelectTagColumns, DropTagColumns])
    def test_tag_column_spellings_share_identity(self, op_cls: Any) -> None:
        """Every spelling of one set of tag columns gives one identity."""
        source = _xy_source()
        spellings = [["k"], ("k",), {"k"}, "k", ["k", "k"]]
        streams = [op_cls(columns=c)(source) for c in spellings]
        assert len({s.pipeline_hash() for s in streams}) == 1

    def test_argument_order_is_not_observable(self) -> None:
        """The output column order is the stream's, not the argument's."""
        source = _xy_source()
        a = SelectDataColumns(columns=["x", "y"])(source).as_table()
        b = SelectDataColumns(columns=["y", "x"])(source).as_table()
        assert a.column_names == b.column_names

    def test_caller_list_is_not_aliased(self) -> None:
        """Mutating the caller's list later does not change the operator."""
        source = _xy_source()
        columns = ["x"]
        operator = SelectDataColumns(columns=columns)
        before = operator(source).pipeline_hash()
        columns.append("y")
        # The operator built before the mutation still selects "x" only.
        after = operator(source)
        assert after.keys()[1] == ("x",)
        assert after.pipeline_hash() == before
        assert SelectDataColumns(columns=["x"])(source).pipeline_hash() == before


# ---------------------------------------------------------------------------
# E. Commutative operators: input identity (behaviour kept by decision)
# ---------------------------------------------------------------------------


def _weights(source_id: str, weight: float) -> DictSource:
    """Return a one-row source with tag k and data w under ``source_id``."""
    return DictSource(
        data=[{"k": 1, "w": weight}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id=source_id,
    )


class TestCommutativeInputIdentity:
    """Finding E, reviewed and KEPT by decision (developer, November 11, 2026).

    A pipeline hash covers the structure of the pipeline, not only schemas:
    ``MergeJoin(m1, m1)`` (a stream joined with itself) is a different pipeline
    from ``MergeJoin(m1, m2)`` even when both inputs share one schema. Inputs are
    an unordered set, so two inputs with the same source identity count as one.
    """

    def test_self_merge_join_is_a_different_pipeline(self) -> None:
        """A stream joined with itself is its own pipeline structure."""
        m1 = _weights("m", 1.0)
        m2 = _weights("m2", 3.0)
        assert (
            MergeJoin()(m1, m1).pipeline_hash() != MergeJoin()(m1, m2).pipeline_hash()
        )

    def test_inputs_with_one_source_identity_count_as_one(self) -> None:
        """Inputs with one source identity count as one input."""
        m1 = _weights("m", 1.0)
        m1b = _weights("m", 2.0)  # same source_id: same source identity
        assert MergeJoin()(m1, m1b).content_hash() == MergeJoin()(m1, m1).content_hash()
        assert (
            MergeJoin()(m1, m1b).pipeline_hash() == MergeJoin()(m1, m1).pipeline_hash()
        )

    def test_input_order_is_not_identity(self) -> None:
        """Argument order of a commutative operator is not identity."""
        m1 = _weights("m", 1.0)
        m2 = _weights("m2", 3.0)
        assert MergeJoin()(m1, m2).content_hash() == MergeJoin()(m2, m1).content_hash()
        assert (
            MergeJoin()(m1, m2).pipeline_hash() == MergeJoin()(m2, m1).pipeline_hash()
        )


# ---------------------------------------------------------------------------
# G. The record-id system tag keeps one Arrow type
# ---------------------------------------------------------------------------


def _record_id_types(table: pa.Table) -> set[pa.DataType]:
    """Return the Arrow types of a table's record-id system-tag columns."""
    return {f.type for f in table.schema if f.name.startswith("_tag_record_id")}


class TestRecordIdType:
    """Finding G: the record-id system tag is large_binary everywhere."""

    def test_source_stream_and_tag_agree(self) -> None:
        """A source's stream and its Tag objects give record ids one type."""
        source = DictSource(
            data=[{"k": 1, "x": 1}, {"k": 2, "x": 2}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )
        stream_types = _record_id_types(source.as_table(columns={"system_tags": True}))
        tag, _ = next(iter(source.iter_data()))
        tag_types = _record_id_types(tag.as_table(columns={"system_tags": True}))
        assert stream_types == tag_types == {pa.large_binary()}

    def test_batch_keeps_the_type(self) -> None:
        """Batch keeps the record-id type."""
        source = DictSource(
            data=[{"k": 1, "x": 1}, {"k": 2, "x": 2}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )
        batched = Batch(batch_size=2)(source)
        assert _record_id_types(batched.as_table(columns={"system_tags": True})) == {
            pa.large_binary()
        }

    def test_operator_cache_keys_match_on_sync_and_async_paths(self) -> None:
        """One row has one operator-cache key on both execution paths."""
        # OperatorJobNode is driven directly: the test is about its two execution
        # paths (sync run and async_execute), which the cache key must not tell apart.
        import asyncio

        from orcapod.channels import Channel
        from orcapod.core.nodes.operator_node import OperatorJobNode
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.types import CacheMode

        def source() -> DictSource:
            """Return the two-row source fed to both paths."""
            return DictSource(
                data=[{"k": i, "x": i, "y": i} for i in range(2)],
                tag_columns=["k"],
                tag_only_columns=True,
                source_id="s",
            )

        def stored_keys(db: Any, node: Any) -> list[bytes]:
            """Return the sorted stored operator-cache keys of a node."""
            # The cache table is the node's path plus its layout component (ITL-944).
            (path,) = db.list_sources()
            assert path == node.node_identity_path + ("odb_v1",)
            table = db.get_all_records(path, record_id_column="_record_hash")
            return sorted(table.column("_record_hash").to_pylist())

        sync_db = InMemoryArrowDatabase()
        sync_node = OperatorJobNode(
            SelectDataColumns(["x"]),
            [source()],
            pipeline_database=sync_db,
            cache_mode=CacheMode.LOG,
        )
        sync_node.run()

        async def run_async() -> tuple[Any, Any]:
            """Run an equal node with ``async_execute``; return its database and it."""
            db = InMemoryArrowDatabase()
            node = OperatorJobNode(
                SelectDataColumns(["x"]),
                [source()],
                pipeline_database=db,
                cache_mode=CacheMode.LOG,
            )
            inputs, outputs = Channel(buffer_size=16), Channel(buffer_size=16)
            for item in source().iter_data():
                await inputs.writer.send(item)
            await inputs.writer.close()
            await node.async_execute([inputs.reader], outputs.writer)
            await outputs.reader.collect()
            return db, node

        async_db, async_node = asyncio.run(run_async())
        sync_keys = stored_keys(sync_db, sync_node)
        async_keys = stored_keys(async_db, async_node)
        # One key per input row on each path: two equal empty lists must not pass.
        assert len(sync_keys) == len(async_keys) == 2
        assert sync_keys == async_keys


# ---------------------------------------------------------------------------
# N. One default function version, whatever the entry point
# ---------------------------------------------------------------------------


def _inc(x: int) -> int:
    """Add one; the function under test for the default version."""
    return x + 1


class TestDefaultFunctionVersion:
    """Finding N: every entry point defaults to version v0.0."""

    def test_decorator_and_from_fn_agree(self) -> None:
        """function_pod and FunctionPod.from_fn give one uri."""
        decorated = function_pod(output_keys="y", function_name="_inc")(_inc)
        built = FunctionPod.from_fn(_inc, output_keys="y", name="_inc")
        assert decorated.pod.uri == built.uri

    def test_side_effect_decorator_agrees(self) -> None:
        """side_effect_function_pod and function_pod(ctx_arg=...) give one uri."""

        def _inc_ctx(x: int, ctx: Any) -> int:
            """Add one; ignores the context."""
            return x + 1

        pod = side_effect_function_pod(_inc_ctx, output_keys="y")
        decorated = function_pod(
            output_keys="y", function_name="_inc_ctx", ctx_arg="ctx"
        )(_inc_ctx)
        assert isinstance(pod, FunctionPod)
        assert pod.uri == decorated.pod.uri
