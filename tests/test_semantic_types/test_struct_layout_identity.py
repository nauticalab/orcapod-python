"""A struct's Python type does not depend on its children's layouts (ITL-832).

The type converter synthesizes one ``TypedDict`` class per Arrow struct type.
It keys that class by the struct normalized to its large variants
(``arrow_utils.normalize_to_large_types``), so ``struct<b: string>`` and
``struct<b: large_string>`` (hash-identical under the Arrow hasher) map to the
same Python class, and that class maps back to the large-normalized struct.
Without this, a stream with a ``struct<b: string>`` column reported a
different Python type in ``output_schema()`` than in its own ``Data`` rows
(which normalize to large), and operators that normalize their output could
not match their predicted schema.

The hash pins were computed on ``main`` (365a5f74) before the change: keying
the Python class differently must not move any content or pipeline hash.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_type_converter
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.streams import ArrowTableStream

SMALL = pa.struct([("a", pa.int8()), ("b", pa.string())])
LARGE = pa.struct([("a", pa.int8()), ("b", pa.large_string())])
NESTED = pa.struct([("inner", SMALL), ("c", pa.list_(pa.string()))])


def _tables() -> dict[str, pa.Table]:
    """Build the tables whose hashes are pinned below."""
    keys = pa.array([1, 2], pa.int64())
    flat = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    nested = [
        {"inner": {"a": 1, "b": "x"}, "c": ["p"]},
        {"inner": {"a": 2, "b": "y"}, "c": []},
    ]
    return {
        "small": pa.table({"k": keys, "x": pa.array(flat, type=SMALL)}),
        "large": pa.table({"k": keys, "x": pa.array(flat, type=LARGE)}),
        "nested": pa.table({"k": keys, "x": pa.array(nested, type=NESTED)}),
        "list_of_struct": pa.table(
            {
                "k": keys,
                "x": pa.array([[flat[0]], []], type=pa.list_(SMALL)),
            }
        ),
    }


_FLAT_HASHES: dict[str, object] = {
    "stream_content": "2fe5b28e81974699cc443f1894020a04148a420b796f38ae868c8b03c1e95c0b",
    "stream_pipeline": "2de99c2b86b7549f70f1e9979b31b8c5ddd1ab9040a2df9451d5aec358aebdd9",
    "source_content": "127e1faf2a01bcff447441f5b4260512343b752022e79db82896967c2c4f3994",
    "source_pipeline": "2de99c2b86b7549f70f1e9979b31b8c5ddd1ab9040a2df9451d5aec358aebdd9",
    "data_rows": [
        "000002a3b465dfc79ea3f32b39f55417f4c2372d7a3cc7ad63aef00aa9108b21b76b9c",
        "000002e540dc3b59a01d14edc26eebff96453fb1de9018c351676cef8d1f67ee3346b2",
    ],
}

# Hashes on main (365a5f74), before the converter change; the stream content and per-row
# data hashes re-recorded in starfix hash format 2 (arrow_v1, ITL-910).
MAIN_HASHES: dict[str, dict[str, object]] = {
    "small": _FLAT_HASHES,
    "large": _FLAT_HASHES,
    "nested": {
        "stream_content": "3237a7d2a26b1cb8e4b04ef537fdb2fd61a05f52e24e026d2a3165ebe1ac9379",
        "stream_pipeline": "bb128873919dea74c0708a58b36cb01cee6d3fdd33df5b4ccc8738c911edfd7c",
        "source_content": "961b6ebdeeb64d6515b718afd46b31868db52a277699fa2d3b9788df6d61c43d",
        "source_pipeline": "bb128873919dea74c0708a58b36cb01cee6d3fdd33df5b4ccc8738c911edfd7c",
        "data_rows": [
            "000002b734209953036fae7ee935c232f7949f4874dc22dca2af4ecfa2882e5ddaf457",
            "0000024d11adcf6077042679df69776a748d71f2818fc47042913d83fa1fbebca67ad9",
        ],
    },
    "list_of_struct": {
        "stream_content": "2013ec100a93b194922ceeb46d9318a33963acc4647230a9b6c0a30c82e7d407",
        "stream_pipeline": "feb2e8255a96f47b1a76017278fe43bccf9cdba9a55b247401f1227e0dc3f926",
        "source_content": "4905b10d4d1b0e000a5192f13a2c5d3b4fa4d0b6eac6379418d31e20ba2dfa8e",
        "source_pipeline": "feb2e8255a96f47b1a76017278fe43bccf9cdba9a55b247401f1227e0dc3f926",
        "data_rows": [
            "000002c262cfe3995378fe521abbc19b8cd14f10f626b5e5b1259a20f6d4277677fb87",
            "000002188277589a87f65b49271c1a2cf461ebed48ddd2fd3cb943198f420fced6862a",
        ],
    },
}


class TestStructPythonType:
    """Both layouts of a struct map to one Python class."""

    def test_layouts_share_one_class(self) -> None:
        """``struct<b: string>`` and ``struct<b: large_string>`` give the same class."""
        converter = get_default_type_converter()
        assert converter.arrow_type_to_python_type(
            SMALL
        ) is converter.arrow_type_to_python_type(LARGE)

    def test_nested_layouts_share_one_class(self) -> None:
        """Layout differences inside nested structs and lists do not matter either."""
        converter = get_default_type_converter()
        nested_large = pa.struct(
            [("inner", LARGE), ("c", pa.large_list(pa.large_string()))]
        )
        assert converter.arrow_type_to_python_type(
            NESTED
        ) is converter.arrow_type_to_python_type(nested_large)

    def test_width_still_distinguishes_classes(self) -> None:
        """Only layout is ignored: an ``int64`` field is a different struct."""
        converter = get_default_type_converter()
        wide = pa.struct([("a", pa.int64()), ("b", pa.string())])
        assert converter.arrow_type_to_python_type(
            wide
        ) is not converter.arrow_type_to_python_type(SMALL)

    @pytest.mark.parametrize("struct_type", [SMALL, LARGE], ids=str)
    def test_class_maps_back_to_the_large_struct(
        self, struct_type: pa.DataType
    ) -> None:
        """The class converts back to the large-normalized struct, whichever came first."""
        converter = get_default_type_converter()
        python_type = converter.arrow_type_to_python_type(struct_type)
        assert converter.python_type_to_arrow_type(python_type) == LARGE

    def test_stream_schema_matches_its_data_rows(self) -> None:
        """A stream reports the same struct class as the ``Data`` rows it yields.

        ``Data`` normalizes its table to large types, so before the change the
        row-level class differed from the stream-level one on ``main``.
        """
        stream = ArrowTableStream(_tables()["small"], tag_columns=["k"])
        _, data = next(iter(stream.iter_data()))
        assert data.schema()["x"] == stream.output_schema()[1]["x"]


class TestStructHashesUnchanged:
    """Content and pipeline hashes of struct columns are the same as on ``main``."""

    @pytest.mark.parametrize("name", sorted(MAIN_HASHES))
    def test_hashes_match_main(self, name: str) -> None:
        """Stream, source and per-row data hashes equal the values pinned on main."""
        table = _tables()[name]
        stream = ArrowTableStream(table, tag_columns=["k"])
        source = ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="fixed"
        )
        expected = MAIN_HASHES[name]
        assert stream.content_hash().to_hex() == expected["stream_content"]
        assert stream.pipeline_hash().to_hex() == expected["stream_pipeline"]
        assert source.content_hash().to_hex() == expected["source_content"]
        assert source.pipeline_hash().to_hex() == expected["source_pipeline"]
        assert [
            data.content_hash().to_hex() for _, data in stream.iter_data()
        ] == expected["data_rows"]


class TestNullFields:
    """Only layouts are normalized: a ``null`` field stays ``null``."""

    def test_null_field_round_trips(self) -> None:
        """``struct<a: null, b: string>`` maps back to ``struct<a: null, b: large_string>``.

        ``normalize_to_large_types`` turns ``null`` into ``large_string`` by
        default, which is a type change, not a layout change; the converter
        must not apply it.
        """
        converter = get_default_type_converter()
        struct_type = pa.struct([("a", pa.null()), ("b", pa.string())])
        python_type = converter.arrow_type_to_python_type(struct_type)
        assert python_type.__annotations__["b"] is str
        assert python_type.__annotations__["a"] is not str
        assert converter.python_type_to_arrow_type(python_type) == pa.struct(
            [("a", pa.null()), ("b", pa.large_string())]
        )
