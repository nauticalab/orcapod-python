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
    "stream_content": "e34f7e47f388b93db0403fe3e882ea4c2c7ce30cdcf7937eb928868e6aeaf93a",
    "stream_pipeline": "0caab8d581b700f6746fcf508ba50945757be2abe7360c6e33fb87836e291b65",
    "source_content": "a1679332b537fe2c2f0d950e71c122e2e25812a6ed8988e1d4dc36a9db9e6b48",
    "source_pipeline": "0caab8d581b700f6746fcf508ba50945757be2abe7360c6e33fb87836e291b65",
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
        "stream_content": "413fa479269c7957fef99caca81336f0d94c0cb240160c831e4b7b719c0a3ff1",
        "stream_pipeline": "705efaca724d9d35569743799eeba69fe910e011c84adfe8d934961c04c61840",
        "source_content": "0babf998742cc24277ee8adc9937f9d4dc2d53ecaf76bf4a5594d0be627132e9",
        "source_pipeline": "705efaca724d9d35569743799eeba69fe910e011c84adfe8d934961c04c61840",
        "data_rows": [
            "000002b734209953036fae7ee935c232f7949f4874dc22dca2af4ecfa2882e5ddaf457",
            "0000024d11adcf6077042679df69776a748d71f2818fc47042913d83fa1fbebca67ad9",
        ],
    },
    "list_of_struct": {
        "stream_content": "145c0f2bdd30c6ded9e29067f7d6fa7649b18712ee75cb9a6c5eb6582500075e",
        "stream_pipeline": "f364ae4074150182fcb7b386f605b6f3b7d4d0d56b7b44d1da439f9ea0eecc83",
        "source_content": "fdc31658092bce777befba057bd843a57382656f443efc597302d26023bc097c",
        "source_pipeline": "f364ae4074150182fcb7b386f605b6f3b7d4d0d56b7b44d1da439f9ea0eecc83",
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
