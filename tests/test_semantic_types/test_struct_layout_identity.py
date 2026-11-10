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
    "stream_content": "90dab8a6e5b7b03c83ef20fbfdd4040410f05f9e20d8ca2f64d6f2ef374b8e99",
    "stream_pipeline": "34209011cc4048b6425d8b01c9c11e20dfc27a6f45b40a9948389bced49a76e4",
    "source_content": "7263fb2ff4346ac025631fd34c7204971478d83165084f9a748e61acc5e9f919",
    "source_pipeline": "34209011cc4048b6425d8b01c9c11e20dfc27a6f45b40a9948389bced49a76e4",
    "data_rows": [
        "0000010aa7ec467ed05cf6156b4c537186edb548c2e0e61f09d66dde96d9f644aee318",
        "000001df354be2d86566d498a075294b6cbd403e4566dac064c6306787245d7ecf8004",
    ],
}

# Hashes on main (365a5f74), before the converter change.
MAIN_HASHES: dict[str, dict[str, object]] = {
    "small": _FLAT_HASHES,
    "large": _FLAT_HASHES,
    "nested": {
        "stream_content": "9e7d62a473a0e47a8ca74ad950245a3256248a7ed8994645198d82e4d6b33902",
        "stream_pipeline": "f8745264f2422bf76eaec06120aa00b510cafa0ee3abdfac3d5396a614afb3a1",
        "source_content": "2ee725a65a6050703c4164f901ac06e2137a0e2c71f7cf7b636423a1e1ac2a9b",
        "source_pipeline": "f8745264f2422bf76eaec06120aa00b510cafa0ee3abdfac3d5396a614afb3a1",
        "data_rows": [
            "0000010f2cedeebe0599b55b059f918c148c3c0056dd9848d7fdecfc1eba992c23962f",
            "000001618d4b5959d543202f49171b6c2b89bc5fef30b9bd157c03a13458b3ae882c8c",
        ],
    },
    "list_of_struct": {
        "stream_content": "276d8ec2a45a4e62be7496c9ce33687558647c5369c0828671f51f357a4fb6ea",
        "stream_pipeline": "85b8d45d79762e42d1edf56c3853379193c21d5879a30166fcf3bc3c8e56a733",
        "source_content": "19a5d5780cd84af633704022bf8f7a80ba72555533390ee5e69c487f4ec253cf",
        "source_pipeline": "85b8d45d79762e42d1edf56c3853379193c21d5879a30166fcf3bc3c8e56a733",
        "data_rows": [
            "00000151b3c44eaf49ed64174b9b47f89a779cd478c6874cbcac5a7df049d33ae1a7f9",
            "000001dbb51ff7212e3b1dbe16f90d8d3133c5b8743d76d823fd9c903f6c9d2538eca5",
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
        source = ArrowTableSource(table, tag_columns=["k"], source_id="fixed")
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
