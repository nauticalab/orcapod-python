"""What a data context can configure the pandas value hash to ignore (ITL-897, ITL-981).

The hash of a ``pd.DataFrame`` / ``pd.Series`` is strict by default. A data context can
name properties to ignore, per type: ``column_order`` and ``attrs`` for a frame;
``series_name`` and ``attrs`` for a Series. The list is saved in the logical type's extension
metadata, so two lists are two logical types. Storage is the same under every list.
``struct_field_order`` is not an option: it acted only on a ``pd.ArrowDtype`` struct column,
which storage cannot read back and so is refused (ITL-1054).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal

import orcapod.contexts as contexts_package
from orcapod.contexts import DataContext, get_default_context
from orcapod.contexts.core import ContextValidationError
from orcapod.contexts.registry import JSONDataContextRegistry
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    PandasDataFrameHandler,
    PandasSeriesHandler,
)
from orcapod.logical_types import LogicalPandasDataFrame, LogicalPandasSeries
from orcapod.logical_types.identifier import LogicalTypeIdentifierError
from orcapod.semantic_types.universal_converter import UniversalTypeConverter

FRAME_NAME = "orcapod.pandas.dataframe"
SERIES_NAME = "orcapod.pandas.series"


def _frame() -> pd.DataFrame:
    """Columns ``a`` (ints) and ``b`` (strings), two rows."""
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


def _with_attrs(value: Any) -> Any:
    """A copy of ``value`` with ``attrs`` set."""
    out = value.copy()
    out.attrs = {"unit": "mV"}
    return out


def _reordered(frame: pd.DataFrame) -> pd.DataFrame:
    """``frame`` with its columns in reverse order."""
    return frame.reindex(columns=list(frame.columns)[::-1])


# option -> two values that differ only in that property
FRAME_PAIRS: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {
    "column_order": (_frame(), _reordered(_frame())),
    "attrs": (_frame(), _with_attrs(_frame())),
}
SERIES_PAIRS: dict[str, tuple[pd.Series, pd.Series]] = {
    "series_name": (pd.Series([1, 2], name="a"), pd.Series([1, 2], name="b")),
    "attrs": (pd.Series([1, 2]), _with_attrs(pd.Series([1, 2]))),
}


def _context(tmp_path: Path, ignore: dict[str, list[str]]) -> DataContext:
    """A data context whose registration file sets the pandas ignore lists."""
    shipped = Path(contexts_package.__file__).parent / "logical_types.json"
    registration = json.loads(shipped.read_text())
    registration["hash_ignore"].update(ignore)
    custom = tmp_path / "logical_types_custom.json"
    custom.write_text(json.dumps(registration))
    return JSONDataContextRegistry(logical_types_file=custom).get_context()


def _converter(context: DataContext) -> UniversalTypeConverter:
    """A type converter whose pandas types and handlers use the given ignore lists."""
    converter = context.type_converter
    assert isinstance(converter, UniversalTypeConverter)
    return converter


class TestHandlers:
    """``hash_ignoring`` is how a cell is hashed under its column's own list."""

    @pytest.mark.parametrize("option", list(FRAME_PAIRS))
    def test_frame_option_makes_the_pair_equal_and_only_then(self, option: str) -> None:
        """A frame option makes its pair hash equal; without it the pair differs."""
        first, second = FRAME_PAIRS[option]
        handler = PandasDataFrameHandler()
        assert handler.hash_ignoring(first, []) != handler.hash_ignoring(second, [])
        assert handler.hash_ignoring(first, [option]) == handler.hash_ignoring(
            second, [option]
        )
        for other in set(FRAME_PAIRS) - {option}:
            assert handler.hash_ignoring(first, [other]) != handler.hash_ignoring(
                second, [other]
            )

    @pytest.mark.parametrize("option", list(SERIES_PAIRS))
    def test_series_option_makes_the_pair_equal_and_only_then(
        self, option: str
    ) -> None:
        """A Series option makes its pair hash equal; without it the pair differs."""
        first, second = SERIES_PAIRS[option]
        handler = PandasSeriesHandler()
        assert handler.hash_ignoring(first, []) != handler.hash_ignoring(second, [])
        assert handler.hash_ignoring(first, [option]) == handler.hash_ignoring(
            second, [option]
        )
        for other in set(SERIES_PAIRS) - {option}:
            assert handler.hash_ignoring(first, [other]) != handler.hash_ignoring(
                second, [other]
            )

    def test_unnamed_and_named_series_are_equal_when_the_name_is_ignored(self) -> None:
        """With ``series_name`` ignored, a named and an unnamed Series hash equal."""
        handler = PandasSeriesHandler(ignore=["series_name"])
        assert handler.handle(pd.Series([1, 2]), None) == handler.handle(  # pyright: ignore[reportArgumentType]  # the hasher argument is unused
            pd.Series([1, 2], name="x"),
            None,  # pyright: ignore[reportArgumentType]
        )

    def test_everything_else_still_counts_when_column_order_is_ignored(self) -> None:
        """Ignoring column order leaves values, labels, dtypes and the index in the hash."""
        handler = PandasDataFrameHandler(ignore=["column_order"])

        def digest(frame: pd.DataFrame) -> bytes:
            """The digest of ``frame`` with column order ignored."""
            return handler.hash_ignoring(frame, ["column_order"]).digest

        base = _frame()
        assert digest(base) == digest(_reordered(base))
        # a value, a label, a label's type, a column's dtype, the index
        assert digest(base) != digest(base.assign(a=[1, 3]))
        assert digest(base) != digest(base.rename(columns={"a": "c"}))
        assert digest(pd.DataFrame({0: [1], 1: [2]})) != digest(
            pd.DataFrame({"0": [1], "1": [2]})
        )
        assert digest(base) != digest(base.astype({"a": "int32"}))
        assert digest(base) != digest(base.set_axis([5, 6], axis="index"))
        # the columns travel with their values: swapping the values of two columns of
        # one dtype is not a reordering
        pair = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        assert digest(pair) != digest(pd.DataFrame({"a": [3, 4], "b": [1, 2]}))

    def test_the_label_is_the_same_under_every_list(self) -> None:
        """The hash label does not depend on the ignore list."""
        handler = PandasDataFrameHandler()
        assert (
            handler.hash_ignoring(_frame(), ["attrs"]).method == "pandas_dataframe_v1"
        )

    def test_a_name_that_is_not_an_option_of_the_type_is_refused(self) -> None:
        """A name the type does not offer raises."""
        with pytest.raises(ValueError, match="series_name"):
            PandasDataFrameHandler(ignore=["series_name"])
        with pytest.raises(ValueError, match="column_order"):
            PandasSeriesHandler(ignore=["column_order"])
        with pytest.raises(ValueError, match="flags"):
            LogicalPandasDataFrame(ignore=["flags"])

    def test_handler_and_logical_type_offer_the_same_options(self) -> None:
        """The handler and the logical type of one pandas type offer the same options."""
        assert PandasDataFrameHandler.OPTIONS == LogicalPandasDataFrame.OPTIONS
        assert PandasSeriesHandler.OPTIONS == LogicalPandasSeries.OPTIONS
        assert PandasDataFrameHandler.OPTIONS == {"attrs", "column_order"}
        assert PandasSeriesHandler.OPTIONS == {"attrs", "series_name"}

    def test_struct_field_order_is_not_an_option(self) -> None:
        """``struct_field_order`` is refused: no value storage accepts has a struct column."""
        for refuse in (
            PandasDataFrameHandler,
            PandasSeriesHandler,
            LogicalPandasDataFrame,
            LogicalPandasSeries,
        ):
            with pytest.raises(
                ValueError, match="unknown property name 'struct_field_order'"
            ):
                refuse(ignore=["struct_field_order"])


class TestIdentifier:
    """The extension identifier of the pandas logical types under each ignore list."""

    def test_the_list_is_in_the_extension_metadata_sorted(self) -> None:
        """The ignore list is written into the extension metadata, sorted."""
        logical = LogicalPandasDataFrame(ignore=["column_order", "attrs"])
        arrow_type = logical.get_arrow_extension_type()
        assert arrow_type.extension_name == FRAME_NAME
        assert (
            arrow_type.__arrow_ext_serialize__()
            == b'{"ignore":["attrs","column_order"],"version":1}'
        )

    def test_two_lists_are_two_arrow_types(self) -> None:
        """Two ignore lists give two Arrow extension types."""
        strict = LogicalPandasSeries().get_arrow_extension_type()
        loose = LogicalPandasSeries(ignore=["series_name"]).get_arrow_extension_type()
        assert strict != loose
        assert strict.__arrow_ext_serialize__() == b'{"ignore":[],"version":1}'

    def test_storage_is_the_same_under_every_list(self) -> None:
        """The stored bytes do not depend on the ignore list."""
        frame = _with_attrs(_reordered(_frame()))
        strict = LogicalPandasDataFrame()
        loose = LogicalPandasDataFrame(ignore=["attrs", "column_order"])
        assert loose.python_to_storage(frame) == strict.python_to_storage(frame)
        back = loose.storage_to_python(loose.python_to_storage(frame))
        assert_frame_equal(back, frame)
        assert list(back.columns) == ["b", "a"]
        assert back.attrs == {"unit": "mV"}


class TestDataContext:
    """The list a data context configures, end to end through its public objects."""

    def test_default_context_ignores_nothing(self) -> None:
        """The default data context ignores no property."""
        hasher = get_default_context().semantic_hasher
        for first, second in [*FRAME_PAIRS.values(), *SERIES_PAIRS.values()]:
            assert hasher.hash_object(first) != hasher.hash_object(second)

    def test_configured_list_is_used_for_bare_values_and_for_cells(
        self, tmp_path: Path
    ) -> None:
        """A configured list applies to a value given to the hasher and to a cell of a table."""
        context = _context(tmp_path, {FRAME_NAME: ["column_order"]})
        first, second = FRAME_PAIRS["column_order"]
        assert context.semantic_hasher.hash_object(
            first
        ) == context.semantic_hasher.hash_object(second)

        def table(value: pd.DataFrame, of: DataContext) -> pa.Table:
            """A one-column table holding ``value`` as a logical value."""
            return of.type_converter.python_dicts_to_arrow_table(
                [{"x": value}], python_schema={"x": pd.DataFrame}
            )

        column_type = table(first, context).schema.field("x").type
        assert isinstance(column_type, pa.ExtensionType)
        assert (
            column_type.__arrow_ext_serialize__()
            == b'{"ignore":["column_order"],"version":1}'
        )
        # A row holding the frame: equal under the configured list ...
        assert context.arrow_hasher.hash_table(
            table(first, context)
        ) == context.arrow_hasher.hash_table(table(second, context))
        # ... and different in the default context.
        default = get_default_context()
        assert default.arrow_hasher.hash_table(
            table(first, default)
        ) != default.arrow_hasher.hash_table(table(second, default))
        # The two lists are two logical types, so the rows differ across contexts too.
        assert context.arrow_hasher.hash_table(
            table(first, context)
        ) != default.arrow_hasher.hash_table(table(first, default))

    def test_series_list_is_configured_separately(self, tmp_path: Path) -> None:
        """The Series list is configured apart from the frame list."""
        context = _context(tmp_path, {SERIES_NAME: ["series_name"]})
        first, second = SERIES_PAIRS["series_name"]
        hasher = context.semantic_hasher
        assert hasher.hash_object(first) == hasher.hash_object(second)
        frames = FRAME_PAIRS["column_order"]
        assert hasher.hash_object(frames[0]) != hasher.hash_object(frames[1])

    def test_a_column_stored_under_another_list_is_refused(
        self, tmp_path: Path
    ) -> None:
        """A column stored under another ignore list is refused on read."""
        loose = _context(tmp_path, {FRAME_NAME: ["column_order"]})
        stored = loose.type_converter.python_dicts_to_arrow_table(
            [{"x": _frame()}], python_schema={"x": pd.DataFrame}
        )
        # As a store hands it back: the storage column with the identifier on the field.
        field = stored.schema.field("x")
        assert isinstance(field.type, pa.ExtensionType)
        chunk = stored.column("x").chunk(0)
        assert isinstance(chunk, pa.ExtensionArray)
        plain = pa.table(
            [chunk.storage],
            schema=pa.schema(
                [
                    pa.field(
                        "x",
                        field.type.storage_type,
                        metadata={
                            b"ARROW:extension:name": FRAME_NAME.encode(),
                            b"ARROW:extension:metadata": (
                                field.type.__arrow_ext_serialize__()
                            ),
                        },
                    )
                ]
            ),
        )
        assert _converter(loose).load_logical_types(plain).schema.field("x").type == (
            field.type
        )
        with pytest.raises(LogicalTypeIdentifierError):
            _converter(get_default_context()).load_logical_types(plain)

    def test_an_unknown_name_in_the_configuration_is_refused(
        self, tmp_path: Path
    ) -> None:
        """A configured name that is not an option raises."""
        with pytest.raises(ContextValidationError, match="flags"):
            _context(tmp_path, {FRAME_NAME: ["flags"]})

    def test_struct_field_order_in_a_data_context_is_refused(
        self, tmp_path: Path
    ) -> None:
        """A data context naming ``struct_field_order`` for a pandas type is refused (ITL-1054)."""
        for name in (FRAME_NAME, SERIES_NAME):
            with pytest.raises(ContextValidationError, match="struct_field_order"):
                _context(tmp_path, {name: ["struct_field_order"]})
