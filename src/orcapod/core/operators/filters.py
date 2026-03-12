import logging
from collections.abc import Collection, Iterable, Mapping
from typing import TYPE_CHECKING, Any, TypeAlias

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import numpy as np
    import polars as pl
    import polars._typing as pl_type
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    pl_type = LazyModule("polars._typing")

logger = logging.getLogger(__name__)

polars_predicate: TypeAlias = "pl_type.IntoExprColumn| Iterable[pl_type.IntoExprColumn]| bool| list[bool]| np.ndarray[Any, Any]"


class PolarsFilter(UnaryOperator):
    """
    Operator that applies Polars filtering to a stream
    """

    def __init__(
        self,
        predicates: Collection[
            "pl_type.IntoExprColumn| Iterable[pl_type.IntoExprColumn]| bool| list[bool]| np.ndarray[Any, Any]"
        ] = (),
        constraints: Mapping[str, Any] | None = None,
        **kwargs,
    ):
        self.predicates = predicates
        self.constraints = constraints if constraints is not None else {}
        super().__init__(**kwargs)

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        if len(self.predicates) == 0 and len(self.constraints) == 0:
            logger.info(
                "No predicates or constraints specified. Returning stream unaltered."
            )
            return stream

        # TODO: improve efficiency here...
        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )
        df = pl.DataFrame(table)
        filtered_table = df.filter(*self.predicates, **self.constraints).to_arrow()

        return ArrowTableStream(
            filtered_table,
            tag_columns=stream.keys()[0],
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # Any valid stream would work
        return

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
        include_system_tags: bool = False,
    ) -> tuple[Schema, Schema]:
        # data types are not modified
        return stream.output_schema(columns=columns, all_info=all_info)

    def to_config(self) -> dict[str, Any]:
        """Serialize this PolarsFilter operator to a config dict.

        Polars ``Expr`` predicates are serialized to JSON strings when possible.
        If any predicate cannot be serialized, ``reconstructable`` is set to
        ``False`` and ``predicates`` is set to ``None``.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys.
        """
        config = super().to_config()
        serialized_predicates = []
        reconstructable = True
        for pred in self.predicates:
            if hasattr(pred, "meta") and hasattr(pred.meta, "serialize"):
                serialized = pred.meta.serialize(format="json")
                # serialize() returns bytes in some Polars versions, str in others
                if isinstance(serialized, bytes):
                    serialized = serialized.decode()
                serialized_predicates.append(serialized)
            else:
                reconstructable = False
                break
        config["config"] = {
            "constraints": dict(self.constraints) if self.constraints else None,
            "predicates": serialized_predicates if reconstructable else None,
            "reconstructable": reconstructable,
        }
        return config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PolarsFilter":
        """Reconstruct a PolarsFilter from a config dict.

        Args:
            config: Dict as returned by ``to_config()``.

        Returns:
            A new ``PolarsFilter`` instance.

        Raises:
            NotImplementedError: If ``reconstructable`` is ``False``.
        """
        inner = config.get("config", {})
        if not inner.get("reconstructable", True):
            raise NotImplementedError(
                "PolarsFilter with non-serializable predicates cannot be reconstructed"
            )
        predicates = []
        if inner.get("predicates"):
            import polars as pl

            predicates = []
            for p in inner["predicates"]:
                # deserialize() accepts bytes in some Polars versions, str in others
                try:
                    predicates.append(pl.Expr.deserialize(p.encode(), format="json"))
                except TypeError:
                    predicates.append(pl.Expr.deserialize(p, format="json"))
        constraints = inner.get("constraints")
        return cls(predicates=predicates, constraints=constraints)

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.predicates,
            self.constraints,
        )


class SelectPacketColumns(UnaryOperator):
    """
    Operator that selects specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.strict = strict
        super().__init__(**kwargs)

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        tag_columns, packet_columns = stream.keys()
        packet_columns_to_drop = [c for c in packet_columns if c not in self.columns]
        new_packet_columns = [
            c for c in packet_columns if c not in packet_columns_to_drop
        ]

        if len(new_packet_columns) == len(packet_columns):
            logger.info("All packet columns are selected. Returning stream unaltered.")
            return stream

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )
        # make sure to drop associated source fields
        associated_source_fields = [
            f"{constants.SOURCE_PREFIX}{c}" for c in packet_columns_to_drop
        ]
        packet_columns_to_drop.extend(associated_source_fields)

        modified_table = table.drop_columns(packet_columns_to_drop)

        return ArrowTableStream(
            modified_table,
            tag_columns=tag_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        _, packet_columns = stream.keys()
        columns_to_select = self.columns
        missing_columns = set(columns_to_select) - set(packet_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing packet columns: {missing_columns}. Make sure all specified columns to select are present or use strict=False to ignore missing columns"
            )

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
        include_system_tags: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, packet_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        _, packet_columns = stream.keys()
        packets_to_drop = [pc for pc in packet_columns if pc not in self.columns]

        # this ensures all system tag columns are preserved
        new_packet_schema = {
            k: v for k, v in packet_schema.items() if k not in packets_to_drop
        }

        return tag_schema, new_packet_schema

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        )
