from __future__ import annotations

from collections.abc import Callable, Collection
from typing import Any, Literal

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.base import RootSource
from orcapod.protocols.core_protocols import TagProtocol
from orcapod.types import ColumnConfig, Schema


class ListSource(RootSource):
    """
    A source backed by a Python list.

    Each element in the list becomes one (tag, packet) pair.  The element is
    stored as the packet under ``name``; the tag is either the element's index
    (default) or the dict returned by ``tag_function(element, index)``.

    The list is converted to an Arrow table at construction time so the same
    ``TableStream`` is returned from every ``process()`` call.  Source-info
    provenance and schema-hash system tags are added via ``ArrowTableSource``.

    Parameters
    ----------
    name:
        PacketProtocol column name under which each list element is stored.
    data:
        The list of elements.
    tag_function:
        Optional callable ``(element, index) -> dict[str, Any]`` producing the
        tag fields for each element.  Defaults to ``{"element_index": index}``.
    tag_function_hash_mode:
        How to identify the tag function for content-hash purposes.
    expected_tag_keys:
        Explicit tag key names (used when ``tag_function`` is provided and the
        keys are known statically).
    """

    @staticmethod
    def _default_tag(element: Any, idx: int) -> dict[str, Any]:
        return {"element_index": idx}

    def __init__(
        self,
        name: str,
        data: list[Any],
        tag_function: Callable[[Any, int], dict[str, Any] | TagProtocol] | None = None,
        expected_tag_keys: Collection[str] | None = None,
        tag_function_hash_mode: Literal["content", "signature", "name"] = "name",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.name = name
        self._elements = list(data)
        self._tag_function_hash_mode = tag_function_hash_mode

        if tag_function is None:
            tag_function = self.__class__._default_tag
            if expected_tag_keys is None:
                expected_tag_keys = ["element_index"]

        self._tag_function = tag_function
        self._expected_tag_keys = (
            tuple(expected_tag_keys) if expected_tag_keys is not None else None
        )

        # Hash the tag function for identity purposes.
        self._tag_function_hash = self._hash_tag_function()

        # Build rows: each row is tag_fields merged with {name: element}.
        rows = []
        for idx, element in enumerate(self._elements):
            tag_fields = tag_function(element, idx)
            if hasattr(tag_fields, "as_dict"):
                tag_fields = tag_fields.as_dict()  # TagProtocol protocol → plain dict
            row = dict(tag_fields)
            row[name] = element
            rows.append(row)

        tag_columns = (
            list(self._expected_tag_keys)
            if self._expected_tag_keys is not None
            else [k for k in (rows[0].keys() if rows else []) if k != name]
        )

        self._arrow_source = ArrowTableSource(
            table=self.data_context.type_converter.python_dicts_to_arrow_table(rows),
            tag_columns=tag_columns,
            data_context=self.data_context,
            config=self.orcapod_config,
        )

    def _hash_tag_function(self) -> str:
        """Produce a stable hash string for the tag function."""
        if self._tag_function_hash_mode == "name":
            fn = self._tag_function
            return f"{fn.__module__}.{fn.__qualname__}"
        elif self._tag_function_hash_mode == "signature":
            import inspect

            return str(inspect.signature(self._tag_function))
        else:  # "content"
            import inspect

            src = inspect.getsource(self._tag_function)
            return self.data_context.semantic_hasher.hash_object(src).to_hex()

    def identity_structure(self) -> Any:
        try:
            elements_repr: Any = tuple(self._elements)
        except TypeError:
            elements_repr = tuple(str(e) for e in self._elements)
        return (
            self.__class__.__name__,
            self.name,
            elements_repr,
            self._tag_function_hash,
        )

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._arrow_source.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._arrow_source.keys(columns=columns, all_info=all_info)

    def iter_packets(self):
        return self._arrow_source.iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._arrow_source.as_table(columns=columns, all_info=all_info)
