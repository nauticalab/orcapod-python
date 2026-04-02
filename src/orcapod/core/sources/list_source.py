from __future__ import annotations

from collections.abc import Callable, Collection
from typing import TYPE_CHECKING, Any, Literal

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.protocols.core_protocols import TagProtocol
from orcapod.types import SchemaLike
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    pass


class ListSource(RootSource):
    """A source backed by a Python list.

    Each element in the list becomes one (tag, packet) pair. The element is
    stored as the packet under ``name``; the tag is either the element's index
    (default) or the dict returned by ``tag_function(element, index)``.
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
        data_schema: SchemaLike | None = None,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id=source_id, **kwargs)

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
                tag_fields = tag_fields.as_dict()
            row = dict(tag_fields)
            row[name] = element
            rows.append(row)

        tag_columns = (
            list(self._expected_tag_keys)
            if self._expected_tag_keys is not None
            else [k for k in (rows[0].keys() if rows else []) if k != name]
        )

        arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
            rows, python_schema=data_schema
        )
        if data_schema is None:
            # No explicit schema — infer nullable from actual values.
            # The type converter defaults all fields to nullable=True; derive
            # the correct flags here so the builder can trust the schema as-is.
            arrow_table = arrow_table.cast(arrow_utils.infer_schema_nullable(arrow_table))

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=tag_columns,
            source_id=self._source_id,
        )

        self._stream = result.stream
        if self._source_id is None:
            self._source_id = result.source_id

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
        """Return identity including class name, field name, elements, and tag
        function hash.
        """
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

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize metadata-only config (data is not serializable)."""
        return {
            "source_type": "list",
            "name": self.name,
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> "ListSource":
        """Not supported — ListSource data cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "ListSource cannot be reconstructed from config — "
            "original list data is not serializable."
        )
