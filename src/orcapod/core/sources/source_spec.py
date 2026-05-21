"""SourceSpec — a named schema declaration for pipeline input slots."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import orcapod.contexts as contexts
from orcapod.core.base import ContentIdentifiableBase, LabelableMixin, PipelineElementBase
from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.types import ColumnConfig, Schema

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.core_protocols import StreamProtocol


class SourceSpec(LabelableMixin, ContentIdentifiableBase, PipelineElementBase):
    """A named schema declaration for a pipeline input slot.

    ``SourceSpec`` describes what a pipeline input looks like — its key schema
    and data schema — without referencing any concrete data source. It is used
    as the typed input slot concept for both ``Pipeline`` and ``PipelineJob``.

    Note:
        ``SourceSpec`` is designed to be treated as immutable — all attributes
        are stored as private members and exposed only through read-only
        properties. Immutability is a convention rather than a type-system
        guarantee; external mutation of private attributes is unsupported.

    A ``SourceSpec`` can appear as an upstream in operator chains during a
    ``with Pipeline:`` or ``with PipelineJob:`` recording block. Calling data-
    producing methods (``iter_data``, ``as_table``) raises ``UnboundSourceError``
    until the spec is bound to a concrete source via ``PipelineJob.bind()``.

    Identity and hashing:
        - ``pipeline_hash()`` — schema-only, ignoring ``name``. Matches a
          schema-compatible ``RootSource.pipeline_hash()``, enabling DB path
          reuse across different sources bound to the same spec.
        - ``content_hash()`` — includes ``name``. Two specs with identical
          schemas but different names are distinct elements.

    Args:
        name: Human-readable identifier for this input slot. Used as the
            source label when auto-promoting concrete sources in
            ``PipelineJob``. Must be unique within a pipeline.
        tag_schema: Mapping of tag column names to Python types.
        data_schema: Mapping of data column names to Python types.
        data_context: Optional data context override. Defaults to the default data context.
    """

    def __init__(
        self,
        name: str,
        tag_schema: Schema,
        data_schema: Schema,
        data_context: str | contexts.DataContext | None = None,
    ) -> None:
        super().__init__(data_context=data_context)
        self._name = name
        self._tag_schema = tag_schema
        self._data_schema = data_schema

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """Human-readable name for this input slot."""
        return self._name

    def computed_label(self) -> str | None:
        """Return the spec name as the computed label.

        Implements the ``LabelableMixin.computed_label()`` hook so that
        ``self.label`` resolves to the spec name without needing an explicit
        label assignment.  An explicit ``label`` assignment (via the setter)
        would still take priority, but is not expected for immutable specs.

        Returns:
            The spec name.
        """
        return self._name

    @property
    def tag_schema(self) -> Schema:
        """Key schema for this input slot."""
        return self._tag_schema

    @property
    def data_schema(self) -> Schema:
        """Data schema for this input slot."""
        return self._data_schema

    # ------------------------------------------------------------------
    # ContentIdentifiableBase
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Content identity includes name + both schemas."""
        return ("SourceSpec", self._name, self._tag_schema, self._data_schema)

    # ------------------------------------------------------------------
    # PipelineElementBase
    # ------------------------------------------------------------------

    def pipeline_identity_structure(self) -> Any:
        """Pipeline identity is schema-only (no name).

        Matches ``RootSource.pipeline_identity_structure()`` so that a
        schema-compatible concrete source and this spec share the same
        pipeline hash — and therefore the same DB table paths.
        """
        return (self._tag_schema, self._data_schema)

    # ------------------------------------------------------------------
    # StreamProtocol surface (minimal — no data access)
    # ------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return ``(tag_schema, data_schema)``.

        Args:
            columns: Ignored — SourceSpec always returns the full declared schemas.
            all_info: Ignored — SourceSpec always returns the full declared schemas.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        return (self._tag_schema, self._data_schema)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return ``(tag_keys, data_keys)``.

        Args:
            columns: Ignored.
            all_info: Ignored.

        Returns:
            Tuple of ``(tag_column_names, data_column_names)``.
        """
        return (tuple(self._tag_schema.keys()), tuple(self._data_schema.keys()))

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Raise ``UnboundSourceError`` — spec is not bound to a concrete source.

        Raises:
            UnboundSourceError: Always.
        """
        raise UnboundSourceError(
            f"SourceSpec '{self._name}' is not bound to a concrete source. "
            "Call PipelineJob.bind(sources={...}) to attach a source before running."
        )

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Raise ``UnboundSourceError`` — spec is not bound to a concrete source.

        Raises:
            UnboundSourceError: Always.
        """
        raise UnboundSourceError(
            f"SourceSpec '{self._name}' is not bound to a concrete source. "
            "Call PipelineJob.bind(sources={...}) to attach a source before running."
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self, source: "StreamProtocol") -> None:
        """Check that *source* is schema-compatible with this spec.

        Validates that the source's tag and data schemas have exactly the
        same columns as this spec (no extra, no missing columns). Type
        compatibility is not checked here — Arrow conversion handles
        coercions at runtime.

        Args:
            source: The concrete source to validate.

        Raises:
            SourceSpecMismatchError: If the source schema does not match.
        """
        source_tag, source_data = source.output_schema()

        tag_issues: list[str] = []
        data_issues: list[str] = []

        spec_tag_cols = set(self._tag_schema.keys())
        src_tag_cols = set(source_tag.keys())
        if spec_tag_cols != src_tag_cols:
            missing = spec_tag_cols - src_tag_cols
            extra = src_tag_cols - spec_tag_cols
            if missing:
                tag_issues.append(f"missing tag columns: {sorted(missing)}")
            if extra:
                tag_issues.append(f"unexpected tag columns: {sorted(extra)}")

        spec_data_cols = set(self._data_schema.keys())
        src_data_cols = set(source_data.keys())
        if spec_data_cols != src_data_cols:
            missing = spec_data_cols - src_data_cols
            extra = src_data_cols - spec_data_cols
            if missing:
                data_issues.append(f"missing data columns: {sorted(missing)}")
            if extra:
                data_issues.append(f"unexpected data columns: {sorted(extra)}")

        if tag_issues or data_issues:
            all_issues = tag_issues + data_issues
            raise SourceSpecMismatchError(
                f"SourceSpec '{self._name}' is not compatible with the provided source. "
                + "; ".join(all_issues)
            )

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"SourceSpec(name={self._name!r}, "
            f"tag_schema={dict(self._tag_schema)!r}, "
            f"data_schema={dict(self._data_schema)!r})"
        )
